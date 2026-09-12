use super::stats::WorkerLayer as Layer;
use super::{
    config::Resource,
    config::{Config, Mode, SLOTS_PER_EPOCH},
    consensus::{ConsensusTarget, ConsensusWork},
    payload::{ConsensusPayload, RelayPayload},
    recording::{Record, Recording},
    source::{race_sources, Announcement, BeaconSource},
    stats::{self, Shared, Stats},
    target::Target,
    transport::Rpc,
};
use alloy::primitives::B256;
use anyhow::Result;
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use moka::future::Cache;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc, watch, Notify},
    task::JoinSet,
    time::Instant,
};

#[derive(Debug)]
pub(super) struct Work {
    pub payload: Arc<RelayPayload>,
    pub first_seen: Instant,
    pub first_seen_unix_us: u64,
    pub acquired: Instant,
    pub deadline: Instant,
    pub source: String,
    pub announcement_source: String,
    pub event: &'static str,
    pub mode: Mode,
    pub mode_epoch: u64,
    pub first_seen_us: u64,
}

struct Prepared {
    config: Config,
    errors: Vec<(Layer, String, String)>,
    sources: Vec<Arc<BeaconSource>>,
    execution_targets: Vec<Arc<Target>>,
    consensus_targets: Vec<Arc<ConsensusTarget>>,
    decoders: Arc<tokio::sync::Semaphore>,
    proof_workers: Arc<tokio::sync::Semaphore>,
}
impl Prepared {
    fn new(config: Config, previous: Option<Arc<Self>>) -> Result<Self> {
        config.validate()?;
        if !config.consensus_targets.is_empty() {
            // Prepared::new runs on the blocking pool, before any source intake starts.
            let _ = alloy::eips::eip4844::env_settings::EnvKzgSettings::Default.get();
        }
        if let Some(previous) = &previous {
            anyhow::ensure!(
                previous.config.state_dir == config.state_dir,
                "changing the relay state directory requires a process restart"
            );
            anyhow::ensure!(
                previous.config.proof_workers == config.proof_workers,
                "changing proof workers requires a process restart"
            );
        }
        let mut errors = Vec::new();
        let network_identity = format!(
            "{}:{}:{:?}",
            config.network.genesis_validators_root,
            config.network.genesis_time,
            config.network.forks
        );
        let mut coordinators = super::transport::BeaconReadCoordinators::default();
        let sources = config
            .sources
            .iter()
            .filter_map(|(name, config)| {
                match coordinators
                    .get_or_insert(&config.beacon_url, &config.headers, &network_identity)
                    .and_then(|http| BeaconSource::new_with_http(name.clone(), config, Some(http)))
                {
                    Ok(source) => Some(Arc::new(source)),
                    Err(error) => {
                        errors.push((Layer::Source, name.clone(), error.to_string()));
                        None
                    }
                }
            })
            .collect();
        let ttl = Duration::from_secs(2 * SLOTS_PER_EPOCH * config.network.seconds_per_slot);
        let targets = config
            .execution_targets
            .iter()
            .filter_map(|(name, target_config)| {
                let previous_target = previous.as_ref().and_then(|p| {
                    p.config
                        .execution_targets
                        .iter()
                        .find_map(|(old_name, old)| {
                            (super::config::url(&old.engine_url).ok()
                                == super::config::url(&target_config.engine_url).ok())
                            .then(|| p.execution_targets.iter().find(|t| &t.name == old_name))
                            .flatten()
                        })
                });
                let create =
                    || -> Result<Target> {
                        Ok(Target {
                            name: name.clone(),
                            engine: Rpc::with_jwt_file(
                                &target_config.engine_url,
                                &target_config.jwt_secret_path,
                            )?,
                            rpc: Rpc::new(&target_config.rpc_url, None)?,
                            handled: previous_target.map(|t| t.handled.clone()).unwrap_or_else(
                                || Cache::builder().max_capacity(512).time_to_live(ttl).build(),
                            ),
                            confirmed: previous_target.map(|t| t.confirmed.clone()).unwrap_or_else(
                                || Cache::builder().max_capacity(512).time_to_live(ttl).build(),
                            ),
                            probes: tokio::sync::Semaphore::new(4),
                            evidence: Notify::new(),
                        })
                    };
                match create() {
                    Ok(target) => Some(Arc::new(target)),
                    Err(error) => {
                        errors.push((Layer::Execution, name.clone(), error.to_string()));
                        None
                    }
                }
            })
            .collect();
        let decoders = previous
            .as_ref()
            .map(|p| p.decoders.clone())
            .unwrap_or_else(|| Arc::new(tokio::sync::Semaphore::new(4)));
        let proof_workers = previous
            .as_ref()
            .map(|p| p.proof_workers.clone())
            .unwrap_or_else(|| Arc::new(tokio::sync::Semaphore::new(config.proof_workers)));
        let consensus_targets = config
            .consensus_targets
            .iter()
            .filter_map(|(name, c)| {
                match coordinators
                    .get_or_insert(&c.beacon_url, &c.headers, &network_identity)
                    .and_then(|http| {
                        ConsensusTarget::new_with_http(name.clone(), c, ttl, Some(http))
                    }) {
                    Ok(mut target) => {
                        if let Some(old) = previous.as_ref().and_then(|p| {
                            p.config
                                .consensus_targets
                                .iter()
                                .find_map(|(old_name, old)| {
                                    (super::config::url(&old.beacon_url).ok()
                                        == super::config::url(&c.beacon_url).ok())
                                    .then(|| {
                                        p.consensus_targets.iter().find(|t| &t.name == old_name)
                                    })
                                    .flatten()
                                })
                        }) {
                            target.retain_delivery(old);
                        }
                        Some(Arc::new(target))
                    }
                    Err(error) => {
                        errors.push((Layer::Consensus, name.clone(), error.to_string()));
                        None
                    }
                }
            })
            .collect();
        Ok(Self {
            config,
            errors,
            sources,
            execution_targets: targets,
            consensus_targets,
            decoders,
            proof_workers,
        })
    }
}

/// A separate service: monitor endpoints never enter the proxy routing pools.
pub struct BlockRelay {
    desired: watch::Sender<Option<Arc<Prepared>>>,
    mode: watch::Sender<Mode>,
    applied: tokio::sync::Mutex<Option<Config>>,
    stats: Shared,
}
impl BlockRelay {
    pub fn live(&self) -> bool {
        self.stats.lock().supervisor_running
    }
    pub fn ready(&self) -> bool {
        self.stats.lock().ready()
    }
    pub fn config_failed(&self) {
        self.stats.lock().config_error = Some("cannot read or validate relay config".into());
    }
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            desired: watch::channel(None).0,
            mode: watch::channel(Mode::Observe).0,
            applied: tokio::sync::Mutex::new(None),
            stats: Arc::new(Mutex::new(Stats::default())),
        })
    }
    pub fn snapshot(&self) -> sonic_rs::Value {
        let stats = self.stats.lock();
        use sonic_rs::JsonValueMutTrait;
        let mut value = sonic_rs::to_value(&*stats).expect("serializable relay status");
        let object = value.as_object_mut().expect("status object");
        object.insert(
            "operation",
            sonic_rs::to_value(&stats.operation()).expect("operation"),
        );
        object.insert(
            "forwarding_available",
            sonic_rs::to_value(&stats.ready()).expect("availability"),
        );
        value
    }
    pub fn samples(&self) -> Vec<super::stats::Sample> {
        self.stats.lock().samples.iter().cloned().collect()
    }
    pub async fn apply(&self, config: Option<&Config>) -> Result<()> {
        let mut applied = self.applied.lock().await;
        if applied.as_ref() == config {
            self.stats.lock().config_error = None;
            return Ok(());
        }
        if let (Some(old), Some(new)) = (applied.as_ref(), config) {
            let mut old = old.clone();
            old.mode = new.mode;
            if &old == new {
                {
                    let mut stats = self.stats.lock();
                    stats.change_mode(new.mode);
                    stats.config_error = None;
                    self.mode.send_replace(new.mode);
                }
                *applied = Some(new.clone());
                return Ok(());
            }
        }
        let prepared = if let Some(config) = config {
            let config = config.clone();
            let previous = self.desired.borrow().clone();
            match tokio::task::spawn_blocking(move || Prepared::new(config, previous)).await? {
                Ok(prepared) => Some(Arc::new(prepared)),
                Err(error) => {
                    self.stats.lock().config_error = Some(error.to_string());
                    return Err(error);
                }
            }
        } else {
            None
        };
        {
            let mut stats = self.stats.lock();
            let next = config.map(|c| c.mode).unwrap_or_default();
            stats.change_mode(next);
            self.mode.send_replace(next);
        }
        self.desired.send_replace(prepared);
        *applied = config.cloned();
        self.stats.lock().config_error = None;
        Ok(())
    }
    pub async fn run(self: Arc<Self>, chain_id: u64, mut shutdown: broadcast::Receiver<()>) {
        struct Running(Shared);
        impl Drop for Running {
            fn drop(&mut self) {
                let mut s = self.0.lock();
                s.supervisor_running = false;
                s.enabled = false;
            }
        }
        self.stats.lock().supervisor_running = true;
        let _running = Running(self.stats.clone());
        let mut desired = self.desired.subscribe();
        loop {
            let prepared = desired.borrow_and_update().clone();
            let Some(prepared) = prepared else {
                self.stats.lock().enabled = false;
                tokio::select! { _ = shutdown.recv() => return, _ = desired.changed() => continue }
            };
            {
                let mut stats = self.stats.lock();
                stats.enabled = true;
                stats.mode = *self.mode.borrow();
                stats.seconds_per_slot = prepared.config.network.seconds_per_slot;
                stats
                    .sources
                    .retain(|name, _| prepared.config.sources.contains_key(name));
                stats
                    .execution_targets
                    .retain(|name, _| prepared.config.execution_targets.contains_key(name));
                stats
                    .consensus_targets
                    .retain(|name, _| prepared.config.consensus_targets.contains_key(name));
                for name in prepared.config.sources.keys() {
                    stats.sources.entry(name.clone()).or_default().connected = false;
                }
                for name in prepared.config.execution_targets.keys() {
                    stats
                        .execution_targets
                        .entry(name.clone())
                        .or_default()
                        .health
                        .connected = false;
                }
                for name in prepared.config.consensus_targets.keys() {
                    stats
                        .consensus_targets
                        .entry(name.clone())
                        .or_default()
                        .health
                        .connected = false;
                }
                for (layer, name, detail) in &prepared.errors {
                    stats.worker_error(*layer, name, detail);
                }
            }
            let (stop, stop_rx) = watch::channel(false);
            let generation = run_generation(
                prepared,
                chain_id,
                self.mode.subscribe(),
                self.stats.clone(),
                stop_rx,
            );
            tokio::pin!(generation);
            let exit = tokio::select! {
                _ = shutdown.recv() => true,
                _ = desired.changed() => false,
                result = &mut generation => {
                    self.stats.lock().enabled = false;
                    self.stats.lock().config_error = Some(format!("relay worker stopped: {}", result.err().map(|e| e.to_string()).unwrap_or_default()));
                    tokio::select! { _ = shutdown.recv() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    continue;
                }
            };
            self.stats.lock().enabled = false;
            stop.send_replace(true);
            // Finish an outstanding Engine request before starting a replacement target worker.
            let _ = generation.await;
            if exit {
                self.stats.lock().enabled = false;
                return;
            }
        }
    }
}

enum ConsensusAcquisition {
    Complete(Arc<ConsensusPayload>, String),
    SkippedAllTargetsKnown,
}

async fn acquire_consensus(
    prepared: Arc<Prepared>,
    work: Arc<Work>,
    notify: Arc<Notify>,
    stats: Shared,
) -> Result<ConsensusAcquisition> {
    if work.payload.blob_commitments.is_empty() {
        let block = work.payload.clone();
        let permit = prepared.proof_workers.clone().acquire_owned().await?;
        let payload = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            ConsensusPayload::from_blobs(block, Vec::new())
        })
        .await??;
        return Ok(ConsensusAcquisition::Complete(
            Arc::new(payload),
            work.source.clone(),
        ));
    }
    // Execution clients can serve complete blobs locally. Try each eligible
    // endpoint once before paying for a Beacon blob request.
    let local = futures_util::stream::iter(prepared.execution_targets.iter().cloned())
        .map(|target| {
            let hashes = work.payload.versioned_hashes();
            async move {
                target
                    .engine
                    .get_blobs_v2(&hashes)
                    .await
                    .map(|v| (target, v))
            }
        })
        .buffer_unordered(prepared.execution_targets.len().max(1));
    tokio::pin!(local);
    while let Some(result) = local.next().await {
        if let Ok((target, Some(blobs))) = result {
            if let Ok(payload) =
                build_consensus_payload(work.payload.clone(), blobs, prepared.proof_workers.clone())
                    .await
            {
                return Ok(ConsensusAcquisition::Complete(
                    Arc::new(payload),
                    format!("engine:{}", target.name),
                ));
            }
        }
    }
    if !prepared.consensus_targets.is_empty()
        && prepared
            .consensus_targets
            .iter()
            .all(|target| target.confirmed(work.payload.beacon_root))
    {
        return Ok(ConsensusAcquisition::SkippedAllTargetsKnown);
    }
    race_sources(
        &prepared.sources,
        work.deadline,
        &notify,
        Resource::Blob,
        Duration::from_millis(prepared.config.rpc.metered_fallback_delay_ms),
        |source| {
            let block = work.payload.clone();
            let workers = prepared.proof_workers.clone();
            let stats = stats.clone();
            async move {
                let bytes = source
                    .read_measured(
                        &format!("/eth/v1/beacon/blobs/{}", block.beacon_root),
                        "blobs",
                        stats,
                    )
                    .await?;
                let permit = workers.acquire_owned().await?;
                let payload = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    #[derive(serde::Deserialize)]
                    struct Blobs {
                        data: Vec<alloy::primitives::Bytes>,
                    }
                    let blobs: Blobs = sonic_rs::from_slice(&bytes)
                        .map_err(|_| anyhow::anyhow!("invalid blob response"))?;
                    ConsensusPayload::from_blobs(block, blobs.data)
                })
                .await??;
                Ok(Arc::new(payload))
            }
        },
    )
    .await
    .map(|(payload, source)| ConsensusAcquisition::Complete(payload, source))
}

async fn build_consensus_payload(
    block: Arc<RelayPayload>,
    blobs: Vec<alloy::primitives::Bytes>,
    workers: Arc<tokio::sync::Semaphore>,
) -> Result<ConsensusPayload> {
    // Keep KZG work on the bounded proof pool. The transport response proof is
    // intentionally ignored; ConsensusPayload recomputes and checks proofs.
    let permit = workers.acquire_owned().await?;
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        ConsensusPayload::from_blobs(block, blobs)
    })
    .await??;
    Ok(result)
}

async fn acquire(
    sources: Vec<Arc<BeaconSource>>,
    network: super::config::Network,
    announcement: Announcement,
    notify: Arc<Notify>,
    decoders: Arc<tokio::sync::Semaphore>,
    stats: Shared,
) -> Result<(Arc<RelayPayload>, String)> {
    let deadline = announcement.at + Duration::from_secs(network.seconds_per_slot);
    race_sources(
        &sources,
        deadline,
        &notify,
        Resource::Block,
        Duration::ZERO,
        |source| {
            let network = network.clone();
            let decoders = decoders.clone();
            let root = announcement.root;
            let stats = stats.clone();
            async move {
                let bytes = source
                    .read_measured(&format!("/eth/v2/beacon/blocks/{root}"), "block", stats)
                    .await?;
                let permit = decoders.acquire_owned().await?;
                let payload = tokio::task::spawn_blocking(move || {
                    // A canceled request must not release capacity while its decoder still runs.
                    let _permit = permit;
                    RelayPayload::decode(&bytes, root, &network)
                })
                .await??;
                Ok(Arc::new(payload))
            }
        },
    )
    .await
}

async fn run_generation(
    prepared: Arc<Prepared>,
    chain_id: u64,
    mode: watch::Receiver<Mode>,
    stats: Shared,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let (record_tx, record_rx) = mpsc::channel(1024);
    {
        let mut s = stats.lock();
        s.recorder = Some(record_tx);
        s.telemetry.configured();
        s.record(Record::Configured {
            sources: prepared.config.sources.keys().cloned().collect(),
            execution_targets: prepared.config.execution_targets.keys().cloned().collect(),
            consensus_targets: prepared.config.consensus_targets.keys().cloned().collect(),
            genesis_time: prepared.config.network.genesis_time,
            seconds_per_slot: prepared.config.network.seconds_per_slot,
        });
    }
    let mut recording = tokio::spawn(Recording::supervise(
        prepared.config.state_dir.clone(),
        record_rx,
        stats.clone(),
    ));
    let ttl = Duration::from_secs(2 * SLOTS_PER_EPOCH * prepared.config.network.seconds_per_slot);
    let payloads = Cache::<B256, Arc<RelayPayload>>::builder()
        .max_capacity(prepared.config.cache_max_bytes)
        .weigher(|_, p| p.cache_bytes())
        .time_to_live(ttl)
        .build();
    let seen = Cache::<B256, ()>::builder()
        .max_capacity(512)
        .time_to_live(ttl)
        .build();
    let (announcements, mut incoming) = mpsc::channel::<Announcement>(256);
    let (delivery, _) = broadcast::channel::<Arc<Work>>(128);
    let (consensus_delivery, _) = broadcast::channel::<Arc<ConsensusWork>>(128);
    let consensus_cache = Cache::<B256, Arc<ConsensusPayload>>::builder()
        .max_capacity(prepared.config.cache_max_bytes)
        .weigher(|_, p| {
            (p.body.len() as u64 + u64::from(p.block.cache_bytes())).min(u32::MAX as u64) as u32
        })
        .time_to_live(ttl)
        .build();
    let mut source_tasks = JoinSet::new();
    let mut telemetry_tasks = JoinSet::new();
    let mut target_tasks = JoinSet::new();
    let mut acquisitions = JoinSet::new();
    let mut observations = JoinSet::new();
    let mut blob_acquisitions = JoinSet::new();
    let mut blobs_in_progress = HashMap::<B256, Arc<Notify>>::new();
    let mut in_progress = HashMap::<B256, Arc<Notify>>::new();
    let (worker_stop, worker_stop_rx) = watch::channel(false);
    telemetry_tasks.spawn(super::telemetry::monitor_resources(
        stats.clone(),
        worker_stop_rx.clone(),
    ));
    for source in &prepared.sources {
        let source = source.clone();
        let network = prepared.config.network.clone();
        let announcements = announcements.clone();
        let stats = stats.clone();
        source_tasks.spawn(supervise(
            Layer::Source,
            source.name.clone(),
            stats.clone(),
            worker_stop_rx.clone(),
            move || {
                source
                    .clone()
                    .run(network.clone(), announcements.clone(), stats.clone())
            },
        ));
    }
    for target in &prepared.execution_targets {
        let target = target.clone();
        telemetry_tasks.spawn(
            target.clone().observe_heads(
                prepared.config.execution_targets[&target.name]
                    .ws_url
                    .clone(),
                stats.clone(),
                worker_stop_rx.clone(),
            ),
        );
        let delivery = delivery.clone();
        let context = super::target::WorkerContext {
            chain_id,
            stop: worker_stop_rx.clone(),
            mode: mode.clone(),
            cache: payloads.clone(),
            stats: stats.clone(),
        };
        target_tasks.spawn(supervise(
            Layer::Execution,
            target.name.clone(),
            stats.clone(),
            worker_stop_rx.clone(),
            move || target.clone().run(delivery.subscribe(), context.clone()),
        ));
    }
    for target in &prepared.consensus_targets {
        let target = target.clone();
        let delivery = consensus_delivery.clone();
        let context = super::consensus::Context {
            network: prepared.config.network.clone(),
            stop: worker_stop_rx.clone(),
            mode: mode.clone(),
            cache: consensus_cache.clone(),
            stats: stats.clone(),
        };
        target_tasks.spawn(supervise(
            Layer::Consensus,
            target.name.clone(),
            stats.clone(),
            worker_stop_rx.clone(),
            move || target.clone().run(delivery.subscribe(), context.clone()),
        ));
    }
    let result = loop {
        tokio::select! {
            _ = stop.changed() => break Ok(()),
            _ = observations.join_next(), if !observations.is_empty() => {},
            event = incoming.recv() => {
                let Some(event) = event else { break Err(anyhow::anyhow!("all Beacon sources stopped")); };
                {
                    let mut s = stats.lock();
                    let observed_us = s.telemetry.at(event.at);
                    s.record(Record::Announcement { root: event.root, slot: event.slot,
                        source: event.source.clone(), event: event.kind, at_unix_us: event.at_unix_us, observed_us });
                }
                let current = prepared.config.network.slot();
                if event.slot > current.saturating_add(1) || current.saturating_sub(event.slot) > 2 * SLOTS_PER_EPOCH {
                    stats.lock().stale_events += 1; continue;
                }
                if let Some(notify) = blobs_in_progress.get(&event.root) { notify.notify_one(); }
                if seen.contains_key(&event.root) { stats.lock().duplicates += 1; continue; }
                if let Some(notify) = in_progress.get(&event.root) {
                    notify.notify_one(); stats.lock().duplicates += 1; continue;
                }
                if acquisitions.len() >= 8 { stats.lock().acquisition_dropped += 1; continue; }
                let notify = Arc::new(Notify::new()); in_progress.insert(event.root, notify.clone());
                let sources = prepared.sources.clone(); let network = prepared.config.network.clone();
                let decoders = prepared.decoders.clone();
                let acquisition_stats = stats.clone();
                let started_mode_epoch = stats.lock().telemetry.mode_epoch;
                acquisitions.spawn(async move {
                    let result = acquire(sources, network, event.clone(), notify, decoders, acquisition_stats).await;
                    (event, started_mode_epoch, result)
                });
            }
            acquired = acquisitions.join_next(), if !acquisitions.is_empty() => {
                let Some(Ok((event, started_mode_epoch, acquired))) = acquired else {
                    stats.lock().acquisition_failed += 1;
                    in_progress.retain(|_, notify| Arc::strong_count(notify) > 1);
                    continue;
                };
                in_progress.remove(&event.root);
                let (payload, source) = match acquired {
                    Ok(value) => value,
                    Err(_) => { let mut s = stats.lock(); s.acquisition_failed += 1; s.acquisition.failure("block acquisition failed");
                        s.record(Record::AcquisitionFailed { root: event.root, slot: event.slot, consensus: false, started_mode_epoch }); continue; }
                };
                if payload.slot != event.slot { let mut s = stats.lock(); s.acquisition_failed += 1; s.acquisition.failure("block slot mismatch");
                    s.record(Record::AcquisitionFailed { root: event.root, slot: event.slot, consensus: false, started_mode_epoch }); continue; }
                seen.insert(event.root, ()).await;
                payloads.insert(payload.hash, payload.clone()).await;
                let acquired = Instant::now();
                let mode_epoch = started_mode_epoch;
                let first_seen_us = stats.lock().telemetry.at(event.at);
                let work = Arc::new(Work { payload, first_seen: event.at, acquired,
                    first_seen_unix_us: event.at_unix_us,
                    deadline: event.at + Duration::from_secs(prepared.config.network.seconds_per_slot),
                    source, announcement_source: event.source, event: event.kind, mode: *mode.borrow(), mode_epoch, first_seen_us });
                {
                    let mut s = stats.lock(); s.acquired += 1; s.acquisition.success(work.payload.slot, "verified block acquired");
                    s.acquisition_latency.record(stats::micros(acquired.duration_since(event.at)));
                    s.record(Record::Acquired { started_mode_epoch, root: work.payload.beacon_root, hash: work.payload.hash,
                        slot: work.payload.slot, number: work.payload.number, first_seen_us,
                        acquired_us: stats::micros(acquired.duration_since(event.at)), source: work.source.clone(),
                        blob_count: work.payload.blob_commitments.len() });
                }
                let _ = delivery.send(work.clone());
                if !prepared.consensus_targets.is_empty() {
                    if blob_acquisitions.len() < 8 {
                        let notify = Arc::new(Notify::new());
                        blobs_in_progress.insert(work.payload.beacon_root, notify.clone());
                        let prepared = prepared.clone(); let work = work.clone();
                        let blob_stats = stats.clone();
                        blob_acquisitions.spawn(async move {
                            let result = tokio::time::timeout_at(work.deadline, acquire_consensus(prepared, work.clone(), notify, blob_stats)).await;
                            (work, result)
                        });
                    } else { stats.lock().consensus_acquisition_dropped += 1; }
                }
                if observations.len() >= 128 { stats.lock().observation_dropped += 1; continue; }
                // Probes cannot delay fanout. They use each target's direct RPC endpoint.
                let targets = prepared.execution_targets.clone(); let stats = stats.clone();
                let consensus_targets = prepared.consensus_targets.clone();
                let count = prepared.config.execution_targets.len() + prepared.config.consensus_targets.len();
                observations.spawn(async move {
                    let mut probes = FuturesUnordered::new();
                    for target in &targets { probes.push(target.observe(work.clone(), stats.clone()).boxed()); }
                    for target in &consensus_targets { probes.push(target.observe(work.clone(), stats.clone()).boxed()); }
                    let mut ready = Vec::new();
                    let mut canonical = Vec::new();
                    while let Some(result) = probes.next().await {
                        if let Some(value) = result.ready { ready.push(value); }
                        if let Some(value) = result.canonical { canonical.push(value); }
                    }
                    let mut s = stats.lock();
                    if work.mode_epoch == s.telemetry.mode_epoch && work.mode == s.mode {
                        let mode = s.telemetry.mode(work.mode);
                        if ready.len() == count && !ready.is_empty() {
                            let last = *ready.iter().max().expect("targets");
                            mode.fleet_ready.record(last);
                            mode.fleet_spread.record(last - *ready.iter().min().expect("targets"));
                        }
                        if canonical.len() == count && !canonical.is_empty() {
                            mode.fleet_canonical.record(*canonical.iter().max().expect("targets"));
                        } else { mode.fleet_incomplete += 1; }
                    }
                    if ready.len() == count {
                        let last = *ready.iter().max().expect("nonempty targets");
                        let first = *ready.iter().min().expect("nonempty targets");
                        s.fleet_ready_latency.record(last); s.fleet_spread.record(last - first);
                    } else { s.fleet_incomplete += 1; }
                    if canonical.len() == count { s.fleet_canonical_latency.record(*canonical.iter().max().expect("nonempty targets")); }
                    else { s.fleet_canonical_incomplete += 1; }
                });
            }
            acquired = blob_acquisitions.join_next(), if !blob_acquisitions.is_empty() => {
                let Some(Ok((work, result))) = acquired else {
                    stats.lock().consensus_acquisition_failed += 1;
                    blobs_in_progress.retain(|_, notify| Arc::strong_count(notify) > 1);
                    continue;
                };
                blobs_in_progress.remove(&work.payload.beacon_root);
                match result {
                    Ok(Ok(ConsensusAcquisition::Complete(payload, blob_source))) => {
                        { let mut s = stats.lock(); s.consensus_acquired += 1; s.consensus_acquisition.success(work.payload.slot, "complete blobs and proofs acquired");
                          s.record(Record::BlobReady { root: work.payload.beacon_root, slot: work.payload.slot,
                            mode: work.mode, mode_epoch: work.mode_epoch, blob_count: work.payload.blob_commitments.len(), source: blob_source, ready_us: stats::micros(work.first_seen.elapsed()) });
                          s.consensus_acquisition_latency.record(stats::micros(work.first_seen.elapsed())); }
                        consensus_cache.insert(payload.block.beacon_root, payload.clone()).await;
                        let _ = consensus_delivery.send(Arc::new(ConsensusWork { work, payload }));
                    }
                    Ok(Ok(ConsensusAcquisition::SkippedAllTargetsKnown)) => {
                        let mut s = stats.lock();
                        s.consensus_blob_skipped += 1;
                        s.consensus_acquisition.success(work.payload.slot, "all consensus targets already known");
                    }
                    _ => { let mut s = stats.lock(); s.consensus_acquisition_failed += 1; s.consensus_acquisition.failure("complete blobs or proofs unavailable");
                        s.record(Record::AcquisitionFailed { root: work.payload.beacon_root, slot: work.payload.slot, consensus: true, started_mode_epoch: work.mode_epoch }); },
                }
            }
        }
    };
    source_tasks.abort_all();
    acquisitions.abort_all();
    blob_acquisitions.abort_all();
    observations.abort_all();
    // Stop only for shutdown or configuration replacement. Drain in-flight imports.
    drop(delivery);
    drop(consensus_delivery);
    worker_stop.send_replace(true);
    while telemetry_tasks.join_next().await.is_some() {}
    while target_tasks.join_next().await.is_some() {}
    while source_tasks.join_next().await.is_some() {}
    while blob_acquisitions.join_next().await.is_some() {}
    while acquisitions.join_next().await.is_some() {}
    while observations.join_next().await.is_some() {}
    {
        let mut s = stats.lock();
        s.record_totals();
        s.recorder.take();
    }
    if tokio::time::timeout(Duration::from_secs(2), &mut recording)
        .await
        .is_err()
    {
        recording.abort();
    }
    result
}

/// A worker owns no durable delivery history. Restart only this endpoint after a panic or exit.
pub(super) async fn supervise<F, Fut>(
    layer: Layer,
    name: String,
    stats: Shared,
    mut stop: watch::Receiver<bool>,
    mut run: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        if *stop.borrow() {
            return;
        }
        let result = std::panic::AssertUnwindSafe(run()).catch_unwind().await;
        if *stop.borrow() {
            return;
        }
        stats.lock().worker_error(
            layer,
            &name,
            if result.is_err() {
                "worker panicked; restarting"
            } else {
                "worker exited; restarting"
            },
        );
        tokio::select! { _ = stop.changed() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    }
}
