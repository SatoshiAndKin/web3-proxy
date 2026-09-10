use super::{
    config::{Config, Mode, SLOTS_PER_EPOCH},
    consensus::{ConsensusTarget, ConsensusWork},
    journal::StateStore,
    payload::{ConsensusPayload, RelayPayload},
    source::{race_sources, Announcement, BeaconSource},
    stats::{self, Shared, Stats},
    target::Target,
    transport::Rpc,
};
use alloy::primitives::B256;
use alloy_rpc_types_engine::JwtSecret;
use anyhow::Result;
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use moka::future::Cache;
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc, watch, Notify},
    task::JoinSet,
    time::Instant,
};

#[derive(Debug)]
pub(super) struct Work {
    pub payload: Arc<RelayPayload>,
    pub first_seen: Instant,
    pub acquired: Instant,
    pub deadline: Instant,
    pub source: String,
    pub announcement_source: String,
    pub event: &'static str,
    pub mode: Mode,
    pub known: BTreeMap<String, AtomicBool>,
}

struct Prepared {
    config: Config,
    store: Arc<StateStore>,
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
        let store = match &previous {
            Some(previous) => previous.store.clone(),
            None => StateStore::open(&config.state_dir)?,
        };
        let sources = config
            .sources
            .iter()
            .map(|(name, config)| BeaconSource::new(name.clone(), config).map(Arc::new))
            .collect::<Result<_>>()?;
        let targets = config
            .execution_targets
            .iter()
            .map(|(name, config)| {
                let jwt = JwtSecret::from_file(&config.jwt_secret_path)
                    .map_err(|_| anyhow::anyhow!("cannot read a target JWT secret"))?;
                Ok(Arc::new(Target {
                    name: name.clone(),
                    engine: Rpc::new(&config.engine_url, Some(jwt))?,
                    rpc: Rpc::new(&config.rpc_url, None)?,
                    journal: store.journal(&config.engine_url)?,
                    probes: tokio::sync::Semaphore::new(4),
                }))
            })
            .collect::<Result<_>>()?;
        let decoders = previous
            .as_ref()
            .map(|p| p.decoders.clone())
            .unwrap_or_else(|| Arc::new(tokio::sync::Semaphore::new(4)));
        let proof_workers = previous
            .as_ref()
            .map(|p| p.proof_workers.clone())
            .unwrap_or_else(|| Arc::new(tokio::sync::Semaphore::new(config.proof_workers)));
        let ttl = Duration::from_secs(2 * SLOTS_PER_EPOCH * config.network.seconds_per_slot);
        let consensus_targets = config
            .consensus_targets
            .iter()
            .map(|(name, config)| ConsensusTarget::new(name.clone(), config, ttl).map(Arc::new))
            .collect::<Result<_>>()?;
        Ok(Self {
            config,
            store,
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
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            desired: watch::channel(None).0,
            mode: watch::channel(Mode::Observe).0,
            applied: tokio::sync::Mutex::new(None),
            stats: Arc::new(Mutex::new(Stats::default())),
        })
    }
    pub fn snapshot(&self) -> sonic_rs::Value {
        sonic_rs::to_value(&*self.stats.lock()).expect("serializable relay status")
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
                self.mode.send_replace(new.mode);
                {
                    let mut stats = self.stats.lock();
                    stats.mode = new.mode;
                    stats.config_error = None;
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
        self.mode
            .send_replace(config.map(|c| c.mode).unwrap_or_default());
        self.desired.send_replace(prepared);
        *applied = config.cloned();
        self.stats.lock().config_error = None;
        Ok(())
    }
    pub async fn run(self: Arc<Self>, chain_id: u64, mut shutdown: broadcast::Receiver<()>) {
        let mut desired = self.desired.subscribe();
        loop {
            let prepared = desired.borrow_and_update().clone();
            let Some(prepared) = prepared else {
                self.stats.lock().enabled = false;
                tokio::select! { _ = shutdown.recv() => return, _ = desired.changed() => continue }
            };
            {
                let mut stats = self.stats.lock();
                *stats = Stats {
                    enabled: true,
                    mode: *self.mode.borrow(),
                    sources: prepared
                        .sources
                        .iter()
                        .map(|s| (s.name.clone(), Default::default()))
                        .collect(),
                    execution_targets: prepared
                        .execution_targets
                        .iter()
                        .map(|t| (t.name.clone(), Default::default()))
                        .collect(),
                    consensus_targets: prepared
                        .consensus_targets
                        .iter()
                        .map(|t| (t.name.clone(), Default::default()))
                        .collect(),
                    ..Default::default()
                };
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
                    self.stats.lock().config_error = Some(format!("relay worker stopped: {}", result.err().map(|e| e.to_string()).unwrap_or_default()));
                    tokio::select! { _ = shutdown.recv() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    continue;
                }
            };
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

async fn acquire_consensus(
    prepared: Arc<Prepared>,
    work: Arc<Work>,
    notify: Arc<Notify>,
) -> Result<(Arc<ConsensusPayload>, String)> {
    if work.payload.blob_commitments.is_empty() {
        let block = work.payload.clone();
        let permit = prepared.proof_workers.clone().acquire_owned().await?;
        let payload = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            ConsensusPayload::from_blobs(block, Vec::new())
        })
        .await??;
        return Ok((Arc::new(payload), work.source.clone()));
    }
    race_sources(&prepared.sources, work.deadline, &notify, |source| {
        let block = work.payload.clone();
        let workers = prepared.proof_workers.clone();
        async move {
            let bytes = source
                .get_bytes(&format!("/eth/v1/beacon/blobs/{}", block.beacon_root))
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
    })
    .await
}

async fn acquire(
    sources: Vec<Arc<BeaconSource>>,
    network: super::config::Network,
    announcement: Announcement,
    notify: Arc<Notify>,
    decoders: Arc<tokio::sync::Semaphore>,
) -> Result<(Arc<RelayPayload>, String)> {
    let deadline = announcement.at + Duration::from_secs(network.seconds_per_slot);
    race_sources(&sources, deadline, &notify, |source| {
        let network = network.clone();
        let decoders = decoders.clone();
        let root = announcement.root;
        async move {
            let bytes = source
                .get_bytes(&format!("/eth/v2/beacon/blocks/{root}"))
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
    })
    .await
}

async fn run_generation(
    prepared: Arc<Prepared>,
    chain_id: u64,
    mode: watch::Receiver<Mode>,
    stats: Shared,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
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
    let mut target_tasks = JoinSet::new();
    let mut acquisitions = JoinSet::new();
    let mut observations = JoinSet::new();
    let mut blob_acquisitions = JoinSet::new();
    let mut blobs_in_progress = HashMap::<B256, Arc<Notify>>::new();
    let mut in_progress = HashMap::<B256, Arc<Notify>>::new();
    let (worker_stop, worker_stop_rx) = watch::channel(false);
    for source in &prepared.sources {
        source_tasks.spawn(source.clone().run(
            prepared.config.network.clone(),
            announcements.clone(),
            stats.clone(),
        ));
    }
    for target in &prepared.execution_targets {
        target_tasks.spawn(target.clone().run(
            delivery.subscribe(),
            super::target::WorkerContext {
                chain_id,
                stop: worker_stop_rx.clone(),
                mode: mode.clone(),
                cache: payloads.clone(),
                stats: stats.clone(),
                ttl,
            },
        ));
    }
    for target in &prepared.consensus_targets {
        target_tasks.spawn(target.clone().run(
            consensus_delivery.subscribe(),
            super::consensus::Context {
                network: prepared.config.network.clone(),
                stop: worker_stop_rx.clone(),
                mode: mode.clone(),
                cache: consensus_cache.clone(),
                stats: stats.clone(),
                ttl,
            },
        ));
    }
    drop(announcements);
    let result = loop {
        tokio::select! {
            _ = stop.changed() => break Ok(()),
            ended = source_tasks.join_next(), if !source_tasks.is_empty() => break Err(anyhow::anyhow!("source worker stopped: {}", ended.is_some())),
            ended = target_tasks.join_next(), if !target_tasks.is_empty() => break Err(anyhow::anyhow!("target worker stopped: {}", ended.is_some())),
            _ = observations.join_next(), if !observations.is_empty() => {},
            event = incoming.recv() => {
                let Some(event) = event else { break Err(anyhow::anyhow!("all Beacon sources stopped")); };
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
                acquisitions.spawn(async move {
                    let result = acquire(sources, network, event.clone(), notify, decoders).await;
                    (event, result)
                });
            }
            acquired = acquisitions.join_next(), if !acquisitions.is_empty() => {
                let Some(Ok((event, acquired))) = acquired else { break Err(anyhow::anyhow!("acquisition worker failed")); };
                in_progress.remove(&event.root);
                let (payload, source) = match acquired {
                    Ok(value) => value,
                    Err(_) => { stats.lock().acquisition_failed += 1; continue; }
                };
                if payload.slot != event.slot { stats.lock().acquisition_failed += 1; continue; }
                seen.insert(event.root, ()).await;
                payloads.insert(payload.hash, payload.clone()).await;
                let acquired = Instant::now();
                let work = Arc::new(Work { payload, first_seen: event.at, acquired,
                    deadline: event.at + Duration::from_secs(prepared.config.network.seconds_per_slot),
                    source, announcement_source: event.source, event: event.kind, mode: *mode.borrow(),
                    known: prepared.execution_targets.iter().map(|t| (t.name.clone(), AtomicBool::new(false))).collect() });
                {
                    let mut s = stats.lock(); s.acquired += 1;
                    s.acquisition_latency.record(stats::micros(acquired.duration_since(event.at)));
                }
                let _ = delivery.send(work.clone());
                if !prepared.consensus_targets.is_empty() {
                    if blob_acquisitions.len() < 8 {
                        let notify = Arc::new(Notify::new());
                        blobs_in_progress.insert(work.payload.beacon_root, notify.clone());
                        let prepared = prepared.clone(); let work = work.clone();
                        blob_acquisitions.spawn(async move {
                            let result = tokio::time::timeout_at(work.deadline, acquire_consensus(prepared, work.clone(), notify)).await;
                            (work, result)
                        });
                    } else { stats.lock().consensus_acquisition_dropped += 1; }
                }
                if observations.len() >= 128 { stats.lock().observation_dropped += 1; continue; }
                // Probes cannot delay fanout. They use each target's direct RPC endpoint.
                let targets = prepared.execution_targets.clone(); let stats = stats.clone();
                let consensus_targets = prepared.consensus_targets.clone();
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
                    let count = targets.len() + consensus_targets.len();
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
                let Some(Ok((work, result))) = acquired else { break Err(anyhow::anyhow!("blob worker failed")); };
                blobs_in_progress.remove(&work.payload.beacon_root);
                match result {
                    Ok(Ok((payload, _blob_source))) => {
                        { let mut s = stats.lock(); s.consensus_acquired += 1;
                          s.consensus_acquisition_latency.record(stats::micros(work.first_seen.elapsed())); }
                        consensus_cache.insert(payload.block.beacon_root, payload.clone()).await;
                        let _ = consensus_delivery.send(Arc::new(ConsensusWork { work, payload }));
                    }
                    _ => stats.lock().consensus_acquisition_failed += 1,
                }
            }
        }
    };
    source_tasks.abort_all();
    acquisitions.abort_all();
    blob_acquisitions.abort_all();
    observations.abort_all();
    // On worker failure, drop senders and stop peers as well; do not leave detached imports.
    drop(delivery);
    drop(consensus_delivery);
    worker_stop.send_replace(true);
    while target_tasks.join_next().await.is_some() {}
    while source_tasks.join_next().await.is_some() {}
    while blob_acquisitions.join_next().await.is_some() {}
    while acquisitions.join_next().await.is_some() {}
    while observations.join_next().await.is_some() {}
    result
}
