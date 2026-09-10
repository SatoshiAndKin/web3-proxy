use super::{
    config::{Config, Mode, SLOTS_PER_EPOCH},
    payload::RelayPayload,
    source::{race_sources, Announcement, BeaconSource},
    stats::{self, Shared, Stats},
    target::Target,
    transport::Rpc,
};
use alloy::primitives::B256;
use alloy_rpc_types_engine::JwtSecret;
use anyhow::Result;
use futures_util::{stream::FuturesUnordered, StreamExt};
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
    sources: Vec<Arc<BeaconSource>>,
    targets: Vec<Arc<Target>>,
    decoders: Arc<tokio::sync::Semaphore>,
}
impl Prepared {
    fn new(config: Config, previous: Option<Arc<Self>>) -> Result<Self> {
        config.validate()?;
        let sources = config
            .sources
            .iter()
            .map(|(name, config)| BeaconSource::new(name.clone(), config).map(Arc::new))
            .collect::<Result<_>>()?;
        let targets = config
            .targets
            .iter()
            .map(|(name, config)| {
                let engine_url = super::config::url(&config.engine_url)?;
                let jwt = JwtSecret::from_file(&config.jwt_secret_path)
                    .map_err(|_| anyhow::anyhow!("cannot read a target JWT secret"))?;
                let uncertain = previous
                    .as_ref()
                    .and_then(|p| {
                        p.config
                            .targets
                            .iter()
                            .find(|(_, t)| {
                                super::config::url(&t.engine_url).as_ref().ok() == Some(&engine_url)
                            })
                            .and_then(|(name, _)| p.targets.iter().find(|t| &t.name == name))
                    })
                    .map(|t| t.uncertain.clone())
                    .unwrap_or_default();
                Ok(Arc::new(Target {
                    name: name.clone(),
                    engine: Rpc::new(&config.engine_url, Some(jwt))?,
                    rpc: Rpc::new(&config.rpc_url, None)?,
                    uncertain,
                    probes: tokio::sync::Semaphore::new(4),
                }))
            })
            .collect::<Result<_>>()?;
        let decoders = previous
            .map(|p| p.decoders.clone())
            .unwrap_or_else(|| Arc::new(tokio::sync::Semaphore::new(4)));
        Ok(Self {
            config,
            sources,
            targets,
            decoders,
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
                    targets: prepared
                        .targets
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
        .weigher(|_, p| p.body.len().min(u32::MAX as usize) as u32)
        .time_to_live(ttl)
        .build();
    let seen = Cache::<B256, ()>::builder()
        .max_capacity(512)
        .time_to_live(ttl)
        .build();
    let (announcements, mut incoming) = mpsc::channel::<Announcement>(256);
    let (delivery, _) = broadcast::channel::<Arc<Work>>(128);
    let mut source_tasks = JoinSet::new();
    let mut target_tasks = JoinSet::new();
    let mut acquisitions = JoinSet::new();
    let mut observations = JoinSet::new();
    let mut in_progress = HashMap::<B256, Arc<Notify>>::new();
    let (worker_stop, worker_stop_rx) = watch::channel(false);
    for source in &prepared.sources {
        source_tasks.spawn(source.clone().run(
            prepared.config.network.clone(),
            announcements.clone(),
            stats.clone(),
        ));
    }
    for target in &prepared.targets {
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
                    known: prepared.targets.iter().map(|t| (t.name.clone(), AtomicBool::new(false))).collect() });
                {
                    let mut s = stats.lock(); s.acquired += 1;
                    s.acquisition_latency.record(stats::micros(acquired.duration_since(event.at)));
                }
                let _ = delivery.send(work.clone());
                if observations.len() >= 128 { stats.lock().observation_dropped += 1; continue; }
                // Probes cannot delay fanout. They use each target's direct RPC endpoint.
                let targets = prepared.targets.clone(); let stats = stats.clone();
                observations.spawn(async move {
                    let mut probes = FuturesUnordered::new();
                    for target in &targets { probes.push(target.observe(work.clone(), stats.clone())); }
                    let mut ready = Vec::new();
                    while let Some(result) = probes.next().await { if let Some(value) = result { ready.push(value); } }
                    let mut s = stats.lock();
                    if ready.len() == targets.len() {
                        let last = *ready.iter().max().expect("nonempty targets");
                        let first = *ready.iter().min().expect("nonempty targets");
                        s.fleet_ready_latency.record(last); s.fleet_spread.record(last - first);
                    } else { s.fleet_incomplete += 1; }
                });
            }
        }
    };
    source_tasks.abort_all();
    acquisitions.abort_all();
    observations.abort_all();
    // On worker failure, drop senders and stop peers as well; do not leave detached imports.
    drop(delivery);
    worker_stop.send_replace(true);
    while target_tasks.join_next().await.is_some() {}
    while source_tasks.join_next().await.is_some() {}
    while acquisitions.join_next().await.is_some() {}
    while observations.join_next().await.is_some() {}
    result
}
