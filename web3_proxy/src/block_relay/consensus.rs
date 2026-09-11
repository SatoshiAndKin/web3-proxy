//! Publish complete signed Beacon blocks to independent consensus destinations.
use super::{
    config::{self, Mode},
    payload::ConsensusPayload,
    source,
    stats::{self, Sample, Shared},
    transport::BeaconHttp,
    Work,
};
use alloy::primitives::B256;
use alloy_rpc_types_beacon::header::HeaderResponse;
use anyhow::{ensure, Result};
use moka::future::Cache;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch, Notify, Semaphore},
    time::Instant,
};

#[derive(Debug)]
pub(super) struct ConsensusWork {
    pub work: Arc<Work>,
    pub payload: Arc<ConsensusPayload>,
}

pub(super) struct ConsensusTarget {
    pub name: String,
    http: BeaconHttp,
    confirmed: Cache<B256, ()>,
    attempts: Cache<B256, Attempt>,
    evidence: Notify,
    probes: Semaphore,
}

#[derive(Clone)]
pub(super) struct Context {
    pub network: config::Network,
    pub stop: watch::Receiver<bool>,
    pub mode: watch::Receiver<Mode>,
    pub cache: Cache<B256, Arc<ConsensusPayload>>,
    pub stats: Shared,
}

#[derive(Clone, Copy)]
struct Attempt {
    count: u8,
    parent_confirmed: bool,
}

impl ConsensusTarget {
    pub fn new(name: String, config: &config::ConsensusTarget, ttl: Duration) -> Result<Self> {
        Ok(Self {
            name,
            http: BeaconHttp::new(&config.beacon_url, &config.headers)?,
            confirmed: Cache::builder().max_capacity(512).time_to_live(ttl).build(),
            attempts: Cache::builder().max_capacity(512).time_to_live(ttl).build(),
            evidence: Notify::new(),
            probes: Semaphore::new(4),
        })
    }

    pub fn retain_delivery(&mut self, previous: &Self) {
        self.confirmed = previous.confirmed.clone();
        self.attempts = previous.attempts.clone();
    }

    async fn header(&self, root: B256) -> Result<Option<HeaderResponse>> {
        let header: Option<HeaderResponse> = self
            .http
            .get_optional(&format!("/eth/v1/beacon/headers/{root}"))
            .await?;
        if let Some(header) = &header {
            ensure!(header.data.root == root, "Beacon target root mismatch");
            ensure!(
                super::tree_hash::header(&header.data.header.message) == root,
                "Beacon target header root mismatch"
            );
            if !header.execution_optimistic && !self.confirmed.contains_key(&root) {
                self.confirmed.insert(root, ()).await;
                self.evidence.notify_one();
            }
        }
        Ok(header)
    }

    async fn has_block(&self, root: B256) -> bool {
        self.confirmed.contains_key(&root)
            || self
                .header(root)
                .await
                .is_ok_and(|h| h.is_some_and(|h| !h.execution_optimistic))
    }

    async fn publish(&self, payload: &ConsensusPayload, stats: &Shared) {
        if self.confirmed.contains_key(&payload.block.beacon_root) {
            stats
                .lock()
                .consensus_targets
                .entry(self.name.clone())
                .or_default()
                .skipped_known += 1;
            return;
        }
        let started = Instant::now();
        stats
            .lock()
            .consensus_targets
            .entry(self.name.clone())
            .or_default()
            .sent += 1;
        let result = self.http.publish(payload).await;
        {
            let mut s = stats.lock();
            let t = s.consensus_targets.entry(self.name.clone()).or_default();
            t.publish_latency.record(stats::micros(started.elapsed()));
            match &result {
                Ok(200) => {
                    t.published += 1;
                    t.health
                        .success(payload.block.slot, "Beacon publication HTTP 200");
                }
                Ok(202) => {
                    t.accepted += 1;
                    t.health.success(
                        payload.block.slot,
                        "Beacon publication HTTP 202; import unconfirmed",
                    );
                }
                Ok(code) => {
                    t.rejected += 1;
                    t.health.connected = false;
                    t.health.errors += 1;
                    t.health.detail = format!("Beacon publication HTTP {code}");
                }
                Err(error) => {
                    t.unknown += 1;
                    t.health.connected = false;
                    t.health.errors += 1;
                    t.health.detail = error.to_string();
                }
            }
        }
        stats.lock().record(super::recording::Record::Submission {
            layer: stats::Layer::Consensus,
            root: payload.block.beacon_root,
            hash: payload.block.hash,
            slot: payload.block.slot,
            target: self.name.clone(),
            elapsed_us: stats::micros(started.elapsed()),
            outcome: result
                .map(|code| format!("http_{code}"))
                .unwrap_or_else(|_| "unknown".into()),
        });
        // Independent observers confirm imports and wake children. Never await a read here.
    }

    fn active(work: &Work, context: &Context) -> bool {
        !*context.stop.borrow()
            && *context.mode.borrow() == Mode::Inject
            && work.mode == Mode::Inject
            && Instant::now() < work.deadline
    }

    async fn ancestors(
        &self,
        work: &ConsensusWork,
        context: &Context,
    ) -> Vec<Arc<ConsensusPayload>> {
        let mut root = work.payload.block.parent_beacon_root;
        let mut chain = Vec::new();
        for _ in 0..=8 {
            if !Self::active(&work.work, context) {
                return Vec::new();
            }
            if self.has_block(root).await {
                return chain;
            }
            if chain.len() == 8 {
                break;
            }
            let Some(parent) = context.cache.get(&root).await else {
                break;
            };
            if parent.block.slot
                >= chain
                    .last()
                    .map_or(work.payload.block.slot, |p: &Arc<ConsensusPayload>| {
                        p.block.slot
                    })
            {
                break;
            }
            root = parent.block.parent_beacon_root;
            chain.push(parent);
        }
        context
            .stats
            .lock()
            .consensus_targets
            .entry(self.name.clone())
            .or_default()
            .repair_gaps += 1;
        Vec::new()
    }

    pub async fn run(
        self: Arc<Self>,
        mut incoming: broadcast::Receiver<Arc<ConsensusWork>>,
        mut context: Context,
    ) {
        loop {
            if *context.stop.borrow() {
                return;
            }
            let result = source::validate_network(&self.http, &context.network).await;
            {
                let mut s = context.stats.lock();
                let t = s.consensus_targets.entry(self.name.clone()).or_default();
                t.health.connected = result.is_ok();
                t.health.detail = result
                    .as_ref()
                    .map(|_| "Beacon publication ready".to_owned())
                    .unwrap_or_else(|e| e.to_string());
                if result.is_err() {
                    t.health.errors += 1;
                }
            }
            if result.is_ok() {
                break;
            }
            tokio::select! { _ = context.stop.changed() => return, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
        }
        let attempts = &self.attempts;
        let mut repairs = tokio::task::JoinSet::<(Arc<Work>, Vec<Arc<ConsensusPayload>>)>::new();
        let mut confirmations = tokio::task::JoinSet::new();
        let mut waiting = BTreeMap::<B256, Arc<ConsensusWork>>::new();
        let mut pending = VecDeque::new();
        loop {
            if *context.stop.borrow() {
                return;
            }
            waiting.retain(|root, work| {
                !self.confirmed.contains_key(root) && Self::active(&work.work, &context)
            });
            let ready: Vec<_> = waiting
                .iter()
                .filter(|(_, work)| {
                    self.confirmed
                        .contains_key(&work.payload.block.parent_beacon_root)
                })
                .map(|(root, _)| *root)
                .collect();
            for root in ready {
                if pending.len() == 128 {
                    pending.pop_front();
                    context
                        .stats
                        .lock()
                        .consensus_targets
                        .entry(self.name.clone())
                        .or_default()
                        .queue_dropped += 1;
                }
                pending.push_back(waiting.remove(&root).expect("waiting child"));
            }
            loop {
                match incoming.try_recv() {
                    Ok(work) => {
                        if pending.len() == 128 {
                            pending.pop_front();
                            context
                                .stats
                                .lock()
                                .consensus_targets
                                .entry(self.name.clone())
                                .or_default()
                                .queue_dropped += 1;
                        }
                        pending.push_back(work);
                    }
                    Err(broadcast::error::TryRecvError::Lagged(n)) => {
                        context
                            .stats
                            .lock()
                            .consensus_targets
                            .entry(self.name.clone())
                            .or_default()
                            .queue_dropped += n
                    }
                    Err(_) => break,
                }
            }
            let work = if let Some(index) = pending
                .iter()
                .enumerate()
                .max_by_key(|(_, w)| w.payload.block.slot)
                .map(|(i, _)| i)
            {
                pending.remove(index).expect("queued block")
            } else {
                tokio::select! {
                    _ = context.stop.changed() => return,
                    _ = self.evidence.notified() => continue,
                    _ = confirmations.join_next(), if !confirmations.is_empty() => continue,
                    result = repairs.join_next(), if !repairs.is_empty() => {
                        if let Some(Ok((work, parents))) = result {
                            for payload in parents {
                                if waiting.len() == 128 {
                                    context.stats.lock().consensus_targets.entry(self.name.clone()).or_default().queue_dropped += 1;
                                    break;
                                }
                                waiting.entry(payload.block.beacon_root).or_insert_with(|| Arc::new(ConsensusWork { work: work.clone(), payload }));
                            }
                        }
                        continue;
                    }
                    item = incoming.recv() => match item {
                        Ok(work) => work,
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(n)) => { context.stats.lock().consensus_targets.entry(self.name.clone()).or_default().queue_dropped += n; continue; }
                    }
                }
            };
            if !Self::active(&work.work, &context) {
                continue;
            }
            let root = work.payload.block.beacon_root;
            if self.confirmed.contains_key(&root) {
                context
                    .stats
                    .lock()
                    .consensus_targets
                    .entry(self.name.clone())
                    .or_default()
                    .skipped_known += 1;
                continue;
            }
            let previous = attempts.get(&root).await;
            let parent_confirmed = self
                .confirmed
                .contains_key(&work.payload.block.parent_beacon_root);
            if previous.is_some_and(|a| a.count >= 2 || a.parent_confirmed || !parent_confirmed) {
                continue;
            }
            let count = previous.map_or(1, |a| a.count + 1);
            attempts
                .insert(
                    root,
                    Attempt {
                        count,
                        parent_confirmed,
                    },
                )
                .await;
            self.publish(&work.payload, &context.stats).await;
            // The observer has its own bounded task and read permits. A stalled header
            // cannot delay the next publication. It also confirms repaired ancestors.
            while confirmations.try_join_next().is_some() {}
            if confirmations.len() < 128 {
                let target = self.clone();
                let work = work.clone();
                confirmations.spawn(async move {
                    let poll = async {
                        while Instant::now() < work.work.deadline {
                            if target.has_block(work.payload.block.beacon_root).await {
                                return;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    };
                    let _ = tokio::time::timeout_at(work.work.deadline, poll).await;
                });
            } else {
                context.stats.lock().observation_dropped += 1;
            }
            if count == 1 && repairs.len() < 8 {
                let target = self.clone();
                let work = work.clone();
                let context = context.clone();
                repairs.spawn(async move {
                    let parents = tokio::time::timeout_at(
                        work.work.deadline,
                        target.ancestors(&work, &context),
                    )
                    .await
                    .unwrap_or_default();
                    if !parents.is_empty() {
                        context
                            .stats
                            .lock()
                            .consensus_targets
                            .entry(target.name.clone())
                            .or_default()
                            .repairs += parents.len() as u64;
                    }
                    (work.work.clone(), parents)
                });
            }
            if waiting.len() < 128 {
                waiting.insert(root, work);
            } else {
                context
                    .stats
                    .lock()
                    .consensus_targets
                    .entry(self.name.clone())
                    .or_default()
                    .queue_dropped += 1;
            }
        }
    }

    pub async fn observe(&self, work: Arc<Work>, stats: Shared) -> stats::Observation {
        let mut sample = Sample::new(&work, &self.name, stats::Layer::Consensus);
        let permit = tokio::time::timeout_at(work.deadline, self.probes.acquire())
            .await
            .ok()
            .and_then(Result::ok);
        let mut optimistic = false;
        while permit.is_some() && Instant::now() < work.deadline {
            let query_start = Instant::now();
            match self.header(work.payload.beacon_root).await {
                Ok(Some(header)) if header.data.header.message.slot == work.payload.slot => {
                    optimistic |= header.execution_optimistic;
                    if !header.execution_optimistic {
                        sample
                            .first_ready_us
                            .get_or_insert_with(|| stats::micros(work.first_seen.elapsed()));
                        if header.data.canonical {
                            sample.canonical_us = Some(stats::micros(work.first_seen.elapsed()));
                            break;
                        }
                    }
                }
                Ok(None) if sample.first_ready_us.is_none() => {
                    sample.last_missing_us =
                        Some(stats::micros(query_start.duration_since(work.first_seen)))
                }
                Err(error) => stats
                    .lock()
                    .consensus_targets
                    .entry(self.name.clone())
                    .or_default()
                    .observation
                    .failure(&error.to_string()),
                _ => {} // Optimistic responses are not proof of absence or completed import.
            }
            tokio::time::sleep(if work.first_seen.elapsed() < Duration::from_secs(1) {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(100)
            })
            .await;
        }
        let ready = sample.first_ready_us;
        let mut s = stats.lock();
        let t = s.consensus_targets.entry(self.name.clone()).or_default();
        if let Some(ready) = ready {
            t.health.progress(work.payload.slot);
            t.observation
                .success(work.payload.slot, "Beacon import confirmed");
            t.ready += 1;
            t.ready_latency.record(ready);
            if sample.last_missing_us.is_none() {
                t.left_censored += 1;
            }
        } else {
            t.incomplete += 1;
            t.observation
                .failure("Beacon import unconfirmed before deadline");
        }
        if optimistic {
            t.optimistic += 1;
        }
        if let Some(canonical) = sample.canonical_us {
            t.canonical_latency.record(canonical);
        }
        let observation = stats::Observation {
            ready,
            canonical: sample.canonical_us,
        };
        s.sample(sample);
        observation
    }
}
