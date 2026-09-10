use super::{
    config::Mode,
    payload::RelayPayload,
    stats::{self, Sample, Shared},
    transport::Rpc,
    Work,
};
use alloy::primitives::{B256, U64};
use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusEnum};
use anyhow::{ensure, Result};
use moka::future::Cache;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch},
    time::Instant,
};

pub struct Target {
    pub name: String,
    pub engine: Rpc,
    pub rpc: Rpc,
    /// A lost Engine response does not cancel execution on the node. Keep the
    /// target suspended until its RPC confirms this block, including across reloads.
    pub uncertain: Arc<parking_lot::Mutex<Option<(B256, u64)>>>,
    pub probes: tokio::sync::Semaphore,
}
#[derive(Clone)]
enum Delivery {
    Status(PayloadStatusEnum),
    Unknown,
}
impl Delivery {
    fn invalid_ancestor() -> Self {
        Self::Status(PayloadStatusEnum::Invalid {
            validation_error: "invalid ancestor".into(),
        })
    }
    fn is_invalid(&self) -> bool {
        matches!(self, Self::Status(s) if s.is_invalid())
    }
    fn is_valid(&self) -> bool {
        matches!(self, Self::Status(s) if s.is_valid())
    }
}
#[derive(Deserialize)]
struct Block {
    hash: B256,
    number: U64,
}

pub(super) struct WorkerContext {
    pub chain_id: u64,
    pub stop: watch::Receiver<bool>,
    pub mode: watch::Receiver<Mode>,
    pub cache: Cache<B256, Arc<RelayPayload>>,
    pub stats: Shared,
    pub ttl: Duration,
}

impl Target {
    pub async fn validate(&self, chain_id: u64) -> Result<()> {
        let (engine_chain, rpc_chain, methods): (U64, U64, Vec<String>) = tokio::try_join!(
            self.engine.call("eth_chainId", [] as [u8; 0]),
            self.rpc.call("eth_chainId", [] as [u8; 0]),
            self.engine
                .call("engine_exchangeCapabilities", (["engine_newPayloadV4"],)),
        )?;
        ensure!(
            engine_chain.to::<u64>() == chain_id && rpc_chain == engine_chain,
            "execution chain ID mismatch"
        );
        ensure!(
            methods.iter().any(|m| m == "engine_newPayloadV4"),
            "target lacks engine_newPayloadV4"
        );
        Ok(())
    }
    pub async fn has_block(&self, hash: B256, number: u64) -> Result<bool> {
        let block: Option<Block> = self.rpc.call("eth_getBlockByHash", (hash, false)).await?;
        let Some(block) = block else {
            return Ok(false);
        };
        ensure!(
            block.hash == hash && block.number.to::<u64>() == number,
            "RPC block identity mismatch"
        );
        Ok(true)
    }
    pub async fn run(
        self: Arc<Self>,
        mut incoming: broadcast::Receiver<Arc<Work>>,
        context: WorkerContext,
    ) {
        let WorkerContext {
            chain_id,
            mut stop,
            mode,
            cache,
            stats,
            ttl,
        } = context;
        let handled = Cache::<B256, Delivery>::builder()
            .max_capacity(512)
            .time_to_live(ttl)
            .build();
        let mut pending = VecDeque::new();
        let mut waiting = BTreeMap::<B256, Arc<Work>>::new();
        loop {
            if *stop.borrow() {
                return;
            }
            match self.validate(chain_id).await {
                Ok(()) => {
                    let mut s = stats.lock();
                    let t = s.execution_targets.entry(self.name.clone()).or_default();
                    t.health.connected = true;
                    t.health.detail = "Engine V4 ready".into();
                    break;
                }
                Err(error) => {
                    let mut s = stats.lock();
                    let t = s.execution_targets.entry(self.name.clone()).or_default();
                    t.health.connected = false;
                    t.health.errors += 1;
                    t.health.detail = error.to_string();
                }
            }
            tokio::select! { _ = stop.changed() => return, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
        }
        loop {
            if *stop.borrow() {
                return;
            }
            let uncertain = *self.uncertain.lock();
            if let Some((hash, number)) = uncertain {
                if self.has_block(hash, number).await.unwrap_or(false) {
                    *self.uncertain.lock() = None;
                    let mut s = stats.lock();
                    let t = s.execution_targets.entry(self.name.clone()).or_default();
                    t.health.connected = true;
                    t.health.detail = "Engine response lost; RPC has confirmed the block".into();
                } else {
                    tokio::select! { _ = stop.changed() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    continue;
                }
            }
            // Drain to a bounded local queue, then prefer the newest slot. Preserve competitors.
            loop {
                let work = match incoming.try_recv() {
                    Ok(work) => work,
                    Err(broadcast::error::TryRecvError::Lagged(n)) => {
                        stats
                            .lock()
                            .execution_targets
                            .entry(self.name.clone())
                            .or_default()
                            .queue_dropped += n;
                        continue;
                    }
                    Err(_) => break,
                };
                if pending.len() == 128 {
                    pending.pop_front();
                    stats
                        .lock()
                        .execution_targets
                        .entry(self.name.clone())
                        .or_default()
                        .queue_dropped += 1;
                }
                pending.push_back(work);
            }
            let work = if let Some(index) = pending
                .iter()
                .enumerate()
                .max_by_key(|(_, w)| w.payload.slot)
                .map(|(i, _)| i)
            {
                pending.remove(index).expect("existing index")
            } else {
                tokio::select! {
                    _ = stop.changed() => return,
                    work = incoming.recv() => match work {
                        Ok(work) => work,
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            stats.lock().execution_targets.entry(self.name.clone()).or_default().queue_dropped += n; continue;
                        }
                    }
                }
            };
            if work.mode == Mode::Observe
                || *mode.borrow() == Mode::Observe
                || Instant::now() >= work.deadline
            {
                continue;
            }
            let p = &work.payload;
            if work.known[&self.name].load(Ordering::Acquire) {
                handled
                    .insert(p.hash, Delivery::Status(PayloadStatusEnum::Valid))
                    .await;
                self.wake_children(p.hash, &mut waiting, &mut pending, &stats);
                stats
                    .lock()
                    .execution_targets
                    .entry(self.name.clone())
                    .or_default()
                    .skipped_known += 1;
                continue;
            }
            let previous = handled.get(&p.hash).await;
            let parent_became_valid =
                matches!(previous, Some(Delivery::Status(PayloadStatusEnum::Syncing)))
                    && handled
                        .get(&p.parent_hash)
                        .await
                        .is_some_and(|s| s.is_valid());
            if previous.is_some() && !parent_became_valid {
                stats
                    .lock()
                    .execution_targets
                    .entry(self.name.clone())
                    .or_default()
                    .skipped_known += 1;
                continue;
            }
            if handled
                .get(&p.parent_hash)
                .await
                .is_some_and(|s| s.is_invalid())
            {
                handled.insert(p.hash, Delivery::invalid_ancestor()).await;
                stats
                    .lock()
                    .execution_targets
                    .entry(self.name.clone())
                    .or_default()
                    .skipped_invalid_ancestor += 1;
                continue;
            }
            let result = self.deliver(p, &stats).await;
            if let Ok(status) = result {
                let syncing = status.is_syncing();
                handled
                    .insert(p.hash, Delivery::Status(status.status))
                    .await;
                if syncing && !*stop.borrow() && Instant::now() < work.deadline {
                    self.repair(&work, &cache, &handled, &stats, &stop, &mode)
                        .await;
                }
                waiting.retain(|_, w| Instant::now() < w.deadline);
                if handled
                    .get(&p.hash)
                    .await
                    .is_some_and(|s| matches!(s, Delivery::Status(PayloadStatusEnum::Syncing)))
                {
                    if waiting.len() < 128 {
                        waiting.insert(p.hash, work.clone());
                    } else {
                        stats
                            .lock()
                            .execution_targets
                            .entry(self.name.clone())
                            .or_default()
                            .queue_dropped += 1;
                    }
                } else if handled.get(&p.hash).await.is_some_and(|s| s.is_valid()) {
                    // SYNCING is retryable only after new ancestry evidence, never on a timer.
                    self.wake_children(p.hash, &mut waiting, &mut pending, &stats);
                }
                // Rejection can happen during repair, not only on this work item's first send.
                let mut rejected = Vec::new();
                for (hash, child) in &waiting {
                    if handled
                        .get(&child.payload.parent_hash)
                        .await
                        .is_some_and(|s| s.is_invalid())
                    {
                        rejected.push(*hash);
                    }
                }
                while let Some(hash) = rejected.pop() {
                    handled.insert(hash, Delivery::invalid_ancestor()).await;
                    waiting.remove(&hash);
                    rejected.extend(
                        waiting
                            .iter()
                            .filter(|(_, child)| child.payload.parent_hash == hash)
                            .map(|(hash, _)| *hash),
                    );
                }
            } else {
                // A timeout can leave execution in progress. Do not issue another request
                // for this hash merely because the response was lost.
                handled.insert(p.hash, Delivery::Unknown).await;
            }
        }
    }
    fn wake_children(
        &self,
        parent: B256,
        waiting: &mut BTreeMap<B256, Arc<Work>>,
        pending: &mut VecDeque<Arc<Work>>,
        stats: &Shared,
    ) {
        let children: Vec<_> = waiting
            .iter()
            .filter(|(_, child)| child.payload.parent_hash == parent)
            .map(|(hash, _)| *hash)
            .collect();
        for child in children {
            if pending.len() == 128 {
                pending.pop_front();
                stats
                    .lock()
                    .execution_targets
                    .entry(self.name.clone())
                    .or_default()
                    .queue_dropped += 1;
            }
            pending.push_back(waiting.remove(&child).expect("existing child"));
        }
    }
    async fn deliver(&self, payload: &RelayPayload, stats: &Shared) -> Result<PayloadStatus> {
        stats
            .lock()
            .execution_targets
            .entry(self.name.clone())
            .or_default()
            .sent += 1;
        let start = Instant::now();
        let result = self.engine.new_payload(payload).await.and_then(|status| {
            ensure!(
                !status.is_valid() || status.latest_valid_hash == Some(payload.hash),
                "VALID response hash mismatch"
            );
            Ok(status)
        });
        if result.is_err() {
            *self.uncertain.lock() = Some((payload.hash, payload.number));
        }
        let mut s = stats.lock();
        let t = s.execution_targets.entry(self.name.clone()).or_default();
        t.engine_latency.record(stats::micros(start.elapsed()));
        match &result {
            Ok(status) => match &status.status {
                PayloadStatusEnum::Valid => t.valid += 1,
                PayloadStatusEnum::Accepted => t.accepted += 1,
                PayloadStatusEnum::Syncing => t.syncing += 1,
                PayloadStatusEnum::Invalid { .. } => t.invalid += 1,
            },
            Err(error) => {
                t.unknown += 1;
                t.health.connected = false;
                t.health.errors += 1;
                t.health.detail = format!("injection suspended: {error}");
            }
        }
        if result.as_ref().is_ok_and(|s| s.is_invalid()) {
            tracing::error!(target_name = %self.name, block_hash = %payload.hash, "Engine rejected relay payload");
        }
        result
    }
    async fn repair(
        &self,
        work: &Work,
        cache: &Cache<B256, Arc<RelayPayload>>,
        handled: &Cache<B256, Delivery>,
        stats: &Shared,
        stop: &watch::Receiver<bool>,
        mode: &watch::Receiver<Mode>,
    ) {
        let mut chain: Vec<Arc<RelayPayload>> = Vec::new();
        let mut hash = work.payload.parent_hash;
        let mut number = work.payload.number.saturating_sub(1);
        let mut anchored = false;
        for _ in 0..=8 {
            if *stop.borrow() || *mode.borrow() == Mode::Observe || Instant::now() >= work.deadline
            {
                return;
            }
            let previous = handled.get(&hash).await;
            if previous.as_ref().is_some_and(|s| s.is_invalid()) {
                for descendant in chain.iter().chain(std::iter::once(&work.payload)) {
                    handled
                        .insert(descendant.hash, Delivery::invalid_ancestor())
                        .await;
                }
                return;
            }
            if matches!(previous, Some(Delivery::Unknown)) {
                return;
            }
            if handled.get(&hash).await.is_some_and(|s| s.is_valid())
                || self.has_block(hash, number).await.unwrap_or(false)
            {
                anchored = true;
                break;
            }
            if chain.len() == 8 {
                break;
            }
            let Some(parent) = cache.get(&hash).await else {
                break;
            };
            if parent.number != number {
                break;
            }
            hash = parent.parent_hash;
            number = number.saturating_sub(1);
            chain.push(parent);
        }
        if !anchored || chain.is_empty() {
            stats
                .lock()
                .execution_targets
                .entry(self.name.clone())
                .or_default()
                .repair_gaps += 1;
            return;
        }
        stats
            .lock()
            .execution_targets
            .entry(self.name.clone())
            .or_default()
            .repairs += 1;
        let mut repair = chain
            .into_iter()
            .rev()
            .chain(std::iter::once(work.payload.clone()));
        while let Some(payload) = repair.next() {
            if *stop.borrow() || *mode.borrow() == Mode::Observe || Instant::now() >= work.deadline
            {
                return;
            }
            let Ok(status) = self.deliver(&payload, stats).await else {
                return;
            };
            let valid = status.is_valid();
            let invalid = status.is_invalid();
            handled
                .insert(payload.hash, Delivery::Status(status.status))
                .await;
            if !valid {
                if invalid {
                    for descendant in repair {
                        handled
                            .insert(descendant.hash, Delivery::invalid_ancestor())
                            .await;
                    }
                }
                return;
            }
        }
    }
    pub async fn observe(&self, work: Arc<Work>, stats: Shared) -> stats::Observation {
        let mut sample = Sample::new(&work, &self.name, stats::Layer::Execution);
        let permit = tokio::time::timeout_at(work.deadline, self.probes.acquire())
            .await
            .ok()
            .and_then(Result::ok);
        while permit.is_some() && Instant::now() < work.deadline {
            let query_start = Instant::now();
            if sample.first_ready_us.is_none() {
                match self.has_block(work.payload.hash, work.payload.number).await {
                    Ok(true) => {
                        sample.first_ready_us = Some(stats::micros(work.first_seen.elapsed()));
                        work.known[&self.name].store(true, Ordering::Release);
                    }
                    Ok(false) => {
                        sample.last_missing_us =
                            Some(stats::micros(query_start.duration_since(work.first_seen)))
                    }
                    Err(_) => {} // An RPC error is not evidence that the block is absent.
                }
            }
            if sample.first_ready_us.is_some() {
                let canonical: Result<Option<Block>> = self
                    .rpc
                    .call(
                        "eth_getBlockByNumber",
                        (U64::from(work.payload.number), false),
                    )
                    .await;
                if canonical.is_ok_and(|b| {
                    b.is_some_and(|b| {
                        b.hash == work.payload.hash && b.number.to::<u64>() == work.payload.number
                    })
                }) {
                    sample.canonical_us = Some(stats::micros(work.first_seen.elapsed()));
                    break;
                }
            }
            let delay = if work.first_seen.elapsed() < Duration::from_secs(1) {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(100)
            };
            tokio::time::sleep(delay).await;
        }
        let ready = sample.first_ready_us;
        let mut s = stats.lock();
        let t = s.execution_targets.entry(self.name.clone()).or_default();
        if let Some(ready) = ready {
            t.ready += 1;
            t.ready_latency.record(ready);
            if sample.last_missing_us.is_none() {
                t.left_censored += 1;
            }
        } else {
            t.incomplete += 1;
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
