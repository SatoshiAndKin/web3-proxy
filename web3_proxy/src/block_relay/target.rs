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
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch, Notify},
    time::Instant,
};

pub struct Target {
    pub name: String,
    pub engine: Rpc,
    pub rpc: Rpc,
    pub(super) handled: Cache<B256, Delivery>,
    pub probes: tokio::sync::Semaphore,
    pub(super) confirmed: Cache<B256, ()>,
    pub(super) evidence: Notify,
}
#[derive(Clone)]
pub(super) enum Delivery {
    Valid,
    Accepted,
    Syncing { parent_was_valid: bool },
    Invalid,
    Unknown,
}
impl Delivery {
    fn from_status(status: PayloadStatusEnum, parent_was_valid: bool) -> Self {
        match status {
            PayloadStatusEnum::Valid => Self::Valid,
            PayloadStatusEnum::Accepted => Self::Accepted,
            PayloadStatusEnum::Syncing => Self::Syncing { parent_was_valid },
            PayloadStatusEnum::Invalid { .. } => Self::Invalid,
        }
    }
    fn invalid_ancestor() -> Self {
        Self::Invalid
    }
    fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid)
    }
    fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}
#[derive(Deserialize)]
struct Block {
    hash: B256,
    number: U64,
}

#[derive(Clone)]
pub(super) struct WorkerContext {
    pub chain_id: u64,
    pub stop: watch::Receiver<bool>,
    pub mode: watch::Receiver<Mode>,
    pub cache: Cache<B256, Arc<RelayPayload>>,
    pub stats: Shared,
}

impl Target {
    /// Independent canonical-head notifications. Never mutate delivery caches or
    /// gate a payload on telemetry. Reconnect only this observation stream.
    pub async fn observe_heads(
        self: Arc<Self>,
        ws_url: String,
        stats: Shared,
        mut stop: watch::Receiver<bool>,
    ) {
        use alloy::providers::Provider;
        use futures_util::StreamExt;
        let mut stream_id = 0u64;
        loop {
            if *stop.borrow() {
                return;
            }
            let subscribe = async {
                let url = url::Url::parse(&ws_url)?;
                ensure!(
                    matches!(url.scheme(), "ws" | "wss")
                        && url.host_str().is_some()
                        && url.fragment().is_none(),
                    "invalid head telemetry URL"
                );
                let provider = crate::rpcs::provider::connect_ws(url).await?;
                let subscription = provider.subscribe_blocks().await?;
                Ok::<_, anyhow::Error>((provider, subscription))
            };
            let connection = tokio::select! {
                _ = stop.changed() => return,
                result = tokio::time::timeout(Duration::from_secs(8), subscribe) => result,
            };
            if let Ok(Ok((_provider, subscription))) = connection {
                stream_id += 1;
                let mut headers = subscription.into_stream();
                {
                    let mut s = stats.lock();
                    s.telemetry
                        .head_streams
                        .entry(self.name.clone())
                        .or_default()
                        .success(0, "newHeads subscribed");
                }
                loop {
                    let header = tokio::select! {
                        _ = stop.changed() => return,
                        header = tokio::time::timeout(Duration::from_secs(60), headers.next()) => header.ok().flatten(),
                    };
                    let Some(header) = header else {
                        break;
                    };
                    let mut s = stats.lock();
                    let head = super::telemetry::Head {
                        hash: header.hash,
                        number: header.number,
                        block_timestamp: header.timestamp,
                        observed_us: s.telemetry.at(Instant::now()),
                        stream_id,
                    };
                    let endpoint = s
                        .telemetry
                        .head_streams
                        .entry(self.name.clone())
                        .or_default();
                    endpoint.events += 1;
                    endpoint.success(0, "newHeads received");
                    s.telemetry.heads.insert(self.name.clone(), head.clone());
                    s.record(super::recording::Record::Head {
                        target: self.name.clone(),
                        head,
                    });
                }
            }
            stats
                .lock()
                .telemetry
                .head_streams
                .entry(self.name.clone())
                .or_default()
                .failure("newHeads connection failed; retry pending");
            tokio::select! { _ = stop.changed() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
        }
    }
    pub async fn validate(&self, chain_id: u64) -> Result<()> {
        let (engine_chain, methods): (U64, Vec<String>) = tokio::try_join!(
            self.engine.call("eth_chainId", [] as [u8; 0]),
            self.engine
                .call("engine_exchangeCapabilities", (["engine_newPayloadV4"],)),
        )?;
        ensure!(
            engine_chain.to::<u64>() == chain_id,
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
        if !self.confirmed.contains_key(&hash) {
            self.confirmed.insert(hash, ()).await;
            self.evidence.notify_one();
        }
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
        } = context;
        let handled = &self.handled;
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
            // RPC probes and repair reads report through the same target state.
            // Process it before dequeuing work, including after an idle wake-up.
            for (hash, ()) in self.confirmed.iter() {
                if !handled
                    .get(hash.as_ref())
                    .await
                    .is_some_and(|s| s.is_invalid() || s.is_valid())
                {
                    handled.insert(*hash, Delivery::Valid).await;
                }
            }
            waiting.retain(|_, work| Instant::now() < work.deadline);
            let mut completed = Vec::new();
            let mut parents = Vec::new();
            for (hash, child) in &waiting {
                match handled.get(hash).await {
                    Some(Delivery::Valid | Delivery::Invalid) => completed.push(*hash),
                    Some(Delivery::Syncing {
                        parent_was_valid: false,
                    }) if handled
                        .get(&child.payload.parent_hash)
                        .await
                        .is_some_and(|s| s.is_valid()) =>
                    {
                        parents.push(child.payload.parent_hash);
                    }
                    _ => {}
                }
            }
            for hash in completed {
                waiting.remove(&hash);
            }
            for parent in parents {
                self.wake_children(parent, &mut waiting, &mut pending, &stats);
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
                    _ = self.evidence.notified() => continue,
                    work = incoming.recv() => match work {
                        Ok(work) => { pending.push_back(work); continue; },
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            stats.lock().execution_targets.entry(self.name.clone()).or_default().queue_dropped += n; continue;
                        }
                    }
                }
            };
            if work.mode == Mode::Observe || *mode.borrow() == Mode::Observe {
                stats.lock().disposition(
                    stats::Layer::Execution,
                    &work.payload,
                    &self.name,
                    "observe",
                );
                continue;
            }
            if Instant::now() >= work.deadline {
                stats.lock().disposition(
                    stats::Layer::Execution,
                    &work.payload,
                    &self.name,
                    "deadline",
                );
                continue;
            }
            let p = &work.payload;
            let previous = handled.get(&p.hash).await;
            if self.confirmed.contains_key(&p.hash)
                && !previous.as_ref().is_some_and(|s| s.is_invalid())
            {
                handled.insert(p.hash, Delivery::Valid).await;
                self.wake_children(p.hash, &mut waiting, &mut pending, &stats);
                stats
                    .lock()
                    .execution_targets
                    .entry(self.name.clone())
                    .or_default()
                    .skipped_known += 1;
                stats
                    .lock()
                    .disposition(stats::Layer::Execution, p, &self.name, "already_known");
                continue;
            }
            let parent_became_valid = matches!(
                previous,
                Some(Delivery::Syncing {
                    parent_was_valid: false
                })
            ) && handled
                .get(&p.parent_hash)
                .await
                .is_some_and(|s| s.is_valid());
            if previous.is_some() && !parent_became_valid {
                stats
                    .lock()
                    .execution_targets
                    .entry(self.name.clone())
                    .or_default()
                    .suppressed_duplicate += 1;
                stats
                    .lock()
                    .disposition(stats::Layer::Execution, p, &self.name, "duplicate");
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
                stats.lock().disposition(
                    stats::Layer::Execution,
                    p,
                    &self.name,
                    "invalid_ancestor",
                );
                continue;
            }
            let parent_was_valid = self.confirmed.contains_key(&p.parent_hash)
                || handled
                    .get(&p.parent_hash)
                    .await
                    .is_some_and(|s| s.is_valid());
            handled.insert(p.hash, Delivery::Unknown).await;
            let result = self.deliver(p, &stats).await;
            if let Ok(status) = result {
                let syncing = status.is_syncing();
                handled
                    .insert(
                        p.hash,
                        Delivery::from_status(status.status, parent_was_valid),
                    )
                    .await;
                if syncing && !*stop.borrow() && Instant::now() < work.deadline {
                    self.repair(&work, &cache, handled, &stats, &stop, &mode)
                        .await;
                }
                waiting.retain(|_, w| Instant::now() < w.deadline);
                if handled
                    .get(&p.hash)
                    .await
                    .is_some_and(|s| matches!(s, Delivery::Syncing { .. }))
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
        let (attempt_id, started_mode, started_mode_epoch) = {
            let mut s = stats.lock();
            let attempt_id = s.telemetry.attempt();
            let canonical_before_call = s
                .telemetry
                .heads
                .get(&self.name)
                .filter(|head| head.hash == payload.hash)
                .cloned();
            s.record(super::recording::Record::SubmissionStarted {
                attempt_id,
                layer: stats::Layer::Execution,
                root: payload.beacon_root,
                hash: payload.hash,
                slot: payload.slot,
                target: self.name.clone(),
                serialized_request_bytes: payload.body.len(),
                canonical_before_call,
            });
            (attempt_id, s.mode, s.telemetry.mode_epoch)
        };
        let result = self.engine.new_payload(payload).await.and_then(|status| {
            ensure!(
                !status.is_valid() || status.latest_valid_hash == Some(payload.hash),
                "VALID response hash mismatch"
            );
            Ok(status)
        });
        let mut s = stats.lock();
        let t = s.execution_targets.entry(self.name.clone()).or_default();
        t.engine_latency.record(stats::micros(start.elapsed()));
        match &result {
            Ok(status) => {
                t.health.connected = true;
                t.health.detail = "Engine response received".into();
                t.health.progress(payload.slot);
                match &status.status {
                    PayloadStatusEnum::Valid => t.valid += 1,
                    PayloadStatusEnum::Accepted => t.accepted += 1,
                    PayloadStatusEnum::Syncing => t.syncing += 1,
                    PayloadStatusEnum::Invalid { .. } => {
                        t.invalid += 1;
                        t.health.failure("Engine rejected payload");
                    }
                }
            }
            Err(error) => {
                t.unknown += 1;
                t.health.connected = false;
                t.health.errors += 1;
                t.health.detail = error.to_string();
            }
        }
        s.record(super::recording::Record::Submission {
            attempt_id,
            started_mode,
            started_mode_epoch,
            layer: stats::Layer::Execution,
            root: payload.beacon_root,
            hash: payload.hash,
            slot: payload.slot,
            target: self.name.clone(),
            elapsed_us: stats::micros(start.elapsed()),
            outcome: match &result {
                Ok(status) => match status.status {
                    PayloadStatusEnum::Valid => "valid",
                    PayloadStatusEnum::Accepted => "accepted",
                    PayloadStatusEnum::Syncing => "syncing",
                    PayloadStatusEnum::Invalid { .. } => "invalid",
                },
                Err(_) => "unknown",
            }
            .into(),
        });
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
            if handled.get(&hash).await.is_some_and(|s| s.is_valid())
                || tokio::time::timeout_at(work.deadline, self.has_block(hash, number))
                    .await
                    .is_ok_and(|result| result.unwrap_or(false))
            {
                anchored = true;
                break;
            }
            if matches!(previous, Some(Delivery::Unknown)) {
                return;
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
        if !anchored {
            stats
                .lock()
                .execution_targets
                .entry(self.name.clone())
                .or_default()
                .repair_gaps += 1;
            return;
        }
        // A newly confirmed immediate parent is enough evidence for the worker
        // to retry. It does not require an ancestor payload to be re-imported.
        if chain.is_empty() {
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
            let parent_was_valid = self.confirmed.contains_key(&payload.parent_hash)
                || handled
                    .get(&payload.parent_hash)
                    .await
                    .is_some_and(|s| s.is_valid());
            handled.insert(payload.hash, Delivery::Unknown).await;
            let Ok(status) = self.deliver(&payload, stats).await else {
                handled.insert(payload.hash, Delivery::Unknown).await;
                return;
            };
            let valid = status.is_valid();
            let invalid = status.is_invalid();
            handled
                .insert(
                    payload.hash,
                    Delivery::from_status(status.status, parent_was_valid),
                )
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
            let query_us = stats::micros(query_start.duration_since(work.first_seen));
            sample.first_probe_us.get_or_insert(query_us);
            if sample.first_ready_us.is_none() {
                sample.probes += 1;
                match self.has_block(work.payload.hash, work.payload.number).await {
                    Ok(true) => {
                        sample.first_ready_probe_started_us = Some(query_us);
                        sample.first_ready_us = Some(stats::micros(work.first_seen.elapsed()));
                    }
                    Ok(false) => {
                        sample.last_missing_us =
                            Some(stats::micros(query_start.duration_since(work.first_seen)))
                    }
                    Err(error) => {
                        sample.probe_errors += 1;
                        stats
                            .lock()
                            .execution_targets
                            .entry(self.name.clone())
                            .or_default()
                            .observation
                            .failure(&error.to_string());
                    }
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
                if canonical.is_err() {
                    sample.probe_errors += 1;
                }
                sample.probes += 1;
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
            t.health.progress(work.payload.slot);
            t.observation
                .success(work.payload.slot, "RPC import confirmed");
            t.ready += 1;
            t.ready_latency.record(ready);
            if sample.last_missing_us.is_none() {
                t.left_censored += 1;
            }
        } else {
            t.incomplete += 1;
            t.observation
                .failure("RPC import unconfirmed before deadline");
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
