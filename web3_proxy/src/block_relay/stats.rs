use super::config::Mode;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::Serialize;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

pub type Shared = Arc<Mutex<Stats>>;

#[derive(Default, Serialize)]
pub struct Endpoint {
    pub connected: bool,
    pub detail: String,
    pub events: u64,
    pub errors: u64,
    pub last_event_unix_us: Option<u64>,
    pub last_progress_slot: u64,
    #[serde(skip)]
    pub last_progress: Option<tokio::time::Instant>,
}
impl Endpoint {
    pub fn progress(&mut self, slot: u64) {
        self.last_event_unix_us = Some(unix_micros());
        self.last_progress = Some(tokio::time::Instant::now());
        self.last_progress_slot = self.last_progress_slot.max(slot);
    }
    pub fn fresh(&self, limit: std::time::Duration) -> bool {
        self.connected && self.last_progress.is_some_and(|at| at.elapsed() <= limit)
    }
}

#[derive(Serialize, Default)]
pub struct TargetStats {
    pub health: Endpoint,
    pub sent: u64,
    pub valid: u64,
    pub accepted: u64,
    pub syncing: u64,
    pub invalid: u64,
    pub unknown: u64,
    pub skipped_known: u64,
    pub skipped_invalid_ancestor: u64,
    pub queue_dropped: u64,
    pub repairs: u64,
    pub repair_gaps: u64,
    pub ready: u64,
    pub incomplete: u64,
    pub left_censored: u64,
    pub ready_latency: Distribution,
    pub engine_latency: Distribution,
    pub canonical_latency: Distribution,
}

#[derive(Serialize, Default)]
pub struct ConsensusStats {
    pub health: Endpoint,
    pub sent: u64,
    pub published: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub unknown: u64,
    pub skipped_known: u64,
    pub queue_dropped: u64,
    pub repairs: u64,
    pub repair_gaps: u64,
    pub ready: u64,
    pub optimistic: u64,
    pub incomplete: u64,
    pub left_censored: u64,
    pub publish_latency: Distribution,
    pub ready_latency: Distribution,
    pub canonical_latency: Distribution,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Layer {
    Execution,
    Consensus,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Observation {
    pub ready: Option<u64>,
    pub canonical: Option<u64>,
}

pub struct Distribution(Histogram<u64>);
impl Default for Distribution {
    fn default() -> Self {
        Self(Histogram::new(3).expect("valid significant figures"))
    }
}
impl Distribution {
    pub fn record(&mut self, micros: u64) {
        let _ = self.0.record(micros);
    }
}
impl Serialize for Distribution {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Values {
            count: u64,
            p50_us: u64,
            p95_us: u64,
            p99_us: u64,
        }
        Values {
            count: self.0.len(),
            p50_us: self.0.value_at_quantile(0.50),
            p95_us: self.0.value_at_quantile(0.95),
            p99_us: self.0.value_at_quantile(0.99),
        }
        .serialize(serializer)
    }
}

#[derive(Default, Serialize)]
pub struct Stats {
    pub enabled: bool,
    pub mode: Mode,
    pub config_error: Option<String>,
    pub seconds_per_slot: u64,
    pub latest_source_slot: u64,
    pub recording_dropped: u64,
    #[serde(skip)]
    pub recorder: Option<super::recording::Sender>,
    pub sources: BTreeMap<String, Endpoint>,
    pub execution_targets: BTreeMap<String, TargetStats>,
    pub consensus_targets: BTreeMap<String, ConsensusStats>,
    pub acquired: u64,
    pub acquisition_failed: u64,
    pub acquisition_dropped: u64,
    pub duplicates: u64,
    pub stale_events: u64,
    pub acquisition_latency: Distribution,
    pub fleet_ready_latency: Distribution,
    pub fleet_spread: Distribution,
    pub fleet_incomplete: u64,
    pub observation_dropped: u64,
    pub consensus_acquired: u64,
    pub consensus_acquisition_failed: u64,
    pub consensus_acquisition_dropped: u64,
    pub consensus_acquisition_latency: Distribution,
    pub fleet_canonical_latency: Distribution,
    pub fleet_canonical_incomplete: u64,
    #[serde(skip)]
    pub samples: VecDeque<Sample>,
}

/// Private diagnostic records. `/status` exposes aggregates only.
#[derive(Clone, Debug, Serialize)]
pub struct Sample {
    pub layer: Layer,
    pub beacon_root: alloy::primitives::B256,
    pub hash: alloy::primitives::B256,
    pub slot: u64,
    pub source: String,
    pub announcement_source: String,
    pub event: &'static str,
    pub mode: Mode,
    /// Wall clock only joins records across observers. Durations use Instant.
    pub first_seen_unix_us: u64,
    pub acquired_us: u64,
    pub target: String,
    pub last_missing_us: Option<u64>,
    pub first_ready_us: Option<u64>,
    pub canonical_us: Option<u64>,
}
impl Sample {
    pub(super) fn new(work: &super::Work, target: &str, layer: Layer) -> Self {
        Self {
            layer,
            beacon_root: work.payload.beacon_root,
            hash: work.payload.hash,
            slot: work.payload.slot,
            source: work.source.clone(),
            announcement_source: work.announcement_source.clone(),
            event: work.event,
            mode: work.mode,
            first_seen_unix_us: work.first_seen_unix_us,
            acquired_us: micros(work.acquired.duration_since(work.first_seen)),
            target: target.into(),
            last_missing_us: None,
            first_ready_us: None,
            canonical_us: None,
        }
    }
}
impl Stats {
    pub fn ready(&self) -> bool {
        let freshness = std::time::Duration::from_secs(self.seconds_per_slot * 3);
        let fresh = |endpoint: &Endpoint| {
            endpoint.fresh(freshness)
                && endpoint.last_progress_slot >= self.latest_source_slot.saturating_sub(3)
        };
        self.enabled
            && self.config_error.is_none()
            && self.recording_dropped == 0
            && self
                .recorder
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
            && !self.sources.is_empty()
            && !(self.execution_targets.is_empty() && self.consensus_targets.is_empty())
            && self.sources.values().all(fresh)
            && self.execution_targets.values().all(|t| fresh(&t.health))
            && self.consensus_targets.values().all(|t| fresh(&t.health))
    }
    pub fn source_error(&mut self, name: &str, detail: &str) {
        let source = self.sources.entry(name.to_string()).or_default();
        source.connected = false;
        source.errors += 1;
        source.detail = detail.to_string();
    }
    pub fn source_connected(&mut self, name: &str, topic: &str) {
        let source = self.sources.entry(name.to_string()).or_default();
        source.connected = true;
        source.last_progress = None;
        source.detail = topic.to_string();
    }
    pub fn source_event(&mut self, name: &str, slot: u64) {
        self.latest_source_slot = self.latest_source_slot.max(slot);
        let source = self.sources.entry(name.to_string()).or_default();
        source.events += 1;
        source.progress(slot);
    }
    pub fn record(&mut self, record: super::recording::Record) {
        if self
            .recorder
            .as_ref()
            .is_some_and(|sender| sender.try_send(record).is_err())
        {
            self.recording_dropped += 1;
        }
    }
    pub fn sample(&mut self, sample: Sample) {
        self.record(super::recording::Record::Observation(sample.clone()));
        tracing::debug!(target: "web3_proxy::block_relay::samples", sample = %sonic_rs::to_string(&sample).unwrap_or_default(), "block relay observation");
        if self.samples.len() == 1024 {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }
}
pub fn unix_micros() -> u64 {
    micros(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default(),
    )
}
pub fn micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn readiness_requires_recent_events_from_every_source_and_both_target_layers() {
        let (send, _recv) = tokio::sync::mpsc::channel(1);
        let mut stats = Stats {
            enabled: true,
            seconds_per_slot: 12,
            recorder: Some(send),
            ..Default::default()
        };
        stats.source_connected("local", "block");
        stats.source_connected("external", "block");
        stats
            .execution_targets
            .entry("el".into())
            .or_default()
            .health
            .connected = true;
        stats
            .consensus_targets
            .entry("cl".into())
            .or_default()
            .health
            .connected = true;
        assert!(!stats.ready());
        stats.source_event("local", 10);
        stats
            .execution_targets
            .get_mut("el")
            .unwrap()
            .health
            .progress(10);
        stats
            .consensus_targets
            .get_mut("cl")
            .unwrap()
            .health
            .progress(10);
        assert!(
            !stats.ready(),
            "external keepalives do not prove a working feed"
        );
        stats.source_event("external", 10);
        assert!(stats.ready());
        stats
            .execution_targets
            .get_mut("el")
            .unwrap()
            .health
            .connected = false;
        assert!(!stats.ready(), "suspended Engine must not report ready");
        stats
            .execution_targets
            .get_mut("el")
            .unwrap()
            .health
            .connected = true;
        stats.recording_dropped = 1;
        assert!(
            !stats.ready(),
            "missing measurement records invalidate readiness"
        );
        stats.recording_dropped = 0;
        tokio::time::advance(std::time::Duration::from_secs(37)).await;
        assert!(!stats.ready());
        stats.source_event("local", 10);
        stats.source_event("external", 10);
        stats
            .execution_targets
            .get_mut("el")
            .unwrap()
            .health
            .progress(10);
        assert!(
            !stats.ready(),
            "stale consensus confirmation must not report ready"
        );
        stats
            .consensus_targets
            .get_mut("cl")
            .unwrap()
            .health
            .progress(10);
        assert!(stats.ready());
        stats.source_connected("external", "block");
        assert!(!stats.ready(), "reconnect requires a new block event");
    }
}
