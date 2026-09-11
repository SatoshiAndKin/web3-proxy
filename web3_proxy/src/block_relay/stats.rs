use super::config::Mode;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use serde::Serialize;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

pub type Shared = Arc<Mutex<Stats>>;

#[derive(Clone, Copy)]
pub(super) enum WorkerLayer {
    Source,
    Execution,
    Consensus,
}

#[derive(Default, Serialize)]
pub struct Endpoint {
    pub connected: bool,
    pub detail: String,
    pub events: u64,
    pub errors: u64,
    pub restarts: u64,
    pub last_event_unix_us: Option<u64>,
    pub last_progress_slot: u64,
    #[serde(skip)]
    pub last_progress: Option<tokio::time::Instant>,
}
impl Endpoint {
    pub fn success(&mut self, slot: u64, detail: &str) {
        self.connected = true;
        self.detail = detail.into();
        self.progress(slot);
    }
    pub fn failure(&mut self, detail: &str) {
        self.connected = false;
        self.errors += 1;
        self.detail = detail.into();
    }
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
    pub observation: Endpoint,
    pub sent: u64,
    pub valid: u64,
    pub accepted: u64,
    pub syncing: u64,
    pub invalid: u64,
    pub unknown: u64,
    pub skipped_known: u64,
    pub suppressed_duplicate: u64,
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
    pub observation: Endpoint,
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
impl Layer {
    pub fn name(self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::Consensus => "consensus",
        }
    }
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
    pub telemetry: super::telemetry::Telemetry,
    pub enabled: bool,
    pub supervisor_running: bool,
    pub acquisition: Endpoint,
    pub consensus_acquisition: Endpoint,
    pub recording: Endpoint,
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
    pub mode_epoch: u64,
    pub first_seen_us: u64,
    pub first_probe_us: Option<u64>,
    pub first_ready_probe_started_us: Option<u64>,
    pub probes: u64,
    pub probe_errors: u64,
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
            mode_epoch: work.mode_epoch,
            first_seen_us: work.first_seen_us,
            first_probe_us: None,
            first_ready_probe_started_us: None,
            probes: 0,
            probe_errors: 0,
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
    pub fn change_mode(&mut self, next: Mode) {
        if self.mode == next {
            return;
        }
        let recording = self.recorder.is_some();
        if recording {
            self.record_totals();
        }
        let previous = self.mode;
        self.mode = next;
        self.telemetry.mode_epoch += 1;
        if recording {
            self.record(super::recording::Record::ModeChanged { previous });
        }
    }
    fn fresh(&self, endpoint: &Endpoint) -> bool {
        endpoint.fresh(std::time::Duration::from_secs(self.seconds_per_slot * 3))
            && endpoint.last_progress_slot >= self.latest_source_slot.saturating_sub(3)
    }
    pub fn ready(&self) -> bool {
        self.enabled
            && self.sources.values().any(|s| self.fresh(s))
            && ((self.fresh(&self.acquisition)
                && self
                    .execution_targets
                    .values()
                    .any(|t| self.fresh(&t.health)))
                || (self.fresh(&self.consensus_acquisition)
                    && self
                        .consensus_targets
                        .values()
                        .any(|t| self.fresh(&t.health))))
    }
    pub fn operation(&self) -> &'static str {
        if !self.ready() {
            return "unavailable";
        }
        if self.config_error.is_some()
            || self
                .recorder
                .as_ref()
                .is_none_or(|sender| sender.is_closed() || sender.capacity() == 0)
            || !self.recording.fresh(std::time::Duration::from_secs(5))
            || !self.sources.values().all(|s| self.fresh(s))
            || !self
                .execution_targets
                .values()
                .all(|t| self.fresh(&t.health) && self.fresh(&t.observation))
            || !self
                .consensus_targets
                .values()
                .all(|t| self.fresh(&t.health) && self.fresh(&t.observation))
            || (!self.execution_targets.is_empty() && !self.fresh(&self.acquisition))
            || (!self.consensus_targets.is_empty() && !self.fresh(&self.consensus_acquisition))
        {
            "degraded"
        } else {
            "healthy"
        }
    }
    pub fn worker_error(&mut self, layer: WorkerLayer, name: &str, detail: &str) {
        let endpoint = match layer {
            WorkerLayer::Source => self.sources.entry(name.into()).or_default(),
            WorkerLayer::Execution => {
                &mut self
                    .execution_targets
                    .entry(name.into())
                    .or_default()
                    .health
            }
            WorkerLayer::Consensus => {
                &mut self
                    .consensus_targets
                    .entry(name.into())
                    .or_default()
                    .health
            }
        };
        endpoint.failure(detail);
        endpoint.restarts += 1;
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
        source.success(slot, "recent block event");
    }
    pub fn record(&mut self, record: super::recording::Record) {
        use super::recording::Record;
        match &record {
            Record::Acquired {
                acquired_us,
                started_mode_epoch,
                ..
            } if *started_mode_epoch == self.telemetry.mode_epoch => {
                let mode = self.telemetry.mode(self.mode);
                mode.acquired += 1;
                mode.acquisition.record(*acquired_us);
            }
            Record::BlobReady {
                mode,
                mode_epoch,
                blob_count,
                ready_us,
                ..
            } if *mode_epoch == self.telemetry.mode_epoch => {
                let mode = self.telemetry.mode(*mode);
                mode.blob_complete += 1;
                mode.blobs += *blob_count as u64;
                mode.blob_ready.record(*ready_us);
            }
            Record::AcquisitionFailed {
                consensus,
                started_mode_epoch,
                ..
            } if *started_mode_epoch == self.telemetry.mode_epoch => {
                let mode = self.telemetry.mode(self.mode);
                if *consensus {
                    mode.blob_failed += 1;
                } else {
                    mode.acquisition_failed += 1;
                }
            }
            Record::SubmissionStarted {
                layer,
                target,
                serialized_request_bytes,
                canonical_before_call,
                ..
            } => {
                let target = self.telemetry.target(self.mode, *layer, target);
                target.calls += 1;
                target.serialized_request_bytes += *serialized_request_bytes as u64;
                target.canonical_before_call += u64::from(canonical_before_call.is_some());
            }
            Record::Submission {
                layer,
                target,
                outcome,
                elapsed_us,
                started_mode,
                started_mode_epoch,
                ..
            } if *started_mode_epoch == self.telemetry.mode_epoch => {
                let target = self.telemetry.target(*started_mode, *layer, target);
                *target.outcomes.entry(outcome.clone()).or_default() += 1;
                target.request_duration.record(*elapsed_us);
            }
            Record::Submission { .. }
            | Record::BlobReady { .. }
            | Record::Acquired { .. }
            | Record::AcquisitionFailed { .. } => self.telemetry.mixed_mode_samples += 1,
            Record::Disposition {
                layer,
                target,
                reason,
                ..
            } => {
                *self
                    .telemetry
                    .target(self.mode, *layer, target)
                    .dispositions
                    .entry((*reason).into())
                    .or_default() += 1;
            }
            _ => {}
        }
        let record = super::telemetry::Envelope {
            context: self.telemetry.context(self.mode),
            event: record,
        };
        if self
            .recorder
            .as_ref()
            .is_none_or(|sender| sender.try_send(record).is_err())
        {
            self.recording_dropped += 1;
            self.recording
                .failure("measurement queue full or unavailable");
        }
    }
    pub fn disposition(
        &mut self,
        layer: Layer,
        payload: &super::payload::RelayPayload,
        target: &str,
        reason: &'static str,
    ) {
        self.record(super::recording::Record::Disposition {
            layer,
            root: payload.beacon_root,
            hash: payload.hash,
            slot: payload.slot,
            target: target.into(),
            reason,
        });
    }
    pub fn record_totals(&mut self) {
        let totals = sonic_rs::to_value(&self.telemetry.modes).expect("serializable mode totals");
        // These are process counters. Report deltas within a capture; never subtract
        // counters from different sessions. Missing rows are never successful work.
        let losses = sonic_rs::json!({
            "recording_dropped": self.recording_dropped,
            "recording_errors": self.recording.errors,
            "acquisition_dropped": self.acquisition_dropped,
            "consensus_acquisition_dropped": self.consensus_acquisition_dropped,
            "observation_dropped": self.observation_dropped,
            "mixed_mode_samples": self.telemetry.mixed_mode_samples,
            "execution_queue_dropped": self.execution_targets.iter().map(|(name, target)| (name, target.queue_dropped)).collect::<BTreeMap<_, _>>(),
            "consensus_queue_dropped": self.consensus_targets.iter().map(|(name, target)| (name, target.queue_dropped)).collect::<BTreeMap<_, _>>(),
        });
        self.record(super::recording::Record::ModeTotals { totals, losses });
    }
    pub fn sample(&mut self, sample: Sample) {
        if sample.mode_epoch == self.telemetry.mode_epoch && sample.mode == self.mode {
            let target = self
                .telemetry
                .target(sample.mode, sample.layer, &sample.target);
            target.observations += 1;
            if let Some(ready) = sample.first_ready_us {
                target.ready.record(ready);
                target.left_censored += u64::from(sample.last_missing_us.is_none());
            } else {
                target.missing += 1;
            }
            if let Some(canonical) = sample.canonical_us {
                target.canonical.record(canonical);
            }
        } else {
            self.telemetry.mixed_mode_samples += 1;
        }
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
    async fn health_tracks_useful_layers_and_recovers_without_erasing_history() {
        let mut s = Stats {
            enabled: true,
            seconds_per_slot: 12,
            ..Default::default()
        };
        s.source_connected("local", "block");
        s.source_connected("silent", "block");
        s.execution_targets.entry("el".into()).or_default();
        s.consensus_targets.entry("cl".into()).or_default();
        assert_eq!(s.operation(), "unavailable");
        s.source_event("local", 10);
        s.acquisition.success(10, "block");
        s.execution_targets
            .get_mut("el")
            .unwrap()
            .health
            .success(10, "Engine");
        assert!(s.ready());
        assert_eq!(s.operation(), "degraded");
        s.recording.failure("disk full");
        s.recording_dropped = 5;
        s.execution_targets
            .get_mut("el")
            .unwrap()
            .health
            .failure("timeout");
        assert!(!s.ready());
        s.consensus_acquisition.success(10, "blobs");
        s.consensus_targets
            .get_mut("cl")
            .unwrap()
            .health
            .success(10, "published");
        assert!(s.ready(), "one useful layer is sufficient");
        s.source_event("silent", 10);
        s.execution_targets
            .get_mut("el")
            .unwrap()
            .health
            .success(10, "Engine");
        s.execution_targets
            .get_mut("el")
            .unwrap()
            .observation
            .success(10, "RPC");
        s.consensus_targets
            .get_mut("cl")
            .unwrap()
            .observation
            .success(10, "Beacon");
        s.recording.success(0, "recorded");
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        s.recorder = Some(sender);
        assert_eq!(s.operation(), "healthy");
        assert_eq!(s.recording.errors, 1);
        assert_eq!(s.recording_dropped, 5);
        assert_eq!(s.execution_targets["el"].health.errors, 1);
        tokio::time::advance(std::time::Duration::from_secs(37)).await;
        assert!(!s.ready());
    }
}
