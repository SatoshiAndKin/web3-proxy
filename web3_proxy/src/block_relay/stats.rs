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
    pub sources: BTreeMap<String, Endpoint>,
    pub targets: BTreeMap<String, TargetStats>,
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
    #[serde(skip)]
    pub samples: VecDeque<Sample>,
}

/// Private diagnostic records. `/status` exposes aggregates only.
#[derive(Clone, Debug, Serialize)]
pub struct Sample {
    pub hash: alloy::primitives::B256,
    pub slot: u64,
    pub source: String,
    pub announcement_source: String,
    pub event: &'static str,
    pub mode: Mode,
    pub acquired_us: u64,
    pub target: String,
    pub last_missing_us: Option<u64>,
    pub first_ready_us: Option<u64>,
    pub canonical_us: Option<u64>,
}
impl Stats {
    pub fn source_error(&mut self, name: &str, detail: &str) {
        let source = self.sources.entry(name.to_string()).or_default();
        source.connected = false;
        source.errors += 1;
        source.detail = detail.to_string();
    }
    pub fn source_connected(&mut self, name: &str, topic: &str) {
        let source = self.sources.entry(name.to_string()).or_default();
        source.connected = true;
        source.detail = topic.to_string();
    }
    pub fn source_event(&mut self, name: &str) {
        self.sources.entry(name.to_string()).or_default().events += 1;
    }
    pub fn sample(&mut self, sample: Sample) {
        tracing::debug!(target: "web3_proxy::block_relay::samples", sample = %sonic_rs::to_string(&sample).unwrap_or_default(), "block relay observation");
        if self.samples.len() == 1024 {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }
}
pub fn micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}
