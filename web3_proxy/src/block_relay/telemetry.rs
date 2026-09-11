//! One bounded, monotonic measurement contract for observe and inject.
//! A successful import is not evidence of time saved without a control sample.
use super::{
    config::Mode,
    recording::Record,
    stats::{self, Distribution, Layer, Shared},
};
use alloy::primitives::B256;
use serde::Serialize;
use std::{collections::BTreeMap, time::Duration};
use tokio::{sync::watch, time::Instant};

pub const SCHEMA: u32 = 1;

#[derive(Clone, Debug, Serialize)]
pub struct Context {
    pub schema: u32,
    pub session: String,
    pub sequence: u64,
    pub monotonic_us: u64,
    pub unix_us: u64,
    pub generation: u64,
    pub mode_epoch: u64,
    pub mode: Mode,
}

#[derive(Serialize)]
pub struct Envelope {
    pub context: Context,
    #[serde(flatten)]
    pub event: Record,
}

#[derive(Default, Serialize)]
pub struct Traffic {
    pub attempts: u64,
    pub responses: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub response_bytes: u64,
    pub duration: Distribution,
}

#[derive(Default, Serialize)]
pub struct TargetMetrics {
    pub calls: u64,
    pub serialized_request_bytes: u64,
    pub outcomes: BTreeMap<String, u64>,
    pub dispositions: BTreeMap<String, u64>,
    pub observations: u64,
    pub missing: u64,
    pub left_censored: u64,
    pub ready: Distribution,
    pub canonical: Distribution,
    pub request_duration: Distribution,
    pub canonical_before_call: u64,
}

#[derive(Default, Serialize)]
pub struct ModeMetrics {
    pub acquired: u64,
    pub acquisition_failed: u64,
    pub blob_complete: u64,
    pub blob_failed: u64,
    pub blobs: u64,
    pub acquisition: Distribution,
    pub blob_ready: Distribution,
    pub sources: BTreeMap<String, Traffic>,
    pub targets: BTreeMap<String, TargetMetrics>,
    pub fleet_ready: Distribution,
    pub fleet_canonical: Distribution,
    pub fleet_spread: Distribution,
    pub fleet_incomplete: u64,
    pub measured_wall_us: u64,
    pub measured_cpu_us: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Head {
    pub hash: B256,
    pub number: u64,
    pub block_timestamp: u64,
    pub observed_us: u64,
    pub stream_id: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Resources {
    pub cpu_us: u64,
    pub peak_rss_bytes: u64,
    pub rss_bytes: Option<u64>,
}

#[derive(Serialize)]
pub struct Telemetry {
    pub session: String,
    pub generation: u64,
    pub mode_epoch: u64,
    pub modes: BTreeMap<Mode, ModeMetrics>,
    pub mixed_mode_samples: u64,
    pub resource_errors: u64,
    pub resource_status: super::stats::Endpoint,
    pub resources: Option<Resources>,
    pub head_streams: BTreeMap<String, super::stats::Endpoint>,
    #[serde(skip)]
    pub heads: BTreeMap<String, Head>,
    #[serde(skip)]
    started: Instant,
    #[serde(skip)]
    sequence: u64,
    #[serde(skip)]
    next_attempt: u64,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self {
            session: ulid::Ulid::generate().to_string(),
            generation: 0,
            mode_epoch: 0,
            modes: BTreeMap::new(),
            mixed_mode_samples: 0,
            resource_errors: 0,
            resource_status: Default::default(),
            resources: None,
            head_streams: BTreeMap::new(),
            heads: BTreeMap::new(),
            started: Instant::now(),
            sequence: 0,
            next_attempt: 0,
        }
    }
}

impl Telemetry {
    pub fn at(&self, instant: Instant) -> u64 {
        stats::micros(instant.saturating_duration_since(self.started))
    }
    pub fn context(&mut self, mode: Mode) -> Context {
        self.sequence += 1;
        Context {
            schema: SCHEMA,
            session: self.session.clone(),
            sequence: self.sequence,
            monotonic_us: self.at(Instant::now()),
            unix_us: stats::unix_micros(),
            generation: self.generation,
            mode_epoch: self.mode_epoch,
            mode,
        }
    }
    pub fn configured(&mut self) {
        self.generation += 1;
        self.mode_epoch += 1;
        // A different target/source contract must not contaminate mode comparisons.
        self.modes.clear();
        self.heads.clear();
        self.head_streams.clear();
    }
    pub fn attempt(&mut self) -> u64 {
        self.next_attempt += 1;
        self.next_attempt
    }
    pub fn mode(&mut self, mode: Mode) -> &mut ModeMetrics {
        self.modes.entry(mode).or_default()
    }
    pub fn target(&mut self, mode: Mode, layer: Layer, name: &str) -> &mut TargetMetrics {
        self.mode(mode)
            .targets
            .entry(format!("{}:{name}", layer.name()))
            .or_default()
    }
}

/// Count cancelled source races too. No I/O or await occurs while recording cost.
pub struct Fetch {
    stats: Shared,
    mode: Mode,
    generation: u64,
    source: String,
    started: Instant,
    result: Option<(bool, u64)>,
}
impl Fetch {
    pub fn start(stats: Shared, source: &str, kind: &str) -> Self {
        let (mode, generation) = {
            let mut s = stats.lock();
            let mode = s.mode;
            s.telemetry
                .mode(mode)
                .sources
                .entry(format!("{source}:{kind}"))
                .or_default()
                .attempts += 1;
            (mode, s.telemetry.generation)
        };
        Self {
            stats,
            mode,
            generation,
            source: format!("{source}:{kind}"),
            started: Instant::now(),
            result: None,
        }
    }
    pub fn finish(&mut self, success: bool, bytes: usize) {
        self.result = Some((success, bytes as u64));
    }
}
impl Drop for Fetch {
    fn drop(&mut self) {
        let mut s = self.stats.lock();
        if s.telemetry.generation != self.generation {
            return;
        }
        let traffic = s
            .telemetry
            .mode(self.mode)
            .sources
            .entry(self.source.clone())
            .or_default();
        traffic
            .duration
            .record(stats::micros(self.started.elapsed()));
        match self.result {
            Some((true, bytes)) => {
                traffic.responses += 1;
                traffic.response_bytes += bytes;
            }
            Some((false, _)) => traffic.failed += 1,
            None => traffic.cancelled += 1,
        }
    }
}

fn resources() -> std::io::Result<Resources> {
    use nix::sys::{
        resource::{getrusage, UsageWho},
        time::TimeValLike,
    };
    let usage = getrusage(UsageWho::RUSAGE_SELF).map_err(std::io::Error::from)?;
    let cpu_us =
        (usage.user_time().num_microseconds() + usage.system_time().num_microseconds()) as u64;
    #[cfg(target_os = "macos")]
    let peak_rss_bytes = usage.max_rss() as u64;
    #[cfg(not(target_os = "macos"))]
    let peak_rss_bytes = usage.max_rss() as u64 * 1024;
    #[cfg(target_os = "linux")]
    let rss_bytes = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .map(|v| v * 1024)
            })
        });
    #[cfg(not(target_os = "linux"))]
    let rss_bytes = None;
    Ok(Resources {
        cpu_us,
        peak_rss_bytes,
        rss_bytes,
    })
}

pub async fn monitor_resources(stats: Shared, mut stop: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut window = CpuWindow::default();
    let mut next_totals = Instant::now();
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            _ = tick.tick() => {}
        }
        let started_mode_epoch = stats.lock().telemetry.mode_epoch;
        match tokio::task::spawn_blocking(resources).await {
            Ok(Ok(resource)) => {
                let now = Instant::now();
                let mut s = stats.lock();
                let epoch = s.telemetry.mode_epoch;
                let mode = s.mode;
                if let Some((wall_us, cpu_us)) =
                    window.sample(started_mode_epoch, epoch, resource.cpu_us, now)
                {
                    let totals = s.telemetry.mode(mode);
                    totals.measured_wall_us += wall_us;
                    totals.measured_cpu_us += cpu_us;
                }
                s.telemetry.resources = Some(resource.clone());
                s.telemetry
                    .resource_status
                    .success(0, "process resources sampled");
                s.record(Record::Resources {
                    resources: resource,
                    started_mode_epoch,
                });
                // Snapshots preserve per-mode cost even when an individual block is missed.
                if now >= next_totals {
                    s.record_totals();
                    next_totals = now + Duration::from_secs(60);
                }
            }
            _ => {
                window = CpuWindow::default();
                let mut s = stats.lock();
                s.telemetry.resource_errors += 1;
                s.telemetry
                    .resource_status
                    .failure("process resource sampling failed");
            }
        }
    }
}

#[derive(Default)]
struct CpuWindow(Option<(u64, u64, Instant)>);
impl CpuWindow {
    fn sample(
        &mut self,
        started_epoch: u64,
        epoch: u64,
        cpu_us: u64,
        at: Instant,
    ) -> Option<(u64, u64)> {
        let old = self.0.take();
        if started_epoch != epoch {
            return None;
        }
        self.0 = Some((epoch, cpu_us, at));
        old.filter(|(old_epoch, cpu, _)| *old_epoch == epoch && *cpu <= cpu_us)
            .map(|(_, cpu, previous)| {
                (
                    stats::micros(at.saturating_duration_since(previous)),
                    cpu_us - cpu,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cpu_cost_excludes_mode_transitions_and_failed_samples() {
        let mut window = CpuWindow::default();
        let now = Instant::now();
        assert_eq!(window.sample(1, 1, 100, now), None);
        assert_eq!(
            window.sample(1, 1, 600, now + Duration::from_secs(1)),
            Some((1_000_000, 500))
        );
        assert_eq!(window.sample(1, 2, 800, now + Duration::from_secs(2)), None);
        assert_eq!(window.sample(2, 2, 900, now + Duration::from_secs(3)), None);
        assert_eq!(
            window.sample(2, 2, 950, now + Duration::from_secs(4)),
            Some((1_000_000, 50))
        );
        assert_eq!(
            window.sample(3, 3, 1000, now + Duration::from_secs(5)),
            None
        );
    }

    #[tokio::test]
    async fn source_race_cost_counts_cancellation_without_claiming_a_response() {
        let stats = std::sync::Arc::new(parking_lot::Mutex::new(stats::Stats::default()));
        let mut success = Fetch::start(stats.clone(), "fast", "block");
        let cancelled = Fetch::start(stats.clone(), "slow", "block");
        success.finish(true, 420);
        drop(success);
        drop(cancelled);
        let mut s = stats.lock();
        let mode = s.telemetry.mode(Mode::Observe);
        assert_eq!(mode.sources["fast:block"].response_bytes, 420);
        assert_eq!(mode.sources["fast:block"].responses, 1);
        assert_eq!(mode.sources["slow:block"].attempts, 1);
        assert_eq!(mode.sources["slow:block"].cancelled, 1);
        assert_eq!(mode.sources["slow:block"].responses, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn context_preserves_session_sequence_and_discards_cross_mode_latency() {
        let mut s = stats::Stats::default();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        s.recorder = Some(sender);
        s.telemetry.configured();
        s.record(Record::AcquisitionFailed {
            root: B256::ZERO,
            slot: 1,
            consensus: false,
            started_mode_epoch: 1,
        });
        s.change_mode(Mode::Inject);
        tokio::time::advance(Duration::from_secs(1)).await;
        s.record(Record::AcquisitionFailed {
            root: B256::ZERO,
            slot: 2,
            consensus: false,
            started_mode_epoch: 1,
        });
        s.record(Record::AcquisitionFailed {
            root: B256::ZERO,
            slot: 3,
            consensus: false,
            started_mode_epoch: 2,
        });
        let first = receiver.try_recv().unwrap();
        let totals = receiver.try_recv().unwrap();
        let transition = receiver.try_recv().unwrap();
        let mixed = receiver.try_recv().unwrap();
        assert_eq!(first.context.session, mixed.context.session);
        assert_eq!(first.context.sequence, 1);
        assert_eq!(mixed.context.sequence, 4);
        assert!(matches!(
            transition.event,
            Record::ModeChanged {
                previous: Mode::Observe
            }
        ));
        assert_eq!(
            mixed.context.monotonic_us - first.context.monotonic_us,
            1_000_000
        );
        assert_eq!(totals.context.mode, Mode::Observe);
        assert_eq!(mixed.context.mode, Mode::Inject);
        assert_eq!(s.telemetry.mode(Mode::Observe).acquisition_failed, 1);
        assert_eq!(s.telemetry.mode(Mode::Inject).acquisition_failed, 1);
        assert_eq!(s.telemetry.mixed_mode_samples, 1);
    }
}
