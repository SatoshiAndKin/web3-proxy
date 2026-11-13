mod rtt_estimate;

use self::rtt_estimate::{AtomicRttEstimate, RttEstimate};
use crate::queue::{LatencyQueueStats, QueueMetrics, Sample};
use crate::util::nanos::nanos;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::{enabled, error, trace, Level};

const RECEIVE_BATCH: usize = 128;
const REPORTS_PER_TURN: usize = 1_024;

/// Latency calculation using Peak EWMA algorithm
///
/// Updates are done in a separate task to avoid locking or race
/// conditions. Reads may happen on any thread.
#[derive(Debug)]
pub struct PeakEwmaLatency {
    /// Join handle for the latency calculation task
    pub join_handle: JoinHandle<()>,
    /// Send to update with each request duration
    request_tx: mpsc::Sender<Sample<Observation>>,
    queue: QueueMetrics,
    /// Latency average and last update time
    rtt_estimate: Arc<AtomicRttEstimate>,
    /// Decay time
    decay_ns: f64,
}

impl PeakEwmaLatency {
    /// Spawn the task for calculating peak request latency
    ///
    /// Returns a handle that can also be used to read the current
    /// average latency.
    pub fn spawn(decay: Duration, buf_size: usize, start_latency: Duration) -> Self {
        let decay_ns = decay.as_nanos() as f64;

        debug_assert!(decay_ns > 0.0, "decay_ns must be positive");

        let (request_tx, request_rx) = mpsc::channel(buf_size);
        let estimate = RttEstimate::new(start_latency);
        let rtt_estimate = Arc::new(AtomicRttEstimate::new(&estimate));
        let queue = QueueMetrics::new();
        let task = PeakEwmaLatencyTask {
            queue_id: queue.id,
            request_rx,
            rtt_estimate: rtt_estimate.clone(),
            estimate,
            decay_ns,
        };
        let join_handle = tokio::spawn(task.run());
        Self {
            join_handle,
            request_tx,
            queue,
            rtt_estimate,
            decay_ns,
        }
    }

    /// Get the current peak-ewma latency estimate
    pub fn latency(&self) -> Duration {
        let mut estimate = self.rtt_estimate.load();

        let now = Instant::now();

        if estimate.update_at > now {
            if enabled!(Level::TRACE) {
                trace!(
                    "update_at is {}ns in the future",
                    estimate.update_at.duration_since(now).as_nanos()
                );
            }
            estimate.rtt
        } else {
            // Update the RTT estimate to account for decay since the last update.
            estimate.update(0.0, self.decay_ns, now)
        }
    }

    /// Current channel capacity use and dropped reports.
    pub fn queue_stats(&self) -> LatencyQueueStats {
        self.queue.snapshot(&self.request_tx)
    }

    /// Report a backend exchange observed now.
    pub fn report(&self, duration: Duration) {
        self.report_at(duration, Instant::now());
    }

    /// Report a backend exchange using its completion time for decay.
    pub fn report_at(&self, duration: Duration, completed_at: Instant) {
        let observation = Observation {
            duration,
            completed_at,
        };
        if let Err(error) = self.request_tx.try_send(self.queue.sample(observation)) {
            let dropped_reports = self.queue.dropped(&error);
            // Keep exact counters, but emit only logarithmically many events
            // when producers outrun the consumer.
            if dropped_reports.is_power_of_two() {
                error!(
                    queue_id = self.queue.id,
                    dropped_reports,
                    reason = %error,
                    "Latency report dropped"
                );
            }
        }
    }
}

#[derive(Debug)]
struct Observation {
    duration: Duration,
    completed_at: Instant,
}

/// Task to be spawned per-Web3Rpc for calculating the peak request latency
#[derive(Debug)]
struct PeakEwmaLatencyTask {
    /// Receive new request timings for update
    request_rx: mpsc::Receiver<Sample<Observation>>,
    queue_id: u64,
    /// Current estimate and update time
    rtt_estimate: Arc<AtomicRttEstimate>,
    estimate: RttEstimate,
    /// Decay time
    decay_ns: f64,
}

impl PeakEwmaLatencyTask {
    /// Run the loop for updating latency
    async fn run(mut self) {
        let mut reports = Vec::with_capacity(RECEIVE_BATCH);
        let mut processed = 0;
        loop {
            let count = self.request_rx.recv_many(&mut reports, RECEIVE_BATCH).await;
            if count == 0 {
                break;
            }
            for (index, rtt) in reports.drain(..).enumerate() {
                rtt.received(
                    self.queue_id,
                    "peak_ewma",
                    self.request_rx.len() + count - index - 1,
                );
                let observation = rtt.value;
                self.estimate.update(
                    nanos(observation.duration),
                    self.decay_ns,
                    observation.completed_at,
                );
            }
            self.rtt_estimate.store(&self.estimate);
            processed += count;
            // Give other tasks a turn even if the receiver stays ready.
            if processed >= REPORTS_PER_TURN {
                processed = 0;
                tokio::task::yield_now().await;
            }
        }
        trace!("latency loop exited");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::time::{self, Duration};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::{layer::Context, prelude::*, Layer};

    struct CountErrors(Arc<AtomicUsize>);

    impl<S: Subscriber> Layer<S> for CountErrors {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            if *event.metadata().level() == Level::ERROR {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[tokio::test]
    async fn full_queue_counts_all_drops_and_bounds_error_events() {
        let estimate =
            super::PeakEwmaLatency::spawn(Duration::from_secs(1), 1, Duration::from_millis(10));
        let errors = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(CountErrors(errors.clone()));
        tracing::subscriber::with_default(subscriber, || {
            // Keep the receiver from running on the current-thread runtime.
            estimate.report(Duration::from_millis(10));
            for _ in 0..10_000 {
                estimate.report(Duration::from_millis(10));
            }
        });

        let queue = estimate.queue_stats();
        assert_eq!(queue.used_capacity, 1);
        assert_eq!(queue.dropped_full, 10_000);
        assert_eq!(queue.dropped_closed, 0);
        assert_eq!(errors.load(Ordering::Relaxed), 14);
    }

    #[tokio::test]
    async fn a_sparse_report_updates_the_estimate() {
        let estimate =
            super::PeakEwmaLatency::spawn(Duration::from_secs(1), 1, Duration::from_millis(1));
        estimate.report(Duration::from_millis(100));
        time::timeout(Duration::from_secs(2), async {
            while estimate.latency() < Duration::from_millis(50) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_backlog_uses_completion_intervals_for_decay() {
        let estimator =
            super::PeakEwmaLatency::spawn(Duration::from_secs(1), 8, Duration::from_millis(10));
        let start = time::Instant::now();
        time::advance(Duration::from_secs(3)).await;

        estimator.report_at(Duration::from_millis(100), start + Duration::from_secs(1));
        estimator.report_at(Duration::from_millis(20), start + Duration::from_secs(2));
        tokio::task::yield_now().await;

        let latency = estimator.latency();
        assert!(Duration::from_millis(18) < latency && latency < Duration::from_millis(19));
    }

    /// The default RTT estimate decays, so that new nodes are considered if the
    /// default RTT is too high.
    #[tokio::test(start_paused = true)]
    async fn default_decay() {
        let estimate = super::PeakEwmaLatency::spawn(
            Duration::from_millis(1_000),
            8,
            Duration::from_millis(10),
        );
        let load = estimate.latency();
        assert_eq!(load, Duration::from_millis(10));

        time::advance(Duration::from_millis(100)).await;
        let load = estimate.latency();
        assert!(Duration::from_millis(9) < load && load < Duration::from_millis(10));

        time::advance(Duration::from_millis(100)).await;
        let load = estimate.latency();
        assert!(Duration::from_millis(8) < load && load < Duration::from_millis(9));
    }
}
