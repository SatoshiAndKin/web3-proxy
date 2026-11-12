mod rtt_estimate;

use self::rtt_estimate::AtomicRttEstimate;
use crate::queue::{LatencyQueueStats, QueueMetrics, Sample};
use crate::util::nanos::nanos;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::{enabled, error, trace, Level};

/// Latency calculation using Peak EWMA algorithm
///
/// Updates are done in a separate task to avoid locking or race
/// conditions. Reads may happen on any thread.
#[derive(Debug)]
pub struct PeakEwmaLatency {
    /// Join handle for the latency calculation task
    pub join_handle: JoinHandle<()>,
    /// Send to update with each request duration
    request_tx: mpsc::Sender<Sample<Duration>>,
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
        let rtt_estimate = Arc::new(AtomicRttEstimate::new(start_latency));
        let queue = QueueMetrics::new();
        let task = PeakEwmaLatencyTask {
            queue_id: queue.id,
            request_rx,
            rtt_estimate: rtt_estimate.clone(),
            update_at: Instant::now(),
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

    /// Report latency from a single request from the owning backend.
    pub fn report(&self, duration: Duration) {
        if let Err(error) = self.request_tx.try_send(self.queue.sample(duration)) {
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

/// Task to be spawned per-Web3Rpc for calculating the peak request latency
#[derive(Debug)]
struct PeakEwmaLatencyTask {
    /// Receive new request timings for update
    request_rx: mpsc::Receiver<Sample<Duration>>,
    queue_id: u64,
    /// Current estimate and update time
    rtt_estimate: Arc<AtomicRttEstimate>,
    /// Last update time, used for decay calculation
    update_at: Instant,
    /// Decay time
    decay_ns: f64,
}

impl PeakEwmaLatencyTask {
    /// Run the loop for updating latency
    async fn run(mut self) {
        while let Some(rtt) = self.request_rx.recv().await {
            rtt.received(self.queue_id, "peak_ewma", self.request_rx.len());
            self.update(rtt.value);
        }
        trace!("latency loop exited");
    }

    /// Update the estimate object atomically.
    fn update(&self, rtt: Duration) {
        let rtt = nanos(rtt);

        let now = Instant::now();
        assert!(
            self.update_at <= now,
            "update_at is {}ns in the future",
            self.update_at.duration_since(now).as_nanos(),
        );

        self.rtt_estimate
            .fetch_update(|mut rtt_estimate| rtt_estimate.update(rtt, self.decay_ns, now));
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
