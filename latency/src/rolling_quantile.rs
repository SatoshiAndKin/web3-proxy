use crate::queue::{LatencyQueueStats, QueueMetrics, Sample};
use std::sync::Arc;

use portable_atomic::{AtomicF32, Ordering};
use serde::ser::Serializer;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use watermill::quantile::RollingQuantile;
use watermill::stats::Univariate;

pub struct RollingQuantileLatency {
    /// Join handle for the latency calculation task.
    pub join_handle: JoinHandle<()>,
    /// Send to update with each request duration.
    latency_tx: mpsc::Sender<Sample<f32>>,
    queue: QueueMetrics,
    /// rolling quantile latency in seconds. Updated async.
    seconds: Arc<AtomicF32>,
}

/// Task to be spawned per-RollingMedianLatency for calculating the value
struct RollingQuantileLatencyTask {
    /// Receive to update each request duration
    latency_rx: mpsc::Receiver<Sample<f32>>,
    queue_id: u64,
    /// Current estimate and update time
    seconds: Arc<AtomicF32>,
    /// quantile value.
    quantile: RollingQuantile<f32>,
}

impl RollingQuantileLatencyTask {
    fn new(
        latency_rx: mpsc::Receiver<Sample<f32>>,
        queue_id: u64,
        seconds: Arc<AtomicF32>,
        q: f32,
        window_size: usize,
    ) -> Self {
        let quantile = RollingQuantile::new(q, window_size).unwrap();

        Self {
            queue_id,
            latency_rx,
            seconds,
            quantile,
        }
    }

    /// Run the loop for updating latency.
    async fn run(mut self) {
        while let Some(rtt) = self.latency_rx.recv().await {
            rtt.received(self.queue_id, "rolling_quantile", self.latency_rx.len());
            self.update(rtt.value);
        }
    }

    /// Update the estimate object atomically.
    /// This is relaxed because we care more about speed than accuracy.
    fn update(&mut self, rtt: f32) {
        self.quantile.update(rtt);

        self.seconds
            .store(self.quantile.get(), portable_atomic::Ordering::Relaxed);
    }
}

impl RollingQuantileLatency {
    #[inline]
    pub fn record(&self, duration: Duration) {
        self.record_secs(duration.as_secs_f32())
    }

    /// if the channel is full (unlikely), entries will be silently dropped
    #[inline]
    pub fn record_secs(&self, secs: f32) {
        // self.latency_tx.send_async(secs).await.unwrap()
        if let Err(error) = self.latency_tx.try_send(self.queue.sample(secs)) {
            self.queue.dropped(&error);
        }
    }

    pub fn queue_stats(&self) -> LatencyQueueStats {
        self.queue.snapshot(&self.latency_tx)
    }

    /// Current .
    #[inline]
    pub fn seconds(&self) -> f32 {
        self.seconds.load(portable_atomic::Ordering::Relaxed)
    }

    /// Current median.
    #[inline]
    pub fn latency(&self) -> Duration {
        Duration::from_secs_f32(self.seconds())
    }
}

impl RollingQuantileLatency {
    pub async fn spawn(quantile_value: f32, window_size: usize) -> Self {
        // TODO: how should queue size and window size be related?
        let (latency_tx, latency_rx) = mpsc::channel(window_size);

        let seconds = Arc::new(AtomicF32::new(0.0));

        let queue = QueueMetrics::new();
        let task = RollingQuantileLatencyTask::new(
            latency_rx,
            queue.id,
            seconds.clone(),
            quantile_value,
            window_size,
        );

        let join_handle = tokio::spawn(task.run());

        Self {
            join_handle,
            latency_tx,
            queue,
            seconds,
        }
    }

    #[inline]
    pub async fn spawn_median(window_size: usize) -> Self {
        Self::spawn(0.5, window_size).await
    }
}

/// serialize as seconds
impl Serialize for RollingQuantileLatency {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f32(self.seconds.load(Ordering::Relaxed))
    }
}
