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

// Reporting bursts and the statistical window have independent bounds.
const REPORT_QUEUE_CAPACITY: usize = 4_096;
const RECEIVE_BATCH: usize = 128;
const REPORTS_PER_TURN: usize = 1_024;

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
        let mut reports = Vec::with_capacity(RECEIVE_BATCH);
        let mut processed = 0;
        loop {
            let count = self.latency_rx.recv_many(&mut reports, RECEIVE_BATCH).await;
            if count == 0 {
                break;
            }
            // One receive per batch avoids spending the cooperative task budget
            // on every report. Preserve sample order and the statistical window.
            for (index, rtt) in reports.drain(..).enumerate() {
                rtt.received(
                    self.queue_id,
                    "rolling_quantile",
                    self.latency_rx.len() + count - index - 1,
                );
                self.update(rtt.value);
            }
            processed += count;
            // Bound uninterrupted work even when producers keep the queue full.
            if processed >= REPORTS_PER_TURN {
                processed = 0;
                tokio::task::yield_now().await;
            }
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
        // Absorb short reporting bursts without increasing the rolling window.
        let (latency_tx, latency_rx) = mpsc::channel(REPORT_QUEUE_CAPACITY);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reporting_burst_preserves_the_requested_rolling_window() {
        let median = RollingQuantileLatency::spawn_median(3).await;
        // The current-thread runtime cannot drain the receiver until we yield.
        // This burst is larger than both the window and the former 1,000-slot
        // production queue, but the final median still uses only three samples.
        for value in 1..=2_051 {
            median.record_secs(value as f32);
        }
        assert_eq!(median.queue_stats().dropped_full, 0);
        assert_eq!(median.queue_stats().used_capacity, 2_051);
        let RollingQuantileLatency {
            join_handle,
            latency_tx,
            seconds,
            ..
        } = median;
        drop(latency_tx);
        tokio::time::timeout(Duration::from_secs(2), join_handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seconds.load(Ordering::Relaxed), 2_050.0);
    }

    #[tokio::test]
    async fn a_sparse_report_does_not_wait_for_a_full_receive_batch() {
        let median = RollingQuantileLatency::spawn_median(3).await;
        median.record_secs(7.0);
        tokio::time::timeout(Duration::from_secs(2), async {
            while median.seconds() != 7.0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(median.queue_stats().dropped_full, 0);
        let RollingQuantileLatency {
            join_handle,
            latency_tx,
            ..
        } = median;
        drop(latency_tx);
        tokio::time::timeout(Duration::from_secs(2), join_handle)
            .await
            .unwrap()
            .unwrap();
    }
}
