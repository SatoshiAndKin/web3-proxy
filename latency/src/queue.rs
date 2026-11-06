//! Channel snapshots avoid an extra counter on every successful send/receive.
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{sync::mpsc, time::Instant};

static NEXT_QUEUE: AtomicU64 = AtomicU64::new(1);
#[derive(Debug)]
pub(crate) struct QueueMetrics {
    pub(crate) id: u64,
    sampled: AtomicU64,
    full: AtomicU64,
    closed: AtomicU64,
}
#[derive(Debug, Serialize)]
pub struct LatencyQueueStats {
    pub id: u64,
    /// Includes outstanding reservations as well as queued messages.
    pub used_capacity: usize,
    pub capacity: usize,
    pub dropped_full: u64,
    pub dropped_closed: u64,
}
impl QueueMetrics {
    pub(crate) fn new() -> Self {
        // These independent diagnostic counters publish no other state.
        Self {
            id: NEXT_QUEUE.fetch_add(1, Ordering::Relaxed),
            sampled: AtomicU64::new(0),
            full: AtomicU64::new(0),
            closed: AtomicU64::new(0),
        }
    }
    pub(crate) fn sample<T>(&self, value: T) -> Sample<T> {
        let enabled = tracing::enabled!(target: "latency::perf", tracing::Level::INFO);
        let queued_at = (enabled && self.sampled.fetch_add(1, Ordering::Relaxed) & 1023 == 0)
            .then(Instant::now);
        Sample { value, queued_at }
    }
    pub(crate) fn dropped<T>(&self, error: &mpsc::error::TrySendError<T>) {
        match error {
            mpsc::error::TrySendError::Full(_) => &self.full,
            mpsc::error::TrySendError::Closed(_) => &self.closed,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn snapshot<T>(&self, tx: &mpsc::Sender<T>) -> LatencyQueueStats {
        LatencyQueueStats {
            id: self.id,
            used_capacity: tx.max_capacity() - tx.capacity(),
            capacity: tx.max_capacity(),
            dropped_full: self.full.load(Ordering::Relaxed),
            dropped_closed: self.closed.load(Ordering::Relaxed),
        }
    }
}
#[derive(Debug)]
pub(crate) struct Sample<T> {
    pub(crate) value: T,
    queued_at: Option<Instant>,
}
impl<T> Sample<T> {
    pub(crate) fn received(&self, id: u64, channel: &'static str, remaining: usize) {
        if let Some(start) = self.queued_at {
            tracing::info!(target: "latency::perf", parent: None, queue = id, channel, remaining,
                wait_us = start.elapsed().as_secs_f64() * 1_000_000.0, "latency channel");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueueMetrics;
    use tokio::sync::mpsc;
    #[test]
    fn snapshots_distinguish_full_and_closed_without_changing_queued_data() {
        let metrics = QueueMetrics::new();
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(metrics.sample(7)).unwrap();
        let error = tx.try_send(metrics.sample(8)).unwrap_err();
        metrics.dropped(&error);
        let stats = metrics.snapshot(&tx);
        assert_eq!(stats.used_capacity, 1);
        assert_eq!(stats.capacity, 1);
        assert_eq!(stats.dropped_full, 1);
        assert_eq!(stats.dropped_closed, 0);
        assert_eq!(rx.try_recv().unwrap().value, 7);
        assert_eq!(metrics.snapshot(&tx).used_capacity, 0);
        drop(rx);
        let error = tx.try_send(metrics.sample(9)).unwrap_err();
        metrics.dropped(&error);
        let stats = metrics.snapshot(&tx);
        assert_eq!(stats.dropped_full, 1);
        assert_eq!(stats.dropped_closed, 1);
    }
}
