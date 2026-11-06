//! Bounded diagnostic delivery. Formatting and output stay off RPC workers.
use serde::Serialize;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc as std_mpsc, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;

const CAPACITY: usize = 1024;
static QUEUE: OnceLock<Diagnostics> = OnceLock::new();

struct Diagnostics {
    tx: mpsc::Sender<Message>,
    full: AtomicU64,
    closed: AtomicU64,
}

enum Message {
    Event(Envelope),
    Flush(std_mpsc::SyncSender<()>),
}

struct Envelope {
    dispatch: tracing::Dispatch,
    queued_at: Instant,
    observed_unix_us: u64,
    event: Event,
}

enum Event {
    Request {
        sample: u64,
        stage: &'static str,
        backend: String,
        elapsed: Duration,
        complete: bool,
    },
    Channel {
        queue: u64,
        channel: &'static str,
        remaining: usize,
        wait: Duration,
    },
}

#[derive(Debug, Serialize)]
pub struct DiagnosticQueueStats {
    /// Includes outstanding send reservations.
    pub used_capacity: usize,
    pub capacity: usize,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub closed: bool,
}

impl Envelope {
    fn emit(self) {
        let queue_wait_us = self.queued_at.elapsed().as_secs_f64() * 1_000_000.0;
        let observed_unix_us = self.observed_unix_us;
        tracing::dispatcher::with_default(&self.dispatch, || match self.event {
            Event::Request {
                sample,
                stage,
                backend,
                elapsed,
                complete,
            } => {
                tracing::debug!(target: "web3_proxy::perf", parent: None,
                    sample, stage, backend, elapsed_us = elapsed.as_secs_f64() * 1_000_000.0,
                    complete, observed_unix_us, diagnostic_queue_wait_us = queue_wait_us,
                    "request stage");
            }
            Event::Channel {
                queue,
                channel,
                remaining,
                wait,
            } => {
                tracing::debug!(target: "latency::perf", parent: None,
                    queue, channel, remaining, wait_us = wait.as_secs_f64() * 1_000_000.0,
                    observed_unix_us, diagnostic_queue_wait_us = queue_wait_us, "latency channel");
            }
        });
    }
}

fn submit(event: Event) {
    let observed_unix_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_micros() as u64);
    let queue = QUEUE.get_or_init(|| {
        let (tx, mut rx) = mpsc::channel(CAPACITY);
        // If creation fails, dropping the receiver makes failures visible through
        // dropped_closed. Diagnostic failure must not panic or block an RPC.
        let _ = std::thread::Builder::new()
            .name("rpc-perf-events".into())
            .spawn(move || {
                while let Some(message) = rx.blocking_recv() {
                    match message {
                        Message::Event(event) => event.emit(),
                        Message::Flush(done) => {
                            let _ = done.send(());
                        }
                    }
                }
            });
        Diagnostics {
            tx,
            full: AtomicU64::new(0),
            closed: AtomicU64::new(0),
        }
    });
    let event = Envelope {
        dispatch: tracing::dispatcher::get_default(Clone::clone),
        queued_at: Instant::now(),
        observed_unix_us,
        event,
    };
    if let Err(error) = queue.tx.try_send(Message::Event(event)) {
        // Independent diagnostic counters publish no other state.
        match error {
            mpsc::error::TrySendError::Full(_) => &queue.full,
            mpsc::error::TrySendError::Closed(_) => &queue.closed,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}

pub fn record_request_stage(
    sample: u64,
    stage: &'static str,
    backend: &str,
    elapsed: Duration,
    complete: bool,
) {
    if tracing::enabled!(target: "web3_proxy::perf", tracing::Level::DEBUG) {
        submit(Event::Request {
            sample,
            stage,
            backend: backend.to_owned(),
            elapsed,
            complete,
        });
    }
}

pub(crate) fn record_channel(queue: u64, channel: &'static str, remaining: usize, wait: Duration) {
    if tracing::enabled!(target: "latency::perf", tracing::Level::DEBUG) {
        submit(Event::Channel {
            queue,
            channel,
            remaining,
            wait,
        });
    }
}

/// Snapshot an existing queue. This does not start a worker or read a clock.
pub fn diagnostic_queue_stats() -> Option<DiagnosticQueueStats> {
    QUEUE.get().map(|queue| DiagnosticQueueStats {
        used_capacity: queue.tx.max_capacity() - queue.tx.capacity(),
        capacity: queue.tx.max_capacity(),
        dropped_full: queue.full.load(Ordering::Relaxed),
        dropped_closed: queue.closed.load(Ordering::Relaxed),
        closed: queue.tx.is_closed(),
    })
}

/// Best-effort drain of records queued before this call. Use after RPC shutdown,
/// outside async workers; request handling must never wait for diagnostic output.
pub fn flush_diagnostics(timeout: Duration) -> bool {
    let Some(queue) = QUEUE.get() else {
        return true;
    };
    let deadline = Instant::now() + timeout;
    let (done, wait) = std_mpsc::sync_channel(1);
    let mut marker = Message::Flush(done);
    loop {
        match queue.tx.try_send(marker) {
            Ok(()) => break,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(message)) => {
                marker = message;
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return false;
                }
                std::thread::sleep(remaining.min(Duration::from_millis(1)));
            }
        }
    }
    wait.recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        sync::{atomic::AtomicBool, Arc, Condvar, Mutex},
    };

    #[derive(Clone)]
    struct BlockedWriter {
        entered: Arc<AtomicBool>,
        started: std_mpsc::SyncSender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }
    impl io::Write for BlockedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if !self.entered.swap(true, Ordering::Relaxed) {
                let _ = self.started.send(());
            }
            let (lock, wake) = &*self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, wake) = &*self.0;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
    }

    #[test]
    fn blocked_output_drops_diagnostics_without_blocking_latency_reports() {
        let (started, ready) = std_mpsc::sync_channel(1);
        let release = Release(Arc::new((Mutex::new(false), Condvar::new())));
        let writer = BlockedWriter {
            entered: Arc::new(AtomicBool::new(false)),
            started,
            release: release.0.clone(),
        };
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(move || writer.clone())
                .finish(),
        );
        let first_dispatch = dispatch.clone();
        let first = std::thread::spawn(move || {
            tracing::dispatcher::with_default(&first_dispatch, || {
                record_request_stage(1, "first", "", Duration::ZERO, true);
            });
        });
        ready.recv_timeout(Duration::from_secs(2)).unwrap();
        let (done, result) = std_mpsc::sync_channel(1);
        let producer = std::thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                let flushed_while_blocked = flush_diagnostics(Duration::from_millis(10));
                for _ in 0..=CAPACITY {
                    record_request_stage(2, "queued", "", Duration::ZERO, true);
                }
                let metrics = crate::queue::QueueMetrics::new();
                let (tx, mut rx) = mpsc::channel(1);
                tx.try_send(metrics.sample(7)).unwrap();
                let message = rx.try_recv().unwrap();
                message.received(metrics.id, "test", 0);
                done.send((
                    flushed_while_blocked,
                    message.value,
                    metrics.snapshot(&tx).dropped_full,
                ))
                .unwrap();
            });
        });
        let delivered = result.recv_timeout(Duration::from_secs(2));
        drop(release);
        first.join().unwrap();
        producer.join().unwrap();
        assert_eq!(delivered.unwrap(), (false, 7, 0));
        assert!(diagnostic_queue_stats().unwrap().dropped_full > 0);
        assert!(flush_diagnostics(Duration::from_secs(2)));
    }
}
