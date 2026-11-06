//! Sampled wall-time diagnostics. Enable only `web3_proxy::perf=info`.
//! No RPC parameters, client IDs, headers, or response data enter these events.
use std::{
    cell::Cell,
    num::NonZeroU64,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::time::Instant;

thread_local! { static SAMPLES: Cell<u16> = const { Cell::new(0) }; }
// IDs are independent counters. They do not publish or protect other data.
static NEXT_SAMPLE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn sample() -> Option<NonZeroU64> {
    if !tracing::enabled!(target: "web3_proxy::perf", tracing::Level::INFO) {
        return None;
    }
    SAMPLES.with(|counter| {
        let value = counter.get();
        counter.set(value.wrapping_add(1));
        (value & 1023 == 0)
            .then(|| NEXT_SAMPLE.fetch_add(1, Ordering::Relaxed))
            .and_then(NonZeroU64::new)
    })
}

pub(crate) struct Stage<'a> {
    sample: Option<(NonZeroU64, Instant)>,
    stage: &'static str,
    backend: &'a str,
    complete: bool,
}
impl<'a> Stage<'a> {
    pub(crate) fn new(sample: Option<NonZeroU64>, stage: &'static str, backend: &'a str) -> Self {
        Self {
            sample: sample.map(|id| (id, Instant::now())),
            stage,
            backend,
            complete: false,
        }
    }
    pub(crate) fn since(
        sample: Option<NonZeroU64>,
        start: Instant,
        stage: &'static str,
        backend: &'a str,
    ) -> Self {
        Self {
            sample: sample.map(|id| (id, start)),
            stage,
            backend,
            complete: false,
        }
    }
    pub(crate) fn finish(&mut self) {
        self.complete = true;
    }
}
impl Drop for Stage<'_> {
    fn drop(&mut self) {
        if let Some((sample, start)) = self.sample {
            tracing::info!(target: "web3_proxy::perf", parent: None, sample = sample.get(), stage = self.stage,
                backend = self.backend, elapsed_us = start.elapsed().as_secs_f64() * 1_000_000.0,
                complete = self.complete, "request stage");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Stage;
    use std::{
        io,
        num::NonZeroU64,
        sync::{Arc, Mutex},
    };
    #[derive(Clone)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn sampled_scopes_report_completion_and_drop_without_client_context() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer(output.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!("request", client_header = "private-client-value");
            let _entered = parent.enter();
            let mut finished = Stage::new(NonZeroU64::new(1), "finished", "node");
            finished.finish();
            drop(finished);
            drop(Stage::new(NonZeroU64::new(2), "cancelled", "node"));
            drop(Stage::new(None, "disabled", "node"));
        });
        let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert_eq!(text.lines().count(), 2, "{text}");
        assert!(!text.contains("private-client-value"));
        assert!(text
            .lines()
            .any(|x| x.contains("finished") && x.contains("complete=true")));
        assert!(text
            .lines()
            .any(|x| x.contains("cancelled") && x.contains("complete=false")));
    }
}
