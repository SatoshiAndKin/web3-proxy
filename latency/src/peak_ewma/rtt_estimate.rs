use std::sync::atomic::Ordering;

use tokio::time::{Duration, Instant};
use tracing::trace;

use crate::util::atomic_f32_pair::AtomicF32Pair;
use crate::util::nanos::{nanos, NANOS_PER_MILLI};

/// Holds the current RTT estimate and the last time this value was updated.
#[derive(Debug)]
pub struct RttEstimate {
    pub update_at: Instant,
    pub rtt: Duration,
}

impl RttEstimate {
    /// Update the estimate with a new rtt value. Use rtt=0.0 for simply
    /// decaying the current value.
    pub fn update(&mut self, rtt: f64, decay_ns: f64, now: Instant) -> Duration {
        // Reports can arrive out of order across producers. Keep the peak
        // from a late report, without rewinding the estimator clock.
        let now = now.max(self.update_at);
        let ewma = nanos(self.rtt);
        self.rtt = if ewma < rtt {
            // For Peak-EWMA, always use the worst-case (peak) value as the estimate for
            // subsequent requests.
            trace!(
                "update peak rtt={}ms prior={}ms",
                rtt / NANOS_PER_MILLI,
                ewma / NANOS_PER_MILLI,
            );
            Duration::from_nanos(rtt as u64)
        } else {
            // When a latency is observed that is less than the estimated latency, we decay the
            // prior estimate according to how much time has elapsed since the last
            // update. The inverse of the decay is used to scale the estimate towards the
            // observed latency value.
            let elapsed = nanos(now.saturating_duration_since(self.update_at));
            let decay = (-elapsed / decay_ns).exp();
            let recency = 1.0 - decay;
            let next_estimate = (ewma * decay) + (rtt * recency);
            trace!(
                "update duration={:03.0}ms decay={:06.0}ns; next={:03.0}ms",
                rtt / NANOS_PER_MILLI,
                ewma - next_estimate,
                next_estimate / NANOS_PER_MILLI,
            );
            Duration::from_nanos(next_estimate as u64)
        };

        self.update_at = now;
        self.rtt
    }

    /// Build a new estimate object using current time.
    pub(super) fn new(start_duration: Duration) -> Self {
        Self {
            update_at: Instant::now(),
            rtt: start_duration,
        }
    }

    /// Convert to pair of f32
    fn as_pair(&self, start_time: Instant) -> [f32; 2] {
        let update_at = self
            .update_at
            .saturating_duration_since(start_time)
            .as_secs_f32();
        let rtt = self.rtt.as_secs_f32();
        [update_at, rtt]
    }

    /// Build from pair of f32
    fn from_pair(pair: [f32; 2], start_time: Instant) -> Self {
        let update_at = start_time + Duration::from_secs_f32(pair[0]);
        let rtt = Duration::from_secs_f32(pair[1]);
        Self { update_at, rtt }
    }
}

/// Atomic storage of RttEstimate using AtomicF32Pair
///
/// Start time is needed to (de-)serialize the update_at instance.
#[derive(Debug)]
pub struct AtomicRttEstimate {
    pair: AtomicF32Pair,
    start_time: Instant,
}

impl AtomicRttEstimate {
    /// Creates a new atomic rtt estimate.
    pub(super) fn new(estimate: &RttEstimate) -> Self {
        Self {
            pair: AtomicF32Pair::new(estimate.as_pair(estimate.update_at)),
            start_time: estimate.update_at,
        }
    }

    /// Loads a value from the atomic rtt estimate.
    ///
    /// This method omits the ordering argument since loads may use
    /// slightly stale data to avoid adding additional latency.
    pub fn load(&self) -> RttEstimate {
        RttEstimate::from_pair(self.pair.load(Ordering::Relaxed), self.start_time)
    }

    /// The latency worker is the only writer. Readers load both fields
    /// together from the atomic pair, which does not publish any other data.
    pub(super) fn store(&self, estimate: &RttEstimate) {
        self.pair
            .store(estimate.as_pair(self.start_time), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{self, Duration, Instant};

    use super::{AtomicRttEstimate, RttEstimate};

    #[test]
    fn test_rtt_estimate_f32_conversions() {
        let rtt = Duration::from_secs(1);
        let expected = RttEstimate::new(rtt);
        let actual =
            RttEstimate::from_pair(expected.as_pair(expected.update_at), expected.update_at);
        assert_eq!(expected.update_at, actual.update_at);
        assert_eq!(expected.rtt, actual.rtt);
    }

    #[tokio::test(start_paused = true)]
    async fn late_peak_does_not_rewind_the_clock() {
        let mut estimate = RttEstimate::new(Duration::from_millis(10));
        let start = estimate.update_at;
        let newer = start + Duration::from_secs(2);
        estimate.update(20_000_000.0, 1e9, newer);
        estimate.update(100_000_000.0, 1e9, start + Duration::from_secs(1));
        assert_eq!(estimate.update_at, newer);
        assert_eq!(estimate.rtt, Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn test_atomic_rtt_estimate_load() {
        let rtt = Duration::from_secs(1);
        let estimate = AtomicRttEstimate::new(&RttEstimate::new(rtt));
        let actual = estimate.load();
        assert_eq!(Instant::now(), actual.update_at);
        assert_eq!(rtt, actual.rtt);
    }

    #[tokio::test(start_paused = true)]
    async fn test_atomic_rtt_estimate_store() {
        let start_time = Instant::now();
        let rtt = Duration::from_secs(1);
        let estimate = AtomicRttEstimate::new(&RttEstimate::new(rtt));
        time::advance(Duration::from_secs(1)).await;
        estimate.store(&RttEstimate {
            update_at: Instant::now(),
            rtt: Duration::from_secs(2),
        });
        let rv = estimate.load();
        assert_eq!(start_time + Duration::from_secs(1), rv.update_at);
        assert_eq!(Duration::from_secs(2), rv.rtt);
    }
}
