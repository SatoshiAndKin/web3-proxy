use std::sync::atomic::{AtomicU64, Ordering};

/// Implements an atomic pair of f32s
///
/// This uses an AtomicU64 internally.
#[derive(Debug)]
pub struct AtomicF32Pair(AtomicU64);

impl AtomicF32Pair {
    /// Creates a new atomic pair.
    pub fn new(pair: [f32; 2]) -> Self {
        Self(AtomicU64::new(to_bits(pair)))
    }

    /// Loads a value from the atomic pair.
    pub fn load(&self, ordering: Ordering) -> [f32; 2] {
        from_bits(self.0.load(ordering))
    }

    /// Stores both values with one atomic operation.
    pub fn store(&self, pair: [f32; 2], ordering: Ordering) {
        self.0.store(to_bits(pair), ordering);
    }
}

/// Convert a f32 pair to its bit-representation as u64
fn to_bits(pair: [f32; 2]) -> u64 {
    let f1 = pair[0].to_bits() as u64;
    let f2 = pair[1].to_bits() as u64;
    (f1 << 32) | f2
}

/// Build a f32 pair from its bit-representation as u64
fn from_bits(bits: u64) -> [f32; 2] {
    let f1 = f32::from_bits((bits >> 32) as u32);
    let f2 = f32::from_bits(bits as u32);
    [f1, f2]
}

#[cfg(test)]
mod tests {
    use std::f32;
    use std::sync::atomic::Ordering;

    use super::{from_bits, to_bits, AtomicF32Pair};

    #[test]
    fn test_f32_pair_bit_conversions() {
        let pair = [f32::consts::PI, f32::consts::E];
        assert_eq!(pair, from_bits(to_bits(pair)));
    }

    #[test]
    fn test_atomic_f32_pair_load() {
        let pair = [f32::consts::PI, f32::consts::E];
        let atomic = AtomicF32Pair::new(pair);
        assert_eq!(pair, atomic.load(Ordering::Relaxed));
    }

    #[test]
    fn test_atomic_f32_pair_store() {
        let pair = [f32::consts::PI, f32::consts::E];
        let atomic = AtomicF32Pair::new(pair);
        atomic.store([pair[0] + 1.0, pair[1] + 1.0], Ordering::Relaxed);
        assert_eq!(
            [pair[0] + 1.0, pair[1] + 1.0],
            atomic.load(Ordering::Relaxed)
        );
    }
}
