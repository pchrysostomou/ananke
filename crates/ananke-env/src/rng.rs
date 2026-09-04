//! The [`Rng`] trait.

/// A source of random bytes.
///
/// Every method takes `&self`; implementations use interior mutability so one `Rng` can
/// be shared across tasks. Under [`RealEnv`](crate::RealEnv) this is OS entropy; under
/// simulation it is a generator seeded from the run seed, so every draw is reproducible.
pub trait Rng: Send + Sync + 'static {
    /// Fills `dest` with random bytes.
    fn fill_bytes(&self, dest: &mut [u8]);

    /// A uniformly distributed `u64`.
    fn next_u64(&self) -> u64 {
        let mut bytes = [0u8; 8];
        self.fill_bytes(&mut bytes);
        u64::from_le_bytes(bytes)
    }

    /// A uniformly distributed `u32`.
    fn next_u32(&self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// A uniformly distributed integer in `0..bound`.
    ///
    /// # Panics
    ///
    /// If `bound` is zero.
    fn below(&self, bound: u64) -> u64 {
        assert!(bound > 0, "Rng::below called with bound 0");
        // Rejection sampling: draws from the top, partial bucket are discarded so that
        // `% bound` is unbiased. `zone` is the largest multiple of `bound` below 2^64.
        let zone = u64::MAX - (u64::MAX % bound);
        loop {
            let draw = self.next_u64();
            if draw < zone {
                return draw % bound;
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    use super::Rng;

    /// Hands out a fixed sequence of `u64`s; panics when exhausted.
    pub(crate) struct Scripted(Mutex<Vec<u64>>);

    impl Scripted {
        pub(crate) fn new(values: impl IntoIterator<Item = u64>) -> Self {
            let mut v: Vec<u64> = values.into_iter().collect();
            v.reverse();
            Self(Mutex::new(v))
        }
    }

    impl Rng for Scripted {
        fn fill_bytes(&self, dest: &mut [u8]) {
            for chunk in dest.chunks_mut(8) {
                let value = self
                    .0
                    .lock()
                    .unwrap()
                    .pop()
                    .expect("Scripted rng exhausted");
                let bytes = value.to_le_bytes();
                chunk.copy_from_slice(&bytes[..chunk.len()]);
            }
        }
    }

    #[test]
    fn next_u64_is_little_endian_over_fill_bytes() {
        let rng = Scripted::new([0x0102_0304_0506_0708]);
        assert_eq!(rng.next_u64(), 0x0102_0304_0506_0708);
    }

    #[test]
    fn below_rejects_the_partial_top_bucket() {
        // u64::MAX % 10 == 5, so the zone ends at u64::MAX - 5 and u64::MAX is rejected.
        let rng = Scripted::new([u64::MAX, 7]);
        assert_eq!(rng.below(10), 7);
    }

    #[test]
    fn below_one_is_always_zero() {
        let rng = Scripted::new([12_345]);
        assert_eq!(rng.below(1), 0);
    }

    #[test]
    #[should_panic(expected = "bound 0")]
    fn below_zero_panics() {
        let rng = Scripted::new([0]);
        let _ = rng.below(0);
    }
}
