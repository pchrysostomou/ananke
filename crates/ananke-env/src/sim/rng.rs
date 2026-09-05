//! The simulator's seeded generator.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::Rng;

/// xoshiro256** (Blackman and Vigna, 2018), seeded through SplitMix64.
///
/// Implemented here rather than pulled from a crate so that the byte stream behind a
/// seed can never change under a dependency bump (SPEC.md §1.5).
#[derive(Clone)]
pub(super) struct Xoshiro256StarStar {
    s: [u64; 4],
}

impl Xoshiro256StarStar {
    pub(super) fn seed_from_u64(seed: u64) -> Self {
        let mut x = seed;
        let mut s = [0u64; 4];
        for word in &mut s {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            *word = z ^ (z >> 31);
        }
        Self { s }
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        let s = &mut self.s;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    pub(super) fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    /// Uniform in `0..bound`; `bound` must be non-zero.
    pub(super) fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "below called with bound 0");
        let zone = u64::MAX - (u64::MAX % bound);
        loop {
            let draw = self.next_u64();
            if draw < zone {
                return draw % bound;
            }
        }
    }

    /// `true` with probability `p`; `p <= 0` is never, `p >= 1` is always.
    pub(super) fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            false
        } else if p >= 1.0 {
            true
        } else {
            // 53 random bits make a uniform f64 in [0, 1).
            let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
            unit < p
        }
    }

    /// Uniform in `min..=max`.
    pub(super) fn duration_between(&mut self, min: Duration, max: Duration) -> Duration {
        let (lo, hi) = (min.as_nanos(), max.as_nanos());
        let span = u64::try_from(hi.saturating_sub(lo)).unwrap_or(u64::MAX);
        let lo = u64::try_from(lo).unwrap_or(u64::MAX);
        Duration::from_nanos(lo.saturating_add(self.below(span.saturating_add(1))))
    }
}

/// A node's own seeded generator, exposed through [`Rng`]. Clones share one stream.
#[derive(Clone)]
pub struct SimRng {
    inner: Arc<Mutex<Xoshiro256StarStar>>,
}

impl SimRng {
    pub(super) fn new(seed: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Xoshiro256StarStar::seed_from_u64(seed))),
        }
    }
}

impl Rng for SimRng {
    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fill_bytes(dest);
    }

    fn next_u64(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_u64()
    }
}

impl fmt::Debug for SimRng {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SimRng(..)")
    }
}
