//! The simulator's random streams (DECISIONS.md D-017).
//!
//! Every stream is derived from the seed by name through [`moirae_sched::stream`]:
//! `sched` belongs to the scheduling policy, `net` to drops and delays, `fs` to lost
//! fsyncs and torn writes, `clock` to skew and drift, and each node has `n{id}/protocol`
//! for what the protocol asks and `n{id}/sched` for what the executor asks inside its
//! tasks. Nothing on one stream can perturb another.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moirae_sched::Pcg32;

use crate::Rng;

/// One named stream handed to a node as its [`Rng`]. Clones share the stream, so every
/// handle to a node draws from the same sequence.
#[derive(Clone)]
pub struct SimRng {
    inner: Arc<Mutex<Pcg32>>,
}

impl SimRng {
    pub(super) fn new(rng: Pcg32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(rng)),
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

/// Uniform in `min..=max`, in whole nanoseconds, from `rng`.
pub(super) fn duration_between(rng: &mut Pcg32, min: Duration, max: Duration) -> Duration {
    let (lo, hi) = (min.as_nanos(), max.as_nanos());
    let span = u64::try_from(hi.saturating_sub(lo)).unwrap_or(u64::MAX);
    let lo = u64::try_from(lo).unwrap_or(u64::MAX);
    Duration::from_nanos(lo.saturating_add(rng.below(span.saturating_add(1))))
}

/// Uniform in `-max..=max` from `rng`; zero when `max` is not positive.
pub(super) fn symmetric(rng: &mut Pcg32, max: i64) -> i64 {
    if max <= 0 {
        return 0;
    }
    let span = (max as u64) * 2 + 1;
    i64::try_from(rng.below(span)).unwrap_or(0) - max
}
