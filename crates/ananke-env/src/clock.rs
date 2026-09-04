//! The [`Clock`] trait (SPEC.md §1.2).

use std::future::Future;
use std::time::Duration;

use crate::{Instant, WallTime};

/// Monotonic time, wall time and timers.
///
/// Under simulation each node has its own clock with configurable skew and drift, so
/// nothing may compare instants or wall times taken on different nodes as if they
/// shared an epoch. Timers are futures resolved by the scheduler; there is no other
/// way to wait for time to pass.
pub trait Clock: Send + Sync + 'static {
    /// The current monotonic time. Never decreases between calls on the same clock.
    fn now(&self) -> Instant;

    /// The current wall-clock time. May jump in either direction.
    fn wall(&self) -> WallTime;

    /// Resolves once [`now`](Self::now) is at or past `deadline`, immediately if it
    /// already is.
    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send;

    /// Resolves after at least `duration` has elapsed on this clock.
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.sleep_until(self.now() + duration)
    }
}
