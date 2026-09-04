//! The [`Environment`] trait (SPEC.md §1.1).

use std::future::Future;

use crate::{Clock, FileSystem, Network, Rng, TaskHandle, TraceEvent};

/// Everything non-deterministic a node can do.
///
/// Every crate is generic over `E: Environment` and never touches the outside world
/// except through it (DECISIONS.md D-003). Implementations are handles: cheap to clone
/// and passed by value into the tasks they spawn.
pub trait Environment: Send + Sync + 'static {
    /// Monotonic time, wall time and timers.
    type Clock: Clock;
    /// The filesystem.
    type Fs: FileSystem;
    /// Node-to-node transport.
    type Net: Network;
    /// Randomness.
    type Rng: Rng;

    /// This node's clock.
    fn clock(&self) -> &Self::Clock;
    /// This node's filesystem.
    fn fs(&self) -> &Self::Fs;
    /// This node's network.
    fn net(&self) -> &Self::Net;
    /// This node's randomness.
    fn rng(&self) -> &Self::Rng;
    /// Runs `f` as a task named `name`.
    ///
    /// The name appears in traces and need not be unique. Dropping the returned handle
    /// detaches the task; it keeps running.
    fn spawn<F: Future<Output = ()> + Send + 'static>(
        &self,
        name: &'static str,
        f: F,
    ) -> TaskHandle;
    /// Records `event` in the run's trace.
    fn trace(&self, event: TraceEvent);
}
