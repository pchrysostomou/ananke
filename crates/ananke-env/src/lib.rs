//! The [`Environment`] trait through which every source of non-determinism in ananke
//! flows: clock, filesystem, network, randomness and task spawning.
//!
//! Two implementations live here (SPEC.md §1.1):
//!
//! - [`RealEnv`] — tokio, `std::fs` with fsync semantics honoured, OS entropy, system
//!   clock.
//! - `SimEnv` — a single-threaded deterministic executor with a virtual clock, seeded
//!   RNG, and in-memory network and filesystem with fault models (§1.3, §1.4). Not yet
//!   implemented.
//!
//! This is the only crate permitted to touch `std::time`, `std::fs`, `std::net`, tokio's
//! I/O and timers, or OS entropy, and inside it only the [`real`] module may do so.
//! Everything else in the workspace is generic over `E: Environment` (DECISIONS.md
//! D-003).
//!
//! # Conventions
//!
//! - Async trait methods return `impl Future<Output = _> + Send` rather than being
//!   `async fn`, so generic code can spawn the futures it builds.
//! - Time is [`Instant`] and [`WallTime`], never `std::time` (D-013).
//! - Hash maps are [`DetHashMap`] / [`DetHashSet`], seeded from an [`Rng`] (D-014).

mod clock;
mod collections;
mod env;
mod fs;
mod net;
mod rng;
mod task;
mod time;
mod trace;

pub mod real;

pub use clock::Clock;
pub use collections::{DetHashMap, DetHashSet, DetState, det_hash_map, det_hash_set};
pub use env::Environment;
pub use fs::{File, FileSystem, OpenOptions};
pub use net::Network;
pub use real::RealEnv;
pub use rng::Rng;
pub use task::{TaskHandle, TaskId};
pub use time::{Instant, WallTime, WallTimeOutOfRange};
pub use trace::TraceEvent;
