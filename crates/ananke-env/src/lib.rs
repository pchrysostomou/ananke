//! The `Environment` trait through which every source of non-determinism in ananke
//! flows: clock, filesystem, network, randomness and task spawning.
//!
//! Two implementations live here (SPEC.md §1.1):
//!
//! - `RealEnv` — tokio, `std::fs` with fsync semantics honoured, OS RNG, system clock.
//! - `SimEnv` — a single-threaded deterministic executor with a virtual clock, seeded
//!   RNG, and in-memory network and filesystem with fault models (§1.3, §1.4).
//!
//! This is the only crate permitted to call `std::time`, `std::fs`, `tokio::net`,
//! `tokio::time`, `rand`, or to spawn threads. Everything else in the workspace is
//! generic over `E: Environment` (DECISIONS.md D-003).
