//! ananke is a distributed SQL database written in Rust, built from the ground up to
//! be deterministically testable: every source of non-determinism goes through one
//! `Environment` abstraction, so the code that runs in production also runs inside a
//! deterministic simulator.
//!
//! Phase 0 ships the deterministic runtime as the `ananke-env` crate; this crate is
//! still the placeholder that reserves the name, and becomes the public facade over the
//! workspace crates once there is a database to expose. Development happens at
//! <https://github.com/pchrysostomou/ananke>.
