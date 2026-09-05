//! The ananke storage engine (SPEC.md §2): a write-ahead log, versioned memtables and
//! SSTables under a manifest, an LSM tree as Phase 1 proceeds. Everything is generic over [`ananke_env::Environment`], so the
//! code that runs on a real disk is the code the simulator crashes ten thousand times.
//!
//! This is the one crate the workspace permits `unsafe` in (BOOTSTRAP_PROMPT.md,
//! principle 3). Nothing here uses it yet, so the crate carries no `allow` and the
//! workspace lint still denies it.
//!
//! # Fault-model tests
//!
//! Every piece of this crate ships with a [`wal::Variant`]-style pair: the correct
//! code and known-buggy versions of it. The crash sweeps in `sim/` must pass the
//! former and catch the latter; either alone proves nothing (CLAUDE.md).

pub mod compaction;
pub mod crc32c;
pub mod engine;
pub mod ikey;
pub mod manifest;
pub mod memtable;
pub mod merge;
pub mod sst;
pub mod turnstile;
pub mod wal;

pub use compaction::Compaction;
pub use engine::{Engine, EngineConfig, EngineRecovery, OpenRefused, Snapshot, Write};
pub use memtable::{Memtable, Value};
pub use wal::{
    Append, CoveredStop, HeadGap, HeadGapPolicy, Recovery, Seq, Variant, Wal, WalConfig,
};
