//! Simulation scenarios for ananke.
//!
//! Each scenario is a module in this directory that builds a small cluster under
//! [`ananke_env::sim::Sim`], injects faults, and returns a report with the trace it
//! produced. Integration tests under `tests/` drive the scenarios and check
//! properties such as byte-identical traces for equal seeds (SPEC.md §1.6).
//!
//! Every sweep runs [`seeds`] consecutive seeds, in parallel through [`sweep`]
//! (D-040), in four tiers: 20 by default and under `scripts/gate.sh`, 100 in CI,
//! 1000 under `scripts/premerge.sh` in release on the machine in front of you, and
//! 10 000 in the nightly workflow on GitHub, the only place ten thousand run. A seed
//! that fails a sweep has its trace written through [`write_trace`] so the nightly
//! can upload it and the studio can open it.

use std::path::Path;

use ananke_env::sim::TraceRecord;
use ananke_env::{Environment, File, FileSystem, OpenOptions, RealEnv};
use ananke_raft::invariants::Traced;
use bytes::Bytes;

pub mod echo;
pub mod engine;
pub mod lin;
pub mod membership;
pub mod parallel;
pub mod quorum;
pub mod raft;
pub mod ranges;
pub mod wal;

pub use parallel::{sweep, verdict};

/// The records as the log invariants read them: each event with the node that
/// traced it ([`Traced`]). Every `Raft*` event about a replica names its server,
/// and the two range events the checks fold — `RangeCreated`, which sets a
/// replica's floor, and `RangeRemoved`, which ends a replica's memory — name their
/// range and are recorded with their node alone (SHARD.md §8). A scenario's node
/// ids are its server ids.
// PROPOSED(D-071): checks 1 to 4 keyed by group.
pub fn traced(records: &[TraceRecord]) -> impl Iterator<Item = Traced<'_>> {
    records
        .iter()
        .map(|record| Traced::new(record.node.map(|node| u64::from(node.get())), &record.event))
}

/// The default number of seeds a sweep runs.
pub const DEFAULT_SEEDS: u64 = 20;

/// How many seeds a sweep runs: `ANANKE_SEEDS` if set and a number, else
/// [`DEFAULT_SEEDS`].
#[must_use]
pub fn seeds() -> u64 {
    std::env::var("ANANKE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_SEEDS)
}

/// How many seeds the nightly's deep-levels run sweeps: `ANANKE_DEEP_SEEDS`, 0 when
/// unset, which skips the run. Set only by the nightly.
#[must_use]
pub fn deep_seeds() -> u64 {
    std::env::var("ANANKE_DEEP_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Writes a moirae trace to `out/<name>.jsonl` under the crate directory, replacing
/// any previous one, so a mismatch or a failing seed leaves the bytes on disk to open
/// in the studio or upload from CI.
pub fn write_trace(name: &str, jsonl: &str) {
    let path = format!("out/{name}.jsonl");
    let jsonl = jsonl.to_owned();
    RealEnv::run(|env| async move {
        let fs = env.fs();
        fs.create_dir_all(Path::new("out"))
            .await
            .expect("out/ can be created");
        let file = fs
            .open(
                Path::new(&path),
                OpenOptions::new().write(true).create(true).truncate(true),
            )
            .await
            .expect("the trace file can be opened");
        file.write_at(0, Bytes::from(jsonl))
            .await
            .expect("the trace can be written");
        file.sync().await.expect("the trace can be synced");
    });
}
