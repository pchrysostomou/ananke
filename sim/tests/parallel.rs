//! The parallel driver changes nothing about a seed (D-040): a seed run alone and
//! the same seed run inside the driver, beside its neighbours on other threads,
//! produce the same trace, byte for byte, for every scenario. Every pinned hash and
//! every failing seed therefore means the same thing whichever way the sweep ran.

use ananke_raft::core::Variant as RaftVariant;
use ananke_sim::{echo, engine, raft, sweep, wal};
use ananke_storage::Variant as WalVariant;
use moirae_trace::trace_hash;

/// Every seed of a batch, both ways, for the cheapest scenario; one seed among its
/// neighbours for each of the others, the raft sweep being the heaviest.
#[test]
fn a_seed_inside_the_driver_gives_the_sequential_trace() {
    let alone: Vec<String> = (0..8)
        .map(|seed| trace_hash(&echo::run(seed, echo::Variant::NoSyncDir).jsonl))
        .collect();
    let together = sweep(8, |seed| {
        trace_hash(&echo::run(seed, echo::Variant::NoSyncDir).jsonl)
    });
    assert_eq!(alone, together, "echo");

    let alone = trace_hash(&wal::run(42, WalVariant::Correct).jsonl);
    let together = sweep(4, |seed| {
        trace_hash(&wal::run(40 + seed, WalVariant::Correct).jsonl)
    });
    assert_eq!(together[2], alone, "wal");

    let alone = trace_hash(&engine::run(42, engine::Variant::Correct).jsonl);
    let together = sweep(4, |seed| {
        trace_hash(&engine::run(40 + seed, engine::Variant::Correct).jsonl)
    });
    assert_eq!(together[2], alone, "engine");

    let alone = trace_hash(&raft::run(42, RaftVariant::Correct).jsonl());
    let together = sweep(4, |seed| {
        trace_hash(&raft::run(40 + seed, RaftVariant::Correct).jsonl())
    });
    assert_eq!(together[2], alone, "raft");
}
