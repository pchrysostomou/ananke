//! The parallel driver changes nothing about a seed (D-040): a seed run alone and
//! the same seed run inside the driver, beside its neighbours on other threads,
//! produce the same trace, byte for byte, for every scenario. Every pinned hash and
//! every failing seed therefore means the same thing whichever way the sweep ran.

use ananke_raft::core::{Variant as RaftVariant, Variants};
use ananke_shard::range_variant::RangeVariants;
use ananke_shard::variant::NodeVariants;
use ananke_sim::raft::Cluster;
use ananke_sim::{
    echo, engine, install, membership, quorum, raft, ranges, reseed, shard, split, sweep, wal,
};
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

    // PROPOSED(D-054): the live install's crash test is the engine scenario with
    // installs and crashes aimed at them.
    let alone = trace_hash(
        &engine::run_with(42, engine::Schedule::install(), engine::Variant::Correct).jsonl,
    );
    let together = sweep(4, |seed| {
        trace_hash(
            &engine::run_with(
                40 + seed,
                engine::Schedule::install(),
                engine::Variant::Correct,
            )
            .jsonl,
        )
    });
    assert_eq!(together[2], alone, "engine with installs");

    let alone = trace_hash(&raft::run(42, RaftVariant::Correct).jsonl());
    let together = sweep(4, |seed| {
        trace_hash(&raft::run(40 + seed, RaftVariant::Correct).jsonl())
    });
    assert_eq!(together[2], alone, "raft");
}

/// A scenario's seed 42 run alone and the same seed inside the driver beside its
/// neighbours give one trace, for `trace`, the scenario's trace as text.
fn same_inside_the_driver(name: &str, trace: impl Fn(u64) -> String + Sync + Send) {
    let alone = trace(42);
    let together = sweep(4, |seed| trace(40 + seed));
    assert!(together[2] == alone, "{name}");
}

/// The scenarios that run the node of SHARD.md §4, each on one seed among its neighbours:
/// every Phase 3 scenario's pinned seed means the same whichever way its sweep ran.
/// `sim/reseed.rs` and `sim/install.rs` keep no moirae header in their reports, so theirs
/// is the records themselves, which is the moirae export's whole input but for the header.
// PROPOSED(D-113): the node scenarios join the driver's proof.
#[test]
fn a_seed_of_a_node_scenario_inside_the_driver_gives_the_sequential_trace() {
    let (cores, node, range) = (
        Variants::default(),
        NodeVariants::correct(),
        RangeVariants::correct(),
    );
    same_inside_the_driver("the node under the raft sweep's arms", |seed| {
        raft::run_on_the_node(seed, cores, node, range).jsonl()
    });
    same_inside_the_driver("the node scenario", |seed| {
        ranges::run(seed, cores, node).checked.jsonl()
    });
    same_inside_the_driver("membership on the node", |seed| {
        membership::run_on_node(Cluster::Node, seed, cores, node, range).jsonl()
    });
    same_inside_the_driver("quorum on the node", |seed| {
        quorum::node_run(seed, cores, node).jsonl()
    });
    same_inside_the_driver("the install scenario", |seed| {
        format!("{:?}", install::run(seed, cores, node).records)
    });
    same_inside_the_driver("the re-seed scenario", |seed| {
        format!("{:?}", reseed::run(seed, cores, node).records)
    });
    same_inside_the_driver("split's vote shape", |seed| split::run(seed, range).jsonl());
    same_inside_the_driver("split's overlap shape", |seed| {
        split::run_overlap(seed, range).jsonl()
    });
    same_inside_the_driver("the sharded sweep", |seed| shard::run(seed).jsonl());
}
