//! The node's snapshot wiring: a stream flows and an install completes, per range and
//! per follower (SHARD.md §12, Stage B's "installs on the node"; §11, storage 5).
//!
//! `ananke_shard::snapshot` has existed since PR #83 and `ananke_shard::server::run`
//! never ran it (PR #86). This binary is the directed evidence that it does now: eight
//! streams and eight installs on every seed, and each of the mutations below seen to
//! fail the check the correct node passes (CLAUDE.md's pair rule).
//!
//! The scenario is `ananke_sim::install`: five voters, four ranges, three nodes
//! running while two are absent, and the two joining after the running three have
//! compacted past them.

use ananke_raft::core::Variants;
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::install::{self, Report};
use ananke_sim::{sweep, verdict};

/// The correct node.
fn correct(seed: u64) -> Report {
    install::run(seed, Variants::default(), NodeVariants::correct())
}

/// The correct node's cores, with one of the wiring's variants on the node itself.
fn with(seed: u64, variant: NodeVariant) -> Report {
    install::run(
        seed,
        Variants::default(),
        NodeVariants::correct().with(variant),
    )
}

#[test]
fn a_stream_flows_and_an_install_completes_on_every_range_and_every_follower() {
    let report = correct(1);
    report
        .check()
        .expect("the correct node installs on every range and every follower");

    // The figures the claim rests on, printed rather than asserted in the abstract:
    // eight (range, follower) streams, eight installs, eight creations.
    let owed = Report::owed();
    println!(
        "install: {} streams opened, {} installs completed, {} replicas created by \
         their install, {} states read back, {} writes committed before the late nodes \
         joined, {} streams at once on one leader; {} owed",
        report.streams().len(),
        report.installs().len(),
        report.created_by_install().len(),
        report.states().len(),
        report.wrote,
        report.streams_at_once(),
        owed.len()
    );
    assert!(
        report.installs().len() >= owed.len(),
        "the run installed on fewer (server, range) pairs than it owed"
    );
    // The client made progress at all: without this the whole run could have elected,
    // idled, taken nothing and installed nothing, and every clause above would be
    // vacuous. It is an observation, not a tuned figure — seed 1 committed well over a
    // thousand writes — so what is asserted is that it is not zero.
    assert!(
        report.wrote > 0,
        "the client committed nothing before the late nodes joined, so every range's \
         log was empty and nothing above is evidence of anything"
    );

    // The refusals, by reason: what RAFT.md:209-212's bounds count and what they must
    // not (D-087). Printed rather than asserted — the figure is evidence about this
    // tree, and the bounds themselves are checked where they are decided, in
    // `ananke_shard::install`.
    // PROPOSED(D-087): the restart and cap-wait counts are readable from a run.
    println!("install: start-overs by reason: {:?}", report.start_overs());
}

/// How many seeds this binary runs, at every tier.
///
/// It does **not** read `ANANKE_SEEDS`, and that is deliberate rather than an
/// oversight. This is a directed scenario: its situation — four ranges compacted past
/// two absent voters — is reached by construction on every seed, not searched for
/// among rare interleavings, and `Report::reached` fails any seed that does not reach
/// it. What varies between seeds here is the network's losses and the scheduler's
/// order, which is worth a handful of runs and is not worth a shard of the nightly:
/// at the nightly's tier this scenario would be ten thousand runs of five nodes for
/// twenty simulated seconds, about as much CPU as one whole shard carries today
/// (D-064), bought against a catch that does not vary.
///
/// Putting it under the four tiers, with the weight D-064's table wants measured on a
/// quiet machine, is owed and is recorded as owed in D-083 and as issue #108 — not
/// quietly taken here. A figure measured on this laptop, with several Stage B slices
/// building on it, would be exactly the incomparable kind D-070 exists to stop.
///
/// **Thirty-two is what this binary runs; two hundred and fifty is what the claim was
/// measured at.** An earlier draft ran eight and said "every seed", and the review of
/// #107 found four seeds in two hundred and fifty where an install never landed. The
/// figure below is the gate's budget; the evidence behind the claim is the 250-seed
/// run recorded in D-083, re-taken after the fixes that review produced — green both
/// with this scenario's writer stopped and with it left running, which is what says a
/// product fix and not a scenario tweak is what moved it.
const SEEDS: u64 = 32;

/// Every one of [`SEEDS`] reaches the path and completes every install; and the claim
/// behind it was measured at 250 (D-083).
///
/// The first clause is the one that matters most: a scenario built to exercise a path
/// is worth nothing if a seed quietly fails to reach it, and `Report::reached` fails
/// the seed rather than letting the rest of the check pass against a silence.
#[test]
fn the_correct_node_installs_on_every_seed() {
    let results = sweep(SEEDS, |seed| correct(seed).check());
    verdict(&results).expect("the correct node passes every seed");
}

/// The pair (CLAUDE.md:52-67). Each mutation is run alone, on the seeds of this tier,
/// and each must be *seen to fail* the check the correct node passes.
///
/// Three of them need more than one range or more than one follower to be wrong about
/// at all, and are marked below. That is the owner's standing demand on this
/// stage: a check this wiring gives more than one range to be wrong about must show
/// the mutation a single-range world could not catch.
#[test]
fn every_wiring_variant_is_caught() {
    // The mutations this scenario's *situation* reaches, each run alone.
    //
    // They are run on three seeds, for the reason `SEEDS` gives: this is a directed
    // scenario whose situation every seed reaches, so the catch does not vary, and the
    // figure printed beside each says so. D-061 asks a tier of an assertion against a
    // *measured rate*, and there is no rate here — each of these is caught on every
    // seed it is run on.
    //
    // Four of the node's wiring variants are not in this list and are caught where
    // they are decided, deterministically and without a simulation:
    // `StepWhileInstalling`, `InstallKeepsTheOldCore` and `InstallHoldsEveryRange` in
    // `ananke_shard::node`, which asserts the node's round without a simulation, and
    // `SnapshotAckToEveryCore` (issue #103) and `InstallSweepsEveryStaging` in
    // `ananke_shard::install`.
    //
    // `VersionDirWithoutRange` and `SweepAcrossRanges` are not here either, and for a
    // different reason: their situation is two ranges taking at *one index*, and one
    // range's sweep running over another's versions, neither of which this scenario
    // produces — the four ranges' indices do not coincide and a sweep only ever runs
    // against its own range's record here. They keep D-075's deterministic checks,
    // which build those situations on purpose. Listing them here would have been a
    // sweep that passes because nothing was injected.
    //
    // `InstallWithoutRepair` is absent for the same reason once removed: the install
    // still *completes* under it — the switch is made, the event is traced — and what
    // it breaks is state machine safety after a crash, which this scenario has no
    // crash arm to reach. It keeps D-075's deterministic check, and the crash arm
    // aimed at the live install's switch belongs to the re-seed slice that owns
    // `Fault::CrashInstalling`.
    let seeds = 0..3u64;
    let caught = [
        // Needs more than one range: with one range on the node, the whole engine
        // directory *is* that range's key intervals and the two takes are the same
        // bytes.
        NodeVariant::TakeCheckpointsTheWholeNode,
        // Needs more than one follower behind the prefix: D-043's rule.
        NodeVariant::CapStreamsSent,
        // Wrong with one range too, and here because issue #96 is.
        NodeVariant::ChunksToTheInbox,
        // The two the reviewer of #107 wrote, which the event-counting check could not
        // see and the state read-back can. Both are wrong with one range too; they are
        // here because "an install completed" and "the right thing was installed" are
        // different claims and this binary is the only place in the tree that makes
        // either.
        NodeVariant::TakeSkipsTheUserKeys,
        NodeVariant::TakeStreamsTheLogToo,
    ];
    for variant in caught {
        let results: Vec<Result<(), String>> = seeds
            .clone()
            .map(|seed| with(seed, variant).check())
            .collect();
        let failed = results.iter().filter(|result| result.is_err()).count();
        let first = results
            .iter()
            .find_map(|result| result.as_ref().err().cloned());
        let total = results.len();
        assert!(
            failed > 0,
            "{variant} was not caught on any of {total} seeds: either the mutation is \
             not reached by this scenario or the check does not see it, and a sweep \
             that only passes may not be injecting the fault at all (CLAUDE.md)"
        );
        println!(
            "install: {variant} caught on {failed}/{total} seeds; first: {}",
            first.unwrap_or_default()
        );
    }
}
