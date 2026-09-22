//! The directed re-seed shape: Stage B's exit criterion for Q15's path, (a) to (e) on
//! every seed (SHARD.md §12; §11, storage 8).
//!
//! The scenario is `ananke_sim::reseed`: three servers of four ranges each, one of them
//! refused by its store's lost mark at a restart, its cap on streams received set to two
//! so the four re-seeds share two slots, and a `reseed-crash` arm that crashes it on the
//! trace event of one replica's refused mark and restarts it.
//!
//! What each test here is for:
//!
//! - the correct system meets (a) to (e) on every seed, and the arm fires on every seed;
//! - each clause with a plant is seen to fail on it: `ServeBeforeRefusedMark` on (c)'s
//!   ordering, `ReseedMarkNotSynced` on (d), `RecordNeverQueued` on (a) and (b);
//! - (c)'s second half — a replica answering *after its own install* — is shown not to be
//!   a constant, which is what it was when it was keyed on the mark alone.

use ananke_raft::core::Variants;
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::reseed::{self, Report};
use ananke_sim::{seeds, sweep, verdict};

/// The correct system.
fn correct(seed: u64) -> Report {
    reseed::run(seed, Variants::default(), NodeVariants::correct())
}

/// The correct system with one variant on the node.
fn with(seed: u64, variant: NodeVariant) -> Report {
    reseed::run(
        seed,
        Variants::default(),
        NodeVariants::correct().with(variant),
    )
}

/// The most seeds this binary runs at any tier, until the owner rules on its tiers.
///
/// This shape is the most expensive scenario in the tree per seed: four nodes, twenty
/// simulated seconds, about 325 000 trace records and 0.47 CPU seconds a seed in release
/// (200 seeds: 87.73 user + 5.63 sys, peak RSS 1.03 GiB for the parallel sweep). At the
/// nightly's ten thousand the correct system's sweep alone would be some 4 700 CPU
/// seconds, about three and a half times what a whole nightly shard carries today
/// (D-064), and the memory is what actually constrains it.
///
/// So the tier is capped here and the cap is **proposed, not taken**: the entry puts it
/// to the owner beside the measurement, as SHARD.md §12 excepts `sim/balance.rs` by name
/// and as D-083 recorded `sim/install.rs`'s tiers as owed. The gate's twenty and CI's
/// hundred run in full; the premerge and the nightly run two hundred. Every test here
/// prints the tier it was asked for and the number it ran, so a capped run never reads
/// as a full one.
const TIER_CAP: u64 = 200;

/// The seeds this binary's sweeps run: the tier's, capped.
fn shape_seeds() -> u64 {
    seeds().min(TIER_CAP)
}

/// The seeds a variant caught at a high rate runs.
///
/// Under [`TIER_CAP`] this is **twenty at every tier**, and it is written as a constant
/// rather than as a tenth of the tier so that it says so: a tenth of a capped tier is
/// twenty from the gate to the nightly, and D-082's phrase "a tenth of the tier" would be
/// false here above two hundred seeds. Both variants run on it are caught at 48 % and
/// 77 %, at which twenty seeds miss with probability 4e-7 and 5e-13.
const HIGH_RATE_SHARE: u64 = 20;

/// Prints what tier was asked for and what actually ran, once per test.
fn tier(what: &str, run: u64) {
    println!(
        "reseed shape, {what}: ANANKE_SEEDS={} ran {run} seeds (TIER_CAP={TIER_CAP}, \
         PROPOSED — see D-081)",
        seeds()
    );
}

#[test]
fn the_reseed_shape_holds_on_every_seed() {
    let run = shape_seeds();
    tier("the correct system", run);
    let results = sweep(run, |seed| {
        let report = correct(seed);
        let held: usize = report.read().waited_for_a_slot.values().sum();
        (report.check(), held)
    });
    let verdicts: Vec<Result<(), String>> = results.iter().map(|(v, _)| v.clone()).collect();
    verdict(&verdicts).expect("the correct system meets (a) to (e) on every seed");

    // (b)'s cap clause, as a rate rather than as an every-seed property, printed at
    // every tier (§10). The cap holds a stream back on 199 seeds of 200 — median 9
    // chunks, quartiles 7 and 13 — and the seed it does not, 113, is a legitimate
    // interleaving in which each slot is free again before the next chunk arrives. That
    // is why it is not asserted per seed: the nightly failed on that seed when it was.
    let mut held: Vec<usize> = results.iter().map(|(_, h)| *h).collect();
    held.sort_unstable();
    let exercised = held.iter().filter(|h| **h > 0).count();
    #[allow(clippy::cast_precision_loss)]
    let rate = exercised as f64 * 100.0 / run as f64;
    println!(
        "reseed shape: the cap of {} held a stream back on {exercised} of {run} seeds          ({rate:.1} %); held-back chunks median {}, most {}",
        reseed::RECEIVE_CAP,
        held[held.len() / 2],
        held[held.len() - 1]
    );
    // The floor is measured, not chosen: 199 of 200 on this tree, and a floor of four
    // fifths leaves room for the draw at the gate's twenty while still failing a tree
    // where the cap stops biting at all — raise `RECEIVE_CAP` to the range count and
    // this is 0 %.
    assert!(
        rate >= CAP_RATE_FLOOR,
        "the cap held a stream back on only {rate:.1} % of {run} seeds, under the \
         measured floor of {CAP_RATE_FLOOR} %: four re-seeds are no longer sharing two \
         slots, and (b) is about nothing"
    );
}

/// The floor (b)'s cap rate is asserted against, measured on the correct system: 199 of
/// 200 seeds hold a stream back, and the floor sits well below that so that the draw at
/// the gate's twenty cannot fail a tree with nothing wrong, while a tree where the cap
/// stops holding anything back — a cap at or above the range count — fails it at once.
const CAP_RATE_FLOOR: f64 = 80.0;

/// Seed 1, where the cap's mechanism is pinned for every tier: four re-seeds against two
/// slots, with the node telling at least one of them to wait.
#[test]
fn seed_1_pins_the_cap_holding_a_re_seed_back() {
    let report = correct(1);
    report.check().expect("seed 1 meets (a) to (e)");
    let read = report.read();
    let held: usize = read.waited_for_a_slot.values().sum();
    assert!(
        held > 0,
        "seed 1 is pinned for the cap's mechanism: four re-seeds, two slots, and at \
         least one chunk answered `StartOver {{ reason: Cap }}` ({:?})",
        read.waited_for_a_slot
    );
    println!(
        "reseed shape, seed 1: {held} chunks held back by the cap of {}, across ranges          {:?}",
        reseed::RECEIVE_CAP,
        read.waited_for_a_slot.keys().collect::<Vec<_>>()
    );
}

#[test]
fn the_shape_reaches_its_situation_and_says_what_it_saw() {
    let report = correct(1);
    report.check().expect("seed 1 meets (a) to (e)");
    let read = report.read();
    println!(
        "reseed shape, seed 1: {} refusal, {} replicas refused, {} refused marks, \
         {} streams opened toward the node ({:?}), {} chunks held back by the cap ({:?}), \
         {} re-seeds in flight at once against a cap of {}, {} installs, {} replicas \
         created by their install, {} replicas answered after their own install, \
         {} node starts between the refusal and the last install, {} adoptions, \
         {} trace records",
        read.refusals,
        read.replicas_refused.len(),
        read.marked.len(),
        read.streams_to_victim.values().sum::<usize>(),
        read.streams_to_victim,
        read.waited_for_a_slot.values().sum::<usize>(),
        read.waited_for_a_slot,
        read.in_flight_at_once(),
        reseed::RECEIVE_CAP,
        read.installs.len(),
        read.created_by_install.len(),
        read.answered_after_the_install.len(),
        read.starts_after_refusal.len(),
        read.adoptions,
        report.records.len(),
    );
    println!(
        "reseed shape, seed 1: the arm crashed the node on range {:?}'s refused mark at \
         {:?} and restarted it at {:?}",
        report.arm.marked, report.arm.at, report.arm.restarted
    );
}

/// D-067's variant: the refused mark written in a batch that is not synced.
///
/// What a crash keeps of an unsynced write is the disk's draw, so the mark survives on
/// some seeds and not on others; where it does not, the replica opens fresh — no mark, no
/// quarantine, incarnation 1 — and (d) sees it restate as `Neither` where it must restate
/// as `Refused`. Measured at 48 % of a hundred seeds, so the catch is asserted at every
/// tier. The rate depends on the arm landing on the mark's own trace event, before
/// anything else syncs the new engine's log (D-067): with an audit of the refused
/// directory in front of the crash it falls to 1 %, which is why that audit is taken
/// after the crash and the entry says so.
#[test]
fn a_refused_mark_written_unsynced_is_caught() {
    caught(NodeVariant::ReseedMarkNotSynced, HIGH_RATE_SHARE);
}

/// A follower's compaction record queued nowhere.
///
/// The core that asked for one never hears back and never asks again, so it never
/// compacts and — once it takes office over a range it had been following — never takes a
/// snapshot, and the re-seed it owes that range's replica never streams. 77 % of a
/// hundred seeds.
#[test]
fn a_node_that_queues_no_compaction_record_is_caught() {
    caught(NodeVariant::RecordNeverQueued, HIGH_RATE_SHARE);
}

/// (c)'s plant: a node whose replicas answer with no refused mark written at all.
///
/// Caught on 100 of 100 seeds, **by (c)**, which is the point — while (a) was asked
/// first, this variant was caught by (a) instead, and a clause with no plant of its own
/// is a clause nothing has shown can fail.
#[test]
fn a_node_that_serves_before_its_refused_mark_is_caught_by_the_order() {
    let run = HIGH_RATE_SHARE;
    tier("ServeBeforeRefusedMark", run);
    let whys: Vec<String> = sweep(run, |seed| {
        with(seed, NodeVariant::ServeBeforeRefusedMark)
            .check()
            .err()
    })
    .into_iter()
    .flatten()
    .collect();
    assert_eq!(
        whys.len() as u64,
        run,
        "ServeBeforeRefusedMark is caught on every seed"
    );
    for why in &whys {
        assert!(
            why.contains("(c)") && why.contains("answered before their refused mark"),
            "and it is (c) that catches it, not another clause: {why}"
        );
    }
    println!(
        "ServeBeforeRefusedMark: caught by (c) on {}/{run} seeds",
        whys.len()
    );
}

/// (c)'s second half is not a constant.
///
/// Keyed on the mark alone it was: an empty replica answers AppendEntries with rejections
/// whether or not its re-seed ever completed, so every run had all four ranges in it —
/// including runs where a range was never installed at all. Keyed on each range's *own*
/// install it tracks the installs, which is what it is evidence of.
#[test]
fn an_answer_that_is_evidence_of_the_install_is_not_the_same_as_any_answer() {
    // A run where one or more ranges are never re-seeded at all.
    let report = with(0, NodeVariant::RecordNeverQueued);
    let read = report.read();
    let every = Report::every_range();
    assert!(
        read.installs.len() < every.len(),
        "the plant leaves at least one range un-installed: {:?}",
        read.installs
    );
    assert_eq!(
        read.answered_after_the_mark, every,
        "every range answers after its mark even where nothing was installed, which is \
         why the mark is the wrong key: {read:?}"
    );
    assert!(
        read.answered_after_the_install.len() < every.len(),
        "and keyed on its own install the set follows the installs: {:?} against {:?}",
        read.answered_after_the_install,
        read.installs
    );
    println!(
        "reseed shape: on a run with {} of {} ranges installed, answers after the mark \
         {:?}, answers after the install {:?}",
        read.installs.len(),
        every.len(),
        read.answered_after_the_mark,
        read.answered_after_the_install
    );
}

/// Runs `variant` over `run` seeds, prints its measured catch rate, and asserts the catch.
///
/// Both variants here are caught on about half the seeds or more, so the catch is
/// asserted at every tier (D-061 gates only what is caught on under 5 %).
fn caught(variant: NodeVariant, run: u64) {
    tier(&format!("{variant}"), run);
    let results = sweep(run, |seed| {
        with(seed, variant).check().err().map(|why| (seed, why))
    });
    let caught: Vec<(u64, String)> = results.into_iter().flatten().collect();
    #[allow(clippy::cast_precision_loss)]
    let rate = caught.len() as f64 * 100.0 / run as f64;
    println!(
        "{variant}: caught on {} of {run} seeds ({rate:.1} %); first catch {:?}",
        caught.len(),
        caught
            .first()
            .map(|(seed, why)| (*seed, why.chars().take(140).collect::<String>()))
    );
    assert!(
        !caught.is_empty(),
        "{variant} was caught on no seed of {run}"
    );
}
