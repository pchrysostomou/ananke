//! The directed re-seed shape: Stage B's exit criterion for Q15's path, (a) to (e) on
//! every seed (SHARD.md §12; §11, storage 8).
//!
//! The scenario is `ananke_sim::reseed`: three servers of four ranges each, one of them
//! refused by its store's lost mark at a restart, its cap on streams received set to
//! two so the four re-seeds share two slots, and a `reseed-crash` arm that crashes it
//! on the trace event of one replica's refused mark and restarts it.
//!
//! What each test here is for:
//!
//! - the correct system meets (a) to (e) on every seed, and the arm fires on every
//!   seed;
//! - each known-buggy variant of `NodeVariant::SHAPE` the shape catches is seen to fail
//!   the same check, at the tier its measured rate supports (D-061), with the rate
//!   printed. `RestartAppliesFromZero`'s pair is a unit test in `ananke_shard::round`,
//!   where the situation is one line of state rather than an interleaving to search
//!   for, and the entry says why.
//!
//! **A seed the correct system does not pass, named rather than hidden.** At a hundred
//! seeds, seed 56 fails (a): one of the four re-seeds never completes. Its stream is
//! restarted by the receiver for the whole run — 417 chunks sent, no install — because
//! the node's two assembly slots are held by assemblies whose senders stopped, and
//! nothing reclaims an admitted assembly whose sender is gone (D-075 says a *waiter*
//! is not held a slot; it says nothing about a holder that goes quiet). That is a gap
//! in the snapshot wiring, not in this shape, and this shape is what shows it: it is
//! reported with the entry and is the reason Stage B's re-seed criterion is not yet
//! met. The check fails loudly on that seed rather than passing quietly, which is what
//! a criterion is for.

use ananke_raft::core::Variants;
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::reseed::{self, Report};
use ananke_sim::{seeds, sweep, verdict};

/// The correct system.
fn correct(seed: u64) -> Report {
    reseed::run(seed, Variants::default(), NodeVariants::correct())
}

/// The correct system with one of the shape's variants on the node.
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
/// simulated seconds, a quarter of a million trace records and about 0.9 CPU seconds a
/// seed in release. At the nightly's ten thousand the correct system's sweep alone would
/// be some nine thousand CPU seconds — about seven times what a whole nightly shard
/// carries today (D-064) — for a criterion whose situation is reached by construction on
/// every seed rather than searched for among rare interleavings.
///
/// So the tier is capped here and the cap is **proposed, not taken**: the entry puts it
/// to the owner beside the measurement, as SHARD.md §12 excepts `sim/balance.rs` by name
/// and as D-083 recorded `sim/install.rs`'s tiers as owed. The gate's twenty and CI's
/// hundred run in full; the premerge and the nightly run two hundred.
const TIER_CAP: u64 = 200;

/// The seeds this binary's sweeps run: the tier's, capped.
fn shape_seeds() -> u64 {
    seeds().min(TIER_CAP)
}

/// The tier each catch is asserted from, set from the rate measured on this tree and
/// never before it (D-061, D-039). The measurements, at a hundred seeds in release, are
/// in the entry: `ReseedMarkNotSynced` 47 %, `RecordNeverQueued` 77 %, and
/// `DueOnlyWhenIdle` 4 %, which is under D-061's five and so is asserted from the
/// thousand-seed tier with a pinned seed below it.
const EVERY_TIER: u64 = 0;
const THOUSAND: u64 = 1000;

#[test]
fn the_reseed_shape_holds_on_every_seed() {
    let results = sweep(shape_seeds(), |seed| correct(seed).check());
    verdict(&results).expect("the correct system meets (a) to (e) on every seed");
}

#[test]
fn the_shape_reaches_its_situation_and_says_what_it_saw() {
    let report = correct(1);
    report.check().expect("seed 1 meets (a) to (e)");
    let read = report.read();
    println!(
        "reseed shape, seed 1: {} refusal, {} replicas refused, {} refused marks, \
         {} streams opened toward the node ({:?}), {} installs, {} replicas created by \
         their install, {} replicas answered after their mark, {} node starts after the \
         refusal, {} adoptions, {} trace records",
        read.refusals,
        read.replicas_refused.len(),
        read.marked.len(),
        read.streams_to_victim.values().sum::<usize>(),
        read.streams_to_victim,
        read.installs.len(),
        read.created_by_install.len(),
        read.answered_after_the_mark.len(),
        read.starts_after_refusal,
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
/// some seeds and not on others; where it does not, the replica opens fresh — no mark,
/// no quarantine, incarnation 1 — and (d) sees it restate as `Neither` where it must
/// restate as `Refused`. Measured at 47 % of a hundred seeds, so the catch is asserted
/// at every tier.
#[test]
fn a_refused_mark_written_unsynced_is_caught() {
    caught_at_its_rate_over(
        NodeVariant::ReseedMarkNotSynced,
        EVERY_TIER,
        high_rate_share(),
    );
}

/// A follower's compaction record queued nowhere.
///
/// The core that asked for one never hears back and never asks again, so it never
/// compacts and — once it takes office over a range it had been following — never takes
/// a snapshot, and the re-seed it owes that range's replica never streams. Measured at
/// 77 % of a hundred seeds.
#[test]
fn a_node_that_queues_no_compaction_record_is_caught() {
    caught_at_its_rate_over(
        NodeVariant::RecordNeverQueued,
        EVERY_TIER,
        high_rate_share(),
    );
}

/// The `snapshot` task's chunk deadlines served only when its queue is idle.
///
/// Four ranges keep it busy, so a stream whose chunk was lost is never resent and never
/// given up, and the leader waiting on it feeds that replica nothing for the rest of the
/// run. Measured at 4 % of a hundred seeds: under D-061's five, so the sweep asserts the
/// catch from the thousand-seed tier and seed 14 is pinned below it. Under [`TIER_CAP`]
/// the sweep never reaches that tier, so what asserts this catch today is the pin, and
/// the sweep's assertion comes with the owner's ruling on the cap. The rate is printed
/// at every tier either way.
#[test]
fn a_node_whose_busy_queue_starves_a_resend_is_caught() {
    caught_at_its_rate_over(NodeVariant::DueOnlyWhenIdle, THOUSAND, shape_seeds());
}

/// Seed 14, where `DueOnlyWhenIdle`'s own mechanism is caught, pinned so the gate's
/// twenty and CI's hundred assert it too (CLAUDE.md's pinned-seed rule, as D-056 pinned
/// `RefusalNotDurable`'s).
///
/// The mechanism, not a difference: on this seed one of the four ranges is never
/// installed at all, which is what a starved resend leaves behind — the leader holds
/// `installing` for a stream whose chunks stopped, and the replica is fed nothing.
#[test]
fn seed_14_pins_the_resend_a_busy_queue_starves() {
    let report = with(14, NodeVariant::DueOnlyWhenIdle);
    let why = report
        .check()
        .expect_err("DueOnlyWhenIdle strands a re-seed on seed 14");
    assert!(
        why.contains("(a)") && why.contains("never traced"),
        "seed 14 catches the starved resend as a range that was never installed: {why}"
    );
    // And the correct system passes the same seed, which is what makes the pair a pair.
    correct(14)
        .check()
        .expect("the correct system passes seed 14");
}

/// The seeds a variant caught at a high rate runs: a tenth of the tier and never fewer
/// than twenty, as `sim/tests/node.rs` and `sim/tests/engine.rs` run theirs (D-055,
/// D-061, D-082).
///
/// This shape is the most expensive thing in the tree per seed — four nodes, twenty
/// simulated seconds and a quarter of a million trace records — so a variant whose catch
/// is measured at 47 % and 77 % buys nothing from ten times the seeds. The rate the
/// assertion rests on is over the share, as D-061 requires. The 4 % variant is not on a
/// share: it needs the tier it asserts from.
fn high_rate_share() -> u64 {
    (shape_seeds() / 10).max(shape_seeds().min(20))
}

/// The applied index a live install's switch made durable told to neither the store nor
/// the `apply` task.
///
/// Caught on every seed and by construction: the shape installs four ranges into a
/// re-seeded node and then keeps writing to all four, so the first commit after each
/// install is an entry the task cannot place. The node stops, which `Report::check`
/// fails on before it looks at (a).
#[test]
fn a_node_that_never_learns_an_installs_applied_index_is_caught() {
    caught_at_its_rate_over(
        NodeVariant::InstallWatermarkNotTold,
        EVERY_TIER,
        high_rate_share(),
    );
}

/// Runs `variant` over `run` seeds, prints its measured catch rate, and asserts the
/// catch from `tier` (D-061: a catch seen on under 5 % of seeds is asserted from the
/// thousand-seed tier and never at the gate, where the draw alone would fail a tree with
/// nothing wrong).
fn caught_at_its_rate_over(variant: NodeVariant, tier: u64, run: u64) {
    let results = sweep(run, |seed| {
        with(seed, variant).check().err().map(|why| (seed, why))
    });
    let caught: Vec<(u64, String)> = results.into_iter().flatten().collect();
    #[allow(clippy::cast_precision_loss)]
    let rate = caught.len() as f64 * 100.0 / run as f64;
    println!(
        "{variant}: caught on {} of {run} seeds ({rate:.1} %), asserted from {tier} \
         seeds; first catch {:?}",
        caught.len(),
        caught
            .first()
            .map(|(seed, why)| (*seed, why.chars().take(140).collect::<String>()))
    );
    if run >= tier {
        assert!(
            !caught.is_empty(),
            "{variant} was caught on no seed of {run}"
        );
    }
}
