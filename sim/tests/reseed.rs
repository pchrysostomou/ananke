//! The directed re-seed shape: Stage B's exit criterion for Q15's path, (a) to (e) on
//! every seed (SHARD.md §12; §11, storage 8).
//!
//! The scenario is `ananke_sim::reseed`: three servers of six replicas each — four user
//! ranges and, since PROPOSED D-096, the root and the meta range — one of them refused by
//! its store's lost mark at a restart, its cap on streams received set to two so the four
//! user ranges' re-seeds share two slots, and a `reseed-crash` arm that crashes it on the
//! trace event of one of those replicas' refused mark and restarts it. The two system
//! ranges are re-seeded by the log from its first entry, and (a) asserts that of them.
//!
//! What each test here is for:
//!
//! - the correct system meets (a) to (e) on every seed, and the arm fires on every seed;
//! - each clause with a plant is seen to fail on it: `ServeBeforeRefusedMark` on (c)'s
//!   ordering, `ReseedMarkNotSynced` on (d), `RecordNeverQueued` on (a) and (b);
//! - (c)'s second half — a replica answering *after its own install* — is shown not to be
//!   a constant, which is what it was when it was keyed on the mark alone;
//! - a re-seeded node's right half, which configuration never names, comes back through
//!   its placeholder quarantined (`reseed::run_after_split`, PROPOSED D-109 §2), and the
//!   node, re-seeded and not restarted, writes nothing into the directory it refused
//!   (D-041, PROPOSED D-117);
//! - `TasksOnTheRefusedDir`, the start that re-seeded handing its tasks the refused
//!   directory, is caught there on every seed (PROPOSED D-117);
//! - a right half made after a re-seed takes the parent's quarantine, so its node neither
//!   leads it nor takes a checkpoint of it (`reseed::run_split_after_reseed`, D-130), and a
//!   node that does lead one, under `SplitLeavesQuarantineBehind`, takes and sweeps in the
//!   directory it runs on, and the variant is caught there too (PROPOSED D-117);
//! - the same run's install of the parent is created at the split's generation and agrees
//!   with check 7's map at its floor (issue #172; PROPOSED D-107 §2, D-115);
//! - the same run's right half keeps its keys, whichever of the node's two installs lands
//!   first, off the node's disk, and `InstallReplacesHostedSpan` loses them exactly where
//!   the right half's lands first (PROPOSED D-128);
//! - a leader with both followers re-seeded refuses the reads it can never confirm and goes
//!   on committing, and the leader as it was is caught by the read bound
//!   (`reseed::run_unconfirmable_reads`, PROPOSED D-129).

use ananke_env::{RecoveredAs, TraceEvent};
use ananke_raft::core::{Variant, Variants};
use ananke_shard::invariants::Fold;
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
///
/// The binary's other shape, the re-seed after a split (`reseed::run_after_split`), runs
/// under this cap on its own figures, not this shape's: about 98 000 trace records and
/// 0.17 to 0.21 CPU seconds a seed in release, measured at load averages from 86 to 541.
/// Uncapped, the premerge's thousand would be some 170 to 210 CPU seconds and the
/// nightly's ten thousand some 1 700 to 2 100, a whole nightly shard (D-064), for clauses
/// that hold or fail on every seed at twenty. Like this shape's, that cap is the owner's
/// to rule on (PROPOSED D-115).
// PROPOSED(D-115): the second shape's cap, on its own figures.
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
/// false here above two hundred seeds. Four variants run on it. `ReseedMarkNotSynced` and
/// `RecordNeverQueued` are caught at 48 % and 77 %, at which twenty seeds miss with
/// probability 4e-7 and 5e-13. `ServeBeforeRefusedMark` and `TasksOnTheRefusedDir` are caught
/// on every seed measured, 100 of 100 and 200 of 200.
const HIGH_RATE_SHARE: u64 = 20;

/// Prints what tier was asked for and what actually ran, once per test.
fn tier(what: &str, run: u64) {
    tier_under(what, run, "D-081");
}

/// [`tier`], naming the entry whose figures the cap stands on for this run.
// PROPOSED(D-115)
fn tier_under(what: &str, run: u64, entry: &str) {
    println!(
        "reseed shape, {what}: ANANKE_SEEDS={} ran {run} seeds (TIER_CAP={TIER_CAP}, \
         PROPOSED — see {entry})",
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
    let streamed = Report::streamed_ranges();
    assert!(
        read.installs.len() < streamed.len(),
        "the plant leaves at least one range un-installed: {:?}",
        read.installs
    );
    assert_eq!(
        read.answered_after_the_mark, every,
        "every range answers after its mark even where nothing was installed, which is \
         why the mark is the wrong key: {read:?}"
    );
    assert!(
        read.answered_after_the_install.len() < streamed.len(),
        "and keyed on its own install the set follows the installs: {:?} against {:?}",
        read.answered_after_the_install,
        read.installs
    );
    println!(
        "reseed shape: on a run with {} of {} streamed ranges installed, answers after \
         the mark {:?}, answers after the install {:?}",
        read.installs.len(),
        streamed.len(),
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

/// A re-seeded node's right half comes back through its placeholder, quarantined
/// (PROPOSED D-109 §2), the node writes nothing into the directory it refused (PROPOSED
/// D-117), and P's install is created at the split's generation (PROPOSED D-115):
/// `reseed::run_after_split`, on every seed of the tier.
///
/// What it holds, per seed, on the correct node:
///
/// - **the quarantine** (PROPOSED D-109 §2). The node held the right half before its
///   loss: its own apply of the split created it, and it may have voted in it. The
///   re-seed rebuilds configuration's replicas, and configuration never names a right
///   half, so the right half is not among them; the fresh engine holds no key of it. So
///   it comes back only by its placeholder's install, and the replica that install
///   creates is quarantined — stated so, `RaftReseeded` and `RaftRecovered { state:
///   quarantined }` — and grants the right half nothing. Before that fix it was
///   installed unquarantined, and this failed on every seed;
/// - **the refused directory untouched** (D-041, PROPOSED D-117). The node is not
///   restarted after its re-seed, so the start that re-seeded is the one that receives
///   both streams, its re-seed's of P and the right half's to its placeholder. Not one
///   entry of the refused directory, at any depth, changes from the audit before the
///   restart to the one at the end, other than the refusal's own marker, and both
///   streams staged in the directory the re-seed built and were cleared there, so no file
///   is left in either staging directory. Before that fix the start's tasks were handed the
///   refused directory and both streams staged, synced and were cleared there: this failed
///   on 200 of 200 seeds (#176);
/// - **an install creation's generation** (issue #172; PROPOSED D-107 §2, D-115). `x`
///   applied the split before its loss, and P's leader compacted past it while `x` was
///   down, so the install that creates P on `x`'s fresh engine puts P's left half in place,
///   `[k0, k1)` at generation 2. Its `RangeCreated { cause: snapshot }` must name that
///   descriptor (SHARD.md §8) and equal check 7's value for P at its floor
///   (`AfterSplit::parent_created_at_the_splits_generation`). Reverting the creation's
///   reading to the first generation, whatever the snapshot held, fails this on every seed
///   by check 7. The revert gives the call site of `created_from` in `ananke-shard`'s
///   `server.rs` no descriptor. Seed 0 then fails with "descriptor agreement: node
///   Some(3)'s replica of range 2 was installed at index 128 with Held { …, generation: 1,
///   … }, where the range's descriptor there is Held { …, generation: 2, … }". The unit
///   test of `created_from` passes that revert, so this run is what holds the call site.
// PROPOSED(D-115): the parent's install creation, asked on the same run.
#[test]
fn a_reseeded_nodes_right_half_comes_back_through_its_placeholder_quarantined() {
    let run = shape_seeds();
    tier_under("the re-seed after a split", run, "D-115");
    let verdicts = sweep(run, |seed| {
        let report = reseed::run_after_split(seed);
        report.check()?;
        report.reached()?;
        // PROPOSED(D-117)
        report.refused_untouched()?;
        report.staged_where_it_runs()?;
        report.parent_created_at_the_splits_generation()?;
        let (_, right) = report.split.expect("`reached` names the split");
        let (at, ..) = report
            .installed_after_refusal(right)
            .expect("`reached` names the install");
        if !report.quarantined_after(right, at) {
            return Err(format!(
                "seed {seed}: node {} was created the right half {right} by its \
                 placeholder's install after its re-seed and did not state it \
                 quarantined: the replica may be one whose votes were in the engine the \
                 node lost (D-035)",
                report.x
            ));
        }
        match report.granted_after(right, at) {
            0 => Ok(()),
            granted => Err(format!(
                "seed {seed}: node {}'s quarantined right half {right} granted {granted} \
                 votes or pre-votes",
                report.x
            )),
        }
    });
    verdict(&verdicts).expect(
        "a re-seed after a split creates P at the split's generation, brings the right half \
         back quarantined and leaves the directory it refused untouched, on every seed",
    );
}

/// D-117's variant: the start that re-seeded hands its tasks the refused directory, the
/// node as it stood before #176. `reseed::run_after_split` on the variant, on
/// [`HIGH_RATE_SHARE`] seeds.
///
/// Caught on every seed, and by the clauses it breaks: the refused directory changed, what
/// changed is staging and nothing else, and it includes the two streams this start received,
/// the re-seed's own of P and the right half's to its placeholder; and the directory the
/// node runs on holds no staging of either. Asking for a stream of each rather than for
/// exactly two names leaves room for a leader's change during a stream, which would stage
/// under a second sender. Measured on 200 of 200 seeds, so the catch is asserted at every
/// tier (D-061 gates only what is caught on under 5 %).
///
/// The run is otherwise the correct one, and the test asks that too: every check of the
/// trace passes the variant, so the audit is what catches it.
// PROPOSED(D-117): the start's tasks run on the directory the re-seed built.
#[test]
fn a_node_whose_tasks_stay_on_the_refused_directory_is_caught() {
    let run = HIGH_RATE_SHARE;
    let variant = NodeVariant::TasksOnTheRefusedDir;
    tier(&format!("{variant}"), run);
    let verdicts = sweep(run, |seed| {
        let report = reseed::run_after_split_under(seed, NodeVariants::correct().with(variant));
        report.check()?;
        report.reached()?;
        let (_, right) = report.split.expect("`reached` names the split");
        if report.refused_untouched().is_ok() {
            return Err(format!(
                "seed {seed}: {variant} left the refused directory untouched"
            ));
        }
        let changed = report.refused_changed().ok_or_else(|| {
            format!("seed {seed}: {variant}: an audit of the refused directory did not finish")
        })?;
        let staged = |range: u64| {
            let prefix = format!("staging-r{range}-s");
            changed.iter().any(|name| name.starts_with(&prefix))
        };
        if !staged(reseed::SPLIT_PARENT)
            || !staged(right)
            || !changed.iter().all(|name| name.starts_with("staging-"))
        {
            return Err(format!(
                "seed {seed}: {variant} should change the staging of ranges {} and {right} in \
                 the refused directory, and nothing else: {changed:?}",
                reseed::SPLIT_PARENT
            ));
        }
        if report.staged_where_it_runs().is_ok() {
            return Err(format!(
                "seed {seed}: {variant}'s streams staged in the directory the node runs on"
            ));
        }
        Ok(())
    });
    verdict(&verdicts).expect("the variant is caught on every seed, by the staging it moved");
    println!("{variant}: caught on {run} of {run} seeds, by its staging in the refused directory");
}

/// The figures the two halves of D-129's pair print.
fn unconfirmable_figures(what: &str, run: u64, reports: &[(u64, u64, u64, usize)]) {
    let most = reports.iter().map(|(_, o, _, _)| *o).max().unwrap_or(0);
    let refused: usize = reports.iter().map(|(_, _, _, r)| *r).sum();
    let dropped: u64 = reports.iter().map(|(_, _, d, _)| *d).sum();
    let least_refused = reports.iter().map(|(_, _, _, r)| *r).min().unwrap_or(0);
    println!(
        "{what}: over {run} seeds a replica held at most {most} registered reads; the leader \
         refused {refused} reads (at least {least_refused} on a seed) and dropped {dropped}"
    );
}

/// A leader that can confirm no read refuses it, and goes on committing (the owner's ruling
/// in D-129, amending D-035's consequences): `reseed::run_unconfirmable_reads`, on every
/// seed of the tier.
///
/// The run holds a leader with both followers re-seeded and quarantined and two clients
/// reading. The state is asserted reached and then each thing the ruling asks of it: reads
/// refused, the reads waiting on the second follower dropped at its first answer, what the
/// replica holds bounded, none confirmed there, writes committing the whole time. The state
/// is built on every seed and each clause holds there by construction, so it is asserted at
/// every tier (D-061), and the run is capped as this binary's are (`TIER_CAP`).
// PROPOSED(D-129): a leader refuses a read it can never confirm.
#[test]
fn a_leader_that_can_confirm_no_read_refuses_it_and_goes_on_committing() {
    let run = shape_seeds();
    tier("a leader that can confirm no read", run);
    let results = sweep(run, |seed| {
        let report = reseed::run_unconfirmable_reads(seed, Variants::default());
        let verdict = report
            .check()
            .and_then(|()| report.reached())
            .and_then(|()| report.refused_and_committing());
        let figures = (
            seed,
            report.outstanding(),
            report.dropped(),
            report.refused_from(0),
        );
        (verdict, figures)
    });
    let figures: Vec<(u64, u64, u64, usize)> = results.iter().map(|(_, f)| *f).collect();
    unconfirmable_figures("the correct leader", run, &figures);
    let verdicts: Vec<Result<(), String>> = results.into_iter().map(|(v, _)| v).collect();
    verdict(&verdicts).expect("the correct leader refuses what it can never confirm");
}

/// The buggy half: the leader as it was, on the correct half's first seeds. It reaches the
/// same state, keeps every read it can never confirm, and passes `READS_OUTSTANDING`, the
/// bound that stays at 38 and is what the sweep tripped on (S10's seeds 128 and 643). Caught
/// on every seed by that bound and nothing else, so asserted at every tier (D-061), on
/// [`HIGH_RATE_SHARE`] seeds as this binary's other high-rate variants are.
// PROPOSED(D-129): a leader refuses a read it can never confirm.
#[test]
fn a_leader_that_keeps_the_reads_it_can_never_confirm_is_caught_by_the_bound() {
    let run = HIGH_RATE_SHARE;
    tier("a leader that keeps the reads it can never confirm", run);
    let results = sweep(run, |seed| {
        let report = reseed::run_unconfirmable_reads(seed, Variant::UnconfirmableReadKept);
        let verdict =
            report
                .reached()
                .and_then(|()| match (report.over_the_bound(), report.check()) {
                    (Some(_), Err(violation)) if violation.contains("registered reads") => Ok(()),
                    (bound, check) => Err(format!(
                        "seed {seed}: the run reached the state, and the bound did not catch the \
                     read it kept: bound {bound:?}, check {check:?}"
                    )),
                });
        let verdict = verdict.and_then(|()| {
            if report.refused_from(0) == 0 && report.dropped() == 0 {
                Ok(())
            } else {
                Err(format!(
                    "seed {seed}: the variant refused {} reads and dropped {}: it is not the \
                     leader as it was",
                    report.refused_from(0),
                    report.dropped()
                ))
            }
        });
        (
            verdict,
            (
                seed,
                report.outstanding(),
                report.dropped(),
                report.refused_from(0),
            ),
        )
    });
    let figures: Vec<(u64, u64, u64, usize)> = results.iter().map(|(_, f)| *f).collect();
    unconfirmable_figures("UnconfirmableReadKept", run, &figures);
    let caught = results.iter().filter(|(v, _)| v.is_ok()).count();
    println!("UnconfirmableReadKept: caught by the bound on {caught} of {run} seeds");
    let verdicts: Vec<Result<(), String>> = results.into_iter().map(|(v, _)| v).collect();
    verdict(&verdicts).expect("the leader as it was is caught by the bound on every seed");
}

/// How a refused follower counts, the one thing the verifiers read differently (PROPOSED
/// D-129 §2): the same as a quarantined one, since the mark is no promise of a later
/// confirmation and a stream can stall for as long as the leader's other follower keeps it
/// in office. So the second follower's stream is lost for good here, it stays refused, and
/// the correct leader refuses reads and commits on exactly as in the state's other shape.
/// On [`HIGH_RATE_SHARE`] seeds, since the state is built on each.
///
/// No variant plants the other reading, because a core variant for a reading the owner did
/// not take would widen this slice (#198's review, N2). The counterfactual is a one-line
/// revert instead, and this is it: in `ananke-raft`'s `core.rs`, in
/// `on_append_entries_response`, `progress.echoed_zero = echo == 0` becomes
/// `progress.echoed_zero = echo == 0 && !refused`. That is "echo 0 and no refused mark" —
/// a refused follower then counts as able to confirm for the whole of its stalled
/// re-seed, the reads wait on it, and the registrations only rise. Measured for D-129 at
/// twenty seeds in release: the revert fails **20 of 20** on `READS_OUTSTANDING`, "range 2
/// holds 39 registered reads"; the rule taken passes 20 of 20, holding at most 9. The
/// figures in the entry's table were measured the same way. The revert's own unit case is
/// `paper.rs`'s `a_refused_followers_echo_of_zero_counts_as_a_quarantined_ones`, which it
/// also fails; this run is what holds the reading under the fault model.
// PROPOSED(D-129): a refused follower's echo of 0 counts as a quarantined one's.
#[test]
fn a_refused_follower_whose_stream_stalls_counts_as_unable_to_confirm() {
    let run = HIGH_RATE_SHARE;
    tier("a refused follower whose stream stalls", run);
    let results = sweep(run, |seed| {
        let report = reseed::run_unconfirmable_reads_stalled(seed);
        let verdict = report
            .check()
            .and_then(|()| report.reached())
            .and_then(|()| report.refused_and_committing());
        let figures = (
            seed,
            report.outstanding(),
            report.dropped(),
            report.refused_from(0),
        );
        (verdict, figures)
    });
    let figures: Vec<(u64, u64, u64, usize)> = results.iter().map(|(_, f)| *f).collect();
    unconfirmable_figures("a stalled refused follower", run, &figures);
    let verdicts: Vec<Result<(), String>> = results.into_iter().map(|(v, _)| v).collect();
    verdict(&verdicts).expect("a refused follower that stays refused counts as unable");
}

/// A re-seeded node's right half keeps its keys, whichever of its two installs lands first
/// (SHARD.md §5, §11 storage 5; PROPOSED D-128): `reseed::run_after_split`, on every seed
/// of the tier.
///
/// The re-seed hosts P, which has no descriptor in the fresh engine, at configuration's
/// span, and the right half comes back through its placeholder at its own. Which stream
/// completes first is the schedule's draw. Where the right half's does, P's install finds
/// a replica holding keys of the span configuration names for P: it may replace the
/// snapshot's span alone, and the right half's keys must still be in the node's engine
/// when the run is over, read off its disk. The run writes each key once, so nothing
/// puts one back. Every traced descriptor is right whichever span the switch replaced,
/// so only the keys, and check 8's install clause, can say.
// PROPOSED(D-128)
#[test]
fn a_reseeded_nodes_right_half_keeps_its_keys_whichever_install_lands_first() {
    let run = shape_seeds();
    tier("the re-seed after a split, the right half's keys", run);
    let outcomes = sweep(run, |seed| {
        let report = reseed::run_after_split(seed);
        report.check()?;
        report.reached()?;
        report.right_keys_survive()?;
        report.state_read_back_is_the_takes(reseed::SPLIT_PARENT)?;
        let (_, right) = report.split.expect("`reached` names the split");
        report.state_read_back_is_the_takes(right)?;
        Ok::<bool, String>(report.right_installed_first())
    });
    let first: Vec<u64> = outcomes
        .iter()
        .enumerate()
        .filter(|(_, outcome)| matches!(outcome, Ok(true)))
        .map(|(seed, _)| seed as u64)
        .collect();
    println!(
        "reseed: the right half's install landed before P's on {} of {run} seeds: {first:?}",
        first.len()
    );
    let verdicts: Vec<Result<(), String>> = outcomes
        .into_iter()
        .map(|outcome| outcome.map(|_| ()))
        .collect();
    verdict(&verdicts).expect("a re-seeded node's right half keeps its keys on every seed");
    assert!(
        !first.is_empty(),
        "the right half's install landed before P's on no seed of {run}: this test saw only \
         the order that loses nothing"
    );
}

/// The pair's buggy half (CLAUDE.md; PROPOSED D-128): `InstallReplacesHostedSpan` on
/// `reseed::run_after_split`, the same seeds as the correct node's test above.
///
/// On every seed where the right half's install lands before P's the switch of P replaces
/// configuration's span. The right half's keys go from `x`'s engine, every descriptor
/// traced is as it was, and check 8's install clause is the fold that sees it; the overlap
/// clause and check 7 stay green. On a seed where P's lands first the variant is the
/// correct node, and all of them hold. Measured on the tier's seeds, about a third, so
/// asserted at every tier (D-061).
// PROPOSED(D-128)
#[test]
fn a_span_less_install_that_replaces_its_hosted_span_loses_the_right_half_and_check_8_says_so() {
    let run = shape_seeds();
    tier("the re-seed after a split, InstallReplacesHostedSpan", run);
    let variant = NodeVariants::of(&[NodeVariant::InstallReplacesHostedSpan]);
    let outcomes = sweep(run, |seed| {
        let report = reseed::run_after_split_under(seed, variant);
        report.reached()?;
        let checker = ananke_shard::invariants::folded(&report.records);
        let clause = checker.verdict_of(Fold::InstallSpan);
        let keys = report.right_keys_survive();
        if let Err(words) = checker.verdict_of(Fold::Overlap) {
            return Err(format!(
                "seed {seed}: the overlap clause tripped, on a variant no descriptor shows: \
                 {words}"
            ));
        }
        if let Err(words) = checker.verdict_of(Fold::DescriptorAgreement) {
            return Err(format!("seed {seed}: check 7 tripped: {words}"));
        }
        let first = report.right_installed_first();
        match (first, clause, keys) {
            (true, Err(words), Err(lost)) => {
                if words.starts_with(Fold::InstallSpan.words())
                    && lost.contains("before P")
                    && lost.contains("lost")
                {
                    Ok(true)
                } else {
                    Err(format!(
                        "seed {seed}: caught in other words: {words}; {lost}"
                    ))
                }
            }
            (false, Ok(()), Ok(())) => Ok(false),
            (first, clause, keys) => Err(format!(
                "seed {seed}: the right half installed first: {first}, and check 8's install \
                 clause said {clause:?} and its keys {keys:?}"
            )),
        }
    });
    let caught = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, Ok(true)))
        .count();
    println!(
        "reseed: InstallReplacesHostedSpan lost the right half's keys and tripped check 8's \
         install clause on {caught} of {run} seeds, and on the others neither"
    );
    let verdicts: Vec<Result<(), String>> = outcomes
        .into_iter()
        .map(|outcome| outcome.map(|_| ()))
        .collect();
    verdict(&verdicts).expect("the variant is caught exactly where the order reaches it");
    assert!(
        caught > 0,
        "InstallReplacesHostedSpan was caught on no seed of {run}"
    );
}

/// A node that leads a right half made after its re-seed takes its checkpoints and sweeps
/// them in the directory it runs on (D-041, PROPOSED D-117): `reseed::run_split_after_reseed`,
/// on [`HIGH_RATE_SHARE`] seeds, under `SplitLeavesQuarantineBehind`, alone and with
/// `TasksOnTheRefusedDir`.
///
/// `run_after_split` reaches no take and no sweep: every replica of its re-seeded node is
/// quarantined and never campaigns, so its `apply` task writes nothing and its `snapshot`
/// task sweeps nothing. Since D-130 that is every replica a re-seeded node holds, a right half
/// made after the re-seed too, and the correct node reaches no take here either
/// (`a_right_half_made_after_a_reseed_is_quarantined_and_its_node_neither_leads_nor_takes`).
/// Where the tasks' directories are read at all is on a node that may lead one, so this test
/// asks what it asked with `SplitLeavesQuarantineBehind` standing in for that node: the split
/// writes the right half no quarantine, as it did when this test was written. Here P's left
/// half is split, whichever other node leads the new right half is crashed until `x` does,
/// and `x` is written to past the threshold several times over.
///
/// On such a node, on every seed: every check of the trace passes, `x` leads and takes
/// at least two checkpoints, each take's version directory is in the directory the re-seed
/// built and none is in the refused one, a sweep has emptied an earlier one there, and the
/// refused directory is untouched. Measured on 200 seeds: the split applied and every clause
/// held on all 200, with 8 to 10 takes and 1 or 2 versions still holding files at the end.
///
/// Under the variant, on every seed: the same trace checks pass, the same takes are traced,
/// and every version directory is in the refused directory and none in the fresh one.
/// Measured on 200 of 200. It catches the two ways to leave a task behind that
/// `run_after_split` cannot: the `apply` task's `engine_dir`, by where the versions are, and
/// the `snapshot` task's, by the sweep that empties nothing (PROPOSED D-117 gives the table).
///
/// It splits P's left half and not a range the log rebuilt, whose split fails state machine
/// safety on the re-seeded node, which is #199's.
// PROPOSED(D-117): the start's tasks run on the directory the re-seed built.
// PROPOSED(D-130): on a node that may lead a right half made after its re-seed, the variant's.
#[test]
fn a_node_leading_a_right_half_after_its_reseed_takes_and_sweeps_where_it_runs() {
    let run = HIGH_RATE_SHARE;
    let may_lead = NodeVariants::of(&[NodeVariant::SplitLeavesQuarantineBehind]);
    tier(&format!("the split after a re-seed, {may_lead}"), run);
    let verdicts = sweep(run, |seed| {
        let report = reseed::run_split_after_reseed(seed, may_lead);
        report.check()?;
        report.reached()?;
        report.refused_untouched()?;
        report.took_where_it_runs()
    });
    verdict(&verdicts).expect("a node that leads it takes and sweeps where it runs on every seed");

    let variant = NodeVariant::TasksOnTheRefusedDir;
    tier(&format!("the split after a re-seed, {variant}"), run);
    let verdicts = sweep(run, |seed| {
        let report = reseed::run_split_after_reseed(seed, may_lead.with(variant));
        report.check()?;
        report.reached()?;
        let right = report
            .second_right
            .ok_or_else(|| format!("seed {seed}: {variant}: no split after the re-seed"))?;
        let takes = report.takes_of(right);
        let audit = |dir: &Option<reseed::DirAudit>| {
            dir.as_ref()
                .map(|audit| reseed::versions_of(audit, right).len())
                .ok_or_else(|| format!("seed {seed}: {variant}: an audit did not finish"))
        };
        let (refused, fresh) = (audit(&report.refused_at_end)?, audit(&report.fresh_at_end)?);
        if takes < 2 || refused < takes || fresh != 0 || report.took_where_it_runs().is_ok() {
            return Err(format!(
                "seed {seed}: {variant} should put every take of range {right} in the refused \
                 directory: {takes} takes, {refused} versions there and {fresh} where it runs"
            ));
        }
        Ok(())
    });
    verdict(&verdicts).expect("the variant's takes are all in the refused directory on every seed");
}

/// What the run above can no longer reach on the correct node: a right half made after a
/// re-seed takes the parent's quarantine (D-130), so the node that made it does not lead it
/// and takes no checkpoint of it, and nothing on a re-seeded node does.
/// `reseed::run_split_after_reseed` on [`HIGH_RATE_SHARE`] seeds: on every one the run passes
/// every check of the trace, reaches its re-seed and leaves the refused directory untouched,
/// `x` applies the split, states the right half quarantined, grants nothing in it, and is
/// never its leader, for all the leaders the run crashes to make it one. The day a re-seeded
/// node can lead again this fails, and the test above can ask its question of the correct
/// node once more.
// PROPOSED(D-130): the situation D-117's run reached is absent on the correct node.
#[test]
fn a_right_half_made_after_a_reseed_is_quarantined_and_its_node_neither_leads_nor_takes() {
    let run = HIGH_RATE_SHARE;
    tier("the split after a re-seed, correct", run);
    let verdicts = sweep(run, |seed| {
        let report = reseed::run_split_after_reseed(seed, NodeVariants::correct());
        report.check()?;
        report.reached()?;
        report.refused_untouched()?;
        let x = report.x;
        let right = report
            .second_right
            .ok_or_else(|| format!("seed {seed}: node {x} applied no split after its re-seed"))?;
        let stated = report.records.iter().any(|record| {
            matches!(record.event, TraceEvent::RaftRecovered {
                server,
                range,
                state: RecoveredAs::Quarantined,
                ..
            } if server == x && range == right)
        });
        let granted = report.records.iter().any(|record| {
            matches!(record.event, TraceEvent::RaftVote { server, range, granted: true, .. }
                if server == x && range == right)
        });
        let led = report.records.iter().any(|record| {
            matches!(record.event, TraceEvent::RaftLeader { server, range, .. }
                if server == x && range == right)
        });
        let takes = report.takes_of(right);
        if !stated || granted || led || takes != 0 {
            return Err(format!(
                "seed {seed}: node {x}'s right half {right}, made after its re-seed: stated \
                 quarantined {stated}, granted a vote {granted}, led {led}, {takes} takes: it \
                 should be quarantined and do none of it (D-130)"
            ));
        }
        Ok(())
    });
    verdict(&verdicts).expect("a right half made after a re-seed is quarantined on every seed");
}
