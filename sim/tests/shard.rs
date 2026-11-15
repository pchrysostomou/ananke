//! `sim/shard.rs`, the sharded sweep (SHARD.md §10; §12, Stage C): five nodes, three of
//! them the bootstrap nodes, ranges made by the splits the driver draws on stream
//! `shard`, leadership transfers, four clients over sixteen keys, and Phase 2's network
//! and disk faults.
//!
//! What it asserts, at every tier: the correct system passes every seed under every
//! check `sim/shard.rs`'s `Report::check` makes — checks 1 to 4 keyed by range, the range
//! layer's folds (7, 8, 9, 10, 16, 17, 18, 20), linearizability of every key across every
//! split, pre-vote's property, on uniform seeds the timer check, the write bound per key
//! and per range the run ended with and the meta convergence bound — and, beside them, check
//! 8's right half's clause under the owner's reading R of D-107 (PROPOSED D-141); that nodes 4
//! and 5 hold nothing, that each of the workload's shapes reached its window (D-131),
//! and that `Fault::MetaReorder` fired and reordered meta's updates (PROPOSED D-135); over the
//! thousand, that it reordered more than half the seeds meta applied its second split's update
//! on. It prints the driver's work, the refusals by the loss that made them (#45), the empty
//! blocks a split finds (D-092), the shapes' rates, the reorder arm's and its hold's, the seeds it
//! reordered, the bounds' margins, how often an install lets a hold go with no replica restored
//! (PROPOSED D-134) and the installed voters' waits against a candidate bound that is not
//! asserted (PROPOSED D-141).
// D-113: `sim/shard.rs`, Stage C's sharded sweep.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ananke_env::sim::TraceRecord;
use ananke_env::{Instant, NodeId, RangeCause, RecoveredAs, TraceEvent};
use ananke_raft::apply::SplitRefusal;
use ananke_raft::core::{Variant, Variants};
use ananke_shard::invariants::{self, Fold};
use ananke_shard::range_variant::{RangeVariant, RangeVariants};
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::raft;
use ananke_sim::shard::{self, DriverStats, DroppedTable, Load, Loss, NoRangeId};
use ananke_sim::{lin, seeds, sweep, verdict, write_trace};

/// What one seed of the sweep hands back: its verdict and its coverage.
struct Seen {
    /// The seed (PROPOSED D-135: the reordered seeds are printed).
    seed: u64,
    verdict: Result<(), String>,
    uniform: bool,
    drift_exceeded: bool,
    splits: usize,
    splits_of_halves: usize,
    ranges_at_end: usize,
    driver: DriverStats,
    refusals: BTreeMap<Loss, usize>,
    refused: usize,
    dropped: Vec<DroppedTable>,
    no_range_ids: Vec<NoRangeId>,
    most_ids: usize,
    /// The meta convergence lags asked of the run, `None` a descriptor not named.
    meta_lags: Vec<Option<Duration>>,
    /// The descriptors #220's exception excused from that bound, each with its seed.
    meta_excused: Vec<(u64, shard::MetaLag)>,
    /// Those of them only D-142's reading excuses: a replica quarantined under D-130 counted as
    /// a voter that cannot vote, by the owner's ruling of 2026-11-15.
    meta_excused_by_the_quarantine: Vec<(u64, shard::MetaLag)>,
    /// Whether every key the fixed map would ask is asked by the write bound.
    keys_asked: Result<(), String>,
    slowest_write: Option<Duration>,
    recovery_from_heal: Option<Duration>,
    recovery_from_call: Option<Duration>,
    /// The whole keyspace's first completion from the heal, where it is asked: the reading
    /// from the heal (D-113 §5).
    keyspace_from_heal: Option<Duration>,
    /// How long each asker D-092's assertion asks of took (D-113 §8).
    retries_asked: Vec<Duration>,
    /// What each of those askers was answered: `done`, or the refusal it stopped on; whether its
    /// split had applied by then, and whether its key was outside its range by then
    /// (PROPOSED D-137).
    retries_answered: Vec<(&'static str, bool, bool)>,
    /// Check 8's right-half clause under reading R tripped (PROPOSED D-141).
    right_half: bool,
    /// The restatements past a split the clause excused, by a voter that had not applied it
    /// and whose P an install had put there (`Excused`), and of those how many held no
    /// right half, where §8's reading fails and reading R passes (PROPOSED D-141).
    right_half_excused: (usize, usize),
    /// The installs check 8's fold read off a restatement, a crash having cut the switch
    /// before its read-back (PROPOSED D-141, review F5).
    crash_cut_installs: usize,
    /// The installed voters, measured and not asserted (PROPOSED D-141): how long each given
    /// its right half by its creation waited, how long each whose wait a restatement holding
    /// R ended had waited (round 3), how many were refused first, how long each still open
    /// at the run's end had waited, and the first late against the candidate bound
    /// `raft::RIGHT_HALF_ARRIVAL`, in words.
    installed_waits: Vec<Duration>,
    installed_restated: Vec<Duration>,
    installed_refused: usize,
    installed_open: Vec<Duration>,
    installed_late: Option<String>,
    partitions: usize,
    blocks: usize,
    crashes: usize,
    lease_reads: usize,
    placeholders: usize,
    /// What the workload's shapes reached on this seed (D-131).
    shapes: shard::Shapes,
    /// Which shapes this seed drew: a write burst, a read burst, the stale cache, the restart.
    drawn: [bool; 4],
    /// The holds an install let go with no replica restored (PROPOSED D-134).
    released: Released,
    /// Whether this seed drew `Fault::MetaReorder` (PROPOSED D-135).
    reorder_drawn: bool,
    /// What each reorder arm that found a range came to (PROPOSED D-135).
    reorders: Vec<shard::MetaReorder>,
}

/// What PROPOSED D-134's record did on one run: the holds an install let go with no replica
/// restored (`RaftSnapshotReleased`), how many of those a switch that rebuilt no replica let go,
/// the rest being abandoned installs, and whether the timer check finds a different number of
/// gaps with D-134's arm (`TimerResets::ALL`) and without it (`WITHOUT_RELEASED_HOLD`). Printed,
/// not asserted: a count of a correct system's events is no property, and neither is the gap
/// count, since a gap inside a hold let go is rightly excused.
// PROPOSED(D-134): the record's rate, printed by the sweep that cites it.
struct Released {
    holds: usize,
    at_a_switch: usize,
    gaps_moved: bool,
}

/// A release is counted as a switch's where a `RaftSnapshot { taken: false }` of the same server
/// and range is decided at its decision: PROPOSED D-134's rule, as seed 16725's pin told them apart
/// until the Stage C integration made that pin an absence. Only a run with a release
/// replays the timer check twice: the arm acts on that record alone, so with none the two
/// replays are one.
// PROPOSED(D-134)
fn released_of(report: &shard::Report) -> Released {
    let records = report.records();
    let releases: Vec<(u64, u64, &TraceRecord)> = records
        .iter()
        .filter_map(|record| match record.event {
            TraceEvent::RaftSnapshotReleased { server, range } => Some((server, range, record)),
            _ => None,
        })
        .collect();
    let at_a_switch = releases
        .iter()
        .filter(|&&(server, range, release)| {
            records.iter().any(|record| {
                matches!(record.event,
                    TraceEvent::RaftSnapshot { server: s, range: r, taken: false, .. }
                        if (s, r) == (server, range) && record.decided == release.decided)
            })
        })
        .count();
    let gaps_moved = !releases.is_empty()
        && report.checked.timer_gaps(raft::TimerResets::ALL).len()
            != report
                .checked
                .timer_gaps(raft::TimerResets::WITHOUT_RELEASED_HOLD)
                .len();
    Released {
        holds: releases.len(),
        at_a_switch,
        gaps_moved,
    }
}

/// Prints [`Released`] over a sweep, its runs in seed order.
// PROPOSED(D-134)
fn print_released<'a>(name: &str, runs: impl Iterator<Item = &'a Released>) {
    let (mut count, mut holds, mut at_a_switch, mut seeds) = (0_usize, 0, 0, 0);
    let mut moved = Vec::new();
    for (seed, run) in runs.enumerate() {
        count += 1;
        holds += run.holds;
        at_a_switch += run.at_a_switch;
        seeds += usize::from(run.holds > 0);
        if run.gaps_moved {
            moved.push(seed);
        }
    }
    println!(
        "shard: {name}: {holds} holds an install let go with no replica restored, on \
         {seeds}/{count} seeds (PROPOSED D-134), {} of them abandoned and {at_a_switch} at a \
         switch that rebuilt no replica; the timer check's gaps differ in number with D-134's arm \
         and without it on seeds {moved:?}",
        holds - at_a_switch,
    );
}

/// SHARD.md §8's write bound, as `raft::Report::liveness_by_final_range` asks it, keeps the
/// fixed map's keys (`raft::Report::liveness`, under the map that sends every key to its first
/// range): where the first range has a majority up, the range serving every key the clients wrote
/// after the heal has one too, so every key the old reading asks is still asked, under the same
/// constant. It is about which keys are asked and not about when a reading's clock starts; the
/// reading from the heal, kept beside the one from each range's first call, is what answers for
/// that (D-113 §5).
fn every_key_still_asked(report: &shard::Report) -> Result<(), String> {
    let checked = &report.checked;
    let live = checked.ranges_with_a_majority_up();
    if !live.contains(&ananke_sim::ranges::FIRST_RANGE) {
        return Ok(());
    }
    for key in checked.writes_after_heal_by_key().into_keys() {
        let serving = checked.final_range_of(&key);
        if !live.contains(&serving) {
            return Err(format!(
                "seed {}: the first range has a majority up and range {serving}, serving {:?} \
                 where the run stopped, does not: the final range's reading asks less than the \
                 fixed map's",
                report.seed,
                String::from_utf8_lossy(&key)
            ));
        }
    }
    Ok(())
}

fn seen_of(seed: u64) -> Seen {
    let report = shard::run(seed);
    // PROPOSED(D-141): check 8's right half's clause under reading R, asked of the correct
    // system beside the sweep's own checks.
    let verdict = report
        .check()
        .and_then(|()| report.checked.right_half_held());
    if verdict.is_err() {
        write_trace(&format!("shard-{seed}"), &report.jsonl());
    }
    let checked = &report.checked;
    let live = checked.ranges_with_a_majority_up();
    let (mut from_heal, mut from_call) = (None, None);
    if checked.uniform() {
        for (range, recovery) in checked.recoveries_by_final_range() {
            if !live.contains(&range) {
                continue;
            }
            if let Some(completed) = recovery.first_completed {
                let heal = completed.duration_since(checked.last_heal);
                let call = completed.duration_since(recovery.first_call.max(checked.last_heal));
                from_heal = from_heal.max(Some(heal));
                from_call = from_call.max(Some(call));
            }
        }
    }
    let slowest_write = checked
        .uniform()
        .then(|| {
            checked
                .writes_after_heal_by_key()
                .into_iter()
                .filter(|(key, _)| live.contains(&checked.final_range_of(key)))
                .filter_map(|(_, took)| took)
                .max()
        })
        .flatten();
    let keyspace_from_heal = (checked.uniform() && live.contains(&ananke_sim::ranges::FIRST_RANGE))
        .then(|| checked.time_to_write_after_heal())
        .flatten();
    let count =
        |want: fn(&TraceEvent) -> bool| report.records().iter().filter(|r| want(&r.event)).count();
    // PROPOSED(D-141)
    let folded = invariants::folded(report.records());
    let owners = folded.owners();
    let mut installed_waits = Vec::new();
    let mut installed_restated = Vec::new();
    let mut installed_refused = 0;
    let mut installed_open = Vec::new();
    for voter in owners.installed_voters() {
        match voter.ended {
            Some((at, invariants::WaitEnded::Created)) => {
                installed_waits.push(at.duration_since(voter.since));
            }
            Some((at, invariants::WaitEnded::Restated)) => {
                installed_restated.push(at.duration_since(voter.since));
            }
            Some((_, invariants::WaitEnded::Refused)) => installed_refused += 1,
            None => installed_open.push(report.checked.ended.duration_since(voter.since)),
        }
    }
    let installed_late = raft::right_half_late(
        owners.installed_voters(),
        report.checked.ended,
        raft::RIGHT_HALF_ARRIVAL,
    );
    Seen {
        uniform: checked.uniform(),
        drift_exceeded: report.schedule.drift_exceeded(),
        splits: report.splits(),
        splits_of_halves: report.splits_of_halves(),
        ranges_at_end: report.spans().user().count(),
        driver: report.driver.clone(),
        refusals: report.refusals_by_loss(),
        refused: report.refusals().len(),
        dropped: report.dropped_tables(),
        no_range_ids: report.no_range_ids(),
        most_ids: report.most_ids_from_one_block(),
        meta_lags: report
            .meta_lags_asked()
            .iter()
            .map(shard::MetaLag::lag)
            .collect(),
        meta_excused: report
            .meta_lags_excused()
            .into_iter()
            .map(|lag| (seed, lag))
            .collect(),
        meta_excused_by_the_quarantine: report
            .meta_lags_excused_by_the_quarantine()
            .into_iter()
            .map(|lag| (seed, lag))
            .collect(),
        keys_asked: every_key_still_asked(&report),
        slowest_write,
        recovery_from_heal: from_heal,
        recovery_from_call: from_call,
        keyspace_from_heal,
        retries_asked: report
            .retries_asked()
            .iter()
            .map(|retried| retried.took(report.last_fault_healed))
            .collect(),
        retries_answered: report
            .retries_asked()
            .iter()
            .map(|retried| {
                let fate = report.split_fate(retried);
                let by_the_answer =
                    |at: Option<ananke_env::Instant>| at.is_some_and(|at| at <= retried.ended);
                let answer = match (retried.answered, retried.refusal) {
                    (false, _) => "no answer",
                    (true, None) => "done",
                    (true, Some(refusal)) => refusal.name(),
                };
                (
                    answer,
                    by_the_answer(fate.applied),
                    by_the_answer(fate.outside),
                )
            })
            .collect(),
        right_half: folded.verdict_of(Fold::RightHalf).is_err(),
        right_half_excused: (
            owners.excused().len(),
            owners.excused().iter().filter(|e| !e.held).count(),
        ),
        crash_cut_installs: owners.crash_cut_installs(),
        installed_waits,
        installed_restated,
        installed_refused,
        installed_open,
        installed_late,
        partitions: count(|e| matches!(e, TraceEvent::PartitionStarted { .. })),
        blocks: count(|e| matches!(e, TraceEvent::LinkBlocked { .. })),
        crashes: count(|e| matches!(e, TraceEvent::NodeCrashed { .. })),
        lease_reads: count(|e| matches!(e, TraceEvent::RaftRead { lease: true, .. })),
        placeholders: count(|e| matches!(e, TraceEvent::RangeReplicaCreated { .. })),
        released: released_of(&report),
        shapes: report.shapes(),
        drawn: [
            report
                .schedule
                .actions
                .iter()
                .any(|a| matches!(a.load, Load::WriteBurst { .. })),
            report
                .schedule
                .actions
                .iter()
                .any(|a| matches!(a.load, Load::ReadBurst { .. })),
            report
                .schedule
                .actions
                .iter()
                .any(|a| matches!(a.load, Load::StaleCache { .. })),
            report
                .schedule
                .faults
                .iter()
                .any(|f| matches!(f, shard::Fault::SplitFromRestarted { .. })),
        ],
        reorder_drawn: report
            .schedule
            .faults
            .iter()
            .any(|f| matches!(f, shard::Fault::MetaReorder { .. })),
        reorders: report.meta_reorders(),
        seed,
        verdict,
    }
}

/// The correct system, under every check, at every tier. Seeds 3870 and 5303, the two the nightly
/// failed on with a committed entry lost (#175), pass since a right half made on a quarantined
/// parent takes the quarantine (D-130), and are pinned below on that mechanism.
/// Seeds 2272 and 7949, a right half with no possible majority, pass under the owner's ruling
/// of 2026-11-09, which excuses exactly that shape from the meta convergence bound and from
/// nothing else, and which this prints (#220; D-113 §6, §11). Seed 1137, that shape
/// with a replica quarantined under D-130 in a placeholder's place, passes under the owner's
/// ruling of 2026-11-15, which counts that replica as a voter that cannot vote in the exception
/// alone, and this prints what that reading added (D-142).
#[test]
fn every_seed_passes_on_the_correct_sharded_sweep() {
    let seen = sweep(seeds(), seen_of);
    let count = seen.len();
    let splits: usize = seen.iter().map(|s| s.splits).sum();
    let halves: usize = seen.iter().map(|s| s.splits_of_halves).sum();
    let unsplit = seen.iter().filter(|s| s.splits == 0).count();
    let most_ranges = seen.iter().map(|s| s.ranges_at_end).max().unwrap_or(0);
    let asked: u64 = seen.iter().map(|s| s.driver.splits_asked).sum();
    let done: u64 = seen.iter().map(|s| s.driver.splits_done).sum();
    let skipped: u64 = seen.iter().map(|s| s.driver.splits_skipped).sum();
    let unanswered: u64 = seen.iter().map(|s| s.driver.splits_unanswered).sum();
    let transfers: u64 = seen.iter().map(|s| s.driver.transfers_asked).sum();
    let transferred: u64 = seen.iter().map(|s| s.driver.transfers_done).sum();
    let mut refused: BTreeMap<&str, u64> = BTreeMap::new();
    for s in &seen {
        for (reason, n) in &s.driver.refused {
            *refused.entry(reason).or_default() += n;
        }
    }
    println!(
        "shard: {count} seeds, {} uniform, {} past the lease's drift bound; {splits} splits took \
         effect, {halves} of them of a range a split made, on all but {unsplit} seeds; at most \
         {most_ranges} user ranges at a run's end; the driver asked {asked} splits, {done} done, \
         refused {refused:?}, {unanswered} unanswered, {skipped} with no span to split; \
         {transferred}/{transfers} transfers done",
        seen.iter().filter(|s| s.uniform).count(),
        seen.iter().filter(|s| s.drift_exceeded).count(),
    );
    println!(
        "shard: {} partitions, {} one-way blocks, {} crashes, {} lease reads, {} of them on seeds \
         past the drift bound, {} placeholders made",
        seen.iter().map(|s| s.partitions).sum::<usize>(),
        seen.iter().map(|s| s.blocks).sum::<usize>(),
        seen.iter().map(|s| s.crashes).sum::<usize>(),
        seen.iter().map(|s| s.lease_reads).sum::<usize>(),
        seen.iter()
            .filter(|s| s.drift_exceeded)
            .map(|s| s.lease_reads)
            .sum::<usize>(),
        seen.iter().map(|s| s.placeholders).sum::<usize>(),
    );

    // #45: the refusals by the loss that made them, and what a dropped table touches.
    let mut losses: BTreeMap<Loss, usize> = BTreeMap::new();
    for s in &seen {
        for (loss, n) in &s.refusals {
            *losses.entry(*loss).or_default() += n;
        }
    }
    let refusal_seeds = seen.iter().filter(|s| s.refused > 0).count();
    let refusals: usize = seen.iter().map(|s| s.refused).sum();
    let dropped: Vec<&DroppedTable> = seen.iter().flat_map(|s| &s.dropped).collect();
    let whole = dropped.iter().filter(|t| t.touched == t.held).count();
    let mut by_touched: BTreeMap<usize, usize> = BTreeMap::new();
    for table in &dropped {
        *by_touched.entry(table.touched).or_default() += 1;
    }
    println!(
        "shard: {refusals} store refusals on {refusal_seeds}/{count} seeds, by the loss that \
         made them (#45): {losses:?}; {} dropped tables, {whole} of them touching every replica \
         on their node; by replicas touched {by_touched:?}",
        dropped.len()
    );

    // D-092: how often a split finds an empty block, and what emptied it.
    let empties: Vec<&NoRangeId> = seen.iter().flat_map(|s| &s.no_range_ids).collect();
    let empty_seeds = seen.iter().filter(|s| !s.no_range_ids.is_empty()).count();
    let new_run = empties.iter().filter(|e| !e.leased_since_restart).count();
    let within = empties.iter().filter(|e| e.leased_within).count();
    let most_taken = empties.iter().map(|e| e.taken).max().unwrap_or(0);
    let most_ids = seen.iter().map(|s| s.most_ids).max().unwrap_or(0);
    println!(
        "shard: `NoRangeId` {} times on {empty_seeds}/{count} seeds at a block of {} and a \
         threshold of {} (PROPOSED D-092): {new_run} on a node with no block granted since its \
         restart, {within} with a grant within a minimum election timeout, at most {most_taken} \
         ids taken from the block in force; the most ids any one block gave splits was {most_ids}",
        empties.len(),
        ananke_sim::ranges::ID_BLOCK,
        ananke_sim::ranges::REFILL_AT,
    );

    // The bounds' margins.
    let asked_lags: Vec<&Option<Duration>> = seen.iter().flat_map(|s| &s.meta_lags).collect();
    let unnamed = asked_lags.iter().filter(|lag| lag.is_none()).count();
    let mut lags: Vec<Duration> = asked_lags.iter().filter_map(|lag| **lag).collect();
    lags.sort_unstable();
    println!(
        "shard: meta convergence (SHARD.md §8) asked of {} descriptors: {unnamed} never named, \
         median {:?}, p99 {:?}, worst {:?}, against the bound of {:?}",
        asked_lags.len(),
        lags.get(lags.len() / 2),
        lags.get(lags.len() * 99 / 100),
        lags.last(),
        shard::meta_convergence_bound(),
    );
    let excused: Vec<&(u64, shard::MetaLag)> = seen.iter().flat_map(|s| &s.meta_excused).collect();
    println!(
        "shard: meta convergence excused {} descriptors, a right half with no possible majority \
         (#220; PROPOSED D-113 §6), {} of them never named; as (seed, range, generation): {:?}",
        excused.len(),
        excused
            .iter()
            .filter(|(_, lag)| lag.named.is_none())
            .count(),
        excused
            .iter()
            .map(|(seed, lag)| (*seed, lag.range, lag.generation))
            .collect::<Vec<_>>(),
    );
    // D-142: what the owner's ruling of 2026-11-15 added to the exception.
    let by_the_quarantine: Vec<&(u64, shard::MetaLag)> = seen
        .iter()
        .flat_map(|s| &s.meta_excused_by_the_quarantine)
        .collect();
    println!(
        "shard: of those, {} descriptors on {} seeds excused only because a voter's replica \
         quarantined under D-130 counts as one that cannot vote (D-142), {} of them never named; \
         as (seed, range, generation): {:?}",
        by_the_quarantine.len(),
        by_the_quarantine
            .iter()
            .map(|(seed, _)| *seed)
            .collect::<BTreeSet<_>>()
            .len(),
        by_the_quarantine
            .iter()
            .filter(|(_, lag)| lag.named.is_none())
            .count(),
        by_the_quarantine
            .iter()
            .map(|(seed, lag)| (*seed, lag.range, lag.generation))
            .collect::<Vec<_>>(),
    );
    println!(
        "shard: the write bound of {:?}: slowest post-heal write {:?}; a range's first completion \
         from the heal at worst {:?}, from its own first call at worst {:?}; the keyspace's first \
         completion from the heal, asserted, asked of {} seeds, at worst {:?}",
        raft::election_max() * raft::LIVENESS_TIMEOUTS,
        seen.iter().filter_map(|s| s.slowest_write).max(),
        seen.iter().filter_map(|s| s.recovery_from_heal).max(),
        seen.iter().filter_map(|s| s.recovery_from_call).max(),
        seen.iter()
            .filter(|s| s.keyspace_from_heal.is_some())
            .count(),
        seen.iter().filter_map(|s| s.keyspace_from_heal).max(),
    );
    let retries: Vec<Duration> = seen
        .iter()
        .flat_map(|s| s.retries_asked.iter().copied())
        .collect();
    let mut answered: BTreeMap<(&str, bool, bool), usize> = BTreeMap::new();
    for answer in seen.iter().flat_map(|s| &s.retries_answered) {
        *answered.entry(*answer).or_default() += 1;
    }
    println!(
        "shard: D-092's asker: {} askers refused `NoRangeId`, {} of them still asking at the last \
         fault's heal on a uniform seed with their ranges live; the slowest of those took {:?}; \
         their answers, with whether the split had applied by then and whether its key was outside \
         its range by then (PROPOSED D-137): {answered:?}",
        seen.iter().map(|s| s.driver.retried.len()).sum::<usize>(),
        retries.len(),
        retries.iter().max(),
    );
    // PROPOSED(D-141): check 8's right-half clause under the owner's reading R of D-107,
    // asserted by every seed's verdict above; printed with what reading R excused —
    // restatements by a voter that had not applied the split, where the two readings
    // differ, and of them those without R, where §8's reading fails and reading R passes —
    // each with how many of its seeds fall in the gate's twenty, CI's hundred and the
    // premerge's thousand, and with the installs read off a restatement. The installed
    // voters' waits are printed against the candidate bound `raft::RIGHT_HALF_ARRIVAL` and
    // **not asserted**: the correct system fails it on right halves that elect no leader
    // after the install (#220), and it waits on the owner.
    let excused_seeds: Vec<u64> = (0u64..)
        .zip(&seen)
        .filter(|(_, s)| s.right_half_excused.0 > 0)
        .map(|(seed, _)| seed)
        .collect();
    let unheld_seeds: Vec<u64> = (0u64..)
        .zip(&seen)
        .filter(|(_, s)| s.right_half_excused.1 > 0)
        .map(|(seed, _)| seed)
        .collect();
    let below = |of: &[u64]| [20, 100, 1000].map(|tier| of.iter().filter(|s| **s < tier).count());
    let mut waits: Vec<Duration> = seen
        .iter()
        .flat_map(|s| s.installed_waits.iter().copied())
        .collect();
    waits.sort_unstable();
    let restated: Vec<Duration> = seen
        .iter()
        .flat_map(|s| s.installed_restated.iter().copied())
        .collect();
    let open: Vec<Duration> = seen
        .iter()
        .flat_map(|s| s.installed_open.iter().copied())
        .collect();
    let late: Vec<(u64, &str)> = seen
        .iter()
        .zip(0u64..)
        .filter_map(|(s, seed)| s.installed_late.as_deref().map(|late| (seed, late)))
        .collect();
    println!(
        "shard: check 8's right-half clause under reading R (PROPOSED D-141), asserted, trips on \
         {}/{count} seeds; it excused {} restatements past a split by a voter that had not \
         applied it, on {}/{count} seeds (below seeds 20, 100, 1000: {:?}), {} of them without \
         the right half, on {}/{count} seeds (below 20, 100, 1000: {:?}); installs read off a \
         restatement, their switch cut by a crash: {} on {}/{count} seeds",
        seen.iter().filter(|s| s.right_half).count(),
        seen.iter().map(|s| s.right_half_excused.0).sum::<usize>(),
        excused_seeds.len(),
        below(&excused_seeds),
        seen.iter().map(|s| s.right_half_excused.1).sum::<usize>(),
        unheld_seeds.len(),
        below(&unheld_seeds),
        seen.iter().map(|s| s.crash_cut_installs).sum::<usize>(),
        seen.iter().filter(|s| s.crash_cut_installs > 0).count(),
    );
    println!(
        "shard: installed voters (PROPOSED D-141), measured and not asserted: {} on {} seeds, {} \
         given their right half by its creation, median {:?}, p99 {:?}, worst {:?}; {} at a \
         restatement holding it, the longest {:?}; {} refused first; {} still waiting at the \
         run's end, the longest {:?}; late against the candidate bound of {:?} on {}/{count} \
         seeds {:?}, first: {}",
        waits.len()
            + restated.len()
            + open.len()
            + seen.iter().map(|s| s.installed_refused).sum::<usize>(),
        seen.iter()
            .filter(|s| {
                !s.installed_waits.is_empty()
                    || !s.installed_restated.is_empty()
                    || !s.installed_open.is_empty()
                    || s.installed_refused > 0
            })
            .count(),
        waits.len(),
        waits.get(waits.len() / 2),
        waits.get(waits.len() * 99 / 100),
        waits.last(),
        restated.len(),
        restated.iter().max(),
        seen.iter().map(|s| s.installed_refused).sum::<usize>(),
        open.len(),
        open.iter().max(),
        raft::RIGHT_HALF_ARRIVAL,
        late.len(),
        late.iter().map(|(seed, _)| *seed).collect::<Vec<_>>(),
        late.first().map_or("none", |(_, words)| words),
    );
    // PROPOSED(D-134): how often an install lets a hold go with no replica restored.
    print_released("the correct node", seen.iter().map(|s| &s.released));

    // D-131: each shape, seeds that drew it and seeds that reached its window, each
    // reached well above D-061's 5 % over the seeds that draw it.
    let drawn = |i: usize| seen.iter().filter(|s| s.drawn[i]).count();
    let reached =
        |of: fn(&shard::Shapes) -> usize| seen.iter().filter(|s| of(&s.shapes) > 0).count();
    let reached_writes = reached(|s| s.writes_refused_at_apply);
    let reached_gets = reached(|s| s.gets_refused_at_serving);
    let reached_stale = reached(|s| s.stale_reads_refused);
    let reached_restart = reached(|s| s.splits_from_a_restarted_node);
    let reached_load = reached(|s| s.clients_mismatched_by_a_split);
    println!(
        "shard: the workload's shapes over {count} seeds, drew it -> reached its window: a split \
         under a right-half write burst {} -> {reached_writes}; under right-half gets {} -> \
         {reached_gets}; while a client's cache names the parent {} -> {reached_stale}; a \
         restart, then a split from that node {} -> {reached_restart}; any split with load -> \
         {reached_load} (a sweep client answered `RangeMismatch` by a range that had split); no \
         shape drawn on {}",
        drawn(0),
        drawn(1),
        drawn(2),
        drawn(3),
        seen.iter().filter(|s| !s.drawn.iter().any(|d| *d)).count(),
    );

    // PROPOSED(D-135): `Fault::MetaReorder`, by seeds: drew it, fired (found a range to split
    // twice led off meta's leader's node), handed the left half on (the first split landed), asked
    // the second split, meta applied the second's update, and meta applied an update of the first
    // after it. The hold, measured: how long after the block meta applied the second's update.
    let with = |of: fn(&shard::MetaReorder) -> bool| {
        seen.iter().filter(|s| s.reorders.iter().any(of)).count()
    };
    let reorder_drawn = seen.iter().filter(|s| s.reorder_drawn).count();
    let reorder_fired = seen.iter().filter(|s| !s.reorders.is_empty()).count();
    let handed = with(|r| r.arm.transferred);
    let second_asked = with(|r| r.arm.second_cut.is_some());
    let second_applied = with(|r| r.second_applied.is_some());
    let reordered = with(|r| r.reordered);
    let mut second: Vec<Duration> = seen
        .iter()
        .flat_map(|s| s.reorders.iter().filter_map(|r| r.second_applied))
        .collect();
    second.sort_unstable();
    let within = second
        .iter()
        .filter(|took| **took <= shard::META_REORDER_HOLD)
        .count();
    println!(
        "shard: `Fault::MetaReorder` over {count} seeds: drew it {reorder_drawn} -> fired \
         {reorder_fired} -> the left half handed on {handed} -> the second split asked \
         {second_asked} -> meta applied the second split's update {second_applied} -> and an \
         update of the first split after it on {reordered} seeds"
    );
    // PROPOSED(D-135): the seeds themselves, as SHARD.md §12's exit has them, beside §10's count
    // (#240's review).
    println!(
        "shard: `Fault::MetaReorder`: meta applied the second split's update before the first's on \
         seeds {:?}",
        seen.iter()
            .filter(|s| s.reorders.iter().any(|r| r.reordered))
            .map(|s| s.seed)
            .collect::<Vec<_>>(),
    );
    println!(
        "shard: `Fault::MetaReorder`'s hold of {:?}: meta applied the second split's update {} \
         times, {within} of them within the hold, from the block at median {:?}, p75 {:?}, p90 \
         {:?}, p99 {:?}, worst {:?}",
        shard::META_REORDER_HOLD,
        second.len(),
        second.get(second.len() / 2),
        second.get(second.len() * 3 / 4),
        second.get(second.len() * 9 / 10),
        second.get(second.len() * 99 / 100),
        second.last(),
    );

    // The firings, each far above D-061's 5 % over the seeds that see them.
    assert!(splits > 0, "no split took effect");
    assert!(halves > 0, "no half split again");
    assert!(transferred > 0, "no leadership transfer was done");
    assert!(seen.iter().any(|s| s.partitions > 0), "no partition");
    assert!(seen.iter().any(|s| s.crashes > 0), "no crash");
    assert!(
        seen.iter().any(|s| s.drift_exceeded),
        "no seed drew clocks past the lease's bound: check 6 asks nothing here"
    );
    assert!(
        seen.iter().any(|s| s.lease_reads > 0),
        "no lease read was served: check 6 asks nothing here"
    );
    assert!(
        refusal_seeds > 0,
        "no refusal: the disk's rot reached nothing"
    );
    if seeds() >= 1000 {
        assert!(
            empty_seeds > 0,
            "no split found an empty block over the thousand (PROPOSED D-092; D-061)"
        );
    }
    // PROPOSED(D-141), review F2: reading R reached where it differs from §8's reading.
    // First, a voter installed past a split restarts before applying it, and its restatement
    // past the split is read as no apply of it: measured on the correct system on 4 of the
    // first 20 seeds, 13 of the first 100, 111 of the first 1 000 (11.1 %) and 1 248 of the
    // first 10 000 (12.5 %), so asserted from CI's hundred (D-061: well above 5 %; at the
    // first thousand's rate, which D-061 reads, twenty seeds see none about one time in 10
    // (0.889^20 = 0.095), a hundred about once in 130 000 (0.889^100 = 7.8e-6)).
    // Second, such a restatement without R, which §8's reading fails and reading R passes:
    // measured on 2 of the first 100, 16 of the first 1 000 (1.6 %) and 202 of the first
    // 10 000 (2.0 %), under 5 %, so asserted from the thousand (D-061; at 1.6 % a thousand
    // see none about once in 10^7). Without them the clause would be asserted where reading
    // R and §8's reading are one. The node sweep sees the first on 3 of its first 1 000
    // seeds, too few to assert at any tier. Those are D-141's own tree's figures; on the Stage
    // C integration's tree the first is 3, 21 and 182 of the first 20, 100 and 1 000 (18.2 %),
    // the second 1, 2 and 24 (2.4 %), and the node sweep's first is none of its first 1 000,
    // so each assertion keeps its tier with D-061's margin at least as wide.
    if seeds() >= 100 {
        assert!(
            !excused_seeds.is_empty(),
            "no restatement past a split by a voter that had not applied it was read past as \
             installed on any of {count} seeds: check 8's right half's clause was asserted where \
             reading R and §8's reading read every restatement alike (PROPOSED D-141)"
        );
    }
    if seeds() >= 1000 {
        assert!(
            !unheld_seeds.is_empty(),
            "no restatement past a split without its right half was excused as installed on any \
             of {count} seeds: check 8's right half's clause was asserted where reading R and \
             §8's reading give one verdict (PROPOSED D-141)"
        );
    }
    // D-131: the shapes, each asserted reached at every tier, each well above D-061's
    // 5 % of the seeds that draw it.
    assert!(
        reached_writes > 0,
        "no split's apply check refused a burst's write: the write burst's window was not reached"
    );
    assert!(
        reached_gets > 0,
        "no check at serving refused a burst's get: the read burst's window was not reached"
    );
    assert!(
        reached_stale > 0,
        "no stale reader was refused at receipt: the stale-cache shape's window was not reached"
    );
    assert!(
        reached_restart > 0,
        "no restarted node split a range: the restart arm's window was not reached"
    );
    assert!(
        reached_load > 0,
        "no client was answered `RangeMismatch` by a split range: no split had load"
    );
    // PROPOSED(D-135): `Fault::MetaReorder`'s firing, and the order it aims at, each at every
    // tier: fired on 482 and reordered on 293 of 1 000 seeds, well above D-061's 5 %. These two say
    // the arm ran and the order was reached, not that the hold made it: with the block taken out
    // the sweep's own drops and resends still reorder meta on 3 of 100 seeds, so both pass at CI's
    // hundred without it (#240's review), though on none of the gate's twenty, where the second
    // fails (#240's second review). The hold's effect is the variant's test's below the thousand,
    // and over it the share below's.
    assert!(
        reorder_fired > 0,
        "no reorder arm found a range to split twice led off the meta range's leader's node"
    );
    assert!(
        reordered > 0,
        "meta applied no update of a reorder arm's first split after its second's: the order \
         `MetaOverwritesByArrival` needs was not reached"
    );
    // PROPOSED(D-135): the hold's effect, over the thousand: of the seeds on which meta applied
    // the second split's update, those it then applied a first's update on, 293 of 383 at the
    // thousand and 2 924 of 3 790 at the ten thousand with the hold; with the block taken out, 3 of
    // 38 at the hundred (#240's review) and 19 of 396 at the thousand, where this fails (#240's
    // second review). Under the thousand the sample is too small for a share.
    if seeds() >= 1000 {
        assert!(
            reordered * 2 > second_applied,
            "meta applied a first split's update after the second's on {reordered} of the \
             {second_applied} seeds it applied the second's on, not more than half: the hold no \
             longer holds the first split's update from meta"
        );
    }

    let inclusion: Vec<Result<(), String>> = seen.iter().map(|s| s.keys_asked.clone()).collect();
    verdict(&inclusion).unwrap();
    let verdicts: Vec<Result<(), String>> = seen.into_iter().map(|s| s.verdict).collect();
    verdict(&verdicts).unwrap();
}

// --- The pair rule on this sweep (CLAUDE.md; SHARD.md §10): the correct system above, and
// below the variants the sweep's own faults, driver and workload shapes (D-131) reach,
// each caught by the check that names it. `Fault::MetaReorder` is S12's (PROPOSED D-135).

/// A variant's run of one seed: what the verdict said, and what each of the range layer's
/// folds found on its own (D-106), since the verdict names only the first.
struct Under {
    why: Option<String>,
    tripped: BTreeSet<Fold>,
    /// Check 18's split clause's own verdict, where it tripped (PROPOSED D-137).
    lineage: Option<String>,
    /// Check 19's own verdict, in its words where it is in violation (PROPOSED D-140).
    tiling: Option<String>,
    splits: usize,
    /// The splits that took an id from a block of an earlier run of its holder.
    from_an_earlier_run: usize,
    /// The holds an install let go with no replica restored (PROPOSED D-134), measured only when
    /// [`under`] is asked to.
    released: Option<Released>,
}

/// `high_rate_share` of `sim/tests/node.rs`: a tenth of the tier, at least the gate's twenty.
fn share() -> u64 {
    (seeds() / 10).max(seeds().min(20))
}

/// The first words of a violation, past its seed.
fn words_of(violation: &str) -> String {
    let rest = violation
        .split_once(": ")
        .map_or(violation, |(_, rest)| rest);
    rest.split(':').next().unwrap_or(rest).to_owned()
}

/// Runs `seeds` seeds under the three sets and prints the rate of every check that spoke. With
/// `released`, each run also measures PROPOSED D-134's record ([`released_of`]); only the sweep
/// that prints it asks, since on a run with a release the measure replays the timer check twice.
fn under(
    name: &str,
    seeds: u64,
    cores: Variants,
    node: NodeVariants,
    range: RangeVariants,
    released: bool,
) -> Vec<Under> {
    let found = sweep(seeds, |seed| {
        let report = shard::run_of(seed, cores, node, range);
        let folded = invariants::folded(report.records());
        Under {
            why: report.check().err(),
            tripped: folded
                .verdicts()
                .into_iter()
                .filter_map(|(fold, verdict)| verdict.is_err().then_some(fold))
                .collect(),
            lineage: folded.verdict_of(Fold::SplitLineage).err(),
            tiling: folded.verdict_of(Fold::Tiling).err(),
            splits: report.splits(),
            from_an_earlier_run: shard::splits_from_a_block_of_an_earlier_run_of(report.records()),
            released: released.then(|| released_of(&report)),
        }
    });
    let mut by: BTreeMap<String, usize> = BTreeMap::new();
    for why in found.iter().filter_map(|u| u.why.as_deref()) {
        *by.entry(words_of(why)).or_default() += 1;
    }
    let mut folds: BTreeMap<Fold, usize> = BTreeMap::new();
    for fold in found.iter().flat_map(|u| &u.tripped) {
        *folds.entry(*fold).or_default() += 1;
    }
    println!(
        "shard: {name} failed {}/{seeds} seeds, by {by:?}; the folds on their own {folds:?}; a \
         split took effect on {}",
        found.iter().filter(|u| u.why.is_some()).count(),
        found.iter().filter(|u| u.splits > 0).count(),
    );
    found
}

fn range_variant(name: &str, seeds: u64, variant: RangeVariant) -> Vec<Under> {
    under(
        name,
        seeds,
        Variants::default(),
        NodeVariants::correct(),
        RangeVariants::of(&[variant]),
        false,
    )
}

fn core_variant(name: &str, variant: Variant) -> Vec<Under> {
    under(
        name,
        share(),
        variant.into(),
        NodeVariants::correct(),
        RangeVariants::correct(),
        false,
    )
}

fn tripped(found: &[Under], fold: Fold) -> usize {
    found.iter().filter(|u| u.tripped.contains(&fold)).count()
}

fn said(found: &[Under], words: &str) -> usize {
    found
        .iter()
        .filter_map(|u| u.why.as_deref())
        .filter(|why| why.contains(words))
        .count()
}

/// §10's `ClientIgnoresMismatch` on any split with load: 99 of 100 seeds.
#[test]
fn a_client_that_ignores_a_mismatch_is_caught_by_check_17_on_the_sharded_sweep() {
    let found = range_variant(
        "ClientIgnoresMismatch",
        share(),
        RangeVariant::ClientIgnoresMismatch,
    );
    assert!(
        tripped(&found, Fold::ClientRefreshes) > 0,
        "no client's resend after a mismatch reached check 17"
    );
}

/// §10's second catch of `ClientIgnoresMismatch`, "the liveness bound a client that never
/// converges": the write bound read on the uniform seeds, each run taken to its end since check
/// 17 would otherwise stop it first. A client that resends to the range it knew gets nothing
/// through while its cache is stale. On 12 of 500 uniform seeds no write of any key completed
/// after the heal, and on 6 more the first completion anywhere came later than the bound from
/// the heal, which the arm from each range's first call passes and only the reading from the heal
/// says (D-113 §5). Each is under D-061's 5 %, so each is asserted over the thousand and
/// printed below it.
// D-113: §10's second catch, and the reading from the heal on a variant.
#[test]
fn a_client_that_ignores_a_mismatch_trips_the_write_bound_on_the_sharded_sweep() {
    let run = if seeds() >= 1000 {
        seeds().min(1000)
    } else {
        share()
    };
    let found = sweep(run, |seed| {
        let report = shard::run_to_the_end(
            seed,
            Variants::default(),
            NodeVariants::correct(),
            RangeVariants::of(&[RangeVariant::ClientIgnoresMismatch]),
        );
        let uniform = report.checked.uniform();
        (
            uniform,
            uniform
                .then(|| report.checked.liveness_by_final_range().err())
                .flatten(),
        )
    });
    let uniform = found.iter().filter(|f| f.0).count();
    let caught: Vec<&String> = found.iter().filter_map(|f| f.1.as_ref()).collect();
    let by = |words: &str| caught.iter().filter(|why| why.contains(words)).count();
    let nothing = by("no client write completed");
    let late = by("after the last heal to complete a client write");
    println!(
        "shard: ClientIgnoresMismatch, each run to its end: the write bound on {}/{uniform} uniform \
         seeds of {run}, {nothing} with no write completed after the heal and {late} with the first \
         completion later than the bound from the heal",
        caught.len()
    );
    assert!(
        caught.iter().all(|why| why.starts_with("liveness: ")),
        "the write bound names itself: {caught:?}"
    );
    if seeds() >= 1000 {
        assert!(
            nothing > 0,
            "no client that ignores a mismatch left the cluster unwritten over the thousand"
        );
        assert!(
            late > 0,
            "no client that ignores a mismatch tripped the reading from the heal over the thousand"
        );
    }
}

/// §10's `TrustStaleDescriptor`: 100 of 100 seeds, every client stale after the first split.
#[test]
fn a_server_that_trusts_a_stale_descriptor_is_caught_by_check_9_on_the_sharded_sweep() {
    let found = range_variant(
        "TrustStaleDescriptor",
        share(),
        RangeVariant::TrustStaleDescriptor,
    );
    assert!(
        tripped(&found, Fold::ServingWithinSpan) > 0,
        "no read outside its range's span reached check 9"
    );
}

/// §10's `ApplyIgnoresSpan`: 81 of 100 seeds by check 9, the run stopped there and 11 of them
/// by check 10 too on the part of the run the stop left it (each run to its end, below).
#[test]
fn a_server_that_ignores_the_span_at_apply_is_caught_by_check_9_on_the_sharded_sweep() {
    let found = range_variant("ApplyIgnoresSpan", share(), RangeVariant::ApplyIgnoresSpan);
    assert!(
        tripped(&found, Fold::ServingWithinSpan) > 0,
        "no write applied outside its range's span reached check 9"
    );
}

/// §10's `ReadCheckAtReceiptOnly`, the pair's buggy half (the correct half is the sweep above,
/// which reaches this window and refuses every such read at serving: D-131): 57 of 100
/// seeds and 13 of the gate's 20 by check 9, the variant's named catch, at a `RaftRead` served for a right-half key at
/// or past the split it let in. Every failure is that catch, and none trips first on anything else,
/// so the words are asserted of every one.
// D-131
#[test]
fn a_read_checked_only_at_receipt_is_caught_by_check_9_on_the_sharded_sweep() {
    let found = range_variant(
        "ReadCheckAtReceiptOnly",
        share(),
        RangeVariant::ReadCheckAtReceiptOnly,
    );
    let failed: Vec<&str> = found.iter().filter_map(|u| u.why.as_deref()).collect();
    assert!(
        tripped(&found, Fold::ServingWithinSpan) > 0,
        "no read served past the split that let it in reached check 9: the read burst's window was \
         not reached under the variant"
    );
    assert!(
        failed.iter().all(|why| why.contains("at serving")),
        "the variant is caught by check 9 at serving and by nothing else first: {failed:?}"
    );
}

/// §10's `ApplyIgnoresSpan`, the rest of what its row names: a write P applied out of its span
/// over one R had already written is check 10 (72 of 100 seeds and 13 of the gate's 20), asserted at
/// every tier (D-061); each run taken to its end, since check 9 would otherwise stop it first. §10's third
/// catch, linearizability, is measured and printed and asserted of no tier: the keys of every
/// range on a node share one engine, so R's readers do see a write P made outside its span,
/// and the row's "a write P acknowledged that R's readers never see" has been reached on no
/// seed (PROPOSED D-131, the owner's question).
// PROPOSED(D-131)
#[test]
fn a_server_that_ignores_the_span_at_apply_trips_check_10_on_the_sharded_sweep() {
    let run = share();
    let found = sweep(run, |seed| {
        let report = shard::run_to_the_end(
            seed,
            Variants::default(),
            NodeVariants::correct(),
            RangeVariants::of(&[RangeVariant::ApplyIgnoresSpan]),
        );
        let folded = invariants::folded(report.records());
        (
            folded.verdict_of(Fold::ServingWithinSpan).is_err(),
            folded.verdict_of(Fold::GenerationsRise).is_err(),
            ananke_sim::lin::check(&report.checked.history).is_err(),
        )
    });
    let (nine, ten, lin) = (
        found.iter().filter(|f| f.0).count(),
        found.iter().filter(|f| f.1).count(),
        found.iter().filter(|f| f.2).count(),
    );
    println!(
        "shard: ApplyIgnoresSpan, each run to its end over {run}: check 9 on {nine}, check 10 on \
         {ten}, linearizability on {lin} (measured, not asserted: PROPOSED D-131)"
    );
    assert!(nine > 0, "no write applied out of its span reached check 9");
    assert!(
        ten > 0,
        "no key was written by two owners at one generation"
    );
}

/// §10's `TrustStaleDescriptor`, the rest of its row: the stale-cache shape's reader served by the
/// parent, and linearizability's catch, a read through P returning what a write through R has
/// replaced: 28 of 1 000 seeds, under D-061's 5 %, so asserted over the thousand and printed
/// below it, on the sweep's own seeds taken to their ends as check 9 would otherwise stop them
/// first (as the write bound's catch does for `ClientIgnoresMismatch`).
// D-131
#[test]
fn a_server_that_trusts_a_stale_descriptor_serves_what_a_write_through_r_replaced() {
    let run = if seeds() >= 1000 {
        seeds().min(1000)
    } else {
        share()
    };
    let found = sweep(run, |seed| {
        let report = shard::run_to_the_end(
            seed,
            Variants::default(),
            NodeVariants::correct(),
            RangeVariants::of(&[RangeVariant::TrustStaleDescriptor]),
        );
        (
            shard::stale_reads_served_of(report.records()) > 0,
            ananke_sim::lin::check(&report.checked.history).is_err(),
        )
    });
    let served = found.iter().filter(|f| f.0).count();
    let lin = found.iter().filter(|f| f.1).count();
    println!(
        "shard: TrustStaleDescriptor, each run to its end over {run}: the stale reader served by the \
         parent on {served}, linearizability on {lin}"
    );
    assert!(
        served > 0,
        "no stale reader was served by the parent: the stale-cache shape did not reach the variant"
    );
    if seeds() >= 1000 {
        assert!(
            lin > 0,
            "no read through the parent returned what a write through the right half replaced"
        );
    }
}

/// §10's `UninitialisedReplicaVotes` off its directed shape: a re-seeded node's placeholders
/// meet its ranges' elections on 99 of 100 seeds.
#[test]
fn a_placeholder_that_grants_votes_is_caught_by_check_20_on_the_sharded_sweep() {
    let found = range_variant(
        "UninitialisedReplicaVotes",
        share(),
        RangeVariant::UninitialisedReplicaVotes,
    );
    assert!(
        tripped(&found, Fold::PlaceholderGrants) > 0,
        "no placeholder's grant reached check 20"
    );
}

/// What one seed of `UninitialisedReplicaVotes`, run to its end, shows of its placeholders'
/// grants, each grant judged by check 20 on its own: the fold fed every record before it and
/// none of the run's other grants, so a grant behind another's violation is still judged.
/// A placeholder is folded from its node's `RangeReplicaCreated` of the range to the node's
/// `RangeCreated` of it or the node's crash, which no placeholder survives (D-109).
// PROPOSED(D-136): issue #221's plant, on the sharded sweep.
#[derive(Debug, Default)]
struct PlaceholderGrants {
    /// Every placeholder's grant, vote or pre-vote.
    granted: usize,
    /// Those check 20 does not name.
    unseen: usize,
    /// Those of a range the granting node restated before its last refusal, and neither
    /// restated nor created since: issue #221's plant.
    planted: usize,
    /// Of the plant, those check 20 names.
    planted_caught: usize,
}

// PROPOSED(D-136): issue #221's plant, on the sharded sweep.
fn placeholder_grants_judged(records: &[TraceRecord]) -> PlaceholderGrants {
    let mut found = PlaceholderGrants::default();
    // Check 20 fed every record but the grants, cloned at each grant to judge it alone.
    let mut fold = invariants::PlaceholderGrantsNothing::default();
    let mut placeholders: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut restated: BTreeSet<(u64, u64)> = BTreeSet::new();
    // (node, range): restated before the node's refusal, and nothing since.
    let mut lost: BTreeSet<(u64, u64)> = BTreeSet::new();
    for record in records {
        let Some(node) = record.node.map(|node| u64::from(node.get())) else {
            continue;
        };
        match &record.event {
            TraceEvent::RaftVote {
                range,
                granted: true,
                ..
            } => {
                if placeholders.contains(&(node, *range)) {
                    let mut alone = fold.clone();
                    alone.push(record);
                    let seen = alone.verdict().is_err();
                    found.granted += 1;
                    found.unseen += usize::from(!seen);
                    if lost.contains(&(node, *range)) {
                        found.planted += 1;
                        found.planted_caught += usize::from(seen);
                    }
                }
                continue;
            }
            TraceEvent::RangeReplicaCreated { range } => {
                placeholders.insert((node, *range));
            }
            TraceEvent::RangeCreated { range, .. } => {
                placeholders.remove(&(node, *range));
                lost.remove(&(node, *range));
            }
            TraceEvent::NodeCrashed { .. } => placeholders.retain(|(of, _)| *of != node),
            TraceEvent::RangesRestated { ranges, .. } => {
                for range in ranges {
                    restated.insert((node, *range));
                    lost.remove(&(node, *range));
                }
            }
            TraceEvent::RangeRemoved { range, .. } => {
                restated.remove(&(node, *range));
                lost.remove(&(node, *range));
            }
            TraceEvent::RaftRefused { .. } => {
                let held: Vec<(u64, u64)> = restated
                    .iter()
                    .filter(|(of, _)| *of == node)
                    .copied()
                    .collect();
                for held in held {
                    restated.remove(&held);
                    lost.insert(held);
                }
            }
            _ => {}
        }
        fold.push(record);
    }
    found
}

/// Issue #221's plant and its catch (PROPOSED D-136): `UninitialisedReplicaVotes` run to its
/// end, its placeholders' grants limited to the ranges the granting node had restated before
/// its refusal, each judged by check 20 on its own. Check 20 kept a range held across its
/// node's refusal until D-136, so a refused node's placeholder of a range it held before its
/// loss granted unseen. Measured on this tree, release, over 1 000 seeds: placeholders granted
/// 10 427 times on 984 seeds, and with the fold before D-136 1 064 of those grants were unseen,
/// every one on a refused node, and the plant's 96 grants on 18 seeds all were (#221 measured
/// 601 of 9 142 and 42 on 7 at 97b72e4). With D-136 none is unseen and all 96 are named. Every
/// grant of the plant named, and every placeholder's grant of the run, is asserted at every
/// tier. The plant is reached on 1.8 % of seeds, under D-061's 5 %, so its reach is asserted
/// from the thousand, and printed at every tier. The other half of the pair is the correct
/// system's sweep above, whose check 20 holds every seed green with the same fold. Re-measured
/// on the Stage C integration's tree, the same way: placeholders granted 12 881 times on 991
/// seeds, none unseen, and the plant's 199 grants on 38 seeds (3.8 %, still under 5 %) all named.
// PROPOSED(D-136): issue #221's plant, on the sharded sweep.
#[test]
fn a_refused_nodes_placeholder_of_a_range_it_restated_before_its_loss_is_caught_by_check_20() {
    let run = if seeds() >= 1000 {
        seeds().min(1000)
    } else {
        share()
    };
    let found = sweep(run, |seed| {
        let report = shard::run_to_the_end(
            seed,
            Variants::default(),
            NodeVariants::correct(),
            RangeVariants::of(&[RangeVariant::UninitialisedReplicaVotes]),
        );
        placeholder_grants_judged(report.records())
    });
    let sum = |of: fn(&PlaceholderGrants) -> usize| found.iter().map(of).sum::<usize>();
    let on = |of: fn(&PlaceholderGrants) -> usize| found.iter().filter(|f| of(f) > 0).count();
    let (granted, unseen) = (sum(|f| f.granted), sum(|f| f.unseen));
    let (planted, caught) = (sum(|f| f.planted), sum(|f| f.planted_caught));
    println!(
        "shard: UninitialisedReplicaVotes, each run to its end over {run}: placeholders granted \
         {granted} times on {} seeds, {unseen} unseen by check 20; the plant, a refused node's \
         placeholder of a range it restated before its loss, granted {planted} times on {} \
         seeds, {caught} named by check 20",
        on(|f| f.granted),
        on(|f| f.planted),
    );
    assert!(
        granted > 0,
        "no placeholder granted: UninitialisedReplicaVotes did not fire"
    );
    let seeds_of = |of: fn(&PlaceholderGrants) -> bool| -> Vec<usize> {
        (0..found.len()).filter(|&seed| of(&found[seed])).collect()
    };
    assert_eq!(
        caught,
        planted,
        "check 20 left a grant of the plant unseen on seeds {:?}: a refused node's placeholder \
         of a range it restated before its loss granted, and the range was still held",
        seeds_of(|f| f.planted_caught < f.planted)
    );
    assert_eq!(
        unseen,
        0,
        "check 20 left a placeholder's grant unseen on seeds {:?}",
        seeds_of(|f| f.unseen > 0)
    );
    if seeds() >= 1000 {
        assert!(
            planted > 0,
            "no refused node's placeholder granted for a range it restated before its loss \
             over the thousand: the plant was not reached (D-061)"
        );
    }
}

/// §10's `SnapshotOverlapsReplica` off its directed shape: 25 of 100 seeds by check 8's
/// overlap clause.
#[test]
fn a_snapshot_that_overlaps_a_replica_is_caught_by_check_8_on_the_sharded_sweep() {
    let found = range_variant(
        "SnapshotOverlapsReplica",
        share(),
        RangeVariant::SnapshotOverlapsReplica,
    );
    assert!(
        tripped(&found, Fold::Overlap) > 0,
        "no install over another replica's span reached check 8's overlap clause"
    );
}

/// Check 19's instrument, `SplitLeavesGap` (the owner's ruling on D-093, option A; outside
/// §10's count): every replica of a split's parent writes the right half one key short, and
/// the split key lies in no range from the split's apply on. Check 19 sees the gap at the
/// split's apply on its first replica, beside check 18's split clause, which sees the short
/// right half there and which the run's verdict names, check 19 being asked last. D-104 §2:
/// splits are asserted only in aggregate, so the seeds a split took effect on are read and
/// asserted non-empty, and check 19 asserted in violation on every one of them and on no
/// other, on each in its gap's words with check 18's split clause the only other fold in
/// violation, so a right half written over its parent's span, an overlap, does not pass for
/// the instrument. Measured on this tree, release: a split took effect on 99 of 100 seeds and
/// check 19 tripped on the same 99, 19 of the gate's 20 — at every tier (D-061) — and every run
/// stopped at check 18's split clause. On the Stage C integration's tree it is 100 of 100 and 20
/// of the gate's 20, every run stopped the same way. The correct sweep above holds check 19 green
/// on every seed.
// PROPOSED(D-140)
#[test]
fn a_split_that_leaves_its_key_in_no_range_is_caught_by_check_19_on_the_sharded_sweep() {
    let found = range_variant("SplitLeavesGap", share(), RangeVariant::SplitLeavesGap);
    let split = found.iter().filter(|u| u.splits > 0).count();
    let caught = found
        .iter()
        .filter(|u| u.tripped.contains(&Fold::Tiling))
        .count();
    println!(
        "shard: SplitLeavesGap: a split took effect on {split}/{} seeds, check 19 tripped on \
         {caught}",
        found.len()
    );
    assert!(
        split > 0,
        "no split took effect under SplitLeavesGap, so no right half was written short (D-104 \
         §2: the seeds a split took effect on are read, not assumed)"
    );
    for (seed, u) in found.iter().enumerate() {
        assert_eq!(
            u.tripped.contains(&Fold::Tiling),
            u.splits > 0,
            "seed {seed}: check 19 under SplitLeavesGap does not follow the split: {} splits \
             took effect, and the folds in violation are {:?}",
            u.splits,
            u.tripped
        );
        if u.splits == 0 {
            continue;
        }
        // A gap and not an overlap, which is why D-093 took this instrument: check 19 in
        // its gap's words, and beside it check 18's split clause alone, check 8's overlap
        // clause silent.
        let words = u.tiling.as_deref().unwrap_or_default();
        assert!(
            words.contains("no range's latest span holds") && words.contains("range command"),
            "seed {seed}: check 19 tripped under SplitLeavesGap in words other than a gap at a \
             range command's apply: {words}"
        );
        assert_eq!(
            u.tripped,
            BTreeSet::from([Fold::SplitLineage, Fold::Tiling]),
            "seed {seed}: under SplitLeavesGap the folds in violation are check 18's split \
             clause and check 19, and no other"
        );
    }
    let failed: Vec<&str> = found.iter().filter_map(|u| u.why.as_deref()).collect();
    assert!(
        failed
            .iter()
            .all(|why| why.contains("descriptor lineage: right half")),
        "the run stops at check 18's split clause, the first fold the verdict asks that sees \
         the short right half, and at nothing before it: {failed:?}"
    );
}

/// §10's `IdBlockResumed`: 61 of 100 seeds and 8 of the gate's 20 by check 18, on the restart arm's shape
/// (`Fault::SplitFromRestarted`, D-131), where it was 10 of 100 on the restarts the faults
/// drew and the splits that landed on them by chance before the arm. So the catch is asserted at
/// every tier, in the words of §12's "wrong if": a tier that runs this and catches nothing says
/// the sweep does not reach the variant. The shape the variant aims at is asserted too: a split that
/// took an id from a block of an earlier run of its node
/// (`shard::splits_from_a_block_of_an_earlier_run_of`), on 62 of 100 seeds and 8 of the gate's 20. The correct node's new run
/// holds none, so that is the variant seen firing.
///
/// **Check 18 compares the run** (PROPOSED D-137, issue #222). Compared by the node alone it caught
/// the variant only where an id was then taken twice, 42 of 100 and 4 of 20. Its split clause now
/// names the run on 28 of 100 and 6 of the gate's 20, and an id taken twice on 33 and 2, the words of
/// whichever split it met first; so the run's catch is asserted at every tier too.
// D-113: §10, the shape at every tier.
// D-131: §12's "wrong if", at every tier the arm reaches it.
// PROPOSED(D-137): the run compared, at every tier.
#[test]
fn a_node_that_resumes_another_runs_block_of_ids_is_caught_by_check_18_on_the_sharded_sweep() {
    let found = range_variant("IdBlockResumed", share(), RangeVariant::IdBlockResumed);
    let reached = found.iter().filter(|u| u.from_an_earlier_run > 0).count();
    println!(
        "shard: IdBlockResumed: a split took an id from a block of an earlier run of its node on \
         {reached}/{} seeds",
        found.len()
    );
    assert!(
        reached > 0,
        "no split took an id from a block of an earlier run of its node: the restart and the \
         split IdBlockResumed needs were not reached"
    );
    assert!(
        tripped(&found, Fold::SplitLineage) > 0,
        "IdBlockResumed was caught by check 18 on no seed: the sharded sweep does not reach it \
         (SHARD.md §12's \"wrong if\")"
    );
    // PROPOSED(D-137): what check 18's split clause said, the run compared (issue #222) or an id
    // taken twice.
    let lineage = |words: &str| {
        found
            .iter()
            .filter(|u| u.lineage.as_deref().is_some_and(|why| why.contains(words)))
            .count()
    };
    let by_run = lineage("a node takes ids only from a block of its current run");
    let twice = lineage("which a creation or a split had named before it");
    println!(
        "shard: IdBlockResumed: check 18's split clause by the run compared on {by_run}/{}, by an \
         id taken twice on {twice}",
        found.len()
    );
    assert!(
        by_run > 0,
        "check 18 did not compare the run on any seed: no split out of a block of an earlier run \
         of its leader's node was read as one (issue #222)"
    );
}

/// §10's `MetaOverwritesByArrival` on what this sweep's drops and resends do by themselves
/// and under `Fault::MetaReorder` (PROPOSED D-135), which reorders meta's updates on 294 of 1 000
/// seeds: check 16 on 351 of 1 000 seeds, every one of the 294 among them, where it was 68 of 1 000
/// on the drops and resends alone; and the meta convergence bound on 29 of 1 000, 24 of them
/// reordered by the arm, where it was 12. The arm sits in the first half of the faults, and the
/// bound sees a regressed record only where no later update of the range, a leader taking office or
/// a split, writes over it before the run ends. Check 16 is asserted at every tier, the bound over
/// the thousand and printed below it (D-061). The bound is asked of the run taken to its end, which
/// check 16 would otherwise stop first. What every tier asserts besides is the shape the variant
/// aims at: a split's update stored again over a key meta already named at its generation or a
/// newer one (`shard::meta_resends_stored_of`), 993 of 1 000 seeds, the correct meta range storing
/// none; the arm's reorder; and check 16 on every seed the arm reordered, since the first split's
/// left half, applied over the second's records, lowers the generation meta names.
// D-113: §10, the shape at every tier.
// PROPOSED(D-135): check 16 at every tier under `Fault::MetaReorder`.
#[test]
fn a_meta_range_that_stores_updates_as_they_arrive_is_caught_on_the_sharded_sweep() {
    let run = if seeds() >= 1000 {
        seeds().min(1000)
    } else {
        share()
    };
    let found = sweep(run, |seed| {
        let report = shard::run_to_the_end(
            seed,
            Variants::default(),
            NodeVariants::correct(),
            RangeVariants::of(&[RangeVariant::MetaOverwritesByArrival]),
        );
        (
            invariants::folded(report.records())
                .verdict_of(Fold::MetaNeverGoesBack)
                .is_err(),
            report.meta_converges().is_err(),
            shard::meta_resends_stored_of(report.records()) > 0,
            report.meta_reorders().iter().any(|r| r.reordered),
        )
    });
    let sixteen = found.iter().filter(|f| f.0).count();
    let bound = found.iter().filter(|f| f.1).count();
    let reached = found.iter().filter(|f| f.2).count();
    let reordered = found.iter().filter(|f| f.3).count();
    println!(
        "shard: MetaOverwritesByArrival, each run to its end: a split's update stored over a key \
         meta named at its generation or a newer one on {reached}/{run} seeds, check 16 on \
         {sixteen}/{run}, the meta convergence bound on {bound}/{run}; `Fault::MetaReorder` \
         reordered meta's updates on {reordered}/{run}, and of those check 16 tripped on {} and \
         the bound on {}",
        found.iter().filter(|f| f.3 && f.0).count(),
        found.iter().filter(|f| f.3 && f.1).count(),
    );
    assert!(
        reached > 0,
        "no split's update was stored over a key meta named at its generation or a newer one: \
         the resend MetaOverwritesByArrival needs was not reached"
    );
    // PROPOSED(D-135): the arm's reorder, and check 16 on each seed of it, at every tier.
    assert!(
        reordered > 0,
        "`Fault::MetaReorder` reordered meta's updates on no seed under the variant"
    );
    let missed: Vec<u64> = (0..run)
        .zip(&found)
        .filter(|(_, f)| f.3 && !f.0)
        .map(|(seed, _)| seed)
        .collect();
    assert!(
        missed.is_empty(),
        "seeds {missed:?}: meta applied the first split's update over the second's under the \
         variant, and check 16 did not trip"
    );
    assert!(
        sixteen > 0,
        "no update overwrote a newer one under the variant"
    );
    if seeds() >= 1000 {
        assert!(
            bound > 0,
            "meta named every live descriptor over the thousand under the variant"
        );
    }
}

/// Phase 2's pre-vote variant under this sweep's isolations: 81 of 100 seeds.
#[test]
fn a_server_without_pre_vote_is_caught_by_the_isolations_on_the_sharded_sweep() {
    let found = core_variant("NoPreVote", Variant::NoPreVote);
    assert!(said(&found, "pre-vote:") > 0, "no isolation raised a term");
}

/// Phase 2's timer variant: 30 of 100 seeds by the timer check, and 5 more by the write bound.
/// It prints the seeds where PROPOSED D-134's arm changes the number of gaps the check finds.
// PROPOSED(D-134): the arm's effect on the variant, printed.
#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught_on_the_sharded_sweep() {
    let found = under(
        "ResetTimerOnAnyRpc",
        share(),
        Variant::ResetTimerOnAnyRpc.into(),
        NodeVariants::correct(),
        RangeVariants::correct(),
        true,
    );
    print_released(
        "ResetTimerOnAnyRpc",
        found.iter().map(|u| {
            u.released
                .as_ref()
                .expect("measured on every run, as asked")
        }),
    );
    assert!(
        said(&found, "timers:") > 0,
        "no timer stayed reset by a stale sender"
    );
}

/// The disk's half of the fault model: Q15's whole-node refusal, which the rot reaches on 401
/// of 1 000 seeds. A node that refuses one range alone goes on serving the others over an
/// engine that lost state: 18 of 100 seeds by state machine safety until PROPOSED D-136, and
/// since D-136, which has check 20 forget at a refusal what the node held, 37 of 100: 23 by
/// check 20, the unrefused empty replicas' grants, and 14 by state machine safety, fewer than
/// its 18 since a run stops at its first violation and check 20's can come first.
// PROPOSED(D-136)
#[test]
fn a_node_that_refuses_only_one_range_is_caught_by_the_disks_rot_on_the_sharded_sweep() {
    let found = under(
        "RefuseOneRangeOnly",
        share(),
        Variants::default(),
        NodeVariants::of(&[NodeVariant::RefuseOneRangeOnly]),
        RangeVariants::correct(),
        false,
    );
    assert!(
        said(&found, "state machine safety") > 0,
        "no refused node's other replicas diverged"
    );
}

/// The round's half: Phase 2's `SendBeforePersist`, on every seed by commit by majority.
#[test]
fn a_server_that_sends_before_it_persists_is_caught_on_the_sharded_sweep() {
    let found = core_variant("SendBeforePersist", Variant::SendBeforePersist);
    assert!(
        said(&found, "commit majority:") > 0,
        "no commit outran a persist"
    );
}

/// D-092's rule that makes answer A enough, broken: a node at or below its threshold keeps a
/// refill outstanding, resent "until a grant carrying its current run nonce arrives". Under
/// `RefillNotResent` a refill sent while range 0 has no leader is the last, every split on that
/// node is refused `NoRangeId`, and an asker's retries after the heal end with no answer: 32 of
/// 100 seeds, by this check and no other. Re-measured once the retry has to take effect (PROPOSED
/// D-137): 32 of 100 and 8 of the gate's 20, every one with no answer and none by a retry answered
/// with no effect, since this variant's askers are never answered at all.
// D-092: answer A; the asker retries.
// D-113: §8's pair for the check read from the heal.
#[test]
fn a_node_that_does_not_resend_its_refill_leaves_the_askers_retries_unanswered() {
    let found = range_variant("RefillNotResent", share(), RangeVariant::RefillNotResent);
    // PROPOSED(D-137): which of D-092's two readings named each seed.
    println!(
        "shard: RefillNotResent: an asker with no answer on {}, a retry answered with no effect on \
         {}",
        said(&found, "with no answer"),
        said(&found, "and took no effect"),
    );
    assert!(
        said(&found, "with no answer") > 0,
        "no asker's retries after the heal went unanswered"
    );
}

/// The reading of D-092's check as first written, which took any answer but `NoRangeId` for the
/// retry's effect: a fate in which the split applied as the answer came (PROPOSED D-137).
fn as_first_written(retried: &shard::Retried) -> shard::SplitFate {
    shard::SplitFate {
        applied: Some(retried.ended),
        outside: Some(retried.ended),
    }
}

/// D-092's "the same asker's retry after the heal takes effect", broken where the reading as
/// first written could not see it (issue #222): under `RefusedAfterRefill` a node whose block a
/// split found empty refuses every split once its refill lands, `NotLive`. The asker refused
/// `NoRangeId` retries as on the correct node, is answered with a reason it does not retry, and the
/// split never applies. `retries_take_effect` now asks that the split applied by the answer, and
/// says "took no effect"; the reading that took any answer but `NoRangeId` for an effect, asked of
/// the same askers (each read as though its split applied as its answer came), passes every one.
///
/// Measured over seeds 0 to 999: an asker refused `NoRangeId` and then `NotLive` on 43, one the
/// check asks of (still asking at the last fault's heal, on a uniform seed with its ranges live) on
/// 7, and all 7 caught, in those words and in no others, each one the reading as first written
/// passes. 7 of 1 000 and 43 of 1 000 are under D-061's 5 %, so both are asserted over the thousand
/// and printed below it, where it sweeps `share()`, the same twenty at the gate and in CI (1 fires,
/// none asked), as D-113 §8 placed `NoRangeId`'s own count. Seed 992's pin asserts the catch at
/// every tier.
// PROPOSED(D-137): the retry takes effect, not only gets an answer.
#[test]
fn a_node_that_refuses_splits_once_its_refill_lands_leaves_the_retry_without_effect() {
    let run = if seeds() >= 1000 {
        seeds().min(1000)
    } else {
        share()
    };
    let bound = raft::election_max() * raft::LIVENESS_TIMEOUTS;
    let found = sweep(run, |seed| {
        let report = shard::run_of(
            seed,
            Variants::default(),
            NodeVariants::correct(),
            RangeVariants::of(&[RangeVariant::RefusedAfterRefill]),
        );
        let heal = report.last_fault_healed;
        let refused_after = report
            .driver
            .retried
            .iter()
            .filter(|retried| retried.refusal == Some(SplitRefusal::NotLive))
            .count();
        let asked = report.retries_asked();
        let old_reading = asked
            .iter()
            .find_map(|retried| retried.took_no_effect(heal, bound, as_first_written(retried)));
        (
            report.check().err(),
            refused_after,
            asked.len(),
            old_reading,
        )
    });
    let no_effect = |why: &Option<String>| {
        why.as_deref()
            .is_some_and(|why| why.contains("and took no effect"))
    };
    let fired = found.iter().filter(|f| f.1 > 0).count();
    let asked = found.iter().filter(|f| f.2 > 0).count();
    let failed = found.iter().filter(|f| f.0.is_some()).count();
    let caught = found.iter().filter(|f| no_effect(&f.0)).count();
    let old = found.iter().filter(|f| f.3.is_some()).count();
    let only_new = found
        .iter()
        .filter(|f| f.3.is_none() && no_effect(&f.0))
        .count();
    println!(
        "shard: RefusedAfterRefill over {run}: an asker refused `NoRangeId` and then `NotLive` on \
         {fired}, one D-092's check asks of on {asked}; failed {failed}, {caught} by the retry taking \
         no effect, {only_new} of them where the reading as first written passes; that reading \
         trips {old}"
    );
    if seeds() >= 1000 {
        assert!(
            fired > 0,
            "no asker refused `NoRangeId` was then refused `NotLive` over the thousand: the variant \
             was not seen firing"
        );
        assert!(
            caught > 0,
            "no asker's retry was read as taking no effect over the thousand"
        );
        assert!(
            only_new > 0,
            "the reading as first written trips every seed the retry's effect does: the pair no \
             longer shows what #222 asked of it"
        );
    }
}

/// The write bound read from the heal, D-076's "how long after the heal the range became writable
/// at all", kept beside the arm read from each range's first call (D-113 §5). The clients
/// are cut off from every node for the bound and 50 ms more of the 2.2 s window, the heal not moved:
/// a cluster nothing can write to for longer than the bound after the heal. No range's first call
/// in the history comes before the cut ends, so the arm from the first call is satisfied and this
/// arm is what is left to say so; where the 150 ms left complete nothing, the clients-wrote-nothing
/// arm speaks first. Above the 5 % of D-061 on every tier: 41 of 100 seeds, a uniform 41 of 50.
// D-113: §5's pair for the reading from the heal.
#[test]
fn clients_cut_off_past_the_bound_after_the_heal_are_caught_by_the_reading_from_the_heal() {
    let held = raft::election_max() * raft::LIVENESS_TIMEOUTS + Duration::from_millis(50);
    let run = share();
    let found = sweep(run, |seed| {
        let mut schedule = shard::Schedule::draw(seed);
        schedule.clients_held_off = Some(held);
        shard::run_with(
            seed,
            schedule,
            Variants::default(),
            NodeVariants::correct(),
            RangeVariants::correct(),
        )
        .check()
        .err()
    });
    let failed = found.iter().flatten().count();
    let from_the_heal = found
        .iter()
        .flatten()
        .filter(|why| why.contains("after the last heal to complete a client write"))
        .count();
    println!(
        "shard: the clients cut off for {held:?} past the heal failed {failed}/{run} seeds, \
         {from_the_heal} of them first by the reading from the heal"
    );
    assert!(
        from_the_heal > 0,
        "no seed's first completion read as later than the bound from the heal"
    );
}

// --- The first Phase 3 seeds pinned, each asserting its mechanism (CLAUDE.md).
//
// PROPOSED(D-131): the workload's shapes moved every seed's schedule but the one seed in eight that
// draws none. Of these pins, seeds 128, 8934 and 7949 draw none and did not move; 286, 368, 41, 643,
// 986, 463, 3291, 3870, 5303, 2097, 9 and 2272 moved. Each was found on a schedule with no shape on
// it, so each runs that one (`shard::Schedule::plain`, the schedule of the one seed in eight that
// draws no shape, and not the one the sweep runs for the twelve seeds that moved), byte for byte the
// trace it was pinned on, which D-131 measured by the traces' hashes. On the shaped schedule most of
// these pins fail, a shape having landed elsewhere in the run; seed 286's passes unchanged, its
// situation reached there as well. The rule's own case, read: what these pins were pinned for is
// kept, by running the schedule that reaches it. Seeds 707, 992, 5620, 16725 and 18925, at the end,
// are the first found on the shaped schedule, and run it.
//
// PROPOSED(D-127): the node's answers to the chunks it holds moved every schedule again, and every
// pin here was re-audited on D-127's tree, searching the plain and the shaped schedules' first ten
// thousand seeds with each pin's own conditions: seeds 463, 3291, 3870 and 5303, 2097, 9, 992 and
// 16725 moved to 1068, 9699, 9535, 329, 68, 1314 and 73, which carry their shapes; 368, 8934, 707,
// 5620, 18925 and 7949 are the absence with its reason; 2272 keeps its shape.
//
// PROPOSED(D-135): `Fault::MetaReorder` is drawn over the shaped schedule on every seed that draws
// a shape, so it moved those five and none of the plain schedule's. Re-read: seed 16725 still reaches
// its situation on the sweep's schedule and runs it unchanged. Seeds 707, 5620, 992 and 18925 no
// longer do, each for a reason its pin names, so each runs the schedule it was found on,
// `shard::Schedule::shaped`, the sweep's before the arm, and asserts its situation absent from the
// sweep's, so that the day the schedule reaches it again the test says so. Corrected after #240's
// review: the situation is asserted present on the shaped run first, so that a predicate that
// names something else, false on both schedules, fails there and does not pass as an absence.
//
// The Stage C integration (D-127, D-133, D-134, D-135 and D-137 together): D-127 audited the shaped
// schedule as the sweep drew it before the arm, so the pins it found there, 1314 and 73, run
// `shard::Schedule::shaped` by name, and so do D-137's 992 under `RefusedAfterRefill` and 1248,
// which still reach their shapes there. The sweep's schedule loses each of those four situations,
// so each pin asserts its situation absent from the sweep's (`absent_from_the_sweep`, D-135's rule,
// with its guard that the situation holds on the shaped run first). On the sweep's schedule seed
// 1314's restart arm's split is still refused `NoRangeId` on its node's new run, so its absence
// names the answer lost and the asker asking anew, not that refusal. Every other shape-drawn pin is
// an absence on both schedules: D-127's 707, 5620 and 18925; D-134's 16725 and D-137's 1032, each
// re-audited by a search of seeds 0 to 2 999 on both schedules and 3 000 to 9 999 on the sweep's
// that found its shape on no seed; and D-133's 13283, which the integration did not search, its
// shape having been found past the nightly's range, where D-132 counted the window on 2 of 60 226
// switches (13283 draws no shape, as one seed in eight does, so its two schedules are one, the
// plain one, and its two runs one run). 707, 16725, 18925 and 1032 assert it through
// `absent_from_both`, which has no run on this tree to assert its predicate true on first; 5620
// and 13283 compare the correct run with their variant's. Corrected at the integration's finish:
// this said that no pin was left whose situation the sweep's schedule alone lost and that
// `absent_from_the_sweep` went; four were, and it is back.

/// A shape-drawn pin's run on the schedule it was found on, [`shard::Schedule::shaped`]: the
/// sweep's before `Fault::MetaReorder` moved it.
// PROPOSED(D-135)
fn shaped_of(seed: u64, node: NodeVariants) -> shard::Report {
    shard::run_with(
        seed,
        shard::Schedule::shaped(seed),
        Variants::default(),
        node,
        RangeVariants::correct(),
    )
}

/// A shape-drawn pin's situation, absent from the sweep's schedule since `Fault::MetaReorder` moved
/// it: asserted absent, so that the day the schedule reaches it again this says so and the pin can
/// run the sweep's schedule again (CLAUDE.md). `pinned` is the pin's correct run on the schedule it
/// was found on, and `reaches` must hold there first: a predicate false on the very run the pin
/// asserts its mechanism on would assert no absence at all (#240's review, seed 18925).
// PROPOSED(D-135)
fn absent_from_the_sweep(
    pinned: &shard::Report,
    what: &str,
    reaches: impl Fn(&shard::Report) -> bool,
) {
    absent_from_the_sweep_of(pinned, &shard::run(pinned.seed), what, reaches);
}

/// [`absent_from_the_sweep`] for a pin whose run carries variants: `sweep` is the same seed and the
/// same variants on the sweep's schedule.
// PROPOSED(D-135): the Stage C integration, D-137's variant pin beside D-135.
fn absent_from_the_sweep_of(
    pinned: &shard::Report,
    sweep: &shard::Report,
    what: &str,
    reaches: impl Fn(&shard::Report) -> bool,
) {
    let seed = pinned.seed;
    assert_eq!(sweep.seed, seed, "the sweep's run is the pin's seed");
    assert!(
        reaches(pinned),
        "seed {seed} does not reach {what} on the schedule it was found on: the predicate names \
         something else, and its absence from the sweep's schedule would assert nothing"
    );
    assert!(
        sweep
            .schedule
            .faults
            .iter()
            .any(|f| matches!(f, shard::Fault::MetaReorder { .. })),
        "seed {seed} draws no reorder arm: the sweep's schedule is the one the pin was found on"
    );
    assert!(
        !reaches(sweep),
        "seed {seed} reaches {what} again on the sweep's schedule: run the pin there (CLAUDE.md)"
    );
}

/// A shape-drawn pin's situation, which PROPOSED D-127 moved its seed off: asserted absent from the
/// schedule it was found on, [`shard::Schedule::shaped`], where D-127 re-audited it, and from the
/// sweep's, where `Fault::MetaReorder` had already moved it (PROPOSED D-135), so that the day either
/// reaches it again this says so and the pin is upgraded back to its mechanism (CLAUDE.md). The
/// correct system passes both runs.
// PROPOSED(D-127)
// PROPOSED(D-135): the Stage C integration, D-127 beside D-135.
fn absent_from_both(seed: u64, what: &str, reaches: impl Fn(&shard::Report) -> bool) {
    for (schedule, report) in [
        (
            "the shaped schedule",
            shaped_of(seed, NodeVariants::correct()),
        ),
        ("the sweep's schedule", shard::run(seed)),
    ] {
        report.check().unwrap();
        assert!(
            !reaches(&report),
            "seed {seed} reaches {what} again on {schedule}: upgrade the pin back to its \
             mechanism (CLAUDE.md)"
        );
    }
}

/// Whether a split the restart arm asked was refused `NoRangeId` on its node's new run: the
/// situation seed 992 was pinned in.
// PROPOSED(D-135)
fn restart_arms_split_refused_no_range_id(report: &shard::Report) -> bool {
    report.no_range_ids().iter().any(|empty| {
        empty.restarted.is_some()
            && report
                .restart_arms
                .iter()
                .any(|arm| arm.victim == empty.node && arm.seq.is_some())
    })
}

/// Whether the restart arm's split, retried on its restarted node, took effect at that proposal, its
/// answer was dropped on the way to the asker, and the asker asked anew and was refused
/// `KeyOutsideSpan`: the situation seed 1314 carries (seed 992's until PROPOSED D-127). On the
/// sweep's schedule seed 1314's arm's split is still refused `NoRangeId` on its node's new run
/// ([`restart_arms_split_refused_no_range_id`]), so that predicate cannot name the absence there.
// PROPOSED(D-135): the Stage C integration, D-127 beside D-135.
fn restart_arms_answer_lost_and_asked_anew(report: &shard::Report) -> bool {
    let records = report.records();
    report.restart_arms.iter().any(|arm| {
        let (Some(range), Some(seq)) = (arm.range, arm.seq) else {
            return false;
        };
        let Some((proposed, index)) =
            records
                .iter()
                .enumerate()
                .find_map(|(at, record)| match record.event {
                    TraceEvent::RaftProposed {
                        server,
                        client,
                        seq: of,
                        index,
                        ..
                    } if server == arm.victim && client == 96 << 32 && of == seq => {
                        Some((at, index))
                    }
                    _ => None,
                })
        else {
            return false;
        };
        let admin = raft::admin_addr(seq);
        records.iter().any(|record| {
            matches!(&record.event,
                TraceEvent::RangeSplit { index: at, range: of, .. } if *at == index && *of == range)
        }) && records[proposed..].iter().any(|record| {
            matches!(&record.event,
                TraceEvent::MessageDropped { from, to, .. }
                    if *from == raft::server_addr(arm.victim) && *to == admin)
        }) && report.driver.retried.iter().any(|retried| {
            retried.range == range && retried.refusal == Some(SplitRefusal::KeyOutsideSpan)
        })
    })
}

/// How many asks the admin of split `seq` sent before `refused` and how many at or after it.
// PROPOSED(D-135)
fn asks_around(report: &shard::Report, seq: u64, refused: Instant) -> (usize, usize) {
    let admin = raft::admin_addr(seq);
    report
        .records()
        .iter()
        .filter(|record| {
            matches!(&record.event, TraceEvent::MessageSent { from, .. } if *from == admin)
        })
        .fold((0, 0), |(before, after), record| {
            if record.at < refused {
                (before + 1, after)
            } else {
                (before, after + 1)
            }
        })
}

/// Whether the restart arm's split's asker spent all its asks before its first `NoRangeId` and
/// retried after it: the situation seed 18925 was pinned in. Its first `NoRangeId` came from a node
/// that had not restarted, so [`restart_arms_split_refused_no_range_id`] is false on its shaped run
/// as on the sweep's (#240's review).
///
/// A [`shard::Retried`] names the range, not the asker, and a split's left half keeps the range's
/// id, so another asker's first refusal on the arm's range can be the one read. What ties it to the
/// arm's asker is the asker's ask after it: only its own first `NoRangeId` gives it a thirteenth.
/// One case is left: the arm's own first `NoRangeId` on an earlier ask, and another asker's
/// landing between the arm's twelfth and thirteenth. That reads as the situation reached where it
/// is not: on the sweep's schedule a false alarm, never an absence that passes, and on the shaped
/// run the pin asserts the one retried split and its counts itself (#240's second review).
// PROPOSED(D-135)
fn restart_arms_asker_spent_its_asks_before_its_first_no_range_id(report: &shard::Report) -> bool {
    let asks = usize::try_from(shard::BOOTSTRAP * 4).expect("small");
    report.restart_arms.iter().any(|arm| {
        let (Some(range), Some(seq)) = (arm.range, arm.seq) else {
            return false;
        };
        report
            .driver
            .retried
            .iter()
            .filter(|retried| retried.range == range)
            .any(|retried| {
                let (before, after) = asks_around(report, seq, retried.refused);
                before == asks && after >= 1
            })
    })
}

/// A pin's correct run, on the schedule it was found on ([`shard::Schedule::plain`]).
// PROPOSED(D-131)
fn pinned(seed: u64) -> shard::Report {
    pinned_of(
        seed,
        Variants::default(),
        NodeVariants::correct(),
        RangeVariants::correct(),
    )
}

/// A pin's run under the three sets, on the schedule it was found on.
// PROPOSED(D-131)
fn pinned_of(
    seed: u64,
    cores: impl Into<Variants>,
    node: NodeVariants,
    range: RangeVariants,
) -> shard::Report {
    shard::run_with(
        seed,
        shard::Schedule::plain(seed),
        cores.into(),
        node,
        range,
    )
}

/// [`pinned_of`], taken to its schedule's end with no check stopping it.
// PROPOSED(D-131)
fn pinned_to_the_end(
    seed: u64,
    cores: impl Into<Variants>,
    node: NodeVariants,
    range: RangeVariants,
) -> shard::Report {
    shard::run_to_the_end_with(
        seed,
        shard::Schedule::plain(seed),
        cores.into(),
        node,
        range,
    )
}

/// Seed 286, no store refused, so D-128 and D-129 leave it where it is: node 1 restarts, leads
/// a range the driver then asks it to split, and holds no id, since a new run holds none until
/// its refill lands (SHARD.md §5's second rule). The split is refused `NoRangeId`
/// (D-092's A); range 0 grants the block within a minimum election timeout; the asker's retry
/// gets its answer. What the block's size cannot change (D-113 §8).
#[test]
fn seed_286_a_new_runs_empty_block_refuses_a_split_until_its_refill_lands() {
    let report = pinned(286);
    report.check().unwrap();
    assert!(report.refusals().is_empty(), "no store refused");
    let empties = report.no_range_ids();
    assert_eq!(empties.len(), 1, "one `NoRangeId`: {empties:?}");
    let empty = empties[0];
    assert_eq!(empty.node, 1);
    assert!(empty.restarted.is_some(), "its node restarted before it");
    assert!(!empty.leased_since_restart, "no block granted since then");
    assert!(
        empty.leased_within,
        "the refill landed within a minimum election timeout"
    );
    assert_eq!(empty.taken, 0, "and no id had been taken from anything");
    assert!(
        report.driver.retried.iter().all(|retried| retried.answered),
        "the asker's retry got its answer: {:?}",
        report.driver.retried
    );
}

/// Seed 368, no store refused and uniformly scheduled, so the asker's retry is asked of too:
/// the other way a split finds an empty block. Range 0 has already granted node 2's block,
/// traced before the refusal, and the answer has not reached the node, so the node still holds
/// no id. Trace says a block was granted; the node says `NoRangeId`; neither is wrong
/// (D-113 §8).
///
/// **PROPOSED D-127 moved the seed off the shape**: with the node answering the chunks it holds,
/// seed 368's run answers no split `NoRangeId` at all, and no plain schedule of the first ten
/// thousand that the correct system passes on D-127's tree answers one in this pin's shape, every
/// condition asserted here holding at once. The pin is the absence with its reason: no `NoRangeId` on this seed came after
/// a block was granted to its node, so the day one does, this says so and the pin is upgraded back
/// to its mechanism. Seed 286's pin keeps D-113 §8's other way to an empty block.
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
#[test]
fn seed_368_a_grant_whose_answer_has_not_landed_still_refuses_a_split() {
    let report = pinned(368);
    report.check().unwrap();
    let empties = report.no_range_ids();
    assert!(
        empties.iter().all(|empty| !empty.leased_since_restart),
        "seed 368 answers a `NoRangeId` after a block was granted to its node again: {empties:?}; \
         upgrade the pin back to its mechanism (PROPOSED D-113 §8)"
    );
}

/// Seed 41, the first seed the sweep failed on (D-113, (a)): node 2 is refused, its placeholder
/// of the right half 19 is installed, and then its re-seeded range 2 is, whose switch replaced
/// configuration's `[k, l)` and took 19's keys with it, so node 2 later led 19's right half and
/// answered `k15` with nothing. Since #200 the switch replaces the snapshot's span and the run
/// is green; under `InstallReplacesHostedSpan` check 8's install clause names that switch
/// (D-128). Since D-130 a right half made on a quarantined parent is quarantined, which
/// moved this seed's schedule: a second store, node 1's, is refused after node 2's, and node 2's
/// is the first and the one this pins.
#[test]
fn seed_41_an_install_onto_a_reseeded_replica_keeps_the_right_half_beside_it() {
    let report = pinned(41);
    report.check().unwrap();
    let refused = report.refusals();
    assert_eq!(refused.len(), 2, "two stores refused: {refused:?}");
    let node = refused[0].0;
    assert_eq!(node, 2, "node 2's is the first: {refused:?}");
    let created = |of: u64| {
        report.records().iter().position(|record| {
            record.node.map(|at| u64::from(at.get())) == Some(node)
                && matches!(&record.event, TraceEvent::RangeCreated {
                    range,
                    cause: ananke_env::RangeCause::Snapshot,
                    ..
                } if *range == of)
        })
    };
    let (right, parent) = (created(19), created(ananke_sim::ranges::FIRST_RANGE));
    assert!(
        right.is_some() && right < parent,
        "the right half's install comes first: {right:?}, {parent:?}"
    );
    let why = pinned_to_the_end(
        41,
        Variants::default(),
        NodeVariants::of(&[NodeVariant::InstallReplacesHostedSpan]),
        RangeVariants::correct(),
    )
    .check()
    .expect_err("the variant fails seed 41");
    assert!(
        why.contains("one owner per key") && why.contains("overlaps its replica of range 19"),
        "the violation seed 41 is pinned for: {why}"
    );
}

/// Seeds 128, 643 and 986, where the sweep found D-113's (b): a leader whose followers were
/// both quarantined, reads no round could confirm, and each client's retry a registration, until
/// `READS_OUTSTANDING`. #200 moved 128 and 643 off it: with no right half's keys taken, each has
/// one store refused where it had two, and D-130 left that as it was. D-130 moved 986 off it: with
/// a right half made on a quarantined parent now quarantined, it has one store refused where it
/// had two. So no leader on any of the three is left unable to confirm a read, and
/// `UnconfirmableReadKept` has nothing to keep. The situation is absent, asserted, with those
/// reasons; seed 463 carried it until PROPOSED D-127, and seed 1068 carries it now.
///
/// PROPOSED D-127 moved all three again: with the node answering the chunks it holds, none of
/// them has a store refused at all, where each had one, so there is no quarantine for a leader
/// to be left behind by.
// PROPOSED(D-127): no store refused on any of the three.
#[test]
fn seeds_128_643_and_986_no_longer_reach_a_leader_that_cannot_confirm_a_read() {
    for seed in [128, 643, 986] {
        let report = pinned(seed);
        report.check().unwrap();
        assert_eq!(
            report.refusals().len(),
            0,
            "seed {seed}: {:?}",
            report.refusals()
        );
        assert!(
            !report.records().iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftReadRefused { .. }
                    | TraceEvent::RaftReadsConfirmable {
                        confirmable: false,
                        ..
                    }
            )),
            "seed {seed} reaches a read no round can confirm again: pin it on its mechanism"
        );
        pinned_of(
            seed,
            Variant::UnconfirmableReadKept,
            NodeVariants::correct(),
            RangeVariants::correct(),
        )
        .check()
        .unwrap_or_else(|why| panic!("seed {seed} has a read to keep again: {why}"));
    }
}

/// Seed 1068, D-113's (b) where it stands since PROPOSED D-127 moved seed 463 off it, as D-130
/// moved seed 986 and #200 seeds 128 and 643: two stores refused and a leader left unable to
/// confirm a read, which it refuses at the step and drops (D-129). Under
/// `UnconfirmableReadKept` nothing answers them, the clients' retries pile up and the node fails
/// itself at `READS_OUTSTANDING`. Seed 1068 is the lowest of the plain schedules with two stores
/// refused, a read refused and the variant caught there on D-127's tree.
// PROPOSED(D-127): re-pinned from seed 463, whose schedule D-127 moved.
#[test]
fn seed_1068_a_leader_refuses_the_reads_it_can_never_confirm() {
    let seed = 1068;
    let report = pinned(seed);
    report.check().unwrap();
    assert_eq!(report.refusals().len(), 2, "two stores refused");
    let refused = report
        .records()
        .iter()
        .filter(|record| matches!(record.event, TraceEvent::RaftReadRefused { .. }))
        .count();
    assert!(refused > 0, "no read was refused");
    assert!(
        report.records().iter().any(|record| matches!(
            record.event,
            TraceEvent::RaftReadsConfirmable {
                confirmable: false,
                ..
            }
        )),
        "no leader was traced unable to confirm a read"
    );
    let why = pinned_of(
        seed,
        Variant::UnconfirmableReadKept,
        NodeVariants::correct(),
        RangeVariants::correct(),
    )
    .check()
    .expect_err("the variant fails the seed");
    assert!(
        why.contains("registered reads"),
        "the violation seed {seed} is pinned for: {why}"
    );
}

/// The node that traced `record`, as the checks number it.
fn node_of(record: &TraceRecord) -> Option<u64> {
    record.node.map(|node| u64::from(node.get()))
}

/// The window seed 3291 showed, the sweep's first failure of check 20 on the correct system, at
/// the nightly's ten thousand, and seed 88 the payload oracle (D-113 §5): a node commits
/// a split, crashes with the apply's batch durable and neither `RangeSplit` nor `RangeCreated`
/// traced, and at its restart restates a replica of the right half before any node has traced
/// that split. It then grants for it, from a core and not from a placeholder, and no node ever
/// hears of a creation on it. Check 20 read only `RangeCreated`, and named the grant a node's
/// that holds no replica; with the trace's `RangesRestated` taken out, the fold says so again.
///
/// On seed 3291 it was node 3, range 27's split at 122 and right half 30. PROPOSED D-127's
/// answers to the chunks a node holds moved that schedule off the window, and seed 9699 is the
/// one of the plain schedules' first ten thousand to carry it on D-127's tree: node 3 commits
/// range 2's split at 84, crashes before tracing it, restates its replica of right half 51 at
/// its restart before any node has traced the split, and grants 51's pre-vote. The fold without
/// `RangesRestated` flags two other seeds there, 1006 and 7319, and on both a node traced the
/// split before the restatement.
// PROPOSED(D-127): re-pinned from seed 3291, whose schedule D-127 moved.
#[test]
fn seed_9699_a_replica_restated_before_any_node_traced_its_split_may_grant() {
    let (seed, node, parent, right, split) = (9699, 3, 2, 51, 84);
    let report = pinned(seed);
    report.check().unwrap();
    let records = report.records();
    let on_node = |record: &TraceRecord| node_of(record) == Some(node);
    let after = |from: usize, what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records[from..]
            .iter()
            .position(want)
            .map(|at| from + at)
            .unwrap_or_else(|| panic!("seed {seed} no longer reaches {what}: read its trace"))
    };
    let at = |what: &str, want: &dyn Fn(&TraceRecord) -> bool| after(0, what, want);
    let committed = at("the node's commit of the split", &|record| {
        on_node(record)
            && matches!(record.event, TraceEvent::RaftCommit { range, index, .. }
                if range == parent && index == split)
    });
    let crashed = after(committed, "the node's crash", &|record| {
        matches!(record.event, TraceEvent::NodeCrashed { node: crashed }
            if u64::from(crashed.get()) == node)
    });
    let recovered = at("the restatement of the right half", &|record| {
        matches!(record.event, TraceEvent::RaftRecovered { server, range, applied, .. }
            if server == node && range == right && applied == split)
    });
    let restated = at("`RangesRestated` naming the right half", &|record| {
        on_node(record)
            && matches!(&record.event, TraceEvent::RangesRestated { ranges, .. }
                if ranges.contains(&right))
    });
    let first_traced = at("the split's first trace, on any node", &|record| {
        matches!(record.event, TraceEvent::RangeSplit { right: traced, .. }
            | TraceEvent::RangeCreated { range: traced, .. } if traced == right)
    });
    let granted = at("the node's grant", &|record| {
        on_node(record)
            && matches!(record.event, TraceEvent::RaftVote { range, granted: true, .. }
                if range == right)
    });
    assert!(
        committed < crashed
            && crashed < recovered
            && recovered < restated
            && restated < first_traced
            && first_traced < granted,
        "the window's order: commit {committed}, crash {crashed}, restated replica {recovered}, \
         `RangesRestated` {restated}, the split's first trace {first_traced}, grant {granted}"
    );
    assert!(
        !records[..granted].iter().any(|record| on_node(record)
            && matches!(record.event,
                TraceEvent::RangeSplit { right: range, .. }
                | TraceEvent::RangeCreated { range, .. }
                | TraceEvent::RangeReplicaCreated { range }
                    if range == right)),
        "node {node} traced the split, a creation or a placeholder of range {right} before the \
         grant: the window is gone from this seed"
    );
    let without: Vec<TraceRecord> = records
        .iter()
        .filter(|record| !matches!(record.event, TraceEvent::RangesRestated { .. }))
        .cloned()
        .collect();
    let old_reading = invariants::folded(&without)
        .verdict_of(Fold::PlaceholderGrants)
        .expect_err("the restatement is what check 20 holds this grant by");
    assert!(
        old_reading.contains(&format!("node {node} granted range {right}'s pre-vote"))
            && old_reading.contains("from a node that holds no replica of it"),
        "the violation the old reading made of it: {old_reading}"
    );
}

/// Seed 2147 on the plain schedule, which PROPOSED D-127's re-audit found failing the correct
/// system on its tree and passing on c5cd18d: "pre-vote: server 3 raised its term from 4 to 5
/// while isolated from Instant(6.088s) to Instant(6.441s)". A defect of the check. Node 3 led
/// range 30 at term 4, crashed at 5.336 s over a rotted log block and was refused at 5.487 s,
/// which left range 30 a placeholder with no core. Its `snapshot` task made the installs of
/// ranges 1, 28, 19, 2 and 29 first, and range 30's, of a term-5 stream whose chunks reached the
/// node before it was cut off at 6.088 s, was decided at 6.389 s, inside the isolation, and
/// traced at 6.449 s, after it. The decided reading took the window's start from the term-4
/// replica the refusal had taken away and its end from the install, while the skip and the
/// creation clause read the creation's trace time, past the window. A replica created inside
/// the window now begins it at the term its creation wrote (`created_term_by` in `sim/raft.rs`; its
/// unit test plants the reading before). This pin asserts the shape and that the run passes.
// PROPOSED(D-127): a creation inside the window, by the reading's clock, starts it.
#[test]
fn seed_2147_an_install_decided_inside_an_isolation_begins_it_at_the_term_it_wrote() {
    let (seed, node, range) = (2147, 3, 30);
    let report = pinned(seed);
    report.check().unwrap();
    let checked = &report.checked;
    let records = report.records();
    let on_node = |record: &TraceRecord| node_of(record) == Some(node);
    let (from, until, created) = checked
        .isolations
        .iter()
        .filter(|(server, ..)| *server == node)
        .find_map(|&(_, from, until)| {
            records
                .iter()
                .find(|record| {
                    on_node(record)
                        && matches!(record.event, TraceEvent::RangeCreated {
                            range: made, cause: RangeCause::Snapshot, ..
                        } if made == range)
                        && from < record.decided
                        && record.decided <= until
                        && record.at > until
                })
                .map(|created| (from, until, created))
        })
        .unwrap_or_else(|| {
            panic!(
                "seed {seed} no longer decides an install of range {range} on node {node} inside \
                 an isolation and traces it after: read its trace"
            )
        });
    let term_of = |record: &TraceRecord| match record.event {
        TraceEvent::RaftTerm {
            server,
            range: of,
            term,
            ..
        } if server == node && of == range => Some(term),
        _ => None,
    };
    let led = records
        .iter()
        .filter(|record| record.decided <= from)
        .filter_map(|record| term_of(record).map(|term| (record.at, term)))
        .last();
    let refused = records.iter().find(|record| {
        matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node)
            && record.at <= from
            && led.is_some_and(|(at, _)| at < record.at)
    });
    let restated = records
        .iter()
        .filter(|record| record.at > created.at)
        .find_map(term_of);
    let stream = checked
        .install_stream_terms()
        .get(&(node, range, created.at))
        .copied();
    assert_eq!(
        (
            led.map(|(_, term)| term),
            refused.is_some(),
            stream,
            restated
        ),
        (Some(4), true, Some(5), Some(5)),
        "the shape: node {node}'s replica of range {range} at term 4 before the window, its \
         refusal after that and before the window, and the install's stream and restatement at \
         term 5"
    );
    assert_eq!(
        checked.isolation_keeps_its_term_by(raft::RecordTime::Decided, node, from, until),
        Ok(())
    );
}

/// Seed 6773 on the plain schedule, the other seed PROPOSED D-127's re-audit found failing the
/// correct system on its tree and passing on c5cd18d: "liveness: no client write to k08 completed
/// after the last heal at Instant(4.152s)". The per-key reading counts as a wedge a write no
/// system can complete. The only post-heal write to `k08` in the history, a put called at
/// 4.239 s, reached node 3, which still led range 30, `[k08, k11)`, at term 3 and proposed it at
/// index 105 at 4.245 s; node 1 had won term 4 at 4.243 s, and node 3 truncated 105 at 4.247 s.
/// A write gets one try (`WRITE_TIMEOUT`, D-131's `ask_once`), so its client gave up at 4.299 s
/// and its call stays open; the run ends 2.113 s after that call, past the 2 s bound, which takes
/// away the exemption for a pending write on a range shown live. The range was live: under node
/// 1 it completed writes to `k09` and `k10` after that call and served `k08`'s reads to the end.
/// The client's next write to `k08`, at 5.581 s, was sent to node 4, which holds no replica of
/// range 30, and went unanswered under one try, so no leader proposed it and it left no
/// operation.
///
/// Whether a write the trace shows truncated before any apply is evidence for its key is a
/// design question the documents do not settle, asked of the owner in D-127 (Q1, #250); this
/// pin holds the shape and the verdict until then, so the day either moves it says so.
///
/// `Report::check` stops at its first violation, so while this pin stands the checks after the
/// write bound are asked of the run here directly (D-127's second review, F3): the timer check
/// on every live range, the shape, the meta convergence bound, D-092's retries and #45's kinds;
/// and, since the Stage C integration, check 8's right half's clause under reading R, which
/// the sweep's verdict asks after `check` (PROPOSED D-141).
// PROPOSED(D-127): an open question for the owner, the shape held until the ruling.
#[test]
fn seed_6773_a_keys_only_post_heal_write_truncated_by_an_election_is_read_as_a_wedge() {
    let (seed, leader, range, index, term) = (6773, 3, 30, 105, 3);
    let report = pinned(seed);
    let checked = &report.checked;
    let k08 = shard::key(8);
    let after_heal: Vec<_> = checked
        .history
        .ops
        .iter()
        .filter(|op| op.op.is_write() && op.call >= checked.last_heal)
        .collect();
    let to_k08: Vec<_> = after_heal.iter().filter(|op| *op.op.key() == k08).collect();
    let [write] = to_k08.as_slice() else {
        panic!("seed {seed} no longer has one post-heal write to k08: {to_k08:?}");
    };
    assert_eq!(write.ret, None, "the write never completed");
    let records = report.records();
    let proposed = records
        .iter()
        .position(|record| {
            record.event
                == TraceEvent::RaftProposed {
                    server: leader,
                    range,
                    client: write.process,
                    seq: write.seq,
                    index,
                    term,
                }
        })
        .unwrap_or_else(|| panic!("seed {seed} no longer proposes the write at {index}"));
    assert!(
        records[proposed..]
            .iter()
            .any(|record| matches!(record.event,
            TraceEvent::RaftTruncate { server, range: of, from_index }
                if server == leader && of == range && from_index <= index)),
        "node {leader} truncates the write's entry"
    );
    assert!(
        !records.iter().any(|record| matches!(record.event,
            TraceEvent::RaftApply { range: of, index: at, entry_term, .. }
                if of == range && at == index && entry_term == term)),
        "no node applies the write's entry"
    );
    let served = after_heal.iter().any(|op| {
        [shard::key(9), shard::key(10)].contains(op.op.key())
            && op.call > write.call
            && op.ret.is_some()
    });
    assert!(
        served,
        "range {range} completes other writes after the call"
    );
    assert!(
        checked.ended.duration_since(write.call) > raft::election_max() * raft::LIVENESS_TIMEOUTS,
        "the run ends past the bound of the call"
    );
    let why = report
        .check()
        .expect_err("the per-key reading reads it as a wedge");
    assert!(
        why.contains("liveness: no client write to k08 completed after the last heal"),
        "the verdict this seed is pinned for: {why}"
    );
    // The checks `check` makes after the write bound, which its first violation hides.
    let live = checked.ranges_with_a_majority_up();
    let gaps: Vec<_> = checked
        .timer_gaps(raft::TimerResets::ALL)
        .into_iter()
        .filter(|gap| live.contains(&gap.range))
        .map(|gap| gap.violation())
        .collect();
    assert_eq!(
        gaps,
        Vec::<String>::new(),
        "the timer check on every live range"
    );
    assert_eq!(report.shape(), Ok(()), "the shape");
    assert_eq!(checked.stopped, None, "the run reached its schedule's end");
    assert_eq!(
        report.meta_converges(),
        Ok(()),
        "the meta convergence bound"
    );
    assert_eq!(report.retries_take_effect(), Ok(()), "D-092's retries");
    assert_eq!(report.losses_are_named(), Ok(()), "#45's kinds");
    // PROPOSED(D-141): the Stage C integration, D-127 beside D-141.
    assert_eq!(
        checked.right_half_held(),
        Ok(()),
        "check 8's right half's clause under reading R, which the sweep asks after `check`"
    );
}

/// Seed 8934, the sweep's first failure of the match-starts fold on the correct system, at the
/// nightly's ten thousand. Node 2's placeholder of range 43 answers leader 1 with incarnation 0,
/// node 2 applies the split that makes the replica, and the new store's first answer overtakes
/// the placeholder's. So the leader traces a first rise under the new store's number, a reset to
/// 0 at the placeholder's answer, a reset back at the store's next and a first rise again. D-042
/// names the 0 delayed past a store's first answer as a change that costs a round trip. The
/// fold said a retired incarnation had come back; the fold never read a reset, so with the
/// trace's `RaftProgressReset`s taken out it says so again (D-113 §5).
///
/// **PROPOSED D-127 moved the seed off the shape**: with the node answering the chunks it holds,
/// seed 8934's leader traces no first rise that only a reset excuses, and the fold without the
/// trace's resets passes it, as it passes every plain schedule of the first ten thousand that
/// the correct system passes on D-127's tree. The pin is the absence with its reason, and the day
/// the shape is back this says so.
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
#[test]
fn seed_8934_a_placeholders_answer_overtaken_by_the_new_stores_begins_the_tracking_afresh() {
    let report = pinned(8934);
    report.check().unwrap();
    let records = report.records();
    let without: Vec<TraceRecord> = records
        .iter()
        .filter(|record| !matches!(record.event, TraceEvent::RaftProgressReset { .. }))
        .cloned()
        .collect();
    assert_eq!(
        raft::match_starts_are_first_rises(&without),
        Ok(()),
        "seed 8934's leader traces a second first rise that only a reset excuses again: the \
         shape may be back, so upgrade the pin back to its mechanism (PROPOSED D-113 §5)"
    );
}

/// Seeds 3870 and 5303, #175's two, on which the correct system lost committed entries at the
/// nightly's ten thousand until D-130. Node 3 applied the split and voted in the right half; it
/// is refused, its parent is re-seeded from a checkpoint below the split, and it replays the
/// split and makes the right half a second time, knowing nothing of the votes the first one
/// cast. That replica takes the parent's quarantine: it is stated quarantined at its creation,
/// grants no vote, leads nothing, and the entries committed on node 3 before its loss are on
/// the next leader and applied at their own term. Under `SplitLeavesQuarantineBehind` it makes
/// the right half with no quarantine, and votes. On 3870 it leads term 4 from last index 106
/// without the 107 committed before its loss: leader completeness, which since PROPOSED D-131
/// reads the store the re-made replica holds, where it had read the store it lost, and so caught
/// this no later than state machine safety did at the apply of a 107 of its own. On 5303 it grants
/// node 1 term 5 with last index 88 against a committed 89: leader completeness. Two seeds of ten
/// thousand are far below D-061's 5 %, so the pair lives on these pins, at every tier.
///
/// **PROPOSED D-127 moved both seeds off the shape**, its answers to the chunks a node holds moving
/// every node schedule: under `SplitLeavesQuarantineBehind` neither fails on leader completeness
/// now, which is asserted, with the reason. Of the plain schedules' first ten thousand on D-127's
/// tree one seed carries it, 9535, the shape of 5303: node 2 applied range 2's split at 72 and voted
/// in right half 19, committed it to 90, is refused, is re-seeded below the split at 70, replays the
/// split and makes 19 a second time, stated quarantined. Under the variant it grants node 1 term 3
/// without the 74 committed before its loss: leader completeness. The pair lives on this pin.
// D-130
// PROPOSED(D-127): re-pinned from seeds 3870 and 5303, whose schedules D-127 moved.
#[test]
fn seed_9535_a_right_half_remade_on_a_quarantined_parent_keeps_its_committed_entries() {
    type Row<'a> = (
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        std::ops::RangeInclusive<u64>,
        std::ops::RangeInclusive<u64>,
        &'a str,
    );
    let pins: [Row<'_>; 1] = [(
        9535,
        2,
        2,
        19,
        72,
        70,
        74..=90,
        86..=90,
        "leader completeness: index 74 of group 19 (term 2) was committed in term 2 but server 1, \
         leader of term 3, does not hold it",
    )];
    for (seed, node, parent, right, split, below, committed, reapplied, named) in pins {
        let report = pinned(seed);
        report.check().unwrap();
        let records = report.records();
        let on_node = |record: &TraceRecord| node_of(record) == Some(node);
        let at = |from: usize, what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
            records[from..]
                .iter()
                .position(want)
                .map(|found| from + found)
                .unwrap_or_else(|| panic!("seed {seed} no longer reaches {what}: read its trace"))
        };
        // The loss, and what it lost: the node had made the right half and voted in it.
        let refused = at(
            0,
            "the node's refusal",
            &|record| matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node),
        );
        let first_made = at(
            0,
            "the node's first creation of the right half",
            &|record| {
                on_node(record)
                    && matches!(record.event, TraceEvent::RangeCreated {
                    range,
                    cause: RangeCause::Split,
                    ..
                } if range == right)
            },
        );
        assert!(first_made < refused, "seed {seed}: made before its loss");
        assert!(
            records[first_made..refused].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftVote { server, range, granted: true, .. }
                    if server == node && range == right
            )),
            "seed {seed}: node {node}'s lost replica granted no vote: nothing was lost"
        );
        // The re-seed below the split, the replay, and the second creation, stated quarantined.
        let installed = at(
            refused,
            "the node's parent installed below the split",
            &|record| {
                matches!(record.event, TraceEvent::RaftSnapshot {
                server,
                range,
                last_index,
                taken: false,
                ..
            } if server == node && range == parent && last_index == below && below < split)
            },
        );
        let replayed = at(installed, "the node's replay of the split", &|record| {
            on_node(record)
                && matches!(record.event, TraceEvent::RangeSplit {
                    range,
                    right: of,
                    index,
                    ..
                } if range == parent && of == right && index == split)
        });
        let remade = at(replayed, "the node's second creation", &|record| {
            on_node(record)
                && matches!(record.event, TraceEvent::RangeCreated {
                    range,
                    cause: RangeCause::Split,
                    floor_index,
                    ..
                } if range == right && floor_index == split)
        });
        let marked = at(remade, "the replica's quarantine", &|record| {
            matches!(record.event, TraceEvent::RaftReseeded { server, range }
                if server == node && range == right)
        });
        let stated = at(remade, "the replica's restatement", &|record| {
            matches!(record.event, TraceEvent::RaftRecovered {
                server,
                range,
                applied,
                state: RecoveredAs::Quarantined,
                ..
            } if server == node && range == right && applied == split)
        });
        assert!(
            remade < marked && marked < stated,
            "seed {seed}: the creation, the mark and the statement in order"
        );
        // It grants no vote and leads nothing, for the rest of its life on that store.
        assert!(
            !records[remade..].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftVote { server, range, granted: true, .. }
                    | TraceEvent::RaftLeader { server, range, .. }
                    if server == node && range == right
            )),
            "seed {seed}: the quarantined replica granted a vote or led"
        );
        // The committed entries survive: the node had them committed before its loss, the first
        // leader elected after the second creation holds them, no replica applies one at
        // another term, and the third node applies the rest at their own.
        let highest = records[..refused]
            .iter()
            .filter_map(|record| match record.event {
                TraceEvent::RaftCommit {
                    server,
                    range,
                    index,
                    ..
                } if server == node && range == right => Some(index),
                _ => None,
            })
            .max();
        assert_eq!(
            highest,
            Some(*committed.end()),
            "seed {seed}: what node {node} had committed before its loss"
        );
        let next = at(
            remade,
            "the next leader of the right half",
            &|record| matches!(record.event, TraceEvent::RaftLeader { range, .. } if range == right),
        );
        assert!(
            matches!(records[next].event, TraceEvent::RaftLeader { server, last_index, .. }
                if server != node && last_index >= *committed.end()),
            "seed {seed}: the next leader does not hold them: {:?}",
            records[next].event
        );
        for record in records {
            if let TraceEvent::RaftApply {
                range,
                index,
                entry_term,
                ..
            } = record.event
                && range == right
                && committed.contains(&index)
            {
                assert_eq!(entry_term, 2, "seed {seed}: index {index} changed its term");
            }
        }
        for index in reapplied {
            assert!(
                records[remade..].iter().any(|record| matches!(
                    record.event,
                    TraceEvent::RaftApply {
                        server,
                        range,
                        index: of,
                        entry_term: 2,
                        ..
                    } if server != node && range == right && of == index
                )),
                "seed {seed}: no other node applied index {index} after the second creation"
            );
        }

        // The buggy half: the seed's named failure, and the vote that made it.
        let variant = pinned_of(
            seed,
            Variants::default(),
            NodeVariants::of(&[NodeVariant::SplitLeavesQuarantineBehind]),
            RangeVariants::correct(),
        );
        let why = variant
            .check()
            .expect_err("the variant fails the seed #175 was found on");
        assert!(
            why.contains(named),
            "seed {seed}: the violation the seed is pinned for: {why}"
        );
        let records = variant.records();
        let remade = records
            .iter()
            .rposition(|record| {
                node_of(record) == Some(node)
                    && matches!(record.event, TraceEvent::RangeCreated {
                        range,
                        cause: RangeCause::Split,
                        ..
                    } if range == right)
            })
            .expect("the variant replays the split");
        assert!(
            records[remade..].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftVote { server, range, granted: true, .. }
                    if server == node && range == right
            )),
            "seed {seed}: the replica with no quarantine granted no vote under the variant"
        );
        assert!(
            !records[remade..].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftRecovered { server, range, state: RecoveredAs::Quarantined, .. }
                    if server == node && range == right
            )),
            "seed {seed}: the variant quarantined the replica after all"
        );
    }
    // Seeds 3870 and 5303 carried the shape until PROPOSED D-127 moved their schedules: under
    // the variant neither fails on leader completeness now.
    // PROPOSED(D-127)
    for seed in [3870, 5303] {
        let variant = pinned_of(
            seed,
            Variants::default(),
            NodeVariants::of(&[NodeVariant::SplitLeavesQuarantineBehind]),
            RangeVariants::correct(),
        );
        if let Err(why) = variant.check() {
            assert!(
                !why.contains("leader completeness"),
                "seed {seed} fails on leader completeness under the variant again: its shape may \
                 be back, so re-audit the pin: {why}"
            );
        }
    }
}

/// Seed 329: a quarantine a right half took at its split's apply is durable and a restart
/// restates it. Node 1 is refused and re-seeded, applies a split of range 28 on its quarantined
/// replica of it, and range 51 takes the quarantine in the split's batch and states it. Node 1
/// then crashes and restarts on the same store, which says quarantined, and the restart says
/// so. Under `SplitLeavesQuarantineBehind` the same restart states range 51 with no quarantine,
/// and the replica has already voted (D-130).
///
/// **Pinned on seed 2097 until PROPOSED D-127**, where node 2's range 27 took the quarantine at
/// range 2's split at 143: the node's answers to the chunks it holds moved that schedule off the
/// shape, and seed 329 is the lowest of the plain schedules to carry it whole on this tree, the
/// variant's half included (seed 40 carries the correct half, and under the variant its replica
/// never votes).
// D-130
// PROPOSED(D-127): re-pinned from seed 2097, whose schedule D-127 moved.
#[test]
fn seed_329_a_restart_restates_the_quarantine_a_right_half_took_at_its_split() {
    let (seed, node, parent, right, split) = (329, 1, 28, 51, 151);
    let correct = pinned(seed);
    correct.check().unwrap();
    let records = correct.records();
    assert_eq!(correct.refusals().len(), 1, "one store refused");
    assert_eq!(correct.refusals()[0].0, node, "node {node}'s");
    let at = |from: usize, what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records[from..]
            .iter()
            .position(want)
            .map(|found| from + found)
            .unwrap_or_else(|| panic!("seed {seed} no longer reaches {what}: read its trace"))
    };
    let refused = at(
        0,
        "the node's refusal",
        &|record| matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node),
    );
    let made = at(refused, "the split's apply on the node", &|record| {
        node_of(record) == Some(node)
            && matches!(record.event, TraceEvent::RangeCreated {
                range,
                cause: RangeCause::Split,
                parent: Some(of),
                floor_index,
                ..
            } if range == right && of == parent && floor_index == split)
    });
    let stated = at(made, "the quarantine stated at the creation", &|record| {
        matches!(record.event, TraceEvent::RaftRecovered {
            server,
            range,
            applied,
            state: RecoveredAs::Quarantined,
            ..
        } if server == node && range == right && applied == split)
    });
    let crashed = at(stated, "the node's crash", &|record| {
        matches!(record.event, TraceEvent::NodeCrashed { node: gone }
            if u64::from(gone.get()) == node)
    });
    let restarted = at(crashed, "the node's restart", &|record| {
        matches!(record.event, TraceEvent::NodeRestarted { node: back }
            if u64::from(back.get()) == node)
    });
    let marked = at(restarted, "the restart's mark", &|record| {
        matches!(record.event, TraceEvent::RaftReseeded { server, range }
            if server == node && range == right)
    });
    let restated = at(restarted, "the restart's statement", &|record| {
        matches!(record.event, TraceEvent::RaftRecovered { server, range, .. }
            if server == node && range == right)
    });
    assert!(marked < restated, "the mark, then the statement");
    assert!(
        matches!(
            records[restated].event,
            TraceEvent::RaftRecovered {
                state: RecoveredAs::Quarantined,
                ..
            }
        ),
        "the restart states the replica as: {:?}",
        records[restated].event
    );
    assert!(
        !records[made..].iter().any(|record| matches!(
            record.event,
            TraceEvent::RaftVote { server, range, granted: true, .. }
                if server == node && range == right
        )),
        "the quarantined replica granted a vote"
    );

    let variant = pinned_of(
        seed,
        Variants::default(),
        NodeVariants::of(&[NodeVariant::SplitLeavesQuarantineBehind]),
        RangeVariants::correct(),
    );
    variant
        .check()
        .expect("the variant's named failure is 9535's");
    let on_variant = variant.records();
    let granted = on_variant.iter().any(|record| {
        matches!(record.event, TraceEvent::RaftVote { server, range, granted: true, .. }
            if server == node && range == right)
    });
    let states: Vec<RecoveredAs> = on_variant
        .iter()
        .filter_map(|record| match record.event {
            TraceEvent::RaftRecovered {
                server,
                range,
                state,
                ..
            } if server == node && range == right => Some(state),
            _ => None,
        })
        .collect();
    assert!(granted, "under the variant the replica votes");
    assert_eq!(
        states,
        [RecoveredAs::Neither],
        "under the variant the restart states no quarantine"
    );
}

/// Seed 68: a right half made on a quarantined right half takes the quarantine too, so a chain of
/// splits carries it down. Node 1 is refused and the re-seed quarantines its replica of range 2.
/// It applies range 2's split at 54 and range 27 takes the quarantine. A live install on range 27
/// builds no new store and leaves it quarantined. It applies range 27's split at 86 and range 28,
/// the second generation, takes the quarantine from a parent a split made; and it applies range
/// 28's at 96 and range 3 takes it from a parent that took no install. Each is stated quarantined
/// at its creation and, asked for a vote, grants none. Only this pin fails if a split-made parent
/// stops passing the quarantine on: the store's test shows a right half reopens quarantined, the
/// other pins that a quarantined parent passes it on, and the sweep's checks pass either way.
/// Under `SplitLeavesQuarantineBehind` the first link's replica has no quarantine, and votes,
/// and no right half the node makes by a split after its refusal is quarantined (D-130).
///
/// **Pinned on seed 9 until PROPOSED D-127**, node 2's chain 2, 3, 4, 5 at 57, 76 and 90: the
/// node's answers to the chunks it holds moved that schedule off the chain, and seed 68 is the
/// lowest of the plain schedules to carry it whole on this tree, the live install on the first
/// link and none on the second. Under the variant seed 9 made the same three right halves; no
/// seed of the plain schedules searched on D-127's tree does, since the first link's vote moves
/// the variant's run, so the variant's half asserts the first link and every right half after.
// D-130
// PROPOSED(D-127): re-pinned from seed 9, whose schedule D-127 moved.
#[test]
fn seed_68_a_right_half_made_on_a_quarantined_right_half_takes_the_quarantine_too() {
    let (seed, node) = (68, 1);
    // The links, in order: the parent on the node, the right half it makes and the split's index.
    let chain = [(2, 27, 54), (27, 28, 86), (28, 3, 96)];
    let made_on = |record: &TraceRecord, parent: u64, right: u64, split: u64| {
        node_of(record) == Some(node)
            && matches!(record.event, TraceEvent::RangeCreated {
                range,
                cause: RangeCause::Split,
                parent: Some(of),
                floor_index,
                ..
            } if range == right && of == parent && floor_index == split)
    };
    let granted_on = |record: &TraceRecord, right: u64| {
        matches!(record.event, TraceEvent::RaftVote { server, range, granted: true, .. }
            if server == node && range == right)
    };
    let correct = pinned(seed);
    correct.check().unwrap();
    let records = correct.records();
    assert_eq!(correct.refusals().len(), 1, "one store refused");
    assert_eq!(correct.refusals()[0].0, node, "node {node}'s");
    let at = |from: usize, what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records[from..]
            .iter()
            .position(want)
            .map(|found| from + found)
            .unwrap_or_else(|| panic!("seed {seed} no longer reaches {what}: read its trace"))
    };
    let refused = at(
        0,
        "the node's refusal",
        &|record| matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node),
    );
    let reseeded = at(
        refused,
        "the re-seed's quarantine of range 2",
        &|record| matches!(record.event, TraceEvent::RaftReseeded { server, range: 2 } if server == node),
    );
    let mut from = reseeded;
    let mut made_at = Vec::new();
    for (parent, right, split) in chain {
        let made = at(
            from,
            &format!("the split that makes range {right}"),
            &|record| made_on(record, parent, right, split),
        );
        let marked = at(made, &format!("range {right}'s quarantine"), &|record| {
            matches!(record.event, TraceEvent::RaftReseeded { server, range }
                if server == node && range == right)
        });
        let stated = at(marked, &format!("range {right}'s statement"), &|record| {
            matches!(record.event, TraceEvent::RaftRecovered {
                server,
                range,
                applied,
                state: RecoveredAs::Quarantined,
                ..
            } if server == node && range == right && applied == split)
        });
        assert!(
            records[made..].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftVote { server, range, granted: false, .. }
                    if server == node && range == right
            )),
            "range {right}: never asked for a vote, so its granting none says nothing"
        );
        assert!(
            !records[made..]
                .iter()
                .any(|record| granted_on(record, right)
                    || matches!(record.event, TraceEvent::RaftLeader { server, range, .. }
                    if server == node && range == right)),
            "range {right}: the quarantined replica granted a vote or led"
        );
        made_at.push(made);
        from = stated;
    }
    // Each statement is the creation's and not a restart's, and the chain is the one pinned: the
    // first link's quarantine came through a live install on the store its split built, and the
    // second link's store is the one its split built with no install on it before it split in
    // turn.
    assert!(
        !records[refused..from].iter().any(|record| matches!(
            record.event,
            TraceEvent::NodeCrashed { node: gone } if u64::from(gone.get()) == node
        )),
        "node {node} crashed inside the chain, so a restart may have stated it"
    );
    let installed = |range: u64, between: std::ops::Range<usize>| {
        records[between].iter().any(|record| {
            matches!(record.event, TraceEvent::RaftSnapshot { server, range: of, taken: false, .. }
                if server == node && of == range)
        })
    };
    let (first, second) = (chain[0].1, chain[1].1);
    assert!(
        installed(first, made_at[0]..made_at[1]),
        "range {first} took no live install before its split"
    );
    assert!(
        !installed(second, made_at[1]..made_at[2]),
        "range {second} took an install before its split"
    );

    let variant = pinned_of(
        seed,
        Variants::default(),
        NodeVariants::of(&[NodeVariant::SplitLeavesQuarantineBehind]),
        RangeVariants::correct(),
    );
    variant
        .check()
        .expect("the variant's named failure is 9535's");
    let on_variant = variant.records();
    // The first link is made as on the correct run, and votes with no quarantine; its vote moves
    // the variant's schedule, so the later links are not the correct run's (on seed 9, where
    // this was first pinned, all three were), and what is asserted of them is the variant's
    // effect wherever it lands: no right half the node makes by a split after its refusal is
    // stated quarantined.
    // PROPOSED(D-127)
    let (parent, right, split) = chain[0];
    let made = on_variant
        .iter()
        .rposition(|record| made_on(record, parent, right, split))
        .expect("the variant makes the first link's right half on the node");
    assert!(
        on_variant[made..]
            .iter()
            .any(|record| granted_on(record, right)),
        "range {right}: under the variant the replica votes"
    );
    let variant_refused = on_variant
        .iter()
        .position(|record| {
            matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node)
        })
        .expect("the variant's node is refused too");
    let halves: Vec<u64> = on_variant[variant_refused..]
        .iter()
        .filter_map(|record| match record.event {
            TraceEvent::RangeCreated {
                range,
                cause: RangeCause::Split,
                ..
            } if node_of(record) == Some(node) => Some(range),
            _ => None,
        })
        .collect();
    assert!(
        halves.contains(&right),
        "under the variant the node made no right half by a split after its refusal"
    );
    for half in halves {
        assert!(
            !on_variant[variant_refused..].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftRecovered { server, range, state: RecoveredAs::Quarantined, .. }
                    if server == node && range == half
            )),
            "range {half}: the variant quarantined a right half after all"
        );
    }
}

// --- The owner's ruling of 2026-11-09 (#220; D-113 §6): a right half the correct system
// leaves wedged for good, excused from the meta convergence bound and from nothing else.

/// Seeds 2272 and 7949, the two seeds of ten thousand on which the correct system fails the meta
/// convergence bound without the exception. Node 3 alone ever leads the right half, crashes
/// before meta applied its update and is refused; node 1 and node 3 hold only placeholders, whose
/// parent reached the split by an install; and node 2 alone is initialised and stays a
/// pre-candidate for ever. Range 36 of seed 7949, a child of its range 35, has voters that hold
/// no parent at all, and meta had named it, so excusing it changes no verdict. Each condition of
/// the ruling is asserted of the reading and the leaders of the trace, each range is live by the
/// majority rule all the same, and the bound says what it said before the ruling.
///
/// **PROPOSED D-127 moved both schedules**, its answers to the chunks a node holds moving every
/// node schedule. Seed 2272 keeps the shape, with node 3's parent past the split at 123 where it
/// was 122, and a child of range 36 now beside it, range 37, made at 123, whose voters hold no
/// parent at all, as 7949's range 36 was. Seed 7949 passes the bound without the exception now,
/// which is asserted. Of the plain schedules' first ten thousand on D-127's tree, 2272 and 7736 fail
/// the bound without the exception, and 7736 with no store refused, so it is not this shape.
// PROPOSED(D-127): seed 7949's schedule moved off the shape; seed 2272 keeps it.
#[test]
fn seed_2272_a_right_half_with_no_possible_majority_is_excused_from_the_meta_bound() {
    use shard::Stuck::{NoParent, ParentPastTheSplit as Past};
    for (seed, first_failure, pins) in [(
        2272,
        36,
        vec![
            (36, 2, 119, [(1, Past(119)), (3, Past(123))]),
            (37, 36, 123, [(1, NoParent), (3, NoParent)]),
        ],
    )] {
        let report = pinned(seed);
        report.check().unwrap();
        report.meta_converges().unwrap();
        assert!(
            report.refusals().iter().any(|(node, _)| *node == 3),
            "seed {seed}: node 3's store was not refused: read its trace"
        );
        let live = report.checked.ranges_with_a_majority_up();
        let halves = report.right_halves();
        for (range, parent, split, stuck) in pins {
            let half = &halves[&range];
            assert!(
                live.contains(&range),
                "seed {seed}: range {range} is live by the majority rule, which the exception \
                 leaves alone"
            );
            assert_eq!((half.parent, half.split), (parent, split), "seed {seed}");

            assert_eq!(
                half.able,
                BTreeSet::from([2]),
                "seed {seed}, range {range}: condition one, fewer than two voters able to vote"
            );

            assert_eq!(
                (half.led_since_heal, half.leader_at_end),
                (0, None),
                "seed {seed}, range {range}: condition two, no leader since the heal or at the end"
            );
            let leaders: Vec<_> = report
                .records()
                .iter()
                .filter(
                    |r| matches!(r.event, TraceEvent::RaftLeader { range: of, .. } if of == range),
                )
                .collect();
            assert!(
                !leaders.is_empty() && leaders.iter().all(|r| r.at < report.last_fault_healed),
                "seed {seed}, range {range}: the trace's leaders, all before the last fault healed"
            );

            assert_eq!(
                half.stuck,
                BTreeMap::from(stuck),
                "seed {seed}, range {range}: condition three, every other voter a placeholder no \
                 apply can end"
            );
            assert!(half.others.is_empty(), "seed {seed}, range {range}");

            assert!(half.excused(), "seed {seed}, range {range}");
            let lag_of = |lags: Vec<shard::MetaLag>| lags.iter().any(|lag| lag.range == range);
            assert!(lag_of(report.meta_lags_excused()), "seed {seed}");
            assert!(lag_of(report.meta_lags_of_live_ranges()), "seed {seed}");
            assert!(!lag_of(report.meta_lags_asked()), "seed {seed}");
        }
        let without = report
            .meta_converges_without_the_exception()
            .expect_err("the bound fails this seed without the exception");
        assert!(
            without.contains(&format!("meta does not name range {first_failure} ")),
            "seed {seed}: the failure the exception stands in front of: {without}"
        );
    }
    // Seed 7949 carried the shape until PROPOSED D-127 moved its schedule: the bound holds on it
    // without the exception now.
    // PROPOSED(D-127)
    let report = pinned(7949);
    report.check().unwrap();
    assert_eq!(
        report.meta_converges_without_the_exception(),
        Ok(()),
        "seed 7949 fails the meta convergence bound without the exception again: its shape may be \
         back, so re-audit the pin"
    );
}

/// The bound keeps a tripping case with the exception in place, and at the price of one run: seed
/// 2272's own trace with one condition of the ruling broken at a time, and the bound fails on
/// its range 36 again. The last one is the shape of the plant D-113 §6 measures, a voter whose
/// parent reached the split with no install, which is a bug where the wedge is none.
#[test]
fn seed_2272_the_meta_bound_trips_again_on_any_condition_of_the_exception_broken() {
    let (range, parent, split) = (36, 2, 119);
    let mut report = pinned(2272);
    report.meta_converges().unwrap();
    let whole = report.checked.records.clone();
    let heal = report.last_fault_healed;
    let is_right = |record: &&TraceRecord, node: u64| {
        record.node == node_id(node)
            && matches!(
                record.event, TraceEvent::RangeCreated { range: of, .. } if of == range
            )
    };
    let leader_at_a_time_before_the_heal = whole
        .iter()
        .find(|r| matches!(r.event, TraceEvent::RaftLeader { range: of, .. } if of == range))
        .expect("the range's one leader")
        .clone();
    assert!(leader_at_a_time_before_the_heal.at < heal);

    let mut trips =
        |case: &str, records: Vec<TraceRecord>, broke: &dyn Fn(&shard::RightHalf) -> bool| {
            report.checked.records = records;
            let halves = report.right_halves();
            let half = &halves[&range];
            assert!(
                broke(half),
                "{case}: the record edited did not break its condition: {half:?}"
            );
            assert!(!half.excused(), "{case}: {half:?}");
            let tripped = report.meta_converges().expect_err(case);
            assert!(
                tripped.contains(&format!("meta does not name range {range} ")),
                "{case}: {tripped}"
            );
        };

    // Condition one, two voters able to vote: node 1 creates the right half node 2 created.
    let mut records = whole.clone();
    let mut on_node_1 = whole
        .iter()
        .find(|record| is_right(record, 2))
        .expect("node 2 created the right half")
        .clone();
    on_node_1.node = node_id(1);
    records.push(on_node_1);
    trips("two voters able to vote", records, &|half| {
        half.able.len() == 2
    });

    // Condition two, a leader since the heal, gone again by the end.
    let mut records = whole.clone();
    let at = |event| TraceRecord {
        at: heal,
        decided: heal,
        node: node_id(2),
        event,
    };
    records.push(at(TraceEvent::RaftLeader {
        server: 2,
        range,
        term: 9,
        last_index: 0,
    }));
    records.push(at(TraceEvent::RaftTerm {
        server: 2,
        range,
        term: 10,
        role: "follower",
        received: None,
    }));
    trips("a leader since the heal", records, &|half| {
        half.led_since_heal == 1 && half.leader_at_end.is_none()
    });

    // Condition two, a leader from before the heal with its office unended where the trace ends.
    let mut records = whole.clone();
    records.push(leader_at_a_time_before_the_heal);
    trips("a leader standing at the end", records, &|half| {
        half.led_since_heal == 0 && half.leader_at_end == Some(3)
    });

    // Condition three: node 1's parent reached the split with no install, so it should have
    // applied the split: the "only node 1 creates R" plant's shape.
    let mut records = whole.clone();
    let before = records.len();
    records.retain(|record| {
        !matches!(record.event,
        TraceEvent::RaftSnapshot { server: 1, range: of, last_index, taken: false, .. }
            if of == parent && last_index >= split)
    });
    assert!(records.len() < before, "node 1's install past the split");
    trips(
        "a placeholder an apply should have ended",
        records,
        &|half| half.others == BTreeSet::from([1]),
    );
}

// --- The owner's ruling of 2026-11-15 (D-142): in #220's exception alone, and until #220 is fixed,
// a replica quarantined under D-130 counts as a voter that cannot vote: one D-130's rule
// quarantined, made by a split's apply on a quarantined replica of its parent.

/// Seed 1137 on the sweep's own schedule (`Schedule::draw`, which draws `Fault::MetaReorder` on it),
/// which the Stage C integration found failing the correct system on the meta convergence bound
/// (PROPOSED D-135 §5): "meta does not name range 51 at generation 3 where the run stopped, owed
/// since 8.157s". Range 51 is the right half of range 2's split at 215. Node 2 alone holds it able
/// to vote, and pre-votes to the end. Node 1 holds a placeholder: its range 2 was installed past
/// the split, at 216. Node 3 was refused at 1.357 s and re-seeded, its range 2 installed and
/// stated quarantined, and its apply of the split made range 51 on that quarantined parent, which
/// took the quarantine and is stated so (D-130); it grants no vote. #220's exception
/// as the ruling of 2026-11-09 wrote it kept range 51 asked for node 3, a replica that holds the
/// range, and meta never names it, since no leader is there to send the update.
///
/// Under D-142 the range is excused with able {2}, stuck {1} and quarantined {3}: node 3 is still
/// in `others`, which every other reading sees as it did, and only the exception counts it as
/// unable to vote, since D-130's rule quarantined it, and nodes 1 and 2 could elect were #220
/// fixed. With the same replica made by an install instead, quarantined by another route, and with
/// its quarantine ended, by a restatement saying the disk carries no mark, the bound trips on
/// range 51 again.
// D-142
#[test]
fn seed_1137_a_right_half_whose_third_voter_is_quarantined_is_excused_from_the_meta_bound() {
    use shard::Stuck::ParentPastTheSplit as Past;
    let (seed, range, parent, split) = (1137, 51, 2, 215);
    let mut report = shard::run(seed);
    assert!(
        report
            .schedule
            .faults
            .iter()
            .any(|f| matches!(f, shard::Fault::MetaReorder { .. })),
        "seed {seed}: the sweep's schedule draws `Fault::MetaReorder` on it"
    );
    report.check().unwrap();
    report.meta_converges().unwrap();
    let records = report.records().to_vec();
    let at = |what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records
            .iter()
            .position(want)
            .unwrap_or_else(|| panic!("seed {seed} no longer reaches {what}: read its trace"))
    };

    // Node 3's quarantine, as D-130 has it: the refusal, range 2 re-seeded and stated quarantined,
    // range 51 made on it by the split's apply, and range 51 stated quarantined.
    let refused = at("node 3's refusal", &|r| {
        matches!(r.event, TraceEvent::RaftRefused { server: 3, .. })
    });
    let parent_stated = at("node 3's range 2 stated quarantined", &|r| {
        matches!(r.event, TraceEvent::RaftRecovered {
            server: 3,
            range: of,
            state: RecoveredAs::Quarantined,
            ..
        } if of == parent)
    });
    let made = at("node 3's range 51 made by the split's apply", &|r| {
        node_of(r) == Some(3)
            && matches!(r.event, TraceEvent::RangeCreated {
                range: of,
                cause: RangeCause::Split,
                parent: Some(from),
                floor_index,
                ..
            } if of == range && from == parent && floor_index == split)
    });
    let stated = at("node 3's range 51 stated quarantined", &|r| {
        matches!(r.event, TraceEvent::RaftRecovered {
            server: 3,
            range: of,
            state: RecoveredAs::Quarantined,
            ..
        } if of == range)
    });
    assert!(
        refused < parent_stated && parent_stated < made && made < stated,
        "seed {seed}: the refusal, the parent's quarantine, the creation and its statement in order"
    );
    // Nothing ends it: no later refusal of node 3, and no restatement of its range 51 with no mark.
    assert!(
        !records[made..]
            .iter()
            .any(|r| matches!(r.event, TraceEvent::RaftRefused { server: 3, .. })),
        "seed {seed}: node 3 refused again"
    );
    assert!(
        !records[made..].iter().any(|r| matches!(r.event,
            TraceEvent::RaftRecovered { server: 3, range: of, state: RecoveredAs::Neither, .. }
                if of == range)),
        "seed {seed}: node 3's range 51 restated with no mark"
    );
    assert!(
        !records[made + 1..].iter().any(|r| node_of(r) == Some(3)
            && matches!(r.event, TraceEvent::RangeCreated { range: of, .. } if of == range)),
        "seed {seed}: node 3's range 51 made again"
    );
    // The wedge: range 51 never has a leader, node 3 grants no vote in it, and node 2 is a
    // pre-candidate where the trace ends.
    assert!(
        !records
            .iter()
            .any(|r| matches!(r.event, TraceEvent::RaftLeader { range: of, .. } if of == range)),
        "seed {seed}: range {range} had a leader"
    );
    assert!(
        !records.iter().any(|r| matches!(r.event,
            TraceEvent::RaftVote { server: 3, range: of, granted: true, .. } if of == range)),
        "seed {seed}: node 3's quarantined replica granted a vote"
    );
    let last_role = records.iter().rev().find_map(|r| match r.event {
        TraceEvent::RaftTerm {
            server: 2,
            range: of,
            role,
            ..
        } if of == range => Some(role),
        _ => None,
    });
    assert_eq!(
        last_role,
        Some("pre-candidate"),
        "seed {seed}: node 2 at the end"
    );

    // The reading: excused under the extended exception, and by it alone.
    assert!(
        report.checked.ranges_with_a_majority_up().contains(&range),
        "seed {seed}: range {range} is live by the majority rule, which D-142 leaves alone"
    );
    let halves = report.right_halves();
    let half = &halves[&range];
    assert_eq!((half.parent, half.split), (parent, split), "seed {seed}");
    assert_eq!(half.able, BTreeSet::from([2]), "seed {seed}: condition one");
    assert_eq!(
        (half.led_since_heal, half.leader_at_end),
        (0, None),
        "seed {seed}: condition two"
    );
    assert_eq!(
        half.stuck,
        BTreeMap::from([(1, Past(216))]),
        "seed {seed}: node 1's placeholder, its parent installed past the split"
    );
    assert_eq!(
        half.quarantined,
        BTreeSet::from([3]),
        "seed {seed}: node 3's replica, quarantined under D-130"
    );
    assert_eq!(
        half.others,
        BTreeSet::from([3]),
        "seed {seed}: `others` as every other reading of it sees it"
    );
    assert!(
        half.a_majority_were_220_fixed(),
        "seed {seed}: nodes 1 and 2 could elect were #220 fixed, so #220 is part of the wedge"
    );
    assert!(half.excused(), "seed {seed}: excused under D-142");
    assert!(half.excused_by_the_quarantine(), "seed {seed}");
    let mut before = half.clone();
    before.quarantined.clear();
    assert!(
        !before.excused(),
        "seed {seed}: the exception as it stood before the ruling asks of range {range}"
    );
    let has = |lags: Vec<shard::MetaLag>| lags.iter().any(|lag| lag.range == range);
    assert!(
        has(report.meta_lags_excused_by_the_quarantine()),
        "seed {seed}"
    );
    assert!(has(report.meta_lags_excused()), "seed {seed}");
    assert!(!has(report.meta_lags_asked()), "seed {seed}");
    let without = report
        .meta_converges_without_the_exception()
        .expect_err("the bound fails this seed without the exception");
    assert!(
        without.contains(&format!(
            "meta does not name range {range} at generation 3 where the run stopped"
        )),
        "seed {seed}: the failure the integration found: {without}"
    );

    // The same replica made by an install, as D-109 §2 makes one on a re-seeded node, and stated
    // quarantined just the same: not D-130's rule, so the bound asks of the range. D-142 counts
    // the rule's quarantine and no other.
    let creation = report.checked.records[made].clone();
    if let TraceEvent::RangeCreated { cause, parent, .. } = &mut report.checked.records[made].event
    {
        (*cause, *parent) = (RangeCause::Snapshot, None);
    }
    let halves = report.right_halves();
    let half = &halves[&range];
    assert_eq!(
        (&half.others, &half.quarantined),
        (&BTreeSet::from([3]), &BTreeSet::new()),
        "seed {seed}: quarantined, and not under D-130"
    );
    assert!(!half.excused(), "seed {seed}");
    let tripped = report
        .meta_converges()
        .expect_err("the bound asks of range 51 where its third voter is not D-130's");
    assert!(
        tripped.contains(&format!("meta does not name range {range} ")),
        "seed {seed}: {tripped}"
    );
    report.checked.records[made] = creation;

    // The quarantine ended where the trace ends, by a restatement of node 3's range 51 with no
    // mark: node 3 can vote from there by the trace's account, so the bound asks of the range.
    let mut ended = records[stated].clone();
    if let TraceEvent::RaftRecovered { state, .. } = &mut ended.event {
        *state = RecoveredAs::Neither;
    }
    let last = records.last().expect("a trace");
    (ended.at, ended.decided) = (last.at, last.decided);
    report.checked.records.push(ended);
    let halves = report.right_halves();
    let half = &halves[&range];
    assert_eq!(
        half.quarantined,
        BTreeSet::new(),
        "seed {seed}: its quarantine ended"
    );
    assert_eq!(half.others, BTreeSet::from([3]), "seed {seed}");
    assert!(!half.excused(), "seed {seed}");
    let tripped = report
        .meta_converges()
        .expect_err("the bound asks of range 51 once its third voter can vote");
    assert!(
        tripped.contains(&format!("meta does not name range {range} ")),
        "seed {seed}: {tripped}"
    );
}

/// Seed 707, the first shape-drawn seed the correct system failed on, and a defect of the log
/// view and not of the system (PROPOSED D-131). Node 1 restarted with its log of range 19 compacted
/// to 119 and one entry at 120, and its next restart was refused: the re-seed's floor is 112, below
/// both. The store that held 120 is gone, but the view kept that entry with nothing under it, so
/// node 2's restated 120 read as agreeing with a log that differs at 119, though every entry any
/// node traced at 119 and at 120 carries the same hash. The view now forgets a replica's entries at
/// its creation; with node 1's creation taken out of the trace it says what it said before.
///
/// `Fault::MetaReorder` moved it (PROPOSED D-135): drawn second of nine faults, the arm split range
/// 20 on the sweep's schedule and moved every later step, and node 1's log of range 19 is never
/// compacted to 119 there. So the pin ran the schedule it was found on and asserted that absence.
///
/// **PROPOSED D-127 moved the seed off the shape**, its answers to the chunks a node holds
/// moving every node schedule: node 1 restarts with no log compacted to 119 now, and the log view
/// does not need any re-seed's creation on the run. The pin is the absence with its reason: taken
/// out of the trace one at a time, no re-seed's creation leaves the log view failing, as on every
/// seed of the shaped schedules a search of D-127's tree reached. At the Stage C integration (D-127
/// beside D-135) the absence is asserted on both schedules, the shaped one D-127 audited and the
/// sweep's ([`absent_from_both`]).
// PROPOSED(D-131)
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
// PROPOSED(D-135): on both schedules.
#[test]
fn seed_707_a_reseed_below_a_compacted_tail_leaves_nothing_of_the_lost_store_in_the_log_view() {
    absent_from_both(
        707,
        "a re-seed's creation the log view forgets a lost store by",
        |report| {
            let records = report.records();
            let refused: BTreeSet<u64> = report.refusals().iter().map(|(node, _)| *node).collect();
            // Every re-seed's creation on a refused node, taken out of the trace one at a time:
            // the situation is the log view failing without one of them.
            records.iter().enumerate().any(|(at, record)| {
                let reseeded = node_of(record).is_some_and(|node| refused.contains(&node))
                    && matches!(
                        record.event,
                        TraceEvent::RangeCreated {
                            cause: RangeCause::Snapshot,
                            ..
                        }
                    );
                reseeded && {
                    let without: Vec<ananke_raft::invariants::Traced<'_>> = records
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != at)
                        .map(|(_, record)| {
                            ananke_raft::invariants::Traced::new(node_of(record), &record.event)
                        })
                        .collect();
                    ananke_raft::invariants::log_matching(without).is_err()
                }
            })
        },
    );
}

/// Seed 992, the other shape-drawn seed the correct system failed on, and a defect of the asker
/// and not of the system (PROPOSED D-131). The restart arm's split is refused `NoRangeId` on node
/// 1's new run, and its retry is proposed and applied 130 ms later, but the answer to the retry is
/// dropped by the network. A leader answers a split's entry once, at its apply, and a copy with
/// silence, so an asker that sent the same copy heard nothing for 3.2 s and D-092's bound said so
/// (`Report::retries_take_effect`). The bound is unchanged. The asker now asks anew after a silent
/// wait, is refused `KeyOutsideSpan` at once since the split has landed, and stops within it.
///
/// **Pinned on seed 992 until PROPOSED D-127**, whose answers to the chunks a node holds moved its
/// schedule off the shape. Seed 1314 carries it on D-127's tree: the restart arm's split refused `NoRangeId` on node 3's new run, its retry applied, the
/// answer dropped on the way to the asker, the asker asking anew and refused `KeyOutsideSpan`,
/// within D-092's bound. D-127 searched the shaped schedules, so at the Stage C integration (D-127
/// beside D-135) the pin runs `shard::Schedule::shaped`, the schedule it was found on, and asserts
/// its situation absent from the sweep's ([`absent_from_the_sweep`]). There `Fault::MetaReorder`
/// moved it: the restart arm crashes node 1, not node 3, and its split of range 19 is refused
/// `NoRangeId` twice on node 1's new run, then proposed, applied and answered done, the answer
/// arriving. The broad predicate, [`restart_arms_split_refused_no_range_id`], holds there too, so
/// the absence names the answer lost and the asker asking anew
/// ([`restart_arms_answer_lost_and_asked_anew`]).
// PROPOSED(D-131)
// PROPOSED(D-127): re-pinned from seed 992, whose schedule D-127 moved.
// PROPOSED(D-135): on the shaped schedule it was found on; the situation absent from the sweep's.
#[test]
fn seed_1314_an_asker_whose_answer_is_lost_asks_anew_and_is_answered_within_the_bound() {
    let seed = 1314;
    let report = shaped_of(seed, NodeVariants::correct());
    absent_from_the_sweep(
        &report,
        "the restart arm's split's answer lost and its asker asking anew",
        restart_arms_answer_lost_and_asked_anew,
    );
    report.check().unwrap();
    assert!(
        restart_arms_split_refused_no_range_id(&report),
        "seed {seed}: the restart arm's split is no longer refused `NoRangeId` on its node's new run"
    );
    assert_eq!(report.restart_arms.len(), 1, "{:?}", report.restart_arms);
    let arm = report.restart_arms[0];
    let seq = arm.seq.expect("the arm asked its split");
    assert_eq!(
        report.driver.retried.len(),
        1,
        "{:?}",
        report.driver.retried
    );
    let retried = report.driver.retried[0].clone();
    let empties = report.no_range_ids();
    assert_eq!(empties.len(), 1, "one `NoRangeId`: {empties:?}");
    assert_eq!(empties[0].node, arm.victim);
    assert!(
        empties[0].restarted.is_some(),
        "its node restarted before it"
    );
    assert!(
        retried.answered && retried.asking_at(report.last_fault_healed),
        "the asker is asked of, and got an answer: {retried:?}"
    );
    let records = report.records();
    let index_of_the_proposal = records
        .iter()
        .find_map(|record| match record.event {
            TraceEvent::RaftProposed {
                server,
                client,
                seq: of,
                index,
                ..
            } if server == arm.victim && client == 96 << 32 && of == seq => Some(index),
            _ => None,
        })
        .unwrap_or_else(|| panic!("seed {seed}'s retry was proposed on the restarted node"));
    assert!(
        records.iter().any(|record| matches!(&record.event,
            TraceEvent::RangeSplit { index, range, .. }
                if *index == index_of_the_proposal && Some(*range) == arm.range)),
        "the split took effect"
    );
    let admin = raft::admin_addr(seq);
    assert!(
        records.iter().any(|record| matches!(&record.event,
            TraceEvent::MessageDropped { from, to, .. }
                if *from == raft::server_addr(arm.victim) && *to == admin)),
        "seed {seed}'s answer was not dropped on the way to its asker: pin it on its mechanism"
    );
    assert_eq!(
        report.driver.refused.get("key-outside-span"),
        Some(&1),
        "the asker asked anew and was refused at once: {:?}",
        report.driver.refused
    );
    // PROPOSED(D-137): the refusal follows from the split's effect, which the check now asks of
    // the trace: the split had applied before the answer came.
    assert_eq!(retried.refusal, Some(SplitRefusal::KeyOutsideSpan));
    assert!(
        report
            .split_fate(&retried)
            .applied
            .is_some_and(|at| at <= retried.ended),
        "the retry took effect by its answer: {retried:?}"
    );
    let bound = raft::election_max() * raft::LIVENESS_TIMEOUTS;
    assert!(
        retried.took(report.last_fault_healed) <= bound,
        "answered within D-092's bound: {retried:?}"
    );
}

/// Seed 992 under `RefusedAfterRefill`, the instrument's pair at every tier (PROPOSED D-137, issue
/// #222): the restart arm's split, refused `NoRangeId` on node 1's new run, is retried after the
/// last fault's heal, and once the refill lands the variant refuses it `NotLive`. The asker stops on
/// that answer and the split never applies. D-092's check names it as taking no effect, and the
/// reading as first written, which took any answer but `NoRangeId` for one, passes the same asker.
/// The correct half is asserted here too, on the same seed and schedule: on the correct node the
/// same asker's retry is proposed and applied, and the split had applied when it was answered.
/// Until the Stage C integration it was the pin above on the same seed; PROPOSED D-127 moved that
/// pin's lost answer to seed 1314, where the variant's asker is never asked of, and this seed
/// keeps the variant's catch.
///
/// Both halves run the schedule the pin was found on, `shard::Schedule::shaped`: on the sweep's,
/// `Fault::MetaReorder` moved the restart arm's split off its `NoRangeId` (PROPOSED D-135; the Stage
/// C integration, D-137 beside D-135), and under the variant no asker is asked of there. The pin
/// asserts that absence ([`absent_from_the_sweep_of`], the variant on the sweep's schedule).
// PROPOSED(D-137)
// PROPOSED(D-135): on the shaped schedule, as the correct half; the situation absent from the sweep's.
#[test]
fn seed_992_a_retry_refused_once_the_refill_lands_takes_no_effect_and_the_check_says_so() {
    let refill = RangeVariants::of(&[RangeVariant::RefusedAfterRefill]);
    let report = shard::run_with(
        992,
        shard::Schedule::shaped(992),
        Variants::default(),
        NodeVariants::correct(),
        refill,
    );
    absent_from_the_sweep_of(
        &report,
        &shard::run_of(992, Variants::default(), NodeVariants::correct(), refill),
        "an asker asked of under `RefusedAfterRefill` and refused `NotLive`",
        |report| {
            report
                .retries_asked()
                .iter()
                .any(|retried| retried.refusal == Some(SplitRefusal::NotLive))
        },
    );
    let asked = report.retries_asked();
    assert_eq!(
        asked.len(),
        1,
        "seed 992's asker is asked of under the variant: {:?}",
        report.driver.retried
    );
    let retried = &asked[0];
    assert_eq!(
        retried.refusal,
        Some(SplitRefusal::NotLive),
        "the retry was refused `NotLive` once the refill landed: {retried:?}"
    );
    assert_eq!(
        report.split_fate(retried),
        shard::SplitFate::default(),
        "the split never applied, and nothing left its key outside its range"
    );
    let why = report.check().expect_err("the retry took no effect");
    assert!(
        why.contains("was answered `not-live`") && why.contains("and took no effect"),
        "seed 992's catch under the variant is the retry's effect: {why}"
    );
    let bound = raft::election_max() * raft::LIVENESS_TIMEOUTS;
    assert_eq!(
        retried.took_no_effect(report.last_fault_healed, bound, as_first_written(retried)),
        None,
        "the reading as first written, any answer but `NoRangeId`, passes this asker"
    );
    // The correct half on the same seed and schedule: the same split's asker, asked of, takes
    // effect by its answer.
    let correct = shaped_of(992, NodeVariants::correct());
    correct.check().unwrap();
    assert!(
        correct.retries_asked().iter().any(|asker| {
            (asker.range, &asker.key) == (retried.range, &retried.key)
                && correct
                    .split_fate(asker)
                    .applied
                    .is_some_and(|at| at <= asker.ended)
        }),
        "seed 992: on the correct node the same split's asker is not asked of, or its retry took \
         no effect: {:?}",
        correct.retries_asked()
    );
}

/// Seed 1032, a correct-system failure of D-092's check as #222 first asked it, a defect of the
/// check (PROPOSED D-137): the asker of range 2's split at `k05` is refused `NoRangeId`, and before
/// its retry is answered another asker's split of range 2 at `k04` lands, so `k05` lies in the
/// right half and range 2's leader answers `KeyOutsideSpan`. The split asked for can no longer
/// take effect, and that answer is the true one. The check reads it so: the asker's range had left
/// its key outside before the answer ([`shard::SplitFate`]'s `outside`), and with that reading
/// taken out the same asker reads as taking no effect.
///
/// **The Stage C integration moved the seed off the shape** (D-137 beside PROPOSED D-127 and D-135).
/// D-127's answers to the chunks a node holds moved every node schedule, and on seed 1032 no asker
/// is answered `KeyOutsideSpan` after another split of its range left its key outside now, on the
/// shaped schedule it was found on or on the sweep's. A search of seeds 0 to 2 999 on both
/// schedules and 3 000 to 9 999 on the sweep's found the shape on no seed, so the pin is the
/// absence on its seed, with its reason ([`absent_from_both`]), and the day either schedule reaches
/// it again this says so. The reading itself stays paired by `sim/shard.rs`'s directed cases of
/// `Retried::took_no_effect`, the true `KeyOutsideSpan` among them, which no schedule moves.
// PROPOSED(D-137)
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
// PROPOSED(D-135): on both schedules.
#[test]
fn seed_1032_a_retry_another_split_left_outside_its_range_is_answered_truly() {
    absent_from_both(
        1032,
        "an asker answered `KeyOutsideSpan` after another split left its key outside",
        |report| {
            report.retries_asked().iter().any(|retried| {
                let fate = report.split_fate(retried);
                retried.refusal == Some(SplitRefusal::KeyOutsideSpan)
                    && fate.applied.is_none()
                    && fate.outside.is_some_and(|at| at <= retried.ended)
            })
        },
    );
}

/// Seed 1248, the other correct-system failure of D-092's check as #222 first asked it, and a
/// defect of the check (PROPOSED D-137): the asker of range 35's split at `k10` is answered done,
/// and the records put the split's first apply 12 ms after the answer. The asker reads its times
/// off its own node's clock, which is skewed and drifts, and the records carry global time; read
/// on the asker's clock the split applied 11 ms before the answer, as a leader that answers after
/// its own apply has it. Compared across the two clocks the asker took no effect, as 17 of the 76
/// askers the correct system's ten thousand seeds ask of did.
///
/// Found on the sweep's schedule before `Fault::MetaReorder`, `shard::Schedule::shaped`, and run
/// there (PROPOSED D-135; the Stage C integration, D-137 beside D-135). On the sweep's schedule no
/// asker is asked of on this seed, and the pin asserts that its situation, an asker answered done
/// whose split's first apply in the records' time comes after the answer, is absent there
/// ([`absent_from_the_sweep`]).
// PROPOSED(D-137)
// PROPOSED(D-135): on the shaped schedule it was found on; the situation absent from the sweep's.
#[test]
fn seed_1248_a_retry_answered_done_is_read_on_the_askers_clock() {
    let report = shaped_of(1248, NodeVariants::correct());
    absent_from_the_sweep(
        &report,
        "an asker answered done before its split's first apply in the records' time",
        |report| {
            let records = report.records();
            report.retries_asked().iter().any(|retried| {
                retried.answered
                    && retried.refusal.is_none()
                    && records
                        .iter()
                        .find_map(|record| match &record.event {
                            TraceEvent::RangeSplit { range, key, .. }
                                if *range == retried.range
                                    && key[..]
                                        == ananke_raft::apply::user_key(&retried.key)[..] =>
                            {
                                Some(record.at)
                            }
                            _ => None,
                        })
                        .is_some_and(|at| at > retried.ended)
            })
        },
    );
    report.check().unwrap();
    let asked = report.retries_asked();
    assert_eq!(asked.len(), 1, "{:?}", report.driver.retried);
    let retried = &asked[0];
    assert_eq!((retried.range, &retried.key[..]), (35, &b"k10"[..]));
    assert_eq!(retried.refusal, None, "answered done");
    let global = report
        .records()
        .iter()
        .find_map(|record| match &record.event {
            TraceEvent::RangeSplit { range: 35, key, .. }
                if key[..] == ananke_raft::apply::user_key(b"k10")[..] =>
            {
                Some(record.at)
            }
            _ => None,
        })
        .expect("the split applied");
    assert!(
        global > retried.ended,
        "the records' time of the apply is past the asker's time of its answer: seed 1248's \
         clocks no longer straddle them, so pin it on its mechanism"
    );
    let fate = report.split_fate(retried);
    assert!(
        fate.applied.is_some_and(|at| at <= retried.ended),
        "on the asker's clock the split applied before the answer: {fate:?}"
    );
    let bound = raft::election_max() * raft::LIVENESS_TIMEOUTS;
    let across = shard::SplitFate {
        applied: Some(global),
        outside: Some(global),
    };
    assert!(
        retried
            .took_no_effect(report.last_fault_healed, bound, across)
            .is_some(),
        "read across the two clocks the asker takes no effect"
    );
}

/// Seed 5620, the third shape-drawn seed the correct system failed on, and this one a defect of the
/// system: a data loss D-132 fixes (D-131, D-132). The restart arm leaves node 3 behind on
/// range 3, node 1 takes a snapshot at 113, the index of its split of range 3 at `k05`, and streams
/// it to node 3; and node 3's apply of that split, which makes range 20, lands while the stream's
/// switch is being prepared. The old switch replaced the copy of the span it took at the stream's
/// last chunk, `[k03, k10)`, and range 20's keys went with it.
///
/// **A switch that replaces only the left half is not what this seed shows, and the pin does not
/// claim it.** The fix has two halves and this seed reaches the first: the snapshot is at the
/// split's own index, so at the switch the store holds everything the snapshot carries, and the
/// install is answered installed and switches nothing (`Task::finish`'s second re-check). Replacing
/// `[k03, k05)` is the case of a snapshot past the split, which `a_split_applied_through_the_log_...`
/// in `install.rs` asserts. What stands for the mechanism on the trace, since an answer without a
/// switch traces nothing of its own: the correct run and the variant's are one run, record for
/// record, up to the variant's switch being spawned, and by then range 20 is made, so the correct
/// node stood at that switch with the split landed and did not make it. Range 20's keys stay because
/// nothing on node 3 replaces its span but range 20's own install, at 115.
///
/// `Fault::MetaReorder` moved it (PROPOSED D-135): drawn first of seven faults, the arm found no range
/// to split twice led off the meta range's leader's node and did nothing, but its hold still moved
/// every later step; on the sweep's schedule node 1 takes no snapshot of range 3 at 113. So the pin
/// ran the schedule it was found on, the variant's run too, and asserted that absence.
///
/// **PROPOSED D-127 moved the seed off the shape**, its answers to the chunks a node holds
/// moving every node schedule: node 1's snapshot of range 3 at the split is no longer taken, and
/// under `InstallReplacesCopiedSpan` no install removes a right half's keys on this seed. The
/// pin is the absence with its reason; the variant's catch is the unit tests' in `install.rs`
/// (`InstallReplacesCopiedSpan`, D-132), which no schedule moves. At the Stage C integration
/// (D-127 beside D-135) the absence is asserted on both schedules, the shaped one D-127 audited
/// and the sweep's.
// D-131
// D-132
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
// PROPOSED(D-135): on both schedules.
#[test]
fn seed_5620_an_install_a_split_overtook_switches_nothing_and_leaves_the_right_half_its_keys() {
    let buggy = NodeVariants::of(&[NodeVariant::InstallReplacesCopiedSpan]);
    for (schedule, report, variant) in [
        (
            "the shaped schedule",
            shaped_of(5620, NodeVariants::correct()),
            shaped_of(5620, buggy),
        ),
        (
            "the sweep's schedule",
            shard::run(5620),
            shard::run_of(5620, Variants::default(), buggy, RangeVariants::correct()),
        ),
    ] {
        report.check().unwrap();
        if let Err(why) = variant.check() {
            assert!(
                !why.contains("and removes that replica's keys"),
                "seed 5620: an install replaces a right half's keys under the variant again on \
                 {schedule}: the shape may be back, so upgrade the pin back to its mechanism \
                 (PROPOSED D-132): {why}"
            );
        }
    }
}

/// Seed 13283, found past the nightly's range on the shaped schedule (#225): route 1 of #225, a split's
/// apply inside an install's switch. Node 2 installs range 44's snapshot at 167, which its leader took
/// after applying range 44's split at 167, and the install is numbered at 8.7545 s, with the split's
/// entry and the one after it, 168, queued to node 2's `apply` task before the hold. Before D-133 the
/// split applied at 8.7845 s, between the numbering and the switch: it made range 3 over `[k09, k14)`
/// and narrowed the table after the switch had read it, the switch at 8.8365 s replaced `[k06, k14)`
/// and range 3's keys with it, and 168, applied inside the switch, was taken back to 167 by it and
/// applied a second time, which is the sweep's own verdict, state machine safety.
///
/// The mechanism the pin asserted until the Stage C integration: the correct run and
/// `SplitAppliesInsideTheSwitch`'s are one run up to the moment node 2's `apply` task reaches the
/// split, with the install numbered and not yet switched; there the variant applies the split inside
/// the switch, and the correct node waits out the switch and the host's `Switched`, which restates
/// range 44's applied index to 167, so the split is the install's and 168 applies once; the variant
/// fails with check 8's install clause and with the sweep's verdict.
///
/// **The Stage C integration moved the seed off the shape** (D-133 beside PROPOSED D-127 and D-135).
/// D-127's answers to the chunks a node holds moved every node schedule, and on seed 13283 no split's
/// apply meets its range's switch now, on the shaped schedule it was found on or on the sweep's:
/// `SplitAppliesInsideTheSwitch`, which changes only what such a meeting does, leaves both runs record
/// for record the correct node's. For this seed the two schedules are one, the plain one: it draws
/// no shape, as one seed in eight does (`shard::PLAIN_ONE_IN`), so the sweep adds no reorder arm to
/// it; the pin names both so that a change to either schedule's draw is still covered. The pin is
/// that absence, with its reason, and the day either schedule reaches the shape again the runs part
/// and this says so. The pair stays D-133's directed test of four cases in `install.rs`, which no
/// schedule moves. The shape was found past the nightly's range, where D-132 counted the window on
/// 2 of 60 226 switches, so the integration did not search for a seed to carry it.
// PROPOSED(D-133)
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
// PROPOSED(D-135): on both schedules.
#[test]
fn seed_13283_a_split_inside_an_install_waits_for_switched_and_nothing_is_applied_twice() {
    let buggy = NodeVariants::of(&[NodeVariant::SplitAppliesInsideTheSwitch]);
    for (schedule, correct, variant) in [
        (
            "the shaped schedule",
            shaped_of(13283, NodeVariants::correct()),
            shaped_of(13283, buggy),
        ),
        (
            "the sweep's schedule",
            shard::run(13283),
            shard::run_of(13283, Variants::default(), buggy, RangeVariants::correct()),
        ),
    ] {
        correct.check().unwrap();
        assert!(
            correct.records() == variant.records(),
            "seed 13283: `SplitAppliesInsideTheSwitch` changes the run on {schedule}, so a split's \
             apply meets its range's switch again: upgrade the pin back to its mechanism \
             (PROPOSED D-133)"
        );
    }
}

/// The stale-cache shape's writer, found on seed 16725 past the nightly's range
/// (D-131). There node 1 led range 14, proposed the writer's put at index 100, applied it, and
/// crashed before it answered. The writer heard nothing, and asked the next node under the same (client, seq). A
/// leader's record of an applied (client, seq) is that leader's, so node 3, leading next, proposed
/// it at 103 and it applied a second time, over a write the history had between the two. The history
/// closes the call at its first apply and never models the second, so a get of the twice-applied
/// value could not be placed, and linearizability failed a correct system.
///
/// A write now gets one try, as the sweep's own client gives it (`raft::client_over`: a get can be
/// asked again elsewhere, a write cannot). The mechanism asserted: up to the silence the run is what
/// it was, node 1 proposing it at 100 and crashing with it applied and unanswered; the write is
/// asked once, so node 1 is the only proposal of it and its entry applies at one place; the writer
/// stays open, so the shape's reader asks nothing; and linearizability holds over the history.
///
/// On seed 16725 the run's verdict named `timers` (issue #227), which was not that pin's to assert:
/// PROPOSED D-134 holds a hold let go with no switch to its release, and its own pin on seed 16725
/// follows this one.
///
/// **Pinned on seed 16725 until PROPOSED D-127**, whose answers to the chunks a node holds moved
/// that schedule off the shape. Seed 73 is the lowest of the shaped schedules to carry it on
/// D-127's tree: node 2 leads range 21, proposes the writer's put (burst 4) at index 98, applies it
/// and crashes before it answers, and the writer, asked once, stays open. What is asserted is the
/// same: one proposal, one place applied, the crash with it applied and unanswered, no reader, and
/// linearizability over the history. The run's verdict is not this pin's to assert. D-127 searched
/// the shaped schedules, so at the Stage C integration (D-127 beside D-135) the pin runs
/// `shard::Schedule::shaped`, the schedule it was found on, and asserts its situation absent from
/// the sweep's ([`absent_from_the_sweep`]): there node 2 proposes the write at index 113 and the
/// writer is answered.
// PROPOSED(D-127): re-pinned from seed 16725, whose schedule D-127 moved.
// D-131
// PROPOSED(D-135): on the shaped schedule it was found on; the situation absent from the sweep's.
#[test]
fn seed_73_a_write_that_heard_nothing_is_asked_once_and_nothing_reads_a_value_applied_twice() {
    const SEED: u64 = 73;
    const BURST: u64 = 4;
    const WRITER: u64 = (95 << 32) | (1 << 16) | BURST;
    const READER: u64 = (95 << 32) | (2 << 16) | BURST;
    let (node, range, index) = (2, 21, 98);
    let report = shaped_of(SEED, NodeVariants::correct());
    absent_from_the_sweep(
        &report,
        "the writer's put applied by the node that proposed it, which crashed with the writer unanswered",
        |report| {
            let records = report.records();
            !records.iter().any(|record| {
                matches!(record.event, TraceEvent::ClientReturn { client, .. } if client == WRITER)
            }) && records.iter().any(|record| match record.event {
                TraceEvent::RaftProposed {
                    server,
                    range,
                    client,
                    index,
                    ..
                } if client == WRITER => records
                    .iter()
                    .position(|applied| {
                        matches!(applied.event, TraceEvent::RaftApply {
                            server: s, range: of, index: i, ..
                        } if (s, of, i) == (server, range, index))
                    })
                    .is_some_and(|at| {
                        records[at..].iter().any(|later| {
                            matches!(later.event,
                                TraceEvent::NodeCrashed { node: gone } if Some(gone) == node_id(server))
                        })
                    }),
                _ => false,
            })
        },
    );
    let records = report.records();

    let proposed: Vec<(u64, u64, u64)> = records
        .iter()
        .filter_map(|record| match record.event {
            TraceEvent::RaftProposed {
                server,
                range,
                client,
                index,
                ..
            } if client == WRITER => Some((server, range, index)),
            _ => None,
        })
        .collect();
    assert_eq!(
        proposed,
        vec![(node, range, index)],
        "seed {SEED}'s write was proposed by node {node} at index {index} of range {range}, and \
         by no one else"
    );
    let (at, hash) = records
        .iter()
        .enumerate()
        .find_map(|(at, record)| match record.event {
            TraceEvent::RaftApply {
                server,
                range: of,
                index: i,
                hash,
                ..
            } if server == node && of == range && i == index => Some((at, hash)),
            _ => None,
        })
        .unwrap_or_else(|| panic!("seed {SEED}: node {node} applied the write"));
    assert!(
        records[at..].iter().any(|record| matches!(
            record.event,
            TraceEvent::NodeCrashed { node: gone } if Some(gone) == node_id(node)
        )),
        "seed {SEED}: node {node} crashed with the write applied: pin it on its mechanism"
    );
    assert!(
        !records.iter().any(|record| matches!(
            record.event,
            TraceEvent::ClientReturn { client, .. } if client == WRITER
        )),
        "seed {SEED}: the writer heard nothing, so its call stays open"
    );

    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(&record.event,
                TraceEvent::MessageSent { from, .. } if *from == raft::burst_addr(BURST)))
            .count(),
        1,
        "the write is asked once, and the reader, which asks after the writer's answer, not at all"
    );
    assert!(
        !records.iter().any(|record| matches!(
            record.event,
            TraceEvent::ClientInvoke { client, .. } if client == READER
        )),
        "the shape's reader asked with no answer to the writer"
    );

    let elsewhere: Vec<(u64, u64, u64)> = records
        .iter()
        .filter_map(|record| match record.event {
            TraceEvent::RaftApply {
                server,
                range,
                index,
                hash: of,
                ..
            } if of == hash => Some((server, range, index)),
            _ => None,
        })
        .filter(|&(_, of, i)| (of, i) != (range, index))
        .collect();
    assert!(
        elsewhere.is_empty(),
        "the write's entry applied at a second place: {elsewhere:?}"
    );
    lin::check(&report.checked.history).unwrap_or_else(|why| {
        panic!(
            "seed {SEED}'s history, every call placed: nothing reads a value applied twice: \
                 {why:?}"
        )
    });
}

/// Seed 16725, issue #227's run (PROPOSED D-134), found past the nightly's range on the shaped
/// schedule. A hold on range 2 of node 3, taken at 7.681 s for an install that ended in `Unusable`
/// at 8.222 s with no switch, was measured against the timer's bound, since the replay's exemption
/// opened only on the switch's read-back (`reads_back`), and the run's verdict was `timers`. The
/// node now traces that release, decided at the hold (`RaftSnapshotReleased`), and the replay holds
/// the replica from the one to the other. On D-134's tree the pin asserted the check as it stood,
/// `TimerResets::WITHOUT_RELEASED_HOLD`, flagging exactly #227's gap inside that hold, the check as
/// it stands, `TimerResets::ALL`, flagging none, and the run passing every check.
///
/// **The Stage C integration moved the seed off the shape** (D-134 beside PROPOSED D-127 and D-135).
/// It was one pin with the stale-cache writer's on this seed; D-127 moved the writer's situation to
/// seed 73, and its answers to the chunks a node holds moved #227's hold too, on the shaped schedule
/// and on the sweep's, which `Fault::MetaReorder` had already moved off it. A search of seeds 0 to
/// 2 999 on both schedules and 3 000 to 9 999 on the sweep's found no run whose timer gaps differ in
/// number with D-134's arm and without it, as D-134 found none in seeds 0 to 999 on its own tree.
/// So the pin is the absence on its seed, with its reason ([`absent_from_both`]), and the day either
/// schedule makes the arm matter on it again this says so; the pair stays D-134's two hand-built
/// replays in `sim/raft.rs`, which no schedule moves.
// PROPOSED(D-134): the timer check exempts a hold however it ends.
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
// PROPOSED(D-135): on both schedules.
#[test]
fn seed_16725_a_hold_let_go_with_no_switch_is_held_to_its_release_by_the_timer_check() {
    absent_from_both(
        16725,
        "a timer gap D-134's arm excuses, inside a hold let go with no switch",
        |report| {
            report.checked.timer_gaps(raft::TimerResets::ALL).len()
                != report
                    .checked
                    .timer_gaps(raft::TimerResets::WITHOUT_RELEASED_HOLD)
                    .len()
        },
    );
}

/// Seed 18925, D-092's check on an asker that had not asked, found past the nightly's range
/// (D-131). Right after the restart arm's transfer the split's asker bounced between nodes on
/// `NotLeader`; its twelve asks went on those bounces, and its first `NoRangeId` came on the last, so
/// it made no retry and the check reported it as stopping with no answer: a report of the asker's
/// count, not of the system. Its first `NoRangeId` now starts the count again, so a retry is made.
/// The mechanism asserted: the arm's split's asker sent all twelve asks before its first refusal, the
/// retry came after it, and the answer came within D-092's bound, which is unchanged.
///
/// `Fault::MetaReorder` moved it (PROPOSED D-135): drawn first of eight faults, the arm split range
/// 2 twice and reordered meta's updates, and on the sweep's schedule the restart arm finds no range
/// that can still split and asks no split at all. So the pin runs the schedule it was found on and
/// asserts that absence: of its own situation, the arm's asker spending its asks before its first
/// `NoRangeId`, which here came from node 1, not from the restarted node 3 (corrected after #240's
/// review, where the absence named seed 992's situation and held on either schedule).
///
/// **PROPOSED D-127 moved the seed off the shape**, its answers to the chunks a node holds
/// moving every node schedule: no asker of the restart arm's split spends all its asks before its
/// first `NoRangeId` on this seed now. The pin is the absence with its reason, and the day an asker
/// does again this says so; the asker's count starting again at its first `NoRangeId` is D-131's
/// and no schedule moves it. At the Stage C integration (D-127 beside D-135) the absence is
/// asserted on both schedules, the shaped one D-127 audited and the sweep's.
// D-131
// PROPOSED(D-127): the schedule moved off the shape; the absence, with its reason.
// PROPOSED(D-135): on both schedules.
#[test]
fn seed_18925_an_asker_whose_tries_went_on_bounces_still_retries_after_its_first_no_range_id() {
    absent_from_both(
        18925,
        "the restart arm's asker spending its asks before its first `NoRangeId`",
        restart_arms_asker_spent_its_asks_before_its_first_no_range_id,
    );
}

fn node_id(node: u64) -> Option<NodeId> {
    Some(NodeId::new(u32::try_from(node).expect("small")))
}
