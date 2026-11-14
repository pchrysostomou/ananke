//! `sim/shard.rs`, the sharded sweep (SHARD.md §10; §12, Stage C): five nodes, three of
//! them the bootstrap nodes, ranges made by the splits the driver draws on stream
//! `shard`, leadership transfers, four clients over sixteen keys, and Phase 2's network
//! and disk faults.
//!
//! What it asserts, at every tier: the correct system passes every seed under every
//! check `sim/shard.rs`'s `Report::check` makes — checks 1 to 4 keyed by range, the range
//! layer's folds (7, 8, 9, 10, 16, 17, 18, 20), linearizability of every key across every
//! split, pre-vote's property, on uniform seeds the timer check, the write bound per key
//! and per range the run ended with and the meta convergence bound — that nodes 4 and 5
//! hold nothing, and that each of the workload's shapes reached its window (PROPOSED D-131).
//! It prints the driver's work, the refusals by the loss that made them (#45), the empty blocks
//! a split finds (D-092), the shapes' rates and the bounds' margins.
// PROPOSED(D-113): `sim/shard.rs`, Stage C's sharded sweep.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ananke_env::sim::TraceRecord;
use ananke_env::{NodeId, RangeCause, RecoveredAs, TraceEvent};
use ananke_raft::core::{Variant, Variants};
use ananke_raft::message::Message;
use ananke_shard::invariants::{self, Fold};
use ananke_shard::range_variant::{RangeVariant, RangeVariants};
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::raft;
use ananke_sim::shard::{self, DriverStats, DroppedTable, Load, Loss, NoRangeId};
use ananke_sim::{lin, seeds, sweep, verdict, write_trace};

/// What one seed of the sweep hands back: its verdict and its coverage.
struct Seen {
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
    /// Whether every key the fixed map would ask is asked by the write bound.
    keys_asked: Result<(), String>,
    slowest_write: Option<Duration>,
    recovery_from_heal: Option<Duration>,
    recovery_from_call: Option<Duration>,
    /// The whole keyspace's first completion from the heal, where it is asked: the reading
    /// from the heal (PROPOSED D-113 §5).
    keyspace_from_heal: Option<Duration>,
    /// How long each asker D-092's assertion asks of took (PROPOSED D-113 §8).
    retries_asked: Vec<Duration>,
    right_half: bool,
    partitions: usize,
    blocks: usize,
    crashes: usize,
    lease_reads: usize,
    placeholders: usize,
    /// What the workload's shapes reached on this seed (PROPOSED D-131).
    shapes: shard::Shapes,
    /// Which shapes this seed drew: a write burst, a read burst, the stale cache, the restart.
    drawn: [bool; 4],
}

/// SHARD.md §8's write bound, as `raft::Report::liveness_by_final_range` asks it, keeps the
/// fixed map's keys (`raft::Report::liveness`, under the map that sends every key to its first
/// range): where the first range has a majority up, the range serving every key the clients wrote
/// after the heal has one too, so every key the old reading asks is still asked, under the same
/// constant. It is about which keys are asked and not about when a reading's clock starts; the
/// reading from the heal, kept beside the one from each range's first call, is what answers for
/// that (PROPOSED D-113 §5).
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
    let verdict = report.check();
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
        right_half: invariants::folded(report.records())
            .verdict_of(Fold::RightHalf)
            .is_err(),
        partitions: count(|e| matches!(e, TraceEvent::PartitionStarted { .. })),
        blocks: count(|e| matches!(e, TraceEvent::LinkBlocked { .. })),
        crashes: count(|e| matches!(e, TraceEvent::NodeCrashed { .. })),
        lease_reads: count(|e| matches!(e, TraceEvent::RaftRead { lease: true, .. })),
        placeholders: count(|e| matches!(e, TraceEvent::RangeReplicaCreated { .. })),
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
        verdict,
    }
}

/// The correct system, under every check, at every tier. Seeds 3870 and 5303, the two the nightly
/// failed on with a committed entry lost (#175), pass since a right half made on a quarantined
/// parent takes the quarantine (D-130), and are pinned below on that mechanism.
/// Seeds 2272 and 7949, a right half with no possible majority, pass under the owner's ruling
/// of 2026-11-09, which excuses exactly that shape from the meta convergence bound and from
/// nothing else, and which this prints (#220; PROPOSED D-113 §6, §11).
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
    println!(
        "shard: D-092's asker: {} askers refused `NoRangeId`, {} of them still asking at the last \
         fault's heal on a uniform seed with their ranges live; the slowest of those took {:?}",
        seen.iter().map(|s| s.driver.retried.len()).sum::<usize>(),
        retries.len(),
        retries.iter().max(),
    );
    println!(
        "shard: check 8's right-half clause, folded and not asserted (PROPOSED D-107), trips on \
         {}/{count} seeds",
        seen.iter().filter(|s| s.right_half).count()
    );

    // PROPOSED(D-131): each shape, seeds that drew it and seeds that reached its window, each
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
    // PROPOSED(D-131): the shapes, each asserted reached at every tier, each well above D-061's
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

    let inclusion: Vec<Result<(), String>> = seen.iter().map(|s| s.keys_asked.clone()).collect();
    verdict(&inclusion).unwrap();
    let verdicts: Vec<Result<(), String>> = seen.into_iter().map(|s| s.verdict).collect();
    verdict(&verdicts).unwrap();
}

// --- The pair rule on this sweep (CLAUDE.md; SHARD.md §10): the correct system above, and
// below the variants the sweep's own faults, driver and workload shapes (PROPOSED D-131) reach,
// each caught by the check that names it. S12 brings `Fault::MetaReorder`.

/// A variant's run of one seed: what the verdict said, and what each of the range layer's
/// folds found on its own (PROPOSED D-106), since the verdict names only the first.
struct Under {
    why: Option<String>,
    tripped: BTreeSet<Fold>,
    splits: usize,
    /// The splits that took an id from a block of an earlier run of its holder.
    from_an_earlier_run: usize,
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

/// Runs `seeds` seeds under the three sets and prints the rate of every check that spoke.
fn under(
    name: &str,
    seeds: u64,
    cores: Variants,
    node: NodeVariants,
    range: RangeVariants,
) -> Vec<Under> {
    let found = sweep(seeds, |seed| {
        let report = shard::run_of(seed, cores, node, range);
        Under {
            why: report.check().err(),
            tripped: invariants::folded(report.records())
                .verdicts()
                .into_iter()
                .filter_map(|(fold, verdict)| verdict.is_err().then_some(fold))
                .collect(),
            splits: report.splits(),
            from_an_earlier_run: shard::splits_from_a_block_of_an_earlier_run_of(report.records()),
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
    )
}

fn core_variant(name: &str, variant: Variant) -> Vec<Under> {
    under(
        name,
        share(),
        variant.into(),
        NodeVariants::correct(),
        RangeVariants::correct(),
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
/// says (PROPOSED D-113 §5). Each is under D-061's 5 %, so each is asserted over the thousand and
/// printed below it.
// PROPOSED(D-113): §10's second catch, and the reading from the heal on a variant.
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
/// which reaches this window and refuses every such read at serving: PROPOSED D-131): 57 of 100
/// seeds and 13 of the gate's 20 by check 9, the variant's named catch, at a `RaftRead` served for a right-half key at
/// or past the split it let in. Every failure is that catch, and none trips first on anything else,
/// so the words are asserted of every one.
// PROPOSED(D-131)
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
// PROPOSED(D-131)
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

/// §10's `IdBlockResumed`: 42 of 100 seeds and 4 of the gate's 20 by check 18, on the restart arm's shape
/// (`Fault::SplitFromRestarted`, PROPOSED D-131), where it was 10 of 100 on the restarts the faults
/// drew and the splits that landed on them by chance before the arm. So the catch is asserted at
/// every tier, in the words of §12's "wrong if": a tier that runs this and catches nothing says
/// the sweep does not reach the variant. The shape the variant aims at is asserted too: a split that
/// took an id from a block of an earlier run of its node
/// (`shard::splits_from_a_block_of_an_earlier_run_of`), on 62 of 100 seeds and 8 of the gate's 20. The correct node's new run
/// holds none, so that is the variant seen firing.
// PROPOSED(D-113): §10, the shape at every tier.
// PROPOSED(D-131): §12's "wrong if", at every tier the arm reaches it.
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
}

/// §10's `MetaOverwritesByArrival` on what this sweep's drops and resends do by themselves:
/// 68 of 1 000 seeds by check 16, and 12 of 1 000 by the meta convergence bound, so both are
/// asserted over the thousand and printed below it (D-061). `Fault::MetaReorder` is S12's. The
/// bound is asked of the run taken to its end, which check 16 would otherwise stop first. What
/// every tier asserts is the shape the variant aims at: a split's update stored again over a key
/// meta already named at its generation or a newer one (`shard::meta_resends_stored_of`), 988 of
/// 1 000 seeds and 19 of the gate's 20. The correct meta range stores none, so that is the variant
/// seen firing.
// PROPOSED(D-113): §10, the shape at every tier.
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
        )
    });
    let sixteen = found.iter().filter(|f| f.0).count();
    let bound = found.iter().filter(|f| f.1).count();
    let reached = found.iter().filter(|f| f.2).count();
    println!(
        "shard: MetaOverwritesByArrival, each run to its end: a split's update stored over a key \
         meta named at its generation or a newer one on {reached}/{run} seeds, check 16 on \
         {sixteen}/{run}, the meta convergence bound on {bound}/{run}"
    );
    assert!(
        reached > 0,
        "no split's update was stored over a key meta named at its generation or a newer one: \
         the resend MetaOverwritesByArrival needs was not reached"
    );
    if seeds() >= 1000 {
        assert!(
            sixteen > 0,
            "no update overwrote a newer one over the thousand"
        );
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
#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught_on_the_sharded_sweep() {
    let found = core_variant("ResetTimerOnAnyRpc", Variant::ResetTimerOnAnyRpc);
    assert!(
        said(&found, "timers:") > 0,
        "no timer stayed reset by a stale sender"
    );
}

/// The disk's half of the fault model: Q15's whole-node refusal, which the rot reaches on 401
/// of 1 000 seeds. A node that refuses one range alone goes on serving the others over an
/// engine that lost state: 18 of 100 seeds by state machine safety.
#[test]
fn a_node_that_refuses_only_one_range_is_caught_by_the_disks_rot_on_the_sharded_sweep() {
    let found = under(
        "RefuseOneRangeOnly",
        share(),
        Variants::default(),
        NodeVariants::of(&[NodeVariant::RefuseOneRangeOnly]),
        RangeVariants::correct(),
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
/// 100 seeds, by this check and no other.
// PROPOSED(D-092): answer A; the asker retries.
// PROPOSED(D-113): §8's pair for the check read from the heal.
#[test]
fn a_node_that_does_not_resend_its_refill_leaves_the_askers_retries_unanswered() {
    let found = range_variant("RefillNotResent", share(), RangeVariant::RefillNotResent);
    assert!(
        said(&found, "with no answer") > 0,
        "no asker's retries after the heal went unanswered"
    );
}

/// The write bound read from the heal, D-076's "how long after the heal the range became writable
/// at all", kept beside the arm read from each range's first call (PROPOSED D-113 §5). The clients
/// are cut off from every node for the bound and 50 ms more of the 2.2 s window, the heal not moved:
/// a cluster nothing can write to for longer than the bound after the heal. No range's first call
/// in the history comes before the cut ends, so the arm from the first call is satisfied and this
/// arm is what is left to say so; where the 150 ms left complete nothing, the clients-wrote-nothing
/// arm speaks first. Above the 5 % of D-061 on every tier: 41 of 100 seeds, a uniform 41 of 50.
// PROPOSED(D-113): §5's pair for the reading from the heal.
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
/// its refill lands (SHARD.md §5's second rule). The split is refused `NoRangeId` (PROPOSED
/// D-092's A); range 0 grants the block within a minimum election timeout; the asker's retry
/// gets its answer. What the block's size cannot change (PROPOSED D-113 §8).
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
/// (PROPOSED D-113 §8).
#[test]
fn seed_368_a_grant_whose_answer_has_not_landed_still_refuses_a_split() {
    let report = pinned(368);
    report.check().unwrap();
    assert!(report.refusals().is_empty(), "no store refused");
    assert!(report.checked.uniform(), "uniformly scheduled");
    let empties = report.no_range_ids();
    assert_eq!(empties.len(), 1, "one `NoRangeId`: {empties:?}");
    let empty = empties[0];
    assert_eq!(empty.node, 2);
    assert!(empty.restarted.is_some(), "its node restarted before it");
    assert!(
        empty.leased_since_restart,
        "the block was granted before the answer"
    );
    assert_eq!(empty.taken, 0, "and no id had been taken from it");
    assert!(
        report.driver.retried.iter().all(|retried| retried.answered),
        "the asker's retry got its answer: {:?}",
        report.driver.retried
    );
    // The refusal came after the last fault healed, so this is an asker D-092's assertion asks
    // of, and it is answered; `retries_take_effect` has a case, here, to read.
    let asked = report.retries_asked();
    assert_eq!(
        asked.len(),
        1,
        "seed 368's asker is asked of: {:?}",
        report.driver.retried
    );
    assert!(
        asked[0].refused >= report.last_fault_healed,
        "refused at {:?}, the last fault healed at {:?}",
        asked[0].refused,
        report.last_fault_healed
    );
}

/// Seed 41, the first seed the sweep failed on (D-113, (a)): node 2 is refused, its placeholder
/// of the right half 19 is installed, and then its re-seeded range 2 is, whose switch replaced
/// configuration's `[k, l)` and took 19's keys with it, so node 2 later led 19's right half and
/// answered `k15` with nothing. Since #200 the switch replaces the snapshot's span and the run
/// is green; under `InstallReplacesHostedSpan` check 8's install clause names that switch
/// (PROPOSED D-128). Since D-130 a right half made on a quarantined parent is quarantined, which
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
/// reasons; seed 463 carries it now.
#[test]
fn seeds_128_643_and_986_no_longer_reach_a_leader_that_cannot_confirm_a_read() {
    for seed in [128, 643, 986] {
        let report = pinned(seed);
        report.check().unwrap();
        assert_eq!(
            report.refusals().len(),
            1,
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

/// Seed 463, D-113's (b) where it stands since D-130 moved seed 986 off it, as #200 moved seeds
/// 128 and 643: two stores refused and a leader left unable to confirm a read, which it refuses
/// at the step and drops (PROPOSED D-129). Under `UnconfirmableReadKept` nothing answers them,
/// the clients' retries pile up and the node fails itself at `READS_OUTSTANDING`.
#[test]
fn seed_463_a_leader_refuses_the_reads_it_can_never_confirm() {
    let report = pinned(463);
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
        463,
        Variant::UnconfirmableReadKept,
        NodeVariants::correct(),
        RangeVariants::correct(),
    )
    .check()
    .expect_err("the variant fails seed 463");
    assert!(
        why.contains("registered reads"),
        "the violation seed 463 is pinned for: {why}"
    );
}

/// The node that traced `record`, as the checks number it.
fn node_of(record: &TraceRecord) -> Option<u64> {
    record.node.map(|node| u64::from(node.get()))
}

/// Seed 3291, the sweep's first failure of check 20 on the correct system, at the nightly's ten
/// thousand. It is the window seed 88 showed the payload oracle (PROPOSED D-113 §5): node 3
/// commits the split of range 27 at 122, crashes with the apply's batch durable and neither
/// `RangeSplit` nor `RangeCreated` traced, and at its restart restates a replica of the right
/// half, 30, before any node has traced that split. It then grants for it, from a core and not
/// from a placeholder, and no node ever hears of a creation on it. Check 20 read only
/// `RangeCreated`, and named the grant a node's that holds no replica; with the trace's
/// `RangesRestated` taken out, the fold says so again.
#[test]
fn seed_3291_a_replica_restated_before_any_node_traced_its_split_may_grant() {
    let (node, parent, right, split) = (3, 27, 30, 122);
    let report = pinned(3291);
    report.check().unwrap();
    let records = report.records();
    let on_node = |record: &TraceRecord| node_of(record) == Some(node);
    let after = |from: usize, what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records[from..]
            .iter()
            .position(want)
            .map(|at| from + at)
            .unwrap_or_else(|| panic!("seed 3291 no longer reaches {what}: read its trace"))
    };
    let at = |what: &str, want: &dyn Fn(&TraceRecord) -> bool| after(0, what, want);
    let committed = at("node 3's commit of the split", &|record| {
        on_node(record)
            && matches!(record.event, TraceEvent::RaftCommit { range, index, .. }
                if range == parent && index == split)
    });
    let crashed = after(committed, "node 3's crash", &|record| {
        matches!(record.event, TraceEvent::NodeCrashed { node: crashed }
            if u64::from(crashed.get()) == node)
    });
    let recovered = at("the restatement of the right half", &|record| {
        matches!(record.event, TraceEvent::RaftRecovered { server, range, applied, .. }
            if server == node && range == right && applied == split)
    });
    let restated = at("`RangesRestated` naming the right half", &|record| {
        on_node(record)
            && matches!(&record.event, TraceEvent::RangesRestated { ranges }
                if ranges.contains(&right))
    });
    let first_traced = at("the split's first trace, on any node", &|record| {
        matches!(record.event, TraceEvent::RangeSplit { right: traced, .. }
            | TraceEvent::RangeCreated { range: traced, .. } if traced == right)
    });
    let granted = at("node 3's grant", &|record| {
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
        "node 3 traced the split, a creation or a placeholder of range 30 before the grant: \
         the window is gone from this seed"
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
        old_reading.contains("node 3 granted range 30's pre-vote")
            && old_reading.contains("from a node that holds no replica of it"),
        "the violation the old reading made of it: {old_reading}"
    );
}

/// Seed 8934, the sweep's first failure of the match-starts fold on the correct system, at the
/// nightly's ten thousand. Node 2's placeholder of range 43 answers leader 1 with incarnation 0,
/// node 2 applies the split that makes the replica, and the new store's first answer overtakes
/// the placeholder's. So the leader traces a first rise under the new store's number, a reset to
/// 0 at the placeholder's answer, a reset back at the store's next and a first rise again. D-042
/// names the 0 delayed past a store's first answer as a change that costs a round trip. The
/// fold said a retired incarnation had come back; the fold never read a reset, so with the
/// trace's `RaftProgressReset`s taken out it says so again (PROPOSED D-113 §5).
#[test]
fn seed_8934_a_placeholders_answer_overtaken_by_the_new_stores_begins_the_tracking_afresh() {
    let (leader, follower, range) = (1, 2, 43);
    let report = pinned(8934);
    report.check().unwrap();
    let records = report.records();
    let on_follower = |record: &TraceRecord| node_of(record) == Some(follower);
    let placeholder = records.iter().position(|record| {
        on_follower(record)
            && matches!(record.event, TraceEvent::RangeReplicaCreated { range: of } if of == range)
    });
    let (created, store) = records
        .iter()
        .enumerate()
        .find_map(|(at, record)| match record.event {
            TraceEvent::RangeCreated {
                range: of,
                incarnation,
                ..
            } if of == range && on_follower(record) => Some((at, incarnation)),
            _ => None,
        })
        .expect("seed 8934 no longer creates range 43 on node 2: read its trace");
    assert!(
        placeholder.is_some_and(|made| made < created),
        "node 2 made no placeholder of range 43 before its replica"
    );

    let tracked: Vec<(usize, &str, u64)> = records
        .iter()
        .enumerate()
        .filter(|(_, record)| node_of(record) == Some(leader))
        .filter_map(|(at, record)| match record.event {
            TraceEvent::RaftProgressReset {
                range: of,
                follower: f,
                incarnation,
                ..
            } if of == range && f == follower => Some((at, "reset", incarnation)),
            TraceEvent::RaftMatchStarted {
                range: of,
                follower: f,
                incarnation,
                ..
            } if of == range && f == follower => Some((at, "rise", incarnation)),
            _ => None,
        })
        .collect();
    let shape: Vec<(&str, u64)> = tracked.iter().map(|&(_, what, of)| (what, of)).collect();
    assert_eq!(
        shape,
        [
            ("reset", store),
            ("rise", store),
            ("reset", 0),
            ("reset", store),
            ("rise", store)
        ],
        "seed 8934's leader no longer tracks node 2 through a delayed 0: read its trace"
    );
    let (first_rise, zero, second_rise) = (tracked[1].0, tracked[2].0, tracked[4].0);

    // A zero-stamped answer about this range, from the follower.
    let zero_stamped = |record: &TraceRecord| match &record.event {
        TraceEvent::MessageSent { id, payload, .. }
            if on_follower(record)
                && raft::messages_of(payload).iter().any(|(of, message)| {
                    *of == range
                        && matches!(
                            message,
                            Message::AppendEntriesResponse { incarnation: 0, .. }
                        )
                }) =>
        {
            Some(*id)
        }
        _ => None,
    };
    let placeholders_answers: BTreeSet<_> =
        records[..created].iter().filter_map(zero_stamped).collect();
    assert!(
        records[first_rise..zero].iter().any(|record| matches!(
            &record.event,
            TraceEvent::MessageDelivered { id, .. } if placeholders_answers.contains(id)
        )),
        "no answer the placeholder sent before the replica existed reached the leader between \
         the first rise and the reset to 0"
    );
    assert!(
        !records[created..second_rise]
            .iter()
            .any(|record| zero_stamped(record).is_some()),
        "node 2 stamped 0 on an answer after its store existed, so the 0 was not a delayed one"
    );
    assert!(
        !records[created..second_rise].iter().any(|record| matches!(
            &record.event,
            TraceEvent::RaftRefused { server, .. } if *server == follower
        ) || (on_follower(record)
            && matches!(record.event, TraceEvent::RangeRemoved { range: of, .. } if of == range))),
        "node 2's store went away between the two rises: that is not this shape"
    );

    let without: Vec<TraceRecord> = records
        .iter()
        .filter(|record| !matches!(record.event, TraceEvent::RaftProgressReset { .. }))
        .cloned()
        .collect();
    let old_reading = raft::match_starts_are_first_rises(&without)
        .expect_err("the reset to 0 is what the fold excuses this repeat by");
    assert!(
        old_reading.contains(&format!(
            "leader 1 of term 2 of group 43 traced a second first rise of 2's match under \
             incarnation {store}"
        )),
        "the violation the old reading made of it: {old_reading}"
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
// PROPOSED(D-130)
#[test]
fn seeds_3870_and_5303_a_right_half_remade_on_a_quarantined_parent_keeps_its_committed_entries() {
    type Row<'a> = (
        u64,
        u64,
        u64,
        u64,
        u64,
        std::ops::RangeInclusive<u64>,
        std::ops::RangeInclusive<u64>,
        &'a str,
    );
    let pins: [Row<'_>; 2] = [
        (
            3870,
            11,
            19,
            106,
            103,
            107..=115,
            108..=115,
            "leader completeness: index 107 of group 19 (term 2) was committed in term 2 but \
             server 3, leader of term 4, does not hold it",
        ),
        (
            5303,
            43,
            44,
            87,
            83,
            88..=89,
            89..=89,
            "leader completeness: index 89 of group 44 (term 2) was committed in term 2 but \
             server 1, leader of term 5, does not hold it",
        ),
    ];
    let node = 3;
    for (seed, parent, right, split, below, committed, reapplied, named) in pins {
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
        // The loss, and what it lost: node 3 had made the right half and voted in it.
        let refused = at(
            0,
            "node 3's refusal",
            &|record| matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node),
        );
        let first_made = at(0, "node 3's first creation of the right half", &|record| {
            on_node(record)
                && matches!(record.event, TraceEvent::RangeCreated {
                    range,
                    cause: RangeCause::Split,
                    ..
                } if range == right)
        });
        assert!(first_made < refused, "seed {seed}: made before its loss");
        assert!(
            records[first_made..refused].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftVote { server, range, granted: true, .. }
                    if server == node && range == right
            )),
            "seed {seed}: node 3's lost replica granted no vote: nothing was lost"
        );
        // The re-seed below the split, the replay, and the second creation, stated quarantined.
        let installed = at(
            refused,
            "node 3's parent installed below the split",
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
        let replayed = at(installed, "node 3's replay of the split", &|record| {
            on_node(record)
                && matches!(record.event, TraceEvent::RangeSplit {
                    range,
                    right: of,
                    index,
                    ..
                } if range == parent && of == right && index == split)
        });
        let remade = at(replayed, "node 3's second creation", &|record| {
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
        // The committed entries survive: node 3 had them committed before its loss, the first
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
            "seed {seed}: what node 3 had committed before its loss"
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
}

/// Seed 2097: a quarantine a right half took at its split's apply is durable and a restart
/// restates it. Node 2 is refused and re-seeded, applies a split of range 2 on its quarantined
/// replica of it, and range 27 takes the quarantine in the split's batch and states it. Node 2
/// then crashes and restarts on the same store, which says quarantined, and the restart says
/// so. Under `SplitLeavesQuarantineBehind` the same restart states range 27 with no quarantine,
/// and the replica has already voted (PROPOSED D-130).
// PROPOSED(D-130)
#[test]
fn seed_2097_a_restart_restates_the_quarantine_a_right_half_took_at_its_split() {
    let (node, parent, right, split) = (2, 2, 27, 143);
    let correct = pinned(2097);
    correct.check().unwrap();
    let records = correct.records();
    assert_eq!(correct.refusals().len(), 1, "one store refused");
    assert_eq!(correct.refusals()[0].0, node, "node 2's");
    let at = |from: usize, what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records[from..]
            .iter()
            .position(want)
            .map(|found| from + found)
            .unwrap_or_else(|| panic!("seed 2097 no longer reaches {what}: read its trace"))
    };
    let refused = at(
        0,
        "node 2's refusal",
        &|record| matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node),
    );
    let made = at(refused, "the split's apply on node 2", &|record| {
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
    let crashed = at(stated, "node 2's crash", &|record| {
        matches!(record.event, TraceEvent::NodeCrashed { node: gone }
            if u64::from(gone.get()) == node)
    });
    let restarted = at(crashed, "node 2's restart", &|record| {
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
        2097,
        Variants::default(),
        NodeVariants::of(&[NodeVariant::SplitLeavesQuarantineBehind]),
        RangeVariants::correct(),
    );
    variant
        .check()
        .expect("the variant's named failures are 3870's and 5303's");
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

/// Seed 9: a right half made on a quarantined right half takes the quarantine too, so a chain of
/// splits carries it down. Node 2 is refused and the re-seed quarantines its replica of range 2.
/// It applies range 2's split at 57 and range 3 takes the quarantine. A live install on range 3
/// builds no new store and leaves it quarantined. It applies range 3's split at 76 and range 4, the
/// second generation, takes the quarantine from a parent a split made; and it applies range 4's at
/// 90 and range 5 takes it from a parent that took no install. Each is stated quarantined at its
/// creation and, asked for a vote, grants none. Only this pin fails if a split-made parent stops
/// passing the quarantine on: the store's test shows a right half reopens quarantined, the other
/// pins that a quarantined parent passes it on, and the sweep's checks pass either way. Under
/// `SplitLeavesQuarantineBehind` the same three replicas have no quarantine, and vote (PROPOSED
/// D-130).
// PROPOSED(D-130)
#[test]
fn seed_9_a_right_half_made_on_a_quarantined_right_half_takes_the_quarantine_too() {
    let node = 2;
    // The links, in order: the parent on node 2, the right half it makes and the split's index.
    let chain = [(2, 3, 57), (3, 4, 76), (4, 5, 90)];
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
    let correct = pinned(9);
    correct.check().unwrap();
    let records = correct.records();
    assert_eq!(correct.refusals().len(), 1, "one store refused");
    assert_eq!(correct.refusals()[0].0, node, "node 2's");
    let at = |from: usize, what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records[from..]
            .iter()
            .position(want)
            .map(|found| from + found)
            .unwrap_or_else(|| panic!("seed 9 no longer reaches {what}: read its trace"))
    };
    let refused = at(
        0,
        "node 2's refusal",
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
    // Each statement is the creation's and not a restart's, and the chain is the one pinned: range
    // 3's quarantine came through a live install on the store its split built, and range 4's store
    // is the one its split built with no install on it before it split in turn.
    assert!(
        !records[refused..from].iter().any(|record| matches!(
            record.event,
            TraceEvent::NodeCrashed { node: gone } if u64::from(gone.get()) == node
        )),
        "node 2 crashed inside the chain, so a restart may have stated it"
    );
    let installed = |range: u64, between: std::ops::Range<usize>| {
        records[between].iter().any(|record| {
            matches!(record.event, TraceEvent::RaftSnapshot { server, range: of, taken: false, .. }
                if server == node && of == range)
        })
    };
    assert!(
        installed(3, made_at[0]..made_at[1]),
        "range 3 took no live install before its split"
    );
    assert!(
        !installed(4, made_at[1]..made_at[2]),
        "range 4 took an install before its split"
    );

    let variant = pinned_of(
        9,
        Variants::default(),
        NodeVariants::of(&[NodeVariant::SplitLeavesQuarantineBehind]),
        RangeVariants::correct(),
    );
    variant
        .check()
        .expect("the variant's named failures are 3870's and 5303's");
    let on_variant = variant.records();
    for (parent, right, split) in chain {
        let made = on_variant
            .iter()
            .rposition(|record| made_on(record, parent, right, split))
            .expect("the variant makes the same right halves on node 2");
        assert!(
            on_variant[made..]
                .iter()
                .any(|record| granted_on(record, right)),
            "range {right}: under the variant the replica votes"
        );
        assert!(
            !on_variant[made..].iter().any(|record| matches!(
                record.event,
                TraceEvent::RaftRecovered { server, range, state: RecoveredAs::Quarantined, .. }
                    if server == node && range == right
            )),
            "range {right}: the variant quarantined the replica after all"
        );
    }
}

// --- The owner's ruling of 2026-11-09 (#220; PROPOSED D-113 §6): a right half the correct system
// leaves wedged for good, excused from the meta convergence bound and from nothing else.

/// Seeds 2272 and 7949, the two seeds of ten thousand on which the correct system fails the meta
/// convergence bound without the exception. Node 3 alone ever leads the right half, crashes
/// before meta applied its update and is refused; node 1 and node 3 hold only placeholders, whose
/// parent reached the split by an install; and node 2 alone is initialised and stays a
/// pre-candidate for ever. Range 36 of seed 7949, a child of its range 35, has voters that hold
/// no parent at all, and meta had named it, so excusing it changes no verdict. Each condition of
/// the ruling is asserted of the reading and the leaders of the trace, each range is live by the
/// majority rule all the same, and the bound says what it said before the ruling.
#[test]
fn seeds_2272_and_7949_a_right_half_with_no_possible_majority_is_excused_from_the_meta_bound() {
    use shard::Stuck::{NoParent, ParentPastTheSplit as Past};
    for (seed, first_failure, pins) in [
        (
            2272,
            36,
            vec![(36, 2, 119, [(1, Past(119)), (3, Past(122))])],
        ),
        (
            7949,
            35,
            vec![
                (35, 2, 89, [(1, Past(107)), (3, Past(124))]),
                (36, 35, 102, [(1, NoParent), (3, NoParent)]),
            ],
        ),
    ] {
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

/// Seed 707, the first shape-drawn seed the correct system failed on, and a defect of the log
/// view and not of the system (PROPOSED D-131). Node 1 restarted with its log of range 19 compacted
/// to 119 and one entry at 120, and its next restart was refused: the re-seed's floor is 112, below
/// both. The store that held 120 is gone, but the view kept that entry with nothing under it, so
/// node 2's restated 120 read as agreeing with a log that differs at 119, though every entry any
/// node traced at 119 and at 120 carries the same hash. The view now forgets a replica's entries at
/// its creation; with node 1's creation taken out of the trace it says what it said before.
// PROPOSED(D-131)
#[test]
fn seed_707_a_reseed_below_a_compacted_tail_leaves_nothing_of_the_lost_store_in_the_log_view() {
    let (node, range) = (1, 19);
    let report = shard::run(707);
    report.check().unwrap();
    let records = report.records();
    let find = |what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records
            .iter()
            .position(want)
            .unwrap_or_else(|| panic!("seed 707 no longer reaches {what}: read its trace"))
    };
    let compacted = find(
        "node 1's restart with its log compacted to 119",
        &|record| {
            matches!(record.event, TraceEvent::RaftSnapshot {
            server, range: of, last_index: 119, taken: false, ..
        } if server == node && of == range)
        },
    );
    let refused = find(
        "node 1's refusal",
        &|record| matches!(record.event, TraceEvent::RaftRefused { server, .. } if server == node),
    );
    let created = find("node 1's re-seed at 112", &|record| {
        node_of(record) == Some(node)
            && matches!(record.event, TraceEvent::RangeCreated {
                range: of, cause: RangeCause::Snapshot, floor_index: 112, ..
            } if of == range)
    });
    let restated = find("node 2's 120 restated after the re-seed", &|record| {
        matches!(record.event, TraceEvent::RaftAppend {
            server: 2, range: of, index: 120, ..
        } if of == range)
            && record.at > records[created].at
    });
    assert!(
        compacted < refused && refused < created && created < restated,
        "the shape's order: compacted {compacted}, refused {refused}, re-seeded {created}, \
         restated {restated}"
    );
    let hashes_at = |index: u64| -> BTreeSet<u64> {
        records
            .iter()
            .filter_map(|record| match record.event {
                TraceEvent::RaftAppend {
                    range: of,
                    index: at,
                    hash,
                    ..
                } if of == range && at == index => Some(hash),
                _ => None,
            })
            .collect()
    };
    assert_eq!(hashes_at(119).len(), 1, "every copy of 119 is one entry");
    assert_eq!(hashes_at(120).len(), 1, "every copy of 120 is one entry");
    let without: Vec<ananke_raft::invariants::Traced<'_>> = records
        .iter()
        .enumerate()
        .filter(|(at, _)| *at != created)
        .map(|(_, record)| ananke_raft::invariants::Traced::new(node_of(record), &record.event))
        .collect();
    let old_reading = ananke_raft::invariants::log_matching(without)
        .expect_err("the creation is what the view forgets the lost store by");
    assert!(
        old_reading.contains(
            "servers 2 and 1 agree at index 120 of group 19 term 3 but differ at index 119"
        ),
        "the violation the old reading made of it: {old_reading}"
    );
}

/// Seed 992, the other shape-drawn seed the correct system failed on, and a defect of the asker
/// and not of the system (PROPOSED D-131). The restart arm's split is refused `NoRangeId` on node
/// 1's new run, and its retry is proposed and applied 130 ms later, but the answer to the retry is
/// dropped by the network. A leader answers a split's entry once, at its apply, and a copy with
/// silence, so an asker that sent the same copy heard nothing for 3.2 s and D-092's bound said so
/// (`Report::retries_take_effect`). The bound is unchanged. The asker now asks anew after a silent
/// wait, is refused `KeyOutsideSpan` at once since the split has landed, and stops within it.
// PROPOSED(D-131)
#[test]
fn seed_992_an_asker_whose_answer_is_lost_asks_anew_and_is_answered_within_the_bound() {
    let report = shard::run(992);
    report.check().unwrap();
    assert_eq!(report.restart_arms.len(), 1, "{:?}", report.restart_arms);
    let arm = report.restart_arms[0];
    let seq = arm.seq.expect("the arm asked its split");
    assert_eq!(
        report.driver.retried.len(),
        1,
        "{:?}",
        report.driver.retried
    );
    let retried = report.driver.retried[0];
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
        .expect("seed 992's retry was proposed on the restarted node");
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
        "seed 992's answer was not dropped on the way to its asker: pin it on its mechanism"
    );
    assert_eq!(
        report.driver.refused.get("key-outside-span"),
        Some(&1),
        "the asker asked anew and was refused at once: {:?}",
        report.driver.refused
    );
    let bound = raft::election_max() * raft::LIVENESS_TIMEOUTS;
    assert!(
        retried.took(report.last_fault_healed) <= bound,
        "answered within D-092's bound: {retried:?}"
    );
}

/// Seed 5620, the third shape-drawn seed the correct system failed on, and this one a defect of the
/// system: a data loss D-132 fixes (PROPOSED D-131, D-132). The restart arm leaves node 3 behind on
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
// PROPOSED(D-131)
// PROPOSED(D-132)
#[test]
fn seed_5620_an_install_a_split_overtook_switches_nothing_and_leaves_the_right_half_its_keys() {
    let (node, parent, right, index) = (3, 3, 20, 113);
    let report = shard::run(5620);
    report.check().unwrap();
    assert!(
        report.restart_arms.iter().any(|arm| arm.victim == node),
        "the restart arm crashed node 3: {:?}",
        report.restart_arms
    );
    let records = report.records();
    let find = |what: &str, want: &dyn Fn(&TraceRecord) -> bool| {
        records
            .iter()
            .position(want)
            .unwrap_or_else(|| panic!("seed 5620 no longer reaches {what}: read its trace"))
    };
    let taken = find("node 1's snapshot of range 3 at the split", &|record| {
        matches!(record.event, TraceEvent::RaftSnapshot {
            server: 1, range: of, last_index, taken: true, ..
        } if of == parent && last_index == index)
    });
    let streamed = find("node 1's stream of range 3 to node 3", &|record| {
        matches!(record.event, TraceEvent::RaftSnapshotStreams {
            server: 1, range: of, to, ..
        } if of == parent && to == node && record.at >= records[taken].at)
    });
    let split = find("node 3's apply of the split", &|record| {
        node_of(record) == Some(node)
            && matches!(record.event, TraceEvent::RangeSplit {
                range: of, right: made, index: at, ..
            } if of == parent && made == right && at == index)
    });
    let created = find("node 3's replica of range 20", &|record| {
        node_of(record) == Some(node)
            && matches!(record.event, TraceEvent::RangeCreated {
                range: of, cause: RangeCause::Split, ..
            } if of == right)
    });
    assert!(
        taken < streamed && streamed < split && split < created,
        "the shape's order: taken {taken}, streamed {streamed}, split {split}, created {created}"
    );
    let variant = shard::run_of(
        5620,
        Variants::default(),
        NodeVariants::of(&[NodeVariant::InstallReplacesCopiedSpan]),
        RangeVariants::correct(),
    );
    let first = records
        .iter()
        .zip(variant.records())
        .position(|(correct, buggy)| correct != buggy)
        .expect("the variant changes the run");
    let switch = &variant.records()[first];
    assert!(
        created < first
            && node_of(switch) == Some(node)
            && matches!(switch.event, TraceEvent::TaskSpawned { name, .. } if name == "span-install"),
        "the two runs are one until the variant's switch on node 3, with range 20 made \
         (at {created}): they part at {first}, {switch:?}"
    );

    let (start, end) = match &records[created].event {
        TraceEvent::RangeCreated { start, end, .. } => (start.clone(), end.clone()),
        other => unreachable!("{other:?}"),
    };
    let own = |range: u64| {
        let interval = ananke_raft::store::KeyPrefix::group(range).span();
        (interval.start, interval.end)
    };
    let mut own_installs = 0;
    for record in &records[created..] {
        if node_of(record) != Some(node) {
            continue;
        }
        match &record.event {
            TraceEvent::SpanInstalled { spans, .. } => {
                assert!(
                    !spans.contains(&own(parent)),
                    "seed 5620 switched range 3 on node 3 after the split: {spans:?}"
                );
                if spans.contains(&own(right)) {
                    own_installs += 1;
                } else {
                    assert!(
                        !spans.iter().any(|(from, to)| *from < end && start < *to),
                        "a switch that was not range 20's replaced its keys: {spans:?}"
                    );
                }
            }
            TraceEvent::RangeRemoved { range, .. } => {
                assert_ne!(*range, right, "node 3 removed its replica of range 20");
            }
            _ => {}
        }
    }
    assert_eq!(own_installs, 1, "range 20's own install on node 3, at 115");
    assert!(
        !records.iter().any(|record| matches!(
            record.event,
            TraceEvent::RaftSnapshotStartOver { server, range: of, .. }
                if server == node && of == parent
        )),
        "the stream was answered, not refused: an install answered installed starts nothing over"
    );

    let why = variant.check().expect_err("the variant fails seed 5620");
    assert!(
        why.contains("one owner per key")
            && why.contains("an install of range 3 on node 3 replaced")
            && why.contains("k03\", ")
            && why.contains("k10\"), which overlaps its replica of range 20 over")
            && why.contains("k05\", ")
            && why.contains("and removes that replica's keys"),
        "the violation seed 5620 is pinned for: {why}"
    );
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
/// The mechanism asserted: the correct run and `SplitAppliesInsideTheSwitch`'s are one run up to the
/// moment node 2's `apply` task reaches the split, with the install numbered and not yet switched; there
/// the variant applies the split inside the switch, and the correct node waits. It waits out the switch
/// and the host's `Switched` too, which restates range 44's applied index to 167: the split is then the
/// install's, as the apply loop skips an entry the store is past, so node 2 applies no split at 167,
/// makes no range 3 from it, applies 168 once and after the restate, and gets range 3 through its
/// placeholder's install afterwards. The switch replaced `[k06, k14)` while node 2 held no replica of
/// range 3, and the snapshot's left half alone came back, so the keys it removed were no replica's on
/// the node. The variant fails with check 8's install clause on the original words, and with the
/// sweep's verdict on them.
// PROPOSED(D-133)
#[test]
fn seed_13283_a_split_inside_an_install_waits_for_switched_and_nothing_is_applied_twice() {
    let (node, parent, right, index) = (2, 44, 3, 167);
    let report = shard::run(13283);
    report.check().unwrap();
    let records = report.records();
    let variant = shard::run_of(
        13283,
        Variants::default(),
        NodeVariants::of(&[NodeVariant::SplitAppliesInsideTheSwitch]),
        RangeVariants::correct(),
    );
    let first = records
        .iter()
        .zip(variant.records())
        .position(|(correct, buggy)| correct != buggy)
        .expect("the variant changes the run");
    let on_node = |record: &TraceRecord| node_of(record) == Some(node);
    let own = {
        let interval = ananke_raft::store::KeyPrefix::group(parent).span();
        (interval.start, interval.end)
    };
    let replaces_44 = |record: &TraceRecord| {
        on_node(record)
            && matches!(&record.event, TraceEvent::SpanInstalled { spans, .. }
                if spans.contains(&own)
                    && spans.iter().any(|(from, to)| from.ends_with(b"k06") && to.ends_with(b"k14")))
    };
    let split_at = |record: &TraceRecord| {
        on_node(record)
            && matches!(record.event, TraceEvent::RangeSplit { range: of, right: made, index: at, .. }
                if of == parent && made == right && at == index)
    };
    // Up to where the two runs part, node 2's install of range 44 is numbered and not switched.
    let numbered = records[..first]
        .iter()
        .rposition(|record| {
            on_node(record)
                && matches!(record.event, TraceEvent::TaskSpawned { name, .. } if name == "span-install")
        })
        .unwrap_or_else(|| panic!("seed 13283 no longer numbers node 2's install before {first}"));
    assert!(
        !records[numbered..first]
            .iter()
            .any(|record| on_node(record)
                && matches!(record.event, TraceEvent::SpanInstalled { .. })),
        "node 2's switch was made before the runs part at {first}"
    );
    let switched = numbered
        + records[numbered..]
            .iter()
            .position(replaces_44)
            .unwrap_or_else(|| {
                panic!("seed 13283 no longer switches range 44's [k06, k14) on node 2")
            });
    let installed = switched
        + records[switched..]
            .iter()
            .position(|record| {
                matches!(record.event, TraceEvent::RaftSnapshot {
                    server, range: of, last_index, taken: false, ..
                } if server == node && of == parent && last_index == index)
            })
            .expect("node 2's install of range 44 at 167 is traced");
    // The variant: the split applies between the numbering and the switch.
    let buggy = variant.records();
    let landed = buggy
        .iter()
        .position(split_at)
        .expect("the variant applies range 44's split at 167 on node 2");
    let buggy_switch = buggy[numbered..]
        .iter()
        .position(replaces_44)
        .map(|at| numbered + at)
        .expect("the variant switches range 44 on node 2");
    assert!(
        first <= landed && landed < buggy_switch,
        "the variant's split lands inside the switch: parted at {first}, split at {landed}, \
         switch at {buggy_switch}"
    );
    // The correct node: no split at 167 on node 2, 168 applied once and after the restate, and
    // range 3 made through its placeholder after the switch.
    assert!(
        !records.iter().any(split_at),
        "node 2 applied range 44's split at 167: the entry is the install's"
    );
    let applies_168: Vec<usize> = records
        .iter()
        .enumerate()
        .filter(|(_, record)| {
            matches!(record.event, TraceEvent::RaftApply { server, range: of, index: at, .. }
                if server == node && of == parent && at == index + 1)
        })
        .map(|(at, _)| at)
        .collect();
    assert!(
        applies_168.len() == 1 && applies_168[0] > installed,
        "node 2 applies 168 once, after its install at 167 ({installed}): {applies_168:?}"
    );
    let made = records
        .iter()
        .position(|record| {
            on_node(record)
                && matches!(record.event, TraceEvent::RangeCreated { range: of, .. } if of == right)
        })
        .expect("node 2 makes range 3");
    assert!(
        made > switched
            && matches!(
                records[made].event,
                TraceEvent::RangeCreated {
                    cause: RangeCause::Snapshot,
                    ..
                }
            ),
        "range 3 made on node 2 through its placeholder, after the switch: {:?}",
        records[made]
    );

    let clause = invariants::folded(buggy)
        .verdict_of(Fold::InstallSpan)
        .expect_err("check 8's install clause catches the variant on seed 13283");
    assert!(
        clause.contains("one owner per key")
            && clause.contains("an install of range 44 on node 2 replaced")
            && clause.contains("k06\", ")
            && clause.contains("k14\"), which overlaps its replica of range 3 over")
            && clause.contains("k09\", ")
            && clause.contains("and removes that replica's keys"),
        "the violation seed 13283 is pinned for: {clause}"
    );
    let why = variant.check().expect_err("the variant fails seed 13283");
    assert!(
        why.contains("state machine safety: server 2 applied index 168 of group 44 after 168"),
        "the sweep's own verdict, the store taken back: {why}"
    );
}

/// Seed 16725, the stale-cache shape's writer, found past the nightly's range (PROPOSED D-131). Node
/// 1 led range 14, proposed the writer's put at index 100, applied it, and crashed before it
/// answered. The writer heard nothing, and asked the next node under the same (client, seq). A
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
/// This is not a bare green: the run's verdict names `timers`, which is not what this seed is pinned
/// for and not this pin's to assert. A hold on range 2 of node 3, taken at 7.681 s for an install
/// that ended in `Unusable` at 8.222 s with no switch, is measured against the timer's bound, since
/// the replay's exemption opens only on the switch's read-back (`reads_back`). The review named that
/// schedule; D-131 records it.
// PROPOSED(D-131)
#[test]
fn seed_16725_a_write_that_heard_nothing_is_asked_once_and_nothing_reads_a_value_applied_twice() {
    const WRITER: u64 = (95 << 32) | (1 << 16) | 5;
    const READER: u64 = (95 << 32) | (2 << 16) | 5;
    let (range, index) = (14, 100);
    let report = shard::run(16725);
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
        vec![(1, range, index)],
        "seed 16725's write was proposed by node 1 at index 100 of range 14, and by no one else"
    );
    let (at, hash) = records
        .iter()
        .enumerate()
        .find_map(|(at, record)| match record.event {
            TraceEvent::RaftApply {
                server: 1,
                range: of,
                index: i,
                hash,
                ..
            } if of == range && i == index => Some((at, hash)),
            _ => None,
        })
        .expect("seed 16725: node 1 applied the write");
    assert!(
        records[at..].iter().any(|record| matches!(
            record.event,
            TraceEvent::NodeCrashed { node } if Some(node) == node_id(1)
        )),
        "seed 16725: node 1 crashed with the write applied: pin it on its mechanism"
    );
    assert!(
        !records.iter().any(|record| matches!(
            record.event,
            TraceEvent::ClientReturn { client, .. } if client == WRITER
        )),
        "seed 16725: the writer heard nothing, so its call stays open"
    );

    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(&record.event,
                TraceEvent::MessageSent { from, .. } if *from == raft::burst_addr(5)))
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
    lin::check(&report.checked.history)
        .expect("seed 16725's history, every call placed: nothing reads a value applied twice");
}

/// Seed 18925, D-092's check on an asker that had not asked, found past the nightly's range (PROPOSED
/// D-131). Right after the restart arm's transfer the split's asker bounced between nodes on
/// `NotLeader`; its twelve asks went on those bounces, and its first `NoRangeId` came on the last, so
/// it made no retry and the check reported it as stopping with no answer: a report of the asker's
/// count, not of the system. Its first `NoRangeId` now starts the count again, so a retry is made.
/// The mechanism asserted: the arm's split's asker sent all twelve asks before its first refusal, the
/// retry came after it, and the answer came within D-092's bound, which is unchanged.
// PROPOSED(D-131)
#[test]
fn seed_18925_an_asker_whose_tries_went_on_bounces_still_retries_after_its_first_no_range_id() {
    let report = shard::run(18925);
    report.check().unwrap();
    assert_eq!(report.restart_arms.len(), 1, "{:?}", report.restart_arms);
    let arm = report.restart_arms[0];
    let seq = arm.seq.expect("the arm asked its split");
    assert_eq!(
        report.driver.retried.len(),
        1,
        "{:?}",
        report.driver.retried
    );
    let retried = report.driver.retried[0];
    assert_eq!(
        Some(retried.range),
        arm.range,
        "the asker is the arm's: {retried:?}"
    );
    let admin = raft::admin_addr(seq);
    let records = report.records();
    let asks = |before: bool| {
        records
            .iter()
            .filter(|record| {
                matches!(&record.event, TraceEvent::MessageSent { from, .. } if *from == admin)
                    && (record.at < retried.refused) == before
            })
            .count()
    };
    assert_eq!(
        asks(true),
        usize::try_from(shard::BOOTSTRAP * 4).expect("small"),
        "seed 18925's asker spent its asks before its first `NoRangeId`: pin it on its mechanism"
    );
    assert!(
        asks(false) >= 1,
        "the asker made its retry after its first `NoRangeId`"
    );
    assert!(
        retried.answered && retried.asking_at(report.last_fault_healed),
        "the asker is asked of, and got an answer: {retried:?}"
    );
    let bound = raft::election_max() * raft::LIVENESS_TIMEOUTS;
    assert!(
        retried.took(report.last_fault_healed) <= bound,
        "answered within D-092's bound: {retried:?}"
    );
}

fn node_id(node: u64) -> Option<NodeId> {
    Some(NodeId::new(u32::try_from(node).expect("small")))
}
