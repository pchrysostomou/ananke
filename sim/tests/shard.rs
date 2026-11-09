//! `sim/shard.rs`, the sharded sweep (SHARD.md §10; §12, Stage C): five nodes, three of
//! them the bootstrap nodes, ranges made by the splits the driver draws on stream
//! `shard`, leadership transfers, four clients over sixteen keys, and Phase 2's network
//! and disk faults.
//!
//! What it asserts, at every tier: the correct system passes every seed under every
//! check `sim/shard.rs`'s `Report::check` makes — checks 1 to 4 keyed by range, the range
//! layer's folds (7, 8, 9, 10, 16, 17, 18, 20), linearizability of every key across every
//! split, pre-vote's property, on uniform seeds the timer check, the write bound per key
//! and per range the run ended with and the meta convergence bound — and that nodes 4 and
//! 5 hold nothing. It prints the driver's work, the refusals by the loss that made them
//! (#45), the empty blocks a split finds (D-092) and the bounds' margins.
// PROPOSED(D-113): `sim/shard.rs`, Stage C's sharded sweep.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ananke_env::sim::TraceRecord;
use ananke_env::{NodeId, TraceEvent};
use ananke_raft::core::{Variant, Variants};
use ananke_raft::message::Message;
use ananke_shard::invariants::{self, Fold};
use ananke_shard::range_variant::{RangeVariant, RangeVariants};
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::raft;
use ananke_sim::shard::{self, DriverStats, DroppedTable, Loss, NoRangeId};
use ananke_sim::{seeds, sweep, verdict, write_trace};

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
        verdict,
    }
}

/// The correct system, under every check, at every tier. At the nightly's ten thousand it fails
/// two seeds and this test says so: 3870 and 5303, #175's committed entry lost, whose fix the
/// owner ruled lands in its own PR after the PRs now open. Nothing here is narrowed for them.
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

    let inclusion: Vec<Result<(), String>> = seen.iter().map(|s| s.keys_asked.clone()).collect();
    verdict(&inclusion).unwrap();
    let verdicts: Vec<Result<(), String>> = seen.into_iter().map(|s| s.verdict).collect();
    verdict(&verdicts).unwrap();
}

// --- The pair rule on this sweep (CLAUDE.md; SHARD.md §10): the correct system above, and
// below the variants the sweep's own faults and driver reach, each caught by the check that
// names it. S11 re-homes §10's routing variants with their shapes, and S12 brings
// `Fault::MetaReorder`; what is asserted here is what this sweep reaches without them.

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
/// through while its cache is stale. On 14 of 500 uniform seeds no write of any key completed
/// after the heal, and on 8 more the first completion anywhere came later than the bound from
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

/// §10's `TrustStaleDescriptor`: 99 of 100 seeds, every client stale after the first split.
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

/// §10's `ApplyIgnoresSpan`: 57 of 100 seeds by check 9, and 10 of them by check 10 too.
#[test]
fn a_server_that_ignores_the_span_at_apply_is_caught_by_check_9_on_the_sharded_sweep() {
    let found = range_variant("ApplyIgnoresSpan", share(), RangeVariant::ApplyIgnoresSpan);
    assert!(
        tripped(&found, Fold::ServingWithinSpan) > 0,
        "no write applied outside its range's span reached check 9"
    );
}

/// §10's `UninitialisedReplicaVotes` off its directed shape: a re-seeded node's placeholders
/// meet its ranges' elections on 98 of 100 seeds.
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

/// §10's `SnapshotOverlapsReplica` off its directed shape: 19 of 100 seeds by check 8's
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

/// §10's `IdBlockResumed`: 10 of 100 seeds by check 18, on restarts the faults draw and
/// splits the driver happens to land on the restarted node. A rate the gate's twenty would miss
/// about one time in eight, so the catch is asserted over the thousand's share alone (D-061);
/// S11's restart and split shape is what is meant to raise it. What every tier asserts is the
/// shape the variant aims at: a split that took an id from a block of an earlier run of its node
/// (`shard::splits_from_a_block_of_an_earlier_run_of`), 30 of 100 seeds and 6 of the gate's 20. The
/// correct node's new run holds none, so that is the variant seen firing.
// PROPOSED(D-113): §10, the shape at every tier.
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
    if seeds() >= 1000 {
        assert!(
            tripped(&found, Fold::SplitLineage) > 0,
            "no id was taken twice over the thousand's share"
        );
    }
}

/// §10's `MetaOverwritesByArrival` on what this sweep's drops and resends do by themselves:
/// 46 of 1 000 seeds by check 16, and 8 of 1 000 by the meta convergence bound, so both are
/// asserted over the thousand and printed below it (D-061). `Fault::MetaReorder` is S12's. The
/// bound is asked of the run taken to its end, which check 16 would otherwise stop first. What
/// every tier asserts is the shape the variant aims at: a split's update stored again over a key
/// meta already named at its generation or a newer one (`shard::meta_resends_stored_of`), 985 of
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

/// Phase 2's pre-vote variant under this sweep's isolations: 80 of 100 seeds.
#[test]
fn a_server_without_pre_vote_is_caught_by_the_isolations_on_the_sharded_sweep() {
    let found = core_variant("NoPreVote", Variant::NoPreVote);
    assert!(said(&found, "pre-vote:") > 0, "no isolation raised a term");
}

/// Phase 2's timer variant: 31 of 100 seeds by the timer check, and 2 more by the write bound.
#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught_on_the_sharded_sweep() {
    let found = core_variant("ResetTimerOnAnyRpc", Variant::ResetTimerOnAnyRpc);
    assert!(
        said(&found, "timers:") > 0,
        "no timer stayed reset by a stale sender"
    );
}

/// The disk's half of the fault model: Q15's whole-node refusal, which the rot reaches on 31
/// of 100 seeds. A node that refuses one range alone goes on serving the others over an
/// engine that lost state: 19 of 100 seeds by state machine safety.
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
/// node is refused `NoRangeId`, and an asker's retries after the heal end with no answer: 40 of
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
/// arm speaks first. Above the 5 % of D-061 on every tier: 42 of 100 seeds, a uniform 42 of 50.
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

/// Seed 286, no store refused, so D-128 and D-129 leave it where it is: node 1 restarts, leads
/// a range the driver then asks it to split, and holds no id, since a new run holds none until
/// its refill lands (SHARD.md §5's second rule). The split is refused `NoRangeId` (PROPOSED
/// D-092's A); range 0 grants the block within a minimum election timeout; the asker's retry
/// gets its answer. What the block's size cannot change (PROPOSED D-113 §8).
#[test]
fn seed_286_a_new_runs_empty_block_refuses_a_split_until_its_refill_lands() {
    let report = shard::run(286);
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
    let report = shard::run(368);
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
/// (PROPOSED D-128).
#[test]
fn seed_41_an_install_onto_a_reseeded_replica_keeps_the_right_half_beside_it() {
    let report = shard::run(41);
    report.check().unwrap();
    let refused = report.refusals();
    assert_eq!(refused.len(), 1, "one store refused: {refused:?}");
    let node = refused[0].0;
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
    let why = shard::run_to_the_end(
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

/// Seeds 128 and 643, where the sweep first found D-113's (b): a leader whose followers were
/// both quarantined, reads no round could confirm, and each client's retry a registration, until
/// `READS_OUTSTANDING`. #200 moved their schedules: with no right half's keys taken, each has one
/// store refused where it had two, so no leader is left unable to confirm a read. The situation
/// is absent, asserted, with that reason; seed 986 carries it now.
#[test]
fn seeds_128_and_643_no_longer_reach_a_leader_that_cannot_confirm_a_read() {
    for seed in [128, 643] {
        let report = shard::run(seed);
        report.check().unwrap();
        assert_eq!(
            report.refusals().len(),
            1,
            "seed {seed}: {:?}",
            report.refusals()
        );
        assert!(
            !report
                .records()
                .iter()
                .any(|record| matches!(record.event, TraceEvent::RaftReadRefused { .. })),
            "seed {seed} reaches a read no round can confirm again: pin it on its mechanism"
        );
    }
}

/// Seed 986, D-113's (b) where it stands since #200 moved seeds 128 and 643 off it: two stores
/// refused and a leader left unable to confirm a read, which it refuses at the step and drops
/// (PROPOSED D-129). Under `UnconfirmableReadKept` nothing answers them, the clients' retries
/// pile up and the node fails itself at `READS_OUTSTANDING`.
#[test]
fn seed_986_a_leader_refuses_the_reads_it_can_never_confirm() {
    let report = shard::run(986);
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
    let why = shard::run_of(
        986,
        Variant::UnconfirmableReadKept,
        NodeVariants::correct(),
        RangeVariants::correct(),
    )
    .check()
    .expect_err("the variant fails seed 986");
    assert!(
        why.contains("registered reads"),
        "the violation seed 986 is pinned for: {why}"
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
    let report = shard::run(3291);
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
    let report = shard::run(8934);
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
        let report = shard::run(seed);
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
    let mut report = shard::run(2272);
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

fn node_id(node: u64) -> Option<NodeId> {
    Some(NodeId::new(u32::try_from(node).expect("small")))
}
