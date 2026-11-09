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

use ananke_env::TraceEvent;
use ananke_raft::core::{Variant, Variants};
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
    /// The write bound's inclusion: whether every key the fixed map would ask is asked.
    widening: Result<(), String>,
    slowest_write: Option<Duration>,
    recovery_from_heal: Option<Duration>,
    recovery_from_call: Option<Duration>,
    right_half: bool,
    partitions: usize,
    blocks: usize,
    crashes: usize,
    lease_reads: usize,
    placeholders: usize,
}

/// SHARD.md §8's write bound, as `raft::Report::liveness_by_final_range` asks it, is no
/// narrower than the fixed map's reading (`raft::Report::liveness`, under the map that sends
/// every key to its first range): where the first range has a majority up, the range serving
/// every key the clients wrote after the heal has one too, so every key the old reading
/// asks is still asked, under the same constant (PROPOSED D-113 §5).
fn not_widened(report: &shard::Report) -> Result<(), String> {
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
        widening: not_widened(&report),
        slowest_write,
        recovery_from_heal: from_heal,
        recovery_from_call: from_call,
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
    println!(
        "shard: the write bound of {:?}: slowest post-heal write {:?}; a range's first completion \
         from the heal at worst {:?}, from its own first call at worst {:?}",
        raft::election_max() * raft::LIVENESS_TIMEOUTS,
        seen.iter().filter_map(|s| s.slowest_write).max(),
        seen.iter().filter_map(|s| s.recovery_from_heal).max(),
        seen.iter().filter_map(|s| s.recovery_from_call).max(),
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

    let inclusion: Vec<Result<(), String>> = seen.iter().map(|s| s.widening.clone()).collect();
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

/// §10's `ClientIgnoresMismatch` on any split with load: 98 of 100 seeds.
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

/// §10's `ApplyIgnoresSpan`: 59 of 100 seeds by check 9, and 10 of them by check 10 too.
#[test]
fn a_server_that_ignores_the_span_at_apply_is_caught_by_check_9_on_the_sharded_sweep() {
    let found = range_variant("ApplyIgnoresSpan", share(), RangeVariant::ApplyIgnoresSpan);
    assert!(
        tripped(&found, Fold::ServingWithinSpan) > 0,
        "no write applied outside its range's span reached check 9"
    );
}

/// §10's `UninitialisedReplicaVotes` off its directed shape: a re-seeded node's placeholders
/// meet its ranges' elections on 96 of 100 seeds.
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

/// §10's `IdBlockResumed`: 7 of 100 seeds by check 18, on restarts the faults draw and
/// splits the driver happens to land on the restarted node. Under D-061's 5 % at the shares
/// below the premerge's, whose twenty would miss it about one time in four; S11's restart
/// and split shape is what is meant to raise it.
#[test]
fn a_node_that_resumes_another_runs_block_of_ids_is_caught_by_check_18_on_the_sharded_sweep() {
    let found = range_variant("IdBlockResumed", share(), RangeVariant::IdBlockResumed);
    if seeds() >= 1000 {
        assert!(
            tripped(&found, Fold::SplitLineage) > 0,
            "no id was taken twice over the thousand's share"
        );
    }
}

/// §10's `MetaOverwritesByArrival` on what this sweep's drops and resends do by themselves:
/// 45 of 1 000 seeds by check 16, and 7 of 1 000 by the meta convergence bound, so both are
/// asserted over the thousand and printed below it (D-061). `Fault::MetaReorder` is S12's. The
/// bound is asked of the run taken to its end, which check 16 would otherwise stop first.
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
        )
    });
    let sixteen = found.iter().filter(|f| f.0).count();
    let bound = found.iter().filter(|f| f.1).count();
    println!(
        "shard: MetaOverwritesByArrival, each run to its end: check 16 on {sixteen}/{run} seeds, \
         the meta convergence bound on {bound}/{run}"
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

/// Phase 2's pre-vote variant under this sweep's isolations: 78 of 100 seeds.
#[test]
fn a_server_without_pre_vote_is_caught_by_the_isolations_on_the_sharded_sweep() {
    let found = core_variant("NoPreVote", Variant::NoPreVote);
    assert!(said(&found, "pre-vote:") > 0, "no isolation raised a term");
}

/// Phase 2's timer variant: 29 of 100 seeds by the timer check.
#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught_on_the_sharded_sweep() {
    let found = core_variant("ResetTimerOnAnyRpc", Variant::ResetTimerOnAnyRpc);
    assert!(
        said(&found, "timers:") > 0,
        "no timer stayed reset by a stale sender"
    );
}

/// The disk's half of the fault model: Q15's whole-node refusal, which the rot reaches on 29
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
