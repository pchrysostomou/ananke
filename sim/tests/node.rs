//! `sim/raft.rs`'s arms on the node (SHARD.md §12, Stage B's first exit criterion):
//! three nodes, **four ranges on every node**, under the Phase 2 sweep's own fault
//! schedule.
//!
//! `sim/tests/ranges.rs` is the node's *structure* — four replicas a node, frames
//! carrying several ranges, creations that agree — under three arms of its own.
//! This binary is the node under the *raft sweep's* arms: the two lease trials, the
//! isolations, the leader isolation cut per range, the one-way blocks, the crashes,
//! the leader crash, `StaleSender`'s rule-5 shape and the Figure 8 driver, drawn
//! from the same seed and the same streams as the one-group sweep draws them
//! (`raft::Schedule::draw_on_the_node`).
//!
//! What it asserts, at every tier:
//!
//! - the correct node passes every seed, under every check the one-group sweep
//!   makes: the four log invariants keyed by range, linearizability of the
//!   clients' history, pre-vote's property per (range, server), the timer check,
//!   the liveness bound and the write bound per key of a range whose unimpaired
//!   replicas form a majority;
//! - Phase 2's variants on these arms are caught on the node too, each at the tier
//!   its **measured** rate supports and no stronger than its Phase 2 test asserts
//!   (§10, D-061). Every rate is printed;
//! - the run reaches the shape the keyed checks need: four replicas a node, every
//!   range electing and applying, and frames between two nodes carrying messages
//!   of several ranges;
//! - the paths this node has not got are absent, asserted with their reason: no
//!   snapshot action asked for and no store refused (`raft::Report::check`).
//!
//! It also prints the measurements SHARD.md §12 asks for under the sweeps: the
//! inbox's drops under its byte bound, the apply lag per range against §4's
//! twenty-millisecond threshold, how long one range's applies hold the node's
//! others, and the trace records a run holds per range per virtual second against
//! `TRACE_CAP`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ananke_raft::core::{Variant, Variants};
use ananke_shard::variant::NodeVariants;
use ananke_sim::raft::{self, Cluster};
use ananke_sim::{seeds, sweep, verdict, write_trace};

/// The correct node, on the seeds this tier runs.
fn correct(seed: u64) -> raft::Report {
    raft::run_on_the_node(seed, Variants::default(), NodeVariants::correct())
}

/// A node whose cores carry `variants`, with the node itself correct.
fn buggy(seed: u64, variants: impl Into<Variants>) -> raft::Report {
    raft::run_on_the_node(seed, variants, NodeVariants::correct())
}

/// The ranges the node hosts.
fn ranges() -> Vec<u64> {
    Cluster::Node.ranges()
}

/// The run's verdict: every check the one-group sweep makes, then the snapshot path
/// asserted **reached on every seed**, and then the one path this node still has not
/// got, asserted **absent with its reason**.
///
/// Both halves are the same rule, which is why they sit together. A variant whose
/// situation the run cannot reach is a test that passes because nothing was injected,
/// the one failure mode a sweep cannot report on its own (CLAUDE.md). Until PROPOSED
/// D-083 the node had no `snapshot` task, so this function asserted the *absence* of a
/// snapshot action and of the condition behind one, naming the slice that owed the
/// path. The path is wired now and this slice puts the four stream variants of §10
/// under these arms, so the absence becomes its opposite: a seed that takes no
/// snapshot fails here, because `SnapshotWithoutCurrentLast` and `SharedSnapshotDir`
/// assert nothing on a run with no take and no stream in it.
///
/// What stays an absence is the refusal, which is Q15's and PR #86's. It is asserted
/// exactly as it was, so the day that path arrives the sweep says so rather than
/// passing over it.
// PROPOSED(D-086): the snapshot path is reached, and the absence becomes a reach.
fn checked(report: &raft::Report) -> Result<(), String> {
    let seed = report.seed;
    report.check()?;
    // The reach, per seed. `snapshot_actions` counts what a core *asked* for, which
    // the node's host counts whether or not the task served it; the takes and the
    // installs below are what the task actually did, read off the trace. The three
    // are asserted separately on purpose: an action asked for and never served is
    // exactly the shape D-082 found `SendBeforePersist` in — a bit set, carried, and
    // read by nothing — and a count of asks would not have shown it.
    if report.snapshot_actions() == 0 {
        return Err(format!(
            "seed {seed}: no core asked for a snapshot action at all, though \
             `snapshot_threshold` is {}: the stream variants of §10 assert nothing on a \
             run with no take in it, so this seed's evidence is vacuous rather than \
             green",
            raft::NODE_SNAPSHOT_THRESHOLD
        ));
    }
    let takes = report.snapshot_takes();
    if takes.is_empty() {
        return Err(format!(
            "seed {seed}: a core asked for a snapshot action and no take completed, so \
             nothing was checkpointed for a stream to read: `SharedSnapshotDir`'s \
             re-take and `SnapshotWithoutCurrentLast`'s install both assert nothing here"
        ));
    }
    if let Some((server, reason)) = report.refused.first() {
        return Err(format!(
            "seed {seed}: server {server} refused its store ({reason}), a path this node does \
             not have: Q15's whole-node refusal and re-seed are PR #86's. \
             `RefusalNotDurable` is not re-asserted here, and a run that reaches this must \
             say so rather than pass"
        ));
    }
    Ok(())
}

#[test]
fn a_node_under_the_raft_sweeps_arms_runs_every_range() {
    let report = correct(1);
    checked(&report).expect("the correct node passes seed 1");
    let mut leaders: BTreeMap<u64, usize> = BTreeMap::new();
    let mut applies: BTreeMap<u64, usize> = BTreeMap::new();
    for record in &report.records {
        match &record.event {
            ananke_env::TraceEvent::RaftLeader { range, .. } => {
                *leaders.entry(*range).or_default() += 1;
            }
            ananke_env::TraceEvent::RaftApply { range, .. } => {
                *applies.entry(*range).or_default() += 1;
            }
            _ => {}
        }
    }
    println!("node: seed 1 leaders by range {leaders:?}, applies by range {applies:?}");
    for range in ranges() {
        assert!(
            leaders.get(&range).copied().unwrap_or(0) > 0,
            "range {range} never elected a leader: {leaders:?}"
        );
        assert!(
            applies.get(&range).copied().unwrap_or(0) > 0,
            "range {range} applied nothing: {applies:?}"
        );
    }
}

/// What the correct node's sweep saw, and what it must have seen.
///
/// `sim/tests/raft.rs` has had a `Coverage` since Phase 2 for a reason this slice
/// then proved again: a sweep that passes may not have injected anything.
/// `SendBeforePersist` was caught on 0 of 20 seeds here not because the node was
/// right but because nothing on the node read the variant. The counters below are
/// the same idea one level out — not "was the bug caught" but "did the arm fire at
/// all" — and two of them exist because *drawn* is not *reached*: `StaleSender` and
/// `FigureEight` each spend a budget waiting for a shape that may not come, and the
/// Figure 8 driver's burst is the one whose silence would be invisible. The review of
/// this slice aimed the burst at the wrong range entirely; `CountOlderTermForCommit`
/// fell from 14/20 to 11/20 and every test stayed green.
///
/// It carries the node's arms, which are `sim/raft.rs`'s less the four
/// `Schedule::draw_on_the_node` removes.
// PROPOSED(D-082): the node's sweep has a coverage of its own.
#[derive(Default)]
struct Coverage {
    seeds: u64,
    uniform_seeds: u64,
    partitions: usize,
    one_way_blocks: usize,
    crashes: usize,
    isolate_leader_faults: usize,
    leader_crashes: usize,
    stale_sender_faults: usize,
    figure_eight_faults: usize,
    burst_puts: usize,
    drift_exceeded_seeds: u64,
    lease_reads: usize,
    read_index_reads: usize,
    lease_revokes: usize,
    quorum_losses: usize,
    duplicates: usize,
    drops: usize,
    leaders: usize,
    terms_above_one: u64,
    truncations: usize,
    commits: usize,
    applies: usize,
    inbox_drops: BTreeMap<(String, u64), usize>,
    /// The snapshot path, counted so the *reach* is a number and not a hope, and the
    /// one path this node still has not got, counted so the absence is one too. Both
    /// were absences until PROPOSED D-086 lowered this cluster's `snapshot_threshold`
    /// to the one-group sweep's and put the stream variants under these arms.
    snapshot_actions: usize,
    /// The fewest any one seed asked for, beside the tier's total: a mean over the
    /// tier is what the floor is taken on, and this says how thin the thinnest seed
    /// was (PROPOSED D-086).
    least_actions: Option<usize>,
    /// Seeds on which the correct node re-took a range at an index it had already
    /// taken that range at — legitimate after an install — and, of those, the seeds
    /// on which it wrote the re-take into the **same version directory**, which is
    /// what the take counter exists to prevent and `SharedSnapshotDir` turns off
    /// (PROPOSED D-086).
    took_an_index_twice: usize,
    retook_into_one_directory: usize,
    refusals: usize,
    highest_index: u64,
    puts: u64,
    gets: u64,
    deletes: u64,
    cas: u64,
    completed: u64,
    abandoned: u64,
    redirected: u64,
    leaders_by_range: BTreeMap<u64, usize>,
    applies_by_range: BTreeMap<u64, usize>,
    multi_range_frames: usize,
    arms_hit: usize,
    arms_fired: usize,
    lags_by_range: BTreeMap<u64, Vec<Duration>>,
    holds: Vec<Duration>,
    holds_dropped_for_a_crash: usize,
    records: usize,
    per_range_per_second: f64,
    busiest_range_per_second: f64,
    /// The largest in-memory log any follower replica held, and the distribution in
    /// multiples of the one-group scenario's `snapshot_threshold`: printed, never
    /// asserted, because this node has no follower compaction to bound it.
    largest_follower_log: u64,
    follower_log_multiples: BTreeMap<u64, u64>,
}

/// Everything but the two sample vectors, which are summarised.
///
/// `lags_by_range` holds one duration per apply: 133 464 of them at a hundred seeds
/// and **1 327 475 at a thousand**, and `holds` tens of thousands beside it. Deriving
/// `Debug` and printing the struct put all of them in the sweep's output and so into
/// every nightly log — some three hundred kilobytes of durations that no reader was
/// ever going to read. The summary is what the coverage is for: how many samples, and
/// what they came to.
// PROPOSED(D-082): the coverage prints its samples' shape, not its samples.
impl std::fmt::Debug for Coverage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lag_samples: usize = self.lags_by_range.values().map(Vec::len).sum();
        let (median_lag, per_range) = medians(&self.lags_by_range);
        let mut holds = self.holds.clone();
        holds.sort_unstable();
        f.debug_struct("Coverage")
            .field("seeds", &self.seeds)
            .field("uniform_seeds", &self.uniform_seeds)
            .field("partitions", &self.partitions)
            .field("one_way_blocks", &self.one_way_blocks)
            .field("crashes", &self.crashes)
            .field("isolate_leader_faults", &self.isolate_leader_faults)
            .field("leader_crashes", &self.leader_crashes)
            .field("stale_sender_faults", &self.stale_sender_faults)
            .field("figure_eight_faults", &self.figure_eight_faults)
            .field("burst_puts", &self.burst_puts)
            .field("drift_exceeded_seeds", &self.drift_exceeded_seeds)
            .field("lease_reads", &self.lease_reads)
            .field("read_index_reads", &self.read_index_reads)
            .field("lease_revokes", &self.lease_revokes)
            .field("quorum_losses", &self.quorum_losses)
            .field("duplicates", &self.duplicates)
            .field("drops", &self.drops)
            .field("leaders", &self.leaders)
            .field("terms_above_one", &self.terms_above_one)
            .field("truncations", &self.truncations)
            .field("commits", &self.commits)
            .field("applies", &self.applies)
            .field("inbox_drops", &self.inbox_drops)
            .field("snapshot_actions", &self.snapshot_actions)
            .field("least_actions", &self.least_actions)
            .field("took_an_index_twice", &self.took_an_index_twice)
            .field("retook_into_one_directory", &self.retook_into_one_directory)
            .field("refusals", &self.refusals)
            .field("highest_index", &self.highest_index)
            .field("puts", &self.puts)
            .field("gets", &self.gets)
            .field("deletes", &self.deletes)
            .field("cas", &self.cas)
            .field("completed", &self.completed)
            .field("abandoned", &self.abandoned)
            .field("redirected", &self.redirected)
            .field("leaders_by_range", &self.leaders_by_range)
            .field("applies_by_range", &self.applies_by_range)
            .field("multi_range_frames", &self.multi_range_frames)
            .field("arms_hit", &self.arms_hit)
            .field("arms_fired", &self.arms_fired)
            .field("apply_lag_samples", &lag_samples)
            .field("apply_lag_median", &median_lag)
            .field("apply_lag_median_per_range", &per_range)
            .field("holds", &holds.len())
            .field("hold_median", &median_of(&holds))
            .field("hold_max", &holds.last())
            .field("holds_dropped_for_a_crash", &self.holds_dropped_for_a_crash)
            .field("records", &self.records)
            .field("per_range_per_second", &self.per_range_per_second)
            .field("busiest_range_per_second", &self.busiest_range_per_second)
            .field("largest_follower_log", &self.largest_follower_log)
            .field("follower_log_multiples", &self.follower_log_multiples)
            .finish()
    }
}

impl Coverage {
    fn add(&mut self, report: &raft::Report) {
        use ananke_env::{ClientOp, DropReason, TraceEvent};
        self.seeds += 1;
        self.uniform_seeds += u64::from(report.uniform());
        self.partitions += report.count(|e| matches!(e, TraceEvent::PartitionStarted { .. }));
        self.crashes += report.count(|e| matches!(e, TraceEvent::NodeCrashed { .. }));
        for fault in &report.schedule.faults {
            match fault {
                raft::Fault::OneWay { .. } => self.one_way_blocks += 1,
                raft::Fault::IsolateLeader { .. } => self.isolate_leader_faults += 1,
                raft::Fault::CrashLeader { .. } => self.leader_crashes += 1,
                raft::Fault::StaleSender { .. } => self.stale_sender_faults += 1,
                raft::Fault::FigureEight { .. } => self.figure_eight_faults += 1,
                _ => {}
            }
        }
        self.burst_puts += report.burst_puts();
        self.drift_exceeded_seeds += u64::from(report.drift_exceeded());
        self.lease_reads += report.lease_reads();
        self.read_index_reads += report.read_index_reads();
        self.lease_revokes += report.lease_revokes();
        self.quorum_losses += report.quorum_losses();
        self.duplicates +=
            report.count(|e| matches!(e, TraceEvent::MessageDelivered { dup: true, .. }));
        self.drops += report.count(|e| {
            matches!(
                e,
                TraceEvent::MessageDropped {
                    reason: DropReason::Injected,
                    ..
                }
            )
        });
        self.leaders += report.count(|e| matches!(e, TraceEvent::RaftLeader { .. }));
        self.terms_above_one += u64::from(
            report.has(|e| matches!(e, TraceEvent::RaftLeader { term, .. } if *term > 1)),
        );
        self.truncations += report.count(|e| matches!(e, TraceEvent::RaftTruncate { .. }));
        self.commits += report.count(|e| matches!(e, TraceEvent::RaftCommit { .. }));
        self.applies += report.count(|e| matches!(e, TraceEvent::RaftApply { .. }));
        for ((kind, range), count) in report.inbox_drops() {
            *self
                .inbox_drops
                .entry((kind.to_owned(), range))
                .or_default() += count;
        }
        let actions = report.snapshot_actions();
        self.snapshot_actions += actions;
        // PROPOSED(D-086): the asks per seed, and the take counter's property.
        self.least_actions = Some(
            self.least_actions
                .map_or(actions, |least| least.min(actions)),
        );
        self.took_an_index_twice += usize::from(took_an_index_twice_on_the_node(report));
        self.retook_into_one_directory += usize::from(retook_into_one_directory(report));
        self.refusals += report.refused.len();
        self.highest_index = self.highest_index.max(report.highest_index());
        for record in &report.records {
            match &record.event {
                TraceEvent::ClientInvoke { op, .. } => match op {
                    ClientOp::Put { .. } => self.puts += 1,
                    ClientOp::Get { .. } => self.gets += 1,
                    ClientOp::Delete { .. } => self.deletes += 1,
                    ClientOp::Cas { .. } => self.cas += 1,
                },
                TraceEvent::RaftLeader { range, .. } => {
                    *self.leaders_by_range.entry(*range).or_default() += 1;
                }
                TraceEvent::RaftApply { range, .. } => {
                    *self.applies_by_range.entry(*range).or_default() += 1;
                }
                _ => {}
            }
        }
        self.completed += report.clients.completed;
        self.abandoned += report.clients.abandoned;
        self.redirected += report.clients.redirected;
        self.multi_range_frames += report.frames_of_several_ranges();
        let (hit, fired) = report.arms_hit_their_ranges();
        self.arms_hit += hit;
        self.arms_fired += fired;
        for (range, lags) in report.apply_lags() {
            self.lags_by_range.entry(range).or_default().extend(lags);
        }
        let (holds, dropped) = report.cross_range_apply_holds_counted();
        self.holds.extend(holds);
        self.holds_dropped_for_a_crash += dropped;
        self.records += report.records.len();
        self.per_range_per_second = self
            .per_range_per_second
            .max(report.records_per_second_per_range());
        self.busiest_range_per_second = self
            .busiest_range_per_second
            .max(report.busiest_range_records_per_second());
        let (longest, _) = report.largest_follower_log();
        self.largest_follower_log = self.largest_follower_log.max(longest);
        *self
            .follower_log_multiples
            .entry(longest / raft::SNAPSHOT_THRESHOLD)
            .or_default() += 1;
    }

    /// Everything the sweep must have seen, at every tier.
    ///
    /// The floors were measured on the correct node at 100 seeds before they were
    /// written (D-061) and are recorded in the entry; each is set at about a quarter
    /// of its observation, which is what keeps a tier of twenty from failing a tree
    /// with nothing wrong while still failing a tree where an arm stopped firing.
    /// They are scaled by the tier, since every one of them is a per-seed rate.
    fn assert_complete(&self) {
        let per_seed = |floor: f64| (floor * self.seeds as f64 / 100.0).max(1.0) as usize;
        let floors: [(&str, usize, usize); 13] = [
            ("partitions", self.partitions, per_seed(108.0)),
            ("crashes", self.crashes, per_seed(69.0)),
            ("one-way blocks", self.one_way_blocks, per_seed(12.0)),
            (
                "leader isolations",
                self.isolate_leader_faults,
                per_seed(14.0),
            ),
            ("leader crashes", self.leader_crashes, per_seed(14.0)),
            (
                "stale-sender arms",
                self.stale_sender_faults,
                per_seed(11.0),
            ),
            ("figure-8 arms", self.figure_eight_faults, per_seed(29.0)),
            ("burst puts", self.burst_puts, per_seed(4200.0)),
            ("truncations", self.truncations, per_seed(620.0)),
            ("quorum losses", self.quorum_losses, per_seed(280.0)),
            ("lease reads", self.lease_reads, per_seed(2000.0)),
            ("read-index reads", self.read_index_reads, per_seed(5400.0)),
            (
                "client redirects",
                self.redirected as usize,
                per_seed(1300.0),
            ),
        ];
        for (what, saw, floor) in floors {
            assert!(
                saw >= floor,
                "the sweep saw {saw} {what} over {} seeds, under the floor of {floor}: an arm \
                 that stops firing is a sweep that passes because it injected nothing",
                self.seeds
            );
        }
        // Every client operation kind, and both outcomes: a history with no
        // abandoned operation is a history the checker never had to leave pending.
        for (what, saw) in [
            ("puts", self.puts),
            ("gets", self.gets),
            ("deletes", self.deletes),
            ("compare-and-swaps", self.cas),
            ("completed operations", self.completed),
            ("abandoned operations", self.abandoned),
        ] {
            assert!(saw > 0, "the sweep invoked no {what}");
        }
        assert!(
            self.duplicates > 0 && self.drops > 0,
            "the network delivered no duplicate or dropped nothing: {self:?}"
        );
        // **The clock guard's path, restored by the merge with
        // `phase-3-stage-b-wiring`.** These two were asserted before that branch turned
        // this sweep's tuples into `Coverage`; the refactor kept both counters as
        // printed fields and stopped asserting them, which is the shape this project
        // treats as a loss — `LeaseTrustsTheClock` is asserted on this cluster from the
        // thousand-seed tier, and it asserts nothing on a run where the bound was never
        // exceeded and the guard never asked. Measured on the merged tree at the gate's
        // twenty: the bound is exceeded on 11 of 20 seeds and the guard revokes 2 607
        // times, so both are far above a floor of one at every tier.
        // PROPOSED(D-086): the drift bound and the guard's revoke are asserted again.
        assert!(
            self.drift_exceeded_seeds > 0,
            "no seed of this tier exceeded the drift bound, so the guard was never asked"
        );
        assert!(
            self.lease_revokes > 0,
            "the correct node never revoked a promise, so the guard's path was not reached"
        );
        assert!(
            self.terms_above_one > 0 && self.leaders > 0 && self.commits > 0,
            "the sweep elected nobody past term 1, or committed nothing"
        );
        // **The reach, and the one absence left.** All three of these were absences
        // until PROPOSED D-086: this cluster held `snapshot_threshold` far above what
        // its clients write, so no core asked for an action and no replica reached the
        // threshold, and the coverage counted the absence so it was a number rather
        // than a hope. That slice lowers the threshold to the one-group sweep's and
        // puts §10's four stream variants under these arms, so the first two invert —
        // a sweep with no take in it is a sweep those variants assert nothing in, which
        // is a vacuous pass and not a green one. `checked` says so per seed; these say
        // so over the tier, which is where an arm that stopped firing shows up.
        // PROPOSED(D-086): the snapshot path is reached, and the absence becomes a reach.
        assert!(
            self.snapshot_actions > 0,
            "no core asked for a snapshot action over {} seeds, so the stream variants under \
             these arms assert nothing at all",
            self.seeds
        );
        assert!(
            self.highest_index >= raft::NODE_SNAPSHOT_THRESHOLD,
            "no replica reached index {}, this cluster's `snapshot_threshold`, over {} seeds, \
             so no core could ask for a take and nothing was checkpointed for a stream",
            raft::NODE_SNAPSHOT_THRESHOLD,
            self.seeds
        );
        // What stays an absence is Q15's whole-node refusal, which is PR #86's. It is
        // asserted exactly as it was, so the day that path arrives the sweep says so.
        assert_eq!(
            self.refusals, 0,
            "a store was refused on a node whose whole-node refusal is PR #86's"
        );
    }
}

#[test]
fn every_seed_passes_on_the_correct_node_under_the_raft_sweeps_arms() {
    let seeds = seeds();
    let coverage = std::sync::Mutex::new(Coverage::default());
    let verdicts = sweep(seeds, |seed| {
        let report = correct(seed);
        let outcome = checked(&report);
        coverage.lock().expect("the coverage").add(&report);
        if outcome.is_err() {
            write_trace(&format!("node-{seed}"), &report.jsonl());
        }
        outcome
    });
    let coverage = coverage.into_inner().expect("the coverage");
    let (median_lag, per_range_median) = medians(&coverage.lags_by_range);
    let worst_range_lag = per_range_median.values().copied().max();
    let mut holds = coverage.holds.clone();
    holds.sort_unstable();
    println!(
        "node: {seeds} seeds, leaders by range {:?}, applies by range {:?}, {} records in all; \
         at most {:.0} trace records per virtual second per range and {:.0} of the busiest \
         range's own, against TRACE_CAP of {}; {} peer frames carried messages of more than \
         one range",
        coverage.leaders_by_range,
        coverage.applies_by_range,
        coverage.records,
        coverage.per_range_per_second,
        coverage.busiest_range_per_second,
        raft::TRACE_CAP,
        coverage.multi_range_frames
    );
    println!("node: coverage {coverage:?}");
    println!(
        "node: the inbox dropped {:?} under its byte bound",
        coverage.inbox_drops
    );
    println!(
        "node: apply lag, median over every range {median_lag:?}, per range \
         {per_range_median:?}, worst range {worst_range_lag:?}, against SHARD.md §4's threshold \
         of {HEARTBEAT:?}; {} applies measured",
        coverage.lags_by_range.values().map(Vec::len).sum::<usize>()
    );
    println!(
        "node: one range's applies held another's for a median of {:?} and at most {:?}, over {} \
         waits that crossed a range, with {} windows dropped for holding a crash or a restart \
         of their node",
        median_of(&holds),
        holds.last(),
        holds.len(),
        coverage.holds_dropped_for_a_crash
    );
    println!(
        "node: the largest follower log was {} entries, distribution by multiple of {} {:?} — \
         printed, not asserted: this node has no follower compaction to bound it",
        coverage.largest_follower_log,
        raft::SNAPSHOT_THRESHOLD,
        coverage.follower_log_multiples
    );
    let aimed_rate = if coverage.arms_fired == 0 {
        0.0
    } else {
        coverage.arms_hit as f64 * 100.0 / coverage.arms_fired as f64
    };
    println!(
        "node: {}/{} leader-relative arms ({aimed_rate:.1}%) hit the leader of the range they \
         drew",
        coverage.arms_hit, coverage.arms_fired
    );

    coverage.assert_complete();

    // §4's threshold is on the median apply lag, and it is asked **per range**. The
    // pooled median hides a breach: with the scenario's key map answering one range,
    // three of the four ranges' medians go past 20 ms while the pooled figure still
    // reads 3 ms, because the one busy range carries 85 % of the applies and so 85 %
    // of the samples. Measured at 100 seeds: per range 2.86 / 3.27 / 2.96 / 3.12 ms
    // correct, and 27.6 / 26.8 / 26.0 ms on the three starved ranges mutated.
    // PROPOSED(D-082): the apply lag's threshold is asked per range.
    for (range, median) in &per_range_median {
        assert!(
            *median <= HEARTBEAT,
            "range {range}'s median apply lag is {median:?}, past SHARD.md §4's threshold of \
             {HEARTBEAT:?}: Q14's grouped applies go to the owner"
        );
    }

    // §11, env 8's teeth. An arm that resolved "the leader" without its range would
    // cut off whichever range elected last — a perfectly good fault that no check of
    // the run would report, which is why this is here rather than left to the run's
    // other checks.
    //
    // Both figures were measured before the floor was written (D-061), by planting the
    // mutation: the correct node hits **115 of 115** over a hundred seeds, and a
    // `leader_of_range` that ignores its range argument hits **82 of 115, 71.3 %** —
    // not one in four, because three nodes hold four ranges and the leader of the
    // range that elected last is often the leader of the range the arm drew as well.
    // The floor sits between them with room on both sides. It is a bound the correct
    // system must not trip, so a tier that comes in under it is a model error to take
    // to the owner and not a number to widen (D-030, D-039).
    assert!(coverage.arms_fired > 0, "no leader-relative arm fired");
    assert!(
        aimed_rate >= 90.0,
        "{}/{} leader-relative arms hit the leader of the range they drew ({aimed_rate:.1}%), \
         under the 90% floor: a harness that resolved a leader without its range sits at \
         about 71%",
        coverage.arms_hit,
        coverage.arms_fired
    );

    // The shape the keyed checks need, on every seed: four ranges on every node,
    // each electing and applying. It is what says the sweep can tell a keyed check
    // from a wrongly keyed one at all, so it is asserted at every tier.
    for range in ranges() {
        assert!(
            coverage.leaders_by_range.contains_key(&range),
            "range {range} never led"
        );
        assert!(
            coverage.applies_by_range.contains_key(&range),
            "range {range} never applied"
        );
    }
    // And the work is spread over the four, not piled on one. "Every range applied
    // something" is satisfied by a range that applied only its leader's no-ops, which
    // is what the node looks like when the scenario's key map sends every client key
    // to one range: the sweep would then be a one-range sweep wearing four names, and
    // every check keyed by range would have nothing to be wrong about.
    //
    // Measured before it was asserted (D-061), by planting that mutation. Over a
    // hundred seeds the correct node applies {2: 31 264, 3: 36 021, 4: 32 403,
    // 5: 33 776} — least over busiest, **0.87** — and a key map that answers one range
    // gives {2: 95 986, 3: 16 459, 4: 12 886, 5: 14 353}, **0.13**. The floor is half,
    // between them and far from both.
    let least = coverage
        .applies_by_range
        .values()
        .copied()
        .min()
        .unwrap_or(0);
    let busiest = coverage
        .applies_by_range
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let spread = least as f64 / busiest.max(1) as f64;
    println!(
        "node: the least-applied range took {least} entries against the busiest range's \
         {busiest}, a spread of {spread:.2}"
    );
    assert!(
        spread >= 0.5,
        "the least-applied range took {least} entries against the busiest's {busiest} \
         ({spread:.2}), which is what a sweep whose client work all lands on one range looks \
         like"
    );
    // Read off the frames themselves, not off the scenario's parameters: a frame
    // between two nodes carries messages of several ranges, which is what four
    // ranges to a node is the parameter for (SHARD.md §12). A one-range world can
    // never produce one, so this is the owner's class of check, and the floor has a
    // mutant behind it as well as an observation: the correct node sends about 2 707
    // such frames a seed, and an outbox that cuts one frame per (peer, range) instead
    // of one per peer sends **0**. The floor is a hundred a seed.
    assert!(
        coverage.multi_range_frames >= 100 * seeds as usize,
        "{} frames of several ranges over {seeds} seeds is too few to say a frame between two \
         nodes carries several ranges",
        coverage.multi_range_frames
    );
    // **The correct node never writes two takes of one range at one index into one
    // version directory**, over the tier. That is what the directory's take counter is
    // for (D-043, D-075): every take goes to a name of its own, so a stream reading one
    // version is never read out from under. It is the property `SharedSnapshotDir`
    // turns off.
    //
    // The *index* alone is not the property and the figure beside it says so: the
    // correct node re-takes at an index it has already taken at on 52 of 100 seeds (44
    // before the merge with `phase-3-stage-b-wiring`), after an install leaves the
    // core's `taken` naming a snapshot this replica never took (D-078) while the applied
    // index stands still. Asserting the index would have been a bound the correct system
    // trips.
    // PROPOSED(D-086): the take counter's property, keyed by range.
    println!(
        "node: {} snapshot actions over {seeds} seeds, {:.1} a seed, fewest on any one seed {}",
        coverage.snapshot_actions,
        coverage.snapshot_actions as f64 / seeds as f64,
        coverage.least_actions.unwrap_or(0)
    );
    // **A floor on the snapshot actions a seed asks for, on average over the tier.**
    // Nothing else in the tree checks that a replica which asks for a snapshot gets an
    // answer, and D-078's follower compaction has no direct check at all: a `Record` is
    // written between two applies and traces nothing, by design, so the ask and the
    // answer are both invisible. What *is* visible is the consequence — the core sets
    // `take_pending` when it asks and only an answer clears it, so a node that drops
    // the ask stops taking snapshots altogether.
    //
    // Measured, all on the same tiers: the correct node asks **60.4 a seed** over 100
    // seeds (6 041, fewest 26 on any one) and **58.8** over 20 (1 176, fewest 32); after
    // the merge with `phase-3-stage-b-wiring` it asks **58.5** over 100 (5 849, fewest
    // 27), so the floor's margin is unchanged and the mutants below are not re-run. A
    // node whose `Record` goes to the `snapshot` task, which drops it, asks **11.6**
    // over 20; one that routes `Record` to the node's *first* range rather than the
    // range that asked — textually a no-op on one group, and a mutation no
    // single-range world could catch — asks **23.3**.
    //
    // The floor is **30 a seed**: a little under half the correct node's mean, and
    // over both mutants. It is taken over the tier and not per seed because a single
    // heavily-partitioned seed writes little, and a per-seed minimum would be a bound
    // the correct system could trip (D-030, D-039); a mean over the tier moves only
    // when the mechanism does.
    //
    // The range-routing mutant is why the floor is not 20: at 20 it passed, and the
    // node sweep sees it otherwise only from seed 25, so the gate's twenty were green.
    // PROPOSED(D-086): a floor on the snapshot actions a seed asks for.
    const ACTIONS_A_SEED: usize = 30;
    assert!(
        coverage.snapshot_actions >= ACTIONS_A_SEED * seeds as usize,
        "the node asked for {} snapshot actions over {seeds} seeds, under the floor of \
         {ACTIONS_A_SEED} a seed: a replica that asks for a take or a compaction record and \
         is not answered keeps `take_pending` set and never asks again, which is how a range \
         wedges (PROPOSED D-086)",
        coverage.snapshot_actions
    );
    println!(
        "node: the correct node re-took a range at an index it had already taken it at on \
         {}/{seeds} seeds — which is legitimate after an install — and into the **same \
         directory** on {}/{seeds}",
        coverage.took_an_index_twice, coverage.retook_into_one_directory
    );
    assert_eq!(
        coverage.retook_into_one_directory, 0,
        "the correct node took a snapshot of a range at an index it had already taken that \
         range at *and wrote it into the same version directory*, which a stream may have \
         open: that is the behaviour `SharedSnapshotDir` exists to be the opposite of, and \
         the take counter exists to prevent"
    );
    verdict(&verdicts).expect("the correct node passes every seed");
}

/// One heartbeat interval: two ticks (SHARD.md §4), and the threshold §4 sets for
/// the median apply lag.
const HEARTBEAT: Duration = Duration::from_millis(20);

fn median_of(sorted: &[Duration]) -> Option<Duration> {
    (!sorted.is_empty()).then(|| sorted[(sorted.len() - 1) / 2])
}

fn medians(by_range: &BTreeMap<u64, Vec<Duration>>) -> (Option<Duration>, BTreeMap<u64, Duration>) {
    let mut all: Vec<Duration> = Vec::new();
    let mut per_range = BTreeMap::new();
    for (range, lags) in by_range {
        let mut lags = lags.clone();
        all.extend(lags.iter().copied());
        lags.sort_unstable();
        if let Some(median) = median_of(&lags) {
            per_range.insert(*range, median);
        }
    }
    all.sort_unstable();
    (median_of(&all), per_range)
}

/// The seeds a variant caught at a high rate runs: a tenth of the tier and never
/// fewer than twenty, `sim/tests/engine.rs`'s `high_rate_share` (D-055, D-061).
///
/// The rate the assertion rests on is over the share, as D-061 requires, and the
/// share is what keeps this binary's cost near the sweeps beside it. Every variant
/// run on the share was measured over the share before the assertion was written,
/// and the lowest of them is `ResetTimerOnAnyRpc`: at its rate a share of twenty
/// catches none with probability about 1e-6 and a share of a hundred about 1e-30.
// PROPOSED(D-082): the high-rate variants on the node run a share of the tier.
fn high_rate_share() -> u64 {
    (seeds() / 10).max(seeds().min(20))
}

/// The violations `variants` was caught by over the share, with the rate and the
/// mechanisms printed.
///
/// It returns the violations and not a count, because a count cannot tell a catch by
/// the check the variant is about from a catch by something else that happened to go
/// wrong first. §12 asks each variant to be re-asserted "to the standard its Phase 2
/// test asserts and no stronger", and two of Phase 2's tests assert the mechanism by
/// name rather than a bare catch.
// PROPOSED(D-082): a catch on the node is attributed, not counted.
fn caught_on(name: &str, variants: impl Into<Variants> + Copy + Send + Sync) -> Vec<String> {
    let seeds = high_rate_share();
    let caught: Vec<String> = sweep(seeds, |seed| checked(&buggy(seed, variants)).err())
        .into_iter()
        .flatten()
        .collect();
    let rate = caught.len() as f64 * 100.0 / seeds as f64;
    let mut by_check: BTreeMap<&str, usize> = BTreeMap::new();
    for violation in &caught {
        *by_check.entry(mechanism(violation)).or_default() += 1;
    }
    println!(
        "node: {name} caught on {}/{seeds} seeds ({rate:.1}%), by {by_check:?}, first: {}",
        caught.len(),
        caught.first().map_or("", String::as_str)
    );
    caught
}

/// Which check a violation came from, for the attribution `caught_on` prints.
///
/// The names are the checks' own words. `server failed` is not a check at all: it is
/// the node telling the scenario its own apply stream had a hole, which is a real
/// catch of a real bug and a different statement from a safety fold's.
// PROPOSED(D-082): a catch on the node is attributed, not counted.
fn mechanism(violation: &str) -> &'static str {
    for (needle, name) in [
        ("pre-vote:", "pre-vote"),
        ("timers:", "timers"),
        ("state machine safety", "state machine safety"),
        ("committed entries", "committed entries stay"),
        ("commit majority:", "commit by majority"),
        ("commit by current term", "commit by current term"),
        ("log matching", "log matching"),
        ("election safety", "election safety"),
        ("leader completeness", "leader completeness"),
        ("linearizability", "linearizability"),
        ("liveness", "liveness"),
        ("follower log:", "the follower-log bound"),
        ("failed:", "the node failed"),
    ] {
        if violation.contains(needle) {
            return name;
        }
    }
    "something else"
}

// Phase 2's variants on `sim/raft.rs`'s arms, re-asserted on the node (§10, §12).
// Each is caught on some seed at every tier on the one-group server; the rate on
// the node was measured before the assertion was written (D-061) and is in the
// entry. Where a rate falls under 5 % the assertion moves to the thousand-seed
// tier, as D-061 requires, and says so.

#[test]
fn a_server_that_sends_before_it_persists_is_caught_on_the_node() {
    // Against Q41's round the one that matters most: a send that follows a core's
    // persist leaves when that persist resolves, and the variant sends it first
    // (§10). On the node the round submits four ranges' persists together, so the
    // send the variant lets out early races a sync that carries other ranges' work.
    let caught = caught_on("SendBeforePersist", Variant::SendBeforePersist);
    assert!(
        !caught.is_empty(),
        "SendBeforePersist was never caught on the node"
    );
}

#[test]
fn a_server_that_applies_before_commit_is_caught_on_the_node() {
    // Its Phase 2 test asserts a bare catch, so this does too (§12, "no stronger").
    // The attribution is printed rather than asserted, and it is worth reading: on
    // this tier three of the twenty catches are the **node failing**, not a safety
    // fold — `server 1 failed: range 2: apply of 189 after 186`, the node's own apply
    // task refusing a hole in its applied stream. That is a real catch of the real
    // bug by the node's own oracle, and it is a different statement from state
    // machine safety's, which is why the entry says so rather than leaving it in the
    // count.
    let caught = caught_on("ApplyBeforeCommit", Variant::ApplyBeforeCommit);
    assert!(
        !caught.is_empty(),
        "ApplyBeforeCommit was never caught on the node"
    );
}

#[test]
fn a_server_without_pre_vote_is_caught_on_the_node() {
    // Its Phase 2 test asserts the **mechanism**, not a bare catch: `by_pre_vote > 0`
    // (`sim/tests/raft.rs`). §12 asks for that standard and no stronger, so a bare
    // catch here would be weaker than Phase 2's and this asserts the same thing — the
    // catch is pre-vote's own property, a server raising its term while cut off.
    // PROPOSED(D-082): a catch on the node is attributed, not counted.
    let caught = caught_on("NoPreVote", Variant::NoPreVote);
    let by_pre_vote = caught
        .iter()
        .filter(|violation| violation.contains("pre-vote:"))
        .count();
    assert!(
        by_pre_vote > 0,
        "NoPreVote was never caught on the node by pre-vote's own property, which is what \
         its Phase 2 test asserts: {caught:?}"
    );
}

#[test]
fn a_leader_that_commits_an_older_terms_entry_by_count_is_caught_on_the_node() {
    // The Figure 8 driver's window (D-031), on the node: the burst writes a key of
    // the range the arm drew, so the backlog the restarted leader re-sends is that
    // range's.
    let caught = caught_on("CountOlderTermForCommit", Variant::CountOlderTermForCommit);
    assert!(
        !caught.is_empty(),
        "CountOlderTermForCommit was never caught on the node"
    );
}

#[test]
fn a_follower_that_truncates_on_every_append_is_caught_on_the_node() {
    let caught = caught_on("TruncateOnEveryAppend", Variant::TruncateOnEveryAppend);
    assert!(
        !caught.is_empty(),
        "TruncateOnEveryAppend was never caught on the node"
    );
}

#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught_on_the_node() {
    let caught = caught_on("ResetTimerOnAnyRpc", Variant::ResetTimerOnAnyRpc);
    assert!(
        !caught.is_empty(),
        "ResetTimerOnAnyRpc was never caught on the node"
    );
}

#[test]
fn a_leader_that_trusts_the_clock_is_caught_on_the_node() {
    // The lease trial on the node hands **every** range to the slowest clock, so a
    // node cut off with the reading client holds four leases and not one. What the
    // correct node does — revoke — and that some seed exceeds the drift bound at all
    // are asserted where the correct node is already run, in the sweep above; here
    // the buggy node runs alone, which is what keeps this test to one run a seed.
    //
    // The catch is a stale read, found by linearizability, and it is asserted from
    // the thousand-seed tier: exactly where the one-group test asserts it and no
    // stronger (§10, §12). On one group the stale read is caught on 4.0 % of the
    // first thousand seeds, and D-061 puts a catch under 5 % at `seeds() >= 1000` —
    // at 4 % the gate's twenty catch none with probability 0.44 and a hundred with
    // 0.017, so an assertion there would fail a tree with nothing wrong the day a
    // change redraws the schedules. The node's own rate is in the entry.
    let seeds = seeds();
    let caught: Vec<String> = sweep(seeds, |seed| {
        checked(&buggy(seed, Variant::LeaseTrustsTheClock)).err()
    })
    .into_iter()
    .flatten()
    .collect();
    let rate = caught.len() as f64 * 100.0 / seeds as f64;
    println!(
        "node: LeaseTrustsTheClock caught on {}/{seeds} seeds ({rate:.1}%), first: {}",
        caught.len(),
        caught.first().map_or("", String::as_str)
    );
    if seeds >= 1000 {
        assert!(
            !caught.is_empty(),
            "LeaseTrustsTheClock was never caught on the node"
        );
    }
}

#[test]
fn the_nodes_arms_aim_at_every_range_and_not_at_one() {
    // §11, env 8: a leader-relative arm on a node of many ranges chooses its range
    // from its own stream. What says the draw is spent is the set of ranges the
    // schedules of a run of seeds aim at — one range would mean three of the four
    // never saw a leader isolation, a leader crash or a Figure 8 burst at all.
    let mut aimed: BTreeSet<u64> = BTreeSet::new();
    for seed in 0..64 {
        let schedule = raft::Schedule::draw_on_the_node(seed);
        assert_eq!(schedule.range_picks.len(), schedule.faults.len());
        for i in 0..schedule.faults.len() {
            aimed.insert(schedule.range_of(Cluster::Node, i));
        }
    }
    println!("node: over 64 seeds the arms aimed at ranges {aimed:?}");
    assert_eq!(
        aimed,
        ranges().into_iter().collect::<BTreeSet<u64>>(),
        "the arms do not reach every range of the node"
    );
}

#[test]
fn a_seed_replays_to_the_same_trace_on_the_node() {
    // The node's schedule is the simulator's: one engine, one socket and four cores
    // on one ticker, with the persists of a round resolved in the order the
    // scheduling stream draws (D-073). Two runs of one seed must still be the same
    // run, records and all (SPEC.md §1.6).
    let first = correct(2);
    let second = correct(2);
    assert_eq!(first.records.len(), second.records.len());
    for (a, b) in first.records.iter().zip(&second.records) {
        assert_eq!(a.at, b.at);
        assert_eq!(a.event, b.event);
    }
}

// Phase 2's four **stream** variants, re-asserted on the node (§10, §12's Stage B).
//
// These four were the part of Stage B's variant criterion that PROPOSED D-082 recorded
// as blocked: "until [the install path] exists a variant on that path cannot be
// re-asserted on the node at all". PROPOSED D-083 built the wiring and left them owed,
// by name. This slice puts them under `sim/raft.rs`'s arms, with the node's
// `snapshot_threshold` at the one-group sweep's 12 and `Fault::CrashInstalling` and
// `Fault::RetakeUnderStream` back in `Schedule::draw_on_the_node`.
//
// Each is asserted to the standard its Phase 2 test asserts and no stronger, at the
// tier that test uses today (Q39). Every rate below was measured on the node before its
// assertion was written (D-061) and is printed at every tier.

/// The install that makes its switch without the range's repair (RAFT.md:694, D-066),
/// on the node — **injected, and not yet caught at its Phase 2 tier**.
///
/// Phase 2's standard is `is_caught`: caught on some seed at every tier
/// (`a_server_that_installs_without_current_last_is_caught`, sim/tests/raft.rs). On the
/// node the measured rate does not support it, and D-061's rule is that the tier a
/// Phase 2 variant keeps is the owner's, so the catch is **not** asserted here and the
/// numbers go to the owner instead of a quieter assertion (PROPOSED D-086):
///
/// - the variant is **injected**, which the assertion below tests from the
///   thousand-seed tier: the arm has to have crashed its victim at the final chunk of
///   the range it drew for the switch-without-repair to have been reached at all, and
///   that count is 0 when the variant's bit is disconnected;
/// - `Fault::CrashInstalling` reached the final chunk of the range it drew and crashed
///   its victim there on **0 of 100** seeds and **1 of 1 000**, against one group, where
///   the arm's whole scenario is the one range it has. On a node the victim is drawn
///   without regard to which of its four ranges it is behind on, so the arm must find a
///   victim that is behind *that* range's compacted prefix, designated for it, and
///   streamed within `INSTALL_WAIT_BUDGET`;
/// - the catch is **0 of 1 000**, which at an arm firing on a tenth of a per cent is
///   what a variant that is never aimed at looks like, not one that is aimed at and
///   survives.
///
/// So the catch is asserted nowhere, and the arm's firing only from the **nightly's ten
/// thousand**: at 0.1 % a thousand seeds see none about one run in three. **Below that
/// tier this test asserts nothing about the variant**, which is said here rather than
/// left to be discovered. The rate is printed at every tier so the day the arm is aimed
/// better the number is visible, and the variant keeps its Phase 2 assertion on
/// `Cluster::OneGroup`, which this sweep leaves running exactly as it is.
// PROPOSED(D-086): Phase 2's stream variants re-asserted on the node.
#[test]
fn a_server_that_installs_without_current_last_is_injected_on_the_node() {
    let seeds = seeds();
    let outcomes: Vec<(Option<String>, usize, usize)> = sweep(seeds, |seed| {
        let report = buggy(seed, Variant::SnapshotWithoutCurrentLast);
        (
            checked(&report).err(),
            report.aimed_installs,
            report.snapshot_actions(),
        )
    });
    let caught: Vec<&String> = outcomes.iter().filter_map(|(v, _, _)| v.as_ref()).collect();
    let fired = outcomes.iter().filter(|(_, aimed, _)| *aimed > 0).count();
    let actions: usize = outcomes.iter().map(|(_, _, a)| a).sum();
    let rate = caught.len() as f64 * 100.0 / seeds as f64;
    println!(
        "node: SnapshotWithoutCurrentLast caught on {}/{seeds} seeds ({rate:.1}%), the install \
         crash reached the final chunk of the range it drew and crashed there on {fired}/{seeds} \
         seeds, {actions} snapshot actions asked for, first: {}",
        caught.len(),
        caught.first().map_or("", |v| v.as_str())
    );
    // **The arm's own firing, from the thousand-seed tier.** This is the only thing
    // here that can fail, and until PROPOSED D-086's review the test asserted
    // `actions > 0` instead — which `checked()` already asserts on every seed of every
    // sweep, so the test could not fail at all. Removing the variant's bit from
    // `with_repair` left the node binary green at a hundred seeds and
    // `sim/tests/install.rs` green at twenty: a bit set, carried, and read by nothing,
    // which is the exact shape D-082 found `SendBeforePersist` in and this entry
    // claimed to have fixed.
    //
    // `aimed_installs` is the discriminator, and it is **thin**: the arm reaches the
    // final chunk of the range it drew on **0 of 100** seeds and **1 of 1 000** — about
    // a tenth of a per cent. Asserted from a thousand, as this first did, it would see
    // none about one run in three and fail a tree with nothing wrong, which is the
    // model error D-030 and D-039 forbid; asserted from ten thousand the sample expects
    // about ten and sees none about once in 20 000.
    //
    // So it is asserted **only at the nightly's tier**, and the consequence is stated
    // rather than buried: below ten thousand seeds this test asserts nothing about
    // `SnapshotWithoutCurrentLast` at all. Its catch is 0 of 1 000 and is asserted
    // nowhere. That is the variant's real state on this node and it goes to the owner;
    // aiming the arm at a range its victim is behind on is what would change it, and
    // that is a change to how the arms are drawn (PROPOSED D-086).
    if seeds >= 10_000 {
        assert!(
            fired > 0,
            "`Fault::CrashInstalling` never crashed its victim at the final chunk of the \
             range it drew over {seeds} seeds, so `SnapshotWithoutCurrentLast` was not \
             injected at the moment it is about and its 0 catches say nothing"
        );
    }
    let _ = actions;
}

/// The leader that shares one snapshot directory per index and streams one follower at
/// a time (D-043), on the node.
///
/// Phase 2 asserts four things
/// (`a_leader_that_shares_one_snapshot_directory_and_streams_one_follower_at_a_time_is_caught`).
/// All four are asserted, each at the tier **this cluster's** measured rate supports
/// rather than one group's — three at Phase 2's own tiers, one moved down:
///
/// - **the fault fired**, at every tier as Phase 2 has it: a take at an index that
///   server had already taken **of that range**, the re-take the variant rewrites one
///   directory for. **100 of 100** seeds — every one;
/// - **the aimed arm reached its stream**, moved to the **nightly's** tier where Phase 2
///   has it at every tier. **1 of 100** and 21 of 1 000, about 2 %: the variant wedges
///   the run so readily that `RetakeUnderStream`'s own setup often never completes, and
///   a thousand's sample of a hundred would see none about one run in eight;
/// - **a re-take landing under a live stream the follower never installs at** — from
///   the **thousand-seed** tier, where Phase 2 has it from a hundred. **29 of 100**
///   (17 of 100 and 172 of 1 000 before the merge with `phase-3-stage-b-wiring`),
///   against 13.5 % of a thousand there. The tier was argued from 17 %, at which the
///   hundred-seed tier's sample of twenty sees none about one run in forty; at 29 % that
///   tier would hold, so the assertion is left where it is and the number goes to the
///   owner rather than a merge moving a tier (PROPOSED D-086). It is *not* "the wedge's
///   stream half": pre-merge it accounted for at most 17 of the 30 catches, and after
///   the merge it fires on more seeds than are caught at all, so it cannot be a subset
///   of them. The dominant observable is the rewrite making a version unfindable —
///   miss, retake, cascade, wedge;
/// - **the liveness catch at ten thousand**, Phase 2's own tier and no stronger. On the
///   node it is **26 of 100** (30 of 100 and 266 of 1 000 before that merge), against
///   one group's **4 of 10 000**. The
///   range stays in the name, so it is not ranges colliding with each other: it is that
///   a node takes four ranges' snapshots through one `apply` task and one `snapshot`
///   task, so repeated applied indices — and therefore takes into a directory a stream
///   already has open — come round far more often than on a server with one range. That
///   rate would carry an assertion at a hundred seeds and it is deliberately not written
///   there — §12 re-asserts a Phase 2 variant to its own standard **and no stronger**.
///
/// Every fold behind these is keyed by `(server, range, index)` since PROPOSED D-086.
/// On one group they were keyed by `(server, index)`, which named a take uniquely
/// there; on a node four ranges cross `snapshot_threshold` within a few indices of one
/// another, so `(server, index)` names two different snapshots of two different ranges
/// over and over.
///
/// **None of these numbers existed until the variant could express its bug.** Until
/// PROPOSED D-086's review the node's take did not empty its version directory before
/// checkpointing, `Engine::checkpoint_spans` refuses a directory that is not empty, and
/// the variant's re-take therefore *failed* rather than rewriting — so all three folds
/// read zero and the first measurement of this variant on the node was a measurement of
/// that defect. `ServerApplier::take` empties it now, as `snapshot::take_numbered`
/// always has for the one-group server.
// PROPOSED(D-086): Phase 2's stream variants re-asserted on the node.
#[test]
fn a_leader_that_shares_one_snapshot_directory_is_caught_on_the_node() {
    // The share, as the high-rate variants use (D-055, D-061), and here it is a cost
    // decision as much as a statistical one. This variant **wedges the node on 26 % of
    // seeds**, against one group's 4 in 10 000, and a wedged run plays out its whole
    // length with a scrambled stream restarting under it — so the full tier costs many
    // times what the correct node's does. At rates of 100 % (the fault), 29 % (the
    // scramble) and 26 % (the catch) a share measures each as well as the tier: over a
    // share of 20 the catch is missed with probability 0.74^20, about one run in 350.
    // The rate is over the share, as D-061 requires (PROPOSED D-086).
    // The four were re-measured on the merge with `phase-3-stage-b-wiring`, whose
    // re-seed, verification pass and snapshot fixes move every schedule: the fault and
    // the arm did not move, the scramble went 17 % to 29 % and the catch 30 % to 26 %.
    // **Two numbers, named apart, and every tier gate below reads `tier`.** They were
    // one until PROPOSED D-086's re-review: `seeds` held the *share* and the gates
    // compared it against 100, 1 000 and 10 000, so each assertion sat a tier higher
    // than it claimed and the liveness catch — the whole point of re-asserting this
    // variant — needed `ANANKE_SEEDS` of 100 000 and never ran at all. At the nightly's
    // ten thousand the test printed `/1000`, exited 0 and reached none of it. That is
    // the same defect as the variant that could not express its bug, one level up: a
    // check that cannot fail, reported as a check that passes.
    // PROPOSED(D-086): the tier gates read the tier, the counts read the share.
    let tier = seeds();
    let share = high_rate_share();
    let outcomes: Vec<(Option<String>, bool, usize, bool, usize)> = sweep(share, |seed| {
        let report = buggy(seed, Variant::SharedSnapshotDir);
        let scrambled: Vec<_> = report
            .retakes_under_streams()
            .into_iter()
            .filter(|retake| !retake.installed_after)
            .collect();
        // D-043's own symptom under a scrambled stream: the follower answering `More`
        // for a file it has already been sent, over and over — of that range's stream.
        let looped = scrambled
            .iter()
            .map(|retake| {
                report.duplicate_chunk_loop(
                    retake.leader,
                    retake.range,
                    retake.follower,
                    retake.retook,
                )
            })
            .sum();
        (
            checked(&report).err(),
            took_an_index_twice_on_the_node(&report),
            report.aimed_streams,
            !scrambled.is_empty(),
            looped,
        )
    });
    let caught: Vec<&String> = outcomes
        .iter()
        .filter_map(|(v, _, _, _, _)| v.as_ref())
        .collect();
    let fired = outcomes.iter().filter(|(_, fired, _, _, _)| *fired).count();
    let aimed = outcomes
        .iter()
        .filter(|(_, _, aimed, _, _)| *aimed > 0)
        .count();
    let scrambled = outcomes.iter().filter(|(_, _, _, s, _)| *s).count();
    let looped: usize = outcomes.iter().map(|(_, _, _, _, l)| l).sum();
    let liveness = caught.iter().filter(|v| v.contains(": liveness: ")).count();
    println!(
        "node: SharedSnapshotDir caught on {}/{share} seeds (tier {tier}), {liveness} by the \
         liveness check, re-took at an index already taken of one range on {fired} seeds, \
         scrambled a live stream the follower never installed after on {scrambled} seeds \
         ({looped} duplicate-chunk loops after those), the aimed re-take arm reached its \
         stream on {aimed} seeds, first: {}",
        caught.len(),
        caught.first().map_or("", |v| v.as_str())
    );
    // **The fault itself, at every tier**, as Phase 2 asserts it: a take at an index
    // this server had already taken **that range** at, which is the re-take the variant
    // rewrites one directory for. **100 of 100** seeds — every one. The shared name is
    // why it is every one and not a fraction: with the take counter pinned, a second
    // take at a repeated applied index writes the directory the first wrote, so the
    // re-take is a re-take by construction rather than by coincidence.
    assert!(
        fired > 0,
        "SharedSnapshotDir never re-took at an index it had already taken that range at: the \
         fault was not injected on any of the {share} seeds"
    );
    // **A re-take landing under a live stream the follower never installs at, from the
    // hundred-seed tier**, which is Phase 2's own tier for it. **29 of 100** on the
    // merged tree, 17 of 100 before it, both above D-061's 5 %.
    //
    // It is *not* "the wedge's stream half", which is what this comment called it
    // until the re-review: pre-merge it accounted for a minority of the catches — at
    // most 17 of the 30 caught, and 4 of 25 on a per-seed probe — and on the merged tree
    // it fires on 29 seeds against 26 caught, so it is not a subset of them at all. The dominant observable is the
    // rewrite making the version unfindable: a lookup miss, then a retake, then the
    // cascade, then the wedge. Under the variant those misses run about 64 000 over a
    // hundred seeds against about 1 100 on the correct node. Same bug, and this fold
    // sees one face of it.
    //
    // **The tier is the thousand, not Phase 2's hundred, and the reason is the share.**
    // The share at tier T is T/10, so CI's hundred runs twenty seeds here: at 17 % a
    // sample of twenty sees none about one run in forty, which is a flake, and a
    // thousand's sample of a hundred sees none about once in 10^8. This is a weakening
    // of Phase 2's tier, it is weaker because the sample says so, and it goes to the
    // owner with the number (PROPOSED D-086).
    // **The merge with `phase-3-stage-b-wiring` took this rate to 29 %**, at which a
    // sample of twenty sees none about one run in nine hundred and Phase 2's own
    // hundred-seed tier would hold. The assertion is left here deliberately: putting it
    // back is a decision about a tier and belongs to the owner, not to a merge.
    if tier >= 1000 {
        assert!(
            scrambled > 0,
            "SharedSnapshotDir never re-took into a directory a live stream had open and left \
             unfinished over {share} seeds"
        );
    }
    // **The aimed arm, from the nightly's ten thousand**, where Phase 2 asserts it at
    // every tier. It reaches a stream of the range it drew on **1 of 100** seeds and 21
    // of 1 000 — about 2 %, under D-061's 5 % — because the variant wedges the run so
    // readily that `RetakeUnderStream`'s own setup often never completes. With the
    // directory half disabled the arm returns to 15 of 100, which is what says the fall
    // is the variant's effect and not a broken arm. At a thousand the sample is a
    // hundred and sees none about one run in eight, so the assertion belongs a tier
    // higher still. This is the largest of the two weakenings here and it is the
    // owner's to confirm (PROPOSED D-086).
    if tier >= 10_000 {
        assert!(
            aimed > 0,
            "the aimed re-take arm never reached a stream of the range it drew on the node \
             over {share} seeds"
        );
    }
    // **The liveness catch at the nightly's ten thousand, Phase 2's tier for it and no
    // stronger.** On the node it is **26 of 100** on the merged tree — 30 of 100 and
    // 266 of 1 000 before the merge — against one group's 4 of 10 000: the variant
    // wedges a node far more readily than a server. The range
    // stays in the directory name, so this is not two ranges colliding — it is that one
    // `apply` task takes for four ranges and one `snapshot` task streams for them, so a
    // repeated applied index, and with it a take into a directory a stream has open,
    // comes round far more often than on a server with one range. The rate would carry an assertion at a hundred seeds and is
    // deliberately not written there — §12 re-asserts a Phase 2 variant to its own
    // standard and no stronger.
    if tier >= 10_000 {
        assert!(
            liveness > 0,
            "SharedSnapshotDir's wedge was never caught by the liveness check on the node"
        );
    }
    println!(
        "node: SharedSnapshotDir's liveness catch is {liveness}/{share} over a tier of \
         {tier}, asserted from ten thousand as Phase 2 asserts it and no stronger"
    );
}

/// Whether some server took a snapshot **of one range** at an index it had already
/// taken that range at: the re-take D-043's variant rewrites one directory for.
///
/// Keyed by `(server, range, index)`. `sim/tests/raft.rs`'s own `took_an_index_twice`
/// keys by `(server, index)`, which is the same key on a server of one group and is
/// **not** on a node: four ranges taking at one index would answer true on the correct
/// node (PROPOSED D-086).
fn took_an_index_twice_on_the_node(report: &raft::Report) -> bool {
    let takes = report.snapshot_takes();
    takes.iter().enumerate().any(|(n, take)| {
        takes[..n].iter().any(|earlier| {
            earlier.server == take.server
                && earlier.range == take.range
                && earlier.index == take.index
        })
    })
}

/// Whether some server took a snapshot of one range at an index it had already taken
/// that range at **into the directory it had already written**: `SharedSnapshotDir`'s
/// symptom exactly, and the thing the correct node's take counter exists to prevent.
///
/// The index alone is *not* the property, and finding that out is what the campaign
/// behind PROPOSED D-086 was for. **The correct node** re-takes at an index it has
/// already taken at on **52 of 100 seeds** — its own figure, not one group's, and 44 of
/// 100 before the merge with `phase-3-stage-b-wiring` — and does so legitimately: after
/// a live install the
/// core's `taken` names a snapshot this replica never took (D-078), the record behind
/// it names an older one, the stream asks for a take, and the applied index has not
/// moved — so the take is at the same index, into `snap-r<range>-<index>-<take + 1>`,
/// a directory of its own that no stream can be reading. Asserting the index alone
/// would have been a bound the correct system trips, which is a model error and not a
/// bound to keep (D-030, D-039).
// PROPOSED(D-086): the take counter's property is about the directory, not the index.
fn retook_into_one_directory(report: &raft::Report) -> bool {
    let takes = report.snapshot_takes();
    takes.iter().enumerate().any(|(n, take)| {
        takes[..n].iter().any(|earlier| {
            earlier.server == take.server
                && earlier.range == take.range
                && earlier.index == take.index
                && earlier.dir == take.dir
        })
    })
}

/// `IgnoreIncarnation` on the node (D-042), and **why it is still blocked** — not by
/// the snapshot wiring, which this slice puts under these arms, but by Q15's whole-node
/// refusal and re-seed, which is PR #86's.
///
/// Phase 2 asserts two things
/// (`a_leader_that_ignores_incarnations_never_forgets`, sim/tests/raft.rs):
///
/// - **the injection**, at every tier: the leader as built never forgets a follower's
///   progress, so it traces no `RaftProgressReset` on any seed, "while the correct
///   server's own sweep above requires one wherever it saw a refusal";
/// - **the reach**, from a hundred seeds: a refused follower re-seeded and applying
///   again, the state the wedge is built on.
///
/// Neither is assertable here, and the reason is one fact with a citation. A leader
/// resets a follower's progress when the store incarnation that follower answers with
/// **changes** (`Raft::note_incarnation`, core.rs:1834-1862), and a store's incarnation
/// is "1 for a store started fresh, a fresh value on every store a re-seed rebuilt"
/// (store.rs:28). The node's live install deliberately **keeps** the incarnation — "an
/// install into a live store keeps its incarnation: the kept tail is everything
/// acknowledged past the snapshot, so nothing a leader matched is lost"
/// (server.rs, `ServerHost::repair`, D-042) — so on this node no replica's incarnation
/// ever changes, the correct leader never resets a progress either, and asserting that
/// the variant's leader traces no reset would pass on a node where nothing was
/// injected. That is the one failure mode a sweep cannot report on its own.
///
/// What this test does instead is assert the **absence with its reason and its
/// non-vacuity** (CLAUDE.md): the correct node traces no `RaftProgressReset` and no
/// refusal, over a run that is asserted to reach the install path. The day Q15's
/// re-seed lands, a reset appears here and this test fails, which is when the variant
/// can be re-asserted rather than a day later.
// PROPOSED(D-086): `IgnoreIncarnation` is blocked on Q15's re-seed, not on the wiring.
#[test]
fn a_leader_that_ignores_incarnations_has_no_incarnation_to_ignore_on_the_node_yet() {
    // The share, as the high-rate variants use (D-055, D-061): this runs the correct
    // node *and* the variant on every seed it takes, so the full tier would cost twice
    // the correct sweep beside it, and what it asserts is an absence of a mechanism
    // that is absent by construction — a store incarnation that never changes — rather
    // than a rate that more seeds would sharpen.
    let seeds = high_rate_share();
    let outcomes: Vec<(usize, usize, usize, usize)> = sweep(seeds, |seed| {
        let correct = correct(seed);
        let buggy = buggy(seed, Variant::IgnoreIncarnation);
        let resets = |report: &raft::Report| {
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftProgressReset { .. }))
        };
        (
            resets(&correct),
            resets(&buggy),
            correct.refused.len(),
            correct.snapshot_actions(),
        )
    });
    let correct_resets: usize = outcomes.iter().map(|(c, _, _, _)| c).sum();
    let buggy_resets: usize = outcomes.iter().map(|(_, b, _, _)| b).sum();
    let refusals: usize = outcomes.iter().map(|(_, _, r, _)| r).sum();
    let actions: usize = outcomes.iter().map(|(_, _, _, a)| a).sum();
    println!(
        "node: IgnoreIncarnation over {seeds} seeds — the correct node reset a follower's \
         progress {correct_resets} times and the variant {buggy_resets}, over {refusals} store \
         refusals and {actions} snapshot actions. A leader resets on a *change* of store \
         incarnation, and only a re-seed rebuilds a store with a fresh one (Q15, PR #86); a \
         live install keeps it (D-042). So neither leader has anything to forget yet"
    );
    // Non-vacuity: the install path is reached on these seeds, so this is an absence of
    // incarnation changes and not an absence of runs.
    assert!(
        actions > 0,
        "no core asked for a snapshot action over {seeds} seeds, so this says nothing"
    );
    // The absence, with its reason. Both halves are asserted so a move either way is
    // seen: the day a store's incarnation changes on this node, the correct leader
    // resets and this fails, and the variant can be re-asserted at its Phase 2 tier.
    assert_eq!(
        correct_resets, 0,
        "the correct node reset a follower's progress, so a store incarnation changed on it: \
         Q15's re-seed has landed and `IgnoreIncarnation` can be re-asserted here now — \
         re-audit this test rather than this absence"
    );
    assert_eq!(
        buggy_resets, 0,
        "the leader that ignores incarnations reset a follower's progress, which is the fix \
         the variant turns off"
    );
    assert_eq!(
        refusals, 0,
        "a store was refused on the node, which is Q15's path (PR #86): re-audit this test"
    );
}

/// The pair `{IgnoreIncarnation, SharedSnapshotDir}` on the node, and what became of
/// **seed 680**, which D-045 pinned it on (RAFT.md:658-661; sim/tests/raft.rs:405).
///
/// SHARD.md's Stage B says the node's schedule move retires the pin, that seed 680's
/// test asserts the wedge where the moved schedule still reaches it or, with the reason,
/// the situation's absence, and that the first thousand seeds are searched again for a
/// seed the pair is caught on.
///
/// **Seed 680's own pin did not move, and the evidence is not an argument.** This slice
/// changes nothing about `Cluster::OneGroup`: seed 42's moirae JSONL is 12 898 025
/// bytes and hashes to
/// `445f970010f9d493d489d7627543b77cccba863cb5675af4602686f5de182217` on this branch,
/// which is the hash D-082 recorded for `origin/main` and for its own branch. So
/// `seed_680_which_pinned_the_combined_variant_before_d056_no_longer_wedges` runs the
/// run it was pinned on and keeps saying exactly what it says, and this slice does not
/// touch it.
///
/// **On the node the pair is the stream half alone**, because the incarnation half has
/// nothing to ignore until Q15's re-seed lands (see the test above). So the search
/// SHARD.md asks for cannot be run against the node yet: a seed the pair is "caught on"
/// here would be a seed `SharedSnapshotDir` alone is caught on, which is not a wedge
/// that needs both bugs and so is not what D-045 pinned. That goes to the owner as the
/// entry records, and this test pins what the node *does* do, so that the day the other
/// half arrives the difference is visible.
// PROPOSED(D-086): the pair on the node is the stream half alone until Q15's re-seed.
#[test]
fn the_pair_on_the_node_is_the_stream_half_alone_until_the_reseed_lands() {
    // The share (D-055, D-061): this runs three variants on every seed it takes, and
    // what it asserts is the *equality* of two of them — a structural claim about one
    // half being a no-op, which a share settles as well as a tier and at a tenth of
    // the cost.
    let seeds = high_rate_share();
    let both = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
    let outcomes: Vec<(u64, bool, bool, bool)> = sweep(seeds, |seed| {
        (
            seed,
            checked(&buggy(seed, both)).is_err(),
            checked(&buggy(seed, Variant::SharedSnapshotDir)).is_err(),
            checked(&buggy(seed, Variant::IgnoreIncarnation)).is_err(),
        )
    });
    // **Per seed, not per count.** Counts were what this compared until PROPOSED
    // D-086's review, and a count equality cannot see the pin disappear: a genuine
    // D-045 wedge on one seed and a stream-only catch on another leave `pair` and
    // `stream` equal, and the wedge — the whole reason the pair exists — goes
    // unreported. The sets are compared, and the seeds where the pair is caught and
    // neither half alone is are asserted empty, which is the wedge itself.
    let seeds_where = |f: fn(&(u64, bool, bool, bool)) -> bool| -> BTreeSet<u64> {
        outcomes.iter().filter(|o| f(o)).map(|o| o.0).collect()
    };
    let pair = seeds_where(|o| o.1);
    let stream = seeds_where(|o| o.2);
    let incarnation = seeds_where(|o| o.3);
    let only_the_pair = seeds_where(|o| o.1 && !o.2 && !o.3);
    println!(
        "node: the pair caught on {}/{seeds} seeds {pair:?}, `SharedSnapshotDir` alone on {} \
         {stream:?}, `IgnoreIncarnation` alone on {} {incarnation:?}, and on {} seeds \
         {only_the_pair:?} the pair is caught where neither half alone is — which is the \
         wedge D-045 pinned",
        pair.len(),
        stream.len(),
        incarnation.len(),
        only_the_pair.len()
    );
    // The wedge itself, asserted rather than printed: a seed the pair is caught on and
    // neither half alone is, is a wedge that needs both bugs, which is what D-045
    // pinned seed 680 for and what this node cannot produce while `IgnoreIncarnation`
    // is a no-op on it. The day one appears, pin it here as D-045 pinned 680.
    assert!(
        only_the_pair.is_empty(),
        "the pair is caught on seeds {only_the_pair:?} where neither half alone is: that is a \
         wedge needing both bugs, which is what D-045 pinned seed 680 for — pin it here \
         rather than this absence, and take it to the owner"
    );
    // And the equality that says why: `IgnoreIncarnation` is a no-op on this node, so
    // the pair is exactly the stream half, seed for seed.
    assert_eq!(
        pair, stream,
        "the pair no longer catches exactly the seeds `SharedSnapshotDir` alone catches on \
         the node, so `IgnoreIncarnation` is doing something here now: search the first \
         thousand seeds for a wedge that needs both bugs and pin it, as D-045 pinned 680"
    );
    // `IgnoreIncarnation` alone is caught on 0 of 10 000 on one group too
    // (RAFT.md:697, SHARD.md:1579), so this is **not** the guard that sees Q15's
    // re-seed land — the two assertions above are. It is kept because a catch here
    // would mean something new either way.
    assert!(
        incarnation.is_empty(),
        "`IgnoreIncarnation` alone is caught on the node on seeds {incarnation:?}, which it \
         cannot be while a store's incarnation never changes: re-audit this test"
    );
}

/// A Phase 2 variant this node has no path for, run on the node anyway.
///
/// m1 of the review of PROPOSED D-082: until then the blocked variants were named in
/// prose and by nothing executable, so `cargo test --list` and the nightly's shard
/// table carried none of the debt. Each of these runs its variant over the share and
/// asserts that the correct-system checks still pass — the variant injects nothing,
/// because the path it breaks is not here. It is an absence with a reason, and its
/// upgrade trigger is the day the path it waits on lands: the variant starts being
/// caught, this test fails, and it asks to be turned into the assertion §10 wants.
///
/// **Four of the six that used to be here have made that crossing and are gone.**
/// PROPOSED D-086 reaches the stream path and re-asserts `SnapshotWithoutCurrentLast`,
/// `SharedSnapshotDir`, `IgnoreIncarnation` and D-045's pair as tests of their own,
/// above — each with the rate it was measured at. What is left waits on something this
/// branch does not build: `AdoptionAsBuilt` on Q15's refused directory, and
/// `RefusalNotDurable` on the whole-node refusal and re-seed, both PR #86's.
///
/// The second half of what this asserted is gone with them, and deliberately. It used
/// to check that the snapshot path was still *unreached*, through `checked`; `checked`
/// now asserts the opposite, because the path is reached on every seed (PROPOSED
/// D-086). So these two run over a node that takes snapshots and streams them, and
/// still catch nothing — which is a stronger statement of the same absence than the
/// one it replaces, not a weaker one.
// PROPOSED(D-082): each blocked variant is named by a test, not by prose.
// PROPOSED(D-086): the four the stream path reaches are re-asserted above, not blocked.
fn blocked_on_the_node(
    name: &str,
    variants: impl Into<Variants> + Copy + Send + Sync,
    waits_on: &str,
) {
    let seeds = high_rate_share();
    let caught: Vec<String> = sweep(seeds, |seed| checked(&buggy(seed, variants)).err())
        .into_iter()
        .flatten()
        .collect();
    println!(
        "node: {name} is not re-asserted here; over {seeds} seeds it was caught {} times, and \
         it waits on {waits_on}",
        caught.len()
    );
    assert!(
        caught.is_empty(),
        "{name} is caught on the node, so the path it breaks is reachable after all: turn this \
         absence into the assertion §10 asks for. {caught:?}"
    );
}

#[test]
fn a_server_whose_adoption_is_as_built_is_not_re_asserted_on_the_node_yet() {
    // The owner ruled on 2026-09-20 that this is re-asserted on the two rules that
    // remain rather than §10 amended. The two are the live install's single switch and
    // Q15's refused directory; neither path is in this tree. The third rule, a damaged
    // staging `CURRENT` refused, has no subject on a node that adopts no staged store.
    blocked_on_the_node(
        "AdoptionAsBuilt",
        Variant::AdoptionAsBuilt,
        "the node's snapshot wiring and PR #86's refused directory",
    );
}

#[test]
fn a_server_whose_refusal_is_not_durable_is_not_re_asserted_on_the_node_yet() {
    // The node *does* read this one (`quiesce_on_loss` in `server::run`), and it still
    // has nothing to do: the disk does not rot on this cluster, because a refusal
    // stops the node until Q15's whole-node re-seed lands.
    blocked_on_the_node(
        "RefusalNotDurable",
        Variant::RefusalNotDurable,
        "PR #86's whole-node refusal and re-seed",
    );
}
