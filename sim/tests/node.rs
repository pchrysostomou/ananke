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

/// The run's verdict: every check the one-group sweep makes, and then the two paths
/// this node has not got, asserted **absent on every seed with their reason**.
///
/// The absences are the point, not a formality. A variant whose situation the run
/// cannot reach is a test that passes because nothing was injected, which is the one
/// failure mode a sweep cannot report on its own (CLAUDE.md). This node takes no
/// snapshot and cannot be refused, so `snapshot_threshold` is held far above what a
/// run writes and the disk does not rot; if either changed and the sweep said
/// nothing, the install-path variants would look re-asserted on the node when
/// nothing had been injected at all. So the seed that first reaches one of these
/// fails here, naming the path and the slice that owns it.
// PROPOSED(D-082): what the node cluster does not reach yet, asserted absent.
fn checked(report: &raft::Report) -> Result<(), String> {
    let seed = report.seed;
    report.check()?;
    let actions = report.snapshot_actions();
    if actions > 0 {
        return Err(format!(
            "seed {seed}: {actions} snapshot actions were traced, a path this node does not \
             have: `ananke_shard::snapshot` is not wired to \
             `ananke_shard::server::ServerHost`. The install-path variants are not \
             re-asserted here, and a run that reaches this must say so rather than pass"
        ));
    }
    // The absence proper. A `RaftSnapshot` record is not evidence on its own: the
    // node's `Host::snapshot` bumps `Gaps::snapshot_actions` and traces nothing, so a
    // take the node dropped on the floor would leave the line above green against a
    // silence. What is asserted here is the condition behind every action a core can
    // ask for — a log longer than `snapshot_threshold` — which the trace does carry.
    let highest = report.highest_index();
    if highest >= raft::NODE_SNAPSHOT_THRESHOLD {
        return Err(format!(
            "seed {seed}: a replica reached index {highest}, at or past the {} this cluster \
             sets `snapshot_threshold` to, so a core could ask for a take, a record or an \
             install and the node would drop it in silence: the snapshot path is not wired \
             here",
            raft::NODE_SNAPSHOT_THRESHOLD
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
    /// The two paths this node has not got, counted so the absence is a number and
    /// not a hope.
    snapshot_actions: usize,
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
        self.snapshot_actions += report.snapshot_actions();
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
        assert!(
            self.terms_above_one > 0 && self.leaders > 0 && self.commits > 0,
            "the sweep elected nobody past term 1, or committed nothing"
        );
        // The two absences, asserted as numbers. `highest_index` is the one that
        // means something: the node's `Host::snapshot` traces nothing, so a take it
        // dropped would leave `snapshot_actions` at zero either way.
        assert_eq!(
            self.snapshot_actions, 0,
            "a snapshot action was traced on a node whose `snapshot` task is not wired"
        );
        assert_eq!(
            self.refusals, 0,
            "a store was refused on a node whose whole-node refusal is PR #86's"
        );
        assert!(
            self.highest_index < raft::NODE_SNAPSHOT_THRESHOLD,
            "a replica reached index {}, at or past this cluster's `snapshot_threshold`",
            self.highest_index
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

// --- The sharded check-quorum scenario (PROPOSED D-085, D-049, D-077) ---

use ananke_shard::variant::NodeVariant;
use ananke_sim::quorum::{self, NodeReport};

/// What a sweep of the sharded scenario saw, printed at every tier.
#[derive(Debug, Default)]
struct QuorumNodeFigures {
    seeds: u64,
    ranges_refused: usize,
    ranges_reseeded: usize,
    ranges_on_the_keepers_majority: usize,
    ranges_on_the_cut_off_leader: usize,
    commits_through_a_reseeded_replica: usize,
    step_downs: usize,
    /// The whole of D-049's rule on the node: rejections stamped incarnation 0.
    refused_rejections: usize,
    /// What the node answers instead: rejections carrying the re-seeded store's own
    /// incarnation, and answers that fitted.
    store_rejections: usize,
    fitted: usize,
    step_downs_naming_anyone_uncounted: usize,
    streams: usize,
    seeds_carrying_both_outcomes: u64,
}

impl QuorumNodeFigures {
    fn add(&mut self, report: &NodeReport) {
        self.seeds += 1;
        self.seeds_carrying_both_outcomes += u64::from(report.both_outcomes);
        let (Some(cast), Some(holds)) = (report.cast.as_ref(), report.holds()) else {
            return;
        };
        for hold in holds.values() {
            self.ranges_refused += usize::from(hold.replica_refused);
            self.ranges_reseeded += usize::from(hold.reseeded);
            self.refused_rejections += hold.refused_rejections;
            self.store_rejections += hold.store_rejections;
            self.fitted += hold.fitted;
            self.streams += hold.streams;
            if hold.leader == cast.keeper {
                self.ranges_on_the_keepers_majority += 1;
                self.commits_through_a_reseeded_replica +=
                    usize::from(hold.commit_after_cut.is_some());
            } else if hold.leader == cast.other {
                self.ranges_on_the_cut_off_leader += 1;
            }
            if let Some((_, uncounted)) = &hold.quorum_lost {
                self.step_downs += 1;
                self.step_downs_naming_anyone_uncounted += usize::from(!uncounted.is_empty());
            }
        }
    }
}

/// The sharded scenario over the tier's seeds under `variants` and `node`: every
/// seed's violation, if it has one, and the figures.
fn quorum_node_sweep(
    variants: impl Into<Variants> + Copy + Send + Sync,
    node: NodeVariants,
) -> (Vec<String>, QuorumNodeFigures) {
    let figures = std::sync::Mutex::new(QuorumNodeFigures::default());
    let violations: Vec<String> = sweep(seeds(), |seed| {
        let report = quorum::node_run(seed, variants, node);
        figures.lock().unwrap().add(&report);
        report.check().err()
    })
    .into_iter()
    .flatten()
    .collect();
    (violations, figures.into_inner().unwrap())
}

/// The sharded check-quorum scenario's positive control (PROPOSED D-085): **one
/// fault, four answers**.
///
/// One `mark_store_lost` at the victim's restart refuses the node and every replica
/// it holds (D-077), each re-seeded with a store incarnation of its own; the third
/// node is then cut off, and check quorum is asked separately of each of the four
/// ranges' leaders about that one refused node. On every seed the run carries **both**
/// outcomes at once, which no single-range scenario can: the ranges the keeper leads
/// keep their leader through the hold and commit past their commit index at the cut,
/// on a majority of the keeper and the node the refusal re-seeded; the ranges the
/// cut-off node leads lose theirs within two windows and three ticks and stay
/// leaderless, because a re-seeded replica neither votes nor campaigns (D-035) and the
/// keeper alone is no majority.
///
/// The figures print the two absences D-049's own halves need and the node does not
/// have; `d_049s_pair_has_no_site_on_the_node_and_this_says_the_day_it_does` is where
/// they are asserted.
// PROPOSED(D-085): sharded is one fault and four answers, not the scenario four times.
#[test]
fn the_sharded_quorum_scenario_asks_four_leaders_about_one_refused_node() {
    let (violations, figures) = quorum_node_sweep(Variants::default(), NodeVariants::correct());
    eprintln!(
        "sharded quorum, correct: {} of {} seeds failed, {figures:?}, first: {}",
        violations.len(),
        seeds(),
        violations.first().map_or("", String::as_str)
    );
    assert!(violations.is_empty(), "{}", violations[0]);
    // The shape the checks need, asserted rather than assumed: four replicas refused
    // and re-seeded a seed, both outcomes reached on every seed, and the answers that
    // carried the keeper's office actually delivered.
    let ranges = u64::try_from(ranges().len()).expect("small");
    assert_eq!(
        figures.ranges_refused as u64,
        figures.seeds * ranges,
        "the one refusal did not fan out to every range on every seed"
    );
    assert_eq!(figures.ranges_reseeded as u64, figures.seeds * ranges);
    assert!(
        figures.ranges_on_the_keepers_majority > 0 && figures.ranges_on_the_cut_off_leader > 0,
        "the sweep reached only one of the scenario's two outcomes"
    );
    assert!(
        figures.seeds_carrying_both_outcomes > 0,
        "no seed carried both outcomes at once, which is the whole of what a sharded \
         scenario says over a single-range one"
    );
    assert!(
        figures.fitted > 0,
        "no answer of a re-seeded replica fitted, so no keeper's office was kept on one"
    );
}

/// The node's own variant beside the correct node (the pair rule, CLAUDE.md):
/// `RefuseOneRangeOnly` (D-077) marks down only the range whose store open failed,
/// where a loss in the shared engine is every replica's.
///
/// **This is the catch a single-range world cannot make.** On one group, refusing the
/// one range *is* refusing the node, so the variant does nothing there and D-077
/// catches it on its own scenario's shape. Here it is caught from this scenario's
/// side, by the clause that asks the fan-out per range: three of the four replicas
/// were never refused, so they were never re-seeded either.
///
/// Measured before it was asserted (Q39, D-061): caught on every seed of the gate's
/// twenty, and the rate is printed at every tier.
// PROPOSED(D-085): sharded is one fault and four answers, not the scenario four times.
#[test]
fn a_node_that_refuses_only_one_range_is_caught_on_the_sharded_quorum_scenario() {
    let (caught, figures) = quorum_node_sweep(
        Variants::default(),
        NodeVariants::of(&[NodeVariant::RefuseOneRangeOnly]),
    );
    eprintln!(
        "sharded quorum, RefuseOneRangeOnly: caught on {} of {} seeds, {figures:?}, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert_eq!(
        caught.len() as u64,
        seeds(),
        "RefuseOneRangeOnly was not caught on every seed: {caught:?}"
    );
    assert!(
        caught
            .iter()
            .all(|v| v.contains("but not this range's replica")),
        "caught by something other than the fan-out: {caught:?}"
    );
}

/// D-049's pair, `RefusedCountsForQuorum` and `RefusedNeverCounts`, **has no site on
/// the node**, asserted per seed with the reason and with what would upgrade it.
///
/// This is what replaces D-085's tripwire, and it is narrower and sharper than the
/// tripwire was. D-085 read the scenario as blocked behind two paths and predicted
/// that PR #86's whole-node refusal would unblock the first half. **It did not**, and
/// the measurement is the reason: D-049's rule is keyed on *a rejection stamped
/// incarnation 0*, which is a server that has **no store** (`Progress::refused_answered`,
/// core.rs:2760; D-042). The one-group server has one: its re-seed loop answers from
/// no store at all until a leader's stream installs one. D-077's node has none — it
/// marks the loss, opens a **fresh engine beside the refused one** and creates every
/// range's store in it with a store incarnation of its own *before any replica
/// answers*, so every answer the node ever sends carries a store incarnation and
/// `refused_answered` is never set. Both variants then compute the same thing as the
/// correct core and change nothing.
///
/// So the two variants are run here and asserted **not caught**, per seed, beside the
/// counts that make that evidence rather than silence: the refusal landed, the node
/// answered, and not one of those answers was a store-less refused server's. The rate
/// is 0 of the tier's seeds and is printed at every tier. **Nothing is lowered**: §10's
/// standard for this pair — caught on every seed at every tier — is asserted where the
/// pair has a site, which is `sim/tests/raft.rs`'s four D-049 tests on the one-group
/// server, and this says why it cannot yet be asserted here.
///
/// The day it can, this fails. Two things would do it, and the message says which:
/// a node that answers from no store, and the snapshot wiring (PR #107) that gives
/// D-049's open half its stream. **The second alone will not be enough** — a leader
/// that compacts past a re-seeded replica's log will find that replica answering from
/// its own store, counted as any follower's, which is the hazard D-049 was written
/// about, reintroduced on the node. That is the finding this test carries to the
/// owner, and `NodeReport::check`'s install clause is where it comes due.
// PROPOSED(D-085): D-049's pair has no site on the node; the absence asserted per seed.
#[test]
fn d_049s_pair_has_no_site_on_the_node_and_this_says_the_day_it_does() {
    for variant in [Variant::RefusedCountsForQuorum, Variant::RefusedNeverCounts] {
        let (caught, figures) = quorum_node_sweep(variant, NodeVariants::correct());
        eprintln!(
            "sharded quorum, {:?}: caught on {} of {} seeds, {figures:?}",
            Variants::from(variant),
            caught.len(),
            seeds()
        );
        // The counts that make the absence evidence: the node was refused on every
        // seed and did answer, and D-049's own key never matched.
        assert_eq!(
            figures.ranges_reseeded as u64,
            figures.seeds * u64::try_from(ranges().len()).expect("small"),
            "the refusal did not land, so this seed's absence says nothing"
        );
        assert!(
            figures.store_rejections + figures.fitted > 0,
            "the refused node answered nothing at all, so the absence below says nothing"
        );
        assert_eq!(
            figures.refused_rejections, 0,
            "the node answered a rejection stamped incarnation 0: D-049's rule has a site \
             here now and its pair must be re-asserted on the node at §10's standard"
        );
        assert_eq!(
            figures.step_downs_naming_anyone_uncounted, 0,
            "a step-down left a follower uncounted, which only a refused server's answer can \
             do (core.rs `heard_this_window`): D-049's rule has a site on the node now"
        );
        assert_eq!(
            caught.len(),
            0,
            "{:?} is caught on the node: it has a site here now, so assert the catch at \
             §10's standard instead of this absence: {caught:?}",
            Variants::from(variant)
        );
    }
}

/// A Phase 2 variant this node has no path for, run on the node anyway.
///
/// m1 of the review of this slice: until now the four blocked variants were named in
/// prose and by nothing executable, so `cargo test --list` and the nightly's shard
/// table carried none of the debt. Each of these runs its variant over the share and
/// asserts two things: the correct-system checks still pass (the variant injects
/// nothing, because the path it breaks is not here), and the path itself is still
/// unreached. Both are absences with a reason, and both have an upgrade trigger: the
/// day the wiring lands, either the variant starts being caught — and this test fails,
/// asking to be turned into the assertion — or the path counters move and it fails
/// saying so.
// PROPOSED(D-082): each blocked variant is named by a test, not by prose.
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
fn a_server_that_installs_without_current_last_is_not_re_asserted_on_the_node_yet() {
    // §12 translates this one to the node as the live install's manifest switch made
    // only with the range's repair durable or carried in it, which is
    // `NodeVariant::InstallWithoutRepair` (D-075) — and there is no install on this
    // node to make a switch at all.
    blocked_on_the_node(
        "SnapshotWithoutCurrentLast",
        Variant::SnapshotWithoutCurrentLast,
        "the node's snapshot wiring (PROPOSED D-082)",
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
fn a_leader_that_ignores_incarnations_is_not_re_asserted_on_the_node_yet() {
    // A core variant, so the node carries it — but an incarnation only ever changes
    // when a store is re-seeded, and nothing re-seeds here.
    blocked_on_the_node(
        "IgnoreIncarnation",
        Variant::IgnoreIncarnation,
        "PR #86's re-seed and the node's snapshot wiring",
    );
}

#[test]
fn a_leader_that_shares_one_snapshot_directory_is_not_re_asserted_on_the_node_yet() {
    blocked_on_the_node(
        "SharedSnapshotDir",
        Variant::SharedSnapshotDir,
        "the node's snapshot wiring (PROPOSED D-082)",
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

#[test]
fn the_pair_that_wedged_seed_680_is_not_re_asserted_on_the_node_yet() {
    // D-045's control for a wedge that needs both bugs. SHARD.md expects the node's
    // schedule move to retire its pin on seed 680; under this slice's shape nothing
    // moved it, so the pin stands where it is and the pair waits with the rest.
    blocked_on_the_node(
        "{IgnoreIncarnation, SharedSnapshotDir}",
        Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]),
        "PR #86's re-seed and the node's snapshot wiring",
    );
}
