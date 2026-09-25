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
//!
//! # The membership scenario on the node
//!
//! The second half of this binary is `sim/membership.rs` on the same node (PROPOSED
//! D-084, following D-082): five nodes, **four ranges on every one of them**, 3 → 5 → 3
//! on every range under the partitions the seed draws, each range placed as today's one
//! group is. It is here rather than in a binary of its own for the reason the one-group
//! membership tests sit in `sim/tests/raft.rs`: the scenario shares the sweep's client,
//! its addresses and its schedule's clocks, and a second binary would be a second build
//! of all of it.
//!
//! What it adds to the list above:
//!
//! - the other half of issue #46's extension, the half four ranges make possible and
//!   one group could not have: **a change of a range while another range on the same
//!   node is changing**, asserted on every seed and named on one;
//! - liveness and availability asked of **each range** and not of the cluster, on
//!   bounds measured on the correct node before they were written;
//! - `SingleMajorityInJointConsensus` re-asserted on the node to the standard its
//!   Phase 2 test asserts and no stronger, at the tier it uses today;
//! - the half of issue #46 the node **cannot** reach — a joining server fed by a
//!   snapshot in its learner phase — asserted absent on every seed with its reason and
//!   the slice that owns the wiring, and left asserted where the path is, on
//!   `Cluster::OneGroup`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ananke_env::TraceEvent;
use ananke_env::sim::TraceRecord;
use ananke_raft::core::{Variant, Variants};
use ananke_shard::variant::{NodeVariant, NodeVariants};
use ananke_sim::folds::{self, ApplyLagFold, CrossRangeHoldFold, NodeCoverage, NodeCoverageFold};
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
/// asserted **reached on every seed**, and then D-077's fan-out asserted of every
/// refusal the run reached.
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
/// The refusal is no longer an absence either. Q15's whole-node refusal and re-seed
/// are in the tree (D-077), and a crash can leave this node an engine its restart
/// cannot open even though its disk does not rot (`Cluster::bitrot`): #123's nightly
/// reached one on 1 of 10 000 seeds (run 35949696476, on 677cad3) and the absence
/// this asserted tripped. So a refusal is counted — its seed, server and reason are
/// printed by the coverage at every tier — and what is asserted of it is what D-077
/// promises: every replica the node held is refused with it, and none of them serves
/// again before its own re-seed is durable (`whole_node_reseed`).
// PROPOSED(D-086): the snapshot path is reached, and the absence becomes a reach.
// PROPOSED(D-086): a refusal is counted, and D-077's fan-out is asserted of it.
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
    whole_node_reseed(report)
}

/// D-077's promise, asked of every refusal a run reached: the node is refused whole —
/// one `RaftReplicaRefused` for each of the ranges it holds, and no other — and no
/// replica of it serves again before its own `RaftReseeded`.
///
/// The second half is the one a crash at the run's end cannot fake: a re-seed the run
/// ended inside leaves a replica that never served again, which passes, where a
/// replica that answered anything on a store its node had lost fails naming the
/// range. The violations say `refused its store`, so `mechanism` files them under
/// `a store refused`.
// PROPOSED(D-086): a refusal is counted, and D-077's fan-out is asserted of it.
fn whole_node_reseed(report: &raft::Report) -> Result<(), String> {
    let seed = report.seed;
    let ranges: BTreeSet<u64> = ranges().into_iter().collect();
    let refusals: Vec<(usize, u64, String)> = report
        .records
        .iter()
        .enumerate()
        .filter_map(|(at, record)| match &record.event {
            TraceEvent::RaftRefused { server, reason } => Some((at, *server, reason.clone())),
            _ => None,
        })
        .collect();
    for (at, server, reason) in refusals {
        let after = &report.records[at + 1..];
        let refused: BTreeSet<u64> = after
            .iter()
            .filter_map(|record| match &record.event {
                TraceEvent::RaftReplicaRefused { server: s, range } if *s == server => Some(*range),
                _ => None,
            })
            .collect();
        if refused != ranges {
            return Err(format!(
                "seed {seed}: server {server} refused its store ({reason}) and the whole-node \
                 re-seed refused its replicas of ranges {refused:?}, not of every range the \
                 node holds, {ranges:?}: D-077 refuses the node whole"
            ));
        }
        for range in &ranges {
            let reseeded = after.iter().position(|record| {
                matches!(
                    &record.event,
                    TraceEvent::RaftReseeded { server: s, range: r } if *s == server && r == range
                )
            });
            let served = after
                .iter()
                .position(|record| served_by(&record.event) == Some((server, *range)));
            if let Some(served) = served
                && reseeded.is_none_or(|reseeded| reseeded > served)
            {
                return Err(format!(
                    "seed {seed}: server {server} refused its store ({reason}) and its replica \
                     of range {range} served again before its re-seed was durable, where D-077 \
                     writes the refused mark before any replica of a re-seeded node serves: \
                     {:?}",
                    after[served].event
                ));
            }
        }
    }
    Ok(())
}

/// The replica an event says served, for [`whole_node_reseed`]: a restatement, a
/// term, an election, an append, a commit or an apply of (server, range).
fn served_by(event: &TraceEvent) -> Option<(u64, u64)> {
    match event {
        TraceEvent::RaftRecovered { server, range, .. }
        | TraceEvent::RaftTerm { server, range, .. }
        | TraceEvent::RaftLeader { server, range, .. }
        | TraceEvent::RaftAppend { server, range, .. }
        | TraceEvent::RaftCommit { server, range, .. }
        | TraceEvent::RaftApply { server, range, .. } => Some((*server, *range)),
        _ => None,
    }
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
    /// Store refusals over the tier, and each one's seed, server and reason. Counted
    /// and printed, not asserted absent: the whole-node re-seed is in the tree (D-077)
    /// and a crash reaches it here about once in ten thousand seeds; what D-077
    /// promises of each is asserted per seed by `whole_node_reseed` (PROPOSED D-086).
    refusals: usize,
    refused: Vec<(u64, u64, String)>,
    /// The most registered reads any one replica held at once over the tier, with its
    /// seed, server and range: the number `READS_OUTSTANDING` is held to, read from
    /// `RaftReadsOutstanding` so the tier has a figure and not a failure string
    /// (PROPOSED D-076, D-086).
    reads_outstanding_worst: Option<(u64, u64, u64, u64)>,
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
    /// The range each stream arm **aimed** at, counted per range, and how many install
    /// arms reached the final chunk there (PROPOSED D-089).
    ///
    /// The two stream arms draw their victim from one stream and their range from
    /// another, so until this slice an arm reached its situation only where the two
    /// coincided — on 0 of 100 seeds for the install crash. The aim is resolved
    /// against the trace now, at the moment the victim has been cut off and healed,
    /// and lands on a range that victim is behind the leader's compacted prefix of.
    /// These three say what the aim did: which ranges it chose, and how often the
    /// choice carried the arm to the moment it is about.
    install_aims_by_range: BTreeMap<u64, usize>,
    stream_aims_by_range: BTreeMap<u64, usize>,
    installs_fired: usize,
    /// Of all the stream arms' aims, how many kept the range the schedule drew — the
    /// aim does, wherever the victim is behind that range's compacted prefix — and how
    /// many there were (PROPOSED D-089).
    aims_kept_the_draw: usize,
    aims: usize,
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
            .field("refused", &self.refused)
            .field("reads_outstanding_worst", &self.reads_outstanding_worst)
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
            .field("install_aims_by_range", &self.install_aims_by_range)
            .field("stream_aims_by_range", &self.stream_aims_by_range)
            .field("installs_fired", &self.installs_fired)
            .field("aims_kept_the_draw", &self.aims_kept_the_draw)
            .field("aims", &self.aims)
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
        for (server, reason) in &report.refused {
            self.refused.push((report.seed, *server, reason.clone()));
        }
        if let Some((server, range, outstanding)) = report.reads_outstanding_worst()
            && self
                .reads_outstanding_worst
                .is_none_or(|(_, _, _, worst)| outstanding > worst)
        {
            self.reads_outstanding_worst = Some((report.seed, server, range, outstanding));
        }
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
        // PROPOSED(D-089): the stream arms aim their victim at a range it lags.
        for (drawn, aimed) in &report.install_aims {
            *self.install_aims_by_range.entry(*aimed).or_default() += 1;
            self.aims_kept_the_draw += usize::from(drawn == aimed);
            self.aims += 1;
        }
        for (drawn, aimed) in &report.stream_aims {
            *self.stream_aims_by_range.entry(*aimed).or_default() += 1;
            self.aims_kept_the_draw += usize::from(drawn == aimed);
            self.aims += 1;
        }
        self.installs_fired += report.aimed_installs;
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
    ///
    /// **`per_seed` clamps at one**, so a floor whose hundred-seed observation is in the
    /// single figures stops being a quarter of anything at the gate's twenty and becomes
    /// "at least one". The thirteen in the array are all far above that; the install
    /// arm's floor below is not, and is asserted from a hundred seeds for that reason.
    // PROPOSED(D-089): the stream arms aim their victim at a range it lags.
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
        // **The install arm reaching the moment it aims at**, which was not a floor
        // before PROPOSED D-089 because it was not a number: the arm drew its victim and
        // its range apart and reached the final chunk of the range it drew on **0 of
        // 100** seeds. Aimed at a range its victim is behind the compacted prefix of,
        // the correct node's arm reaches it on **9 of 100** and **131 of 1 000**. An aim
        // that stops working — a scan that reads the wrong range's prefix, a fallback
        // that swallows every candidate, a range resolved before the lag exists — shows
        // up here as an arm that stopped firing, which is the one failure a sweep cannot
        // otherwise report.
        //
        // **It is asserted from a hundred seeds and not at the gate's twenty**, which is
        // where the review of this slice found it. The floor above is a per-seed rate
        // clamped at one, so at twenty it reads "at least one" against an observation of
        // two — not the quarter of the observation the twelve above it sit at. Twenty
        // seeds draw about **8** install arms and each reaches its final chunk about a
        // **quarter** of the time (131 of 517 at a thousand), so all eight missing is
        // 0.75^8, about **one run in ten**: a worse bound than the one run in twenty
        // this file refuses for `aimed_installs > 0`, and a bound the correct tree trips
        // is one to fix and never one to widen (D-030, D-039). At a hundred seeds the
        // observation is 9 against a floor of 2 and the same arithmetic gives about one
        // run in a hundred thousand. The count is printed at every tier, and both
        // mutations this floor catches fail it at a hundred as well as at twenty.
        // PROPOSED(D-089): the stream arms aim their victim at a range it lags.
        if self.seeds >= 100 {
            let floor = per_seed(2.0);
            assert!(
                self.installs_fired >= floor,
                "the sweep saw {} install arms that reached their range's final chunk over {} \
                 seeds, under the floor of {floor}: an arm that stops firing is a sweep that \
                 passes because it injected nothing",
                self.installs_fired,
                self.seeds
            );
        }
        // **The aim is a per-range one and not a constant** (PROPOSED D-089). The two
        // stream arms resolve their range against the trace now, and an aim that
        // answered one range every time would fire as often as this one, pass every
        // check above, and leave three of the node's four ranges' installs never
        // crashed at and never re-taken under — a fault model narrower than it reads.
        // **A single-range world cannot be wrong about this**: with one range every
        // aim is that range and this assertion is about nothing, which is why it is
        // here and not in `sim/tests/raft.rs`. Measured on the correct node: the
        // install arm's aims land on all four ranges at the gate's twenty
        // (`{2: 3, 3: 2, 4: 1, 5: 2}`) and at a hundred
        // (`{2: 11, 3: 15, 4: 11, 5: 12}`), the re-take arm's on three at twenty and
        // four at a hundred. The sweep runs seeds `0..tier`, so a larger tier holds
        // the gate's seeds and these only grow: the assertion is two, not four.
        for (what, aims) in [
            ("install", &self.install_aims_by_range),
            ("re-take", &self.stream_aims_by_range),
        ] {
            assert!(
                aims.len() > 1,
                "the {what} arm aimed at {} range(s) over {} seeds ({aims:?}): an aim that \
                 answers one range leaves the node's others' streams unreached, which every \
                 check of a single-range world would pass",
                aims.len(),
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
        // The refusal is counted, not asserted absent (PROPOSED D-086, on the merge
        // with `main`). Q15's whole-node refusal and re-seed are in the tree (D-077),
        // and a crash can leave this node an engine its restart cannot open although
        // its disk does not rot: #123's nightly reached one on 1 of 10 000 seeds (run
        // 35949696476, on 677cad3), where this line asserted none. What D-077 promises
        // of each is asserted per seed in `whole_node_reseed`; here every one is
        // printed with its seed, server and reason at every tier, so a rate that moves
        // is seen. It is not asserted above zero: at about one seed in ten thousand
        // no tier supports a floor (D-061).
        println!(
            "node: {} store refusals over {} seeds, each re-seeded whole (D-077): {:?}",
            self.refusals, self.seeds, self.refused
        );
        // The bound's own figure at this tier (D-076's `READS_OUTSTANDING`), printed
        // and not asserted here: the node fails itself the moment a replica holds more,
        // and `checked` reports that as the node's failure.
        println!(
            "node: the most registered reads one replica held at once over {} seeds was \
             {:?} as (seed, server, range, reads)",
            self.seeds, self.reads_outstanding_worst
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
    // The spread is the user ranges' (PROPOSED D-096): ranges 0 and 1 apply their
    // terms' no-ops and nothing else, since no client writes a system range, and a
    // floor that counted them would read the bootstrap as a key map answering one
    // range. They are printed with the rest of `applies_by_range`.
    let user_applies: Vec<usize> = coverage
        .applies_by_range
        .iter()
        .filter(|(range, _)| ranges().contains(range))
        .map(|(_, applies)| *applies)
        .collect();
    let least = user_applies.iter().copied().min().unwrap_or(0);
    let busiest = user_applies.iter().copied().max().unwrap_or(0);
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

// --- The node's folds under the equivalence test (SHARD.md §11, raft 12; §12) ---

/// The node variants the folds' comparison runs, one per seed in turn: the correct
/// node, the variant the lag's verdict is written for, and two that move
/// the coverage's counters — chunks charged to the inbox, which moves its drops, and
/// persists paid one at a time, which moves every timing — so the comparison sees
/// verdicts of both kinds and counters that differ from the correct node's, and not
/// only `Ok` and one shape of trace.
// PROPOSED(D-095): the node's folds under the equivalence test.
const FOLDS_COMPARED: [Option<NodeVariant>; 4] = [
    None,
    Some(NodeVariant::ApplyWaitsForEveryRange),
    Some(NodeVariant::ChunksToTheInbox),
    Some(NodeVariant::PersistsOneAtATime),
];

/// How many prefixes of a run's trace the folds are compared over, and how many
/// records are pushed at a time: the raft test's eight and its prime, so no prefix
/// is a chunk boundary (`sim/tests/raft.rs`).
const FOLD_PREFIXES: usize = 8;
const FOLD_CHUNK: usize = 37;

/// The node variants `variant` names, over the correct node.
fn node_with(variant: Option<NodeVariant>) -> NodeVariants {
    variant.map_or_else(NodeVariants::correct, |v| NodeVariants::correct().with(v))
}

/// The reference verdict, from the whole-prefix reading and the same rule the fold
/// states: each range's median lag at most the threshold.
fn lag_verdict_of(lags: &BTreeMap<u64, Vec<Duration>>, threshold: Duration) -> Result<(), String> {
    let (_, per_range) = folds::medians_of(lags);
    match per_range.iter().find(|(_, median)| **median > threshold) {
        Some((range, median)) => Err(format!(
            "apply lag: range {range}'s median apply lag is {median:?}, past the threshold of \
             {threshold:?}"
        )),
        None => Ok(()),
    }
}

/// What one run's comparison found beside its verdict: whether the lag's verdict was
/// in violation at some prefix, and whether it was over the whole trace.
#[derive(Clone, Copy, Debug, Default)]
struct Compared {
    lag_at_some_prefix: bool,
    lag_over_the_whole: bool,
}

/// One run's comparison: at every prefix, each fold fed the records in chunks
/// against its whole-prefix reading from the first record — the lag's samples,
/// medians and verdict, the hold's holds and dropped count, and the coverage's
/// counters value for value.
fn compare_folds(
    seed: u64,
    variant: Option<NodeVariant>,
    records: &[TraceRecord],
) -> Result<Compared, String> {
    let mut lag = ApplyLagFold::default();
    let mut hold = CrossRangeHoldFold::default();
    let mut coverage = NodeCoverageFold::default();
    let mut fed = 0;
    let mut found = Compared::default();
    let differ = |what: &str, stop: usize| {
        format!(
            "seed {seed}: under {variant:?}, over the first {stop} of {} records, the {what} fold \
             fed in chunks differs from its reading over the whole prefix",
            records.len()
        )
    };
    for step in 1..=FOLD_PREFIXES {
        let stop = records.len() * step / FOLD_PREFIXES;
        while fed < stop {
            let next = (fed + FOLD_CHUNK).min(stop);
            lag.extend(&records[fed..next]);
            hold.extend(&records[fed..next]);
            coverage.extend(&records[fed..next]);
            fed = next;
        }
        let prefix = &records[..stop];

        let read = raft::apply_lags_of(prefix);
        if lag.lags() != &read || lag.medians() != folds::medians_of(&read) {
            return Err(differ("apply-lag", stop));
        }
        let whole = lag_verdict_of(&read, HEARTBEAT);
        if lag.verdict(HEARTBEAT) != whole {
            return Err(format!(
                "{}: the fold said {:?} and the reading {whole:?}",
                differ("apply-lag", stop),
                lag.verdict(HEARTBEAT)
            ));
        }
        found.lag_at_some_prefix |= whole.is_err();
        found.lag_over_the_whole = whole.is_err();

        let (holds, dropped) = raft::cross_range_apply_holds_of(prefix);
        if hold.holds() != (holds.as_slice(), dropped) {
            return Err(differ("cross-range hold", stop));
        }

        let read = NodeCoverage::read(prefix);
        if coverage.coverage() != &read {
            return Err(format!(
                "{}: the fold read {:?} and the reading {read:?}",
                differ("coverage", stop),
                coverage.coverage()
            ));
        }
    }
    Ok(found)
}

/// The node's three measurement folds under the equivalence test the checker's
/// checks run under (D-046; SHARD.md §11, raft 12): the apply lag per range, the
/// cross-range hold and the coverage's counters, each fed a run's records in chunks
/// and read at every prefix, say exactly what their whole-prefix readings say — the
/// same samples, the same holds, the same counters, and for the lag, the one with a
/// verdict, the same `Ok` or `Err` with the same words. Stage B's tag names these
/// three as measurements that had never run under the test. A quarter of the compared
/// seeds run the variant that trips the lag's verdict over the whole run, and the
/// count of seeds found in violation is printed per variant, at some prefix and over
/// the whole trace, so a comparison that saw only `Ok` is visible — and so is what
/// the per-run verdict says of the correct node, which is why the sweep asserts the
/// pooled median and not this one.
// PROPOSED(D-095): the node's folds under the equivalence test.
#[test]
fn the_node_folds_agree_with_their_whole_trace_readings() {
    let compared = seeds().min(100);
    let outcomes: Vec<(Option<NodeVariant>, Result<Compared, String>)> = sweep(compared, |seed| {
        let variant = FOLDS_COMPARED[seed as usize % FOLDS_COMPARED.len()];
        let report = raft::run_on_the_node(seed, Variants::default(), node_with(variant));
        (variant, compare_folds(seed, variant, &report.records))
    });
    let mut by_variant: BTreeMap<String, (usize, usize, usize)> = BTreeMap::new();
    for (variant, outcome) in &outcomes {
        let name = variant.map_or_else(|| "Correct".to_owned(), |v| v.to_string());
        let entry = by_variant.entry(name).or_default();
        entry.0 += 1;
        if let Ok(found) = outcome {
            entry.1 += usize::from(found.lag_at_some_prefix);
            entry.2 += usize::from(found.lag_over_the_whole);
        }
    }
    println!(
        "node folds: {compared} seeds compared at {FOLD_PREFIXES} prefixes each; the apply-lag \
         verdict in violation, per variant as (seeds, at some prefix, over the whole run): \
         {by_variant:?}"
    );
    let tripped = outcomes
        .iter()
        .filter(|(variant, outcome)| {
            *variant == Some(NodeVariant::ApplyWaitsForEveryRange)
                && matches!(outcome, Ok(found) if found.lag_over_the_whole)
        })
        .count();
    let verdicts: Vec<Result<(), String>> =
        outcomes.into_iter().map(|(_, o)| o.map(|_| ())).collect();
    if let Err(mismatch) = verdict(&verdicts) {
        panic!("{mismatch}");
    }
    assert!(
        tripped > 0,
        "no compared seed under ApplyWaitsForEveryRange reached a violation of the lag verdict \
         over the whole run: the comparison saw only Ok of the variant it runs for"
    );
}

/// The variant the lag fold's verdict is written for, caught by it: an `apply` task
/// that waits for a job of every range before it runs any stalls the node's applies
/// behind its quietest range, and every range's median lag goes past SHARD.md §4's
/// heartbeat. What else catches it — the liveness bound on uniform seeds, where a
/// range with no leader holds every other — is counted and printed beside it, since a
/// variant caught only by something it does not break would be no evidence for the
/// fold (D-091's attribution). The hold fold's figures under it are printed too: the
/// wait a stalled apply spends is the lag's to see, not the hold's, which measures
/// one job's duration (D-082), and the figure says so.
// PROPOSED(D-095): the variant the apply-lag fold trips on.
#[test]
fn an_apply_task_that_waits_for_every_range_is_caught_by_the_lag_fold_on_the_node() {
    /// What one run under the variant showed: the lag verdict's violation, the run's
    /// other checks' failure, the worst range median, and the hold fold's median and
    /// longest.
    struct Caught {
        by_lag: Option<String>,
        by_other: Option<String>,
        worst: Option<Duration>,
        hold_median: Option<Duration>,
        hold_longest: Option<Duration>,
    }
    let seeds = seeds();
    let outcomes: Vec<Caught> = sweep(seeds, |seed| {
        let report = raft::run_on_the_node(
            seed,
            Variants::default(),
            NodeVariants::correct().with(NodeVariant::ApplyWaitsForEveryRange),
        );
        let mut lag = ApplyLagFold::default();
        lag.extend(&report.records);
        let mut hold = CrossRangeHoldFold::default();
        hold.extend(&report.records);
        let (hold_median, hold_longest) = hold.median_and_longest();
        Caught {
            by_lag: lag.verdict(HEARTBEAT).err(),
            by_other: checked(&report).err(),
            worst: lag.medians().1.into_values().max(),
            hold_median,
            hold_longest,
        }
    });
    let by_lag = outcomes.iter().filter(|o| o.by_lag.is_some()).count();
    let by_other = outcomes.iter().filter(|o| o.by_other.is_some()).count();
    let worst = outcomes.iter().filter_map(|o| o.worst).max();
    let mut hold_medians: Vec<Duration> = outcomes.iter().filter_map(|o| o.hold_median).collect();
    hold_medians.sort_unstable();
    let longest_hold = outcomes.iter().filter_map(|o| o.hold_longest).max();
    println!(
        "ApplyWaitsForEveryRange: the apply-lag verdict caught it on {by_lag} of {seeds} seeds; \
         the run's other checks failed it on {by_other}; the worst range median lag was \
         {worst:?} against {HEARTBEAT:?}; the hold fold read a median hold of {:?} over the \
         seeds' medians and a longest of {longest_hold:?}; first by lag: {}",
        median_of(&hold_medians),
        outcomes
            .iter()
            .find_map(|o| o.by_lag.as_deref())
            .unwrap_or("none")
    );
    assert!(
        by_lag > 0,
        "ApplyWaitsForEveryRange was never caught by the lag fold over {seeds} seeds"
    );
}

// --- The bootstrap (SHARD.md §2; PROPOSED D-096) ---

/// A node that takes a fresh store for a bootstrap (`AnyFreshNodeBootstraps`), caught
/// by check 7 on the membership scenario's five nodes, whose nodes 4 and 5 are not
/// bootstrap nodes: each writes the initial state with the address book as every
/// range's voters, so its creations disagree with the three bootstrap nodes' on every
/// range, and `creations_agree_of` — check 7's first step — says so. The run's own
/// checks are counted beside it, since what they make of two clusters bootstrapping
/// where configuration named one is not this test's claim. Measured before asserted
/// (D-061): the variant writes at every start, so the catch is every seed — 1 000 of
/// 1 000 over a whole thousand in release — and the test runs [`high_rate_share`], a
/// tenth of the tier, as every variant caught at a high rate on the node does
/// (D-082): over the whole tier it weighed 404 cpu s, two clusters' traffic on every
/// seed, and a catch on every seed needs no more of a tier than the share.
///
/// On a cluster of three, every node is a bootstrap node and the variant has nothing
/// to do: its run is byte-identical to the correct node's, which is the absence
/// asserted with its reason (CLAUDE.md), rather than a sweep passed by injecting
/// nothing.
// PROPOSED(D-096): the bootstrap nodes named in configuration.
#[test]
fn a_fresh_node_that_bootstraps_itself_is_caught_by_check_7_where_it_is_not_a_bootstrap_node() {
    let seeds = high_rate_share();
    let taken = NodeVariants::of(&[NodeVariant::AnyFreshNodeBootstraps]);
    let outcomes: Vec<(Option<String>, Option<String>)> = sweep(seeds, |seed| {
        let report = membership::run_on_node(Cluster::Node, seed, Variants::default(), taken);
        (
            ananke_sim::ranges::creations_agree_of(&report.records).err(),
            report.check().err(),
        )
    });
    let by_check_7 = outcomes.iter().filter(|o| o.0.is_some()).count();
    let by_others = outcomes.iter().filter(|o| o.1.is_some()).count();
    println!(
        "AnyFreshNodeBootstraps: check 7 caught it on {by_check_7} of {seeds} membership seeds, \
         the run's other checks on {by_others}; first: {}",
        outcomes
            .iter()
            .find_map(|o| o.0.as_deref())
            .unwrap_or("none")
    );
    assert_eq!(
        by_check_7, seeds as usize,
        "AnyFreshNodeBootstraps was not caught by check 7 on every membership seed"
    );

    let correct = raft::run_on_the_node(1, Variants::default(), NodeVariants::correct());
    let pretending = raft::run_on_the_node(1, Variants::default(), taken);
    assert_eq!(
        correct.jsonl(),
        pretending.jsonl(),
        "on three nodes every node is a bootstrap node, so the variant should have had \
         nothing to do and the trace should not have moved"
    );
}

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

/// The violations a sweep found, split by the check that reported each: the ones
/// the variant is **about**, and the ones some other check made.
///
/// **A catch is the variant's only when the check that reported it is a check the
/// variant breaks** (RAFT.md §5's table, the `What catches it` column). Until
/// PROPOSED D-091 every test here read `checked(...).err()` and counted whatever came
/// back, so a violation by an unrelated check propped up a positive assertion —
/// "caught" — and, in the absence tests, was reported as the variant being caught at
/// all. One gap in the **correct** node's own run did exactly that: the timer bound
/// PROPOSED D-089's per-range aim reached was reported on three different variants,
/// on seeds where each of the three injects nothing, and the quoted violations were
/// the timer strings word for word. Attribution is what tells those apart, and it is
/// the shape `sim/tests/raft.rs` already uses for `NoPreVote`, whose catch is counted
/// by the pre-vote check and not by whichever check a run failed first.
// PROPOSED(D-082): a catch on the node is attributed, not counted.
// PROPOSED(D-091): a catch is attributed to the variant's own violation, and a catch
// by an unrelated violation fails rather than passes.
#[derive(Debug, Default)]
struct Caught {
    /// Violations by one of the checks named for this variant: its catch.
    own: Vec<String>,
    /// Violations by any other check. These are real failures and they are reported
    /// as themselves — never as this variant's catch.
    other: Vec<String>,
}

impl Caught {
    /// Splits `violations` by whether the check that made each is one of `checks`,
    /// named as [`mechanism`] names them.
    // PROPOSED(D-091): a catch is attributed to the variant's own violation.
    fn split(violations: Vec<String>, checks: &[&str]) -> Self {
        let mut caught = Self::default();
        for violation in violations {
            if checks.contains(&mechanism(&violation)) {
                caught.own.push(violation);
            } else {
                caught.other.push(violation);
            }
        }
        caught
    }
}

/// The violations `variants` was caught by over the share, split by [`Caught`]
/// against the `checks` this variant's catch is attributed to, with the rate and the
/// mechanisms printed.
///
/// §12 asks each variant to be re-asserted "to the standard its Phase 2 test asserts
/// and no stronger", and `checks` is that standard: RAFT.md §5's `What catches it`
/// for the variant, which is what its Phase 2 test asserts against.
// PROPOSED(D-082): a catch on the node is attributed, not counted.
// PROPOSED(D-091): a catch is attributed to the variant's own violation.
fn caught_on(
    name: &str,
    variants: impl Into<Variants> + Copy + Send + Sync,
    checks: &[&str],
) -> Caught {
    let seeds = high_rate_share();
    let violations: Vec<String> = sweep(seeds, |seed| checked(&buggy(seed, variants)).err())
        .into_iter()
        .flatten()
        .collect();
    let mut by_check: BTreeMap<&str, usize> = BTreeMap::new();
    for violation in &violations {
        *by_check.entry(mechanism(violation)).or_default() += 1;
    }
    let caught = Caught::split(violations, checks);
    let rate = caught.own.len() as f64 * 100.0 / seeds as f64;
    println!(
        "node: {name} caught on {}/{seeds} seeds ({rate:.1}%) by {checks:?}, its own checks; \
         {} more violations came from other checks and are not its catch; every violation by \
         {by_check:?}, first of its own: {}",
        caught.own.len(),
        caught.other.len(),
        caught.own.first().map_or("", String::as_str)
    );
    caught
}

/// Which check a violation came from, for the attribution [`Caught`] rests on.
///
/// The names are the checks' own words. `the node failed` is not a check at all: it
/// is the node telling the scenario its own apply stream had a hole, which is a real
/// catch of a real bug and a different statement from a safety fold's. The last two
/// are `checked`'s own, not `Report::check`'s: what a run must reach for the stream
/// variants to assert anything, and the one path this node has not got.
// PROPOSED(D-082): a catch on the node is attributed, not counted.
fn mechanism(violation: &str) -> &'static str {
    for (needle, name) in [
        ("pre-vote:", "pre-vote"),
        ("timers:", "timers"),
        ("state machine safety", "state machine safety"),
        ("committed entries", "committed entries stay"),
        ("commit majority:", "commit by majority"),
        ("commit by current term", "commit by current term"),
        ("match starts:", "match starts"),
        ("log matching", "log matching"),
        ("election safety", "election safety"),
        ("leader completeness", "leader completeness"),
        ("linearizability", "linearizability"),
        ("liveness", "liveness"),
        ("follower log:", "the follower-log bound"),
        ("failed:", "the node failed"),
        // PROPOSED(D-091): `checked`'s own, which were "something else" before and
        // are named now that a name decides where a violation is counted. Since the
        // merge with `main` the first is `whole_node_reseed`'s: D-077's fan-out
        // broken, which is a refusal not made durable after the next crash.
        ("refused its store", "a store refused"),
        ("this seed's evidence is vacuous", "no snapshot action"),
        ("nothing was checkpointed for a stream", "no take completed"),
    ] {
        if violation.contains(needle) {
            return name;
        }
    }
    "something else"
}

/// Whether the run's ranges wedged by the liveness check's own reading: the fold,
/// asked as `check()` asks it, of uniform schedules only (D-016).
fn wedged(report: &raft::Report) -> bool {
    report.uniform() && report.liveness().is_err()
}

/// Whether a violation is the read bound's (`READS_OUTSTANDING`, D-076 point 12).
fn by_the_read_bound(violation: &str) -> bool {
    mechanism(violation) == "the node failed" && violation.contains("registered reads")
}

/// Whether a read-bound trip on this run is the **wedge's second reporter**: the
/// liveness fold, asked of the run regardless of its schedule, reports that a range's
/// clients stopped completing writes after the heal.
///
/// The difference matters since the merge with `main`. A wedged range's clients retry
/// their reads, each retry is registered again (issue #125), and `READS_OUTSTANDING` —
/// sized on the correct node, which never wedges — fails the node before `check()`
/// reaches its liveness clause, or on a schedule `check()` never asks liveness of
/// (D-016 asks it of uniform schedules only). Either way a run the wedge caught would
/// read as the node's own failure, which is how `main`'s nightly on c178682 read one
/// read-bound trip as five catches. The fold's reading is used here only to attribute
/// a trip the bound has already reported; it asserts nothing on its own, so D-016's
/// rule that liveness is a claim about uniform schedules is untouched.
// PROPOSED(D-086): a read-bound trip downstream of a wedge is attributed to the wedge.
fn read_bound_reports_a_wedge(report: &raft::Report, violation: &str) -> bool {
    by_the_read_bound(violation) && report.liveness().is_err()
}

/// Whether a read-bound trip with **no** wedge the fold reports is the **variant's
/// load** and not the node's failure: the correct node, run on the same seed, passes.
///
/// `SharedSnapshotDir`'s stream never completes, and on a run whose writes still
/// complete within the liveness bound its leader's reads can still wait past the
/// clients' retries, each retry registered again (issue #125), until the replica
/// holds more than `READS_OUTSTANDING` — a bound sized on the correct node, which
/// holds at most 16 over a thousand seeds of these arms. That is the variant's doing
/// through a check RAFT.md §5 does not name for it, so it is neither its catch nor
/// the node's failure; it is counted and printed as what it is. A seed the correct
/// node trips the bound on too is the node's, and stays a failure.
// PROPOSED(D-086): a read-bound trip with no wedge is the variant's load only where the
// correct node passes the seed.
fn read_bound_is_the_variants_load(report: &raft::Report, violation: &str) -> bool {
    by_the_read_bound(violation)
        && report.liveness().is_ok()
        && correct(report.seed).check().is_ok()
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
    // Attributed to the checks RAFT.md §5 names for it: commit by majority, and
    // leader completeness where a crash falls between the send and the persist.
    // PROPOSED(D-091): a catch is attributed to the variant's own violation.
    let caught = caught_on(
        "SendBeforePersist",
        Variant::SendBeforePersist,
        &["commit by majority", "leader completeness"],
    );
    assert!(
        !caught.own.is_empty(),
        "SendBeforePersist was never caught on the node by a check it breaks, which is what \
         RAFT.md §5 names for it: {caught:?}"
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
    // PROPOSED(D-091): a catch is attributed to the variant's own violation. The
    // node's own oracle is named beside RAFT.md §5's two, for the reason above: a
    // hole in the applied stream is this bug and nothing else's.
    let caught = caught_on(
        "ApplyBeforeCommit",
        Variant::ApplyBeforeCommit,
        &["state machine safety", "linearizability", "the node failed"],
    );
    assert!(
        !caught.own.is_empty(),
        "ApplyBeforeCommit was never caught on the node by a check it breaks: {caught:?}"
    );
}

#[test]
fn a_server_without_pre_vote_is_caught_on_the_node() {
    // Its Phase 2 test asserts the **mechanism**, not a bare catch: `by_pre_vote > 0`
    // (`sim/tests/raft.rs`). §12 asks for that standard and no stronger, so a bare
    // catch here would be weaker than Phase 2's and this asserts the same thing — the
    // catch is pre-vote's own property, a server raising its term while cut off.
    // PROPOSED(D-082): a catch on the node is attributed, not counted.
    let caught = caught_on("NoPreVote", Variant::NoPreVote, &["pre-vote"]);
    assert!(
        !caught.own.is_empty(),
        "NoPreVote was never caught on the node by pre-vote's own property, which is what \
         its Phase 2 test asserts: {caught:?}"
    );
}

#[test]
fn a_leader_that_commits_an_older_terms_entry_by_count_is_caught_on_the_node() {
    // The Figure 8 driver's window (D-031), on the node: the burst writes a key of
    // the range the arm drew, so the backlog the restarted leader re-sends is that
    // range's.
    // PROPOSED(D-091): a catch is attributed to the variant's own violation.
    let caught = caught_on(
        "CountOlderTermForCommit",
        Variant::CountOlderTermForCommit,
        &["commit by current term", "leader completeness"],
    );
    assert!(
        !caught.own.is_empty(),
        "CountOlderTermForCommit was never caught on the node by a check it breaks: {caught:?}"
    );
}

#[test]
fn a_follower_that_truncates_on_every_append_is_caught_on_the_node() {
    // PROPOSED(D-091): a catch is attributed to the variant's own violation.
    let caught = caught_on(
        "TruncateOnEveryAppend",
        Variant::TruncateOnEveryAppend,
        &["committed entries stay"],
    );
    assert!(
        !caught.own.is_empty(),
        "TruncateOnEveryAppend was never caught on the node by committed entries staying, \
         which is what RAFT.md §5 names for it: {caught:?}"
    );
}

#[test]
fn a_server_that_resets_its_timer_on_any_message_is_caught_on_the_node() {
    // PROPOSED(D-091): a catch is attributed to the variant's own violation — the
    // timer check, which is the one this variant is written for. That matters more
    // here than anywhere else on this binary: PROPOSED D-091's fourth arm narrows
    // the timer check, and a variant whose catch *is* the timer check is where a
    // narrowing would show up as a loss. It does not: the rate is in the entry,
    // measured before and after.
    let caught = caught_on(
        "ResetTimerOnAnyRpc",
        Variant::ResetTimerOnAnyRpc,
        &["timers"],
    );
    assert!(
        !caught.own.is_empty(),
        "ResetTimerOnAnyRpc was never caught on the node by the timer check, which is the \
         check it is written for: {caught:?}"
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
    // The catch is a stale read, found by linearizability. On the node it is asserted
    // from the **nightly's ten thousand**, not from the thousand-seed tier the
    // one-group test asserts it at, and the reason is the node's own rate: the stale
    // read is caught on **78 of 10 000 seeds, 0.78 %** here, and 6 of the first 1 000,
    // 0.6 %, against one group's 4.0 % on the same arms.
    //
    // D-061 reasons with P(none) = (1 − p)^n, n the seeds the assertion sees at the
    // lowest tier it is asserted at. At 0.78 % a **thousand** seeds catch none with
    // probability 0.9922^1000 = 4.0e-4, about one run in 2 500, and at the 0.6 % the
    // thousand itself measured, 0.994^1000 = 2.4e-3, one run in 410 — either way a
    // tier that reddens a tree with nothing wrong, against the 0.96^1000 = 1.9e-18 of
    // the one-group assertion this was copied from. At the **nightly's ten thousand**
    // the same arithmetic gives 0.9922^10000 = 9.8e-35, and 7.3e-27 at the thinner
    // rate. So the tier moves to where the statistics are, as `SharedSnapshotDir`'s
    // liveness catch already is (D-061; `sim/tests/raft.rs`). The owner ruled it on
    // 2026-09-23; PROPOSED(D-088) has the measurement and its machine.
    //
    // It is **not** a narrower window: the node's lease trial hands over *every* range
    // the node holds, so the window is wider than the one-group trial's, not
    // narrower. The rate keeps printing at every tier, which is D-061's other half —
    // a tier that asserts nothing still has to say what it saw, or the day the rate
    // collapses nobody learns of it until the nightly. The one-group assertion is
    // untouched at its own tier, where 4.0 % belongs.
    // PROPOSED(D-088): the node's stale read asserts at the nightly's ten thousand.
    //
    // The catch is counted by the check that makes it — the linearizability search,
    // which is where a stale lease read is reported — and not by whichever check a
    // run failed first (PROPOSED D-091).
    let seeds = seeds();
    let violations: Vec<String> = sweep(seeds, |seed| {
        checked(&buggy(seed, Variant::LeaseTrustsTheClock)).err()
    })
    .into_iter()
    .flatten()
    .collect();
    let caught = Caught::split(violations, &["linearizability"]);
    let rate = caught.own.len() as f64 * 100.0 / seeds as f64;
    println!(
        "node: LeaseTrustsTheClock caught on {}/{seeds} seeds ({rate:.1}%) by the \
         linearizability search, its own check; {} more violations came from other checks \
         and are not its catch, first of its own: {}",
        caught.own.len(),
        caught.other.len(),
        caught.own.first().map_or("", String::as_str)
    );
    // The tier, not a share of it: this test sweeps `seeds()` itself, so the gate is
    // reached at exactly the seed count the nightly sets and not a tenth of it.
    if seeds >= 10_000 {
        assert!(
            !caught.own.is_empty(),
            "LeaseTrustsTheClock was never caught on the node over {seeds} seeds by the \
             linearizability search, which is the check RAFT.md §5 names for it; at the \
             0.78 % PROPOSED(D-088) measured over ten thousand, ten thousand seeds catch \
             none with probability 9.8e-35, so this is the node having changed and not a \
             draw: {caught:?}"
        );
    }
}

/// The pair rule for the registration a refused read leaves behind
/// (`NodeVariant::RefusedReadLeft`, D-076), **under the raft arms**: the node exactly
/// as it was before D-076, whose `reads` map grows by one on every read a replica
/// refuses and never shrinks.
///
/// D-076 caught it on `sim/ranges.rs` at a bound of 8, on 535 of 1 000 seeds. The
/// bound is 38 since PR #109's merge with `main` re-measured it by D-076's own rule
/// (point 12), and at 38 `sim/ranges.rs`'s runs are too short for the leak to reach
/// it — **0 of 1 000** there, in release — so the catch is asserted here, where a run
/// holds four ranges' leaders through crashes, isolations and streams for long enough
/// that the refused reads pile up past the bound; `sim/ranges.rs`'s pair keeps the
/// fault's firing and prints its rate. The bound is the only oracle for this leak
/// (D-076's review, R2), so the catch and the firing are one measurement here: the
/// most reads the leaking node held is printed beside the rate at every tier.
///
/// Measured before it was asserted (D-061), on the merged tree in release: caught on
/// **108 of 1 000 seeds (10.8 %)**, 8 of 100 and 2 of 20, every one by the bound, the
/// leaking replica holding up to **61** reads at once against the correct node's 16.
/// At 10.8 % the gate's twenty see none about one run in ten (0.892^20 = 0.10), which
/// is a flake, and a hundred see none about once in ninety thousand (1.1e-5), so the
/// catch is asserted **from the hundred-seed tier** and printed at every tier — the
/// tier D-089 put `SnapshotWithoutCurrentLast`'s injection at, for the same arithmetic.
// PROPOSED(D-086): the read leak's catch moves to the arms, where the bound of 38 sees it.
#[test]
fn a_node_that_leaves_a_refused_reads_registration_behind_is_caught_on_the_node() {
    /// One seed under the leaking node: whether it failed, whether the bound named
    /// it, and the most reads one replica held at once.
    struct Leak {
        failed: bool,
        named: bool,
        worst: Option<u64>,
    }
    let seeds = seeds();
    let outcomes: Vec<Leak> = sweep(seeds, |seed| {
        let report = raft::run_on_the_node(
            seed,
            Variants::default(),
            NodeVariants::of(&[NodeVariant::RefusedReadLeft]),
        );
        let verdict = report.check();
        let named = verdict
            .as_ref()
            .err()
            .is_some_and(|violation| violation.contains("registered reads"));
        Leak {
            failed: verdict.is_err(),
            named,
            worst: report
                .reads_outstanding_worst()
                .map(|(_, _, outstanding)| outstanding),
        }
    });
    let caught = outcomes.iter().filter(|leak| leak.failed).count();
    let named = outcomes.iter().filter(|leak| leak.named).count();
    let worst = outcomes.iter().filter_map(|leak| leak.worst).max();
    let rate = caught as f64 * 100.0 / seeds as f64;
    println!(
        "node: RefusedReadLeft caught on {caught}/{seeds} seeds ({rate:.1}%), {named} of them \
         by the outstanding-reads bound; the most reads a leaking replica held at once was \
         {worst:?}"
    );
    // The tier, not a share of it: this sweeps `seeds()` itself.
    if seeds >= 100 {
        assert!(
            caught > 0,
            "the node that keeps every read it refuses passed every one of {seeds} seeds under \
             the raft arms; at the 10.8 % measured over a thousand, a hundred seeds catch none \
             with probability 1.1e-5, so this is the node or the bound having changed and not \
             a draw"
        );
    }
    assert_eq!(
        named,
        caught,
        "{} of the {caught} seeds caught were caught by something other than the \
         outstanding-reads bound, which is not what this pair is for",
        caught - named
    );
}

#[test]
fn the_nodes_arms_aim_at_every_range_and_not_at_one() {
    // §11, env 8: a leader-relative arm on a node of many ranges chooses its range
    // from its own stream. What says the draw is spent is the set of ranges the
    // schedules of a run of seeds aim at — one range would mean three of the four
    // never saw a leader isolation, a leader crash or a Figure 8 burst at all.
    //
    // This reads `Schedule::range_of`, which is the range each fault **draws**, and for
    // every arm but two that is also the range it aims at. `Fault::CrashInstalling` and
    // `Fault::RetakeUnderStream` resolve theirs against the trace at the arm
    // (`lagging_range`), preferring this draw where it qualifies; what those two aimed
    // at is `Report::install_aims` and `Report::stream_aims`, asserted on the correct
    // node's sweep. So what this measures is the **draw**, which is still what §11 asks
    // about: a range the draw never reaches is one an arm aims at only by the accident
    // of no other range qualifying.
    // PROPOSED(D-089): the stream arms aim their victim at a range it lags.
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
    /// The answers D-049's rule is about: rejections **carrying the refused mark**
    /// (PROPOSED D-087). Under the key this figure was written for — a rejection
    /// stamped incarnation 0 — it was 0 on every seed at every tier, because the
    /// node has no store-less state. It is not 0 now.
    refused_rejections: usize,
    /// Answers from **no store at all**, stamped incarnation 0. D-077's node has
    /// none, which is the half of PROPOSED D-085's finding that D-087 leaves
    /// standing, and `NodeReport::check` fails the seed if it ever does.
    store_less_answers: usize,
    /// Marked rejections the victim **sent** after it had sent an answer of the same
    /// range that fitted (send order, since delays reorder deliveries): the figure a
    /// mark read per node rather than per replica moves when it is set too **widely**,
    /// and `NodeReport::check` fails the seed on it.
    marked_after_fitted: usize,
    /// Its dual: **unmarked** rejections the victim sent after that range's replica
    /// was re-seeded and before it had sent anything of that range that fitted — what
    /// the same per-node mark does when it is set too **narrowly**, which is what a
    /// mark read off one replica and stamped on the other three looks like.
    /// `refused_rejections` above is a sweep-wide total and sees only the mark's
    /// complete disappearance; this sees three quarters of it go. Also 0, and
    /// `NodeReport::check` fails the seed on it, naming the range.
    // m1 of the review of this slice.
    unmarked_before_fitted: usize,
    /// The node's other answers: rejections a caught-up re-seeded replica sends,
    /// carrying its store's own incarnation and no mark, and answers that fitted.
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
            self.store_less_answers += hold.store_less_answers;
            self.marked_after_fitted += hold.marked_after_fitted;
            self.unmarked_before_fitted += hold.unmarked_before_fitted;
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
/// The figures print what D-049's own halves need of the node;
/// `d_049s_rule_has_a_site_on_the_node_now_and_its_pair_waits_on_the_scenarios_own_threshold` is
/// where they are asserted.
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

/// D-049's rule **has a site on the node now**, and its pair is measured there:
/// what the mark buys, and the one thing the pair still waits for.
///
/// This replaces the absence PROPOSED D-085 asserted here. D-085 measured that
/// D-049's rule was keyed on *a rejection stamped incarnation 0* — a server with no
/// store — that the one-group server's re-seed loop is such a server and D-077's
/// node never is, and that `Progress::refused_answered` was therefore never set on
/// the node, on any seed, for any range. PROPOSED D-087 changed the key: the answer
/// carries a refused mark ([`ananke_raft::Raft::refused`]), which the node's
/// re-seeded replicas send on a store of their own, and the leader reads the mark
/// instead of inferring the absence of a store from a stamp. What this test asserts
/// now, per variant, over the tier's seeds:
///
/// 1. **The refusal landed** — four replicas a seed, as before, or nothing below is
///    evidence.
/// 2. **The node sends the mark**: `refused_rejections` is no longer 0. This is the
///    assertion that would fail if the key were reverted, if the mark stopped being
///    set where a re-seed builds the store, or if it stopped surviving the wire, and
///    it is the whole of what D-087 buys on this cluster. It is a **sweep-wide
///    total**, so on its own it sees the mark disappearing and nothing short of
///    that: a mark read once per node and stamped on its other three replicas only
///    takes it from 282 to 72 at a thousand seeds. What sees *that* is
///    `NodeReport::check`'s pair of per-range clauses — no marked rejection sent
///    after that range's replica answered something that fitted, and no **unmarked**
///    one sent after its re-seed and before it did — which fail the seed naming the
///    range, and which the figures `marked_after_fitted` and
///    `unmarked_before_fitted` print at every tier. Both are 0 here.
/// 3. **And the node still answers from no store nowhere**: `store_less_answers` is
///    0. That half of D-085's finding stands, and `NodeReport::check` fails the seed
///    that breaks it. The mark is a replacement for the stamp, not a re-creation of
///    the store-less state D-077 decided against — which is the owner's ruling, in
///    the one figure that can tell the two shapes apart.
/// 4. **The pair is still caught on 0 seeds, for a narrower reason than before, and
///    the rate is printed at every tier.** The rule has its site; what it does not
///    yet have is a *window the site decides*. Under this scenario's own threshold
///    nothing compacts — no core asks for a take — so a leader always feeds a
///    re-seeded replica from index 1, the replica answers a refused rejection and
///    then a success **inside the same check-quorum window**, and `active` settles
///    the window whichever way the variant counts the rejection. Measured, and the
///    figures say it: over a hundred seeds the node delivered 33 refused rejections
///    and 25 010 answers that fitted, and **0** step-downs left anyone `uncounted`.
///
/// **Nothing is lowered.** §10's standard for this pair — caught on every seed at
/// every tier — is asserted where the pair is caught, `sim/tests/raft.rs`'s four
/// D-049 tests on the one-group server, whose every figure D-087 left unmoved. What
/// this says is why it is not yet caught here, and the reason is now one thing and
/// not two: **a compaction past the re-seeded replica, which this scenario's own
/// `snapshot_threshold` keeps from happening** (`quorum::NODE_SNAPSHOT_THRESHOLD`,
/// `1 << 30`; the wiring itself is in the tree, PR #107, and the raft-arms sweep
/// runs its node at 12). A leader that can compact past a re-seeded replica
/// can no longer feed it from its log, its refused rejections become the only answer
/// in the window, and the rule decides. The bare core already does decide it —
/// `crates/ananke-raft/tests/paper.rs` steps a leader down naming a follower that
/// answers a refused rejection on a store of its own, which is the node's shape and
/// which under the old key kept its office. This test fails the day the threshold is
/// lowered and the node reaches it, and says so.
// PROPOSED(D-087): D-049's rule keyed on a refused mark the answer carries.
#[test]
fn d_049s_rule_has_a_site_on_the_node_now_and_its_pair_waits_on_the_scenarios_own_threshold() {
    let ranges = u64::try_from(ranges().len()).expect("small");
    for variant in [Variant::RefusedCountsForQuorum, Variant::RefusedNeverCounts] {
        let (caught, figures) = quorum_node_sweep(variant, NodeVariants::correct());
        eprintln!(
            "sharded quorum, {:?}: caught on {} of {} seeds, {figures:?}",
            Variants::from(variant),
            caught.len(),
            seeds()
        );
        assert_eq!(
            figures.ranges_reseeded as u64,
            figures.seeds * ranges,
            "the refusal did not land, so nothing below is evidence"
        );
        assert!(
            figures.store_rejections + figures.fitted > 0,
            "the refused node answered nothing at all, so nothing below is evidence"
        );
        // The site: the node's re-seeded replicas say they hold nothing of the log.
        assert!(
            figures.refused_rejections > 0,
            "no answer of the node carried the refused mark, so D-049's rule has no \
             site here after all: PROPOSED D-087's key is not reaching the leader, and \
             the entry's claim that it does is wrong"
        );
        // And they say it on a store of their own, which is D-077's shape unchanged.
        assert_eq!(
            figures.store_less_answers, 0,
            "the node answered from no store, where D-077 rebuilds every range's store \
             before any replica serves: the re-seed changed shape, which is the shape \
             the owner ruled against changing"
        );
        assert_eq!(
            caught.len(),
            0,
            "{:?} is caught on the node: the rule's site now decides a window here, so \
             assert the catch at §10's standard instead of this: {caught:?}",
            Variants::from(variant)
        );
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
/// node the measured **catch** rate still does not support it, and D-061's rule is that
/// the tier a Phase 2 variant keeps is the owner's, so the catch is **not** asserted
/// here and the numbers go to the owner instead of a quieter assertion. What *is*
/// asserted, and from the hundred-seed tier since PROPOSED D-089, is that the variant
/// was **injected at the moment it is about**:
///
/// - `Fault::CrashInstalling` reaches the final chunk of the range it aims at and
///   crashes its victim there on **14 of 100** seeds and **122 of 1 000**. Until
///   PROPOSED D-089 the arm drew its victim and its range from two streams and reached
///   that moment on **0 of 100** and 1 of 1 000: on a node the victim was drawn without
///   regard to which of its four ranges it was behind on, so the arm reached its
///   situation only where the two draws happened to coincide. The range is resolved
///   against the trace now, after the isolation and the heal, and lands on one the
///   victim is behind the leader's compacted prefix of — designated for it, and
///   streamed within `INSTALL_WAIT_BUDGET`;
/// - the catch is still **0 of 100 and 0 of 1 000**, now over 122 firings rather than
///   one. That is the finding the aim turns up and it goes to the owner: the arm is no
///   longer the reason. A variant injected at its own moment on a tenth of the seeds
///   and caught on none of a thousand is either a bug this node's checks cannot see or
///   one its install path repairs, and which of those it is belongs to the slice that
///   owns the path, not to this one (PROPOSED D-089).
///
/// So the catch is asserted nowhere and the **arm's firing is asserted from a hundred
/// seeds**, where at 14 % a sample of a hundred sees none about three times in ten
/// million. It is deliberately not asserted at the gate's twenty: 0.86^20 is about one
/// run in twenty, which is a flake, and a bound the correct system trips is one to fix
/// rather than to widen (D-030, D-039). The rate is printed at every tier, and the
/// variant keeps its Phase 2 assertion on `Cluster::OneGroup`, which this sweep leaves
/// running exactly as it is.
// PROPOSED(D-086): Phase 2's stream variants re-asserted on the node.
// PROPOSED(D-089): the stream arms aim their victim at a range it lags.
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
    // Attributed (PROPOSED D-091): a run the node failed on its own — the read bound, an
    // apply hole — is the node's failure and not this variant's catch, which is how
    // `main`'s nightly on c178682 came to report one read-bound trip as five catches.
    let mut by_check: BTreeMap<&str, usize> = BTreeMap::new();
    for violation in &caught {
        *by_check.entry(mechanism(violation.as_str())).or_default() += 1;
    }
    let by_the_node = by_check.get("the node failed").copied().unwrap_or(0);
    let rate = (caught.len() - by_the_node) as f64 * 100.0 / seeds as f64;
    println!(
        "node: SnapshotWithoutCurrentLast caught on {}/{seeds} seeds ({rate:.1}%) by a check \
         (every violation by {by_check:?}; {by_the_node} runs the node failed on its own, \
         which are not its catch), the install crash reached the final chunk of the range \
         it aimed at and crashed there on {fired}/{seeds} seeds, {actions} snapshot actions \
         asked for, first: {}",
        caught.len() - by_the_node,
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
    // `aimed_installs` is the discriminator, and it was **thin**: aimed at the range
    // the schedule drew beside the victim it reached that moment on 0 of 100 seeds and
    // 1 of 1 000, so this assertion sat at the nightly's ten thousand and below that
    // tier the test asserted nothing about the variant at all.
    //
    // Aimed at a range the victim actually lags it reaches it on **14 of 100** and
    // **122 of 1 000** (PROPOSED D-089), measured before this line was moved (Q39,
    // D-061). At 14 % a hundred seeds see none about three times in ten million, so
    // the assertion sits at the hundred-seed tier; the gate's twenty would see none
    // about one run in twenty, which is a bound the correct tree trips and D-030 and
    // D-039 forbid writing it there and widening it later.
    //
    // **The catch is still 0 of 1 000**, now over 122 firings rather than one, and it
    // is asserted nowhere. The arm is no longer the reason and that is the finding to
    // take to the owner, not to paper over with a quieter assertion.
    if seeds >= 100 {
        assert!(
            fired > 0,
            "`Fault::CrashInstalling` never crashed its victim at the final chunk of the \
             range it aimed at over {seeds} seeds, so `SnapshotWithoutCurrentLast` was not \
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
///   the **thousand-seed** tier, where Phase 2 has it from a hundred. **30 of 100**
///   (29 before PROPOSED D-089's aim, 17 of 100 and 172 of 1 000 before the merge with
///   `phase-3-stage-b-wiring`),
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
/// **PROPOSED D-089 aimed both stream arms at a range their victim lags, and three of
/// these four did not move.** The aim resolves the range after the isolation and the
/// heal and keeps the drawn range wherever the victim is behind *its* compacted prefix,
/// which for this arm the filling puts and the isolation together already make true:
/// the fault stays **100 of 100**, the arm **1 of 100**, the catch **26 of 100** and its
/// 26 liveness catches, and the scramble moves 29 to **30 of 100** with the same 55
/// duplicate-chunk loops. Nothing asserted changes hands and no tier moves with it. The
/// figures below are the re-measured ones, at the tier each is labelled with.
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
    // Re-measured again on PROPOSED D-089's aim, which moves what both stream arms
    // watch and so moves every schedule that draws one: the fault stays 100/100, the
    // arm 1/100 and the catch 26/100, and the scramble goes 29 to 30 of 100.
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
    /// One seed under the variant: `checked`'s violation, the re-take at a taken index,
    /// the arm's aimed streams, a scramble, the duplicate-chunk loops, and the wedge.
    struct Shared {
        violation: Option<String>,
        fired: bool,
        aimed: usize,
        scrambled: bool,
        looped: usize,
        /// The liveness check's own catch (uniform schedules, D-016).
        wedged: bool,
        /// The read bound fired downstream of a wedge the fold reports.
        read_bound_on_wedge: bool,
        /// The read bound fired with no wedge, where the correct node passes the seed:
        /// the variant's load on a bound that counts retries (issue #125).
        load: bool,
    }
    let outcomes: Vec<Shared> = sweep(share, |seed| {
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
        let violation = checked(&report).err();
        let read_bound_on_wedge = violation
            .as_ref()
            .is_some_and(|v| read_bound_reports_a_wedge(&report, v));
        let load = violation
            .as_ref()
            .is_some_and(|v| read_bound_is_the_variants_load(&report, v));
        Shared {
            violation,
            fired: took_an_index_twice_on_the_node(&report),
            aimed: report.aimed_streams,
            scrambled: !scrambled.is_empty(),
            looped,
            wedged: wedged(&report),
            read_bound_on_wedge,
            load,
        }
    });
    let caught: Vec<&String> = outcomes
        .iter()
        .filter_map(|o| o.violation.as_ref())
        .collect();
    let fired = outcomes.iter().filter(|o| o.fired).count();
    let aimed = outcomes.iter().filter(|o| o.aimed > 0).count();
    let scrambled = outcomes.iter().filter(|o| o.scrambled).count();
    let looped: usize = outcomes.iter().map(|o| o.looped).sum();
    // Attributed (PROPOSED D-091): the liveness check is this variant's catch (RAFT.md §5)
    // and is what the assertions below read — from the fold itself (`wedged`), because
    // since the merge with `main` a wedged range's clients retry their reads into the
    // read bound and `check()` reports the node's failure first (D-076 point 12, issue
    // #125). The rest are named so a run the node failed on its own is not read as a
    // catch, as `main`'s nightly on c178682 read a read-bound trip.
    let liveness = outcomes.iter().filter(|o| o.wedged).count();
    let read_bound_on_wedge = outcomes.iter().filter(|o| o.read_bound_on_wedge).count();
    let read_bound_on_wedge_unasked = outcomes
        .iter()
        .filter(|o| o.read_bound_on_wedge && !o.wedged)
        .count();
    let load = outcomes.iter().filter(|o| o.load).count();
    let mut by_check: BTreeMap<&str, usize> = BTreeMap::new();
    for violation in &caught {
        *by_check.entry(mechanism(violation.as_str())).or_default() += 1;
    }
    println!(
        "node: SharedSnapshotDir failed a check on {}/{share} seeds (tier {tier}) and wedged \
         {liveness} of them by the liveness check's own reading, which is its catch; the \
         read bound fired downstream of a wedge the fold reports on {read_bound_on_wedge} \
         runs, {read_bound_on_wedge_unasked} of them on schedules the check does not ask \
         liveness of, and with no wedge on {load} runs the correct node passes — the \
         variant's load, not a catch (every violation by {by_check:?}), \
         re-took at an index already taken of one range on {fired} seeds, \
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
    // hundred-seed tier**, which is Phase 2's own tier for it. **30 of 100** on the
    // aimed tree, 29 on the merged tree and 17 of 100 before it, all above D-061's 5 %.
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
    // **The merge with `phase-3-stage-b-wiring` took this rate to 29 %**, and PROPOSED
    // D-089's aim to **30 %**, at which a sample of twenty sees none about one run in a
    // thousand and Phase 2's own hundred-seed tier would hold. The assertion is left
    // here deliberately: putting it back is a decision about a tier and belongs to the
    // owner, not to a merge and not to a slice that moved the rate by one seed.
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

/// `IgnoreIncarnation` on the node (D-042), and **why it is still not re-asserted**:
/// not the snapshot wiring, which this slice puts under these arms, and no longer
/// Q15's whole-node refusal and re-seed, which are in the tree (D-077) — but their
/// **rate** under these arms.
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
/// (server.rs, `ServerHost::repair`, D-042) — so on this node an incarnation changes
/// only where a store is refused and re-seeded, and this cluster's disk does not rot
/// (`Cluster::bitrot`), so a refusal is a crash's doing: #123's nightly reached one on
/// **1 of 10 000** seeds (run 35949696476, on 677cad3), and the merged tree's own
/// figure is printed below at every tier. Phase 2's reach is asserted from a hundred
/// seeds, which a rate near one in ten thousand does not support (D-061), and its
/// catch is 0 of 10 000 on one group too; asserting that the variant's leader traces
/// no reset would pass on the seeds where nothing was injected, which is the one
/// failure mode a sweep cannot report on its own.
///
/// What this test asserts instead, over a run that is asserted to reach the install
/// path: **the injection** on every seed — the variant's leader resets nothing — and
/// **the absence with its reason** — the correct leader resets nothing on a seed where
/// no store was refused. A reset on a seed that did refuse a store is the re-seed
/// doing what D-077 says; it is counted and printed, not failed, and the day the rate
/// supports it the reach is asserted here as Phase 2 asserts it.
// PROPOSED(D-086): `IgnoreIncarnation` is blocked on Q15's re-seed, not on the wiring.
// PROPOSED(D-086): on the merge with `main`, the re-seed is in the tree and rare
// here; a reset is asserted absent only where no store was refused.
#[test]
fn a_leader_that_ignores_incarnations_has_an_incarnation_to_ignore_only_where_a_store_is_refused() {
    // The share, as the high-rate variants use (D-055, D-061): this runs the correct
    // node *and* the variant on every seed it takes, so the full tier would cost twice
    // the correct sweep beside it, and what it asserts is an absence of a mechanism
    // that is absent by construction — a store incarnation that never changes — rather
    // than a rate that more seeds would sharpen.
    let seeds = high_rate_share();
    let outcomes: Vec<(u64, usize, usize, usize, usize)> = sweep(seeds, |seed| {
        let correct = correct(seed);
        let buggy = buggy(seed, Variant::IgnoreIncarnation);
        let resets = |report: &raft::Report| {
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftProgressReset { .. }))
        };
        (
            seed,
            resets(&correct),
            resets(&buggy),
            correct.refused.len(),
            correct.snapshot_actions(),
        )
    });
    let correct_resets: usize = outcomes.iter().map(|(_, c, _, _, _)| c).sum();
    let buggy_resets: usize = outcomes.iter().map(|(_, _, b, _, _)| b).sum();
    let refusals: usize = outcomes.iter().map(|(_, _, _, r, _)| r).sum();
    let actions: usize = outcomes.iter().map(|(_, _, _, _, a)| a).sum();
    let refused_seeds: Vec<(u64, usize)> = outcomes
        .iter()
        .filter(|(_, _, _, r, _)| *r > 0)
        .map(|(seed, c, _, _, _)| (*seed, *c))
        .collect();
    let reset_without_a_refusal: Vec<(u64, usize)> = outcomes
        .iter()
        .filter(|(_, c, _, r, _)| *c > 0 && *r == 0)
        .map(|(seed, c, _, _, _)| (*seed, *c))
        .collect();
    println!(
        "node: IgnoreIncarnation over {seeds} seeds — the correct node reset a follower's \
         progress {correct_resets} times and the variant {buggy_resets}, over {refusals} store \
         refusals (seeds and the correct node's resets on them: {refused_seeds:?}) and \
         {actions} snapshot actions. A leader resets on a *change* of store incarnation, \
         and only a re-seed rebuilds a store with a fresh one (D-077); a live install \
         keeps it (D-042). So the leader has something to forget only where a store was \
         refused"
    );
    // Non-vacuity: the install path is reached on these seeds, so this is an absence of
    // incarnation changes and not an absence of runs.
    assert!(
        actions > 0,
        "no core asked for a snapshot action over {seeds} seeds, so this says nothing"
    );
    // The absence, with its reason: an incarnation changes only where a store was
    // refused and re-seeded, so a reset on a seed with no refusal is a change nothing
    // explains. A reset on a seed that did refuse a store is D-077's re-seed doing what
    // it says, and it is in the print above, not in this assertion.
    assert!(
        reset_without_a_refusal.is_empty(),
        "the correct node reset a follower's progress on seeds where no store was refused \
         (seed, resets: {reset_without_a_refusal:?}), so a store incarnation changed on this \
         node without a re-seed: re-audit this test"
    );
    // The injection, on every seed: the variant's leader forgets nothing, refusal or no.
    assert_eq!(
        buggy_resets, 0,
        "the leader that ignores incarnations reset a follower's progress, which is the fix \
         the variant turns off"
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
/// **On the node the pair is the stream half alone wherever no store is refused**,
/// because the incarnation half has nothing to ignore without a re-seed, and a re-seed
/// here is a crash's doing on a disk that does not rot — about one seed in ten thousand
/// (see the test above). So the search SHARD.md asks for cannot be run against the node
/// at any tier that would settle it: a seed the pair is "caught on" here would be a
/// seed `SharedSnapshotDir` alone is caught on, which is not a wedge that needs both
/// bugs and so is not what D-045 pinned. That goes to the owner as the entry records,
/// and this test pins what the node *does* do, so that a seed where the incarnation
/// half does something is seen: the equality below fails on it and says to pin it.
// PROPOSED(D-086): the pair on the node is the stream half alone until Q15's re-seed.
// PROPOSED(D-086): on the merge with `main`, the re-seed is in the tree and rare here.
// PROPOSED(D-091): a catch is attributed to the variant's own violation.
#[derive(Debug)]
struct Outcome {
    /// The seed.
    seed: u64,
    /// The pair was caught by the liveness check on it.
    pair: bool,
    /// `SharedSnapshotDir` alone was.
    stream: bool,
    /// `IgnoreIncarnation` alone was.
    incarnation: bool,
    /// Violations by any other check, under any of the three: the node failing, not a
    /// variant being caught.
    otherwise: Vec<String>,
    /// Store refusals across the three runs: where the incarnation half has something
    /// to ignore.
    refused: usize,
    /// Runs of the three on which the read bound fired downstream of the wedge and
    /// `check()` reported it first: the wedge's second reporter, not the node's failure.
    read_bound_on_wedge: usize,
    /// Runs on which the read bound fired with no wedge and the correct node passes
    /// the seed: the variant's load on a bound that counts retries (issue #125).
    load: usize,
}

#[test]
fn the_pair_on_the_node_is_the_stream_half_alone_where_no_store_is_refused() {
    // The share (D-055, D-061): this runs three variants on every seed it takes, and
    // what it asserts is the *equality* of two of them — a structural claim about one
    // half being a no-op, which a share settles as well as a tier and at a tenth of
    // the cost.
    //
    // **Caught, not failed** (PROPOSED D-091). Both halves are caught by the liveness
    // check and by nothing else — `IgnoreIncarnation`'s wedge stalls a commit where
    // the bound is asked, and `SharedSnapshotDir`'s stream never completes so neither
    // designated follower counts (RAFT.md §5, which says of the second in so many
    // words that "only the liveness check's catches count"). So a seed is counted for
    // a variant only where the liveness check reported it. A violation by any other
    // check is the node failing under a variant that injects nothing of that check's
    // subject, and it is asserted separately, in its own words: the timer bound
    // PROPOSED D-089's aim reached was reported here as `IgnoreIncarnation` being
    // caught alone on seeds 272 and 516, which it cannot be while a store's
    // incarnation never changes — the test said so and was read as a catch anyway.
    let seeds = high_rate_share();
    let both = Variants::of(&[Variant::IgnoreIncarnation, Variant::SharedSnapshotDir]);
    // The catch is the wedge, read from the liveness fold itself (`wedged`): under
    // these variants a wedged range's clients retry their reads into the read bound,
    // and `check()` reports that before it reaches liveness. A read-bound trip on a
    // wedged seed is the wedge's second reporter and is counted as such; any other
    // violation, or a read-bound trip on a seed that did not wedge, is the node's own.
    // PROPOSED(D-086): a wedge is read from the fold.
    /// One run's reading: the wedge (the catch), the read bound firing downstream of
    /// it, the variant's load on the bound with no wedge, or something else.
    enum Read {
        Green,
        Wedge { by_the_bound: bool },
        Load,
        Other(String),
    }
    let by_liveness = |report: &raft::Report| -> Read {
        match checked(report) {
            Ok(()) => Read::Green,
            Err(violation) if mechanism(&violation) == "liveness" => Read::Wedge {
                by_the_bound: false,
            },
            Err(violation) if read_bound_reports_a_wedge(report, &violation) => {
                Read::Wedge { by_the_bound: true }
            }
            Err(violation) if read_bound_is_the_variants_load(report, &violation) => Read::Load,
            Err(violation) => Read::Other(format!(
                "{violation} [uniform: {}, the liveness fold: {:?}]",
                report.uniform(),
                report.liveness().err()
            )),
        }
    };
    let split = |read: Read| -> (bool, bool, bool, Option<String>) {
        match read {
            Read::Green => (false, false, false, None),
            Read::Wedge { by_the_bound } => (true, by_the_bound, false, None),
            Read::Load => (false, false, true, None),
            Read::Other(violation) => (false, false, false, Some(violation)),
        }
    };
    let outcomes: Vec<Outcome> = sweep(seeds, |seed| {
        let under_both = buggy(seed, both);
        let under_stream = buggy(seed, Variant::SharedSnapshotDir);
        let under_incarnation = buggy(seed, Variant::IgnoreIncarnation);
        let (pair, a_bound, a_load, a) = split(by_liveness(&under_both));
        let (stream, b_bound, b_load, b) = split(by_liveness(&under_stream));
        let (incarnation, c_bound, c_load, c) = split(by_liveness(&under_incarnation));
        Outcome {
            seed,
            pair,
            stream,
            incarnation,
            otherwise: [a, b, c].into_iter().flatten().collect(),
            refused: under_both.refused.len()
                + under_stream.refused.len()
                + under_incarnation.refused.len(),
            read_bound_on_wedge: usize::from(a_bound) + usize::from(b_bound) + usize::from(c_bound),
            load: usize::from(a_load) + usize::from(b_load) + usize::from(c_load),
        }
    });
    let otherwise: Vec<String> = outcomes
        .iter()
        .flat_map(|o| o.otherwise.iter().cloned())
        .collect();
    // **Per seed, not per count.** Counts were what this compared until PROPOSED
    // D-086's review, and a count equality cannot see the pin disappear: a genuine
    // D-045 wedge on one seed and a stream-only catch on another leave `pair` and
    // `stream` equal, and the wedge — the whole reason the pair exists — goes
    // unreported. The sets are compared, and the seeds where the pair is caught and
    // neither half alone is are asserted empty, which is the wedge itself.
    let seeds_where = |f: fn(&Outcome) -> bool| -> BTreeSet<u64> {
        outcomes.iter().filter(|o| f(o)).map(|o| o.seed).collect()
    };
    let pair = seeds_where(|o| o.pair);
    let stream = seeds_where(|o| o.stream);
    let incarnation = seeds_where(|o| o.incarnation);
    let only_the_pair = seeds_where(|o| o.pair && !o.stream && !o.incarnation);
    let refused = seeds_where(|o| o.refused > 0);
    let read_bound_on_wedge: usize = outcomes.iter().map(|o| o.read_bound_on_wedge).sum();
    let load = seeds_where(|o| o.load > 0);
    println!(
        "node: the pair caught on {}/{seeds} seeds {pair:?}, `SharedSnapshotDir` alone on {} \
         {stream:?}, `IgnoreIncarnation` alone on {} {incarnation:?}, and on {} seeds \
         {only_the_pair:?} the pair is caught where neither half alone is — which is the \
         wedge D-045 pinned; the wedge is read from the liveness fold, and on \
         {read_bound_on_wedge} runs the read bound fired downstream of it first; on {} seeds \
         {load:?} the read bound fired under a variant with no wedge, where the correct node \
         passes — the variant's load on a bound that counts retries, not a catch and not \
         the node's failure; a store was refused on {} seeds {refused:?}, the only ones the \
         incarnation half has anything to ignore on",
        pair.len(),
        stream.len(),
        incarnation.len(),
        only_the_pair.len(),
        load.len(),
        refused.len()
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
        "`IgnoreIncarnation` alone is caught on the node by the liveness check on seeds \
         {incarnation:?}, which it cannot be while a store's incarnation never changes: \
         re-audit this test"
    );
    // And what none of the three is about: a run that failed some other check. That
    // is the node failing, not a variant being caught, and it is reported as itself.
    // PROPOSED(D-091): a catch is attributed to the variant's own violation.
    assert!(
        otherwise.is_empty(),
        "a run under one of these variants failed a check none of them breaks, so the node \
         itself failed and no variant was caught: read these as the correct node's and fix \
         them there. {otherwise:?}"
    );
}

/// A Phase 2 variant this node has no path for, run on the node anyway.
///
/// m1 of the review of PROPOSED D-082: until then the blocked variants were named in
/// prose and by nothing executable, so `cargo test --list` and the nightly's shard
/// table carried none of the debt. Each of these runs its variant over the share and
/// asserts that the correct-system checks still pass — the variant injects nothing,
/// because the path it breaks is not reached here. It is an absence with a reason, and
/// its upgrade trigger is the day the path is reached at a rate a tier supports: the
/// variant starts being caught, this test fails, and it asks to be turned into the
/// assertion §10 wants.
///
/// **Four of the six that used to be here have made that crossing and are gone.**
/// PROPOSED D-086 reaches the stream path and re-asserts `SnapshotWithoutCurrentLast`,
/// `SharedSnapshotDir`, `IgnoreIncarnation` and D-045's pair as tests of their own,
/// above — each with the rate it was measured at. What is left waits on a **rate**,
/// not on a slice: Q15's whole-node refusal and re-seed are in the tree (D-077), and
/// on this cluster a refusal is a crash's doing on a disk that does not rot
/// (`Cluster::bitrot`), about one seed in ten thousand. `AdoptionAsBuilt` waits on the
/// refused directory being reached at a rate that supports its catch, and on a
/// translation of its rules onto the node's install and re-seed — the node reads that
/// variant nowhere today; `RefusalNotDurable` is read by the node (`quiesce_on_loss`)
/// and waits on the refusal's rate alone, or on the rot, which is the owner's.
///
/// The second half of what this asserted is gone with them, and deliberately. It used
/// to check that the snapshot path was still *unreached*, through `checked`; `checked`
/// now asserts the opposite, because the path is reached on every seed (PROPOSED
/// D-086). So these two run over a node that takes snapshots and streams them, and
/// still catch nothing — which is a stronger statement of the same absence than the
/// one it replaces, not a weaker one.
// PROPOSED(D-082): each blocked variant is named by a test, not by prose.
// PROPOSED(D-086): the four the stream path reaches are re-asserted above, not blocked.
/// **The absence is of the variant's own catch, and of nothing else** (PROPOSED
/// D-091). `checks` names the checks RAFT.md §5 says catch this variant; a violation
/// by one of them is the path arriving and this test asking to be turned into §10's
/// assertion. A violation by any *other* check is the correct node failing under a
/// variant that injects nothing on it — a real failure, and this test's job is to say
/// so in those words rather than to report it as the variant being caught.
///
/// That distinction is not hypothetical. The timer bound PROPOSED D-089's per-range
/// aim reached made this function claim `AdoptionAsBuilt` was caught on seed 440 and
/// `RefusalNotDurable` on seeds 272 and 516, quoting the timer strings as the
/// variants' catches — three false catches of one gap in a run neither variant
/// touched.
// PROPOSED(D-091): a catch is attributed to the variant's own violation.
fn blocked_on_the_node(
    name: &str,
    variants: impl Into<Variants> + Copy + Send + Sync,
    checks: &[&str],
    waits_on: &str,
) {
    let seeds = high_rate_share();
    let violations: Vec<String> = sweep(seeds, |seed| checked(&buggy(seed, variants)).err())
        .into_iter()
        .flatten()
        .collect();
    let caught = Caught::split(violations, checks);
    println!(
        "node: {name} is not re-asserted here; over {seeds} seeds it was caught {} times by \
         {checks:?}, its own checks, and {} runs failed some other check; it waits on \
         {waits_on}",
        caught.own.len(),
        caught.other.len()
    );
    assert!(
        caught.own.is_empty(),
        "{name} is caught on the node by {checks:?}, the checks RAFT.md §5 names for it, so \
         the path it breaks is reachable after all: turn this absence into the assertion §10 \
         asks for. {:?}",
        caught.own
    );
    assert!(
        caught.other.is_empty(),
        "the node failed under {name} by a check {name} does not break, and {name} injects \
         nothing on this node — so this is the **correct node's** failure and not a catch of \
         {name}. Read it as the correct node's and fix it there. {:?}",
        caught.other
    );
}

#[test]
fn a_server_whose_adoption_is_as_built_is_not_re_asserted_on_the_node_yet() {
    // The owner ruled on 2026-09-20 that this is re-asserted on the two rules that
    // remain rather than §10 amended. The two are the live install's single switch and
    // Q15's refused directory; both paths are in the tree now (PROPOSED D-083, D-077),
    // the node reads this variant on neither — the translation is owed with the
    // re-assertion — and the refused directory is reached on about one seed in ten
    // thousand here. The third rule, a damaged staging `CURRENT` refused, has no
    // subject on a node that adopts no staged store.
    // Its catch is `committed entries stay`: a voter restarts on a fresh store and
    // restates a truncation from index 1 below its commit index (RAFT.md §5).
    // PROPOSED(D-091): a catch is attributed to the variant's own violation.
    blocked_on_the_node(
        "AdoptionAsBuilt",
        Variant::AdoptionAsBuilt,
        &["committed entries stay"],
        "a translation of its rules onto the node's live install (PROPOSED D-083) and \
         re-seed (D-077), and a refusal rate a tier supports",
    );
}

#[test]
fn a_server_whose_refusal_is_not_durable_is_not_re_asserted_on_the_node_yet() {
    // The node *does* read this one (`quiesce_on_loss` in `server::run`), and it has
    // almost nothing to do: the disk does not rot on this cluster (`Cluster::bitrot`,
    // the owner's to turn on), so a store is refused only where a crash leaves an
    // engine a restart cannot open — about one seed in ten thousand — and the
    // whole-node re-seed that follows (D-077) is what a refusal not made durable
    // would launder after the next crash.
    // Its catch is the `match starts` oracle — a leader tracing a second first rise
    // of a follower's match under one incarnation — with `state machine safety` the
    // route D-078 measured out of reach and `a store refused` D-077's fan-out broken,
    // a replica serving again before its re-seed is durable (RAFT.md §5;
    // `whole_node_reseed`). All three are named, so any of them turns this absence
    // into §10's assertion.
    // PROPOSED(D-091): a catch is attributed to the variant's own violation.
    blocked_on_the_node(
        "RefusalNotDurable",
        Variant::RefusalNotDurable,
        &["match starts", "state machine safety", "a store refused"],
        "a refusal rate a tier supports, or the rot (`Cluster::bitrot`), which is the owner's",
    );
}

/// Seeds 272 and 516, which PROPOSED D-089's aim reached and nothing before it did,
/// and which PROPOSED D-091's fourth arm answered for: a replica being fed a snapshot
/// of one range went past the timer bound without campaigning, while the node held
/// its core for the **live** install of that range.
///
/// **Since PROPOSED D-096 the situation is absent on both seeds, and that is what
/// this pins — asserted, with its reason, not a bare green** (CLAUDE.md). The
/// bootstrap put two more replicas on every node — the root and the meta range, six
/// Raft groups where there were four — and with them every node's schedule moved:
/// every incarnation and timeout is a later draw of the node's generator, every
/// frame carries two more ranges' traffic, every install competes with two more
/// groups for the node's tasks. The hold this pinned was rare before the move, two
/// seeds in a thousand, and the probe that re-read the tree after it — the check
/// exactly as it stood on 1d17dcc, `TimerResets::WITHOUT_LIVE_INSTALL`, over a
/// thousand seeds in release — found **no seed at all** on which that check flags a
/// stretch held by a live install, so the fourth arm exempts nothing this tree
/// reaches. The arm stays, keyed to the hold as D-066 said it would have to be, and
/// its mechanism is unchanged: the `raft` task hands the repair over and holds the
/// range (D-066; PROPOSED D-083's `CoreWork::Hold`), and from there to the manifest
/// switch the replica's core takes no input and no tick, so it has no election timer
/// to fire. What has moved is the schedule that reached a hold long enough to cross
/// the bound.
///
/// So the assertion here is the absence, both ways: `check()` is green, the replay
/// under every arm finds nothing, **and the replay without the fourth arm finds
/// nothing either** — the arm is exempting nothing on these seeds. The day the last
/// of those fails, the schedule has reached the hold again and this pin is upgraded
/// back to the shape below, not deleted.
///
/// **The shape it pinned, for that day** (identical on both seeds on 1d17dcc's
/// schedule). The flagged replica's clock was last reset by an `AppendEntries` of its
/// term. Its leader then stopped appending to it and started streaming it a snapshot
/// of that range, re-opening the stream at offset 0 five times across the window
/// (`RaftSnapshotResumed`), and no `InstallSnapshot` chunk of that range was delivered
/// to it inside the window, so D-030's reset arm had nothing to fire on. The install
/// of that range was then decided on it and from there to the switch its core was
/// held. Seed 272: server 2's replica of range 4 under leader 3, the window
/// 21.356963658 s to 21.757937867 s, the install decided at 21.647374004 s and switched,
/// with the restatement on it, at 21.789195738 s — the flag 110.6 ms into the hold and
/// 31.3 ms before the restatement. Seed 516: server 2's replica of range 5 under leader
/// 1, the window 16.920911437 s to 17.321568677 s, the install decided at
/// 17.188140918 s and switched at 17.328088046 s — 133.4 ms into the hold, 6.5 ms
/// before the restatement. The upgraded pin asserts, on the seed the probe finds:
/// `timer_gaps_held_by_a_live_install()` is exactly one stretch, on the (server,
/// range) recorded with `live_installs == 1`; it equals
/// `timer_gaps(TimerResets::WITHOUT_LIVE_INSTALL)`, so the arm exempts the hold and
/// not more; the install's decision and switch bracket the flag; the `RaftSnapshot {
/// taken: false }` and the `RaftRecovered` of that replica land at the switch; and a
/// `RaftSnapshotResumed` toward the replica was decided inside the window.
///
/// **Why none of the check's other three arms covered it.** `TimerResets` has an arm
/// for an install chunk delivered (D-030, seed 164), one for an install's
/// restatement (D-039, seed 385) and one for the adoption of a completed install
/// (PROPOSED D-063, seed 2605). The third was written for a server whose run-loop
/// incarnation ends at the completion and begins again at its restatement. A node
/// does not do that: "an install into a live store keeps its incarnation" (D-042,
/// D-066), the range's replica is replaced in place, and the completion's own
/// restatement lands at the same instant — which is what D-063's `restates` predicate
/// reads as a start's re-trace, so that arm does not fire and would be the wrong one
/// if it did. PROPOSED D-091 is the fourth arm the owner ruled on.
// PROPOSED(D-089): the stream arms aim their victim at a range it lags.
// PROPOSED(D-091): the node's live install holds one range, and the replay does not
// measure a replica whose core is held.
// PROPOSED(D-096): the bootstrap's two system ranges moved every node schedule; the
// hold is absent on both seeds and on a thousand, and the pin asserts the absence.
#[test]
fn seeds_272_and_516_are_a_live_installs_hold_and_the_fourth_arm_answers_for_them() {
    for seed in [272u64, 516] {
        let report = correct(seed);
        report
            .check()
            .unwrap_or_else(|violation| panic!("seed {seed} no longer passes: {violation}"));
        let gaps = report.timer_gaps(raft::TimerResets::ALL);
        assert!(
            gaps.is_empty(),
            "seed {seed}: the timer replay reports {gaps:?} under every arm"
        );
        // The absence, asserted: the check without the fourth arm finds no stretch
        // held by a live install, so the arm exempts nothing on this seed. The day
        // this fails, the schedule has reached the hold again: upgrade the pin to the
        // shape in the comment above, with that seed's figures.
        let rescued = report.timer_gaps_held_by_a_live_install();
        assert!(
            rescued.is_empty(),
            "seed {seed}: the check without the fourth arm finds {} stretches held by a \
             live install: {rescued:?}. The schedule has reached PROPOSED D-091's hold \
             again on this seed, and this pin is upgraded to assert it, not left as an \
             absence",
            rescued.len()
        );
        assert!(
            report
                .timer_gaps(raft::TimerResets::WITHOUT_LIVE_INSTALL)
                .is_empty(),
            "seed {seed}: the check without the fourth arm flags a stretch the arm does \
             not answer for, and the replay under every arm did not: the arm is \
             exempting more than a live install's hold"
        );
        // What the seed still does have, printed: live installs on the node, so the
        // hold this arm is written for is exercised even where no stretch crosses the
        // bound inside one.
        let live_installs = report
            .records
            .iter()
            .filter(|record| {
                matches!(
                    &record.event,
                    ananke_env::TraceEvent::RaftSnapshot { taken: false, .. }
                )
            })
            .count();
        println!(
            "seed {seed}: no stretch held by a live install (PROPOSED D-096 moved the \
             schedule off D-091's hold); {live_installs} live installs on the run"
        );
    }
}

// --- The membership scenario on the node (SHARD.md §12's Stage B, issue #46) ---

use ananke_sim::membership;

/// The correct node's membership run for `seed`: five nodes, four ranges on each,
/// 3 → 5 → 3 on **every** range under the partitions the seed draws.
fn membership_node(seed: u64, variants: impl Into<Variants>) -> membership::Report {
    membership::run_on(Cluster::Node, seed, variants)
}

/// The run's verdict on the node: the scenario's own checks — which are now each
/// range's, not the cluster's — and then the shape four ranges are *for*, asserted on
/// every seed rather than printed.
///
/// The two clauses here are the ones a single-range world could not have made, and
/// each is stated per seed because a floor over a tier cannot see a seed that reached
/// nothing (CLAUDE.md): a run where the four changes never overlapped, or where the
/// joiners were admitted to one range and not four, has not run the scenario this
/// binary claims to run, and must say so rather than pass.
// PROPOSED(D-084): the membership scenario on the node, four ranges on every node.
fn membership_checked(report: &membership::Report) -> Result<(), String> {
    let seed = report.seed;
    report.check()?;
    // Issue #46's extension the node makes possible: a change of a range while
    // another range **on the same node** is changing. One group has one range and no
    // overlap to have, which is why this could not be asserted before.
    //
    // Asked as `witnessed_joint_overlap` and not as `joint_overlap`, because the
    // forward fold that produces the answer can be widened into always answering —
    // drop the line that clears a range when its joint configuration ends — and no
    // floor, count or bound could see it, since a fold that always answers passes
    // everything. The witness reads the same trace backwards at the record the fold
    // stopped on and asks whether both ranges really were joint there. It cannot be
    // satisfied by construction and it is not a bound, so the correct node cannot trip
    // it: 100 of 100 witnessed, against 87 of 100 for the never-clearing fold.
    // PROPOSED(D-084): the answer is witnessed, which is what guards the fold.
    if let Err(why) = report.witnessed_joint_overlap() {
        return Err(format!("seed {seed}: {why}"));
    }
    // The grow admits servers 4 and 5 to **every** range. A run that admitted them to
    // one range and left three unchanged completes "a change" and is not this
    // scenario; the whole-cluster reading cannot tell the two apart. Asked only where
    // the grow completed, which `Report::check` asserts of every uniformly scheduled
    // seed and liveness cannot be asked of the others (D-016).
    if report.grow_completed {
        let held: BTreeSet<u64> = report.ranges().into_iter().collect();
        for joiner in membership::INITIAL_VOTERS + 1..=membership::SERVERS {
            let admitted = report.ranges_admitting(joiner);
            if admitted != held {
                return Err(format!(
                    "seed {seed}: the grow completed, but server {joiner} became a voter of \
                     {admitted:?} and not of every range this node holds, {held:?}"
                ));
            }
        }
    }
    Ok(())
}

/// What the correct node's membership runs saw, over a tier.
#[derive(Debug, Default)]
struct NodeMembershipCoverage {
    seeds: u64,
    uniform_seeds: u64,
    grows_completed: u64,
    shrinks_completed: u64,
    /// Seeds on which a node held two ranges' joint configurations at once, **and the
    /// trace read backwards witnessed it** ([`membership::Report::witnessed_joint_overlap`]).
    overlapped: u64,
    /// Seeds on which both joiners became voters of every range.
    joiners_on_every_range: u64,
    /// Partitions that cut off the leader of the range they drew, and partitions made.
    partitions_hit: usize,
    partitions_made: usize,
    /// Snapshot actions traced, and the highest index any replica reached: the two
    /// halves of the absence this cluster asserts (`Report::check`).
    snapshot_actions: usize,
    highest_index: u64,
    /// Ranges that elected a leader and ranges that applied something, over the tier.
    leaders: BTreeMap<u64, usize>,
    applies: BTreeMap<u64, usize>,
    /// The worst gap any one range went without a completed operation, and the worst
    /// first write after a heal, against their bounds.
    worst_range_gap: Duration,
    worst_write_after_heal: Duration,
    /// The same two folded over the **cluster** instead, which is what the one-group
    /// scenario has always asked. They are here to be compared with the two above:
    /// a per-range fold that saw no more than the cluster's fold is not a per-range
    /// fold, and the comparison is the only thing that says so (see
    /// `assert_complete`).
    worst_cluster_gap: Duration,
    worst_cluster_write_after_heal: Duration,
    /// The ranges the run's partitions aimed at, over the tier.
    aimed_ranges: BTreeSet<u64>,
    /// The ranges a leadership transfer was asked for, over the tier, and how many of
    /// those transfers were followed by the asked-for server leading that range.
    // PROPOSED(D-084): the transfer hands over every range the node holds.
    transfer_ranges: BTreeSet<u64>,
    transfers_landed: usize,
    transfers_made: usize,
    /// The states the one-group scenario's own coverage asserts, folded here over the
    /// node's trace so that this positive control demands what that one does. Each is
    /// reachable on the node and none needs a snapshot path; the ones the node cannot
    /// reach are named in `assert_complete` with the reason, not left out.
    // PROPOSED(D-084): the node's positive control asks what the one-group one asks.
    joint_configs_taken: usize,
    new_configs_taken: usize,
    learners_promoted: usize,
    learner_rounds_caught_up: usize,
    config_reverts: usize,
    elections_while_joint: usize,
    step_downs_outside_new: usize,
    changes_accepted: usize,
    match_starts: usize,
    learner_rounds: usize,
    seeds_with_a_match_start: u64,
    seeds_with_a_learner_round: u64,
    seeds_with_a_change_accepted: u64,
    partitions: usize,
    completed: u64,
    /// Trace records, and the longest run in virtual seconds, for the density figure.
    records: usize,
    longest_run: f64,
}

impl NodeMembershipCoverage {
    fn add(&mut self, report: &membership::Report) {
        self.seeds += 1;
        self.uniform_seeds += u64::from(report.uniform());
        self.grows_completed += u64::from(report.grow_completed);
        self.shrinks_completed += u64::from(report.shrink_completed);
        self.overlapped += u64::from(report.witnessed_joint_overlap().is_ok());
        let held: BTreeSet<u64> = report.ranges().into_iter().collect();
        let every = (membership::INITIAL_VOTERS + 1..=membership::SERVERS)
            .all(|joiner| report.ranges_admitting(joiner) == held);
        self.joiners_on_every_range += u64::from(every);
        let (hit, made) = report.partitions_hit_their_ranges();
        self.partitions_hit += hit;
        self.partitions_made += made;
        self.snapshot_actions += report.snapshot_actions();
        self.highest_index = self.highest_index.max(report.highest_index());
        for range in report.ranges() {
            if let Some(gap) = report.longest_completion_gap_of(range) {
                self.worst_range_gap = self.worst_range_gap.max(gap);
            }
            if let Some(took) = report.time_to_write_after_heal_of(range) {
                self.worst_write_after_heal = self.worst_write_after_heal.max(took);
            }
        }
        if let Some(gap) = report.longest_completion_gap() {
            self.worst_cluster_gap = self.worst_cluster_gap.max(gap);
        }
        if let Some(took) = report.time_to_write_after_heal() {
            self.worst_cluster_write_after_heal = self.worst_cluster_write_after_heal.max(took);
        }
        self.aimed_ranges
            .extend(report.aimed.iter().map(|aimed| aimed.range));
        self.transfer_ranges.extend(report.transfer_ranges());
        let (landed, made) = report.transfers_landed();
        self.transfers_landed += landed;
        self.transfers_made += made;
        self.partitions += report.partitions.len();
        self.completed += report.clients.completed;
        self.fold_the_one_group_states(report);
        for record in &report.records {
            match &record.event {
                ananke_env::TraceEvent::RaftLeader { range, .. } => {
                    *self.leaders.entry(*range).or_default() += 1;
                }
                ananke_env::TraceEvent::RaftApply { range, .. } => {
                    *self.applies.entry(*range).or_default() += 1;
                }
                _ => {}
            }
        }
        self.records += report.records.len();
        let end = report.records.last().map_or(0, |r| r.at.as_nanos());
        self.longest_run = self.longest_run.max(end as f64 / 1e9);
    }

    fn merge(&mut self, other: Self) {
        self.seeds += other.seeds;
        self.uniform_seeds += other.uniform_seeds;
        self.grows_completed += other.grows_completed;
        self.shrinks_completed += other.shrinks_completed;
        self.overlapped += other.overlapped;
        self.joiners_on_every_range += other.joiners_on_every_range;
        self.partitions_hit += other.partitions_hit;
        self.partitions_made += other.partitions_made;
        self.snapshot_actions += other.snapshot_actions;
        self.highest_index = self.highest_index.max(other.highest_index);
        self.worst_range_gap = self.worst_range_gap.max(other.worst_range_gap);
        self.worst_write_after_heal = self
            .worst_write_after_heal
            .max(other.worst_write_after_heal);
        self.worst_cluster_gap = self.worst_cluster_gap.max(other.worst_cluster_gap);
        self.worst_cluster_write_after_heal = self
            .worst_cluster_write_after_heal
            .max(other.worst_cluster_write_after_heal);
        for (range, count) in other.leaders {
            *self.leaders.entry(range).or_default() += count;
        }
        for (range, count) in other.applies {
            *self.applies.entry(range).or_default() += count;
        }
        self.records += other.records;
        self.longest_run = self.longest_run.max(other.longest_run);
        self.aimed_ranges.extend(other.aimed_ranges);
        self.transfer_ranges.extend(other.transfer_ranges);
        self.transfers_landed += other.transfers_landed;
        self.transfers_made += other.transfers_made;
        self.joint_configs_taken += other.joint_configs_taken;
        self.new_configs_taken += other.new_configs_taken;
        self.learners_promoted += other.learners_promoted;
        self.learner_rounds_caught_up += other.learner_rounds_caught_up;
        self.config_reverts += other.config_reverts;
        self.elections_while_joint += other.elections_while_joint;
        self.step_downs_outside_new += other.step_downs_outside_new;
        self.changes_accepted += other.changes_accepted;
        self.match_starts += other.match_starts;
        self.learner_rounds += other.learner_rounds;
        self.seeds_with_a_match_start += other.seeds_with_a_match_start;
        self.seeds_with_a_learner_round += other.seeds_with_a_learner_round;
        self.seeds_with_a_change_accepted += other.seeds_with_a_change_accepted;
        self.partitions += other.partitions;
        self.completed += other.completed;
    }

    /// The states `MembershipCoverage` counts on the one-group server, folded over the
    /// node's trace with the same rules — a configuration's own `RaftConfig` per
    /// (server, range), a promotion per (index, joiner), a step-down of a leader the
    /// new configuration leaves out, a revert to a lower index, an election while
    /// joint — so that what this control demands and what that one demands can be
    /// compared line for line (PROPOSED D-084).
    // PROPOSED(D-084): the node's positive control asks what the one-group one asks.
    fn fold_the_one_group_states(&mut self, report: &membership::Report) {
        let match_starts =
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftMatchStarted { .. }));
        let learner_rounds =
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftLearnerRound { .. }));
        let changes =
            report.count(|e| matches!(e, ananke_env::TraceEvent::RaftChangeAccepted { .. }));
        self.match_starts += match_starts;
        self.learner_rounds += learner_rounds;
        self.changes_accepted += changes;
        self.seeds_with_a_match_start += u64::from(match_starts > 0);
        self.seeds_with_a_learner_round += u64::from(learner_rounds > 0);
        self.seeds_with_a_change_accepted += u64::from(changes > 0);
        self.learner_rounds_caught_up += report.count(|e| {
            matches!(
                e,
                ananke_env::TraceEvent::RaftLearnerRound {
                    caught_up: true,
                    ..
                }
            )
        });
        // Keyed by (server, range) and not by server: a node holds four replicas and
        // one entry would read one range's configuration as another's, which is the
        // mistake this whole slice exists to make visible.
        let mut in_force: BTreeMap<(u64, u64), (u64, bool)> = BTreeMap::new();
        let mut leading: BTreeSet<(u64, u64)> = BTreeSet::new();
        let mut promoted: BTreeSet<(u64, u64, u64)> = BTreeSet::new();
        for record in &report.records {
            match &record.event {
                ananke_env::TraceEvent::RaftConfig {
                    server,
                    range,
                    index,
                    old,
                    new,
                    joint,
                    ..
                } => {
                    if *joint {
                        self.joint_configs_taken += 1;
                        for id in new {
                            if !old.contains(id) {
                                promoted.insert((*range, *index, *id));
                            }
                        }
                    } else if *index > 0 {
                        self.new_configs_taken += 1;
                        if leading.contains(&(*range, *server)) && !old.contains(server) {
                            self.step_downs_outside_new += 1;
                        }
                    }
                    if let Some(&(previous, _)) = in_force.get(&(*range, *server))
                        && *index < previous
                    {
                        self.config_reverts += 1;
                    }
                    in_force.insert((*range, *server), (*index, *joint));
                }
                ananke_env::TraceEvent::RaftTerm {
                    server,
                    range,
                    role,
                    ..
                } => {
                    if &**role == "leader" {
                        leading.insert((*range, *server));
                    } else {
                        leading.remove(&(*range, *server));
                    }
                }
                ananke_env::TraceEvent::RaftLeader { server, range, .. } => {
                    leading.insert((*range, *server));
                    if in_force
                        .get(&(*range, *server))
                        .is_some_and(|&(_, joint)| joint)
                    {
                        self.elections_while_joint += 1;
                    }
                }
                _ => {}
            }
        }
        self.learners_promoted += promoted.len();
    }

    /// The floors, each measured on the correct node before it was written (D-061).
    ///
    /// The per-range counts are the ones that mean anything here: a scenario that
    /// elected and applied on one range while three sat idle would satisfy every
    /// whole-cluster count there is, which is the mistake `applies.contains_key` made
    /// in D-082's own campaign.
    fn assert_complete(&self) {
        assert_eq!(self.seeds, seeds(), "every seed of the tier is counted");
        assert_eq!(
            self.overlapped, self.seeds,
            "some seed's four changes never overlapped on one node: {self:?}"
        );
        assert_eq!(
            self.joiners_on_every_range, self.seeds,
            "some seed admitted the joiners to fewer than every range: {self:?}"
        );
        assert_eq!(
            self.grows_completed, self.seeds,
            "some seed's grow did not complete on every range: {self:?}"
        );
        assert_eq!(
            self.shrinks_completed, self.seeds,
            "some seed's shrink did not complete on every range: {self:?}"
        );
        // The absence this scenario's own threshold produces, over the tier as well as
        // per seed (`membership::NODE_SNAPSHOT_THRESHOLD`; PROPOSED D-086 says why it
        // stays at `1 << 30` where the raft-arms sweep runs its node at 12).
        assert_eq!(
            self.snapshot_actions, 0,
            "the node traced a snapshot action, which this scenario's threshold keeps \
             unreached: {self:?}"
        );
        assert!(
            self.highest_index < ananke_sim::membership::NODE_SNAPSHOT_THRESHOLD,
            "a replica reached this scenario's snapshot threshold: {self:?}"
        );
        for range in Cluster::Node.ranges() {
            assert!(
                self.leaders.get(&range).copied().unwrap_or(0) > 0,
                "range {range} never elected a leader over the tier: {self:?}"
            );
            assert!(
                self.applies.get(&range).copied().unwrap_or(0) > 0,
                "range {range} applied nothing over the tier: {self:?}"
            );
        }
        // Every partition aimed at a range cut off *that range's* leader. Measured at
        // 200/200 over a hundred seeds on the correct node; the floor is 90 %, as
        // D-082 set the raft sweep's, because a leadership change between the
        // driver's read and the partition is ordinary and is not a fault.
        assert!(
            self.partitions_made > 0,
            "no partition was made at all: {self:?}"
        );
        let hit = self.partitions_hit as f64 / self.partitions_made as f64;
        assert!(
            hit >= 0.9,
            "only {hit:.3} of the partitions cut off the leader of the range they drew, under \
             the floor of 0.900: {self:?}"
        );
        // The applies are spread over the ranges rather than piled on one. The floor
        // is the least range's share of the busiest, measured **on this scenario** at
        // **0.976** at a hundred seeds ({2: 11 396, 3: 11 438, 4: 11 161, 5: 11 229})
        // and 0.920 at the gate's twenty; D-082 set the same floor at 0.5 against its
        // own sweep's 0.87, for the same reason, that `contains_key` passes a range
        // which applied only its leader's no-ops.
        // Every range was aimed at by some partition over the tier. Without this a
        // schedule whose draw answered the first range every time would leave three
        // ranges' leaders never cut off, and every other floor here would be met:
        // `partitions_hit` counts hits against what was *aimed at*, so a draw that
        // always aims at one range hits it every time.
        assert_eq!(
            self.aimed_ranges,
            Cluster::Node.ranges().into_iter().collect::<BTreeSet<_>>(),
            "the partitions aimed at {:?} and not at every range this node holds: {self:?}",
            self.aimed_ranges
        );
        // A per-range fold that saw no more than the cluster's fold is not a per-range
        // fold. Both quantities below are per-range maxima over folds whose inputs are
        // *subsets* of the cluster fold's, so each is at least the cluster's by
        // construction and the only question is whether it is ever strictly more. Over
        // a tier it is, comfortably — at a hundred seeds the worst range went 996 ms
        // without a completed operation against the cluster's 329 ms, and a range's
        // first write after a heal took 1.130 s against the cluster's first — and this
        // is what fails if either fold is quietly widened back to the whole history,
        // which no bound below could see, since a widened fold only ever passes.
        assert!(
            self.worst_range_gap > self.worst_cluster_gap,
            "the worst gap of any one range, {:?}, is no worse than the cluster's {:?}: the \
             per-range availability fold is reading the whole history: {self:?}",
            self.worst_range_gap,
            self.worst_cluster_gap
        );
        assert!(
            self.worst_write_after_heal > self.worst_cluster_write_after_heal,
            "the worst first write after a heal of any one range, {:?}, is no worse than the \
             cluster's {:?}: the per-range liveness fold is reading the whole history: \
             {self:?}",
            self.worst_write_after_heal,
            self.worst_cluster_write_after_heal
        );
        // Every range a leadership transfer was asked for. D-084's item 6 says the
        // transfer hands over **every** range the node holds, so that the shrink's
        // leader is outside `C_new` on each of them; nothing else records that claim.
        // A transfer that reached one range of four leaves every other floor here met
        // — the elections it costs the other three are lost among the ones the
        // partitions cause anyway — so this is what says the claim was kept.
        // PROPOSED(D-084): the transfer hands over every range the node holds.
        assert!(
            self.transfers_made > 0,
            "no leadership transfer was asked for at all: {self:?}"
        );
        assert_eq!(
            self.transfer_ranges,
            Cluster::Node.ranges().into_iter().collect::<BTreeSet<_>>(),
            "leadership was handed over for {:?} and not for every range this node holds: \
             {self:?}",
            self.transfer_ranges
        );
        // And the transfers were followed by the server they named leading that range,
        // which is what sets the step-down up. Not at one — a transfer is one shot and
        // best effort, as the sweep's lease trial is — but at a floor measured on the
        // correct node: 140 of 192 (72.9 %) at a hundred seeds and 23 of 32 (71.9 %) at
        // the gate's twenty.
        let landed = self.transfers_landed as f64 / self.transfers_made as f64;
        assert!(
            landed >= 0.5,
            "only {landed:.3} of the leadership transfers were followed by the server they \
             named leading that range, under the floor of 0.500: {self:?}"
        );
        // What the one-group scenario's own coverage asserts of this scenario
        // (`MembershipCoverage::assert_complete`), asked here with the same tiering, so
        // that the node's positive control demands what the one-group one demands.
        // Every one of these is reachable on the node and none needs a snapshot path;
        // the ones the node cannot reach are named below with their reason rather than
        // left out.
        // PROPOSED(D-084): the node's positive control asks what the one-group one asks.
        for (what, seen) in [
            (
                "joint configurations taken",
                self.joint_configs_taken as u64,
            ),
            ("new configurations taken", self.new_configs_taken as u64),
            ("learners promoted", self.learners_promoted as u64),
            (
                "learner rounds that caught up",
                self.learner_rounds_caught_up as u64,
            ),
            ("partitions", self.partitions as u64),
            ("completed operations", self.completed),
            ("uniformly scheduled seeds", self.uniform_seeds),
        ] {
            assert!(
                seen > 0,
                "the node's membership runs never saw {what}: {self:?}"
            );
        }
        // SHARD.md §8's three events, each on *every* seed, as the one-group control
        // asserts them: the driver grows the configuration of every range on every
        // seed, so a change is accepted, a learner is tracked and caught up, and every
        // leader's first answer from a follower raises `matched` under the incarnation
        // it carried. 100 of 100 and 20 of 20 for all three here. A total above zero is
        // what an emission rule gone wrong passes; these say the rule fires where it
        // must.
        for (what, seen) in [
            (
                "a leader's match rise under a follower's incarnation",
                self.seeds_with_a_match_start,
            ),
            (
                "a learner's catch-up round",
                self.seeds_with_a_learner_round,
            ),
            ("an accepted change", self.seeds_with_a_change_accepted),
        ] {
            assert_eq!(
                seen, self.seeds,
                "a node membership run saw no {what}: {self:?}"
            );
        }
        // The one-group control's tiering for the two states that need the partition to
        // land inside a narrow phase of a change. On the node they are commoner —
        // 212 step-downs and 47 reverts at a hundred seeds, 41 and 0 at the gate's
        // twenty — because four ranges give four changes a seed; the tier is kept the
        // one-group one rather than tightened, since nothing here measured the rate a
        // tighter tier would rest on.
        if self.seeds >= 100 {
            for (what, seen) in [
                (
                    "step-downs of a leader outside C_new",
                    self.step_downs_outside_new as u64,
                ),
                ("configuration reverts", self.config_reverts as u64),
            ] {
                assert!(
                    seen > 0,
                    "the node's membership runs never saw {what}: {self:?}"
                );
            }
        }
        // An election while joint needs the partition to cut a leader off inside the
        // joint phase itself, and the one-group control asserts it from the
        // thousand-seed tier under D-061 (3.4 % of its seeds). On the node it is 13
        // elections over a hundred seeds and 2 over the gate's twenty, so the same
        // tier is kept.
        if self.seeds >= 1000 {
            assert!(
                self.elections_while_joint > 0,
                "the node's membership runs never saw elections while joint: {self:?}"
            );
        }
        // The spread is the user ranges' (PROPOSED D-096): the two system ranges apply
        // their terms' no-ops and nothing else, and are printed with the rest.
        let user_applies: Vec<usize> = self
            .applies
            .iter()
            .filter(|(range, _)| ranges().contains(range))
            .map(|(_, applies)| *applies)
            .collect();
        let least = user_applies.iter().copied().min().unwrap_or(0) as f64;
        let busiest = user_applies.iter().copied().max().unwrap_or(1).max(1) as f64;
        assert!(
            least / busiest >= 0.5,
            "the least busy range applied {:.2} of the busiest range's entries, under the floor \
             of 0.50: {self:?}",
            least / busiest
        );
    }
}

/// The positive control: the correct node passes 3 → 5 → 3 on every one of its four
/// ranges, on every seed, with the changes overlapping on a node and both joiners
/// admitted to every range.
///
/// Every figure SHARD.md §12 asks of a sweep on the node is printed beside the
/// verdict: the trace records a run holds per range per virtual second against
/// `TRACE_CAP`, the worst gap and first write after a heal any range showed against
/// their bounds, and the two halves of the snapshot path's absence.
// PROPOSED(D-084): the membership scenario on the node, four ranges on every node.
#[test]
fn every_seed_passes_the_membership_scenario_on_the_correct_node() {
    let coverage = std::sync::Mutex::new(NodeMembershipCoverage::default());
    let verdicts = sweep(seeds(), |seed| {
        let report = membership_node(seed, Variants::default());
        let mut mine = NodeMembershipCoverage::default();
        mine.add(&report);
        coverage.lock().unwrap().merge(mine);
        membership_checked(&report)
            .inspect_err(|_| write_trace(&format!("node-membership-{seed}"), &report.jsonl()))
    });
    let coverage = coverage.into_inner().unwrap();
    let per_range_per_second =
        coverage.records as f64 / 4.0 / coverage.longest_run.max(1e-9) / coverage.seeds as f64;
    eprintln!(
        "node membership: {coverage:?}; about {per_range_per_second:.0} records per range per \
         virtual second against a TRACE_CAP of {}",
        membership::TRACE_CAP
    );
    if let Err(violation) = verdict(&verdicts) {
        panic!("{violation}");
    }
    coverage.assert_complete();
}

/// The negative control on the node: a server that counts one merged majority while
/// joint (thesis §4.3) is caught by the scenario's checks on some seed.
///
/// Its Phase 2 test asserts exactly this and no more — caught on some seed, at every
/// tier — and that is what is asserted here (§10, Q39). Its **measured** rate on the
/// node is 14 of 100 seeds against the one-group server's 19 of 100, above D-061's
/// five per cent, so the tier it keeps is the tier it has. At the gate's twenty seeds
/// the catch is deterministic and not a coin: seeds 3 and 15 of the first twenty catch
/// it, so a green gate here is a gate that injected the fault.
// PROPOSED(D-084): Phase 2's variant re-asserted on the node, at the tier it uses today.
#[test]
fn a_server_that_counts_one_majority_in_joint_consensus_is_caught_on_the_node() {
    let outcomes: Vec<(Option<String>, bool)> = sweep(seeds(), |seed| {
        let report = membership_node(seed, Variant::SingleMajorityInJointConsensus);
        (report.check().err(), report.joint_overlap().is_some())
    });
    let overlapped = outcomes.iter().filter(|(_, o)| *o).count();
    let caught: Vec<String> = outcomes.into_iter().filter_map(|(v, _)| v).collect();
    eprintln!(
        "SingleMajorityInJointConsensus on the node: caught on {} of {} seeds, the changes \
         overlapped on {overlapped}, first: {}",
        caught.len(),
        seeds(),
        caught.first().map_or("", String::as_str)
    );
    assert!(
        !caught.is_empty(),
        "SingleMajorityInJointConsensus was never caught on the node"
    );
}

/// The node's membership scenario replays: two runs of one seed give byte-identical
/// traces, so the driver's decisions — which range it asks first, which range's leader
/// the partition cuts off — are functions of the trace and the seed alone.
// PROPOSED(D-084): the membership scenario on the node, four ranges on every node.
#[test]
fn a_membership_seed_replays_to_the_same_trace_on_the_node() {
    let first = membership_node(7, Variants::default());
    let second = membership_node(7, Variants::default());
    assert_eq!(first.jsonl().as_bytes(), second.jsonl().as_bytes());
}

/// One seed's run, read record by record: the two ranges a node changed at once, and
/// the node that held them.
///
/// The sweep asserts the overlap on every seed and this names it, so a reader can see
/// what issue #46's extension looks like on the node without running a tier.
// PROPOSED(D-084): #46's extension the node makes possible — one node, two ranges
// changing at once.
#[test]
fn a_node_changes_two_of_its_ranges_at_once() {
    let report = membership_node(1, Variants::default());
    membership_checked(&report).expect("the correct node passes membership seed 1");
    let (node, first, second, at) = report
        .joint_overlap()
        .expect("a node held two ranges' joint configurations at once");
    println!(
        "node membership: node {node} was jointly configured on ranges {first} and {second} at \
         {at:?}"
    );
    assert_ne!(first, second, "two ranges, not one");
    assert!(
        report.ranges().contains(&first) && report.ranges().contains(&second),
        "both are ranges this node holds"
    );
}
