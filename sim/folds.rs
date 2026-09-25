//! The node's measurement folds in incremental form (SHARD.md §12, Stage B's
//! measurements; the owner's ruling (b) at the stage's close, 2026-09-25; PROPOSED
//! D-095): the apply lag per range, the hold one range's applies put on the node's
//! others, and the coverage counters `sim/tests/node.rs` reads off a run's trace.
//!
//! Each fold is fed one record at a time and can be read at any prefix, and each has
//! a whole-trace reading beside it in [`crate::raft`] — the functions D-082 wrote,
//! kept as the reference — so `sim/tests/node.rs` can compare the two at every prefix
//! of every compared seed, as `sim/tests/raft.rs` compares the incremental checker
//! with the folds over the whole trace (D-046; §11, raft 12). Until this module the
//! three were measurements alone, read once over a finished run and never under that
//! test, which Stage B's tag names as a line of its exit not met.
//!
//! The lag has a verdict, against SHARD.md §4's threshold of one heartbeat interval on
//! each range's **median**, which `sim/tests/node.rs` has asserted of a sweep since
//! D-082 and which this fold asks per run. The hold and the coverage have none: D-082
//! recorded the hold as a figure to the owner and not a bound, and a coverage counter
//! has no rule to break, so what the test asks of those two is that the fold and the
//! reading agree, value for value, at every prefix, on runs a variant has moved.

use std::collections::BTreeMap;
use std::time::Duration;

use ananke_env::sim::TraceRecord;
use ananke_env::{ClientOp, DropReason, Instant, TraceEvent};

use crate::raft::{self, range_of};

/// What a replica has committed: the highest index, and when each index below it
/// became committed on that replica (a later commit over the same index restamps it,
/// as the whole-trace reading does).
#[derive(Default)]
struct Committed {
    highest: u64,
    at: BTreeMap<u64, Instant>,
}

impl Committed {
    fn advance(&mut self, index: u64, at: Instant) {
        for i in (self.highest + 1)..=index {
            self.at.insert(i, at);
        }
        self.highest = self.highest.max(index);
    }
}

/// The median of a sorted slice, the lower of the two middle values for an even
/// count: the same reading `raft::Report` and `sim/tests/node.rs` take.
#[must_use]
pub fn median(sorted: &[Duration]) -> Option<Duration> {
    if sorted.is_empty() {
        return None;
    }
    Some(sorted[(sorted.len() - 1) / 2])
}

/// The apply lag per range, incrementally: from a range's `RaftCommit` reaching an
/// index on a node to that node's `RaftApply` of it, in virtual time
/// ([`raft::apply_lags_of`] is the whole-trace reading).
// PROPOSED(D-095): the apply lag as an incremental fold under the equivalence test.
#[derive(Default)]
pub struct ApplyLagFold {
    committed: BTreeMap<(u64, u64), Committed>,
    lags: BTreeMap<u64, Vec<Duration>>,
}

impl ApplyLagFold {
    /// Folds one more record.
    pub fn push(&mut self, record: &TraceRecord) {
        match &record.event {
            TraceEvent::RaftCommit {
                server,
                range,
                index,
                ..
            } => self
                .committed
                .entry((*server, *range))
                .or_default()
                .advance(*index, record.at),
            TraceEvent::RaftApply {
                server,
                range,
                index,
                ..
            } => {
                if let Some(committed) = self.committed.get(&(*server, *range))
                    && let Some(at) = committed.at.get(index)
                    && record.at >= *at
                {
                    self.lags
                        .entry(*range)
                        .or_default()
                        .push(record.at.duration_since(*at));
                }
            }
            _ => {}
        }
    }

    /// Folds records in order.
    pub fn extend<'a>(&mut self, records: impl IntoIterator<Item = &'a TraceRecord>) {
        for record in records {
            self.push(record);
        }
    }

    /// Every lag measured so far, per range, in trace order.
    #[must_use]
    pub fn lags(&self) -> &BTreeMap<u64, Vec<Duration>> {
        &self.lags
    }

    /// The pooled median and each range's own, over what has been fed so far.
    #[must_use]
    pub fn medians(&self) -> (Option<Duration>, BTreeMap<u64, Duration>) {
        medians_of(&self.lags)
    }

    /// §4's threshold asked per range, as `sim/tests/node.rs` asks it of a sweep: the
    /// first range, in id order, whose median lag over this run exceeds `threshold`.
    ///
    /// # Errors
    ///
    /// That range, its median and the threshold.
    pub fn verdict(&self, threshold: Duration) -> Result<(), String> {
        let (_, per_range) = self.medians();
        match per_range.iter().find(|(_, median)| **median > threshold) {
            Some((range, median)) => Err(format!(
                "apply lag: range {range}'s median apply lag is {median:?}, past the threshold \
                 of {threshold:?}"
            )),
            None => Ok(()),
        }
    }
}

/// The pooled median and each range's own of a per-range sample map.
#[must_use]
pub fn medians_of(
    by_range: &BTreeMap<u64, Vec<Duration>>,
) -> (Option<Duration>, BTreeMap<u64, Duration>) {
    let mut all: Vec<Duration> = Vec::new();
    let mut per_range = BTreeMap::new();
    for (range, of_range) in by_range {
        all.extend(of_range.iter().copied());
        let mut sorted = of_range.clone();
        sorted.sort_unstable();
        if let Some(median) = median(&sorted) {
            per_range.insert(*range, median);
        }
    }
    all.sort_unstable();
    (median(&all), per_range)
}

/// How long a range's ready apply waited through **another** range's apply job on
/// the same node, incrementally, over windows in which that node was up throughout
/// ([`raft::cross_range_apply_holds_of`] is the whole-trace reading, and
/// [`raft::Report::cross_range_apply_holds_counted`] says what a hold is).
///
/// The whole-trace reading takes every crash and restart of the run first and then
/// folds the applies; this one takes them as they come, and can, because a hold's
/// window `[from, t1]` ends at or before the apply that reports it, so every crash
/// inside it has been traced by then.
// PROPOSED(D-095): the cross-range hold as an incremental fold under the equivalence
// test, in one pass.
#[derive(Default)]
pub struct CrossRangeHoldFold {
    downs: BTreeMap<u64, Vec<Instant>>,
    committed: BTreeMap<(u64, u64), Committed>,
    /// The two most recent applies on each node: (t0, t1) with t1's range.
    last: BTreeMap<u64, (Option<Instant>, Instant, u64)>,
    holds: Vec<Duration>,
    dropped: usize,
}

impl CrossRangeHoldFold {
    /// Folds one more record.
    pub fn push(&mut self, record: &TraceRecord) {
        match &record.event {
            TraceEvent::NodeCrashed { node } | TraceEvent::NodeRestarted { node } => {
                self.downs
                    .entry(u64::from(node.get()))
                    .or_default()
                    .push(record.at);
            }
            TraceEvent::RaftCommit {
                server,
                range,
                index,
                ..
            } => self
                .committed
                .entry((*server, *range))
                .or_default()
                .advance(*index, record.at),
            TraceEvent::RaftApply {
                server,
                range,
                index,
                ..
            } => {
                if let Some(&(t0, t1, of)) = self.last.get(server)
                    && of != *range
                    && let Some(committed) = self.committed.get(&(*server, *range))
                    && let Some(&ready) = committed.at.get(index)
                    && ready <= t1
                {
                    let from = match t0 {
                        Some(t0) if t0 > ready => t0,
                        _ => ready,
                    };
                    let crashed = self
                        .downs
                        .get(server)
                        .is_some_and(|at| at.iter().any(|&a| a >= from && a <= t1));
                    if crashed {
                        self.dropped += 1;
                    } else {
                        self.holds.push(t1.duration_since(from));
                    }
                }
                let t0 = self.last.get(server).map(|&(_, t1, _)| t1);
                self.last.insert(*server, (t0, record.at, *range));
            }
            _ => {}
        }
    }

    /// Folds records in order.
    pub fn extend<'a>(&mut self, records: impl IntoIterator<Item = &'a TraceRecord>) {
        for record in records {
            self.push(record);
        }
    }

    /// The holds so far, in trace order, and how many windows were dropped for
    /// holding a crash or a restart of their node.
    #[must_use]
    pub fn holds(&self) -> (&[Duration], usize) {
        (&self.holds, self.dropped)
    }

    /// The median hold over this run so far.
    #[must_use]
    pub fn median(&self) -> Option<Duration> {
        let mut sorted = self.holds.clone();
        sorted.sort_unstable();
        median(&sorted)
    }

    /// D-036's figure over this run so far, as the sweep prints it: the median hold
    /// and the longest. Neither is a verdict. D-082 recorded the hold as a figure to
    /// the owner and not a bound — it is an upper bound on one job's hold and a lower
    /// bound on the wait's total, and a take's hold on the correct node exceeds a
    /// heartbeat on its own (417 ms at the thousand-seed tier on `main`, D-086) — so
    /// what the equivalence test asks of this fold is that its holds agree with the
    /// reading's, value for value, on runs whose holds a variant has moved.
    #[must_use]
    pub fn median_and_longest(&self) -> (Option<Duration>, Option<Duration>) {
        (self.median(), self.holds.iter().copied().max())
    }
}

/// The counters `sim/tests/node.rs`'s coverage reads off one run's trace (D-082),
/// as one value: what the incremental fold and the whole-trace reading must agree
/// on at every prefix.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeCoverage {
    /// Records folded, in all and per range (a record about no range counts in the
    /// first alone).
    pub records: usize,
    /// Records per range, by [`range_of`].
    pub records_by_range: BTreeMap<u64, usize>,
    /// The first and last record's durability time.
    pub first: Option<Instant>,
    /// See `first`.
    pub last: Option<Instant>,
    /// `PartitionStarted` records.
    pub partitions: usize,
    /// `NodeCrashed` records.
    pub crashes: usize,
    /// Messages delivered as duplicates.
    pub duplicates: usize,
    /// Messages dropped by injection.
    pub drops: usize,
    /// `RaftLeader` records, in all and per range.
    pub leaders: usize,
    /// See `leaders`.
    pub leaders_by_range: BTreeMap<u64, usize>,
    /// Whether any leader's term was above one.
    pub terms_above_one: bool,
    /// `RaftTruncate` records.
    pub truncations: usize,
    /// `RaftCommit` records.
    pub commits: usize,
    /// `RaftApply` records, in all and per range.
    pub applies: usize,
    /// See `applies`.
    pub applies_by_range: BTreeMap<u64, usize>,
    /// The inbox's drops under its byte bound, by kind and range.
    pub inbox_drops: BTreeMap<(&'static str, u64), usize>,
    /// Snapshot actions a core asked for.
    pub snapshot_actions: usize,
    /// The highest index any append, commit or apply named.
    pub highest_index: u64,
    /// Client operations invoked, by kind.
    pub puts: u64,
    /// See `puts`.
    pub gets: u64,
    /// See `puts`.
    pub deletes: u64,
    /// See `puts`.
    pub cas: u64,
}

impl NodeCoverage {
    /// The whole-trace reading, through the functions `raft::Report` reads with
    /// (D-082), over `records` from the first: the reference the fold is held to.
    #[must_use]
    pub fn read(records: &[TraceRecord]) -> Self {
        let count =
            |f: &dyn Fn(&TraceEvent) -> bool| records.iter().filter(|r| f(&r.event)).count();
        let mut by_kind = (0u64, 0u64, 0u64, 0u64);
        let mut leaders_by_range = BTreeMap::new();
        let mut applies_by_range = BTreeMap::new();
        for record in records {
            match &record.event {
                TraceEvent::ClientInvoke { op, .. } => match op {
                    ClientOp::Put { .. } => by_kind.0 += 1,
                    ClientOp::Get { .. } => by_kind.1 += 1,
                    ClientOp::Delete { .. } => by_kind.2 += 1,
                    ClientOp::Cas { .. } => by_kind.3 += 1,
                },
                TraceEvent::RaftLeader { range, .. } => {
                    *leaders_by_range.entry(*range).or_default() += 1;
                }
                TraceEvent::RaftApply { range, .. } => {
                    *applies_by_range.entry(*range).or_default() += 1;
                }
                _ => {}
            }
        }
        Self {
            records: records.len(),
            records_by_range: raft::records_by_range_of(records),
            first: records.first().map(|r| r.at),
            last: records.last().map(|r| r.at),
            partitions: count(&|e| matches!(e, TraceEvent::PartitionStarted { .. })),
            crashes: count(&|e| matches!(e, TraceEvent::NodeCrashed { .. })),
            duplicates: count(&|e| matches!(e, TraceEvent::MessageDelivered { dup: true, .. })),
            drops: count(&|e| {
                matches!(
                    e,
                    TraceEvent::MessageDropped {
                        reason: DropReason::Injected,
                        ..
                    }
                )
            }),
            leaders: count(&|e| matches!(e, TraceEvent::RaftLeader { .. })),
            leaders_by_range,
            terms_above_one: records
                .iter()
                .any(|r| matches!(&r.event, TraceEvent::RaftLeader { term, .. } if *term > 1)),
            truncations: count(&|e| matches!(e, TraceEvent::RaftTruncate { .. })),
            commits: count(&|e| matches!(e, TraceEvent::RaftCommit { .. })),
            applies: count(&|e| matches!(e, TraceEvent::RaftApply { .. })),
            applies_by_range,
            inbox_drops: raft::inbox_drops_of(records),
            snapshot_actions: count(&|e| matches!(e, TraceEvent::RaftSnapshot { .. })),
            highest_index: raft::highest_index_of(records),
            puts: by_kind.0,
            gets: by_kind.1,
            deletes: by_kind.2,
            cas: by_kind.3,
        }
    }

    /// How long the run observed so far: last record less first.
    #[must_use]
    pub fn observed(&self) -> Duration {
        match (self.first, self.last) {
            (Some(first), Some(last)) => last.duration_since(first),
            _ => Duration::ZERO,
        }
    }

    /// Trace records per range per virtual second, over `ranges` ranges.
    #[must_use]
    pub fn records_per_second_per_range(&self, ranges: usize) -> f64 {
        let seconds = self.observed().as_secs_f64().max(f64::EPSILON);
        self.records as f64 / ranges.max(1) as f64 / seconds
    }

    /// The busiest range's own records per virtual second.
    #[must_use]
    pub fn busiest_range_records_per_second(&self) -> f64 {
        let seconds = self.observed().as_secs_f64().max(f64::EPSILON);
        self.records_by_range.values().copied().max().unwrap_or(0) as f64 / seconds
    }
}

/// The coverage counters folded one record at a time ([`NodeCoverage::read`] is the
/// whole-trace reading).
// PROPOSED(D-095): the node's coverage as an incremental fold under the equivalence
// test.
#[derive(Default)]
pub struct NodeCoverageFold {
    coverage: NodeCoverage,
}

impl NodeCoverageFold {
    /// Folds one more record.
    pub fn push(&mut self, record: &TraceRecord) {
        let c = &mut self.coverage;
        c.records += 1;
        if c.first.is_none() {
            c.first = Some(record.at);
        }
        c.last = Some(record.at);
        if let Some(range) = range_of(&record.event) {
            *c.records_by_range.entry(range).or_default() += 1;
        }
        match &record.event {
            TraceEvent::PartitionStarted { .. } => c.partitions += 1,
            TraceEvent::NodeCrashed { .. } => c.crashes += 1,
            TraceEvent::MessageDelivered { dup: true, .. } => c.duplicates += 1,
            TraceEvent::MessageDropped {
                reason: DropReason::Injected,
                ..
            } => c.drops += 1,
            TraceEvent::RaftLeader { range, term, .. } => {
                c.leaders += 1;
                *c.leaders_by_range.entry(*range).or_default() += 1;
                c.terms_above_one |= *term > 1;
            }
            TraceEvent::RaftTruncate { .. } => c.truncations += 1,
            TraceEvent::RaftCommit { index, .. } => {
                c.commits += 1;
                c.highest_index = c.highest_index.max(*index);
            }
            TraceEvent::RaftApply { range, index, .. } => {
                c.applies += 1;
                *c.applies_by_range.entry(*range).or_default() += 1;
                c.highest_index = c.highest_index.max(*index);
            }
            TraceEvent::RaftAppend { index, .. } => {
                c.highest_index = c.highest_index.max(*index);
            }
            TraceEvent::RaftInboxDropped { range, kind, .. } => {
                *c.inbox_drops.entry((kind, *range)).or_default() += 1;
            }
            TraceEvent::RaftSnapshot { .. } => c.snapshot_actions += 1,
            TraceEvent::ClientInvoke { op, .. } => match op {
                ClientOp::Put { .. } => c.puts += 1,
                ClientOp::Get { .. } => c.gets += 1,
                ClientOp::Delete { .. } => c.deletes += 1,
                ClientOp::Cas { .. } => c.cas += 1,
            },
            _ => {}
        }
    }

    /// Folds records in order.
    pub fn extend<'a>(&mut self, records: impl IntoIterator<Item = &'a TraceRecord>) {
        for record in records {
            self.push(record);
        }
    }

    /// The counters so far.
    #[must_use]
    pub fn coverage(&self) -> &NodeCoverage {
        &self.coverage
    }
}

#[cfg(test)]
mod tests {
    //! The folds' reasoning on records built by hand, each beside its whole-trace
    //! reading: the two must agree, and the hand-built shapes say what each counts.

    use super::*;
    use ananke_env::{ApplyEffect, NodeId};

    fn ms(n: u64) -> Instant {
        Instant::from_nanos(n * 1_000_000)
    }

    fn record(at: Instant, node: u64, event: TraceEvent) -> TraceRecord {
        TraceRecord {
            at,
            decided: at,
            node: Some(NodeId::new(u32::try_from(node).expect("small"))),
            event,
        }
    }

    fn commit(server: u64, range: u64, index: u64) -> TraceEvent {
        TraceEvent::RaftCommit {
            server,
            range,
            term: 1,
            index,
        }
    }

    fn apply(server: u64, range: u64, index: u64) -> TraceEvent {
        TraceEvent::RaftApply {
            server,
            range,
            index,
            entry_term: 1,
            hash: 0,
            key: None,
            effect: ApplyEffect::None,
        }
    }

    fn fed<F: Default>(records: &[TraceRecord], push: impl Fn(&mut F, &TraceRecord)) -> F {
        let mut fold = F::default();
        for record in records {
            push(&mut fold, record);
        }
        fold
    }

    /// A commit at 10 ms applied at 25 ms is a lag of 15 ms for its range; an apply
    /// of an index no commit stamped is no sample; and both readings say so.
    // PROPOSED(D-095)
    #[test]
    fn the_lag_fold_measures_commit_to_apply_per_range_and_agrees_with_the_reading() {
        let records = vec![
            record(ms(10), 1, commit(1, 2, 1)),
            record(ms(25), 1, apply(1, 2, 1)),
            record(ms(30), 1, apply(1, 3, 7)),
        ];
        let fold: ApplyLagFold = fed(&records, ApplyLagFold::push);
        let mut wanted = BTreeMap::new();
        wanted.insert(2, vec![Duration::from_millis(15)]);
        assert_eq!(
            fold.lags(),
            &wanted,
            "one stamped lag, and the unstamped apply is no sample"
        );
        assert_eq!(fold.lags(), &raft::apply_lags_of(&records));
        assert!(fold.verdict(Duration::from_millis(20)).is_ok());
        assert!(
            fold.verdict(Duration::from_millis(10))
                .unwrap_err()
                .contains("range 2"),
            "a median past the threshold names its range"
        );
    }

    /// The hold fold, one pass, on the shapes the whole-trace reading's own test
    /// uses (sim/raft.rs): a wait across ranges is one hold, a crash inside the
    /// window drops it, one range alone holds nothing, and another node's applies
    /// are not this node's.
    // PROPOSED(D-095)
    #[test]
    fn the_hold_fold_in_one_pass_agrees_with_the_two_pass_reading() {
        let across = vec![
            record(ms(10), 1, commit(1, 3, 1)),
            record(ms(20), 1, apply(1, 2, 1)),
            record(ms(40), 1, apply(1, 2, 2)),
            record(ms(60), 1, apply(1, 3, 1)),
        ];
        let fold: CrossRangeHoldFold = fed(&across, CrossRangeHoldFold::push);
        assert_eq!(fold.holds(), (&[Duration::from_millis(20)][..], 0));
        let (holds, dropped) = raft::cross_range_apply_holds_of(&across);
        assert_eq!(fold.holds(), (holds.as_slice(), dropped));

        let mut crashed = across.clone();
        crashed.insert(
            2,
            record(
                ms(25),
                1,
                TraceEvent::NodeCrashed {
                    node: NodeId::new(1),
                },
            ),
        );
        let fold: CrossRangeHoldFold = fed(&crashed, CrossRangeHoldFold::push);
        assert_eq!(
            fold.holds(),
            (&[][..], 1),
            "a crash inside the window is dropped, and counted"
        );
        let (holds, dropped) = raft::cross_range_apply_holds_of(&crashed);
        assert_eq!(fold.holds(), (holds.as_slice(), dropped));

        let alone = vec![
            record(ms(10), 1, commit(1, 2, 3)),
            record(ms(20), 1, apply(1, 2, 1)),
            record(ms(40), 1, apply(1, 2, 2)),
            record(ms(60), 1, apply(1, 2, 3)),
        ];
        let fold: CrossRangeHoldFold = fed(&alone, CrossRangeHoldFold::push);
        assert_eq!(
            fold.holds(),
            (&[][..], 0),
            "one range's own applies hold nothing"
        );

        let elsewhere = vec![
            record(ms(10), 1, commit(2, 3, 1)),
            record(ms(20), 1, apply(1, 2, 1)),
            record(ms(40), 1, apply(1, 2, 2)),
            record(ms(60), 2, apply(2, 3, 1)),
        ];
        let fold: CrossRangeHoldFold = fed(&elsewhere, CrossRangeHoldFold::push);
        assert_eq!(
            fold.holds(),
            (&[][..], 0),
            "a wait on another node is not this node's"
        );
        assert_eq!(fold.median_and_longest(), (None, None));
    }

    /// The coverage fold counts what the reading counts, on a trace with one of each
    /// kind it reads, and the two are equal as values.
    // PROPOSED(D-095)
    #[test]
    fn the_coverage_fold_agrees_with_the_reading_value_for_value() {
        let records = vec![
            record(
                ms(1),
                1,
                TraceEvent::RaftLeader {
                    server: 1,
                    range: 2,
                    term: 2,
                    last_index: 4,
                },
            ),
            record(ms(2), 1, commit(1, 2, 4)),
            record(ms(3), 1, apply(1, 2, 4)),
            record(
                ms(4),
                1,
                TraceEvent::NodeCrashed {
                    node: NodeId::new(1),
                },
            ),
            record(
                ms(5),
                1,
                TraceEvent::RaftInboxDropped {
                    server: 1,
                    range: 3,
                    kind: "heartbeat",
                },
            ),
        ];
        let fold: NodeCoverageFold = fed(&records, NodeCoverageFold::push);
        let read = NodeCoverage::read(&records);
        assert_eq!(fold.coverage(), &read);
        assert_eq!(read.leaders, 1);
        assert!(read.terms_above_one);
        assert_eq!(read.highest_index, 4);
        assert_eq!(read.crashes, 1);
        assert_eq!(read.inbox_drops.get(&("heartbeat", 3)), Some(&1));
        assert_eq!(read.records_by_range.get(&2), Some(&3));
        assert_eq!(read.observed(), Duration::from_millis(4));
    }
}
