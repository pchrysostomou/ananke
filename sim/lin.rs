//! The linearizability checker (RAFT.md §4): a Wing-Gong search with Lowe's
//! partitioning and Horn and Kroening's memoisation, over the client operations a
//! trace records, in the shape of porcupine.
//!
//! The history is the `ClientInvoke` and `ClientReturn` pairs of a trace with their
//! global virtual times. An operation that never returned is pending: it may be
//! linearized anywhere after its invocation, or left out. The trace closes most of
//! them: a leader traces which entry a request became (`RaftProposed`), and an
//! abandoned operation whose entry applied took effect then, with a result the
//! client never saw, so it returns at the apply with an unknown result; one no
//! leader ever proposed cannot have taken effect and leaves the history; one
//! proposed but never applied stays pending. A pending operation is a candidate at
//! every step of the search, so closing them is what keeps the search small.
//!
//! An entry is an entry of one Raft group, so the closure is keyed by `(range,
//! index, term)` (SHARD.md §9): a trace of several groups holds an entry (5, 2) in
//! each of them, and an operation proposed at (5, 2) in one range must not be
//! closed — given a return time it never had — by another range's apply of its own
//! (5, 2). Nothing else about the search knows of ranges: the model is a product of
//! independent registers and a key's register is one register whichever range
//! served it, so a split, a merge or a move changes no partition (SHARD.md §9).
//!
//! Single-key operations partition by key, since the store is a product of
//! independent registers, so each key is searched on its own: a state is the set of
//! operations linearized so far and the register's value, and a state seen once is
//! not searched twice. The search has a budget of states per key; a key that
//! exhausts it is reported as such, distinct from a violation, and the correct
//! server must never reach it. A violation names the key, how far the search got,
//! and the operations it could not place.
//!
//! One reduction keeps that search out of the exponential (D-080). A candidate that
//! only *reads* the register — a get, or a compare-and-set known to have found
//! something other than what it expected — leaves the value as it found it and has
//! its result settled by the value alone. Such an operation never needs a branch
//! point: if any linearization of the rest exists, one exists with that read first.
//! `reads_only` carries the exchange argument. So every read-only candidate is
//! committed at once, with no alternative kept, and the search branches only over
//! the operations that write. Without it a window of *w* concurrent reads of one
//! value is 2^w states that the memo cannot collapse, because each is a genuinely
//! different set of linearized operations reaching the same register value; with it
//! the window is one state. This is Wing and Gong's search under a partial-order
//! reduction, and it is what makes the checker's cost track the number of writes in
//! a window rather than the number of operations.
//!
//! Each key's search leaves a timeline of (time, value) for the linearization it
//! found, so a multi-key read can later be checked against every key it covers.

use std::collections::{BTreeMap, BTreeSet};

use ananke_env::sim::TraceRecord;
use ananke_env::{ClientOp, ClientResult, Instant, TraceEvent};
use bytes::Bytes;

/// One entry of one Raft group, as `(range, index, term)`: what a proposal names
/// and what an apply closes (SHARD.md §9). With several groups in one trace the
/// index and the term alone name an entry in each of them.
// PROPOSED(D-071): the history's closure keyed by (range, index, term).
type EntryId = (u64, u64, u64);

/// One client operation of the history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op {
    /// The client process.
    pub process: u64,
    /// The operation's number within the process.
    pub seq: u64,
    /// When it was invoked.
    pub call: Instant,
    /// When it returned; `None` for a pending operation.
    pub ret: Option<Instant>,
    /// What it was.
    pub op: ClientOp,
    /// What it returned; `None` for a pending operation.
    pub result: Option<ClientResult>,
}

/// The operations of a run.
#[derive(Clone, Debug, Default)]
pub struct History {
    /// Every operation, in invocation order.
    pub ops: Vec<Op>,
    /// Abandoned operations closed at the apply of their entry.
    pub closed_by_apply: usize,
    /// Abandoned operations no leader proposed, left out.
    pub never_proposed: usize,
}

impl History {
    /// The client operations a trace records, paired by process and sequence
    /// number, with abandoned operations closed by their entries' fate as the module
    /// documentation says.
    #[must_use]
    pub fn from_trace(records: &[TraceRecord]) -> Self {
        let mut ops: Vec<Op> = Vec::new();
        let mut open: BTreeMap<(u64, u64), usize> = BTreeMap::new();
        // The proposals and the applies are keyed by (range, index, term): an entry
        // is an entry *of one group*, and with several groups in one trace an
        // operation proposed at (5, 2) in one range would otherwise be closed by
        // the first apply of (5, 2) in any range (SHARD.md §9; §11, env 9).
        // PROPOSED(D-071): the history's closure keyed by (range, index, term).
        let mut proposed: BTreeMap<(u64, u64), Vec<EntryId>> = BTreeMap::new();
        let mut applied_at: BTreeMap<EntryId, Instant> = BTreeMap::new();
        for record in records {
            match &record.event {
                TraceEvent::ClientInvoke { client, seq, op } => {
                    open.insert((*client, *seq), ops.len());
                    ops.push(Op {
                        process: *client,
                        seq: *seq,
                        call: record.at,
                        ret: None,
                        op: op.clone(),
                        result: None,
                    });
                }
                TraceEvent::ClientReturn {
                    client,
                    seq,
                    result,
                } => {
                    if let Some(at) = open.remove(&(*client, *seq)) {
                        ops[at].ret = Some(record.at);
                        ops[at].result = Some(result.clone());
                    }
                }
                TraceEvent::RaftProposed {
                    range,
                    client,
                    seq,
                    index,
                    term,
                    ..
                } => {
                    proposed
                        .entry((*client, *seq))
                        .or_default()
                        .push((*range, *index, *term));
                }
                TraceEvent::RaftApply {
                    range,
                    index,
                    entry_term,
                    ..
                } => {
                    applied_at
                        .entry((*range, *index, *entry_term))
                        .or_insert(record.at);
                }
                _ => {}
            }
        }
        let mut closed_by_apply = 0;
        let mut never_proposed = 0;
        let mut kept = Vec::with_capacity(ops.len());
        for mut op in ops {
            if op.ret.is_none() {
                match proposed.get(&(op.process, op.seq)) {
                    None => {
                        never_proposed += 1;
                        continue;
                    }
                    Some(entries) => {
                        if let Some(at) = entries.iter().filter_map(|e| applied_at.get(e)).min() {
                            op.ret = Some(*at);
                            closed_by_apply += 1;
                        }
                    }
                }
            }
            kept.push(op);
        }
        Self {
            ops: kept,
            closed_by_apply,
            never_proposed,
        }
    }

    /// How many operations returned.
    #[must_use]
    pub fn completed(&self) -> usize {
        self.ops.iter().filter(|op| op.ret.is_some()).count()
    }

    /// How many operations are pending.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.ops.len() - self.completed()
    }
}

/// A key's value over time under the linearization found: each change, as (the
/// time it took effect, the value from then on). The value before the first change
/// is absent.
pub type Timeline = Vec<(Instant, Option<Bytes>)>;

/// How many states one key's search may visit.
pub const BUDGET: usize = 2_000_000;

/// A history that is not linearizable, or one whose search ran out of budget.
#[derive(Clone, Debug)]
pub struct Violation {
    /// The key whose operations have no linearization.
    pub key: Bytes,
    /// How many of the key's operations there are.
    pub ops: usize,
    /// The most operations any path of the search linearized.
    pub deepest: usize,
    /// The operations the search could not place at its deepest point, by
    /// invocation: the earliest few.
    pub stuck: Vec<Op>,
    /// Whether the search stopped at [`BUDGET`] rather than at the end.
    pub exhausted: bool,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "linearizability: key {:?}: {} of {} operations placed{}; could not place",
            String::from_utf8_lossy(&self.key),
            self.deepest,
            self.ops,
            if self.exhausted {
                " before the search budget ran out"
            } else {
                ""
            }
        )?;
        for op in self.stuck.iter().take(4) {
            write!(
                f,
                " [{}/{} {} {:?}..{:?} -> {:?}]",
                op.process,
                op.seq,
                op.op.name(),
                op.call,
                op.ret,
                op.result
            )?;
        }
        Ok(())
    }
}

/// Checks every key's operations and returns each key's timeline.
///
/// # Errors
///
/// The first key with no linearization, or whose search ran out of budget.
pub fn check(history: &History) -> Result<BTreeMap<Bytes, Timeline>, Violation> {
    let mut by_key: BTreeMap<Bytes, Vec<&Op>> = BTreeMap::new();
    for op in &history.ops {
        by_key.entry(op.op.key().clone()).or_default().push(op);
    }
    let mut timelines = BTreeMap::new();
    for (key, mut ops) in by_key {
        ops.sort_by_key(|op| op.call);
        match linearize(&ops) {
            Ok(order) => {
                timelines.insert(key, timeline(&ops, &order));
            }
            Err(stuck) => {
                return Err(Violation {
                    key,
                    ops: ops.len(),
                    deepest: stuck.deepest,
                    stuck: stuck.stuck.iter().map(|&i| ops[i].clone()).collect(),
                    exhausted: stuck.exhausted,
                });
            }
        }
    }
    Ok(timelines)
}

/// The value after `op` runs on `value`, or `None` when its result contradicts
/// that. A pending operation, with no result, always runs.
fn apply(
    op: &ClientOp,
    result: Option<&ClientResult>,
    value: &Option<Bytes>,
) -> Option<Option<Bytes>> {
    match op {
        ClientOp::Put { value: new, .. } => match result {
            None | Some(ClientResult::Done) => Some(Some(new.clone())),
            Some(_) => None,
        },
        ClientOp::Delete { .. } => match result {
            None | Some(ClientResult::Done) => Some(None),
            Some(_) => None,
        },
        ClientOp::Get { .. } => match result {
            None => Some(value.clone()),
            Some(ClientResult::Value(got)) => (got == value).then(|| value.clone()),
            Some(_) => None,
        },
        ClientOp::Cas {
            expect, value: new, ..
        } => {
            let swaps = value == expect;
            let after = if swaps {
                Some(new.clone())
            } else {
                value.clone()
            };
            match result {
                None => Some(after),
                Some(ClientResult::Swapped(swapped)) => (*swapped == swaps).then_some(after),
                Some(_) => None,
            }
        }
    }
}

/// Whether `op` is *read-only* at `value`: it applies there, it leaves the value
/// exactly as it found it, and its result is settled by that value alone.
///
/// The search commits such an operation without keeping an alternative (D-080), so
/// the exchange argument matters. Let `o` be a read-only candidate at a state, and
/// suppose some linearization `L` of the unlinearized operations exists from there.
///
/// * `o` may go first in the real-time order. `o` is a candidate, so `o.call` is at
///   or before the earliest return among the unlinearized operations; every `x` in
///   `L` therefore has `o.call <= x.ret`, and real-time order forbids `o` before `x`
///   only when `x.ret < o.call`.
/// * `o` may go first in the value order. `o` does not write, so every other
///   operation of `L` sees exactly the values it saw before, in the same order; and
///   `o` itself applies at the state's value, which is what this function checked.
/// * So `L` with `o` moved to the front — or, when `o` is pending and `L` left it
///   out, `o` prepended to `L` — is a linearization too.
///
/// The second point is why the two cases below are the only ones. A put or a delete
/// writes. A compare-and-set that swapped writes. A compare-and-set with **no**
/// result (abandoned, closed at its entry's apply) is not read-only either, even
/// where it happens not to swap at this value: deferred, it may meet a different
/// value, swap there, and be the write a later read needs — so moving it earlier
/// changes the value order, and the exchange argument does not hold for it.
fn reads_only(op: &Op, value: &Option<Bytes>) -> bool {
    match (&op.op, &op.result) {
        // A get never writes. With a result it must have seen this value, which
        // `apply` checks; abandoned, with no result, it matches whatever it meets.
        (ClientOp::Get { .. }, _) => apply(&op.op, op.result.as_ref(), value).is_some(),
        // A compare-and-set that is known not to have swapped is a read assertion:
        // it applies only where the value differs from what it expected, and it
        // leaves that value alone.
        (ClientOp::Cas { .. }, Some(ClientResult::Swapped(false))) => {
            apply(&op.op, op.result.as_ref(), value).is_some()
        }
        _ => false,
    }
}

fn bit(mask: &[u64], i: usize) -> bool {
    mask[i / 64] & (1 << (i % 64)) != 0
}

fn set(mask: &mut [u64], i: usize, on: bool) {
    if on {
        mask[i / 64] |= 1 << (i % 64);
    } else {
        mask[i / 64] &= !(1 << (i % 64));
    }
}

/// Where a failed search got to.
struct Stuck {
    deepest: usize,
    stuck: Vec<usize>,
    exhausted: bool,
}

/// The search state for one key.
struct Search<'a> {
    ops: &'a [&'a Op],
    mask: Vec<u64>,
    value: Option<Bytes>,
    order: Vec<usize>,
    seen: BTreeSet<(Vec<u64>, Option<Bytes>)>,
    budget: usize,
    deepest: usize,
    stuck: Vec<usize>,
}

impl Search<'_> {
    /// Whether every completed operation is linearized.
    fn done(&self) -> bool {
        (0..self.ops.len()).all(|i| bit(&self.mask, i) || self.ops[i].ret.is_none())
    }

    /// `Some(found)` when the search ended, `None` when the budget ran out.
    fn dfs(&mut self) -> Option<bool> {
        if self.done() {
            return Some(true);
        }
        if self.budget == 0 {
            return None;
        }
        self.budget -= 1;
        if !self.seen.insert((self.mask.clone(), self.value.clone())) {
            return Some(false);
        }
        // An operation may go next only if no unlinearized operation returned before
        // it was invoked. Completed operations are tried before pending ones, since
        // a pending one is a candidate at every step.
        //
        // PROPOSED(D-080): the read-only candidates go first, together, with no
        // branch point kept.
        let min_ret = (0..self.ops.len())
            .filter(|&i| !bit(&self.mask, i))
            .filter_map(|i| self.ops[i].ret)
            .min();
        let candidates: Vec<usize> = (0..self.ops.len())
            .filter(|&i| !bit(&self.mask, i))
            .filter(|&i| min_ret.is_none_or(|ret| self.ops[i].call <= ret))
            .collect();
        if self.order.len() >= self.deepest {
            self.deepest = self.order.len();
            self.stuck = candidates.clone();
        }
        // Every read-only candidate goes now, together, and no alternative is kept
        // (D-080). Taking one leaves the value alone, so the others are read-only
        // still; and it only drops an operation from the unlinearized set, which can
        // only move `min_ret` later, so the others are candidates still.
        let forced: Vec<usize> = candidates
            .iter()
            .copied()
            .filter(|&i| reads_only(self.ops[i], &self.value))
            .collect();
        if !forced.is_empty() {
            for &i in &forced {
                set(&mut self.mask, i, true);
                self.order.push(i);
            }
            let found = self.dfs();
            if found != Some(true) {
                for &i in forced.iter().rev() {
                    self.order.pop();
                    set(&mut self.mask, i, false);
                }
            }
            return found;
        }
        let (completed, pending): (Vec<usize>, Vec<usize>) = candidates
            .into_iter()
            .partition(|&i| self.ops[i].ret.is_some());
        for i in completed.into_iter().chain(pending) {
            let op = self.ops[i];
            let Some(after) = apply(&op.op, op.result.as_ref(), &self.value) else {
                continue;
            };
            set(&mut self.mask, i, true);
            self.order.push(i);
            let before = std::mem::replace(&mut self.value, after);
            match self.dfs() {
                Some(true) => return Some(true),
                Some(false) => {}
                None => return None,
            }
            self.value = before;
            self.order.pop();
            set(&mut self.mask, i, false);
        }
        Some(false)
    }
}

/// A linearization of `ops`, as positions in the order they take effect, if one
/// exists. Pending operations left out are not in it.
fn linearize(ops: &[&Op]) -> Result<Vec<usize>, Stuck> {
    let mut search = Search {
        ops,
        mask: vec![0; ops.len().div_ceil(64)],
        value: None,
        order: Vec::new(),
        seen: BTreeSet::new(),
        budget: BUDGET,
        deepest: 0,
        stuck: Vec::new(),
    };
    match search.dfs() {
        Some(true) => Ok(search.order),
        Some(false) => Err(Stuck {
            deepest: search.deepest,
            stuck: search.stuck,
            exhausted: false,
        }),
        None => Err(Stuck {
            deepest: search.deepest,
            stuck: search.stuck,
            exhausted: true,
        }),
    }
}

/// The timeline of the linearization `order`: each operation takes effect at its
/// invocation or right after the operation before it, whichever is later, which is
/// inside its window by construction.
fn timeline(ops: &[&Op], order: &[usize]) -> Timeline {
    let mut value: Option<Bytes> = None;
    let mut at = Instant::ZERO;
    let mut out = Vec::new();
    for &i in order {
        let op = ops[i];
        at = at.max(op.call);
        let after =
            apply(&op.op, op.result.as_ref(), &value).expect("the order is a linearization");
        if after != value {
            out.push((at, after.clone()));
            value = after;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ananke_env::{ApplyEffect, NodeId};
    use std::time::Duration;

    fn at(ms: u64) -> Instant {
        Instant::ZERO + Duration::from_millis(ms)
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    fn put(process: u64, call: u64, ret: Option<u64>, value: &str) -> Op {
        Op {
            process,
            seq: call,
            call: at(call),
            ret: ret.map(at),
            op: ClientOp::Put {
                key: b("k"),
                value: b(value),
            },
            result: ret.map(|_| ClientResult::Done),
        }
    }

    fn del(process: u64, call: u64, ret: Option<u64>) -> Op {
        Op {
            process,
            seq: call,
            call: at(call),
            ret: ret.map(at),
            op: ClientOp::Delete { key: b("k") },
            result: ret.map(|_| ClientResult::Done),
        }
    }

    fn get(process: u64, call: u64, ret: u64, value: Option<&str>) -> Op {
        Op {
            process,
            seq: call,
            call: at(call),
            ret: Some(at(ret)),
            op: ClientOp::Get { key: b("k") },
            result: Some(ClientResult::Value(value.map(b))),
        }
    }

    fn cas(
        process: u64,
        call: u64,
        ret: u64,
        expect: Option<&str>,
        value: &str,
        swapped: bool,
    ) -> Op {
        Op {
            process,
            seq: call,
            call: at(call),
            ret: Some(at(ret)),
            op: ClientOp::Cas {
                key: b("k"),
                expect: expect.map(b),
                value: b(value),
            },
            result: Some(ClientResult::Swapped(swapped)),
        }
    }

    fn history(ops: Vec<Op>) -> History {
        History {
            ops,
            ..History::default()
        }
    }

    #[test]
    fn a_sequential_history_is_linearizable_and_leaves_a_timeline() {
        let h = history(vec![
            put(1, 0, Some(1), "a"),
            get(1, 2, 3, Some("a")),
            cas(1, 4, 5, Some("a"), "b", true),
            get(2, 6, 7, Some("b")),
        ]);
        let timelines = check(&h).unwrap();
        assert_eq!(
            timelines[&b("k")],
            vec![(at(0), Some(b("a"))), (at(4), Some(b("b")))]
        );
    }

    #[test]
    fn a_stale_read_is_a_violation_with_the_shortest_prefix() {
        let h = history(vec![
            put(1, 0, Some(1), "a"),
            put(1, 2, Some(3), "b"),
            get(2, 4, 5, Some("a")),
            get(2, 6, 7, Some("b")),
        ]);
        let violation = check(&h).unwrap_err();
        assert_eq!(
            (violation.deepest, violation.exhausted),
            (2, false),
            "{violation}"
        );
    }

    #[test]
    fn concurrent_operations_may_take_effect_in_either_order() {
        // The put and the get overlap: the get may see either value.
        for seen in [None, Some("a")] {
            let h = history(vec![put(1, 0, Some(5), "a"), get(2, 1, 4, seen)]);
            check(&h).unwrap();
        }
    }

    #[test]
    fn a_pending_write_may_or_may_not_have_happened() {
        let seen_later = history(vec![put(1, 0, None, "a"), get(2, 5, 6, Some("a"))]);
        check(&seen_later).unwrap();
        let never_seen = history(vec![put(1, 0, None, "a"), get(2, 5, 6, None)]);
        check(&never_seen).unwrap();
        // But not both: once seen, it happened.
        let both = history(vec![
            put(1, 0, None, "a"),
            get(2, 5, 6, Some("a")),
            get(2, 7, 8, None),
        ]);
        assert!(check(&both).is_err());
    }

    #[test]
    fn a_double_apply_shows_as_a_wrong_swap() {
        let h = history(vec![
            put(1, 0, Some(1), "a"),
            cas(1, 2, 3, Some("a"), "b", true),
            cas(2, 4, 5, Some("a"), "b", true),
        ]);
        assert!(check(&h).is_err());
        let ok = history(vec![
            put(1, 0, Some(1), "a"),
            cas(1, 2, 3, Some("a"), "b", true),
            cas(2, 4, 5, Some("a"), "b", false),
        ]);
        check(&ok).unwrap();
    }

    #[test]
    fn a_value_nobody_wrote_is_a_violation() {
        let h = history(vec![put(1, 0, Some(1), "a"), get(2, 2, 3, Some("z"))]);
        assert!(check(&h).is_err());
    }

    /// A write that returned and a read wholly after it that did not see it, with
    /// nothing in between to have overwritten it: the write was lost.
    #[test]
    fn a_lost_write_is_a_violation() {
        let h = history(vec![put(1, 0, Some(1), "a"), get(2, 2, 3, None)]);
        assert!(check(&h).is_err(), "a lost write was accepted");
        // The same two with the read concurrent with the write is fine: it may have
        // taken effect after the read.
        let concurrent = history(vec![put(1, 0, Some(4), "a"), get(2, 2, 3, None)]);
        check(&concurrent).unwrap();
    }

    // The tests below pin D-080's reduction: which candidates the search may commit
    // without keeping an alternative, and that committing them hides nothing. Each
    // fails under a different way of getting [`reads_only`] wrong; the entry's
    // mutation table names which.

    /// A read is committed outright only where it saw *this* value. The same two
    /// writes with a read that matches are linearizable; with a stale one they are
    /// not, and the stale read must not be forced past the mismatch.
    #[test]
    fn a_read_is_forced_only_where_it_saw_this_value() {
        let fresh = history(vec![
            put(1, 0, Some(1), "a"),
            put(1, 2, Some(3), "b"),
            get(2, 4, 5, Some("b")),
        ]);
        check(&fresh).unwrap();
        let stale = history(vec![
            put(1, 0, Some(1), "a"),
            put(1, 2, Some(3), "b"),
            get(2, 4, 5, Some("a")),
        ]);
        assert!(
            check(&stale).is_err(),
            "a stale read was forced past its value"
        );
    }

    /// A put writes, so it is never committed as a read and its value is never
    /// dropped: the read after it sees what it wrote, and the timeline holds it.
    #[test]
    fn a_write_is_never_forced_so_its_value_is_never_lost() {
        let h = history(vec![put(1, 0, Some(1), "a"), get(2, 2, 3, Some("a"))]);
        let timelines = check(&h).unwrap();
        assert_eq!(timelines[&b("k")], vec![(at(0), Some(b("a")))]);
        // A delete writes too.
        let deleted = history(vec![
            put(1, 0, Some(1), "a"),
            del(1, 2, Some(3)),
            get(2, 4, 5, None),
        ]);
        check(&deleted).unwrap();
    }

    /// A compare-and-set that swapped writes, so it is not read-only: the read
    /// concurrent with it may go first, but the swap must still take effect for the
    /// read after it.
    #[test]
    fn a_swapping_compare_and_set_is_not_read_only() {
        let h = history(vec![
            cas(1, 0, 10, None, "b", true),
            get(2, 1, 2, None),
            get(3, 20, 21, Some("b")),
        ]);
        check(&h).unwrap();
    }

    /// A compare-and-set abandoned before its result is not read-only either, even
    /// where it would not swap at the value in hand: deferred it meets `"b"`, swaps
    /// there, and is the write the last read needs.
    #[test]
    fn an_abandoned_compare_and_set_is_not_read_only() {
        let mut abandoned = cas(1, 0, 10, Some("b"), "c", true);
        abandoned.result = None;
        let h = history(vec![
            abandoned,
            put(2, 1, Some(2), "b"),
            get(3, 20, 21, Some("c")),
        ]);
        check(&h).unwrap();
    }

    /// Committing a window of reads outright hides no violation after it: the three
    /// concurrent reads of `"a"` all go, and the read of a value nobody wrote is
    /// still caught.
    #[test]
    fn forcing_a_window_of_reads_hides_no_later_violation() {
        let window = vec![
            put(1, 0, Some(1), "a"),
            get(2, 2, 3, Some("a")),
            get(3, 2, 3, Some("a")),
            get(4, 2, 3, Some("a")),
        ];
        check(&history(window.clone())).unwrap();
        let mut with_violation = window;
        with_violation.push(get(5, 4, 5, Some("z")));
        assert!(
            check(&history(with_violation)).is_err(),
            "a window of forced reads swallowed the violation after it"
        );
    }

    /// A window wider than the mask's first word, of reads of one value, with the
    /// writes on either side still ordered and the timeline right.
    ///
    /// It is **not** evidence for D-080's reduction, and must not be read as any:
    /// it passes unchanged with the reduction removed, because a depth-first search
    /// that tries its candidates in index order walks these 80 reads without ever
    /// taking a second branch. What it pins is the mask spanning more than one word
    /// and the timeline of a window that wide. The evidence for the reduction is
    /// `seeds_3085_and_4065_which_the_nightly_found_linearize_inside_the_budget` in
    /// `sim/tests/raft.rs`, which is the only test the reduction's removal fails.
    #[test]
    fn a_wide_window_of_concurrent_reads_is_decided() {
        let mut ops = vec![put(1, 0, Some(1), "a")];
        ops.extend((0..80u64).map(|i| get(10 + i, 2, 400, Some("a"))));
        ops.push(put(1, 3, Some(401), "b"));
        ops.push(get(999, 402, 403, Some("b")));
        let timelines = check(&history(ops)).unwrap();
        assert_eq!(
            timelines[&b("k")],
            vec![(at(0), Some(b("a"))), (at(3), Some(b("b")))]
        );
    }

    /// A compare-and-set is committed as a read assertion only where its own result
    /// holds: it must be known not to have swapped **and** meet a value it would not
    /// have swapped at. The two come apart on one history, and this is it.
    ///
    /// The register is absent and the operation expected it to be absent, so it
    /// would have swapped, and it reported that it did not. Nothing can place it,
    /// and the search must not commit it outright on the strength of its result
    /// alone. Without the check on the value this history is called linearizable.
    #[test]
    fn a_compare_and_set_that_reported_no_swap_is_not_forced_where_it_would_swap() {
        let contradicted = history(vec![cas(1, 0, 1, None, "b", false)]);
        assert!(
            check(&contradicted).is_err(),
            "a compare-and-set was committed as a read at a value its own result says \
             it would have swapped at"
        );
        // Where the register holds something else it did not swap, its result holds,
        // and it is the read assertion the search may commit outright.
        let consistent = history(vec![
            put(1, 0, Some(1), "a"),
            cas(2, 2, 3, None, "b", false),
        ]);
        check(&consistent).unwrap();
    }

    /// The forced commit is undone when the search backtracks past it.
    ///
    /// Both writes are concurrent, so either may go first. Taking `"a"` first forces
    /// `"b"` next — it is the only candidate — and the two reads of `"b"` are then
    /// committed outright, and the read of `"a"` at the end cannot be placed. The
    /// search must give those two reads back and find the other order, where `"b"`
    /// goes first, the same two reads are committed, and `"a"` is written after them.
    /// The timeline is asserted rather than the verdict alone, because it is what
    /// pins the order: with the forced commit left in place on the way out, the mask
    /// and the order carry operations the surviving path never placed.
    #[test]
    fn a_forced_window_is_undone_when_the_search_backtracks_past_it() {
        let h = history(vec![
            put(1, 0, Some(10), "a"),
            put(2, 1, Some(10), "b"),
            get(3, 2, 11, Some("b")),
            get(4, 2, 11, Some("b")),
            get(5, 20, 21, Some("a")),
        ]);
        let timelines = check(&h).unwrap();
        assert_eq!(
            timelines[&b("k")],
            vec![(at(1), Some(b("b"))), (at(2), Some(b("a")))],
            "the search did not take the order that places the write of \"a\" last"
        );
    }

    /// The order a search returns is the linearization it claims to have found:
    /// every operation of the key exactly once, and a replay of it from an absent
    /// register applies at every step.
    ///
    /// Nothing outside this module reads it — `check`'s timelines are dropped at
    /// all three call sites — so a commit that marked an operation linearized
    /// without putting it in the order would be invisible everywhere else. The
    /// history mixes forced reads with writes so the forced commit's own
    /// bookkeeping is what is under test.
    #[test]
    fn the_order_a_search_returns_replays_under_the_specification() {
        let ops = vec![
            put(1, 0, Some(1), "a"),
            get(2, 2, 3, Some("a")),
            get(3, 2, 3, Some("a")),
            get(4, 2, 3, Some("a")),
            cas(5, 4, 5, Some("a"), "b", true),
            get(6, 6, 7, Some("b")),
            cas(7, 8, 9, Some("a"), "c", false),
            del(8, 10, Some(11)),
            get(9, 12, 13, None),
        ];
        let refs: Vec<&Op> = ops.iter().collect();
        let Ok(order) = linearize(&refs) else {
            panic!("the history is linearizable, but the search did not place it")
        };
        assert_eq!(
            order.len(),
            ops.len(),
            "the order does not hold every operation: {order:?}"
        );
        let mut placed = vec![false; ops.len()];
        let mut value: Option<Bytes> = None;
        for &i in &order {
            assert!(!placed[i], "operation {i} is in the order twice: {order:?}");
            placed[i] = true;
            value = apply(&refs[i].op, refs[i].result.as_ref(), &value).unwrap_or_else(|| {
                panic!("the order does not replay: operation {i} does not apply at {value:?}")
            });
        }
        assert!(
            placed.iter().all(|&p| p),
            "an operation is missing from the order: {order:?}"
        );
    }

    /// A trace of two ranges, with a write proposed in one of them and never
    /// applied there, and the other range's entry at the same (index, term)
    /// applied: the record the closure keyed by (index, term) alone would close
    /// the write at (SHARD.md §9).
    ///
    /// Every event of every sweep in this tree carries one range, so the two
    /// keyings say the same thing on every seed; this is the history that tells
    /// them apart. `applied_in` is the range whose apply is traced.
    fn two_range_trace(applied_in: u64) -> Vec<TraceRecord> {
        let record = |ms, event| TraceRecord {
            at: at(ms),
            decided: at(ms),
            node: Some(NodeId::new(1)),
            event,
        };
        vec![
            record(
                0,
                TraceEvent::ClientInvoke {
                    client: 1,
                    seq: 0,
                    op: ClientOp::Put {
                        key: b("k"),
                        value: b("a"),
                    },
                },
            ),
            record(
                10,
                TraceEvent::RaftProposed {
                    server: 1,
                    range: 2,
                    client: 1,
                    seq: 0,
                    index: 5,
                    term: 2,
                },
            ),
            record(
                50,
                TraceEvent::RaftApply {
                    server: 1,
                    range: applied_in,
                    index: 5,
                    entry_term: 2,
                    hash: 0xaa,
                    key: Some(b("k")),
                    effect: ApplyEffect::Applied,
                },
            ),
            record(
                100,
                TraceEvent::ClientInvoke {
                    client: 2,
                    seq: 0,
                    op: ClientOp::Get { key: b("k") },
                },
            ),
            record(
                110,
                TraceEvent::ClientReturn {
                    client: 2,
                    seq: 0,
                    result: ClientResult::Value(None),
                },
            ),
        ]
    }

    /// The write was proposed in range 2 and only range 3's entry at (5, 2)
    /// applied, so the write never took effect: it stays pending, and a later read
    /// seeing nothing is linearizable. A closure keyed by (index, term) alone would
    /// close it at range 3's apply, force it before the read, and report a
    /// violation of nothing.
    #[test]
    fn an_operations_proposal_is_closed_only_by_its_own_ranges_apply() {
        let history = History::from_trace(&two_range_trace(3));
        assert_eq!((history.closed_by_apply, history.pending()), (0, 1));
        check(&history).unwrap();
    }

    /// And the apply of the range it was proposed in closes it, as ever: the write
    /// took effect before the read, which saw nothing, and that is the violation.
    #[test]
    fn an_operations_proposal_is_closed_by_its_own_ranges_apply() {
        let history = History::from_trace(&two_range_trace(2));
        assert_eq!((history.closed_by_apply, history.pending()), (1, 0));
        assert!(check(&history).is_err(), "{history:?}");
    }

    #[test]
    fn keys_are_independent() {
        let mut other = put(3, 0, Some(1), "x");
        other.op = ClientOp::Put {
            key: b("other"),
            value: b("x"),
        };
        let h = history(vec![
            other,
            put(1, 2, Some(3), "a"),
            get(2, 4, 5, Some("a")),
        ]);
        let timelines = check(&h).unwrap();
        assert_eq!(timelines.len(), 2);
    }
}
