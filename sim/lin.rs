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
//! Single-key operations partition by key, since the store is a product of
//! independent registers, so each key is searched on its own: a state is the set of
//! operations linearized so far and the register's value, and a state seen once is
//! not searched twice. The search has a budget of states per key; a key that
//! exhausts it is reported as such, distinct from a violation, and the correct
//! server must never reach it. A violation names the key, how far the search got,
//! and the operations it could not place.
//!
//! Each key's search leaves a timeline of (time, value) for the linearization it
//! found, so a multi-key read can later be checked against every key it covers.

use std::collections::{BTreeMap, BTreeSet};

use ananke_env::sim::TraceRecord;
use ananke_env::{ClientOp, ClientResult, Instant, TraceEvent};
use bytes::Bytes;

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
        let mut proposed: BTreeMap<(u64, u64), Vec<(u64, u64)>> = BTreeMap::new();
        let mut applied_at: BTreeMap<(u64, u64), Instant> = BTreeMap::new();
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
                    client,
                    seq,
                    index,
                    term,
                    ..
                } => {
                    proposed
                        .entry((*client, *seq))
                        .or_default()
                        .push((*index, *term));
                }
                TraceEvent::RaftApply {
                    index, entry_term, ..
                } => {
                    applied_at.entry((*index, *entry_term)).or_insert(record.at);
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
