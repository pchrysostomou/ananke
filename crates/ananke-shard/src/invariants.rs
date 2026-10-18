//! SHARD.md §8's checks 7, 8, 9, 10, 16, 17 and 18 as folds over the trace, in the
//! range layer where Q40 puts checks 7 to 22: descriptor agreement, one owner per key on
//! a node, serving within span, owners' generations rising, meta never going back, a
//! client refreshing on a mismatch, and the lineage of range ids and splits. Each is a
//! fold fed one record at a time, with a verdict readable at any prefix, so one
//! [`Checker`] runs the eight — check 18 is two, [`IdsUnique`] and [`SplitLineage`] —
//! incrementally beside `ananke_raft::invariants`'s and again over the whole trace at
//! the end (D-046; §8).
//!
//! **Check 7** holds a map from (range, index) to the descriptor in force from that
//! index: a `RangeCreated` of a bootstrap or a split is the value at (range,
//! `floor_index`), a `RangeDescriptor` at an apply the value at (range, `index`), and
//! a second value for one (range, index) is a violation. A restatement at a node's
//! start and an install — its `RangeCreated` or its `RangeDescriptor` — are
//! statements: each carries an index and must equal the map's latest value at or below
//! it, judged when it is traced and again at every later value of its range at or
//! below that index (PROPOSED D-107). Every other check reads "the descriptor in force
//! before an index" off this map: the latest value strictly below it.
//!
//! **Check 8**, [`OneOwnerPerKey`], Stage C's two clauses: no two of a node's spans
//! overlap, and a voter of P at a split that applied it holds R at every restatement
//! after (PROPOSED D-107). The second clause is folded and not in the [`Checker`]'s
//! verdict: the correct node trips it until placeholders exist.
//!
//! **Check 9**: a `RaftApply` whose effect is `applied` and whose key lies outside the
//! span of its range's descriptor in force before its index, or whose descriptor is
//! subsumed, is a violation; so is one whose effect is `out_of_span` for a key inside
//! it; a `RaftRead` whose key lies outside the descriptor at its `applied`, or on a
//! subsumed one, is a violation.
//!
//! **Check 10**: the first applied write of each (range, index) is folded with the
//! generation of its range's descriptor in force before that index, into a map from
//! key to the (generation, range) that last wrote it; a write at a lower generation is
//! a violation, and so is a write by a different range at a generation not above the
//! recorded one — sibling halves share `g + 1`, so without the range a left half
//! writing a key its right sibling had written would pass.
//!
//! **Check 16**: every descriptor a meta apply names — range, start, end, generation
//! and voters — equals the value check 7 holds for its range at its generation, traced
//! before the meta apply; and per key, over the sub-intervals each apply won, the
//! generation meta names never falls from one meta index to the next. A read served by
//! range 0 or range 1 is a lookup, which any key may ask (Q36), and check 9 holds it to
//! nothing: check 16 and the convergence bound are what hold meta to account.
//!
//! **Check 17**: a `ClientSend` for an operation, after a `ClientMismatch` for it that
//! named a descriptor of generation G whose span contains the operation's key, to a
//! range at a generation below G, is a violation. The send that was refused is tied to
//! its operation through the `invoked` its own `ClientSend` carried.
//!
//! **Check 18**, two folds: [`IdsUnique`], the range-id clause over `RangeIdsLeased`
//! (PROPOSED D-099), and [`SplitLineage`], the split clause at each split's first
//! apply, which also holds a right half to a block granted to the node that led its
//! parent (PROPOSED D-100).
//!
//! The keys the trace carries are user keys; the spans are encoded (§1), so every
//! comparison encodes the key as the store does (`ananke_raft::apply::user_key`).
// PROPOSED(D-097): checks 7, 9, 10 and 17.
// PROPOSED(D-098): check 16, and check 9's lookups.
// PROPOSED(D-107): check 7's restatement clause, and check 8.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use ananke_env::sim::TraceRecord;
use ananke_env::{ApplyEffect, ClientOp, RangeCause, RangeState, TraceEvent};
use ananke_raft::apply::user_key;
use bytes::Bytes;

use crate::system::{META_RANGE, ROOT_RANGE};

/// A descriptor as check 7 holds it: what a creation or a `RangeDescriptor` carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held {
    /// The span's first key, encoded.
    pub start: Bytes,
    /// The key past the span's last, encoded.
    pub end: Bytes,
    /// The generation.
    pub generation: u64,
    /// The voters.
    pub voters: Vec<u64>,
    /// The state.
    pub state: RangeState,
}

impl Held {
    /// Whether the encoded `key` lies in the span.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start[..] <= *key && *key < self.end[..]
    }

    /// Whether the range serves the encoded `key`: in the span and not subsumed
    /// (SHARD.md §3).
    #[must_use]
    pub fn serves(&self, key: &[u8]) -> bool {
        self.contains(key) && self.state != RangeState::Subsumed
    }
}

/// Check 7's map: per range, the descriptor in force from each index it changed at.
#[derive(Clone, Debug, Default)]
pub struct Descriptors {
    by_range: BTreeMap<u64, BTreeMap<u64, Held>>,
    /// The first value traced for each (range, generation): what a lookup by
    /// generation takes (SHARD.md §8, check 7).
    // PROPOSED(D-098)
    by_generation: BTreeMap<(u64, u64), Held>,
    /// Each replica's applied index as its node last traced it, by (node, range):
    /// a `RaftApply`'s index, a `RaftRecovered`'s `applied`, a creation's floor. A
    /// `RangeDescriptor` at or below it states a value the replica holds at an index
    /// it did not reach by that apply — a restatement or an install — and one above
    /// it is the apply's own change, traced before the apply's `RaftApply` (§8's fixed
    /// order).
    // PROPOSED(D-107)
    applied: BTreeMap<(u64, u64), u64>,
    /// The nodes that closed their restatement (`RangesRestated`) since their last
    /// crash: a statement on any other node is a restatement, which only names it.
    // PROPOSED(D-107)
    restated: BTreeSet<u64>,
    /// Every statement a value is held to, per range: a restatement and an install,
    /// each at its index. Judged when it is traced and again at every later value of
    /// its range, so a value traced after it at or below its index is held to it too.
    // PROPOSED(D-107)
    statements: BTreeMap<u64, Vec<Statement>>,
    /// How many statements were folded.
    stated: usize,
    violation: Option<String>,
}

/// A descriptor a replica stated at an index without changing the range's: a
/// restatement at its node's start, or an install (SHARD.md §8, check 7).
// PROPOSED(D-107)
#[derive(Clone, Debug)]
struct Statement {
    index: u64,
    held: Held,
    node: Option<u64>,
    what: &'static str,
}

impl Descriptors {
    /// The descriptor in force before `index` for `range`: the latest value at an index
    /// strictly below it.
    #[must_use]
    pub fn before(&self, range: u64, index: u64) -> Option<&Held> {
        self.by_range
            .get(&range)?
            .range(..index)
            .next_back()
            .map(|(_, held)| held)
    }

    /// The descriptor in force at `index` for `range`: the latest value at or below it.
    #[must_use]
    pub fn at(&self, range: u64, index: u64) -> Option<&Held> {
        self.by_range
            .get(&range)?
            .range(..=index)
            .next_back()
            .map(|(_, held)| held)
    }

    /// Whether any descriptor of any range has been folded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_range.is_empty()
    }

    /// How many statements — restatements and installs — have been folded, the
    /// first value traced at their index among them.
    // PROPOSED(D-107)
    #[must_use]
    pub fn statements(&self) -> usize {
        self.stated
    }

    /// The first value traced for `range` at `generation`.
    // PROPOSED(D-098)
    #[must_use]
    pub fn first_at_generation(&self, range: u64, generation: u64) -> Option<&Held> {
        self.by_generation.get(&(range, generation))
    }

    fn put(&mut self, range: u64, index: u64, held: Held, node: Option<u64>, what: &str) {
        let at = self.by_range.entry(range).or_default();
        match at.get(&index) {
            None => {
                self.by_generation
                    .entry((range, held.generation))
                    .or_insert_with(|| held.clone());
                at.insert(index, held);
                self.judge(range);
            }
            Some(agreed) if *agreed == held => {}
            Some(agreed) => {
                if self.violation.is_none() {
                    self.violation = Some(format!(
                        "descriptor agreement: node {node:?}'s replica of range {range} was \
                         {what} at index {index} with {held:?}, where the range's descriptor \
                         there is {agreed:?}"
                    ));
                }
            }
        }
    }

    /// A value a replica states at `index` without changing the range's: it must
    /// equal the map's latest value at or below `index` (SHARD.md §8, check 7).
    ///
    /// "Latest" is read at whatever point of the trace the two are both traced, in
    /// either order, and a statement is never a value of the map itself. One whose
    /// range holds no value at or below its index yet — a replica whose node crashed
    /// between a split's durable batch and its trace restates the right half before
    /// any replica has traced its creation — waits for one and is judged when it
    /// lands; it stays unjudged while none does. A statement ahead of every value at
    /// or below its index — of a later generation than the latest — waits the same way
    /// for the change it holds, and is a violation as soon as the range takes its
    /// generation past its index. Any other difference is a violation where it is
    /// seen. Until #166's review a statement with no value under it was put as the
    /// value there, judged against nothing, and made the first value of its
    /// generation, which check 16 reads (PROPOSED D-107).
    // PROPOSED(D-107): check 7's restatement clause.
    fn state(&mut self, range: u64, index: u64, held: Held, node: Option<u64>, what: &'static str) {
        self.stated += 1;
        self.statements.entry(range).or_default().push(Statement {
            index,
            held,
            node,
            what,
        });
        self.judge(range);
    }

    /// Judges every statement of `range` against the map as it stands.
    // PROPOSED(D-107)
    fn judge(&mut self, range: u64) {
        if self.violation.is_some() {
            return;
        }
        let (Some(statements), Some(values)) =
            (self.statements.get(&range), self.by_range.get(&range))
        else {
            return;
        };
        let mut violation = None;
        for statement in statements {
            let Some((_, agreed)) = values.range(..=statement.index).next_back() else {
                continue;
            };
            if *agreed == statement.held {
                continue;
            }
            let (index, held, node, what) = (
                statement.index,
                &statement.held,
                statement.node,
                statement.what,
            );
            if held.generation > agreed.generation {
                if let Some((taken, _)) = values
                    .range((Excluded(index), Unbounded))
                    .find(|(_, value)| value.generation == held.generation)
                {
                    violation = Some(format!(
                        "descriptor agreement: node {node:?}'s replica of range {range} was \
                         {what} at index {index} with {held:?}, a generation the range took \
                         only at index {taken}: a descriptor its applied index cannot hold"
                    ));
                    break;
                }
                continue;
            }
            violation = Some(format!(
                "descriptor agreement: node {node:?}'s replica of range {range} was {what} at \
                 index {index} with {held:?}, where the range's descriptor there is {agreed:?}"
            ));
            break;
        }
        if violation.is_some() {
            self.violation = violation;
        }
    }

    /// Folds one record.
    pub fn push(&mut self, record: &TraceRecord) {
        let node = record.node.map(|node| u64::from(node.get()));
        match &record.event {
            TraceEvent::RangeCreated {
                range,
                cause,
                start,
                end,
                generation,
                voters,
                floor_index,
                ..
            } => {
                if let Some(node) = node {
                    self.applied.insert((node, *range), *floor_index);
                }
                let held = Held {
                    start: start.clone(),
                    end: end.clone(),
                    generation: *generation,
                    voters: voters.clone(),
                    state: RangeState::Live,
                };
                match cause {
                    RangeCause::Bootstrap | RangeCause::Split => {
                        self.put(*range, *floor_index, held, node, "created");
                    }
                    // An install: a statement at its floor.
                    _ => self.state(*range, *floor_index, held, node, "installed"),
                }
            }
            TraceEvent::RangeDescriptor {
                range,
                index,
                applied,
                start,
                end,
                generation,
                voters,
                state,
            } => {
                let held = Held {
                    start: start.clone(),
                    end: end.clone(),
                    generation: *generation,
                    voters: voters.clone(),
                    state: *state,
                };
                // A statement — the restatement at a node's start, or an install onto
                // an initialised replica — follows the `RaftRecovered` that states the
                // applied index it is read at; an apply's change comes before the
                // apply it belongs to (PROPOSED D-107).
                let stated = node
                    .and_then(|node| self.applied.get(&(node, *range)))
                    .is_some_and(|known| *applied <= *known);
                if stated {
                    let restating = node.is_some_and(|node| !self.restated.contains(&node));
                    let what = if restating { "restated" } else { "installed" };
                    self.state(*range, *applied, held, node, what);
                } else {
                    self.put(*range, *index, held, node, "changed");
                }
            }
            TraceEvent::RaftApply { range, index, .. } => {
                if let Some(node) = node {
                    self.applied.insert((node, *range), *index);
                }
            }
            TraceEvent::RaftRecovered { range, applied, .. } => {
                if let Some(node) = node {
                    self.applied.insert((node, *range), *applied);
                }
            }
            TraceEvent::RangesRestated { .. } => {
                if let Some(node) = node {
                    self.restated.insert(node);
                }
            }
            TraceEvent::NodeCrashed { .. } => {
                if let Some(node) = node {
                    self.restated.remove(&node);
                }
            }
            _ => {}
        }
    }

    /// The first violation, if one was folded.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.violation.clone().map_or(Ok(()), Err)
    }
}

/// Check 8, one owner per key on a node (SHARD.md §8), Stage C's two clauses, an
/// interval map per node folded from `RangeCreated`, `RangeDescriptor`,
/// `RangeRemoved`, `RangesRestated`, `NodeCrashed` and the node's `RaftRefused`.
///
/// **The overlap clause**: per node, the spans of its initialised replicas — every
/// creation, every descriptor applied, installed or restated, until the replica's
/// `RangeRemoved` — and after every event that changes one, taken in §8's fixed
/// orders, no two of a node's spans overlap. A crash forgets the node's spans and its
/// restatement states them again; a `RaftRefused` forgets them for good.
///
/// **The right half's clause**: a node that is a voter of P's configuration at `s`
/// — R's `RangeCreated { cause: split }` carries those voters — whose replica of P has
/// applied `s`, traced as `RangeSplit`, as a `RaftApply` of `s` with effect `took`, or
/// restated at an applied index at or past `s`, holds R initialised from then until a
/// `RangeRemoved` of R or a `RaftRefused` on that node: at the `RaftApply` of `s`
/// (R's creation precedes it, §8's fixed order) and at every `RangesRestated` after
/// it, R is among the node's spans. The clause for a node that is not a voter there,
/// that it traces no creation of R, is Stage D's (SHARD.md §12).
///
/// Each clause keeps its own verdict ([`OneOwnerPerKey::overlap_verdict`],
/// [`OneOwnerPerKey::right_half_verdict`]), so a scenario can read them apart.
// PROPOSED(D-107): check 8's two Stage C clauses.
#[derive(Clone, Debug, Default)]
pub struct OneOwnerPerKey {
    /// Per node, the span of every initialised replica it holds.
    spans: BTreeMap<u64, BTreeMap<u64, (Bytes, Bytes)>>,
    /// The nodes that closed their restatement since their last crash.
    restated: BTreeSet<u64>,
    /// The server each node is, from the `RaftRecovered` its restatement traces.
    servers: BTreeMap<u64, u64>,
    /// Every split that took effect, by (parent, index): the right half and the
    /// voters of the parent's configuration there, off R's first creation.
    splits: BTreeMap<(u64, u64), (u64, Vec<u64>)>,
    /// Per node, the splits its replica of the parent applied: (parent, index).
    applied_splits: BTreeMap<u64, BTreeSet<(u64, u64)>>,
    /// Per node, the right halves it must hold: right to (parent, index).
    owed: BTreeMap<u64, BTreeMap<u64, (u64, u64)>>,
    overlap: Option<String>,
    right_half: Option<String>,
}

impl OneOwnerPerKey {
    fn server_of(&self, node: u64) -> u64 {
        self.servers.get(&node).copied().unwrap_or(node)
    }

    /// Records that `node`'s replica of `parent` applied the split at `index`, and
    /// what it then owes, where the split is known.
    fn applied_split(&mut self, node: u64, parent: u64, index: u64) {
        self.applied_splits
            .entry(node)
            .or_default()
            .insert((parent, index));
        self.owe(node, parent, index);
    }

    fn owe(&mut self, node: u64, parent: u64, index: u64) {
        let server = self.server_of(node);
        if let Some((right, voters)) = self.splits.get(&(parent, index))
            && voters.contains(&server)
        {
            self.owed
                .entry(node)
                .or_default()
                .insert(*right, (parent, index));
        }
    }

    /// `node` holds `range` over `[start, end)` from here: no other of its spans may
    /// overlap it.
    fn hold(&mut self, node: u64, range: u64, start: &Bytes, end: &Bytes) {
        let held = self.spans.entry(node).or_default();
        held.insert(range, (start.clone(), end.clone()));
        if self.overlap.is_some() {
            return;
        }
        if let Some((other, (from, to))) = held
            .iter()
            .find(|(other, (from, to))| **other != range && from < end && start < to)
        {
            self.overlap = Some(format!(
                "one owner per key: node {node}'s replica of range {range} over [{start:?}, \
                 {end:?}) overlaps its replica of range {other} over [{from:?}, {to:?}) \
                 (SHARD.md §5, §8: no two of a node's spans overlap)"
            ));
        }
    }

    /// The first right half `node` owes and does not hold, as a violation.
    fn owed_and_missing(&self, node: u64, at: &str) -> Option<String> {
        let held = self.spans.get(&node);
        self.owed
            .get(&node)?
            .iter()
            .find_map(|(right, (parent, index))| {
                (!held.is_some_and(|held| held.contains_key(right))).then(|| {
                    format!(
                        "one owner per key: node {node} is a voter of range {parent}'s \
                     configuration at its split at index {index}, its replica of range \
                     {parent} has applied it, and {at} it holds no replica of the right \
                     half {right} (SHARD.md §8, check 8)"
                    )
                })
            })
    }

    /// Folds one record.
    pub fn push(&mut self, record: &TraceRecord) {
        let Some(node) = record.node.map(|node| u64::from(node.get())) else {
            return;
        };
        match &record.event {
            TraceEvent::RaftRecovered { server, .. } => {
                self.servers.insert(node, *server);
            }
            TraceEvent::RangeCreated {
                range,
                cause,
                parent,
                start,
                end,
                voters,
                floor_index,
                ..
            } => {
                self.hold(node, *range, start, end);
                if *cause == RangeCause::Split
                    && let Some(parent) = parent
                    && !self.splits.contains_key(&(*parent, *floor_index))
                {
                    self.splits
                        .insert((*parent, *floor_index), (*range, voters.clone()));
                    let applied: Vec<u64> = self
                        .applied_splits
                        .iter()
                        .filter(|(_, splits)| splits.contains(&(*parent, *floor_index)))
                        .map(|(node, _)| *node)
                        .collect();
                    for node in applied {
                        self.owe(node, *parent, *floor_index);
                    }
                }
            }
            TraceEvent::RangeDescriptor {
                range,
                applied,
                start,
                end,
                ..
            } => {
                self.hold(node, *range, start, end);
                // A restatement of P at an applied index at or past a split of P.
                if !self.restated.contains(&node) {
                    let splits: Vec<u64> = self
                        .splits
                        .range((*range, 0)..=(*range, *applied))
                        .map(|((_, index), _)| *index)
                        .collect();
                    for index in splits {
                        self.applied_split(node, *range, index);
                    }
                }
            }
            TraceEvent::RangeSplit { range, index, .. } => {
                self.applied_split(node, *range, *index);
            }
            TraceEvent::RaftApply {
                range,
                index,
                effect: ApplyEffect::Took,
                ..
            } if self.splits.contains_key(&(*range, *index)) => {
                self.applied_split(node, *range, *index);
                if self.right_half.is_none() {
                    self.right_half =
                        self.owed_and_missing(node, &format!("at its apply of {index}"));
                }
            }
            TraceEvent::RangeRemoved { range, .. } => {
                if let Some(held) = self.spans.get_mut(&node) {
                    held.remove(range);
                }
                if let Some(owed) = self.owed.get_mut(&node) {
                    owed.remove(range);
                }
            }
            TraceEvent::NodeCrashed { .. } => {
                self.spans.remove(&node);
                self.restated.remove(&node);
            }
            TraceEvent::RaftRefused { .. } => {
                self.spans.remove(&node);
                self.owed.remove(&node);
                self.applied_splits.remove(&node);
            }
            TraceEvent::RangesRestated { .. } => {
                self.restated.insert(node);
                if self.right_half.is_none() {
                    self.right_half = self.owed_and_missing(node, "at its restatement");
                }
            }
            _ => {}
        }
    }

    /// The overlap clause's first violation.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn overlap_verdict(&self) -> Result<(), String> {
        self.overlap.clone().map_or(Ok(()), Err)
    }

    /// The right half's clause's first violation.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn right_half_verdict(&self) -> Result<(), String> {
        self.right_half.clone().map_or(Ok(()), Err)
    }

    /// Both clauses' first violation, the overlap clause's first.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.overlap_verdict()?;
        self.right_half_verdict()
    }
}

/// Check 9, serving within span, over check 7's map.
#[derive(Clone, Debug, Default)]
pub struct ServingWithinSpan {
    violation: Option<String>,
}

impl ServingWithinSpan {
    /// Folds one record against `descriptors` as they stand.
    pub fn push(&mut self, record: &TraceRecord, descriptors: &Descriptors) {
        if self.violation.is_some() {
            return;
        }
        match &record.event {
            TraceEvent::RaftApply {
                server,
                range,
                index,
                key: Some(key),
                effect,
                ..
            } => {
                let encoded = user_key(key);
                let Some(held) = descriptors.before(*range, *index) else {
                    self.violation = Some(format!(
                        "serving within span: server {server} applied index {index} of range \
                         {range} for key {key:?} with no descriptor in force before it"
                    ));
                    return;
                };
                match effect {
                    ApplyEffect::Applied if !held.serves(&encoded) => {
                        self.violation = Some(format!(
                            "serving within span: server {server} applied index {index} of \
                             range {range} for key {key:?}, which its descriptor in force \
                             there ({held:?}) does not serve (SHARD.md §3, at apply)"
                        ));
                    }
                    ApplyEffect::OutOfSpan if held.serves(&encoded) => {
                        self.violation = Some(format!(
                            "serving within span: server {server} refused index {index} of \
                             range {range} for key {key:?} as out of span, which its \
                             descriptor in force there ({held:?}) serves"
                        ));
                    }
                    _ => {}
                }
            }
            // A read served by the root or the meta range is a lookup, which any key
            // may ask (SHARD.md §1, §3; Q36): check 16 holds meta to account.
            // PROPOSED(D-098)
            TraceEvent::RaftRead { range, .. }
                if *range == ROOT_RANGE.get() || *range == META_RANGE.get() => {}
            TraceEvent::RaftRead {
                server,
                range,
                key,
                applied,
                ..
            } => {
                let encoded = user_key(key);
                match descriptors.at(*range, *applied) {
                    Some(held) if held.serves(&encoded) => {}
                    held => {
                        self.violation = Some(format!(
                            "serving within span: server {server} served a read of {key:?} \
                             from range {range} at applied index {applied}, whose descriptor \
                             there ({held:?}) does not serve it (SHARD.md §3, at serving)"
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    /// The first violation, if one was folded.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.violation.clone().map_or(Ok(()), Err)
    }
}

/// Check 10, owners' generations rise, over check 7's map.
#[derive(Clone, Debug, Default)]
pub struct GenerationsRise {
    /// Each (range, index) whose first applied write has been folded.
    seen: BTreeMap<(u64, u64), ()>,
    /// Per key, the (generation, range) that last wrote it.
    last: BTreeMap<Bytes, (u64, u64)>,
    violation: Option<String>,
}

impl GenerationsRise {
    /// Folds one record against `descriptors` as they stand.
    pub fn push(&mut self, record: &TraceRecord, descriptors: &Descriptors) {
        if self.violation.is_some() {
            return;
        }
        let TraceEvent::RaftApply {
            range,
            index,
            key: Some(key),
            effect: ApplyEffect::Applied,
            ..
        } = &record.event
        else {
            return;
        };
        if self.seen.insert((*range, *index), ()).is_some() {
            return;
        }
        let Some(held) = descriptors.before(*range, *index) else {
            self.violation = Some(format!(
                "owners' generations: index {index} of range {range} wrote {key:?} with no \
                 descriptor in force before it"
            ));
            return;
        };
        let generation = held.generation;
        match self.last.get(key) {
            Some(&(last, by)) if generation < last || (by != *range && generation <= last) => {
                self.violation = Some(format!(
                    "owners' generations: range {range} wrote {key:?} at index {index} at \
                     generation {generation}, after range {by} wrote it at generation {last}: \
                     along the ownership of a key generations strictly rise (SHARD.md §1)"
                ));
            }
            _ => {
                self.last.insert(key.clone(), (generation, *range));
            }
        }
    }

    /// The first violation, if one was folded.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.violation.clone().map_or(Ok(()), Err)
    }
}

/// Check 18's range-id clause, the part that needs no split (SHARD.md §8; §5, Q17):
/// folding `RangeIdsLeased` by the first apply of each of range 0's indices, no two
/// grants share an id, and every replica's apply of an index grants what its first
/// apply did. The clause's other half — the right half of a split lies in a block
/// granted before it to the node that led the parent — is [`SplitLineage`]'s.
// PROPOSED(D-099)
#[derive(Clone, Debug, Default)]
pub struct IdsUnique {
    /// The grant at each index of range 0 by its first apply: node, run, first, last.
    granted: BTreeMap<u64, (u64, u64, u64, u64)>,
    /// Every block granted, its first id to its last and the node it went to.
    blocks: BTreeMap<u64, (u64, u64)>,
    violation: Option<String>,
}

impl IdsUnique {
    /// Folds one record.
    pub fn push(&mut self, record: &TraceRecord) {
        if self.violation.is_some() {
            return;
        }
        let TraceEvent::RangeIdsLeased {
            node,
            run,
            first,
            last,
            index,
        } = &record.event
        else {
            return;
        };
        let on = record.node.map(|node| u64::from(node.get()));
        let grant = (*node, *run, *first, *last);
        if let Some(seen) = self.granted.get(index) {
            if *seen != grant {
                self.violation = Some(format!(
                    "range ids: index {index} of range 0 granted node {}'s run {} the block \
                     {}..={} at its first apply and node {node}'s run {run} the block \
                     {first}..={last} on {on:?}",
                    seen.0, seen.1, seen.2, seen.3
                ));
            }
            return;
        }
        self.granted.insert(*index, grant);
        if first > last {
            self.violation = Some(format!(
                "range ids: index {index} of range 0 granted node {node} an empty block \
                 {first}..={last}"
            ));
            return;
        }
        if let Some((held_first, (held_last, _))) = self.blocks.range(..=*last).next_back()
            && *held_last >= *first
        {
            self.violation = Some(format!(
                "range ids: the block {first}..={last} granted to node {node} at index {index} \
                 of range 0 shares ids with the block {held_first}..={held_last} granted before \
                 it (SHARD.md §5: blocks are disjoint)"
            ));
            return;
        }
        self.blocks.insert(*first, (*last, *node));
    }

    /// The blocks granted so far, by first apply: first id to (last id, node).
    #[must_use]
    pub fn blocks(&self) -> &BTreeMap<u64, (u64, u64)> {
        &self.blocks
    }

    /// The node the block holding `id` was granted to, if a block holds it.
    #[must_use]
    pub fn holder_of(&self, id: u64) -> Option<u64> {
        self.blocks
            .range(..=id)
            .next_back()
            .filter(|(_, (last, _))| *last >= id)
            .map(|(_, (_, node))| *node)
    }

    /// The first violation.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.violation.clone().map_or(Ok(()), Err)
    }
}

/// Check 18's split clause (SHARD.md §8; §5), at the first apply of each split: with
/// P's descriptor before `s` `[x, y)` at g, P's value at `s` is `[x, k)` at g + 1 with
/// P's voters; R's `RangeCreated { cause: split }` is `[k, y)` at g + 1 with the voters
/// of P's plain configuration in force at `s`, floor index `s` and floor term 1 (Q20);
/// R is fresh — no `RangeCreated` of R and no `RangeSplit` naming R before that split's
/// first `RangeSplit` — and R lies in a block a `RangeIdsLeased` traced before it
/// granted to the node that led P in the split entry's term (Q17). Every replica's
/// `RangeSplit` of (P, `s`) names the same right half and key.
// PROPOSED(D-100)
#[derive(Clone, Debug, Default)]
pub struct SplitLineage {
    /// Each range's configuration entries by index: whether joint, and the voters.
    configs: BTreeMap<u64, BTreeMap<u64, (bool, Vec<u64>)>>,
    /// The leader of each (range, term).
    leaders: BTreeMap<(u64, u64), u64>,
    /// Ranges a `RangeCreated` or a `RangeSplit` has named.
    named: BTreeSet<u64>,
    /// The first `RangeSplit` of each (parent, index): the right half and the key.
    splits: BTreeMap<(u64, u64), (u64, Bytes)>,
    /// Splits whose values await the first `RaftApply` of (parent, index), which
    /// carries the split entry's term: (parent, index) to the right half.
    pending: BTreeMap<(u64, u64), u64>,
    /// The right halves' creations seen: right to (parent, floor index, floor term).
    creations: BTreeMap<u64, (Option<u64>, u64, u64)>,
    violation: Option<String>,
}

impl SplitLineage {
    fn fail(&mut self, what: String) {
        if self.violation.is_none() {
            self.violation = Some(format!("descriptor lineage: {what}"));
        }
    }

    /// The voters of `range`'s latest configuration at or below `index`, and whether
    /// it is joint.
    fn config_at(&self, range: u64, index: u64) -> Option<(bool, Vec<u64>)> {
        self.configs
            .get(&range)?
            .range(..=index)
            .next_back()
            .map(|(_, config)| config.clone())
    }

    /// Folds one record against check 7's map and check 18's range-id clause as they
    /// stand.
    pub fn push(&mut self, record: &TraceRecord, descriptors: &Descriptors, ids: &IdsUnique) {
        if self.violation.is_some() {
            return;
        }
        let node = record.node.map(|node| u64::from(node.get()));
        match &record.event {
            TraceEvent::RaftConfig {
                range,
                index,
                old,
                joint,
                ..
            } => {
                self.configs
                    .entry(*range)
                    .or_default()
                    .entry(*index)
                    .or_insert_with(|| (*joint, old.clone()));
            }
            TraceEvent::RaftLeader {
                server,
                range,
                term,
                ..
            } => {
                self.leaders.entry((*range, *term)).or_insert(*server);
            }
            TraceEvent::RangeCreated {
                range,
                cause,
                parent,
                floor_index,
                floor_term,
                ..
            } => {
                if *cause == RangeCause::Split {
                    self.creations
                        .entry(*range)
                        .or_insert((*parent, *floor_index, *floor_term));
                } else {
                    self.named.insert(*range);
                }
            }
            TraceEvent::RangeSplit {
                range,
                right,
                key,
                index,
            } => {
                if let Some((agreed_right, agreed_key)) = self.splits.get(&(*range, *index)) {
                    if (*agreed_right, agreed_key) != (*right, key) {
                        self.fail(format!(
                            "node {node:?}'s replica of range {range} split at index {index} \
                             with right half {right} at {key:?}, where the first apply of that \
                             split had right half {agreed_right} at {agreed_key:?}"
                        ));
                    }
                    return;
                }
                if self.named.contains(right) {
                    self.fail(format!(
                        "range {range}'s split at index {index} names right half {right}, \
                         which a creation or a split had named before it (SHARD.md §5, Q17: \
                         the right half's id is fresh)"
                    ));
                    return;
                }
                self.named.insert(*range);
                self.named.insert(*right);
                self.splits.insert((*range, *index), (*right, key.clone()));
                self.pending.insert((*range, *index), *right);
            }
            TraceEvent::RaftApply {
                range,
                index,
                entry_term,
                effect,
                ..
            } => {
                let Some(right) = self.pending.remove(&(*range, *index)) else {
                    return;
                };
                if *effect != ApplyEffect::Took {
                    self.fail(format!(
                        "range {range}'s split at index {index} was traced and its apply's \
                         effect is {effect:?}, not `took`"
                    ));
                    return;
                }
                let Some((_, key)) = self.splits.get(&(*range, *index)) else {
                    return;
                };
                let key = key.clone();
                self.check_values(*range, *index, right, &key, *entry_term, descriptors, ids);
            }
            _ => {}
        }
    }

    /// The values a split leaves, checked at its first apply (SHARD.md §8, check 18).
    #[allow(clippy::too_many_arguments)]
    fn check_values(
        &mut self,
        parent: u64,
        index: u64,
        right: u64,
        key: &Bytes,
        term: u64,
        descriptors: &Descriptors,
        ids: &IdsUnique,
    ) {
        let Some(before) = descriptors.before(parent, index).cloned() else {
            self.fail(format!(
                "range {parent} split at index {index} with no descriptor in force before it"
            ));
            return;
        };
        let Some(left) = descriptors.at(parent, index).cloned() else {
            self.fail(format!(
                "range {parent} split at index {index} and traced no descriptor of its own \
                 at that index"
            ));
            return;
        };
        let expected_left = Held {
            start: before.start.clone(),
            end: key.clone(),
            generation: before.generation + 1,
            voters: before.voters.clone(),
            state: before.state,
        };
        if left != expected_left {
            self.fail(format!(
                "range {parent}'s value at its split's index {index} is {left:?}, where \
                 {expected_left:?} is what the split leaves (SHARD.md §5)"
            ));
            return;
        }
        let Some((plain, voters)) = self
            .config_at(parent, index)
            .filter(|(joint, _)| !joint)
            .map(|(joint, voters)| (!joint, voters))
        else {
            self.fail(format!(
                "range {parent}'s split at index {index} took effect with no plain \
                 configuration in force at or below it (Q23)"
            ));
            return;
        };
        debug_assert!(plain);
        let Some(created) = descriptors.at(right, index).cloned() else {
            self.fail(format!(
                "range {parent}'s split at index {index} traced no creation of its right \
                 half {right} at that index"
            ));
            return;
        };
        let expected_right = Held {
            start: key.clone(),
            end: before.end.clone(),
            generation: before.generation + 1,
            voters,
            state: RangeState::Live,
        };
        if created != expected_right {
            self.fail(format!(
                "right half {right}'s creation at index {index} is {created:?}, where \
                 {expected_right:?} is what range {parent}'s split leaves (SHARD.md §5)"
            ));
            return;
        }
        match self.creations.get(&right) {
            Some((parent_named, floor_index, floor_term))
                if *parent_named == Some(parent) && *floor_index == index && *floor_term == 1 => {}
            Some((parent_named, floor_index, floor_term)) => {
                self.fail(format!(
                    "right half {right}'s creation names parent {parent_named:?} and floor \
                     ({floor_index}, {floor_term}), where the split of range {parent} at \
                     index {index} leaves ({index}, 1) (Q20)"
                ));
                return;
            }
            None => {
                self.fail(format!(
                    "right half {right} of range {parent}'s split at index {index} traced no \
                     `RangeCreated`"
                ));
                return;
            }
        }
        let Some(leader) = self.leaders.get(&(parent, term)).copied() else {
            self.fail(format!(
                "range {parent}'s split at index {index} was proposed in term {term}, whose \
                 leader traced no `RaftLeader`"
            ));
            return;
        };
        match ids.holder_of(right) {
            Some(holder) if holder == leader => {}
            Some(holder) => self.fail(format!(
                "right half {right} of range {parent}'s split at index {index} lies in a \
                 block granted to node {holder}, where node {leader} led range {parent} in \
                 the split entry's term {term} (SHARD.md §5, Q17)"
            )),
            None => self.fail(format!(
                "right half {right} of range {parent}'s split at index {index} lies in no \
                 block a `RangeIdsLeased` granted before it (SHARD.md §5, Q17)"
            )),
        }
    }

    /// The first violation.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.violation.clone().map_or(Ok(()), Err)
    }
}

/// Check 16, the meta range never goes back and names only real descriptors, over
/// check 7's map.
// PROPOSED(D-098)
#[derive(Clone, Debug, Default)]
pub struct MetaNeverGoesBack {
    /// What meta names, as the sub-intervals its applies won: keyed by end, with the
    /// start and the generation.
    named: BTreeMap<Bytes, (Bytes, u64)>,
    violation: Option<String>,
}

impl MetaNeverGoesBack {
    /// Folds one record against `descriptors` as they stand.
    pub fn push(&mut self, record: &TraceRecord, descriptors: &Descriptors) {
        if self.violation.is_some() {
            return;
        }
        let TraceEvent::MetaApplied {
            index,
            descriptors: named,
        } = &record.event
        else {
            return;
        };
        let node = record.node.map(|node| u64::from(node.get()));
        for meta in named {
            match descriptors.first_at_generation(meta.range, meta.generation) {
                None => {
                    self.violation = Some(format!(
                        "meta never goes back: node {node:?}'s meta apply of {index} names \
                         range {} at generation {}, which check 7 had not traced before it \
                         (SHARD.md §8, check 16)",
                        meta.range, meta.generation
                    ));
                    return;
                }
                Some(held)
                    if held.start != meta.start
                        || held.end != meta.end
                        || held.voters != meta.voters =>
                {
                    self.violation = Some(format!(
                        "meta never goes back: node {node:?}'s meta apply of {index} names \
                         range {} at generation {} as [{:?}, {:?}) with voters {:?}, where \
                         check 7 holds [{:?}, {:?}) with voters {:?}",
                        meta.range,
                        meta.generation,
                        meta.start,
                        meta.end,
                        meta.voters,
                        held.start,
                        held.end,
                        held.voters
                    ));
                    return;
                }
                Some(_) => {}
            }
            for (start, end) in &meta.won {
                if start >= end {
                    continue;
                }
                let overlapping: Vec<Bytes> = self
                    .named
                    .range::<[u8], _>((Excluded(&start[..]), Unbounded))
                    .take_while(|(_, (from, _))| from < end)
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in overlapping {
                    let (from, generation) = self.named.remove(&key).expect("listed");
                    if generation > meta.generation {
                        self.violation = Some(format!(
                            "meta never goes back: node {node:?}'s meta apply of {index} names \
                             [{start:?}, {end:?}) for range {} at generation {}, where meta \
                             named [{from:?}, {key:?}) at generation {generation} before it: \
                             the generation meta names never falls (SHARD.md §1, §8)",
                            meta.range, meta.generation
                        ));
                        return;
                    }
                    if from < *start {
                        self.named.insert(start.clone(), (from.clone(), generation));
                    }
                    if key > *end {
                        self.named.insert(key, (end.clone(), generation));
                    }
                }
                self.named
                    .insert(end.clone(), (start.clone(), meta.generation));
            }
        }
    }

    /// The first violation, if one was folded.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.violation.clone().map_or(Ok(()), Err)
    }
}

/// Check 17, a client refreshes on a mismatch.
#[derive(Clone, Debug, Default)]
pub struct ClientRefreshes {
    /// Each operation's key, by (client, invoked).
    keys: BTreeMap<(u64, u64), Bytes>,
    /// Each send's operation, by (client, seq).
    sends: BTreeMap<(u64, u64), u64>,
    /// The highest generation a mismatch named for an operation's key, by (client,
    /// invoked).
    demanded: BTreeMap<(u64, u64), u64>,
    violation: Option<String>,
}

impl ClientRefreshes {
    /// Folds one record.
    pub fn push(&mut self, record: &TraceRecord) {
        if self.violation.is_some() {
            return;
        }
        match &record.event {
            TraceEvent::ClientInvoke { client, seq, op } => {
                let key = match op {
                    ClientOp::Put { key, .. }
                    | ClientOp::Get { key }
                    | ClientOp::Delete { key }
                    | ClientOp::Cas { key, .. } => key.clone(),
                };
                self.keys.insert((*client, *seq), key);
            }
            TraceEvent::ClientSend {
                client,
                seq,
                range,
                generation,
                invoked,
                ..
            } => {
                self.sends.insert((*client, *seq), *invoked);
                if let Some(&demanded) = self.demanded.get(&(*client, *invoked))
                    && *generation < demanded
                {
                    self.violation = Some(format!(
                        "client refreshes: client {client} sent operation {invoked} again as \
                         {seq} to range {range} at generation {generation}, after a mismatch \
                         named its key's owner at generation {demanded} (SHARD.md §3)"
                    ));
                }
            }
            TraceEvent::ClientMismatch {
                client,
                seq,
                descriptors,
            } => {
                let Some(&invoked) = self.sends.get(&(*client, *seq)) else {
                    return;
                };
                let Some(key) = self.keys.get(&(*client, invoked)) else {
                    return;
                };
                let encoded = user_key(key);
                let named = descriptors
                    .iter()
                    .filter(|(_, _, start, end)| start[..] <= encoded[..] && encoded[..] < end[..])
                    .map(|(_, generation, _, _)| *generation)
                    .max();
                if let Some(named) = named {
                    let demanded = self.demanded.entry((*client, invoked)).or_default();
                    *demanded = (*demanded).max(named);
                }
            }
            _ => {}
        }
    }

    /// The first violation, if one was folded.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.violation.clone().map_or(Ok(()), Err)
    }
}

/// The eight folds — checks 7, 8, 9, 10, 16 and 17, and check 18's two clauses — as
/// one incremental checker: fed the records since its last look, with a verdict at any
/// prefix that is what the folds over the whole prefix report, in the same words
/// (D-046).
///
/// A checker built `active: false` folds nothing and reports nothing: what a run of
/// the one-group server, which traces no descriptor, is checked with.
#[derive(Clone, Debug, Default)]
pub struct Checker {
    active: bool,
    descriptors: Descriptors,
    owners: OneOwnerPerKey,
    serving: ServingWithinSpan,
    generations: GenerationsRise,
    meta: MetaNeverGoesBack,
    refreshes: ClientRefreshes,
    ids: IdsUnique,
    lineage: SplitLineage,
}

impl Checker {
    /// A checker that folds every record, or one that folds none.
    #[must_use]
    pub fn new(active: bool) -> Self {
        Self {
            active,
            ..Self::default()
        }
    }

    /// Folds one record.
    pub fn push(&mut self, record: &TraceRecord) {
        if !self.active {
            return;
        }
        self.descriptors.push(record);
        self.owners.push(record);
        self.serving.push(record, &self.descriptors);
        self.generations.push(record, &self.descriptors);
        self.meta.push(record, &self.descriptors);
        self.refreshes.push(record);
        self.ids.push(record);
        self.lineage.push(record, &self.descriptors, &self.ids);
    }

    /// Folds records in order.
    pub fn extend<'a>(&mut self, records: impl IntoIterator<Item = &'a TraceRecord>) {
        for record in records {
            self.push(record);
        }
    }

    /// Check 7's map as it stands.
    #[must_use]
    pub fn descriptors(&self) -> &Descriptors {
        &self.descriptors
    }

    /// Check 8's fold as it stands.
    // PROPOSED(D-107)
    #[must_use]
    pub fn owners(&self) -> &OneOwnerPerKey {
        &self.owners
    }

    /// The first violation, in check order — 7, 9, 10, 16, 17, 18 — and then check 8's
    /// overlap clause, last so that no violation a fold before it reports moves to
    /// it. Check 8's right-half clause is folded and **not** in the verdict: the
    /// correct node trips it on a voter installed past a split, which holds no
    /// placeholder for the right half (Q22), so it is read apart
    /// ([`Checker::owners`]) and left to the owner (PROPOSED D-107).
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        self.descriptors.verdict()?;
        self.serving.verdict()?;
        self.generations.verdict()?;
        self.meta.verdict()?;
        self.refreshes.verdict()?;
        self.ids.verdict()?;
        self.lineage.verdict()?;
        self.owners.overlap_verdict()
    }
}

/// The checks folded over `records` from the first: the whole-trace form of
/// [`Checker`].
///
/// # Errors
///
/// The first violation, in check order.
pub fn all(records: &[TraceRecord]) -> Result<(), String> {
    let mut checker = Checker::new(true);
    checker.extend(records);
    checker.verdict()
}

#[cfg(test)]
mod tests {
    //! The checks' reasoning on records written by hand, one shape each.

    use ananke_env::{ClientOp, Instant, NodeId};

    use super::*;

    fn record(node: u64, event: TraceEvent) -> TraceRecord {
        TraceRecord {
            at: Instant::from_nanos(1),
            decided: Instant::from_nanos(1),
            node: Some(NodeId::new(u32::try_from(node).expect("small"))),
            event,
        }
    }

    fn created(
        node: u64,
        range: u64,
        start: &[u8],
        end: &[u8],
        generation: u64,
        voters: &[u64],
    ) -> TraceRecord {
        record(
            node,
            TraceEvent::RangeCreated {
                range,
                cause: RangeCause::Bootstrap,
                parent: None,
                start: user_key(start),
                end: user_key(end),
                generation,
                voters: voters.to_vec(),
                floor_index: 0,
                floor_term: 0,
                incarnation: 1,
            },
        )
    }

    fn applied(
        server: u64,
        range: u64,
        index: u64,
        key: &[u8],
        effect: ApplyEffect,
    ) -> TraceRecord {
        record(
            server,
            TraceEvent::RaftApply {
                server,
                range,
                index,
                entry_term: 1,
                hash: 0,
                key: Some(Bytes::copy_from_slice(key)),
                effect,
            },
        )
    }

    fn read(server: u64, range: u64, key: &[u8], applied: u64) -> TraceRecord {
        record(
            server,
            TraceEvent::RaftRead {
                server,
                range,
                index: applied,
                lease: false,
                key: Bytes::copy_from_slice(key),
                applied,
            },
        )
    }

    #[test]
    fn two_creations_of_one_range_must_agree() {
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"k", 1, &[1, 2, 3]));
        checker.push(&created(2, 2, b"a", b"k", 1, &[1, 2, 3]));
        checker.verdict().unwrap();
        checker.push(&created(3, 2, b"a", b"k", 1, &[1, 2, 3, 4, 5]));
        let why = checker.verdict().unwrap_err();
        assert!(why.starts_with("descriptor agreement:"), "{why}");
        assert!(why.contains("range 2"), "{why}");
    }

    #[test]
    fn an_install_must_agree_with_the_descriptor_in_force_at_its_index() {
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"k", 1, &[1, 2, 3]));
        let mut install = created(3, 2, b"a", b"k", 1, &[1, 2, 3]);
        if let TraceEvent::RangeCreated {
            cause, floor_index, ..
        } = &mut install.event
        {
            *cause = RangeCause::Snapshot;
            *floor_index = 40;
        }
        checker.push(&install);
        checker.verdict().unwrap();
        let mut wrong = created(3, 2, b"a", b"z", 1, &[1, 2, 3]);
        if let TraceEvent::RangeCreated {
            cause, floor_index, ..
        } = &mut wrong.event
        {
            *cause = RangeCause::Snapshot;
            *floor_index = 41;
        }
        checker.push(&wrong);
        assert!(
            checker
                .verdict()
                .unwrap_err()
                .contains("installed at index 41")
        );
    }

    #[test]
    fn an_applied_write_outside_the_span_and_a_refusal_inside_it_are_violations() {
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"k", 1, &[1, 2, 3]));
        checker.push(&applied(1, 2, 5, b"b", ApplyEffect::Applied));
        checker.push(&applied(1, 2, 6, b"z", ApplyEffect::OutOfSpan));
        checker.verdict().unwrap();
        let mut outside = checker.clone();
        outside.push(&applied(1, 2, 7, b"z", ApplyEffect::Applied));
        assert!(outside.verdict().unwrap_err().contains("does not serve"));
        let mut refused = checker.clone();
        refused.push(&applied(1, 2, 7, b"b", ApplyEffect::OutOfSpan));
        assert!(refused.verdict().unwrap_err().contains("as out of span"));
        let mut unknown = checker;
        unknown.push(&applied(1, 3, 1, b"q", ApplyEffect::Applied));
        assert!(
            unknown
                .verdict()
                .unwrap_err()
                .contains("no descriptor in force")
        );
    }

    #[test]
    fn a_read_served_outside_the_descriptor_at_its_applied_index_is_a_violation() {
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"k", 1, &[1, 2, 3]));
        checker.push(&read(1, 2, b"b", 3));
        checker.verdict().unwrap();
        checker.push(&read(1, 2, b"z", 3));
        assert!(checker.verdict().unwrap_err().contains("at serving"));
    }

    #[test]
    fn a_key_written_at_a_lower_generation_or_by_a_sibling_at_the_same_one_is_a_violation() {
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"k", 1, &[1, 2, 3]));
        // On another node: one node holding both spans is check 8's (PROPOSED D-107).
        let mut higher = created(2, 3, b"a", b"k", 2, &[1, 2, 3]);
        if let TraceEvent::RangeCreated { floor_index, .. } = &mut higher.event {
            *floor_index = 0;
        }
        checker.push(&higher);
        checker.push(&applied(1, 3, 1, b"b", ApplyEffect::Applied));
        checker.verdict().unwrap();
        let mut lower = checker.clone();
        lower.push(&applied(1, 2, 9, b"b", ApplyEffect::Applied));
        assert!(lower.verdict().unwrap_err().contains("strictly rise"));
        // The same range writing again at the same generation is fine.
        checker.push(&applied(1, 3, 2, b"b", ApplyEffect::Applied));
        checker.verdict().unwrap();
        // A second apply of the same (range, index) on another server is not a second
        // write.
        checker.push(&applied(2, 3, 2, b"b", ApplyEffect::Applied));
        checker.verdict().unwrap();
    }

    #[test]
    fn a_resend_below_the_generation_a_mismatch_named_is_a_violation() {
        let key = Bytes::from_static(b"m");
        let mut checker = Checker::new(true);
        checker.push(&record(
            9,
            TraceEvent::ClientInvoke {
                client: 7,
                seq: 1,
                op: ClientOp::Get { key: key.clone() },
            },
        ));
        checker.push(&record(
            9,
            TraceEvent::ClientSend {
                client: 7,
                seq: 1,
                range: 2,
                generation: 0,
                to: 1,
                invoked: 1,
            },
        ));
        checker.push(&record(
            9,
            TraceEvent::ClientMismatch {
                client: 7,
                seq: 1,
                descriptors: vec![(3, 1, user_key(b"k"), user_key(b"z"))],
            },
        ));
        let mut refreshed = checker.clone();
        refreshed.push(&record(
            9,
            TraceEvent::ClientSend {
                client: 7,
                seq: 2,
                range: 3,
                generation: 1,
                to: 1,
                invoked: 1,
            },
        ));
        refreshed.verdict().unwrap();
        checker.push(&record(
            9,
            TraceEvent::ClientSend {
                client: 7,
                seq: 2,
                range: 2,
                generation: 0,
                to: 1,
                invoked: 1,
            },
        ));
        assert!(
            checker
                .verdict()
                .unwrap_err()
                .starts_with("client refreshes:")
        );
    }

    /// A descriptor as a test names it: range, start, end, generation and the
    /// sub-intervals it won, all keys raw.
    type Named<'a> = (u64, &'a [u8], &'a [u8], u64, Vec<(&'a [u8], &'a [u8])>);

    fn leased(node_on: u64, node: u64, run: u64, first: u64, last: u64, index: u64) -> TraceRecord {
        record(
            node_on,
            TraceEvent::RangeIdsLeased {
                node,
                run,
                first,
                last,
                index,
            },
        )
    }

    #[test]
    fn disjoint_blocks_pass_and_every_replica_of_an_index_may_grant_the_same() {
        let mut checker = Checker::new(true);
        checker.push(&leased(1, 1, 7, 3, 10, 1));
        checker.push(&leased(2, 1, 7, 3, 10, 1));
        checker.push(&leased(1, 2, 9, 11, 18, 2));
        checker.push(&leased(3, 3, 5, 19, 26, 3));
        checker.push(&leased(3, 1, 7, 3, 10, 1));
        assert!(checker.verdict().is_ok(), "{:?}", checker.verdict());
        assert_eq!(checker.ids.blocks().len(), 3);
        assert_eq!(checker.ids.holder_of(12), Some(2));
        assert_eq!(checker.ids.holder_of(27), None);
    }

    #[test]
    fn two_grants_that_share_an_id_are_caught() {
        let mut checker = Checker::new(true);
        checker.push(&leased(1, 1, 7, 3, 10, 1));
        checker.push(&leased(1, 2, 9, 10, 17, 2));
        let verdict = checker.verdict().unwrap_err();
        assert!(
            verdict.starts_with("range ids: the block 10..=17"),
            "{verdict}"
        );
        let mut below = Checker::new(true);
        below.push(&leased(1, 1, 7, 11, 18, 1));
        below.push(&leased(1, 2, 9, 3, 11, 2));
        assert!(below.verdict().is_err());
    }

    #[test]
    fn a_replica_that_grants_something_else_at_the_same_index_is_caught() {
        let mut checker = Checker::new(true);
        checker.push(&leased(1, 1, 7, 3, 10, 1));
        checker.push(&leased(2, 1, 7, 11, 18, 1));
        let verdict = checker.verdict().unwrap_err();
        assert!(
            verdict.starts_with("range ids: index 1 of range 0 granted"),
            "{verdict}"
        );
        let mut empty = Checker::new(true);
        empty.push(&leased(1, 1, 7, 5, 4, 1));
        assert!(empty.verdict().is_err());
    }

    /// The records a split of range `parent` at `index` with key `key` and right half
    /// `right` leaves on node `on`, in the order the apply traces them (SHARD.md §5):
    /// `RangeSplit`, the parent's `RangeDescriptor`, the right half's `RangeCreated`
    /// and the entry's `RaftApply` in `term`.
    #[allow(clippy::too_many_arguments)]
    fn split_records(
        on: u64,
        parent: u64,
        index: u64,
        term: u64,
        key: &[u8],
        start: &[u8],
        end: &[u8],
        generation: u64,
        right: u64,
    ) -> Vec<TraceRecord> {
        vec![
            record(
                on,
                TraceEvent::RangeSplit {
                    range: parent,
                    right,
                    key: user_key(key),
                    index,
                },
            ),
            record(
                on,
                TraceEvent::RangeDescriptor {
                    range: parent,
                    index,
                    applied: index,
                    start: user_key(start),
                    end: user_key(key),
                    generation: generation + 1,
                    voters: vec![1, 2, 3],
                    state: RangeState::Live,
                },
            ),
            record(
                on,
                TraceEvent::RangeCreated {
                    range: right,
                    cause: RangeCause::Split,
                    parent: Some(parent),
                    start: user_key(key),
                    end: user_key(end),
                    generation: generation + 1,
                    voters: vec![1, 2, 3],
                    floor_index: index,
                    floor_term: 1,
                    incarnation: 5,
                },
            ),
            record(
                on,
                TraceEvent::RaftApply {
                    server: on,
                    range: parent,
                    index,
                    entry_term: term,
                    hash: 0,
                    key: None,
                    effect: ApplyEffect::Took,
                },
            ),
        ]
    }

    /// The records before a split: the parent's bootstrap creation, its plain
    /// configuration at index 0, its leader in term 2 on node 1, and node 1's block
    /// of ids 6 to 13.
    fn before_a_split() -> Vec<TraceRecord> {
        let mut records = vec![created(1, 2, b"a", b"z", 1, &[1, 2, 3])];
        records.push(record(
            1,
            TraceEvent::RaftConfig {
                server: 1,
                range: 2,
                index: 0,
                old: vec![1, 2, 3],
                new: vec![],
                joint: false,
                learners: vec![],
            },
        ));
        records.push(record(
            1,
            TraceEvent::RaftLeader {
                server: 1,
                range: 2,
                term: 2,
                last_index: 8,
            },
        ));
        records.push(leased(1, 1, 7, 6, 13, 1));
        records
    }

    #[test]
    fn a_split_in_shape_passes_check_18_on_every_replica() {
        let mut checker = Checker::new(true);
        checker.extend(&before_a_split());
        checker.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        checker.extend(&split_records(2, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        assert!(checker.verdict().is_ok(), "{:?}", checker.verdict());
        assert_eq!(
            checker.descriptors().at(6, 9).map(|held| held.generation),
            Some(2)
        );
    }

    #[test]
    fn a_right_half_outside_the_leaders_block_is_caught() {
        let mut checker = Checker::new(true);
        checker.extend(&before_a_split());
        checker.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 20));
        let verdict = checker.verdict().unwrap_err();
        assert!(verdict.contains("lies in no block"), "{verdict}");
        let mut other = Checker::new(true);
        other.extend(&before_a_split());
        other.push(&leased(1, 2, 9, 14, 21, 2));
        other.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 14));
        let verdict = other.verdict().unwrap_err();
        assert!(verdict.contains("granted to node 2"), "{verdict}");
    }

    #[test]
    fn a_right_half_named_before_and_a_wrong_generation_are_caught() {
        let mut checker = Checker::new(true);
        checker.extend(&before_a_split());
        checker.push(&created(1, 6, b"x", b"y", 1, &[1, 2, 3]));
        checker.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        let verdict = checker.verdict().unwrap_err();
        assert!(verdict.contains("had named before it"), "{verdict}");
        let mut wrong = Checker::new(true);
        wrong.extend(&before_a_split());
        let mut records = split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6);
        if let TraceEvent::RangeDescriptor { generation, .. } = &mut records[1].event {
            *generation = 1;
        }
        wrong.extend(&records);
        let verdict = wrong.verdict().unwrap_err();
        assert!(verdict.contains("is what the split leaves"), "{verdict}");
    }

    // --- Check 7's restatement clause and check 8 (PROPOSED D-107) ---

    /// A replica's restatement of its log: what precedes its descriptor's.
    fn recovered(node: u64, range: u64, applied: u64) -> TraceRecord {
        record(
            node,
            TraceEvent::RaftRecovered {
                server: node,
                range,
                term: 2,
                applied,
                last_index: applied,
                incarnation: 1,
                state: ananke_env::RecoveredAs::Neither,
            },
        )
    }

    /// A descriptor stated at `applied`: a restatement, or an install's change.
    fn stated(
        node: u64,
        range: u64,
        applied: u64,
        start: &[u8],
        end: &[u8],
        generation: u64,
    ) -> TraceRecord {
        record(
            node,
            TraceEvent::RangeDescriptor {
                range,
                index: applied,
                applied,
                start: user_key(start),
                end: user_key(end),
                generation,
                voters: vec![1, 2, 3],
                state: RangeState::Live,
            },
        )
    }

    fn restated(node: u64, ranges: &[u64]) -> TraceRecord {
        record(
            node,
            TraceEvent::RangesRestated {
                ranges: ranges.to_vec(),
            },
        )
    }

    fn crashed(node: u64) -> TraceRecord {
        record(
            node,
            TraceEvent::NodeCrashed {
                node: NodeId::new(u32::try_from(node).expect("small")),
            },
        )
    }

    /// The split of range 2 `[a, z)` at index 9 into `[a, m)` and right half 6 `[m,
    /// z)`, on nodes 1 and 2, each having restated range 2 at its bootstrap first.
    fn a_split_applied() -> Vec<TraceRecord> {
        let mut records = before_a_split();
        for node in [1, 2] {
            records.push(recovered(node, 2, 0));
            records.push(stated(node, 2, 0, b"a", b"z", 1));
            records.push(restated(node, &[2]));
        }
        records.extend(split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        records.extend(split_records(2, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        records
    }

    #[test]
    fn a_restatement_must_agree_with_the_descriptor_in_force_at_its_applied_index() {
        let mut checker = Checker::new(true);
        checker.extend(&a_split_applied());
        // Node 1 restarts at applied 12, past the split: P's shrunk descriptor, and R.
        checker.push(&crashed(1));
        checker.push(&recovered(1, 2, 12));
        checker.push(&stated(1, 2, 12, b"a", b"m", 2));
        checker.push(&recovered(1, 6, 12));
        checker.push(&stated(1, 6, 12, b"m", b"z", 2));
        checker.push(&restated(1, &[2, 6]));
        assert!(checker.verdict().is_ok(), "{:?}", checker.verdict());
        // A restatement below the split states P's first value, and agrees.
        checker.push(&crashed(2));
        checker.push(&recovered(2, 2, 5));
        checker.push(&stated(2, 2, 5, b"a", b"z", 1));
        assert!(checker.verdict().is_ok(), "{:?}", checker.verdict());
        // A node that restarts past the split with P's old descriptor holds a
        // descriptor its applied index cannot hold: check 7's restatement clause.
        let mut stale = checker.clone();
        stale.push(&crashed(2));
        stale.push(&recovered(2, 2, 12));
        stale.push(&stated(2, 2, 12, b"a", b"z", 1));
        let why = stale.verdict().unwrap_err();
        assert!(why.starts_with("descriptor agreement:"), "{why}");
        assert!(why.contains("restated at index 12"), "{why}");
        // Read as a value an apply put, the same record would have passed: it is the
        // first value traced at index 12, which a restatement must never be.
        assert_eq!(
            stale.descriptors().at(2, 12).map(|held| held.generation),
            Some(2),
            "a restatement put no value of its own"
        );
    }

    #[test]
    fn a_restatement_ahead_of_the_map_waits_for_the_change_it_holds() {
        // Node 2 states P's post-split descriptor at 12 before any replica traced the
        // split (a crash between the split's batch and its trace): it waits.
        let mut early = Checker::new(true);
        early.extend(&before_a_split());
        early.push(&recovered(2, 2, 12));
        early.push(&stated(2, 2, 12, b"a", b"m", 2));
        assert!(early.verdict().is_ok(), "{:?}", early.verdict());
        // The split then traced at 9 is the change it held.
        let mut agreed = early.clone();
        agreed.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        assert!(agreed.verdict().is_ok(), "{:?}", agreed.verdict());
        // Traced at 15, past the restatement's 12, it is a generation the replica
        // could not hold at 12.
        let mut late = early.clone();
        late.extend(&split_records(1, 2, 15, 2, b"m", b"a", b"z", 1, 6));
        let why = late.verdict().unwrap_err();
        assert!(why.contains("took only at index 15"), "{why}");
        // A restatement that agreed with the map is judged again by a change traced
        // later at or below its index.
        let mut later = Checker::new(true);
        later.extend(&before_a_split());
        later.push(&recovered(2, 2, 12));
        later.push(&stated(2, 2, 12, b"a", b"z", 1));
        assert!(later.verdict().is_ok(), "{:?}", later.verdict());
        later.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        let why = later.verdict().unwrap_err();
        assert!(why.contains("restated at index 12"), "{why}");
    }

    #[test]
    fn an_install_that_changes_an_initialised_replica_is_a_statement_at_its_last_index() {
        let mut checker = Checker::new(true);
        checker.extend(&a_split_applied());
        // Node 3 held P before the split and is installed P's snapshot at 20.
        checker.push(&recovered(3, 2, 0));
        checker.push(&stated(3, 2, 0, b"a", b"z", 1));
        checker.push(&restated(3, &[2]));
        checker.push(&recovered(3, 2, 20));
        checker.push(&stated(3, 2, 20, b"a", b"m", 2));
        assert!(checker.verdict().is_ok(), "{:?}", checker.verdict());
        let mut wrong = checker.clone();
        wrong.push(&recovered(3, 2, 21));
        wrong.push(&stated(3, 2, 21, b"a", b"z", 1));
        let why = wrong.verdict().unwrap_err();
        assert!(why.contains("installed at index 21"), "{why}");
    }

    /// #166's review (B1): node 1 crashes after a durable split of range 2 at 9 and
    /// restates both halves at 12 — the right half 6 over a wrong span `[m, y)` —
    /// before any replica has traced the split, and node 2 applies it after.
    fn a_wrong_right_half_restated_before_its_split_is_traced()
    -> (Vec<TraceRecord>, Vec<TraceRecord>) {
        let mut before = Vec::new();
        for node in [1, 2, 3] {
            before.push(created(node, 2, b"a", b"z", 1, &[1, 2, 3]));
        }
        let restatement = vec![
            crashed(1),
            recovered(1, 2, 12),
            stated(1, 2, 12, b"a", b"m", 2),
            recovered(1, 6, 12),
            stated(1, 6, 12, b"m", b"y", 2),
            restated(1, &[2, 6]),
        ];
        let split = split_records(2, 2, 9, 2, b"m", b"a", b"z", 1, 6);
        let statement_first = before
            .iter()
            .chain(&restatement)
            .chain(&split)
            .cloned()
            .collect();
        let split_first = before
            .iter()
            .chain(&split)
            .chain(&restatement)
            .cloned()
            .collect();
        (statement_first, split_first)
    }

    #[test]
    fn a_statement_traced_before_any_value_of_its_range_is_judged_when_one_lands() {
        let (statement_first, split_first) =
            a_wrong_right_half_restated_before_its_split_is_traced();
        for (order, records) in [
            ("statement first", statement_first),
            ("split first", split_first),
        ] {
            let mut descriptors = Descriptors::default();
            for record in &records {
                descriptors.push(record);
            }
            let why = descriptors
                .verdict()
                .expect_err("the wrong right half is caught in either order");
            assert!(
                why.starts_with(
                    "descriptor agreement: node Some(1)'s replica of range 6 was \
                     restated at index 12"
                ),
                "{order}: {why}"
            );
            // The map holds the creation's value and never the statement's, so check 16
            // reads the right half's true span as the first at its generation.
            assert_eq!(
                descriptors
                    .first_at_generation(6, 2)
                    .map(|held| (held.start.clone(), held.end.clone())),
                Some((user_key(b"m"), user_key(b"z"))),
                "{order}"
            );
            assert_eq!(
                descriptors.at(6, 12).map(|held| held.end.clone()),
                Some(user_key(b"z")),
                "{order}: a statement put no value of its own"
            );
        }
    }

    #[test]
    fn a_statement_with_no_value_under_it_waits_and_agrees_when_the_right_one_lands() {
        // The same crash, the right half restated with its true span: it waits, and the
        // creation traced after it is the value it agrees with.
        let mut records = Vec::new();
        for node in [1, 2, 3] {
            records.push(created(node, 2, b"a", b"z", 1, &[1, 2, 3]));
        }
        records.extend([
            crashed(1),
            recovered(1, 2, 12),
            stated(1, 2, 12, b"a", b"m", 2),
            recovered(1, 6, 12),
            stated(1, 6, 12, b"m", b"z", 2),
            restated(1, &[2, 6]),
        ]);
        let mut descriptors = Descriptors::default();
        for record in &records {
            descriptors.push(record);
        }
        assert!(descriptors.verdict().is_ok(), "{:?}", descriptors.verdict());
        assert!(
            descriptors.at(6, 12).is_none(),
            "a statement with no value under it is no value of the map"
        );
        for record in &split_records(2, 2, 9, 2, b"m", b"a", b"z", 1, 6) {
            descriptors.push(record);
        }
        assert!(descriptors.verdict().is_ok(), "{:?}", descriptors.verdict());
    }

    #[test]
    fn two_spans_of_one_node_that_overlap_are_caught_by_check_8() {
        let mut owners = OneOwnerPerKey::default();
        // A split in §8's fixed order: P shrinks before R is created.
        for record in &a_split_applied() {
            owners.push(record);
        }
        assert!(owners.verdict().is_ok(), "{:?}", owners.verdict());
        // The same ranges on another node, R created before P shrinks: P's old span
        // covers R's.
        let mut overlapping = OneOwnerPerKey::default();
        overlapping.push(&created(3, 2, b"a", b"z", 1, &[1, 2, 3]));
        overlapping.push(&created(3, 6, b"m", b"z", 2, &[1, 2, 3]));
        let why = overlapping.overlap_verdict().unwrap_err();
        assert!(why.starts_with("one owner per key:"), "{why}");
        assert!(why.contains("node 3's replica of range 6"), "{why}");
        // One node's span never meets another node's.
        let mut apart = OneOwnerPerKey::default();
        apart.push(&created(3, 2, b"a", b"z", 1, &[1, 2, 3]));
        apart.push(&created(4, 6, b"m", b"z", 2, &[1, 2, 3]));
        assert!(apart.verdict().is_ok(), "{:?}", apart.verdict());
    }

    #[test]
    fn a_voter_that_applied_a_split_restates_its_right_half_or_check_8_says_so() {
        let mut owners = OneOwnerPerKey::default();
        for record in &a_split_applied() {
            owners.push(record);
        }
        // Node 1 restarts and restates both halves.
        for record in [
            crashed(1),
            recovered(1, 2, 12),
            stated(1, 2, 12, b"a", b"m", 2),
            recovered(1, 6, 12),
            stated(1, 6, 12, b"m", b"z", 2),
            restated(1, &[2, 6]),
        ] {
            owners.push(&record);
        }
        assert!(owners.verdict().is_ok(), "{:?}", owners.verdict());
        // Node 2 restarts without its right half: the restart that does not reopen
        // a right half (PROPOSED D-100's second finding).
        let mut lost = owners.clone();
        for record in [
            crashed(2),
            recovered(2, 2, 12),
            stated(2, 2, 12, b"a", b"m", 2),
            restated(2, &[2]),
        ] {
            lost.push(&record);
        }
        let why = lost.right_half_verdict().unwrap_err();
        assert!(
            why.contains("at its restatement it holds no replica of the right half 6"),
            "{why}"
        );
        // A refused node owes nothing until it applies the split again.
        let mut refused = owners.clone();
        for record in [
            crashed(2),
            record(
                2,
                TraceEvent::RaftRefused {
                    server: 2,
                    reason: "lost".into(),
                },
            ),
            restated(2, &[]),
        ] {
            refused.push(&record);
        }
        assert!(refused.verdict().is_ok(), "{:?}", refused.verdict());
        // And a node that restates P past the split, a voter there, owes R whether or
        // not it applied the split by an apply: an install past it, then a restart.
        let mut installed = refused.clone();
        for record in [
            recovered(2, 2, 20),
            stated(2, 2, 20, b"a", b"m", 2),
            crashed(2),
            recovered(2, 2, 20),
            stated(2, 2, 20, b"a", b"m", 2),
            restated(2, &[2]),
        ] {
            installed.push(&record);
        }
        let why = installed.right_half_verdict().unwrap_err();
        assert!(why.contains("right half 6"), "{why}");
        // A node that is not a voter of P at the split owes no right half.
        let mut learner = OneOwnerPerKey::default();
        for record in &a_split_applied() {
            learner.push(record);
        }
        for record in [
            recovered(4, 2, 12),
            stated(4, 2, 12, b"a", b"m", 2),
            restated(4, &[2]),
        ] {
            learner.push(&record);
        }
        assert!(learner.verdict().is_ok(), "{:?}", learner.verdict());
    }

    #[test]
    fn a_voter_that_applies_a_split_without_creating_its_right_half_is_caught_at_the_apply() {
        let mut owners = OneOwnerPerKey::default();
        for record in &before_a_split() {
            owners.push(record);
        }
        // Node 2 applies the split after node 1 created R, and creates none itself.
        for record in &split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6) {
            owners.push(record);
        }
        let mut records = split_records(2, 2, 9, 2, b"m", b"a", b"z", 1, 6);
        records.remove(2);
        for record in &records {
            owners.push(record);
        }
        let why = owners.right_half_verdict().unwrap_err();
        assert!(why.contains("at its apply of 9"), "{why}");
    }

    fn meta_applied(node: u64, index: u64, named: Vec<Named<'_>>) -> TraceRecord {
        record(
            node,
            TraceEvent::MetaApplied {
                index,
                descriptors: named
                    .into_iter()
                    .map(
                        |(range, start, end, generation, won)| ananke_env::MetaDescriptor {
                            range,
                            start: user_key(start),
                            end: user_key(end),
                            generation,
                            voters: vec![1, 2, 3],
                            won: won
                                .into_iter()
                                .map(|(s, e)| (user_key(s), user_key(e)))
                                .collect(),
                        },
                    )
                    .collect(),
            },
        )
    }

    #[test]
    fn a_meta_apply_names_only_descriptors_check_7_traced_before_it() {
        let mut checker = Checker::new(true);
        checker.push(&meta_applied(
            1,
            0,
            vec![(2, b"a", b"k", 1, vec![(b"a", b"k")])],
        ));
        assert!(
            checker
                .verdict()
                .unwrap_err()
                .contains("had not traced before it")
        );
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"k", 1, &[1, 2, 3]));
        checker.push(&meta_applied(
            1,
            0,
            vec![(2, b"a", b"k", 1, vec![(b"a", b"k")])],
        ));
        checker.push(&meta_applied(
            2,
            0,
            vec![(2, b"a", b"k", 1, vec![(b"a", b"k")])],
        ));
        checker.verdict().unwrap();
        checker.push(&meta_applied(
            3,
            0,
            vec![(2, b"a", b"z", 1, vec![(b"a", b"z")])],
        ));
        assert!(
            checker
                .verdict()
                .unwrap_err()
                .contains("where check 7 holds")
        );
    }

    #[test]
    fn a_meta_apply_that_names_a_lower_generation_for_a_key_is_a_violation() {
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"z", 1, &[1, 2, 3]));
        checker.push(&meta_applied(
            1,
            0,
            vec![(2, b"a", b"z", 1, vec![(b"a", b"z")])],
        ));
        // On another node: one node holding the parent's whole span beside it is check
        // 8's (PROPOSED D-107).
        let mut higher = created(2, 3, b"k", b"z", 2, &[1, 2, 3]);
        if let TraceEvent::RangeCreated {
            cause, floor_index, ..
        } = &mut higher.event
        {
            *cause = RangeCause::Split;
            *floor_index = 7;
        }
        checker.push(&higher);
        checker.push(&meta_applied(
            1,
            5,
            vec![(3, b"k", b"z", 2, vec![(b"k", b"z")])],
        ));
        // A resend of the parent's record wins nothing, and names no regression.
        checker.push(&meta_applied(1, 6, vec![(2, b"a", b"z", 1, vec![])]));
        checker.verdict().unwrap();
        // The variant: the stale parent takes the span back.
        checker.push(&meta_applied(
            1,
            7,
            vec![(2, b"a", b"z", 1, vec![(b"a", b"z")])],
        ));
        assert!(checker.verdict().unwrap_err().contains("never falls"));
    }

    #[test]
    fn a_read_served_by_a_system_range_is_a_lookup_and_check_9_holds_it_to_nothing() {
        let mut checker = Checker::new(true);
        checker.push(&created(1, 2, b"a", b"k", 1, &[1, 2, 3]));
        checker.push(&read(1, 1, b"q", 3));
        checker.push(&read(1, 0, b"", 3));
        checker.verdict().unwrap();
    }

    #[test]
    fn an_inactive_checker_folds_nothing() {
        let mut checker = Checker::new(false);
        checker.push(&applied(1, 3, 1, b"q", ApplyEffect::Applied));
        checker.verdict().unwrap();
        assert!(checker.descriptors().is_empty());
    }
}
