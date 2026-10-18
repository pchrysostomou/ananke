//! SHARD.md §8's checks 7, 9, 10, 16, 17 and 18 as folds over the trace, in the range
//! layer where Q40 puts checks 7 to 22: descriptor agreement, serving within span,
//! owners' generations rising, meta never going back, a client refreshing on a
//! mismatch, and the lineage of range ids and splits. Each is a fold fed one record at
//! a time, with a verdict readable at any prefix, so one [`Checker`] runs the seven —
//! check 18 is two, [`IdsUnique`] and [`SplitLineage`] — incrementally beside
//! `ananke_raft::invariants`'s and again over the whole trace at the end (D-046; §8).
//!
//! **Check 7** holds a map from (range, index) to the descriptor in force from that
//! index: a `RangeCreated` of a bootstrap or a split is the value at (range,
//! `floor_index`), a `RangeDescriptor` at an apply the value at (range, `index`), and
//! a second value for one (range, index) is a violation. An install's `RangeCreated`
//! carries an index and must equal the map's latest value at or below it, whichever of
//! the two is traced first. Every other check reads "the descriptor in force before an
//! index" off this map: the latest value strictly below it.
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
//! parent (PROPOSED D-100). The split clause reads P's configuration in force at the
//! split's index from a history of the configurations each range committed, which a
//! right half's creation seeds at its floor (PROPOSED D-106).
//!
//! **Each fold has a verdict of its own** ([`Fold`], [`Checker::verdict_of`]), so that a
//! run in which two folds are in violation shows both: [`Checker::verdict`], which
//! stops a run, reports the first of them in the order 7, 9, 10, 16, 17, 18, and a
//! catch read through it alone would credit every write applied outside its span to
//! check 9 and never show check 10 trip on the same write (PROPOSED D-106).
//!
//! The keys the trace carries are user keys; the spans are encoded (§1), so every
//! comparison encodes the key as the store does (`ananke_raft::apply::user_key`).
// PROPOSED(D-097): checks 7, 9, 10 and 17.
// PROPOSED(D-098): check 16, and check 9's lookups.
// PROPOSED(D-106): a verdict per fold, and check 18's configuration history.

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
    violation: Option<String>,
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
                    // An install: it must equal the latest value at or below its
                    // index, or be the first value traced there.
                    _ => match self.at(*range, *floor_index) {
                        Some(agreed) if *agreed == held => {}
                        Some(agreed) => {
                            if self.violation.is_none() {
                                self.violation = Some(format!(
                                    "descriptor agreement: node {node:?}'s replica of range \
                                     {range} was installed at index {floor_index} with \
                                     {held:?}, where the range's descriptor there is \
                                     {agreed:?}"
                                ));
                            }
                        }
                        None => self.put(*range, *floor_index, held, node, "installed"),
                    },
                }
            }
            TraceEvent::RangeDescriptor {
                range,
                index,
                start,
                end,
                generation,
                voters,
                state,
                ..
            } => {
                let held = Held {
                    start: start.clone(),
                    end: end.clone(),
                    generation: *generation,
                    voters: voters.clone(),
                    state: *state,
                };
                self.put(*range, *index, held, node, "changed");
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

/// One configuration a range may have had in force from an index, as check 18's
/// history holds it (SHARD.md §8, check 11's history, which check 18 folds).
// PROPOSED(D-106): check 18's configuration history keyed on the configuration in force.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Candidate {
    /// The configuration entry's term and payload hash, as the `RaftAppend` that put it
    /// in the log traced them; `None` for a configuration in force by construction, a
    /// range's creation at its floor, which no entry carries.
    entry: Option<(u64, u64)>,
    /// Whether it is joint.
    joint: bool,
    /// The voters, or the old voters while joint.
    voters: Vec<u64>,
}

/// Check 18's split clause (SHARD.md §8; §5), at the first apply of each split: with
/// P's descriptor before `s` `[x, y)` at g, P's value at `s` is `[x, k)` at g + 1 with
/// P's voters; R's `RangeCreated { cause: split }` is `[k, y)` at g + 1 with the voters
/// of P's plain configuration in force at `s`, floor index `s` and floor term 1 (Q20);
/// R is fresh — no `RangeCreated` of R and no `RangeSplit` naming R before that split's
/// first `RangeSplit` — and R lies in a block a `RangeIdsLeased` traced before it
/// granted to the node that led P in the split entry's term (Q17). Every replica's
/// `RangeSplit` of (P, `s`) names the same right half and key.
///
/// **The configuration in force at `s`** is the one the committed log puts there: the
/// latest configuration at or below `s` that the range committed. The core traces a
/// `RaftConfig` when it *appends* a configuration entry (`Raft::adopt`, RAFT.md §1), so
/// an entry that is later truncated and replaced at the same index was traced too, and
/// the first `RaftConfig` traced at an index is not the one in force (issue #138). So a
/// configuration is held with the entry that carried it — the term and hash of the
/// `RaftAppend` its own server traced just before it — and counts at an index only when
/// the range's first `RaftApply` there applied that entry. A `RaftConfig` traced with no
/// such append before it is a restatement or a revert to an entry already traced at its
/// own append, and adds nothing. The configurations no entry carries are a creation's: a
/// bootstrap's at index 0 and a right half's at its floor `s`, where its core starts from
/// P's configuration and traces none (`restore_compacted`), so a split of a right half
/// reads the configuration its creation names (issue #138).
// PROPOSED(D-100)
// PROPOSED(D-106): check 18's configuration history keyed on the configuration in force.
#[derive(Clone, Debug, Default)]
pub struct SplitLineage {
    /// Each range's configurations by the index they are in force from, every one
    /// traced there, with the entry that carried it.
    configs: BTreeMap<u64, BTreeMap<u64, Vec<Candidate>>>,
    /// The entry each (range, server)'s last `RaftAppend` traced, until its next
    /// record of that replica's log or configuration: index, term and hash.
    appended: BTreeMap<(u64, u64), (u64, u64, u64)>,
    /// The entry each (range, index) holding a configuration was first applied as:
    /// its term and hash, the committed entry there (check 4).
    committed: BTreeMap<(u64, u64), (u64, u64)>,
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

    /// The voters of `range`'s configuration in force at `index`, and whether it is
    /// joint: the latest at or below it that the range committed — carried by the entry
    /// the range first applied at its index, or in force by a creation.
    // PROPOSED(D-106): check 18's configuration history keyed on the configuration in force.
    fn config_at(&self, range: u64, index: u64) -> Option<(bool, Vec<u64>)> {
        self.configs
            .get(&range)?
            .range(..=index)
            .rev()
            .find_map(|(at, candidates)| {
                let committed = self.committed.get(&(range, *at));
                candidates
                    .iter()
                    .find(|candidate| {
                        candidate.entry.is_none() || candidate.entry.as_ref() == committed
                    })
                    .map(|candidate| (candidate.joint, candidate.voters.clone()))
            })
    }

    /// Holds `candidate` as one of `range`'s configurations from `index`.
    fn hold(&mut self, range: u64, index: u64, candidate: Candidate) {
        let held = self
            .configs
            .entry(range)
            .or_default()
            .entry(index)
            .or_default();
        if !held.contains(&candidate) {
            held.push(candidate);
        }
    }

    /// Folds one record against check 7's map and check 18's range-id clause as they
    /// stand.
    pub fn push(&mut self, record: &TraceRecord, descriptors: &Descriptors, ids: &IdsUnique) {
        if self.violation.is_some() {
            return;
        }
        let node = record.node.map(|node| u64::from(node.get()));
        match &record.event {
            // PROPOSED(D-106): the configuration history keyed on the configuration in
            // force — each configuration held with the entry its own append traced.
            TraceEvent::RaftAppend {
                server,
                range,
                index,
                entry_term,
                hash,
            } => {
                self.appended
                    .insert((*range, *server), (*index, *entry_term, *hash));
            }
            TraceEvent::RaftTruncate { server, range, .. } => {
                self.appended.remove(&(*range, *server));
            }
            TraceEvent::RaftConfig {
                server,
                range,
                index,
                old,
                joint,
                ..
            } => {
                if let Some((appended, term, hash)) = self.appended.remove(&(*range, *server))
                    && appended == *index
                {
                    self.hold(
                        *range,
                        *index,
                        Candidate {
                            entry: Some((term, hash)),
                            joint: *joint,
                            voters: old.clone(),
                        },
                    );
                }
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
                voters,
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
                // A bootstrap's configuration at index 0 and a right half's at its
                // floor are in force by the creation, and no entry carries them; an
                // install's is its snapshot's, whose entries their own appends traced,
                // and a creation names no joint configuration's other half.
                // PROPOSED(D-106): a right half's history seeded from its creation.
                if matches!(cause, RangeCause::Bootstrap | RangeCause::Split) {
                    self.hold(
                        *range,
                        *floor_index,
                        Candidate {
                            entry: None,
                            joint: false,
                            voters: voters.clone(),
                        },
                    );
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
                hash,
                effect,
                ..
            } => {
                // The entry a range first applied at an index holding a configuration
                // is the committed one there (check 4 holds every later apply to it).
                // PROPOSED(D-106): the configuration history keyed on the one in force.
                if self
                    .configs
                    .get(range)
                    .is_some_and(|held| held.contains_key(index))
                {
                    self.committed
                        .entry((*range, *index))
                        .or_insert((*entry_term, *hash));
                }
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

/// One of the range layer's seven folds: SHARD.md §8's checks 7, 9, 10, 16 and 17, and
/// check 18's two clauses, each of which keeps a verdict of its own in [`Checker`].
// PROPOSED(D-106): a verdict per fold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Fold {
    /// Check 7, descriptor agreement ([`Descriptors`]).
    DescriptorAgreement,
    /// Check 9, serving within span ([`ServingWithinSpan`]).
    ServingWithinSpan,
    /// Check 10, owners' generations rise ([`GenerationsRise`]).
    GenerationsRise,
    /// Check 16, the meta range never goes back ([`MetaNeverGoesBack`]).
    MetaNeverGoesBack,
    /// Check 17, a client refreshes on a mismatch ([`ClientRefreshes`]).
    ClientRefreshes,
    /// Check 18's range-id clause ([`IdsUnique`]).
    RangeIds,
    /// Check 18's split clause ([`SplitLineage`]).
    SplitLineage,
}

impl Fold {
    /// Every fold, in the order [`Checker::verdict`] asks them: 7, 9, 10, 16, 17, 18.
    pub const ALL: [Self; 7] = [
        Self::DescriptorAgreement,
        Self::ServingWithinSpan,
        Self::GenerationsRise,
        Self::MetaNeverGoesBack,
        Self::ClientRefreshes,
        Self::RangeIds,
        Self::SplitLineage,
    ];

    /// The check of SHARD.md §8 the fold is; check 18 is two folds.
    #[must_use]
    pub const fn check(self) -> u32 {
        match self {
            Self::DescriptorAgreement => 7,
            Self::ServingWithinSpan => 9,
            Self::GenerationsRise => 10,
            Self::MetaNeverGoesBack => 16,
            Self::ClientRefreshes => 17,
            Self::RangeIds | Self::SplitLineage => 18,
        }
    }

    /// The words every violation of the fold begins with.
    #[must_use]
    pub const fn words(self) -> &'static str {
        match self {
            Self::DescriptorAgreement => "descriptor agreement:",
            Self::ServingWithinSpan => "serving within span:",
            Self::GenerationsRise => "owners' generations:",
            Self::MetaNeverGoesBack => "meta never goes back:",
            Self::ClientRefreshes => "client refreshes:",
            Self::RangeIds => "range ids:",
            Self::SplitLineage => "descriptor lineage:",
        }
    }
}

impl std::fmt::Display for Fold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let words = self.words();
        write!(f, "check {} ({})", self.check(), &words[..words.len() - 1])
    }
}

/// The seven folds — checks 7, 9, 10, 16 and 17, and check 18's two clauses — as one
/// incremental checker: fed the records since its last look, with a verdict at any
/// prefix that is what the folds over the whole prefix report, in the same words
/// (D-046), for each fold on its own ([`Checker::verdict_of`]) and for the seven
/// together ([`Checker::verdict`]).
///
/// A checker built `active: false` folds nothing and reports nothing: what a run of
/// the one-group server, which traces no descriptor, is checked with.
#[derive(Clone, Debug, Default)]
pub struct Checker {
    active: bool,
    descriptors: Descriptors,
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

    /// `fold`'s own verdict: its first violation, whatever the other folds found.
    ///
    /// # Errors
    ///
    /// The fold's violation, in words naming it ([`Fold::words`]).
    // PROPOSED(D-106): a verdict per fold.
    pub fn verdict_of(&self, fold: Fold) -> Result<(), String> {
        match fold {
            Fold::DescriptorAgreement => self.descriptors.verdict(),
            Fold::ServingWithinSpan => self.serving.verdict(),
            Fold::GenerationsRise => self.generations.verdict(),
            Fold::MetaNeverGoesBack => self.meta.verdict(),
            Fold::ClientRefreshes => self.refreshes.verdict(),
            Fold::RangeIds => self.ids.verdict(),
            Fold::SplitLineage => self.lineage.verdict(),
        }
    }

    /// Every fold's own verdict, in check order.
    // PROPOSED(D-106): a verdict per fold.
    pub fn verdicts(&self) -> [(Fold, Result<(), String>); 7] {
        Fold::ALL.map(|fold| (fold, self.verdict_of(fold)))
    }

    /// The first violation, in check order: 7, 9, 10, 16, 17, 18. This is what stops a
    /// run, and it names one fold where several may be in violation: read
    /// [`Checker::verdict_of`] for what each fold found.
    ///
    /// # Errors
    ///
    /// The violation, in words naming it.
    pub fn verdict(&self) -> Result<(), String> {
        Fold::ALL
            .into_iter()
            .try_for_each(|fold| self.verdict_of(fold))
    }
}

/// The checker folded over `records` from the first: the whole-trace form of
/// [`Checker`], whose verdicts, together and per fold, an incremental checker fed the
/// same records in pieces must equal (D-046).
// PROPOSED(D-106): a verdict per fold.
#[must_use]
pub fn folded(records: &[TraceRecord]) -> Checker {
    let mut checker = Checker::new(true);
    checker.extend(records);
    checker
}

/// The checks folded over `records` from the first: the whole-trace form of
/// [`Checker::verdict`].
///
/// # Errors
///
/// The first violation, in check order.
pub fn all(records: &[TraceRecord]) -> Result<(), String> {
    folded(records).verdict()
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
        let mut higher = created(1, 3, b"a", b"k", 2, &[1, 2, 3]);
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

    /// A split whose right half's creation names voters other than P's plain
    /// configuration at `s`, and one whose P value at `s` names voters other than P's,
    /// are each caught, each in its own words (issue #150; D-100's item 8 listed the
    /// case and the tree had none).
    // PROPOSED(D-106)
    #[test]
    fn a_split_that_leaves_wrong_voters_on_either_half_is_caught() {
        let mut right = Checker::new(true);
        right.extend(&before_a_split());
        let mut records = split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6);
        if let TraceEvent::RangeCreated { voters, .. } = &mut records[2].event {
            *voters = vec![1, 2, 4];
        }
        right.extend(&records);
        let verdict = right.verdict_of(Fold::SplitLineage).unwrap_err();
        assert!(
            verdict.starts_with("descriptor lineage: right half 6's creation at index 9"),
            "{verdict}"
        );
        assert!(verdict.contains("voters: [1, 2, 4]"), "{verdict}");

        let mut left = Checker::new(true);
        left.extend(&before_a_split());
        let mut records = split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6);
        if let TraceEvent::RangeDescriptor { voters, .. } = &mut records[1].event {
            *voters = vec![1, 2, 5];
        }
        left.extend(&records);
        let verdict = left.verdict_of(Fold::SplitLineage).unwrap_err();
        assert!(
            verdict.starts_with("descriptor lineage: range 2's value at its split's index 9"),
            "{verdict}"
        );
        assert!(verdict.contains("voters: [1, 2, 5]"), "{verdict}");
    }

    /// A split of a right half: the half's core starts from its floor with P's
    /// configuration and traces no `RaftConfig` for it, so the configuration in force
    /// at its own split is the one its creation names (issue #138). Before PROPOSED
    /// D-106 the history held only traced configurations, and the second split failed
    /// the correct trace with "no plain configuration in force".
    // PROPOSED(D-106)
    #[test]
    fn a_split_of_a_right_half_reads_the_configuration_its_creation_names() {
        let mut checker = Checker::new(true);
        checker.extend(&before_a_split());
        checker.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        checker.push(&record(
            1,
            TraceEvent::RaftLeader {
                server: 1,
                range: 6,
                term: 2,
                last_index: 14,
            },
        ));
        checker.extend(&split_records(1, 6, 15, 2, b"t", b"m", b"z", 2, 7));
        checker.extend(&split_records(2, 6, 15, 2, b"t", b"m", b"z", 2, 7));
        assert!(checker.verdict().is_ok(), "{:?}", checker.verdict());
        assert_eq!(
            checker.descriptors().at(7, 15).map(|held| held.generation),
            Some(3)
        );
        // And what is read is the configuration in force at the half's own split: a
        // joint one the half committed below it refuses that split (Q23), as on any
        // range.
        let mut joint = Checker::new(true);
        joint.extend(&before_a_split());
        joint.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        joint.push(&record(
            1,
            TraceEvent::RaftLeader {
                server: 1,
                range: 6,
                term: 2,
                last_index: 14,
            },
        ));
        joint.push(&record(
            1,
            TraceEvent::RaftAppend {
                server: 1,
                range: 6,
                index: 12,
                entry_term: 2,
                hash: 31,
            },
        ));
        joint.push(&record(
            1,
            TraceEvent::RaftConfig {
                server: 1,
                range: 6,
                index: 12,
                old: vec![1, 2, 3],
                new: vec![1, 2, 4],
                joint: true,
                learners: vec![],
            },
        ));
        let mut entry = applied(1, 6, 12, b"q", ApplyEffect::None);
        if let TraceEvent::RaftApply {
            entry_term,
            hash,
            key,
            ..
        } = &mut entry.event
        {
            (*entry_term, *hash, *key) = (2, 31, None);
        }
        joint.push(&entry);
        joint.extend(&split_records(1, 6, 15, 2, b"t", b"m", b"z", 2, 7));
        let verdict = joint.verdict_of(Fold::SplitLineage).unwrap_err();
        assert!(
            verdict.starts_with("descriptor lineage: range 6's split at index 15 took effect"),
            "{verdict}"
        );
    }

    /// The configuration entries a trace shows at one index of range 2: server 3
    /// appends a joint configuration there in term 2, and `committed` is what the
    /// range's first apply of that index applied — that entry, or a no-op of term 3
    /// that replaced it after a truncation.
    fn a_joint_entry_at_5(committed: (u64, u64)) -> Vec<TraceRecord> {
        vec![
            record(
                3,
                TraceEvent::RaftAppend {
                    server: 3,
                    range: 2,
                    index: 5,
                    entry_term: 2,
                    hash: 77,
                },
            ),
            record(
                3,
                TraceEvent::RaftConfig {
                    server: 3,
                    range: 2,
                    index: 5,
                    old: vec![1, 2, 3],
                    new: vec![1, 2, 4],
                    joint: true,
                    learners: vec![],
                },
            ),
            record(
                3,
                TraceEvent::RaftTruncate {
                    server: 3,
                    range: 2,
                    from_index: 5,
                },
            ),
            // The revert to the configuration in force below it, traced with no
            // append before it: a restatement, which adds nothing.
            record(
                3,
                TraceEvent::RaftConfig {
                    server: 3,
                    range: 2,
                    index: 0,
                    old: vec![9],
                    new: vec![],
                    joint: false,
                    learners: vec![],
                },
            ),
            record(
                1,
                TraceEvent::RaftApply {
                    server: 1,
                    range: 2,
                    index: 5,
                    entry_term: committed.0,
                    hash: committed.1,
                    key: None,
                    effect: ApplyEffect::None,
                },
            ),
        ]
    }

    /// A configuration entry the core traced at its append and then truncated is not
    /// the configuration in force at its index: the core traces `RaftConfig` at
    /// append, committed or not, and before PROPOSED D-106 the first one traced at an
    /// index stood (issue #138). The pair: the same joint entry, committed, is in force
    /// and still refuses the split (Q23).
    // PROPOSED(D-106)
    #[test]
    fn a_configuration_truncated_at_its_index_is_not_the_one_in_force_and_a_committed_one_is() {
        let mut truncated = Checker::new(true);
        truncated.extend(&before_a_split());
        truncated.extend(&a_joint_entry_at_5((3, 55)));
        truncated.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        assert!(truncated.verdict().is_ok(), "{:?}", truncated.verdict());

        let mut committed = Checker::new(true);
        committed.extend(&before_a_split());
        committed.extend(&a_joint_entry_at_5((2, 77)));
        committed.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        let verdict = committed.verdict_of(Fold::SplitLineage).unwrap_err();
        assert!(
            verdict.contains("with no plain configuration in force"),
            "{verdict}"
        );
    }

    /// The history is a range's own: a joint configuration another range committed at the
    /// split's own index is not the splitting range's, and the split is in shape (Q23 asks
    /// it of P's configuration alone). With one range every configuration is P's and a
    /// history keyed by the index alone says the same; with two it refuses this split.
    // PROPOSED(D-106)
    #[test]
    fn a_configuration_another_range_committed_is_not_the_splitting_ranges() {
        let mut checker = Checker::new(true);
        checker.extend(&before_a_split());
        checker.push(&created(1, 3, b"z", b"zz", 1, &[1, 2, 3]));
        checker.push(&record(
            1,
            TraceEvent::RaftAppend {
                server: 1,
                range: 3,
                index: 9,
                entry_term: 2,
                hash: 77,
            },
        ));
        checker.push(&record(
            1,
            TraceEvent::RaftConfig {
                server: 1,
                range: 3,
                index: 9,
                old: vec![1, 2, 3],
                new: vec![1, 2, 4],
                joint: true,
                learners: vec![],
            },
        ));
        checker.push(&record(
            1,
            TraceEvent::RaftApply {
                server: 1,
                range: 3,
                index: 9,
                entry_term: 2,
                hash: 77,
                key: None,
                effect: ApplyEffect::None,
            },
        ));
        checker.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        assert!(checker.verdict().is_ok(), "{:?}", checker.verdict());
    }

    /// Two folds in violation on one record — a write applied in a range whose span no
    /// longer holds its key, to a key its sibling wrote at the same generation — are
    /// each reported by their own fold, and the checker's verdict names the first in
    /// check order. Read through that verdict alone, check 10's violation is never seen,
    /// which is how every catch of `ApplyIgnoresSpan` read as check 9's before PROPOSED
    /// D-106.
    // PROPOSED(D-106)
    #[test]
    fn each_fold_keeps_its_own_verdict_and_the_checker_names_the_first() {
        let mut checker = Checker::new(true);
        checker.extend(&before_a_split());
        checker.extend(&split_records(1, 2, 9, 2, b"m", b"a", b"z", 1, 6));
        checker.push(&applied(1, 6, 10, b"q", ApplyEffect::Applied));
        checker.verdict().unwrap();
        // Range 2, now `[a, m)` at generation 2, writes `q`, which its right half
        // wrote at generation 2: outside its span (check 9) and a sibling's write at a
        // generation not above it (check 10).
        checker.push(&applied(1, 2, 11, b"q", ApplyEffect::Applied));
        let nine = checker.verdict_of(Fold::ServingWithinSpan).unwrap_err();
        let ten = checker.verdict_of(Fold::GenerationsRise).unwrap_err();
        assert!(nine.starts_with(Fold::ServingWithinSpan.words()), "{nine}");
        assert!(ten.starts_with(Fold::GenerationsRise.words()), "{ten}");
        assert_eq!(checker.verdict(), Err(nine));
        for (fold, verdict) in checker.verdicts() {
            if !matches!(fold, Fold::ServingWithinSpan | Fold::GenerationsRise) {
                assert_eq!(verdict, Ok(()), "{fold}");
            }
        }
        assert_eq!(
            Fold::ALL.map(Fold::check),
            [7, 9, 10, 16, 17, 18, 18],
            "the order the checker's verdict asks the folds in"
        );
        assert_eq!(
            Fold::GenerationsRise.to_string(),
            "check 10 (owners' generations)"
        );
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
        let mut higher = created(1, 3, b"k", b"z", 2, &[1, 2, 3]);
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
