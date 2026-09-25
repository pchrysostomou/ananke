//! SHARD.md §8's checks 7, 9, 10 and 17 as folds over the trace, in the range layer
//! where Q40 puts checks 7 to 22: descriptor agreement, serving within span, owners'
//! generations rising, and a client refreshing on a mismatch. Each is a fold fed one
//! record at a time, with a verdict readable at any prefix, so one [`Checker`] runs
//! them incrementally beside `ananke_raft::invariants`'s and again over the whole
//! trace at the end (D-046; §8).
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
//! The keys the trace carries are user keys; the spans are encoded (§1), so every
//! comparison encodes the key as the store does (`ananke_raft::apply::user_key`).
// PROPOSED(D-097): checks 7, 9, 10 and 17.
// PROPOSED(D-098): check 16, and check 9's lookups.

use std::collections::BTreeMap;
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

/// Check 18's range-id clause, the part a tree without a split reaches (SHARD.md §8;
/// §5, Q17): folding `RangeIdsLeased` by the first apply of each of range 0's
/// indices, no two grants share an id, and every replica's apply of an index grants
/// what its first apply did. The clause's other half — the right half of a split lies
/// in a block granted before it to the node that led the parent — waits on the split.
// PROPOSED(D-099)
#[derive(Clone, Debug, Default)]
pub struct IdsUnique {
    /// The grant at each index of range 0 by its first apply: node, run, first, last.
    granted: BTreeMap<u64, (u64, u64, u64, u64)>,
    /// Every block granted, its first id to its last.
    blocks: BTreeMap<u64, u64>,
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
        if let Some((held_first, held_last)) = self.blocks.range(..=*last).next_back()
            && *held_last >= *first
        {
            self.violation = Some(format!(
                "range ids: the block {first}..={last} granted to node {node} at index {index} \
                 of range 0 shares ids with the block {held_first}..={held_last} granted before \
                 it (SHARD.md §5: blocks are disjoint)"
            ));
            return;
        }
        self.blocks.insert(*first, *last);
    }

    /// The blocks granted so far, first id to last, by first apply.
    #[must_use]
    pub fn blocks(&self) -> &BTreeMap<u64, u64> {
        &self.blocks
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

/// The four checks as one incremental checker: fed the records since its last look,
/// with a verdict at any prefix that is what the folds over the whole prefix report,
/// in the same words (D-046).
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

    /// The first violation, in check order: 7, 9, 10, 16, 17, 18.
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
        self.ids.verdict()
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
