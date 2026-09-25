//! The meta range's state machine and its lookups (SHARD.md §1; Q3, Q4, Q36).
//!
//! The meta range holds a record per descriptor it has been told, for addressing:
//! under the meta span in the system tenant, keyed by the span's end key and carrying
//! its start, range, generation and voters (`ananke_shard::system::MetaRecord`). It is
//! an index of the range-local authority that lags and self-corrects (Q3): written
//! after the change it records by a request to another Raft group, so it may lag; it
//! never goes back.
//!
//! **The update.** `MetaUpdate { descriptors }` is applied by [`apply_update`]: for
//! each descriptor `d`, every maximal sub-interval of `d`'s span that a record of
//! lower generation names, or that nothing names, is from then on named by `d`
//! restricted to it; a record partly overwritten is cut at `d`'s boundaries in the
//! same batch; a sub-interval a record of `d`'s generation or a higher one names is
//! left alone. The state after any set of updates is a maximum by generation per key
//! and does not depend on their order, so an update may be sent any number of times
//! and a resend changes nothing (§1). [`merge`] is that rule over the records as a
//! map, pure, and what the unit tests exercise; `apply_update` reads the records at
//! the apply's version, merges every descriptor of the update in order over one map,
//! and writes the difference.
//!
//! **The variant.** `NodeVariant::MetaOverwritesByArrival` stores each update as it
//! arrives: every overlapping record is cut and replaced whatever its generation, so
//! an older update that arrives after a newer one takes the newer's place — the
//! regression check 16 sees at the first key whose named generation falls (§8, §10).
//!
//! **The lookups.** [`lookup`] reads the first record whose end key is above the
//! encoded key, by the engine's bounded seek (D-055), and answers it as a descriptor
//! with that record's end, or nothing when no record covers the key;
//! [`meta_descriptor`] reads range 0's copy of the meta range's descriptor. Both are
//! served at the version a read is served at (§3), and neither enters the history
//! (Q36).
// PROPOSED(D-098): the root and the meta range.

use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Unbounded};

use ananke_env::{Environment, MetaDescriptor, RangeState};
use ananke_storage::{Engine, Snapshot, WriteBatch};
use bytes::{BufMut, Bytes, BytesMut};

use crate::descriptor::RangeDescriptor;
use crate::system::{self, MetaRecord};

/// The length of a meta record key's prefix: what precedes the end key it carries.
fn prefix_len() -> usize {
    system::meta_record_key(b"").len()
}

/// Merges `descriptor` into `records` — the meta range's records keyed by end key —
/// by §1's rule, and returns the sub-intervals it won, in key order. With
/// `as_arrived` every overlapping record is replaced whatever its generation: the
/// variant.
pub fn merge(
    records: &mut BTreeMap<Bytes, MetaRecord>,
    descriptor: &RangeDescriptor,
    as_arrived: bool,
) -> Vec<(Bytes, Bytes)> {
    let (start, end) = (descriptor.start.clone(), descriptor.end.clone());
    if start >= end {
        return Vec::new();
    }
    // The records the span overlaps: keyed by end, so every record whose end lies
    // past the span's start, up to the first whose start lies at or past its end.
    let overlapping: Vec<Bytes> = records
        .range::<[u8], _>((Excluded(&start[..]), Unbounded))
        .take_while(|(_, record)| record.start < end)
        .map(|(key, _)| key.clone())
        .collect();
    // The parts of the span a record of the same or a higher generation keeps.
    let mut kept: Vec<(Bytes, Bytes)> = Vec::new();
    for key in overlapping {
        let record = records.remove(&key).expect("listed");
        if !as_arrived && record.generation >= descriptor.generation {
            kept.push((
                record.start.clone().max(start.clone()),
                key.clone().min(end.clone()),
            ));
            records.insert(key, record);
            continue;
        }
        // A lower generation, or the variant: replaced for the overlapped part, cut
        // at the span's boundaries and kept outside them.
        if record.start < start {
            records.insert(
                start.clone(),
                MetaRecord {
                    start: record.start.clone(),
                    ..record.clone()
                },
            );
        }
        if key > end {
            records.insert(
                key.clone(),
                MetaRecord {
                    start: end.clone(),
                    ..record
                },
            );
        }
    }
    kept.sort();
    let mut won: Vec<(Bytes, Bytes)> = Vec::new();
    let mut cursor = start;
    for (from, to) in kept {
        if cursor < from {
            won.push((cursor.clone(), from.clone()));
        }
        if to > cursor {
            cursor = to;
        }
    }
    if cursor < end {
        won.push((cursor, end));
    }
    for (from, to) in &won {
        records.insert(
            to.clone(),
            MetaRecord {
                start: from.clone(),
                range: descriptor.range,
                generation: descriptor.generation,
                voters: descriptor.voters.clone(),
            },
        );
    }
    won
}

/// The records under the meta span at `snapshot`, keyed by end key.
///
/// # Errors
///
/// The engine's, or a record that does not decode.
pub async fn records<E: Environment>(
    engine: &Engine<E>,
    snapshot: &Snapshot<E>,
) -> io::Result<BTreeMap<Bytes, MetaRecord>> {
    let span = system::meta_span();
    let prefix = prefix_len();
    let mut out = BTreeMap::new();
    for (key, value) in engine
        .scan(&span.start[..]..&span.end[..], snapshot)
        .await?
    {
        if key.len() < prefix {
            continue;
        }
        out.insert(key.slice(prefix..), MetaRecord::decode(value)?);
    }
    Ok(out)
}

/// Applies a `MetaUpdate` to the meta range's records as the engine holds them: the
/// batch that writes the difference, and every descriptor with the sub-intervals it
/// won, for `MetaApplied`.
///
/// # Errors
///
/// The engine's, or a record that does not decode.
pub async fn apply_update<E: Environment>(
    engine: &Engine<E>,
    descriptors: &[RangeDescriptor],
    as_arrived: bool,
) -> io::Result<(WriteBatch, Vec<MetaDescriptor>)> {
    let before = {
        let snapshot = engine.snapshot();
        records(engine, &snapshot).await?
    };
    let mut after = before.clone();
    let mut applied = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        let won = merge(&mut after, descriptor, as_arrived);
        applied.push(MetaDescriptor {
            range: descriptor.range.get(),
            start: descriptor.start.clone(),
            end: descriptor.end.clone(),
            generation: descriptor.generation,
            voters: descriptor.voters.iter().map(|voter| voter.0).collect(),
            won,
        });
    }
    let mut batch = WriteBatch::new();
    for end in before.keys() {
        if !after.contains_key(end) {
            batch.delete(system::meta_record_key(end));
        }
    }
    for (end, record) in &after {
        if before.get(end) != Some(record) {
            batch.put(system::meta_record_key(end), record.encode());
        }
    }
    Ok((batch, applied))
}

/// The descriptor of the range whose span holds the encoded `key`, as the meta range's
/// records name it at `snapshot`: the first record whose end key is above `key`,
/// provided its start lies at or below it. `None` where no record covers the key.
///
/// # Errors
///
/// The engine's, or a record that does not decode.
pub async fn lookup<E: Environment>(
    engine: &Engine<E>,
    snapshot: &Snapshot<E>,
    key: &[u8],
) -> io::Result<Option<RangeDescriptor>> {
    // Strictly above: a record keyed by `key` itself ends at it and holds it not.
    let mut from = BytesMut::from(&system::meta_record_key(key)[..]);
    from.put_u8(0);
    let span_end = system::meta_span().end;
    let found = engine.seek(&from[..]..&span_end[..], 1, snapshot).await?;
    let Some((record_key, value)) = found.into_iter().next() else {
        return Ok(None);
    };
    let record = MetaRecord::decode(value)?;
    if record.start[..] > *key {
        return Ok(None);
    }
    Ok(Some(RangeDescriptor {
        range: record.range,
        start: record.start,
        end: record_key.slice(prefix_len()..),
        generation: record.generation,
        voters: record.voters,
        state: RangeState::Live,
    }))
}

/// Range 0's copy of the meta range's descriptor at `snapshot` (SHARD.md §1, §2).
///
/// # Errors
///
/// The engine's, or a descriptor that does not decode.
pub async fn meta_descriptor<E: Environment>(
    engine: &Engine<E>,
    snapshot: &Snapshot<E>,
) -> io::Result<Option<RangeDescriptor>> {
    engine
        .get_at(&system::meta_descriptor_key(), snapshot)
        .await?
        .map(RangeDescriptor::decode)
        .transpose()
}

#[cfg(test)]
mod tests {
    use ananke_raft::types::ServerId;

    use super::*;
    use crate::range::RangeId;

    fn descriptor(range: u64, start: &str, end: &str, generation: u64) -> RangeDescriptor {
        RangeDescriptor {
            range: RangeId(range),
            start: Bytes::copy_from_slice(start.as_bytes()),
            end: Bytes::copy_from_slice(end.as_bytes()),
            generation,
            voters: vec![ServerId(1), ServerId(2), ServerId(3)],
            state: RangeState::Live,
        }
    }

    fn spans(records: &BTreeMap<Bytes, MetaRecord>) -> Vec<(u64, String, String, u64)> {
        records
            .iter()
            .map(|(end, record)| {
                (
                    record.range.get(),
                    String::from_utf8_lossy(&record.start).into_owned(),
                    String::from_utf8_lossy(end).into_owned(),
                    record.generation,
                )
            })
            .collect()
    }

    fn won(intervals: &[(Bytes, Bytes)]) -> Vec<(String, String)> {
        intervals
            .iter()
            .map(|(s, e)| {
                (
                    String::from_utf8_lossy(s).into_owned(),
                    String::from_utf8_lossy(e).into_owned(),
                )
            })
            .collect()
    }

    fn s(a: &str, b: &str) -> (String, String) {
        (a.to_owned(), b.to_owned())
    }

    #[test]
    fn the_first_update_names_its_whole_span_and_a_resend_names_nothing() {
        let mut records = BTreeMap::new();
        let d = descriptor(2, "a", "k", 1);
        assert_eq!(won(&merge(&mut records, &d, false)), vec![s("a", "k")]);
        assert_eq!(spans(&records), vec![(2, "a".into(), "k".into(), 1)]);
        assert!(merge(&mut records, &d, false).is_empty());
        assert_eq!(spans(&records).len(), 1);
    }

    #[test]
    fn a_higher_generation_takes_the_part_it_overlaps_and_cuts_the_record_it_overwrites() {
        let mut records = BTreeMap::new();
        merge(&mut records, &descriptor(2, "a", "k", 1), false);
        merge(&mut records, &descriptor(3, "k", "z", 1), false);
        // The right half's right half, split off at generation 2.
        let w = merge(&mut records, &descriptor(4, "p", "z", 2), false);
        assert_eq!(won(&w), vec![s("p", "z")]);
        assert_eq!(
            spans(&records),
            vec![
                (2, "a".into(), "k".into(), 1),
                (3, "k".into(), "p".into(), 1),
                (4, "p".into(), "z".into(), 2),
            ]
        );
        // The left half's own new descriptor at generation 2, over its cut span.
        let w = merge(&mut records, &descriptor(3, "k", "p", 2), false);
        assert_eq!(won(&w), vec![s("k", "p")]);
        assert_eq!(spans(&records)[1], (3, "k".into(), "p".into(), 2));
    }

    #[test]
    fn a_stale_update_after_a_newer_one_names_nothing_and_the_variant_regresses() {
        let mut correct = BTreeMap::new();
        merge(&mut correct, &descriptor(3, "k", "z", 1), false);
        merge(&mut correct, &descriptor(4, "p", "z", 2), false);
        merge(&mut correct, &descriptor(3, "k", "p", 2), false);
        let mut regressed = correct.clone();
        // The stale update: the parent at generation 1 over the whole old span,
        // arriving after the split's two updates.
        let stale = descriptor(3, "k", "z", 1);
        assert!(merge(&mut correct, &stale, false).is_empty());
        assert_eq!(
            spans(&correct),
            vec![
                (3, "k".into(), "p".into(), 2),
                (4, "p".into(), "z".into(), 2)
            ]
        );
        // As it arrives: the stale record takes the whole span back.
        assert_eq!(won(&merge(&mut regressed, &stale, true)), vec![s("k", "z")]);
        assert_eq!(spans(&regressed), vec![(3, "k".into(), "z".into(), 1)]);
    }

    #[test]
    fn a_record_of_a_higher_generation_inside_the_span_is_left_alone_and_the_rest_is_won() {
        let mut records = BTreeMap::new();
        merge(&mut records, &descriptor(2, "a", "z", 1), false);
        merge(&mut records, &descriptor(5, "c", "d", 3), false);
        let w = merge(&mut records, &descriptor(2, "a", "z", 2), false);
        assert_eq!(won(&w), vec![s("a", "c"), s("d", "z")]);
        assert_eq!(
            spans(&records),
            vec![
                (2, "a".into(), "c".into(), 2),
                (5, "c".into(), "d".into(), 3),
                (2, "d".into(), "z".into(), 2),
            ]
        );
    }

    #[test]
    fn an_empty_span_names_nothing() {
        let mut records = BTreeMap::new();
        assert!(merge(&mut records, &descriptor(2, "k", "k", 1), false).is_empty());
        assert!(records.is_empty());
    }
}
