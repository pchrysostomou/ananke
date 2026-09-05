//! The memtable (SPEC.md §2.3, D-020, D-023): every write since the last flush, in
//! memory, ordered by internal key: user key, then newest first. A skiplist
//! (`crossbeam-skiplist`) so that appenders on different tasks insert without a lock
//! and readers walk it in order; its level generator is a constant-seeded xorshift,
//! so a memtable's shape is a function of its inserts.
//!
//! Every write is kept, not only the newest per key, so that a read at a snapshot
//! sees the newest write at or below it, and a scan at a snapshot is consistent
//! whatever is written meanwhile. Compaction drops versions nobody can see any more
//! (D-023).

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::Seq;
use crate::ikey;

/// What a key holds: a value, or a tombstone left by a delete that must shadow older
/// values in older memtables and in SSTables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// The key is present with this value.
    Live(Bytes),
    /// The key was deleted.
    Tombstone,
}

impl Value {
    /// What a reader sees: the value, or nothing for a tombstone.
    #[must_use]
    pub fn live(self) -> Option<Bytes> {
        match self {
            Value::Live(bytes) => Some(bytes),
            Value::Tombstone => None,
        }
    }

    /// The value's bytes, none for a tombstone.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        match self {
            Value::Live(bytes) => bytes.len() as u64,
            Value::Tombstone => 0,
        }
    }
}

/// Per-entry accounting overhead, on top of the key and value bytes.
const ENTRY_OVERHEAD: u64 = 32;

/// Every write since the last flush, by internal key.
#[derive(Debug)]
pub struct Memtable {
    id: u64,
    map: SkipMap<Bytes, Value>,
    bytes: AtomicU64,
    min_seq: AtomicU64,
    max_seq: AtomicU64,
}

impl Memtable {
    /// An empty memtable numbered `id` (per engine open, from 1; it names the memtable
    /// in the trace).
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self {
            id,
            map: SkipMap::new(),
            bytes: AtomicU64::new(0),
            min_seq: AtomicU64::new(u64::MAX),
            max_seq: AtomicU64::new(0),
        }
    }

    /// The memtable's number.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Records the write numbered `seq`. A number is written once, so this never
    /// replaces anything.
    pub fn apply(&self, seq: Seq, key: Bytes, value: Value) {
        let ikey = ikey::encode(&key, seq);
        let size = ikey.len() as u64 + value.bytes() + ENTRY_OVERHEAD;
        self.map.insert(ikey, value);
        self.min_seq.fetch_min(seq, Ordering::Relaxed);
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
        self.bytes.fetch_add(size, Ordering::Relaxed);
    }

    /// The newest write to `key` numbered at or below `snapshot`, if the memtable has
    /// one, with its number.
    #[must_use]
    pub fn get(&self, key: &[u8], snapshot: Seq) -> Option<(Seq, Value)> {
        let from = ikey::encode(key, snapshot);
        let entry = self.map.range(from..).next()?;
        if !ikey::is_user(entry.key(), key) {
            return None;
        }
        Some((ikey::seq_of(entry.key())?, entry.value().clone()))
    }

    /// The first entry from `from` on, as the internal key and the value: a cursor a
    /// scan advances without holding a borrow, by asking again from past the last
    /// key it saw.
    #[must_use]
    pub fn next_from(&self, from: Bound<&[u8]>) -> Option<(Bytes, Value)> {
        let entry = self.map.range::<[u8], _>((from, Bound::Unbounded)).next()?;
        Some((entry.key().clone(), entry.value().clone()))
    }

    /// Every entry in internal-key order, decoded: user key, sequence number, value.
    #[must_use]
    pub fn entries(&self) -> Vec<(Bytes, Seq, Value)> {
        self.map
            .iter()
            .filter_map(|e| {
                let (user, seq) = ikey::decode(e.key()).ok()?;
                Some((user, seq, e.value().clone()))
            })
            .collect()
    }

    /// Writes held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether nothing was written.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// The bytes the entries account for: keys, values and a fixed overhead each. What
    /// the engine compares with `memtable_bytes`.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// The highest sequence number applied.
    #[must_use]
    pub fn max_seq(&self) -> Seq {
        self.max_seq.load(Ordering::Relaxed)
    }

    /// The lowest sequence number applied; `u64::MAX` when nothing was.
    #[must_use]
    pub fn min_seq(&self) -> Seq {
        self.min_seq.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn every_version_is_kept_and_a_snapshot_sees_the_newest_at_or_below_it() {
        let m = Memtable::new(1);
        m.apply(2, b("k"), Value::Live(b("second")));
        m.apply(1, b("k"), Value::Live(b("first")));
        m.apply(3, b("k"), Value::Tombstone);
        assert_eq!(m.get(b"k", ikey::LATEST), Some((3, Value::Tombstone)));
        assert_eq!(m.get(b"k", 2), Some((2, Value::Live(b("second")))));
        assert_eq!(m.get(b"k", 1), Some((1, Value::Live(b("first")))));
        assert_eq!(m.get(b"k", 0), None);
        assert_eq!(m.get(b"j", ikey::LATEST), None);
        assert_eq!(m.get(b"kk", ikey::LATEST), None);
        assert_eq!((m.min_seq(), m.max_seq(), m.len()), (1, 3, 3));
    }

    #[test]
    fn entries_come_in_key_order_newest_first_and_bytes_add_up() {
        let m = Memtable::new(1);
        m.apply(1, b("b"), Value::Live(b("1")));
        m.apply(2, b("a"), Value::Live(b("22")));
        m.apply(3, b("c"), Value::Tombstone);
        m.apply(4, b("a"), Value::Live(b("2222")));
        let entries: Vec<(Bytes, Seq)> = m.entries().into_iter().map(|(k, s, _)| (k, s)).collect();
        assert_eq!(
            entries,
            vec![(b("a"), 4), (b("a"), 2), (b("b"), 1), (b("c"), 3)]
        );
        // Each key is one byte, ten more encoded.
        assert_eq!(m.bytes(), 4 * (ENTRY_OVERHEAD + 11) + 1 + 2 + 4);
        let mut cursor: Option<Bytes> = None;
        let mut seen = Vec::new();
        while let Some((ikey, _)) =
            m.next_from(cursor.as_deref().map_or(Bound::Unbounded, Bound::Excluded))
        {
            seen.push(ikey::decode(&ikey).unwrap().1);
            cursor = Some(ikey);
        }
        assert_eq!(seen, vec![4, 2, 1, 3]);
        assert_eq!(
            m.next_from(Bound::Included(&ikey::lower_bound(b"b")))
                .map(|(k, _)| ikey::decode(&k).unwrap()),
            Some((b("b"), 1))
        );
    }
}
