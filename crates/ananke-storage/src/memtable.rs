//! The memtable (SPEC.md §2.3, D-020): the newest write per key, in key order, in
//! memory. A skiplist (`crossbeam-skiplist`) so that appenders on different tasks
//! insert without a lock and readers walk it in order; its level generator is a
//! constant-seeded xorshift, so a memtable's shape is a function of its inserts.
//!
//! Every entry carries the log sequence number of the write that made it, and an
//! insert takes effect only if its number is higher than what is there. Acknowledged
//! writes are applied by whichever task is polled first, so two writes to one key can
//! arrive in either order; the number, not the arrival, decides.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::Seq;

/// What a key holds: a value, or a tombstone left by a delete that must shadow older
/// values in older memtables and, later, in SSTables.
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

    fn bytes(&self) -> u64 {
        match self {
            Value::Live(bytes) => bytes.len() as u64,
            Value::Tombstone => 0,
        }
    }
}

/// One key's newest write.
#[derive(Clone, Debug)]
struct Entry {
    seq: Seq,
    value: Value,
}

/// Per-entry accounting overhead, on top of the key and value bytes.
const ENTRY_OVERHEAD: u64 = 32;

/// The newest write per key, in key order.
#[derive(Debug)]
pub struct Memtable {
    id: u64,
    map: SkipMap<Bytes, Entry>,
    bytes: AtomicU64,
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
            max_seq: AtomicU64::new(0),
        }
    }

    /// The memtable's number.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Records the write numbered `seq` unless a newer one is already there. Returns
    /// whether it took effect.
    pub fn apply(&self, seq: Seq, key: Bytes, value: Value) -> bool {
        let size = key.len() as u64 + value.bytes() + ENTRY_OVERHEAD;
        let previous = self
            .map
            .get(&key)
            .map(|e| e.key().len() as u64 + e.value().value.bytes() + ENTRY_OVERHEAD);
        let entry = self
            .map
            .compare_insert(key, Entry { seq, value }, |existing| existing.seq < seq);
        let won = entry.value().seq == seq;
        if won {
            self.max_seq.fetch_max(seq, Ordering::Relaxed);
            let delta = size as i64 - previous.unwrap_or(0) as i64;
            if delta >= 0 {
                self.bytes.fetch_add(delta as u64, Ordering::Relaxed);
            } else {
                self.bytes
                    .fetch_sub(delta.unsigned_abs(), Ordering::Relaxed);
            }
        }
        won
    }

    /// The newest write to `key`, if the memtable has one.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        self.map.get(key).map(|e| e.value().value.clone())
    }

    /// Every entry in key order, with the sequence number of its write.
    pub fn entries(&self) -> Vec<(Bytes, Seq, Value)> {
        self.map
            .iter()
            .map(|e| (e.key().clone(), e.value().seq, e.value().value.clone()))
            .collect()
    }

    /// Keys held.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn the_highest_sequence_number_wins_regardless_of_arrival_order() {
        let m = Memtable::new(1);
        assert!(m.apply(2, b("k"), Value::Live(b("second"))));
        assert!(!m.apply(1, b("k"), Value::Live(b("first"))));
        assert_eq!(m.get(b"k"), Some(Value::Live(b("second"))));
        assert!(m.apply(3, b("k"), Value::Tombstone));
        assert_eq!(m.get(b"k"), Some(Value::Tombstone));
        assert_eq!(m.get(b"k").unwrap().live(), None);
        assert_eq!(m.max_seq(), 3);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn entries_come_in_key_order_and_bytes_follow_replacements() {
        let m = Memtable::new(1);
        m.apply(1, b("b"), Value::Live(b("1")));
        m.apply(2, b("a"), Value::Live(b("22")));
        m.apply(3, b("c"), Value::Tombstone);
        let keys: Vec<Bytes> = m.entries().into_iter().map(|(k, _, _)| k).collect();
        assert_eq!(keys, vec![b("a"), b("b"), b("c")]);
        assert_eq!(m.bytes(), 3 * ENTRY_OVERHEAD + 3 + 3);
        m.apply(4, b("a"), Value::Live(b("2222")));
        assert_eq!(m.bytes(), 3 * ENTRY_OVERHEAD + 3 + 5);
        m.apply(5, b("a"), Value::Tombstone);
        assert_eq!(m.bytes(), 3 * ENTRY_OVERHEAD + 3 + 1);
    }
}
