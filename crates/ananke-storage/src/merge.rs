//! The merge iterator (D-023): one walk in internal-key order over any number of
//! memtables and tables. Every source yields its entries in that order, and no two
//! sources hold the same write, so the merge is the smallest head each step. What a
//! reader makes of the entries, taking the newest write per user key at or below a
//! snapshot, or keeping every version a compaction must keep, is the caller's.

use std::io;
use std::ops::Bound;
use std::sync::Arc;

use ananke_env::File;
use bytes::Bytes;

use crate::memtable::{Memtable, Value};
use crate::sst::SstIter;

/// One ordered source of entries.
pub enum Source<F: File> {
    /// A memtable, walked by a cursor that asks for the next entry past the last.
    Mem {
        /// The memtable.
        table: Arc<Memtable>,
        /// Where the next entry is asked for from.
        cursor: Bound<Bytes>,
    },
    /// A table on disk.
    Sst(SstIter<F>),
}

impl<F: File> Source<F> {
    /// A memtable from its first entry.
    #[must_use]
    pub fn memtable(table: Arc<Memtable>) -> Self {
        Source::Mem {
            table,
            cursor: Bound::Unbounded,
        }
    }

    async fn seek(&mut self, target: &[u8]) -> io::Result<()> {
        match self {
            Source::Mem { cursor, .. } => {
                *cursor = Bound::Included(Bytes::copy_from_slice(target));
                Ok(())
            }
            Source::Sst(iter) => iter.seek(target).await,
        }
    }

    async fn next(&mut self) -> io::Result<Option<(Bytes, Value)>> {
        match self {
            Source::Mem { table, cursor } => {
                let from = match cursor {
                    Bound::Unbounded => Bound::Unbounded,
                    Bound::Included(key) => Bound::Included(&key[..]),
                    Bound::Excluded(key) => Bound::Excluded(&key[..]),
                };
                let entry = table.next_from(from);
                if let Some((key, _)) = &entry {
                    *cursor = Bound::Excluded(key.clone());
                }
                Ok(entry)
            }
            Source::Sst(iter) => iter.next().await,
        }
    }
}

/// The merge of several sources.
pub struct MergeIter<F: File> {
    sources: Vec<Source<F>>,
    /// Each source's next entry, once loaded.
    heads: Vec<Option<(Bytes, Value)>>,
    loaded: bool,
}

impl<F: File> MergeIter<F> {
    /// A merge over `sources`, from their first entries.
    #[must_use]
    pub fn new(sources: Vec<Source<F>>) -> Self {
        let heads = sources.iter().map(|_| None).collect();
        Self {
            sources,
            heads,
            loaded: false,
        }
    }

    /// Positions every source at the first entry at or after `target`.
    ///
    /// # Errors
    ///
    /// A table read's error.
    pub async fn seek(&mut self, target: &[u8]) -> io::Result<()> {
        for source in &mut self.sources {
            source.seek(target).await?;
        }
        self.loaded = false;
        Ok(())
    }

    /// The next entry in internal-key order: its internal key and value.
    ///
    /// # Errors
    ///
    /// A table read's error.
    pub async fn next(&mut self) -> io::Result<Option<(Bytes, Value)>> {
        if !self.loaded {
            for (i, source) in self.sources.iter_mut().enumerate() {
                self.heads[i] = source.next().await?;
            }
            self.loaded = true;
        }
        let smallest = self
            .heads
            .iter()
            .enumerate()
            .filter_map(|(i, head)| head.as_ref().map(|(key, _)| (i, key)))
            .min_by(|(_, a), (_, b)| a.cmp(b))
            .map(|(i, _)| i);
        let Some(i) = smallest else {
            return Ok(None);
        };
        let entry = self.heads[i].take();
        self.heads[i] = self.sources[i].next().await?;
        Ok(entry)
    }
}
