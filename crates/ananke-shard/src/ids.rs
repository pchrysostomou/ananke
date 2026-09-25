//! A node's block of range ids (SHARD.md §5, Q17): what range 0 grants at a refill's
//! apply, and what the node keeps of a block — in memory alone, so a restart abandons
//! the rest of every block it held.
//!
//! Three rules keep an id from being taken twice, and this module is two of them.
//! *Blocks are disjoint*: [`grant`] is range 0's apply of a refill, one batch that
//! records the block starting at the counter in the asking node's lease record and
//! advances the counter past it; the counter only rises and every replica of range 0
//! applies the same entries. *A node takes ids only from a block of its current run*:
//! [`IdBlocks`] adopts a grant only when it carries the run nonce this run drew, and
//! each block at most once. *A node's position in a block is volatile*: [`IdBlocks`]
//! takes the ids of its block in order, each once, from memory, and writes nothing.
//! A refill's answer lost is a refill sent again, which grants a second block, a gap
//! and harmless; so is a block abandoned at a restart, or one a node stopped using.
//!
//! What a split does on a node whose block has run out while range 0 is unreachable
//! is Stage C's first question (PROPOSED D-092), answered by the slice that builds
//! the split; nothing on this tree takes an id.

use std::collections::BTreeSet;
use std::io;

use ananke_env::Environment;
use ananke_raft::types::ServerId;
use ananke_storage::{Engine, WriteBatch};

use crate::system::{self, FIRST_USER_RANGE, LeaseRecord};

/// Range 0's grant of the next `block` ids to `node`'s run `run` (SHARD.md §5): the
/// batch that writes the node's lease record and the counter past the block, and the
/// record itself, which the refill's answer carries back. `engine` is read as it
/// stands, the state at the entry before this one on the replica applying it, so
/// every replica grants the same block at the same index.
///
/// # Errors
///
/// The engine's, or a counter that does not decode; a counter at the end of `u64`.
// PROPOSED(D-099)
pub async fn grant<E: Environment>(
    engine: &Engine<E>,
    node: ServerId,
    run: u64,
    block: u64,
) -> io::Result<(WriteBatch, LeaseRecord)> {
    let counter = match engine.get(&system::counter_key()).await? {
        Some(bytes) => system::decode_counter(bytes)?,
        None => FIRST_USER_RANGE,
    };
    let first = counter;
    let last = counter
        .checked_add(block.max(1) - 1)
        .ok_or_else(|| io::Error::other("range 0's counter has no id left to grant"))?;
    let record = LeaseRecord { run, first, last };
    let mut batch = WriteBatch::new();
    batch.put(system::lease_key(node), record.encode());
    batch.put(system::counter_key(), system::encode_counter(last + 1));
    Ok((batch, record))
}

/// The node's block of range ids for one run (SHARD.md §5): the run's nonce, the
/// block it takes from and its position in it, and the blocks it adopted, each once.
// PROPOSED(D-099)
#[derive(Debug)]
pub struct IdBlocks {
    /// The nonce drawn at this run's start; every refill carries it.
    run: u64,
    /// The next id to take and the block's last, while a block holds one.
    block: Option<(u64, u64)>,
    /// The first ids of the blocks adopted in this run.
    adopted: BTreeSet<u64>,
    /// Whether a refill is outstanding: asked and not yet answered by a grant.
    asked: bool,
}

impl IdBlocks {
    /// A run that holds no block yet.
    #[must_use]
    pub fn new(run: u64) -> Self {
        Self {
            run,
            block: None,
            adopted: BTreeSet::new(),
            asked: false,
        }
    }

    /// The run's nonce.
    #[must_use]
    pub fn run(&self) -> u64 {
        self.run
    }

    /// The ids left in the block held, if any.
    #[must_use]
    pub fn left(&self) -> u64 {
        self.block
            .map_or(0, |(next, last)| last.saturating_sub(next) + 1)
    }

    /// Whether the ids left are at or below `threshold`, where a refill is asked for.
    #[must_use]
    pub fn needs_refill(&self, threshold: u64) -> bool {
        self.left() <= threshold
    }

    /// Marks a refill asked, if none is outstanding; whether this call was the one
    /// that asked.
    pub fn ask(&mut self) -> bool {
        !std::mem::replace(&mut self.asked, true)
    }

    /// Adopts `record`'s block as the one to take from — only a grant of this run,
    /// and each block once — and clears the outstanding refill. `any_run` is
    /// `IdBlockResumed`'s reading, which adopts whatever run the grant was to.
    /// Whether the block was adopted.
    pub fn adopt(&mut self, record: &LeaseRecord, any_run: bool) -> bool {
        if (record.run != self.run && !any_run) || !self.adopted.insert(record.first) {
            return false;
        }
        self.block = Some((record.first, record.last));
        self.asked = false;
        true
    }

    /// The next id of the block held, if one is left: what a split's right half takes
    /// (§5). Taken from memory, each id once.
    pub fn take(&mut self) -> Option<u64> {
        let (next, last) = self.block?;
        if next > last {
            return None;
        }
        self.block = if next == last {
            None
        } else {
            Some((next + 1, last))
        };
        Some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(run: u64, first: u64, last: u64) -> LeaseRecord {
        LeaseRecord { run, first, last }
    }

    #[test]
    fn a_node_adopts_only_its_own_runs_grant_and_each_block_once() {
        let mut ids = IdBlocks::new(7);
        assert_eq!(ids.left(), 0);
        assert!(ids.needs_refill(2));
        assert!(ids.ask(), "the first ask is the one that asks");
        assert!(!ids.ask(), "a second ask while one is outstanding is not");
        assert!(!ids.adopt(&record(6, 3, 10), false), "another run's grant");
        assert!(ids.needs_refill(2));
        assert!(ids.adopt(&record(7, 3, 10), false));
        assert_eq!(ids.left(), 8);
        assert!(!ids.needs_refill(2));
        assert!(ids.ask(), "the grant cleared the outstanding refill");
        assert!(!ids.adopt(&record(7, 3, 10), false), "the same block again");
        assert!(ids.adopt(&record(7, 11, 18), false), "the next block");
        assert_eq!(ids.left(), 8);
    }

    #[test]
    fn the_variant_adopts_another_runs_block() {
        let mut ids = IdBlocks::new(7);
        assert!(ids.adopt(&record(6, 3, 10), true));
        assert_eq!(ids.take(), Some(3));
    }

    #[test]
    fn ids_are_taken_in_order_each_once_and_the_threshold_is_met_from_below() {
        let mut ids = IdBlocks::new(1);
        assert_eq!(ids.take(), None);
        assert!(ids.adopt(&record(1, 3, 5), false));
        assert_eq!(ids.take(), Some(3));
        assert!(!ids.needs_refill(1));
        assert_eq!(ids.take(), Some(4));
        assert!(ids.needs_refill(1), "one id left is at the threshold");
        assert_eq!(ids.take(), Some(5));
        assert_eq!(ids.left(), 0);
        assert_eq!(ids.take(), None);
        assert!(ids.adopt(&record(1, 9, 9), false));
        assert_eq!(ids.take(), Some(9));
        assert_eq!(ids.take(), None);
    }
}
