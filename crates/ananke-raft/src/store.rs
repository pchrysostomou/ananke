//! The persistent state, in the storage engine under tenant 0 (RAFT.md §3): the
//! hard state under one key, the applied index under another, the log one key per
//! index. A persist is one synced batch, so the term, the vote and the entries of a
//! step are durable together before the step's messages leave. Applying an entry is
//! one batch with the entry's writes and the applied index, so an entry is applied
//! exactly once whatever the crash schedule: a crash between the two cannot exist.
//!
//! The hard state and the applied index are separate keys because separate tasks
//! write them (RAFT.md §3): the `raft` task persists, the `apply` task applies, and
//! neither waits for the other. A [`RaftStore`] is shared between them behind an
//! `Arc`; its methods take `&self`, and the cached copies of what is on disk are
//! atomics, each written by one task only.
//!
//! Keys follow SPEC §2.6's shape, tenant and table as big-endian `u64`s in front of
//! the user key, so the Raft state sorts apart from everything else and a scan over
//! one table is a scan over one key range.
//!
//! | Key | Value |
//! |---|---|
//! | `0 / 0 / hard` | `term: u64 \| vote: u64 (u64::MAX for none)` |
//! | `0 / 0 / applied` | `applied: u64` |
//! | `0 / 1 / <index: u64 BE>` | `term: u64 \| payload` |
//!
//! The engine recovers what it can and reports what it lost: a table it could not
//! read, a manifest it fell back from, a log head it discarded, a log it stopped
//! reading at a bad checksum or a gap, a corrupt record in a segment the tables
//! cover, past which the rest of the segment is gone. Any of those is a hole in the
//! middle of the state, and a Raft server started on one would apply from an applied
//! index whose history is gone, or vote in a term it had already voted in (D-022).
//! Raft's safety argument assumes persistent state is persistent. The server opens
//! the engine with fallback and head-gap discard off and log-damage refusal on, so
//! the engine itself refuses those before it touches the disk (D-027); what the
//! engine tolerates, a dropped table, [`RaftStore::open`] refuses here with
//! [`LostState`], which also names any log damage should an engine be opened
//! without the flag. A refused server takes part in nothing, no votes and no
//! responses, until a snapshot re-seeds it.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ananke_env::{Environment, WalStop, WalStopReason};
use ananke_storage::{Engine, EngineRecovery, WriteBatch};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::core::Persist;
use crate::message::{get_payload, put_payload};
use crate::types::{Entry, Index, ServerId, Term};

/// The tenant the protocol's state lives under.
pub const RAFT_TENANT: u64 = 0;
const META_TABLE: u64 = 0;
const LOG_TABLE: u64 = 1;
const NO_VOTE: u64 = u64::MAX;

/// A key under `tenant` and `table`.
#[must_use]
pub fn key(tenant: u64, table: u64, user: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(16 + user.len());
    out.put_u64(tenant);
    out.put_u64(table);
    out.put_slice(user);
    out.freeze()
}

fn hard_key() -> Bytes {
    key(RAFT_TENANT, META_TABLE, b"hard")
}

fn applied_key() -> Bytes {
    key(RAFT_TENANT, META_TABLE, b"applied")
}

fn log_key(index: Index) -> Bytes {
    key(RAFT_TENANT, LOG_TABLE, &index.to_be_bytes())
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

/// Why [`RaftStore::open`] refused an engine: its recovery lost writes in the middle
/// of the state, so the applied index no longer names a state that existed and the
/// hard state may be older than what was promised.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LostState {
    /// Tables the manifest listed that could not be read.
    pub dropped: Vec<u64>,
    /// The manifest an older one was used instead of.
    pub fallback_from: Option<u64>,
    /// A discarded log head, as (expected, found).
    pub head_gap: Option<(u64, u64)>,
    /// A log stopped short at a bad checksum or a gap in the numbering: records past
    /// it, acknowledged or not, are gone. A torn record at the end is not this: it
    /// was in flight at the crash and never acknowledged.
    pub log_stop: Option<WalStop>,
    /// Corrupt records in segments the tables cover, which recovery skipped: the
    /// rest of each such segment, acknowledged or not, is gone. The sweep found this
    /// on its first seed, a rotted block under a flushed record with acknowledged
    /// records after it (D-026).
    pub covered_stops: Vec<WalStop>,
}

impl LostState {
    /// What the recovery lost, if anything.
    fn of(recovery: &EngineRecovery) -> Option<Self> {
        let log_stop = recovery.wal.stop.filter(|stop| {
            matches!(
                stop.reason,
                WalStopReason::BadChecksum | WalStopReason::Gap { .. }
            )
        });
        let lost = Self {
            dropped: recovery.dropped.iter().map(|t| t.number).collect(),
            fallback_from: recovery.fallback_from,
            head_gap: recovery.wal.head_gap,
            log_stop,
            covered_stops: recovery.wal.covered_stops.iter().map(|c| c.stop).collect(),
        };
        (!lost.dropped.is_empty()
            || lost.fallback_from.is_some()
            || lost.head_gap.is_some()
            || lost.log_stop.is_some()
            || !lost.covered_stops.is_empty())
        .then_some(lost)
    }

    /// The refusal an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<LostState> {
        error.get_ref()?.downcast_ref::<LostState>().cloned()
    }
}

impl std::fmt::Display for LostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the engine's recovery lost state:")?;
        if !self.dropped.is_empty() {
            write!(f, " dropped tables {:?}", self.dropped)?;
        }
        if let Some(n) = self.fallback_from {
            write!(f, " fell back from manifest {n}")?;
        }
        if let Some((expected, found)) = self.head_gap {
            write!(
                f,
                " discarded a log head (expected {expected}, found {found})"
            )?;
        }
        if let Some(stop) = &self.log_stop {
            write!(
                f,
                " stopped reading the log at segment {} offset {} ({})",
                stop.segment,
                stop.offset,
                stop.reason.as_str()
            )?;
        }
        for stop in &self.covered_stops {
            write!(
                f,
                " skipped the rest of log segment {} from offset {} ({})",
                stop.segment,
                stop.offset,
                stop.reason.as_str()
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for LostState {}

/// The Raft state in the engine, for one server. Shared between the task that
/// persists and the task that applies; see the module documentation.
pub struct RaftStore<E: Environment> {
    engine: Arc<Engine<E>>,
    /// The hard state on disk, written by [`persist`](Self::persist) only.
    term: AtomicU64,
    vote: AtomicU64,
    last_index: AtomicU64,
    /// The applied index on disk, written by [`apply`](Self::apply) only.
    applied: AtomicU64,
}

impl<E: Environment> RaftStore<E> {
    /// Loads the state the engine holds: the hard state, the applied index, and the
    /// log, in index order. `recovery` is what the engine's open reported.
    ///
    /// # Errors
    ///
    /// `InvalidData` carrying a [`LostState`] when the recovery lost writes in the
    /// middle of the state; the engine's; or `InvalidData` for a value that is not
    /// what was written.
    pub async fn open(
        engine: Arc<Engine<E>>,
        recovery: &EngineRecovery,
    ) -> io::Result<(Self, Vec<Entry>)> {
        if let Some(lost) = LostState::of(recovery) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, lost));
        }
        let (term, vote) = match engine.get(&hard_key()).await? {
            None => (0, None),
            Some(bytes) => decode_hard(bytes)?,
        };
        let applied = match engine.get(&applied_key()).await? {
            None => 0,
            Some(bytes) => decode_applied(bytes)?,
        };
        let snapshot = engine.snapshot();
        let start = key(RAFT_TENANT, LOG_TABLE, &[]);
        let end = key(RAFT_TENANT, LOG_TABLE + 1, &[]);
        let mut log = Vec::new();
        for (k, value) in engine.scan(&start[..]..&end[..], &snapshot).await? {
            let index = u64::from_be_bytes(k[16..24].try_into().map_err(|_| bad("log key"))?);
            let entry = decode_entry(index, value)?;
            if entry.index != log.len() as Index + 1 {
                return Err(bad("log indices not consecutive"));
            }
            log.push(entry);
        }
        let last_index = log.len() as Index;
        Ok((
            Self {
                engine,
                term: AtomicU64::new(term),
                vote: AtomicU64::new(vote.map_or(NO_VOTE, |v| v.0)),
                last_index: AtomicU64::new(last_index),
                applied: AtomicU64::new(applied),
            },
            log,
        ))
    }

    /// The current term on disk.
    #[must_use]
    pub fn term(&self) -> Term {
        self.term.load(Ordering::Acquire)
    }

    /// The vote on disk.
    #[must_use]
    pub fn vote(&self) -> Option<ServerId> {
        match self.vote.load(Ordering::Acquire) {
            NO_VOTE => None,
            id => Some(ServerId(id)),
        }
    }

    /// The applied index on disk.
    #[must_use]
    pub fn applied(&self) -> Index {
        self.applied.load(Ordering::Acquire)
    }

    /// The last log index on disk.
    #[must_use]
    pub fn last_index(&self) -> Index {
        self.last_index.load(Ordering::Acquire)
    }

    /// The engine.
    #[must_use]
    pub fn engine(&self) -> &Arc<Engine<E>> {
        &self.engine
    }

    /// Makes a step's persistent changes durable as one synced batch: the hard
    /// state when it changed, the truncation's deletes, the appends. Resolves once
    /// the batch is durable. One task calls this.
    ///
    /// # Errors
    ///
    /// The engine's.
    pub async fn persist(&self, persist: &Persist) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        let vote = persist.vote.map_or(NO_VOTE, |v| v.0);
        let hard_changed = persist.term != self.term() || vote != self.vote.load(Ordering::Acquire);
        if hard_changed {
            let mut out = BytesMut::with_capacity(16);
            out.put_u64_le(persist.term);
            out.put_u64_le(vote);
            batch.put(hard_key(), out.freeze());
        }
        let mut last_index = self.last_index();
        if let Some(from) = persist.truncate_from {
            for index in from..=last_index {
                batch.delete(log_key(index));
            }
            last_index = from.saturating_sub(1).min(last_index);
        }
        for entry in &persist.append {
            batch.put(log_key(entry.index), encode_entry(entry));
            last_index = last_index.max(entry.index);
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.engine.write(batch, true).await?;
        if hard_changed {
            self.term.store(persist.term, Ordering::Release);
            self.vote.store(vote, Ordering::Release);
        }
        self.last_index.store(last_index, Ordering::Release);
        Ok(())
    }

    /// Applies entry `index`: its writes and the applied index in one synced batch,
    /// so both are durable or neither is. One task calls this.
    ///
    /// # Errors
    ///
    /// The engine's, or `InvalidInput` if `index` is not the next to apply.
    pub async fn apply(&self, index: Index, mut writes: WriteBatch) -> io::Result<()> {
        let applied = self.applied();
        if index != applied + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("applying {index} after {applied}"),
            ));
        }
        let mut out = BytesMut::with_capacity(8);
        out.put_u64_le(index);
        writes.put(applied_key(), out.freeze());
        self.engine.write(writes, true).await?;
        self.applied.store(index, Ordering::Release);
        Ok(())
    }
}

fn decode_hard(mut bytes: Bytes) -> io::Result<(Term, Option<ServerId>)> {
    if bytes.len() != 16 {
        return Err(bad("hard state value"));
    }
    let term = bytes.get_u64_le();
    let vote = match bytes.get_u64_le() {
        NO_VOTE => None,
        id => Some(ServerId(id)),
    };
    Ok((term, vote))
}

fn decode_applied(mut bytes: Bytes) -> io::Result<Index> {
    if bytes.len() != 8 {
        return Err(bad("applied index value"));
    }
    Ok(bytes.get_u64_le())
}

fn encode_entry(entry: &Entry) -> Bytes {
    let mut out = BytesMut::with_capacity(16);
    out.put_u64_le(entry.term);
    put_payload(&mut out, &entry.payload);
    out.freeze()
}

fn decode_entry(index: Index, mut bytes: Bytes) -> io::Result<Entry> {
    if bytes.len() < 9 {
        return Err(bad("log value"));
    }
    let term = bytes.get_u64_le();
    let payload = get_payload(&mut bytes)?;
    if !bytes.is_empty() {
        return Err(bad("log value has trailing bytes"));
    }
    Ok(Entry {
        term,
        index,
        payload,
    })
}
