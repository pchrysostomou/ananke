//! The persistent state, in the storage engine under tenant 0 (RAFT.md §3): the
//! hard state and the applied index under one key, the log one key per index. A
//! persist is one synced batch, so the term, the vote and the entries of a step are
//! durable together before the step's messages leave. Applying an entry is one batch
//! with the entry's writes and the applied index, so an entry is applied exactly once
//! whatever the crash schedule: a crash between the two cannot exist.
//!
//! Keys follow SPEC §2.6's shape, tenant and table as big-endian `u64`s in front of
//! the user key, so the Raft state sorts apart from everything else and a scan over
//! one table is a scan over one key range.
//!
//! The engine recovers what it can and reports what it lost: a table it could not
//! read, a manifest it fell back from, a log head it discarded. Any of those is a
//! hole in the middle of the state, and a Raft server started on one would apply from
//! an applied index whose history is gone (D-022). [`RaftStore::open`] refuses such a
//! recovery with [`LostState`]; the answer is a snapshot from the leader, not a start.
//!
//! | Key | Value |
//! |---|---|
//! | `0 / 0 / meta` | `term: u64 \| vote: u64 (u64::MAX for none) \| applied: u64` |
//! | `0 / 1 / <index: u64 BE>` | `term: u64 \| payload` |

use std::io;
use std::sync::Arc;

use ananke_env::Environment;
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

fn meta_key() -> Bytes {
    key(RAFT_TENANT, META_TABLE, b"meta")
}

fn log_key(index: Index) -> Bytes {
    key(RAFT_TENANT, LOG_TABLE, &index.to_be_bytes())
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

/// Why [`RaftStore::open`] refused an engine: its recovery lost writes in the middle
/// of the state, so the applied index no longer names a state that existed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LostState {
    /// Tables the manifest listed that could not be read.
    pub dropped: Vec<u64>,
    /// The manifest an older one was used instead of.
    pub fallback_from: Option<u64>,
    /// A discarded log head, as (expected, found).
    pub head_gap: Option<(u64, u64)>,
}

impl LostState {
    /// What the recovery lost, if anything.
    fn of(recovery: &EngineRecovery) -> Option<Self> {
        let lost = Self {
            dropped: recovery.dropped.iter().map(|t| t.number).collect(),
            fallback_from: recovery.fallback_from,
            head_gap: recovery.wal.head_gap,
        };
        (!lost.dropped.is_empty() || lost.fallback_from.is_some() || lost.head_gap.is_some())
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
        Ok(())
    }
}

impl std::error::Error for LostState {}

/// The Raft state in the engine, for one server.
pub struct RaftStore<E: Environment> {
    engine: Arc<Engine<E>>,
    term: Term,
    vote: Option<ServerId>,
    applied: Index,
    last_index: Index,
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
        let (term, vote, applied) = match engine.get(&meta_key()).await? {
            None => (0, None, 0),
            Some(bytes) => decode_meta(bytes)?,
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
                term,
                vote,
                applied,
                last_index,
            },
            log,
        ))
    }

    /// The current term on disk.
    #[must_use]
    pub fn term(&self) -> Term {
        self.term
    }

    /// The vote on disk.
    #[must_use]
    pub fn vote(&self) -> Option<ServerId> {
        self.vote
    }

    /// The applied index on disk.
    #[must_use]
    pub fn applied(&self) -> Index {
        self.applied
    }

    /// The last log index on disk.
    #[must_use]
    pub fn last_index(&self) -> Index {
        self.last_index
    }

    /// The engine.
    #[must_use]
    pub fn engine(&self) -> &Arc<Engine<E>> {
        &self.engine
    }

    fn meta_value(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(24);
        out.put_u64_le(self.term);
        out.put_u64_le(self.vote.map_or(NO_VOTE, |v| v.0));
        out.put_u64_le(self.applied);
        out.freeze()
    }

    /// Makes a step's persistent changes durable as one synced batch: the hard
    /// state when it changed, the truncation's deletes, the appends. Resolves once
    /// the batch is durable.
    ///
    /// # Errors
    ///
    /// The engine's.
    pub async fn persist(&mut self, persist: &Persist) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        if persist.term != self.term || persist.vote != self.vote {
            self.term = persist.term;
            self.vote = persist.vote;
            batch.put(meta_key(), self.meta_value());
        }
        if let Some(from) = persist.truncate_from {
            for index in from..=self.last_index {
                batch.delete(log_key(index));
            }
            self.last_index = from.saturating_sub(1).min(self.last_index);
        }
        for entry in &persist.append {
            batch.put(log_key(entry.index), encode_entry(entry));
            self.last_index = self.last_index.max(entry.index);
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.engine.write(batch, true).await?;
        Ok(())
    }

    /// Applies entry `index`: its writes and the applied index in one synced batch,
    /// so both are durable or neither is.
    ///
    /// # Errors
    ///
    /// The engine's, or `InvalidInput` if `index` is not the next to apply.
    pub async fn apply(&mut self, index: Index, mut writes: WriteBatch) -> io::Result<()> {
        if index != self.applied + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("applying {index} after {}", self.applied),
            ));
        }
        self.applied = index;
        writes.put(meta_key(), self.meta_value());
        self.engine.write(writes, true).await?;
        Ok(())
    }
}

fn decode_meta(mut bytes: Bytes) -> io::Result<(Term, Option<ServerId>, Index)> {
    if bytes.len() != 24 {
        return Err(bad("meta value"));
    }
    let term = bytes.get_u64_le();
    let vote = match bytes.get_u64_le() {
        NO_VOTE => None,
        id => Some(ServerId(id)),
    };
    let applied = bytes.get_u64_le();
    Ok((term, vote, applied))
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
