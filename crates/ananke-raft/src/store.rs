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
//! | `0 / 0 / reseeded` | present on a store a re-seed rebuilt (RAFT.md §3) |
//! | `0 / 0 / incarnation` | `incarnation: u64`: 1 for a store started fresh, a fresh value on every store a re-seed rebuilt |
//! | `0 / 1 / <index: u64 BE>` | `term: u64 \| payload` |
//! | `0 / 2 / config` | `index: u64 \| configuration` |
//! | `0 / 3 / snapshot` | the last snapshot's index, term, configuration, checkpoint directory |
//!
//! The `config` key carries the latest configuration entry's index and content
//! (RAFT.md §3), written in the same synced batch as the append or truncation
//! that changed which entry that is, so the two can never disagree, and the open
//! checks the two against each other. On a compacted store the entry itself may
//! sit at or below the snapshot's last index: the log then holds nothing to
//! check against, and the key — rewritten by an install's repair, checked here
//! against the snapshot record — is how the store still knows its configuration.
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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ananke_env::{Environment, File, FileSystem, OpenOptions, WalStop, WalStopReason};
use ananke_storage::manifest;
use ananke_storage::{Engine, EngineRecovery, WriteBatch};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::core::Persist;
use crate::message::{get_payload, put_payload};
use crate::types::{Configuration, Entry, Index, Payload, ServerId, Term};

/// The tenant the protocol's state lives under.
pub const RAFT_TENANT: u64 = 0;
const META_TABLE: u64 = 0;
pub(crate) const LOG_TABLE: u64 = 1;
const CONFIG_TABLE: u64 = 2;
/// The table the snapshot record lives under (RAFT.md §3): written into the live
/// store before a checkpoint is taken, so the checkpoint's copy carries the
/// snapshot's own identity, before the checkpoint's `CURRENT` (D-024).
const SNAP_TABLE: u64 = 3;
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

pub(crate) fn hard_key() -> Bytes {
    key(RAFT_TENANT, META_TABLE, b"hard")
}

pub(crate) fn applied_key() -> Bytes {
    key(RAFT_TENANT, META_TABLE, b"applied")
}

pub(crate) fn log_key(index: Index) -> Bytes {
    key(RAFT_TENANT, LOG_TABLE, &index.to_be_bytes())
}

/// The `0 / 2 / config` key (RAFT.md §3): the latest configuration entry's index
/// and content, written in the same synced batch as the append or truncation that
/// changed which entry that is, and rewritten by an install's repair so a
/// compacted store still knows its configuration.
pub(crate) fn config_key() -> Bytes {
    key(RAFT_TENANT, CONFIG_TABLE, b"config")
}

/// The `0 / 3 / snapshot` key (RAFT.md §3).
pub(crate) fn snapshot_key() -> Bytes {
    key(RAFT_TENANT, SNAP_TABLE, b"snapshot")
}

/// The re-seed quarantine flag: present on a store rebuilt from a snapshot after a
/// refusal (RAFT.md §3), durable so a later clean restart keeps the suppression.
// PROPOSED(D-035): re-seeded servers are quarantined from voting for good.
pub(crate) fn quarantine_key() -> Bytes {
    key(RAFT_TENANT, META_TABLE, b"reseeded")
}

/// The store's incarnation number (RAFT.md §3): written as 1 at a fresh store's
/// first open, and by an install's repair — carried forward on an install into a
/// live store, drawn afresh for a re-seed, whose predecessor's value is lost with
/// the rest of the refused store. Followers answer with it so a leader can tell a
/// rebuilt store, whose log may have lost acknowledged entries, from the one it
/// recorded a match index for.
// PROPOSED(D-042): store incarnations, so a leader forgets what a re-seeded
// follower forgot.
pub(crate) fn incarnation_key() -> Bytes {
    key(RAFT_TENANT, META_TABLE, b"incarnation")
}

/// The incarnation of a store started fresh.
// PROPOSED(D-042): store incarnations.
pub const FIRST_INCARNATION: u64 = 1;

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
    /// Damage found before the engine opened: a store directory that is not a
    /// whole store any more, or a completed install whose commit point rotted.
    /// Set on its own, with every field above empty.
    // PROPOSED(D-041): the crash-safe adoption and the store identity marker.
    pub damaged: Option<Damage>,
}

/// What was found wrong with a store directory before the engine opened, each a
/// refusal ([`LostState`]) rather than a fresh start or a sweep: a directory that
/// carries the store marker ([`STORE_MARKER`]) once held a Raft store and its
/// state, term and vote included, is gone with its `CURRENT`; and a staging
/// directory whose `CURRENT` exists is a completed install, the only copy of a
/// state the leader may have compacted past, so a `CURRENT` that does not parse
/// is damage, never debris (RAFT.md §3, D-022).
// PROPOSED(D-041): the crash-safe adoption and the store identity marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Damage {
    /// The staging directory's `CURRENT` exists but does not parse.
    StagingCurrentUnreadable,
    /// The manifest the staging directory's `CURRENT` names is missing or does
    /// not decode.
    StagingManifestUnreadable,
    /// A table the staged manifest lists is not in the staging directory.
    StagingTableMissing(u64),
    /// The directory carries the store marker but no `CURRENT`.
    MarkedCurrentMissing,
    /// The directory carries the store marker and a `CURRENT` that does not
    /// parse.
    MarkedCurrentUnreadable,
}

impl std::fmt::Display for Damage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Damage::StagingCurrentUnreadable => {
                write!(
                    f,
                    "the staging directory's CURRENT exists but cannot be read"
                )
            }
            Damage::StagingManifestUnreadable => write!(
                f,
                "the manifest the staging directory's CURRENT names is missing or cannot be read"
            ),
            Damage::StagingTableMissing(n) => write!(
                f,
                "table {n:06}, which the staged manifest lists, is missing from the staging directory"
            ),
            Damage::MarkedCurrentMissing => {
                write!(
                    f,
                    "the directory carries the {STORE_MARKER} marker but no CURRENT"
                )
            }
            Damage::MarkedCurrentUnreadable => write!(
                f,
                "the directory carries the {STORE_MARKER} marker and a CURRENT that cannot be read"
            ),
        }
    }
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
            damaged: None,
        };
        (!lost.dropped.is_empty()
            || lost.fallback_from.is_some()
            || lost.head_gap.is_some()
            || lost.log_stop.is_some()
            || !lost.covered_stops.is_empty())
        .then_some(lost)
    }

    /// The refusal for damage found before the engine opened: nothing recovered,
    /// since nothing was opened.
    // PROPOSED(D-041): the crash-safe adoption and the store identity marker.
    #[must_use]
    pub fn from_damage(damage: Damage) -> Self {
        Self {
            dropped: Vec::new(),
            fallback_from: None,
            head_gap: None,
            log_stop: None,
            covered_stops: Vec::new(),
            damaged: Some(damage),
        }
    }

    /// The refusal an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<LostState> {
        error.get_ref()?.downcast_ref::<LostState>().cloned()
    }

    /// This refusal as the `InvalidData` error [`RaftStore::open`] and the
    /// adoption fail with, which [`from_io`](Self::from_io) reads back.
    // PROPOSED(D-041): the crash-safe adoption and the store identity marker.
    pub(crate) fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, self)
    }
}

impl std::fmt::Display for LostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.damaged {
            Some(damage) => write!(f, "the store is damaged: {damage}")?,
            None => write!(f, "the engine's recovery lost state:")?,
        }
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

/// The store identity marker: a file of this name in the engine directory,
/// written once the engine has opened a genuinely fresh directory for the first
/// time and never removed — not by an adoption, which deletes only the store's
/// own files, nor by the engine's orphan sweep, which knows only tables,
/// manifests and `CURRENT.tmp`. Before the engine opens, a directory that carries
/// the marker but no valid `CURRENT` is refused with [`LostState`]: it once held a
/// Raft store, so it is a lost store, never a fresh one (RAFT.md §3, D-022). The
/// engine's own rule (D-024) refuses a missing `CURRENT` only while manifests or
/// tables remain; a directory emptied past that opened fresh, which is how the
/// nightly's seed 6325 turned a voter with a hundred and nineteen committed
/// entries into a blank one.
// PROPOSED(D-041): the crash-safe adoption and the store identity marker.
pub const STORE_MARKER: &str = "RAFT-STORE";

/// The marker's path under `engine_dir`.
// PROPOSED(D-041): the crash-safe adoption and the store identity marker.
#[must_use]
pub fn marker_path(engine_dir: &Path) -> PathBuf {
    engine_dir.join(STORE_MARKER)
}

/// Reads a whole file, or `None` if it does not exist.
async fn read_whole<E: Environment>(env: &E, path: &Path) -> io::Result<Option<Bytes>> {
    let file = match env.fs().open(path, OpenOptions::new().read(true)).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let size = file.size().await?;
    let size = usize::try_from(size).map_err(|_| bad("file too large"))?;
    Ok(Some(file.read_at(0, size).await?))
}

/// Whether `engine_dir` carries the store marker.
async fn marked<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<bool> {
    match env
        .fs()
        .open(&marker_path(engine_dir), OpenOptions::new().read(true))
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// The check before the engine opens: a directory that carries the store marker
/// ([`STORE_MARKER`]) must hold a `CURRENT` that parses, or it is a lost store.
/// A directory without the marker passes, whatever else it holds: the engine
/// decides whether that is a fresh store or one it refuses (D-024).
///
/// # Errors
///
/// `InvalidData` carrying a [`LostState`] with [`Damage::MarkedCurrentMissing`]
/// or [`Damage::MarkedCurrentUnreadable`]; otherwise the filesystem's.
// PROPOSED(D-041): the crash-safe adoption and the store identity marker.
pub async fn refuse_lost_store<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<()> {
    if !marked(env, engine_dir).await? {
        return Ok(());
    }
    match read_whole(env, &manifest::current_path(engine_dir)).await? {
        None => Err(LostState::from_damage(Damage::MarkedCurrentMissing).into_io()),
        Some(bytes) if manifest::parse_current(&bytes).is_none() => {
            Err(LostState::from_damage(Damage::MarkedCurrentUnreadable).into_io())
        }
        Some(_) => Ok(()),
    }
}

/// Writes the store marker ([`STORE_MARKER`]) into `engine_dir`, synced, with the
/// directory synced after, unless it is already there. Called only after the
/// engine and the store have opened successfully, so a directory is marked as a
/// store once it has been one: a fresh directory at its first open, or a store
/// from before the marker existed at its next.
///
/// # Errors
///
/// The filesystem's.
// PROPOSED(D-041): the crash-safe adoption and the store identity marker.
pub async fn mark_store<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<()> {
    if marked(env, engine_dir).await? {
        return Ok(());
    }
    let fs = env.fs();
    let file = fs
        .open(
            &marker_path(engine_dir),
            OpenOptions::new().write(true).create(true),
        )
        .await?;
    file.write_at(0, Bytes::from_static(b"ananke raft store\n"))
        .await?;
    file.sync().await?;
    fs.sync_dir(engine_dir).await
}

/// The last snapshot, as `0 / 3 / snapshot` records it (RAFT.md §3): written into
/// the live store before its checkpoint is taken, so the checkpoint carries its own
/// identity before its `CURRENT`; written by an install's repair with the identity
/// of the snapshot installed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRecord {
    /// The snapshot's last applied index.
    pub last_index: Index,
    /// That entry's term.
    pub last_term: Term,
    /// The configuration at that index.
    pub config: Configuration,
    /// The checkpoint's directory on this server; empty for an installed snapshot,
    /// whose checkpoint was the leader's.
    pub dir: String,
    /// Whether the snapshot was taken here rather than installed.
    pub taken: bool,
}

/// What [`RaftStore::open`] found beside the store itself.
#[derive(Clone, Debug)]
pub struct Recovered {
    /// The log's tail past the snapshot, in index order; the whole log without one.
    pub log: Vec<Entry>,
    /// The last snapshot, if the store records one.
    pub snapshot: Option<SnapshotRecord>,
    /// Whether this store was rebuilt by a re-seed: the server must grant no vote,
    /// no pre-vote and no lease promise on it, ever (RAFT.md §3).
    // PROPOSED(D-035): re-seeded servers are quarantined from voting for good.
    pub quarantined: bool,
}

pub(crate) fn encode_snapshot_record(record: &SnapshotRecord) -> Bytes {
    let mut out = BytesMut::with_capacity(64);
    out.put_u64_le(record.last_index);
    out.put_u64_le(record.last_term);
    out.put_u8(u8::from(record.taken));
    out.put_u32_le(u32::try_from(record.dir.len()).expect("directory fits u32"));
    out.put_slice(record.dir.as_bytes());
    put_payload(&mut out, &Payload::Config(record.config.clone()));
    out.freeze()
}

pub(crate) fn decode_snapshot_record(mut bytes: Bytes) -> io::Result<SnapshotRecord> {
    if bytes.len() < 21 {
        return Err(bad("snapshot record"));
    }
    let last_index = bytes.get_u64_le();
    let last_term = bytes.get_u64_le();
    let taken = match bytes.get_u8() {
        0 => false,
        1 => true,
        _ => return Err(bad("snapshot record malformed")),
    };
    let len = bytes.get_u32_le() as usize;
    if bytes.len() < len {
        return Err(bad("snapshot record torn"));
    }
    let dir = String::from_utf8(bytes.split_to(len).to_vec()).map_err(|_| bad("directory"))?;
    let Payload::Config(config) = get_payload(&mut bytes)? else {
        return Err(bad("snapshot record configuration"));
    };
    if !bytes.is_empty() {
        return Err(bad("snapshot record has trailing bytes"));
    }
    Ok(SnapshotRecord {
        last_index,
        last_term,
        config,
        dir,
        taken,
    })
}

/// The Raft state in the engine, for one server. Shared between the task that
/// persists and the task that applies; see the module documentation.
pub struct RaftStore<E: Environment> {
    engine: Arc<Engine<E>>,
    /// The hard state on disk, written by [`persist`](Self::persist) only.
    term: AtomicU64,
    vote: AtomicU64,
    /// The first log index on disk: one past the snapshot's.
    first_index: AtomicU64,
    last_index: AtomicU64,
    /// The applied index on disk, written by [`apply`](Self::apply) only.
    applied: AtomicU64,
    /// The store's incarnation number: fixed for the life of the store, since
    /// only an install's repair writes it, and that builds a new store.
    // PROPOSED(D-042): store incarnations.
    incarnation: u64,
}

impl<E: Environment> RaftStore<E> {
    /// Loads the state the engine holds: the hard state, the applied index, the
    /// snapshot record and the log's tail past it, in index order. `recovery` is
    /// what the engine's open reported. Log keys the snapshot covers are deleted
    /// here: a crash between a snapshot's record and its compaction leaves them,
    /// and this cleanup is idempotent.
    ///
    /// # Errors
    ///
    /// `InvalidData` carrying a [`LostState`] when the recovery lost writes in the
    /// middle of the state; the engine's; or `InvalidData` for a value that is not
    /// what was written.
    pub async fn open(
        engine: Arc<Engine<E>>,
        recovery: &EngineRecovery,
    ) -> io::Result<(Self, Recovered)> {
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
        let stored_config = match engine.get(&config_key()).await? {
            None => None,
            Some(bytes) => Some(decode_config(bytes)?),
        };
        let record = match engine.get(&snapshot_key()).await? {
            None => None,
            Some(bytes) => Some(decode_snapshot_record(bytes)?),
        };
        let quarantined = engine.get(&quarantine_key()).await?.is_some();
        // The incarnation number: a fresh store starts at the first and writes
        // it here, synced, so every store carries the key explicitly; an
        // installed store carries the one its repair wrote.
        // PROPOSED(D-042): store incarnations.
        let incarnation = match engine.get(&incarnation_key()).await? {
            Some(bytes) => decode_incarnation(bytes)?,
            None => {
                let mut first = WriteBatch::new();
                first.put(incarnation_key(), encode_incarnation(FIRST_INCARNATION));
                engine.write(first, true).await?;
                FIRST_INCARNATION
            }
        };
        let snap_index = record.as_ref().map_or(0, |r| r.last_index);
        let snapshot = engine.snapshot();
        let start = key(RAFT_TENANT, LOG_TABLE, &[]);
        let end = key(RAFT_TENANT, LOG_TABLE + 1, &[]);
        let mut log = Vec::new();
        let mut stale = WriteBatch::new();
        for (k, value) in engine.scan(&start[..]..&end[..], &snapshot).await? {
            let index = u64::from_be_bytes(k[16..24].try_into().map_err(|_| bad("log key"))?);
            if index <= snap_index {
                stale.delete(Bytes::copy_from_slice(&k));
                continue;
            }
            let entry = decode_entry(index, value)?;
            if entry.index != snap_index + log.len() as Index + 1 {
                return Err(bad("log indices not consecutive"));
            }
            log.push(entry);
        }
        if !stale.is_empty() {
            engine.write(stale, true).await?;
        }
        // The config key and the log are written in one batch, so they can only
        // disagree if something else wrote the store: refuse it (RAFT.md §3). On
        // a compacted store the latest configuration entry may sit at or below
        // the snapshot's last index; the log then holds nothing to check against,
        // and the key must agree with the snapshot record instead, which an
        // install's repair wrote in the same table swap.
        let in_log = log.iter().fold(None, |kept, entry| match &entry.payload {
            Payload::Config(config) => Some((entry.index, config.clone())),
            _ => kept,
        });
        match (&stored_config, &in_log) {
            (None, None) => {}
            (Some((0, _)), None) => {}
            (Some(stored), Some(latest)) if stored == latest => {}
            (Some((index, config)), None)
                if *index <= snap_index && record.as_ref().is_some_and(|r| r.config == *config) => {
            }
            _ => return Err(bad("the configuration key is out of step with the log")),
        }
        let last_index = snap_index + log.len() as Index;
        // The snapshot's state is applied by construction; the applied key says at
        // least as much on any store a take or an install wrote.
        let applied = applied.max(snap_index);
        Ok((
            Self {
                engine,
                term: AtomicU64::new(term),
                vote: AtomicU64::new(vote.map_or(NO_VOTE, |v| v.0)),
                first_index: AtomicU64::new(snap_index + 1),
                last_index: AtomicU64::new(last_index),
                applied: AtomicU64::new(applied),
                incarnation,
            },
            Recovered {
                log,
                snapshot: record,
                quarantined,
            },
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

    /// The store's incarnation number (RAFT.md §3): what this server's
    /// AppendEntries and InstallSnapshot responses carry.
    // PROPOSED(D-042): store incarnations.
    #[must_use]
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// The first log index on disk: one past the snapshot's last.
    #[must_use]
    pub fn first_index(&self) -> Index {
        self.first_index.load(Ordering::Acquire)
    }

    /// The last log index on disk.
    #[must_use]
    pub fn last_index(&self) -> Index {
        self.last_index.load(Ordering::Acquire)
    }

    /// Writes the snapshot record (RAFT.md §3), synced: called by the snapshot task
    /// before it takes the checkpoint, so the checkpoint's copy carries the
    /// snapshot's own identity before the checkpoint's `CURRENT` is written
    /// (D-024).
    ///
    /// # Errors
    ///
    /// The engine's.
    pub async fn record_snapshot(&self, record: &SnapshotRecord) -> io::Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(snapshot_key(), encode_snapshot_record(record));
        self.engine.write(batch, true).await?;
        Ok(())
    }

    /// Reads the snapshot record back, if the store holds one.
    ///
    /// # Errors
    ///
    /// The engine's, or `InvalidData` for a value that is not a record.
    pub async fn snapshot_record(&self) -> io::Result<Option<SnapshotRecord>> {
        match self.engine.get(&snapshot_key()).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(decode_snapshot_record(bytes)?)),
        }
    }

    /// The engine.
    #[must_use]
    pub fn engine(&self) -> &Arc<Engine<E>> {
        &self.engine
    }

    /// Makes a step's persistent changes durable as one synced batch: the hard
    /// state when it changed, the truncation's deletes, the appends, the
    /// compaction's deletes (RAFT.md §3). Resolves once the batch is durable. One
    /// task calls this.
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
        let mut first_index = self.first_index();
        let mut last_index = self.last_index();
        if let Some(to) = persist.compact_to {
            // The log compacted to a snapshot: the engine's compaction reclaims
            // the space in its own time (RAFT.md §3).
            for index in first_index..=to.min(last_index) {
                batch.delete(log_key(index));
            }
            first_index = first_index.max(to + 1);
            last_index = last_index.max(to);
        }
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
        if let Some((index, config)) = &persist.config {
            batch.put(config_key(), encode_config(*index, config));
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.engine.write(batch, true).await?;
        if hard_changed {
            self.term.store(persist.term, Ordering::Release);
            self.vote.store(vote, Ordering::Release);
        }
        self.first_index.store(first_index, Ordering::Release);
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

/// The value under the hard-state key: term and vote, as one persist writes it.
pub(crate) fn encode_hard(term: Term, vote: Option<ServerId>) -> Bytes {
    let mut out = BytesMut::with_capacity(16);
    out.put_u64_le(term);
    out.put_u64_le(vote.map_or(NO_VOTE, |v| v.0));
    out.freeze()
}

/// The value under the applied-index key.
pub(crate) fn encode_applied(applied: Index) -> Bytes {
    let mut out = BytesMut::with_capacity(8);
    out.put_u64_le(applied);
    out.freeze()
}

fn decode_applied(mut bytes: Bytes) -> io::Result<Index> {
    if bytes.len() != 8 {
        return Err(bad("applied index value"));
    }
    Ok(bytes.get_u64_le())
}

/// The value under the incarnation key.
// PROPOSED(D-042): store incarnations.
pub(crate) fn encode_incarnation(incarnation: u64) -> Bytes {
    let mut out = BytesMut::with_capacity(8);
    out.put_u64_le(incarnation);
    out.freeze()
}

fn decode_incarnation(mut bytes: Bytes) -> io::Result<u64> {
    if bytes.len() != 8 {
        return Err(bad("incarnation value"));
    }
    Ok(bytes.get_u64_le())
}

pub(crate) fn encode_entry(entry: &Entry) -> Bytes {
    let mut out = BytesMut::with_capacity(16);
    out.put_u64_le(entry.term);
    put_payload(&mut out, &entry.payload);
    out.freeze()
}

pub(crate) fn encode_config(index: Index, config: &Configuration) -> Bytes {
    let mut out = BytesMut::with_capacity(32);
    out.put_u64_le(index);
    put_payload(&mut out, &Payload::Config(config.clone()));
    out.freeze()
}

fn decode_config(mut bytes: Bytes) -> io::Result<(Index, Configuration)> {
    if bytes.len() < 8 {
        return Err(bad("config value"));
    }
    let index = bytes.get_u64_le();
    match get_payload(&mut bytes)? {
        Payload::Config(config) if bytes.is_empty() => Ok((index, config)),
        _ => Err(bad("config value")),
    }
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
