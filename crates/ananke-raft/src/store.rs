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
//! one range of one purpose is a scan over one key range. A store is opened for one
//! Raft *group*, under the [`KeyPrefix`] `0 / <group: u64 BE>`; every key of the
//! group is `prefix / <purpose: u64 BE> / name`, so a group's whole Raft state is
//! one key interval, and several groups share one engine without sharing a key
//! (PROPOSED D-060, Q5 and Q40). Today's one group is
//! [`node::SINGLE_GROUP`](crate::node::SINGLE_GROUP).
//!
//! | Key | Value |
//! |---|---|
//! | `0 / g / 0 / hard` | `term: u64 \| vote: u64 (u64::MAX for none)` |
//! | `0 / g / 0 / applied` | `applied: u64` |
//! | `0 / g / 0 / reseeded` | present on a store a re-seed rebuilt (RAFT.md §3) |
//! | `0 / g / 0 / incarnation` | `incarnation: u64`: 1 for a store started fresh, a fresh value on every store a re-seed rebuilt |
//! | `0 / g / 1 / <index: u64 BE>` | `term: u64 \| payload` |
//! | `0 / g / 2 / config` | `index: u64 \| configuration` |
//! | `0 / g / 3 / snapshot` | the last snapshot's index, term, configuration, checkpoint directory |
//!
//! Purposes 4 and up are unassigned, so a later session table (#21, Q11) or a
//! range descriptor takes one inside the group's interval and moves no key
//! (PROPOSED D-060). User data is tenant 2 ([`crate::apply::USER_TENANT`]);
//! tenant 1 is the system tenant, which nothing in Stage A writes.
//!
//! Which layout a directory holds is recorded in a file beside the store marker,
//! [`crate::format::FORMAT_FILE`], read before anything
//! writes: a store of ananke-raft 0.3.0's format, which records none, is refused
//! at open rather than read under this layout, and so is one recording any other
//! version (D-059). [`RaftStore::open`] takes the proof of that check,
//! [`FormatChecked`], so no caller can read a key of
//! a directory the gate has not seen; [`RaftStore::open_dir`] runs the gate itself.
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
use ananke_storage::{Engine, EngineConfig, EngineRecovery, Snapshot, WriteBatch};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::core::Persist;
use crate::format::{self, FormatChecked};
use crate::message::{get_payload, put_payload};
use crate::types::{Configuration, Entry, Index, Payload, ServerId, Term};

/// The tenant the protocol's state lives under.
pub const RAFT_TENANT: u64 = 0;
/// The purpose the hard state, the applied index, the quarantine flag and the
/// incarnation live under, inside a group's prefix: RAFT.md §3's table 0.
// PROPOSED(D-060): RAFT.md §3's table ids, as purposes under a group's prefix.
pub const PURPOSE_META: u64 = 0;
/// The purpose the log lives under: one key per index, `prefix / 1 / <index: u64 BE>`.
// PROPOSED(D-060)
pub const PURPOSE_LOG: u64 = 1;
/// The purpose the configuration key lives under (RAFT.md §3): the latest
/// configuration entry's index and content, written in the same synced batch as
/// the append or truncation that changed which entry that is, and rewritten by an
/// install's repair so a compacted store still knows its configuration.
// PROPOSED(D-060)
pub const PURPOSE_CONFIG: u64 = 2;
/// The purpose the snapshot record lives under (RAFT.md §3): written into the live
/// store before a checkpoint is taken, so the checkpoint's copy carries the
/// snapshot's own identity, before the checkpoint's `CURRENT` (D-024).
// PROPOSED(D-060)
pub const PURPOSE_SNAPSHOT: u64 = 3;
const NO_VOTE: u64 = u64::MAX;

/// A key under `tenant` and `table`, SPEC §2.6's encoding.
#[must_use]
pub fn key(tenant: u64, table: u64, user: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(16 + user.len());
    out.put_u64(tenant);
    out.put_u64(table);
    out.put_slice(user);
    out.freeze()
}

/// The key prefix one Raft group's state lives under: `0 / <group: u64 BE>`,
/// sixteen bytes. Every key of the group is `prefix / <purpose: u64 BE> / name`,
/// so the group's whole Raft state is one key interval and a store is
/// parameterised by the prefix rather than by a fixed pair of tables (Q40).
/// `ananke-raft` names a group, never a range or a span: which range a group
/// replicates is the layer above's (SHARD.md §2).
// PROPOSED(D-060): the store parameterised by a key prefix (Q40).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyPrefix(Bytes);

impl KeyPrefix {
    /// The prefix of `group`'s Raft state.
    #[must_use]
    pub fn group(group: u64) -> Self {
        Self(key(RAFT_TENANT, group, &[]))
    }

    /// The prefix's bytes: `0 / <group: u64 BE>`.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The group's whole interval, `[0 / g, 0 / (g + 1))`; for the last group,
    /// everything above it in tenant 0.
    #[must_use]
    pub fn span(&self) -> std::ops::Range<Bytes> {
        let group = u64::from_be_bytes(self.0[8..16].try_into().expect("eight bytes"));
        let end = match group.checked_add(1) {
            Some(next) => key(RAFT_TENANT, next, &[]),
            None => Bytes::copy_from_slice(&(RAFT_TENANT + 1).to_be_bytes()),
        };
        self.0.clone()..end
    }

    /// One purpose's interval inside the group's, `[prefix / p, prefix / (p + 1))`.
    #[must_use]
    pub fn purpose_span(&self, purpose: u64) -> std::ops::Range<Bytes> {
        let start = self.key(purpose, &[]);
        let end = match purpose.checked_add(1) {
            Some(next) => self.key(next, &[]),
            None => self.span().end,
        };
        start..end
    }

    /// The key `prefix / <purpose: u64 BE> / name`.
    #[must_use]
    pub fn key(&self, purpose: u64, name: &[u8]) -> Bytes {
        let mut out = BytesMut::with_capacity(self.0.len() + 8 + name.len());
        out.put_slice(&self.0);
        out.put_u64(purpose);
        out.put_slice(name);
        out.freeze()
    }

    pub(crate) fn hard_key(&self) -> Bytes {
        self.key(PURPOSE_META, b"hard")
    }

    pub(crate) fn applied_key(&self) -> Bytes {
        self.key(PURPOSE_META, b"applied")
    }

    /// The log key of `index`: the prefix, the log purpose and the index as a
    /// big-endian `u64`, so the log sorts by index.
    pub(crate) fn log_key(&self, index: Index) -> Bytes {
        self.key(PURPOSE_LOG, &index.to_be_bytes())
    }

    /// The index a log key names: exactly the prefix, the log purpose and eight
    /// bytes, or `None` for anything else.
    pub(crate) fn log_index(&self, key: &[u8]) -> Option<Index> {
        let at = self.0.len();
        if key.len() != at + 16 || key[..at] != self.0[..] {
            return None;
        }
        if u64::from_be_bytes(key[at..at + 8].try_into().ok()?) != PURPOSE_LOG {
            return None;
        }
        Some(Index::from_be_bytes(key[at + 8..].try_into().ok()?))
    }

    /// The `prefix / 2 / config` key (RAFT.md §3): the latest configuration
    /// entry's index and content, written in the same synced batch as the append
    /// or truncation that changed which entry that is, and rewritten by an
    /// install's repair so a compacted store still knows its configuration.
    pub(crate) fn config_key(&self) -> Bytes {
        self.key(PURPOSE_CONFIG, b"config")
    }

    /// The `prefix / 3 / snapshot` key (RAFT.md §3).
    pub(crate) fn snapshot_key(&self) -> Bytes {
        self.key(PURPOSE_SNAPSHOT, b"snapshot")
    }

    /// The re-seed quarantine flag: present on a store rebuilt from a snapshot
    /// after a refusal (RAFT.md §3), durable so a later clean restart keeps the
    /// suppression.
    // D-035: re-seeded servers are quarantined from voting for good.
    pub(crate) fn quarantine_key(&self) -> Bytes {
        self.key(PURPOSE_META, b"reseeded")
    }

    /// The store's incarnation number (RAFT.md §3): written as 1 at a fresh
    /// store's first open, and by an install's repair — carried forward on an
    /// install into a live store, drawn afresh for a re-seed, whose predecessor's
    /// value is lost with the rest of the refused store. Followers answer with it
    /// so a leader can tell a rebuilt store, whose log may have lost acknowledged
    /// entries, from the one it recorded a match index for.
    // D-042: store incarnations, so a leader forgets what a re-seeded
    // follower forgot.
    pub(crate) fn incarnation_key(&self) -> Bytes {
        self.key(PURPOSE_META, b"incarnation")
    }
}

/// The incarnation of a store started fresh.
// D-042: store incarnations.
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
    // D-041: the crash-safe adoption and the store identity marker.
    pub damaged: Option<Damage>,
    /// What the store's marker says was lost, when the refusal is
    /// [`Damage::MarkedLost`]: the reason the refusal that marked it recorded,
    /// word for word, so a refusal outlives the process that made it.
    // D-044: a durable refusal, and a refused engine that does no work.
    pub lost_mark: Option<String>,
}

/// What was found wrong with a store directory before the engine opened, each a
/// refusal ([`LostState`]) rather than a fresh start or a sweep: a directory that
/// carries the store marker ([`STORE_MARKER`]) once held a Raft store and its
/// state, term and vote included, is gone with its `CURRENT`; and a staging
/// directory whose `CURRENT` exists is a completed install, the only copy of a
/// state the leader may have compacted past, so a `CURRENT` that does not parse
/// is damage, never debris (RAFT.md §3, D-022).
// D-041: the crash-safe adoption and the store identity marker.
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
    /// The store's marker says this store lost state: a refusal wrote it before
    /// it traced anything, and only an install replaces it.
    // D-044: a durable refusal, and a refused engine that does no work.
    MarkedLost,
    /// Neither copy of the store's format record can be read, beside a store:
    /// damage, never another format, since this build writes only its own and
    /// the record's encoding is permanent (PROPOSED D-060). The adoption of the
    /// re-seed that follows rewrites it.
    // PROPOSED(D-060)
    FormatUnreadable,
    /// Neither copy of a staged install's format record can be read: staging
    /// damage, refused and never swept, like a staged `CURRENT` that does not
    /// parse (D-041).
    // PROPOSED(D-060)
    StagingFormatUnreadable,
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
            Damage::MarkedLost => write!(f, "the {STORE_MARKER} marker says this store lost state"),
            Damage::FormatUnreadable => write!(
                f,
                "the {} record beside the store cannot be read",
                crate::format::FORMAT_FILE
            ),
            Damage::StagingFormatUnreadable => write!(
                f,
                "the staging directory's {} record cannot be read",
                crate::format::FORMAT_FILE
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
            lost_mark: None,
        };
        // The engine's own rule for a hole in the middle of the state
        // (D-044), so the fields above and the engine's decision to
        // start such an open quiesced can never disagree.
        recovery.lost_writes().then_some(lost)
    }

    /// The refusal for damage found before the engine opened: nothing recovered,
    /// since nothing was opened.
    // D-041: the crash-safe adoption and the store identity marker.
    #[must_use]
    pub fn from_damage(damage: Damage) -> Self {
        Self {
            dropped: Vec::new(),
            fallback_from: None,
            head_gap: None,
            log_stop: None,
            covered_stops: Vec::new(),
            damaged: Some(damage),
            lost_mark: None,
        }
    }

    /// The refusal a store's own marker records: the reason the refusal that
    /// wrote it gave, read back at an open that may be days and several
    /// processes later. Only an install replaces the store and with it the
    /// marker (RAFT.md §3).
    // D-044: a durable refusal, and a refused engine that does no work.
    #[must_use]
    pub fn from_mark(reason: String) -> Self {
        Self {
            lost_mark: Some(reason),
            ..Self::from_damage(Damage::MarkedLost)
        }
    }

    /// The refusal an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<LostState> {
        error.get_ref()?.downcast_ref::<LostState>().cloned()
    }

    /// This refusal as the `InvalidData` error [`RaftStore::open`] and the
    /// adoption fail with, which [`from_io`](Self::from_io) reads back.
    // D-041: the crash-safe adoption and the store identity marker.
    pub(crate) fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, self)
    }
}

impl std::fmt::Display for LostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.damaged {
            Some(damage) => write!(f, "the store is damaged: {damage}")?,
            None => write!(f, "{LOST_STATE}:")?,
        }
        // D-044: the reason the refusal that marked the store gave,
        // carried word for word so the story survives the restart that reads it.
        if let Some(reason) = &self.lost_mark {
            write!(f, ", recorded at the refusal: {reason}")?;
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
///
/// The marker also carries the refusal itself (D-044): a store whose
/// recovery lost state is marked lost, with the reason, before the server traces
/// anything, and every open after that refuses on the mark alone until an
/// install replaces the store. A refusal that lives only in the running process
/// is undone by the next restart, which is how the premerge's seed 687 let a
/// voter with a hole in its state machine rejoin.
// D-041: the crash-safe adoption and the store identity marker.
pub const STORE_MARKER: &str = "RAFT-STORE";

/// How a refusal's reason begins when the engine opened and its recovery lost
/// writes, as opposed to a store found damaged before the engine could open
/// (`the store is damaged: ...`, which a marked-lost re-refusal also says). Only
/// an engine that opened has a replayed memtable to flush over the loss
/// (D-044), so the sweep's predicate for seed 687's shape reads the
/// refusal by this prefix.
pub const LOST_STATE: &str = "the engine's recovery lost state";

/// The marker's path under `engine_dir`.
// D-041: the crash-safe adoption and the store identity marker.
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

/// What a whole store's marker holds.
// D-044: a durable refusal, and a refused engine that does no work.
const MARKER_WHOLE: &[u8] = b"ananke raft store\n";

/// What a lost store's marker starts with; the refusal's reason follows, on one
/// line of its own.
// D-044: a durable refusal, and a refused engine that does no work.
const MARKER_LOST: &[u8] = b"ananke raft store lost\n";

/// What `engine_dir`'s marker says.
// D-044: a durable refusal, and a refused engine that does no work.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Marker {
    /// No marker: the directory has never opened as a store.
    Absent,
    /// A store, whole as far as the marker knows.
    Whole,
    /// A store that lost state, with the reason the refusal recorded.
    Lost(String),
}

/// What `engine_dir`'s marker says. Any content that is not exactly a whole
/// store's is a lost one: the marker is written in place, so a write that a
/// crash cut short leaves a file that is neither, and the store it stands for is
/// the one the refusal was writing about (RAFT.md §3, D-022).
// D-044: a durable refusal, and a refused engine that does no work.
async fn marker<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<Marker> {
    let Some(bytes) = read_whole(env, &marker_path(engine_dir)).await? else {
        return Ok(Marker::Absent);
    };
    if bytes.as_ref() == MARKER_WHOLE {
        return Ok(Marker::Whole);
    }
    let reason = match bytes.as_ref().strip_prefix(MARKER_LOST) {
        Some(rest) => String::from_utf8_lossy(rest).trim().to_owned(),
        None => "the marker itself cannot be read".to_owned(),
    };
    Ok(Marker::Lost(reason))
}

/// The check before the engine opens: a directory whose store marker
/// ([`STORE_MARKER`]) says the store lost state is refused, whatever is on disk
/// now, and one that carries a whole store's marker must hold a `CURRENT` that
/// parses, or it is a lost store. A directory without the marker passes,
/// whatever else it holds: the engine decides whether that is a fresh store or
/// one it refuses (D-024).
///
/// A lost mark outlives the process that wrote it and every restart after it,
/// so a store refused once is refused at every open until an install replaces
/// it and writes the marker fresh (D-044). Without it the refusal
/// lived only in the running server, and a store the refused engine had since
/// flushed into self-consistency opened clean at the next start: the premerge's
/// seed 687.
///
/// # Errors
///
/// `InvalidData` carrying a [`LostState`] with [`Damage::MarkedLost`],
/// [`Damage::MarkedCurrentMissing`] or [`Damage::MarkedCurrentUnreadable`];
/// otherwise the filesystem's.
// D-041: the crash-safe adoption and the store identity marker.
// D-044: a durable refusal, and a refused engine that does no work.
pub async fn refuse_lost_store<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<()> {
    match marker(env, engine_dir).await? {
        Marker::Absent => return Ok(()),
        Marker::Lost(reason) => return Err(LostState::from_mark(reason).into_io()),
        Marker::Whole => {}
    }
    match read_whole(env, &manifest::current_path(engine_dir)).await? {
        None => Err(LostState::from_damage(Damage::MarkedCurrentMissing).into_io()),
        Some(bytes) if manifest::parse_current(&bytes).is_none() => {
            Err(LostState::from_damage(Damage::MarkedCurrentUnreadable).into_io())
        }
        Some(_) => Ok(()),
    }
}

/// Writes the marker into `engine_dir`, synced, with the directory synced after,
/// unless it already says the store is whole. Called after the engine and the
/// store have opened successfully, so a directory is marked as a store once it
/// has been one — a fresh directory at its first open, or a store from before
/// the marker existed at its next — and by the adoption of an installed
/// snapshot, which replaces the store and with it whatever the marker said
/// about the one before (D-044).
///
/// # Errors
///
/// The filesystem's.
// D-041: the crash-safe adoption and the store identity marker.
pub async fn mark_store<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<()> {
    if marker(env, engine_dir).await? == Marker::Whole {
        return Ok(());
    }
    write_marker(env, engine_dir, Bytes::from_static(MARKER_WHOLE)).await
}

/// Records in `engine_dir`'s marker that this store lost state, with `reason`,
/// synced, with the directory synced after: the first thing a refusal does, and
/// the reason every open after it refuses too, until an install replaces the
/// store (RAFT.md §3). It is written through the filesystem and not through the
/// engine, which is the thing that is damaged.
///
/// # Errors
///
/// The filesystem's.
// D-044: a durable refusal, and a refused engine that does no work.
pub async fn mark_store_lost<E: Environment>(
    env: &E,
    engine_dir: &Path,
    reason: &str,
) -> io::Result<()> {
    let mut content = Vec::with_capacity(MARKER_LOST.len() + reason.len() + 1);
    content.extend_from_slice(MARKER_LOST);
    content.extend(reason.bytes().map(|b| if b == b'\n' { b' ' } else { b }));
    content.push(b'\n');
    write_marker(env, engine_dir, Bytes::from(content)).await
}

/// Writes the marker's content in place, truncating what was there, synced, with
/// the directory synced after. A crash part way leaves a marker that is neither
/// form, which reads as a lost store.
// D-044: a durable refusal, and a refused engine that does no work.
async fn write_marker<E: Environment>(
    env: &E,
    engine_dir: &Path,
    content: Bytes,
) -> io::Result<()> {
    let fs = env.fs();
    let file = fs
        .open(
            &marker_path(engine_dir),
            OpenOptions::new().write(true).create(true).truncate(true),
        )
        .await?;
    file.write_at(0, content).await?;
    file.sync().await?;
    fs.sync_dir(engine_dir).await
}

/// The last snapshot, as `<prefix> / 3 / snapshot` records it (RAFT.md §3): written into
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
    /// The store's take counter as of this record: every take numbers its own
    /// directory with the next count, so two takes at one index are two
    /// directories and a restart continues the numbering. Zero for a store that
    /// never took one, and after an install, whose versions start over; a name
    /// that recurs after an install is an empty directory by then, swept before
    /// the incarnation's tasks run. (D-043).
    pub take: u64,
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
    // D-035: re-seeded servers are quarantined from voting for good.
    pub quarantined: bool,
}

pub(crate) fn encode_snapshot_record(record: &SnapshotRecord) -> Bytes {
    let mut out = BytesMut::with_capacity(64);
    out.put_u64_le(record.last_index);
    out.put_u64_le(record.last_term);
    out.put_u8(u8::from(record.taken));
    // D-043: the take counter rides between the flag and the directory.
    out.put_u64_le(record.take);
    out.put_u32_le(u32::try_from(record.dir.len()).expect("directory fits u32"));
    out.put_slice(record.dir.as_bytes());
    put_payload(&mut out, &Payload::Config(record.config.clone()));
    out.freeze()
}

pub(crate) fn decode_snapshot_record(mut bytes: Bytes) -> io::Result<SnapshotRecord> {
    if bytes.len() < 29 {
        return Err(bad("snapshot record"));
    }
    let last_index = bytes.get_u64_le();
    let last_term = bytes.get_u64_le();
    let taken = match bytes.get_u8() {
        0 => false,
        1 => true,
        _ => return Err(bad("snapshot record malformed")),
    };
    let take = bytes.get_u64_le();
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
        take,
    })
}

/// The Raft state in the engine, for one server. Shared between the task that
/// persists and the task that applies; see the module documentation.
pub struct RaftStore<E: Environment> {
    engine: Arc<Engine<E>>,
    /// The group's key prefix: every key this store reads or writes is under it.
    // PROPOSED(D-060): the store parameterised by a key prefix (Q40).
    prefix: KeyPrefix,
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
    // D-042: store incarnations.
    incarnation: u64,
}

impl<E: Environment> RaftStore<E> {
    /// Loads the state the engine holds under `prefix`: the hard state, the
    /// applied index, the snapshot record and the log's tail past it, in index
    /// order. `recovery` is what the engine's open reported, and `checked` the
    /// proof that the directory's on-disk format is this build's (D-059): it
    /// carries the directory it was taken for, so no key of an ungated store can
    /// be read here. Log keys the snapshot covers are deleted here: a crash
    /// between a snapshot's record and its compaction leaves them, and this
    /// cleanup is idempotent.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when `checked` names another directory; `InvalidData`
    /// carrying a [`LostState`] when the recovery lost writes in the middle of
    /// the state; the engine's; or `InvalidData` for a value that is not what was
    /// written.
    pub async fn open(
        engine: Arc<Engine<E>>,
        recovery: &EngineRecovery,
        prefix: KeyPrefix,
        checked: FormatChecked,
    ) -> io::Result<(Self, Recovered)> {
        // PROPOSED(D-060): the token binds its directory, so the format that was
        // read is this engine's and not another store's.
        if checked.dir() != engine.dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the store's format was checked for another directory",
            ));
        }
        if let Some(lost) = LostState::of(recovery) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, lost));
        }
        Self::open_checked(engine, prefix).await
    }

    /// Another range's store on the engine this one already opened (SHARD.md §2,
    /// §4): the node has one engine and a store per range, each under its own
    /// prefix.
    ///
    /// The two checks [`open`](Self::open) makes first are the *engine's* and not
    /// the store's — that the directory's format was read for this engine (D-059)
    /// and that the engine's recovery lost nothing in the middle (D-044) — and
    /// both were made when the engine was opened, for every store on it. Nothing
    /// here re-reads them; everything else — the incarnation key a fresh store
    /// writes, the stale log keys a crash left, the configuration key read back
    /// against the log — is per prefix and is done again for this one.
    ///
    /// # Errors
    ///
    /// The engine's, or `InvalidData` for a value that is not what was written.
    // PROPOSED(D-076): one engine, a store per range.
    pub async fn open_sibling(&self, prefix: KeyPrefix) -> io::Result<(Self, Recovered)> {
        Self::open_checked(self.engine.clone(), prefix).await
    }

    /// The part of [`open`](Self::open) that is about the prefix: everything after
    /// the two checks the engine answers for.
    async fn open_checked(
        engine: Arc<Engine<E>>,
        prefix: KeyPrefix,
    ) -> io::Result<(Self, Recovered)> {
        let (term, vote) = match engine.get(&prefix.hard_key()).await? {
            None => (0, None),
            Some(bytes) => decode_hard(bytes)?,
        };
        let applied = match engine.get(&prefix.applied_key()).await? {
            None => 0,
            Some(bytes) => decode_applied(bytes)?,
        };
        let stored_config = match engine.get(&prefix.config_key()).await? {
            None => None,
            Some(bytes) => Some(decode_config(bytes)?),
        };
        let record = match engine.get(&prefix.snapshot_key()).await? {
            None => None,
            Some(bytes) => Some(decode_snapshot_record(bytes)?),
        };
        let quarantined = engine.get(&prefix.quarantine_key()).await?.is_some();
        // The incarnation number: a fresh store starts at the first and writes
        // it here, synced, so every store carries the key explicitly; an
        // installed store carries the one its repair wrote.
        // D-042: store incarnations.
        let incarnation = match engine.get(&prefix.incarnation_key()).await? {
            Some(bytes) => decode_incarnation(bytes)?,
            None => {
                let mut first = WriteBatch::new();
                first.put(
                    prefix.incarnation_key(),
                    encode_incarnation(FIRST_INCARNATION),
                );
                engine.write(first, true).await?;
                FIRST_INCARNATION
            }
        };
        let snap_index = record.as_ref().map_or(0, |r| r.last_index);
        let snapshot = engine.snapshot();
        let log_span = prefix.purpose_span(PURPOSE_LOG);
        let (start, end) = (log_span.start, log_span.end);
        let mut log = Vec::new();
        let mut stale = WriteBatch::new();
        for (k, value) in engine.scan(&start[..]..&end[..], &snapshot).await? {
            let index = prefix.log_index(&k).ok_or_else(|| bad("log key"))?;
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
                prefix,
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

    /// Checks the directory's format, records it if the directory is fresh, heals
    /// a record with one damaged copy, and then opens the engine and the store on
    /// it: what a crate user outside the server does instead of assembling the
    /// start by hand. It neither adopts a staged install nor reads the store
    /// marker, which [`open`](Self::open) never did either; the server's own
    /// start, [`node::start_store`](crate::node::start_store), does all of it in
    /// order.
    ///
    /// # Errors
    ///
    /// `InvalidData` carrying a [`FormatRefused`](crate::format::FormatRefused)
    /// for a directory in another format, which is not a loss and leaves the
    /// directory untouched (D-059); `InvalidData` carrying a [`LostState`] when
    /// the record cannot be read beside a store, or when the recovery lost
    /// writes; otherwise the engine's or the filesystem's.
    // PROPOSED(D-060)
    pub async fn open_dir(
        env: E,
        config: EngineConfig,
        prefix: KeyPrefix,
    ) -> io::Result<(Self, Recovered)> {
        let dir = config.dir.clone();
        let checked = match format::check_format(&env, &dir).await? {
            format::Verdict::Fresh(fresh) => format::record_format(&env, fresh).await?,
            format::Verdict::Recorded {
                checked,
                whole,
                len,
            } => {
                if !whole {
                    format::heal_format(&env, &checked, len).await?;
                }
                checked
            }
            // D-044: a record that cannot be read beside a store is lost state.
            format::Verdict::Damaged => {
                return Err(LostState::from_damage(Damage::FormatUnreadable).into_io());
            }
        };
        let (engine, recovery) = Engine::open(env, config).await?;
        let engine = Arc::new(engine);
        match Self::open(engine.clone(), &recovery, prefix, checked).await {
            Ok(opened) => Ok(opened),
            Err(error) => {
                // D-044: the engine that recovered a hole does no more work.
                engine.quiesce();
                Err(error)
            }
        }
    }

    /// The group's key prefix: what every key of this store is under.
    // PROPOSED(D-060): the store parameterised by a key prefix (Q40).
    #[must_use]
    pub fn prefix(&self) -> &KeyPrefix {
        &self.prefix
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

    /// The applied index as of `snapshot`: the value under the applied-index key
    /// at that engine version, which the apply batch that wrote it wrote with the
    /// entry's own writes. A read taken at the same snapshot is therefore the
    /// state at exactly this index, which is what a served read records
    /// (SHARD.md §8, §11 raft 15).
    ///
    /// **The one version is one version while no live span install and no range
    /// delete runs on this engine.** [`Engine::get_at`] excepts exactly those: from
    /// an install's or a range delete's switch, a read at a version *below* the
    /// install's number sees the span it replaced as empty rather than as it stood
    /// (D-054, and [`Snapshot`]'s own documentation). A value and an applied index
    /// taken at one version could then straddle a switch and be the state at no
    /// index at all. Nothing can reach that today: `Engine::install_span`,
    /// `install_spans`, `delete_range` and `delete_ranges` have no caller outside
    /// ananke-storage's own tests and the engine sweep — none in this crate, in
    /// `ananke-server` or in the raft scenarios — and a server's engine is fixed for
    /// the life of its incarnation, a re-seed opening a fresh one in a new directory
    /// behind a restart. Stage B's live per-range install (SHARD.md §11, storage 8)
    /// is what makes the straddle possible, and that item carries this caveat, filed
    /// as issue #72: it must say what a read served across an install of its own
    /// range sees.
    ///
    /// Zero where the key is absent at that version, which is to say where nothing
    /// had applied when it was taken. A served read cannot see it: the core holds a
    /// confirmed read until `applied >= index` and a read's index is at or above the
    /// leader's first entry of its term, so at least one apply — which writes this
    /// key in its own batch — is durable at the version the read is served from. The
    /// raft sweeps fold that: every `RaftRead`'s `applied` is at or above its index
    /// (`sim/raft.rs`), and a zero here would be below it.
    ///
    /// # Errors
    ///
    /// The engine's, or `InvalidData` if the value is not eight bytes.
    // PROPOSED(D-069): a read is served, and its applied index taken, at one
    // engine version.
    pub async fn applied_at(&self, snapshot: &Snapshot<E>) -> io::Result<Index> {
        match self
            .engine
            .get_at(&self.prefix.applied_key(), snapshot)
            .await?
        {
            None => Ok(0),
            Some(bytes) => decode_applied(bytes),
        }
    }

    /// The store's incarnation number (RAFT.md §3): what this server's
    /// AppendEntries and InstallSnapshot responses carry.
    // D-042: store incarnations.
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

    /// Restates the cached hard state, log bounds and applied index after a **live
    /// span install** has written this range's keys behind them.
    ///
    /// Every one of those fields is an in-memory cache of a key this store writes
    /// itself: [`persist`](Self::persist) writes the hard state and the log, and
    /// [`apply`](Self::apply) the applied index, each updating its cache as it goes.
    /// A live install (`Engine::install_spans`, D-066, D-068) writes all of them in
    /// one manifest switch that passes through neither, so the caches are left
    /// describing the replica the switch replaced — and the store then refuses the
    /// range's next apply, "applying 13 after 10", on an applied index that is the
    /// old replica's.
    ///
    /// A server needs none of this: it ends its run-loop incarnation across an
    /// install and opens the store again (RAFT.md §1), which builds every cache from
    /// the keys. A node cannot, because reopening the engine would restart every
    /// range on it (SHARD.md §11, storage 5), so the values the switch made durable
    /// are handed back here instead. They are the repair's own, which is why this
    /// takes them rather than reading them again: the caller has just written them.
    // PROPOSED(D-086): a live install restates the caches a server gets from
    // reopening its store.
    pub fn restate_after_install(
        &self,
        term: Term,
        vote: Option<ServerId>,
        first_index: Index,
        last_index: Index,
        applied: Index,
    ) {
        self.term.store(term, Ordering::Release);
        self.vote
            .store(vote.map_or(NO_VOTE, |id| id.0), Ordering::Release);
        self.first_index.store(first_index, Ordering::Release);
        self.last_index.store(last_index, Ordering::Release);
        self.applied.store(applied, Ordering::Release);
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
        batch.put(self.prefix.snapshot_key(), encode_snapshot_record(record));
        self.engine.write(batch, true).await?;
        Ok(())
    }

    /// Reads the snapshot record back, if the store holds one.
    ///
    /// # Errors
    ///
    /// The engine's, or `InvalidData` for a value that is not a record.
    pub async fn snapshot_record(&self) -> io::Result<Option<SnapshotRecord>> {
        match self.engine.get(&self.prefix.snapshot_key()).await? {
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
            batch.put(self.prefix.hard_key(), out.freeze());
        }
        let mut first_index = self.first_index();
        let mut last_index = self.last_index();
        if let Some(to) = persist.compact_to {
            // The log compacted to a snapshot: the engine's compaction reclaims
            // the space in its own time (RAFT.md §3).
            for index in first_index..=to.min(last_index) {
                batch.delete(self.prefix.log_key(index));
            }
            first_index = first_index.max(to + 1);
            last_index = last_index.max(to);
        }
        if let Some(from) = persist.truncate_from {
            for index in from..=last_index {
                batch.delete(self.prefix.log_key(index));
            }
            last_index = from.saturating_sub(1).min(last_index);
        }
        for entry in &persist.append {
            batch.put(self.prefix.log_key(entry.index), encode_entry(entry));
            last_index = last_index.max(entry.index);
        }
        if let Some((index, config)) = &persist.config {
            batch.put(self.prefix.config_key(), encode_config(*index, config));
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
        writes.put(self.prefix.applied_key(), out.freeze());
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
// D-042: store incarnations.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply::{SYSTEM_TENANT, USER_TENANT, user_key};
    use crate::node::SINGLE_GROUP;

    /// The groups the layout is checked over: the first few and the last, so a
    /// prefix's arithmetic is held at both ends.
    const GROUPS: [u64; 5] = [0, 1, 2, 3, u64::MAX];

    /// Every key a group's store writes, for the checks below.
    fn keys(prefix: &KeyPrefix) -> Vec<(&'static str, Bytes)> {
        let mut keys = vec![
            ("hard", prefix.hard_key()),
            ("applied", prefix.applied_key()),
            ("reseeded", prefix.quarantine_key()),
            ("incarnation", prefix.incarnation_key()),
            ("config", prefix.config_key()),
            ("snapshot", prefix.snapshot_key()),
        ];
        for index in [0, 1, 1 << 63, u64::MAX] {
            keys.push(("log", prefix.log_key(index)));
        }
        keys
    }

    /// The purpose a key names: bytes 16 to 24, after the prefix.
    fn purpose_of(prefix: &KeyPrefix, key: &[u8]) -> u64 {
        u64::from_be_bytes(
            key[prefix.as_bytes().len()..][..8]
                .try_into()
                .expect("eight"),
        )
    }

    /// Every key of a group lies under its prefix, inside its purpose's span and
    /// inside the group's, and inside no other group's and no other purpose's
    /// (PROPOSED D-060). Purpose 4 is empty: the room #21's session table and a
    /// range descriptor take (Q11).
    #[test]
    fn every_raft_key_lies_under_its_group_prefix_and_purpose() {
        for group in GROUPS {
            let prefix = KeyPrefix::group(group);
            assert_eq!(prefix.as_bytes().len(), 16);
            assert_eq!(&prefix.as_bytes()[..8], &RAFT_TENANT.to_be_bytes());
            assert_eq!(&prefix.as_bytes()[8..], &group.to_be_bytes());
            let span = prefix.span();
            for (what, key) in keys(&prefix) {
                assert!(
                    key.starts_with(prefix.as_bytes()),
                    "group {group}'s {what} key is not under its prefix"
                );
                assert!(
                    key[..] >= span.start[..] && key[..] < span.end[..],
                    "group {group}'s {what} key is outside its span"
                );
                let purpose = purpose_of(&prefix, &key);
                let own = prefix.purpose_span(purpose);
                assert!(key[..] >= own.start[..] && key[..] < own.end[..]);
                for other in [
                    PURPOSE_META,
                    PURPOSE_LOG,
                    PURPOSE_CONFIG,
                    PURPOSE_SNAPSHOT,
                    4,
                ] {
                    if other == purpose {
                        continue;
                    }
                    let span = prefix.purpose_span(other);
                    assert!(
                        key[..] < span.start[..] || key[..] >= span.end[..],
                        "group {group}'s {what} key is in purpose {other}'s span"
                    );
                }
                // And in no other group's span.
                for elsewhere in GROUPS.iter().filter(|g| **g != group) {
                    let span = KeyPrefix::group(*elsewhere).span();
                    assert!(
                        key[..] < span.start[..] || key[..] >= span.end[..],
                        "group {group}'s {what} key is in group {elsewhere}'s span"
                    );
                }
            }
            // The purposes are disjoint and in order, and purpose 4 is free.
            let spans: Vec<_> = (0..5).map(|p| prefix.purpose_span(p)).collect();
            for pair in spans.windows(2) {
                assert!(pair[0].end[..] <= pair[1].start[..], "the purposes overlap");
            }
            let free = prefix.purpose_span(4);
            assert!(
                keys(&prefix)
                    .iter()
                    .all(|(_, key)| key[..] < free.start[..] || key[..] >= free.end[..]),
                "purpose 4 is not free for #21's session table"
            );
            // A log key is read back, and nothing else is read as one.
            for index in [0, 1, 1 << 63, u64::MAX] {
                assert_eq!(prefix.log_index(&prefix.log_key(index)), Some(index));
            }
            // And the log sorts by index. `log_index` round-trips a
            // little-endian index just as well, so only this says which way
            // round the bytes go — and `RaftStore::open_dir` reads the log by
            // scanning the purpose's span in the engine's key order and refuses
            // a log whose indices are not consecutive. With the bytes the other
            // way round, key 256 sorts before key 1, so the first store to hold
            // more than 255 entries above its snapshot would be refused as lost
            // state at every restart and re-seeded.
            for (lower, higher) in [
                (0u64, 1u64),
                (127, 128),
                (255, 256),
                (256, 257),
                (65_535, 65_536),
                (1 << 32, (1 << 32) + 1),
                (u64::MAX - 1, u64::MAX),
            ] {
                assert!(
                    prefix.log_key(lower)[..] < prefix.log_key(higher)[..],
                    "the log key of {lower} does not sort before the log key of {higher}"
                );
            }
            assert_eq!(prefix.log_index(&prefix.hard_key()), None);
            assert_eq!(prefix.log_index(&prefix.key(PURPOSE_LOG, b"xy")), None);
            assert_eq!(prefix.log_index(&prefix.key(PURPOSE_LOG, &[0; 9])), None);
            assert_eq!(prefix.log_index(&prefix.snapshot_key()), None);
            assert_eq!(
                KeyPrefix::group(group ^ 1).log_index(&prefix.log_key(7)),
                None,
                "a log key of another group"
            );
        }
    }

    /// No key this build writes is a key ananke-raft 0.3.0 wrote: the defence in
    /// depth behind the gate (PROPOSED D-060, D.3). 0.3.0's tenant-0 keys are 20
    /// to 24 bytes, or 27 for `incarnation`; format 2's are 28 at the shortest,
    /// and no name of three bytes exists to make one 27.
    #[test]
    fn no_format_2_key_has_a_0_3_0_shape() {
        /// A key as 0.3.0 wrote it, spelled out rather than built by this build.
        fn v030_key(tenant: u64, table: u64, name: &[u8]) -> Vec<u8> {
            let mut out = tenant.to_be_bytes().to_vec();
            out.extend_from_slice(&table.to_be_bytes());
            out.extend_from_slice(name);
            out
        }
        let mut theirs: Vec<Vec<u8>> = vec![
            v030_key(0, 0, b"hard"),
            v030_key(0, 0, b"applied"),
            v030_key(0, 0, b"reseeded"),
            v030_key(0, 0, b"incarnation"),
            v030_key(0, 2, b"config"),
            v030_key(0, 3, b"snapshot"),
        ];
        for index in [0u64, 1, 1 << 63, u64::MAX] {
            theirs.push(v030_key(0, 1, &index.to_be_bytes()));
            // 0.3.0's user keys, tenant 1.
            theirs.push(v030_key(1, 0, format!("k{index}").as_bytes()));
        }
        assert!(
            theirs
                .iter()
                .filter(|key| key[..8] == [0; 8])
                .all(|key| key.len() <= 27),
            "0.3.0's tenant-0 keys are 27 bytes at most"
        );
        for group in GROUPS {
            let prefix = KeyPrefix::group(group);
            for (what, key) in keys(&prefix) {
                assert!(
                    key.len() >= 28,
                    "a format 2 {what} key of {} bytes",
                    key.len()
                );
                assert!(
                    !theirs.iter().any(|old| old[..] == key[..]),
                    "a format 2 {what} key is a key 0.3.0 wrote"
                );
            }
        }
        for name in [b"k".as_slice(), b"a", b"kkk"] {
            let key = user_key(name);
            assert!(
                !theirs.iter().any(|old| old[..] == key[..]),
                "a user key is a key 0.3.0 wrote"
            );
            assert_eq!(&key[..8], &USER_TENANT.to_be_bytes());
        }
    }

    /// The tenants of SHARD.md §1: the protocol's is 0, the system tenant is 1
    /// and nothing here writes it, and the user's data is tenant 2 (PROPOSED
    /// D-060).
    #[test]
    fn user_keys_are_tenant_2_and_tenant_1_is_the_system_tenant() {
        assert_eq!((RAFT_TENANT, SYSTEM_TENANT, USER_TENANT), (0, 1, 2));
        assert_eq!(&user_key(b"k")[..8], &2u64.to_be_bytes());
        assert_eq!(&user_key(b"k")[8..16], &0u64.to_be_bytes());
        assert_eq!(&user_key(b"k")[16..], b"k");
        for group in GROUPS {
            let prefix = KeyPrefix::group(group);
            for (what, key) in keys(&prefix) {
                assert_ne!(
                    &key[..8],
                    &SYSTEM_TENANT.to_be_bytes(),
                    "a {what} key under the system tenant"
                );
            }
        }
    }

    /// Today's group is group 2, SHARD.md §2's range 2 (PROPOSED D-060, Q3).
    #[test]
    fn the_single_group_prefix_is_0_2() {
        assert_eq!(SINGLE_GROUP, 2);
        let mut expected = [0u8; 16];
        expected[15] = 2;
        assert_eq!(KeyPrefix::group(SINGLE_GROUP).as_bytes(), expected);
        assert_eq!(
            KeyPrefix::group(SINGLE_GROUP).hard_key().len(),
            28,
            "the shortest key of format 2"
        );
    }
}
