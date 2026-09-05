//! The engine (SPEC.md §2, D-020, D-021, D-022): a write-ahead log in front of
//! memtables, flushed to SSTables under a manifest.
//!
//! A write is one log record, `put` or `delete` with its key and value, appended
//! through the [`Wal`] and applied to the active [`Memtable`] once the log has
//! acknowledged it: nothing is visible before it is durable. Writes are applied in
//! sequence order whatever order their callers are polled in: the log acknowledges
//! in sequence order, so when a caller sees its write acknowledged every earlier
//! write is durable too, and it applies all of them that are still pending, oldest
//! first (D-021). When the active memtable exceeds `memtable_bytes` it becomes
//! immutable and a fresh one takes its place.
//!
//! A flusher task takes immutable memtables oldest first. For each it writes an
//! SSTable and syncs it, writes the next manifest listing the table and syncs it,
//! points `CURRENT` at that manifest by rename and syncs the directory, and only then
//! releases the memtable and deletes the log segments whose records the tables now
//! hold. A crash anywhere before the switch leaves the old manifest in force and the
//! new files as orphans, which recovery removes; the log still has the records.
//! Reads consult the active memtable, the immutable ones newest first, then the
//! tables newest first, and take the newest write of the key at or below the
//! snapshot they read at (D-023). A [`Snapshot`] pins the versions it can see against
//! compaction until it is dropped; a scan merges every memtable and table into one
//! walk in key order and reports the newest write per key at the snapshot, so it is
//! one consistent view whatever is written meanwhile.
//!
//! Recovery reads `CURRENT` and the manifest it names, opens and fully verifies
//! every table listed, dropping one it cannot read and reporting the writes lost
//! with it, removes orphans, and replays the log from one past the manifest's
//! `flushed_seq`. If `CURRENT` or that manifest cannot be read the open fails,
//! unless `allow_manifest_fallback` is set: then the newest older manifest whose
//! every table is intact is used and `CURRENT` rewritten to say so. A log whose
//! first record is past the manifest's head is missing its head: the open fails
//! unless `allow_head_gap` is set, and then the log is discarded and the tables are
//! the state. Either way what comes back is a state that existed.
//!
//! The [`Variant`]s for the crash sweep: [`Variant::Correct`];
//! [`Variant::NoWalBeforeMemtable`], which applies and acknowledges a write before
//! the log has it; and [`Variant::ReleaseBeforeManifest`], which releases a memtable
//! and its log segments once its table is written but before the manifest names it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use ananke_env::{Environment, File, FileSystem, OpenOptions, TraceEvent};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub use crate::compaction::Compaction;
use crate::ikey;
use crate::manifest::{
    self, LEVELS, Manifest, SstMeta, current_path, current_tmp_path, manifest_path, sst_path,
};
use crate::memtable::{Memtable, Value};
use crate::merge::{MergeIter, Source};
use crate::sst::{SstReader, SstWriter};
use crate::turnstile::Turnstile;
use crate::wal::{self, Append, HeadGapPolicy, Recovery, Seq, Wal, WalConfig};

pub(crate) type FileOf<E> = <<E as Environment>::Fs as FileSystem>::File;

/// Which engine to run: the correct one, or a known bug the crash sweep must catch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Variant {
    /// A write is applied and acknowledged only after the log has made it durable; a
    /// memtable is released only after the manifest naming its table is durable.
    #[default]
    Correct,
    /// A write is applied to the memtable and acknowledged at once; the log record is
    /// queued but nobody waits for it. A crash loses acknowledged writes.
    NoWalBeforeMemtable,
    /// A memtable is released, and the log segments it covered deleted, as soon as
    /// its table is written and serving reads, before the manifest names the table.
    /// A crash before the manifest is durable leaves the table an orphan and its
    /// records nowhere.
    ReleaseBeforeManifest,
    /// A compaction deletes its input tables before the manifest stops naming them.
    /// A crash between leaves a manifest naming tables that are gone, and the
    /// outputs as orphans: their writes are nowhere.
    DeleteBeforeManifest,
}

/// How to open an [`Engine`].
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// The directory holding the log, the tables and the manifests; created if missing.
    pub dir: PathBuf,
    /// The active memtable becomes immutable once it accounts for more than this.
    pub memtable_bytes: u64,
    /// The log's segment size.
    pub segment_bytes: u64,
    /// Which engine to run.
    pub variant: Variant,
    /// Which log to run underneath; `wal::Variant::Correct` outside a sweep.
    pub wal_variant: wal::Variant,
    /// Whether to open when `CURRENT` or the manifest it names cannot be read. Off,
    /// `open` fails with an error carrying an [`OpenRefused`]; on, recovery uses the
    /// newest older manifest whose every table is on disk and passes its checks,
    /// never one that lists a missing or damaged table, and fails when
    /// there is none. A rollback onto a manifest whose tables a later compaction had
    /// deleted is a state that never existed (D-022).
    pub allow_manifest_fallback: bool,
    /// Whether to open when the log's head is missing (its first record is past the
    /// manifest's `flushed_seq + 1`). Off, `open` fails with an error carrying a
    /// [`HeadGap`](crate::wal::HeadGap) and touches nothing; on, the log is discarded and the manifest's
    /// tables are the state, a clean prefix. Replaying past the gap would give a
    /// state that never existed (D-022).
    pub allow_head_gap: bool,
    /// Refuse to open, touching nothing, when the log is damaged past a torn tail:
    /// a bad checksum, a gap, or a corrupt record skipped in a segment the tables
    /// cover (`wal::LogDamaged`, D-027). Off, the log is cut at the damage and the
    /// open reports it. A store under Raft sets this: a log shortened once would
    /// read as whole at the next open.
    pub refuse_log_damage: bool,
    /// Level 0 is compacted once it holds this many tables.
    pub l0_trigger: usize,
    /// Level 1 is compacted once it holds more than this many bytes; each deeper
    /// level, ten times more (SPEC §2.5).
    pub level_base_bytes: u64,
    /// A compaction seals an output table once it reaches this size.
    pub sst_bytes: u64,
    /// Whether the flusher runs compaction rounds after each flush until no level is
    /// over its limit. Off, [`Engine::compact_once`] is the only trigger, for tests.
    pub background_compaction: bool,
}

impl EngineConfig {
    /// The defaults for a store in `dir`: a 64 MiB memtable (SPEC §2.3), 16 MiB log
    /// segments, the correct engine and log, a missing log head refused, level 0
    /// compacted at four tables, level 1 at 256 MiB, outputs of 64 MiB, compaction
    /// in the background.
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            memtable_bytes: 64 << 20,
            segment_bytes: 16 << 20,
            variant: Variant::Correct,
            wal_variant: wal::Variant::Correct,
            allow_manifest_fallback: false,
            allow_head_gap: false,
            refuse_log_damage: false,
            l0_trigger: 4,
            level_base_bytes: 256 << 20,
            sst_bytes: 64 << 20,
            background_compaction: true,
        }
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One log record: `tag: u8 | key_len: u32 LE | key | value`, tag 0 for a put and 1
/// for a delete (which carries no value).
#[must_use]
pub fn encode_op(key: &[u8], value: &Value) -> Bytes {
    let len = u32::try_from(key.len()).expect("key length exceeds u32");
    let mut out = BytesMut::with_capacity(5 + key.len() + value_len(value));
    match value {
        Value::Live(bytes) => {
            out.put_u8(0);
            out.put_u32_le(len);
            out.put_slice(key);
            out.put_slice(bytes);
        }
        Value::Tombstone => {
            out.put_u8(1);
            out.put_u32_le(len);
            out.put_slice(key);
        }
    }
    out.freeze()
}

fn value_len(value: &Value) -> usize {
    match value {
        Value::Live(bytes) => bytes.len(),
        Value::Tombstone => 0,
    }
}

/// One log record for a batch of writes applied together: `tag: u8 = 2 | count: u32 LE`,
/// then per write `tag: u8 | key_len: u32 LE | key | value_len: u32 LE | value`, the
/// value absent for a delete (D-024). A batch of one write is encoded as that write.
#[must_use]
pub fn encode_batch(ops: &[(Bytes, Value)]) -> Bytes {
    if let [(key, value)] = ops {
        return encode_op(key, value);
    }
    let mut out = BytesMut::with_capacity(5 + ops.len() * 9);
    out.put_u8(2);
    out.put_u32_le(u32::try_from(ops.len()).expect("batch size exceeds u32"));
    for (key, value) in ops {
        let len = u32::try_from(key.len()).expect("key length exceeds u32");
        match value {
            Value::Live(bytes) => {
                out.put_u8(0);
                out.put_u32_le(len);
                out.put_slice(key);
                out.put_u32_le(u32::try_from(bytes.len()).expect("value length exceeds u32"));
                out.put_slice(bytes);
            }
            Value::Tombstone => {
                out.put_u8(1);
                out.put_u32_le(len);
                out.put_slice(key);
            }
        }
    }
    out.freeze()
}

/// Decodes a record written by [`encode_op`] or [`encode_batch`]: the writes it
/// carries, in order.
///
/// # Errors
///
/// `InvalidData` for anything else.
pub fn decode_record(mut record: Bytes) -> io::Result<Vec<(Bytes, Value)>> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed engine record");
    if record.first() != Some(&2) {
        return decode_op(record).map(|op| vec![op]);
    }
    if record.len() < 5 {
        return Err(bad());
    }
    record.advance(1);
    let count = record.get_u32_le() as usize;
    let mut ops = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if record.len() < 5 {
            return Err(bad());
        }
        let tag = record.get_u8();
        let len = record.get_u32_le() as usize;
        if record.len() < len {
            return Err(bad());
        }
        let key = record.split_to(len);
        let value = match tag {
            0 => {
                if record.len() < 4 {
                    return Err(bad());
                }
                let len = record.get_u32_le() as usize;
                if record.len() < len {
                    return Err(bad());
                }
                Value::Live(record.split_to(len))
            }
            1 => Value::Tombstone,
            _ => return Err(bad()),
        };
        ops.push((key, value));
    }
    if !record.is_empty() {
        return Err(bad());
    }
    Ok(ops)
}

/// Writes applied together and acknowledged together: one log record, one sequence
/// number, all visible at once. A later write to a key in the same batch replaces
/// an earlier one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch {
    ops: Vec<(Bytes, Value)>,
}

impl WriteBatch {
    /// An empty batch.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a put.
    pub fn put(&mut self, key: Bytes, value: Bytes) -> &mut Self {
        self.ops.push((key, Value::Live(value)));
        self
    }

    /// Adds a delete.
    pub fn delete(&mut self, key: Bytes) -> &mut Self {
        self.ops.push((key, Value::Tombstone));
        self
    }

    /// The writes, in order.
    #[must_use]
    pub fn ops(&self) -> &[(Bytes, Value)] {
        &self.ops
    }

    /// Writes in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the batch has no writes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// What [`Engine::checkpoint`] wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// The version the checkpoint is the state at.
    pub version: Seq,
    /// Tables in it.
    pub tables: usize,
}

/// Decodes a record written by [`encode_op`].
///
/// # Errors
///
/// `InvalidData` for anything else.
pub fn decode_op(mut record: Bytes) -> io::Result<(Bytes, Value)> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed engine record");
    if record.len() < 5 {
        return Err(bad());
    }
    let tag = record.get_u8();
    let len = record.get_u32_le() as usize;
    if record.len() < len {
        return Err(bad());
    }
    let key = record.split_to(len);
    match tag {
        0 => Ok((key, Value::Live(record))),
        1 if record.is_empty() => Ok((key, Value::Tombstone)),
        _ => Err(bad()),
    }
}

/// The memtables and tables reads consult, and the manifest in force.
pub(crate) struct Tables<E: Environment> {
    pub(crate) active: Arc<Memtable>,
    pub(crate) immutable: VecDeque<Arc<Memtable>>,
    /// The tables in service, in the order they were put there: a dropped table is
    /// not here though the manifest may still list it.
    pub(crate) ssts: Vec<(SstMeta, Arc<SstReader<FileOf<E>>>)>,
    pub(crate) manifest: Manifest,
}

impl<E: Environment> Tables<E> {
    /// The readers a lookup of `key` consults after the memtables: level 0 newest
    /// first, then the one table per deeper level whose range holds the key.
    fn readers_for(&self, key: &[u8]) -> Vec<Arc<SstReader<FileOf<E>>>> {
        let mut l0: Vec<&(SstMeta, Arc<SstReader<FileOf<E>>>)> =
            self.ssts.iter().filter(|(m, _)| m.level == 0).collect();
        l0.sort_by_key(|(m, _)| std::cmp::Reverse(m.number));
        let mut readers: Vec<Arc<SstReader<FileOf<E>>>> =
            l0.into_iter().map(|(_, r)| r.clone()).collect();
        for level in 1..LEVELS as u8 {
            if let Some((_, r)) = self
                .ssts
                .iter()
                .find(|(m, _)| m.level == level && m.contains(key))
            {
                readers.push(r.clone());
            }
        }
        readers
    }
}

struct Flusher {
    waker: Option<Waker>,
    closed: bool,
}

pub(crate) struct Shared<E: Environment> {
    pub(crate) env: E,
    pub(crate) config: EngineConfig,
    wal: Wal<E>,
    pub(crate) tables: Mutex<Tables<E>>,
    flusher: Mutex<Flusher>,
    /// One flush or compaction at a time (D-023).
    turnstile: Turnstile,
    /// The number the next table gets.
    pub(crate) next_sst: AtomicU64,
    /// Per level, the last key the last compaction round on it wrote, so rounds walk
    /// the level.
    pub(crate) compact_pointer: Mutex<Vec<Option<Bytes>>>,
    next_memtable: AtomicU64,
    /// Writes appended and not yet applied, by sequence number: each record's
    /// writes in order.
    pending: Mutex<BTreeMap<Seq, Vec<(Bytes, Value)>>>,
    /// The highest sequence number applied: what a read without a snapshot reads at,
    /// and what a new snapshot pins. Writes apply in order (D-021), so everything at
    /// or below it is visible.
    pub(crate) visible: AtomicU64,
    /// Live snapshots by sequence number, with how many pin each.
    pub(crate) snapshots: Mutex<BTreeMap<Seq, usize>>,
}

/// Why [`Engine::open`] refused a store (D-022): what is on disk cannot be trusted
/// to be a state that existed, and no fallback the configuration allows is intact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenRefused {
    /// `CURRENT` exists but cannot be read: torn, or a bit flipped.
    CurrentUnreadable,
    /// `CURRENT` is missing while manifests or tables are on disk: a checkpoint
    /// written only part way, or a store damaged past recognition. Every store has
    /// a `CURRENT` from its first open on.
    CurrentMissing,
    /// The manifest `CURRENT` names cannot be read.
    ManifestUnreadable(u64),
    /// Fallback was allowed, but no older manifest has every table it lists on disk
    /// and intact. `named` is the manifest `CURRENT` named, 0 if `CURRENT` itself
    /// could not be read.
    NoIntactManifest {
        /// See above.
        named: u64,
    },
}

impl OpenRefused {
    /// The refusal an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<OpenRefused> {
        error.get_ref()?.downcast_ref::<OpenRefused>().cloned()
    }

    fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, self)
    }
}

impl std::fmt::Display for OpenRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenRefused::CurrentUnreadable => write!(f, "CURRENT cannot be read"),
            OpenRefused::CurrentMissing => {
                write!(
                    f,
                    "CURRENT is missing while manifests or tables are on disk"
                )
            }
            OpenRefused::ManifestUnreadable(n) => {
                write!(f, "MANIFEST-{n:06}, which CURRENT names, cannot be read")
            }
            OpenRefused::NoIntactManifest { named } => write!(
                f,
                "no manifest older than {named} has every table it lists on disk and intact"
            ),
        }
    }
}

impl std::error::Error for OpenRefused {}

/// Why a fallback passed over a manifest (D-022).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rejected {
    /// The manifest file is missing, torn or corrupt.
    Unreadable,
    /// A table it lists is not on disk.
    TableMissing(u64),
    /// A table it lists does not open or fails its checks.
    TableDamaged(u64),
}

/// What [`Engine::open`] found.
#[derive(Clone, Debug)]
pub struct EngineRecovery {
    /// The manifest in force; 0 is the empty state.
    pub manifest: u64,
    /// Set when the manifest `CURRENT` named could not be read and an older intact
    /// one was used, or when `CURRENT` itself could not be read (then 0): everything
    /// flushed after the one used is lost.
    pub fallback_from: Option<u64>,
    /// The manifests a fallback passed over on its way to the one used, newest
    /// first, each with why.
    pub rejected: Vec<(u64, Rejected)>,
    /// Every log record numbered this or below is in a table, if its table survived.
    pub flushed_seq: Seq,
    /// Tables in service.
    pub ssts: usize,
    /// Every table the manifest lists, dropped ones included: what the file on disk
    /// says, whether or not the trace saw it written.
    pub tables: Vec<SstMeta>,
    /// Tables the manifest lists that could not be read; their writes are lost.
    pub dropped: Vec<SstMeta>,
    /// Files no manifest referred to, removed.
    pub orphans: usize,
    /// What the log recovered.
    pub wal: Recovery,
    /// Log records replayed into memtables: those past `flushed_seq`.
    pub replayed: usize,
}

/// A write-ahead log in front of memtables and tables. Dropping it closes the log and
/// stops the flusher once the queue is empty.
pub struct Engine<E: Environment> {
    shared: Arc<Shared<E>>,
}

/// A point in the engine's history: reads at it see every write numbered at or
/// below its version and nothing newer, and compaction keeps the versions it needs
/// until it is dropped.
pub struct Snapshot<E: Environment> {
    shared: Arc<Shared<E>>,
    version: Seq,
}

impl<E: Environment> Snapshot<E> {
    /// The sequence number the snapshot reads at.
    #[must_use]
    pub fn version(&self) -> Seq {
        self.version
    }
}

impl<E: Environment> std::fmt::Debug for Snapshot<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("version", &self.version)
            .finish()
    }
}

impl<E: Environment> Drop for Snapshot<E> {
    fn drop(&mut self) {
        let mut snapshots = lock(&self.shared.snapshots);
        if let Some(count) = snapshots.get_mut(&self.version) {
            *count -= 1;
            if *count == 0 {
                snapshots.remove(&self.version);
            }
        }
    }
}

impl<E: Environment> std::fmt::Debug for Engine<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("dir", &self.shared.config.dir)
            .finish_non_exhaustive()
    }
}

/// Points `CURRENT` at manifest `number`: written as `CURRENT.tmp`, synced, renamed
/// over `CURRENT`, and the directory synced. Traced as the store's switch unless
/// `quiet`, which a checkpoint's is.
async fn switch_current_in<E: Environment>(
    env: &E,
    dir: &Path,
    number: u64,
    quiet: bool,
) -> io::Result<()> {
    let fs = env.fs();
    let tmp = fs
        .open(
            &current_tmp_path(dir),
            OpenOptions::new().write(true).create(true).truncate(true),
        )
        .await?;
    tmp.write_at(0, manifest::encode_current(number)).await?;
    tmp.sync().await?;
    fs.rename(&current_tmp_path(dir), &current_path(dir))
        .await?;
    fs.sync_dir(dir).await?;
    if !quiet {
        env.trace(TraceEvent::CurrentSwitched { manifest: number });
    }
    Ok(())
}

/// The store's own switch of `CURRENT`.
async fn switch_current<E: Environment>(env: &E, dir: &Path, number: u64) -> io::Result<()> {
    switch_current_in(env, dir, number, false).await
}

/// Writes `bytes` as a new file at `path` and syncs it.
async fn write_file<E: Environment>(env: &E, path: &Path, bytes: Bytes) -> io::Result<()> {
    let file = env
        .fs()
        .open(path, OpenOptions::new().write(true).create_new(true))
        .await?;
    file.write_at(0, bytes).await?;
    file.sync().await
}

/// Writes `manifest` into `dir` and syncs it, traced as the store's unless `quiet`.
async fn write_manifest_in<E: Environment>(
    env: &E,
    dir: &Path,
    manifest: &Manifest,
    quiet: bool,
) -> io::Result<()> {
    write_file(env, &manifest_path(dir, manifest.number), manifest.encode()).await?;
    if !quiet {
        env.trace(TraceEvent::ManifestWritten {
            number: manifest.number,
            flushed_seq: manifest.flushed_seq,
            tables: manifest.ssts.iter().map(|m| m.number).collect(),
        });
    }
    Ok(())
}

/// Reads manifest `number`, or `None` if it is missing or does not decode.
async fn read_manifest<E: Environment>(
    env: &E,
    dir: &Path,
    number: u64,
) -> io::Result<Option<Manifest>> {
    Ok(read_whole(env, &manifest_path(dir, number))
        .await?
        .and_then(|bytes| Manifest::decode(&bytes).ok()))
}

/// Opens and checks whole every table `manifest` lists: the readers of those that
/// are on disk and intact, and the rest with what was wrong, reported as dropped
/// when `report` is set.
async fn open_tables<E: Environment>(
    env: &E,
    dir: &Path,
    manifest: &Manifest,
    report: bool,
) -> io::Result<(Vec<(SstMeta, Arc<SstReader<FileOf<E>>>)>, Vec<SstMeta>)> {
    let fs = env.fs();
    let mut ssts = Vec::new();
    let mut dropped = Vec::new();
    for meta in &manifest.ssts {
        let path = sst_path(dir, meta.number);
        let file = match fs.open(&path, OpenOptions::new().read(true)).await {
            Ok(file) => Some(file),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        let reader = match file {
            None => Err("missing"),
            Some(file) => match SstReader::open(file).await {
                Err(_) => Err("unreadable"),
                Ok(reader) => match reader.verify().await {
                    Err(_) => Err("corrupt"),
                    Ok(()) => Ok(reader),
                },
            },
        };
        match reader {
            Ok(reader) => ssts.push((meta.clone(), Arc::new(reader))),
            Err(reason) => {
                if report {
                    env.trace(TraceEvent::SstDropped {
                        number: meta.number,
                        first_seq: meta.first_seq,
                        max_seq: meta.max_seq,
                        reason,
                    });
                }
                dropped.push(meta.clone());
            }
        }
    }
    Ok((ssts, dropped))
}

/// Reads a whole file, or `None` if it does not exist.
async fn read_whole<E: Environment>(env: &E, path: &Path) -> io::Result<Option<Bytes>> {
    match env.fs().open(path, OpenOptions::new().read(true)).await {
        Ok(file) => {
            let size = file.size().await?;
            Ok(Some(
                file.read_at(0, usize::try_from(size).unwrap_or(usize::MAX))
                    .await?,
            ))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl<E: Environment> Engine<E> {
    /// Recovers what is in `config.dir`, starts the flusher, and returns the engine
    /// with what recovery found.
    ///
    /// # Errors
    ///
    /// Any I/O error from the directory, the manifests, the tables or the log;
    /// `InvalidData` for a log record that is not an op; `InvalidData` carrying an
    /// [`OpenRefused`] when `CURRENT` or the manifest it names cannot be read and no
    /// fallback is allowed or intact; and `InvalidData` carrying a
    /// [`HeadGap`](crate::wal::HeadGap) when the log's head is missing and
    /// `config.allow_head_gap` is off. A refusal touches nothing on disk.
    pub async fn open(env: E, config: EngineConfig) -> io::Result<(Self, EngineRecovery)> {
        let fs = env.fs();
        let dir = config.dir.clone();
        fs.create_dir_all(&dir).await?;
        let names = fs.read_dir(&dir).await?;
        let manifests: BTreeSet<u64> = names
            .iter()
            .filter_map(|n| manifest::manifest_of(n))
            .collect();
        let sst_files: BTreeSet<u64> = names.iter().filter_map(|n| manifest::sst_of(n)).collect();

        // The manifest in force: the one CURRENT names. No CURRENT at all is the empty
        // state, since a switch is what creates it: manifests on disk without it were
        // written and never switched to. A CURRENT that cannot be read, or one naming
        // a manifest that cannot be, refuses the store unless a fallback is allowed;
        // then the newest older manifest whose every table is on disk and intact is
        // used, never one that lists a missing or damaged table; the first manifest,
        // which lists none, is a valid fallback, since the log replays after it. Falling
        // back onto a manifest whose tables a later compaction had deleted gave an
        // empty store at seed 44 (D-022).
        let current_bytes = read_whole(&env, &current_path(&dir)).await?;
        let current = current_bytes.as_deref().and_then(manifest::parse_current);
        let mut manifest = Manifest::empty();
        let mut ssts = Vec::new();
        let mut dropped = Vec::new();
        let mut fallback_from = None;
        let mut rejected = Vec::new();
        if current_bytes.is_none() {
            // A fresh store gets its first manifest, the empty state, and CURRENT
            // naming it, so that from here on a missing CURRENT is damage: a
            // checkpoint written only part way, or a store past recognition.
            if !manifests.is_empty() || !sst_files.is_empty() {
                let refused = OpenRefused::CurrentMissing;
                env.trace(TraceEvent::OpenRefused {
                    reason: refused.to_string(),
                });
                return Err(refused.into_io());
            }
            manifest.number = 1;
            write_manifest_in(&env, &dir, &manifest, false).await?;
            switch_current(&env, &dir, 1).await?;
        } else {
            let named = match current {
                Some(number) => read_manifest(&env, &dir, number).await?,
                None => None,
            };
            match named {
                Some(m) => {
                    manifest = m;
                    (ssts, dropped) = open_tables(&env, &dir, &manifest, true).await?;
                }
                None => {
                    let refused = match current {
                        Some(number) => OpenRefused::ManifestUnreadable(number),
                        None => OpenRefused::CurrentUnreadable,
                    };
                    if !config.allow_manifest_fallback {
                        env.trace(TraceEvent::OpenRefused {
                            reason: refused.to_string(),
                        });
                        return Err(refused.into_io());
                    }
                    let named = current.unwrap_or(0);
                    let mut chosen = None;
                    for &number in manifests
                        .iter()
                        .rev()
                        .filter(|&&m| current.is_none_or(|named| m < named))
                    {
                        let Some(candidate) = read_manifest(&env, &dir, number).await? else {
                            rejected.push((number, Rejected::Unreadable));
                            continue;
                        };
                        // Every table it lists on disk and intact.
                        let (opened, missing) = open_tables(&env, &dir, &candidate, false).await?;
                        if let Some(meta) = missing.first() {
                            let on_disk = sst_files.contains(&meta.number);
                            rejected.push((
                                number,
                                if on_disk {
                                    Rejected::TableDamaged(meta.number)
                                } else {
                                    Rejected::TableMissing(meta.number)
                                },
                            ));
                            continue;
                        }
                        chosen = Some((candidate, opened));
                        break;
                    }
                    let Some((chosen, opened)) = chosen else {
                        let refused = OpenRefused::NoIntactManifest { named };
                        env.trace(TraceEvent::OpenRefused {
                            reason: refused.to_string(),
                        });
                        return Err(refused.into_io());
                    };
                    env.trace(TraceEvent::ManifestFallback {
                        from: named,
                        to: chosen.number,
                    });
                    fallback_from = Some(named);
                    manifest = chosen;
                    ssts = opened;
                }
            }
        }

        // Orphans: tables and manifests no manifest in force refers to, and a
        // CURRENT.tmp a crash left behind.
        let listed: BTreeSet<u64> = manifest.ssts.iter().map(|m| m.number).collect();
        let mut orphans: Vec<PathBuf> = sst_files
            .iter()
            .filter(|n| !listed.contains(n))
            .map(|&n| sst_path(&dir, n))
            .collect();
        orphans.extend(
            manifests
                .iter()
                .filter(|&&m| m > manifest.number)
                .map(|&m| manifest_path(&dir, m)),
        );
        if names
            .iter()
            .any(|n| n.as_path() == Path::new("CURRENT.tmp"))
        {
            orphans.push(current_tmp_path(&dir));
        }
        for path in &orphans {
            fs.remove_file(path).await?;
            env.trace(TraceEvent::OrphanRemoved { path: path.clone() });
        }
        if !orphans.is_empty() {
            fs.sync_dir(&dir).await?;
        }
        // After a fallback, CURRENT is made to say what recovery decided, so the next
        // open does not have to decide again from a damaged file.
        if fallback_from.is_some() {
            switch_current(&env, &dir, manifest.number).await?;
        }

        // The log, from where the tables leave off.
        let (wal, recovery) = Wal::open(
            env.clone(),
            WalConfig {
                dir: dir.clone(),
                segment_bytes: config.segment_bytes,
                variant: config.wal_variant,
                expected_head: manifest.flushed_seq + 1,
                head_gap: if config.allow_head_gap {
                    HeadGapPolicy::Discard
                } else {
                    HeadGapPolicy::Refuse
                },
                refuse_damage: config.refuse_log_damage,
            },
        )
        .await?;
        let flushed_seq = manifest.flushed_seq;
        let manifest_number = manifest.number;
        let next_sst = manifest.next_sst;
        let listed = manifest.ssts.clone();
        let shared = Arc::new(Shared {
            env: env.clone(),
            config,
            wal,
            tables: Mutex::new(Tables {
                active: Arc::new(Memtable::new(1)),
                immutable: VecDeque::new(),
                ssts,
                manifest,
            }),
            flusher: Mutex::new(Flusher {
                waker: None,
                closed: false,
            }),
            turnstile: Turnstile::default(),
            next_sst: AtomicU64::new(next_sst),
            compact_pointer: Mutex::new(vec![None; LEVELS]),
            next_memtable: AtomicU64::new(2),
            pending: Mutex::new(BTreeMap::new()),
            visible: AtomicU64::new(flushed_seq),
            snapshots: Mutex::new(BTreeMap::new()),
        });
        let mut replayed = 0;
        for (i, record) in recovery.records.iter().enumerate() {
            let seq = recovery.first_seq + i as u64;
            if seq <= flushed_seq {
                continue;
            }
            let ops = decode_record(record.clone())?;
            shared.apply(seq, ops);
            replayed += 1;
        }
        env.spawn("flusher", flusher(shared.clone()));
        let ssts = lock(&shared.tables).ssts.len();
        Ok((
            Self { shared },
            EngineRecovery {
                manifest: manifest_number,
                fallback_from,
                rejected,
                flushed_seq,
                ssts,
                tables: listed,
                dropped,
                orphans: orphans.len(),
                wal: recovery,
                replayed,
            },
        ))
    }

    /// Writes `value` under `key`. The returned future resolves once the write is
    /// durable and visible, with its log sequence number.
    pub fn put(&self, key: Bytes, value: Bytes) -> Write<E> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(batch, true)
    }

    /// Deletes `key`, leaving a tombstone. Resolves like [`put`](Self::put).
    pub fn delete(&self, key: Bytes) -> Write<E> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(batch, true)
    }

    /// Writes `batch` as one log record: its writes become visible together, under
    /// one sequence number, and a crash keeps all or none of them. With `sync`, the
    /// future resolves once the record is durable; without it, once the record is
    /// written, and the next synced write, rotation or close makes it durable, so a
    /// crash before then loses it though it was acknowledged and read (D-024). An
    /// empty batch still takes a number.
    pub fn write(&self, batch: WriteBatch, sync: bool) -> Write<E> {
        let ops = batch.ops;
        let append = self.shared.wal.append_with(encode_batch(&ops), sync);
        match self.shared.config.variant {
            Variant::NoWalBeforeMemtable => {
                // The bug: visible and acknowledged before the log has it.
                self.shared.apply(append.seq(), ops);
            }
            _ => {
                lock(&self.shared.pending).insert(append.seq(), ops);
            }
        }
        Write {
            shared: self.shared.clone(),
            append,
        }
    }

    /// A snapshot at the newest write applied.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot<E> {
        let version = self.shared.visible.load(Ordering::Acquire);
        *lock(&self.shared.snapshots).entry(version).or_default() += 1;
        Snapshot {
            shared: self.shared.clone(),
            version,
        }
    }

    /// The value under `key` as of the newest write applied, if it is present.
    ///
    /// # Errors
    ///
    /// A table read's error.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Bytes>> {
        self.shared
            .read(key, self.shared.visible.load(Ordering::Acquire))
            .await
    }

    /// The value under `key` as of `snapshot`, if it is present.
    ///
    /// # Errors
    ///
    /// A table read's error.
    pub async fn get_at(&self, key: &[u8], snapshot: &Snapshot<E>) -> io::Result<Option<Bytes>> {
        self.shared.read(key, snapshot.version).await
    }

    /// Every present key in `range` as of `snapshot`, in key order, with its value:
    /// one merge over every memtable and table, taking the newest write per key at
    /// or below the snapshot.
    ///
    /// # Errors
    ///
    /// A table read's error.
    pub async fn scan(
        &self,
        range: Range<&[u8]>,
        snapshot: &Snapshot<E>,
    ) -> io::Result<Vec<(Bytes, Bytes)>> {
        let mut merge = self.shared.merge_all();
        merge.seek(&ikey::lower_bound(range.start)).await?;
        let mut out = Vec::new();
        let mut last_user: Option<Bytes> = None;
        while let Some((key, value)) = merge.next().await? {
            let (user, seq) = ikey::decode(&key)?;
            if user[..] >= *range.end {
                break;
            }
            if seq > snapshot.version || last_user.as_ref() == Some(&user) {
                continue;
            }
            last_user = Some(user.clone());
            if let Value::Live(bytes) = value {
                out.push((user, bytes));
            }
        }
        Ok(out)
    }

    /// Immutable memtables not yet flushed.
    #[must_use]
    pub fn immutable_memtables(&self) -> usize {
        lock(&self.shared.tables).immutable.len()
    }

    /// Tables in service.
    #[must_use]
    pub fn ssts(&self) -> usize {
        lock(&self.shared.tables).ssts.len()
    }

    /// The tables in service by level, level 0 first: a deeper level in key order.
    #[must_use]
    pub fn levels(&self) -> Vec<Vec<SstMeta>> {
        let tables = lock(&self.shared.tables);
        let mut manifest = tables.manifest.clone();
        manifest.ssts = tables.ssts.iter().map(|(m, _)| m.clone()).collect();
        (0..LEVELS).map(|l| manifest.level(l as u8)).collect()
    }

    /// Runs one round of compaction if any level is over its limit: the manual
    /// trigger (SPEC §2.5). Waits for a flush or another round in progress.
    ///
    /// # Errors
    ///
    /// The filesystem's, or a table's `InvalidData`.
    pub async fn compact_once(&self) -> io::Result<Option<Compaction>> {
        let _turn = self.shared.turnstile.acquire().await;
        self.shared.compact().await
    }

    /// Writes the state as of the newest write applied into `dir`, which must not
    /// exist or must be empty, as a store of its own: a copy of every table in
    /// service, one table of what the memtables hold at that version, a manifest
    /// listing them, and `CURRENT`, each synced, in that order. A crash leaves either
    /// a complete checkpoint or one without `CURRENT`, which `open` refuses. A fresh
    /// [`Engine::open`] on `dir` is the store at that version (SPEC §2.7, D-024).
    /// Holds the turnstile, so no flush or compaction runs meanwhile.
    ///
    /// # Errors
    ///
    /// `AlreadyExists` if `dir` is not empty; else the filesystem's.
    pub async fn checkpoint(&self, dir: &Path) -> io::Result<CheckpointInfo> {
        let _turn = self.shared.turnstile.acquire().await;
        let shared = &self.shared;
        let fs = shared.env.fs();
        fs.create_dir_all(dir).await?;
        if !fs.read_dir(dir).await?.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the checkpoint directory is not empty",
            ));
        }
        let version = shared.visible.load(Ordering::Acquire);
        let (tables, memtables) = {
            let t = lock(&shared.tables);
            let mut memtables: Vec<Arc<Memtable>> = t.immutable.iter().cloned().collect();
            memtables.push(t.active.clone());
            (t.ssts.clone(), memtables)
        };
        let mut listed = Vec::new();
        let mut next_sst = 1;
        for (meta, reader) in &tables {
            write_file(
                &shared.env,
                &sst_path(dir, meta.number),
                reader.bytes().await?,
            )
            .await?;
            next_sst = next_sst.max(meta.number + 1);
            listed.push(meta.clone());
        }
        // What the memtables hold at the version, as one table at level 0.
        let mut merge: MergeIter<FileOf<E>> =
            MergeIter::new(memtables.into_iter().map(Source::memtable).collect());
        let mut writer = SstWriter::new();
        while let Some((key, value)) = merge.next().await? {
            let (user, seq) = ikey::decode(&key)?;
            if seq <= version {
                writer.add(&user, seq, &value);
            }
        }
        if writer.entries() > 0 {
            let number = next_sst;
            next_sst += 1;
            let (first_key, last_key) = writer.key_range().expect("has writes");
            let (first_seq, max_seq) = writer.seq_range().expect("has writes");
            let entries = writer.entries();
            let bytes = writer.finish();
            let len = bytes.len() as u64;
            write_file(&shared.env, &sst_path(dir, number), bytes).await?;
            listed.push(SstMeta {
                number,
                level: 0,
                first_seq,
                max_seq,
                entries,
                bytes: len,
                first_key,
                last_key,
            });
        }
        let manifest = Manifest {
            number: 1,
            next_sst,
            flushed_seq: version,
            ssts: listed,
        };
        write_manifest_in(&shared.env, dir, &manifest, true).await?;
        switch_current_in(&shared.env, dir, 1, true).await?;
        shared.env.trace(TraceEvent::CheckpointWritten {
            dir: dir.to_path_buf(),
            version,
            tables: manifest.ssts.len() as u64,
        });
        Ok(CheckpointInfo {
            version,
            tables: manifest.ssts.len(),
        })
    }

    /// The manifest in force.
    #[must_use]
    pub fn manifest(&self) -> Manifest {
        lock(&self.shared.tables).manifest.clone()
    }

    /// The log's segments on disk, oldest first.
    #[must_use]
    pub fn wal_segments(&self) -> Vec<u64> {
        self.shared.wal.segments()
    }
}

impl<E: Environment> Drop for Engine<E> {
    fn drop(&mut self) {
        let waker = {
            let mut flusher = lock(&self.shared.flusher);
            flusher.closed = true;
            flusher.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<E: Environment> Shared<E> {
    /// The newest write of `key` at or below `snapshot`: the active memtable first,
    /// then the immutable ones newest first, then level 0 newest first, then one
    /// table per deeper level. Each holds newer writes of a key than the next, so the
    /// first that has one has the newest.
    async fn read(&self, key: &[u8], snapshot: Seq) -> io::Result<Option<Bytes>> {
        let (active, immutable, readers) = {
            let tables = lock(&self.tables);
            (
                tables.active.clone(),
                tables.immutable.clone(),
                tables.readers_for(key),
            )
        };
        if let Some((_, value)) = active.get(key, snapshot) {
            return Ok(value.live());
        }
        for memtable in immutable.iter().rev() {
            if let Some((_, value)) = memtable.get(key, snapshot) {
                return Ok(value.live());
            }
        }
        for sst in &readers {
            if let Some((_, value)) = sst.get(key, snapshot).await? {
                return Ok(value.live());
            }
        }
        Ok(None)
    }

    /// One merge over every memtable and table in service right now.
    fn merge_all(&self) -> MergeIter<FileOf<E>> {
        let tables = lock(&self.tables);
        let mut sources = vec![Source::memtable(tables.active.clone())];
        sources.extend(tables.immutable.iter().cloned().map(Source::memtable));
        sources.extend(tables.ssts.iter().map(|(_, r)| Source::Sst(r.iter())));
        MergeIter::new(sources)
    }

    /// Applies every pending write up to and including `seq`, oldest first: the log
    /// acknowledged `seq`, so all of them are durable.
    fn apply_through(&self, seq: Seq) {
        loop {
            let next = {
                let mut pending = lock(&self.pending);
                match pending.first_key_value() {
                    Some((&first, _)) if first <= seq => pending.pop_first(),
                    _ => None,
                }
            };
            let Some((s, ops)) = next else {
                return;
            };
            self.apply(s, ops);
        }
    }

    /// Applies an acknowledged record's writes and rotates the active memtable if it
    /// is now full.
    fn apply(&self, seq: Seq, ops: Vec<(Bytes, Value)>) {
        let active = lock(&self.tables).active.clone();
        for (key, value) in ops {
            active.apply(seq, key, value);
        }
        self.visible.fetch_max(seq, Ordering::AcqRel);
        if active.bytes() <= self.config.memtable_bytes {
            return;
        }
        let rotated = {
            let mut tables = lock(&self.tables);
            if !Arc::ptr_eq(&tables.active, &active) {
                return; // Someone else rotated it already.
            }
            let id = self.next_memtable.fetch_add(1, Ordering::Relaxed);
            let full = std::mem::replace(&mut tables.active, Arc::new(Memtable::new(id)));
            tables.immutable.push_back(full.clone());
            full
        };
        self.env.trace(TraceEvent::MemtableRotated {
            memtable: rotated.id(),
            entries: rotated.len() as u64,
            bytes: rotated.bytes(),
            up_to: rotated.max_seq(),
        });
        let waker = lock(&self.flusher).waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Releases `memtable` from the immutable queue, if it is still its head.
    fn release(&self, memtable: &Arc<Memtable>) {
        let mut tables = lock(&self.tables);
        if tables
            .immutable
            .front()
            .is_some_and(|front| Arc::ptr_eq(front, memtable))
        {
            tables.immutable.pop_front();
        }
        drop(tables);
        self.env.trace(TraceEvent::MemtableFlushed {
            memtable: memtable.id(),
            up_to: memtable.max_seq(),
        });
    }

    /// Writes `bytes` as table `number`, syncs it, and opens it: the reader and the
    /// file's size.
    pub(crate) async fn write_table(
        &self,
        number: u64,
        bytes: Bytes,
    ) -> io::Result<(SstReader<FileOf<E>>, u64)> {
        let fs = self.env.fs();
        let file = fs
            .open(
                &sst_path(&self.config.dir, number),
                OpenOptions::new().read(true).write(true).create_new(true),
            )
            .await?;
        let len = bytes.len() as u64;
        file.write_at(0, bytes).await?;
        file.sync().await?;
        let reader = SstReader::open(file).await?;
        Ok((reader, len))
    }

    /// Writes `memtable` as the next table, at level 0, and syncs it.
    async fn write_sst(&self, memtable: &Memtable) -> io::Result<(SstMeta, SstReader<FileOf<E>>)> {
        let number = self.next_sst.fetch_add(1, Ordering::Relaxed);
        let mut writer = SstWriter::new();
        for (key, seq, value) in memtable.entries() {
            writer.add(&key, seq, &value);
        }
        let entries = writer.entries();
        let (first_key, last_key) = writer.key_range().unwrap_or_default();
        let bytes = writer.finish();
        let (reader, len) = self.write_table(number, bytes).await?;
        let meta = SstMeta {
            number,
            level: 0,
            first_seq: memtable.min_seq(),
            max_seq: memtable.max_seq(),
            entries,
            bytes: len,
            first_key,
            last_key,
        };
        self.env.trace(TraceEvent::SstWritten {
            number,
            level: 0,
            entries,
            bytes: len,
            first_seq: meta.first_seq,
            max_seq: meta.max_seq,
        });
        Ok((meta, reader))
    }

    /// The manifest that follows the one in force: the tables in service without
    /// `removed`, plus `added`. Built from the tables in service rather than the
    /// manifest's list, so a table dropped at open is not listed again.
    pub(crate) fn manifest_edit(&self, removed: &[u64], added: Vec<SstMeta>) -> Manifest {
        let tables = lock(&self.tables);
        let mut next = tables.manifest.clone();
        next.number += 1;
        next.next_sst = self.next_sst.load(Ordering::Relaxed);
        next.ssts = tables
            .ssts
            .iter()
            .map(|(m, _)| m.clone())
            .filter(|m| !removed.contains(&m.number))
            .chain(added)
            .collect();
        next
    }

    /// Writes `next` and syncs it. Nothing names it until [`switch_to`](Self::switch_to).
    pub(crate) async fn write_manifest_file(&self, next: &Manifest) -> io::Result<()> {
        write_manifest_in(&self.env, &self.config.dir, next, false).await
    }

    /// Switches `CURRENT` to `next`, which is on disk and synced.
    pub(crate) async fn switch_to(&self, next: &Manifest) -> io::Result<()> {
        switch_current(&self.env, &self.config.dir, next.number).await
    }

    /// Writes `next`, syncs it, and switches `CURRENT` to it.
    pub(crate) async fn write_manifest(&self, next: &Manifest) -> io::Result<()> {
        self.write_manifest_file(next).await?;
        self.switch_to(next).await
    }

    /// Takes `removed` out of service and puts `added` in, under `next`, which lists
    /// the result.
    pub(crate) fn install(
        &self,
        next: Manifest,
        removed: &[u64],
        added: Vec<(SstMeta, SstReader<FileOf<E>>)>,
    ) {
        let mut tables = lock(&self.tables);
        tables.ssts.retain(|(m, _)| !removed.contains(&m.number));
        tables
            .ssts
            .extend(added.into_iter().map(|(m, r)| (m, Arc::new(r))));
        tables.manifest = next;
    }
}

/// A write on its way to the log; resolves with its sequence number once durable and
/// visible (or, in the buggy variant, at once).
pub struct Write<E: Environment> {
    shared: Arc<Shared<E>>,
    append: Append,
}

impl<E: Environment> Write<E> {
    /// The write's log sequence number, known before it is durable.
    #[must_use]
    pub fn seq(&self) -> Seq {
        self.append.seq()
    }
}

impl<E: Environment> Future for Write<E> {
    type Output = io::Result<Seq>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<Seq>> {
        if self.shared.config.variant == Variant::NoWalBeforeMemtable {
            return Poll::Ready(Ok(self.append.seq()));
        }
        let seq = match Pin::new(&mut self.append).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(seq)) => seq,
        };
        self.shared.apply_through(seq);
        Poll::Ready(Ok(seq))
    }
}

/// Resolves with the oldest immutable memtable, or with nothing once the engine is
/// closed and none are left.
struct NextImmutable<'a, E: Environment>(&'a Shared<E>);

impl<E: Environment> Future for NextImmutable<'_, E> {
    type Output = Option<Arc<Memtable>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Arc<Memtable>>> {
        if let Some(memtable) = lock(&self.0.tables).immutable.front().cloned() {
            return Poll::Ready(Some(memtable));
        }
        let mut flusher = lock(&self.0.flusher);
        if flusher.closed {
            return Poll::Ready(None);
        }
        flusher.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// The one task that flushes immutable memtables, oldest first: table, manifest,
/// switch, release, then the log segments the table made redundant; after each
/// flush, compaction rounds until no level is over its limit, when the engine runs
/// compaction in the background. Each step holds the turnstile. On an I/O error it
/// reports `FlusherFailed` and stops; reads keep working from the memtables it left,
/// and the log grows.
async fn flusher<E: Environment>(shared: Arc<Shared<E>>) {
    while let Some(memtable) = NextImmutable(&shared).await {
        let flushed = async {
            {
                let _turn = shared.turnstile.acquire().await;
                let (meta, reader) = shared.write_sst(&memtable).await?;
                let mut next = shared.manifest_edit(&[], vec![meta.clone()]);
                next.flushed_seq = meta.max_seq;
                let max_seq = meta.max_seq;
                if shared.config.variant == Variant::ReleaseBeforeManifest {
                    // The bug: the table is taken for durable once written. It serves
                    // reads, the memtable goes, the log segments go, and only then is
                    // the manifest written. A crash before the manifest is durable
                    // leaves the table an orphan and its records nowhere.
                    shared.install(next.clone(), &[], vec![(meta, reader)]);
                    shared.release(&memtable);
                    shared.wal.delete_segments_through(max_seq).await?;
                    shared.write_manifest(&next).await?;
                } else {
                    shared.write_manifest(&next).await?;
                    shared.install(next, &[], vec![(meta, reader)]);
                    shared.release(&memtable);
                    shared.wal.delete_segments_through(max_seq).await?;
                }
            }
            if shared.config.background_compaction {
                loop {
                    let _turn = shared.turnstile.acquire().await;
                    if shared.compact().await?.is_none() {
                        break;
                    }
                }
            }
            Ok::<(), io::Error>(())
        };
        if let Err(error) = flushed.await {
            shared.env.trace(TraceEvent::FlusherFailed {
                error: error.to_string(),
            });
            return;
        }
    }
}
