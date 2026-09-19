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
//! A span's keys can be replaced in a running engine (D-054): [`Engine::checkpoint_span`]
//! writes the newest live write of every key of a span as a store of its own, and
//! [`Engine::install_span`] puts such a store in place of the span. The install takes
//! one log record of its own, numbered above every write the engine had taken, splits
//! the memtables there and flushes those at or below it, then writes the tables that
//! held the span's keys again without them and the installed tables with every write
//! at that number, and makes all of it the state with one manifest switch. A crash
//! leaves the span as it was or as installed, and a later write to the span is newer
//! than every installed one.
//!
//! [`Engine::seek`] is a scan bounded by a count, and [`Engine::delete_range`] an
//! install of nothing: the span's keys go with one manifest switch and leave no
//! tombstone (D-055).
//!
//! Each of the three takes a set of spans as well as one (D-068), since a range lives
//! in two: its Raft state and its user keys (D-066). [`Engine::checkpoint_spans`]
//! copies every span at one version; [`Engine::install_spans`] takes every span's
//! keys out, puts the source's in, and carries the receiver's own writes, the repair,
//! as a table of its own at the number after the install's, all in the one manifest
//! switch; and [`Engine::delete_ranges`] is the install of nothing over the set.
//!
//! The [`Variant`]s for the crash sweep: [`Variant::Correct`];
//! [`Variant::NoWalBeforeMemtable`], which applies and acknowledges a write before
//! the log has it; [`Variant::ReleaseBeforeManifest`], which releases a memtable
//! and its log segments once its table is written but before the manifest names it;
//! [`Variant::DeleteBeforeManifest`], whose compaction deletes its inputs first;
//! [`Variant::InstallInTwoSwitches`], whose install takes the span out with one
//! manifest switch and puts the installed tables in with another; and
//! [`Variant::SpanCheckpointUnsynced`], whose checkpoint of a span does not sync its
//! tables; [`Variant::SeekCountsTombstones`], whose bounded seek counts deleted keys
//! against its limit; [`Variant::RangeDeleteSkipsMemtables`], whose range delete
//! leaves the span's writes in the memtables;
//! [`Variant::InstallKeepsSourceNumbers`], whose install keeps the source's sequence
//! numbers; [`Variant::InstallSwitchPerSpan`], whose install of several spans
//! switches them one at a time; [`Variant::RepairAfterSwitch`], whose install puts
//! the repair in with a switch after its own; [`Variant::RepairBeforeSwitch`], whose
//! install puts the repair in with a switch before its own; and
//! [`Variant::CheckpointVersionPerSpan`], whose checkpoint of several spans copies
//! each at its own version.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use ananke_env::{Environment, File, FileSystem, OpenOptions, TraceEvent, WalStopReason};
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
    /// A span's install takes the span's keys out in one manifest switch and puts
    /// the installed tables in with a second. A crash between the two leaves the
    /// span as neither what it was nor what was installed: empty.
    // PROPOSED(D-054): the live install of a span, in one manifest switch.
    InstallInTwoSwitches,
    /// A checkpoint of a span writes its tables without syncing them before the
    /// manifest and `CURRENT` that name them. A crash can leave a checkpoint whose
    /// `CURRENT` names tables that came back empty or short.
    // PROPOSED(D-054): the checkpoint of a span, which the live install installs from.
    SpanCheckpointUnsynced,
    /// A bounded seek counts a deleted key against its limit, so it can return
    /// fewer keys than asked for while more lie in the range.
    // PROPOSED(D-055): the bounded, ordered seek.
    SeekCountsTombstones,
    /// A range delete takes the span's writes out of the tables but leaves the
    /// memtables unflushed and the manifest's `flushed_seq` where it was: the
    /// span's writes still in a memtable stay readable, reach a table at the next
    /// flush, and come back from the log after a crash.
    // PROPOSED(D-055): the range delete, an install of nothing.
    RangeDeleteSkipsMemtables,
    /// An install writes each installed key at the sequence number the source
    /// gave it rather than at the install's own. A source from a store that has
    /// taken more records than the live engine carries numbers above every later
    /// local write, and hides them.
    // PROPOSED(D-054): the installed sequence numbers are the install's.
    InstallKeepsSourceNumbers,
    /// An install of several spans switches them one at a time: each manifest
    /// switch takes out and puts in what first falls in one span, the next span's
    /// with the next switch. A crash between two leaves some spans installed and
    /// the rest as they were: a mixture across the spans.
    // PROPOSED(D-068): several spans in one manifest switch.
    InstallSwitchPerSpan,
    /// An install makes its switch without the repair, the receiver's own writes,
    /// and puts the repair's table in with a second switch after it. A crash
    /// between the two leaves the installed tables without their repair.
    // PROPOSED(D-068): the repair is carried in the install's own switch.
    RepairAfterSwitch,
    /// An install puts the repair's table in with a switch of its own before the
    /// install's, which takes the spans' keys out and puts the installed tables in
    /// after it. A crash between the two leaves the repair without its tables: the
    /// receiver's writes over the spans as they were.
    // PROPOSED(D-068): the repair is carried in the install's own switch.
    RepairBeforeSwitch,
    /// A checkpoint of several spans copies each at the newest version applied
    /// when it reaches that span, and names the last. A write applied between two
    /// spans' copies is in the later span's copy and not in the earlier's.
    // PROPOSED(D-068): the checkpoint of several spans, at one version.
    CheckpointVersionPerSpan,
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
    /// Whether an open whose recovery lost writes in the middle of the state
    /// ([`EngineRecovery::lost_writes`]) starts quiesced: no flusher, so no
    /// table, no manifest, no compaction and no log segment deleted. The engine
    /// that recovered a hole is the one damaged, and a flush of the memtable
    /// that recovery replayed rewrites the manifest without the dropped table
    /// and deletes the log segments it covered — the evidence of the loss,
    /// laundered away, which is how the premerge's seed 687 turned a refused
    /// server into one that opened clean at its next start. A caller that
    /// refuses such a store (`ananke-raft`'s `RaftStore::open`) sets this; a
    /// caller that allows fallbacks and head gaps means to keep running and
    /// leaves it off.
    // D-044: a durable refusal, and a refused engine that does no work.
    pub quiesce_on_loss: bool,
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
            // D-044: off by default, so an engine whose caller allows
            // fallbacks and head gaps keeps the behaviour it had.
            quiesce_on_loss: false,
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

/// What [`Engine::checkpoint`] or [`Engine::checkpoint_span`] wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// The version the checkpoint is the state at.
    pub version: Seq,
    /// Tables in it.
    pub tables: usize,
}

/// What [`Engine::install_span`] did.
// PROPOSED(D-054): the live install of a span, in one manifest switch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallInfo {
    /// The sequence number every installed write carries: the install's own log
    /// record.
    pub seq: Seq,
    /// The sequence number every write of the repair carries, the record after
    /// `seq`, which holds no write; `None` when the install carried no repair.
    // PROPOSED(D-068): the repair, in a table of its own numbered above the source's.
    pub repair_seq: Option<Seq>,
    /// The manifest that made the install the state.
    pub manifest: u64,
    /// Tables taken out of service, the rewritten ones' originals included.
    pub removed: usize,
    /// Of those, tables written again without the span's keys.
    pub rewritten: usize,
    /// Installed tables, the repair's not counted.
    pub added: usize,
    /// Keys installed, the repair's not counted.
    pub keys: u64,
}

/// Why [`Engine::install_span`] or [`Engine::install_spans`] refused an install.
/// Nothing in the store changed, though a refusal after the install took its number
/// leaves its log records, which hold no write.
// PROPOSED(D-054): the live install of a span, in one manifest switch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstallRefused {
    /// A span holds no key, its start not below its end, or no span was given.
    EmptySpan,
    /// The spans are not sorted and disjoint: one ends past the start of the one
    /// after it. A checkpoint of several spans is refused for it too.
    // PROPOSED(D-068): a set of spans is sorted and disjoint.
    SpansOverlap {
        /// Where the earlier span ends.
        end: Bytes,
        /// Where the later one starts, below `end`.
        start: Bytes,
    },
    /// The source holds a key outside every span, which the install would have
    /// written over a key it was not asked to replace.
    OutsideSpan {
        /// The smallest key the source holds.
        first: Bytes,
        /// The largest.
        last: Bytes,
    },
    /// The repair writes a key outside every span. The install removes and
    /// replaces the spans' keys alone, so a repair write outside them would be a
    /// write the log never numbered, over a key the install was not asked to touch.
    // PROPOSED(D-068): the repair lies inside the spans it is carried with.
    RepairOutsideSpan {
        /// The first such key in the repair.
        key: Bytes,
    },
    /// The repair's record was not numbered the one after the install's: another
    /// write took a number between them. The two are numbered under one hold of the
    /// pending lock, so this is never seen while that holds. It is checked so that
    /// a change breaking it refuses the install, rather than switching to a manifest
    /// whose `flushed_seq` claims the write between them was in a table while it sat
    /// in a memtable the flusher held back, to be lost at the next crash (D-068).
    // PROPOSED(D-068): the repair's number is the record after the install's.
    RepairNotNext {
        /// The install's number.
        seq: Seq,
        /// The repair's, which is not `seq + 1`.
        repair_seq: Seq,
    },
    /// Another install has not switched yet: one runs at a time.
    InProgress,
    /// The engine is quiesced and does no work (D-044).
    Quiesced,
    /// The source directory is not a whole store: `CURRENT`, its manifest or a
    /// table it lists is missing or damaged.
    SourceDamaged(String),
}

impl InstallRefused {
    /// The refusal an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<InstallRefused> {
        error.get_ref()?.downcast_ref::<InstallRefused>().cloned()
    }

    fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, self)
    }
}

impl std::fmt::Display for InstallRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstallRefused::EmptySpan => write!(f, "the span holds no key"),
            InstallRefused::SpansOverlap { end, start } => write!(
                f,
                "the spans are not sorted and disjoint: one ends at {end:?}, past the start of the next at {start:?}"
            ),
            InstallRefused::OutsideSpan { first, last } => write!(
                f,
                "the source holds keys {first:?} to {last:?}, not all inside the span"
            ),
            InstallRefused::RepairOutsideSpan { key } => {
                write!(f, "the repair writes {key:?}, outside every span")
            }
            InstallRefused::RepairNotNext { seq, repair_seq } => write!(
                f,
                "the repair was numbered {repair_seq}, not the record after the install's {seq}: another write was numbered between them"
            ),
            InstallRefused::InProgress => write!(f, "another install is in progress"),
            InstallRefused::Quiesced => write!(f, "the engine is quiesced"),
            InstallRefused::SourceDamaged(what) => write!(f, "the source is damaged: {what}"),
        }
    }
}

impl std::error::Error for InstallRefused {}

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
    ///
    /// Newest at level 0 is by the highest sequence number a table holds, then by
    /// number. For flushed tables the two orders are one, since their sequence
    /// ranges are disjoint and numbered in order; a table an install rewrote keeps
    /// its writes' numbers under a new file number, which only the first order
    /// still places right (D-054).
    fn readers_for(&self, key: &[u8]) -> Vec<Arc<SstReader<FileOf<E>>>> {
        let mut l0: Vec<&(SstMeta, Arc<SstReader<FileOf<E>>>)> =
            self.ssts.iter().filter(|(m, _)| m.level == 0).collect();
        // PROPOSED(D-054): level 0 is read newest sequence number first.
        l0.sort_by_key(|(m, _)| std::cmp::Reverse((m.max_seq, m.number)));
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
    /// Set once the engine is quiesced: no flush, no compaction, no log segment
    /// deleted, from the next step on. It is never unset.
    // D-044: a durable refusal, and a refused engine that does no work.
    quiesced: AtomicBool,
    /// One flush or compaction at a time (D-023).
    turnstile: Turnstile,
    /// The number the next table gets.
    pub(crate) next_sst: AtomicU64,
    /// Per level, the last key the last compaction round on it wrote, so rounds walk
    /// the level.
    pub(crate) compact_pointer: Mutex<Vec<Option<Bytes>>>,
    next_memtable: AtomicU64,
    /// Writes appended and not yet applied, by sequence number: each record's
    /// writes in order. A record is numbered by the log and put here under this
    /// lock, taken before the log's own, so no one popping from it can find a
    /// number missing below one that is here (D-021).
    // PROPOSED(D-054): a record is numbered and pending in one step.
    pending: Mutex<BTreeMap<Seq, Vec<(Bytes, Value)>>>,
    /// Held by whoever is applying pending writes, so that two callers on
    /// different threads never apply out of sequence order: a record popped by one
    /// is applied before the next is popped (D-021). The install's split of the
    /// memtables at its number relies on it (D-054).
    // PROPOSED(D-054): applies are serialised.
    apply_order: Mutex<()>,
    /// The highest sequence number applied: what a read without a snapshot reads at,
    /// and what a new snapshot pins. Writes apply in order (D-021), so everything at
    /// or below it is visible.
    pub(crate) visible: AtomicU64,
    /// Live snapshots by sequence number, with how many pin each.
    pub(crate) snapshots: Mutex<BTreeMap<Seq, usize>>,
    /// The sequence number of the install in progress, if one is: the active
    /// memtable is rotated as that record is applied, so every memtable holds
    /// writes from one side of it only, and the flusher leaves the memtables past
    /// it alone until the install has switched.
    // PROPOSED(D-054): the live install of a span, in one manifest switch.
    install: Mutex<Option<Seq>>,
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

impl EngineRecovery {
    /// Whether the recovery lost writes in the middle of the state (D-022): a
    /// table the manifest listed that could not be read, a fallback onto an
    /// older manifest, a discarded log head, a log stopped short at a bad
    /// checksum or a gap, or a corrupt record skipped in a segment the tables
    /// cover. Each is a hole with acknowledged writes on both sides of it, as
    /// against a torn record at the end of the log, which was in flight at the
    /// crash and never acknowledged.
    ///
    /// This is the engine's own name for what `ananke-raft`'s `LostState`
    /// refuses a Raft store for, so the two can never disagree, and what
    /// [`EngineConfig::quiesce_on_loss`] starts an engine quiesced for.
    // D-044: a durable refusal, and a refused engine that does no work.
    #[must_use]
    pub fn lost_writes(&self) -> bool {
        !self.dropped.is_empty()
            || self.fallback_from.is_some()
            || self.wal.head_gap.is_some()
            || self.wal.stop.is_some_and(|stop| {
                matches!(
                    stop.reason,
                    WalStopReason::BadChecksum | WalStopReason::Gap { .. }
                )
            })
            || !self.wal.covered_stops.is_empty()
    }
}

/// A write-ahead log in front of memtables and tables. Dropping it closes the log and
/// stops the flusher once the queue is empty.
pub struct Engine<E: Environment> {
    shared: Arc<Shared<E>>,
}

/// A point in the engine's history: reads at it see every write numbered at or
/// below its version and nothing newer, and compaction keeps the versions it needs
/// until it is dropped. The one exception is a span an install or a range delete
/// replaced once the snapshot was taken: from the install's switch on, a read at
/// a version below the install's number sees the span as empty, and one at or above
/// it sees the installed span (D-054).
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
            quiesced: AtomicBool::new(false),
            turnstile: Turnstile::default(),
            next_sst: AtomicU64::new(next_sst),
            compact_pointer: Mutex::new(vec![None; LEVELS]),
            next_memtable: AtomicU64::new(2),
            pending: Mutex::new(BTreeMap::new()),
            apply_order: Mutex::new(()),
            visible: AtomicU64::new(flushed_seq),
            snapshots: Mutex::new(BTreeMap::new()),
            install: Mutex::new(None),
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
        let ssts = lock(&shared.tables).ssts.len();
        let recovery = EngineRecovery {
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
        };
        // D-044: an engine whose recovery lost writes in the middle of
        // the state starts quiesced when the caller asked for it, the flusher
        // never spawned: the flush of the memtable this open just replayed would
        // write a manifest without the dropped table and delete the log segments
        // that held its records, which is the loss laundered away before the
        // caller has even seen the recovery (the premerge's seed 687).
        if shared.config.quiesce_on_loss && recovery.lost_writes() {
            shared.quiesced.store(true, Ordering::SeqCst);
            env.trace(TraceEvent::EngineQuiesced {
                dir: dir.clone(),
                reason: "the recovery lost writes in the middle of the state",
            });
        } else {
            env.spawn("flusher", flusher(shared.clone()));
        }
        Ok((Self { shared }, recovery))
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
        let payload = encode_batch(&ops);
        let append = if self.shared.config.variant == Variant::NoWalBeforeMemtable {
            // The bug: visible and acknowledged before the log has it.
            let append = self.shared.wal.append_with(payload, sync);
            self.shared.apply(append.seq(), ops);
            append
        } else {
            // PROPOSED(D-054): numbered and pending in one step. Numbered first
            // and put in `pending` after, a record could be synced and still
            // missing from the map when another thread applied through a later
            // record, and land after it: in the memtable past an install's split,
            // under a manifest whose `flushed_seq` said it was flushed.
            let mut pending = lock(&self.shared.pending);
            let append = self.shared.wal.append_with(payload, sync);
            pending.insert(append.seq(), ops);
            append
        };
        Write {
            shared: self.shared.clone(),
            append,
        }
    }

    /// A snapshot at the newest write applied. An install or a range delete that
    /// switches while it is held replaces its span's history under it: see
    /// [`Snapshot`] (D-054).
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

    /// The value under `key` as of `snapshot`, if it is present: the newest write at
    /// or below its version, unless an install or a range delete of a span holding
    /// `key` has switched since the snapshot was taken (D-054; see [`Snapshot`]).
    ///
    /// # Errors
    ///
    /// A table read's error.
    pub async fn get_at(&self, key: &[u8], snapshot: &Snapshot<E>) -> io::Result<Option<Bytes>> {
        self.shared.read(key, snapshot.version).await
    }

    /// Every present key in `range` as of `snapshot`, in key order, with its value:
    /// one merge over every memtable and table, taking the newest write per key at
    /// or below the snapshot. A scan reads the tables in service when it starts to
    /// the end; one that starts after an install or a range delete switched reads
    /// the replaced span as [`Snapshot`] says (D-054).
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
        // D-044: a quiesced engine does no work, this trigger included.
        if self.quiesced() {
            return Ok(None);
        }
        let _turn = self.shared.turnstile.acquire().await;
        self.shared.compact().await
    }

    /// Quiesces the engine: no flush, no compaction and no log segment deleted
    /// from here on, whatever is waiting. The flusher stops before its next
    /// memtable, and nothing unsets it — an engine is quiesced because what it
    /// recovered is not to be written over (RAFT.md §3, D-022): the node whose
    /// store was refused for lost state calls this the moment it learns of the
    /// refusal, so that no flush of the replayed memtable rewrites the manifest
    /// without the dropped table or deletes the log segments that held its
    /// records. An engine opened on a recovery that lost writes starts quiesced
    /// on its own when [`EngineConfig::quiesce_on_loss`] is set, which is the
    /// same fix a step earlier; this call catches a store refused for anything
    /// else the caller knows and the engine does not.
    ///
    /// Writes are not refused: a caller that quiesces has no use for them, and
    /// the log still takes them. Reads keep working from what is in memory.
    // D-044: a durable refusal, and a refused engine that does no work.
    pub fn quiesce(&self) {
        self.shared.quiesce("the store was refused for lost state");
    }

    /// Whether the engine is quiesced.
    // D-044: a durable refusal, and a refused engine that does no work.
    #[must_use]
    pub fn quiesced(&self) -> bool {
        self.shared.quiesced.load(Ordering::SeqCst)
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

    /// Writes the span `range` as of the newest write applied into `dir`, which must
    /// not exist or must be empty, as a store of its own: the newest write at or
    /// below that version of every key in the span that is present, each at its own
    /// sequence number, in tables at level 0 sealed near `sst_bytes` and written in
    /// key order, then a manifest listing them and `CURRENT`, each synced in that
    /// order. Deleted keys leave nothing: the checkpoint is the span's state, not its
    /// history. A crash leaves either a complete checkpoint or one without
    /// `CURRENT`, which `open` and [`open_span_source`](Self::open_span_source)
    /// refuse. Holds the turnstile, as [`checkpoint`](Self::checkpoint) does.
    ///
    /// # Errors
    ///
    /// `AlreadyExists` if `dir` is not empty; else the filesystem's.
    // PROPOSED(D-054): the checkpoint of a span, which the live install installs from.
    pub async fn checkpoint_span(
        &self,
        range: Range<&[u8]>,
        dir: &Path,
    ) -> io::Result<CheckpointInfo> {
        self.checkpoint_spans(&[range], dir).await
    }

    /// Writes the spans `spans` as of the newest write applied into `dir`, as
    /// [`checkpoint_span`](Self::checkpoint_span) writes one: every span at the
    /// one version read before the first is copied, so a write applied while the
    /// copy runs is in no span's copy, and never in one span's and not another's
    /// (D-068). The spans must be sorted and disjoint; one that holds no key copies
    /// nothing. The tables are sealed near `sst_bytes` in key order across the
    /// spans, so one table can hold keys of two.
    ///
    /// # Errors
    ///
    /// `InvalidData` carrying [`InstallRefused::SpansOverlap`] when two spans that
    /// hold keys are out of order or overlap, before anything is written;
    /// `AlreadyExists` if `dir` is not empty; else the filesystem's.
    // PROPOSED(D-068): the checkpoint of several spans, at one version.
    pub async fn checkpoint_spans(
        &self,
        spans: &[Range<&[u8]>],
        dir: &Path,
    ) -> io::Result<CheckpointInfo> {
        // A span that holds no key copies nothing, as a single one always has; the
        // rest must be in key order, or the tables' keys would not be.
        let spans: Vec<Range<&[u8]>> = spans.iter().filter(|s| s.start < s.end).cloned().collect();
        check_disjoint(&spans).map_err(InstallRefused::into_io)?;
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
        // The turnstile holds every flush and compaction off, so every write at or
        // below the version stays in the memtables and tables each span's merge
        // walks, and the version filters out whatever is applied meanwhile.
        let mut version = shared.visible.load(Ordering::Acquire);
        let mut listed = Vec::new();
        let mut writer = SstWriter::new();
        for range in &spans {
            if shared.config.variant == Variant::CheckpointVersionPerSpan {
                // The bug: each span at the version applied when its copy begins.
                version = shared.visible.load(Ordering::Acquire);
            }
            let mut merge = shared.merge_all();
            merge.seek(&ikey::lower_bound(range.start)).await?;
            let mut last_user: Option<Bytes> = None;
            while let Some((key, value)) = merge.next().await? {
                let (user, seq) = ikey::decode(&key)?;
                if user[..] >= *range.end {
                    break;
                }
                if seq > version || last_user.as_ref() == Some(&user) {
                    continue;
                }
                last_user = Some(user.clone());
                if value == Value::Tombstone {
                    continue;
                }
                if writer.entries() > 0 && writer.bytes_so_far() as u64 >= shared.config.sst_bytes {
                    let number = listed.len() as u64 + 1;
                    let full = std::mem::take(&mut writer);
                    listed.push(write_level_0_table(shared, dir, number, full).await?);
                }
                writer.add(&user, seq, &value);
            }
        }
        if writer.entries() > 0 {
            let number = listed.len() as u64 + 1;
            listed.push(write_level_0_table(shared, dir, number, writer).await?);
        }
        let manifest = Manifest {
            number: 1,
            next_sst: listed.len() as u64 + 1,
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

    /// Reads the store in `dir` to install over a span: `CURRENT`, the manifest it
    /// names and every table that lists, each opened and checked whole. Nothing is
    /// written, here or in `dir`.
    ///
    /// # Errors
    ///
    /// `InvalidData` carrying [`InstallRefused::SourceDamaged`] when `CURRENT`, its
    /// manifest or a table is missing or damaged; else the filesystem's.
    // PROPOSED(D-054): the live install of a span, in one manifest switch.
    pub async fn open_span_source(&self, dir: &Path) -> io::Result<SpanSource<E>> {
        let env = &self.shared.env;
        let damaged = |what: String| InstallRefused::SourceDamaged(what).into_io();
        let Some(current) = read_whole(env, &current_path(dir)).await? else {
            return Err(damaged(format!("{} has no CURRENT", dir.display())));
        };
        let Some(number) = manifest::parse_current(&current) else {
            return Err(damaged(format!(
                "{}'s CURRENT cannot be read",
                dir.display()
            )));
        };
        let Some(manifest) = read_manifest(env, dir, number).await? else {
            return Err(damaged(format!(
                "{}'s MANIFEST-{number:06} cannot be read",
                dir.display()
            )));
        };
        let (tables, missing) = open_tables(env, dir, &manifest, false).await?;
        if let Some(meta) = missing.first() {
            return Err(damaged(format!(
                "{}'s table {} is missing or damaged",
                dir.display(),
                meta.number
            )));
        }
        Ok(SpanSource { tables })
    }

    /// The first `limit` present keys at or after `range.start` and below
    /// `range.end` as of `snapshot`, in key order, with their values: the walk
    /// [`scan`](Self::scan) makes, stopped once `limit` keys are found (SHARD.md
    /// §11, storage 2). A deleted key is passed over and does not count, so a seek
    /// returns fewer than `limit` keys only when the range holds no more. The first
    /// key at or after `k` is `seek(k..end, 1, snapshot)`.
    ///
    /// # Errors
    ///
    /// A table read's error.
    // PROPOSED(D-055): the bounded, ordered seek.
    pub async fn seek(
        &self,
        range: Range<&[u8]>,
        limit: usize,
        snapshot: &Snapshot<E>,
    ) -> io::Result<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        let mut merge = self.shared.merge_all();
        merge.seek(&ikey::lower_bound(range.start)).await?;
        let mut last_user: Option<Bytes> = None;
        let mut counted = 0;
        while let Some((key, value)) = merge.next().await? {
            let (user, seq) = ikey::decode(&key)?;
            if user[..] >= *range.end {
                break;
            }
            if seq > snapshot.version || last_user.as_ref() == Some(&user) {
                continue;
            }
            last_user = Some(user.clone());
            match value {
                Value::Live(bytes) => {
                    out.push((user, bytes));
                    counted += 1;
                }
                Value::Tombstone => {
                    if self.shared.config.variant == Variant::SeekCountsTombstones {
                        // The bug: a deleted key uses up the limit.
                        counted += 1;
                    }
                }
            }
            if counted >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Installs `source` over the span `range` while the engine runs: every write
    /// of a key in the span that the engine holds is taken out, and the newest live
    /// write of every key `source` holds put in, all in one manifest switch (SHARD.md
    /// §11, storage 5; Q2). A crash at any point leaves the span as it was or as
    /// installed, and every key outside it as it was.
    ///
    /// The install is numbered when it is asked for: one log record of its own,
    /// holding no write, above every write the engine has taken, which
    /// [`SpanInstall::seq`] reports at once. Every installed write carries that
    /// number, so a write to the span after the call is newer than every installed
    /// one and is read over it. The future then waits for the record to be durable,
    /// flushes every memtable holding writes at or below it (the active memtable is
    /// rotated as the record is applied, and the flusher leaves later memtables
    /// alone until the install has switched), writes each table in service that
    /// holds a write of the span below the number again without those writes, at
    /// its level, or takes it out whole when every write it holds is one, writes the
    /// installed tables at level 0 sealed near `sst_bytes`, and switches to the
    /// manifest listing the result, with `flushed_seq` at least the install's
    /// number; then it deletes the tables taken out and the log segments at or below
    /// the number. Holds the turnstile from the flush to the deletion. Everything
    /// after the numbering runs in a task the engine spawns, so dropping the returned
    /// future, or not polling it, leaves the install to finish.
    ///
    /// One install runs at a time. An install of an empty `source` is a delete of
    /// the span. A snapshot older than the install reads the span as empty once the
    /// switch is made, and one at or above the install's number reads the span as it
    /// was until the switch and as installed after it: the install replaces the
    /// span's history, it does not add to it (D-054).
    ///
    /// # Errors
    ///
    /// The returned future resolves with the refusal ([`InstallRefused`]) or with an
    /// I/O error, and what an error leaves depends on when it came:
    ///
    /// - Before the install's manifest is written (flushing, reading the tables,
    ///   writing the rewrites and the installed tables): the engine is as it was,
    ///   and the tables written so far are orphans the next open removes.
    /// - Writing the install's manifest or switching `CURRENT` to it: whether
    ///   `CURRENT` names it is not known, and a manifest file may hold the next
    ///   number. The engine quiesces (D-044, traced as `EngineQuiesced`), so no
    ///   flush or compaction tries to write a manifest under that number, and the
    ///   next open finds the span as it was or as installed.
    /// - After the switch: the install is in force and resolves `Ok`. An error
    ///   deleting the tables it took out or the log segments at or below its number
    ///   is traced as `InstallCleanupFailed`; the tables left are orphans the next
    ///   open removes, and the segments go with the next flush's.
    ///
    /// If the install's task ends without an outcome, panicking or dropped with its
    /// runtime, the future resolves with an error saying so.
    // PROPOSED(D-054): the live install of a span, in one manifest switch.
    pub fn install_span(&self, range: Range<Bytes>, source: SpanSource<E>) -> SpanInstall {
        self.install_spans(vec![range], source, WriteBatch::new())
    }

    /// Installs `source` over the spans `spans` while the engine runs, and carries
    /// `repair`, the receiver's own writes, with it: every write of a key in any
    /// span that the engine holds is taken out, the newest live write of every key
    /// `source` holds put in, and the repair put in over both, all in one manifest
    /// switch (D-066, D-068). A crash at any point leaves every span as it was, or
    /// every span installed with the repair, never a mixture across the spans or
    /// between the installed tables and the repair.
    ///
    /// It is [`install_span`](Self::install_span) over a set: numbered, flushed,
    /// rewritten and switched as that is, with the same refusals and what a
    /// snapshot sees. The spans must be sorted and disjoint, and every key of the
    /// source and of the repair must lie in one of them. A repair that is not
    /// empty takes the log record after the install's own, numbered with it under
    /// one lock so no other write falls between, and holding no write either;
    /// [`SpanInstall::repair_seq`] reports it. Its writes, the last per key and
    /// deletes kept, are written at that number into a table of their own, which
    /// the install's switch lists beside the installed tables: so a repair write
    /// is read over the source's write of the same key, and a write to a span after
    /// the call is read over both. An empty repair takes no number and writes no
    /// table, as `install_span` does.
    ///
    /// # Errors
    ///
    /// As [`install_span`](Self::install_span)'s, and refused
    /// ([`InstallRefused::SpansOverlap`], [`InstallRefused::RepairOutsideSpan`])
    /// before it is numbered when the spans are out of order or overlap, or when
    /// the repair writes a key outside every span; and refused
    /// ([`InstallRefused::RepairNotNext`]) once numbered if the repair's number is
    /// not the install's plus one, which the one hold of the lock that numbers them
    /// rules out.
    // PROPOSED(D-068): several spans and the receiver's repair, in one manifest switch.
    pub fn install_spans(
        &self,
        spans: Vec<Range<Bytes>>,
        source: SpanSource<E>,
        repair: WriteBatch,
    ) -> SpanInstall {
        self.begin_install(spans, source, repair.ops, false)
    }

    /// Deletes every key in `range` with one manifest switch, and no tombstone: an
    /// install of nothing over the span (SHARD.md §11, storage 3). It is numbered,
    /// flushed and switched as [`install_span`](Self::install_span) is, and shares
    /// its one-at-a-time rule, its refusals and what a snapshot sees; a write to
    /// the span after the call is newer than the delete and survives it.
    // PROPOSED(D-055): the range delete, an install of nothing.
    pub fn delete_range(&self, range: Range<Bytes>) -> SpanInstall {
        self.delete_ranges(vec![range])
    }

    /// Deletes every key in each of `spans` with one manifest switch, and no
    /// tombstone: an install of nothing over the set, as
    /// [`delete_range`](Self::delete_range) is over one span, with
    /// [`install_spans`](Self::install_spans)'s rule for the spans (D-068).
    // PROPOSED(D-068): the range delete over several spans, in one manifest switch.
    pub fn delete_ranges(&self, spans: Vec<Range<Bytes>>) -> SpanInstall {
        self.begin_install(spans, SpanSource::empty(), Vec::new(), true)
    }

    /// An install or a range delete, numbered now; see
    /// [`install_spans`](Self::install_spans).
    fn begin_install(
        &self,
        spans: Vec<Range<Bytes>>,
        source: SpanSource<E>,
        repair: Vec<(Bytes, Value)>,
        delete: bool,
    ) -> SpanInstall {
        let refused = |why: InstallRefused| SpanInstall {
            seq: None,
            repair_seq: None,
            state: InstallState::Refused(Some(why.into_io())),
        };
        if spans.is_empty() || spans.iter().any(|s| s.start >= s.end) {
            return refused(InstallRefused::EmptySpan);
        }
        // PROPOSED(D-068): a set of spans is sorted and disjoint.
        if let Err(why) = check_disjoint(&spans) {
            return refused(why);
        }
        if self.quiesced() {
            return refused(InstallRefused::Quiesced);
        }
        if source.outside(&spans)
            && let Some((first, last)) = source.key_range()
        {
            return refused(InstallRefused::OutsideSpan { first, last });
        }
        // PROPOSED(D-068): the repair lies inside the spans it is carried with.
        if let Some((key, _)) = repair.iter().find(|(key, _)| !in_spans(&spans, key)) {
            return refused(InstallRefused::RepairOutsideSpan { key: key.clone() });
        }
        let (markers, seq, repair_seq) = {
            let mut install = lock(&self.shared.install);
            if install.is_some() {
                return refused(InstallRefused::InProgress);
            }
            // PROPOSED(D-068): the repair's number is the record after the
            // install's, taken with it.
            let markers = self.number_empty(if repair.is_empty() { 1 } else { 2 });
            let seq = markers[0].seq();
            let repair_seq = markers.get(1).map(Write::seq);
            // PROPOSED(D-068): argued from the one hold of the pending lock in
            // `number_empty`, not tested: in the simulator nothing runs between the
            // two numbers, so no test can put a write there. Checked, so that a
            // change which lets one in refuses every install that carries a repair
            // on the real runtime, loudly, rather than switching to a manifest that
            // claims the write between is in a table. Both records hold no write,
            // and the install is not yet marked in progress.
            if let Some(repair_seq) = repair_seq
                && repair_seq != seq + 1
            {
                return refused(InstallRefused::RepairNotNext { seq, repair_seq });
            }
            *install = Some(seq);
            (markers, seq, repair_seq)
        };
        if self.shared.config.variant == Variant::NoWalBeforeMemtable {
            // That engine applied the record as it was written, before the install
            // was marked: the split is made here instead.
            self.shared.split_at_install(seq);
        }
        let slot: Arc<Mutex<InstallSlot>> = Arc::default();
        let task = InstallTask {
            hold: Some(InstallHold {
                shared: self.shared.clone(),
                seq,
            }),
            slot: slot.clone(),
        };
        let shared = self.shared.clone();
        let plan = InstallPlan {
            seq,
            repair_seq,
            spans,
            source,
            repair,
            delete,
        };
        // PROPOSED(D-054): the install runs in a task of its own, so a caller that
        // drops its future or leaves it unpolled cannot stop it half-way, with a
        // manifest written under the next number and never switched to.
        self.shared.env.spawn("span-install", async move {
            let result = async {
                for marker in markers {
                    marker.await?;
                }
                shared.install_span(plan).await
            }
            .await;
            task.finish(result);
        });
        SpanInstall {
            seq: Some(seq),
            repair_seq,
            state: InstallState::Running(slot),
        }
    }

    /// `count` log records holding no write, numbered back to back and synced, and
    /// pending under one hold of the pending lock, so no other write is numbered
    /// between them (D-021). One is what [`write`](Self::write) makes of an empty
    /// batch.
    // PROPOSED(D-068): the repair's number is the record after the install's.
    fn number_empty(&self, count: usize) -> Vec<Write<E>> {
        if self.shared.config.variant == Variant::NoWalBeforeMemtable {
            // That engine takes no pending lock: it applies as it writes.
            return (0..count)
                .map(|_| self.write(WriteBatch::new(), true))
                .collect();
        }
        let mut pending = lock(&self.shared.pending);
        (0..count)
            .map(|_| {
                let append = self.shared.wal.append_with(encode_batch(&[]), true);
                pending.insert(append.seq(), Vec::new());
                Write {
                    shared: self.shared.clone(),
                    append,
                }
            })
            .collect()
    }

    /// The manifest in force.
    #[must_use]
    pub fn manifest(&self) -> Manifest {
        lock(&self.shared.tables).manifest.clone()
    }

    /// The directory this engine runs in: its configuration's, unchanged since
    /// the open, and no I/O to read. A caller that checked something about the
    /// directory before the open — ananke-raft's Raft store format, read before
    /// anything writes — compares it with this, so a check of one directory can
    /// never stand for another's.
    // PROPOSED(D-060): the store's format record, read before anything writes.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.shared.config.dir
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
    /// Quiesces the engine and wakes the flusher, which stops at its check. The
    /// first call traces it; later ones do nothing.
    // D-044: a durable refusal, and a refused engine that does no work.
    fn quiesce(&self, reason: &'static str) {
        if self.quiesced.swap(true, Ordering::SeqCst) {
            return;
        }
        self.env.trace(TraceEvent::EngineQuiesced {
            dir: self.config.dir.clone(),
            reason,
        });
        let waker = lock(&self.flusher).waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

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
        // PROPOSED(D-054): one applier at a time, so the pop order is the apply order.
        let _order = lock(&self.apply_order);
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
            self.split_at_install(s);
        }
    }

    /// Rotates the active memtable, if it holds anything, when `seq` is the install
    /// in progress: every write at or below the install is then in a memtable the
    /// install flushes, and every later one in a memtable the flusher holds back
    /// until the install has switched.
    // PROPOSED(D-054): the live install of a span, in one manifest switch.
    fn split_at_install(&self, seq: Seq) {
        if *lock(&self.install) != Some(seq) {
            return;
        }
        let active = lock(&self.tables).active.clone();
        if !active.is_empty() {
            self.rotate(&active);
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
        self.rotate(&active);
    }

    /// Makes `active` immutable and starts a fresh memtable, unless someone else
    /// already rotated it, and wakes the flusher.
    fn rotate(&self, active: &Arc<Memtable>) {
        let rotated = {
            let mut tables = lock(&self.tables);
            if !Arc::ptr_eq(&tables.active, active) {
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

    /// Flushes `memtable`, the head of the immutable queue: table, manifest,
    /// switch, release, then the log segments the table made redundant. Call it
    /// with the turnstile held.
    async fn flush(&self, memtable: &Arc<Memtable>) -> io::Result<()> {
        let (meta, reader) = self.write_sst(memtable).await?;
        let mut next = self.manifest_edit(&[], vec![meta.clone()]);
        next.flushed_seq = meta.max_seq;
        let max_seq = meta.max_seq;
        if self.config.variant == Variant::ReleaseBeforeManifest {
            // The bug: the table is taken for durable once written. It serves
            // reads, the memtable goes, the log segments go, and only then is
            // the manifest written. A crash before the manifest is durable
            // leaves the table an orphan and its records nowhere.
            self.install(next.clone(), &[], vec![(meta, reader)]);
            self.release(memtable);
            self.wal.delete_segments_through(max_seq).await?;
            self.write_manifest(&next).await?;
        } else {
            self.write_manifest(&next).await?;
            self.install(next, &[], vec![(meta, reader)]);
            self.release(memtable);
            self.wal.delete_segments_through(max_seq).await?;
        }
        Ok(())
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

    /// The install `plan` asks for, once its records are durable: see
    /// [`Engine::install_spans`].
    // PROPOSED(D-054): the live install of a span, in one manifest switch.
    // PROPOSED(D-068): several spans and the receiver's repair, in that one switch.
    async fn install_span(&self, plan: InstallPlan<E>) -> io::Result<InstallInfo> {
        let InstallPlan {
            seq,
            repair_seq,
            spans,
            source,
            repair,
            delete,
        } = plan;
        let _turn = self.turnstile.acquire().await;
        if self.quiesced.load(Ordering::SeqCst) {
            return Err(InstallRefused::Quiesced.into_io());
        }
        // PROPOSED(D-055): the range delete's variant forgets the memtables.
        let skip_memtables = delete && self.config.variant == Variant::RangeDeleteSkipsMemtables;
        // The last record the install numbered: its repair's, if it carries one.
        // Neither holds a write, so every write at or below it is at or below
        // `seq`, and in a table once the memtables below `seq` are flushed.
        let top = repair_seq.unwrap_or(seq);
        // The repair's last write per key.
        let repair: BTreeMap<Bytes, Value> = repair.into_iter().collect();

        // Every memtable holding writes at or below the install, flushed: the
        // active one was rotated as the install's record was applied, and the
        // flusher holds back every memtable after it.
        loop {
            let head = lock(&self.tables).immutable.front().cloned();
            match head {
                Some(memtable) if memtable.min_seq() <= seq && !skip_memtables => {
                    self.flush(&memtable).await?;
                }
                _ => break,
            }
        }
        let flushed = |manifest: &mut Manifest| {
            if !skip_memtables {
                manifest.flushed_seq = manifest.flushed_seq.max(top);
            }
        };
        let metas = |tables: &[Table<E>]| -> Vec<SstMeta> {
            tables.iter().map(|(m, _)| m.clone()).collect()
        };
        let event = |manifest: u64, part: &InstallPart<E>| TraceEvent::SpanInstalled {
            manifest,
            spans: part
                .spans
                .iter()
                .map(|s| (s.start.clone(), s.end.clone()))
                .collect(),
            seq,
            removed: part.removed.clone(),
            rewritten: part
                .rewritten
                .iter()
                .map(|(from, m, _)| (*from, m.number, m.first_key.clone(), m.last_key.clone()))
                .collect(),
            added: part
                .added
                .iter()
                .map(|(m, _)| (m.number, m.first_key.clone(), m.last_key.clone()))
                .collect(),
            repair: part
                .repair
                .as_ref()
                .map(|(m, _)| (top, m.number, m.first_key.clone(), m.last_key.clone())),
        };

        if self.config.variant == Variant::InstallSwitchPerSpan && spans.len() > 1 {
            // The bug: the spans one at a time, each the install of that span alone
            // at the install's number, with the source's keys and the repair's that
            // lie in it, traced and switched before the next is begun. A crash
            // between two switches leaves the earlier spans installed and the later
            // as they were.
            let (mut removed, mut rewrites, mut added, mut keys) = (Vec::new(), 0, 0, 0);
            for span in &spans {
                let part = self
                    .install_part(
                        std::slice::from_ref(span),
                        true,
                        &source,
                        &repair,
                        seq,
                        repair_seq,
                    )
                    .await?;
                let mut next = self.manifest_edit(&part.removed, part.put_in_metas());
                flushed(&mut next);
                self.env.trace(event(next.number, &part));
                self.write_manifest(&next).await?;
                removed.extend(part.removed.iter().copied());
                (rewrites, added, keys) = (
                    rewrites + part.rewritten.len(),
                    added + part.added.len(),
                    keys + part.keys,
                );
                let taken_out = part.removed.clone();
                self.install(next, &taken_out, part.put_in());
            }
            return Ok(self
                .finish_install(
                    seq,
                    repair_seq,
                    top,
                    &removed,
                    skip_memtables,
                    (rewrites, added, keys),
                )
                .await);
        }

        let part = self
            .install_part(&spans, false, &source, &repair, seq, repair_seq)
            .await?;
        let removed = part.removed.clone();
        let counts = (part.rewritten.len(), part.added.len(), part.keys);
        match self.config.variant {
            Variant::InstallInTwoSwitches => {
                // The bug: the spans' keys go with one switch and the installed
                // tables come with a second. A crash between the two leaves the
                // spans empty.
                let traced = event(0, &part);
                let InstallPart {
                    rewritten,
                    added,
                    repair,
                    ..
                } = part;
                let rewritten: Vec<Table<E>> =
                    rewritten.into_iter().map(|(_, m, r)| (m, r)).collect();
                let mut first = self.manifest_edit(&removed, metas(&rewritten));
                flushed(&mut first);
                self.write_manifest(&first).await?;
                self.install(first, &removed, rewritten);
                let mut put_in = added;
                put_in.extend(repair);
                let mut next = self.manifest_edit(&[], metas(&put_in));
                flushed(&mut next);
                self.env.trace(with_manifest(traced, next.number));
                self.write_manifest(&next).await?;
                self.install(next, &[], put_in);
            }
            Variant::RepairAfterSwitch if part.repair.is_some() => {
                // The bug: the switch puts the installed tables in without the
                // repair, and a second switch puts the repair's table in after it.
                // A crash between the two leaves the tables without their repair.
                let traced = event(0, &part);
                let InstallPart {
                    rewritten,
                    added,
                    repair,
                    ..
                } = part;
                let mut put_in: Vec<Table<E>> =
                    rewritten.into_iter().map(|(_, m, r)| (m, r)).collect();
                put_in.extend(added);
                let mut next = self.manifest_edit(&removed, metas(&put_in));
                flushed(&mut next);
                self.env.trace(with_manifest(traced, next.number));
                self.write_manifest(&next).await?;
                self.install(next, &removed, put_in);
                let repair: Vec<Table<E>> = repair.into_iter().collect();
                let mut after = self.manifest_edit(&[], metas(&repair));
                flushed(&mut after);
                self.write_manifest(&after).await?;
                self.install(after, &[], repair);
            }
            Variant::RepairBeforeSwitch if part.repair.is_some() => {
                // The bug: a switch puts the repair's table in first, and the
                // install's own switch takes the spans' keys out and puts the
                // installed tables in after it. A crash between the two leaves the
                // repair without its tables.
                let traced = event(0, &part);
                let InstallPart {
                    rewritten,
                    added,
                    repair,
                    ..
                } = part;
                let repair: Vec<Table<E>> = repair.into_iter().collect();
                let mut before = self.manifest_edit(&[], metas(&repair));
                flushed(&mut before);
                self.write_manifest_file(&before).await?;
                // The replacement is traced once the repair's manifest is written
                // and before `CURRENT` names it, and names the install's own
                // manifest, the next one: the turnstile holds every other manifest
                // off until the install's is written. Traced before the repair's
                // manifest is written, it would name a manifest above one written
                // after it, which the trace's readers take for a lineage a
                // fallback abandoned.
                self.env.trace(with_manifest(traced, before.number + 1));
                self.switch_to(&before).await?;
                self.install(before, &[], repair);
                let mut put_in: Vec<Table<E>> =
                    rewritten.into_iter().map(|(_, m, r)| (m, r)).collect();
                put_in.extend(added);
                let mut next = self.manifest_edit(&removed, metas(&put_in));
                flushed(&mut next);
                self.write_manifest(&next).await?;
                self.install(next, &removed, put_in);
            }
            _ => {
                // PROPOSED(D-068): the repair's table is listed by the install's
                // own manifest, beside the installed tables, so it is in force
                // exactly when they are.
                let mut next = self.manifest_edit(&removed, part.put_in_metas());
                flushed(&mut next);
                self.env.trace(event(next.number, &part));
                if let Err(error) = self.write_manifest(&next).await {
                    // PROPOSED(D-054): whether CURRENT names the install's manifest is
                    // not known, and its file may hold the next number: a flush or a
                    // compaction writing under it would fail for good, or write over
                    // the install. The engine does no more work, and the next open
                    // finds the spans as they were or as installed.
                    self.quiesce("an install's manifest or its switch failed");
                    return Err(error);
                }
                self.install(next, &removed, part.put_in());
            }
        }
        Ok(self
            .finish_install(seq, repair_seq, top, &removed, skip_memtables, counts)
            .await)
    }

    /// An install's tables over `spans`, written and synced but not yet listed:
    /// every table in service holding a write of the spans below `seq` taken out,
    /// whole when every write it holds is one, which one span holding both of its
    /// ends and no write at or past `seq` says, else written again at its level
    /// without those writes; the source's newest live write of each key of the
    /// spans at `seq`, in level-0 tables sealed near `sst_bytes`; and the repair's
    /// writes in the spans at `repair_seq`, in a table of their own. A key of the
    /// source outside every one of `spans` refuses the install, and the tables
    /// written so far are orphans the next open removes; but with `one_of_several`,
    /// which only the variant that switches span by span sets, `spans` is one span
    /// of the install's, and only the source's keys in it are walked.
    // PROPOSED(D-068): several spans and the receiver's repair, in one manifest switch.
    async fn install_part(
        &self,
        spans: &[Range<Bytes>],
        one_of_several: bool,
        source: &SpanSource<E>,
        repair: &BTreeMap<Bytes, Value>,
        seq: Seq,
        repair_seq: Option<Seq>,
    ) -> io::Result<InstallPart<E>> {
        let in_span = |user: &[u8]| in_spans(spans, user);
        let in_service: Vec<(SstMeta, Arc<SstReader<FileOf<E>>>)> = lock(&self.tables).ssts.clone();
        let mut removed = Vec::new();
        let mut rewritten = Vec::new();
        for (meta, reader) in &in_service {
            if !overlaps_spans(spans, &meta.first_key, &meta.last_key) {
                continue;
            }
            if within_one_span(spans, &meta.first_key, &meta.last_key) && meta.max_seq < seq {
                removed.push(meta.number);
                continue;
            }
            let mut writer = SstWriter::new();
            let mut iter = reader.iter();
            let mut dropped = false;
            while let Some((key, value)) = iter.next().await? {
                let (user, s) = ikey::decode(&key)?;
                if in_span(&user) && s < seq {
                    dropped = true;
                } else {
                    writer.add(&user, s, &value);
                }
            }
            if !dropped {
                continue;
            }
            removed.push(meta.number);
            if writer.entries() > 0 {
                let (rewrite, reader) = self.write_output(writer, meta.level).await?;
                rewritten.push((meta.number, rewrite, reader));
            }
        }

        // The installed tables: the source's newest write of each key, if it is
        // live, at the install's number.
        let mut merge: MergeIter<FileOf<E>> = MergeIter::new(
            source
                .tables
                .iter()
                .map(|(_, r)| Source::Sst(r.iter()))
                .collect(),
        );
        if one_of_several && let [only] = spans {
            merge.seek(&ikey::lower_bound(&only.start)).await?;
        }
        let mut added = Vec::new();
        let mut writer = SstWriter::new();
        let mut last_user: Option<Bytes> = None;
        let mut keys = 0;
        while let Some((key, value)) = merge.next().await? {
            let (user, source_seq) = ikey::decode(&key)?;
            if last_user.as_ref() == Some(&user) {
                continue;
            }
            last_user = Some(user.clone());
            if !in_span(&user) {
                if one_of_several {
                    // Past the one span walked: the rest are other spans'.
                    break;
                }
                // The manifest's key ranges said otherwise. The tables written so
                // far are orphans, which the next open removes.
                return Err(InstallRefused::OutsideSpan {
                    first: user.clone(),
                    last: user,
                }
                .into_io());
            }
            if value == Value::Tombstone {
                continue;
            }
            if writer.entries() > 0 && writer.bytes_so_far() as u64 >= self.config.sst_bytes {
                let full = std::mem::take(&mut writer);
                added.push(self.write_output(full, 0).await?);
            }
            if self.config.variant == Variant::InstallKeepsSourceNumbers {
                // The bug: the installed write keeps the number its source gave it.
                writer.add(&user, source_seq, &value);
            } else {
                writer.add(&user, seq, &value);
            }
            keys += 1;
        }
        if writer.entries() > 0 {
            added.push(self.write_output(writer, 0).await?);
        }

        // PROPOSED(D-068): the repair, the receiver's own writes, in a table of its
        // own at the repair's number, above the installed tables' and below every
        // later write: the last write per key, a delete kept as a tombstone, since
        // it must hide the source's write of the key.
        let mut repair_table = None;
        if let Some(repair_seq) = repair_seq {
            let mut writer = SstWriter::new();
            for (key, value) in repair.iter().filter(|(key, _)| in_span(key)) {
                writer.add(key, repair_seq, value);
            }
            if writer.entries() > 0 {
                repair_table = Some(self.write_output(writer, 0).await?);
            }
        }
        Ok(InstallPart {
            spans: spans.to_vec(),
            removed,
            rewritten,
            added,
            repair: repair_table,
            keys,
        })
    }

    /// After an install's switch: the tables it took out deleted, and the log
    /// segments at or below `top`, its last record; an error in either traced and
    /// the install resolved as made.
    // PROPOSED(D-054): an error after the switch does not undo the install.
    async fn finish_install(
        &self,
        seq: Seq,
        repair_seq: Option<Seq>,
        top: Seq,
        removed: &[u64],
        skip_memtables: bool,
        (rewritten, added, keys): (usize, usize, u64),
    ) -> InstallInfo {
        let manifest = lock(&self.tables).manifest.number;
        // PROPOSED(D-054): the switch is durable, so the install is in force and
        // resolves as made whatever the deletions do; what they leave is removed as
        // orphans at the next open, or by the next flush's deletion of the log.
        let cleanup = async {
            self.delete_tables(removed).await?;
            if !skip_memtables {
                self.wal.delete_segments_through(top).await?;
            }
            Ok::<(), io::Error>(())
        }
        .await;
        if let Err(error) = cleanup {
            self.env.trace(TraceEvent::InstallCleanupFailed {
                seq,
                error: error.to_string(),
            });
        }
        InstallInfo {
            seq,
            repair_seq,
            manifest,
            removed: removed.len(),
            rewritten,
            added,
            keys,
        }
    }
}

/// A table written and open, with what the manifest says of it.
type Table<E> = (SstMeta, SstReader<FileOf<E>>);

/// An install's tables, written and not yet listed: see `Shared::install_part`.
// PROPOSED(D-068): several spans and the receiver's repair, in one manifest switch.
struct InstallPart<E: Environment> {
    /// The spans it covers.
    spans: Vec<Range<Bytes>>,
    /// Tables taken out of service, the rewritten ones' originals included.
    removed: Vec<u64>,
    /// Each rewrite: its original's number, and the table written in its place.
    rewritten: Vec<(u64, SstMeta, SstReader<FileOf<E>>)>,
    /// The installed tables.
    added: Vec<Table<E>>,
    /// The repair's table, when there is a repair with a write in the spans.
    repair: Option<Table<E>>,
    /// Keys installed.
    keys: u64,
}

impl<E: Environment> InstallPart<E> {
    /// What the manifest lists in place of the tables taken out: the rewrites,
    /// the installed tables and the repair's.
    fn put_in_metas(&self) -> Vec<SstMeta> {
        self.rewritten
            .iter()
            .map(|(_, m, _)| m.clone())
            .chain(self.added.iter().map(|(m, _)| m.clone()))
            .chain(self.repair.iter().map(|(m, _)| m.clone()))
            .collect()
    }

    /// The tables put in service, in the order [`put_in_metas`](Self::put_in_metas)
    /// lists them.
    fn put_in(self) -> Vec<Table<E>> {
        self.rewritten
            .into_iter()
            .map(|(_, m, r)| (m, r))
            .chain(self.added)
            .chain(self.repair)
            .collect()
    }
}

/// `event`, a [`TraceEvent::SpanInstalled`], naming `manifest` as the one that
/// makes it the state.
fn with_manifest(event: TraceEvent, manifest: u64) -> TraceEvent {
    match event {
        TraceEvent::SpanInstalled {
            spans,
            seq,
            removed,
            rewritten,
            added,
            repair,
            ..
        } => TraceEvent::SpanInstalled {
            manifest,
            spans,
            seq,
            removed,
            rewritten,
            added,
            repair,
        },
        other => other,
    }
}

/// What an install is asked to do, once it is numbered: see
/// [`Engine::install_spans`].
// PROPOSED(D-068): several spans and the receiver's repair, in one manifest switch.
struct InstallPlan<E: Environment> {
    /// The install's own record: every installed write carries it.
    seq: Seq,
    /// The repair's record, the one after `seq`, when there is a repair.
    repair_seq: Option<Seq>,
    /// The spans, sorted and disjoint.
    spans: Vec<Range<Bytes>>,
    /// What is installed over them.
    source: SpanSource<E>,
    /// The receiver's own writes, in the order given.
    repair: Vec<(Bytes, Value)>,
    /// Whether it is a range delete.
    delete: bool,
}

/// Whether `key` lies in one of `spans`.
// PROPOSED(D-068): several spans in one manifest switch.
fn in_spans(spans: &[Range<Bytes>], key: &[u8]) -> bool {
    spans
        .iter()
        .any(|s| s.start[..] <= *key && *key < s.end[..])
}

/// Whether the keys from `first` to `last` meet one of `spans`.
// PROPOSED(D-068): several spans in one manifest switch.
fn overlaps_spans(spans: &[Range<Bytes>], first: &[u8], last: &[u8]) -> bool {
    spans
        .iter()
        .any(|s| *first < s.end[..] && *last >= s.start[..])
}

/// Whether one of `spans` holds both `first` and `last`, and so every key between.
// PROPOSED(D-068): several spans in one manifest switch.
fn within_one_span(spans: &[Range<Bytes>], first: &[u8], last: &[u8]) -> bool {
    spans
        .iter()
        .any(|s| s.start[..] <= *first && *last < s.end[..])
}

/// Refuses spans that are out of order or overlap: each must end at or before the
/// next one starts.
// PROPOSED(D-068): a set of spans is sorted and disjoint.
fn check_disjoint<K: AsRef<[u8]>>(spans: &[Range<K>]) -> Result<(), InstallRefused> {
    for pair in spans.windows(2) {
        let (end, start) = (pair[0].end.as_ref(), pair[1].start.as_ref());
        if end > start {
            return Err(InstallRefused::SpansOverlap {
                end: Bytes::copy_from_slice(end),
                start: Bytes::copy_from_slice(start),
            });
        }
    }
    Ok(())
}

/// Writes `writer` into `dir` as table `number` at level 0, synced, for a checkpoint
/// of a span.
async fn write_level_0_table<E: Environment>(
    shared: &Shared<E>,
    dir: &Path,
    number: u64,
    writer: SstWriter,
) -> io::Result<SstMeta> {
    let (first_key, last_key) = writer.key_range().expect("a table has writes");
    let (first_seq, max_seq) = writer.seq_range().expect("a table has writes");
    let entries = writer.entries();
    let bytes = writer.finish();
    let len = bytes.len() as u64;
    let path = sst_path(dir, number);
    if shared.config.variant == Variant::SpanCheckpointUnsynced {
        // The bug: the table is written and never synced, though the manifest and
        // CURRENT that name it are.
        let file = shared
            .env
            .fs()
            .open(&path, OpenOptions::new().write(true).create_new(true))
            .await?;
        file.write_at(0, bytes).await?;
    } else {
        write_file(&shared.env, &path, bytes).await?;
    }
    Ok(SstMeta {
        number,
        level: 0,
        first_seq,
        max_seq,
        entries,
        bytes: len,
        first_key,
        last_key,
    })
}

/// A store read and checked whole, to install over a span with
/// [`Engine::install_span`]: what [`Engine::checkpoint_span`] writes, or any store
/// whose keys all lie inside the span it is installed over.
// PROPOSED(D-054): the live install of a span, in one manifest switch.
pub struct SpanSource<E: Environment> {
    tables: Vec<(SstMeta, Arc<SstReader<FileOf<E>>>)>,
}

impl<E: Environment> SpanSource<E> {
    /// A source holding nothing: installing it takes the span's keys out and puts
    /// nothing in.
    #[must_use]
    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }

    /// The smallest and largest user key the source's tables say they hold, if
    /// they hold any.
    #[must_use]
    pub fn key_range(&self) -> Option<(Bytes, Bytes)> {
        let first = self.tables.iter().map(|(m, _)| &m.first_key).min()?;
        let last = self.tables.iter().map(|(m, _)| &m.last_key).max()?;
        Some((first.clone(), last.clone()))
    }

    /// Tables in the source.
    #[must_use]
    pub fn tables(&self) -> usize {
        self.tables.len()
    }

    /// Whether a table of the source holds a key outside every one of `spans`, by
    /// the first and last key its manifest gives: either end outside them. A table
    /// whose two ends lie in two spans can still hold a key between them, which
    /// the install finds key by key as it writes.
    // PROPOSED(D-068): the source lies inside the spans.
    fn outside(&self, spans: &[Range<Bytes>]) -> bool {
        self.tables
            .iter()
            .any(|(m, _)| !in_spans(spans, &m.first_key) || !in_spans(spans, &m.last_key))
    }
}

impl<E: Environment> std::fmt::Debug for SpanSource<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpanSource")
            .field("tables", &self.tables.len())
            .finish_non_exhaustive()
    }
}

/// An install on its way: numbered at once, and resolving once the switch that
/// makes it the state is durable, or with the refusal or the error. The install
/// runs in a task of the engine's own; dropping this, or never polling it, leaves it
/// to finish, and a task that ends without an outcome resolves this with an error
/// rather than leaving it pending.
// PROPOSED(D-054): the live install of a span, in one manifest switch.
pub struct SpanInstall {
    seq: Option<Seq>,
    repair_seq: Option<Seq>,
    state: InstallState,
}

/// Where an install stands for its caller.
enum InstallState {
    /// Refused before it was numbered; the error until it is taken.
    Refused(Option<io::Error>),
    /// Running in its task, which leaves the outcome here.
    Running(Arc<Mutex<InstallSlot>>),
}

/// The outcome an install's task leaves for its caller.
#[derive(Default)]
struct InstallSlot {
    result: Option<io::Result<InstallInfo>>,
    /// Set with the outcome, and never unset: taken, it is not given again.
    done: bool,
    waker: Option<Waker>,
}

/// What an install's task owns for as long as it runs: the hold on the flusher and
/// its caller's outcome. However the task ends — its work done, or the task
/// panicking, aborted or dropped with its runtime — the hold is let go first, and
/// then the caller gets an outcome and is woken, an error if the work left none.
// PROPOSED(D-054): the caller always learns how the install ended.
struct InstallTask<E: Environment> {
    hold: Option<InstallHold<E>>,
    slot: Arc<Mutex<InstallSlot>>,
}

impl<E: Environment> InstallTask<E> {
    /// Lets the flusher go, then leaves `result` for the caller.
    fn finish(mut self, result: io::Result<InstallInfo>) {
        drop(self.hold.take());
        self.resolve(|| result);
    }

    /// Leaves an outcome for the caller and wakes it, unless one was left already.
    fn resolve(&self, result: impl FnOnce() -> io::Result<InstallInfo>) {
        let waker = {
            let mut slot = lock(&self.slot);
            if slot.done {
                return;
            }
            slot.done = true;
            slot.result = Some(result());
            slot.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<E: Environment> Drop for InstallTask<E> {
    fn drop(&mut self) {
        drop(self.hold.take());
        self.resolve(|| {
            Err(io::Error::other(
                "the install's task ended without an outcome",
            ))
        });
    }
}

impl SpanInstall {
    /// The install's sequence number, known before anything is written; `None` when
    /// it was refused before it took one.
    #[must_use]
    pub fn seq(&self) -> Option<Seq> {
        self.seq
    }

    /// The repair's sequence number, the record after [`seq`](Self::seq), known
    /// before anything is written; `None` when the install carries no repair or
    /// was refused before it took a number.
    // PROPOSED(D-068): the repair, in a table of its own numbered above the source's.
    #[must_use]
    pub fn repair_seq(&self) -> Option<Seq> {
        self.repair_seq
    }
}

impl std::fmt::Debug for SpanInstall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpanInstall")
            .field("seq", &self.seq)
            .field("repair_seq", &self.repair_seq)
            .finish_non_exhaustive()
    }
}

impl Future for SpanInstall {
    type Output = io::Result<InstallInfo>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<InstallInfo>> {
        let gone = || io::Error::other("the install was polled after it resolved");
        match &mut self.state {
            InstallState::Refused(error) => Poll::Ready(Err(error.take().unwrap_or_else(gone))),
            InstallState::Running(slot) => {
                let mut slot = lock(slot);
                match slot.result.take() {
                    Some(result) => Poll::Ready(result),
                    None if slot.done => Poll::Ready(Err(gone())),
                    None => {
                        slot.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            }
        }
    }
}

/// Marks the install in progress for as long as its task runs, and lets the flusher
/// at the memtables it held back once the task has switched, failed, or been
/// dropped with its node.
// PROPOSED(D-054): the live install of a span, in one manifest switch.
struct InstallHold<E: Environment> {
    shared: Arc<Shared<E>>,
    seq: Seq,
}

impl<E: Environment> Drop for InstallHold<E> {
    fn drop(&mut self) {
        {
            let mut install = lock(&self.shared.install);
            if *install == Some(self.seq) {
                *install = None;
            }
        }
        let waker = lock(&self.shared.flusher).waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
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
            // PROPOSED(D-054): a memtable past an install in progress waits for its
            // switch, so no table holding a write newer than the install is written
            // before the installed tables are.
            let held = lock(&self.0.install).is_some_and(|seq| memtable.min_seq() > seq);
            if !held {
                return Poll::Ready(Some(memtable));
            }
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
        // D-044: a quiesced engine does no work. The flush that
        // follows a recovery which lost state is the one that launders the
        // loss away — a manifest without the dropped table, and the log
        // segments that held the records deleted — so the task stops here and
        // leaves the disk as recovery found it.
        if shared.quiesced.load(Ordering::SeqCst) {
            return;
        }
        let flushed = async {
            {
                let _turn = shared.turnstile.acquire().await;
                // PROPOSED(D-054): an install flushes the memtables at or below its
                // number itself, under the turnstile; one this task was handed
                // before that is gone from the queue's head by the time it gets in.
                let still_head = lock(&shared.tables)
                    .immutable
                    .front()
                    .is_some_and(|front| Arc::ptr_eq(front, &memtable));
                if !still_head {
                    return Ok(());
                }
                shared.flush(&memtable).await?;
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
