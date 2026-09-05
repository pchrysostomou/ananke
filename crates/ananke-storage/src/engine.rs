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
//! tables newest first.
//!
//! Recovery reads `CURRENT`, the manifest it names (falling back to the newest
//! readable one if `CURRENT` or that manifest cannot be read, and then rewriting
//! `CURRENT` to say so), opens and fully verifies every table listed, dropping one it
//! cannot read and reporting the writes lost with it, removes orphans, and replays
//! the log from one past the manifest's `flushed_seq`.
//!
//! The [`Variant`]s for the crash sweep: [`Variant::Correct`];
//! [`Variant::NoWalBeforeMemtable`], which applies and acknowledges a write before
//! the log has it; and [`Variant::ReleaseBeforeManifest`], which releases a memtable
//! once its table is written but before the manifest names it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use ananke_env::{Environment, File, FileSystem, OpenOptions, TraceEvent};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::manifest::{
    self, Manifest, SstMeta, current_path, current_tmp_path, manifest_path, sst_path,
};
use crate::memtable::{Memtable, Value};
use crate::sst::{SstReader, SstWriter};
use crate::wal::{self, Append, Recovery, Seq, Wal, WalConfig};

type FileOf<E> = <<E as Environment>::Fs as FileSystem>::File;

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
    /// A memtable is released as soon as its table is written, before the manifest
    /// names the table. Until it does, the memtable's keys are readable nowhere.
    ReleaseBeforeManifest,
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
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
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
struct Tables<E: Environment> {
    active: Arc<Memtable>,
    immutable: VecDeque<Arc<Memtable>>,
    /// Oldest first.
    ssts: Vec<(SstMeta, Arc<SstReader<FileOf<E>>>)>,
    manifest: Manifest,
}

struct Flusher {
    waker: Option<Waker>,
    closed: bool,
}

struct Shared<E: Environment> {
    env: E,
    config: EngineConfig,
    wal: Wal<E>,
    tables: Mutex<Tables<E>>,
    flusher: Mutex<Flusher>,
    next_memtable: AtomicU64,
    /// Writes appended and not yet applied, by sequence number.
    pending: Mutex<BTreeMap<Seq, (Bytes, Value)>>,
}

/// What [`Engine::open`] found.
#[derive(Clone, Debug)]
pub struct EngineRecovery {
    /// The manifest in force; 0 is the empty state.
    pub manifest: u64,
    /// Set when the manifest `CURRENT` named could not be read and another was used,
    /// or when `CURRENT` itself could not be read (then 0): every table flushed after
    /// the one used is lost.
    pub fallback_from: Option<u64>,
    /// Every log record numbered this or below is in a table, if its table survived.
    pub flushed_seq: Seq,
    /// Tables in service.
    pub ssts: usize,
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

impl<E: Environment> std::fmt::Debug for Engine<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("dir", &self.shared.config.dir)
            .finish_non_exhaustive()
    }
}

/// Points `CURRENT` at manifest `number`: written as `CURRENT.tmp`, synced, renamed
/// over `CURRENT`, and the directory synced.
async fn switch_current<E: Environment>(env: &E, dir: &Path, number: u64) -> io::Result<()> {
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
    env.trace(TraceEvent::CurrentSwitched { manifest: number });
    Ok(())
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
    /// Any I/O error from the directory, the manifests, the tables or the log, or
    /// `InvalidData` for a log record that is not an op.
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

        // The manifest: the one CURRENT names, else the newest readable one below it;
        // if CURRENT itself cannot be read, the newest readable one there is. A crash
        // can leave CURRENT empty (its content's sync lied) or flip a bit in it, and
        // the manifests it would have named are still on disk.
        let current_bytes = read_whole(&env, &current_path(&dir)).await?;
        let current = current_bytes.as_deref().and_then(manifest::parse_current);
        let mut manifest = Manifest::empty();
        let mut fallback_from = None;
        if current.is_some() || !manifests.is_empty() {
            let named = current.unwrap_or(0);
            let mut candidates: Vec<u64> = manifests
                .iter()
                .copied()
                .filter(|&m| current.is_none_or(|named| m < named))
                .collect();
            if let Some(named) = current {
                candidates.push(named);
            }
            let mut found = None;
            while let Some(number) = candidates.pop() {
                let readable = match read_whole(&env, &manifest_path(&dir, number)).await? {
                    Some(bytes) => Manifest::decode(&bytes).ok(),
                    None => None,
                };
                if let Some(m) = readable {
                    found = Some(m);
                    break;
                }
            }
            manifest = found.unwrap_or_else(Manifest::empty);
            // A fallback is anything but reading CURRENT and the manifest it names:
            // an unreadable CURRENT with manifests on disk counts, even when none of
            // them can be read and the empty state is all that is left.
            if manifest.number != named || current.is_none() {
                fallback_from = Some(named);
                env.trace(TraceEvent::ManifestFallback {
                    from: named,
                    to: manifest.number,
                });
            }
        }

        // The tables: opened and checked whole; one that cannot be read is dropped.
        let mut ssts = Vec::new();
        let mut dropped = Vec::new();
        for meta in &manifest.ssts {
            let path = sst_path(&dir, meta.number);
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
                Ok(reader) => ssts.push((*meta, Arc::new(reader))),
                Err(reason) => {
                    env.trace(TraceEvent::SstDropped {
                        number: meta.number,
                        first_seq: meta.first_seq,
                        max_seq: meta.max_seq,
                        reason,
                    });
                    dropped.push(*meta);
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
        // open does not have to decide again from a damaged file: rewritten to name
        // the manifest used, or removed when nothing readable was left.
        if fallback_from.is_some() {
            if manifest.number > 0 {
                switch_current(&env, &dir, manifest.number).await?;
            } else if current_bytes.is_some() {
                fs.remove_file(&current_path(&dir)).await?;
                env.trace(TraceEvent::OrphanRemoved {
                    path: current_path(&dir),
                });
                fs.sync_dir(&dir).await?;
            }
        }

        // The log, from where the tables leave off.
        let (wal, recovery) = Wal::open(
            env.clone(),
            WalConfig {
                dir: dir.clone(),
                segment_bytes: config.segment_bytes,
                variant: config.wal_variant,
                expected_head: manifest.flushed_seq + 1,
            },
        )
        .await?;
        let flushed_seq = manifest.flushed_seq;
        let manifest_number = manifest.number;
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
            next_memtable: AtomicU64::new(2),
            pending: Mutex::new(BTreeMap::new()),
        });
        let mut replayed = 0;
        for (i, record) in recovery.records.iter().enumerate() {
            let seq = recovery.first_seq + i as u64;
            if seq <= flushed_seq {
                continue;
            }
            let (key, value) = decode_op(record.clone())?;
            shared.apply(seq, key, value);
            replayed += 1;
        }
        env.spawn("flusher", flusher(shared.clone()));
        let ssts = lock(&shared.tables).ssts.len();
        Ok((
            Self { shared },
            EngineRecovery {
                manifest: manifest_number,
                fallback_from,
                flushed_seq,
                ssts,
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
        self.write(key, Value::Live(value))
    }

    /// Deletes `key`, leaving a tombstone. Resolves like [`put`](Self::put).
    pub fn delete(&self, key: Bytes) -> Write<E> {
        self.write(key, Value::Tombstone)
    }

    fn write(&self, key: Bytes, value: Value) -> Write<E> {
        let append = self.shared.wal.append(encode_op(&key, &value));
        match self.shared.config.variant {
            Variant::NoWalBeforeMemtable => {
                // The bug: visible and acknowledged before the log has it.
                self.shared.apply(append.seq(), key, value);
            }
            _ => {
                lock(&self.shared.pending).insert(append.seq(), (key, value));
            }
        }
        Write {
            shared: self.shared.clone(),
            append,
        }
    }

    /// The value under `key`, if it is present: the active memtable first, then the
    /// immutable ones newest first, then the tables newest first.
    ///
    /// # Errors
    ///
    /// A table read's error.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Bytes>> {
        let (active, immutable, ssts) = {
            let tables = lock(&self.shared.tables);
            (
                tables.active.clone(),
                tables.immutable.clone(),
                tables
                    .ssts
                    .iter()
                    .map(|(_, r)| r.clone())
                    .collect::<Vec<_>>(),
            )
        };
        if let Some(value) = active.get(key) {
            return Ok(value.live());
        }
        for memtable in immutable.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Ok(value.live());
            }
        }
        for sst in ssts.iter().rev() {
            if let Some(value) = sst.get(key).await? {
                return Ok(value.live());
            }
        }
        Ok(None)
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
            let Some((s, (key, value))) = next else {
                return;
            };
            self.apply(s, key, value);
        }
    }

    /// Applies a durable write and rotates the active memtable if it is now full.
    fn apply(&self, seq: Seq, key: Bytes, value: Value) {
        let active = lock(&self.tables).active.clone();
        active.apply(seq, key, value);
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

    /// Writes `memtable` as the next table and syncs it.
    async fn write_sst(&self, memtable: &Memtable) -> io::Result<(SstMeta, SstReader<FileOf<E>>)> {
        let number = lock(&self.tables).manifest.next_sst;
        let mut writer = SstWriter::new();
        for (key, _, value) in memtable.entries() {
            writer.add(&key, &value);
        }
        let entries = writer.entries();
        let bytes = writer.finish();
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
        let meta = SstMeta {
            number,
            first_seq: memtable.min_seq(),
            max_seq: memtable.max_seq(),
            entries,
        };
        self.env.trace(TraceEvent::SstWritten {
            number,
            entries,
            bytes: len,
            first_seq: meta.first_seq,
            max_seq: meta.max_seq,
        });
        let reader = SstReader::open(file).await?;
        Ok((meta, reader))
    }

    /// Writes the next manifest listing `meta`, syncs it, switches `CURRENT` to it,
    /// and puts the table in service.
    async fn commit_manifest(&self, meta: SstMeta, reader: SstReader<FileOf<E>>) -> io::Result<()> {
        let next = {
            let tables = lock(&self.tables);
            let mut next = tables.manifest.clone();
            next.number += 1;
            next.next_sst = meta.number + 1;
            next.flushed_seq = meta.max_seq;
            next.ssts.push(meta);
            next
        };
        let fs = self.env.fs();
        let dir = &self.config.dir;
        let file = fs
            .open(
                &manifest_path(dir, next.number),
                OpenOptions::new().write(true).create_new(true),
            )
            .await?;
        file.write_at(0, next.encode()).await?;
        file.sync().await?;
        self.env.trace(TraceEvent::ManifestWritten {
            number: next.number,
            flushed_seq: next.flushed_seq,
            ssts: next.ssts.len() as u64,
        });
        switch_current(&self.env, dir, next.number).await?;
        let mut tables = lock(&self.tables);
        tables.ssts.push((meta, Arc::new(reader)));
        tables.manifest = next;
        Ok(())
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
/// switch, release, then the log segments the table made redundant. On an I/O error
/// it stops; reads keep working from the memtables it left.
async fn flusher<E: Environment>(shared: Arc<Shared<E>>) {
    while let Some(memtable) = NextImmutable(&shared).await {
        let flushed = async {
            let (meta, reader) = shared.write_sst(&memtable).await?;
            if shared.config.variant == Variant::ReleaseBeforeManifest {
                // The bug: gone from the memtables before any manifest names the table.
                shared.release(&memtable);
                shared.commit_manifest(meta, reader).await?;
            } else {
                shared.commit_manifest(meta, reader).await?;
                shared.release(&memtable);
            }
            shared.wal.delete_segments_through(meta.max_seq).await?;
            Ok::<(), io::Error>(())
        };
        if flushed.await.is_err() {
            return;
        }
    }
}
