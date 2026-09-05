//! The engine so far (SPEC.md §2, D-020): a write-ahead log in front of a memtable.
//!
//! A write is one log record, `put` or `delete` with its key and value, appended
//! through the [`Wal`] and applied to the active [`Memtable`] once the log has
//! acknowledged it: nothing is visible before it is durable. When the active memtable
//! exceeds `memtable_bytes` it becomes immutable and a fresh one takes its place; a
//! flusher task hands each immutable memtable, oldest first, to the [`FlushSink`] and
//! releases it once the sink has it. Reads consult the active memtable, then the
//! immutable ones newest first, then the sink.
//!
//! The sink is where SSTables will go (SPEC §2.4). Until then [`Retain`] stands in:
//! it keeps flushed memtables in memory and answers reads from them, so the pipeline
//! and its crash behaviour are what they will be, and nothing is lost. The log is
//! never truncated yet, so recovery replays every record into fresh memtables.
//!
//! The [`Variant`] pair for the crash sweep: [`Variant::Correct`], and
//! [`Variant::NoWalBeforeMemtable`], which applies a write and acknowledges it
//! before the log has, the bug the sweep must catch.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use ananke_env::{Environment, TraceEvent};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::memtable::{Memtable, Value};
use crate::wal::{self, Append, Recovery, Seq, Wal, WalConfig};

/// Which engine to run: the correct one, or the known bug the crash sweep must catch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Variant {
    /// A write is applied and acknowledged only after the log has made it durable.
    #[default]
    Correct,
    /// A write is applied to the memtable and acknowledged at once; the log record is
    /// queued but nobody waits for it. A crash loses acknowledged writes.
    NoWalBeforeMemtable,
}

/// How to open an [`Engine`].
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// The directory holding the log (and, later, the tables); created if missing.
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

/// Where flushed memtables go and how they are read back. SSTables, once they exist;
/// [`Retain`] until then.
pub trait FlushSink: Send + Sync + 'static {
    /// Makes `memtable` readable from the sink. Once this returns, the engine drops
    /// its own reference and reads for its keys reach the sink.
    fn flush(&self, memtable: Arc<Memtable>) -> impl Future<Output = io::Result<()>> + Send;

    /// The newest write to `key` the sink holds, if any.
    fn get(&self, key: &[u8]) -> impl Future<Output = io::Result<Option<Value>>> + Send;
}

/// The stand-in sink: flushed memtables stay in memory, newest last.
#[derive(Debug, Default)]
pub struct Retain {
    flushed: Mutex<Vec<Arc<Memtable>>>,
}

impl Retain {
    /// How many memtables were flushed into it.
    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.flushed).len()
    }

    /// Whether nothing was flushed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl FlushSink for Retain {
    fn flush(&self, memtable: Arc<Memtable>) -> impl Future<Output = io::Result<()>> + Send {
        lock(&self.flushed).push(memtable);
        std::future::ready(Ok(()))
    }

    fn get(&self, key: &[u8]) -> impl Future<Output = io::Result<Option<Value>>> + Send {
        let found = lock(&self.flushed)
            .iter()
            .rev()
            .find_map(|memtable| memtable.get(key));
        std::future::ready(Ok(found))
    }
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

/// The memtables: the active one and the immutable ones waiting to be flushed,
/// oldest first.
struct Tables {
    active: Arc<Memtable>,
    immutable: VecDeque<Arc<Memtable>>,
}

struct Flusher {
    waker: Option<Waker>,
    closed: bool,
}

struct Shared<E: Environment, S: FlushSink> {
    env: E,
    config: EngineConfig,
    wal: Wal<E>,
    sink: Arc<S>,
    tables: Mutex<Tables>,
    flusher: Mutex<Flusher>,
    next_memtable: AtomicU64,
}

/// What [`Engine::open`] found.
#[derive(Clone, Debug)]
pub struct EngineRecovery {
    /// What the log recovered.
    pub wal: Recovery,
    /// Records replayed into memtables: every recovered record.
    pub replayed: usize,
}

/// A write-ahead log in front of memtables. Dropping it closes the log and stops the
/// flusher once the queue is empty.
pub struct Engine<E: Environment, S: FlushSink> {
    shared: Arc<Shared<E, S>>,
}

impl<E: Environment, S: FlushSink> std::fmt::Debug for Engine<E, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("dir", &self.shared.config.dir)
            .finish_non_exhaustive()
    }
}

impl<E: Environment, S: FlushSink> Engine<E, S> {
    /// Opens the log in `config.dir`, replays what it recovers into memtables, starts
    /// the flusher, and returns the engine with what recovery found.
    ///
    /// # Errors
    ///
    /// Any I/O error from the log, or `InvalidData` for a record that is not an op.
    pub async fn open(env: E, config: EngineConfig, sink: S) -> io::Result<(Self, EngineRecovery)> {
        let (wal, recovery) = Wal::open(
            env.clone(),
            WalConfig {
                dir: config.dir.clone(),
                segment_bytes: config.segment_bytes,
                variant: config.wal_variant,
            },
        )
        .await?;
        let shared = Arc::new(Shared {
            env: env.clone(),
            config,
            wal,
            sink: Arc::new(sink),
            tables: Mutex::new(Tables {
                active: Arc::new(Memtable::new(1)),
                immutable: VecDeque::new(),
            }),
            flusher: Mutex::new(Flusher {
                waker: None,
                closed: false,
            }),
            next_memtable: AtomicU64::new(2),
        });
        for (i, record) in recovery.records.iter().enumerate() {
            let (key, value) = decode_op(record.clone())?;
            shared.apply(i as u64 + 1, key, value);
        }
        env.spawn("flusher", flusher(shared.clone()));
        let replayed = recovery.records.len();
        Ok((
            Self { shared },
            EngineRecovery {
                wal: recovery,
                replayed,
            },
        ))
    }

    /// Writes `value` under `key`. The returned future resolves once the write is
    /// durable and visible, with its log sequence number.
    pub fn put(&self, key: Bytes, value: Bytes) -> Write<E, S> {
        self.write(key, Value::Live(value))
    }

    /// Deletes `key`, leaving a tombstone. Resolves like [`put`](Self::put).
    pub fn delete(&self, key: Bytes) -> Write<E, S> {
        self.write(key, Value::Tombstone)
    }

    fn write(&self, key: Bytes, value: Value) -> Write<E, S> {
        let append = self.shared.wal.append(encode_op(&key, &value));
        let pending = match self.shared.config.variant {
            Variant::Correct => Some((key, value)),
            Variant::NoWalBeforeMemtable => {
                // The bug: visible and acknowledged before the log has it.
                self.shared.apply(append.seq(), key, value);
                None
            }
        };
        Write {
            shared: self.shared.clone(),
            append,
            pending,
        }
    }

    /// The value under `key`, if it is present: the active memtable first, then the
    /// immutable ones newest first, then the sink.
    ///
    /// # Errors
    ///
    /// The sink's error.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Bytes>> {
        let (active, immutable) = {
            let tables = lock(&self.shared.tables);
            (tables.active.clone(), tables.immutable.clone())
        };
        if let Some(value) = active.get(key) {
            return Ok(value.live());
        }
        for memtable in immutable.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Ok(value.live());
            }
        }
        Ok(self.shared.sink.get(key).await?.and_then(Value::live))
    }

    /// The sink.
    #[must_use]
    pub fn sink(&self) -> &S {
        &self.shared.sink
    }

    /// Immutable memtables not yet flushed.
    #[must_use]
    pub fn immutable_memtables(&self) -> usize {
        lock(&self.shared.tables).immutable.len()
    }
}

impl<E: Environment, S: FlushSink> Drop for Engine<E, S> {
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

impl<E: Environment, S: FlushSink> Shared<E, S> {
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
}

/// A write on its way to the log; resolves with its sequence number once durable and
/// visible (or, in the buggy variant, at once).
pub struct Write<E: Environment, S: FlushSink> {
    shared: Arc<Shared<E, S>>,
    append: Append,
    /// What to apply on acknowledgement; `None` once applied.
    pending: Option<(Bytes, Value)>,
}

impl<E: Environment, S: FlushSink> Write<E, S> {
    /// The write's log sequence number, known before it is durable.
    #[must_use]
    pub fn seq(&self) -> Seq {
        self.append.seq()
    }
}

impl<E: Environment, S: FlushSink> Future for Write<E, S> {
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
        if let Some((key, value)) = self.pending.take() {
            self.shared.apply(seq, key, value);
        }
        Poll::Ready(Ok(seq))
    }
}

/// Resolves with the oldest immutable memtable, or with nothing once the engine is
/// closed and none are left.
struct NextImmutable<'a, E: Environment, S: FlushSink>(&'a Shared<E, S>);

impl<E: Environment, S: FlushSink> Future for NextImmutable<'_, E, S> {
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

/// The one task that hands immutable memtables to the sink, oldest first. On a sink
/// error it stops; reads keep working from the immutable memtables it left.
async fn flusher<E: Environment, S: FlushSink>(shared: Arc<Shared<E, S>>) {
    while let Some(memtable) = NextImmutable(&shared).await {
        if shared.sink.flush(memtable.clone()).await.is_err() {
            return;
        }
        {
            let mut tables = lock(&shared.tables);
            if tables
                .immutable
                .front()
                .is_some_and(|front| Arc::ptr_eq(front, &memtable))
            {
                tables.immutable.pop_front();
            }
        }
        shared.env.trace(TraceEvent::MemtableFlushed {
            memtable: memtable.id(),
            up_to: memtable.max_seq(),
        });
    }
}
