//! The write-ahead log (SPEC.md §2.2, D-018): append-only, segmented, group committed.
//!
//! A record on disk is `len: u32 LE | crc32c: u32 LE | payload`, the checksum covering
//! the length bytes and the payload. Records go to numbered segment files
//! (`000001.wal`, `000002.wal`, ...) in a directory; a segment is rotated before a
//! group that would start at or beyond [`WalConfig::segment_bytes`].
//!
//! Every [`Wal`] has one writer task, spawned through the [`Environment`], and
//! [`append`](Wal::append) only enqueues. The writer takes everything queued as one
//! group, writes it with one `write_at`, syncs once, and acknowledges every record in
//! the group: that is group commit, and it is where every appender waiting on the same
//! fsync shares it.
//!
//! [`open`](Wal::open) recovers first: it reads the segments in order and stops at the
//! first torn record, bad checksum, or missing segment. Everything after the stop is
//! discarded, as the SPEC says: the stopping segment is cut to its last good record,
//! later segments are removed, and a fresh segment is started.
//!
//! The [`Variant`] enum carries the correct log and three with known bugs. The crash
//! sweep in `sim/wal.rs` must pass the first and catch each of the others; that pair is
//! what shows the fault model works (CLAUDE.md). Production uses the default,
//! [`Variant::Correct`].

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ananke_env::{
    Clock, Environment, File, FileSystem, Instant, OpenOptions, TraceEvent, WalStop, WalStopReason,
};
use bytes::Bytes;

use crate::crc32c;

/// A record's position in the log, counting from 1. Position, not identity: after a
/// recovery that discarded records, the next append reuses the discarded numbers.
pub type Seq = u64;

/// The bytes before the payload: `len: u32 LE | crc32c: u32 LE`.
pub const HEADER_LEN: usize = 8;

/// The log to run: the correct one, or one of the known bugs the crash sweep must
/// catch. The sweep passing the correct log and failing each buggy one is what shows
/// the fault model can tell them apart; either half alone shows nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Variant {
    /// What ships: every group synced before it is acknowledged, every new segment
    /// synced into its directory, every record verified at recovery.
    #[default]
    Correct,
    /// Rotation creates the next segment without `sync_dir`, so a crash can lose the
    /// directory entry and with it every record the segment held, acknowledged or not.
    NoSyncDir,
    /// Recovery trusts the length and never verifies the checksum, so bit rot comes
    /// back as data.
    NoChecksum,
    /// A group is acknowledged as soon as it is written; the file is synced only when
    /// a later group finds that at least `interval` has passed since the last sync,
    /// as in "fsync every second" designs. A crash inside the window loses records
    /// that were acknowledged.
    AckBeforeSync {
        /// How long acknowledged records may sit unsynced.
        interval: Duration,
    },
}

/// How to open a [`Wal`].
#[derive(Clone, Debug)]
pub struct WalConfig {
    /// The directory holding the segments; created if missing.
    pub dir: PathBuf,
    /// A segment is rotated before a group that would start at or beyond this size.
    pub segment_bytes: u64,
    /// Which log to run; [`Variant::Correct`] outside a fault-model test.
    pub variant: Variant,
}

/// The name of segment `n`: six digits and `.wal`, so a listing sorts in log order.
#[must_use]
pub fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{segment:06}.wal"))
}

/// The segment number a file name carries, if it is a segment.
fn segment_of(name: &Path) -> Option<u64> {
    name.to_str()?.strip_suffix(".wal")?.parse().ok()
}

/// Appends one encoded record to `out`.
pub fn encode_record(out: &mut Vec<u8>, payload: &[u8]) {
    let len = u32::try_from(payload.len()).expect("record payload exceeds u32");
    let len_bytes = len.to_le_bytes();
    let mut hasher = crc32c::Hasher::new();
    hasher.update(&len_bytes);
    hasher.update(payload);
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(&hasher.finish().to_le_bytes());
    out.extend_from_slice(payload);
}

/// What [`Wal::open`] found on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// The records, in order from the first segment up to the stop.
    pub records: Vec<Bytes>,
    /// Where reading stopped short of the end of the last segment, if it did.
    pub stop: Option<WalStop>,
    /// Segments after the stop that were removed.
    pub discarded: u64,
    /// The sequence number the next append gets: one past the last recovered record.
    pub next_seq: Seq,
}

/// Parses one segment's bytes; `verify` false is the [`Variant::NoChecksum`] bug.
fn parse_segment(
    bytes: &[u8],
    verify: bool,
    records: &mut Vec<Bytes>,
) -> Result<(), (u64, WalStopReason)> {
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = &bytes[offset..];
        let at = offset as u64;
        if rest.len() < HEADER_LEN {
            return Err((at, WalStopReason::TornRecord));
        }
        let len = u32::from_le_bytes(rest[..4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(rest[4..8].try_into().expect("4 bytes"));
        let Some(payload) = rest.get(HEADER_LEN..HEADER_LEN + len) else {
            return Err((at, WalStopReason::TornRecord));
        };
        if verify {
            let mut hasher = crc32c::Hasher::new();
            hasher.update(&rest[..4]);
            hasher.update(payload);
            if hasher.finish() != crc {
                return Err((at, WalStopReason::BadChecksum));
            }
        }
        records.push(Bytes::copy_from_slice(payload));
        offset += HEADER_LEN + len;
    }
    Ok(())
}

/// A record the writer has not taken yet.
struct Queued {
    seq: Seq,
    payload: Bytes,
    slot: Arc<Slot>,
}

/// Where the writer leaves a record's outcome for its [`Append`] future.
#[derive(Default)]
struct Slot {
    state: Mutex<SlotState>,
}

#[derive(Default)]
struct SlotState {
    done: Option<io::Result<()>>,
    waker: Option<Waker>,
}

impl Slot {
    fn resolve(&self, outcome: io::Result<()>) {
        let waker = {
            let mut st = lock(&self.state);
            st.done = Some(outcome);
            st.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Shared between the handle and the writer task.
struct Shared<E: Environment> {
    env: E,
    config: WalConfig,
    state: Mutex<State>,
}

struct State {
    queue: VecDeque<Queued>,
    next_seq: Seq,
    writer: Option<Waker>,
    closed: bool,
}

/// An open write-ahead log. Dropping it closes the log: the writer finishes what is
/// queued and exits.
pub struct Wal<E: Environment> {
    shared: Arc<Shared<E>>,
}

impl<E: Environment> std::fmt::Debug for Wal<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("dir", &self.shared.config.dir)
            .finish_non_exhaustive()
    }
}

impl<E: Environment> Wal<E> {
    /// Recovers what is in `config.dir`, starts a fresh segment and the writer task,
    /// and returns the log with what recovery found.
    ///
    /// # Errors
    ///
    /// Any I/O error from listing, reading, cutting or creating segments.
    pub async fn open(env: E, config: WalConfig) -> io::Result<(Self, Recovery)> {
        let fs = env.fs();
        fs.create_dir_all(&config.dir).await?;
        let mut segments: Vec<u64> = fs
            .read_dir(&config.dir)
            .await?
            .iter()
            .filter_map(|name| segment_of(name))
            .collect();
        segments.sort_unstable();

        let verify = config.variant != Variant::NoChecksum;
        let mut records = Vec::new();
        let mut stop = None;
        let mut last_good = 0;
        let mut expected = segments.first().copied().unwrap_or(1);
        for &segment in &segments {
            if segment != expected {
                stop = Some(WalStop {
                    segment: expected,
                    offset: 0,
                    reason: WalStopReason::MissingSegment,
                });
                break;
            }
            let path = segment_path(&config.dir, segment);
            let file = fs.open(&path, OpenOptions::new().read(true)).await?;
            let size = file.size().await?;
            let bytes = file
                .read_at(0, usize::try_from(size).unwrap_or(usize::MAX))
                .await?;
            if let Err((offset, reason)) = parse_segment(&bytes, verify, &mut records) {
                stop = Some(WalStop {
                    segment,
                    offset,
                    reason,
                });
                break;
            }
            last_good = segment;
            expected = segment + 1;
        }

        // Everything after the stop is discarded: the stopping segment is cut to its
        // last good record, and later segments are removed.
        let mut discarded = 0;
        if let Some(stop) = stop {
            if stop.reason != WalStopReason::MissingSegment {
                let path = segment_path(&config.dir, stop.segment);
                let file = fs
                    .open(&path, OpenOptions::new().read(true).write(true))
                    .await?;
                file.set_size(stop.offset).await?;
                file.sync().await?;
                env.trace(TraceEvent::WalTruncated {
                    segment: stop.segment,
                    len: stop.offset,
                });
                last_good = stop.segment;
            }
            for &segment in segments.iter().filter(|&&s| s > last_good) {
                fs.remove_file(&segment_path(&config.dir, segment)).await?;
                discarded += 1;
            }
            fs.sync_dir(&config.dir).await?;
        }
        let next_seq = records.len() as u64 + 1;
        env.trace(TraceEvent::WalRecovered {
            records: records.len() as u64,
            stop,
            discarded,
        });

        // A fresh segment, synced into the directory: rotation may skip that in the
        // NoSyncDir variant, opening never does.
        let segment = last_good + 1;
        let file = fs
            .open(
                &segment_path(&config.dir, segment),
                OpenOptions::new().write(true).create_new(true),
            )
            .await?;
        fs.sync_dir(&config.dir).await?;
        env.trace(TraceEvent::WalSegmentOpened {
            segment,
            first: next_seq,
        });

        let shared = Arc::new(Shared {
            env: env.clone(),
            config,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                next_seq,
                writer: None,
                closed: false,
            }),
        });
        let writer = Writer {
            shared: shared.clone(),
            file,
            segment,
            segment_first: next_seq,
            written_up_to: next_seq - 1,
            size: 0,
            last_sync: env.clock().now(),
            unsynced: false,
        };
        env.spawn("wal-writer", writer.run());
        Ok((
            Self { shared },
            Recovery {
                records,
                stop,
                discarded,
                next_seq,
            },
        ))
    }

    /// Enqueues `payload` and returns the future that resolves once the record is
    /// durable, with its sequence number. The number is assigned now:
    /// [`Append::seq`] knows it before the record is written.
    pub fn append(&self, payload: Bytes) -> Append {
        let slot = Arc::new(Slot::default());
        let (seq, waker) = {
            let mut st = lock(&self.shared.state);
            let seq = st.next_seq;
            st.next_seq += 1;
            st.queue.push_back(Queued {
                seq,
                payload,
                slot: slot.clone(),
            });
            (seq, st.writer.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Append { seq, slot }
    }

    /// Where the segments live.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.shared.config.dir
    }
}

impl<E: Environment> Drop for Wal<E> {
    fn drop(&mut self) {
        let waker = {
            let mut st = lock(&self.shared.state);
            st.closed = true;
            st.writer.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A record on its way to disk; resolves once it is durable.
pub struct Append {
    seq: Seq,
    slot: Arc<Slot>,
}

impl Append {
    /// The record's sequence number, known before it is written.
    #[must_use]
    pub fn seq(&self) -> Seq {
        self.seq
    }
}

impl Future for Append {
    type Output = io::Result<Seq>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<Seq>> {
        let mut st = lock(&self.slot.state);
        match st.done.take() {
            Some(outcome) => Poll::Ready(outcome.map(|()| self.seq)),
            None => {
                st.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Resolves with everything queued, or with nothing once the log is closed and empty.
struct NextGroup<'a>(&'a Mutex<State>);

impl Future for NextGroup<'_> {
    type Output = Vec<Queued>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<Queued>> {
        let mut st = lock(self.0);
        if !st.queue.is_empty() {
            return Poll::Ready(st.queue.drain(..).collect());
        }
        if st.closed {
            return Poll::Ready(Vec::new());
        }
        st.writer = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// The one task that touches the segment files after recovery.
struct Writer<E: Environment> {
    shared: Arc<Shared<E>>,
    file: <E::Fs as FileSystem>::File,
    segment: u64,
    /// The first sequence number in the current segment.
    segment_first: Seq,
    /// The last sequence number written to the current segment.
    written_up_to: Seq,
    /// The current segment's size.
    size: u64,
    last_sync: Instant,
    /// Whether records were written since the last sync (only ever true in the
    /// `AckBeforeSync` variant).
    unsynced: bool,
}

impl<E: Environment> Writer<E> {
    async fn run(mut self) {
        loop {
            let group = NextGroup(&self.shared.state).await;
            if group.is_empty() {
                return;
            }
            if let Err(e) = self.commit(&group).await {
                for queued in &group {
                    queued
                        .slot
                        .resolve(Err(io::Error::new(e.kind(), e.to_string())));
                }
            }
        }
    }

    /// One group: rotate if the segment is full, write, sync, acknowledge.
    async fn commit(&mut self, group: &[Queued]) -> io::Result<()> {
        if self.size >= self.shared.config.segment_bytes {
            self.rotate().await?;
        }
        let mut buf = Vec::new();
        for queued in group {
            encode_record(&mut buf, &queued.payload);
        }
        let len = buf.len() as u64;
        self.file.write_at(self.size, Bytes::from(buf)).await?;
        self.size += len;
        self.written_up_to = group.last().expect("non-empty group").seq;
        match self.shared.config.variant {
            Variant::AckBeforeSync { interval } => {
                for queued in group {
                    queued.slot.resolve(Ok(()));
                }
                self.unsynced = true;
                if self.shared.env.clock().now() - self.last_sync >= interval {
                    self.sync().await?;
                }
            }
            _ => {
                self.sync().await?;
                for queued in group {
                    queued.slot.resolve(Ok(()));
                }
            }
        }
        Ok(())
    }

    async fn sync(&mut self) -> io::Result<()> {
        self.file.sync().await?;
        self.shared.env.trace(TraceEvent::WalSynced {
            segment: self.segment,
            first: self.segment_first,
            up_to: self.written_up_to,
        });
        self.last_sync = self.shared.env.clock().now();
        self.unsynced = false;
        Ok(())
    }

    /// Starts the next segment. Its directory entry is synced unless the log is the
    /// `NoSyncDir` variant, whose bug this is.
    async fn rotate(&mut self) -> io::Result<()> {
        if self.unsynced {
            self.sync().await?;
        }
        let segment = self.segment + 1;
        let fs = self.shared.env.fs();
        let file = fs
            .open(
                &segment_path(&self.shared.config.dir, segment),
                OpenOptions::new().write(true).create_new(true),
            )
            .await?;
        if self.shared.config.variant != Variant::NoSyncDir {
            fs.sync_dir(&self.shared.config.dir).await?;
        }
        let first = self.written_up_to + 1;
        self.shared
            .env
            .trace(TraceEvent::WalSegmentOpened { segment, first });
        self.file = file;
        self.segment = segment;
        self.segment_first = first;
        self.size = 0;
        Ok(())
    }
}
