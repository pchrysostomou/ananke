//! The write-ahead log (SPEC.md §2.2, D-018): append-only, segmented, group committed.
//!
//! A record on disk is `len: u32 LE | header crc32c: u32 LE | crc32c: u32 LE | seq:
//! u64 LE | payload`: the header checksum covers the length and the sequence number,
//! the record checksum covers those and the payload. The header has its own so that
//! a flipped bit in the length reads as a bad checksum and not as a record torn at
//! the end of the file, which is what a length that runs past the end would
//! otherwise look like; a torn record is one whose bytes are missing, and only a
//! write in flight at a crash leaves one (D-027). Records go to
//! numbered segment files (`000001.wal`, `000002.wal`, ...) in a directory; a segment
//! is rotated before a group that would start at or beyond
//! [`WalConfig::segment_bytes`]. The sequence number is there for recovery: a sync the
//! disk lied about can leave a segment shorter than the one after it, and without
//! the number the hole would read as a valid log (D-019).
//!
//! Every [`Wal`] has one writer task, spawned through the [`Environment`], and
//! [`append`](Wal::append) only enqueues. The writer takes everything queued as one
//! group, writes it with one `write_at`, syncs once, and acknowledges every record in
//! the group: that is group commit, and it is where every appender waiting on the same
//! fsync shares it.
//!
//! [`open`](Wal::open) recovers first: it reads the segments in order and stops at the
//! first torn record, bad checksum, or gap in the numbering (which is how a missing
//! segment shows: the segment numbers themselves may have holes, since a discarded
//! segment's number is never reused), unless
//! the record it would stop at is numbered below [`WalConfig::expected_head`], which
//! the caller holds elsewhere: then the rest of that segment is skipped and reading
//! goes on with the next. Everything after a stop is
//! discarded, as the SPEC says: the stopping segment is cut to its last good record,
//! later segments are removed, and a fresh segment is started, numbered past every
//! segment the directory held so that a segment number is never reused. A first
//! record numbered past the expected head is a missing head: the open is refused, or
//! under [`HeadGapPolicy::Discard`] the whole log is discarded, since replaying past
//! the gap would produce a state that never existed (D-022).
//!
//! The [`Variant`] enum carries the correct log and three with known bugs. The crash
//! sweep in `sim/wal.rs` must pass the first and catch each of the others; that pair is
//! what shows the fault model works (CLAUDE.md). Production uses the default,
//! [`Variant::Correct`].

use std::collections::{BTreeMap, VecDeque};
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

/// The bytes before the payload: `len: u32 LE | header crc32c: u32 LE | crc32c: u32
/// LE | seq: u64 LE`.
pub const HEADER_LEN: usize = 20;

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

/// What to do when the log's head is missing: its first record is numbered past
/// [`WalConfig::expected_head`], so the records between are gone with the segments
/// that held them. Replaying past the gap would produce a state that never existed
/// (D-022), so neither choice does; they differ in what becomes of the tail.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HeadGapPolicy {
    /// Open fails with a [`HeadGap`] error and the files are left as they are, for
    /// someone to look at.
    #[default]
    Refuse,
    /// The whole log is discarded, every segment removed: what the caller holds
    /// elsewhere is the state, and a fresh segment starts at the expected head.
    Discard,
}

/// The error [`Wal::open`] fails with under [`HeadGapPolicy::Refuse`]: the first
/// record found is `found`, the caller expected `expected`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadGap {
    /// The sequence number the caller expected the log to start at.
    pub expected: Seq,
    /// The one the first record carried.
    pub found: Seq,
}

impl HeadGap {
    /// The head gap an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<HeadGap> {
        error.get_ref()?.downcast_ref::<HeadGap>().copied()
    }
}

impl std::fmt::Display for HeadGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the log's head is missing: expected record {}, found {}",
            self.expected, self.found
        )
    }
}

impl std::error::Error for HeadGap {}

/// How to open a [`Wal`].
#[derive(Clone, Debug)]
pub struct WalConfig {
    /// The directory holding the segments; created if missing.
    pub dir: PathBuf,
    /// A segment is rotated before a group that would start at or beyond this size.
    pub segment_bytes: u64,
    /// Which log to run; [`Variant::Correct`] outside a fault-model test.
    pub variant: Variant,
    /// The highest sequence number the oldest record may carry: 1 for a log nothing
    /// was ever deleted from; one past the manifest's `flushed_seq` for a log behind
    /// SSTables, whose segments are deleted once their records are flushed (D-022).
    /// An older record is fine, it is skipped by the engine; a newer one means the
    /// log's head is missing, which [`head_gap`](Self::head_gap) decides.
    pub expected_head: Seq,
    /// What to do about a missing head.
    pub head_gap: HeadGapPolicy,
    /// Fail the open with [`LogDamaged`], touching nothing on disk, when reading
    /// stopped at a bad checksum or a gap, or skipped a corrupt record in a segment
    /// below the expected head: records past the damage, acknowledged or not, are
    /// gone, and a caller that promised them cannot go on (D-027). Off, the log is
    /// cut at the stop as the SPEC says. A torn record is cut either way: only a
    /// write in flight at a crash leaves one, and it was never acknowledged.
    pub refuse_damage: bool,
}

/// The error [`Wal::open`] fails with under [`WalConfig::refuse_damage`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogDamaged {
    /// Where reading stopped, at a bad checksum or a gap.
    pub stop: Option<WalStop>,
    /// Corrupt records skipped in segments below the expected head.
    pub covered: Vec<WalStop>,
}

impl LogDamaged {
    /// The damage an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<LogDamaged> {
        error.get_ref()?.downcast_ref::<LogDamaged>().cloned()
    }
}

impl std::fmt::Display for LogDamaged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the log is damaged:")?;
        if let Some(stop) = &self.stop {
            write!(
                f,
                " reading stopped at segment {} offset {} ({})",
                stop.segment,
                stop.offset,
                stop.reason.as_str()
            )?;
        }
        for stop in &self.covered {
            write!(
                f,
                " a record at segment {} offset {} ({}) was skipped and the rest of the segment with it",
                stop.segment,
                stop.offset,
                stop.reason.as_str()
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for LogDamaged {}

/// The name of segment `n`: six digits and `.wal`, so a listing sorts in log order.
#[must_use]
pub fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{segment:06}.wal"))
}

/// The segment number a path names, if it names a segment.
#[must_use]
pub fn segment_of(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_suffix(".wal")?
        .parse()
        .ok()
}

/// Appends one encoded record to `out`.
pub fn encode_record(out: &mut Vec<u8>, seq: Seq, payload: &[u8]) {
    let len = u32::try_from(payload.len()).expect("record payload exceeds u32");
    let len_bytes = len.to_le_bytes();
    let seq_bytes = seq.to_le_bytes();
    let mut header = crc32c::Hasher::new();
    header.update(&len_bytes);
    header.update(&seq_bytes);
    let mut hasher = crc32c::Hasher::new();
    hasher.update(&len_bytes);
    hasher.update(&seq_bytes);
    hasher.update(payload);
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(&header.finish().to_le_bytes());
    out.extend_from_slice(&hasher.finish().to_le_bytes());
    out.extend_from_slice(&seq_bytes);
    out.extend_from_slice(payload);
}

/// A stop recovery skipped because the record it would have stopped at is numbered
/// below the expected head: the rest of that segment is gone, reading resumed with
/// the next segment's first record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoveredStop {
    /// Where and why.
    pub stop: WalStop,
    /// The sequence number the stopped-at record would have had; unknown when no
    /// record of the log had been read yet, that is, when the stop is the first
    /// retained segment's first record.
    pub from: Option<Seq>,
    /// The first record read after the skip, if any followed.
    pub resumed: Option<Seq>,
}

/// What [`Wal::open`] found on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recovery {
    /// The sequence number of the first record; `next_seq` when there are none.
    pub first_seq: Seq,
    /// Set when the first record's number was past `WalConfig::expected_head`: records
    /// from the expected head up to it were gone with the segments that held them,
    /// and under [`HeadGapPolicy::Discard`] the whole log went with them: `records`
    /// is empty and `stop` is the gap at that first record.
    pub head_gap: Option<(Seq, Seq)>,
    /// Stops at records the caller holds elsewhere, skipped rather than stopped at.
    pub covered_stops: Vec<CoveredStop>,
    /// The records, in order from the first segment up to the stop.
    pub records: Vec<Bytes>,
    /// Where reading stopped short of the end of the last segment, if it did.
    pub stop: Option<WalStop>,
    /// Segments after the stop that were removed.
    pub discarded: u64,
    /// The sequence number the next append gets: one past the last recovered record,
    /// or `WalConfig::expected_head` if that is higher.
    pub next_seq: Seq,
}

/// Parses one segment's bytes, expecting the records to continue the numbering from
/// `first_seq + records.len()`; the log's first record sets `first_seq`. A jump
/// forward that lands at or below `expected_head` skips only records held elsewhere,
/// so the records before it are dropped and the numbering restarts there rather than
/// stopping. `verify` false is the [`Variant::NoChecksum`] bug.
fn parse_segment(
    bytes: &[u8],
    verify: bool,
    expected_head: Seq,
    first_seq: &mut Option<Seq>,
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
        let header_crc = u32::from_le_bytes(rest[4..8].try_into().expect("4 bytes"));
        let crc = u32::from_le_bytes(rest[8..12].try_into().expect("4 bytes"));
        let seq = u64::from_le_bytes(rest[12..20].try_into().expect("8 bytes"));
        if verify {
            let mut header = crc32c::Hasher::new();
            header.update(&rest[..4]);
            header.update(&rest[12..20]);
            if header.finish() != header_crc {
                return Err((at, WalStopReason::BadChecksum));
            }
        }
        let Some(payload) = rest.get(HEADER_LEN..HEADER_LEN + len) else {
            return Err((at, WalStopReason::TornRecord));
        };
        if verify {
            let mut hasher = crc32c::Hasher::new();
            hasher.update(&rest[..4]);
            hasher.update(&rest[12..20]);
            hasher.update(payload);
            if hasher.finish() != crc {
                return Err((at, WalStopReason::BadChecksum));
            }
        }
        match *first_seq {
            None => *first_seq = Some(seq),
            Some(first) => {
                let expected = first + records.len() as u64;
                if seq > expected && seq <= expected_head {
                    // Everything skipped is below the head the caller holds elsewhere.
                    records.clear();
                    *first_seq = Some(seq);
                } else if seq != expected {
                    return Err((
                        at,
                        WalStopReason::Gap {
                            expected,
                            found: seq,
                        },
                    ));
                }
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
    /// Whether the record asks for a sync before it is acknowledged.
    sync: bool,
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
    /// Every segment on disk and the sequence number its first record has, or will
    /// have; what [`Wal::delete_segments_through`] decides from.
    segments: Mutex<BTreeMap<u64, Seq>>,
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
        let mut first_seq = None;
        let mut firsts: BTreeMap<u64, Seq> = BTreeMap::new();
        let mut stop = None;
        let mut last_good = 0;
        // A stop at a record the caller holds elsewhere (numbered below the expected
        // head) loses nothing: the rest of that segment is skipped and reading goes on
        // with the next one, whose first record then sets the numbering again.
        let head = config.expected_head;
        let covered = |first_seq: Option<Seq>, len: usize| {
            first_seq.map_or(head > 1, |first| (first + len as u64) < head)
        };
        let mut covered_stops: Vec<CoveredStop> = Vec::new();
        // Segment numbers may have holes, where a discarded segment's number was
        // never reused; it is the records' numbering that must be whole. A segment
        // that went missing with records in it shows as a gap at the next segment's
        // first record.
        // The segment the log's first record was read from, for a head gap.
        let mut first_segment = None;
        for &segment in &segments {
            let path = segment_path(&config.dir, segment);
            let file = fs.open(&path, OpenOptions::new().read(true)).await?;
            let size = file.size().await?;
            let bytes = file
                .read_at(0, usize::try_from(size).unwrap_or(usize::MAX))
                .await?;
            let before = records.len() as u64;
            let unread = first_seq.is_none();
            let parsed = parse_segment(
                &bytes,
                verify,
                config.expected_head,
                &mut first_seq,
                &mut records,
            );
            // The segment's first number: its first record's, or the next number if
            // it is empty; unknown until the log's first record is seen.
            if let Some(first) = first_seq {
                if unread {
                    first_segment = Some(segment);
                }
                firsts.insert(segment, first + before);
                if let Some(skip) = covered_stops.last_mut()
                    && skip.resumed.is_none()
                {
                    skip.resumed = Some(first + before);
                }
            }
            if let Err((offset, reason)) = parsed {
                if covered(first_seq, records.len()) {
                    covered_stops.push(CoveredStop {
                        stop: WalStop {
                            segment,
                            offset,
                            reason,
                        },
                        from: first_seq.map(|f| f + records.len() as u64),
                        resumed: None,
                    });
                    records.clear();
                    first_seq = None;
                    last_good = segment;
                    continue;
                }
                stop = Some(WalStop {
                    segment,
                    offset,
                    reason,
                });
                break;
            }
            last_good = segment;
        }
        // A missing head: the records from the expected head to the first one found
        // are gone, and replaying past them would produce a state that never existed.
        // Refused, nothing on disk is touched; discarded, the stop is the first record
        // and the whole log goes with it.
        let head_gap = first_seq
            .filter(|&first| first > config.expected_head)
            .map(|first| (config.expected_head, first));
        if let Some((expected, found)) = head_gap {
            let discard = config.head_gap == HeadGapPolicy::Discard;
            env.trace(TraceEvent::HeadGap {
                expected,
                found,
                discarded: discard,
            });
            if !discard {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    HeadGap { expected, found },
                ));
            }
            stop = Some(WalStop {
                segment: first_segment.expect("a first record has a segment"),
                offset: 0,
                reason: WalStopReason::Gap { expected, found },
            });
            records.clear();
            first_seq = None;
        }

        // Damage the caller will not live with: refused before anything is cut, so
        // the next open sees the same damage and refuses again, rather than a log
        // that was quietly shortened once (D-027).
        if config.refuse_damage {
            let damaged_stop = stop.filter(|s| {
                matches!(
                    s.reason,
                    WalStopReason::BadChecksum | WalStopReason::Gap { .. }
                )
            });
            if damaged_stop.is_some() || !covered_stops.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    LogDamaged {
                        stop: damaged_stop,
                        covered: covered_stops.iter().map(|c| c.stop).collect(),
                    },
                ));
            }
        }

        // Everything after the stop is discarded: the stopping segment is cut to its
        // last good record, and later segments are removed. A missing head discards
        // every segment by removal, never by a cut to nothing: a cut whose sync the
        // disk lied about brings the old records back at the next crash, in front of
        // the new ones and numbered as if they were current (the sweep found this at
        // seed 191), whereas a removal is durable once the directory is synced.
        let mut discarded = 0;
        if let Some(stop) = stop {
            if head_gap.is_some() {
                for &segment in &segments {
                    fs.remove_file(&segment_path(&config.dir, segment)).await?;
                    discarded += 1;
                }
                // Nothing read before the gap was detected is on disk any more.
                firsts.clear();
                last_good = 0;
            } else {
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
                for &segment in segments.iter().filter(|&&s| s > last_good) {
                    fs.remove_file(&segment_path(&config.dir, segment)).await?;
                    discarded += 1;
                }
            }
            fs.sync_dir(&config.dir).await?;
        }
        let first_seq = first_seq.unwrap_or(config.expected_head);
        // The next number follows the last record recovered, and never reuses a
        // number the caller says is already held elsewhere (below the expected head).
        let next_seq = (first_seq + records.len() as u64).max(config.expected_head);
        env.trace(TraceEvent::WalRecovered {
            records: records.len() as u64,
            stop,
            discarded,
        });

        // A fresh segment, numbered past every segment the directory held, discarded
        // ones included: a segment number is never reused, so a number names one file
        // for the life of the log and the trace can tell them apart. Synced into the
        // directory: rotation may skip that in the NoSyncDir variant, opening never
        // does.
        let segment = segments.last().copied().unwrap_or(0).max(last_good) + 1;
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
        // Segments that were cut or emptied, or preceded the first record, count from
        // the first sequence number they could hold.
        for &s in segments.iter().filter(|&&s| s <= last_good) {
            firsts.entry(s).or_insert(first_seq);
        }
        firsts.insert(segment, next_seq);

        let shared = Arc::new(Shared {
            env: env.clone(),
            config,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                next_seq,
                writer: None,
                closed: false,
            }),
            segments: Mutex::new(firsts),
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
                first_seq,
                head_gap,
                covered_stops,
                records,
                stop,
                discarded,
                next_seq,
            },
        ))
    }

    /// Deletes every segment whose records are all numbered `seq` or below, never
    /// the one being written, and syncs the directory. Returns the segments deleted.
    /// Call it only once the records are durable elsewhere (D-022).
    ///
    /// # Errors
    ///
    /// The filesystem's.
    pub async fn delete_segments_through(&self, seq: Seq) -> io::Result<Vec<u64>> {
        let deletable: Vec<u64> = {
            let segments = lock(&self.shared.segments);
            let ids: Vec<(u64, Seq)> = segments.iter().map(|(&n, &first)| (n, first)).collect();
            ids.windows(2)
                .filter(|pair| pair[1].1 <= seq + 1)
                .map(|pair| pair[0].0)
                .collect()
        };
        let fs = self.shared.env.fs();
        for &segment in &deletable {
            fs.remove_file(&segment_path(&self.shared.config.dir, segment))
                .await?;
            lock(&self.shared.segments).remove(&segment);
            self.shared
                .env
                .trace(TraceEvent::WalSegmentDeleted { segment });
        }
        if !deletable.is_empty() {
            fs.sync_dir(&self.shared.config.dir).await?;
        }
        Ok(deletable)
    }

    /// The segments on disk, oldest first.
    #[must_use]
    pub fn segments(&self) -> Vec<u64> {
        lock(&self.shared.segments).keys().copied().collect()
    }

    /// Enqueues `payload` and returns the future that resolves once the record is
    /// durable, with its sequence number. The number is assigned now:
    /// [`Append::seq`] knows it before the record is written.
    pub fn append(&self, payload: Bytes) -> Append {
        self.append_with(payload, true)
    }

    /// Like [`append`](Self::append), but with `sync` off the record is acknowledged
    /// once written, without a sync forced for it: the next group that asks for one,
    /// or the next rotation or close, makes it durable (D-024). A crash before then
    /// loses it, acknowledged or not.
    pub fn append_with(&self, payload: Bytes, sync: bool) -> Append {
        let slot = Arc::new(Slot::default());
        let (seq, waker) = {
            let mut st = lock(&self.shared.state);
            let seq = st.next_seq;
            st.next_seq += 1;
            st.queue.push_back(Queued {
                seq,
                payload,
                slot: slot.clone(),
                sync,
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
                // Closed: what was acknowledged without a sync gets one now.
                if self.unsynced {
                    let _ = self.sync().await;
                }
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
            encode_record(&mut buf, queued.seq, &queued.payload);
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
                // A group nobody asked a sync for is acknowledged as written; the
                // next synced group, rotation or close covers it.
                if group.iter().any(|q| q.sync) {
                    self.sync().await?;
                } else {
                    self.unsynced = true;
                }
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
        lock(&self.shared.segments).insert(segment, first);
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
