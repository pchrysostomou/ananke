//! Checkpoints as snapshots, chunked both ways (RAFT.md §1, §3).
//!
//! A snapshot is an [`Engine::checkpoint`](ananke_storage::Engine::checkpoint) of
//! the state machine's store at an applied index, with its identity, the index,
//! term and configuration, written into the live store's `0 / 3 / snapshot` key
//! *before* the checkpoint is taken, so the checkpoint's copy carries it before the
//! checkpoint's own `CURRENT` (D-024). [`take`] is that sequence; the `apply` task
//! runs it between applies, so the recorded index is exactly the index the
//! checkpoint captures — the apply task is the only writer of user state and it is
//! busy checkpointing (PROPOSED(D-036)).
//!
//! Streaming is `InstallSnapshot` chunks of at most `snapshot_chunk` bytes, each
//! naming its file, the offset the data starts at and the file's total size, files
//! in the order the checkpoint directory lists them. [`Sender`] is the leader's
//! bookkeeping: one chunk outstanding, the receiver's acknowledgement naming the
//! next byte it wants, so a resend after loss resumes from the last acknowledged
//! offset of the last file rather than from zero (RAFT.md §1). [`Assembler`] is the
//! receiver's: chunks land in the staging directory, a well-known path under the
//! server's data directory, and the streamed `CURRENT` is held aside in memory so
//! the staging directory is never a complete store while the stream runs.
//!
//! Installing is [`Assembler::finish`]: the staged tables verified with the
//! engine's own checks, then one repair table and a new manifest written with the
//! receiver's own hard state, the snapshot record, the applied index, the kept log
//! tail and tombstones for the leader's log keys — and only then the staged
//! `CURRENT`, named last the way every store switch is (D-024). A crash before
//! that `CURRENT` leaves an incomplete staging directory the next open sweeps
//! away; a crash after it leaves a complete install [`adopt_staged`] finishes.
//! [`Variant::SnapshotWithoutCurrentLast`] writes the streamed `CURRENT` into the
//! staging directory the moment it arrives instead: a crash mid-install then
//! leaves a complete-looking store carrying the *leader's* tenant 0, a state this
//! server never held, which state machine safety reports after the restart
//! (RAFT.md §5).
//!
//! [`adopt_staged`] is the switch, run at every server start before the engine
//! opens: a staging directory with a valid `CURRENT` wins over the old store. The
//! old store's `CURRENT` is removed first, then its files, then the staged files
//! are copied over with `CURRENT` again last, and only then the staging
//! directory's own `CURRENT` is removed — so a crash at any point either re-runs
//! the adoption or has already retired the staging directory, and the operation is
//! idempotent without directory renames, which the fault model does not have
//! (D-024, PROPOSED(D-038)).

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ananke_env::{Environment, File, FileSystem, OpenOptions};
use ananke_storage::manifest::{self, Manifest, SstMeta};
use ananke_storage::sst::{SstReader, SstWriter};
use ananke_storage::{Value, ikey};
use bytes::{Bytes, BytesMut};

use crate::core::Variant;
use crate::message::{Message, SnapshotStatus};
use crate::store::{self, RaftStore, SnapshotRecord};
use crate::types::{Configuration, Entry, Index, ServerId, Term};

/// The staging directory, a well-known path under the server's data directory
/// (RAFT.md §1): where chunks assemble, and what [`adopt_staged`] looks for.
#[must_use]
pub fn staging_dir(engine_dir: &Path) -> PathBuf {
    engine_dir.join("install")
}

/// The directory a checkpoint taken at `index` goes to, under the server's data
/// directory. Never removed once a stream may have read from it; the snapshot
/// record names the one in force.
#[must_use]
pub fn checkpoint_dir(engine_dir: &Path, index: Index) -> PathBuf {
    engine_dir.join(format!("snap-{index}"))
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
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

/// Writes a whole file, truncating whatever was there, syncing if asked.
async fn write_file<E: Environment>(
    env: &E,
    path: &Path,
    bytes: &[u8],
    sync: bool,
) -> io::Result<()> {
    let file = env
        .fs()
        .open(
            path,
            OpenOptions::new().write(true).create(true).truncate(true),
        )
        .await?;
    file.write_at(0, Bytes::copy_from_slice(bytes)).await?;
    if sync {
        file.sync().await?;
    }
    Ok(())
}

/// Whether `name` is a file of the store proper: what an adoption deletes from the
/// old store. Checkpoint and staging directories are not.
fn is_store_file(name: &str) -> bool {
    name == "CURRENT"
        || name == "CURRENT.tmp"
        || name.ends_with(".sst")
        || name.ends_with(".wal")
        || name.starts_with("MANIFEST-")
}

/// A file name a chunk may carry: one path component, nothing that walks.
fn valid_name(name: &[u8]) -> bool {
    let Ok(name) = std::str::from_utf8(name) else {
        return false;
    };
    !name.is_empty()
        && name.len() <= 100
        && name != "."
        && name != ".."
        && name != "CURRENT.tmp"
        && !name.contains('/')
        && !name.contains('\\')
}

/// Adopts a completed install at the staging path, if there is one: the switch of
/// RAFT.md §1, run before the engine opens (PROPOSED(D-038)). Returns whether a
/// store was adopted. An incomplete staging directory (no valid `CURRENT`) is
/// swept away instead. See the module documentation for the crash-safety order.
///
/// # Errors
///
/// The filesystem's, while replacing the old store; the caller should not open
/// the engine after one.
pub async fn adopt_staged<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<bool> {
    let fs = env.fs();
    let staging = staging_dir(engine_dir);
    let Ok(names) = fs.read_dir(&staging).await else {
        return Ok(false);
    };
    let current = match read_whole(env, &manifest::current_path(&staging)).await {
        Ok(Some(bytes)) if manifest::parse_current(&bytes).is_some() => bytes,
        _ => {
            // Debris of an install that never finished: sweep it so the next
            // stream starts clean, and the old store stays in force.
            for name in &names {
                let _ = fs.remove_file(&staging.join(name)).await;
            }
            let _ = fs.sync_dir(&staging).await;
            return Ok(false);
        }
    };
    // The staged store is complete: it wins. The old store's CURRENT goes first,
    // so a half-adopted old store can never open; the staged CURRENT goes last,
    // so a crash anywhere before it re-runs this adoption on the same bytes.
    let old = fs.read_dir(engine_dir).await?;
    let _ = fs.remove_file(&manifest::current_path(engine_dir)).await;
    fs.sync_dir(engine_dir).await?;
    for name in &old {
        let Some(text) = name.to_str() else { continue };
        if text != "CURRENT" && is_store_file(text) {
            let _ = fs.remove_file(&engine_dir.join(name)).await;
        }
    }
    fs.sync_dir(engine_dir).await?;
    for name in &names {
        let Some(text) = name.to_str() else { continue };
        if text == "CURRENT" || text == "CURRENT.tmp" {
            continue;
        }
        let Some(bytes) = read_whole(env, &staging.join(name)).await? else {
            continue;
        };
        write_file(env, &engine_dir.join(name), &bytes, true).await?;
    }
    fs.sync_dir(engine_dir).await?;
    write_file(env, &manifest::current_path(engine_dir), &current, true).await?;
    fs.sync_dir(engine_dir).await?;
    // The point of no return: without its CURRENT the staging directory never
    // wins again, so the adopted store's own writes are safe from a replay.
    fs.remove_file(&manifest::current_path(&staging)).await?;
    fs.sync_dir(&staging).await?;
    for name in &names {
        if name.to_str() != Some("CURRENT") {
            let _ = fs.remove_file(&staging.join(name)).await;
        }
    }
    let _ = fs.sync_dir(&staging).await;
    Ok(true)
}

/// Takes a snapshot at `index`, whose entry has `term`: the identity is recorded
/// under `0 / 3 / snapshot` in the live store first, synced, and then the
/// checkpoint is written to `dir`, so the checkpoint's copy of the record precedes
/// the checkpoint's `CURRENT` (RAFT.md §1, D-024). The caller must be the `apply`
/// task with no apply in flight, so `index` is exactly what the checkpoint
/// captures (PROPOSED(D-036)). Any earlier attempt at `dir` is swept first.
///
/// # Errors
///
/// The engine's or the filesystem's; the record may then name a checkpoint that
/// was never completed, which only ever fails a stream, never the store.
pub async fn take<E: Environment>(
    env: &E,
    store: &RaftStore<E>,
    dir: &Path,
    index: Index,
    term: Term,
    config: &Configuration,
) -> io::Result<()> {
    let fs = env.fs();
    if let Ok(names) = fs.read_dir(dir).await {
        for name in names {
            let _ = fs.remove_file(&dir.join(name)).await;
        }
        let _ = fs.sync_dir(dir).await;
    }
    store
        .record_snapshot(&SnapshotRecord {
            last_index: index,
            last_term: term,
            config: config.clone(),
            dir: dir.display().to_string(),
            taken: true,
        })
        .await?;
    store.engine().checkpoint(dir).await?;
    Ok(())
}

/// The leader's side of one stream (RAFT.md §1): which follower, which snapshot,
/// which files, and the next byte to send. One chunk is outstanding at a time;
/// the driver resends it on a timeout and moves wherever an acknowledgement says.
pub struct Sender {
    /// The follower being fed.
    pub to: ServerId,
    /// The snapshot's last index.
    pub last_index: Index,
    /// That entry's term.
    pub last_term: Term,
    /// The leader's term when the stream began: part of the stream's identity,
    /// stamped on every chunk.
    pub term: Term,
    dir: PathBuf,
    /// The checkpoint's files in directory order, each with its size.
    files: Vec<(Bytes, u64)>,
    /// The next byte to send: file position and offset. `file == files.len()`
    /// once everything was sent and the final acknowledgement is awaited.
    file: usize,
    offset: u64,
}

impl Sender {
    /// A stream of the checkpoint in `dir` to `to`, under the leader's `term`.
    ///
    /// # Errors
    ///
    /// The filesystem's, or `InvalidData` for an empty checkpoint directory: the
    /// checkpoint recorded was never completed, and the caller should take a
    /// fresh one.
    pub async fn open<E: Environment>(
        env: &E,
        dir: &Path,
        to: ServerId,
        last_index: Index,
        last_term: Term,
        term: Term,
    ) -> io::Result<Self> {
        let fs = env.fs();
        let mut files = Vec::new();
        for name in fs.read_dir(dir).await? {
            let file = fs
                .open(&dir.join(&name), OpenOptions::new().read(true))
                .await?;
            let size = file.size().await?;
            let text = name.to_str().ok_or_else(|| bad("checkpoint file name"))?;
            files.push((Bytes::copy_from_slice(text.as_bytes()), size));
        }
        if files.is_empty() {
            return Err(bad("the checkpoint directory is empty"));
        }
        Ok(Self {
            to,
            last_index,
            last_term,
            term,
            dir: dir.to_path_buf(),
            files,
            file: 0,
            offset: 0,
        })
    }

    /// The chunk to send now: at the current position, or the final chunk again
    /// when everything was sent and the answer is still owed.
    ///
    /// # Errors
    ///
    /// The filesystem's, reading the checkpoint's file.
    pub async fn chunk<E: Environment>(&self, env: &E, chunk_bytes: usize) -> io::Result<Message> {
        let (index, offset) = self.position(chunk_bytes);
        let (name, total) = &self.files[index];
        let len = usize::try_from((total - offset).min(chunk_bytes as u64)).expect("chunk fits");
        let data = if len == 0 {
            Bytes::new()
        } else {
            let path = self.dir.join(str::from_utf8(name).expect("listed name"));
            let file = env.fs().open(&path, OpenOptions::new().read(true)).await?;
            let data = file.read_at(offset, len).await?;
            if data.len() != len {
                return Err(bad("checkpoint file shrank"));
            }
            data
        };
        let done = index + 1 == self.files.len() && offset + data.len() as u64 >= *total;
        Ok(Message::InstallSnapshot {
            term: self.term,
            last_index: self.last_index,
            last_term: self.last_term,
            file: name.clone(),
            offset,
            total: *total,
            done,
            data,
        })
    }

    /// The position [`chunk`](Self::chunk) reads: the current one, clamped back to
    /// the final chunk when everything was sent.
    fn position(&self, chunk_bytes: usize) -> (usize, u64) {
        if self.file < self.files.len() {
            return (self.file, self.offset);
        }
        let index = self.files.len() - 1;
        let total = self.files[index].1;
        let last = if total == 0 {
            0
        } else {
            (total - 1) / chunk_bytes as u64 * chunk_bytes as u64
        };
        (index, last)
    }

    /// Whether every byte was sent and only the final answer is owed.
    #[must_use]
    pub fn at_end(&self) -> bool {
        self.file >= self.files.len()
    }

    /// The offset of the chunk [`chunk`](Self::chunk) would send: what a
    /// resumption trace reports.
    #[must_use]
    pub fn position_offset(&self, chunk_bytes: usize) -> u64 {
        self.position(chunk_bytes).1
    }

    /// Moves to where an acknowledgement says the receiver is: `file` and
    /// `offset` name the next byte it wants, an empty `file` the very start.
    /// Returns whether the stream moved backwards — a resumption after loss,
    /// worth a trace (RAFT.md §1). An acknowledgement naming no known file is
    /// stale and moves nothing.
    pub fn on_more(&mut self, file: &[u8], offset: u64) -> bool {
        let target = if file.is_empty() {
            (0, 0)
        } else {
            let Some(index) = self.files.iter().position(|(name, _)| name == file) else {
                return false;
            };
            if offset >= self.files[index].1 && self.files[index].1 > 0 || self.files[index].1 == 0
            {
                (index + 1, 0)
            } else {
                (index, offset)
            }
        };
        let rewound = target < (self.file, self.offset);
        (self.file, self.offset) = target;
        rewound
    }

    /// Starts the stream over from the first byte: the receiver asked.
    pub fn restart(&mut self) {
        self.file = 0;
        self.offset = 0;
    }
}

/// What the repair writes into the staged store before its `CURRENT` (RAFT.md §1):
/// the receiver's own identity, which must survive the switch.
pub struct Repair {
    /// The receiver's current term.
    pub term: Term,
    /// The receiver's vote in it.
    pub vote: Option<ServerId>,
    /// The receiver's log entries past the snapshot whose prefix matched: kept.
    pub tail: Vec<Entry>,
    /// Whether the store replaces one that was refused for lost state: the server
    /// then grants no vote, no pre-vote and no lease promise on it, for good.
    // PROPOSED(D-035): re-seeded servers are quarantined from voting for good.
    pub quarantined: bool,
}

/// A verified, fully-assembled stream, ready for [`Assembler::finish`].
pub struct Staged {
    /// The sender.
    pub from: ServerId,
    /// The snapshot's last index.
    pub last_index: Index,
    /// That entry's term.
    pub last_term: Term,
    /// The sender's term: the stream's, for the response.
    pub term: Term,
    /// The configuration at the snapshot, from its record.
    pub config: Configuration,
    /// The staged store's manifest, as streamed.
    manifest: Manifest,
}

/// What feeding one chunk to the [`Assembler`] asks the caller to send back.
pub enum Feed {
    /// Acknowledge with [`SnapshotStatus::More`] naming the next byte wanted.
    Ack {
        /// The file the receiver expects data for.
        file: Bytes,
        /// How many bytes of it the receiver has.
        offset: u64,
    },
    /// Acknowledge with [`SnapshotStatus::Restart`]: the stream cannot continue.
    Restart,
    /// The stream is complete and verified: decide, then call
    /// [`Assembler::finish`] or [`Assembler::abandon`].
    Staged(Staged),
}

/// One in-flight stream at the receiver.
struct Stream {
    from: ServerId,
    last_index: Index,
    last_term: Term,
    term: Term,
    /// Files fully received, with their sizes.
    done: BTreeMap<Bytes, u64>,
    /// The file arriving now: name, bytes received, total.
    current: Option<(Bytes, u64, u64)>,
    /// The last file completed: where an acknowledgement points between files.
    last_done: Option<(Bytes, u64)>,
    /// The streamed `CURRENT`, held aside in memory so the staging directory is
    /// not a complete store until [`Assembler::finish`] writes its own
    /// (RAFT.md §1); the buggy variant writes it to disk as it arrives.
    current_file: BytesMut,
}

/// The receiver's side of a stream (RAFT.md §1): assembles chunks in the staging
/// directory, acknowledges offsets so a resend resumes rather than restarts, and
/// on the final chunk verifies every staged table with the engine's own checks.
pub struct Assembler<E: Environment> {
    env: E,
    staging: PathBuf,
    variant: Variant,
    stream: Option<Stream>,
}

impl<E: Environment> Assembler<E> {
    /// An assembler staging under `engine_dir` (see [`staging_dir`]).
    pub fn new(env: E, engine_dir: &Path, variant: Variant) -> Self {
        Self {
            env,
            staging: staging_dir(engine_dir),
            variant,
            stream: None,
        }
    }

    /// Feeds one chunk. The identity is (sender, leader term, last index, last
    /// term): a chunk of a different identity starts the staging directory over
    /// if it starts a stream (offset 0), and asks for a restart otherwise. A
    /// duplicate or out-of-order chunk is answered with where the stream stands,
    /// which is what lets the sender resume after loss.
    ///
    /// # Errors
    ///
    /// The filesystem's, or `InvalidData` for a stream that completed but did not
    /// verify; the caller should [`abandon`](Self::abandon) and answer
    /// [`Feed::Restart`].
    #[expect(clippy::too_many_arguments, reason = "a chunk names everything")]
    pub async fn on_chunk(
        &mut self,
        from: ServerId,
        term: Term,
        last_index: Index,
        last_term: Term,
        file: Bytes,
        offset: u64,
        total: u64,
        done: bool,
        data: Bytes,
    ) -> io::Result<Feed> {
        if !valid_name(&file) {
            return Ok(Feed::Restart);
        }
        let matches = self.stream.as_ref().is_some_and(|s| {
            s.from == from
                && s.term == term
                && s.last_index == last_index
                && s.last_term == last_term
        });
        if !matches {
            self.abandon().await;
            if offset != 0 {
                return Ok(Feed::Restart);
            }
            self.env.fs().create_dir_all(&self.staging).await?;
            self.stream = Some(Stream {
                from,
                last_index,
                last_term,
                term,
                done: BTreeMap::new(),
                current: None,
                last_done: None,
                current_file: BytesMut::new(),
            });
        }
        let stream = self.stream.as_mut().expect("a stream");
        let acceptable = match &stream.current {
            Some((name, have, _)) => *name == file && *have == offset,
            None => offset == 0 && !stream.done.contains_key(&file),
        };
        if !acceptable {
            // A duplicate, or the sender ran ahead: say where the stream stands.
            let (file, offset) = match &stream.current {
                Some((name, have, _)) => (name.clone(), *have),
                None => stream.last_done.clone().unwrap_or((Bytes::new(), 0)),
            };
            return Ok(Feed::Ack { file, offset });
        }
        if stream.current.is_none() {
            stream.current = Some((file.clone(), 0, total));
        }
        let is_current = file[..] == b"CURRENT"[..];
        if is_current {
            stream.current_file.extend_from_slice(&data);
        }
        if !is_current || self.variant == Variant::SnapshotWithoutCurrentLast {
            // The buggy variant writes the streamed CURRENT straight to disk: the
            // staging directory then looks complete before the repair is durable,
            // and a crash mid-install adopts the leader's state (RAFT.md §5).
            let path = self
                .staging
                .join(std::str::from_utf8(&file).expect("validated name"));
            let handle = self
                .env
                .fs()
                .open(&path, OpenOptions::new().write(true).create(true))
                .await?;
            if !data.is_empty() {
                handle.write_at(offset, data.clone()).await?;
            }
            let complete = offset + data.len() as u64 >= total;
            if complete {
                // The file and its directory entry: a completed file survives a
                // crash, so what an offset was acknowledged for is really there.
                handle.sync().await?;
                self.env.fs().sync_dir(&self.staging).await?;
            }
        }
        let stream = self.stream.as_mut().expect("a stream");
        let received = offset + data.len() as u64;
        if received >= total {
            let (name, _, _) = stream.current.take().expect("the file arriving");
            stream.done.insert(name.clone(), total);
            stream.last_done = Some((name, total));
        } else {
            stream.current = Some((file, received, total));
        }
        if done {
            if stream.current.is_some() {
                // A final chunk that did not complete its file is not a stream.
                return Err(bad("the stream ended mid-file"));
            }
            let staged = self.verify().await?;
            return Ok(Feed::Staged(staged));
        }
        let stream = self.stream.as_ref().expect("a stream");
        let (file, offset) = match &stream.current {
            Some((name, have, _)) => (name.clone(), *have),
            None => stream.last_done.clone().unwrap_or((Bytes::new(), 0)),
        };
        Ok(Feed::Ack { file, offset })
    }

    /// Verifies the completed stream: the streamed `CURRENT` names the manifest,
    /// every table it lists opens and passes the engine's checks, and the staged
    /// snapshot record matches the stream's identity (RAFT.md §1).
    async fn verify(&self) -> io::Result<Staged> {
        let stream = self.stream.as_ref().expect("a stream");
        let number =
            manifest::parse_current(&stream.current_file).ok_or_else(|| bad("streamed CURRENT"))?;
        let bytes = read_whole(&self.env, &manifest::manifest_path(&self.staging, number))
            .await?
            .ok_or_else(|| bad("the streamed manifest is missing"))?;
        let staged = Manifest::decode(&bytes)?;
        let mut best: Option<(u64, Value)> = None;
        for meta in &staged.ssts {
            let file = self
                .env
                .fs()
                .open(
                    &manifest::sst_path(&self.staging, meta.number),
                    OpenOptions::new().read(true),
                )
                .await?;
            let reader = SstReader::open(file).await?;
            reader.verify().await?;
            if let Some((seq, value)) = reader.get(&store::snapshot_key(), u64::MAX).await?
                && best.as_ref().is_none_or(|(s, _)| seq > *s)
            {
                best = Some((seq, value));
            }
        }
        let Some((_, Value::Live(bytes))) = best else {
            return Err(bad("the staged store has no snapshot record"));
        };
        let record = store::decode_snapshot_record(bytes)?;
        if record.last_index != stream.last_index || record.last_term != stream.last_term {
            return Err(bad("the staged record does not match the stream"));
        }
        Ok(Staged {
            from: stream.from,
            last_index: stream.last_index,
            last_term: stream.last_term,
            term: stream.term,
            config: record.config,
            manifest: staged,
        })
    }

    /// Completes the install (RAFT.md §1): one repair table and a new manifest
    /// carry the receiver's identity — `repair`'s hard state, the applied index
    /// at the snapshot, the snapshot record, the kept log tail, tombstones for
    /// the leader's log keys, and the quarantine flag for a re-seed — and only
    /// then is the staged `CURRENT` written, tmp-and-rename, the way every store
    /// switch commits (D-024). After this the staged store is a complete install
    /// for [`adopt_staged`].
    ///
    /// # Errors
    ///
    /// The filesystem's; the caller should [`abandon`](Self::abandon) and answer
    /// [`Feed::Restart`].
    pub async fn finish(&mut self, staged: &Staged, repair: &Repair) -> io::Result<()> {
        let fs = self.env.fs();
        let seq = staged.manifest.flushed_seq + 1;
        // Every log key the staged tables hold: the leader's log, to be replaced
        // by the kept tail.
        let start = store::key(store::RAFT_TENANT, store::LOG_TABLE, &[]);
        let end = store::key(store::RAFT_TENANT, store::LOG_TABLE + 1, &[]);
        let mut held: BTreeSet<Index> = BTreeSet::new();
        for meta in &staged.manifest.ssts {
            let file = fs
                .open(
                    &manifest::sst_path(&self.staging, meta.number),
                    OpenOptions::new().read(true),
                )
                .await?;
            let reader = Arc::new(SstReader::open(file).await?);
            let mut entries = reader.iter();
            entries.seek(&ikey::lower_bound(&start)).await?;
            while let Some((key, _)) = entries.next().await? {
                let (user, _) = ikey::decode(&key)?;
                if user[..] >= end[..] {
                    break;
                }
                if user.len() == 24 {
                    held.insert(Index::from_be_bytes(
                        user[16..24].try_into().expect("eight bytes"),
                    ));
                }
            }
        }
        let record = SnapshotRecord {
            last_index: staged.last_index,
            last_term: staged.last_term,
            config: staged.config.clone(),
            dir: String::new(),
            taken: false,
        };
        let mut writes: BTreeMap<Bytes, Value> = BTreeMap::new();
        writes.insert(
            store::hard_key(),
            Value::Live(store::encode_hard(repair.term, repair.vote)),
        );
        writes.insert(
            store::applied_key(),
            Value::Live(store::encode_applied(staged.last_index)),
        );
        writes.insert(
            store::snapshot_key(),
            Value::Live(store::encode_snapshot_record(&record)),
        );
        // The quarantine key is always written explicitly: set when this store's
        // history was ever re-seeded, and a tombstone otherwise — the streamed
        // checkpoint carries the *leader's* tenant 0, and a flag of the leader's
        // must not quarantine the receiver. PROPOSED(D-035).
        if repair.quarantined {
            writes.insert(
                store::quarantine_key(),
                Value::Live(Bytes::from_static(&[1])),
            );
        } else {
            writes.insert(store::quarantine_key(), Value::Tombstone);
        }
        for entry in &repair.tail {
            writes.insert(
                store::log_key(entry.index),
                Value::Live(store::encode_entry(entry)),
            );
        }
        for index in &held {
            writes
                .entry(store::log_key(*index))
                .or_insert(Value::Tombstone);
        }
        let mut writer = SstWriter::new();
        for (key, value) in &writes {
            writer.add(key, seq, value);
        }
        let (first_key, last_key) = writer.key_range().expect("repair writes");
        let (first_seq, max_seq) = writer.seq_range().expect("repair writes");
        let entries = writer.entries();
        let bytes = writer.finish();
        let number = staged.manifest.next_sst;
        let meta = SstMeta {
            number,
            level: 0,
            first_seq,
            max_seq,
            entries,
            bytes: bytes.len() as u64,
            first_key,
            last_key,
        };
        write_file(
            &self.env,
            &manifest::sst_path(&self.staging, number),
            &bytes,
            true,
        )
        .await?;
        let mut ssts = staged.manifest.ssts.clone();
        ssts.push(meta);
        let repaired = Manifest {
            number: staged.manifest.number + 1,
            next_sst: number + 1,
            flushed_seq: seq,
            ssts,
        };
        write_file(
            &self.env,
            &manifest::manifest_path(&self.staging, repaired.number),
            &repaired.encode(),
            true,
        )
        .await?;
        // CURRENT last: the write that makes the install exist (D-024).
        write_file(
            &self.env,
            &manifest::current_tmp_path(&self.staging),
            &manifest::encode_current(repaired.number),
            true,
        )
        .await?;
        fs.rename(
            &manifest::current_tmp_path(&self.staging),
            &manifest::current_path(&self.staging),
        )
        .await?;
        fs.sync_dir(&self.staging).await?;
        self.stream = None;
        Ok(())
    }

    /// Drops the stream and sweeps the staging directory: a failed verify, an
    /// identity change, or an install the server decided against.
    pub async fn abandon(&mut self) {
        self.stream = None;
        let fs = self.env.fs();
        if let Ok(names) = fs.read_dir(&self.staging).await {
            for name in names {
                let _ = fs.remove_file(&self.staging.join(name)).await;
            }
            let _ = fs.sync_dir(&self.staging).await;
        }
    }
}

/// The receiver's acknowledgement of a chunk, ready to send.
#[must_use]
pub fn ack(term: Term, staged: (Index, Term), file: Bytes, offset: u64) -> Message {
    Message::InstallSnapshotResponse {
        term,
        last_index: staged.0,
        last_term: staged.1,
        file,
        offset,
        status: SnapshotStatus::More,
    }
}

/// The receiver's answer that the install is complete, or was already held.
#[must_use]
pub fn installed(term: Term, staged: (Index, Term)) -> Message {
    Message::InstallSnapshotResponse {
        term,
        last_index: staged.0,
        last_term: staged.1,
        file: Bytes::new(),
        offset: 0,
        status: SnapshotStatus::Installed,
    }
}

/// The receiver's answer that the stream must start over.
#[must_use]
pub fn start_over(term: Term, staged: (Index, Term)) -> Message {
    Message::InstallSnapshotResponse {
        term,
        last_index: staged.0,
        last_term: staged.1,
        file: Bytes::new(),
        offset: 0,
        status: SnapshotStatus::Restart,
    }
}
