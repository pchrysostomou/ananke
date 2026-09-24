//! Checkpoints as snapshots, chunked both ways (RAFT.md §1, §3).
//!
//! A snapshot is an [`Engine::checkpoint`](ananke_storage::Engine::checkpoint) of
//! the state machine's store at an applied index, with its identity, the index,
//! term and configuration, written into the live store's `<prefix> / 3 / snapshot`
//! key *before* the checkpoint is taken, so the checkpoint's copy carries it before
//! the checkpoint's own `CURRENT` (D-024). The checkpoint's own format record
//! follows its `CURRENT`, and a checkpoint is complete only with both, so no
//! stream carries a store whose version nothing says (PROPOSED D-060). [`take`]
//! is that sequence; the `apply` task
//! runs it between applies, so the recorded index is exactly the index the
//! checkpoint captures — the apply task is the only writer of user state and it is
//! busy checkpointing (D-036).
//!
//! Streaming is `InstallSnapshot` chunks of at most `snapshot_chunk` bytes, each
//! naming its file, the offset the data starts at and the file's total size, files
//! in the order the checkpoint directory lists them. [`Sender`] is the leader's
//! bookkeeping: one chunk outstanding, the receiver's acknowledgement naming the
//! next byte it wants, so a resend after loss resumes from the last acknowledged
//! offset of the last file rather than from zero (RAFT.md §1). [`Assembler`] is the
//! receiver's: chunks land in the staging directory, a well-known path under the
//! server's data directory, and the streamed `CURRENT` is held aside in memory so
//! the staging directory is never a complete store while the stream runs. The
//! format record is streamed first and checked before any table is opened, so a
//! leader in another format is refused unread and a staged install always carries
//! its version before its `CURRENT` (D-059, PROPOSED D-060).
//!
//! Installing is [`Assembler::finish`]: the staged tables verified with the
//! engine's own checks, then one repair table and a new manifest written with the
//! receiver's own hard state, the snapshot record, the applied index, the kept log
//! tail, the configuration key consistent with both (RAFT.md §3, D-029) and
//! tombstones for the leader's log keys — and only then the staged
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
//! staged tables and manifest are copied into the store directory first, under
//! numbers above everything already there so no copy lands on a file the old
//! store still needs, each synced and the directory synced; then `CURRENT` is
//! switched to the copied manifest, tmp-and-rename, the commit point of every
//! store switch; only then are the old store's files removed, and last the
//! staging directory's own `CURRENT` — so until the switch is durable the old
//! store is whole and opens, a crash anywhere before the staging `CURRENT` is
//! gone re-runs the adoption on the same staged bytes (earlier copies become
//! orphans the engine's open removes), and no directory rename is needed, which
//! the fault model does not have (D-024, D-038, D-041). A
//! staging directory with no `CURRENT` at all is an install that never finished
//! and is swept; one whose `CURRENT` exists but does not parse is damage, refused
//! with [`LostState`] and never swept, since the staged store may be the only
//! copy of a state the leader has compacted past — and the assembler's own
//! sweep ([`Assembler::abandon`]) leaves a `CURRENT` alone for the same reason,
//! so a start during the re-seed that follows a refusal refuses again rather
//! than opening the old store the acknowledged install superseded.
//! [`Variant::AdoptionAsBuilt`] is the adoption as it was built under D-038: the
//! old store removed first and the copies synced after, and a damaged staging
//! `CURRENT` swept as debris — the nightly's seed 6325, where a crash inside the
//! copy rotted the staging `CURRENT` and the server came back on a fresh store.
//!
//! Every take is a *version*: it goes to its own directory, [`version_dir`],
//! `snap-<index>-<take>`, numbered by the store's take counter, which the record
//! carries (D-043). A stream pins the version it opened for its whole
//! life — a resend after loss resumes on it, and a newer take, at the same index
//! or a later one, never touches it. [`find_version`] is what a stream opens: the
//! newest *complete* version at the index the core asked for, complete meaning
//! the checkpoint's own `CURRENT` is there, since the record precedes the
//! checkpoint (D-036) and may name a take still in flight. [`sweep_versions`]
//! deletes the versions that are neither the record's nor pinned by a stream.
//! The as-built behaviour, one mutable directory per index rewritten by each
//! take, is kept as [`Variant::SharedSnapshotDir`] through [`checkpoint_dir`] and
//! [`take`].

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ananke_env::{Environment, File, FileSystem, OpenOptions};
use ananke_storage::manifest::{self, Manifest, SstMeta};
use ananke_storage::sst::{SstReader, SstWriter};
use ananke_storage::{Value, WriteBatch, ikey};
use bytes::{Bytes, BytesMut};

use crate::core::{Variant, Variants};
use crate::format;
use crate::message::{Message, SnapshotStatus};
use crate::store::{self, Damage, KeyPrefix, LostState, RaftStore, SnapshotRecord};
use crate::types::{Configuration, Entry, Index, Payload, ServerId, Term};

/// The staging directory, a well-known path under the server's data directory
/// (RAFT.md §1): where chunks assemble, and what [`adopt_staged`] looks for.
#[must_use]
pub fn staging_dir(engine_dir: &Path) -> PathBuf {
    engine_dir.join("install")
}

/// The directory a checkpoint taken at `index` goes to, under the server's data
/// directory. Never removed once a stream may have read from it; the snapshot
/// record names the one in force. One directory per index, rewritten by every
/// take at that index: the behaviour as built, which
/// [`Variant::SharedSnapshotDir`] keeps; the correct server takes into
/// [`version_dir`] (D-043).
#[must_use]
pub fn checkpoint_dir(engine_dir: &Path, index: Index) -> PathBuf {
    engine_dir.join(format!("snap-{index}"))
}

/// The directory of the `take`th checkpoint this store took, at `index`, under
/// the server's data directory: `snap-<index>-<take>`. Two takes at one index
/// are two directories, and a stream that opened one reads it untouched for its
/// whole life (D-043).
#[must_use]
pub fn version_dir(engine_dir: &Path, index: Index, take: u64) -> PathBuf {
    engine_dir.join(format!("snap-{index}-{take}"))
}

/// The (index, take) a checkpoint directory's name says, if it is one: the
/// versioned `snap-<index>-<take>`, or the shared `snap-<index>` as take 0.
#[must_use]
pub fn parse_version(name: &str) -> Option<(Index, u64)> {
    let rest = name.strip_prefix("snap-")?;
    match rest.split_once('-') {
        Some((index, take)) => Some((index.parse().ok()?, take.parse().ok()?)),
        None => Some((rest.parse().ok()?, 0)),
    }
}

/// Whether the checkpoint in `dir` is complete: its `CURRENT` is there and
/// parses, and its own format record reads as this build's. The engine writes a
/// checkpoint's `CURRENT` last and synced (D-024), the take writes the record
/// after that (PROPOSED D-060), and the snapshot record names the directory
/// before the checkpoint is written (D-036), so the record may name a take still
/// in flight, or one a crash cut short; a stream must open neither, and must
/// never stream a checkpoint without its version.
///
/// # Errors
///
/// The filesystem's, other than a missing file.
pub async fn checkpoint_complete<E: Environment>(env: &E, dir: &Path) -> io::Result<bool> {
    let switched = match read_whole(env, &manifest::current_path(dir)).await? {
        Some(bytes) => manifest::parse_current(&bytes).is_some(),
        None => false,
    };
    // PROPOSED(D-060): every checkpoint carries its own RAFT-FORMAT record, and
    // a checkpoint without one is incomplete: it costs a retake, never a stream
    // of an unversioned store.
    Ok(switched && format::records_this_format(env, dir).await?)
}

/// The newest complete version of the checkpoint at `index` under `engine_dir`,
/// with its take number: what a stream to a follower opens (D-043).
/// None when no complete version of that index exists — the take is in flight,
/// a crash cut it short, or the record is an install's — and the caller should
/// ask for a fresh take.
///
/// # Errors
///
/// The filesystem's, listing the data directory.
pub async fn find_version<E: Environment>(
    env: &E,
    engine_dir: &Path,
    index: Index,
) -> io::Result<Option<(PathBuf, u64)>> {
    let mut takes: Vec<u64> = env
        .fs()
        .read_dir(engine_dir)
        .await?
        .iter()
        .filter_map(|name| parse_version(name.to_str()?))
        .filter(|&(i, _)| i == index)
        .map(|(_, take)| take)
        .collect();
    takes.sort_unstable_by(|a, b| b.cmp(a));
    for take in takes {
        let dir = if take == 0 {
            checkpoint_dir(engine_dir, index)
        } else {
            version_dir(engine_dir, index, take)
        };
        if checkpoint_complete(env, &dir).await? {
            return Ok(Some((dir, take)));
        }
    }
    Ok(None)
}

/// Deletes every version under `engine_dir` that is neither the record's nor
/// pinned, and returns the (index, take) of each (D-043). `pinned`
/// counts the streams reading each directory; a directory with a reader stays.
/// The directories are listed *before* the record is read: a take writes its
/// record before it creates its directory (D-036), so a directory the listing
/// saw and the record does not name is an old version, never one in flight. The
/// filesystem has no directory removal (D-024), so a version is deleted by
/// removing its files, and a directory already empty is not a version.
///
/// # Errors
///
/// The filesystem's or the engine's, reading the record; a file that could not
/// be removed is left for the next sweep.
pub async fn sweep_versions<E: Environment>(
    env: &E,
    store: &RaftStore<E>,
    engine_dir: &Path,
    pinned: &BTreeMap<PathBuf, usize>,
) -> io::Result<Vec<(Index, u64)>> {
    let fs = env.fs();
    let names = fs.read_dir(engine_dir).await?;
    let current = store
        .snapshot_record()
        .await?
        .map(|record| record.dir)
        .unwrap_or_default();
    let mut deleted = Vec::new();
    for name in names {
        let Some(version) = name.to_str().and_then(parse_version) else {
            continue;
        };
        let dir = engine_dir.join(&name);
        if dir.display().to_string() == current || pinned.get(&dir).is_some_and(|&n| n > 0) {
            continue;
        }
        let Ok(files) = fs.read_dir(&dir).await else {
            continue;
        };
        if files.is_empty() {
            continue;
        }
        for file in files {
            let _ = fs.remove_file(&dir.join(file)).await;
        }
        let _ = fs.sync_dir(&dir).await;
        deleted.push(version);
    }
    Ok(deleted)
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
/// RAFT.md §1, run before the engine opens (D-038), in the crash-safe
/// order of D-041. Returns whether a store was adopted. A staging
/// directory with no `CURRENT` at all is an install that never finished and is
/// swept away instead; one whose `CURRENT` exists but does not parse is damage
/// and refused. See the module documentation for the order.
///
/// # Errors
///
/// `InvalidData` carrying a [`LostState`] when the staging directory is damaged
/// — its `CURRENT` or the manifest it names cannot be read, or a table it lists
/// is missing — with nothing touched: the caller should refuse the store the way
/// it refuses one whose recovery lost state. Otherwise the filesystem's, while
/// replacing the old store; the caller should not open the engine after one.
pub async fn adopt_staged<E: Environment>(env: &E, engine_dir: &Path) -> io::Result<bool> {
    adopt_staged_under(env, engine_dir, Variants::correct()).await
}

/// [`adopt_staged`] under `variants`: [`Variant::AdoptionAsBuilt`] runs the
/// adoption as it was built under D-038, which the sweep must catch (RAFT.md §5);
/// every other variant runs the crash-safe order.
///
/// # Errors
///
/// As [`adopt_staged`].
// D-041: the crash-safe adoption and the store identity marker.
// D-045: a variant is a set.
pub async fn adopt_staged_under<E: Environment>(
    env: &E,
    engine_dir: &Path,
    variants: impl Into<Variants>,
) -> io::Result<bool> {
    // D-059: the format is read before anything writes, and an adoption writes.
    // A fresh directory cannot hold a staged install, so there is nothing to
    // adopt; a record that cannot be read is lost state, which the caller
    // decides about, and the adoption of the re-seed that follows rewrites it.
    let rewrite = match format::check_format(env, engine_dir).await? {
        format::Verdict::Fresh(_) => return Ok(false),
        format::Verdict::Recorded { .. } => false,
        format::Verdict::Damaged => true,
    };
    adopt_checked(env, engine_dir, variants.into(), rewrite, true).await
}

/// [`adopt_staged_under`] with the gate already run: `rewrite` says the store's
/// format record could not be read and is to be rewritten once the installed
/// store is in force, and `check_staged` whether the staged install's own record
/// is checked before the first write — off only for the known-buggy start orders
/// a directed test runs beside the correct one (`node::StartOrder`).
///
/// # Errors
///
/// As [`adopt_staged`], plus `InvalidData` carrying a
/// [`FormatRefused`](crate::format::FormatRefused) for a staged install in
/// another format, which is refused with nothing of the store touched.
// PROPOSED(D-060)
pub(crate) async fn adopt_checked<E: Environment>(
    env: &E,
    engine_dir: &Path,
    variants: Variants,
    rewrite: bool,
    check_staged: bool,
) -> io::Result<bool> {
    if variants.contains(Variant::AdoptionAsBuilt) {
        return adopt_staged_as_built(env, engine_dir, rewrite, check_staged).await;
    }
    let fs = env.fs();
    let staging = staging_dir(engine_dir);
    let Ok(names) = fs.read_dir(&staging).await else {
        return Ok(false);
    };
    let Some(current) = read_whole(env, &manifest::current_path(&staging)).await? else {
        // No CURRENT at all: an install that never finished. Sweep it so the
        // next stream starts clean, and the old store stays in force.
        for name in &names {
            let _ = fs.remove_file(&staging.join(name)).await;
        }
        let _ = fs.sync_dir(&staging).await;
        return Ok(false);
    };
    // A CURRENT that exists is a completed install, and one that does not parse
    // is damage: refused, and nothing here is touched.
    let Some(number) = manifest::parse_current(&current) else {
        return Err(LostState::from_damage(Damage::StagingCurrentUnreadable).into_io());
    };
    let staged = match read_whole(env, &manifest::manifest_path(&staging, number)).await? {
        Some(bytes) => Manifest::decode(&bytes)
            .map_err(|_| LostState::from_damage(Damage::StagingManifestUnreadable).into_io())?,
        None => return Err(LostState::from_damage(Damage::StagingManifestUnreadable).into_io()),
    };
    for meta in &staged.ssts {
        if !names
            .iter()
            .any(|n| manifest::sst_of(n) == Some(meta.number))
        {
            return Err(LostState::from_damage(Damage::StagingTableMissing(meta.number)).into_io());
        }
    }
    // D-059: the staged install's own format, read before the first write of
    // this adoption. A stream of another format is refused with nothing of the
    // store touched; one whose record cannot be read is staging damage, refused
    // and never swept (D-041).
    if check_staged {
        format::check_staged_record(env, &staging).await?;
    }
    // The old store's files, listed before anything is copied, and the numbers
    // the copies go under: every staged table one past the highest table on
    // disk and up, the manifest one past the highest manifest, so no copy lands
    // on a file the old store still needs. The staged store's own numbers are
    // the leader's and collide with the receiver's freely. A re-run after a
    // crash lists the earlier copies too and numbers past them; they become
    // orphans the engine's open removes.
    let old = fs.read_dir(engine_dir).await?;
    let sst_base = old
        .iter()
        .filter_map(|n| manifest::sst_of(n))
        .max()
        .unwrap_or(0);
    let manifest_number = old
        .iter()
        .filter_map(|n| manifest::manifest_of(n))
        .max()
        .unwrap_or(0)
        + 1;
    let mut ssts = Vec::with_capacity(staged.ssts.len());
    for meta in &staged.ssts {
        let bytes = read_whole(env, &manifest::sst_path(&staging, meta.number))
            .await?
            .ok_or_else(|| {
                LostState::from_damage(Damage::StagingTableMissing(meta.number)).into_io()
            })?;
        let number = sst_base + meta.number;
        write_file(env, &manifest::sst_path(engine_dir, number), &bytes, true).await?;
        ssts.push(SstMeta {
            number,
            ..meta.clone()
        });
    }
    let adopted = Manifest {
        number: manifest_number,
        next_sst: sst_base + staged.next_sst,
        flushed_seq: staged.flushed_seq,
        ssts,
    };
    write_file(
        env,
        &manifest::manifest_path(engine_dir, adopted.number),
        &adopted.encode(),
        true,
    )
    .await?;
    fs.sync_dir(engine_dir).await?;
    // The switch: CURRENT tmp-and-rename, the commit point of every store switch
    // (D-024). Until this is durable the old CURRENT names the old store, whole.
    write_file(
        env,
        &manifest::current_tmp_path(engine_dir),
        &manifest::encode_current(adopted.number),
        true,
    )
    .await?;
    fs.rename(
        &manifest::current_tmp_path(engine_dir),
        &manifest::current_path(engine_dir),
    )
    .await?;
    fs.sync_dir(engine_dir).await?;
    // PROPOSED(D-060): the store in this directory is the installed one now, so
    // a format record that could not be read is rewritten for it — after the
    // switch, never before, since before it the record would label the old
    // store, whose format could not be read, as this build's. The staging
    // CURRENT is removed last, so a crash anywhere here re-runs the adoption and
    // the rewrite with it.
    if rewrite {
        format::rewrite_format(env, engine_dir).await?;
    }
    // D-044: the store in this directory is the installed one now, so
    // the marker is written fresh and whatever the marker said about the store
    // before it — that it lost state — goes with that store. Straight after the
    // switch, so the window in which a crash leaves the adopted store behind a
    // lost mark, and costs another install, is the smallest it can be; before
    // it, a crash would leave the old store in force with nothing saying it lost
    // state, which is what the mark exists to prevent.
    store::mark_store(env, engine_dir).await?;
    // The old store's files, only now: everything of the store proper that was
    // on disk before the copies, none of which shares a name with a copy. Their
    // log segments go with them; the adopted store starts with an empty log.
    for name in &old {
        let Some(text) = name.to_str() else { continue };
        if text != "CURRENT" && is_store_file(text) {
            let _ = fs.remove_file(&engine_dir.join(name)).await;
        }
    }
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

/// The adoption as built under D-038, kept as [`Variant::AdoptionAsBuilt`] for
/// the sweep to catch (RAFT.md §5): the old store's `CURRENT` goes first, then
/// its files, then the staged files are copied with `CURRENT` last, and a
/// staging `CURRENT` that does not parse is swept as debris. Between the old
/// store's removal and the sync of the copies' directory entries the staged
/// store is the only copy; a crash there whose bit rot lands on the staging
/// `CURRENT` leaves nothing, and the next open — with no marker to say the
/// directory was a store — is a fresh one (the nightly's seed 6325).
// D-041: the crash-safe adoption and the store identity marker.
// D-045: a variant is a set.
async fn adopt_staged_as_built<E: Environment>(
    env: &E,
    engine_dir: &Path,
    rewrite: bool,
    check_staged: bool,
) -> io::Result<bool> {
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
    // D-059: the staged install's own format, read before the first write of
    // this adoption, under this variant as under the correct server: the format
    // rule is not what the variant models.
    if check_staged {
        format::check_staged_record(env, &staging).await?;
    }
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
        // PROPOSED(D-060): the staged copy of the format record is not copied
        // over the store's own, which says what the store in this directory is.
        if text == "CURRENT"
            || text == "CURRENT.tmp"
            || text == format::FORMAT_FILE
            || text == format::FORMAT_TMP
        {
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
    // PROPOSED(D-060): as in the correct adoption, a record that could not be
    // read is rewritten for the store that is in force now.
    if rewrite {
        format::rewrite_format(env, engine_dir).await?;
    }
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
/// under `<prefix> / 3 / snapshot` in the live store first, synced, and then the
/// checkpoint is written to `dir`, so the checkpoint's copy of the record precedes
/// the checkpoint's `CURRENT` (RAFT.md §1, D-024). The caller must be the `apply`
/// task with no apply in flight, so `index` is exactly what the checkpoint
/// captures (D-036). Any earlier attempt at `dir` is swept first — a
/// stream reading `dir` is scrambled by that, which is the as-built behaviour
/// [`Variant::SharedSnapshotDir`] keeps; the correct server takes through
/// [`take_version`] (D-043). The record's take counter advances here
/// too, so the numbering of versions is monotone whichever way a take went.
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
    let number = next_take(store).await?;
    take_numbered(env, store, dir, index, term, config, number).await
}

/// Takes a snapshot at `index` into its own version directory under
/// `engine_dir`, [`version_dir`] numbered by the store's take counter, and
/// returns that directory (D-043). Two takes at one index are two
/// directories, so a stream pinned to the earlier one reads it untouched. The
/// order is [`take`]'s: the record, naming the directory and the new count,
/// synced first; then the checkpoint.
///
/// # Errors
///
/// The engine's or the filesystem's, as for [`take`].
pub async fn take_version<E: Environment>(
    env: &E,
    store: &RaftStore<E>,
    engine_dir: &Path,
    index: Index,
    term: Term,
    config: &Configuration,
) -> io::Result<PathBuf> {
    let number = next_take(store).await?;
    let dir = version_dir(engine_dir, index, number);
    take_numbered(env, store, &dir, index, term, config, number).await?;
    Ok(dir)
}

/// The next take number: one past the record's count, one for a store that
/// never took (D-043).
async fn next_take<E: Environment>(store: &RaftStore<E>) -> io::Result<u64> {
    Ok(store
        .snapshot_record()
        .await?
        .map_or(0, |record| record.take)
        + 1)
}

/// The take itself, numbered: see [`take`].
async fn take_numbered<E: Environment>(
    env: &E,
    store: &RaftStore<E>,
    dir: &Path,
    index: Index,
    term: Term,
    config: &Configuration,
    number: u64,
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
            take: number,
        })
        .await?;
    store.engine().checkpoint(dir).await?;
    // PROPOSED(D-060): the checkpoint's own format record, after the engine's
    // checkpoint, which requires an empty directory. A crash between the two
    // leaves a checkpoint `checkpoint_complete` calls incomplete: a retake, never
    // a stream of a store with no version.
    format::write_checkpoint_record(env, dir).await?;
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
        let mut listing = fs.read_dir(dir).await?;
        // PROPOSED(D-060): the record is streamed first, so the receiver writes
        // and syncs it before any staged table exists — and, under
        // `SnapshotWithoutCurrentLast`, before the staged `CURRENT` the variant
        // writes on arrival, which would otherwise reach the adoption as an
        // install with no version. A stable reorder: the final chunk is still
        // the manifest, as it was.
        if let Some(at) = listing
            .iter()
            .position(|n| n.to_str() == Some(format::FORMAT_FILE))
        {
            let record = listing.remove(at);
            listing.insert(0, record);
        }
        let mut files = Vec::new();
        for name in listing {
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

    /// Where the last acknowledgement put the stream, as (file position, offset):
    /// the next byte the receiver wants, `(files, 0)` once everything was
    /// acknowledged, and `(0, 0)` again after a [`restart`](Self::restart). The
    /// `snapshot` task compares it with the furthest point the stream had reached,
    /// which is what re-seed progress means for check quorum (D-049).
    #[must_use]
    pub fn acknowledged(&self) -> (usize, u64) {
        (self.file, self.offset)
    }

    /// The checkpoint directory this stream is pinned to (D-043): the
    /// one it opened, read for its whole life, and what a reader count keeps
    /// from being swept meanwhile.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
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
    // D-035: re-seeded servers are quarantined from voting for good.
    pub quarantined: bool,
    /// The incarnation number the staged store carries (RAFT.md §3): the
    /// receiver's own on an install into a live store, and a fresh one on a
    /// re-seed, so a leader that recorded a match index against the lost store
    /// forgets it.
    // D-042: store incarnations.
    pub incarnation: u64,
}

/// The snapshot record a *checkpoint directory* carries for `prefix`, read out of
/// the directory's own tables without opening it as a store.
///
/// The node's live install needs one thing from the bytes it was streamed that no
/// message carries: the configuration in force at the snapshot's last index, which
/// the repair writes into the receiver's snapshot record and which
/// `Raft::restore_compacted` takes as the floor a revert cannot go below (D-029). The
/// receiver cannot use the configuration *it* believes is in force, because a replica
/// being fed a snapshot is behind by definition and a membership change inside the
/// compacted prefix is exactly what it has not seen.
///
/// The one-group install reads it while it verifies the staged store
/// ([`Assembler::on_chunk`]); a live install verifies nothing of its own — the engine
/// does it, at `Engine::open_span_source` — so the read is here, on its own, and reads
/// one key. It is the same walk [`Assembler::finish`] makes over the staged manifest's
/// tables, and it lives in this crate for the reason the layout does: `ananke-raft`
/// owns what a Raft key is, and `ananke-shard` must not learn it (Q40, D-060).
///
/// `Ok(None)` is a whole directory with no record for that prefix, which is a stream
/// of a range whose store never took a snapshot.
///
/// # Errors
///
/// The filesystem's, or `InvalidData` when `CURRENT`, the manifest it names or a table
/// cannot be read — the same damage `Engine::open_span_source` refuses the install for.
// PROPOSED(D-083): the node's live install reads the streamed configuration out of the
// staged directory, because no message carries it.
pub async fn staged_record<E: Environment>(
    env: &E,
    dir: &Path,
    prefix: &KeyPrefix,
) -> io::Result<Option<SnapshotRecord>> {
    let fs = env.fs();
    let Some(current) = read_whole(env, &manifest::current_path(dir)).await? else {
        return Err(bad("the staged directory has no CURRENT"));
    };
    let number = manifest::parse_current(&current).ok_or_else(|| bad("the staged CURRENT"))?;
    let staged = match read_whole(env, &manifest::manifest_path(dir, number)).await? {
        Some(bytes) => Manifest::decode(&bytes)?,
        None => return Err(bad("the staged manifest")),
    };
    let key = prefix.snapshot_key();
    // The newest write of the key wins, and a table's own `max_seq` orders the tables:
    // the checkpoint's tables are written in key order at one version, so at most one
    // of them holds it, but the walk does not rely on that.
    let mut newest: Option<(u64, Bytes)> = None;
    for meta in &staged.ssts {
        let file = fs
            .open(
                &manifest::sst_path(dir, meta.number),
                OpenOptions::new().read(true),
            )
            .await?;
        let reader = Arc::new(SstReader::open(file).await?);
        let mut entries = reader.iter();
        entries.seek(&ikey::lower_bound(&key)).await?;
        while let Some((raw, value)) = entries.next().await? {
            let (user, seq) = ikey::decode(&raw)?;
            if user[..] != key[..] {
                break;
            }
            if newest.as_ref().is_none_or(|(at, _)| seq > *at)
                && let Value::Live(bytes) = value
            {
                newest = Some((seq, bytes));
            }
        }
    }
    match newest {
        Some((_, bytes)) => Ok(Some(store::decode_snapshot_record(bytes)?)),
        None => Ok(None),
    }
}

/// The writes an install's repair makes under the **receiver's** key prefix
/// (RAFT.md:225-233), in key order: the receiver's own identity, which must survive
/// the switch.
///
/// This is the one place the list is built. [`Assembler::finish`] writes them into
/// the staged store's own table, before its `CURRENT`, and the node's live install
/// carries the same batch through `Engine::install_spans` (D-066, D-068) — two
/// switches of very different shapes, and one statement of what a repair *is*.
/// Building it twice is how the two would come to disagree.
///
/// `replaced_log` is every log index the installed source holds, so a key the kept
/// tail does not write is tombstoned rather than left as the *leader's* entry at
/// that index. A caller whose source carries no log keys passes an empty set and
/// gets no tombstones, which is not the same thing as forgetting them.
// PROPOSED(D-083): one builder for the repair's writes, shared by the staged
// install and the node's live install.
#[must_use]
pub fn repair_writes(
    prefix: &KeyPrefix,
    repair: &Repair,
    last_index: Index,
    last_term: Term,
    config: &Configuration,
    replaced_log: &BTreeSet<Index>,
) -> WriteBatch {
    let record = SnapshotRecord {
        last_index,
        last_term,
        config: config.clone(),
        dir: String::new(),
        taken: false,
        // The receiver's versions start over on the installed store. A later take
        // may then share a name with a directory the receiver took before — a
        // re-seeded server's lost store may have taken at the very index it takes
        // at again — which is harmless because the next incarnation empties every
        // directory its record does not name before any task of it runs (D-043).
        take: 0,
    };
    let mut writes: BTreeMap<Bytes, Value> = BTreeMap::new();
    writes.insert(
        prefix.hard_key(),
        Value::Live(store::encode_hard(repair.term, repair.vote)),
    );
    writes.insert(
        prefix.applied_key(),
        Value::Live(store::encode_applied(last_index)),
    );
    writes.insert(
        prefix.snapshot_key(),
        Value::Live(store::encode_snapshot_record(&record)),
    );
    // The receiver's `<prefix> / 2 / config` key (RAFT.md §3, D-029): the streamed
    // checkpoint carries the leader's, which may name a configuration entry the kept
    // tail does not hold, and the open refuses a key out of step with the log.
    // Rewritten like the rest of tenant 0: the tail's latest configuration entry
    // when it holds one, and otherwise the snapshot's own configuration at its last
    // index, which the record carries.
    let (config_index, in_force) = repair
        .tail
        .iter()
        .fold(None, |kept, entry| match &entry.payload {
            Payload::Config(config) => Some((entry.index, config.clone())),
            _ => kept,
        })
        .unwrap_or_else(|| (last_index, config.clone()));
    writes.insert(
        prefix.config_key(),
        Value::Live(store::encode_config(config_index, &in_force)),
    );
    // The quarantine key is always written explicitly: set when this store's history
    // was ever re-seeded, and a tombstone otherwise — the streamed checkpoint carries
    // the *leader's* tenant 0, and a flag of the leader's must not quarantine the
    // receiver (D-035).
    if repair.quarantined {
        writes.insert(
            prefix.quarantine_key(),
            Value::Live(Bytes::from_static(&[1])),
        );
    } else {
        writes.insert(prefix.quarantine_key(), Value::Tombstone);
    }
    // The incarnation key, for the same reason: the streamed checkpoint carries the
    // leader's number, and the receiver's own must win (D-042).
    writes.insert(
        prefix.incarnation_key(),
        Value::Live(store::encode_incarnation(repair.incarnation)),
    );
    for entry in &repair.tail {
        writes.insert(
            prefix.log_key(entry.index),
            Value::Live(store::encode_entry(entry)),
        );
    }
    for index in replaced_log {
        writes
            .entry(prefix.log_key(*index))
            .or_insert(Value::Tombstone);
    }
    let mut batch = WriteBatch::new();
    for (key, value) in writes {
        match value {
            Value::Live(bytes) => batch.put(key, bytes),
            Value::Tombstone => batch.delete(key),
        };
    }
    batch
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
    variants: Variants,
    /// The group whose Raft state the repair writes: the receiver's own prefix,
    /// not the stream's, which carries the leader's keys under the same one.
    // PROPOSED(D-060): the store parameterised by a key prefix (Q40).
    prefix: KeyPrefix,
    stream: Option<Stream>,
}

impl<E: Environment> Assembler<E> {
    /// An assembler staging under `engine_dir` (see [`staging_dir`]), for the
    /// group `prefix` names.
    // D-045: a variant is a set.
    // PROPOSED(D-060): the store parameterised by a key prefix (Q40).
    pub fn new(
        env: E,
        engine_dir: &Path,
        variants: impl Into<Variants>,
        prefix: KeyPrefix,
    ) -> Self {
        Self {
            env,
            staging: staging_dir(engine_dir),
            variants: variants.into(),
            prefix,
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
        if !is_current || self.variants.contains(Variant::SnapshotWithoutCurrentLast) {
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
        // D-059: the stream's own format, before the manifest and before any
        // table is opened: a checkpoint of another format, or one carrying no
        // record at all, is refused unread and none of its keys is looked at.
        format::check_staged_record(&self.env, &self.staging).await?;
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
            if let Some((seq, value)) = reader.get(&self.prefix.snapshot_key(), u64::MAX).await?
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
        let log_span = self.prefix.purpose_span(store::PURPOSE_LOG);
        let (start, end) = (log_span.start, log_span.end);
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
                if let Some(index) = self.prefix.log_index(&user) {
                    held.insert(index);
                }
            }
        }
        // The repair's writes, from the one builder both installs use
        // ([`repair_writes`]). The staged tables hold the *leader's* log keys, so
        // `held` is what the kept tail must tombstone back out; the node's live
        // install streams no log keys at all and passes an empty set there.
        let writes = repair_writes(
            &self.prefix,
            repair,
            staged.last_index,
            staged.last_term,
            &staged.config,
            &held,
        );
        let mut writer = SstWriter::new();
        // `repair_writes` emits its ops in key order, which is the order
        // `SstWriter::add` requires and the order this loop had when it walked a
        // `BTreeMap`.
        for (key, value) in writes.ops() {
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

    /// Drops the stream and sweeps the staging directory's files: a failed
    /// verify, an identity change, or an install the server decided against. A
    /// `CURRENT` there is left where it is: it is a completed install, one the
    /// server acknowledged and its next start adopts, or a damaged one its next
    /// start refuses — and either way only the next completed install's own
    /// `CURRENT` replaces it, so no start in between finds an unfinished install
    /// and falls back to the old store, which the acknowledged install
    /// superseded (D-041).
    pub async fn abandon(&mut self) {
        self.stream = None;
        let fs = self.env.fs();
        if let Ok(names) = fs.read_dir(&self.staging).await {
            for name in names {
                // D-041: the acknowledged install's commit point stays.
                if name.to_str() == Some("CURRENT") {
                    continue;
                }
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
        incarnation: 0,
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
        incarnation: 0,
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
        incarnation: 0,
    }
}
