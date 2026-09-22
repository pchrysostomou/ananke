//! The store's on-disk format, recorded in a file of its own and read before
//! anything writes (D-059, PROPOSED D-060).
//!
//! A Raft store directory carries [`FORMAT_FILE`], `RAFT-FORMAT`, beside the store
//! marker: two checksummed copies of the format version in one block. It is the
//! first thing a start reads and, in a fresh directory, the first thing a start
//! writes — before the engine creates a single entry there — so that every
//! directory holding any file of a store holds its version too. A store in
//! ananke-raft 0.3.0's format, which records none, is therefore told apart from a
//! store of this build's by the record's presence, and refused unread (D-059);
//! so is a store recording any other version, older or newer.
//!
//! The gate is [`check_format`]. It only reads: it never creates a directory or a
//! file, never writes, removes, renames or syncs, and traces nothing. Its verdict
//! is one of
//!
//! - [`Verdict::Recorded`] — a record naming [`STORE_FORMAT`]; `whole` is false
//!   when one copy is damaged, which [`heal_format`] repairs in place;
//! - [`Verdict::Fresh`] — no record and nothing else in the directory (or no
//!   directory at all), which [`record_format`] turns into a recorded one;
//! - [`Verdict::Damaged`] — a record that cannot be read beside other files:
//!   lost state, never a format (D-044);
//!
//! or the refusal [`FormatRefused`], which names both versions and says what was
//! found: a store's files with no record (0.3.0's, format 1), a record naming
//! another version, or a directory that was never a store at all.
//!
//! # Why two copies and a checksum
//!
//! The simulated disk rots at most one bit per block per crash — the
//! simulator's own fault model, `ananke-env/src/sim/fs.rs:361-381`, which draws
//! `p_bitrot` independently per block and flips one bit of it; SPEC §1.3 asks
//! only that checksums catch bit rot and states no such bound — and the record
//! is one block. A single flip in a copy fails its CRC, so a flip can never read
//! as another version; the other copy still names it, so one crash's rot never
//! costs a healthy voter its store (D-035). Reaching "unreadable" takes two
//! rots, one in each copy, with no start whose heal *became durable* between
//! them: a start that runs to completion heals the first, but on a disk that
//! loses syncs the heal's own sync can be lost and the record stays half
//! damaged, which
//! `a_record_with_one_bad_copy_is_healed_in_place_and_a_crash_never_loses_the_other_copy`
//! measures (D-060).
//!
//! # Permanence
//!
//! The file name, the magic, the 37-byte copy, the two offsets, and the rule that
//! every checkpoint and staged install carries a record are permanent: only the
//! version value changes, and no build rewrites a record in place with a different
//! version. So any build can read any build's version, a record that does not
//! decode can only be damage rather than an encoding from the future, and a
//! CRC-valid copy naming another version is always a refusal.
// PROPOSED(D-060): the key layout of Q5, and the store's format record read
// before anything writes.

use std::io;
use std::path::{Path, PathBuf};

use ananke_env::{Environment, File, FileSystem, OpenOptions};
use ananke_storage::crc32c::crc32c;
use bytes::{BufMut, Bytes, BytesMut};

use crate::store::{Damage, LostState};

/// The record's file name, in the store directory beside
/// [`STORE_MARKER`](crate::store::STORE_MARKER). Permanent: every later format
/// keeps it.
// PROPOSED(D-060)
pub const FORMAT_FILE: &str = "RAFT-FORMAT";

/// The name the record is written under before it is renamed into place.
// PROPOSED(D-060)
pub const FORMAT_TMP: &str = "RAFT-FORMAT.tmp";

/// The Raft store format this build reads and writes: the layout of RAFT.md §3
/// as Stage A's item 6 leaves it.
// D-059: a store in another format is refused at open, never migrated.
pub const STORE_FORMAT: u64 = 2;

/// The format of a store that records none: ananke-raft 0.3.0's.
// D-059: a store in another format is refused at open, never migrated.
pub const UNRECORDED_FORMAT: u64 = 1;

/// One copy of the record: the magic, the version and the checksum over both.
// PROPOSED(D-060)
pub const COPY_LEN: usize = 37;

/// The whole record: two copies, at offsets 0 and [`COPY_LEN`].
// PROPOSED(D-060)
pub const RECORD_LEN: usize = 74;

/// What every copy begins with. Permanent (see the module documentation).
const MAGIC: &[u8] = b"ananke raft store format\n";

/// The record's path under `dir`.
#[must_use]
pub fn format_path(dir: &Path) -> PathBuf {
    dir.join(FORMAT_FILE)
}

/// The record's temporary path under `dir`.
#[must_use]
fn format_tmp_path(dir: &Path) -> PathBuf {
    dir.join(FORMAT_TMP)
}

/// The record naming `version`: two copies of the magic, the version as a
/// little-endian `u64` and the CRC-32C of the two, at offsets 0 and [`COPY_LEN`].
#[must_use]
// PROPOSED(D-060)
pub fn encode_record(version: u64) -> Bytes {
    let mut copy = BytesMut::with_capacity(COPY_LEN);
    copy.put_slice(MAGIC);
    copy.put_u64_le(version);
    let checksum = crc32c(&copy);
    copy.put_u32_le(checksum);
    let mut out = BytesMut::with_capacity(RECORD_LEN);
    out.put_slice(&copy);
    out.put_slice(&copy);
    out.freeze()
}

/// The version one copy names, if that copy is whole and its checksum matches.
fn copy_version(bytes: &[u8], at: usize) -> Option<u64> {
    let copy = bytes.get(at..at + COPY_LEN)?;
    if &copy[..MAGIC.len()] != MAGIC {
        return None;
    }
    let version = u64::from_le_bytes(copy[MAGIC.len()..MAGIC.len() + 8].try_into().ok()?);
    let checksum = u32::from_le_bytes(copy[COPY_LEN - 4..].try_into().ok()?);
    (crc32c(&copy[..COPY_LEN - 4]) == checksum).then_some(version)
}

/// What a record's bytes say.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// PROPOSED(D-060)
pub enum Decoded {
    /// A version both valid copies agree on, or the one copy that is valid.
    /// `whole` is true only when both copies are valid and the length is
    /// exactly [`RECORD_LEN`]: anything else is healed in place at the start.
    Valid {
        /// The version the record names.
        version: u64,
        /// Whether the record needs no repair.
        whole: bool,
    },
    /// Two valid copies naming different versions. Our own writes never produce
    /// this, and rot cannot: a flip fails a copy's checksum.
    Conflicting {
        /// The two versions, in copy order.
        versions: [u64; 2],
    },
    /// Neither copy is valid: damage, never an encoding this build predates.
    Unreadable,
}

/// What `bytes` say, by the rules in the module documentation.
#[must_use]
// PROPOSED(D-060)
pub fn decode_record(bytes: &[u8]) -> Decoded {
    match (copy_version(bytes, 0), copy_version(bytes, COPY_LEN)) {
        (Some(first), Some(second)) if first == second => Decoded::Valid {
            version: first,
            whole: bytes.len() == RECORD_LEN,
        },
        (Some(first), Some(second)) => Decoded::Conflicting {
            versions: [first, second],
        },
        (Some(version), None) | (None, Some(version)) => Decoded::Valid {
            version,
            whole: false,
        },
        (None, None) => Decoded::Unreadable,
    }
}

/// A directory that holds no store and no readable record: its first write is its
/// record ([`record_format`]). Built only by [`check_format`], so no caller can
/// call a directory fresh that the gate has not.
#[derive(Clone, Debug)]
// PROPOSED(D-060)
pub struct FreshDir {
    dir: PathBuf,
}

impl FreshDir {
    /// The directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// The proof that a directory's format was read and is this build's: what
/// [`RaftStore::open`](crate::store::RaftStore::open) requires before it reads a
/// key. It carries the directory it was taken for, so a token of one store can
/// never let another's keys be read. Built only by [`check_format`],
/// [`record_format`] and the server's own start.
#[derive(Clone, Debug)]
// PROPOSED(D-060)
pub struct FormatChecked {
    dir: PathBuf,
}

impl FormatChecked {
    /// The directory whose format was checked.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The token for `dir`: the start's own, after a rewrite made the record
    /// whole.
    pub(crate) fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }
}

/// What the gate found.
#[derive(Clone, Debug)]
// PROPOSED(D-060)
pub enum Verdict {
    /// No record and nothing else, or an unreadable record and nothing else: a
    /// directory with no state to lose, whose first write is its record.
    Fresh(FreshDir),
    /// A record naming [`STORE_FORMAT`], with the length it was read at;
    /// `whole` is false when one copy is damaged or the length is off.
    Recorded {
        /// The proof the directory's format is this build's.
        checked: FormatChecked,
        /// Whether both copies are valid and the length is [`RECORD_LEN`].
        whole: bool,
        /// The record's length on disk, which a heal truncates back.
        len: u64,
    },
    /// A record that does not decode, beside other entries: lost state, never a
    /// format (D-044).
    Damaged,
}

/// Which directory a refusal is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// D-059: a store in another format is refused at open, never migrated.
pub enum Subject {
    /// The store directory itself.
    Store,
    /// A staged install under it.
    StagedInstall,
}

/// What the gate found instead of this build's format.
#[derive(Clone, Debug, PartialEq, Eq)]
// D-059: a store in another format is refused at open, never migrated.
pub enum Found {
    /// A store's files and no record: ananke-raft 0.3.0's, format 1.
    Unrecorded,
    /// A checksum-valid record naming this version.
    Recorded(u64),
    /// No record and no store file, but not empty: these names.
    Foreign(Vec<String>),
}

/// A directory this build will not read: it is in another Raft store format, or
/// it is not a Raft store at all. Nothing is written to it — no log segment, no
/// marker, no lost mark — and it is not a loss: what becomes of it is its
/// operator's decision (D-059).
#[derive(Clone, Debug, PartialEq, Eq)]
// D-059: a store in another format is refused at open, never migrated.
pub struct FormatRefused {
    /// The directory refused.
    pub dir: PathBuf,
    /// Whether it is the store or a staged install under it.
    pub subject: Subject,
    /// What was found there.
    pub found: Found,
    /// The format this build reads: [`STORE_FORMAT`].
    pub expected: u64,
}

impl FormatRefused {
    /// The refusal an I/O error carries, if it is one.
    #[must_use]
    pub fn from_io(error: &io::Error) -> Option<FormatRefused> {
        error.get_ref()?.downcast_ref::<FormatRefused>().cloned()
    }

    /// This refusal as the `InvalidData` error the start fails with, which
    /// [`from_io`](Self::from_io) reads back.
    pub(crate) fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, self)
    }

    /// The refusal for a record naming `version`.
    fn recorded(dir: &Path, subject: Subject, version: u64) -> io::Error {
        Self {
            dir: dir.to_path_buf(),
            subject,
            found: Found::Recorded(version),
            expected: STORE_FORMAT,
        }
        .into_io()
    }

    /// The refusal for a store's files with no record: 0.3.0's.
    fn unrecorded(dir: &Path, subject: Subject) -> io::Error {
        Self {
            dir: dir.to_path_buf(),
            subject,
            found: Found::Unrecorded,
            expected: STORE_FORMAT,
        }
        .into_io()
    }
}

/// The version a decoded record is refused for: the one that is not this
/// build's, and the higher of two when neither is.
fn refused_version(decoded: Decoded) -> Option<u64> {
    match decoded {
        Decoded::Valid { version, .. } if version != STORE_FORMAT => Some(version),
        Decoded::Conflicting { versions: [a, b] } => match (a == STORE_FORMAT, b == STORE_FORMAT) {
            (true, false) => Some(b),
            (false, true) => Some(a),
            _ => Some(a.max(b)),
        },
        Decoded::Valid { .. } | Decoded::Unreadable => None,
    }
}

/// How many of a foreign directory's names the refusal spells out. The message
/// is read by an operator and copied into a trace record, and the directory it
/// describes is one this build knows nothing about, so its length is bounded
/// here rather than by what is in the directory.
// PROPOSED(D-060)
const FOREIGN_NAMES_SHOWN: usize = 8;

impl std::fmt::Display for FormatRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.subject {
            Subject::Store => "the store in",
            Subject::StagedInstall => "the staged install in",
        };
        let dir = self.dir.display();
        let expected = self.expected;
        match &self.found {
            // A staging carries no version of its own to attribute: this build's
            // stream writes the record first, so a staging without one is not
            // 0.3.0's install but an install of unknown provenance (D-060).
            Found::Unrecorded if self.subject == Subject::StagedInstall => write!(
                f,
                "{what} {dir} holds an install's files and no {FORMAT_FILE} record, which this \
                 build's own stream writes before any table: it names no format version, so it \
                 is refused unread rather than adopted as format {expected}; remove {dir} and \
                 let the leader stream the install again"
            ),
            Found::Unrecorded => write!(
                f,
                "{what} {dir} holds a Raft store's files and no {FORMAT_FILE} record: it is in \
                 Raft store format {UNRECORDED_FORMAT}, ananke-raft 0.3.0's, which records no \
                 format version; this build reads format {expected} only and refuses the store \
                 rather than read or migrate it"
            ),
            Found::Recorded(version) if *version > expected => write!(
                f,
                "{what} {dir} records Raft store format {version}, newer than format {expected}, \
                 the only one this build reads; it is refused unread"
            ),
            Found::Recorded(version) => write!(
                f,
                "{what} {dir} records Raft store format {version}; this build reads format \
                 {expected} only and refuses the store rather than read or migrate it"
            ),
            // The names are the operator's evidence, so some are named; the list
            // is capped because it is an operator's line and a trace record, and
            // a directory can hold any number of entries (D-060).
            Found::Foreign(names) => write!(
                f,
                "the directory {dir} holds {}{} and neither a Raft store's files nor a \
                 {FORMAT_FILE} record: refused rather than started as a new format {expected} \
                 store; point the store at an empty directory",
                names
                    .iter()
                    .take(FOREIGN_NAMES_SHOWN)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
                match names.len().saturating_sub(FOREIGN_NAMES_SHOWN) {
                    0 => String::new(),
                    rest => format!(" and {rest} more"),
                }
            ),
        }
    }
}

impl std::error::Error for FormatRefused {}

/// Whether `name` is a name of the store family: a file the engine, the store or
/// the snapshot machinery writes. A directory holding one of these and no record
/// held a store in a format that records none — 0.3.0's.
fn is_store_family(name: &str) -> bool {
    name == "CURRENT"
        || name == "CURRENT.tmp"
        || name == crate::store::STORE_MARKER
        || name == "install"
        || name.starts_with("MANIFEST-")
        || name.starts_with("snap-")
        || name.ends_with(".sst")
        || name.ends_with(".wal")
}

/// The record's bytes as they are on disk, or `None` when there is no record.
async fn read_record<E: Environment>(env: &E, dir: &Path) -> io::Result<Option<Bytes>> {
    let file = match env
        .fs()
        .open(&format_path(dir), OpenOptions::new().read(true))
        .await
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let size = file.size().await?;
    let len = usize::try_from(size).unwrap_or(usize::MAX).min(4096);
    Ok(Some(file.read_at(0, len).await?))
}

/// What `dir`'s format is, read before anything writes (D-059).
///
/// Three filesystem operations on a healthy start: the open, the size and the
/// read. A directory with no record is listed as well, to tell a fresh directory
/// from a store of 0.3.0's and from one that was never a store.
///
/// # Errors
///
/// `InvalidData` carrying a [`FormatRefused`] for a directory this build will
/// not read; otherwise the filesystem's, which stops the server — nothing is
/// guessed from a failed read.
// D-059: the format is read before anything writes, and before lost state.
pub async fn check_format<E: Environment>(env: &E, dir: &Path) -> io::Result<Verdict> {
    let record = read_record(env, dir).await?;
    let unreadable = match &record {
        Some(bytes) => match decode_record(bytes) {
            Decoded::Valid {
                version: STORE_FORMAT,
                whole,
            } => {
                return Ok(Verdict::Recorded {
                    checked: FormatChecked::new(dir),
                    whole,
                    len: bytes.len() as u64,
                });
            }
            decoded @ (Decoded::Valid { .. } | Decoded::Conflicting { .. }) => {
                let version = refused_version(decoded).expect("another version");
                return Err(FormatRefused::recorded(dir, Subject::Store, version));
            }
            Decoded::Unreadable => true,
        },
        None => false,
    };
    let names = match env.fs().read_dir(dir).await {
        Ok(names) => names,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Verdict::Fresh(FreshDir {
                dir: dir.to_path_buf(),
            }));
        }
        Err(error) => return Err(error),
    };
    let others: Vec<String> = names
        .iter()
        .filter_map(|name| name.to_str())
        .filter(|name| *name != FORMAT_FILE && *name != FORMAT_TMP)
        .map(ToOwned::to_owned)
        .collect();
    if others.is_empty() {
        // Nothing to protect: a fresh directory, or one whose brand-new record
        // was torn by a lost fsync before anything else of a store existed.
        return Ok(Verdict::Fresh(FreshDir {
            dir: dir.to_path_buf(),
        }));
    }
    if unreadable {
        // A record that is there and cannot be read, beside a store: damage,
        // which is lost state (D-044), never another format. Only this build's
        // format is ever written, and a record that does not decode can only be
        // damage (the permanence rule).
        return Ok(Verdict::Damaged);
    }
    if others.iter().any(|name| is_store_family(name)) {
        return Err(FormatRefused::unrecorded(dir, Subject::Store));
    }
    Err(FormatRefused {
        dir: dir.to_path_buf(),
        subject: Subject::Store,
        found: Found::Foreign(others),
        expected: STORE_FORMAT,
    }
    .into_io())
}

/// Writes `dir`'s record: the first write of a fresh store, before the engine
/// creates any entry there, so every durable directory holding a file of a store
/// holds its version too.
///
/// Tmp, sync, rename, directory sync. A crash keeps a prefix of the directory's
/// pending operations, so what survives is nothing, the tmp file, or the record;
/// a lost fsync can leave the renamed record torn, which decodes as damage or as
/// one bad copy, never as another version.
///
/// # Errors
///
/// The filesystem's.
// PROPOSED(D-060)
pub async fn record_format<E: Environment>(env: &E, fresh: FreshDir) -> io::Result<FormatChecked> {
    env.fs().create_dir_all(&fresh.dir).await?;
    write_record_through_tmp(env, &fresh.dir).await?;
    Ok(FormatChecked::new(&fresh.dir))
}

/// Rewrites a record that has no valid copy: the adoption's, after the installed
/// store's `CURRENT` switch. The directory exists and holds a store.
// PROPOSED(D-060)
pub(crate) async fn rewrite_format<E: Environment>(
    env: &E,
    dir: &Path,
) -> io::Result<FormatChecked> {
    write_record_through_tmp(env, dir).await?;
    Ok(FormatChecked::new(dir))
}

/// Tmp, sync, rename, directory sync.
async fn write_record_through_tmp<E: Environment>(env: &E, dir: &Path) -> io::Result<()> {
    let fs = env.fs();
    let tmp = format_tmp_path(dir);
    let file = fs
        .open(
            &tmp,
            OpenOptions::new().write(true).create(true).truncate(true),
        )
        .await?;
    file.write_at(0, encode_record(STORE_FORMAT)).await?;
    file.sync().await?;
    fs.rename(&tmp, &format_path(dir)).await?;
    fs.sync_dir(dir).await
}

/// Repairs a record one of whose copies is damaged, in place and with
/// byte-identical content, so that a torn write over it can only leave bytes the
/// valid copy already had and a lost sync leaves the old ones. A heal therefore
/// never turns a readable record into an unreadable one. `len` is the length the
/// gate read; anything past [`RECORD_LEN`] is truncated away.
///
/// # Errors
///
/// The filesystem's.
// PROPOSED(D-060)
pub async fn heal_format<E: Environment>(
    env: &E,
    checked: &FormatChecked,
    len: u64,
) -> io::Result<()> {
    let file = env
        .fs()
        .open(&format_path(&checked.dir), OpenOptions::new().write(true))
        .await?;
    file.write_at(0, encode_record(STORE_FORMAT)).await?;
    if len > RECORD_LEN as u64 {
        file.set_size(RECORD_LEN as u64).await?;
    }
    file.sync().await
}

/// Writes a checkpoint's own record, after
/// [`Engine::checkpoint`](ananke_storage::Engine::checkpoint), which requires an
/// empty directory. There is no temporary name: a leftover one would be listed
/// and streamed. A crash before it leaves a checkpoint that
/// [`checkpoint_complete`](crate::snapshot::checkpoint_complete) calls
/// incomplete, which costs a retake and never streams an unversioned checkpoint.
///
/// # Errors
///
/// The filesystem's.
// PROPOSED(D-060)
pub async fn write_checkpoint_record<E: Environment>(env: &E, dir: &Path) -> io::Result<()> {
    let fs = env.fs();
    let file = fs
        .open(
            &format_path(dir),
            OpenOptions::new().write(true).create(true).truncate(true),
        )
        .await?;
    file.write_at(0, encode_record(STORE_FORMAT)).await?;
    file.sync().await?;
    fs.sync_dir(dir).await
}

/// Whether the record in `dir` reads as this build's format: what a complete
/// checkpoint carries beside its `CURRENT`.
///
/// # Errors
///
/// The filesystem's, other than a missing file.
// PROPOSED(D-060)
pub(crate) async fn records_this_format<E: Environment>(env: &E, dir: &Path) -> io::Result<bool> {
    Ok(match read_record(env, dir).await? {
        Some(bytes) => matches!(
            decode_record(&bytes),
            Decoded::Valid {
                version: STORE_FORMAT,
                ..
            }
        ),
        None => false,
    })
}

/// The staged install's own record, read before either adoption's first write
/// and by [`Assembler::verify`](crate::snapshot::Assembler): a stream of another
/// format is refused unread, with nothing of the store touched, and one whose
/// record cannot be read is staging damage, refused and never swept (D-041).
///
/// A staging directory with no record at all is unreachable for this build's own
/// stagings — the record is streamed first, and the sweeps that could remove it
/// remove every table before it — so it too is refused rather than swept.
///
/// # Errors
///
/// `InvalidData` carrying a [`FormatRefused`] for another format or none, or a
/// [`LostState`] for a record that cannot be read; otherwise the filesystem's.
// D-059: the format is read before anything writes.
pub(crate) async fn check_staged_record<E: Environment>(env: &E, staging: &Path) -> io::Result<()> {
    let Some(bytes) = read_record(env, staging).await? else {
        return Err(FormatRefused::unrecorded(staging, Subject::StagedInstall));
    };
    match decode_record(&bytes) {
        Decoded::Valid {
            version: STORE_FORMAT,
            ..
        } => Ok(()),
        Decoded::Unreadable => {
            Err(LostState::from_damage(Damage::StagingFormatUnreadable).into_io())
        }
        decoded => Err(FormatRefused::recorded(
            staging,
            Subject::StagedInstall,
            refused_version(decoded).expect("another version"),
        )),
    }
}
