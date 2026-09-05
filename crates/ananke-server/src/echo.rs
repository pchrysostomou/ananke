//! The Phase 0 echo protocol (SPEC.md §1.6). Every node pings a random peer at a fixed
//! interval and answers every ping with a pong carrying the same sequence number. With
//! a [`Journal`] configured, a node also appends every ping it sends to a checksummed
//! journal on disk and replays what it finds there at start, so the SPEC §1.3
//! filesystem faults have something to act on.
//!
//! [`node`] is generic over [`Environment`]: `sim/echo.rs` runs it under the simulator
//! with faults, and `ananke-server echo` runs it on `RealEnv` across real processes.
//! The [`Stats`] a node keeps are the protocol-level invariants both harnesses check.

use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ananke_env::{
    Clock, Either, Environment, File, FileSystem, Network, OpenOptions, Rng, Socket, race,
};
use bytes::Bytes;

/// One node's configuration.
#[derive(Clone, Debug)]
pub struct Echo {
    /// The address to bind; peers see it as the sender.
    pub listen: SocketAddr,
    /// Who to ping. With no peers the node only answers.
    pub peers: Vec<SocketAddr>,
    /// How often to ping.
    pub interval: Duration,
    /// Distinguishes sequence numbers across restarts, so a late pong for a
    /// pre-restart ping is still recognised rather than counted as unknown.
    pub incarnation: u32,
    /// Where to journal the pings sent, if anywhere.
    pub journal: Option<Journal>,
}

/// The journal of pings sent: `journal` in a directory, rotated to `journal.prev`.
///
/// It exists so the §1.3 fault model has something to act on. Records are synced
/// every `sync_every` writes, so a crash finds pending writes to keep, tear or drop.
/// Every record carries a checksum, so bit rot is caught at replay ([`replay`]).
/// Rotation renames the file and creates a new one; with `sync_dir_on_rotate` the
/// directory is synced after each, which is what a correct program does and what
/// `ananke-server` ships. Without it the rename and the create stay pending, so a
/// crash finds directory operations to lose: the known bug the echo scenario runs as
/// its negative control, next to the correct variant as the positive one.
#[derive(Clone, Debug)]
pub struct Journal {
    /// The directory holding `journal` and `journal.prev`; created if missing.
    pub dir: PathBuf,
    /// Records between syncs; 0 never syncs.
    pub sync_every: u64,
    /// Records per file before rotating; 0 never rotates.
    pub rotate_every: u64,
    /// Whether rotation syncs the directory after the rename and after the create.
    /// `false` is a bug, kept so the fault model can be shown to catch it.
    pub sync_dir_on_rotate: bool,
}

impl Journal {
    /// The file being appended to.
    pub const CURRENT: &str = "journal";
    /// Where the last rotation moved the previous file.
    pub const PREVIOUS: &str = "journal.prev";

    /// The path of the current file.
    #[must_use]
    pub fn current(&self) -> PathBuf {
        self.dir.join(Self::CURRENT)
    }

    /// The path of the previous file.
    #[must_use]
    pub fn previous(&self) -> PathBuf {
        self.dir.join(Self::PREVIOUS)
    }
}

/// The size of one journal record: the sequence number and its checksum, both
/// little-endian `u64`.
pub const RECORD_LEN: usize = 16;

/// FNV-1a. A single flipped bit always changes it: every step is an XOR followed by
/// multiplication by an odd constant, both bijections on `u64`.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Encodes one record.
#[must_use]
pub fn encode_record(seq: u64) -> [u8; RECORD_LEN] {
    let mut record = [0u8; RECORD_LEN];
    record[..8].copy_from_slice(&seq.to_le_bytes());
    record[8..].copy_from_slice(&fnv1a(&seq.to_le_bytes()).to_le_bytes());
    record
}

/// What replaying one journal file found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Replay {
    /// Records whose checksum matched.
    pub valid: u64,
    /// Records whose checksum did not: bit rot.
    pub corrupt: u64,
    /// Whether the file ended in a partial record: a torn write.
    pub torn: bool,
}

/// Replays the bytes of a journal file: a record is valid if its checksum matches,
/// corrupt otherwise; a trailing partial record is torn and is neither.
#[must_use]
pub fn replay(bytes: &[u8]) -> Replay {
    let mut found = Replay::default();
    let (records, tail) = bytes.as_chunks::<RECORD_LEN>();
    for record in records {
        let seq = u64::from_le_bytes(record[..8].try_into().expect("8 bytes"));
        if *record == encode_record(seq) {
            found.valid += 1;
        } else {
            found.corrupt += 1;
        }
    }
    found.torn = !tail.is_empty();
    found
}

/// What a node saw of its journal. The replay fields describe the disk at the latest
/// start; the counters accumulate across the incarnations sharing the [`Stats`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalStats {
    /// Whether `journal` existed at start: `false` on a fresh disk, or after a crash
    /// lost its directory entry.
    pub found: bool,
    /// Whether `journal.prev` existed at start.
    pub found_previous: bool,
    /// Valid records across both files.
    pub valid: u64,
    /// Corrupt records across both files.
    pub corrupt: u64,
    /// Files that ended in a partial record.
    pub torn: u64,
    /// Records appended.
    pub written: u64,
    /// Rotations performed.
    pub rotations: u64,
}

/// What one node observed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Pings sent and not yet answered, as (peer, sequence number).
    outstanding: BTreeSet<(SocketAddr, u64)>,
    /// Pings sent.
    pub pings_sent: u64,
    /// Pongs that answered an outstanding ping.
    pub pongs_received: u64,
    /// Pongs that matched no outstanding ping: fabricated or duplicated.
    pub unknown_pongs: u64,
    /// Messages that did not parse.
    pub garbage: u64,
    /// The journal, once a node with one configured has started.
    pub journal: Option<JournalStats>,
}

impl Stats {
    /// Pings still waiting for an answer.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.outstanding.len()
    }

    /// The protocol-level invariants, or a description of the first violation.
    ///
    /// # Errors
    ///
    /// A message naming the violated invariant.
    pub fn check(&self) -> Result<(), String> {
        if self.unknown_pongs > 0 {
            return Err(format!(
                "{} pongs matched no outstanding ping (fabricated or duplicated)",
                self.unknown_pongs
            ));
        }
        if self.garbage > 0 {
            return Err(format!("{} messages failed to parse", self.garbage));
        }
        if self.pongs_received == 0 || self.pongs_received > self.pings_sent {
            return Err(format!(
                "{} pongs for {} pings",
                self.pongs_received, self.pings_sent
            ));
        }
        Ok(())
    }
}

/// [`Stats`] shared between a running node and the harness observing it.
pub type SharedStats = Arc<Mutex<Stats>>;

/// Locks shared stats, ignoring poisoning.
pub fn lock(stats: &SharedStats) -> MutexGuard<'_, Stats> {
    stats.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The wire format: `ping <seq>` and `pong <seq>` as text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    /// A request, carrying the sender's sequence number.
    Ping(u64),
    /// The answer, carrying the ping's sequence number.
    Pong(u64),
}

impl Message {
    /// Encodes for the wire.
    #[must_use]
    pub fn encode(self) -> Bytes {
        match self {
            Message::Ping(seq) => Bytes::from(format!("ping {seq}")),
            Message::Pong(seq) => Bytes::from(format!("pong {seq}")),
        }
    }

    /// Decodes from the wire; `None` for anything that is not a well-formed message.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Message> {
        let text = std::str::from_utf8(bytes).ok()?;
        let (kind, seq) = text.split_once(' ')?;
        let seq = seq.parse().ok()?;
        match kind {
            "ping" => Some(Message::Ping(seq)),
            "pong" => Some(Message::Pong(seq)),
            _ => None,
        }
    }
}

/// A journal open for appending.
struct OpenJournal<F> {
    config: Journal,
    file: F,
    /// Where the next record goes.
    offset: u64,
    /// Records in the current file.
    in_file: u64,
    /// Records since the last sync.
    since_sync: u64,
}

/// Replays what is on disk, records the outcome in `stats`, and opens `journal` for
/// appending. A torn tail is cut off so later records stay aligned. The entry of a
/// freshly created file is made durable here; it is rotation that leaves it pending.
async fn open_journal<E: Environment>(
    env: &E,
    config: &Journal,
    stats: &SharedStats,
) -> io::Result<OpenJournal<<E::Fs as FileSystem>::File>> {
    let fs = env.fs();
    fs.create_dir_all(&config.dir).await?;
    let mut found = [false; 2];
    let mut replayed = JournalStats::default();
    for (slot, path) in [config.previous(), config.current()].iter().enumerate() {
        let file = match fs.open(path, OpenOptions::new().read(true)).await {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        found[slot] = true;
        let size = file.size().await?;
        let bytes = file
            .read_at(0, usize::try_from(size).unwrap_or(usize::MAX))
            .await?;
        let Replay {
            valid,
            corrupt,
            torn,
        } = replay(&bytes);
        replayed.valid += valid;
        replayed.corrupt += corrupt;
        replayed.torn += u64::from(torn);
    }
    let file = fs
        .open(
            &config.current(),
            OpenOptions::new().read(true).write(true).create(true),
        )
        .await?;
    let size = file.size().await?;
    let aligned = size - size % RECORD_LEN as u64;
    if aligned != size {
        file.set_size(aligned).await?;
        file.sync().await?;
    }
    fs.sync_dir(&config.dir).await?;
    {
        let mut stats = lock(stats);
        let journal = stats.journal.get_or_insert_with(JournalStats::default);
        journal.found = found[1];
        journal.found_previous = found[0];
        journal.valid = replayed.valid;
        journal.corrupt = replayed.corrupt;
        journal.torn = replayed.torn;
    }
    Ok(OpenJournal {
        config: config.clone(),
        file,
        offset: aligned,
        in_file: aligned / RECORD_LEN as u64,
        since_sync: 0,
    })
}

impl<F: File> OpenJournal<F> {
    /// Appends one record, syncing and rotating as configured.
    async fn append<E: Environment<Fs: FileSystem<File = F>>>(
        &mut self,
        env: &E,
        seq: u64,
        stats: &SharedStats,
    ) -> io::Result<()> {
        self.file
            .write_at(self.offset, Bytes::copy_from_slice(&encode_record(seq)))
            .await?;
        self.offset += RECORD_LEN as u64;
        self.in_file += 1;
        self.since_sync += 1;
        lock(stats)
            .journal
            .get_or_insert_with(JournalStats::default)
            .written += 1;
        if self.config.sync_every > 0 && self.since_sync >= self.config.sync_every {
            self.file.sync().await?;
            self.since_sync = 0;
        }
        if self.config.rotate_every > 0 && self.in_file >= self.config.rotate_every {
            let fs = env.fs();
            fs.rename(&self.config.current(), &self.config.previous())
                .await?;
            if self.config.sync_dir_on_rotate {
                fs.sync_dir(&self.config.dir).await?;
            }
            self.file = fs
                .open(
                    &self.config.current(),
                    OpenOptions::new().read(true).write(true).create_new(true),
                )
                .await?;
            if self.config.sync_dir_on_rotate {
                fs.sync_dir(&self.config.dir).await?;
            }
            self.offset = 0;
            self.in_file = 0;
            self.since_sync = 0;
            lock(stats)
                .journal
                .get_or_insert_with(JournalStats::default)
                .rotations += 1;
        }
        Ok(())
    }
}

/// Runs one echo node until its socket closes or the task is aborted. A node that
/// cannot bind, or cannot open its journal, returns at once.
pub async fn node<E: Environment>(env: E, echo: Echo, stats: SharedStats) {
    let Ok(sock) = env.net().bind(echo.listen).await else {
        return;
    };
    let mut journal = match &echo.journal {
        Some(config) => match open_journal(&env, config, &stats).await {
            Ok(open) => Some(open),
            Err(_) => return,
        },
        None => None,
    };
    let mut seq = u64::from(echo.incarnation) << 32;
    let mut next_ping = env.clock().now();
    loop {
        let recv = pin!(sock.recv());
        let timer = pin!(env.clock().sleep_until(next_ping));
        match race(&env, recv, timer).await {
            Either::Left(Err(_)) => return,
            Either::Left(Ok((from, msg))) => match Message::decode(&msg) {
                Some(Message::Ping(n)) => {
                    let _ = sock.send(from, Message::Pong(n).encode()).await;
                }
                Some(Message::Pong(n)) => {
                    let mut stats = lock(&stats);
                    if stats.outstanding.remove(&(from, n)) {
                        stats.pongs_received += 1;
                    } else {
                        stats.unknown_pongs += 1;
                    }
                }
                None => lock(&stats).garbage += 1,
            },
            Either::Right(()) => {
                next_ping = env.clock().now() + echo.interval;
                if echo.peers.is_empty() {
                    continue;
                }
                // The peer comes from this node's own seeded stream.
                let index = usize::try_from(env.rng().below(echo.peers.len() as u64))
                    .expect("index fits usize");
                let peer = echo.peers[index];
                seq += 1;
                {
                    let mut stats = lock(&stats);
                    stats.outstanding.insert((peer, seq));
                    stats.pings_sent += 1;
                }
                if let Some(open) = &mut journal {
                    let _ = open.append(&env, seq, &stats).await;
                }
                let _ = sock.send(peer, Message::Ping(seq).encode()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_round_trip_and_reject_garbage() {
        for message in [
            Message::Ping(0),
            Message::Pong(u64::MAX),
            Message::Ping(1 << 32 | 7),
        ] {
            assert_eq!(Message::decode(&message.encode()), Some(message));
        }
        for garbage in ["", "ping", "ping x", "pang 1", "ping 1 2"] {
            assert_eq!(Message::decode(garbage.as_bytes()), None, "{garbage:?}");
        }
    }

    #[test]
    fn stats_check_catches_each_violation() {
        let mut stats = Stats {
            pings_sent: 10,
            pongs_received: 5,
            ..Stats::default()
        };
        assert_eq!(stats.check(), Ok(()));
        stats.unknown_pongs = 1;
        assert!(
            stats
                .check()
                .unwrap_err()
                .contains("fabricated or duplicated")
        );
        stats.unknown_pongs = 0;
        stats.garbage = 1;
        assert!(stats.check().unwrap_err().contains("failed to parse"));
        stats.garbage = 0;
        stats.pongs_received = 0;
        assert!(stats.check().unwrap_err().contains("0 pongs"));
        stats.pongs_received = 11;
        assert!(stats.check().unwrap_err().contains("11 pongs for 10 pings"));
    }

    #[test]
    fn replay_counts_valid_corrupt_and_torn_records() {
        let mut bytes = Vec::new();
        for seq in [1, 2, 3, u64::MAX] {
            bytes.extend_from_slice(&encode_record(seq));
        }
        assert_eq!(
            replay(&bytes),
            Replay {
                valid: 4,
                corrupt: 0,
                torn: false
            }
        );
        // A partial record at the end is torn, not corrupt.
        bytes.extend_from_slice(&encode_record(5)[..RECORD_LEN - 1]);
        assert_eq!(
            replay(&bytes),
            Replay {
                valid: 4,
                corrupt: 0,
                torn: true
            }
        );
        assert_eq!(replay(&[]), Replay::default());
    }

    /// The checksum catches every single-bit flip anywhere in a record, which is
    /// exactly what the simulated bit rot does.
    #[test]
    fn every_single_bit_flip_is_caught() {
        for seq in [0, 1, 0x8000_0000_0000_0000, u64::MAX, 0x1234_5678_9abc_def0] {
            let record = encode_record(seq);
            for byte in 0..RECORD_LEN {
                for bit in 0..8 {
                    let mut flipped = record;
                    flipped[byte] ^= 1 << bit;
                    let found = replay(&flipped);
                    assert_eq!(
                        (found.valid, found.corrupt),
                        (0, 1),
                        "seq {seq} byte {byte} bit {bit}"
                    );
                }
            }
        }
    }
}
