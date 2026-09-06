//! [`TraceEvent`]: what the environment records for moirae (SPEC.md §1.5).

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use bytes::Bytes;

use crate::{Instant, NodeId, TaskId};

/// Identifies one message from its send to its delivery or drop, within one
/// environment. Assigned at send; the same id appears on the receiving side, so the
/// three events of a message correlate (moirae's `msgId`, SPEC §1.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(u64);

impl MessageId {
    /// Wraps a raw id.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&format!("msg#{}", self.0))
    }
}

/// A state transition worth seeing in the moirae studio.
///
/// The environment stamps each event with the node and the time it was recorded, so
/// those are not part of the event. The enum grows with each phase (range id, term, log
/// index, transaction id); it is `non_exhaustive` so downstream matches carry a wildcard
/// arm and keep compiling.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TraceEvent {
    /// A task was handed to the scheduler.
    TaskSpawned {
        /// The new task's id.
        task: TaskId,
        /// The name given to `Environment::spawn`.
        name: &'static str,
    },
    /// The scheduler chose this task to poll next. Simulation only: this is the
    /// scheduling decision SPEC.md §1.1 says must be in the trace.
    TaskPolled {
        /// The polled task's id.
        task: TaskId,
    },
    /// A task's future ran to completion. Aborted tasks do not produce this event.
    TaskCompleted {
        /// The finished task's id.
        task: TaskId,
    },
    /// Virtual time jumped forward to the next timer or delivery. Simulation only.
    TimeAdvanced {
        /// The new global virtual time.
        to: Instant,
    },
    /// A socket accepted a message for sending. It may still be lost.
    MessageSent {
        /// The message's id.
        id: MessageId,
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The destination address.
        to: SocketAddr,
        /// The payload, so the trace can be decoded later; `Bytes` is reference-counted.
        payload: Bytes,
    },
    /// A message reached the destination socket's receive queue.
    MessageDelivered {
        /// The message's id.
        id: MessageId,
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The receiving socket's bound address.
        to: SocketAddr,
        /// Payload length in bytes.
        len: usize,
        /// Whether this is a second delivery of a message the fault model
        /// duplicated (SPEC §1.4); the first carries `false`.
        dup: bool,
    },
    /// A message was discarded and will never be delivered.
    MessageDropped {
        /// The message's id.
        id: MessageId,
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The destination address.
        to: SocketAddr,
        /// Why it was discarded.
        reason: DropReason,
    },
    /// `fsync` returned Ok but the data did not become durable (SPEC.md §1.3).
    FsyncLost {
        /// The file that was synced.
        path: PathBuf,
    },
    /// At a crash one bit of a block flipped on disk (SPEC.md §1.3, bit rot). Checksums
    /// must catch this.
    BlockRotted {
        /// The file.
        path: PathBuf,
        /// Which block, counting from zero in `FsFaults::block_size` units.
        block: u64,
        /// The byte offset of the flipped bit, from the start of the file.
        offset: u64,
        /// Which bit of that byte, 0 to 7.
        bit: u8,
    },
    /// At a crash only a prefix of a pending write survived (SPEC.md §1.3).
    WriteTorn {
        /// The file that was written.
        path: PathBuf,
        /// Where the write started.
        offset: u64,
        /// How many bytes the write carried.
        written: usize,
        /// How many of them reached the disk.
        kept: usize,
    },
    /// At a crash a directory operation that was never followed by `sync_dir` was lost
    /// (SPEC.md §1.3, directory-entry loss).
    DirectoryEntryLost {
        /// The directory whose entries the operation changed.
        dir: PathBuf,
        /// The entry the operation concerned; for a rename, the destination.
        entry: PathBuf,
        /// What was lost.
        op: DirEntryOp,
    },
    /// The write-ahead log opened a segment file for appending (SPEC §2.2).
    WalSegmentOpened {
        /// The segment number, which names the file.
        segment: u64,
        /// The sequence number the first record written to it will have.
        first: u64,
    },
    /// One `sync` covered every record from `first` to `up_to` in `segment`: a group
    /// commit. Recorded after the call returned, so a `FsyncLost` for the segment's
    /// file immediately before it says the call lied.
    WalSynced {
        /// The segment.
        segment: u64,
        /// The first record in the segment.
        first: u64,
        /// The last record the sync covered.
        up_to: u64,
    },
    /// Recovery cut a segment after its last good record and synced it. Recorded after
    /// the sync, so a `FsyncLost` immediately before it says the cut may not hold.
    WalTruncated {
        /// The segment.
        segment: u64,
        /// The length it was cut to.
        len: u64,
    },
    /// The log's first record was numbered past the head its caller expected: the
    /// records between are gone with their segments. Replaying past the gap would
    /// give a state that never existed (D-022), so the open was refused, or the whole
    /// log discarded when the caller allowed that.
    HeadGap {
        /// The sequence number the caller expected the log to start at.
        expected: u64,
        /// The one the first record carried.
        found: u64,
        /// Whether the log was discarded (else the open failed).
        discarded: bool,
    },
    /// Recovery finished.
    WalRecovered {
        /// Records recovered, in order from the first segment.
        records: u64,
        /// Where reading stopped, if not at the end of the last segment.
        stop: Option<WalStop>,
        /// Segments after the stop that were discarded.
        discarded: u64,
    },
    /// The active memtable was full and became immutable; a fresh one took its place
    /// (SPEC §2.3). It stays readable until `MemtableFlushed` says the flush completed.
    MemtableRotated {
        /// The memtable's number, counting from 1 per engine open.
        memtable: u64,
        /// Keys it holds.
        entries: u64,
        /// Bytes it accounts for.
        bytes: u64,
        /// The highest log sequence number it holds.
        up_to: u64,
    },
    /// An SSTable was written and synced (SPEC §2.4). Recorded after the sync
    /// returned, so a `FsyncLost` for its file immediately before it says it lied.
    SstWritten {
        /// The table's number, which names the file.
        number: u64,
        /// The level it is written for: 0 for a flushed memtable, deeper for a
        /// compaction's output.
        level: u8,
        /// Writes it holds.
        entries: u64,
        /// Its size.
        bytes: u64,
        /// The lowest and highest log sequence numbers of the writes it holds.
        first_seq: u64,
        /// See `first_seq`.
        max_seq: u64,
    },
    /// A manifest was written and synced. Recorded after the sync returned, like
    /// `SstWritten`.
    ManifestWritten {
        /// The manifest's number, which names the file.
        number: u64,
        /// Every log record up to this is in an SSTable it lists.
        flushed_seq: u64,
        /// The tables it lists, by number.
        tables: Vec<u64>,
    },
    /// A compaction's outputs are written and synced (SPEC §2.5, D-023); the manifest
    /// listing them in place of the inputs is written next, `CURRENT` switched to it,
    /// the outputs put in service and the inputs deleted. Recorded before the manifest
    /// is written, since a crash can leave the manifest whole on disk, and `CURRENT`
    /// naming it, without the syncs that would have reported either.
    CompactionWritten {
        /// The level compacted; the outputs sit one deeper.
        level: u8,
        /// The manifest that will list the outputs.
        manifest: u64,
        /// The tables merged: the level's and the deeper level's that overlapped.
        inputs: Vec<u64>,
        /// The tables written, each with the first and last user key it holds.
        outputs: Vec<(u64, Bytes, Bytes)>,
        /// The oldest snapshot version live when the merge began: a write hidden by
        /// a newer one at or below it was dropped.
        snapshot: u64,
        /// Writes dropped because a newer write of the key hid them.
        dropped_versions: u64,
        /// Tombstones dropped because no older write of the key lay below.
        dropped_tombstones: u64,
    },
    /// A compaction deleted an input table once the manifest no longer listed it.
    SstDeleted {
        /// The table's number.
        number: u64,
    },
    /// `CURRENT` was pointed at a manifest by rename and the directory synced: the
    /// manifest is now the one recovery reads.
    CurrentSwitched {
        /// The manifest's number.
        manifest: u64,
    },
    /// Recovery refused the store: `CURRENT` or the manifest it names cannot be read
    /// and no fallback was allowed or intact (D-022). Nothing on disk was touched.
    OpenRefused {
        /// Why, as the error says it.
        reason: String,
    },
    /// Recovery could not read the manifest `CURRENT` names, or `CURRENT` itself, and
    /// used an older manifest whose every table is intact; everything flushed after
    /// it is lost.
    ManifestFallback {
        /// The manifest `CURRENT` named; 0 when `CURRENT` could not be read.
        from: u64,
        /// The one used instead; 0 is the empty state.
        to: u64,
    },
    /// Recovery could not read an SSTable the manifest lists and dropped it; the
    /// writes in its sequence range are lost.
    SstDropped {
        /// The table's number.
        number: u64,
        /// The lowest sequence number it held.
        first_seq: u64,
        /// The highest sequence number it held.
        max_seq: u64,
        /// What was wrong with it.
        reason: &'static str,
    },
    /// Recovery removed a file no manifest refers to: an SSTable or manifest whose
    /// flush or switch did not complete.
    OrphanRemoved {
        /// The file.
        path: PathBuf,
    },
    /// The log deleted a segment whose records are all in SSTables.
    WalSegmentDeleted {
        /// The segment.
        segment: u64,
    },
    /// The engine's flusher task stopped on an I/O error: nothing is flushed from
    /// here on, and the log grows until the engine is reopened. Never expected of a
    /// correct engine in the simulator, where the only I/O errors are its own.
    FlusherFailed {
        /// The error's text.
        error: String,
    },
    /// A checkpoint was written whole into a directory (SPEC §2.7, D-024): its
    /// tables, manifest and `CURRENT`, all synced.
    CheckpointWritten {
        /// The directory.
        dir: PathBuf,
        /// The version it is the state at.
        version: u64,
        /// Tables in it.
        tables: u64,
    },
    /// An immutable memtable was flushed and released.
    MemtableFlushed {
        /// The memtable's number.
        memtable: u64,
        /// The highest log sequence number it held.
        up_to: u64,
    },
    /// A Raft server's current term changed, with the role it took (RAFT.md §2).
    RaftTerm {
        /// The server.
        server: u64,
        /// The new term.
        term: u64,
        /// The role: `follower`, `pre-candidate`, `candidate` or `leader`.
        role: &'static str,
    },
    /// A Raft server granted or refused a vote or a pre-vote.
    RaftVote {
        /// The server voting.
        server: u64,
        /// The term voted in (the prospective term, for a pre-vote).
        term: u64,
        /// The candidate.
        candidate: u64,
        /// Whether the vote was granted.
        granted: bool,
        /// Whether it was a pre-vote.
        pre: bool,
    },
    /// A Raft server became leader.
    RaftLeader {
        /// The server.
        server: u64,
        /// Its term.
        term: u64,
        /// Its last log index on election.
        last_index: u64,
    },
    /// A Raft server wrote an entry to its log.
    RaftAppend {
        /// The server.
        server: u64,
        /// The entry's index.
        index: u64,
        /// The entry's term.
        entry_term: u64,
        /// A hash of the entry's payload, for log matching across servers.
        hash: u64,
    },
    /// A Raft server removed entries from its log on a conflict.
    RaftTruncate {
        /// The server.
        server: u64,
        /// The first index removed.
        from_index: u64,
    },
    /// A Raft server's commit index advanced.
    RaftCommit {
        /// The server.
        server: u64,
        /// Its term.
        term: u64,
        /// The new commit index.
        index: u64,
    },
    /// A Raft server applied an entry to its state machine.
    RaftApply {
        /// The server.
        server: u64,
        /// The entry's index.
        index: u64,
        /// The entry's term.
        entry_term: u64,
        /// The hash of the entry's payload.
        hash: u64,
    },
    /// A Raft leader served a linearizable read (RAFT.md §1): at `index`, by its
    /// lease or after a read-index round.
    RaftRead {
        /// The server.
        server: u64,
        /// The read index.
        index: u64,
        /// Whether the lease served it, rather than a heartbeat round.
        lease: bool,
    },
    /// A Raft leader's drift guard stopped trusting a follower's promise (RAFT.md
    /// §1): the follower's clock moved against the leader's by more than the bound.
    RaftLeaseRevoked {
        /// The leader.
        server: u64,
        /// The follower.
        follower: u64,
        /// How far the offset moved, in nanoseconds.
        offset_moved: u64,
    },
    /// A Raft leader asked a follower to take over (thesis §3.10): it sent TimeoutNow.
    RaftTransfer {
        /// The leader.
        server: u64,
        /// The follower asked to lead.
        to: u64,
    },
    /// A Raft leader stepped down for want of a majority within an election timeout
    /// (check quorum, RAFT.md §1).
    RaftQuorumLost {
        /// The server.
        server: u64,
        /// The term it led.
        term: u64,
    },
    /// A Raft server started on what its store held: the term, the applied index
    /// and the last log index it resumes from. An apply durable at a crash but not
    /// yet traced shows here as the applied index being past the last `RaftApply`.
    RaftRecovered {
        /// The server.
        server: u64,
        /// The term it resumes in.
        term: u64,
        /// The applied index on disk.
        applied: u64,
        /// The last log index on disk.
        last_index: u64,
    },
    /// A Raft leader proposed a client's request as a log entry (RAFT.md §4): the
    /// link from an operation of the history to the entry that carries it, so an
    /// abandoned operation can be closed by the entry's fate.
    RaftProposed {
        /// The server.
        server: u64,
        /// The client process.
        client: u64,
        /// The operation's number within the process.
        seq: u64,
        /// The entry's index.
        index: u64,
        /// The entry's term.
        term: u64,
    },
    /// A Raft server refused to start because its store's recovery lost state
    /// (RAFT.md §3): it takes part in nothing, no votes and no responses, until a
    /// snapshot re-seeds it.
    RaftRefused {
        /// The server.
        server: u64,
        /// Why.
        reason: String,
    },
    /// A Raft server stopped on an I/O error after starting. The simulator raises no
    /// I/O error of its own, so under simulation this is an engine bug.
    RaftServerFailed {
        /// The server.
        server: u64,
        /// The error.
        reason: String,
    },
    /// A Raft server's inbox was full and a message was dropped before the core saw
    /// it (RAFT.md §3): the oldest heartbeat first, never an AppendEntries with
    /// entries.
    RaftInboxDropped {
        /// The server.
        server: u64,
        /// The kind of message dropped.
        kind: &'static str,
    },
    /// A client operation started (RAFT.md §2): the invocation end of one operation
    /// of the linearizability history. Its time is the record's.
    ClientInvoke {
        /// The client process. A client that abandons an operation whose fate it
        /// does not know continues as a new process, so every process's history is
        /// sequential.
        client: u64,
        /// The operation's number within the process.
        seq: u64,
        /// The operation.
        op: ClientOp,
    },
    /// A client operation returned with a definite result. An operation with no
    /// return is pending: it may have taken effect or not.
    ClientReturn {
        /// The client process.
        client: u64,
        /// The operation's number within the process.
        seq: u64,
        /// The result.
        result: ClientResult,
    },
    /// A node's tasks were killed and the filesystem fault model applied to its disk.
    NodeCrashed {
        /// The crashed node.
        node: NodeId,
    },
    /// A task was polled more than `SimConfig::poll_budget` times at one virtual instant
    /// without letting time move: a busy loop. The run fails right after this is recorded.
    PollBudgetExceeded {
        /// The looping task.
        task: TaskId,
        /// How many polls it took at that instant.
        polls: u64,
    },
    /// A crashed node is about to be started again from what survived on its disk.
    NodeRestarted {
        /// The node.
        node: NodeId,
    },
    /// A symmetric partition began: the groups cannot reach each other, and together
    /// they cover every node.
    PartitionStarted {
        /// The groups, each sorted.
        groups: Vec<Vec<NodeId>>,
    },
    /// The symmetric partition ended.
    PartitionHealed {
        /// The groups that just ended.
        groups: Vec<Vec<NodeId>>,
    },
    /// One direction of one link was blocked: an asymmetric or partial partition, which
    /// moirae's group partitions cannot express.
    LinkBlocked {
        /// Messages from this node...
        from: NodeId,
        /// ...to this node are dropped.
        to: NodeId,
    },
    /// A blocked link direction was reopened.
    LinkUnblocked {
        /// The sender.
        from: NodeId,
        /// The receiver.
        to: NodeId,
    },
}

/// A key-value operation as a client issues it (RAFT.md §4): the single-key
/// operations of the linearizability model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientOp {
    /// Set `key` to `value`.
    Put {
        /// The key.
        key: Bytes,
        /// The value.
        value: Bytes,
    },
    /// Read `key`.
    Get {
        /// The key.
        key: Bytes,
    },
    /// Remove `key`.
    Delete {
        /// The key.
        key: Bytes,
    },
    /// Set `key` to `value` if it holds `expect`, none for absent.
    Cas {
        /// The key.
        key: Bytes,
        /// What it must hold.
        expect: Option<Bytes>,
        /// The value to set.
        value: Bytes,
    },
}

impl ClientOp {
    /// The key the operation touches.
    #[must_use]
    pub fn key(&self) -> &Bytes {
        match self {
            ClientOp::Put { key, .. }
            | ClientOp::Get { key }
            | ClientOp::Delete { key }
            | ClientOp::Cas { key, .. } => key,
        }
    }

    /// The operation's name: `put`, `get`, `delete` or `cas`.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            ClientOp::Put { .. } => "put",
            ClientOp::Get { .. } => "get",
            ClientOp::Delete { .. } => "delete",
            ClientOp::Cas { .. } => "cas",
        }
    }

    /// Whether the operation changes the key.
    #[must_use]
    pub fn is_write(&self) -> bool {
        !matches!(self, ClientOp::Get { .. })
    }
}

/// What a client operation returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientResult {
    /// A put or delete took effect.
    Done,
    /// What a get found.
    Value(Option<Bytes>),
    /// Whether a compare-and-set took effect.
    Swapped(bool),
}

/// A directory operation that must be followed by `sync_dir` to be durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DirEntryOp {
    /// A file was created.
    Link,
    /// A file was removed.
    Unlink,
    /// A file was renamed.
    Rename,
}

/// Where write-ahead log recovery stopped short of the end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalStop {
    /// The segment.
    pub segment: u64,
    /// The offset of the record that could not be read.
    pub offset: u64,
    /// What was wrong with it.
    pub reason: WalStopReason,
}

/// Why write-ahead log recovery stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WalStopReason {
    /// The segment ended inside a record: a torn write.
    TornRecord,
    /// The record's checksum did not match: bit rot, or a torn write of an earlier
    /// crash that was never cut.
    BadChecksum,
    /// The record carries a sequence number other than the one expected next: a
    /// record before it never reached the disk, typically because the sync that
    /// covered it was lost before the segment was rotated (D-019), or the whole
    /// segment before it is gone with a lost directory entry.
    Gap {
        /// The sequence number recovery expected.
        expected: u64,
        /// The one the record carried.
        found: u64,
    },
}

impl WalStopReason {
    /// The name the moirae bridge writes.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            WalStopReason::TornRecord => "torn-record",
            WalStopReason::BadChecksum => "bad-checksum",
            WalStopReason::Gap { .. } => "gap",
        }
    }
}

/// Why a message was discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DropReason {
    /// The per-destination send queue was full; the oldest frame made room (D-015).
    QueueFull,
    /// The simulated network has the link partitioned.
    Partitioned,
    /// The simulated network chose to lose it (random drop).
    Injected,
    /// No socket is bound at the destination.
    Unreachable,
}
