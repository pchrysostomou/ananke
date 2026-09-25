//! The node's `snapshot` task: the streams' bytes, the engine's calls and the trace
//! events that [`mod@crate::snapshot`]'s planner deliberately has none of.
//!
//! [`Snapshots`] (D-075) is the discipline — which directory a range stages under,
//! which assembly a chunk belongs to, what a stream over the receive cap is told,
//! what a completed stream asks for — with no clock, no socket and no disk. This
//! module is the other half: one task beside `net`, `answers` and `apply`, on a
//! fourth handle of the one socket, that puts the planner's decisions into effect.
//!
//! What it does, in the order a snapshot travels:
//!
//! 1. **A take is not here.** It is the `apply` task's, between two applies, so the
//!    index, term and configuration its record carries are exactly the state a
//!    snapshot at that index captures (RAFT.md §1, D-036). The node's take
//!    checkpoints the range's *own* key intervals
//!    (`Engine::checkpoint_spans`, D-068) into
//!    [`Snapshots::version`](crate::snapshot::Snapshots::version), never the whole
//!    engine directory, which on a node holds every other range as well
//!    ([`NodeVariant::TakeCheckpointsTheWholeNode`]).
//! 2. **A stream out** is an [`ananke_raft::snapshot::Sender`] per (range,
//!    follower), opened on the version directory the range's own snapshot record
//!    names and pinned to it for the stream's life (D-043). Nothing caps the sends,
//!    so a leader feeds every designated follower of a range at once (Q14).
//! 3. **A chunk out** goes through
//!    [`Snapshots::route`](crate::snapshot::Snapshots::route), which cuts it into a
//!    frame of its own on this task's socket handle — never the per-peer outbox,
//!    where a 256 KiB chunk would take the frame a round's heartbeats needed (Q41).
//! 4. **A chunk in** was diverted by the `net` task *before* the inbox, so it is
//!    charged to this task and not to the node's byte bound, and so it arrives at
//!    all: `Raft::on_message`'s arm for `InstallSnapshot` is empty, because the
//!    server is supposed to route these away before the core sees them, and the
//!    node's `net` task did not. That was **issue #96** — a chunk cost a heartbeat
//!    its place under the bound and then vanished — and the divert is its fix
//!    ([`NodeVariant::ChunksToTheInbox`] keeps the hole beside the fix).
//! 5. **A completed stream is D-066's live install**: the staged directory opened as
//!    a span source, the range's two key intervals and the receiver's repair in
//!    **one** manifest switch, the engine left open and the node's other ranges
//!    untouched. The range is held across it ([`CoreWork::Hold`]) and its replica is
//!    replaced by the one the switch built ([`CoreWork::Restore`]).
//!
//! [`CoreWork::Hold`]: crate::node::CoreWork::Hold
//! [`CoreWork::Restore`]: crate::node::CoreWork::Restore
//!
//! **What the stream carries, and what it does not.** The sender checkpoints the
//! range's Raft state *except its log*, and the range's user keys. The install's
//! spans are still the range's two whole intervals, so the switch removes the
//! receiver's own log with everything else; what replaces it is the repair's kept
//! tail and nothing more. The one-group install streams the leader's log and then
//! tombstones every key of it the tail does not replace (`Assembler::finish`), which
//! needs the receiver to enumerate what the sender sent. Not sending it is the same
//! end state, fewer bytes on the wire, and one fewer thing for the two ends to
//! disagree about. See D-083, where it is recorded as a departure from D-082's
//! design.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::ops::Range as KeyRange;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ananke_env::{
    Clock, Either, Environment, File, FileSystem, MAX_FRAME_LEN, Network, OpenOptions, Socket,
    StartOver, TraceEvent, race,
};
use ananke_raft::core::{RaftConfig, SnapshotAction};
use ananke_raft::message::{Frame, Message, SnapshotStatus};
use ananke_raft::queue::Queue;
use ananke_raft::snapshot::{self, Repair, Sender};
use ananke_raft::store::{KeyPrefix, PURPOSE_LOG, RaftStore};
use ananke_raft::types::{Configuration, Index, ServerId, Term};
use bytes::Bytes;

use crate::descriptor::RangeDescriptor;
use crate::range::RangeId;
use crate::server::Range;
use crate::snapshot::{Identity, Install, Landing, Route, Snapshots, Started};
use crate::variant::{NodeVariant, NodeVariants};

/// What the node's `snapshot` task takes from its queue.
pub enum SnapJob {
    /// A core asked for a stream of `range`'s snapshot to a follower behind its
    /// compacted prefix ([`SnapshotAction::Install`]).
    Stream {
        /// The range.
        range: RangeId,
        /// The follower to feed.
        to: ServerId,
        /// The snapshot's last index.
        index: Index,
        /// That entry's term.
        term: Term,
    },
    /// An `InstallSnapshot` chunk, diverted by the `net` task before the inbox.
    Chunk {
        /// The range the frame tagged it with.
        range: RangeId,
        /// The sender.
        from: ServerId,
        /// The chunk.
        message: Message,
    },
    /// An `InstallSnapshotResponse`, diverted the same way.
    Response {
        /// The range the frame tagged it with.
        range: RangeId,
        /// The follower answering.
        from: ServerId,
        /// The answer.
        message: Message,
    },
    /// The `raft` task's decision on a stream that completed: the range is held, and
    /// this is the repair to carry in the switch (RAFT.md:225-233).
    Finish {
        /// The range being installed.
        range: RangeId,
        /// The sender whose stream completed.
        from: ServerId,
        /// The receiver's own identity, which must survive the switch.
        repair: Box<Repair>,
    },
    /// A take of `range` completed: its versions are swept, and only its own
    /// (D-075).
    Taken {
        /// The range.
        range: RangeId,
    },
}

/// What the `snapshot` task has to tell the `raft` task.
///
/// Four of these become an [`Input`](ananke_raft::Input) for the range's core, as the
/// `apply` task's `Applied` does. The last three are not inputs at all: they are what
/// a live install asks of the core itself ([`CoreWork`](crate::node::CoreWork)),
/// because replacing one range's replica is not a step of it (D-066).
pub enum SnapAnswer {
    /// The `apply` task took a checkpoint, or wrote a follower's compaction record,
    /// at `index`.
    Taken {
        /// The range.
        range: RangeId,
        /// The index.
        index: Index,
        /// That entry's term.
        term: Term,
    },
    /// The follower `to` installed the snapshot this node streamed it.
    Installed {
        /// The range.
        range: RangeId,
        /// The follower.
        to: ServerId,
        /// The snapshot's last index.
        index: Index,
        /// The store incarnation its answer carried (D-042).
        incarnation: u64,
    },
    /// The stream to `to` was given up.
    Failed {
        /// The range.
        range: RangeId,
        /// The follower.
        to: ServerId,
        /// Whether the checkpoint itself is unusable and the next need retakes.
        retake: bool,
    },
    /// The stream to `to` was acknowledged past the furthest point it had reached:
    /// the re-seed progress check quorum asks of a refused follower (D-049).
    Acked {
        /// The range.
        range: RangeId,
        /// The follower.
        to: ServerId,
    },
    /// A stream completed and the node has its bytes: the range is to be held, and
    /// the repair built from its core, before anything switches.
    Ready {
        /// The range.
        range: RangeId,
        /// The sender.
        from: ServerId,
        /// The snapshot.
        at: Identity,
    },
    /// The install switched: the range's replica is replaced by the one it built.
    Switched {
        /// The range.
        range: RangeId,
        /// The snapshot installed.
        at: Identity,
        /// The configuration in force at the snapshot's last index, read out of the
        /// streamed bytes (`ananke_raft::snapshot::staged_record`).
        config: Box<Configuration>,
        /// The descriptor the switch put in place, read back from the store: what
        /// the host's receipt check reads for this range from here on (SHARD.md §3).
        // PROPOSED(D-097)
        descriptor: Option<Box<RangeDescriptor>>,
    },
    /// The install did not switch: the range goes on with the replica it has, and its
    /// hold is released.
    Abandoned {
        /// The range.
        range: RangeId,
    },
}

impl SnapAnswer {
    /// The range this answer is about.
    #[must_use]
    pub fn range(&self) -> RangeId {
        match self {
            SnapAnswer::Taken { range, .. }
            | SnapAnswer::Installed { range, .. }
            | SnapAnswer::Failed { range, .. }
            | SnapAnswer::Acked { range, .. }
            | SnapAnswer::Ready { range, .. }
            | SnapAnswer::Switched { range, .. }
            | SnapAnswer::Abandoned { range } => *range,
        }
    }
}

/// How many times one chunk is resent before its stream is given up, as the
/// one-group task counts them (RAFT.md §1).
///
/// **The correct node trips this bound, and it is left alone on purpose.** It reads 4
/// while RAFT.md:210 says eight and the one-group task counts to eight, so correcting
/// it looked like part of D-090's sentence — until it was measured. `sim/install.rs`,
/// 32 seeds in release, correct node: **2 112 streams given up on this bound**, with
/// runs of up to **nine** consecutive resends of one chunk, so raising 4 to 8 would not
/// have stopped it either. The cause is not the number: the receiver answers *nothing*
/// while an install decision is pending (`Inbound::pending`, "the sender retries once
/// the switch settles"), and the sender counts that silence as a lost chunk. A bound
/// the correct system trips is a model error to fix and never a bound to widen (D-030,
/// D-039), the model error is in the wiring this branch stacks on rather than in this
/// slice, and it is reported rather than papered over: **issue #120**, and D-090's
/// consequences.
// PROPOSED(D-090): measured, tripped by the correct node, and deliberately not widened.
const CHUNK_RESENDS: u32 = 4;

/// How many times a stream is restarted from its first byte before the sender counts
/// the checkpoint unusable and asks for a fresh take: RAFT.md:210-212, "restarted from
/// its first byte, twice, and at the third such ask the leader counts the checkpoint
/// as unusable", and `STREAM_RESTARTS` in the one-group task.
///
/// It counts a [`SnapshotStatus::Restart`] and nothing else. A cap-wait is a different
/// answer with a different bound — see [`Step::Wait`].
// PROPOSED(D-090): the node honours RAFT.md's restart bound.
const STREAM_RESTARTS: u32 = 2;

/// The two counters RAFT.md:209-212's give-up bounds are made of.
///
/// A value of its own so that [`booked`] — everything one answer does to them — is a
/// pure function a check can drive, for the same reason [`step_for`] is one. The
/// distinction matters and was found by review: driving `step_for` alone proves what
/// the node *decides* about an answer and nothing about what it then *records*, and
/// the recording is where both bounds actually live.
// PROPOSED(D-090): the bounds' arithmetic is a value a check can drive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counted {
    /// Resends of the chunk outstanding on this stream, which [`CHUNK_RESENDS`]
    /// bounds. Any answer at all clears it: that bound is for a receiver gone silent,
    /// and a receiver that answers is not silent.
    resends: u32,
    /// How many times this stream has been restarted from its first byte at a
    /// receiver's ask, which [`STREAM_RESTARTS`] bounds. Cap-waits are not counted
    /// here: a stream waiting its turn has covered no ground it must cover again.
    // PROPOSED(D-090): the node honours RAFT.md's restart bound.
    restarts: u32,
}

/// One stream being sent to one follower of one range: the sender's bookkeeping, and
/// when the chunk outstanding on it falls due for a resend.
struct Outbound {
    sender: Sender,
    deadline: ananke_env::Instant,
    /// The two counters the give-up bounds are made of.
    // PROPOSED(D-090): the bounds' arithmetic is a value a check can drive.
    counted: Counted,
    /// The furthest point the stream reached, as (file position, offset): an
    /// acknowledgement past it is progress, and progress is what check quorum counts
    /// for a refused follower (D-049).
    furthest: (usize, u64),
}

/// What one arriving answer asks of the stream it answers (RAFT.md:209-212).
///
/// A free function over the status and the restarts already counted, so that the bound
/// is a thing a check can drive directly rather than a branch buried in an async
/// receive loop.
// PROPOSED(D-090): the node honours RAFT.md's restart bound, and a cap-wait has one of
// its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// Send on from where the answer points.
    Resume,
    /// The stream is installed and ends here.
    Done,
    /// Start the stream again from its first byte, and count the restart.
    Restart,
    /// Wait for a slot under the receiver's cap: nothing is restarted, because nothing
    /// was staged and this stream stands exactly where it stood. The chunk outstanding
    /// falls due on the ordinary resend timer and is asked again.
    ///
    /// **It is counted against no give-up bound at all**, and the measurement is what
    /// says so. Bounding it by [`CHUNK_RESENDS`], on the reading that a cap-wait is an
    /// unanswered chunk from the sender's side, was tried first: with four ranges over
    /// a cap of two and this bound at 8, the *correct* node ran to ten consecutive waits
    /// on one stream and two streams in thirty-two seeds were given up for waiting; at
    /// the 4 this tree ships, its worst run of waits over a hundred seeds is 5, which is
    /// over that as well. A bound the correct system trips is a
    /// model error and not a bound to widen (D-030, D-039), and the model error is
    /// plain in RAFT.md:218-220 — "a slot the cap frees is granted to a stream that is
    /// asking for it, never reserved for one that asked earlier". A stream that has
    /// stopped asking cannot be granted the slot it is waiting for, so a bound on the
    /// asking is a bound on the mechanism.
    ///
    /// What bounds it instead is already there, and is two things. A receiver that
    /// answers is alive and is queueing this stream, which is why the answer resets
    /// the resend counter; a receiver that goes *silent* leaves the chunk unanswered
    /// and `CHUNK_RESENDS` bounds it exactly as it bounds any other. And the stream is
    /// ended from above when its range's Raft supersedes it — a leader change, a new
    /// term, a follower that caught up — which is where a stream that should stop
    /// waiting is stopped.
    Wait,
    /// The restart bound is spent: count the checkpoint unusable and ask for a fresh
    /// take (RAFT.md:210-212).
    Unusable,
}

/// The step one answer takes, given how many restarts this stream has already been
/// through.
///
/// `restarts` is the count *before* this answer, so the third `Restart` of a stream —
/// arriving with two already counted — is the one that spends the bound, which is what
/// RAFT.md:210-212 says in words.
// PROPOSED(D-090): the node honours RAFT.md's restart bound.
fn step_for(status: SnapshotStatus, restarts: u32, variants: NodeVariants) -> Step {
    // The variant: the node as it stood before this bound, where a restart reset the
    // resend counter and nothing counted the restarts, so a stream told to start over
    // restarted for as long as it was told to and neither bound was ever reached.
    let bounded = !variants.contains(NodeVariant::RestartsNotCounted);
    match status {
        SnapshotStatus::More => Step::Resume,
        SnapshotStatus::Installed => Step::Done,
        SnapshotStatus::Waiting => Step::Wait,
        SnapshotStatus::Restart if bounded && restarts >= STREAM_RESTARTS => Step::Unusable,
        SnapshotStatus::Restart => Step::Restart,
    }
}

/// What one step does to the sender's two counters: the whole of RAFT.md:209-212's
/// bookkeeping on the node, in one place.
///
/// Hoisted out of [`Task::response`]'s arms, and the review that asked for it gave the
/// reason. A check that drives [`step_for`] alone asserts the *decision* and leaves the
/// *record* free: the two lines that make these bounds real — the reset of the resend
/// counter on a wait, and the increment of the restart counter — could each be deleted
/// with the whole workspace suite still green. Deleting either one here now fails
/// `a_wait_clears_the_resend_counter_and_a_restart_is_the_only_step_that_counts_one`,
/// and the first also fails the four-ranges-over-two-slots check, which is where it
/// shows what it costs: the correct node starts giving streams up for *waiting*.
///
/// What is still not asserted is the one statement below that calls this, and the async
/// loop around it. That wants a simulated environment driving the task and is named as
/// owed rather than papered over.
// PROPOSED(D-090): the bounds' arithmetic is a value a check can drive.
fn booked(step: Step, counted: Counted) -> Counted {
    match step {
        // An answered chunk, whichever answer it was: the receiver is alive, so the
        // silence [`CHUNK_RESENDS`] is for has not happened and its count starts again.
        Step::Resume | Step::Wait => Counted {
            resends: 0,
            ..counted
        },
        // RAFT.md:210-212's count, and the reset beside it: the stream begins again at
        // its first byte, so the chunk outstanding on it is a new one.
        Step::Restart => Counted {
            resends: 0,
            restarts: counted.restarts + 1,
        },
        // The stream ends here, and its bookkeeping ends with it.
        Step::Done | Step::Unusable => counted,
    }
}

/// The answer one refusal carries: the wait a cap-wait gets, and the start-over every
/// other refusal gets.
///
/// A free function for the reason [`step_for`] and [`booked`] are, and this one was
/// found by review rather than reasoned to. It is the whole of this entry *on the
/// wire*, it is one `match`, and inside an async method that also traces, stamps an
/// incarnation, frames and sends, nothing in the tree could assert it —
/// [`snapshot::waiting`] has exactly one caller and the committed scenario answers no
/// cap-wait at all, so making this arm send [`snapshot::start_over`] again reinstated
/// the conflation with every test still passing.
// PROPOSED(D-090): the answer says which refusal it is, not only the trace.
fn refusal(status: SnapshotStatus, term: Term, at: (Index, Term)) -> Message {
    match status {
        SnapshotStatus::Waiting => snapshot::waiting(term, at),
        _ => snapshot::start_over(term, at),
    }
}

/// The status a receiver answers a cap-wait with: a wait of its own, or — under the
/// variant — the start-over the node answered it with before this bound existed.
// PROPOSED(D-090): a cap-wait is answered as a wait, not as a start-over.
fn cap_status(variants: NodeVariants) -> SnapshotStatus {
    if variants.contains(NodeVariant::CapWaitIsAStartOver) {
        // The variant: the conflation as it stood. The sender cannot tell a receiver
        // that is busy from one whose checkpoint identity moved, so a stream merely
        // waiting its turn spends the restart bound and its leader retakes a
        // checkpoint that was never unusable.
        return SnapshotStatus::Restart;
    }
    SnapshotStatus::Waiting
}

/// One stream being assembled from one sender of one range: where its bytes are going
/// and what to acknowledge next.
///
/// The planner keys the assembly and counts its bytes; this is the file-level detail
/// an acknowledgement needs, which no chunk carries on its own.
#[derive(Default)]
struct Inbound {
    /// Files fully received, with their sizes.
    done: BTreeMap<Bytes, u64>,
    /// The file arriving now: name, bytes received, total.
    current: Option<(Bytes, u64, u64)>,
    /// The last file completed: where an acknowledgement points between files.
    last_done: Option<(Bytes, u64)>,
    /// The install this stream completed into, waiting on the `raft` task's repair.
    /// While it is set the sender's further chunks are ignored: it retries once the
    /// switch settles, as the one-group receiver makes it (node.rs:1956-1959).
    pending: Option<Box<Install>>,
}

/// The node's `snapshot` task.
pub struct Task<E: Environment> {
    env: E,
    id: ServerId,
    sock: Arc<<E::Net as Network>::Socket>,
    addrs: BTreeMap<ServerId, SocketAddr>,
    stores: BTreeMap<RangeId, Arc<RaftStore<E>>>,
    config: RaftConfig,
    variants: NodeVariants,
    engine_dir: PathBuf,
    ranges: Vec<Range>,
    /// Shared with the `apply` task: a live install moves a range's applied index,
    /// term and configuration without an apply, and both tasks read this.
    // PROPOSED(D-083): a live install moves the apply task's state with it.
    applied: Arc<std::sync::Mutex<BTreeMap<RangeId, crate::server::Applied>>>,
    plan: Snapshots,
    local: Queue<crate::server::Local>,
    snaps: Queue<SnapJob>,
    sending: BTreeMap<(RangeId, ServerId), Outbound>,
    receiving: BTreeMap<(RangeId, ServerId), Inbound>,
}

impl<E: Environment> Task<E> {
    /// The task, with a planner already told which key intervals each range it hosts
    /// lives in.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "the task names every end it is wired to: socket, stores, planner, queues"
    )]
    pub fn new(
        env: E,
        id: ServerId,
        sock: Arc<<E::Net as Network>::Socket>,
        addrs: BTreeMap<ServerId, SocketAddr>,
        stores: BTreeMap<RangeId, Arc<RaftStore<E>>>,
        config: RaftConfig,
        variants: NodeVariants,
        engine_dir: PathBuf,
        ranges: Vec<Range>,
        applied: Arc<std::sync::Mutex<BTreeMap<RangeId, crate::server::Applied>>>,
        plan: Snapshots,
        local: Queue<crate::server::Local>,
        snaps: Queue<SnapJob>,
    ) -> Self {
        Self {
            env,
            id,
            sock,
            addrs,
            stores,
            config,
            variants,
            engine_dir,
            ranges,
            applied,
            plan,
            local,
            snaps,
            sending: BTreeMap::new(),
            receiving: BTreeMap::new(),
        }
    }

    /// Runs until the queue closes.
    ///
    /// The loop takes whichever comes first of a job and the nearest outstanding
    /// chunk's deadline, so a stream whose chunk was lost resends rather than waiting
    /// on a job that may never come.
    pub async fn run(&mut self) {
        loop {
            let job = match self.deadline() {
                Some(deadline) => {
                    let pop = std::pin::pin!(self.snaps.pop());
                    let timer = std::pin::pin!(self.env.clock().sleep_until(deadline));
                    match race(&self.env, pop, timer).await {
                        Either::Left(Some(job)) => Some(job),
                        Either::Left(None) => return,
                        Either::Right(()) => None,
                    }
                }
                None => match self.snaps.pop().await {
                    Some(job) => Some(job),
                    None => return,
                },
            };
            match job {
                Some(job) => self.job(job).await,
                None => self.due().await,
            }
        }
    }

    /// The nearest deadline of a chunk outstanding on any stream.
    fn deadline(&self) -> Option<ananke_env::Instant> {
        self.sending.values().map(|out| out.deadline).min()
    }

    /// One job.
    async fn job(&mut self, job: SnapJob) {
        match job {
            SnapJob::Stream {
                range,
                to,
                index,
                term,
            } => self.open(range, to, index, term).await,
            SnapJob::Chunk {
                range,
                from,
                message,
            } => self.chunk(range, from, message).await,
            SnapJob::Response {
                range,
                from,
                message,
            } => self.response(range, from, message).await,
            SnapJob::Finish {
                range,
                from,
                repair,
            } => self.finish(range, from, *repair).await,
            SnapJob::Taken { range } => self.sweep(range).await,
        }
    }

    /// Every stream whose chunk is past its deadline: resent from where its receiver
    /// last stood, or given up. Every one of them is serviced here, none behind
    /// another's.
    async fn due(&mut self) {
        let now = self.env.clock().now();
        let due: Vec<(RangeId, ServerId)> = self
            .sending
            .iter()
            .filter(|(_, out)| out.deadline <= now)
            .map(|(key, _)| *key)
            .collect();
        for (range, to) in due {
            let give_up = {
                let Some(out) = self.sending.get_mut(&(range, to)) else {
                    continue;
                };
                out.counted.resends += 1;
                out.counted.resends > CHUNK_RESENDS
            };
            if give_up {
                self.sending.remove(&(range, to));
                self.plan.sent(range, to);
                self.local
                    .push(crate::server::Local::Snapshot(SnapAnswer::Failed {
                        range,
                        to,
                        retake: false,
                    }));
                continue;
            }
            self.env.trace(TraceEvent::RaftSnapshotResumed {
                server: self.id.0,
                range: range.get(),
                to: to.0,
                offset: self.sending.get(&(range, to)).map_or(0, |out| {
                    out.sender.position_offset(self.config.snapshot_chunk)
                }),
            });
            self.send_chunk(range, to).await;
        }
    }

    /// Opens a stream of `range`'s snapshot at `index` to `to`.
    async fn open(&mut self, range: RangeId, to: ServerId, index: Index, term: Term) {
        let at = Identity {
            term: self.stores.get(&range).map_or(0, |store| store.term()),
            last_index: index,
            last_term: term,
        };
        // A range this node does not host has no version to stream and no core to
        // answer; the ask cannot have come from here.
        let Some(store) = self.stores.get(&range).cloned() else {
            return;
        };
        // What the record names, if it names a take of its own: the fast path through
        // `find_version`, which falls back to scanning the engine directory.
        let recorded = match store.snapshot_record().await {
            Ok(Some(record)) if record.taken => Some((record.last_index, record.take)),
            _ => None,
        };
        // The version to stream is a **complete version directory of the index the core
        // asked for**, and a stream pins it for its whole life (D-043). This is the
        // one-group server's own rule (`start_stream` and `snapshot::find_version`,
        // crates/ananke-raft/src/node.rs:2182 and snapshot.rs:165-191), keyed by range:
        // `snap-r<range>-<index>-<take>` names survive later takes, which is exactly
        // what the take counter is for, so a version of the asked index is still there
        // after the record has moved on. A range that has no complete version of it —
        // the store compacted without checkpointing (D-065, D-078), or a crash landed
        // between the record and the checkpoint — is not a stream to open but a take to
        // ask for, which `retake` does.
        //
        // **Complete is D-083's requirement and `find_version` is where it is now
        // enforced**, on every candidate it considers rather than on the record's
        // directory alone. The record is written *before* the checkpoint under it
        // (RAFT.md §1, D-036), so a record naming a version is not a promise the
        // version is there yet, and opening a half-written one is not a slow start but
        // a permanent one: `Sender::open` lists the directory once and keeps that list
        // for the stream's life, so a stream opened on a directory holding one table
        // streams that table, says `done`, and hands the receiver a staged directory
        // with no `CURRENT` that `open_span_source` refuses — for ever, because the
        // identity never changes and the sender is never re-opened. Eleven seeds in two
        // hundred and fifty lost an install to exactly that (D-083). `find_version`
        // calls `ananke_raft::snapshot::checkpoint_complete` on each candidate and
        // returns only one that passes, so the incomplete directory is skipped for a
        // complete take of the same index where there is one and answered with `retake`
        // where there is not — which is D-083's answer, reached by a lookup that can
        // also see past the record.
        // PROPOSED(D-083): a stream opens only a complete version.
        //
        // **It is emphatically not the store's snapshot record**, and PROPOSED D-086
        // tried both of the other readings first. Matching the record's index *exactly*
        // made a take landing between the core's ask and this read answer `retake`, and
        // the core then took, asked and lost the race again: `sim/install.rs`'s seed 7,
        // an install that never completed at any run length. Taking the record's
        // identity *instead* fixed that and broke something worse — the identity then
        // moved with every take, so each re-open of one logical install carried a new
        // one, `is_streaming` never suppressed the duplicate, and each fresh stream
        // restarted the receiver's assembly under it (RAFT.md:203-207). Three leaders
        // of one range were opening streams at three identities at once. The index the
        // core asked for is the only one of the three that is **stable**, which is why
        // it is the one the version is looked up by.
        // PROPOSED(D-086): a stream opens on a complete version of the index asked for.
        let dir = match crate::snapshot::find_version(
            &self.env,
            &self.engine_dir,
            range,
            index,
            recorded,
        )
        .await
        {
            Ok(Some(dir)) => dir,
            _ => return self.retake(range, to),
        };
        if self.plan.is_streaming(range, to, at) {
            return;
        }
        let started = self.plan.stream(range, to, at);
        if matches!(started, Started::Waiting) {
            // Only `CapStreamsSent` answers this: Q14 puts no cap on streams sent.
            return self.give_up(range, to);
        }
        let sender = match Sender::open(&self.env, &dir, to, index, term, at.term).await {
            Ok(sender) => sender,
            Err(_) => {
                self.plan.sent(range, to);
                return self.retake(range, to);
            }
        };
        let streams = u64::try_from(self.plan.meters().sending).expect("small");
        self.env.trace(TraceEvent::RaftSnapshotStreams {
            server: self.id.0,
            range: range.get(),
            to: to.0,
            streams,
        });
        self.sending.insert(
            (range, to),
            Outbound {
                sender,
                deadline: self.env.clock().now(),
                counted: Counted::default(),
                furthest: (0, 0),
            },
        );
        self.send_chunk(range, to).await;
    }

    /// Sends the chunk outstanding on this stream, in a frame of its own (Q41), and
    /// arms its deadline.
    async fn send_chunk(&mut self, range: RangeId, to: ServerId) {
        let Some(out) = self.sending.get(&(range, to)) else {
            return;
        };
        let chunk = match out
            .sender
            .chunk(&self.env, self.config.snapshot_chunk)
            .await
        {
            Ok(chunk) => chunk,
            Err(_) => {
                self.sending.remove(&(range, to));
                self.plan.sent(range, to);
                return self.retake(range, to);
            }
        };
        let encoded = Frame {
            from: self.id,
            message: chunk,
        }
        .encode();
        match self.plan.route(range, to, &encoded) {
            Ok(Route::OwnFrame(frame)) => self.ship(to, frame).await,
            Ok(Route::Outbox) => {
                // The variant: the chunk goes through the per-peer outbox. The node has
                // no outbox handle here, so what the variant costs is measured where it
                // is decided — in the planner's `frames` count, which no longer moves
                // with `chunks` — and the chunk is shipped unframed-with-others so the
                // stream still runs and the check reads a count, not a hang.
                let mut builder = crate::frame::Builder::new(MAX_FRAME_LEN);
                builder.push(range, &encoded);
                let frame = builder.finish();
                self.ship(to, frame).await;
            }
            Err(_) => {
                // A chunk that does not fit a frame at all: nothing splits a message
                // across frames, so the stream cannot run and the checkpoint is not the
                // problem.
                self.sending.remove(&(range, to));
                self.plan.sent(range, to);
                return self.give_up(range, to);
            }
        }
        let timeout = self.chunk_timeout();
        if let Some(out) = self.sending.get_mut(&(range, to)) {
            out.deadline = self.env.clock().now() + timeout;
        }
    }

    /// One arriving chunk.
    async fn chunk(&mut self, range: RangeId, from: ServerId, message: Message) {
        let Message::InstallSnapshot {
            term,
            last_index,
            last_term,
            file,
            offset,
            total,
            done,
            data,
        } = message
        else {
            return;
        };
        let Some(store) = self.stores.get(&range).cloned() else {
            return;
        };
        if term < store.term() {
            // A stale leader's stream: let it time out.
            return;
        }
        if store.applied() >= last_index {
            // Everything the snapshot carries is already here: say so.
            let answer = snapshot::installed(store.term(), (last_index, last_term));
            return self.answer(range, from, answer, store.incarnation()).await;
        }
        if self
            .receiving
            .get(&(range, from))
            .is_some_and(|inbound| inbound.pending.is_some())
        {
            // Mid-decision: the sender retries once the switch settles.
            return;
        }
        let at = Identity {
            term,
            last_index,
            last_term,
        };
        let bytes = data.len();
        match self.plan.on_chunk(range, from, at, bytes, done) {
            Landing::Waiting { .. } => {
                // The node is assembling as many streams as its cap allows and this is
                // not one of them: nothing was written and no other assembly was
                // disturbed. The sender waits, and takes a slot the next time it asks
                // while one is free (D-075).
                //
                // It is told to *wait* and not to start over, because the two are
                // different things to a sender and only one of them is what
                // RAFT.md:210-212's restart bound counts. A node's cap is routinely
                // below its range count — §12's re-seed shape sets it there on
                // purpose — so cap-waits are ordinary here in a way they never are on
                // the one-group server, which has no cap at all.
                // PROPOSED(D-090): a cap-wait is answered as a wait, not as a
                // start-over.
                self.refuse(
                    range,
                    from,
                    cap_status(self.variants),
                    StartOver::Cap,
                    store.term(),
                    (last_index, last_term),
                )
                .await;
            }
            Landing::NotHosted => {
                // `range` is a peer's word, not this node's: a leader that has not
                // learned the range moved (Q33), or a garbled range id, names a range
                // the task does not host. It is refused before admission, so it takes
                // no slot under the cap and disturbs no assembly, and it is refused
                // rather than fatal — failing on it would let any peer abort the node.
                // The sender starts over and finds the range where it now lives
                // (D-075). The reason is its own, so a run refusing unhosted ranges
                // does not read in the trace as one starved of slots.
                self.start_over(
                    range,
                    from,
                    StartOver::NotHosted,
                    store.term(),
                    (last_index, last_term),
                )
                .await;
            }
            Landing::Installed { .. } => {
                // This stream's last chunk again, at the identity this assembly has
                // already completed: its answer was lost, so the node answers the
                // resend what it answered the first — installed — and installs
                // nothing a second time (D-075).
                let answer = snapshot::installed(store.term(), (last_index, last_term));
                self.answer(range, from, answer, store.incarnation()).await;
            }
            Landing::Restarted { dir, .. } => {
                self.clear(&dir).await;
                self.receiving.insert((range, from), Inbound::default());
                self.start_over(
                    range,
                    from,
                    StartOver::Identity,
                    store.term(),
                    (last_index, last_term),
                )
                .await;
            }
            Landing::Staged { dir, .. } => {
                if self.stage(&dir, &file, offset, &data).await.is_err() {
                    return self
                        .start_over(
                            range,
                            from,
                            StartOver::Unusable,
                            store.term(),
                            (last_index, last_term),
                        )
                        .await;
                }
                let inbound = self.receiving.entry((range, from)).or_default();
                advance(inbound, &file, offset, total, data.len());
                let (want, at_offset) = wanted(inbound);
                let answer = snapshot::ack(store.term(), (last_index, last_term), want, at_offset);
                self.answer(range, from, answer, store.incarnation()).await;
            }
            Landing::Complete(install) => {
                if self
                    .stage(&install.source, &file, offset, &data)
                    .await
                    .is_err()
                {
                    return self
                        .start_over(
                            range,
                            from,
                            StartOver::Unusable,
                            store.term(),
                            (last_index, last_term),
                        )
                        .await;
                }
                self.receiving.entry((range, from)).or_default().pending = Some(install);
                // The `raft` task holds the range and builds the repair from its core;
                // the switch waits for it. Nothing here may write to this range's store
                // until that hold is taken (D-066).
                self.local
                    .push(crate::server::Local::Snapshot(SnapAnswer::Ready {
                        range,
                        from,
                        at,
                    }));
            }
        }
    }

    /// One arriving answer to a stream this node is sending.
    async fn response(&mut self, range: RangeId, from: ServerId, message: Message) {
        let Message::InstallSnapshotResponse {
            last_index,
            last_term,
            file,
            offset,
            status,
            incarnation,
            ..
        } = message
        else {
            return;
        };
        let variants = self.variants;
        let Some(out) = self.sending.get_mut(&(range, from)) else {
            return;
        };
        if out.sender.last_index != last_index || out.sender.last_term != last_term {
            // An answer to a stream this node is no longer sending: stale.
            return;
        }
        // The step, and then the record of it: one statement each, so the counters the
        // two bounds are made of are `booked`'s arithmetic and not an arm's.
        // PROPOSED(D-090): the bounds' arithmetic is a value a check can drive.
        let step = step_for(status, out.counted.restarts, variants);
        out.counted = booked(step, out.counted);
        match step {
            Step::Resume => {
                out.sender.on_more(&file, offset);
                let now = out.sender.acknowledged();
                let progressed = now > out.furthest;
                out.furthest = out.furthest.max(now);
                if progressed {
                    // D-049: a refused follower counts for check quorum only while its
                    // re-seed stream progresses, and this is what progress means. It
                    // is progress of *this range's* stream, and it answers this range's
                    // core alone (issue #103, `acked_for`).
                    let hosted: Vec<RangeId> = self.stores.keys().copied().collect();
                    for one in acked_for(range, &hosted, self.variants) {
                        self.local
                            .push(crate::server::Local::Snapshot(SnapAnswer::Acked {
                                range: one,
                                to: from,
                            }));
                    }
                }
                self.send_chunk(range, from).await;
            }
            Step::Done => {
                self.sending.remove(&(range, from));
                self.plan.sent(range, from);
                self.local
                    .push(crate::server::Local::Snapshot(SnapAnswer::Installed {
                        range,
                        to: from,
                        index: last_index,
                        incarnation,
                    }));
            }
            Step::Restart => {
                // RAFT.md:210-212: restarted from its first byte. The count that
                // `booked` took above is what makes the give-up below reachable at all
                // — without it the reset of the resend counter beside it puts a stream
                // that keeps being told to start over into a loop with no bound on
                // either counter, which is the hole D-083 found and this closes.
                // PROPOSED(D-090): the node honours RAFT.md's restart bound.
                out.sender.restart();
                self.send_chunk(range, from).await;
            }
            Step::Unusable => {
                // The third such ask: the leader counts the checkpoint unusable and
                // asks for a fresh take (RAFT.md:210-212), which is what the one-group
                // task does with `StreamFailed { retake: true }`.
                // PROPOSED(D-090): the node honours RAFT.md's restart bound.
                self.sending.remove(&(range, from));
                self.plan.sent(range, from);
                self.retake(range, from);
            }
            Step::Wait => {
                // A slot, not a start-over: nothing was staged, so this stream stands
                // exactly where it stood and there is nothing to cover again. The
                // chunk outstanding is left to fall due on the ordinary resend timer,
                // which paces the next ask — and the ask is what takes the slot, since
                // a freed slot is granted to a stream that is asking for it
                // (RAFT.md:218-220). `booked` above reset the resend counter, because
                // this chunk was answered: a receiver saying "wait" is alive and
                // queueing this stream, and it is a receiver gone *silent* that
                // `CHUNK_RESENDS` is for. Only the timer is re-armed here.
                // PROPOSED(D-090): a cap-wait is counted against no give-up bound.
                let deadline = self.env.clock().now() + self.chunk_timeout();
                if let Some(out) = self.sending.get_mut(&(range, from)) {
                    out.deadline = deadline;
                }
            }
        }
    }

    /// The `raft` task's repair for a stream that completed: D-066's live install, in
    /// one manifest switch, with the node's other ranges left running.
    async fn finish(&mut self, range: RangeId, from: ServerId, repair: Repair) {
        let Some(install) = self
            .receiving
            .get_mut(&(range, from))
            .and_then(|inbound| inbound.pending.take())
        else {
            return self.abandon(range, from).await;
        };
        let Some(store) = self.stores.get(&range).cloned() else {
            return self.abandon(range, from).await;
        };
        // The store may have caught up while the repair was being built: the one-group
        // server cannot have this window, because it quiesces its `apply` task before
        // it decides (node.rs, `install_decision`), and the node's hold stops the
        // *core* rather than the `apply` task, so a job queued before the hold can
        // still land. Its rule applies here unchanged — "the store already holds
        // everything the snapshot carries: answer installed without switching" — and
        // applied at the switch rather than only at the chunk, which is where the
        // window actually is. Switching anyway would take the store backwards.
        // PROPOSED(D-083): the switch re-checks what the chunk path checks.
        if store.applied() >= install.at.last_index {
            self.clear(&install.source).await;
            self.plan.finish(range, from);
            self.receiving.remove(&(range, from));
            let answer =
                snapshot::installed(repair.term, (install.at.last_index, install.at.last_term));
            self.answer(range, from, answer, store.incarnation()).await;
            return self.release(range);
        }
        // D-047: the install is decided when the task takes the repair; it is traced
        // once the manifest switch that carries it is durable.
        let installing = self.env.decision();
        // The configuration in force at the snapshot's last index, out of the streamed
        // bytes: no message carries it, and the receiver's own view of the membership
        // is a replica's that is behind by definition (D-029).
        let config = match snapshot::staged_record(&self.env, &install.source, store.prefix()).await
        {
            Ok(Some(record)) => record.config,
            // A stream whose bytes carry no snapshot record for this range is a stream
            // of a store that never took one; there is nothing to install from it.
            Ok(None) | Err(_) => return self.restart(range, from, &install, &store).await,
        };
        let source = match store.engine().open_span_source(&install.source).await {
            Ok(source) => source,
            // The staged directory is short — a crash inside the stream, a table left
            // half-written — and the engine's own checks are what say so. The stream
            // starts again from its first byte, which is what `Fault::CrashInstalling`
            // aims at.
            Err(_) => return self.restart(range, from, &install, &store).await,
        };
        let batch = snapshot::repair_writes(
            store.prefix(),
            &repair,
            install.at.last_index,
            install.at.last_term,
            &config,
            // No log key is streamed, so there is none for the tail not to replace:
            // the switch removes the receiver's whole log with the rest of the span and
            // the repair's tail is all that goes back (D-083).
            &BTreeSet::new(),
        );
        // Phase 2's `SnapshotWithoutCurrentLast` on the node's install path, by its own
        // bit (Q37). RAFT.md:694 has it break "the staged `CURRENT` written last, after
        // the repair"; on the node the commit point of an install is this manifest
        // switch, and the rule becomes that the switch is made only with the range's
        // repair carried in it (SHARD.md §12, question 2; D-066). That is exactly what
        // [`NodeVariant::InstallWithoutRepair`] does, and D-075 named it "the node's
        // translation" of the Phase 2 variant — but nothing read the Phase 2 bit, so a
        // node whose cores carried it installed correctly. This is the same shape D-082
        // found `SendBeforePersist` in: a bit set, carried, and read by nothing.
        //
        // The two are kept apart rather than merged. `InstallWithoutRepair` is the
        // node's own variant and is asserted deterministically by D-083's checks; this
        // is Phase 2's, re-asserted under `Fault::CrashInstalling` at its Phase 2 tier,
        // and either alone breaks the rule.
        // PROPOSED(D-086): Phase 2's `SnapshotWithoutCurrentLast` on the node's switch.
        let with_repair = install.repair_in_switch
            && !self
                .config
                .variants
                .contains(ananke_raft::core::Variant::SnapshotWithoutCurrentLast);
        let switched = if with_repair {
            store
                .engine()
                .install_spans(install.spans.clone(), source, batch)
                .await
        } else {
            // The variant: the switch is made without the range's repair carried in it
            // (D-066), so the store comes back holding the *leader's* tenant 0 — its
            // term, its vote, its log — under this replica's prefix.
            store
                .engine()
                .install_spans(
                    install.spans.clone(),
                    source,
                    ananke_storage::WriteBatch::new(),
                )
                .await
        };
        if switched.is_err() {
            return self.restart(range, from, &install, &store).await;
        }
        // The switch moved this range's applied index, its term and the configuration
        // in force, without an apply: the store's cached index and the `apply` task's
        // own state are both the replaced replica's until they are told. The first
        // showed up as an install reporting `applied 0` at snapshot 48; the second
        // would have had the next take of this range write a record with the old
        // replica's term and configuration in it.
        // PROPOSED(D-083): a live install moves the applied state with it.
        store.installed_at(install.at.last_index);
        if let Ok(mut applied) = self.applied.lock() {
            applied.insert(
                range,
                crate::server::Applied {
                    index: install.at.last_index,
                    term: install.at.last_term,
                    config: config.clone(),
                },
            );
        }
        self.clear_staging(&install.source).await;
        self.plan.finish(range, from);
        self.receiving.remove(&(range, from));
        // What actually landed, read back from the engine before this range is let go
        // again, so a check can compare it with what the take that fed it put in.
        // Counting events says a stream flowed; it says nothing about what is in the
        // store, and a take that dropped the range's user keys produces exactly the
        // same events as a correct one (D-083).
        // PROPOSED(D-083): what an install installed is read back and traced.
        self.state_of(range, install.at, &store).await;
        // The descriptor the switch put in place, for the host's receipt check
        // (SHARD.md §3). Read back rather than carried in the stream's record: it is
        // what the store holds now, whatever the take put in.
        // PROPOSED(D-097)
        let descriptor = store
            .engine()
            .get(&KeyPrefix::group(range.get()).descriptor_key())
            .await
            .ok()
            .flatten()
            .and_then(|bytes| RangeDescriptor::decode(bytes).ok())
            .map(Box::new);
        self.env.trace_decided(
            installing,
            TraceEvent::RaftSnapshot {
                server: self.id.0,
                range: range.get(),
                last_index: install.at.last_index,
                last_term: install.at.last_term,
                taken: false,
            },
        );
        if install.adopted {
            // The variant: `RaftAdopted` traced for a replica's live install. On the
            // node that event records a node taking a fresh directory after a
            // whole-node refusal, and nothing else (D-066).
            self.env
                .trace_decided(installing, TraceEvent::RaftAdopted { server: self.id.0 });
        }
        let answer =
            snapshot::installed(repair.term, (install.at.last_index, install.at.last_term));
        self.answer(range, from, answer, store.incarnation()).await;
        self.local
            .push(crate::server::Local::Snapshot(SnapAnswer::Switched {
                range,
                at: install.at,
                config: Box::new(config),
                descriptor,
            }));
    }

    /// The install did not switch: the staging directory starts over, the sender is
    /// asked for the stream again, and the range's hold is released.
    async fn restart(
        &mut self,
        range: RangeId,
        from: ServerId,
        install: &Install,
        store: &RaftStore<E>,
    ) {
        self.clear(&install.source).await;
        self.plan.finish(range, from);
        self.receiving.remove(&(range, from));
        self.start_over(
            range,
            from,
            StartOver::Unusable,
            store.term(),
            (install.at.last_index, install.at.last_term),
        )
        .await;
        self.local
            .push(crate::server::Local::Snapshot(SnapAnswer::Abandoned {
                range,
            }));
    }

    /// Lets a held range go without a switch.
    fn release(&self, range: RangeId) {
        self.local
            .push(crate::server::Local::Snapshot(SnapAnswer::Abandoned {
                range,
            }));
    }

    /// The range was held for an install that is no longer there: let it go.
    async fn abandon(&mut self, range: RangeId, from: ServerId) {
        self.plan.finish(range, from);
        self.local
            .push(crate::server::Local::Snapshot(SnapAnswer::Abandoned {
                range,
            }));
    }

    /// A take of `range` completed: its versions are swept, and only its own. A sweep
    /// that took every unpinned version, as the one-group's does against a single
    /// snapshot record, would delete every *other* range's checkpoints (D-075).
    async fn sweep(&mut self, range: RangeId) {
        let Some(store) = self.stores.get(&range).cloned() else {
            return;
        };
        let mut keep: BTreeSet<(Index, u64)> = BTreeSet::new();
        if let Ok(Some(record)) = store.snapshot_record().await {
            keep.insert((record.last_index, record.take));
        }
        // A version a stream is reading is pinned for that stream's whole life (D-043).
        for (key, out) in &self.sending {
            if key.0 == range {
                keep.insert((out.sender.last_index, take_of(out.sender.dir())));
            }
        }
        let Ok(names) = self.env.fs().read_dir(&self.engine_dir).await else {
            return;
        };
        let names: Vec<String> = names
            .iter()
            .filter_map(|name| name.to_str().map(str::to_owned))
            .collect();
        for name in self.plan.sweep(&names, range, &keep) {
            self.clear(&self.engine_dir.join(&name)).await;
        }
    }

    /// Writes one chunk's bytes into the staging directory.
    async fn stage(&self, dir: &Path, file: &Bytes, offset: u64, data: &Bytes) -> io::Result<()> {
        let fs = self.env.fs();
        fs.create_dir_all(dir).await?;
        let name = str::from_utf8(file)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "a chunk's file name"))?;
        let handle = fs
            .open(&dir.join(name), OpenOptions::new().write(true).create(true))
            .await?;
        if !data.is_empty() {
            handle.write_at(offset, data.clone()).await?;
        }
        handle.sync().await?;
        Ok(())
    }

    /// Empties one directory, leaving it there.
    async fn clear(&self, dir: &Path) {
        let fs = self.env.fs();
        let Ok(names) = fs.read_dir(dir).await else {
            return;
        };
        for name in names {
            let _ = fs.remove_file(&dir.join(name)).await;
        }
        let _ = fs.sync_dir(dir).await;
    }

    /// Empties the staging directory an install has just read: this one alone.
    async fn clear_staging(&self, source: &Path) {
        let Ok(names) = self.env.fs().read_dir(&self.engine_dir).await else {
            return self.clear(source).await;
        };
        let names: Vec<String> = names
            .iter()
            .filter_map(|name| name.to_str().map(str::to_owned))
            .collect();
        for name in swept_on_install(source, &names, self.variants) {
            self.clear(&self.engine_dir.join(name)).await;
        }
    }

    /// Reads a range's replica back out of the engine and traces what it holds.
    ///
    /// Called at the two points where a range's state is claimed to be a snapshot's:
    /// the take that writes one, and the live install that lands one. Pairing the two
    /// by `(range, last_index, last_term)` is what lets a check say the bytes that
    /// landed are the bytes that were taken — which no count of events can say.
    // PROPOSED(D-083): what an install installed is read back and traced.
    async fn state_of(&self, range: RangeId, at: Identity, store: &RaftStore<E>) {
        let Some(spans) = self.user_span(range) else {
            return;
        };
        let version = store.engine().snapshot();
        let Ok(user) = store
            .engine()
            .scan(&spans.start[..]..&spans.end[..], &version)
            .await
        else {
            return;
        };
        let log_span = store.prefix().purpose_span(PURPOSE_LOG);
        let log = store
            .engine()
            .scan(&log_span.start[..]..&log_span.end[..], &version)
            .await
            .unwrap_or_default();
        drop(version);
        // Order-independent so the digest does not depend on how the scan happened to
        // walk, and so two replicas of one range agree when they hold the same keys.
        let digest = user.iter().fold(0u64, |acc, (key, value)| {
            acc.wrapping_add(fnv(key).rotate_left(1) ^ fnv(value))
        });
        self.env.trace(TraceEvent::RaftSnapshotState {
            server: self.id.0,
            range: range.get(),
            last_index: at.last_index,
            last_term: at.last_term,
            applied: store.applied(),
            user_keys: user.len() as u64,
            user_digest: digest,
            log_keys: log.len() as u64,
        });
    }

    /// The range's user-key interval, from the ranges configuration.
    fn user_span(&self, range: RangeId) -> Option<KeyRange<Bytes>> {
        self.ranges
            .iter()
            .find(|one| one.id == range)
            .map(Range::span)
    }

    /// Tells a sender to start its stream over, and traces **why** (RAFT.md:203-212).
    // PROPOSED(D-083): the node's start-over is traced, with its reason.
    async fn start_over(
        &self,
        range: RangeId,
        from: ServerId,
        reason: StartOver,
        term: Term,
        at: (Index, Term),
    ) {
        self.refuse(range, from, SnapshotStatus::Restart, reason, term, at)
            .await;
    }

    /// Refuses one chunk: the answer that says so, and the trace that says **why**
    /// (RAFT.md:203-218).
    ///
    /// The node refuses a chunk for conditions that mean different things — a changed
    /// identity, staged bytes it could not use, a range it does not host, and no slot
    /// under its receive cap — and until D-083 the message carried only
    /// `SnapshotStatus::Restart` for every one of them. The trace tells them apart, and
    /// has since D-083: until it did, a run whose streams restarted six hundred times
    /// and one whose streams restarted none produced the same trace, which is how a
    /// livelock sat under a green check.
    ///
    /// The *answer* now tells the last of them apart too, because the sender has to act
    /// differently on it: three of these say the ground this stream covered is gone and
    /// are counted against RAFT.md:210-212's restart bound, and a cap-wait says the
    /// receiver is busy, changes nothing, and is bounded as an unanswered chunk is
    /// ([`Step::Wait`]). The event stays one event with a reason on it, because a check
    /// that wants to tell a cap-wait from an identity change reads the reason (D-090).
    // PROPOSED(D-090): the answer says which refusal it is, not only the trace.
    async fn refuse(
        &self,
        range: RangeId,
        from: ServerId,
        status: SnapshotStatus,
        reason: StartOver,
        term: Term,
        at: (Index, Term),
    ) {
        self.env.trace(TraceEvent::RaftSnapshotStartOver {
            server: self.id.0,
            range: range.get(),
            from: from.0,
            reason,
        });
        let incarnation = self
            .stores
            .get(&range)
            .map_or(0, |store| store.incarnation());
        self.answer(range, from, refusal(status, term, at), incarnation)
            .await;
    }

    /// Answers a sender, stamped with this store's incarnation (D-042).
    async fn answer(&self, range: RangeId, to: ServerId, message: Message, incarnation: u64) {
        let message = match message {
            Message::InstallSnapshotResponse {
                term,
                last_index,
                last_term,
                file,
                offset,
                status,
                ..
            } => Message::InstallSnapshotResponse {
                term,
                last_index,
                last_term,
                file,
                offset,
                status,
                incarnation,
            },
            other => other,
        };
        let mut builder = crate::frame::Builder::new(MAX_FRAME_LEN);
        let encoded = Frame {
            from: self.id,
            message,
        }
        .encode();
        if !builder.fits(encoded.len()) {
            return;
        }
        builder.push(range, &encoded);
        self.ship(to, builder.finish()).await;
    }

    /// Puts one frame on this task's handle of the node's socket.
    async fn ship(&self, to: ServerId, frame: Bytes) {
        if let Some(addr) = self.addrs.get(&to) {
            let _ = self.sock.send(*addr, frame).await;
        }
    }

    /// Gives a stream up without asking for a fresh take.
    fn give_up(&mut self, range: RangeId, to: ServerId) {
        self.local
            .push(crate::server::Local::Snapshot(SnapAnswer::Failed {
                range,
                to,
                retake: false,
            }));
    }

    /// Gives a stream up and asks for a fresh take: the version it would have opened
    /// is not there or is not whole.
    fn retake(&mut self, range: RangeId, to: ServerId) {
        self.local
            .push(crate::server::Local::Snapshot(SnapAnswer::Failed {
                range,
                to,
                retake: true,
            }));
    }

    /// How long a chunk may be outstanding before it is resent, as the one-group task
    /// sets it: half an election timeout's worth of ticks.
    fn chunk_timeout(&self) -> Duration {
        Duration::from_nanos(self.config.tick_nanos * self.config.election_ticks.0 / 2)
    }
}

/// Which staging directories a completed install clears: **its own assembly's, and no
/// other**.
///
/// The names are every entry in the engine directory; the answer is the names to
/// clear. An install has just read one (range, sender)'s staged bytes into the engine
/// and they are debris; every *other* staging directory under the same engine
/// directory belongs to a stream that is still running, and clearing it destroys the
/// bytes that stream has assembled without telling its sender, which then goes on
/// writing the rest of a snapshot into a directory that no longer holds its first
/// ones (D-075's keys, undone at the moment they matter).
///
/// It is a decision rather than a loop because it is the whole of what the variant
/// changes, and because what the variant costs does not show up in a run: the wrecked
/// stream's next chunk fails the engine's own check at `open_span_source`, the node
/// asks its sender to start over, and the stream completes a round trip later. The
/// scenario sees an install that landed either way. So the mutation is caught here,
/// where it is made, and the entry says so rather than claiming a sweep catches it.
// PROPOSED(D-083): an install clears its own assembly's directory and no other.
#[must_use]
pub fn swept_on_install(source: &Path, names: &[String], variants: NodeVariants) -> Vec<String> {
    let mine = source.file_name().and_then(|name| name.to_str());
    if variants.contains(NodeVariant::InstallSweepsEveryStaging) {
        // The variant: every staging directory under the engine directory.
        return names
            .iter()
            .filter(|name| name.starts_with("staging-"))
            .cloned()
            .collect();
    }
    mine.map(|name| vec![name.to_owned()]).unwrap_or_default()
}

/// Which cores one stream's acknowledgement is stepped into: **the stream's range,
/// and no other** (issue #103).
///
/// `Input::SnapshotAcked { to }` names the follower and not the range. On a server
/// with one core that is complete information; on a node with four it is not, and a
/// wiring that took the follower's identity as the address would set `stream_acked` on
/// every range's progress for that follower. D-049's rule — a *refused* follower counts
/// for check quorum only while its re-seed stream progresses (core.rs:1607-1613) —
/// would then hold on one range and be void on the other three, with nothing to show
/// for it: no event, no counter, and every check green.
///
/// The address is the range, and it travels the way every other node-local input's
/// range travels: on the `Local` itself, read back by `Host::local_range`, which is
/// what picks the one core (D-073, D-076). The range is deliberately *not* added to
/// `Input::SnapshotAcked` — see D-083: all four of the snapshot inputs have the same
/// shape, the field would be read by nothing, and one of the four carrying it would
/// make the other three look addressed when they are not.
// PROPOSED(D-083): a stream's answers are stepped into the stream's range alone.
#[must_use]
pub fn acked_for(range: RangeId, hosted: &[RangeId], variants: NodeVariants) -> Vec<RangeId> {
    if variants.contains(NodeVariant::SnapshotAckToEveryCore) {
        // The variant: the follower's identity taken as the address, so one range's
        // chunks answer for every range on the node.
        return hosted.to_vec();
    }
    vec![range]
}

/// The take number a version directory's name carries, for the pin a running stream
/// puts on it.
fn take_of(dir: &Path) -> u64 {
    dir.file_name()
        .and_then(|name| name.to_str())
        .and_then(crate::snapshot::parse_version)
        .map_or(0, |(_, _, take)| take)
}

/// Moves one assembly's file bookkeeping on by the chunk that just landed.
fn advance(inbound: &mut Inbound, file: &Bytes, offset: u64, total: u64, len: usize) {
    let at = offset + len as u64;
    if at >= total {
        inbound.done.insert(file.clone(), total);
        inbound.last_done = Some((file.clone(), total));
        inbound.current = None;
        return;
    }
    inbound.current = Some((file.clone(), at, total));
}

/// What an assembly wants next: the file it is in the middle of, or the one it last
/// completed, which is where a sender between files resumes.
fn wanted(inbound: &Inbound) -> (Bytes, u64) {
    match &inbound.current {
        Some((file, at, _)) => (file.clone(), *at),
        None => match &inbound.last_done {
            Some((file, at)) => (file.clone(), *at),
            None => (Bytes::new(), 0),
        },
    }
}

/// What a snapshot action from a core becomes for the node's tasks.
///
/// A take and a follower's compaction record are the `apply` task's, between two
/// applies (RAFT.md §1, D-036); a stream is this task's. The node routes a take
/// before this is reached ([`crate::node::Node`]), so what arrives here is a stream —
/// and a take that arrives anyway is [`NodeVariant::TakeToSnapshotTask`], which is
/// answered by doing nothing, exactly as the one-group task does (node.rs:1912-1915).
#[must_use]
pub fn job_of(range: RangeId, action: SnapshotAction) -> Option<SnapJob> {
    match action {
        SnapshotAction::Install { to, index, term } => Some(SnapJob::Stream {
            range,
            to,
            index,
            term,
        }),
        SnapshotAction::Take | SnapshotAction::Record => None,
    }
}

/// A small order-free hash for the state digest: FNV-1a over the bytes.
///
/// It is not a cryptographic hash and does not need to be. It exists so two replicas
/// of one range can be compared in a trace without the trace carrying every value.
pub(crate) fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x1000_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const R2: RangeId = RangeId(2);
    const R5: RangeId = RangeId(5);
    const HOSTED: [RangeId; 3] = [RangeId(1), R2, R5];

    /// A completed install clears the staging directory it read, and no other.
    ///
    /// The mutation beside it needs more than one range — with one range and one
    /// sender there is only ever one staging directory under the engine directory, and
    /// "every" one of them is the right one.
    // PROPOSED(D-083): an install clears its own assembly's directory and no other.
    #[test]
    fn a_completed_install_clears_its_own_assemblys_directory_alone() {
        // Two ranges assembling from two senders: r2 from node 1, r5 from node 3.
        let names = [
            "staging-r2-s1".to_owned(),
            "staging-r5-s3".to_owned(),
            "snap-r2-40-1".to_owned(),
            "CURRENT".to_owned(),
        ];
        let source = Path::new("/node/staging-r2-s1");

        let correct = swept_on_install(source, &names, NodeVariants::correct());
        assert_eq!(
            correct,
            vec!["staging-r2-s1".to_owned()],
            "the install cleared {} directories, not its own alone",
            correct.len()
        );

        // The pair: r5's half-assembled stream is destroyed by r2's install, and its
        // sender is never told.
        let every = swept_on_install(
            source,
            &names,
            NodeVariants::of(&[NodeVariant::InstallSweepsEveryStaging]),
        );
        assert!(
            every.contains(&"staging-r5-s3".to_owned()),
            "the variant is meant to clear another range's staging directory: {every:?}"
        );

        // And with one range and one sender the two are the same sweep, which is why
        // this mutation needs a node of several.
        let one = ["staging-r2-s1".to_owned()];
        assert_eq!(
            swept_on_install(source, &one, NodeVariants::correct()),
            swept_on_install(
                source,
                &one,
                NodeVariants::of(&[NodeVariant::InstallSweepsEveryStaging])
            ),
            "with one staging directory the variant is the correct sweep"
        );
    }

    /// Issue #103: a stream's acknowledgement answers the stream's range and no other.
    ///
    /// The variant beside it is the mutation the issue describes, and it is one a node
    /// of a single range could not be wrong about: with one core, "every core on the
    /// node" and "the stream's range's core" name the same core.
    // PROPOSED(D-083): a stream's answers are stepped into the stream's range alone.
    #[test]
    fn a_streams_acknowledgement_answers_its_own_ranges_core_alone() {
        let correct = acked_for(R2, &HOSTED, NodeVariants::correct());
        assert_eq!(
            correct,
            vec![R2],
            "an acknowledgement of r2's stream answered {} cores",
            correct.len()
        );

        // The pair. Every range on the node is answered, including the two whose
        // stream was never opened — which is exactly what makes D-049's rule void
        // there: `stream_acked` is set on a follower's progress in a core that is
        // streaming nothing to it.
        let fanned = acked_for(
            R2,
            &HOSTED,
            NodeVariants::of(&[NodeVariant::SnapshotAckToEveryCore]),
        );
        assert_eq!(
            fanned, HOSTED,
            "the variant is meant to answer every core on the node"
        );
        let unasked: Vec<RangeId> = fanned.iter().copied().filter(|one| *one != R2).collect();
        assert_eq!(
            unasked,
            vec![RangeId(1), R5],
            "the variant is meant to answer ranges that opened no stream at all"
        );

        // And the property the issue is really about: on a node of one range the two
        // are the same answer, so no single-range scenario can tell them apart.
        let one = [R2];
        assert_eq!(
            acked_for(R2, &one, NodeVariants::correct()),
            acked_for(
                R2,
                &one,
                NodeVariants::of(&[NodeVariant::SnapshotAckToEveryCore])
            ),
            "with one range the fan-out is the correct route, which is why this \
             mutation needs a node of several"
        );
    }

    // ---- RAFT.md:209-212's bounds on the node, and the cap-wait they must not count.

    /// The four ranges of the check below, and the two slots they share.
    const FOUR: [RangeId; 4] = [RangeId(1), R2, RangeId(3), RangeId(4)];
    /// The receive cap: below the range count, which is where D-075 puts it and where
    /// §12's re-seed shape puts it on purpose.
    const SLOTS: usize = 2;
    /// The leader streaming to this node, one per range.
    const LEADER: ServerId = ServerId(7);
    /// How many chunks one stream carries here, and therefore how many rounds a stream
    /// that waits is made to wait. Long enough that the two ranges holding slots are
    /// still streaming while the two that wait spend a bound — **either** bound — so the
    /// check reads which bound was spent and not who finished first.
    ///
    /// It is written against [`CHUNK_RESENDS`] rather than as a number, and that is the
    /// whole point of the value. At 4 it sat one round *under* the resend bound, so a
    /// cap-wait that failed to clear the resend counter was still not given up here and
    /// the mutation went uncaught — which is exactly what the review found by planting
    /// it. Above the resend bound, the same mutation ends two of these four streams with
    /// `retake: false`, which is what it costs on a running node.
    // PROPOSED(D-090): a cap-wait that stopped clearing the resend counter is caught.
    const CHUNKS: usize = CHUNK_RESENDS as usize + 2;

    /// Where one stream of the check below ended.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Ended {
        /// Installed: the stream got its slot and ran to its last chunk.
        Installed,
        /// Given up, and whether its leader was told to take a fresh checkpoint.
        GaveUp {
            /// `true` is RAFT.md:210-212's "counts the checkpoint as unusable".
            retake: bool,
        },
    }

    /// One (range, sender) stream as its sender sees it.
    struct Stream {
        range: RangeId,
        /// The sender's own two counters, stepped by [`booked`] and by the resend the
        /// round below stands for — the node's arithmetic, not a copy of it.
        // PROPOSED(D-090): the bounds' arithmetic is a value a check can drive.
        counted: Counted,
        waits: u32,
        chunks: usize,
        ended: Option<Ended>,
    }

    /// A range's two key intervals: its Raft state under tenant 0, its user keys under
    /// tenant 2 (D-066).
    fn spans(range: RangeId) -> Vec<KeyRange<Bytes>> {
        let at = |tenant: u64, g: u64| {
            Bytes::copy_from_slice(&[tenant.to_be_bytes(), g.to_be_bytes()].concat())
        };
        let g = range.get();
        vec![at(0, g)..at(0, g + 1), at(2, g)..at(2, g + 1)]
    }

    /// Runs `ranges` streams into one node of `cap` slots, each stream sending one chunk
    /// per round, and says where each ended and how often it was made to wait.
    ///
    /// The receiver is the real planner — the cap, the queue and the slot granting are
    /// `Snapshots`' own — and the three halves this entry adds are the real functions:
    /// [`cap_status`] for the answer a cap-wait gets, [`step_for`] for what the sender
    /// does with an answer, and [`booked`] for what it then records. What the harness
    /// stands in for is the I/O between them and the clock that paces a resend.
    ///
    /// **Each round is one stream's chunk falling due**, so the round counts a resend
    /// exactly as `Task::due` counts one and gives the stream up on `CHUNK_RESENDS`
    /// exactly as `Task::due` gives it up. That is not decoration: it is what makes a
    /// cap-wait that fails to clear the resend counter show up here as the correct node
    /// giving streams up for *waiting*, which is the outcome this entry's design
    /// section rejects and which nothing in the tree could see before.
    fn run(
        ranges: &[RangeId],
        cap: usize,
        variants: NodeVariants,
    ) -> BTreeMap<RangeId, (Ended, u32)> {
        let mut plan = Snapshots::new("/n1", ServerId(9), cap, variants);
        for range in ranges {
            plan.host(*range, spans(*range));
        }
        let at = Identity {
            term: 4,
            last_index: 40,
            last_term: 4,
        };
        let mut streams: Vec<Stream> = ranges
            .iter()
            .map(|range| Stream {
                range: *range,
                counted: Counted::default(),
                waits: 0,
                chunks: 0,
                ended: None,
            })
            .collect();
        // A bound on the harness itself, so a stream that neither lands nor gives up
        // fails this check rather than hanging it. Nothing correct comes near it.
        for _ in 0..1_000 {
            if streams.iter().all(|s| s.ended.is_some()) {
                break;
            }
            for stream in &mut streams {
                if stream.ended.is_some() {
                    continue;
                }
                // This round is this stream's chunk falling due, and a resend is what
                // `Task::due` counts and what it gives a stream up on. A stream whose
                // answers stop clearing the counter dies here, without a retake asked
                // for, because the checkpoint was never what was wrong.
                // PROPOSED(D-090): a cap-wait is counted against no give-up bound.
                stream.counted.resends += 1;
                if stream.counted.resends > CHUNK_RESENDS {
                    stream.ended = Some(Ended::GaveUp { retake: false });
                    continue;
                }
                let last = stream.chunks + 1 == CHUNKS;
                let landing = plan.on_chunk(stream.range, LEADER, at, 16, last);
                let status = match landing {
                    Landing::Staged { .. } => SnapshotStatus::More,
                    Landing::Waiting { .. } => cap_status(variants),
                    Landing::Restarted { .. } => SnapshotStatus::Restart,
                    Landing::Complete(_) => {
                        // The install's switch is made and the slot given back, which
                        // is `finish`'s job on the running node too.
                        plan.finish(stream.range, LEADER);
                        SnapshotStatus::Installed
                    }
                    Landing::Installed { .. } | Landing::NotHosted => {
                        panic!(
                            "{} landed {landing:?}, which this check does not build",
                            stream.range
                        )
                    }
                };
                let step = step_for(status, stream.counted.restarts, variants);
                stream.counted = booked(step, stream.counted);
                match step {
                    Step::Resume => stream.chunks += 1,
                    Step::Done => stream.ended = Some(Ended::Installed),
                    Step::Restart => stream.chunks = 0,
                    Step::Unusable => stream.ended = Some(Ended::GaveUp { retake: true }),
                    Step::Wait => {
                        // The chunk falls due on the resend timer and is asked again,
                        // and the ask is what takes the slot. Nothing is given up for
                        // waiting: the answer resets the resend counter, because a
                        // receiver that answers is not a receiver gone silent.
                        stream.waits += 1;
                    }
                }
            }
        }
        streams
            .iter()
            .map(|s| {
                let ended = s.ended.unwrap_or_else(|| {
                    panic!(
                        "{} neither installed nor gave up in a thousand rounds",
                        s.range
                    )
                });
                (s.range, (ended, s.waits))
            })
            .collect()
    }

    /// A stream waiting for a slot is not a stream whose checkpoint is unusable.
    ///
    /// Four ranges share two slots. Under the correct node the two that wait are told
    /// to *wait*, keep their place and install when a slot frees. Under the variant
    /// they are told to start over, which is what the node said before this entry, and
    /// RAFT.md:210-212's bound spends itself on them: at the third ask their leader
    /// throws away a checkpoint nothing was ever wrong with and takes a fresh one. On
    /// the running node that is not a permanent failure — the leader retakes and opens
    /// the stream again — which is worse than it reads here, because the cycle repeats
    /// for as long as the cap is contended, and the cap is contended by design.
    ///
    /// **A single-range world cannot catch it.** With one range there is no cap-wait to
    /// answer at all: the only sender that could contend the slot is a stale leader of
    /// that same range, and a chunk at a higher term displaces the assembly
    /// (`Snapshots::superseded`) rather than queueing behind it. The last clause below
    /// is that statement as a check — one range, one slot, and the two answers
    /// identical.
    // PROPOSED(D-090): a cap-wait is answered as a wait, not as a start-over.
    #[test]
    fn a_stream_waiting_for_a_slot_is_not_a_checkpoint_counted_unusable() {
        let correct = run(&FOUR, SLOTS, NodeVariants::correct());
        assert!(
            correct.values().all(|(end, _)| *end == Ended::Installed),
            "every range should install once a slot frees: {correct:?}"
        );
        // And the situation was reached: two ranges over two slots did wait, so the
        // clause above is not a green against a cap that was never contended.
        let waited = correct.values().filter(|(_, waits)| *waits > 0).count();
        assert_eq!(
            waited,
            FOUR.len() - SLOTS,
            "two of four ranges should have waited for a slot: {correct:?}"
        );

        // The pair: the conflation as it stood. The two ranges over the cap are given
        // up with a retake asked for, which is RAFT.md's answer to an unusable
        // checkpoint and the wrong answer to a busy receiver.
        let conflated = run(
            &FOUR,
            SLOTS,
            NodeVariants::of(&[NodeVariant::CapWaitIsAStartOver]),
        );
        let retaken = conflated
            .values()
            .filter(|(end, _)| *end == Ended::GaveUp { retake: true })
            .count();
        assert_eq!(
            retaken,
            FOUR.len() - SLOTS,
            "the variant is meant to count a cap-wait against the restart bound: \
             {conflated:?}"
        );

        // And the property this rests on, at its tightest: one range on a node of
        // **one** slot — the smallest cap there is — never waits either, because the
        // range's own stream takes the slot and the only sender that could contend it
        // is a stale leader the assembly displaces or the store's term refuses. The
        // variant and the correct node are the same node there, which is why four
        // ranges sharing one cap is what this mutation needs.
        let one = [R2];
        let waited = run(&one, 1, NodeVariants::correct());
        assert!(
            waited
                .values()
                .all(|(end, waits)| *end == Ended::Installed && *waits == 0),
            "one range on one slot should install without ever waiting: {waited:?}"
        );
        assert_eq!(
            waited,
            run(
                &one,
                1,
                NodeVariants::of(&[NodeVariant::CapWaitIsAStartOver])
            ),
            "with one range no chunk ever waits, so neither answer is ever sent"
        );
    }

    /// RAFT.md:210-212's restart bound, which the node did not have: a stream is
    /// restarted from its first byte twice, and the third such ask counts the
    /// checkpoint unusable.
    ///
    /// The variant is the node as it stood — nothing counted the restarts and every
    /// restart reset the resend counter, so neither bound was reachable for a stream
    /// that kept being told to start over. That is the shape of the livelock D-083
    /// found: 669 start-overs in a run, and no bound between it and forever.
    // PROPOSED(D-090): the node honours RAFT.md's restart bound.
    #[test]
    fn a_stream_restarted_twice_has_its_checkpoint_counted_unusable_at_the_third_ask() {
        let correct = NodeVariants::correct();
        assert_eq!(step_for(SnapshotStatus::Restart, 0, correct), Step::Restart);
        assert_eq!(step_for(SnapshotStatus::Restart, 1, correct), Step::Restart);
        assert_eq!(
            step_for(SnapshotStatus::Restart, 2, correct),
            Step::Unusable,
            "the third ask is the one RAFT.md:210-212 gives up on"
        );

        // The pair: unbounded. A receiver that answers `Restart` forever is answered
        // forever, which is the loop this bound exists to cut.
        let unbounded = NodeVariants::of(&[NodeVariant::RestartsNotCounted]);
        for restarts in [0, 1, 2, 3, 100, 669] {
            assert_eq!(
                step_for(SnapshotStatus::Restart, restarts, unbounded),
                Step::Restart,
                "the variant is meant to restart however many times it is asked"
            );
        }
    }

    /// A cap-wait is a wait however many restarts a stream has behind it: the two
    /// bounds do not share a counter.
    ///
    /// This is the half of the ruling that keeps the re-seed shape working. §12 sets
    /// the cap below the range count deliberately, so a stream that has restarted twice
    /// for an honest reason — its leader retook while it streamed — and then finds the
    /// cap full must still be told to wait, not counted out.
    // PROPOSED(D-090): a cap-wait is not counted against the restart bound.
    #[test]
    fn a_cap_wait_is_a_wait_whatever_the_stream_has_restarted() {
        let correct = NodeVariants::correct();
        for restarts in [0, 1, 2, 3, 100] {
            assert_eq!(
                step_for(SnapshotStatus::Waiting, restarts, correct),
                Step::Wait,
                "a stream with {restarts} restarts behind it is still only waiting"
            );
        }
        // And the answer the node sends for one, which is what makes the sender able to
        // tell the two apart at all.
        assert_eq!(cap_status(correct), SnapshotStatus::Waiting);
        assert_eq!(
            cap_status(NodeVariants::of(&[NodeVariant::CapWaitIsAStartOver])),
            SnapshotStatus::Restart,
            "the variant is meant to be the one answer the node had for both"
        );
    }

    /// The sender's two counters, which are what the two bounds are actually made of.
    ///
    /// [`step_for`] says what an answer *means*; this says what the node then
    /// *records*, and a check that drives only the first proves nothing about the
    /// second. The review of this entry found that gap by planting it: both lines
    /// below could be deleted with the entire workspace suite green in release.
    /// Deleting the reset makes the correct node give streams up for waiting — the
    /// outcome this entry's design section spends a section rejecting — and deleting
    /// the increment is `RestartsNotCounted` at the site the node actually runs, rather
    /// than inside the function the check above drives.
    // PROPOSED(D-090): the bounds' arithmetic is a value a check can drive.
    #[test]
    fn a_wait_clears_the_resend_counter_and_a_restart_is_the_only_step_that_counts_one() {
        // A stream one resend from the bound, with two restarts behind it.
        let spent = Counted {
            resends: CHUNK_RESENDS,
            restarts: 2,
        };

        // A cap-wait clears the resends and counts against nothing — whatever the
        // restarts, because the two bounds do not share a counter.
        assert_eq!(
            booked(Step::Wait, spent),
            Counted {
                resends: 0,
                restarts: 2
            },
            "a cap-wait must clear the resend counter and count against no bound"
        );

        // And it never runs out, which is the claim "counted against no give-up bound"
        // actually makes: a stream answered `Waiting` for ever, its chunk falling due
        // between each answer as `Task::due` makes it, never reaches either bound.
        let mut counted = Counted::default();
        for ask in 0..CHUNK_RESENDS * 100 {
            counted.resends += 1;
            counted = booked(Step::Wait, counted);
            assert!(
                counted.resends <= CHUNK_RESENDS,
                "ask {ask}: a waiting stream reached the resend bound at {counted:?}"
            );
        }
        assert_eq!(
            counted.restarts, 0,
            "a wait is counted against no give-up bound at all"
        );

        // A restart is counted, and starts its stream's resends afresh because the
        // chunk outstanding is a new one: the first byte again.
        assert_eq!(
            booked(Step::Restart, spent),
            Counted {
                resends: 0,
                restarts: 3
            },
            "a restart is the one step RAFT.md:210-212's bound counts"
        );
        // Any other answer clears the resends and counts nothing; a stream that has
        // ended carries its bookkeeping nowhere.
        assert_eq!(
            booked(Step::Resume, spent),
            Counted {
                resends: 0,
                restarts: 2
            }
        );
        assert_eq!(booked(Step::Done, spent), spent);
        assert_eq!(booked(Step::Unusable, spent), spent);

        // The two together are the bound, driven as the node drives them: three asks,
        // and the third is the one that gives the stream up.
        let mut counted = Counted::default();
        let correct = NodeVariants::correct();
        for expected in [Step::Restart, Step::Restart, Step::Unusable] {
            let step = step_for(SnapshotStatus::Restart, counted.restarts, correct);
            assert_eq!(step, expected, "at {counted:?}");
            counted = booked(step, counted);
        }
        assert_eq!(
            counted.restarts, STREAM_RESTARTS,
            "the bound is spent by exactly the restarts RAFT.md:210-212 allows"
        );
    }

    /// The answer a cap-wait carries **on the wire**, which is the half of this entry a
    /// sender actually reads.
    ///
    /// [`cap_status`] chooses the status and [`refusal`] turns it into the message, and
    /// a check of the first alone leaves the second free to send `start_over` for a
    /// `Waiting` — `CapWaitIsAStartOver` reinstated one line further down, and
    /// invisible: [`snapshot::waiting`] has exactly one caller in the tree, the
    /// committed scenario answers no cap-wait at all, and the review planted exactly
    /// this with the whole suite green.
    // PROPOSED(D-090): the answer says which refusal it is, not only the trace.
    #[test]
    fn a_cap_wait_is_refused_with_a_waiting_answer_and_every_other_refusal_with_a_start_over() {
        const TERM: Term = 4;
        const AT: (Index, Term) = (40, 3);
        let read = |message: Message| match message {
            Message::InstallSnapshotResponse {
                term,
                last_index,
                last_term,
                status,
                ..
            } => (term, last_index, last_term, status),
            other => panic!("a refusal is an InstallSnapshotResponse, not {other:?}"),
        };

        // The cap-wait, taken through the function that chooses the status: the two
        // halves joined, which is what the node does at `Landing::Waiting`.
        assert_eq!(
            read(refusal(cap_status(NodeVariants::correct()), TERM, AT)),
            (TERM, AT.0, AT.1, SnapshotStatus::Waiting),
            "a cap-wait must go out as `Waiting`, and carry the staged identity"
        );

        // The three refusals that are start-overs — a changed identity, staged bytes
        // the node could not use, a range it does not host — all carry `Restart`, which
        // is what RAFT.md:210-212's bound counts.
        assert_eq!(
            read(refusal(SnapshotStatus::Restart, TERM, AT)),
            (TERM, AT.0, AT.1, SnapshotStatus::Restart)
        );

        // The pair: under the variant the same site sends the one answer the node had
        // for both, so the sender cannot tell a busy receiver from a moved checkpoint.
        assert_eq!(
            read(refusal(
                cap_status(NodeVariants::of(&[NodeVariant::CapWaitIsAStartOver])),
                TERM,
                AT
            ))
            .3,
            SnapshotStatus::Restart,
            "the variant is meant to answer a cap-wait with a start-over"
        );
    }
}
