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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ananke_env::{
    Clock, Either, Environment, File, FileSystem, MAX_FRAME_LEN, Network, OpenOptions, Socket,
    TraceEvent, race,
};
use ananke_raft::core::{RaftConfig, SnapshotAction};
use ananke_raft::message::{Frame, Message, SnapshotStatus};
use ananke_raft::queue::Queue;
use ananke_raft::snapshot::{self, Repair, Sender};
use ananke_raft::store::RaftStore;
use ananke_raft::types::{Configuration, Index, ServerId, Term};
use bytes::Bytes;

use crate::range::RangeId;
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
const CHUNK_RESENDS: u32 = 4;

/// One stream being sent to one follower of one range: the sender's bookkeeping, and
/// when the chunk outstanding on it falls due for a resend.
struct Outbound {
    sender: Sender,
    deadline: ananke_env::Instant,
    resends: u32,
    /// The furthest point the stream reached, as (file position, offset): an
    /// acknowledgement past it is progress, and progress is what check quorum counts
    /// for a refused follower (D-049).
    furthest: (usize, u64),
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
                Some(job) => {
                    self.job(job).await;
                    // And then whatever is due, because the race above fires the timer
                    // only when the queue is *empty*. On a node of four ranges it
                    // rarely is: a chunk of one range, an answer of another and the
                    // `raft` task's repairs keep arriving, the timer loses every race,
                    // and a stream whose chunk was lost is never resent and never
                    // given up. Its leader has `installing` set and will not ask again
                    // until it is told, so that replica is fed nothing for the rest of
                    // the run — one range's traffic starving another range's re-seed.
                    //
                    // The directed re-seed shape found it: four re-seeds against a cap
                    // of two, where two ranges' streams keep the queue busy while the
                    // two waiting for a slot go quiet. `due` acts only on streams whose
                    // deadline has passed, so asking it after every job costs a
                    // comparison per job.
                    // PROPOSED(D-081): a busy queue does not starve a stream's resend.
                    if !self.variants.contains(NodeVariant::DueOnlyWhenIdle)
                        && self
                            .deadline()
                            .is_some_and(|deadline| deadline <= self.env.clock().now())
                    {
                        self.due().await;
                    }
                }
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
                out.resends += 1;
                out.resends > CHUNK_RESENDS
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
        if self.plan.is_streaming(range, to, at) {
            return;
        }
        let Some(store) = self.stores.get(&range).cloned() else {
            return;
        };
        // The version to stream is the one this range's own snapshot record names, and
        // a stream pins it for its whole life (D-043). A record that names no complete
        // take — the store compacted without checkpointing (D-065, D-078), or a crash
        // landed between the record and the checkpoint — is not a stream to open but a
        // take to ask for, which `retake` does.
        let dir = match store.snapshot_record().await {
            Ok(Some(record)) if record.taken && record.last_index == index => record.dir,
            _ => return self.retake(range, to),
        };
        let started = self.plan.stream(range, to, at);
        if matches!(started, Started::Waiting) {
            // Only `CapStreamsSent` answers this: Q14 puts no cap on streams sent.
            return self.give_up(range, to);
        }
        let sender = match Sender::open(&self.env, Path::new(&dir), to, index, term, at.term).await
        {
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
                resends: 0,
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
                // disturbed. The sender restarts, and takes a slot the next time it
                // asks while one is free (D-075).
                let answer = snapshot::start_over(store.term(), (last_index, last_term));
                self.answer(range, from, answer, store.incarnation()).await;
            }
            Landing::Restarted { dir, .. } => {
                self.clear(&dir).await;
                self.receiving.insert((range, from), Inbound::default());
                let answer = snapshot::start_over(store.term(), (last_index, last_term));
                self.answer(range, from, answer, store.incarnation()).await;
            }
            Landing::Staged { dir, .. } => {
                if self.stage(&dir, &file, offset, &data).await.is_err() {
                    let answer = snapshot::start_over(store.term(), (last_index, last_term));
                    return self.answer(range, from, answer, store.incarnation()).await;
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
                    let answer = snapshot::start_over(store.term(), (last_index, last_term));
                    return self.answer(range, from, answer, store.incarnation()).await;
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
        let Some(out) = self.sending.get_mut(&(range, from)) else {
            return;
        };
        if out.sender.last_index != last_index || out.sender.last_term != last_term {
            // An answer to a stream this node is no longer sending: stale.
            return;
        }
        match status {
            SnapshotStatus::More => {
                out.sender.on_more(&file, offset);
                out.resends = 0;
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
            SnapshotStatus::Installed => {
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
            SnapshotStatus::Restart => {
                out.sender.restart();
                out.resends = 0;
                self.send_chunk(range, from).await;
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
        let switched = if install.repair_in_switch {
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
        self.clear_staging(&install.source).await;
        self.plan.finish(range, from);
        self.receiving.remove(&(range, from));
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
        let answer =
            snapshot::start_over(store.term(), (install.at.last_index, install.at.last_term));
        self.answer(range, from, answer, store.incarnation()).await;
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
}
