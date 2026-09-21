//! The node's tasks (SHARD.md §4, §13 Q41; §11, raft 1, 2, 10, 11).
//!
//! Today's unit is a group: one server is one Raft group with a socket and a set of
//! tasks of its own. The unit becomes a *node*, and this module is the two tasks that
//! makes it one:
//!
//! - [`Node::raft`], one task holding every core on the node, keyed by range, driven by one
//!   ticker. Each tick steps a `Tick` into every core; each message steps into its
//!   range's core. A round is a tick's steps, or the messages drained since the last
//!   round, and its order is Q41's ([`crate::round`]). Sends leave through the
//!   per-peer [`Outbox`] the wire already has, one frame per peer per
//!   flush.
//! - [`apply()`], one task per node, applying every range's entries, snapshot takes and
//!   structural batches **one at a time** (Q14). Under D-036 a take is a job between
//!   two applies and applies wait behind the take, so one range's take stalls every
//!   range's applies on the node; the task keeps that, and the stall is one of the
//!   figures Stage B measures.
//!
//! What the tasks need of the node's disk, socket and state machine is [`Host`] and
//! [`Applier`]. The node is the *schedule*; the host is what it drives. Splitting them
//! is what lets a check drive the round with a host that resolves persists in an order
//! it chooses, which is the only way to assert Q41's rule that each core's later
//! outputs wait on *that core's own* persist.
//!
//! This slice runs one range, [`ananke_raft::node::SINGLE_GROUP`]. Bootstrap ranges
//! and four ranges per node, the `snapshot` task, Q15's refusal and follower
//! compaction are each a later slice of Stage B; nothing here assumes one range, and
//! [`Cores`] is keyed by range throughout.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};
use std::time::Duration;

use ananke_env::{Clock, Decision, Either, Environment, Rng, race};
use ananke_raft::core::SnapshotAction;
use ananke_raft::queue::Queue;
use ananke_raft::{Entry, Index, Input, Message, Output, Persist, ServerId};
use bytes::Bytes;

use crate::inbox::{Inbox, Received};
use crate::outbox::Outbox;
use crate::range::RangeId;
use crate::round::{Act, Cores, Meters, Round, entries_to_apply};
use crate::variant::{NodeVariant, NodeVariants};

/// A future a host hands back, borrowing the host and awaited before the task takes
/// its next step.
pub type Boxed<'h, T> = Pin<Box<dyn Future<Output = T> + Send + 'h>>;

/// What the node's `raft` task needs of the node's disk, socket and clients.
///
/// Every method takes `&self`: the task drives one host for the whole node, and the
/// futures it hands back are held side by side while the round's persists share one
/// group commit.
pub trait Host {
    /// Makes `range`'s state durable. The future does nothing until it is polled, and
    /// the task polls every persist of a round before it awaits anything else, so the
    /// WAL writer takes them as one group and syncs them once (wal.rs:16-20, D-018).
    ///
    /// It borrows nothing of the host, because the round holds several of them side
    /// by side while it goes on stepping the cores that persisted nothing: a host
    /// clones what the write needs — the store handle — into the future.
    fn persist(&self, range: RangeId, persist: Persist) -> BoxedPersist;

    /// Stamps a message on its way out: the clock where the lease reads it and the
    /// store incarnation a response carries (RAFT.md §1, D-042). This is the node's
    /// side of `send_message` in `ananke-raft`, which the node cannot do itself
    /// because the incarnation is the store's.
    fn stamp(&self, range: RangeId, message: &mut Message);

    /// Puts one frame on the socket, addressed to `to`.
    fn ship(&self, to: ServerId, frame: Bytes) -> Boxed<'_, ()>;

    /// A read the core confirmed: serve it from the state machine and trace it
    /// (SHARD.md §8, §11 raft 15).
    fn read_ready(
        &self,
        range: RangeId,
        id: u64,
        index: Index,
        lease: bool,
    ) -> Boxed<'_, io::Result<()>>;

    /// A read this server will not serve: it stopped leading first.
    fn read_dropped(&self, range: RangeId, id: u64) -> Boxed<'_, ()>;

    /// A proposal or a read refused because this server is not `range`'s leader.
    fn rejected(&self, range: RangeId, leader: Option<ServerId>);

    /// Hands the node's `apply` task a job. The host owns the queue, so that what the
    /// node does with an `Apply` is one call in the order the round fixes: an `Apply`
    /// handed out before its core's persist resolved would let the `apply` task make
    /// an applied index durable above the durable log (SHARD.md §4).
    fn apply(&self, job: ApplyJob);

    /// Snapshot work for the `snapshot` task, which is a later slice's; a
    /// [`SnapshotAction::Take`] goes to the `apply` task instead (RAFT.md §1), and the
    /// node routes it there rather than here.
    fn snapshot(&self, range: RangeId, action: SnapshotAction);

    /// The node cannot go on: the disk failed under it. The host traces it; the task
    /// returns the error.
    fn failed(&self, reason: String);
}

/// What the node's `apply` task runs, one job at a time.
pub trait Applier {
    /// Runs one job. The task awaits it before it takes the next, whatever range the
    /// next belongs to: that is the whole of Q14's rule, and D-036's consequence that
    /// one range's take holds every range's applies.
    fn run(&self, job: ApplyJob) -> Boxed<'_, ()>;
}

/// What the `apply` task does with one job.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ApplyWork {
    /// Committed entries to apply in order.
    Entries(Vec<Entry>),
    /// Take a checkpoint at the applied index, between two applies (RAFT.md §1).
    Take,
    /// Take a fresh checkpoint even at the recorded index: the recorded one was found
    /// unusable (D-043).
    Retake,
}

/// One job for the node's `apply` task, with the range it belongs to.
///
/// A split's or a merge's structural batch is a later stage's job kind; the rule this
/// task keeps — one at a time, in the order queued, whatever the range — already
/// covers it, which is why the enum is `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyJob {
    /// The range the job is for.
    pub range: RangeId,
    /// What to do.
    pub work: ApplyWork,
}

/// How the node is configured.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// This server's id.
    pub id: ServerId,
    /// The ticker's interval: one tick steps every core on the node.
    pub tick: Duration,
    /// The node's known-buggy variants (CLAUDE.md's pair rule).
    pub variants: NodeVariants,
}

/// The frames and flushes the node's rounds cost: the measurement §4 leaves to this
/// stage (SHARD.md:490-492, §12).
///
/// A round flushes once before its sync, and again as each of its persists resolves.
/// A flush at a resolution is credited to the round whose persist resolved; the
/// replayed work's own outputs ride that flush, which is why the count is of the
/// round's frames and not of one step's.
// PROPOSED(D-073): a resolution's flush is credited to the round whose persist
// resolved, and carries the replayed work's early outputs with it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Frames {
    /// Frames shipped over the run.
    pub frames: u64,
    /// Flushes over the run; a flush with nothing queued ships no frame.
    pub flushes: u64,
    /// Rounds that submitted at least one persist.
    pub rounds_with_persists: u64,
    /// Rounds that submitted none.
    pub rounds_without_persists: u64,
    /// The most frames one round put on the wire toward one peer.
    pub most_frames_to_a_peer_in_a_round: usize,
    /// The most flushes one round took: one, plus one per persisting core.
    pub most_flushes_in_a_round: usize,
    /// Messages the outbox dropped over its per-peer bound (D-072).
    pub outbox_dropped: u64,
    /// Messages refused by the outbox because they would not fit a frame of their
    /// own (D-072).
    pub outbox_oversized: u64,
}

/// A round whose persists are still outstanding, and the frames it has cost so far.
struct Open {
    outstanding: usize,
    frames: BTreeMap<ServerId, usize>,
    flushes: usize,
}

/// A persist in flight: what [`Host::persist`] hands the round.
pub type BoxedPersist = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'static>>;

/// The node: every core on it, its outbox, and what it has measured.
pub struct Node<E: Environment, H: Host> {
    env: E,
    config: NodeConfig,
    cores: Cores,
    outbox: Outbox,
    host: H,
    /// The highest index handed to the `apply` task, per range.
    applied_sent: BTreeMap<RangeId, Index>,
    next_round: u64,
    open: BTreeMap<u64, Open>,
    frames: Frames,
}

impl<E: Environment, H: Host> Node<E, H> {
    /// A node with these cores on it, sending through `outbox` and driving `host`.
    pub fn new(env: E, config: NodeConfig, cores: Cores, outbox: Outbox, host: H) -> Self {
        Self {
            env,
            config,
            cores,
            outbox,
            host,
            applied_sent: BTreeMap::new(),
            next_round: 0,
            open: BTreeMap::new(),
            frames: Frames::default(),
        }
    }

    /// What the node's rounds cost in frames and flushes.
    #[must_use]
    pub fn frames(&self) -> Frames {
        self.frames
    }

    /// What the node measured about held work and replayed ticks.
    #[must_use]
    pub fn meters(&self) -> Meters {
        self.cores.meters()
    }

    /// The cores, for a check that reads a range's state.
    #[must_use]
    pub fn cores(&self) -> &Cores {
        &self.cores
    }

    /// The host, for a check that reads what the node drove it to do.
    #[must_use]
    pub fn host(&self) -> &H {
        &self.host
    }

    /// Where the highest applied index handed out per range stands.
    #[must_use]
    pub fn applied_sent(&self, range: RangeId) -> Index {
        self.applied_sent.get(&range).copied().unwrap_or(0)
    }

    /// The `raft` task (SHARD.md §4): one ticker, every core, Q41's round.
    ///
    /// The loop takes whichever comes first of a message on the inbox, the ticker, and
    /// one of the outstanding persists resolving. It never waits on a persist: while a
    /// sync is outstanding the task goes on stepping the cores that persisted nothing,
    /// so a later round's persists can be submitted behind it (SHARD.md §4, the loaded
    /// case).
    ///
    /// # Errors
    ///
    /// The first persist that fails: a node whose disk failed under it cannot go on.
    pub async fn raft(&mut self, inbox: &Inbox) -> io::Result<()> {
        let mut persists = Persists::default();
        let mut next_tick = self.env.clock().now() + self.config.tick;
        loop {
            let event = {
                let pop = pin!(inbox.pop());
                let timer = pin!(self.env.clock().sleep_until(next_tick));
                let arrived = pin!(race(&self.env, pop, timer));
                let resolved = pin!(persists.next(&self.env));
                match race(&self.env, arrived, resolved).await {
                    Either::Left(Either::Left(Some(first))) => Event::Messages(first),
                    Either::Left(Either::Left(None)) => return Ok(()),
                    Either::Left(Either::Right(())) => {
                        next_tick += self.config.tick;
                        Event::Tick
                    }
                    Either::Right(resolved) => Event::Resolved(resolved),
                }
            };
            let round = match event {
                // A round is a tick's steps, ...
                Event::Tick => self.cores.tick(&self.env),
                // ... or the messages drained since the last round: the one that woke
                // the task and everything queued behind it now.
                Event::Messages(first) => {
                    let mut drained = vec![received(&self.env, first)];
                    while let Some(next) = inbox.take() {
                        drained.push(received(&self.env, next));
                    }
                    self.cores.messages(&self.env, drained)
                }
                Event::Resolved(Resolved {
                    range,
                    round,
                    result,
                }) => {
                    if let Err(error) = result {
                        self.host.failed(error.to_string());
                        return Err(error);
                    }
                    // The core's later outputs, then the work held while its persist
                    // was outstanding: the flush that follows is this round's.
                    let resolved = self.cores.resolved(&self.env, range);
                    self.drive(resolved, Some(round), &mut persists).await?;
                    inbox.hold_at(self.cores.held_bytes());
                    continue;
                }
            };
            self.drive(round, None, &mut persists).await?;
            // A message held for a core whose persist is outstanding was taken from
            // the inbox and is still counted against the node's byte bound (Q14).
            inbox.hold_at(self.cores.held_bytes());
        }
    }

    /// Runs a round and everything it sets off, without recursion: a round whose
    /// persists are awaited one at a time (the variant) queues the resolutions it
    /// produces behind it.
    async fn drive(
        &mut self,
        first: Round,
        credit: Option<u64>,
        persists: &mut Persists,
    ) -> io::Result<()> {
        let mut queue = VecDeque::from([(first, credit)]);
        while let Some((round, credit)) = queue.pop_front() {
            if round.is_empty() && credit.is_none() {
                continue;
            }
            for act in round.early {
                self.act(act).await?;
            }
            // The round's flush: before the round's sync when the round is its own,
            // and the later flush of the core whose persist resolved when it is a
            // resolution's.
            let frames = self.flush().await;
            match credit {
                Some(id) => self.credit(id, &frames),
                None if round.persists.is_empty() => {
                    self.frames.rounds_without_persists += 1;
                    self.fold(&frames, 1);
                }
                None => {}
            }
            if round.persists.is_empty() {
                continue;
            }
            let id = self.next_round;
            self.next_round += 1;
            self.frames.rounds_with_persists += 1;
            self.open.insert(
                id,
                Open {
                    outstanding: round.persists.len(),
                    frames: if credit.is_none() {
                        frames
                    } else {
                        BTreeMap::new()
                    },
                    flushes: usize::from(credit.is_none()),
                },
            );
            if self
                .config
                .variants
                .contains(NodeVariant::PersistsOneAtATime)
            {
                // The variant: each persist pays a sync of its own, awaited here,
                // rather than joining the round's group.
                for submission in round.persists {
                    let range = submission.range;
                    if let Err(error) = self.host.persist(range, submission.persist).await {
                        self.host.failed(error.to_string());
                        return Err(error);
                    }
                    let resolved = self.cores.resolved(&self.env, range);
                    queue.push_back((resolved, Some(id)));
                }
                continue;
            }
            // Submitted together: nothing is awaited between them, so the WAL writer
            // takes the round's records as one group and syncs them once.
            for submission in round.persists {
                let range = submission.range;
                let future = self.host.persist(range, submission.persist);
                persists.submit(range, id, future);
            }
        }
        Ok(())
    }

    /// One output of one core.
    async fn act(&mut self, act: Act) -> io::Result<()> {
        let Act {
            range,
            stamps,
            output,
        } = act;
        match output {
            Output::Persist(_) => {
                debug_assert!(false, "a persist is submitted, never executed in order");
            }
            Output::Send { to, mut message } => {
                self.host.stamp(range, &mut message);
                let frame = ananke_raft::Frame {
                    from: self.config.id,
                    message,
                };
                // PROPOSED(D-073): the outbox's drops are counted here and not yet
                // traced. `TraceEvent` has no kind for them; adding one belongs with
                // the slice that puts the node under the sweeps, where the pinned
                // trace hashes move anyway and the event can be seen to fire.
                match self.outbox.push(to, range, &frame) {
                    Ok(dropped) => self.frames.outbox_dropped += dropped.len() as u64,
                    // A message that cannot fit a frame of its own is the wire's own
                    // refusal (D-072): it is dropped, never cut.
                    Err(_) => self.frames.outbox_oversized += 1,
                }
            }
            Output::Apply { through } => {
                let after = self.applied_sent(range);
                let Some(core) = self.cores.core(range) else {
                    return Ok(());
                };
                let entries = entries_to_apply(core, after, through);
                if entries.is_empty() {
                    return Ok(());
                }
                self.applied_sent.insert(range, through);
                self.host.apply(ApplyJob {
                    range,
                    work: ApplyWork::Entries(entries),
                });
            }
            Output::Rejected { leader } => self.host.rejected(range, leader),
            Output::ReadReady { id, index, lease } => {
                if let Err(error) = self.host.read_ready(range, id, index, lease).await {
                    self.host.failed(error.to_string());
                    return Err(error);
                }
            }
            Output::ReadDropped { id } => self.host.read_dropped(range, id).await,
            // A take is the `apply` task's, between two applies (RAFT.md §1).
            Output::Snapshot(SnapshotAction::Take) => self.host.apply(ApplyJob {
                range,
                work: ApplyWork::Take,
            }),
            Output::Snapshot(action) => self.host.snapshot(range, action),
            // D-047: decided at the step, traced now, which is when it is durable for
            // every event that followed a persist.
            Output::Trace(mut event) => {
                if let ananke_env::TraceEvent::RaftTerm { received: at, .. } = &mut event {
                    *at = stamps.received;
                }
                self.env.trace_decided(stamps.decided, event);
            }
        }
        Ok(())
    }

    /// One flush of the outbox: one frame per peer with something queued.
    async fn flush(&mut self) -> BTreeMap<ServerId, usize> {
        let frames = self.outbox.flush();
        let mut per_peer: BTreeMap<ServerId, usize> = BTreeMap::new();
        for (to, frame) in frames {
            *per_peer.entry(to).or_default() += 1;
            self.frames.frames += 1;
            self.host.ship(to, frame).await;
        }
        self.frames.flushes += 1;
        per_peer
    }

    /// One of round `id`'s persists resolved: add what its later flush cost, and fold
    /// the round into the measurements once the last of its persists has resolved.
    fn credit(&mut self, id: u64, frames: &BTreeMap<ServerId, usize>) {
        let done = {
            let Some(open) = self.open.get_mut(&id) else {
                return;
            };
            open.outstanding = open.outstanding.saturating_sub(1);
            open.flushes += 1;
            for (to, count) in frames {
                *open.frames.entry(*to).or_default() += count;
            }
            open.outstanding == 0
        };
        if done && let Some(open) = self.open.remove(&id) {
            self.fold(&open.frames, open.flushes);
        }
    }

    /// Records a finished round's frames and flushes.
    fn fold(&mut self, frames: &BTreeMap<ServerId, usize>, flushes: usize) {
        let most = frames.values().copied().max().unwrap_or(0);
        self.frames.most_frames_to_a_peer_in_a_round =
            self.frames.most_frames_to_a_peer_in_a_round.max(most);
        self.frames.most_flushes_in_a_round = self.frames.most_flushes_in_a_round.max(flushes);
    }
}

/// What woke the `raft` task.
enum Event {
    /// The ticker.
    Tick,
    /// A message, and whatever is queued behind it.
    Messages(Received),
    /// A persist resolved.
    Resolved(Resolved),
}

/// One message from the inbox as the round takes it.
fn received<E: Environment>(
    env: &E,
    message: Received,
) -> (RangeId, Input, Option<Decision>, usize) {
    // PROPOSED(D-050): when the frame reached the node, for the term change the step
    // may trace. The inbox does not carry the stamp yet, so the node takes one as it
    // hands the message to its core; the wire's stamp is the wire slice's to add.
    // PROPOSED(D-073): the receipt is taken here until the inbox carries the `net`
    // task's.
    let at = env.decision();
    let bytes = message.bytes;
    (
        message.range,
        Input::Message {
            from: message.from,
            message: message.message,
            now: now_nanos(env),
        },
        Some(at),
        bytes,
    )
}

/// The node's clock in nanoseconds, where the core's lease arithmetic reads it: the
/// same figure `send_message` and the one-group server's loop read (node.rs:152-155).
fn now_nanos<E: Environment>(env: &E) -> u64 {
    env.clock().now().as_nanos()
}

/// The `apply` task (SHARD.md §4, Q14): every range's jobs, one at a time.
///
/// One task per node, and no more: if applying is too slow, several ranges' ready
/// applies are grouped into one synced batch *inside* this task, never into more
/// tasks. The grouping is built only if the measured apply lag asks for it
/// (SHARD.md §12), so this is the ungrouped task.
pub async fn apply<A: Applier>(jobs: &Queue<ApplyJob>, applier: &A) {
    while let Some(job) = jobs.pop().await {
        applier.run(job).await;
    }
}

/// The persists a round submitted together, resolved as each core's own resolves.
///
/// The futures are held side by side and every one of them is polled on every poll of
/// [`next`](Persists::next), so each is enqueued with the WAL writer before the task
/// awaits anything, and the writer takes them as one group (wal.rs:16-20, D-018).
///
/// Which of several ready persists is reported first is drawn from the environment's
/// scheduling stream, as [`race`] draws it: a fixed order would let one range's
/// persists always be seen before another's, and the round's whole point is that no
/// core waits on another's disk.
// PROPOSED(D-073): the order several ready persists resolve in is the scheduling
// stream's, not the range order.
#[derive(Default)]
pub struct Persists {
    outstanding: Vec<Outstanding>,
}

struct Outstanding {
    range: RangeId,
    round: u64,
    future: BoxedPersist,
}

/// A persist that resolved.
pub struct Resolved {
    /// Whose it was.
    pub range: RangeId,
    /// The round that submitted it.
    pub round: u64,
    /// What the disk said.
    pub result: io::Result<()>,
}

impl Persists {
    /// Adds a persist to the outstanding set. It is not polled here: the task polls
    /// the whole set at once, which is what puts the round's records in one group.
    pub fn submit(&mut self, range: RangeId, round: u64, future: BoxedPersist) {
        self.outstanding.push(Outstanding {
            range,
            round,
            future,
        });
    }

    /// How many persists are outstanding.
    #[must_use]
    pub fn len(&self) -> usize {
        self.outstanding.len()
    }

    /// Whether none is.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outstanding.is_empty()
    }

    /// Resolves with the next persist to resolve; pending for ever while none is
    /// outstanding, so a task can race it against its inbox and its ticker.
    pub fn next<'a, E: Environment>(&'a mut self, env: &'a E) -> Next<'a, E> {
        Next { set: self, env }
    }
}

/// See [`Persists::next`].
#[must_use = "futures do nothing unless polled"]
pub struct Next<'a, E: Environment> {
    set: &'a mut Persists,
    env: &'a E,
}

impl<E: Environment> Future for Next<'_, E> {
    type Output = Resolved;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let len = this.set.outstanding.len();
        if len == 0 {
            return Poll::Pending;
        }
        // One draw only where there is a choice to make, so a node with one range
        // draws nothing and its schedule is the one-group server's.
        let offset = if len > 1 {
            usize::try_from(this.env.sched_rng().next_u64() % len as u64).unwrap_or(0)
        } else {
            0
        };
        let mut ready = None;
        for step in 0..len {
            let index = (offset + step) % len;
            let outstanding = &mut this.set.outstanding[index];
            if let Poll::Ready(result) = outstanding.future.as_mut().poll(cx) {
                ready = Some((index, result));
                break;
            }
        }
        let Some((index, result)) = ready else {
            return Poll::Pending;
        };
        let outstanding = this.set.outstanding.remove(index);
        Poll::Ready(Resolved {
            range: outstanding.range,
            round: outstanding.round,
            result,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use ananke_env::sim::{Sim, SimConfig, SimEnv};
    use ananke_raft::message::Message;
    use ananke_raft::types::{Configuration, Entry, Payload};
    use ananke_raft::{Raft, RaftConfig};

    use super::*;
    use crate::frame::decode;

    const R1: RangeId = RangeId(1);
    const R2: RangeId = RangeId(2);
    const ME: ServerId = ServerId(1);
    const LEADER: ServerId = ServerId(2);
    const TICK: Duration = Duration::from_millis(10);

    /// What the node drove the host to do, in the order it did it. The log is the
    /// whole oracle: Q41's round is an *order*, so a check of it is a check of this
    /// list.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Note {
        /// A persist was polled for the first time: when it reached the WAL writer,
        /// and so which persists share a group.
        Submitted(RangeId),
        /// A persist resolved.
        Persisted(RangeId),
        /// A frame left for a peer, carrying these ranges' messages.
        Shipped(ServerId, Vec<RangeId>),
        /// A job for the `apply` task.
        Applied(RangeId),
    }

    /// A host whose persists take a time chosen per range, and which writes down
    /// everything the node asks of it.
    struct Probe {
        env: SimEnv,
        log: Arc<Mutex<Vec<Note>>>,
        delays: BTreeMap<RangeId, Duration>,
    }

    impl Probe {
        fn new(env: &SimEnv, delays: &[(RangeId, Duration)]) -> Self {
            Self {
                env: env.clone(),
                log: Arc::new(Mutex::new(Vec::new())),
                delays: delays.iter().copied().collect(),
            }
        }

        fn note(&self, note: Note) {
            self.log.lock().expect("the log").push(note);
        }
    }

    impl Host for Probe {
        fn persist(&self, range: RangeId, _persist: Persist) -> BoxedPersist {
            let env = self.env.clone();
            let log = self.log.clone();
            let delay = self.delays.get(&range).copied().unwrap_or(TICK);
            Box::pin(async move {
                log.lock().expect("the log").push(Note::Submitted(range));
                env.clock().sleep(delay).await;
                log.lock().expect("the log").push(Note::Persisted(range));
                Ok(())
            })
        }

        fn stamp(&self, _range: RangeId, _message: &mut Message) {}

        fn ship(&self, to: ServerId, frame: Bytes) -> Boxed<'_, ()> {
            let ranges = decode(&frame)
                .expect("the node's own frame decodes")
                .messages
                .iter()
                .map(|tagged| tagged.range)
                .collect();
            self.note(Note::Shipped(to, ranges));
            Box::pin(async {})
        }

        fn read_ready(
            &self,
            _range: RangeId,
            _id: u64,
            _index: Index,
            _lease: bool,
        ) -> Boxed<'_, io::Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn read_dropped(&self, _range: RangeId, _id: u64) -> Boxed<'_, ()> {
            Box::pin(async {})
        }

        fn rejected(&self, _range: RangeId, _leader: Option<ServerId>) {}

        fn apply(&self, job: ApplyJob) {
            self.note(Note::Applied(job.range));
        }

        fn snapshot(&self, _range: RangeId, _action: SnapshotAction) {}

        fn failed(&self, reason: String) {
            panic!("the node failed: {reason}");
        }
    }

    fn core(range: RangeId) -> Raft {
        let config = RaftConfig {
            range: range.get(),
            ..RaftConfig::default()
        };
        Raft::new(ME, Configuration::of(&[ME, LEADER, ServerId(3)]), config, 7)
    }

    /// An AppendEntries carrying one entry: a follower that takes it appends, so its
    /// step asks for a `Persist` and answers after it (RAFT.md §3).
    fn append(index: Index, term: u64) -> Message {
        Message::AppendEntries {
            term,
            prev_index: index - 1,
            prev_term: if index > 1 { term } else { 0 },
            entries: vec![Entry {
                index,
                term,
                payload: Payload::Command(Bytes::from_static(b"x")),
            }],
            commit: 0,
            sent: 0,
        }
    }

    fn arrival(range: RangeId, message: Message) -> Received {
        Received {
            range,
            from: LEADER,
            message,
            bytes: 64,
        }
    }

    /// What one run of the node produced: its log, and what it measured once its
    /// inbox closed.
    struct Run {
        log: Vec<Note>,
        meters: Meters,
        frames: Frames,
        held_bytes_seen: usize,
    }

    /// Runs a node with two ranges for `duration`, handing it `arrivals` at the
    /// start, then closes its inbox and takes its measurements.
    fn run(
        variants: NodeVariants,
        delays: &[(RangeId, Duration)],
        arrivals: &[Received],
        duration: Duration,
    ) -> Run {
        let mut sim = Sim::new(SimConfig::new(11));
        let node = sim.add_node();
        let env = sim.env(node);
        let probe = Probe::new(&env, delays);
        let log = probe.log.clone();
        let inbox = Inbox::new(4096);
        let mut cores = Cores::new(variants);
        cores.insert(R1, core(R1));
        cores.insert(R2, core(R2));
        let mut node = Node::new(
            env.clone(),
            NodeConfig {
                id: ME,
                tick: TICK,
                variants,
            },
            cores,
            Outbox::new(),
            probe,
        );
        for arrival in arrivals {
            assert!(inbox.admit(arrival.clone()).is_admitted());
        }
        let taken: Arc<Mutex<Option<(Meters, Frames)>>> = Arc::default();
        env.clone().spawn("raft", {
            let inbox = inbox.clone();
            let taken = taken.clone();
            async move {
                let _ = node.raft(&inbox).await;
                *taken.lock().expect("the cell") = Some((node.meters(), node.frames()));
            }
        });
        let mut held_bytes_seen = 0;
        let step = Duration::from_millis(1);
        let mut elapsed = Duration::ZERO;
        while elapsed < duration {
            sim.run_for(step);
            elapsed += step;
            held_bytes_seen = held_bytes_seen.max(inbox.held_bytes());
        }
        inbox.close();
        sim.run_for(TICK);
        let (meters, frames) = taken.lock().expect("the cell").expect("the node stopped");
        Run {
            log: log.lock().expect("the log").clone(),
            meters,
            frames,
            held_bytes_seen,
        }
    }

    fn position(log: &[Note], note: &Note) -> usize {
        log.iter()
            .position(|seen| seen == note)
            .unwrap_or_else(|| panic!("{note:?} is not in {log:?}"))
    }

    /// Q41's round, the whole of it, on two cores that both persist: nothing a core
    /// produced after its `Persist` leaves before that core's own persist resolves,
    /// and a core whose disk is quick does not wait for a core whose disk is slow.
    ///
    /// The pair (CLAUDE.md:52-57): `DeferredFlushedEarly` is the node that treats a
    /// core's later outputs as the round's early ones, and this check catches it —
    /// the frame leaves before either persist resolved.
    #[test]
    fn a_cores_later_outputs_wait_on_its_own_persist_and_on_no_others() {
        let arrivals = [arrival(R1, append(1, 1)), arrival(R2, append(1, 1))];
        let delays = [
            (R1, Duration::from_millis(40)),
            (R2, Duration::from_millis(5)),
        ];
        let correct = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(120),
        );
        // Both cores persisted, so the round's early flush had nothing to send: the
        // first frame of the run is r2's response, after r2's persist resolved.
        let first_ship = correct
            .log
            .iter()
            .position(|note| matches!(note, Note::Shipped(..)))
            .expect("a response leaves");
        assert!(
            first_ship > position(&correct.log, &Note::Persisted(R2)),
            "a send after a core's persist left before it: {:?}",
            correct.log
        );
        assert_eq!(
            correct.log[first_ship],
            Note::Shipped(LEADER, vec![R2]),
            "the quick core's response is the first frame: {:?}",
            correct.log
        );
        // And it left before the slow core's persist resolved: no core waits on
        // another core's disk.
        assert!(
            first_ship < position(&correct.log, &Note::Persisted(R1)),
            "the quick core waited on the slow one: {:?}",
            correct.log
        );
        assert!(
            position(&correct.log, &Note::Shipped(LEADER, vec![R1]))
                > position(&correct.log, &Note::Persisted(R1)),
            "the slow core's response left before its persist: {:?}",
            correct.log
        );

        let buggy = run(
            NodeVariants::correct().with(NodeVariant::DeferredFlushedEarly),
            &delays,
            &arrivals,
            Duration::from_millis(120),
        );
        let first_ship = buggy
            .log
            .iter()
            .position(|note| matches!(note, Note::Shipped(..)))
            .expect("a response leaves");
        assert!(
            first_ship < position(&buggy.log, &Note::Persisted(R2)),
            "the variant is meant to send before the persist resolves: {:?}",
            buggy.log
        );
    }

    /// The round's persists are submitted together, so the WAL writer's group commit
    /// syncs them once: every persist of the round reaches the writer before any of
    /// them resolves.
    ///
    /// The pair: `PersistsOneAtATime` submits the second only once the first has
    /// resolved, and this check catches it.
    #[test]
    fn a_rounds_persists_are_submitted_together() {
        let arrivals = [arrival(R1, append(1, 1)), arrival(R2, append(1, 1))];
        let delays = [
            (R1, Duration::from_millis(20)),
            (R2, Duration::from_millis(20)),
        ];
        let correct = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        let first_resolution = correct
            .log
            .iter()
            .position(|note| matches!(note, Note::Persisted(_)))
            .expect("a persist resolves");
        assert!(
            position(&correct.log, &Note::Submitted(R1)) < first_resolution
                && position(&correct.log, &Note::Submitted(R2)) < first_resolution,
            "the round's persists did not reach the writer as one group: {:?}",
            correct.log
        );

        let buggy = run(
            NodeVariants::correct().with(NodeVariant::PersistsOneAtATime),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        let first_resolution = buggy
            .log
            .iter()
            .position(|note| matches!(note, Note::Persisted(_)))
            .expect("a persist resolves");
        assert!(
            position(&buggy.log, &Note::Submitted(R2)) > first_resolution,
            "the variant is meant to submit the second persist after the first \
             resolved: {:?}",
            buggy.log
        );
    }

    /// A core whose persist is outstanding steps nothing, and every tick it missed is
    /// stepped when the persist resolves — none collapsed (SHARD.md §4).
    ///
    /// The pair: `CollapseHeldTicks` replays one tick for all of them, and
    /// `StepWhilePersisting` steps the core while its persist is outstanding so it
    /// holds nothing at all. The same check catches both.
    #[test]
    fn every_tick_a_core_missed_is_stepped_when_its_persist_resolves() {
        // r1's persist outlasts three of the node's ticks; r2's resolves at once.
        let delays = [
            (R1, Duration::from_millis(35)),
            (R2, Duration::from_millis(1)),
        ];
        let arrivals = [arrival(R1, append(1, 1))];
        let correct = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert_eq!(
            correct.meters.ticks_replayed_most, 3,
            "a 35 ms persist spans three 10 ms ticks, every one of them stepped: \
             {:?}",
            correct.meters
        );
        assert_eq!(correct.meters.cores_held_most, 1);

        let collapsed = run(
            NodeVariants::correct().with(NodeVariant::CollapseHeldTicks),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert_eq!(
            collapsed.meters.ticks_replayed_most, 1,
            "the variant is meant to collapse the missed ticks into one: {:?}",
            collapsed.meters
        );

        let stepped = run(
            NodeVariants::correct().with(NodeVariant::StepWhilePersisting),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert_eq!(
            stepped.meters.ticks_replayed_most, 0,
            "the variant is meant to step the core rather than hold its ticks: {:?}",
            stepped.meters
        );
        assert_eq!(stepped.meters.held_most, 0);
    }

    /// A message for a core whose persist is outstanding is taken from the inbox and
    /// held for that core, *counted against the node's byte bound* (Q14).
    ///
    /// The pair: `HeldNotCounted` takes the message and stops counting it, and this
    /// check catches it — the node's charged bytes fall back to the queue's.
    #[test]
    fn a_held_message_still_counts_against_the_nodes_bound() {
        let delays = [
            (R1, Duration::from_millis(35)),
            (R2, Duration::from_millis(1)),
        ];
        // Two appends for the same range: the first makes it persist, the second
        // arrives at a core that cannot step and is held.
        let arrivals = [arrival(R1, append(1, 1)), arrival(R1, append(2, 1))];
        let correct = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert!(
            correct.meters.held_most >= 1,
            "the second append is held for the persisting core: {:?}",
            correct.meters
        );
        assert_eq!(
            correct.held_bytes_seen, correct.meters.held_bytes_most,
            "what the node holds is what the inbox charges"
        );
        assert!(
            correct.held_bytes_seen >= 64,
            "the held message's bytes were not charged against the bound: {}",
            correct.held_bytes_seen
        );

        let buggy = run(
            NodeVariants::correct().with(NodeVariant::HeldNotCounted),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert!(
            buggy.meters.held_most >= 1,
            "the variant still holds the message: {:?}",
            buggy.meters
        );
        assert_eq!(
            buggy.held_bytes_seen, 0,
            "the variant is meant to stop counting what it holds: {}",
            buggy.held_bytes_seen
        );
    }

    /// The frames a round costs per peer (SHARD.md:490-492, §12): one flush before
    /// the sync, and one as each persisting core's persist resolves.
    #[test]
    fn a_round_with_persists_costs_one_frame_per_peer_per_flush() {
        let arrivals = [arrival(R1, append(1, 1)), arrival(R2, append(1, 1))];
        let delays = [
            (R1, Duration::from_millis(40)),
            (R2, Duration::from_millis(5)),
        ];
        let measured = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(120),
        );
        // The arrivals' round persisted for both ranges and sent nothing early, so it
        // took three flushes — its own and one per resolution — and put one frame on
        // the wire toward the one peer at each resolution.
        assert_eq!(
            measured.frames.most_flushes_in_a_round, 3,
            "one flush before the sync and one per persisting core: {:?}",
            measured.frames
        );
        assert_eq!(
            measured.frames.most_frames_to_a_peer_in_a_round, 2,
            "one frame per peer per flush that has sends for it: {:?}",
            measured.frames
        );
        assert!(measured.frames.rounds_with_persists >= 1);
        assert_eq!(measured.frames.outbox_dropped, 0);
        assert_eq!(measured.frames.outbox_oversized, 0);
    }

    /// The frames a round costs when the group commit resolves its persists
    /// *together*, which is the case §4 leaves to this stage (SHARD.md:490-492): each
    /// core's later outputs are still flushed when that core's own persist resolves,
    /// so a round with `p` persisting cores costs up to `p` later flushes and, per
    /// peer, one frame at each of them that has sends for it — not one frame for the
    /// group.
    #[test]
    fn a_shared_sync_still_costs_one_later_flush_per_persisting_core() {
        let arrivals = [arrival(R1, append(1, 1)), arrival(R2, append(1, 1))];
        // Both persists resolve at the same instant, as one group commit's do.
        let delays = [
            (R1, Duration::from_millis(20)),
            (R2, Duration::from_millis(20)),
        ];
        let measured = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert_eq!(
            measured.frames.most_flushes_in_a_round, 3,
            "one flush before the sync and one per persisting core, even when the \
             sync is one: {:?}",
            measured.frames
        );
        assert_eq!(
            measured.frames.most_frames_to_a_peer_in_a_round, 2,
            "two persisting cores, two later flushes, two frames to the one peer: \
             {:?}",
            measured.frames
        );
    }

    /// The `apply` task takes every range's jobs one at a time, in the order they
    /// were queued: one range's job never runs beside another's, which is Q14's rule
    /// and D-036's consequence that one range's take holds every range's applies.
    #[test]
    fn the_apply_task_runs_every_ranges_jobs_one_at_a_time() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        enum Step {
            Enter(RangeId),
            Leave(RangeId),
        }

        struct Slow {
            env: SimEnv,
            log: Arc<Mutex<Vec<Step>>>,
        }

        impl Applier for Slow {
            fn run(&self, job: ApplyJob) -> Boxed<'_, ()> {
                let env = self.env.clone();
                let log = self.log.clone();
                Box::pin(async move {
                    log.lock().expect("the log").push(Step::Enter(job.range));
                    env.clock().sleep(Duration::from_millis(5)).await;
                    log.lock().expect("the log").push(Step::Leave(job.range));
                })
            }
        }

        let mut sim = Sim::new(SimConfig::new(5));
        let node = sim.add_node();
        let env = sim.env(node);
        let log: Arc<Mutex<Vec<Step>>> = Arc::default();
        let jobs: Queue<ApplyJob> = Queue::new();
        for range in [R1, R2, R1] {
            jobs.push(ApplyJob {
                range,
                work: ApplyWork::Take,
            });
        }
        env.clone().spawn("apply", {
            let jobs = jobs.clone();
            let applier = Slow {
                env: env.clone(),
                log: log.clone(),
            };
            async move { apply(&jobs, &applier).await }
        });
        sim.run_for(Duration::from_millis(40));
        jobs.close();
        sim.run_for(Duration::from_millis(10));
        assert_eq!(
            *log.lock().expect("the log"),
            vec![
                Step::Enter(R1),
                Step::Leave(R1),
                Step::Enter(R2),
                Step::Leave(R2),
                Step::Enter(R1),
                Step::Leave(R1),
            ],
            "the jobs did not run one at a time in the order they were queued"
        );
    }

    /// An `Apply` is one of the outputs that follow a core's `Persist`, so it reaches
    /// the `apply` task only once that core's persist resolved: an `Apply` handed out
    /// early would let the task make an applied index durable above the durable log
    /// (SHARD.md §4).
    #[test]
    fn an_apply_reaches_the_task_only_once_its_cores_persist_resolved() {
        // An AppendEntries that commits what it carries: the follower appends and
        // pushes an `Apply` in the same step (core.rs:2514-2522).
        let mut message = append(1, 1);
        if let Message::AppendEntries { commit, .. } = &mut message {
            *commit = 1;
        }
        let arrivals = [arrival(R1, message)];
        let delays = [(R1, Duration::from_millis(30))];
        let correct = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert!(
            position(&correct.log, &Note::Applied(R1))
                > position(&correct.log, &Note::Persisted(R1)),
            "an apply left before its core's persist resolved: {:?}",
            correct.log
        );

        let buggy = run(
            NodeVariants::correct().with(NodeVariant::DeferredFlushedEarly),
            &delays,
            &arrivals,
            Duration::from_millis(80),
        );
        assert!(
            position(&buggy.log, &Note::Applied(R1)) < position(&buggy.log, &Note::Persisted(R1)),
            "the variant is meant to hand the apply out early: {:?}",
            buggy.log
        );
    }
}
