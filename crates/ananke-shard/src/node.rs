//! The node's tasks (SHARD.md §4, §13 Q41; §11, raft 1, 2, 11).
//!
//! Not raft 10, "a seed per range: a core's generator is seeded from the node's
//! protocol stream at its incarnation's start". [`Cores::insert`] takes a core already
//! constructed and nothing here touches a seed; that is the slice that *builds* a
//! node's cores, not this one, which is the schedule over cores it is handed.
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
use ananke_raft::{Entry, Index, Input, Message, Output, Persist, Raft, ServerId};
use bytes::Bytes;

use crate::inbox::{Inbox, Received};
use crate::outbox::Outbox;
use crate::range::RangeId;
use crate::round::{Act, Cores, Meters, Round};
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
    /// What the node's own tasks and its clients hand the `raft` task besides the
    /// peer messages on its inbox: a client's request (a range is on every client
    /// message, SHARD.md §4), an index the `apply` task made durable. A host with no
    /// such inputs sets it to `()`.
    // PROPOSED(D-076): the node's local inputs are the host's own type, so the
    // client protocol and the applied feedback stay on the server's side of Q40's
    // boundary and the `raft` task keeps one loop.
    type Local: Send + 'static;

    /// Which range a node-local event is for. It is asked before the event is
    /// stepped or held, so a client's request waits for its own range's persist and
    /// for no other's (SHARD.md §4).
    fn local_range(&self, local: &Self::Local) -> RangeId;

    /// The input a node-local event steps into its range's core, with that core as
    /// it stands. `None` when there is nothing to step: a duplicate request whose
    /// entry is still in the log, or an event for a range this node does not hold.
    /// The host keeps whatever it needs to answer the client.
    fn local_input(&self, local: Self::Local, core: &Raft) -> Option<Input>;

    /// The local input was stepped and its round driven, with the core as the step
    /// left it: where the host reads the index and term a proposal took. `decided`
    /// is the stamp the step was taken at (D-047).
    fn local_stepped(&self, range: RangeId, core: &Raft, decided: Decision);

    /// Makes `range`'s state durable. The future does nothing until it is polled; the
    /// round submits all of its persists and then polls every one of them in one pass
    /// with no await between, so the WAL writer takes the round's records as one group
    /// and syncs them once (wal.rs:16-20, D-018).
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

    /// Snapshot work for the node's `snapshot` task: a stream to a follower behind the
    /// compacted prefix. A [`SnapshotAction::Take`] goes to the `apply` task instead
    /// (RAFT.md §1), and the node routes it there rather than here.
    fn snapshot(&self, range: RangeId, action: SnapshotAction);

    /// What a node-local input asks of the range's *core*, beyond being stepped into
    /// it.
    ///
    /// Every local input the node had before the snapshot wiring was a step: a
    /// client's request, an index the `apply` task made durable. A live install is not
    /// (D-066). It replaces one range's replica with the state a manifest switch made
    /// durable, and it has to hold that range from the moment the install is decided
    /// until the switch is done, or a step of the replica being replaced writes into
    /// the store above the state the switch is about to install. A server gets both
    /// for free: it ends its run-loop incarnation and comes back on the switched store
    /// (RAFT.md §1). A node cannot, because that would restart every range on it
    /// (SHARD.md §11, storage 5) — so this is the one range's version of it.
    ///
    /// The default is [`CoreWork::Step`]: a host with no install path answers it for
    /// every input, and nothing about the round changes for it.
    // PROPOSED(D-083): a live install holds one range and replaces its replica.
    fn local_core(&self, local: &Self::Local, core: &Raft) -> CoreWork {
        let _ = (local, core);
        CoreWork::Step
    }

    /// Whether this local input is what *releases* a range's hold — a live install's
    /// switch, or its abandonment — and so must be taken while that range is held
    /// rather than queued behind the hold it ends.
    ///
    /// It is separate from [`local_core`](Self::local_core), and pure, for a reason
    /// that is a bug this had: `local_core` may have a side effect of its own (the
    /// install's repair is built there and handed to the `snapshot` task), and the
    /// node has to know whether an input is held *before* it asks what the input
    /// wants. Asking first and holding afterwards would build a repair, and let the
    /// switch that carries it proceed, for a range whose own persist was still in
    /// flight — which is the one thing the hold exists to prevent.
    // PROPOSED(D-083): a live install holds one range and replaces its replica.
    fn local_releases(&self, local: &Self::Local) -> bool {
        let _ = local;
        false
    }

    /// The node cannot go on: the disk failed under it. The host traces it; the task
    /// returns the error.
    fn failed(&self, reason: String);
}

/// What a node-local input asks of the range's core ([`Host::local_core`]).
///
/// It is deliberately not an `Input`: the two things a live install needs of a core
/// are not steps of it, and expressing them as steps would put the install's
/// bookkeeping inside the Raft core, which knows nothing about a node's engine.
// PROPOSED(D-083): a live install holds one range and replaces its replica.
pub enum CoreWork {
    /// An ordinary local input: step it into the core ([`Host::local_input`]).
    Step,
    /// Hold the range: its messages and its node-local inputs queue where a persisting
    /// core's already queue, and nothing steps it until a [`Restore`](Self::Restore)
    /// arrives. A hold asked for while that range's persist is outstanding waits its
    /// turn behind it, like any other local input, so the install never switches under
    /// a write that is still in flight.
    Hold,
    /// The install switched: this is the replica it built, and it replaces the one that
    /// was there. The hold is released with it and the range's held work is stepped
    /// into the new core, in the order it arrived.
    Restore(Box<Raft>),
    /// The install did not switch — the staged directory was short, or the engine
    /// refused it — so the range goes on with the replica it has. The hold is released
    /// and the held work is stepped into that core.
    ///
    /// It is a separate answer from [`Restore`](Self::Restore) of the core that is
    /// already there, because the two differ in what they leave behind: a restore
    /// replaces the replica and a release does not, and a release that quietly
    /// re-restored would hide an install that failed as one that worked.
    Release,
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
    ///
    /// D-043's *re*take — a fresh checkpoint even at the recorded index, because the
    /// recorded one was found unusable — is the slice that reads checkpoints back, and
    /// is one more kind of this `non_exhaustive` enum when it lands.
    Take,
    /// Write the range's snapshot record at the applied index with no checkpoint under
    /// it: a follower's compaction point (D-065, D-078). It runs in the `apply` task
    /// between two applies for the same reason a take does — so the index, the term
    /// and the configuration the record carries are exactly the state a snapshot at
    /// that index would capture (D-036).
    // PROPOSED(D-083): a follower's compaction record is the `apply` task's job too.
    Record,
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
    /// Rounds that put a frame on the wire *before* their sync and then persisted: the
    /// rounds where the "1" of a round's `1 + p` frames per peer is a frame and not
    /// only arithmetic (SHARD.md:490-492).
    pub rounds_sending_before_their_sync: u64,
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
    next_round: u64,
    open: BTreeMap<u64, Open>,
    frames: Frames,
    /// The node's own inputs held for a core whose persist is outstanding, in the
    /// order they arrived (SHARD.md §4).
    deferred: BTreeMap<RangeId, VecDeque<H::Local>>,
    /// Ranges whose persist resolved *inside* [`Node::drive`] rather than through
    /// [`Event::Resolved`], which is only the `PersistsOneAtATime` variant's doing.
    /// The loop drains their held inputs when `drive` hands back, so that variant
    /// strands nothing and is caught for what it is — a round that pays a sync per
    /// persist — and not for holding a client's request until some later resolution.
    // PROPOSED(D-076): a node-local input is held for a persisting core as a message
    // of its range is.
    resolved_inside: VecDeque<RangeId>,
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
            next_round: 0,
            open: BTreeMap::new(),
            frames: Frames::default(),
            deferred: BTreeMap::new(),
            resolved_inside: VecDeque::new(),
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

    /// Where the highest applied index handed out per range stands. It is the cores'
    /// bookkeeping, because the entries an `Apply` names are read at the step that
    /// named them, where the `after` has to be known ([`Cores::applied_sent`]).
    #[must_use]
    pub fn applied_sent(&self, range: RangeId) -> Index {
        self.cores.applied_sent(range)
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
    pub async fn raft(&mut self, inbox: &Inbox, local: &Queue<H::Local>) -> io::Result<()> {
        let mut persists = Persists::default();
        let mut next_tick = self.env.clock().now() + self.config.tick;
        loop {
            let event = {
                let pop = pin!(inbox.pop());
                let timer = pin!(self.env.clock().sleep_until(next_tick));
                let arrived = pin!(race(&self.env, pop, timer));
                let resolved = pin!(persists.next(&self.env));
                let peers = pin!(race(&self.env, arrived, resolved));
                let mine = pin!(local.pop());
                match race(&self.env, peers, mine).await {
                    Either::Left(Either::Left(Either::Left(Some(first)))) => Event::Messages(first),
                    Either::Left(Either::Left(Either::Left(None))) => return Ok(()),
                    Either::Left(Either::Left(Either::Right(()))) => {
                        next_tick += self.config.tick;
                        Event::Tick
                    }
                    Either::Left(Either::Right(resolved)) => Event::Resolved(resolved),
                    Either::Right(Some(mine)) => Event::Local(mine),
                    // The node's own tasks are done with it: nothing local can
                    // arrive again, and the loop goes on serving its peers.
                    Either::Right(None) => return Ok(()),
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
                    // The node's own inputs for that core were held beside its
                    // messages, and are stepped in the order they arrived.
                    self.replay_held(range, &mut persists).await?;
                    self.drain_resolved_inside(&mut persists).await?;
                    inbox.hold_at(self.cores.held_bytes());
                    continue;
                }
                Event::Local(mine) => {
                    self.local(mine, &mut persists).await?;
                    self.drain_resolved_inside(&mut persists).await?;
                    inbox.hold_at(self.cores.held_bytes());
                    continue;
                }
            };
            self.drive(round, None, &mut persists).await?;
            self.drain_resolved_inside(&mut persists).await?;
            // A message held for a core whose persist is outstanding was taken from
            // the inbox and is still counted against the node's byte bound (Q14).
            inbox.hold_at(self.cores.held_bytes());
        }
    }

    /// The inputs held for `range` while its persist was outstanding, stepped in the
    /// order they arrived, for as long as the core is not persisting again.
    async fn replay_held(&mut self, range: RangeId, persists: &mut Persists) -> io::Result<()> {
        while !self.cores.held(range) {
            let Some(mine) = self.deferred.get_mut(&range).and_then(VecDeque::pop_front) else {
                break;
            };
            self.local(mine, persists).await?;
        }
        Ok(())
    }

    /// The held inputs of every range whose persist resolved inside [`Node::drive`],
    /// which is the `PersistsOneAtATime` variant's doing alone: on the correct node
    /// this list is always empty, and this is a no-op. It is here so that the variant
    /// is caught for the sync it pays per persist and not for stranding a client's
    /// request until some later resolution of its range.
    async fn drain_resolved_inside(&mut self, persists: &mut Persists) -> io::Result<()> {
        while let Some(range) = self.resolved_inside.pop_front() {
            self.replay_held(range, persists).await?;
        }
        Ok(())
    }

    /// One node-local input: a client's request, or an index the `apply` task made
    /// durable.
    ///
    /// A core whose persist is outstanding holds its own node's inputs exactly as it
    /// holds the messages of its range (SHARD.md §4): they are stepped, in the order
    /// they arrived, once that core's persist resolves. A client of one range
    /// therefore waits behind that range's disk and behind no other range's, which
    /// is the whole point of Q41's round.
    // PROPOSED(D-076): a node-local input is held for a persisting core as a message
    // of its range is.
    async fn local(&mut self, mine: H::Local, persists: &mut Persists) -> io::Result<()> {
        let range = self.host.local_range(&mine);
        // A live install's two asks of the core are not steps of it (D-066). The
        // replacement is taken *before* the hold is consulted, because it is what
        // releases the hold; the hold is taken after it, so an install decided while
        // that range's persist is outstanding waits behind that persist like any other
        // local input, and never switches under a write still in flight.
        // PROPOSED(D-083): a live install holds one range and replaces its replica.
        // A held range holds its own inputs as it holds its messages (SHARD.md §4).
        // `StepWhileInstalling` is honoured here as `Cores::drive` honours it for a
        // message, because the step that matters is any step that can persist, and a
        // client's proposal persists exactly as an append does.
        //
        // This is asked *before* the host is asked what the input wants, because that
        // question has a side effect — a live install's repair is built there — and an
        // install whose repair was built and handed on while its own range's persist
        // was still in flight would switch the store under that write. What ends a
        // hold is the one thing that must not be queued behind it.
        // PROPOSED(D-083): a live install holds one range and replaces its replica.
        let held = self.cores.persisting(range)
            || (self.cores.installing(range)
                && !self
                    .config
                    .variants
                    .contains(NodeVariant::StepWhileInstalling));
        if self
            .config
            .variants
            .contains(NodeVariant::AsksTheHostBeforeTheHold)
        {
            // The variant: the host is asked what the input wants before the node has
            // checked whether the range is held. `local_core` builds a live install's
            // repair as a side effect, so a `Ready` that arrives while its own range's
            // persist is outstanding hands that repair on and lets the switch carrying
            // it proceed against a write still in flight.
            if let Some(core) = self.cores.core(range) {
                let _ = self.host.local_core(&mine, core);
            }
        }
        if held && !self.host.local_releases(&mine) {
            self.cores.count_local_held();
            if self.config.variants.contains(NodeVariant::HeldLocalDropped) {
                // The variant: the input is thrown away instead of held. A client
                // retries and an `Applied` is superseded by the next one, so nothing
                // downstream of a sweep can tell it from the correct node — which is
                // why it is caught by a check of its own here (`a_local_input_for_a
                // _persisting_core_is_held_and_stepped_in_order`).
                return Ok(());
            }
            self.deferred.entry(range).or_default().push_back(mine);
            return Ok(());
        }
        let work = match self.cores.core(range) {
            Some(core) => self.host.local_core(&mine, core),
            // A local input for a range this node does not hold is counted below,
            // where every other one of them is.
            None => CoreWork::Step,
        };
        match work {
            CoreWork::Restore(core) => return self.restore(range, Some(*core), persists).await,
            CoreWork::Release => return self.restore(range, None, persists).await,
            CoreWork::Hold => {
                self.hold_for_install(range);
                return Ok(());
            }
            CoreWork::Step => {}
        }
        let Some(core) = self.cores.core(range) else {
            // A local input for a range this node does not hold: counted, never
            // silent, as a message for one is.
            self.cores.count_input_for_a_range_not_held();
            return Ok(());
        };
        let Some(input) = self.host.local_input(mine, core) else {
            return Ok(());
        };
        // D-047: the step's decision time. The stamp reads the time and nothing
        // else, so taking it here rather than inside the step moves no schedule.
        let decided = self.env.decision();
        let round = self.cores.messages(&self.env, [(range, input, None, 0)]);
        self.drive(round, None, persists).await?;
        if let Some(core) = self.cores.core(range) {
            self.host.local_stepped(range, core, decided);
        }
        Ok(())
    }

    /// Holds `range` across its live install, from the install's decision to its
    /// manifest switch ([`CoreWork::Hold`]).
    // PROPOSED(D-083): the hold is one range's.
    fn hold_for_install(&mut self, range: RangeId) {
        if self
            .config
            .variants
            .contains(NodeVariant::InstallHoldsEveryRange)
        {
            // The variant: the node reaches for the incarnation a server ends and
            // stops every range it hosts, so one range's install costs every other
            // range on the node the length of a stream's switch — which is the cost
            // the live install exists to avoid (SHARD.md §11, storage 5).
            for other in self.cores.ranges().collect::<Vec<_>>() {
                self.cores.hold_for_install(other);
            }
            return;
        }
        self.cores.hold_for_install(range);
    }

    /// `range`'s live install switched: `core` is the replica it built. It takes the
    /// place of the one it replaced, the hold goes, and the work held while the install
    /// ran is stepped into it ([`CoreWork::Restore`]).
    // PROPOSED(D-083): the replica a switch builds replaces the one it replaced.
    async fn restore(
        &mut self,
        range: RangeId,
        core: Option<Raft>,
        persists: &mut Persists,
    ) -> io::Result<()> {
        let round = match core {
            Some(core)
                if !self
                    .config
                    .variants
                    .contains(NodeVariant::InstallKeepsTheOldCore) =>
            {
                self.cores.installed(&self.env, range, core)
            }
            // The variant: the switch is made and the replica is not replaced, so the
            // store holds the snapshot and the core goes on from the log it had — the
            // half of an install a server gets for free by reopening its store (D-066).
            // And the ordinary failure path, which replaces nothing on purpose.
            _ => self.cores.release_install(&self.env, range),
        };
        self.drive(round, None, persists).await?;
        // The range's *own* inputs were held beside its messages and are stepped in
        // the order they arrived, exactly as a resolution replays them. Boxed because
        // the replay steps local inputs and a local input is what called this: a
        // *held* one is never a restore — a restore is taken before the hold is
        // consulted — but the type system cannot see that.
        Box::pin(self.replay_held(range, persists)).await?;
        if self
            .config
            .variants
            .contains(NodeVariant::InstallHoldsEveryRange)
        {
            // The variant's hold is the node's, so its release is too: every other
            // range is let go here, which is what keeps the variant a cost and not a
            // wedge, and is what makes the figure it is caught by a figure.
            for other in self.cores.ranges().collect::<Vec<_>>() {
                if other == range {
                    continue;
                }
                let round = self.cores.release_install(&self.env, other);
                self.drive(round, None, persists).await?;
                Box::pin(self.replay_held(other, persists)).await?;
            }
        }
        Ok(())
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
            let sends_early = round.sends_early();
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
            if sends_early && credit.is_none() {
                self.frames.rounds_sending_before_their_sync += 1;
            }
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
            // Submitted together, then armed in one pass with no await between them,
            // so the WAL writer takes the round's records as one group and syncs them
            // once — and never splits a round across two groups.
            for submission in round.persists {
                let range = submission.range;
                let future = self.host.persist(range, submission.persist);
                persists.submit(range, id, future);
            }
            if !self.config.variants.contains(NodeVariant::PersistsNotArmed) {
                persists.arm().await;
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
            entries,
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
                // The entries were read from the core at the step that asked for the
                // apply, not here: by now the replay may have stepped that core past a
                // compaction, and `entry(..)` would answer for a log the `Apply` was
                // never about (SHARD.md §4).
                let entries = match entries {
                    Some(Ok(entries)) => entries,
                    // A gap in the applied stream: the core did not hold an index its
                    // own `Apply` named. Passing over it would hand the state machine a
                    // job with a hole in it and advance the node's applied index past
                    // the hole; the node fails instead.
                    Some(Err(index)) => {
                        let reason = format!(
                            "range {range:?}: an apply through {through} names index                              {index}, which the core does not hold"
                        );
                        self.host.failed(reason.clone());
                        return Err(io::Error::other(reason));
                    }
                    None => {
                        debug_assert!(false, "an apply carries the entries it names");
                        return Ok(());
                    }
                };
                if entries.is_empty() {
                    return Ok(());
                }
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
            // A take is the `apply` task's, between two applies (RAFT.md §1): that is
            // what makes D-036's consequence hold, that one range's take stalls every
            // range's applies on the node.
            Output::Snapshot(SnapshotAction::Take) => {
                if self
                    .config
                    .variants
                    .contains(NodeVariant::TakeToSnapshotTask)
                {
                    // The variant: the take goes to the `snapshot` task, so it no
                    // longer runs between two applies.
                    self.host.snapshot(range, SnapshotAction::Take);
                } else {
                    self.host.apply(ApplyJob {
                        range,
                        work: ApplyWork::Take,
                    });
                }
            }
            // A follower's compaction record is the `apply` task's too, for the same
            // reason and by the same route: it is written between two applies so the
            // index and term it records are exact (D-065, D-078, D-036).
            //
            // **It went to the `snapshot` task until PROPOSED D-086, which dropped
            // it**: `install::job_of` answers `None` for `Record` as it does for
            // `Take`, and only `Take` had an arm here, so a follower's compaction was
            // asked for and nothing happened. The cost is not a missing compaction. The
            // core sets `take_pending` when it asks (`core.rs`), and only an answer —
            // `SnapshotTaken` or `SnapshotFailed` — clears it, so a replica that asked
            // once **never asked for another snapshot for the rest of its life**. When
            // that replica later leads and a follower falls behind its log, `replicate`
            // finds `taken` empty and `take_pending` set, asks for nothing, and sends
            // an empty AppendEntries at its own last index forever while the follower
            // rejects with a hint the leader is not in a branch to read. The range
            // commits nothing again: the liveness bound, on the correct node, on seeds
            // 17, 20 and 64 of the first hundred.
            //
            // Nothing before this slice could see it. `sim/install.rs` has no follower
            // that compacts and later leads, and every other node scenario holds
            // `snapshot_threshold` above what its clients write, so `Record` was never
            // asked for at all.
            // PROPOSED(D-086): the follower's compaction record reaches the `apply`
            // task, and the core is answered.
            Output::Snapshot(SnapshotAction::Record) => {
                self.host.apply(ApplyJob {
                    range,
                    work: ApplyWork::Record,
                });
            }
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
enum Event<L> {
    /// The ticker.
    Tick,
    /// A message, and whatever is queued behind it.
    Messages(Received),
    /// A persist resolved.
    Resolved(Resolved),
    /// One of the node's own inputs: a client's request, an applied index.
    Local(L),
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
/// The futures are held side by side and every one of them is polled in one
/// synchronous pass, with no await between, by [`arm`](Persists::arm) as the round
/// submits them and by [`next`](Persists::next) on every poll. So a round's records
/// reach the WAL writer together and are never split across two of its groups, and
/// the writer syncs them once (wal.rs:16-20, D-018). A round submitted while an
/// earlier sync is outstanding joins the group that sync's records did not take,
/// which is what §4's loaded case describes (SHARD.md:519-533).
///
/// Which of several *ready* persists is reported first is drawn from the environment's
/// scheduling stream, as [`race`] draws it: a fixed order would let one range's
/// persists always be seen before another's, and the round's whole point is that no
/// core waits on another's disk.
// PROPOSED(D-073): the order several ready persists resolve in is the scheduling
// stream's, not the range order.
#[derive(Default)]
pub struct Persists {
    outstanding: Vec<Outstanding>,
    /// Persists that resolved during a poll and have not been reported yet.
    ready: VecDeque<Resolved>,
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
    /// Adds a persist to the outstanding set. It is not polled here: the round submits
    /// all of its persists and then arms them in one pass, which is what puts its
    /// records in one group.
    pub fn submit(&mut self, range: RangeId, round: u64, future: BoxedPersist) {
        self.outstanding.push(Outstanding {
            range,
            round,
            future,
        });
    }

    /// Polls every outstanding persist once, with the task's own waker, and resolves
    /// at once: what the round awaits after submitting, so its records are enqueued
    /// with the WAL writer before the task awaits anything that could let the writer
    /// close a group without them.
    pub fn arm(&mut self) -> Arm<'_> {
        Arm(self)
    }

    /// How many persists are outstanding, the ones that resolved and are waiting to be
    /// reported included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.outstanding.len() + self.ready.len()
    }

    /// Whether none is.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resolves with the next persist to resolve; pending for ever while none is
    /// outstanding, so a task can race it against its inbox and its ticker.
    pub fn next<'a, E: Environment>(&'a mut self, env: &'a E) -> Next<'a, E> {
        Next { set: self, env }
    }

    /// Polls every outstanding persist once, in one pass with no await between, and
    /// moves the ones that resolved to `ready`.
    fn poll_all(&mut self, cx: &mut Context<'_>) {
        let mut index = 0;
        while index < self.outstanding.len() {
            if let Poll::Ready(result) = self.outstanding[index].future.as_mut().poll(cx) {
                let outstanding = self.outstanding.remove(index);
                self.ready.push_back(Resolved {
                    range: outstanding.range,
                    round: outstanding.round,
                    result,
                });
                continue;
            }
            index += 1;
        }
    }

    /// One of the persists that have resolved, drawn from the scheduling stream where
    /// there is more than one to choose from.
    fn take_ready<E: Environment>(&mut self, env: &E) -> Option<Resolved> {
        let len = self.ready.len();
        if len == 0 {
            return None;
        }
        // One draw only where there is a choice to make, so a node with one range
        // draws nothing and its schedule is the one-group server's.
        let index = if len > 1 {
            usize::try_from(env.sched_rng().next_u64() % len as u64).unwrap_or(0)
        } else {
            0
        };
        self.ready.remove(index)
    }
}

/// See [`Persists::arm`].
#[must_use = "futures do nothing unless polled"]
pub struct Arm<'a>(&'a mut Persists);

impl Future for Arm<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_all(cx);
        Poll::Ready(())
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
        this.set.poll_all(cx);
        match this.set.take_ready(this.env) {
            Some(resolved) => Poll::Ready(resolved),
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use ananke_env::sim::{Sim, SimConfig, SimEnv};
    use ananke_raft::message::Message;
    use ananke_raft::types::{Configuration, Entry, Payload, Term};
    use ananke_raft::{Raft, RaftConfig};

    use super::*;
    use crate::frame::decode;
    use crate::round::Stamps;

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
        /// A job for the `apply` task, and **what the job carries**: the applied
        /// stream is only checkable if the check can see the indices, which is what
        /// tells re-applying the whole log from a job that partitions it.
        Applied(RangeId, Job),
        /// Work for the `snapshot` task.
        Snapshotted(RangeId, &'static str),
        /// A read the core confirmed.
        ReadReady(RangeId, u64),
        /// A read this server will not serve.
        ReadDropped(RangeId, u64),
        /// A proposal or a read refused: this server does not lead the range.
        Rejected(RangeId),
        /// A node-local input reached its core, and which one it was.
        Local(RangeId, u64),
        /// The host was asked what a local input wants of its core. It is a note of
        /// its own because the question is not pure — a live install's repair is built
        /// in answering it — so *when* it is asked is a property worth checking.
        Asked(RangeId),
        /// The host asked for a range to be held across its live install.
        Held(RangeId),
        /// The host handed back the replica a switch built.
        Restored(RangeId),
    }

    /// What one node-local input asks of its range's core, so a check can drive the
    /// two halves of a live install through the node.
    // PROPOSED(D-083): a live install holds one range and replaces its replica.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Work {
        Step,
        Hold,
        Restore,
    }

    /// The term the replica a switch builds carries, so a check can tell it from the
    /// core it replaced without reaching inside either.
    const INSTALLED_TERM: Term = 9;

    /// One node-local input the probe is fed: its range, and its place in the order
    /// it was pushed in.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Mine {
        range: RangeId,
        n: u64,
        work: Work,
    }

    /// What one [`Note::Applied`] carries.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Job {
        /// The indices of the entries, in the order the job carries them.
        Entries(Vec<Index>),
        /// A checkpoint, between two applies.
        Take,
        /// A follower's compaction record, between two applies: no checkpoint under
        /// it (D-065, D-078).
        Record,
    }

    impl Job {
        fn of(work: &ApplyWork) -> Self {
            match work {
                ApplyWork::Entries(entries) => {
                    Job::Entries(entries.iter().map(|entry| entry.index).collect())
                }
                ApplyWork::Take => Job::Take,
                ApplyWork::Record => Job::Record,
            }
        }
    }

    /// What a message is, for a check that asks what left the node rather than only
    /// how many frames did.
    fn kind(message: &Message) -> &'static str {
        match message {
            Message::PreVote { .. } => "pre-vote",
            Message::PreVoteResponse { .. } => "pre-vote-response",
            Message::RequestVote { .. } => "request-vote",
            Message::RequestVoteResponse { .. } => "request-vote-response",
            Message::AppendEntries { entries, .. } if entries.is_empty() => "heartbeat",
            Message::AppendEntries { .. } => "append",
            Message::AppendEntriesResponse { .. } => "append-response",
            Message::TimeoutNow { .. } => "timeout-now",
            _ => "other",
        }
    }

    /// A host whose persists take a time chosen per range, and which writes down
    /// everything the node asks of it.
    struct Probe {
        env: SimEnv,
        log: Arc<Mutex<Vec<Note>>>,
        /// Every message that left the node, by range and kind, in order.
        sent: Arc<Mutex<Vec<(RangeId, &'static str)>>>,
        /// For every append-response that left the node, whether the frame it left in
        /// carried this host's stamp: the check reads the *shipped bytes*, so a send
        /// the node never stamped is distinguishable from one it did.
        stamped: Arc<Mutex<Vec<(RangeId, bool)>>>,
        /// Every reason the node gave for failing. Recorded rather than panicked, so
        /// that a check can assert the node fails and says why; [`run_with`] asserts
        /// this is empty, so a run that was not meant to fail is still loud.
        failures: Arc<Mutex<Vec<String>>>,
        delays: BTreeMap<RangeId, Duration>,
        /// How long the socket takes a frame: zero unless a check needs the node's
        /// loop to come back round with something else already due.
        ship: Duration,
    }

    /// What [`Probe::stamp`] writes into a response's `local`: a value no core
    /// produces, so a frame carrying it was stamped by the host on its way out and a
    /// frame without it was not (RAFT.md §1, D-042).
    const STAMPED_LOCAL: u64 = 0x5854_414D_5053;

    /// What [`Probe::stamp`] writes into a response's `incarnation`. The store's, which
    /// the core cannot know: a leader that sees it change forgets what it knew of this
    /// follower's log, so a response that left unstamped carries a 0 that means "a
    /// refused server with no store" (D-042).
    const STAMPED_INCARNATION: u64 = 9;

    impl Probe {
        fn new(env: &SimEnv, delays: &[(RangeId, Duration)]) -> Self {
            Self {
                env: env.clone(),
                log: Arc::new(Mutex::new(Vec::new())),
                sent: Arc::default(),
                stamped: Arc::default(),
                failures: Arc::default(),
                delays: delays.iter().copied().collect(),
                ship: Duration::ZERO,
            }
        }

        fn note(&self, note: Note) {
            self.log.lock().expect("the log").push(note);
        }
    }

    impl Host for Probe {
        // A node-local input, as the server's are: a range and a number of its own,
        // so a check can feed the node inputs and read back which were stepped and
        // in what order (`Note::Local`).
        type Local = Mine;

        fn local_range(&self, mine: &Self::Local) -> RangeId {
            mine.range
        }

        fn local_releases(&self, mine: &Self::Local) -> bool {
            matches!(mine.work, Work::Restore)
        }

        fn local_core(&self, mine: &Self::Local, _core: &Raft) -> CoreWork {
            self.note(Note::Asked(mine.range));
            match mine.work {
                Work::Step => CoreWork::Step,
                Work::Hold => {
                    self.note(Note::Held(mine.range));
                    CoreWork::Hold
                }
                Work::Restore => {
                    self.note(Note::Restored(mine.range));
                    // The check drives this while the range is held; `local_releases`
                    // above is what lets it through.
                    // The replica a switch built: a core of its own, at a term no
                    // core this node started with holds, so a check can say which of
                    // the two the node is running afterwards.
                    CoreWork::Restore(Box::new(Raft::restore(
                        ME,
                        Configuration::of(&[ME, LEADER, ServerId(3)]),
                        RaftConfig {
                            range: mine.range.get(),
                            ..RaftConfig::default()
                        },
                        7,
                        INSTALLED_TERM,
                        None,
                        Vec::new(),
                    )))
                }
            }
        }

        fn local_input(&self, mine: Self::Local, _core: &Raft) -> Option<Input> {
            self.note(Note::Local(mine.range, mine.n));
            // Stepped as a client's proposal is, so that a held input goes through
            // the same round as the message beside it. A follower refuses it and the
            // note above is what the check reads.
            Some(Input::Propose(Bytes::from_static(b"mine")))
        }

        fn local_stepped(&self, _range: RangeId, _core: &Raft, _decided: Decision) {}

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

        fn stamp(&self, _range: RangeId, message: &mut Message) {
            // What a real host puts here is the follower's clock for the leader's
            // drift guard and its store incarnation (RAFT.md §1, D-042); what this one
            // puts here is a pair of values no core produces, so that a check can see
            // on the wire whether the node stamped at all.
            if let Message::AppendEntriesResponse {
                local, incarnation, ..
            } = message
            {
                *local = STAMPED_LOCAL;
                *incarnation = STAMPED_INCARNATION;
            }
        }

        fn ship(&self, to: ServerId, frame: Bytes) -> Boxed<'_, ()> {
            let decoded = decode(&frame).expect("the node's own frame decodes");
            let ranges = decoded.messages.iter().map(|tagged| tagged.range).collect();
            self.stamped
                .lock()
                .expect("the stamps")
                .extend(
                    decoded
                        .messages
                        .iter()
                        .filter_map(|tagged| match &tagged.frame.message {
                            Message::AppendEntriesResponse {
                                local, incarnation, ..
                            } => Some((
                                tagged.range,
                                *local == STAMPED_LOCAL && *incarnation == STAMPED_INCARNATION,
                            )),
                            _ => None,
                        }),
                );
            self.sent.lock().expect("the sent log").extend(
                decoded
                    .messages
                    .iter()
                    .map(|tagged| (tagged.range, kind(&tagged.frame.message))),
            );
            self.note(Note::Shipped(to, ranges));
            let env = self.env.clone();
            let ship = self.ship;
            Box::pin(async move {
                if !ship.is_zero() {
                    env.clock().sleep(ship).await;
                }
            })
        }

        fn read_ready(
            &self,
            range: RangeId,
            id: u64,
            _index: Index,
            _lease: bool,
        ) -> Boxed<'_, io::Result<()>> {
            self.note(Note::ReadReady(range, id));
            Box::pin(async { Ok(()) })
        }

        fn read_dropped(&self, range: RangeId, id: u64) -> Boxed<'_, ()> {
            self.note(Note::ReadDropped(range, id));
            Box::pin(async {})
        }

        fn rejected(&self, range: RangeId, _leader: Option<ServerId>) {
            self.note(Note::Rejected(range));
        }

        fn apply(&self, job: ApplyJob) {
            self.note(Note::Applied(job.range, Job::of(&job.work)));
        }

        fn snapshot(&self, range: RangeId, action: SnapshotAction) {
            let what = match action {
                SnapshotAction::Take => "take",
                _ => "stream",
            };
            self.note(Note::Snapshotted(range, what));
        }

        fn failed(&self, reason: String) {
            self.failures.lock().expect("the failures").push(reason);
        }
    }

    fn core(range: RangeId) -> Raft {
        core_with(range, RaftConfig::default())
    }

    fn core_with(range: RangeId, config: RaftConfig) -> Raft {
        let config = RaftConfig {
            range: range.get(),
            ..config
        };
        Raft::new(ME, Configuration::of(&[ME, LEADER, ServerId(3)]), config, 7)
    }

    /// One range on the node: what a mutation about *which* ranges are touched has to
    /// be run against before it may claim a single-range world could not catch it.
    fn one_range() -> Vec<(RangeId, Raft)> {
        vec![(R1, core(R1))]
    }

    /// The node's usual two followers.
    fn two_followers() -> Vec<(RangeId, Raft)> {
        vec![(R1, core(R1)), (R2, core(R2))]
    }

    /// r2 campaigns on the first tick it takes: a node whose loop comes back round
    /// with a tick already due then has something to *do* with it that the host can
    /// see.
    fn r2_campaigns_at_once() -> Vec<(RangeId, Raft)> {
        let quick = RaftConfig {
            election_ticks: (1, 2),
            ..RaftConfig::default()
        };
        vec![(R1, core(R1)), (R2, core_with(R2, quick))]
    }

    /// r1 with an election timeout of four ticks, so that a replay of the ticks it
    /// missed behind a slow sync is *seen* in its timer and not only counted.
    fn quick_to_campaign() -> Vec<(RangeId, Raft)> {
        let config = RaftConfig {
            election_ticks: (4, 5),
            ..RaftConfig::default()
        };
        vec![(R1, core_with(R1, config)), (R2, core(R2))]
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
        /// Each range's term when the run ended: what says whether the replica a
        /// switch built is the one the node is running.
        terms: BTreeMap<RangeId, Term>,
        /// Every message that left, by range and kind, in order.
        sent: Vec<(RangeId, &'static str)>,
        /// For every append-response that left, whether its frame carried the host's
        /// stamp.
        stamped: Vec<(RangeId, bool)>,
        /// The simulation's trace: what the node recorded, when the step that produced
        /// it decided, and when the record was made (D-026, D-047).
        trace: Vec<ananke_env::sim::TraceRecord>,
        meters: Meters,
        frames: Frames,
        held_bytes_seen: usize,
        /// The most the inbox ever charged against the bound: the queue and what the
        /// node holds together.
        charged_most: usize,
        /// Whether each of the setup's fed arrivals was admitted, in order.
        fed: Vec<bool>,
    }

    /// What the `raft` task leaves behind when it stops: what it measured, and each
    /// range's term, which says whether the replica a switch built is the one the node
    /// is running.
    type Taken = (Meters, Frames, BTreeMap<RangeId, Term>);

    /// How a run is set up. [`run`] is the usual one: two followers, a bound no check
    /// reaches, and every arrival admitted before the node starts.
    struct Setup<'a> {
        variants: NodeVariants,
        /// The node's byte bound.
        bound: usize,
        /// The cores the node starts with.
        cores: fn() -> Vec<(RangeId, Raft)>,
        /// How long each range's persists take.
        delays: &'a [(RangeId, Duration)],
        /// Admitted before the node starts.
        arrivals: &'a [Received],
        /// Admitted while the node runs, at these offsets from the start: what it
        /// takes to reach the bound, which only a node that is already holding
        /// something can be at.
        feed: &'a [(Duration, Received)],
        /// The node's own inputs, pushed on its local queue at these offsets from
        /// the start: what it takes to reach the holding path, which only a core
        /// whose persist is outstanding can be on.
        locals: &'a [(Duration, Mine)],
        duration: Duration,
        /// How long the socket takes a frame.
        ship: Duration,
        /// The simulation's seed, which is also the scheduling stream's.
        seed: u64,
    }

    impl<'a> Setup<'a> {
        fn new(variants: NodeVariants, delays: &'a [(RangeId, Duration)]) -> Self {
            Self {
                variants,
                bound: 4096,
                cores: two_followers,
                delays,
                arrivals: &[],
                feed: &[],
                locals: &[],
                duration: Duration::from_millis(80),
                ship: Duration::ZERO,
                seed: 11,
            }
        }
    }

    /// Runs a node for `duration`, then closes its inbox and takes its measurements.
    fn run_with(setup: Setup<'_>) -> Run {
        let variants = setup.variants;
        let mut sim = Sim::new(SimConfig::new(setup.seed));
        let id = sim.add_node();
        let env = sim.env(id);
        let mut probe = Probe::new(&env, setup.delays);
        probe.ship = setup.ship;
        let log = probe.log.clone();
        let sent = probe.sent.clone();
        let stamped = probe.stamped.clone();
        let failures = probe.failures.clone();
        let inbox = Inbox::new(setup.bound);
        let mut cores = Cores::new(variants);
        for (range, core) in (setup.cores)() {
            cores.insert(range, core);
        }
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
        for arrival in setup.arrivals {
            assert!(inbox.admit(arrival.clone()).is_admitted());
        }
        let taken: Arc<Mutex<Option<Taken>>> = Arc::default();
        // The node's local queue, held here so a check can push the node its own
        // inputs while it runs. The `raft` task races it against the inbox and the
        // ticker whether or not anything is ever pushed.
        let local: Queue<Mine> = Queue::new();
        env.clone().spawn("raft", {
            let inbox = inbox.clone();
            let taken = taken.clone();
            let local = local.clone();
            async move {
                let _ = node.raft(&inbox, &local).await;
                let terms = node
                    .cores()
                    .ranges()
                    .filter_map(|range| node.cores().core(range).map(|core| (range, core.term())))
                    .collect();
                *taken.lock().expect("the cell") = Some((node.meters(), node.frames(), terms));
            }
        });
        let mut held_bytes_seen = 0;
        let mut charged_most = 0;
        let mut fed = Vec::new();
        let mut next = 0;
        let mut next_local = 0;
        let step = Duration::from_millis(1);
        let mut elapsed = Duration::ZERO;
        while elapsed < setup.duration {
            while next < setup.feed.len() && setup.feed[next].0 <= elapsed {
                fed.push(inbox.admit(setup.feed[next].1.clone()).is_admitted());
                next += 1;
            }
            while next_local < setup.locals.len() && setup.locals[next_local].0 <= elapsed {
                local.push(setup.locals[next_local].1);
                next_local += 1;
            }
            sim.run_for(step);
            elapsed += step;
            held_bytes_seen = held_bytes_seen.max(inbox.held_bytes());
            charged_most = charged_most.max(inbox.charged_bytes());
        }
        inbox.close();
        // Long enough for the task to see the closed inbox through whatever it is in
        // the middle of, however slow the socket this check gave it.
        sim.run_for(TICK * 4 + setup.ship * 60);
        let (meters, frames, terms) = taken
            .lock()
            .expect("the cell")
            .take()
            .expect("the node stopped");
        // A run that was not meant to fail is loud about it: `Probe::failed` records
        // rather than panics, so that a check can drive a failure on purpose.
        let failures = failures.lock().expect("the failures").clone();
        assert!(failures.is_empty(), "the node failed: {failures:?}");
        Run {
            log: log.lock().expect("the log").clone(),
            terms,
            sent: sent.lock().expect("the sent log").clone(),
            stamped: stamped.lock().expect("the stamps").clone(),
            trace: sim.trace(),
            meters,
            frames,
            held_bytes_seen,
            charged_most,
            fed,
        }
    }

    /// The usual run: two followers, arrivals admitted before the node starts.
    fn run(
        variants: NodeVariants,
        delays: &[(RangeId, Duration)],
        arrivals: &[Received],
        duration: Duration,
    ) -> Run {
        run_with(Setup {
            arrivals,
            duration,
            ..Setup::new(variants, delays)
        })
    }

    fn position(log: &[Note], note: &Note) -> usize {
        log.iter()
            .position(|seen| seen == note)
            .unwrap_or_else(|| panic!("{note:?} is not in {log:?}"))
    }

    /// Where the first note the predicate accepts is.
    fn find(log: &[Note], what: &str, pred: impl Fn(&Note) -> bool) -> usize {
        log.iter()
            .position(pred)
            .unwrap_or_else(|| panic!("no {what} in {log:?}"))
    }

    /// A pre-vote, which a core answers **without persisting**: the term is not
    /// started and nothing of it is durable (thesis §9.6). It is how a round gets an
    /// output before its sync.
    fn prevote(range: RangeId) -> Received {
        Received {
            range,
            from: LEADER,
            message: Message::PreVote {
                term: 1,
                last_index: 0,
                last_term: 0,
            },
            bytes: 64,
        }
    }

    /// A node-local input for a core whose persist is outstanding is **held**, and
    /// stepped when that core's persist resolves, in the order it arrived — while a
    /// local input for a core that is not persisting is stepped at once (SHARD.md
    /// §4). It is the rule that makes a client of one range wait behind that range's
    /// disk and behind no other's.
    ///
    /// The pair (CLAUDE.md:52-57): `HeldLocalDropped` is the node that throws a held
    /// input away instead, and this check catches it — its inputs never reach their
    /// core at all. Nothing downstream can: a client retries and an `Applied` is
    /// superseded, so the node scenario's sweep passes that variant at a thousand
    /// seeds, and this is the oracle the rule has.
    ///
    /// The order is asserted as well as the arrival, because a queue replayed
    /// last-in-first-out is the same check's other failure: a client's two requests
    /// would reach its core in the wrong order.
    // PROPOSED(D-076): a node-local input is held for a persisting core as a message
    // of its range is.
    /// The host is asked what a local input wants of its core only **after** the node
    /// has checked whether that range is held.
    ///
    /// `Host::local_core` is not a pure question: a live install's repair is built in
    /// answering it, from the core, and handed to the `snapshot` task there. Asked
    /// first and held afterwards, a stream whose `Ready` arrives while its own range's
    /// persist is still outstanding hands that repair on and lets the switch carrying
    /// it proceed against a write in flight — the one thing the hold exists to
    /// prevent.
    ///
    /// The situation is reached: instrumenting the node's directed scenario found a
    /// stream's `Ready` arriving for a persisting range six times across eight seeds.
    /// The scenario cannot see it, because the repair it builds early is the same
    /// repair and the run goes on looking identical; this can, because it asks when
    /// the question was put.
    // PROPOSED(D-083): a live install holds one range and replaces its replica.
    #[test]
    fn the_host_is_asked_what_an_input_wants_only_after_the_hold_is_checked() {
        // r1's disk is slow. A `Hold` — a stream ready to switch — arrives while r1's
        // persist is outstanding.
        let arrivals = [arrival(R1, append(1, 1))];
        let delays = [(R1, Duration::from_millis(40))];
        let locals = [(
            Duration::from_millis(2),
            Mine {
                range: R1,
                n: 1,
                work: Work::Hold,
            },
        )];
        let setup = |variants| Setup {
            arrivals: &arrivals,
            locals: &locals,
            duration: Duration::from_millis(120),
            ..Setup::new(variants, &delays)
        };

        let correct = run_with(setup(NodeVariants::correct()));
        let persisted = position(&correct.log, &Note::Persisted(R1));
        let asked = position(&correct.log, &Note::Asked(R1));
        assert!(
            asked > persisted,
            "the host was asked what the input wanted while r1's persist was still \
             outstanding, so a live install's repair would have been built and handed \
             on against a write in flight: {:?}",
            correct.log
        );
        // And the hold is still taken, once the persist is out of the way.
        assert_eq!(
            correct.meters.ranges_held_for_install, 1,
            "the held input was not stepped at all after its persist resolved: {:?}",
            correct.log
        );

        // The pair: the question asked before the hold is checked.
        let buggy = run_with(setup(
            NodeVariants::correct().with(NodeVariant::AsksTheHostBeforeTheHold),
        ));
        assert!(
            position(&buggy.log, &Note::Asked(R1)) < position(&buggy.log, &Note::Persisted(R1)),
            "the variant is meant to ask before the hold is checked: {:?}",
            buggy.log
        );
    }

    /// A live install holds **one** range across its switch, and the replica the    /// A live install holds **one** range across its switch, and the replica the
    /// switch built is the one the node runs afterwards (D-066, D-083).
    ///
    /// A server ends its whole run-loop incarnation across an install and comes back
    /// on the switched store (RAFT.md §1). A node cannot: reopening the engine would
    /// restart every range on it (SHARD.md §11, storage 5). So the node holds the one
    /// range, steps every other range's work meanwhile, and replaces that range's core
    /// when the switch is durable.
    ///
    /// Three variants are caught here, and the third is a mutation a node of one range
    /// could not be wrong about at all.
    // PROPOSED(D-083): a live install holds one range and replaces its replica.
    #[test]
    fn a_live_install_holds_its_range_alone_and_replaces_that_ranges_replica() {
        // r1 is held from 1 ms to 40 ms. Work arrives for both ranges inside the hold:
        // r1's must wait for the switch, r2's must not.
        let locals = [
            (
                Duration::from_millis(1),
                Mine {
                    range: R1,
                    n: 0,
                    work: Work::Hold,
                },
            ),
            (
                Duration::from_millis(5),
                Mine {
                    range: R1,
                    n: 1,
                    work: Work::Step,
                },
            ),
            (
                Duration::from_millis(6),
                Mine {
                    range: R2,
                    n: 9,
                    work: Work::Step,
                },
            ),
            (
                Duration::from_millis(40),
                Mine {
                    range: R1,
                    n: 2,
                    work: Work::Restore,
                },
            ),
        ];
        let setup = |variants| Setup {
            locals: &locals,
            duration: Duration::from_millis(90),
            ..Setup::new(variants, &[])
        };

        let correct = run_with(setup(NodeVariants::correct()));
        assert_eq!(
            correct.meters.ranges_held_for_install, 1,
            "one range was held, not {}: {:?}",
            correct.meters.ranges_held_for_install, correct.log
        );
        let held = position(&correct.log, &Note::Held(R1));
        let restored = position(&correct.log, &Note::Restored(R1));
        let r1_work = position(&correct.log, &Note::Local(R1, 1));
        let r2_work = position(&correct.log, &Note::Local(R2, 9));
        assert!(
            r1_work > restored,
            "the held range was stepped inside its own install's window: {:?}",
            correct.log
        );
        assert!(
            r2_work > held && r2_work < restored,
            "another range waited on this range's install, which is the cost a live \
             install exists to avoid: {:?}",
            correct.log
        );
        assert_eq!(
            correct.terms.get(&R1),
            Some(&INSTALLED_TERM),
            "the node is not running the replica the switch built: {:?}",
            correct.terms
        );
        assert_eq!(
            correct.terms.get(&R2),
            Some(&0),
            "a range that did not install had its replica replaced: {:?}",
            correct.terms
        );

        // The pair, one variant at a time.
        //
        // `StepWhileInstalling`: the range is stepped inside the window. The replica
        // being replaced is behind its leader by definition, so a step of it there
        // appends at indices the switch is about to compact past.
        let stepping = run_with(setup(
            NodeVariants::correct().with(NodeVariant::StepWhileInstalling),
        ));
        assert!(
            position(&stepping.log, &Note::Local(R1, 1))
                < position(&stepping.log, &Note::Restored(R1)),
            "the variant is meant to step the installing range: {:?}",
            stepping.log
        );

        // `InstallKeepsTheOldCore`: the switch is made and the replica is not
        // replaced. The store holds the snapshot and the core goes on from the log it
        // had — the half of an install a server gets for free by reopening its store.
        let kept = run_with(setup(
            NodeVariants::correct().with(NodeVariant::InstallKeepsTheOldCore),
        ));
        assert_eq!(
            kept.terms.get(&R1),
            Some(&0),
            "the variant is meant to keep the core it should have replaced: {:?}",
            kept.terms
        );
        assert!(
            position(&kept.log, &Note::Local(R1, 1)) > position(&kept.log, &Note::Restored(R1)),
            "the variant still releases the hold, so only the replacement is missing: {:?}",
            kept.log
        );

        // `InstallHoldsEveryRange`: **the mutation a single-range world cannot see.**
        // The node reaches for the incarnation a server ends and stops every range it
        // hosts, so one range's install costs every other range on the node the length
        // of a stream's switch. With one range on the node, holding "every" range and
        // holding "this" range are the same hold, and nothing distinguishes them; with
        // two, r2's work waits behind r1's install.
        let every = run_with(setup(
            NodeVariants::correct().with(NodeVariant::InstallHoldsEveryRange),
        ));
        assert_eq!(
            every.meters.ranges_held_for_install, 2,
            "the variant is meant to hold every range on the node: {:?}",
            every.log
        );
        assert!(
            position(&every.log, &Note::Local(R2, 9)) > position(&every.log, &Note::Restored(R1)),
            "the variant is meant to make another range wait on this one's install: {:?}",
            every.log
        );
        // And it is still only a cost: every range is let go with the install, so the
        // node does not wedge. A variant that wedged would be caught for the wrong
        // reason.
        assert_eq!(
            every.terms.get(&R2),
            Some(&0),
            "the variant replaced a replica it only held: {:?}",
            every.terms
        );

        // The claim that this mutation needs more than one range, asserted rather than
        // asserted-in-a-comment: on a node of one range the variant holds exactly what
        // the correct node holds, and the two runs are indistinguishable.
        let alone = |variants| Setup {
            locals: &locals,
            duration: Duration::from_millis(90),
            cores: one_range,
            ..Setup::new(variants, &[])
        };
        let one_correct = run_with(alone(NodeVariants::correct()));
        let one_buggy = run_with(alone(
            NodeVariants::correct().with(NodeVariant::InstallHoldsEveryRange),
        ));
        assert_eq!(
            one_correct.meters.ranges_held_for_install, one_buggy.meters.ranges_held_for_install,
            "with one range the variant is meant to be the correct hold exactly, which \
             is what makes it a mutation only a node of several ranges can be wrong about"
        );
        assert_eq!(
            one_correct.terms.get(&R1),
            one_buggy.terms.get(&R1),
            "with one range the two runs left different replicas running"
        );
    }

    #[test]
    fn a_local_input_for_a_persisting_core_is_held_and_stepped_in_order() {
        // r1's disk is slow and r2's is never asked: an input for r1 arriving while
        // its persist is outstanding is held, and one for r2 is stepped at once.
        let arrivals = [arrival(R1, append(1, 1))];
        let delays = [(R1, Duration::from_millis(40))];
        let locals = [
            (
                Duration::from_millis(1),
                Mine {
                    range: R1,
                    n: 1,
                    work: Work::Step,
                },
            ),
            (
                Duration::from_millis(2),
                Mine {
                    range: R2,
                    n: 9,
                    work: Work::Step,
                },
            ),
            (
                Duration::from_millis(3),
                Mine {
                    range: R1,
                    n: 2,
                    work: Work::Step,
                },
            ),
            (
                Duration::from_millis(4),
                Mine {
                    range: R1,
                    n: 3,
                    work: Work::Step,
                },
            ),
        ];
        let setup = |variants| Setup {
            arrivals: &arrivals,
            locals: &locals,
            duration: Duration::from_millis(120),
            ..Setup::new(variants, &delays)
        };
        let correct = run_with(setup(NodeVariants::correct()));
        assert_eq!(
            correct.meters.locals_held, 3,
            "the three inputs for the persisting core were held: {:?}",
            correct.log
        );
        // r2 is not persisting, so its input was stepped where it arrived — before
        // r1's persist resolved, and before any of r1's held inputs.
        let resolved = position(&correct.log, &Note::Persisted(R1));
        let free = position(&correct.log, &Note::Local(R2, 9));
        assert!(
            free < resolved,
            "a local input for a core that is not persisting waited on another \
             range's disk: {:?}",
            correct.log
        );
        // r1's three were stepped after its own persist resolved, in the order they
        // arrived.
        let held: Vec<usize> = (1..=3)
            .map(|n| position(&correct.log, &Note::Local(R1, n)))
            .collect();
        assert!(
            held[0] > resolved,
            "a held input was stepped before its core's persist resolved: {:?}",
            correct.log
        );
        assert!(
            held[0] < held[1] && held[1] < held[2],
            "the held inputs were stepped out of order: {:?}",
            correct.log
        );

        // The pair: the node that drops what it should hold. The path is reached
        // exactly as often — the meter counts before the drop — and nothing of it
        // arrives.
        let buggy = run_with(setup(
            NodeVariants::correct().with(NodeVariant::HeldLocalDropped),
        ));
        assert_eq!(buggy.meters.locals_held, 3, "the variant reaches the path");
        assert_eq!(
            buggy
                .log
                .iter()
                .filter(|note| matches!(note, Note::Local(R1, _)))
                .count(),
            0,
            "the variant is meant to drop every held input: {:?}",
            buggy.log
        );
        assert_eq!(
            buggy
                .log
                .iter()
                .filter(|note| matches!(note, Note::Local(R2, _)))
                .count(),
            1,
            "the variant drops only what is held: {:?}",
            buggy.log
        );
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

        // Every response that left carried the host's stamp (RAFT.md §1, D-042). The
        // node cannot make it itself — the incarnation is the store's — and a send
        // that left unstamped carries a zero incarnation, which a leader reads as a
        // server with no store, and a zero clock, which is the lease's. Nothing else
        // in this crate looks at what `Host::stamp` did.
        assert!(
            !correct.stamped.is_empty(),
            "no append-response left the node, so the stamp was never asked"
        );
        assert!(
            correct.stamped.iter().all(|(_, stamped)| *stamped),
            "a response left the node unstamped: {:?}",
            correct.stamped
        );

        // What the node traced, and when (D-026, D-047). The append is recorded when
        // it is *durable* — after the core's persist resolved — and carries the
        // decision time of the step that made it, which is before the persist was even
        // submitted. Nothing else in this crate asserts that the node traces at all.
        let appends: Vec<(u64, u64, u128, u128)> = correct
            .trace
            .iter()
            .filter_map(|record| match record.event {
                ananke_env::TraceEvent::RaftAppend { range, index, .. } => Some((
                    range,
                    index,
                    u128::from(record.decided.as_nanos()),
                    u128::from(record.at.as_nanos()),
                )),
                _ => None,
            })
            .collect();
        assert!(
            !appends.is_empty(),
            "the node traced no append at all: {:?}",
            correct.trace
        );
        for (range, sync) in [(R1, 40u128), (R2, 5)] {
            let sync = sync * 1_000_000;
            let (_, _, decided, at) = *appends
                .iter()
                .find(|(traced, index, _, _)| *traced == range.get() && *index == 1)
                .unwrap_or_else(|| panic!("no append traced for {range:?}: {appends:?}"));
            assert!(
                at >= sync,
                "{range:?}'s append was traced at {at} ns, before its {sync} ns sync \
                 resolved: a `RaftAppend` in the trace before it is durable (D-026)"
            );
            assert!(
                decided < sync,
                "{range:?}'s append carries a decision time of {decided} ns, at or \
                 after its {sync} ns sync: the flush's time, not the step's (D-047)"
            );
            assert!(
                decided < at,
                "{range:?}'s append decided at {decided} ns and was recorded at {at} \
                 ns: a record made where it was decided cannot have waited for the \
                 disk"
            );
        }
        // D-050: a term's record carries when the message whose step changed the term
        // reached the node, which the node puts there and no core can.
        let terms: Vec<_> = correct
            .trace
            .iter()
            .filter(|record| {
                matches!(record.event, ananke_env::TraceEvent::RaftTerm { range, .. } if range == R1.get())
            })
            .collect();
        assert!(
            terms.iter().any(|record| matches!(
                record.event,
                ananke_env::TraceEvent::RaftTerm {
                    received: Some(_),
                    ..
                }
            )),
            "no term record carries the receipt of the message that caused it: {terms:?}"
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
        let (first, second) = (
            position(&correct.log, &Note::Submitted(R1)),
            position(&correct.log, &Note::Submitted(R2)),
        );
        assert!(
            first < first_resolution && second < first_resolution,
            "the round's persists did not reach the writer as one group: {:?}",
            correct.log
        );
        assert_eq!(
            second,
            first + 1,
            "the round's persists reach the writer in one pass, with nothing between \
             them that could let the writer close a group: {:?}",
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

    /// And the ticks reach the *core*: a follower behind a slow sync is as many ticks
    /// closer to its election timeout when the sync resolves as the sync cost it.
    ///
    /// The counter above says how many entries the replay popped; this says what they
    /// did. It is the point of the rule — a core behind a slow sync must not have its
    /// election and heartbeat timers run slow — and the arithmetic §4 leans on, that a
    /// sync past the election timeout starts an election.
    ///
    /// The pair: `CollapseHeldTicks` replays one tick for all of them, so the
    /// follower's timer is short by the whole sync and it does not campaign; a node
    /// that popped the held tick without stepping it into the core would not either.
    #[test]
    fn the_ticks_a_core_missed_reach_its_timer_and_not_only_its_counter() {
        // r1 campaigns after four ticks of silence, and its sync spans six of them:
        // the replay must take it past its election timeout at the resolution.
        let delays = [
            (R1, Duration::from_millis(65)),
            (R2, Duration::from_millis(1)),
        ];
        let arrivals = [arrival(R1, append(1, 1))];
        let setup = |variants| Setup {
            cores: quick_to_campaign,
            arrivals: &arrivals,
            // Long enough for the resolution at 65 ms and its replay, and short
            // enough that a node whose timer stood still has not campaigned yet: it
            // would need four more ticks, so 105 ms.
            duration: Duration::from_millis(75),
            ..Setup::new(variants, &delays)
        };

        let correct = run_with(setup(NodeVariants::correct()));
        assert_eq!(
            correct.meters.ticks_replayed_most, 6,
            "a 65 ms sync spans six 10 ms ticks: {:?}",
            correct.meters
        );
        let campaign = find(
            &correct.log,
            "campaign",
            |note| matches!(note, Note::Shipped(_, ranges) if ranges.contains(&R1)),
        );
        assert!(
            campaign > position(&correct.log, &Note::Persisted(R1)),
            "the campaign left before the sync resolved: {:?}",
            correct.log
        );
        assert!(
            correct
                .sent
                .iter()
                .any(|(range, what)| *range == R1 && *what == "pre-vote"),
            "the replayed ticks did not reach r1's election timer: {:?}",
            correct.sent
        );

        let collapsed = run_with(setup(
            NodeVariants::correct().with(NodeVariant::CollapseHeldTicks),
        ));
        assert!(
            !collapsed
                .sent
                .iter()
                .any(|(range, what)| *range == R1 && *what == "pre-vote"),
            "the variant is meant to leave r1's timer short by the whole sync, so it \
             has not campaigned yet: {:?}",
            collapsed.sent
        );
    }

    /// A message for a core whose persist is outstanding is taken from the inbox and
    /// held for that core, *counted against the node's byte bound*, **and the bound
    /// refuses on it** (Q14).
    ///
    /// The second half is the whole of it. The `raft` task drains the queue to empty
    /// on every wake, so at the moment a frame arrives the queue is always empty and
    /// D-072's "nothing is refused into an empty queue" would exempt every arrival: a
    /// bound that is arithmetic only, and a node that holds messages without limit for
    /// the whole of a slow sync. So this check does not read the figure the node just
    /// wrote — it puts arrivals against a bound a few messages wide and asks the
    /// *inbox* whether they got in (PROPOSED D-074).
    ///
    /// The pair: `HeldNotCounted` takes the message and stops counting it, and this
    /// check catches it — the node refuses nothing and holds everything.
    #[test]
    fn what_the_node_holds_fills_the_nodes_bound() {
        // Four 64-byte messages fill the bound.
        const BOUND: usize = 256;
        // r1's sync outlasts every arrival; r2's disk is quick.
        let delays = [
            (R1, Duration::from_millis(65)),
            (R2, Duration::from_millis(1)),
        ];
        // The first append puts r1's core behind a sync; each one after it arrives at
        // a core that cannot step and is held.
        let arrivals = [arrival(R1, append(1, 1))];
        let feed: Vec<(Duration, Received)> = (2..=11u64)
            .map(|index| (Duration::from_millis(index), arrival(R1, append(index, 1))))
            .chain([(
                // After the sync resolved at 65 ms and before the tick at 70 ms: what
                // the node no longer holds it no longer charges, so this one gets in.
                Duration::from_millis(67),
                arrival(R1, append(12, 1)),
            )])
            .collect();
        let setup = |variants| Setup {
            bound: BOUND,
            arrivals: &arrivals,
            feed: &feed,
            duration: Duration::from_millis(75),
            ..Setup::new(variants, &delays)
        };

        let correct = run_with(setup(NodeVariants::correct()));
        assert!(
            correct.charged_most <= BOUND,
            "the node was charged {} against a bound of {BOUND}",
            correct.charged_most
        );
        assert!(
            correct.held_bytes_seen >= BOUND - 64,
            "what the node held never reached the bound, so the bound was never \
             asked: {}",
            correct.held_bytes_seen
        );
        let refused = correct.fed.iter().filter(|got_in| !**got_in).count();
        assert!(
            refused >= 1,
            "every arrival got in: what the node holds does not bind it, and a node \
             behind a slow sync holds messages without limit: {:?}",
            correct.fed
        );
        assert_eq!(
            correct.fed.last(),
            Some(&true),
            "an arrival after the sync resolved was refused, so the node goes on \
             charging for what it has let go: {:?}",
            correct.fed
        );

        let buggy = run_with(setup(
            NodeVariants::correct().with(NodeVariant::HeldNotCounted),
        ));
        assert_eq!(
            buggy.held_bytes_seen, 0,
            "the variant is meant to stop counting what it holds: {}",
            buggy.held_bytes_seen
        );
        assert!(
            buggy.fed.iter().all(|got_in| *got_in),
            "the variant is meant to refuse nothing: {:?}",
            buggy.fed
        );
        assert!(
            buggy.meters.held_most > correct.meters.held_most,
            "the variant is meant to hold more than the bound allows: {:?} against \
             {:?}",
            buggy.meters,
            correct.meters
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

    /// The "1" of a round's `1 + p` frames per peer is a *frame*, not only
    /// arithmetic: a round that sends before its sync and then persists puts a frame
    /// on the wire at its own flush and one more at each core's resolution
    /// (SHARD.md:490-492, §12).
    ///
    /// The two checks above have rounds where every core persists on its first step,
    /// so their pre-sync flush ships nothing and what they measure is the `p`. This is
    /// the round that has both.
    #[test]
    fn a_round_that_sends_before_its_sync_costs_that_frame_too() {
        // r2 answers a pre-vote, which persists nothing, so its response is one of the
        // round's early outputs; r1's append persists.
        let arrivals = [prevote(R2), arrival(R1, append(1, 1))];
        let delays = [
            (R1, Duration::from_millis(20)),
            (R2, Duration::from_millis(1)),
        ];
        let measured = run(
            NodeVariants::correct(),
            &delays,
            &arrivals,
            Duration::from_millis(60),
        );
        assert_eq!(
            measured.frames.rounds_sending_before_their_sync, 1,
            "the round sent before its sync: {:?}",
            measured.frames
        );
        assert_eq!(
            measured.frames.most_flushes_in_a_round, 2,
            "the round's own flush and the one persisting core's: {:?}",
            measured.frames
        );
        assert_eq!(
            measured.frames.most_frames_to_a_peer_in_a_round, 2,
            "1 + p frames to the one peer, p = 1: the pre-sync flush's and the \
             resolution's: {:?}",
            measured.frames
        );
        assert_eq!(
            measured.sent,
            vec![(R2, "pre-vote-response"), (R1, "append-response")],
            "the early send left first and the deferred one after its sync: {:?}",
            measured.sent
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
    /// (SHARD.md §4). And the jobs it hands the task **partition** the committed log:
    /// every index once, in order, no gap and no repeat.
    ///
    /// The second half is the node's own bookkeeping — the one-group server keeps it
    /// in `ananke-raft` — so nothing else in the tree covers it. A job is also the
    /// only place the entries an `Apply` names can be seen: without the indices a
    /// check can only say *that* a job arrived, which a node re-applying its whole log
    /// from index 1 also does.
    ///
    /// The pair: `DeferredFlushedEarly` hands the apply out before the persist
    /// resolves; `AppliedNotAdvanced` forgets what it handed out, so every job starts
    /// again at the first index. The same check catches both.
    #[test]
    fn the_applies_reach_the_task_after_their_persists_and_partition_the_log() {
        /// An AppendEntries that commits what it carries: the follower appends and
        /// pushes an `Apply` in the same step (core.rs:2514-2522).
        fn committing(index: Index) -> Received {
            let mut message = append(index, 1);
            if let Message::AppendEntries { commit, .. } = &mut message {
                *commit = index;
            }
            arrival(R1, message)
        }

        let arrivals = [committing(1)];
        // Each append reaches a core whose previous persist has resolved, so each is
        // its own round and each names one more index.
        let feed = [
            (Duration::from_millis(15), committing(2)),
            (Duration::from_millis(30), committing(3)),
        ];
        let delays = [
            (R1, Duration::from_millis(10)),
            (R2, Duration::from_millis(1)),
        ];
        let setup = |variants| Setup {
            arrivals: &arrivals,
            feed: &feed,
            duration: Duration::from_millis(60),
            ..Setup::new(variants, &delays)
        };

        let correct = run_with(setup(NodeVariants::correct()));
        let first_apply = find(&correct.log, "apply", |note| {
            matches!(note, Note::Applied(..))
        });
        assert!(
            first_apply > position(&correct.log, &Note::Persisted(R1)),
            "an apply left before its core's persist resolved: {:?}",
            correct.log
        );
        let jobs: Vec<Vec<Index>> = correct
            .log
            .iter()
            .filter_map(|note| match note {
                Note::Applied(_, Job::Entries(indices)) => Some(indices.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            jobs,
            vec![vec![1], vec![2], vec![3]],
            "the applied stream is not a partition of the committed log: {jobs:?}"
        );

        let early = run_with(setup(
            NodeVariants::correct().with(NodeVariant::DeferredFlushedEarly),
        ));
        assert!(
            find(&early.log, "apply", |note| matches!(
                note,
                Note::Applied(..)
            )) < position(&early.log, &Note::Persisted(R1)),
            "the variant is meant to hand the apply out early: {:?}",
            early.log
        );

        let again = run_with(setup(
            NodeVariants::correct().with(NodeVariant::AppliedNotAdvanced),
        ));
        let jobs: Vec<Vec<Index>> = again
            .log
            .iter()
            .filter_map(|note| match note {
                Note::Applied(_, Job::Entries(indices)) => Some(indices.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            jobs,
            vec![vec![1], vec![1, 2], vec![1, 2, 3]],
            "the variant is meant to hand the task the whole log again every time: \
             {jobs:?}"
        );
    }

    /// One resolution, two applies: the deferred one **before** the replayed one.
    ///
    /// The check above feeds appends that each land on a core whose previous persist
    /// has resolved, so each is its own round and no resolution ever carries both a
    /// deferred `Apply` and a replayed one. This is the case that does, and it is the
    /// only thing that pins the order of the two inside `Cores::resolved`: the
    /// deferred outputs are the ones the persisting step produced, so they are older
    /// than anything the replay steps afterwards, and appending them after the replay
    /// instead — a one-line move — hands the `apply` task index 2 before index 1.
    ///
    /// An applied stream out of index order is not a crash: the state machine is
    /// handed a later index first, and `applied_sent` has already passed both, so
    /// nothing downstream ever says so.
    ///
    /// The shape: an `AppendEntries{entries: [1, 2], commit: 1}` — the follower
    /// appends (a `Persist`) and its `Apply{through: 1}` is *deferred* behind it —
    /// then, while that sync is outstanding, a heartbeat `{entries: [], commit: 2}`,
    /// which is held. The heartbeat persists nothing, so when the replay steps it its
    /// `Apply{through: 2}` is one of the resolution round's **early** outputs.
    #[test]
    fn a_resolution_hands_the_apply_task_its_deferred_job_before_its_replayed_one() {
        fn entry(index: Index) -> Entry {
            Entry {
                index,
                term: 1,
                payload: Payload::Command(Bytes::from_static(b"x")),
            }
        }
        /// Two entries, committing the first: the step persists and defers its apply.
        fn two_committing_one() -> Received {
            arrival(
                R1,
                Message::AppendEntries {
                    term: 1,
                    prev_index: 0,
                    prev_term: 0,
                    entries: vec![entry(1), entry(2)],
                    commit: 1,
                    sent: 0,
                },
            )
        }
        /// A heartbeat committing through index 2: it carries no entry, so the step
        /// persists nothing and its apply is one of its round's early outputs.
        fn beat_committing_two() -> Received {
            arrival(
                R1,
                Message::AppendEntries {
                    term: 1,
                    prev_index: 2,
                    prev_term: 1,
                    entries: Vec::new(),
                    commit: 2,
                    sent: 0,
                },
            )
        }

        let arrivals = [two_committing_one()];
        // The heartbeat arrives well inside r1's 20 ms sync, so it is held.
        let feed = [(Duration::from_millis(5), beat_committing_two())];
        let delays = [
            (R1, Duration::from_millis(20)),
            (R2, Duration::from_millis(1)),
        ];
        let run = run_with(Setup {
            arrivals: &arrivals,
            feed: &feed,
            duration: Duration::from_millis(45),
            ..Setup::new(NodeVariants::correct(), &delays)
        });
        assert_eq!(run.fed, vec![true], "the heartbeat was admitted");
        let jobs: Vec<Vec<Index>> = run
            .log
            .iter()
            .filter_map(|note| match note {
                Note::Applied(_, Job::Entries(indices)) => Some(indices.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            jobs,
            vec![vec![1], vec![2]],
            "the applied stream left the resolution out of index order: {:?}",
            run.log
        );
        // Both jobs are of the one resolution: they follow the persist, and there is
        // only one persist in the run.
        let persisted = position(&run.log, &Note::Persisted(R1));
        let applies: Vec<usize> = run
            .log
            .iter()
            .enumerate()
            .filter(|(_, note)| matches!(note, Note::Applied(..)))
            .map(|(at, _)| at)
            .collect();
        assert_eq!(applies.len(), 2, "{:?}", run.log);
        assert!(
            applies.iter().all(|at| *at > persisted),
            "an apply of the resolution left before the persist resolved: {:?}",
            run.log
        );
        assert_eq!(
            run.log
                .iter()
                .filter(|note| matches!(note, Note::Submitted(R1)))
                .count(),
            1,
            "the heartbeat was meant to persist nothing: {:?}",
            run.log
        );
    }

    /// Where each of a step's outputs goes (SHARD.md §4, RAFT.md §1).
    ///
    /// A [`SnapshotAction::Take`] is the **`apply` task's**, a job between two
    /// applies, and that routing is the whole of D-036's consequence: one range's take
    /// stalls every range's applies on the node, which §12 asks Stage B to measure. A
    /// take handed to the `snapshot` task instead would stall nothing, and the
    /// measurement would measure a stall that no longer exists. Every other snapshot
    /// action *is* the `snapshot` task's.
    ///
    /// The outputs are handed to [`Node::act`] rather than drawn out of a core: what
    /// is under test is the node's routing table, and a core emits a take only after a
    /// leader has run two election timeouts past its threshold, which is a scenario
    /// the sweeps get to first (slice 4). A proposal's refusal and a read's answer are
    /// here for the same reason — the node has no client path until that slice, so
    /// nothing else in this crate executes them at all.
    ///
    /// The pair: `TakeToSnapshotTask` routes the take to the `snapshot` task, and this
    /// check catches it.
    #[test]
    fn every_output_of_a_step_goes_where_section_four_sends_it() {
        fn routed(variants: NodeVariants) -> Vec<Note> {
            let mut sim = Sim::new(SimConfig::new(3));
            let id = sim.add_node();
            let env = sim.env(id);
            let probe = Probe::new(&env, &[]);
            let log = probe.log.clone();
            let mut cores = Cores::new(variants);
            cores.insert(R1, core(R1));
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
            let stamps = Stamps {
                decided: env.decision(),
                received: None,
            };
            let outputs = vec![
                (Output::Snapshot(SnapshotAction::Take), Some(Ok(Vec::new()))),
                (
                    Output::Snapshot(SnapshotAction::Install {
                        to: LEADER,
                        index: 1,
                        term: 1,
                    }),
                    None,
                ),
                (
                    Output::Apply { through: 1 },
                    Some(Ok(vec![Entry {
                        index: 1,
                        term: 1,
                        payload: Payload::Command(Bytes::from_static(b"x")),
                    }])),
                ),
                (
                    Output::Rejected {
                        leader: Some(LEADER),
                    },
                    None,
                ),
                (
                    Output::ReadReady {
                        id: 7,
                        index: 1,
                        lease: true,
                    },
                    None,
                ),
                (Output::ReadDropped { id: 8 }, None),
            ];
            env.clone().spawn("acts", async move {
                for (output, entries) in outputs {
                    node.act(Act {
                        range: R1,
                        stamps,
                        output,
                        entries,
                    })
                    .await
                    .expect("no act here fails");
                }
            });
            sim.run_for(TICK);
            log.lock().expect("the log").clone()
        }

        assert_eq!(
            routed(NodeVariants::correct()),
            vec![
                Note::Applied(R1, Job::Take),
                Note::Snapshotted(R1, "stream"),
                Note::Applied(R1, Job::Entries(vec![1])),
                Note::Rejected(R1),
                Note::ReadReady(R1, 7),
                Note::ReadDropped(R1, 8),
            ],
            "an output went somewhere §4 does not send it"
        );

        let buggy = routed(NodeVariants::correct().with(NodeVariant::TakeToSnapshotTask));
        assert_eq!(
            buggy.first(),
            Some(&Note::Snapshotted(R1, "take")),
            "the variant is meant to hand the take to the `snapshot` task: {buggy:?}"
        );
        assert!(
            !buggy.contains(&Note::Applied(R1, Job::Take)),
            "the variant is meant to keep the take off the `apply` queue: {buggy:?}"
        );
    }

    /// The two arms of the `Apply` route that no run reaches, driven at the node.
    ///
    /// **A gap fails the node.** `entries_to_apply` answers `Err(index)` for an index
    /// the core did not hold, and `the_entries_an_apply_names_are_the_ones_not_handed_out_yet`
    /// asserts that of the *helper*. This asserts it of [`Node::act`]: the node tells
    /// its host it failed and returns the error. Passing over the gap instead hands the
    /// state machine nothing while [`Cores::applied_sent`] has already advanced past
    /// the hole, so the entries in the hole are never applied and nothing anywhere
    /// later says so — the one shape that cannot show up as a failure afterwards.
    ///
    /// **An empty job never reaches the queue.** An `Apply` naming only indices already
    /// handed out is not a failure, but it must not be handed on: a job on the
    /// one-at-a-time `apply` queue costs a slot, and every other range's applies wait
    /// behind it (D-036, Q14).
    ///
    /// The outputs are handed to [`Node::act`] for the reason the routing check above
    /// gives: the scenario in which a gap *arises* needs the node's compaction path,
    /// which is issue #79's; its reaction to one is testable now.
    #[test]
    fn a_gap_in_the_applied_stream_fails_the_node_and_an_empty_job_is_not_a_job() {
        fn act_on(entries: Option<Result<Vec<Entry>, Index>>) -> (bool, Vec<String>, Vec<Note>) {
            let mut sim = Sim::new(SimConfig::new(5));
            let id = sim.add_node();
            let env = sim.env(id);
            let probe = Probe::new(&env, &[]);
            let log = probe.log.clone();
            let failures = probe.failures.clone();
            let variants = NodeVariants::correct();
            let mut cores = Cores::new(variants);
            cores.insert(R1, core(R1));
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
            let stamps = Stamps {
                decided: env.decision(),
                received: None,
            };
            let failed = Arc::new(Mutex::new(false));
            env.clone().spawn("act", {
                let failed = failed.clone();
                async move {
                    let outcome = node
                        .act(Act {
                            range: R1,
                            stamps,
                            output: Output::Apply { through: 2 },
                            entries,
                        })
                        .await;
                    *failed.lock().expect("the cell") = outcome.is_err();
                }
            });
            sim.run_for(TICK);
            let failed = *failed.lock().expect("the cell");
            (
                failed,
                failures.lock().expect("the failures").clone(),
                log.lock().expect("the log").clone(),
            )
        }

        // The gap: the core's own `Apply` named index 2, which it does not hold.
        let (failed, failures, log) = act_on(Some(Err(2)));
        assert!(
            failed,
            "a gap in the applied stream did not fail the node: {log:?}"
        );
        assert_eq!(
            failures.len(),
            1,
            "the host was not told the node failed: {failures:?}"
        );
        assert!(
            failures[0].contains("which the core does not hold"),
            "the failure does not say what it was: {failures:?}"
        );
        assert!(
            !log.iter().any(|note| matches!(note, Note::Applied(..))),
            "the state machine was handed a job with a hole in it: {log:?}"
        );

        // The empty job: every index the apply named has been handed out already.
        let (failed, failures, log) = act_on(Some(Ok(Vec::new())));
        assert!(!failed, "an empty apply is not a failure: {failures:?}");
        assert!(failures.is_empty(), "{failures:?}");
        assert!(
            !log.iter().any(|note| matches!(note, Note::Applied(..))),
            "an empty job took a slot on the one-at-a-time `apply` queue, where every \
             other range's applies wait behind it: {log:?}"
        );

        // And the ordinary job does reach it, so the two above are refusals of
        // something this path otherwise does.
        let (failed, failures, log) = act_on(Some(Ok(vec![
            Entry {
                index: 1,
                term: 1,
                payload: Payload::Command(Bytes::from_static(b"x")),
            },
            Entry {
                index: 2,
                term: 1,
                payload: Payload::Command(Bytes::from_static(b"y")),
            },
        ])));
        assert!(!failed, "{failures:?}");
        assert_eq!(log, vec![Note::Applied(R1, Job::Entries(vec![1, 2]))]);
    }

    /// The round arms its persists: they are polled in one pass **before the task
    /// awaits anything else**, so the round's records are with the WAL writer before
    /// anything can let it close a group without them (wal.rs:16-20, D-018).
    ///
    /// Submitting without arming is not a split round — the next poll still takes them
    /// all — it is a *late* one: the task's loop races its inbox, its ticker and the
    /// outstanding persists, and `race` returns the first side that is ready without
    /// polling the other, so a round's records can sit unsubmitted while the task
    /// handles a tick and a later round's work, and reach the writer behind it.
    ///
    /// Which side of that race wins is the scheduling stream's, so the directed
    /// scenario is a set of seeds and not one: the correct node submits before its
    /// next note on *every* seed, and `PersistsNotArmed` does not on at least one.
    #[test]
    fn a_rounds_persists_reach_the_writer_before_the_task_takes_anything_else() {
        // r2 answers a pre-vote without persisting and r1 persists, so the round has
        // an early flush — a `Shipped` — and then a submission. The flush's frame is
        // shipped at the round's start; nothing may come between it and the
        // submission.
        let arrivals = [prevote(R2), arrival(R1, append(1, 1))];
        let delays = [
            (R1, Duration::from_millis(25)),
            (R2, Duration::from_millis(1)),
        ];
        let submits_at_once = |variants, seed| {
            let run = run_with(Setup {
                // The socket takes longer than a tick, so the task's loop comes back
                // round from the round's own flush with the ticker already due — and
                // r2 campaigns on that tick, which the host sees.
                cores: r2_campaigns_at_once,
                ship: Duration::from_millis(12),
                arrivals: &arrivals,
                duration: Duration::from_millis(60),
                seed,
                ..Setup::new(variants, &delays)
            });
            let shipped = find(&run.log, "the round's early flush", |note| {
                matches!(note, Note::Shipped(..))
            });
            let submitted = position(&run.log, &Note::Submitted(R1));
            (submitted == shipped + 1, run.log)
        };

        for seed in 0..24 {
            let (at_once, log) = submits_at_once(NodeVariants::correct(), seed);
            assert!(
                at_once,
                "seed {seed}: the round's persist reached the writer only after the \
                 task had taken another event: {log:?}"
            );
        }
        let late = (0..24)
            .filter(|seed| {
                !submits_at_once(
                    NodeVariants::correct().with(NodeVariant::PersistsNotArmed),
                    *seed,
                )
                .0
            })
            .count();
        assert!(
            late > 0,
            "the variant is meant to leave the round's persists unpolled until the \
             loop happens to poll them, which on some seeds is after another event"
        );
    }
}
