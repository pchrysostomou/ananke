//! The node's known-buggy variants (CLAUDE.md's pair rule).
//!
//! [`ananke_raft::Variant`] is the Raft core's set: a protocol bug beside the correct
//! protocol, caught by the sweeps. These are the *node's*: each is a plausible way to
//! get Q41's round wrong, built beside the correct round and caught by the same check
//! that asserts the correct round's order (SHARD.md §4). They are not Raft bugs — the
//! core is untouched — so they are a set of their own rather than more of the core's,
//! and they do not appear in [`ananke_raft::Variant::BUGS`] or in §10's count.
//!
//! Most are caught by a deterministic check in this crate, which is the pair rule's
//! requirement — the buggy variant is *seen to fail* the check the correct code passes
//! — without a rate to measure (D-061 asks a tier of a *sweep's* assertion). Since
//! D-076 the node is under a sweep of its own as well (`sim/tests/ranges.rs`), and
//! [`NodeVariant::StepWhilePersisting`] is caught there on 63.9 % of seeds, asserted at
//! every tier. A variant no sweep can see keeps its deterministic check and says so:
//! [`NodeVariant::HeldLocalDropped`] is one, because a client retries a request it
//! loses and an `Applied` is superseded by the next one, so the sweep passes it at a
//! thousand seeds. The `snapshot` task's own are [`NodeVariant::SNAPSHOT`], each a way
//! to get a snapshot keyed by range and follower wrong; they are caught the same way,
//! by deterministic checks in [`mod@crate::snapshot`].
//!
//! [`NodeVariant::PersistsNotArmed`] is caught over a small fixed set of scheduling
//! seeds rather than on one, because what it breaks depends on which side of the
//! task's race is polled first: the directed scenario is the set, not a seed.

use std::fmt;

/// One way to get the node's round wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum NodeVariant {
    /// A core's outputs after its `Persist` are executed with the round's early
    /// outputs, before the round's sync, instead of when that core's own persist
    /// resolves. This is the node's version of the bug D-026 keeps
    /// [`ananke_raft::Variant::SendBeforePersist`] from hiding: an `Apply` handed out
    /// early lets the `apply` task make an applied index durable above the durable
    /// log, and a trace event handed out early puts a `RaftAppend` in the trace before
    /// it is durable (SHARD.md §4).
    DeferredFlushedEarly,
    /// A core whose persist is outstanding is stepped anyway, rather than having its
    /// messages and ticks held: Figure 2's rule that no step of a core runs on state
    /// its own disk does not yet hold (RAFT.md §1, SHARD.md §4).
    StepWhilePersisting,
    /// Every tick a core missed while its persist was outstanding is collapsed into
    /// one, rather than each being stepped: the election and heartbeat timers of a
    /// core behind a slow sync then run slow by however long the sync took (SHARD.md
    /// §4, "every missed tick stepped, none collapsed").
    CollapseHeldTicks,
    /// The round's persists are awaited one at a time rather than submitted together,
    /// so each pays a sync of its own instead of sharing the WAL writer's group commit
    /// (wal.rs:16-20, D-018). The round is still ordered correctly; what it loses is
    /// the group.
    PersistsOneAtATime,
    /// A message taken from the inbox and held for a core whose persist is outstanding
    /// stops counting against the node's byte bound, so a node behind a slow sync
    /// holds messages without limit (SHARD.md §4, Q14).
    HeldNotCounted,
    /// A node-local input — a client's request, an index the `apply` task made
    /// durable — arriving for a core whose persist is outstanding is thrown away
    /// instead of held, so a client of that range loses its request and the core is
    /// never told that index applied (SHARD.md §4).
    ///
    /// Holding a *local* input exactly as a message of its range is held is the
    /// node's own rule, and nothing downstream of it can see the difference: a client
    /// retries, and an `Applied` is superseded by the next one. That is why this
    /// variant is caught by a check of its own in [`mod@crate::node`] rather than by
    /// a sweep — the node scenario's checks pass it at a thousand seeds — and why
    /// [`Meters::locals_held`] exists to say the path was reached at all.
    ///
    /// [`Meters::locals_held`]: crate::round::Meters::locals_held
    HeldLocalDropped,
    /// The round's persists are submitted but not armed: the futures are left unpolled
    /// until the loop happens to poll them, so the round's records reach the WAL writer
    /// only after the task has taken another event — and join whatever group is open
    /// then, which may be a later round's (wal.rs:16-20, D-018).
    PersistsNotArmed,
    /// The index handed to the `apply` task is not remembered, so every `Apply` hands
    /// the task the whole log again from the first index (SHARD.md §4, Q14): a
    /// non-idempotent state machine applies every committed command twice.
    AppliedNotAdvanced,
    /// A [`SnapshotAction::Take`] is handed to the `snapshot` task rather than to the
    /// `apply` task, so the take no longer runs between two applies and D-036's whole
    /// consequence — one range's take stalls every range's applies on the node —
    /// silently stops holding (RAFT.md §1, D-036).
    ///
    /// [`SnapshotAction::Take`]: ananke_raft::core::SnapshotAction::Take
    TakeToSnapshotTask,
    /// One staging directory for the whole engine directory, as the one-group
    /// receiver has (snapshot.rs:96-101), instead of one per (range, sender): two
    /// assemblies then write over each other's files (SHARD.md §11, raft 14).
    SharedStagingDir,
    /// One assembly for the whole node, abandoned for a chunk of another identity, as
    /// the one-group snapshot task's is (snapshot.rs:1018-1027; node.rs:1430). The
    /// re-seeds heading for one node then restart each other (SHARD.md §4, §11 raft
    /// 14).
    OneAssemblyPerNode,
    /// A version directory named by index and take alone, `snap-<index>-<take>`
    /// (snapshot.rs:119-121): two ranges' takes at one index share a directory.
    VersionDirWithoutRange,
    /// A sweep that deletes every unpinned version directory, whatever range it
    /// belongs to, as today's does against the store's single snapshot record
    /// (snapshot.rs:193-228): one range's sweep deletes another range's checkpoints.
    SweepAcrossRanges,
    /// A per-node cap on streams *sent*, so a leader feeds its designated followers
    /// one at a time instead of all at once (Q14, D-043).
    CapStreamsSent,
    /// Snapshot chunks put through the per-peer outbox, where they are cut into frames
    /// with whatever else is queued, instead of going in frames of their own on the
    /// snapshot task's socket handle: a 256 KiB chunk then spends the frame a round's
    /// heartbeats needed (Q41, SHARD.md §4).
    ChunksInBatchFrames,
    /// The install's manifest switch made without the range's repair carried in it, as
    /// the stream's last chunk arrives (D-066; RAFT.md:238-246). It is
    /// [`ananke_raft::Variant::SnapshotWithoutCurrentLast`] on the node's install path.
    InstallWithoutRepair,
    /// `RaftAdopted` traced for a replica's live install. On the node that event
    /// records only a node taking a fresh directory after a whole-node refusal
    /// (D-066), and a reader that counts adoptions would count every install as one.
    AdoptedOnRangeInstall,
    /// The staging directory keyed by range alone, `staging-r<range>`, which is what
    /// §11's raft item 14 says in so many words. Two senders of one range — a leader
    /// and the stale leader it replaced — each hold an assembly of their own, and
    /// under this name the two assemblies write over each other's files exactly as
    /// two ranges would (SHARD.md:1910-1911; D-075, proposed).
    StagingByRangeAlone,
    /// A freed receive slot reserved for the waiter at the head of the queue, instead
    /// of granted to a waiter when its next chunk arrives. The reservation is held for
    /// a (range, sender) that may never send again — its leader changed while it
    /// waited, which is the case §11 raft 14 exists for — and nothing here has a clock
    /// to reclaim it, so the node's slots fill with reservations for departed senders
    /// and it re-seeds nothing more (SHARD.md §12; Q14).
    SlotReservedForWaiter,
    /// Every core of the node seeded from the node's own stream, `env.rng()`, rather
    /// than from `n{id}/r{range}/protocol` (D-057, `Environment::range_rng`). The
    /// four cores still draw four different seeds, so nothing about *one* run tells
    /// the two apart — what the keying buys is that range r's stream is range r's
    /// alone, so a range added to or removed from the configuration moves no other
    /// range's schedule, and under this variant it moves all of them (SHARD.md §2,
    /// Q13).
    OneSeedForEveryCore,
    /// A read a replica refuses leaves its registration behind: the step's refusal
    /// takes the work in flight and the `reads` map keeps its `(SocketAddr,
    /// Request)` for the life of the node. This is the node exactly as it was before
    /// D-076's review — a follower refusing reads for a living grows an unbounded
    /// map — and it is here rather than in a scratch file so that the bug has a half
    /// beside the correct code that the node scenario runs on every seed
    /// (CLAUDE.md:52-57; SHARD.md §4).
    RefusedReadLeft,
    /// A stream completed on the very chunk that restarted it: the node is told to
    /// install, never that the staging directory must start over, so the install takes
    /// the abandoned stream's files for the new snapshot's (RAFT.md:203-207).
    CompleteOnRestart,
    /// An install that carries the node's *first hosted* range's spans instead of the
    /// completed range's. `Engine::install_spans` removes every key of the spans it is
    /// given and adds the staged tables in one switch (D-068), so a re-seed of one
    /// range then deletes another range's Raft state and user keys while that range is
    /// running — silently, on a node whose ranges are otherwise correct (D-066,
    /// D-075).
    InstallWrongRangesSpans,
    /// A chunk naming a range the node does not host taken in and given an assembly,
    /// instead of refused on arrival. `range` is a peer's word: a leader that has not
    /// learned the rebalancer moved the range (Q33), or a garbled range id, then holds
    /// a slot under the node's receive cap and starves the ranges the node does host
    /// (SHARD.md §11 raft 14; D-075).
    AdmitsAnUnhostedRange,
    /// An assembly kept until it finishes even when the range's own Raft has
    /// superseded its sender: a chunk of that range from a leader at a higher term
    /// waits behind an assembly no stream will ever complete. Where
    /// [`NodeVariant::SlotReservedForWaiter`] wedges the node on a *waiter's* slot,
    /// this wedges it on an admitted one (Q14; D-075).
    AssemblyHeldForDepartedSender,
    /// A resent last chunk installed a second time. A chunk unanswered for half a
    /// minimum election timeout is resent as a matter of course (RAFT.md:200-202), so
    /// the node makes a second manifest switch from a staging directory the first
    /// switch may already have consumed (D-068, D-075).
    InstallsADuplicateLastChunk,
    /// A loss in the shared engine treated as one range's: only the range whose store
    /// open failed is refused — traced as refused, and given a refused mark — and the
    /// node's other replicas are neither. A node owns one engine (Q2), so a loss in it
    /// is every replica's: this is the whole of Q15 got wrong (SHARD.md §11, storage 8).
    ///
    /// What the three unrefused replicas then do is worth stating exactly, because the
    /// obvious sentence — that they "carry on over the same engine" — is not available
    /// to any implementation: the refused directory's marker says lost, so nothing can
    /// open it at all. They are re-created in the *new* engine instead, and because
    /// nothing refused them they are created there as a first start would create them:
    /// quarantine clear, incarnation 1, empty. That is the same harm one range along —
    /// three replicas voting again on state their node lost (D-035) and three leaders
    /// keeping a `matched` the rebuilt log cannot honour (D-042) — reached by the
    /// mistake a reader of §11 would actually make.
    RefuseOneRangeOnly,
    /// The re-seed built in the refused directory instead of a new one beside it,
    /// which opens fresh a directory that held a store (D-041).
    ReseedIntoRefusedDir,
    /// The re-seed's new directory taking the lowest generation not in use rather than
    /// the one past the highest present: a node refused twice hands back its first
    /// refusal's directory (D-041).
    ReuseLostGeneration,
    /// A start opening the newest directory whatever its marker says, instead of the
    /// newest not marked lost (D-066). A node refused into a new directory that
    /// crashed before that directory held anything then reopens the refused one.
    OpenNewestEvenIfLost,
    /// A replica of a re-seeded node answering before its durable refused mark is
    /// written into the new engine. Every replica in a fresh engine would otherwise
    /// open as a first start — quarantine clear, incarnation 1 — so a replica that
    /// answers first is a replica that may vote again on state its node lost (D-035)
    /// and whose leader keeps a `matched` the rebuilt log cannot honour (D-042).
    ServeBeforeRefusedMark,
    /// The replica's incarnation drawn from the per-range protocol stream rather than
    /// from the node's own generator. `SimEnv` derives a named stream from the seed and
    /// the name alone, so a replica created again for the same (range, node) draws the
    /// number its predecessor drew, and a leader that compares incarnations for
    /// inequality only (D-042) never resets (Q26; SHARD.md:1372-1379).
    IncarnationPerRangeStream,
    /// A range whose live install is between its decision and its manifest switch is
    /// stepped anyway, rather than held. The replica being replaced is behind its
    /// leader by definition, so a step of it in that window appends entries at indices
    /// the switch is about to compact past, and the store comes back with a log below
    /// its own snapshot record (RAFT.md §1; D-066).
    ///
    /// A server ends its whole run-loop incarnation across an install and so has no
    /// such window; a node holds one range instead (SHARD.md §11, storage 5). This is
    /// that hold removed.
    // PROPOSED(D-083): a range is held across its live install.
    StepWhileInstalling,
    /// The install's manifest switch is made and the range's replica is *not* replaced:
    /// the store holds the snapshot and the old core goes on from the log it had. A
    /// server gets the replacement for free by reopening its store; a node has to do it
    /// for the one range, and this is that left undone (D-066).
    // PROPOSED(D-083): the replica a switch builds replaces the one it replaced.
    InstallKeepsTheOldCore,
    /// `InstallSnapshot` and its response admitted to the node's byte-bounded inbox
    /// instead of diverted to the `snapshot` task: the chunk costs a heartbeat its
    /// place under the bound ([`carries_data`]) and then vanishes, because
    /// `Raft::on_message`'s arm for it is empty — the core is told the server routed it
    /// away, which the one-group server's `net` loop does and the node's did not. The
    /// node as it stood, and the hole filed as **issue #96**.
    ///
    /// [`carries_data`]: crate::inbox::carries_data
    // PROPOSED(D-083): the `net` task diverts snapshot chunks before the inbox.
    ChunksToTheInbox,
    /// A take checkpoints the whole engine directory rather than the range's own key
    /// intervals, as the one-group take does (`Engine::checkpoint`, snapshot.rs:688).
    /// Every range's take then carries every *other* range's keys, and installing one
    /// on a follower writes three ranges' state it was never sent (D-066, D-068).
    ///
    /// With one range on the node the whole engine *is* that range's spans, so this
    /// variant and the correct take produce the same bytes: it is a mutation only a
    /// node of several ranges can be wrong about.
    // PROPOSED(D-083): a take checkpoints the range's spans, not the node's store.
    TakeCheckpointsTheWholeNode,
    /// The hold across a live install is taken on every range the node hosts rather
    /// than on the one installing, which is the node reaching for the incarnation a
    /// server ends. One range's install then stops every other range on the node for
    /// the length of a stream's switch — the exact cost SHARD.md §11, storage 5 says a
    /// live install exists to avoid.
    ///
    /// With one range it is the correct hold exactly, and nothing can tell them apart.
    // PROPOSED(D-083): the hold is one range's.
    InstallHoldsEveryRange,
    /// A completed install clears every staging directory under the engine directory
    /// rather than the one its own (range, sender) assembled in. Another range's
    /// half-assembled stream is destroyed by a neighbour's install, and its sender is
    /// never told, so it streams the rest of a snapshot into a directory that no longer
    /// holds its first bytes (D-075's keys, undone at the moment they matter).
    ///
    /// With one range and one sender there is only ever one staging directory, and
    /// clearing "every" one is clearing the right one.
    // PROPOSED(D-083): an install clears its own assembly's directory and no other.
    InstallSweepsEveryStaging,
    /// A stream's acknowledgement stepped into **every** core on the node rather than
    /// into the one range's.
    ///
    /// `Input::SnapshotAcked { to }` names the follower and not the range, which is
    /// complete information for a server with one core and incomplete for a node with
    /// four. Under this variant one range's chunks set `stream_acked` on every range's
    /// progress for that follower, so a *refused* follower keeps counting for check
    /// quorum on ranges whose stream was never opened, and D-049's rule — a refused
    /// follower counts only while its re-seed stream progresses (core.rs:1607-1613) —
    /// is silently void while every test stays green. **Issue #103.**
    ///
    /// With one range on the node, every core *is* the range's core, and the fan-out
    /// and the correct route are the same route.
    // PROPOSED(D-083): a stream's answers are stepped into the stream's range alone.
    SnapshotAckToEveryCore,
    /// A take copies the range's Raft state and **drops its user keys**, so the
    /// install's switch removes the receiver's user keys and puts nothing in their
    /// place: total, silent state-machine loss on every range installed from it.
    ///
    /// A range lives in two key intervals (D-066) and the take has to carry both. This
    /// carries one. Every event a correct install emits, it emits — the stream flows,
    /// the switch is made, the replica is created — which is why a check that counts
    /// events cannot see it and one that reads the installed state can.
    // PROPOSED(D-083): what an install installed is read back and traced.
    TakeSkipsTheUserKeys,
    /// A take copies the **whole** Raft interval, the log purpose included, as the
    /// one-group take does. The live install then puts the *leader's* log keys into
    /// the receiver's store, and nothing tombstones them back out: the node's repair
    /// carries no log tombstones precisely because the take carries no log keys
    /// (D-083's first departure from D-082). The two halves of that argument have to
    /// agree, and this is what catches them disagreeing.
    // PROPOSED(D-083): the stream carries no log key, so the repair tombstones none.
    TakeStreamsTheLogToo,
    /// The host is asked what a local input wants of its core **before** the node has
    /// checked whether that range is held.
    ///
    /// `Host::local_core` is not a pure question: a live install's repair is built in
    /// its `Ready` arm and handed to the `snapshot` task there. Asked first and held
    /// afterwards, a stream whose `Ready` arrives while its own range's persist is
    /// still outstanding builds a repair and lets the switch carrying it proceed
    /// against a write in flight — which is the one thing the hold exists to prevent.
    // PROPOSED(D-083): a live install holds one range and replaces its replica.
    AsksTheHostBeforeTheHold,
    /// Each replica's durable refused mark written in a batch that is **not synced**
    /// (D-067). Everything else is the correct node's: the same two keys, the same
    /// `RaftReseeded` when the write returns, and the same silence until the install.
    ///
    /// What a crash keeps of an unsynced write is the disk's draw, so the mark is
    /// there on some seeds and gone on others; where it is gone the replica opens
    /// fresh — term 0, no vote, incarnation 1 — and votes from then on, which is
    /// D-035's hole reopened. Its standard is therefore *rate* and not every seed,
    /// as `RemovalNotDurable`'s is (§10, a variant of a later stage), and the re-seed
    /// shape's arm crashes on the mark's own trace event so that nothing else has
    /// synced the new engine's log by then.
    // PROPOSED(D-081): the re-seed shape's variant, approved as D-067.
    ReseedMarkNotSynced,
    /// The highest index handed to the `apply` task left at zero when a core is put
    /// on the node at its start, rather than started where the replica's own applied
    /// index stands (`Cores::insert`).
    ///
    /// It is D-083's watermark bug one moment earlier: that one left the watermark
    /// where the *replaced* replica stood across a live install, this one never sets
    /// it at all. A node whose replica's log was compacted past its applied index —
    /// every replica a snapshot has filled — then names, in its first `Apply` after a
    /// restart, indices the core no longer holds, and the node fails that range and
    /// stops. Where the log does still hold them the state machine simply does its
    /// whole life's work again.
    // PROPOSED(D-081): the applied watermark starts where the replica does.
    RestartAppliesFromZero,
    /// A follower's compaction record ([`SnapshotAction::Record`]) asked for by a core
    /// and queued nowhere: the node as it stood, where `Host::snapshot` counted the
    /// action and `install::job_of` answered `None` for it.
    ///
    /// The core sets `take_pending` when it asks and clears it when it is told the
    /// record was written, so a record that goes nowhere leaves that core asking for
    /// nothing ever again. It never compacts, which is the whole of D-065 undone and
    /// the follower-log bound with it; and a replica that has never taken a snapshot
    /// cannot stream one when it takes office, so a re-seed toward a range whose new
    /// leader had been a follower waits forever.
    ///
    /// [`SnapshotAction::Record`]: ananke_raft::core::SnapshotAction::Record
    // PROPOSED(D-081): a follower's compaction record reaches the `apply` task.
    RecordNeverQueued,
    /// A cap-wait answered with the same `Restart` a changed identity gets, which is
    /// the one answer the node had for both before D-090.
    ///
    /// The sender cannot tell the two apart, so RAFT.md:210-212's restart bound counts
    /// a stream that is merely waiting its turn: at the third ask its leader declares
    /// unusable a checkpoint nothing was ever wrong with, throws the stream away and
    /// asks for a fresh take. A node's receive cap sits below its range count on
    /// purpose (D-075; §12's re-seed shape), so this is not an edge — it is what every
    /// range over the cap gets.
    ///
    /// **It needs more than one range to be wrong about.** A cap-wait between ranges is
    /// the situation, and a node of one range never reaches it: the only sender its cap
    /// could be contended by is a stale leader of that same range, whose chunk a
    /// higher-term assembly displaces ([`Snapshots::superseded`]) and whose term the
    /// receiving store refuses outright. With one range and one slot the correct answer
    /// and this one are the same answer, because neither is ever sent.
    ///
    /// [`Snapshots::superseded`]: crate::snapshot::Snapshots
    // PROPOSED(D-090): a cap-wait is answered as a wait, not as a start-over.
    CapWaitIsAStartOver,
    /// A stream's restarts not counted: the node as it stood, with RAFT.md:210-212's
    /// bound absent.
    ///
    /// Neither bound is then reachable for a stream that keeps being told to start
    /// over — the restart bound because nothing counts, and the resend bound because
    /// every restart resets it — so a receiver that answers `Restart` forever is
    /// answered forever. That is the unbounded loop D-083 recorded: a run that told
    /// senders to start over 669 times and one that told them none differed in nothing
    /// a check could read.
    // PROPOSED(D-090): the node honours RAFT.md's restart bound.
    RestartsNotCounted,
    /// Q14's grouped applies built wrong: the `apply` task holds every range's ready
    /// job until it holds one of each range it has seen, and then runs the group, so
    /// a range that goes quiet — no leader, no writes — stalls the node's other
    /// ranges' applies behind it. SHARD.md §4 and §12 build grouping only if the
    /// measured apply lag asks for it, and never by waiting: this is the shape that
    /// measurement exists to rule out, and what `sim/folds.rs`'s apply-lag and
    /// cross-range hold folds trip on.
    // PROPOSED(D-095): the variant the apply-lag and cross-range hold folds trip on.
    ApplyWaitsForEveryRange,
    /// A node takes a fresh store for a bootstrap: whether or not configuration
    /// names it among range 0's replicas, a node whose store holds no digest writes
    /// the initial state, and, lacking the bootstrap list it was not given, takes the
    /// address book for every range's voters. Two clusters then bootstrap where
    /// configuration named one, and check 7 reads the disagreement off the replicas'
    /// creations (SHARD.md §2, §8; PROPOSED D-096).
    AnyFreshNodeBootstraps,
}

impl NodeVariant {
    /// Every variant, in order: what a check that runs them all iterates.
    pub const BUGS: &'static [NodeVariant] = &[
        NodeVariant::DeferredFlushedEarly,
        NodeVariant::StepWhilePersisting,
        NodeVariant::CollapseHeldTicks,
        NodeVariant::PersistsOneAtATime,
        NodeVariant::HeldNotCounted,
        NodeVariant::HeldLocalDropped,
        NodeVariant::PersistsNotArmed,
        NodeVariant::AppliedNotAdvanced,
        NodeVariant::TakeToSnapshotTask,
        NodeVariant::SharedStagingDir,
        NodeVariant::OneAssemblyPerNode,
        NodeVariant::VersionDirWithoutRange,
        NodeVariant::SweepAcrossRanges,
        NodeVariant::CapStreamsSent,
        NodeVariant::ChunksInBatchFrames,
        NodeVariant::InstallWithoutRepair,
        NodeVariant::AdoptedOnRangeInstall,
        NodeVariant::StagingByRangeAlone,
        NodeVariant::SlotReservedForWaiter,
        NodeVariant::CompleteOnRestart,
        NodeVariant::InstallWrongRangesSpans,
        NodeVariant::AdmitsAnUnhostedRange,
        NodeVariant::AssemblyHeldForDepartedSender,
        NodeVariant::InstallsADuplicateLastChunk,
        NodeVariant::RefuseOneRangeOnly,
        NodeVariant::ReseedIntoRefusedDir,
        NodeVariant::ReuseLostGeneration,
        NodeVariant::OpenNewestEvenIfLost,
        NodeVariant::ServeBeforeRefusedMark,
        NodeVariant::IncarnationPerRangeStream,
        NodeVariant::StepWhileInstalling,
        NodeVariant::InstallKeepsTheOldCore,
        NodeVariant::ChunksToTheInbox,
        NodeVariant::TakeCheckpointsTheWholeNode,
        NodeVariant::InstallHoldsEveryRange,
        NodeVariant::InstallSweepsEveryStaging,
        NodeVariant::SnapshotAckToEveryCore,
        NodeVariant::TakeSkipsTheUserKeys,
        NodeVariant::TakeStreamsTheLogToo,
        NodeVariant::AsksTheHostBeforeTheHold,
        NodeVariant::ReseedMarkNotSynced,
        NodeVariant::RestartAppliesFromZero,
        NodeVariant::RecordNeverQueued,
        NodeVariant::CapWaitIsAStartOver,
        NodeVariant::RestartsNotCounted,
        NodeVariant::RefusedReadLeft,
        NodeVariant::OneSeedForEveryCore,
        NodeVariant::ApplyWaitsForEveryRange,
        NodeVariant::AnyFreshNodeBootstraps,
    ];

    /// Q15's whole-node refusal and re-seed, in order: the six ways to get a node's
    /// refusal wrong (SHARD.md §11, storage 8; D-077). Four of the six are mutations a
    /// single-range world could not catch at all — with one range on a node, refusing
    /// only that range *is* refusing the node, and a per-range incarnation stream is
    /// the node's own generator drawn once — and the two about directories need a node
    /// refused twice in a run, which one range reaches no sooner but which no check
    /// before this slice asked of any node.
    pub const RESEED: &'static [NodeVariant] = &[
        NodeVariant::RefuseOneRangeOnly,
        NodeVariant::ReseedIntoRefusedDir,
        NodeVariant::ReuseLostGeneration,
        NodeVariant::OpenNewestEvenIfLost,
        NodeVariant::ServeBeforeRefusedMark,
        NodeVariant::IncarnationPerRangeStream,
    ];

    /// The `snapshot` task's own, in order: the fifteen ways to get a snapshot keyed by
    /// range and follower wrong (SHARD.md §11, raft 14; D-066). Nine of the fifteen
    /// are mutations a single-range, single-follower world could not catch at all:
    /// with one range and one stream, a shared staging directory, a staging directory
    /// keyed by range alone, one assembly, a version name without a range, a sweep
    /// across ranges, a cap of one stream sent and a slot reserved for a waiter are
    /// each indistinguishable from the correct node, and so are an install carrying
    /// the node's first hosted range's spans and an assembly held for a sender its
    /// range has superseded.
    pub const SNAPSHOT: &'static [NodeVariant] = &[
        NodeVariant::SharedStagingDir,
        NodeVariant::OneAssemblyPerNode,
        NodeVariant::VersionDirWithoutRange,
        NodeVariant::SweepAcrossRanges,
        NodeVariant::CapStreamsSent,
        NodeVariant::ChunksInBatchFrames,
        NodeVariant::InstallWithoutRepair,
        NodeVariant::AdoptedOnRangeInstall,
        NodeVariant::StagingByRangeAlone,
        NodeVariant::SlotReservedForWaiter,
        NodeVariant::CompleteOnRestart,
        NodeVariant::InstallWrongRangesSpans,
        NodeVariant::AdmitsAnUnhostedRange,
        NodeVariant::AssemblyHeldForDepartedSender,
        NodeVariant::InstallsADuplicateLastChunk,
    ];

    /// The node's snapshot *wiring*: the ten ways to get the running of
    /// [`mod@crate::snapshot`] inside the node's server wrong, as against the fifteen
    /// ways to get its discipline wrong ([`SNAPSHOT`](Self::SNAPSHOT)). Three of the
    /// ten — [`InstallHoldsEveryRange`](Self::InstallHoldsEveryRange),
    /// [`InstallSweepsEveryStaging`](Self::InstallSweepsEveryStaging) and
    /// [`SnapshotAckToEveryCore`](Self::SnapshotAckToEveryCore), issue #103's — are the
    /// correct wiring exactly on a node of one range, and can only be caught where a
    /// node hosts several.
    ///
    /// [`TakeCheckpointsTheWholeNode`](Self::TakeCheckpointsTheWholeNode) needs more
    /// than one range as well, but on narrower ground: it is *not* the correct take on
    /// a node of one range, because `checkpoint_spans` leaves the log purpose out and a
    /// whole-engine checkpoint takes it in, so the two differ by the whole log however
    /// many ranges there are. What one range cannot do is tell it from
    /// [`TakeStreamsTheLogToo`](Self::TakeStreamsTheLogToo), whose check reads the log
    /// keys this one's does not (D-083's review, correction 5).
    // PROPOSED(D-083): the node's snapshot wiring.
    pub const WIRING: &'static [NodeVariant] = &[
        NodeVariant::StepWhileInstalling,
        NodeVariant::InstallKeepsTheOldCore,
        NodeVariant::ChunksToTheInbox,
        NodeVariant::TakeCheckpointsTheWholeNode,
        NodeVariant::InstallHoldsEveryRange,
        NodeVariant::InstallSweepsEveryStaging,
        NodeVariant::SnapshotAckToEveryCore,
        NodeVariant::TakeSkipsTheUserKeys,
        NodeVariant::TakeStreamsTheLogToo,
        NodeVariant::AsksTheHostBeforeTheHold,
    ];

    /// The directed re-seed shape's own, outside §10's count of range-layer variants:
    /// D-067's [`ReseedMarkNotSynced`](Self::ReseedMarkNotSynced), the one way to get
    /// Q15's *durability* wrong as against the six ways to get the refusal itself wrong
    /// ([`RESEED`](Self::RESEED)), and the four holes the shape found in the node when
    /// a refusal and the wiring first met on one tree — the applied index an install
    /// makes durable — no, that one is D-083's — the watermark a start begins at, a
    /// follower's compaction record, and the watermark a start begins at (D-081).
    ///
    /// `ReseedMarkNotSynced` is a mutation a single-range world could not catch
    /// either, for a reason of its own: the shape crashes the node on one replica's
    /// mark, and what the *other three* replicas restate afterwards is the evidence.
    /// With one range there is one mark, the crash is on the only replica there is,
    /// and a node that lost it has nothing left to compare it against.
    // PROPOSED(D-081): the re-seed shape's variant, approved as D-067.
    pub const SHAPE: &'static [NodeVariant] = &[
        NodeVariant::ReseedMarkNotSynced,
        NodeVariant::RestartAppliesFromZero,
        NodeVariant::RecordNeverQueued,
    ];

    /// RAFT.md:209-212's bounds on a stream, and the answer that keeps the restart bound
    /// from counting the wrong thing: the two ways to get D-090 wrong.
    ///
    /// They are neither the `snapshot` task's discipline ([`SNAPSHOT`](Self::SNAPSHOT))
    /// nor its running inside the node ([`WIRING`](Self::WIRING)) but the *sender's*
    /// bookkeeping over a stream's answers, which had no bound at all until D-090.
    /// [`CapWaitIsAStartOver`](Self::CapWaitIsAStartOver) is one a node of a single range
    /// could not be wrong about; [`RestartsNotCounted`](Self::RestartsNotCounted) is
    /// wrong with one range too.
    // PROPOSED(D-090): the node honours RAFT.md's restart bound, and a cap-wait is not
    // one of the asks it counts.
    pub const BOUNDS: &'static [NodeVariant] = &[
        NodeVariant::CapWaitIsAStartOver,
        NodeVariant::RestartsNotCounted,
    ];

    /// The bit this variant takes in a [`NodeVariants`].
    const fn bit(self) -> u64 {
        match self {
            NodeVariant::DeferredFlushedEarly => 1,
            NodeVariant::StepWhilePersisting => 1 << 1,
            NodeVariant::CollapseHeldTicks => 1 << 2,
            NodeVariant::PersistsOneAtATime => 1 << 3,
            NodeVariant::HeldNotCounted => 1 << 4,
            NodeVariant::HeldLocalDropped => 1 << 19,
            NodeVariant::PersistsNotArmed => 1 << 5,
            NodeVariant::AppliedNotAdvanced => 1 << 6,
            NodeVariant::TakeToSnapshotTask => 1 << 7,
            NodeVariant::SharedStagingDir => 1 << 8,
            NodeVariant::OneAssemblyPerNode => 1 << 9,
            NodeVariant::VersionDirWithoutRange => 1 << 10,
            NodeVariant::SweepAcrossRanges => 1 << 11,
            NodeVariant::CapStreamsSent => 1 << 12,
            NodeVariant::ChunksInBatchFrames => 1 << 13,
            NodeVariant::InstallWithoutRepair => 1 << 14,
            NodeVariant::AdoptedOnRangeInstall => 1 << 15,
            NodeVariant::StagingByRangeAlone => 1 << 16,
            NodeVariant::SlotReservedForWaiter => 1 << 17,
            NodeVariant::CompleteOnRestart => 1 << 18,
            NodeVariant::RefuseOneRangeOnly => 1 << 20,
            NodeVariant::ReseedIntoRefusedDir => 1 << 21,
            NodeVariant::ReuseLostGeneration => 1 << 22,
            NodeVariant::OpenNewestEvenIfLost => 1 << 23,
            NodeVariant::ServeBeforeRefusedMark => 1 << 24,
            NodeVariant::IncarnationPerRangeStream => 1 << 25,
            // Bits 20 to 25 are D-077's six, which `main` took while the snapshot
            // review's slice was open; before that merge the review's four held 20 to
            // 23. They take 26 to 29 instead, and D-083's ten follow at 30 to 39, so no
            // two variants share a bit. D-081's three for the directed re-seed shape
            // follow at 40 to 42. Forty-three variants no longer fit a `u32`, which is
            // why [`NodeVariants`] is a `u64`, as `ananke_raft`'s set was widened for
            // the same reason; D-090's two at 43 and 44 and D-076's review's two at 45 and 46;
            // seventeen bits are left.
            NodeVariant::InstallWrongRangesSpans => 1 << 26,
            NodeVariant::AdmitsAnUnhostedRange => 1 << 27,
            NodeVariant::AssemblyHeldForDepartedSender => 1 << 28,
            NodeVariant::InstallsADuplicateLastChunk => 1 << 29,
            NodeVariant::StepWhileInstalling => 1 << 30,
            NodeVariant::InstallKeepsTheOldCore => 1 << 31,
            NodeVariant::ChunksToTheInbox => 1 << 32,
            NodeVariant::TakeCheckpointsTheWholeNode => 1 << 33,
            NodeVariant::InstallHoldsEveryRange => 1 << 34,
            NodeVariant::InstallSweepsEveryStaging => 1 << 35,
            NodeVariant::SnapshotAckToEveryCore => 1 << 36,
            NodeVariant::TakeSkipsTheUserKeys => 1 << 37,
            NodeVariant::TakeStreamsTheLogToo => 1 << 38,
            NodeVariant::AsksTheHostBeforeTheHold => 1 << 39,
            NodeVariant::ReseedMarkNotSynced => 1 << 40,
            NodeVariant::RestartAppliesFromZero => 1 << 41,
            NodeVariant::RecordNeverQueued => 1 << 42,
            // D-090's two: the restart bound RAFT.md states and the cap-wait it must
            // not count. Nineteen bits are left.
            NodeVariant::CapWaitIsAStartOver => 1 << 43,
            NodeVariant::RestartsNotCounted => 1 << 44,
            // D-076's review's two: a refused read's registration left behind, and one
            // seed for every core. Seventeen bits are left.
            NodeVariant::RefusedReadLeft => 1 << 45,
            NodeVariant::OneSeedForEveryCore => 1 << 46,
            // PROPOSED(D-095): the apply task that waits for every range. Sixteen bits are
            // left.
            NodeVariant::ApplyWaitsForEveryRange => 1 << 47,
            // PROPOSED(D-096): a fresh store taken for a bootstrap. Fifteen bits are left.
            NodeVariant::AnyFreshNodeBootstraps => 1 << 48,
        }
    }

    /// The name a report prints.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            NodeVariant::DeferredFlushedEarly => "DeferredFlushedEarly",
            NodeVariant::StepWhilePersisting => "StepWhilePersisting",
            NodeVariant::CollapseHeldTicks => "CollapseHeldTicks",
            NodeVariant::PersistsOneAtATime => "PersistsOneAtATime",
            NodeVariant::HeldNotCounted => "HeldNotCounted",
            NodeVariant::HeldLocalDropped => "HeldLocalDropped",
            NodeVariant::PersistsNotArmed => "PersistsNotArmed",
            NodeVariant::AppliedNotAdvanced => "AppliedNotAdvanced",
            NodeVariant::TakeToSnapshotTask => "TakeToSnapshotTask",
            NodeVariant::SharedStagingDir => "SharedStagingDir",
            NodeVariant::OneAssemblyPerNode => "OneAssemblyPerNode",
            NodeVariant::VersionDirWithoutRange => "VersionDirWithoutRange",
            NodeVariant::SweepAcrossRanges => "SweepAcrossRanges",
            NodeVariant::CapStreamsSent => "CapStreamsSent",
            NodeVariant::ChunksInBatchFrames => "ChunksInBatchFrames",
            NodeVariant::InstallWithoutRepair => "InstallWithoutRepair",
            NodeVariant::AdoptedOnRangeInstall => "AdoptedOnRangeInstall",
            NodeVariant::StagingByRangeAlone => "StagingByRangeAlone",
            NodeVariant::SlotReservedForWaiter => "SlotReservedForWaiter",
            NodeVariant::CompleteOnRestart => "CompleteOnRestart",
            NodeVariant::RefuseOneRangeOnly => "RefuseOneRangeOnly",
            NodeVariant::ReseedIntoRefusedDir => "ReseedIntoRefusedDir",
            NodeVariant::ReuseLostGeneration => "ReuseLostGeneration",
            NodeVariant::OpenNewestEvenIfLost => "OpenNewestEvenIfLost",
            NodeVariant::ServeBeforeRefusedMark => "ServeBeforeRefusedMark",
            NodeVariant::IncarnationPerRangeStream => "IncarnationPerRangeStream",
            NodeVariant::InstallWrongRangesSpans => "InstallWrongRangesSpans",
            NodeVariant::AdmitsAnUnhostedRange => "AdmitsAnUnhostedRange",
            NodeVariant::AssemblyHeldForDepartedSender => "AssemblyHeldForDepartedSender",
            NodeVariant::InstallsADuplicateLastChunk => "InstallsADuplicateLastChunk",
            NodeVariant::StepWhileInstalling => "StepWhileInstalling",
            NodeVariant::InstallKeepsTheOldCore => "InstallKeepsTheOldCore",
            NodeVariant::ChunksToTheInbox => "ChunksToTheInbox",
            NodeVariant::TakeCheckpointsTheWholeNode => "TakeCheckpointsTheWholeNode",
            NodeVariant::InstallHoldsEveryRange => "InstallHoldsEveryRange",
            NodeVariant::InstallSweepsEveryStaging => "InstallSweepsEveryStaging",
            NodeVariant::SnapshotAckToEveryCore => "SnapshotAckToEveryCore",
            NodeVariant::TakeSkipsTheUserKeys => "TakeSkipsTheUserKeys",
            NodeVariant::TakeStreamsTheLogToo => "TakeStreamsTheLogToo",
            NodeVariant::AsksTheHostBeforeTheHold => "AsksTheHostBeforeTheHold",
            NodeVariant::ReseedMarkNotSynced => "ReseedMarkNotSynced",
            NodeVariant::RestartAppliesFromZero => "RestartAppliesFromZero",
            NodeVariant::RecordNeverQueued => "RecordNeverQueued",
            NodeVariant::CapWaitIsAStartOver => "CapWaitIsAStartOver",
            NodeVariant::RestartsNotCounted => "RestartsNotCounted",
            NodeVariant::RefusedReadLeft => "RefusedReadLeft",
            NodeVariant::OneSeedForEveryCore => "OneSeedForEveryCore",
            NodeVariant::ApplyWaitsForEveryRange => "ApplyWaitsForEveryRange",
            NodeVariant::AnyFreshNodeBootstraps => "AnyFreshNodeBootstraps",
        }
    }
}

impl fmt::Display for NodeVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A set of [`NodeVariant`]s, shaped like [`ananke_raft::core::Variants`] so the two
/// read alike where a scenario configures both. The word is 64 bits and Raft's is 32:
/// the node's variants outgrew a `u32` when the snapshot wiring's ten, Q15's six and
/// the re-seed shape's three met on one tree, and nothing but this module reads the
/// bits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeVariants(u64);

impl NodeVariants {
    /// The correct node: no variant.
    #[must_use]
    pub const fn correct() -> Self {
        Self(0)
    }

    /// The node with exactly these variants.
    #[must_use]
    pub fn of(variants: &[NodeVariant]) -> Self {
        let mut bits = 0;
        let mut i = 0;
        while i < variants.len() {
            bits |= variants[i].bit();
            i += 1;
        }
        Self(bits)
    }

    /// Whether `variant` is in the set.
    #[must_use]
    pub const fn contains(self, variant: NodeVariant) -> bool {
        self.0 & variant.bit() != 0
    }

    /// Whether this is the correct node.
    #[must_use]
    pub const fn is_correct(self) -> bool {
        self.0 == 0
    }

    /// The set with `variant` added.
    #[must_use]
    pub const fn with(self, variant: NodeVariant) -> Self {
        Self(self.0 | variant.bit())
    }
}

impl fmt::Display for NodeVariants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_correct() {
            return f.write_str("correct");
        }
        let mut first = true;
        for variant in NodeVariant::BUGS {
            if self.contains(*variant) {
                if !first {
                    f.write_str("+")?;
                }
                first = false;
                f.write_str(variant.name())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_takes_a_bit_of_its_own() {
        let mut seen = 0u64;
        for variant in NodeVariant::BUGS {
            assert_eq!(seen & variant.bit(), 0, "{variant} shares a bit");
            seen |= variant.bit();
        }
        // Twenty of the round's and the snapshot task's, the snapshot review's four,
        // D-077's six for Q15's whole-node refusal and re-seed, D-083's ten for the
        // snapshot wiring, D-081's three for the directed re-seed shape, D-090's two for a
        // stream's bounds, D-076's review's two, D-095's one for the `apply` task that
        // waits for every range, and D-096's one for a fresh store taken for a bootstrap.
        assert_eq!(NodeVariant::BUGS.len(), 49);
        for variant in NodeVariant::SNAPSHOT
            .iter()
            .chain(NodeVariant::WIRING)
            .chain(NodeVariant::RESEED)
            .chain(NodeVariant::SHAPE)
            .chain(NodeVariant::BOUNDS)
        {
            assert!(
                NodeVariant::BUGS.contains(variant),
                "{variant} is not in BUGS"
            );
        }
        assert_eq!(NodeVariant::SNAPSHOT.len(), 15);
        assert_eq!(NodeVariant::WIRING.len(), 10);
        assert_eq!(NodeVariant::RESEED.len(), 6);
        assert_eq!(NodeVariant::SHAPE.len(), 3);
        assert_eq!(NodeVariant::BOUNDS.len(), 2);
        // The discipline, the wiring, the re-seed, the shape and the bounds are pairwise
        // disjoint: a
        // variant is a way to get the `snapshot` task's keys, caps and frames wrong, a
        // way to get its running inside the node wrong, a way to get Q15's whole-node
        // refusal wrong, one of the shape's own, or a way to get a stream's bounds wrong,
        // and never two of them.
        let sets = [
            ("SNAPSHOT", NodeVariant::SNAPSHOT),
            ("WIRING", NodeVariant::WIRING),
            ("RESEED", NodeVariant::RESEED),
            ("SHAPE", NodeVariant::SHAPE),
            ("BOUNDS", NodeVariant::BOUNDS),
        ];
        for (i, (left, one)) in sets.iter().enumerate() {
            for (right, other) in &sets[i + 1..] {
                for variant in *one {
                    assert!(
                        !other.contains(variant),
                        "{variant} is in both {left} and {right}"
                    );
                }
            }
        }
        // D-090's two are a fourth kind: the sender's bookkeeping over a stream's
        // answers, which is none of the three above.
        for variant in NodeVariant::BOUNDS {
            assert!(
                !NodeVariant::SNAPSHOT.contains(variant)
                    && !NodeVariant::WIRING.contains(variant)
                    && !NodeVariant::RESEED.contains(variant),
                "{variant} is in two sets"
            );
        }
    }

    #[test]
    fn a_set_holds_what_it_was_given_and_nothing_else() {
        let set = NodeVariants::of(&[
            NodeVariant::CollapseHeldTicks,
            NodeVariant::PersistsOneAtATime,
        ]);
        assert!(set.contains(NodeVariant::CollapseHeldTicks));
        assert!(set.contains(NodeVariant::PersistsOneAtATime));
        assert!(!set.contains(NodeVariant::DeferredFlushedEarly));
        assert!(!set.is_correct());
        assert!(NodeVariants::correct().is_correct());
        assert_eq!(set.to_string(), "CollapseHeldTicks+PersistsOneAtATime");
        assert_eq!(NodeVariants::correct().to_string(), "correct");
        assert_eq!(
            NodeVariants::correct()
                .with(NodeVariant::CollapseHeldTicks)
                .with(NodeVariant::PersistsOneAtATime),
            set
        );
    }
}
