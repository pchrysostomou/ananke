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
        NodeVariant::RefusedReadLeft,
        NodeVariant::OneSeedForEveryCore,
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

    /// The bit this variant takes in a [`NodeVariants`].
    const fn bit(self) -> u32 {
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
            // Bits 20 to 25 are D-077's six, which `main` took while this slice was
            // open; before that merge the snapshot review's four held 20 to 23. They
            // take the next free bits instead, so no two variants share one.
            NodeVariant::InstallWrongRangesSpans => 1 << 26,
            NodeVariant::AdmitsAnUnhostedRange => 1 << 27,
            NodeVariant::AssemblyHeldForDepartedSender => 1 << 28,
            NodeVariant::InstallsADuplicateLastChunk => 1 << 29,
            // D-076's review adds the last two. Thirty-two variants take bits 0 to
            // 31 and the `u32` is full: the next slice to add one must widen
            // `NodeVariants` to a `u64`, as `ananke_raft`'s set was widened for the
            // same reason, rather than reusing a bit. `variants_have_distinct_bits`
            // is what says so the day one is reused.
            NodeVariant::RefusedReadLeft => 1 << 30,
            NodeVariant::OneSeedForEveryCore => 1 << 31,
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
            NodeVariant::RefusedReadLeft => "RefusedReadLeft",
            NodeVariant::OneSeedForEveryCore => "OneSeedForEveryCore",
        }
    }
}

impl fmt::Display for NodeVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A set of [`NodeVariant`]s, shaped like [`ananke_raft::core::Variants`] so the two
/// read alike where a scenario configures both.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeVariants(u32);

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
        let mut seen = 0u32;
        for variant in NodeVariant::BUGS {
            assert_eq!(seen & variant.bit(), 0, "{variant} shares a bit");
            seen |= variant.bit();
        }
        // Twenty of the round's and the snapshot task's, the snapshot review's four,
        // D-077's six for Q15's whole-node refusal and re-seed, and D-076's review's
        // two. Thirty-two is every bit of the `u32` (see `NodeVariant::bit`).
        assert_eq!(NodeVariant::BUGS.len(), 32);
        for variant in NodeVariant::SNAPSHOT {
            assert!(
                NodeVariant::BUGS.contains(variant),
                "{variant} is not in BUGS"
            );
        }
        assert_eq!(NodeVariant::SNAPSHOT.len(), 15);
        for variant in NodeVariant::RESEED {
            assert!(
                NodeVariant::BUGS.contains(variant),
                "{variant} is not in BUGS"
            );
            assert!(
                !NodeVariant::SNAPSHOT.contains(variant),
                "{variant} is in two sets"
            );
        }
        assert_eq!(NodeVariant::RESEED.len(), 6);
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
