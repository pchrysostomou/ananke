//! The node's known-buggy variants (CLAUDE.md's pair rule).
//!
//! [`ananke_raft::Variant`] is the Raft core's set: a protocol bug beside the correct
//! protocol, caught by the sweeps. These are the *node's*: each is a plausible way to
//! get Q41's round wrong, built beside the correct round and caught by the same check
//! that asserts the correct round's order (SHARD.md §4). They are not Raft bugs — the
//! core is untouched — so they are a set of their own rather than more of the core's,
//! and they do not appear in [`ananke_raft::Variant::BUGS`] or in §10's count.
//!
//! The node is not yet under the sweeps: slice 4 of Stage B puts it there. Until then
//! each variant is caught by a deterministic check in this crate, which is the pair
//! rule's requirement — the buggy variant is *seen to fail* the check the correct code
//! passes — without a rate to measure (D-061 asks a tier of a *sweep's* assertion).

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
}

impl NodeVariant {
    /// Every variant, in order: what a check that runs them all iterates.
    pub const BUGS: &'static [NodeVariant] = &[
        NodeVariant::DeferredFlushedEarly,
        NodeVariant::StepWhilePersisting,
        NodeVariant::CollapseHeldTicks,
        NodeVariant::PersistsOneAtATime,
        NodeVariant::HeldNotCounted,
    ];

    /// The bit this variant takes in a [`NodeVariants`].
    const fn bit(self) -> u32 {
        match self {
            NodeVariant::DeferredFlushedEarly => 1,
            NodeVariant::StepWhilePersisting => 1 << 1,
            NodeVariant::CollapseHeldTicks => 1 << 2,
            NodeVariant::PersistsOneAtATime => 1 << 3,
            NodeVariant::HeldNotCounted => 1 << 4,
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
        assert_eq!(NodeVariant::BUGS.len(), 5);
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
