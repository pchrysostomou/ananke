//! The range layer's known-buggy variants (SHARD.md §10, §13 Q37; CLAUDE.md's pair rule).
//!
//! A node is configured with three sets of known bugs, one per layer, and each is read
//! only by the layer whose rule its members break:
//!
//! - [`ananke_raft::core::Variants`], the Raft core's, which Phase 3 leaves unchanged:
//!   Phase 2's variants, re-asserted on the node with the bits they had (Q37).
//! - [`NodeVariants`](crate::variant::NodeVariants), the node's (SHARD.md §4): Q41's
//!   round, the wire, the `snapshot` and `apply` tasks, Q15's whole-node refusal and
//!   the node's start. None of them is one of §10's.
//! - [`RangeVariants`], this module's: the range layer's, whose rules are §1 to §7's
//!   — descriptors, routing, the meta range, range ids, split, and in later stages
//!   merge, moves and the rebalancer. `ananke-raft` never names a range (Q40), so
//!   these could not live in its set, and they are not the node's.
//!
//! A member is one of two kinds, and [`RangeVariant::in_section_10`] says which by an
//! exhaustive match, so a variant added later does not compile until it has been
//! classified as well as given a bit:
//!
//! - **One of §10's twenty-four**, the variants SHARD.md tables, each breaking one
//!   rule with a reference, built by the stage §12 assigns it. Five are built:
//!   [`RangeVariant::TrustStaleDescriptor`], [`RangeVariant::ApplyIgnoresSpan`] and
//!   [`RangeVariant::ClientIgnoresMismatch`] (routing, PROPOSED D-097),
//!   [`RangeVariant::MetaOverwritesByArrival`] (the meta range, PROPOSED D-098) and
//!   [`RangeVariant::IdBlockResumed`] (range ids, PROPOSED D-099). The stale descriptor
//!   is both a server's trust and a client's: Q38 ships `TrustStaleDescriptor` and
//!   `ClientIgnoresMismatch` side by side.
//! - **A test instrument outside that count**: a variant built so that a check of
//!   §8 has something that trips it where no protocol mistake of §10 does — PROPOSED
//!   D-093's `SplitLeavesGap` for check 19's fold, and Stage D's variant for the
//!   re-add window assertion (§12, Stage D's question 1). None is built yet.
//!
//! The set is a `u64` where Raft's is a `u32` (PROPOSED D-105): twenty-four of §10's
//! and two instruments already proposed would leave a `u32` six bits for every
//! variant the stages' reviews add, and the node's own set, planned at twenty, reached
//! fifty-four and outgrew its `u32` inside Stage B. The bits reach no wire frame and
//! no trace event (D-045), so the numbering is free to change; and a variant past the
//! sixty-fourth bit is an overflow in `RangeVariant::bit`'s match, which does not
//! compile, so the width cannot be outgrown silently.
//!
//! Until D-105 the five sat in [`NodeVariants`](crate::variant::NodeVariants) at bits 49
//! to 53. Moving them moved no schedule and no trace: D-105 measures it.
// PROPOSED(D-105): the range layer's own variant set (Q37).

use std::fmt;

/// One way to get a rule of the range layer wrong (SHARD.md §10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum RangeVariant {
    /// A server takes the range a request names as proof that the key is in it: it
    /// checks the key against its own descriptor neither at receipt nor at a read's
    /// serving, and keeps the apply check (SHARD.md §3, §10; Q38). This is the owner's
    /// "client that trusts a stale descriptor", placed where trust can do harm: a
    /// client's cache is advisory by design and a stale one costs a round trip unless
    /// a server honours it. Check 9 catches the first read served for a key outside
    /// the serving range's span; the writes it proposes the apply check refuses.
    // PROPOSED(D-097)
    TrustStaleDescriptor,
    /// The apply check skipped: a command applies whatever its key, where §3's check
    /// is what makes a split take effect for proposals in flight and a
    /// `RangeMismatch` at apply definite. Check 9 catches the first applied write
    /// outside the span, check 10 a write by the wrong owner (SHARD.md §10).
    // PROPOSED(D-097)
    ApplyIgnoresSpan,
    /// The sweep's client resends to the range and generation it had, merging nothing
    /// of what a `RangeMismatch` carried (SHARD.md §3, §10; Q38). Check 17 catches
    /// the first resend, and the liveness bound a client that never converges.
    // PROPOSED(D-097)
    ClientIgnoresMismatch,
    /// The meta range stores each update as it arrives: every record an update
    /// overlaps is cut and replaced whatever its generation, where §1 keeps, per key,
    /// the descriptor of the highest generation. An older update that arrives after a
    /// newer one takes the newer's place, which check 16 sees at the first key whose
    /// named generation falls (SHARD.md §8, §10); `Fault::MetaReorder` on the sharded
    /// sweep is what makes one arrive so.
    // PROPOSED(D-098)
    MetaOverwritesByArrival,
    /// The node adopts, at its start, the block of range ids its lease record names,
    /// whatever run it was granted to, and takes ids from that block's first, where
    /// §5's second rule has a node adopt only a block granted to its current run:
    /// a restart abandons the rest of every block it held, and the ids the earlier
    /// run took from that block are taken twice. Check 18's range-id clause sees it
    /// at the first apply of a split whose right half a `RangeCreated` or `RangeSplit`
    /// named before (SHARD.md §8, §10); the sharded sweep's crash and restart of a
    /// node that took an id, followed by a split led from it, is what reaches it.
    // PROPOSED(D-099)
    IdBlockResumed,
}

impl RangeVariant {
    /// Every variant, in declaration order: what a check that runs them all iterates.
    pub const BUGS: &'static [RangeVariant] = &[
        RangeVariant::TrustStaleDescriptor,
        RangeVariant::ApplyIgnoresSpan,
        RangeVariant::ClientIgnoresMismatch,
        RangeVariant::MetaOverwritesByArrival,
        RangeVariant::IdBlockResumed,
    ];

    /// The bit this variant takes in a [`RangeVariants`]. The match is exhaustive on
    /// purpose, so a new variant does not compile until it has a bit, and a bit past
    /// the sixty-fourth is an overflow, which does not compile either.
    const fn bit(self) -> u64 {
        match self {
            // PROPOSED(D-097): §10's three routing variants.
            RangeVariant::TrustStaleDescriptor => 1,
            RangeVariant::ApplyIgnoresSpan => 1 << 1,
            RangeVariant::ClientIgnoresMismatch => 1 << 2,
            // PROPOSED(D-098): the meta range storing updates as they arrive.
            RangeVariant::MetaOverwritesByArrival => 1 << 3,
            // PROPOSED(D-099): a node resuming another run's block of ids.
            RangeVariant::IdBlockResumed => 1 << 4,
        }
    }

    /// Whether this variant is one of SHARD.md §10's twenty-four, rather than a test
    /// instrument outside that count. Exhaustive, so a variant added later is
    /// classified before it compiles.
    #[must_use]
    pub const fn in_section_10(self) -> bool {
        match self {
            RangeVariant::TrustStaleDescriptor
            | RangeVariant::ApplyIgnoresSpan
            | RangeVariant::ClientIgnoresMismatch
            | RangeVariant::MetaOverwritesByArrival
            | RangeVariant::IdBlockResumed => true,
        }
    }

    /// The name a report prints.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            RangeVariant::TrustStaleDescriptor => "TrustStaleDescriptor",
            RangeVariant::ApplyIgnoresSpan => "ApplyIgnoresSpan",
            RangeVariant::ClientIgnoresMismatch => "ClientIgnoresMismatch",
            RangeVariant::MetaOverwritesByArrival => "MetaOverwritesByArrival",
            RangeVariant::IdBlockResumed => "IdBlockResumed",
        }
    }
}

impl fmt::Display for RangeVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A set of [`RangeVariant`]s, shaped like
/// [`NodeVariants`](crate::variant::NodeVariants) and [`ananke_raft::core::Variants`]
/// so the three read alike where a scenario configures them. The empty set is the
/// correct range layer (D-045), and a set turns off exactly its members' fixes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RangeVariants(u64);

impl RangeVariants {
    /// The correct range layer: no variant.
    #[must_use]
    pub const fn correct() -> Self {
        Self(0)
    }

    /// The range layer with exactly these variants.
    #[must_use]
    pub const fn of(variants: &[RangeVariant]) -> Self {
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
    pub const fn contains(self, variant: RangeVariant) -> bool {
        self.0 & variant.bit() != 0
    }

    /// Whether this is the correct range layer.
    #[must_use]
    pub const fn is_correct(self) -> bool {
        self.0 == 0
    }

    /// The set with `variant` added.
    #[must_use]
    pub const fn with(self, variant: RangeVariant) -> Self {
        Self(self.0 | variant.bit())
    }
}

impl fmt::Display for RangeVariants {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_correct() {
            return f.write_str("correct");
        }
        let mut first = true;
        for variant in RangeVariant::BUGS {
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

    /// SHARD.md §10's table, row by row: the names a variant classified as one of
    /// §10's must carry, and an instrument must not.
    const SECTION_10: [&str; 24] = [
        "SplitNotAtomicWithDescriptor",
        "ApplyIgnoresSpan",
        "ReadCheckAtReceiptOnly",
        "TrustStaleDescriptor",
        "ClientIgnoresMismatch",
        "SnapshotOverlapsReplica",
        "UninitialisedReplicaVotes",
        "PlaceholderAcknowledges",
        "SplitCreatesRightOnNonVoter",
        "IdBlockResumed",
        "MergeDivergentReplicas",
        "MergeBeforeRightApplied",
        "MergeTakesLeftGeneration",
        "ServeAfterSubsume",
        "UnfreezeBeforeAbortCommitted",
        "MergeNotResumed",
        "ChangeWhileFrozen",
        "RemoveBeforeCaughtUp",
        "JointBeforeCaughtUp",
        "GcBeforeRemovalCommitted",
        "RemovalNotDurable",
        "MoveNeverRetried",
        "RebalancerIgnoresLeaders",
        "MetaOverwritesByArrival",
    ];

    #[test]
    fn every_variant_takes_a_bit_of_its_own_and_says_whether_it_is_one_of_section_10s() {
        let mut seen = 0u64;
        for variant in RangeVariant::BUGS {
            assert_eq!(seen & variant.bit(), 0, "{variant} shares a bit");
            seen |= variant.bit();
        }
        // D-097's three for routing, D-098's one for the meta range and D-099's one for
        // the range-id blocks, all of them §10's.
        assert_eq!(RangeVariant::BUGS.len(), 5);
        for variant in RangeVariant::BUGS {
            assert_eq!(
                variant.in_section_10(),
                SECTION_10.contains(&variant.name()),
                "{variant} is classified against SHARD.md §10's table"
            );
        }
        let counted = RangeVariant::BUGS
            .iter()
            .filter(|variant| variant.in_section_10())
            .count();
        assert!(counted <= SECTION_10.len());
        // No range variant is also a node variant: the two sets share no name.
        for variant in RangeVariant::BUGS {
            assert!(
                !crate::variant::NodeVariant::BUGS
                    .iter()
                    .any(|node| node.name() == variant.name()),
                "{variant} is in both sets"
            );
        }
    }

    #[test]
    fn a_set_holds_what_it_was_given_and_nothing_else() {
        let set = RangeVariants::of(&[
            RangeVariant::TrustStaleDescriptor,
            RangeVariant::ApplyIgnoresSpan,
        ]);
        assert!(set.contains(RangeVariant::TrustStaleDescriptor));
        assert!(set.contains(RangeVariant::ApplyIgnoresSpan));
        assert!(!set.contains(RangeVariant::ClientIgnoresMismatch));
        assert!(!set.is_correct());
        assert!(RangeVariants::correct().is_correct());
        assert_eq!(RangeVariants::default(), RangeVariants::correct());
        assert_eq!(set.to_string(), "TrustStaleDescriptor+ApplyIgnoresSpan");
        assert_eq!(RangeVariants::correct().to_string(), "correct");
        assert_eq!(
            RangeVariants::correct()
                .with(RangeVariant::TrustStaleDescriptor)
                .with(RangeVariant::ApplyIgnoresSpan),
            set
        );
    }
}
