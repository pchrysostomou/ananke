//! A range's id (SHARD.md §1, §13 Q10).

/// The id of a range: the 8 bytes every message on the wire, in the inbox and in the
/// trace carries (Q10, "range ids are a fixed 8 bytes").
///
/// A separate type from [`ananke_raft::ServerId`] on purpose: both are `u64` and the
/// wire carries both, so a range where a server belongs — or the other way about — is
/// the mistake this layer is most exposed to, and the type system is the cheapest place
/// to catch it. The trace and `RaftConfig::range` keep the bare `u64` (D-069), so the
/// field is public and the conversion is [`RangeId::get`] in one direction and the
/// tuple constructor in the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RangeId(pub u64);

impl RangeId {
    /// The id as the `u64` the trace and the store's key layout use (D-060, D-069).
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RangeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_range_id_is_a_u64_that_prints_as_a_range() {
        assert_eq!(RangeId(7).get(), 7);
        assert_eq!(RangeId(7).to_string(), "r7");
        assert!(RangeId(2) < RangeId(3));
    }
}
