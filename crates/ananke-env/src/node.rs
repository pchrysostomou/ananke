//! [`NodeId`]: which simulated node a task, socket or event belongs to.

use std::fmt;

/// Identifies a node within one simulation. `Sim::add_node` allocates these
/// sequentially from 1: moirae traces number nodes `1..=nodes` (SPEC §1.5), and using
/// the same numbering here means the bridge carries ids through unchanged, with no
/// mapping function to get wrong. Zero is never a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u32);

impl NodeId {
    /// Wraps a raw id.
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `pad` honours width and alignment, so trace lines stay column-aligned.
        f.pad(&format!("n{}", self.0))
    }
}
