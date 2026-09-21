//! The range layer of ananke (SHARD.md §4, §13 Q40): the node, where many Raft groups
//! share one socket, one ticker and one engine.
//!
//! Today's unit is a group: one server is one Raft group with a socket and a set of
//! tasks of its own (SHARD.md §4). The unit becomes a *node*, and everything a node
//! sends or receives has to say which range it is about. This crate is that layer;
//! [`ananke_raft`] keeps the core, the codec, the store, snapshots, refusal and
//! adoption, and does not depend on this crate. Q40's line is that it carries **no
//! range descriptor and no span**: it names a range where the trace needs one —
//! `RaftConfig::range`, and `range` on every `Raft*` event (D-069) — and that is a
//! `u64` label on a group, not knowledge of what keys the range holds. Where the
//! range's bounds, its splits and its placement live is this crate's side of the
//! boundary.
//!
//! This slice is the wire, and only the wire:
//!
//! - [`RangeId`], the 8 bytes every message on the wire carries (Q10).
//! - [`mod@frame`], the batch frame: one frame, several messages, each tagged with its
//!   range, wrapping [`ananke_raft::message`]'s codec unchanged. [`frame::studio`] is
//!   the moirae studio's view of one, which shows every message a frame carries.
//! - [`Outbox`], the per-peer outbox every send leaves through: one frame per peer per
//!   flush, cut round-robin over the peer's ranges under
//!   [`ananke_env::MAX_FRAME_LEN`], and bounded per peer in bytes.
//! - [`Inbox`], one per node, bounded in bytes, admitting in constant or logarithmic
//!   time (Q14): a message carrying entries or snapshot data makes room by dropping
//!   the noisiest (sender, range) pair's oldest heartbeat, and nothing is ever refused
//!   into an empty queue.
//!
//! On the wire it builds the node's tasks:
//!
//! - [`mod@round`], Q41's round: the order one `raft` task keeps over every core on
//!   the node — what leaves before the round's sync, what is submitted together, and
//!   what waits for a core's own persist. It is the discipline alone: no clock, no
//!   socket, no disk, so the order can be asserted without a simulation.
//! - [`mod@node`], the two tasks: [`node::Node::raft`], one task holding every core
//!   keyed by range on one ticker, and [`node::apply`], one task per node applying
//!   every range's jobs one at a time.
//! - [`mod@variant`], the node's known-buggy variants, each a plausible way to get
//!   the round wrong, built beside the correct round (CLAUDE.md's pair rule).
//!
//! The snapshot task, descriptors, split, merge and the rebalancer are each a later
//! slice's.

pub mod frame;
pub mod inbox;
pub mod node;
pub mod outbox;
pub mod range;
pub mod round;
pub mod variant;

pub use frame::{Decoded, Tagged, decode, encoded_len, studio};
pub use inbox::{Admission, Inbox, Received, carries_data, is_heartbeat};
pub use node::{
    Applier, ApplyJob, ApplyWork, Boxed, BoxedPersist, Frames, Host, Node, NodeConfig, Persists,
    apply,
};
pub use outbox::{Dropped, Outbox, Oversized};
pub use range::RangeId;
pub use round::{Act, Cores, Meters, Round, Stamps};
pub use variant::{NodeVariant, NodeVariants};
