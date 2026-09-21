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
//! The `raft` and `apply` tasks, the round (Q41), the snapshot task, descriptors,
//! split, merge and the rebalancer are each a later stage's; nothing here spawns
//! anything or touches a clock, a disk or a socket.

pub mod frame;
pub mod inbox;
pub mod outbox;
pub mod range;

pub use frame::{Decoded, Tagged, decode, encoded_len, studio};
pub use inbox::{Admission, Inbox, Received, carries_data, is_heartbeat};
pub use outbox::{Dropped, Outbox, Oversized};
pub use range::RangeId;
