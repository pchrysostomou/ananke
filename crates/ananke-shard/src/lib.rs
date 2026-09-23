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
//! - [`mod@snapshot`], the `snapshot` task: one task keyed by (range, follower) on the
//!   way out and (range, sender) on the way in, its staging and version directories
//!   and their sweep keyed by range, its chunks in frames of their own, and D-066's
//!   live install of a range's spans with the repair carried in the switch. A stream
//!   over the receive cap takes a slot by asking again once one is free, and a stream
//!   that starts its assembly over is restarted from its first byte rather than
//!   installed from a directory that has just been cleared.
//! - [`mod@install`], the `snapshot` task itself: the streams' bytes, the engine's
//!   calls and the trace events the planner has none of, on a fourth handle of the
//!   node's one socket. It is what makes an install *complete* on the node — a take
//!   of the range's own key intervals, a stream per (range, follower), a chunk
//!   diverted before the node's inbox rather than admitted and then dropped
//!   (issue #96), and D-066's live install of the range's two spans in one manifest
//!   switch, with the range held across it and its replica replaced by the one the
//!   switch built.
//! - [`mod@reseed`], Q15's whole-node refusal: a loss in the shared engine refuses
//!   every replica the node holds, and the node re-seeds into a fresh engine in a new
//!   directory beside the refused one, which stays marked lost and quiesced. This
//!   module is the naming the two rules that meet there fix — D-041's directory that
//!   held a store never opening fresh, and D-066's start opening the newest directory
//!   not marked lost — decided over a listing rather than over a disk.
//! - [`mod@variant`], the node's known-buggy variants, each a plausible way to get
//!   the round or the snapshot task wrong, built beside the correct code (CLAUDE.md's
//!   pair rule).
//!
//! Descriptors, split, merge and the rebalancer are each a later slice's.

pub mod client;
pub mod frame;
pub mod inbox;
pub mod install;
pub mod node;
pub mod outbox;
pub mod range;
pub mod reseed;
pub mod round;
pub mod server;
pub mod snapshot;
pub mod variant;

pub use client::{RangedRequest, RangedResponse, is_ranged};
pub use frame::{Decoded, Tagged, decode, encoded_len, studio};
pub use inbox::{Admission, Inbox, Received, carries_data, is_heartbeat};
pub use node::{
    Applier, ApplyJob, ApplyWork, Boxed, BoxedPersist, Frames, Host, Node, NodeConfig, Persists,
    apply,
};
pub use outbox::{Dropped, Outbox, Oversized};
pub use range::RangeId;
pub use reseed::{
    Candidate, generation_dir, generation_of, newest_not_lost, next_generation, reseed_dir,
};
pub use round::{Act, Cores, Meters, Round, Stamps};
pub use server::{Gaps, Local, Range, ServerConfig, ServerHost, run};
pub use snapshot::{
    Adopted, Identity, Install, Landing, Route, Snapshots, Started, Streams, parse_version,
    staging_name, version_name,
};
pub use variant::{NodeVariant, NodeVariants};
