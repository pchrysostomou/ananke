//! Raft for ananke (SPEC.md §3, RAFT.md): a pure protocol core, its persistent state
//! in the storage engine, and the invariants the crash sweep folds from the trace.
//!
//! [`core::Raft`] is Figure 2 of the paper with pre-vote, batching and pipelining,
//! stepped by [`core::Input`]s and producing [`core::Output`]s in an order the server
//! must keep: a persist before the messages that depend on it. [`store::RaftStore`]
//! keeps the hard state and the log under a reserved tenant of the engine and applies
//! an entry's writes with the applied index in one batch. [`message`] is the wire
//! form and the studio's view of it, and [`client`] the requests and responses that
//! share the servers' socket. [`invariants`] are the four log properties of Figure 3
//! and three folds of the rules behind them, over trace events. [`node`] runs a core
//! under the `Environment` as the `raft`, `net` and `apply` tasks, joined by
//! [`queue`]s; the sweep and the linearizability checker live in `sim/`.
//!
//! # Fault-model tests
//!
//! [`core::Variant`] carries the known-buggy cores beside the correct one; the
//! paper's scenarios in `tests/` show each buggy core breaking its rule and the
//! correct one holding, and the sweep must catch each under faults (CLAUDE.md).

pub mod apply;
pub mod client;
pub mod core;
pub mod invariants;
pub mod message;
pub mod node;
pub mod queue;
pub mod store;
pub mod types;

pub use apply::{Command, Outcome};
pub use core::{Input, Output, Persist, Raft, RaftConfig, Role, Variant};
pub use message::{Frame, Message};
pub use node::{NodeConfig, run};
pub use store::{LostState, RaftStore};
pub use types::{Configuration, Entry, Index, Payload, ServerId, Term};
