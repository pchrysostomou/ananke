//! [`TraceEvent`]: what the environment records for moirae (SPEC.md §1.5).

use std::net::SocketAddr;

use crate::TaskId;

/// A state transition worth seeing in the moirae studio.
///
/// The environment stamps each event with the node and the time it was recorded, so
/// those are not part of the event. The enum grows with each phase (range id, term, log
/// index, transaction id); it is `non_exhaustive` so downstream matches carry a wildcard
/// arm and keep compiling.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TraceEvent {
    /// A task was handed to the scheduler.
    TaskSpawned {
        /// The new task's id.
        task: TaskId,
        /// The name given to `Environment::spawn`.
        name: &'static str,
    },
    /// A task's future ran to completion. Aborted tasks do not produce this event.
    TaskCompleted {
        /// The finished task's id.
        task: TaskId,
    },
    /// A socket accepted a message for sending. It may still be lost.
    MessageSent {
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The destination address.
        to: SocketAddr,
        /// Payload length in bytes.
        len: usize,
    },
    /// A message reached the destination socket's receive queue.
    MessageDelivered {
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The receiving socket's bound address.
        to: SocketAddr,
        /// Payload length in bytes.
        len: usize,
    },
    /// A message was discarded and will never be delivered.
    MessageDropped {
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The destination address.
        to: SocketAddr,
        /// Why it was discarded.
        reason: DropReason,
    },
}

/// Why a message was discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DropReason {
    /// The per-destination send queue was full; the oldest frame made room (D-015).
    QueueFull,
    /// The simulated network has the link partitioned.
    Partitioned,
    /// The simulated network chose to lose it (random drop).
    Injected,
    /// No socket is bound at the destination.
    Unreachable,
}
