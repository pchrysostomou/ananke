//! [`TraceEvent`]: what the environment records for moirae (SPEC.md §1.5).

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use bytes::Bytes;

use crate::{Instant, NodeId, TaskId};

/// Identifies one message from its send to its delivery or drop, within one
/// environment. Assigned at send; the same id appears on the receiving side, so the
/// three events of a message correlate (moirae's `msgId`, SPEC §1.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(u64);

impl MessageId {
    /// Wraps a raw id.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&format!("msg#{}", self.0))
    }
}

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
    /// The scheduler chose this task to poll next. Simulation only: this is the
    /// scheduling decision SPEC.md §1.1 says must be in the trace.
    TaskPolled {
        /// The polled task's id.
        task: TaskId,
    },
    /// A task's future ran to completion. Aborted tasks do not produce this event.
    TaskCompleted {
        /// The finished task's id.
        task: TaskId,
    },
    /// Virtual time jumped forward to the next timer or delivery. Simulation only.
    TimeAdvanced {
        /// The new global virtual time.
        to: Instant,
    },
    /// A socket accepted a message for sending. It may still be lost.
    MessageSent {
        /// The message's id.
        id: MessageId,
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The destination address.
        to: SocketAddr,
        /// The payload, so the trace can be decoded later; `Bytes` is reference-counted.
        payload: Bytes,
    },
    /// A message reached the destination socket's receive queue.
    MessageDelivered {
        /// The message's id.
        id: MessageId,
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The receiving socket's bound address.
        to: SocketAddr,
        /// Payload length in bytes.
        len: usize,
    },
    /// A message was discarded and will never be delivered.
    MessageDropped {
        /// The message's id.
        id: MessageId,
        /// The sending socket's bound address.
        from: SocketAddr,
        /// The destination address.
        to: SocketAddr,
        /// Why it was discarded.
        reason: DropReason,
    },
    /// `fsync` returned Ok but the data did not become durable (SPEC.md §1.3).
    FsyncLost {
        /// The file that was synced.
        path: PathBuf,
    },
    /// At a crash only a prefix of a pending write survived (SPEC.md §1.3).
    WriteTorn {
        /// The file that was written.
        path: PathBuf,
        /// Where the write started.
        offset: u64,
        /// How many bytes the write carried.
        written: usize,
        /// How many of them reached the disk.
        kept: usize,
    },
    /// A node's tasks were killed and the filesystem fault model applied to its disk.
    NodeCrashed {
        /// The crashed node.
        node: NodeId,
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
