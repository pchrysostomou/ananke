//! The node's inbox (SHARD.md §4; Q14): one per node, bounded in bytes, with
//! admission in constant time.
//!
//! One socket feeds one inbox, whatever range a message is about. It sits between the
//! `net` task, which decodes a frame and admits its messages one by one, and the
//! `raft` task, which drains it; it is an async queue with no runtime behind it, the
//! shape [`ananke_raft::queue::Queue`] has — a consumer that finds it empty parks its
//! waker and the next admission wakes it, one consumer at a time and any number of
//! producers.
//!
//! **Bounded in bytes, not in messages.** Today's inbox counts messages, 128 in the
//! sweep, and admits an AppendEntries carrying entries over that bound
//! (`ananke_raft::node`), so what it holds is unbounded in bytes — a bound that says
//! nothing about a node whose ranges carry 64-byte commands and one whose ranges carry
//! 4 MiB ones. This one counts the frame bytes each message occupied, which is exactly
//! what the node took off the wire for it.
//!
//! **What it drops when it is full** is not settled by §4, and this is the
//! conservative answer, PROPOSED D-072: *the arriving message is refused and nothing
//! already admitted is dropped.* Admission is final; the byte bound is never exceeded;
//! and a refused arrival is, to the protocol, a message the network lost, which the
//! transport is allowed to do at any time (`ananke_env::net`) and which Raft answers
//! by sending again. The alternative — today's policy, which drops the oldest
//! heartbeat of any sender first — needs the index of heartbeats by sender and range
//! that SHARD.md §11's raft item 11 names, and destroys work the node has already
//! done; D-072 records it, and what would have to be measured before taking it.
//!
//! Admission is per message, not per frame: a frame is admitted as far as it fits, in
//! order, and the messages after that are refused. Every message admitted is whole —
//! the framing is [`crate::frame`]'s, and nothing is torn by a refusal.
//!
//! Ticks are not in here. The inbox holds what arrived on the socket; the round's
//! ticks are the ticker's, and how the `raft` task races them against this queue is
//! Q41's, a later stage's.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use ananke_raft::message::Message;
use ananke_raft::types::ServerId;

use crate::frame::Tagged;
use crate::range::RangeId;

/// The queued messages, and the one way to look at one.
///
/// Every read of a queued entry goes through this module and is counted, and the
/// `VecDeque` is private to it, so nothing in this file can examine the queue without
/// the count moving. [`Inbox::probes`] is therefore a measurement of an operation's
/// work over the queue and not an estimate of it: the figure Q14's "constant or
/// logarithmic time" is asserted on, in entries examined rather than in nanoseconds,
/// so it is the same on any machine and under any load.
mod slots {
    use std::collections::VecDeque;

    pub(super) struct Slots<T> {
        items: VecDeque<T>,
        probes: u64,
    }

    impl<T> Slots<T> {
        pub(super) fn new() -> Self {
            Self {
                items: VecDeque::new(),
                probes: 0,
            }
        }

        /// Appends. Nothing queued is examined, so nothing is counted.
        pub(super) fn push_back(&mut self, item: T) {
            self.items.push_back(item);
        }

        /// Takes the oldest, one entry examined.
        pub(super) fn pop_front(&mut self) -> Option<T> {
            let item = self.items.pop_front();
            self.probes += u64::from(item.is_some());
            item
        }

        pub(super) fn len(&self) -> usize {
            self.items.len()
        }

        /// The queue entries examined since this queue was made.
        pub(super) fn probes(&self) -> u64 {
            self.probes
        }
    }
}

use slots::Slots;

/// A message that arrived on the node's socket, with the range it is about and what
/// it cost the node to receive it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Received {
    /// The range the message is about (Q10).
    pub range: RangeId,
    /// The sender.
    pub from: ServerId,
    /// The message.
    pub message: Message,
    /// The frame bytes it occupied, tag included: what it counts for against the
    /// inbox's bound.
    pub bytes: usize,
}

impl From<Tagged> for Received {
    fn from(tagged: Tagged) -> Self {
        Self {
            range: tagged.range,
            from: tagged.frame.from,
            message: tagged.frame.message,
            bytes: tagged.bytes,
        }
    }
}

/// What became of a message offered to the inbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// It is on the queue.
    Admitted,
    /// It did not fit under the bound and is handed back, so the caller can trace the
    /// drop it is: `RaftInboxDropped` carries the range (D-069).
    // PROPOSED(D-072): a full inbox refuses the arriving message and drops nothing it
    // has already admitted.
    Refused(Received),
}

impl Admission {
    /// Whether the message was admitted.
    #[must_use]
    pub fn is_admitted(&self) -> bool {
        matches!(self, Admission::Admitted)
    }
}

struct Inner {
    items: Slots<Received>,
    bytes: usize,
    refused: u64,
    waker: Option<Waker>,
    closed: bool,
}

/// See the module documentation.
pub struct Inbox {
    inner: Arc<Mutex<Inner>>,
    bound: usize,
}

impl Clone for Inbox {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            bound: self.bound,
        }
    }
}

impl Inbox {
    /// An empty inbox holding at most `bound` bytes.
    ///
    /// # Panics
    ///
    /// If `bound` is zero, which is an inbox that can take nothing.
    #[must_use]
    pub fn new(bound: usize) -> Self {
        assert!(bound > 0, "an inbox holds something");
        Self {
            inner: Arc::new(Mutex::new(Inner {
                items: Slots::new(),
                bytes: 0,
                refused: 0,
                waker: None,
                closed: false,
            })),
            bound,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The bound, in bytes.
    #[must_use]
    pub fn bound(&self) -> usize {
        self.bound
    }

    /// Offers `message` to the queue, in constant time: the queued bytes are kept, so
    /// the decision is one comparison and no entry of the queue is examined (Q14).
    pub fn admit(&self, message: Received) -> Admission {
        let waker = {
            let mut inner = self.lock();
            if inner.bytes.saturating_add(message.bytes) > self.bound {
                inner.refused += 1;
                return Admission::Refused(message);
            }
            inner.bytes += message.bytes;
            inner.items.push_back(message);
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Admission::Admitted
    }

    /// The next message, once there is one; `None` once the inbox is closed and
    /// empty.
    pub fn pop(&self) -> Pop<'_> {
        Pop(self)
    }

    /// Closes the inbox: what is in it is still drained, then [`pop`](Self::pop)
    /// returns `None`.
    pub fn close(&self) {
        let waker = {
            let mut inner = self.lock();
            inner.closed = true;
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// The bytes queued.
    #[must_use]
    pub fn queued_bytes(&self) -> usize {
        self.lock().bytes
    }

    /// The messages queued.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().items.len()
    }

    /// Whether nothing is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many messages this inbox has refused.
    #[must_use]
    pub fn refused(&self) -> u64 {
        self.lock().refused
    }

    /// The queue entries every operation on this inbox has examined between them: the
    /// work an admission costs, counted rather than timed, so a test of Q14's bound
    /// is deterministic. Every look at a queued entry goes through one private module
    /// that counts it, so this is a measurement and not an estimate.
    #[must_use]
    pub fn probes(&self) -> u64 {
        self.lock().items.probes()
    }
}

/// See [`Inbox::pop`].
#[must_use = "futures do nothing unless polled"]
pub struct Pop<'a>(&'a Inbox);

impl Future for Pop<'_> {
    type Output = Option<Received>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.0.lock();
        if let Some(message) = inner.items.pop_front() {
            inner.bytes -= message.bytes;
            return Poll::Ready(Some(message));
        }
        if inner.closed {
            return Poll::Ready(None);
        }
        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::frame::{Builder, TAG_LEN, decode};
    use ananke_raft::message::Frame;
    use ananke_raft::types::{Entry, Payload};

    /// A message of `bytes` bytes against the bound, tagged `range`.
    fn message(range: u64, term: u64, bytes: usize) -> Received {
        Received {
            range: RangeId(range),
            from: ServerId(1),
            message: Message::TimeoutNow { term },
            bytes,
        }
    }

    fn drain(inbox: &Inbox) -> Vec<Received> {
        let mut cx = Context::from_waker(Waker::noop());
        let mut drained = Vec::new();
        while let Poll::Ready(Some(message)) = Pin::new(&mut inbox.pop()).poll(&mut cx) {
            drained.push(message);
        }
        drained
    }

    /// The bound is in bytes and is never passed, whatever the sizes arriving.
    #[test]
    fn the_bound_is_in_bytes_and_admission_never_goes_over_it() {
        let inbox = Inbox::new(1000);
        assert!(inbox.admit(message(2, 1, 600)).is_admitted());
        assert_eq!(inbox.queued_bytes(), 600);
        assert!(inbox.admit(message(9, 2, 400)).is_admitted());
        assert_eq!(inbox.queued_bytes(), 1000, "exactly the bound fits");
        assert_eq!(inbox.len(), 2);
        // One byte more does not.
        assert_eq!(
            inbox.admit(message(7, 3, 1)),
            Admission::Refused(message(7, 3, 1))
        );
        assert_eq!(inbox.queued_bytes(), 1000);
        assert_eq!(inbox.refused(), 1);
        // Draining gives the room back, message by message.
        let mut cx = Context::from_waker(Waker::noop());
        let first = Pin::new(&mut inbox.pop()).poll(&mut cx);
        assert_eq!(first, Poll::Ready(Some(message(2, 1, 600))));
        assert_eq!(inbox.queued_bytes(), 400);
        assert!(inbox.admit(message(7, 3, 600)).is_admitted());
        assert_eq!(inbox.queued_bytes(), 1000);
    }

    /// Nothing already admitted is dropped to make room, and what is refused is the
    /// message that did not fit: the queue after a refusal is the queue before it,
    /// entry for entry (PROPOSED D-072).
    #[test]
    fn a_full_inbox_refuses_the_arrival_and_keeps_every_message_it_admitted() {
        let inbox = Inbox::new(300);
        for range in 1..=3 {
            assert!(inbox.admit(message(range, range, 100)).is_admitted());
        }
        for range in 4..=6 {
            let refused = inbox.admit(message(range, range, 100));
            assert_eq!(refused, Admission::Refused(message(range, range, 100)));
        }
        assert_eq!(inbox.refused(), 3);
        assert_eq!(inbox.len(), 3);
        assert_eq!(inbox.queued_bytes(), 300);
        assert_eq!(
            drain(&inbox),
            (1..=3).map(|r| message(r, r, 100)).collect::<Vec<_>>(),
            "the three admitted, oldest first, none of them dropped for a newer one"
        );
        assert_eq!(inbox.queued_bytes(), 0);
        assert!(inbox.is_empty());
    }

    /// Q14: admission is constant or logarithmic in the queue's length. The figure is
    /// the queue entries one admission examines, counted by the `slots` module, at
    /// lengths from one to the bound in powers of two — work, not time, so the test is
    /// deterministic. Measured both with room and on a full inbox, because a policy
    /// that looked for a victim would scan only on the full one.
    #[test]
    fn an_admission_examines_no_more_of_the_queue_as_the_queue_grows() {
        const COST: usize = 64;
        const LONGEST: usize = 4096;
        let mut with_room = Vec::new();
        let mut when_full = Vec::new();
        let mut length = 1;
        while length <= LONGEST {
            let roomy = Inbox::new(COST * (length + 1));
            let full = Inbox::new(COST * length);
            for i in 0..length {
                assert!(roomy.admit(message(2, i as u64, COST)).is_admitted());
                assert!(full.admit(message(2, i as u64, COST)).is_admitted());
            }
            let (before_roomy, before_full) = (roomy.probes(), full.probes());
            assert!(roomy.admit(message(9, 0, COST)).is_admitted());
            assert!(!full.admit(message(9, 0, COST)).is_admitted());
            with_room.push((length, roomy.probes() - before_roomy));
            when_full.push((length, full.probes() - before_full));
            length *= 2;
        }
        for (name, rows) in [("with room", &with_room), ("when full", &when_full)] {
            println!(
                "inbox admission, {name}: {}",
                rows.iter()
                    .map(|(length, probes)| format!("{length} -> {probes}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            // Q14's standard: constant, or growing by at most a constant for every
            // doubling of the queue. A scan of the queue fails this by a mile.
            let at_one = rows[0].1;
            for &(length, probes) in rows {
                let doublings = u64::from(length.ilog2());
                assert!(
                    probes <= at_one + 2 * doublings,
                    "{name}: an admission at length {length} examined {probes} entries, \
                     against {at_one} at length 1 and Q14's constant or logarithmic bound"
                );
            }
            // As built it is constant, and the constant is none: the kept size is all
            // an admission reads.
            assert!(
                rows.iter().all(|&(_, probes)| probes == 0),
                "{name}: {rows:?}"
            );
        }
    }

    /// The wire's own accounting: what a frame spent on a message is what that
    /// message costs the inbox, and a frame is admitted as far as it fits.
    #[test]
    fn a_frame_s_messages_are_admitted_one_by_one_under_the_node_s_bound() {
        let entry = |index| Entry {
            term: 3,
            index,
            payload: Payload::Command(Bytes::from_static(b"put k v")),
        };
        let frame = |from, index| Frame {
            from: ServerId(from),
            message: Message::AppendEntries {
                term: 3,
                prev_index: index - 1,
                prev_term: 3,
                entries: vec![entry(index)],
                commit: 6,
                sent: 1,
            },
        };
        let mut builder = Builder::new(ananke_env::MAX_FRAME_LEN);
        for (range, index) in [(2u64, 8u64), (9, 3), (7, 4)] {
            builder.push(RangeId(range), &frame(1, index).encode());
        }
        let messages = decode(&builder.finish()).expect("a frame this crate wrote");
        let each = messages[0].bytes;
        assert_eq!(each, TAG_LEN + frame(1, 8).encode().len());
        // Room for two of the three.
        let inbox = Inbox::new(2 * each);
        let admitted: Vec<_> = messages
            .into_iter()
            .map(|tagged| inbox.admit(Received::from(tagged)))
            .collect();
        assert!(admitted[0].is_admitted());
        assert!(admitted[1].is_admitted());
        let Admission::Refused(refused) = &admitted[2] else {
            panic!("the third does not fit")
        };
        assert_eq!(
            refused.range,
            RangeId(7),
            "and is handed back with its range"
        );
        assert_eq!(inbox.queued_bytes(), 2 * each);
        let drained = drain(&inbox);
        assert_eq!(
            drained.iter().map(|m| m.range).collect::<Vec<_>>(),
            vec![RangeId(2), RangeId(9)],
            "each message under the range its frame tagged it with"
        );
        assert_eq!(drained[0].from, ServerId(1));
    }

    /// The queue between two tasks: an empty inbox parks its consumer, an admission
    /// wakes it, and a closed inbox drains and then ends.
    #[test]
    fn an_empty_inbox_parks_its_consumer_and_an_admission_wakes_it() {
        let inbox = Inbox::new(1000);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut inbox.pop()).poll(&mut cx).is_pending());
        assert!(inbox.admit(message(2, 1, 100)).is_admitted());
        assert_eq!(
            Pin::new(&mut inbox.pop()).poll(&mut cx),
            Poll::Ready(Some(message(2, 1, 100)))
        );
        assert!(inbox.admit(message(9, 2, 100)).is_admitted());
        inbox.close();
        assert_eq!(
            Pin::new(&mut inbox.pop()).poll(&mut cx),
            Poll::Ready(Some(message(9, 2, 100)))
        );
        assert_eq!(Pin::new(&mut inbox.pop()).poll(&mut cx), Poll::Ready(None));
        assert_eq!(inbox.clone().len(), 0, "the same queue through a clone");
    }
}
