//! The node's inbox (SHARD.md §4; Q14): one per node, bounded in bytes, with
//! admission in constant or logarithmic time.
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
//! The bound is what the wire spent, not what the heap holds. A decoded command is a
//! `Bytes` slice of the frame it arrived in (`ananke_raft::message`), so one admitted
//! message can keep its whole frame alive. A heartbeat cannot: it holds no `Bytes` at
//! all. The message that pins a frame is the *smallest one carrying a payload* — an
//! AppendEntries with one entry, 93 bytes of the bound, holding every byte of the
//! frame it arrived in, up to `MAX_FRAME_LEN` over those 93 and so about 180 000×.
//! Bounding the heap instead means copying every message out of its frame at
//! admission, a copy per message on the receive path, and the figure to decide that on
//! — the inbox's live bytes against its bound — is the node's stage's to measure.
//!
//! **What it drops when it is full**, PROPOSED D-072, is not one rule but two, because
//! the messages are not alike:
//!
//! - A message that *carries data* — an AppendEntries with entries, or an
//!   InstallSnapshot chunk — is admitted by **making room**: the oldest heartbeat of
//!   the (sender, range) pair holding the most of them is dropped, and again until it
//!   fits. It is never refused for want of room a heartbeat is holding. That is what
//!   `ananke_raft::node` protects today by admitting these over the bound; dropping
//!   heartbeats for them keeps the protection without breaking the bound.
//! - Any other message — a heartbeat, a vote, a response — is refused when it does
//!   not fit. It is the cheap one to lose: a heartbeat repeats every 20 ms against a
//!   minimum election timeout of 100 ms, and a follower taking entries has its timer
//!   reset by the entries themselves, so refusing heartbeats under pressure cannot
//!   start the election that refusing entry-carriers would need to repair.
//!
//! and one rule over both: **nothing is ever refused into an empty queue, while the
//! node holds nothing.** A message larger than the whole bound is admitted over it
//! rather than refused for ever — an AppendEntries carrying a 64 KiB command costs
//! 65 622 bytes against a 16 kB inbox, and refusing it refuses every retransmission of
//! it identically, so the range it is about never replicates again. The bound is
//! exceeded only by an inbox holding exactly one message, which the next pop empties.
//!
//! The second half of that rule is PROPOSED D-074 and is what makes the bound a bound
//! of *the node*: the node's `raft` task takes a message it cannot step yet and
//! [holds](Inbox::hold_at) it, draining the queue to empty on every wake, so an
//! exemption that asked only about the queue would be met at every arrival and a node
//! behind a slow sync would hold messages without limit. What the node holds drains
//! when the sync resolves, so the oversized message is admitted on the retransmission
//! that finds the node holding nothing, and nothing is refused for ever.
//!
//! The victim is chosen through the `slots` module's index of heartbeats **by sender
//! and range**, which SHARD.md §11's raft item 11 names: a map from (sender, range) to
//! that pair's heartbeats in arrival order, and a ranking of those pairs by how many
//! each holds. The fullest pair's oldest heartbeat is two lookups and no scan, so the
//! pair flooding the inbox pays before any other and one range cannot be starved of
//! its heartbeats by another's.
//!
//! Admission is per message, not per frame: a frame is admitted as far as it fits, in
//! order. Every message admitted is whole — the framing is [`crate::frame`]'s, and
//! nothing is torn by a refusal.
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

/// Whether a message carries data no retransmission makes cheap: entries to append,
/// or a chunk of a snapshot.
///
/// These are the messages a range's commit index moves on, and `ananke_raft::node`
/// admits them over its bound for exactly that reason. Here they are admitted by
/// dropping heartbeats instead, so the byte bound holds and they are still never
/// refused for want of room a heartbeat is holding.
#[must_use]
pub fn carries_data(message: &Message) -> bool {
    match message {
        Message::AppendEntries { entries, .. } => !entries.is_empty(),
        Message::InstallSnapshot { .. } => true,
        _ => false,
    }
}

/// Whether a message is a heartbeat: an AppendEntries with no entries, which is what
/// `ananke_raft::node`'s own policy drops first and what this one keeps an index of.
#[must_use]
pub fn is_heartbeat(message: &Message) -> bool {
    matches!(message, Message::AppendEntries { entries, .. } if entries.is_empty())
}

/// The queued messages, the index of the heartbeats among them, and the only ways to
/// look at one.
///
/// Every read of a queued entry goes through this module and is counted, and the queue
/// itself is private to it. The surface is deliberately too small to scan with: no
/// iterator, no indexing, no `front`, no `get`, no borrow of a queued message. The
/// only ways to reach one are [`Slots::pop_front`] and
/// [`Slots::take_noisiest_heartbeat`], and both count what they touch, so
/// [`Inbox::probes`] is a measurement of an operation's work over the queue and not an
/// estimate of it: the figure Q14's "constant or logarithmic time" is asserted on, in
/// entries examined rather than in nanoseconds, so it is the same on any machine and
/// under any load.
///
/// The index is SHARD.md §11's raft item 11: heartbeats by (sender, range), and a
/// ranking of those pairs by how many each holds, so the fullest pair's oldest
/// heartbeat is found without examining a single queued entry.
mod slots {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    use ananke_raft::types::ServerId;

    use crate::range::RangeId;

    /// A queued message's place in arrival order. Handed out by
    /// [`Slots::push_back`] and never by anything a caller of the inbox can reach.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Seq(u64);

    /// What a heartbeat is indexed under: who sent it and what range it is about.
    pub(super) type Pair = (ServerId, RangeId);

    struct Slot<T> {
        item: T,
        /// `Some` for a heartbeat: the pair it is indexed under and what it cost.
        heartbeat: Option<(Pair, usize)>,
    }

    pub(super) struct Slots<T> {
        /// The queue in arrival order. A message removed from the middle leaves a
        /// hole, which `pop_front` skips and counts; `base` is the sequence number
        /// of `items[0]`.
        items: VecDeque<Option<Slot<T>>>,
        base: u64,
        next: u64,
        live: usize,
        /// Each pair's queued heartbeats, oldest first.
        heartbeats: BTreeMap<Pair, VecDeque<Seq>>,
        /// Those pairs by how many heartbeats each holds, so the fullest is
        /// `next_back`. Ties go to the highest (sender, range), a total order, so the
        /// choice is the same on every node and in every run.
        ranking: BTreeSet<(usize, Pair)>,
        heartbeat_bytes: usize,
        probes: u64,
    }

    impl<T> Slots<T> {
        pub(super) fn new() -> Self {
            Self {
                items: VecDeque::new(),
                base: 0,
                next: 0,
                live: 0,
                heartbeats: BTreeMap::new(),
                ranking: BTreeSet::new(),
                heartbeat_bytes: 0,
                probes: 0,
            }
        }

        /// Appends, indexing it under `heartbeat`'s pair when it is one. Nothing
        /// queued is examined, so nothing is counted.
        pub(super) fn push_back(&mut self, item: T, heartbeat: Option<(Pair, usize)>) {
            let seq = Seq(self.next);
            self.next += 1;
            self.live += 1;
            if let Some((pair, bytes)) = heartbeat {
                let held = self.heartbeats.entry(pair).or_default();
                held.push_back(seq);
                let count = held.len();
                if count > 1 {
                    self.ranking.remove(&(count - 1, pair));
                }
                self.ranking.insert((count, pair));
                self.heartbeat_bytes += bytes;
            }
            self.items.push_back(Some(Slot { item, heartbeat }));
        }

        /// Takes the oldest message: one entry examined for it, and one more for
        /// every hole an earlier removal left in front of it.
        pub(super) fn pop_front(&mut self) -> Option<T> {
            while let Some(slot) = self.items.pop_front() {
                self.base += 1;
                self.probes += 1;
                if let Some(slot) = slot {
                    self.live -= 1;
                    self.unindex(Seq(self.base - 1), slot.heartbeat);
                    return Some(slot.item);
                }
            }
            None
        }

        /// The oldest heartbeat of the (sender, range) pair holding the most of them,
        /// removed: one entry examined. Finding it examines none, since the index
        /// holds sequence numbers and not messages.
        ///
        /// `None` when no heartbeat is queued.
        pub(super) fn take_noisiest_heartbeat(&mut self) -> Option<T> {
            let &(_, pair) = self.ranking.iter().next_back()?;
            let seq = *self
                .heartbeats
                .get(&pair)
                .expect("a ranked pair holds heartbeats")
                .front()
                .expect("a ranked pair holds at least one");
            let at = usize::try_from(seq.0 - self.base).expect("a queued sequence");
            let slot = self
                .items
                .get_mut(at)
                .expect("an indexed heartbeat is queued");
            self.probes += 1;
            let slot = slot.take().expect("an indexed heartbeat is not a hole");
            self.live -= 1;
            self.unindex(seq, slot.heartbeat);
            Some(slot.item)
        }

        /// Takes a heartbeat out of the index. Touches the index only, never a queued
        /// message: the pair comes from the message that is leaving, so there is
        /// nothing to search for.
        fn unindex(&mut self, seq: Seq, heartbeat: Option<(Pair, usize)>) {
            let Some((pair, bytes)) = heartbeat else {
                return;
            };
            let held = self
                .heartbeats
                .get_mut(&pair)
                .expect("a queued heartbeat is indexed under its pair");
            let count = held.len();
            debug_assert_eq!(
                held.front(),
                Some(&seq),
                "a pair's heartbeats leave oldest first"
            );
            held.pop_front();
            self.ranking.remove(&(count, pair));
            if held.is_empty() {
                self.heartbeats.remove(&pair);
            } else {
                self.ranking.insert((count - 1, pair));
            }
            self.heartbeat_bytes -= bytes;
        }

        /// The messages queued.
        pub(super) fn len(&self) -> usize {
            self.live
        }

        /// The heartbeats among them.
        pub(super) fn heartbeats(&self) -> usize {
            self.ranking.iter().map(|(count, _)| count).sum()
        }

        /// What those heartbeats cost against the bound: the room a message carrying
        /// data can make for itself.
        pub(super) fn heartbeat_bytes(&self) -> usize {
            self.heartbeat_bytes
        }

        /// The queue entries examined since this queue was made.
        pub(super) fn probes(&self) -> u64 {
            self.probes
        }
    }
}

use slots::{Pair, Slots};

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
    /// The frame bytes it occupied — its tag, its own length, and for the first
    /// message of a frame that frame's header: what it counts for against the inbox's
    /// bound.
    // PROPOSED(D-072): the bound is in wire bytes, not in live heap; see the module
    // documentation.
    pub bytes: usize,
}

impl Received {
    /// What this message is indexed under if it is a heartbeat, and nothing if it is
    /// not.
    fn heartbeat(&self) -> Option<(Pair, usize)> {
        is_heartbeat(&self.message).then_some(((self.from, self.range), self.bytes))
    }
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
    /// It is on the queue, and these heartbeats were dropped to make room for it,
    /// oldest first. The caller traces each as the drop it is: `RaftInboxDropped`
    /// carries the range (D-069). Empty for every admission that needed no room.
    // PROPOSED(D-072): a message carrying entries or snapshot data makes room by
    // dropping heartbeats rather than being refused.
    Admitted(Vec<Received>),
    /// It did not fit under the bound and is handed back, so the caller can trace the
    /// drop it is. Nothing was dropped for it: the inbox decides whether the room can
    /// be made before it makes any.
    // PROPOSED(D-072): an arrival that carries no data is refused by a full inbox that
    // is not empty, and the queue keeps what it has.
    Refused(Received),
}

impl Admission {
    /// An admission that dropped nothing.
    #[must_use]
    pub fn admitted() -> Self {
        Admission::Admitted(Vec::new())
    }

    /// Whether the message was admitted.
    #[must_use]
    pub fn is_admitted(&self) -> bool {
        matches!(self, Admission::Admitted(_))
    }

    /// The heartbeats dropped to make room for it.
    #[must_use]
    pub fn dropped(&self) -> &[Received] {
        match self {
            Admission::Admitted(dropped) => dropped,
            Admission::Refused(_) => &[],
        }
    }
}

struct Inner {
    items: Slots<Received>,
    bytes: usize,
    refused: u64,
    dropped: u64,
    /// The bytes of messages the node has taken from the queue and cannot step yet:
    /// a message for a core whose persist is outstanding is held for that core and
    /// still counted against the node's byte bound (SHARD.md §4, Q14).
    // PROPOSED(D-073): held messages keep their charge against the bound.
    held: usize,
    waker: Option<Waker>,
    closed: bool,
}

impl Inner {
    /// What the bound is measured against: the queue and what the node holds.
    fn charged(&self) -> usize {
        self.bytes.saturating_add(self.held)
    }
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
                dropped: 0,
                held: 0,
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

    /// Offers `message` to the queue.
    ///
    /// The decision is arithmetic on kept sizes, so an admission that needs no room
    /// examines no queue entry at all, at any length (Q14), and whether the room
    /// *can* be made is decided before any of it is made — a refusal never costs a
    /// message that was already admitted. An admission that makes room examines one
    /// entry per heartbeat it drops and finds each through the index in logarithmic
    /// time, without a scan; since every heartbeat dropped was admitted once, that is
    /// at most one entry examined per admission over a run.
    ///
    /// A closed inbox admits nothing: nothing will drain it, so `Admitted` there is a
    /// message lost under a word that says it was not.
    pub fn admit(&self, message: Received) -> Admission {
        let mut dropped = Vec::new();
        let waker = {
            let mut inner = self.lock();
            if inner.closed {
                inner.refused += 1;
                return Admission::Refused(message);
            }
            if inner.charged().saturating_add(message.bytes) > self.bound {
                // The room a message carrying data can make for itself is what the
                // queued heartbeats hold; whether that is enough is decided here,
                // before anything is dropped for it.
                // PROPOSED(D-072): entry-carriers and snapshot chunks make room by
                // dropping heartbeats, noisiest (sender, range) pair first.
                let held = inner.items.heartbeat_bytes();
                let room = if carries_data(&message.message) {
                    held
                } else {
                    0
                };
                let fits_after = (inner.charged() - room.min(inner.charged()))
                    .saturating_add(message.bytes)
                    <= self.bound;
                // PROPOSED(D-072): nothing is refused into an empty queue. A message
                // larger than the whole bound would otherwise be refused for ever,
                // every retransmission of it alike, and the range it is about would
                // never replicate again.
                //
                // PROPOSED(D-074): *and only while the node holds nothing*. The
                // exemption is for a message no emptying of the queue could make room
                // for. A node behind a slow sync drains the queue to empty on every
                // wake, so without this term the queue is empty at every admission and
                // the exemption is the whole path: the bound would bind nothing and
                // the node would hold messages without limit for the whole sync. What
                // the node holds drains when that sync resolves, so a message larger
                // than the bound is still admitted rather than refused for ever — on a
                // retransmission that finds the node holding nothing.
                let empties = inner.held == 0
                    && (inner.items.len() == 0
                        || (room > 0 && inner.items.len() == inner.items.heartbeats()));
                if !fits_after && !empties {
                    inner.refused += 1;
                    return Admission::Refused(message);
                }
                while inner.charged().saturating_add(message.bytes) > self.bound {
                    let Some(victim) = inner.items.take_noisiest_heartbeat() else {
                        break;
                    };
                    inner.bytes -= victim.bytes;
                    inner.dropped += 1;
                    dropped.push(victim);
                }
            }
            let heartbeat = message.heartbeat();
            inner.bytes += message.bytes;
            inner.items.push_back(message, heartbeat);
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Admission::Admitted(dropped)
    }

    /// The next message, once there is one; `None` once the inbox is closed and
    /// empty.
    pub fn pop(&self) -> Pop<'_> {
        Pop(self)
    }

    /// Closes the inbox: what is in it is still drained, then [`pop`](Self::pop)
    /// returns `None` and [`admit`](Self::admit) refuses.
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

    /// Whether the inbox is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// The bytes queued.
    #[must_use]
    pub fn queued_bytes(&self) -> usize {
        self.lock().bytes
    }

    /// The bytes the node holds for cores whose persists are outstanding.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        self.lock().held
    }

    /// What the bound is measured against: the bytes queued and the bytes held.
    #[must_use]
    pub fn charged_bytes(&self) -> usize {
        self.lock().charged()
    }

    /// Sets the bytes the node holds for cores whose persists are outstanding.
    ///
    /// A message for a core whose persist is outstanding is still taken from the
    /// inbox and held for that core, *counted against the node's byte bound*
    /// (SHARD.md §4, Q14). The queue cannot know when that happens, so the node tells
    /// it, with the whole figure rather than a delta: the node knows what it holds
    /// and a lost increment would leak the bound away.
    ///
    /// D-072's rule that nothing is refused into an empty queue reads, with this, as
    /// *nothing is refused into an empty queue while the node holds nothing*: the
    /// node's `raft` task drains the queue to empty on every wake, so a node behind a
    /// slow sync would otherwise meet the exemption at every arrival and hold messages
    /// without limit for the whole sync. A message larger than the whole bound is
    /// still never refused for ever — what the node holds drains when the sync
    /// resolves, and the next retransmission finds the exemption open.
    // PROPOSED(D-073): held messages keep their charge against the bound.
    // PROPOSED(D-074): the empty-queue exemption is narrowed to a node holding
    // nothing, so that what the node holds is bounded too.
    pub fn hold_at(&self, bytes: usize) {
        self.lock().held = bytes;
    }

    /// The next message if one is queued, without waiting: what the `raft` task
    /// drains a round with (SHARD.md §4, "the messages drained since the last
    /// round").
    pub fn take(&self) -> Option<Received> {
        let mut inner = self.lock();
        let message = inner.items.pop_front()?;
        inner.bytes -= message.bytes;
        Some(message)
    }

    /// The messages queued.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().items.len()
    }

    /// The heartbeats queued: what the index holds, and so the room a message
    /// carrying data can make for itself.
    #[must_use]
    pub fn heartbeats(&self) -> usize {
        self.lock().items.heartbeats()
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

    /// How many queued heartbeats this inbox has dropped to make room.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.lock().dropped
    }

    /// The queue entries every operation on this inbox has examined between them: the
    /// work an admission costs, counted rather than timed, so a test of Q14's bound is
    /// deterministic. Every look at a queued entry goes through one private module
    /// whose whole surface counts, and that surface has no iterator, no indexing and
    /// no borrow of a queued message, so this is a measurement and not an estimate.
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
    use crate::frame::{Builder, HEADER_LEN, TAG_LEN, decode};
    use ananke_raft::message::Frame;
    use ananke_raft::types::{Entry, Payload};

    /// A message of `bytes` bytes against the bound, tagged `range`: neither a
    /// heartbeat nor a carrier, which is the ordinary arrival the bound is about.
    fn message(range: u64, term: u64, bytes: usize) -> Received {
        Received {
            range: RangeId(range),
            from: ServerId(1),
            message: Message::TimeoutNow { term },
            bytes,
        }
    }

    /// A heartbeat of `bytes` bytes from `from` about `range`.
    fn beat(from: u64, range: u64, bytes: usize) -> Received {
        Received {
            range: RangeId(range),
            from: ServerId(from),
            message: Message::AppendEntries {
                term: 3,
                prev_index: 7,
                prev_term: 2,
                entries: Vec::new(),
                commit: 6,
                sent: 123_456_789,
            },
            bytes,
        }
    }

    /// An AppendEntries carrying one command: the message a range's commit index
    /// moves on.
    fn carrier(from: u64, range: u64, bytes: usize) -> Received {
        Received {
            range: RangeId(range),
            from: ServerId(from),
            message: Message::AppendEntries {
                term: 3,
                prev_index: 7,
                prev_term: 2,
                entries: vec![Entry {
                    term: 3,
                    index: 8,
                    payload: Payload::Command(Bytes::from_static(b"put k v")),
                }],
                commit: 6,
                sent: 123_456_789,
            },
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

    /// The bound is in bytes and an inbox holding anything never goes over it.
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

    /// Nothing already admitted is dropped for an arrival that carries no data, and
    /// what is refused is the message that did not fit: the queue after such a
    /// refusal is the queue before it, entry for entry (PROPOSED D-072).
    #[test]
    fn a_full_inbox_refuses_an_arrival_that_carries_nothing_and_keeps_what_it_admitted() {
        let inbox = Inbox::new(300);
        for range in 1..=3 {
            assert!(inbox.admit(message(range, range, 100)).is_admitted());
        }
        for range in 4..=6 {
            let refused = inbox.admit(message(range, range, 100));
            assert_eq!(refused, Admission::Refused(message(range, range, 100)));
        }
        assert_eq!(inbox.refused(), 3);
        assert_eq!(inbox.dropped(), 0);
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

    /// The blocker this policy exists for: a message larger than the whole bound.
    ///
    /// An AppendEntries carrying a 64 KiB command costs 65 622 bytes against a 16 kB
    /// inbox. Refusing it refuses every retransmission of it identically — the
    /// arrival is the same size every time and the inbox is empty every time — so the
    /// range it is about never replicates again. Nothing is refused into an empty
    /// queue.
    #[test]
    fn a_message_larger_than_the_whole_bound_is_admitted_and_not_refused_for_ever() {
        let bound = 16 * 1024;
        let command = 64 * 1024;
        let frame = Frame {
            from: ServerId(1),
            message: Message::AppendEntries {
                term: 3,
                prev_index: 7,
                prev_term: 2,
                entries: vec![Entry {
                    term: 3,
                    index: 8,
                    payload: Payload::Command(Bytes::from(vec![7u8; command])),
                }],
                commit: 6,
                sent: 123_456_789,
            },
        };
        let cost = HEADER_LEN + TAG_LEN + frame.encode().len();
        println!(
            "an AppendEntries carrying {command} bytes costs {cost} against a bound of {bound}"
        );
        assert!(cost > bound, "{cost} is over the whole bound of {bound}");
        let big = || Received {
            range: RangeId(9),
            from: ServerId(1),
            message: frame.message.clone(),
            bytes: cost,
        };
        // A thousand retransmissions into an inbox that is empty each time: a
        // thousand admissions, not the thousand refusals refusing the arrival gives.
        let inbox = Inbox::new(bound);
        let mut admitted = 0;
        for _ in 0..1000 {
            if inbox.admit(big()).is_admitted() {
                admitted += 1;
            }
            drain(&inbox);
        }
        assert_eq!(admitted, 1000, "every retransmission admitted");
        assert_eq!(inbox.refused(), 0, "and none of them refused");
        // The bound is exceeded only by an inbox holding exactly that one message,
        // and the next pop empties it.
        let inbox = Inbox::new(bound);
        assert!(inbox.admit(big()).is_admitted());
        assert_eq!(inbox.len(), 1);
        assert!(
            inbox.queued_bytes() > bound,
            "over the bound, by exactly one message"
        );
        assert_eq!(drain(&inbox).len(), 1);
        assert_eq!(inbox.queued_bytes(), 0);
        // The rule is the queue's emptiness and not the arrival's kind: a message
        // carrying nothing is admitted into an empty queue too.
        let inbox = Inbox::new(100);
        assert!(inbox.admit(message(2, 1, 4096)).is_admitted());
        assert_eq!(inbox.len(), 1);
    }

    /// The starvation this policy exists for. A pressured inbox must not admit
    /// heartbeats for ever and refuse entry-carriers for ever: heartbeats keep
    /// arriving, so no election repairs it and the commit index simply stops.
    ///
    /// 63 heartbeats fill a 4 kB inbox; then a hundred rounds, each of a heartbeat and
    /// an entry-carrier arriving and the `raft` task draining two messages. Every
    /// carrier is admitted and the heartbeats pay for the room, where refusing the
    /// arrival admits every heartbeat and refuses every carrier.
    #[test]
    fn a_pressured_inbox_admits_the_messages_that_carry_data_and_drops_heartbeats_for_them() {
        let bound = 4096;
        let beat_cost = 65;
        let carrier_cost = 93;
        let inbox = Inbox::new(bound);
        let mut filled = 0u64;
        while inbox
            .admit(beat(filled % 3 + 1, filled % 7 + 1, beat_cost))
            .is_admitted()
        {
            filled += 1;
        }
        println!("{filled} heartbeats of {beat_cost} bytes fill an inbox of {bound} bytes");
        assert_eq!(filled, (bound / beat_cost) as u64, "full of heartbeats");
        let mut cx = Context::from_waker(Waker::noop());
        let (mut admitted, mut refused, mut beats_dropped) = (0u64, 0u64, 0u64);
        let (mut beats_in, mut applied) = (0u64, 0u64);
        for round in 0..100u64 {
            // A heartbeat arrives, as they do. Under pressure it is refused, which
            // costs a timer reset the next heartbeat repairs 20 ms later.
            if inbox.admit(beat(1, 1, beat_cost)).is_admitted() {
                beats_in += 1;
            }
            match inbox.admit(carrier(2, round % 7 + 1, carrier_cost)) {
                Admission::Admitted(dropped) => {
                    admitted += 1;
                    beats_dropped += dropped.len() as u64;
                    for victim in &dropped {
                        assert!(
                            is_heartbeat(&victim.message),
                            "only heartbeats are dropped to make room"
                        );
                    }
                }
                Admission::Refused(_) => refused += 1,
            }
            // The `raft` task drains: two messages a round, which is what keeps the
            // inbox pressured rather than saturated.
            for _ in 0..2 {
                if let Poll::Ready(Some(message)) = Pin::new(&mut inbox.pop()).poll(&mut cx)
                    && carries_data(&message.message)
                {
                    applied += 1;
                }
            }
        }
        println!(
            "over 100 rounds: {admitted} carriers admitted, {refused} refused, \
             {beats_in} heartbeats admitted, {beats_dropped} dropped for a carrier"
        );
        assert_eq!(refused, 0, "no carrier is refused under pressure");
        assert_eq!(admitted, 100, "every carrier got in");
        assert!(beats_dropped > 0, "and heartbeats paid for the room");
        assert!(inbox.queued_bytes() <= bound, "the bound still holds");
        // Every carrier reaches the core: what was drained plus what is still queued.
        let left = drain(&inbox);
        let queued = left.iter().filter(|m| carries_data(&m.message)).count() as u64;
        assert_eq!(
            applied + queued,
            100,
            "every carrier is drained or still there to be"
        );
    }

    /// The victim is the oldest heartbeat of the (sender, range) pair holding the
    /// most: the pair flooding the inbox pays before any other, so one range cannot
    /// be starved of its heartbeats by another's.
    #[test]
    fn the_heartbeat_dropped_is_the_oldest_of_the_sender_and_range_holding_most() {
        let inbox = Inbox::new(1000);
        // Ten heartbeats from (1, r5), two from (2, r9), one from (1, r7).
        for i in 0..10 {
            assert!(inbox.admit(beat(1, 5, 10 + i)).is_admitted());
        }
        for _ in 0..2 {
            assert!(inbox.admit(beat(2, 9, 10)).is_admitted());
        }
        assert!(inbox.admit(beat(1, 7, 10)).is_admitted());
        assert_eq!(inbox.heartbeats(), 13);
        let queued = inbox.queued_bytes();
        // A carrier needing one heartbeat's room takes it from the fullest pair.
        let admission = inbox.admit(carrier(3, 2, 1000 - queued + 1));
        let dropped = admission.dropped();
        assert_eq!(dropped.len(), 1, "one heartbeat was enough");
        assert_eq!(dropped[0].from, ServerId(1));
        assert_eq!(dropped[0].range, RangeId(5), "the fullest pair");
        assert_eq!(dropped[0].bytes, 10, "and its oldest, which cost 10");
        assert_eq!(inbox.heartbeats(), 12);
        // The quiet pairs keep theirs.
        let left = drain(&inbox);
        let from_seven = left
            .iter()
            .filter(|m| m.range == RangeId(7) && is_heartbeat(&m.message))
            .count();
        assert_eq!(from_seven, 1, "the quiet pair kept its heartbeat");
        let from_nine = left.iter().filter(|m| m.range == RangeId(9)).count();
        assert_eq!(from_nine, 2);
    }

    /// A carrier that no room can hold, into a queue that is not empty, is refused
    /// and nothing is dropped for it: the bound holds, and the messages already
    /// queued are the work in hand. Draining them empties the queue, and the
    /// retransmission is admitted, so nothing is refused for ever.
    #[test]
    fn a_carrier_that_no_room_can_hold_waits_for_the_queue_to_drain_rather_than_for_ever() {
        let inbox = Inbox::new(1000);
        assert!(inbox.admit(carrier(1, 2, 900)).is_admitted());
        assert!(inbox.admit(beat(1, 2, 65)).is_admitted());
        let huge = carrier(1, 9, 5000);
        let admission = inbox.admit(huge.clone());
        assert!(!admission.is_admitted(), "the queue is not empty");
        assert_eq!(
            inbox.dropped(),
            0,
            "and nothing was dropped for a message that could not be made to fit"
        );
        assert_eq!(inbox.heartbeats(), 1);
        assert_eq!(inbox.len(), 2);
        // Drain what was there, and the retransmission is admitted over the bound.
        assert_eq!(drain(&inbox).len(), 2);
        assert!(inbox.is_empty());
        assert!(inbox.admit(huge).is_admitted());
        assert_eq!(inbox.len(), 1);
    }

    /// A carrier bigger than the bound, into a queue that holds heartbeats only,
    /// takes all of them and goes in: dropping them empties the queue, and nothing is
    /// refused into an empty queue.
    #[test]
    fn a_carrier_over_the_bound_takes_every_heartbeat_and_is_admitted() {
        let inbox = Inbox::new(1000);
        for i in 0..10u64 {
            assert!(inbox.admit(beat(i % 3 + 1, i % 4 + 1, 65)).is_admitted());
        }
        let admission = inbox.admit(carrier(1, 9, 4000));
        assert!(admission.is_admitted());
        assert_eq!(admission.dropped().len(), 10, "every heartbeat");
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox.heartbeats(), 0);
        assert_eq!(inbox.queued_bytes(), 4000);
    }

    /// A closed inbox admits nothing: nothing will drain it, so `Admitted` would be a
    /// message lost under a word that says it was not.
    #[test]
    fn a_closed_inbox_refuses_what_is_offered_to_it() {
        let inbox = Inbox::new(1000);
        assert!(inbox.admit(message(2, 1, 100)).is_admitted());
        inbox.close();
        assert!(inbox.is_closed());
        assert_eq!(
            inbox.admit(message(9, 2, 100)),
            Admission::Refused(message(9, 2, 100)),
            "with room to spare, and still refused"
        );
        assert_eq!(
            inbox.admit(carrier(1, 9, 100)),
            Admission::Refused(carrier(1, 9, 100)),
            "a carrier too: nothing will drain it"
        );
        assert_eq!(inbox.len(), 1, "and nothing was queued");
        assert_eq!(inbox.queued_bytes(), 100);
        assert_eq!(inbox.refused(), 2);
        // What was admitted before the close is still drained, then the inbox ends.
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(
            Pin::new(&mut inbox.pop()).poll(&mut cx),
            Poll::Ready(Some(message(2, 1, 100)))
        );
        assert_eq!(Pin::new(&mut inbox.pop()).poll(&mut cx), Poll::Ready(None));
    }

    /// Q14: admission is constant or logarithmic in the queue's length. The figure is
    /// the queue entries one admission examines, counted by the `slots` module, at
    /// lengths from one to the bound in powers of two — work, not time, so the test is
    /// deterministic. Measured three ways: with room; on an inbox full of messages
    /// that cannot be dropped, where the arrival is refused; and on one full of
    /// heartbeats, where a carrier makes room for itself.
    #[test]
    fn an_admission_examines_no_more_of_the_queue_as_the_queue_grows() {
        const COST: usize = 64;
        const LONGEST: usize = 4096;
        let mut with_room = Vec::new();
        let mut when_refused = Vec::new();
        let mut when_making_room = Vec::new();
        let mut length = 1;
        while length <= LONGEST {
            let roomy = Inbox::new(COST * (length + 1));
            let full = Inbox::new(COST * length);
            let beats = Inbox::new(COST * length);
            for i in 0..length as u64 {
                assert!(roomy.admit(message(2, i, COST)).is_admitted());
                assert!(full.admit(message(2, i, COST)).is_admitted());
                // Spread over pairs, so the index has something to choose between.
                assert!(beats.admit(beat(i % 3 + 1, i % 7 + 1, COST)).is_admitted());
            }
            let (before_roomy, before_full, before_beats) =
                (roomy.probes(), full.probes(), beats.probes());
            assert!(roomy.admit(message(9, 0, COST)).is_admitted());
            assert!(!full.admit(message(9, 0, COST)).is_admitted());
            let made_room = beats.admit(carrier(9, 9, COST));
            assert!(made_room.is_admitted(), "a carrier is never starved");
            assert_eq!(made_room.dropped().len(), 1, "one heartbeat of one cost");
            with_room.push((length, roomy.probes() - before_roomy));
            when_refused.push((length, full.probes() - before_full));
            when_making_room.push((length, beats.probes() - before_beats));
            length *= 2;
        }
        for (name, rows) in [
            ("with room", &with_room),
            ("when refused", &when_refused),
            ("when making room", &when_making_room),
        ] {
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
        }
        // As built the constants are exact: an admission that needs no room examines
        // nothing at any length, a refusal examines nothing, and one that makes room
        // examines one entry per heartbeat it drops, each found through the index.
        assert!(
            with_room.iter().all(|&(_, probes)| probes == 0),
            "with room: {with_room:?}"
        );
        assert!(
            when_refused.iter().all(|&(_, probes)| probes == 0),
            "when refused: {when_refused:?}"
        );
        assert!(
            when_making_room.iter().all(|&(_, probes)| probes == 1),
            "when making room: {when_making_room:?}"
        );
    }

    /// The other half of Q14's figure: over a run, the entries examined are bounded
    /// by the messages admitted. Every entry examined is a message leaving — dropped
    /// or popped — and a message leaves once, plus one more look at the hole a drop
    /// leaves behind when the drain reaches it. So at most two per admission, and the
    /// cost is constant however long the queue grows.
    #[test]
    fn the_entries_examined_over_a_run_are_bounded_by_the_messages_admitted() {
        let inbox = Inbox::new(4096);
        let mut admitted = 0u64;
        for round in 0..2000u64 {
            if inbox
                .admit(beat(round % 3 + 1, round % 11 + 1, 65))
                .is_admitted()
            {
                admitted += 1;
            }
            if round % 3 == 0 && inbox.admit(carrier(1, round % 11 + 1, 93)).is_admitted() {
                admitted += 1;
            }
            if round % 7 == 0 {
                drain(&inbox);
            }
        }
        drain(&inbox);
        let probes = inbox.probes();
        println!("{admitted} messages admitted, {probes} queue entries examined in all");
        assert!(
            probes <= 2 * admitted,
            "{probes} entries examined for {admitted} admissions"
        );
    }

    /// The guarantee is the shape of `Slots`, not a number that happens to be zero.
    ///
    /// Its whole surface is `push_back`, `pop_front`, `take_noisiest_heartbeat`,
    /// `len`, `heartbeats`, `heartbeat_bytes` and `probes`. The two that reach a
    /// queued message both count it; the rest cannot reach one — there is no
    /// iterator, no indexing, no `front`, no `get`, no borrow. So a linear admission
    /// can only be written out of those two, and this test writes both traversals and
    /// shows the count moving by one per entry, to a figure that fails the assertion
    /// the measurement above makes.
    #[test]
    fn a_scan_of_the_queue_cannot_be_written_without_the_count_moving() {
        const LENGTH: usize = 64;
        // The bound the measurement above asserts, at length 1's zero probes.
        let fails_q14 = |probes: u64| probes > 2 * u64::from(LENGTH.ilog2());
        // The traversal `pop_front` allows: drain the queue and look at each.
        let inbox = Inbox::new(LENGTH * 64);
        for i in 0..LENGTH as u64 {
            assert!(inbox.admit(message(2, i, 64)).is_admitted());
        }
        let before = inbox.probes();
        assert_eq!(drain(&inbox).len(), LENGTH);
        let probes = inbox.probes() - before;
        assert_eq!(probes, LENGTH as u64, "one probe an entry, every entry");
        assert!(
            fails_q14(probes),
            "and a scan of that size fails Q14's bound"
        );
        // The traversal `take_noisiest_heartbeat` allows: one admission that has to
        // take every heartbeat in the queue.
        let inbox = Inbox::new(LENGTH * 64);
        for i in 0..LENGTH as u64 {
            assert!(inbox.admit(beat(i % 3 + 1, i % 7 + 1, 64)).is_admitted());
        }
        let before = inbox.probes();
        let admission = inbox.admit(carrier(9, 9, LENGTH * 64));
        assert!(admission.is_admitted());
        assert_eq!(
            admission.dropped().len(),
            LENGTH,
            "it had to take every heartbeat"
        );
        let probes = inbox.probes() - before;
        assert_eq!(probes, LENGTH as u64, "and examined one entry for each");
        assert!(
            fails_q14(probes),
            "an admission whose cost is the queue's length cannot hide behind this \
             counter: {probes} entries examined"
        );
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
        let built = builder.finish();
        let messages = decode(&built).expect("a frame this crate wrote").messages;
        // The frame's header is charged to its first message and to no other, so the
        // messages' costs sum to what the node took off the wire.
        assert_eq!(
            messages[0].bytes,
            HEADER_LEN + TAG_LEN + frame(1, 8).encode().len()
        );
        assert_eq!(messages[1].bytes, TAG_LEN + frame(1, 3).encode().len());
        assert_eq!(
            messages.iter().map(|m| m.bytes).sum::<usize>(),
            built.len(),
            "the frame's bytes are charged to its messages exactly"
        );
        // Room for the first two of the three and nothing more. Every one of them
        // carries entries, and there is no heartbeat to make room from.
        let room = messages[0].bytes + messages[1].bytes;
        let inbox = Inbox::new(room);
        let admitted: Vec<_> = messages
            .into_iter()
            .map(|tagged| inbox.admit(Received::from(tagged)))
            .collect();
        assert!(admitted[0].is_admitted());
        assert!(admitted[1].is_admitted());
        let Admission::Refused(refused) = &admitted[2] else {
            panic!("the third does not fit and no heartbeat can be dropped for it")
        };
        assert_eq!(
            refused.range,
            RangeId(7),
            "and is handed back with its range"
        );
        assert_eq!(inbox.queued_bytes(), room);
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

    /// Dropping heartbeats out of the middle leaves the messages around them in the
    /// order they arrived, and the accounting exact.
    #[test]
    fn a_drop_from_the_middle_keeps_the_order_and_the_bytes_of_everything_else() {
        let inbox = Inbox::new(500);
        assert!(inbox.admit(message(1, 1, 100)).is_admitted());
        assert!(inbox.admit(beat(1, 5, 100)).is_admitted());
        assert!(inbox.admit(message(3, 3, 100)).is_admitted());
        assert!(inbox.admit(beat(1, 5, 100)).is_admitted());
        assert!(inbox.admit(message(5, 5, 100)).is_admitted());
        assert_eq!(inbox.queued_bytes(), 500);
        let admission = inbox.admit(carrier(2, 9, 200));
        assert_eq!(admission.dropped().len(), 2, "both heartbeats of the pair");
        assert_eq!(inbox.queued_bytes(), 500);
        assert_eq!(inbox.len(), 4);
        let drained = drain(&inbox);
        assert_eq!(
            drained.iter().map(|m| m.range).collect::<Vec<_>>(),
            vec![RangeId(1), RangeId(3), RangeId(5), RangeId(9)],
            "the messages around the holes, in the order they arrived"
        );
        assert_eq!(inbox.queued_bytes(), 0);
        assert!(inbox.is_empty());
    }
}
