//! The per-peer outbox (SHARD.md §4): every send a node makes leaves through it, "one
//! frame per peer per flush, cut under `MAX_FRAME_LEN`".
//!
//! A node's sends are the sends of every range it holds, so the outbox is where many
//! ranges' messages to one peer become one frame. [`Outbox::push`] queues a message
//! for a peer under its range; [`Outbox::flush`] turns what is queued into at most one
//! frame per peer.
//!
//! **A queue per (peer, range), cut round-robin.** One queue per peer would put every
//! range behind whichever range spoke first: eight in-flight AppendEntries for one
//! range, each near the frame cap, hold every other range's heartbeat to that peer for
//! eight flushes, which at Q41's 10 ms tick is 80 ms against a minimum election
//! timeout of 100 ms (core.rs:373-377) — one slow range starting elections in three
//! hundred others. So the queue is keyed by (peer, range), each range keeps its own
//! FIFO, and a frame is cut by taking one message from each range in turn, starting
//! after the range the last frame was cut at. Order within a range is the order it was
//! queued in, which is all Raft asks; order between ranges is the round-robin's, which
//! is what keeps one range from spending another's frame.
//!
//! A range whose next message does not fit the frame being cut is left for the next
//! flush and leads it, so no range is passed over twice running.
//!
//! **Bounded, per peer, in bytes.** The queue in front of the socket is bounded —
//! 1 024 frames a destination, oldest dropped, traced as `MessageDropped`
//! (`ananke_env::net`) — and an unbounded queue in front of that one bounds nothing:
//! a peer that is not draining would grow this one without limit and the socket's
//! bound would never be reached. Each peer's queue therefore holds at most
//! [`DEFAULT_PEER_BOUND`] bytes of frame, and a push over that drops the oldest
//! message queued for that peer, whatever range it is about, and hands it back as
//! [`Dropped`] so the caller traces it exactly as the socket's own drop is traced. A
//! message is never dropped for one of another peer.
//!
//! Nothing here sends: a flush hands the frames back and the caller puts them on the
//! socket. The `raft` task's round, which decides *when* a flush happens (Q41), is a
//! later stage's.
//!
//! Snapshot chunks do not come through here: they go in frames of their own on the
//! snapshot task's own socket handle, so a 256 KiB chunk never shares a frame with a
//! heartbeat (SHARD.md §4, Q41).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use ananke_env::MAX_FRAME_LEN;
use ananke_raft::message::Frame;
use ananke_raft::types::ServerId;
use bytes::Bytes;

use crate::frame::{Builder, TAG_LEN, encoded_len};
use crate::range::RangeId;

/// The bytes one peer's queue holds before the oldest message queued for that peer is
/// dropped: two frames' worth, the frame a flush is about to cut and one behind it.
///
/// A peer's queue is what has not left yet, and a flush takes a frame of it every
/// round (Q41), so a peer more than one frame behind is holding messages Raft will
/// have retransmitted before they go out. At §4's largest idle frame, 17.3 kB, it is
/// about 1 900 rounds of headroom; at the 4 MiB entry batches the core can build, it
/// is eight of them. It is a bound and not a tuning: what a node's queues actually
/// reach is the node's stage's to measure, with the drops traced.
// PROPOSED(D-072): the outbox is bounded per peer, in bytes, dropping the oldest.
pub const DEFAULT_PEER_BOUND: usize = 2 * MAX_FRAME_LEN;

/// A message that cannot be sent, because it would not fit in a frame of its own.
///
/// The outbox refuses it at [`Outbox::push`] and queues nothing: it is neither
/// truncated, nor split across frames — nothing on the receiving side reassembles a
/// message — nor queued to be refused later by the socket, which fails any frame over
/// `MAX_FRAME_LEN` anyway. The caller is told, in front of the send it asked for.
// PROPOSED(D-072): a message larger than a frame is refused at the outbox.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Oversized {
    /// The peer the message was for.
    pub to: ServerId,
    /// The range it was about.
    pub range: RangeId,
    /// Its length in bytes, which with its tag and a frame header is over the cap.
    pub len: usize,
}

impl fmt::Display for Oversized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a message of {} bytes for {} of server {} does not fit a frame",
            self.len, self.range, self.to.0
        )
    }
}

impl std::error::Error for Oversized {}

/// A message dropped from a peer's queue because the queue was full: the oldest
/// message queued for that peer, whatever range it was about.
///
/// The caller traces it as the drop it is, as `ananke_env::net` traces
/// `MessageDropped` for the frame its own bounded queue drops. To Raft it is a message
/// the network lost, which this transport may do at any time and which a
/// retransmission answers.
// PROPOSED(D-072): the outbox's bound drops the oldest, as the socket's queue does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dropped {
    /// The peer it was queued for.
    pub to: ServerId,
    /// The range it was about.
    pub range: RangeId,
    /// Its length in bytes, without its tag.
    pub len: usize,
}

impl fmt::Display for Dropped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a message of {} bytes for {} of server {} was dropped from a full outbox",
            self.len, self.range, self.to.0
        )
    }
}

/// One peer's queues: one per range, and the arrival order across them, which is what
/// "the oldest" means when the bound drops something.
#[derive(Default)]
struct Peer {
    ranges: BTreeMap<RangeId, VecDeque<(u64, Bytes)>>,
    /// Every queued message as (arrival, range), so the oldest is `first` and a
    /// message taken by a flush is removed in logarithmic time.
    arrivals: BTreeSet<(u64, RangeId)>,
    next: u64,
    /// The frame bytes queued for this peer: each message's tag and its length.
    bytes: usize,
    /// The range the next frame starts at: the first range the last frame had to
    /// pass over for want of room, so no range is passed over twice running.
    resume: Option<RangeId>,
}

impl Peer {
    fn oldest(&self) -> Option<(u64, RangeId)> {
        self.arrivals.iter().next().copied()
    }

    /// Removes one message, by arrival and range, and gives back its length.
    fn take(&mut self, arrival: u64, range: RangeId) -> usize {
        let queued = self.ranges.get_mut(&range).expect("an arrival has a range");
        let (found, message) = queued.pop_front().expect("an arrival is queued");
        debug_assert_eq!(found, arrival, "a range's messages leave in arrival order");
        if queued.is_empty() {
            self.ranges.remove(&range);
        }
        self.arrivals.remove(&(arrival, range));
        self.bytes -= TAG_LEN + message.len();
        message.len()
    }

    fn queued(&self) -> usize {
        self.arrivals.len()
    }

    /// This peer's ranges in the order the next frame visits them: from `resume`,
    /// wrapping round to it.
    fn rotation(&self) -> VecDeque<RangeId> {
        let from = self.resume.unwrap_or(RangeId(0));
        self.ranges
            .range(from..)
            .chain(self.ranges.range(..from))
            .map(|(range, _)| *range)
            .collect()
    }
}

/// See the module documentation.
pub struct Outbox {
    cap: usize,
    bound: usize,
    peers: BTreeMap<ServerId, Peer>,
}

impl Default for Outbox {
    fn default() -> Self {
        Self::new()
    }
}

impl Outbox {
    /// An empty outbox cutting frames under [`MAX_FRAME_LEN`], the socket's cap, and
    /// holding at most [`DEFAULT_PEER_BOUND`] bytes for any one peer.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bounds(MAX_FRAME_LEN, DEFAULT_PEER_BOUND)
    }

    /// An empty outbox cutting frames under `cap` bytes: what a test that must see
    /// the cut uses, since `MAX_FRAME_LEN` is 16 MiB. The per-peer bound is
    /// [`DEFAULT_PEER_BOUND`] still, since a smaller frame is not a smaller queue.
    #[must_use]
    pub fn with_frame_cap(cap: usize) -> Self {
        Self::with_bounds(cap, DEFAULT_PEER_BOUND)
    }

    /// An empty outbox cutting frames under `cap` bytes and holding at most `bound`
    /// bytes for any one peer.
    ///
    /// # Panics
    ///
    /// If `cap` is not larger than a frame's header, or if `bound` is below what a
    /// message of the largest size [`push`] accepts costs in the queue — `cap` less a
    /// frame's header, since the queue counts each message's tag and length and not
    /// the header it will share. A queue smaller than that would drop a message
    /// `push` had just accepted before any flush could carry it.
    ///
    /// [`push`]: Self::push
    #[must_use]
    pub fn with_bounds(cap: usize, bound: usize) -> Self {
        assert!(cap > crate::frame::HEADER_LEN, "a frame holds a header");
        assert!(
            bound >= cap - crate::frame::HEADER_LEN,
            "a peer's queue holds the largest message a push accepts"
        );
        Self {
            cap,
            bound,
            peers: BTreeMap::new(),
        }
    }

    /// The cap a frame is cut under.
    #[must_use]
    pub fn frame_cap(&self) -> usize {
        self.cap
    }

    /// The bytes one peer's queue holds before the oldest queued for it is dropped.
    #[must_use]
    pub fn peer_bound(&self) -> usize {
        self.bound
    }

    /// Queues `frame` for `to`, tagged with `range`, and gives back what had to be
    /// dropped from that peer's queue to fit it under the bound, oldest first.
    ///
    /// The message is encoded here, so a flush is the cut and nothing else.
    ///
    /// # Errors
    ///
    /// [`Oversized`] if the message would not fit in a frame of its own, in which case
    /// nothing is queued and nothing is dropped.
    pub fn push(
        &mut self,
        to: ServerId,
        range: RangeId,
        frame: &Frame,
    ) -> Result<Vec<Dropped>, Oversized> {
        let encoded = frame.encode();
        if encoded_len(encoded.len()) > self.cap {
            return Err(Oversized {
                to,
                range,
                len: encoded.len(),
            });
        }
        let bound = self.bound;
        let peer = self.peers.entry(to).or_default();
        let arrival = peer.next;
        peer.next += 1;
        peer.bytes += TAG_LEN + encoded.len();
        peer.ranges
            .entry(range)
            .or_default()
            .push_back((arrival, encoded));
        peer.arrivals.insert((arrival, range));
        // PROPOSED(D-072): over the bound, the oldest message queued for this peer
        // goes, as the socket's own queue drops its oldest frame.
        let mut dropped = Vec::new();
        while peer.bytes > bound {
            let (oldest, from) = peer.oldest().expect("a queue over its bound holds one");
            let len = peer.take(oldest, from);
            dropped.push(Dropped {
                to,
                range: from,
                len,
            });
        }
        if peer.queued() == 0 {
            self.peers.remove(&to);
        }
        Ok(dropped)
    }

    /// How many messages are queued for every peer together.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.peers.values().map(Peer::queued).sum()
    }

    /// How many messages are queued for `to`.
    #[must_use]
    pub fn queued_to(&self, to: ServerId) -> usize {
        self.peers.get(&to).map_or(0, Peer::queued)
    }

    /// How many messages are queued for `to` about `range`.
    #[must_use]
    pub fn queued_to_range(&self, to: ServerId, range: RangeId) -> usize {
        self.peers
            .get(&to)
            .and_then(|peer| peer.ranges.get(&range))
            .map_or(0, VecDeque::len)
    }

    /// The frame bytes queued for `to`: what the per-peer bound is measured against.
    #[must_use]
    pub fn queued_bytes_to(&self, to: ServerId) -> usize {
        self.peers.get(&to).map_or(0, |peer| peer.bytes)
    }

    /// Whether anything is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// One frame per peer with something queued, each cut under the cap, in peer
    /// order.
    ///
    /// A frame is cut by taking one message from each of the peer's ranges in turn,
    /// starting at the range the last frame was cut at, so a range with a backlog
    /// cannot hold every other range's messages behind it. A range whose next message
    /// does not fit the frame being cut is left for the next flush and leads it.
    /// Since [`push`](Self::push) refuses a message that could not fit an empty frame,
    /// every flush of a non-empty queue takes at least one message from it and a queue
    /// always drains.
    pub fn flush(&mut self) -> Vec<(ServerId, Bytes)> {
        let cap = self.cap;
        let mut frames = Vec::new();
        for (to, peer) in &mut self.peers {
            if peer.queued() == 0 {
                continue;
            }
            let mut builder = Builder::new(cap);
            let mut turns = peer.rotation();
            let mut passed_over: Option<RangeId> = None;
            while let Some(range) = turns.pop_front() {
                let Some((arrival, message)) =
                    peer.ranges.get(&range).and_then(VecDeque::front).cloned()
                else {
                    continue;
                };
                if !builder.fits(message.len()) {
                    // PROPOSED(D-072): the cut is before the message that would
                    // overflow the frame, and the range it belongs to leads the next.
                    passed_over = passed_over.or(Some(range));
                    continue;
                }
                builder.push(range, &message);
                peer.take(arrival, range);
                if peer.ranges.contains_key(&range) {
                    turns.push_back(range);
                }
            }
            debug_assert!(!builder.is_empty(), "a flush always makes progress");
            peer.resume = passed_over;
            frames.push((*to, builder.finish()));
        }
        self.peers.retain(|_, peer| peer.queued() > 0);
        frames
    }
}

#[cfg(test)]
mod tests {
    use ananke_raft::message::Message;
    use ananke_raft::types::{Entry, Payload};

    use super::*;
    use crate::frame::decode;

    fn heartbeat(term: u64, from: u64) -> Frame {
        Frame {
            from: ServerId(from),
            message: Message::AppendEntries {
                term,
                prev_index: 7,
                prev_term: 2,
                entries: Vec::new(),
                commit: 6,
                sent: 123_456_789,
            },
        }
    }

    /// An AppendEntries carrying one command of `len` bytes: the message whose size
    /// the sender chooses, and so the one that decides where a frame is cut.
    fn with_command(term: u64, len: usize) -> Frame {
        Frame {
            from: ServerId(1),
            message: Message::AppendEntries {
                term,
                prev_index: 7,
                prev_term: 2,
                entries: vec![Entry {
                    term,
                    index: 8,
                    payload: Payload::Command(Bytes::from(vec![7u8; len])),
                }],
                commit: 6,
                sent: 123_456_789,
            },
        }
    }

    /// Two peers, three ranges, one flush: each peer gets one frame, carrying its own
    /// messages, each under the range it was pushed with. Within a range the order is
    /// the order it was queued in; between ranges it is the round-robin's, which is
    /// the range order the rotation starts from.
    #[test]
    fn a_flush_is_one_frame_a_peer_carrying_every_range_queued_for_it() {
        let mut outbox = Outbox::new();
        outbox
            .push(ServerId(2), RangeId(5), &heartbeat(3, 1))
            .unwrap();
        outbox
            .push(ServerId(3), RangeId(5), &heartbeat(3, 1))
            .unwrap();
        outbox
            .push(ServerId(2), RangeId(9), &heartbeat(4, 1))
            .unwrap();
        outbox
            .push(ServerId(2), RangeId(7), &heartbeat(5, 1))
            .unwrap();
        assert_eq!(outbox.queued(), 4);
        assert_eq!(outbox.queued_to(ServerId(2)), 3);
        assert_eq!(outbox.queued_to_range(ServerId(2), RangeId(9)), 1);
        let frames = outbox.flush();
        assert_eq!(frames.len(), 2, "one frame a peer");
        assert_eq!(frames[0].0, ServerId(2));
        assert_eq!(frames[1].0, ServerId(3));
        let to_two = decode(&frames[0].1)
            .expect("a frame this crate wrote")
            .messages;
        assert_eq!(
            to_two.iter().map(|t| t.range).collect::<Vec<_>>(),
            vec![RangeId(5), RangeId(7), RangeId(9)],
            "one message of each range in turn, each under the range it was pushed with"
        );
        assert_eq!(
            to_two
                .iter()
                .find(|t| t.range == RangeId(9))
                .expect("range 9")
                .frame
                .message
                .term(),
            4
        );
        let to_three = decode(&frames[1].1)
            .expect("a frame this crate wrote")
            .messages;
        assert_eq!(to_three.len(), 1);
        assert_eq!(to_three[0].range, RangeId(5));
        assert!(outbox.is_empty(), "a flush empties what it sent");
        assert!(outbox.flush().is_empty(), "and sends nothing next time");
    }

    /// A range's own messages keep their order, whatever the round-robin does with
    /// the ranges between them.
    #[test]
    fn a_range_s_messages_leave_in_the_order_they_were_queued() {
        let mut outbox = Outbox::new();
        for term in 0..5 {
            outbox
                .push(ServerId(2), RangeId(5), &heartbeat(term, 1))
                .unwrap();
            outbox
                .push(ServerId(2), RangeId(9), &heartbeat(100 + term, 1))
                .unwrap();
        }
        let frames = outbox.flush();
        let carried = decode(&frames[0].1)
            .expect("a frame this crate wrote")
            .messages;
        let of = |range: RangeId| {
            carried
                .iter()
                .filter(|t| t.range == range)
                .map(|t| t.frame.message.term())
                .collect::<Vec<_>>()
        };
        assert_eq!(of(RangeId(5)), vec![0, 1, 2, 3, 4], "range 5's own order");
        assert_eq!(
            of(RangeId(9)),
            vec![100, 101, 102, 103, 104],
            "range 9's own order"
        );
        // And they alternate, which is the round-robin: neither range waits for the
        // other to finish.
        assert_eq!(
            carried.iter().map(|t| t.range).take(4).collect::<Vec<_>>(),
            vec![RangeId(5), RangeId(9), RangeId(5), RangeId(9)]
        );
    }

    /// One range's backlog must not hold every other range's messages to the same
    /// peer behind it. Eight in-flight AppendEntries for one range against 299 ranges
    /// with a heartbeat each: with one queue a peer the 299 are behind all eight, up
    /// to eight flushes away, which at Q41's 10 ms tick is 80 ms against a minimum
    /// election timeout of 100 ms — one slow range starting elections in 299 others.
    /// With a queue a (peer, range) every one of the 299 leaves in the first frame,
    /// and the backlogged range leads that frame too rather than being starved in
    /// its turn.
    #[test]
    fn a_range_with_a_backlog_does_not_delay_every_other_range_s_messages_to_a_peer() {
        // A cap that holds one of the eight and every one of the 299 heartbeats.
        let cap = 24 * 1024;
        let mut outbox = Outbox::with_frame_cap(cap);
        let big = with_command(3, 4 * 1024);
        for _ in 0..8 {
            outbox.push(ServerId(2), RangeId(1), &big).unwrap();
        }
        for range in 2..=300u64 {
            outbox
                .push(ServerId(2), RangeId(range), &heartbeat(3, 1))
                .unwrap();
        }
        let mut left_at: BTreeMap<RangeId, usize> = BTreeMap::new();
        let mut flushes = 0;
        while !outbox.is_empty() {
            flushes += 1;
            let frames = outbox.flush();
            assert_eq!(frames.len(), 1, "one frame a peer a flush");
            assert!(frames[0].1.len() <= cap);
            for tagged in decode(&frames[0].1).expect("a frame").messages {
                left_at.entry(tagged.range).or_insert(flushes);
            }
            assert!(flushes <= 16, "a flush must make progress");
        }
        let heartbeats_delayed = left_at
            .iter()
            .filter(|(range, _)| **range != RangeId(1))
            .map(|(_, at)| *at)
            .max()
            .expect("299 heartbeats");
        println!(
            "8 in-flight AppendEntries for one range against 299 other ranges: \
             {flushes} flushes in all, and the last heartbeat left on flush \
             {heartbeats_delayed}"
        );
        assert_eq!(
            heartbeats_delayed, 1,
            "every other range's heartbeat leaves in the first frame"
        );
        assert!(
            flushes > 1,
            "the backlogged range really does need more than one frame: {flushes}"
        );
        assert_eq!(left_at.len(), 300, "and every range was carried");
        assert_eq!(
            left_at.get(&RangeId(1)),
            Some(&1),
            "the backlogged range is not starved either: it leads the first frame"
        );
    }

    /// The cut: no frame over the cap, nothing lost, nothing reordered within a
    /// range, and the range that was passed over leading the next frame.
    #[test]
    fn a_message_that_would_overflow_a_frame_starts_the_next_one() {
        let message = heartbeat(3, 1).encode().len();
        // A cap that holds three heartbeats and not a fourth, with 60 bytes over: more
        // than a fourth heartbeat on its own (53) and less than a fourth heartbeat with
        // its tag (65), so a cut that counts a message without its tag puts a fourth in
        // and the frame goes over the cap.
        let cap = crate::frame::HEADER_LEN + 3 * (crate::frame::TAG_LEN + message) + 60;
        let mut outbox = Outbox::with_frame_cap(cap);
        for term in 0..10 {
            outbox
                .push(ServerId(2), RangeId(term + 1), &heartbeat(term, 1))
                .unwrap();
        }
        let mut seen = Vec::new();
        let mut flushes = 0;
        while !outbox.is_empty() {
            let frames = outbox.flush();
            flushes += 1;
            assert_eq!(frames.len(), 1, "one frame a peer a flush");
            let (to, frame) = &frames[0];
            assert_eq!(*to, ServerId(2));
            assert!(
                frame.len() <= cap,
                "a frame of {} bytes over the cap of {cap}",
                frame.len()
            );
            for tagged in decode(frame).expect("a frame this crate wrote").messages {
                seen.push((tagged.range, tagged.frame.message.term()));
            }
            assert!(flushes <= 10, "a flush must make progress");
        }
        assert_eq!(flushes, 4, "three, three, three and one");
        assert_eq!(
            seen,
            (0..10).map(|t| (RangeId(t + 1), t)).collect::<Vec<_>>(),
            "every message, in order, under its own range"
        );
    }

    /// The same cut at the socket's real cap, which is where it has to hold: five
    /// messages of four MiB each are three frames and then two, and no frame is over
    /// `MAX_FRAME_LEN`.
    #[test]
    fn the_cut_holds_at_the_socket_s_own_cap() {
        let mut outbox = Outbox::new();
        assert_eq!(outbox.frame_cap(), MAX_FRAME_LEN);
        assert_eq!(outbox.peer_bound(), DEFAULT_PEER_BOUND);
        let four_mib = 4 * 1024 * 1024;
        for term in 0..5 {
            assert!(
                outbox
                    .push(
                        ServerId(2),
                        RangeId(term + 1),
                        &with_command(term, four_mib),
                    )
                    .unwrap()
                    .is_empty(),
                "under the peer's bound, nothing is dropped"
            );
        }
        let first = outbox.flush();
        assert_eq!(first.len(), 1);
        assert!(first[0].1.len() <= MAX_FRAME_LEN);
        assert_eq!(decode(&first[0].1).expect("a frame").messages.len(), 3);
        assert_eq!(outbox.queued(), 2);
        let second = outbox.flush();
        assert!(second[0].1.len() <= MAX_FRAME_LEN);
        let rest = decode(&second[0].1).expect("a frame").messages;
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].range, RangeId(4), "where the first frame was cut");
        assert!(outbox.is_empty());
    }

    /// A message no frame could hold is refused in front of the send, and nothing is
    /// queued: the alternative is a frame the socket refuses, or a message split
    /// across frames that nothing on the other side puts together.
    #[test]
    fn a_message_larger_than_a_frame_is_refused_and_nothing_is_queued() {
        let cap = 4096;
        let mut outbox = Outbox::with_frame_cap(cap);
        let big = with_command(3, cap);
        let refused = outbox
            .push(ServerId(2), RangeId(9), &big)
            .expect_err("a message over the cap");
        assert_eq!(refused.to, ServerId(2));
        assert_eq!(refused.range, RangeId(9));
        assert_eq!(refused.len, big.encode().len());
        assert!(refused.to_string().contains("does not fit a frame"));
        assert!(outbox.is_empty(), "nothing queued");
        assert!(outbox.flush().is_empty());
        // The largest message that does fit is queued and goes out whole.
        let fits = with_command(3, cap - encoded_len(big.encode().len() - cap));
        outbox.push(ServerId(2), RangeId(9), &fits).unwrap();
        let frames = outbox.flush();
        assert_eq!(frames[0].1.len(), encoded_len(fits.encode().len()));
        assert!(frames[0].1.len() <= cap);
        assert_eq!(
            decode(&frames[0].1).expect("a frame").messages[0].frame,
            fits
        );
    }

    /// The outbox sits in front of the one queue that was bounded — 1 024 frames a
    /// destination, oldest dropped, traced (`ananke_env::net`) — so it has to be
    /// bounded itself, or a peer that is not draining grows it without limit and the
    /// socket's bound is never reached. A hundred thousand pushes with no flush
    /// between them: the bound holds, the oldest go, and each of them is handed back
    /// to be traced.
    #[test]
    fn a_peer_s_queue_is_bounded_in_bytes_and_drops_the_oldest_over_it() {
        let bound = 64 * 1024;
        let mut outbox = Outbox::with_bounds(4096, bound);
        let mut dropped = 0usize;
        let mut pushes = 0usize;
        for term in 0..100_000u64 {
            let gone = outbox
                .push(ServerId(2), RangeId(term % 300 + 1), &heartbeat(term, 1))
                .unwrap();
            for drop in &gone {
                assert_eq!(drop.to, ServerId(2));
                assert!(drop.len > 0);
            }
            dropped += gone.len();
            pushes += 1;
            assert!(
                outbox.queued_bytes_to(ServerId(2)) <= bound,
                "the peer's queue is over its bound after {pushes} pushes"
            );
        }
        println!(
            "{pushes} pushes with no flush: {dropped} dropped, \
             {} bytes queued against a bound of {bound}",
            outbox.queued_bytes_to(ServerId(2))
        );
        assert!(
            dropped > 0,
            "an unbounded queue drops nothing; this one does"
        );
        assert_eq!(
            dropped + outbox.queued_to(ServerId(2)),
            pushes,
            "every message is either queued or handed back as dropped"
        );
        assert!(
            Dropped {
                to: ServerId(2),
                range: RangeId(1),
                len: 53
            }
            .to_string()
            .contains("dropped from a full outbox")
        );
        // Another peer's queue is its own: nothing here dropped anything of its.
        assert_eq!(outbox.queued_to(ServerId(3)), 0);
        outbox
            .push(ServerId(3), RangeId(1), &heartbeat(1, 1))
            .unwrap();
        assert_eq!(outbox.queued_to(ServerId(3)), 1);
        assert!(outbox.queued_bytes_to(ServerId(2)) <= bound);
    }

    /// The oldest dropped is the oldest of the peer, whatever range it is about, and
    /// the ranges around it keep their own order.
    #[test]
    fn the_message_dropped_is_the_oldest_queued_for_that_peer() {
        let message = crate::frame::TAG_LEN + heartbeat(3, 1).encode().len();
        let mut outbox = Outbox::with_bounds(crate::frame::HEADER_LEN + 3 * message, 3 * message);
        for (term, range) in [(0u64, 5u64), (1, 9), (2, 5)] {
            assert!(
                outbox
                    .push(ServerId(2), RangeId(range), &heartbeat(term, 1))
                    .unwrap()
                    .is_empty()
            );
        }
        let gone = outbox
            .push(ServerId(2), RangeId(7), &heartbeat(3, 1))
            .unwrap();
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].range, RangeId(5), "the oldest, which was range 5's");
        assert_eq!(outbox.queued_to(ServerId(2)), 3);
        let carried = decode(&outbox.flush()[0].1)
            .expect("a frame")
            .messages
            .iter()
            .map(|t| (t.range, t.frame.message.term()))
            .collect::<Vec<_>>();
        assert_eq!(
            carried,
            vec![(RangeId(5), 2), (RangeId(7), 3), (RangeId(9), 1)],
            "range 5 kept its later message, in its own order"
        );
    }
}
