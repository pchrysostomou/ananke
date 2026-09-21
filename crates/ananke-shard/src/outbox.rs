//! The per-peer outbox (SHARD.md §4): every send a node makes leaves through it, "one
//! frame per peer per flush, cut under `MAX_FRAME_LEN`".
//!
//! A node's sends are the sends of every range it holds, so the outbox is where many
//! ranges' messages to one peer become one frame. [`Outbox::push`] queues a message
//! for a peer under its range; [`Outbox::flush`] turns what is queued into at most one
//! frame per peer. A message that does not fit the frame being cut stays at the head of
//! its peer's queue and starts the next flush's frame, so the order a peer's messages
//! were queued in is the order they go out in.
//!
//! Nothing here sends: a flush hands the frames back and the caller puts them on the
//! socket. The `raft` task's round, which decides *when* a flush happens (Q41), is a
//! later stage's.
//!
//! Snapshot chunks do not come through here: they go in frames of their own on the
//! snapshot task's own socket handle, so a 256 KiB chunk never shares a frame with a
//! heartbeat (SHARD.md §4, Q41).

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use ananke_env::MAX_FRAME_LEN;
use ananke_raft::message::Frame;
use ananke_raft::types::ServerId;
use bytes::Bytes;

use crate::frame::{Builder, encoded_len};
use crate::range::RangeId;

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

/// See the module documentation.
pub struct Outbox {
    cap: usize,
    peers: BTreeMap<ServerId, VecDeque<(RangeId, Bytes)>>,
}

impl Default for Outbox {
    fn default() -> Self {
        Self::new()
    }
}

impl Outbox {
    /// An empty outbox cutting frames under [`MAX_FRAME_LEN`], the socket's cap.
    #[must_use]
    pub fn new() -> Self {
        Self::with_frame_cap(MAX_FRAME_LEN)
    }

    /// An empty outbox cutting frames under `cap` bytes: what a test that must see
    /// the cut uses, since `MAX_FRAME_LEN` is 16 MiB.
    #[must_use]
    pub fn with_frame_cap(cap: usize) -> Self {
        assert!(cap > crate::frame::HEADER_LEN, "a frame holds a header");
        Self {
            cap,
            peers: BTreeMap::new(),
        }
    }

    /// The cap a frame is cut under.
    #[must_use]
    pub fn frame_cap(&self) -> usize {
        self.cap
    }

    /// Queues `frame` for `to`, tagged with `range`.
    ///
    /// The message is encoded here, so a flush is the cut and nothing else.
    ///
    /// # Errors
    ///
    /// [`Oversized`] if the message would not fit in a frame of its own, in which case
    /// nothing is queued.
    pub fn push(&mut self, to: ServerId, range: RangeId, frame: &Frame) -> Result<(), Oversized> {
        let encoded = frame.encode();
        if encoded_len(encoded.len()) > self.cap {
            return Err(Oversized {
                to,
                range,
                len: encoded.len(),
            });
        }
        self.peers
            .entry(to)
            .or_default()
            .push_back((range, encoded));
        Ok(())
    }

    /// How many messages are queued for every peer together.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.peers.values().map(VecDeque::len).sum()
    }

    /// How many messages are queued for `to`.
    #[must_use]
    pub fn queued_to(&self, to: ServerId) -> usize {
        self.peers.get(&to).map_or(0, VecDeque::len)
    }

    /// Whether anything is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queued() == 0
    }

    /// One frame per peer with something queued, each cut under the cap, in peer
    /// order.
    ///
    /// A message that does not fit the frame being cut is left queued, at the head of
    /// its peer's queue, and starts the next flush's frame. Since [`push`](Self::push)
    /// refuses a message that could not fit an empty frame, every flush of a
    /// non-empty queue takes at least one message from it and a queue always drains.
    pub fn flush(&mut self) -> Vec<(ServerId, Bytes)> {
        let cap = self.cap;
        let mut frames = Vec::new();
        for (to, queued) in &mut self.peers {
            if queued.is_empty() {
                continue;
            }
            let mut builder = Builder::new(cap);
            while let Some((range, message)) = queued.front() {
                if !builder.fits(message.len()) {
                    // PROPOSED(D-072): the cut is before the message that would
                    // overflow the frame, and that message starts the next one.
                    break;
                }
                builder.push(*range, message);
                queued.pop_front();
            }
            debug_assert!(!builder.is_empty(), "a flush always makes progress");
            frames.push((*to, builder.finish()));
        }
        self.peers.retain(|_, queued| !queued.is_empty());
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
    /// messages in the order they were queued, each under the range it was pushed
    /// with.
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
        let frames = outbox.flush();
        assert_eq!(frames.len(), 2, "one frame a peer");
        assert_eq!(frames[0].0, ServerId(2));
        assert_eq!(frames[1].0, ServerId(3));
        let to_two = decode(&frames[0].1).expect("a frame this crate wrote");
        assert_eq!(
            to_two.iter().map(|t| t.range).collect::<Vec<_>>(),
            vec![RangeId(5), RangeId(9), RangeId(7)],
            "each message under the range it was pushed with, in order"
        );
        assert_eq!(to_two[1].frame.message.term(), 4);
        let to_three = decode(&frames[1].1).expect("a frame this crate wrote");
        assert_eq!(to_three.len(), 1);
        assert_eq!(to_three[0].range, RangeId(5));
        assert!(outbox.is_empty(), "a flush empties what it sent");
        assert!(outbox.flush().is_empty(), "and sends nothing next time");
    }

    /// The cut: no frame over the cap, nothing lost, nothing reordered, and the
    /// message that would have overflowed a frame at the head of the next one.
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
            for tagged in decode(frame).expect("a frame this crate wrote") {
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
        let four_mib = 4 * 1024 * 1024;
        for term in 0..5 {
            outbox
                .push(
                    ServerId(2),
                    RangeId(term + 1),
                    &with_command(term, four_mib),
                )
                .unwrap();
        }
        let first = outbox.flush();
        assert_eq!(first.len(), 1);
        assert!(first[0].1.len() <= MAX_FRAME_LEN);
        assert_eq!(decode(&first[0].1).expect("a frame").len(), 3);
        assert_eq!(outbox.queued(), 2);
        let second = outbox.flush();
        assert!(second[0].1.len() <= MAX_FRAME_LEN);
        let rest = decode(&second[0].1).expect("a frame");
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
        assert_eq!(decode(&frames[0].1).expect("a frame")[0].frame, fits);
    }
}
