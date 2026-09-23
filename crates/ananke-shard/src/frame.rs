//! The batch frame (SHARD.md §4): one frame, several messages, each tagged with the
//! range it is about (Q10).
//!
//! `ananke-raft`'s message codec is unchanged and wrapped, never re-implemented
//! (Q40): a message inside a frame is exactly the bytes
//! [`ananke_raft::Frame::encode`] produces, and this module writes a range and a
//! length in front of each so that one frame can carry several and a reader can find
//! where each one ends without parsing it.
//!
//! | field | bytes | |
//! |---|---|---|
//! | `version` | 1 | [`VERSION`]; any other value is refused |
//! | `count` | 4 | how many messages follow |
//! | *per message* `range` | 8 | the range id (Q10) |
//! | *per message* `len` | 4 | the message's length in bytes |
//! | *per message* the message | `len` | `ananke-raft`'s frame, byte for byte |
//!
//! Everything is little-endian, as the message codec is, so a message costs
//! [`TAG_LEN`] bytes more than it does on its own: the 8 bytes Q10 asks for and the
//! 4 that say where it ends. A frame costs [`HEADER_LEN`] on top of its messages.
//!
//! A frame that does not decode is a dropped message, never a panic. Framing and
//! content are refused separately, because they say different things. Framing that
//! does not parse — another version, a torn frame, a count that does not match — hides
//! where every message of the frame ends, so [`decode`] refuses the frame whole and
//! never reads a prefix of it. A frame whose framing parses but one of whose messages
//! the message codec refuses loses *that message only*: the messages beside it were
//! delimited by the framing and not by the parse, [`slices`] validated that framing
//! without reading any of them, and at §4's 222 messages to an idle frame refusing the
//! frame whole would turn one lost message into 222. [`Decoded::malformed`] counts
//! what was dropped so the caller can trace it, and [`studio`] shows the same frame
//! the same way, labelling the message it could not read.

use std::collections::BTreeSet;
use std::io;

use ananke_raft::message::Frame;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use moirae_trace::Json;

use crate::range::RangeId;

/// The batch frame format this crate writes and reads. A frame that starts with
/// anything else is refused rather than misread.
pub const VERSION: u8 = 1;

/// The bytes a frame costs before its first message: the version and the count.
pub const HEADER_LEN: usize = 1 + 4;

/// The bytes a message costs inside a frame beyond its own length: its range id
/// (8, Q10) and the length that says where it ends (4).
pub const TAG_LEN: usize = 8 + 4;

/// The bytes a frame of one message of `message_len` bytes occupies: the header, the
/// tag and the message. Each further message adds `TAG_LEN + its length`.
///
/// Saturating, as [`Builder::fits`] is: a length near `usize::MAX` is a length no
/// frame can hold, and it has to compare as one rather than wrap to a small number
/// that does.
#[must_use]
pub const fn encoded_len(message_len: usize) -> usize {
    HEADER_LEN
        .saturating_add(TAG_LEN)
        .saturating_add(message_len)
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

/// Builds one frame, message by message, and never goes over the cap it was given.
///
/// The caller asks [`Builder::fits`] before every [`Builder::push`]; a message that
/// does not fit starts the next frame, which is what the outbox does with it.
pub struct Builder {
    out: BytesMut,
    count: u32,
    cap: usize,
}

impl Builder {
    /// An empty frame that will not grow past `cap` bytes. `cap` is the socket's
    /// [`ananke_env::MAX_FRAME_LEN`] on the wire; a test or a transport with a
    /// smaller frame passes its own.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        let mut out = BytesMut::with_capacity(HEADER_LEN);
        out.put_u8(VERSION);
        out.put_u32_le(0);
        Self { out, count: 0, cap }
    }

    /// The bytes written so far, header included.
    #[must_use]
    pub fn len(&self) -> usize {
        self.out.len()
    }

    /// Whether no message has been pushed yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The messages pushed so far.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Whether a message of `message_len` bytes still fits under the cap.
    #[must_use]
    pub fn fits(&self, message_len: usize) -> bool {
        self.out
            .len()
            .saturating_add(TAG_LEN)
            .saturating_add(message_len)
            <= self.cap
    }

    /// Appends `message`, tagged `range`.
    ///
    /// # Panics
    ///
    /// If the message does not [`fit`](Self::fits). The cap is what the socket will
    /// carry, so a frame built past it could only be refused by the socket or
    /// truncated on it; a caller that has not asked is a bug here, not a bad frame
    /// from the wire.
    ///
    /// If the message is empty. `Frame::decode` refuses zero bytes, so an empty
    /// message is 12 bytes of frame that every reader of it drops — a message written
    /// that no message comes back from. The caller has lost a message before the
    /// frame is cut, which is its bug and not the wire's.
    pub fn push(&mut self, range: RangeId, message: &[u8]) {
        assert!(
            !message.is_empty(),
            "an empty message is one no reader of the frame can decode"
        );
        assert!(
            self.fits(message.len()),
            "a message of {} bytes does not fit a frame of {} with {} written",
            message.len(),
            self.cap,
            self.out.len()
        );
        self.out.put_u64_le(range.get());
        self.out
            .put_u32_le(u32::try_from(message.len()).expect("a message under the cap fits u32"));
        self.out.put_slice(message);
        self.count += 1;
    }

    /// The frame, with its count filled in.
    #[must_use]
    pub fn finish(mut self) -> Bytes {
        self.out[1..HEADER_LEN].copy_from_slice(&self.count.to_le_bytes());
        self.out.freeze()
    }
}

/// One message of a frame: the range it is about, the message itself, and the frame
/// bytes it occupied — its length, its tag and, for the first message of a frame, the
/// frame's header, which is what the node's inbox bounds itself by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tagged {
    /// The range the message is about (Q10).
    pub range: RangeId,
    /// The message, decoded by `ananke-raft`'s codec, sender and all.
    pub frame: Frame,
    /// The frame bytes this message occupied: [`TAG_LEN`] plus its own length, and
    /// [`HEADER_LEN`] besides for the first message of a frame.
    ///
    /// Every byte the node took off the wire is charged to exactly one message, so a
    /// frame's messages' costs sum to the frame's length and the inbox's bound is the
    /// bytes received and not the bytes received less five per frame. The header goes
    /// to the first message because a frame is carried for its first message as much
    /// as for its last; a frame of no messages carries nobody and charges nobody.
    pub bytes: usize,
}

/// What a frame decoded to.
///
/// `malformed` is the messages whose framing was sound and whose bytes
/// `ananke-raft`'s codec refused: they are dropped, each on its own, and the ones
/// beside them are in `messages`. See the module documentation for why the frame is
/// not refused whole for them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Decoded {
    /// The messages this node could read, in the order the frame carries them.
    pub messages: Vec<Tagged>,
    /// How many messages of the frame were dropped because the codec refused them.
    pub malformed: usize,
}

/// The messages of a frame as the bytes they were written as, each with its range,
/// without parsing any of them.
///
/// This is the framing alone, which is all a reader that does not speak the message
/// codec needs — [`studio`] is one. [`decode`] is this plus `ananke-raft`'s decoder.
///
/// # Errors
///
/// `InvalidData` for anything [`Builder`] did not produce: another version, a torn
/// frame, a count that does not match, or trailing bytes.
pub fn slices(frame: &Bytes) -> io::Result<Vec<(RangeId, Bytes)>> {
    let mut rest = frame.clone();
    if rest.len() < HEADER_LEN {
        return Err(bad("batch frame too short"));
    }
    if rest.get_u8() != VERSION {
        return Err(bad("unknown batch frame version"));
    }
    let count = rest.get_u32_le() as usize;
    // A corrupt count must not reserve a corrupt amount of memory; the loop below
    // fails on the first message that is not there.
    let mut messages = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if rest.len() < TAG_LEN {
            return Err(bad("batch frame torn"));
        }
        let range = RangeId(rest.get_u64_le());
        let len = rest.get_u32_le() as usize;
        if rest.len() < len {
            return Err(bad("batch frame torn"));
        }
        messages.push((range, rest.split_to(len)));
    }
    if !rest.is_empty() {
        return Err(bad("batch frame has trailing bytes"));
    }
    Ok(messages)
}

/// The messages of a frame, in the order they were written, each with its range, and
/// how many of them the message codec refused.
///
/// A message the codec refuses is dropped on its own and counted in
/// [`Decoded::malformed`]; the frame is not refused for it. The framing says where
/// every message of the frame ends and [`slices`] has already checked it, so the
/// messages beside a bad one are exactly as delimited as they were — and a frame
/// carries up to 222 of §4's idle responses, so refusing it whole turns one lost
/// message into 222.
///
/// # Errors
///
/// `InvalidData` for a frame [`Builder`] did not produce: another version, a torn
/// frame, a count that does not match, or trailing bytes. Then nothing in the frame
/// has a known end and the frame goes whole.
pub fn decode(frame: &Bytes) -> io::Result<Decoded> {
    let slices = slices(frame)?;
    let mut decoded = Decoded::default();
    for (position, (range, message)) in slices.into_iter().enumerate() {
        // The frame's header is charged to its first message, so the frame's bytes
        // are charged to its messages exactly.
        let header = if position == 0 { HEADER_LEN } else { 0 };
        let bytes = header + TAG_LEN + message.len();
        match Frame::decode(message) {
            Ok(frame) => decoded.messages.push(Tagged {
                range,
                frame,
                bytes,
            }),
            Err(_) => decoded.malformed += 1,
        }
    }
    Ok(decoded)
}

fn int(v: u64) -> Json {
    i64::try_from(v).map_or_else(|_| Json::Str(v.to_string()), Json::Int)
}

/// The studio's view of a frame: an object whose `type` is `shard.batch`, with the
/// messages it carries in `msgs`, each the object `ananke-raft`'s own decoder makes of
/// it with its `range` after its `type`. Pass it to `ananke_env::moirae::Export`.
///
/// | field | |
/// |---|---|
/// | `type` | `shard.batch`, or `shard.malformed` for a frame this crate did not write |
/// | `count` | the messages in the frame |
/// | `ranges` | how many distinct ranges they are about |
/// | `msgs` | one object per message, in the order the frame carries them |
///
/// So a frame of six messages of three ranges reads in the studio as six messages and
/// not as one, which is the whole point of batching them (SHARD.md §11, raft 1).
///
/// A message whose framing is sound but whose bytes the message codec refuses is
/// `raft.malformed` in its place, and the messages beside it still read: the studio
/// shows what a trace holds, where [`decode`] — which speaks for a node that has to
/// act on it — refuses the frame whole.
#[must_use]
pub fn studio(payload: &[u8]) -> Json {
    let Ok(messages) = slices(&Bytes::copy_from_slice(payload)) else {
        return Json::obj(vec![
            ("type", Json::str("shard.malformed")),
            ("len", int(payload.len() as u64)),
        ]);
    };
    let ranges: BTreeSet<RangeId> = messages.iter().map(|(range, _)| *range).collect();
    let msgs = messages
        .iter()
        .map(|(range, message)| {
            let decoded = ananke_raft::message::studio(message);
            // `range` goes after `type`, as the trace writes it after `server`
            // (D-069): the studio labels a message by its type and filters by its
            // range.
            let Json::Object(mut fields) = decoded else {
                return Json::obj(vec![("range", int(range.get())), ("msg", decoded)]);
            };
            fields.insert(1.min(fields.len()), ("range".to_owned(), int(range.get())));
            Json::Object(fields)
        })
        .collect();
    Json::obj(vec![
        ("type", Json::str("shard.batch")),
        ("count", int(messages.len() as u64)),
        ("ranges", int(ranges.len() as u64)),
        ("msgs", Json::Array(msgs)),
    ])
}

#[cfg(test)]
mod tests {
    use ananke_raft::message::Message;
    use ananke_raft::types::{Payload, ServerId};

    use super::*;

    /// A heartbeat, the message §4's arithmetic counts: 53 bytes on its own, 61 with
    /// its range (SHARD.md §4).
    fn heartbeat(term: u64) -> Frame {
        Frame {
            from: ServerId(1),
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

    fn response(term: u64) -> Frame {
        Frame {
            from: ServerId(2),
            message: Message::AppendEntriesResponse {
                term,
                success: true,
                prev_index: 7,
                match_index: 7,
                hint: 0,
                echo: 123_456_789,
                local: 987_654_321,
                incarnation: 1,
                refused: false,
            },
        }
    }

    fn built(messages: &[(RangeId, Frame)]) -> Bytes {
        let mut builder = Builder::new(ananke_env::MAX_FRAME_LEN);
        for (range, frame) in messages {
            let encoded = frame.encode();
            assert!(builder.fits(encoded.len()));
            builder.push(*range, &encoded);
        }
        builder.finish()
    }

    /// The frame's arithmetic is §4's: a heartbeat is 53 bytes, 61 with its range,
    /// and this framing adds the 4 that say where it ends.
    #[test]
    fn a_message_costs_its_range_and_its_length_beyond_itself() {
        let encoded = heartbeat(3).encode();
        assert_eq!(encoded.len(), 53, "SHARD.md §4's heartbeat");
        assert_eq!(encoded.len() + 8, 61, "Q10's range id");
        let frame = built(&[(RangeId(2), heartbeat(3))]);
        assert_eq!(frame.len(), encoded_len(encoded.len()));
        assert_eq!(frame.len(), HEADER_LEN + TAG_LEN + 53);
    }

    /// Six messages of three ranges go out in one frame and come back as six
    /// messages, each with the range it was tagged with, in order.
    #[test]
    fn a_frame_carries_several_messages_each_with_its_own_range() {
        let sent = vec![
            (RangeId(2), heartbeat(3)),
            (RangeId(9), heartbeat(4)),
            (RangeId(2), response(3)),
            (RangeId(7), heartbeat(5)),
            (RangeId(9), response(4)),
            (RangeId(7), response(5)),
        ];
        let decoded = decode(&built(&sent)).expect("a frame this crate wrote");
        assert_eq!(decoded.malformed, 0);
        let decoded = decoded.messages;
        assert_eq!(decoded.len(), 6, "six messages, not one");
        for (position, (tagged, (range, frame))) in decoded.iter().zip(&sent).enumerate() {
            assert_eq!(tagged.range, *range);
            assert_eq!(tagged.frame, *frame);
            // The frame's header is charged to its first message and to no other.
            let header = if position == 0 { HEADER_LEN } else { 0 };
            assert_eq!(tagged.bytes, header + TAG_LEN + frame.encode().len());
        }
        // The second message's range is its own, not the first's: the mutation this
        // test exists for.
        assert_eq!(decoded[1].range, RangeId(9));
        assert_ne!(decoded[1].range, decoded[0].range);
        // Every byte the node took off the wire is charged to exactly one message:
        // the costs sum to the frame, header included and nothing double-counted.
        assert_eq!(
            decoded.iter().map(|t| t.bytes).sum::<usize>(),
            built(&sent).len(),
            "the frame's bytes are charged to its messages exactly"
        );
    }

    /// A range id and a sender are both `u64` and lie next to each other on the wire;
    /// this frame has them different and crossing, so reading either at the other's
    /// offset is caught.
    #[test]
    fn a_range_is_not_the_sender_and_a_sender_is_not_the_range() {
        let frame = built(&[(RangeId(2), heartbeat(3)), (RangeId(1), response(3))]);
        let decoded = decode(&frame).expect("a frame this crate wrote").messages;
        assert_eq!(decoded[0].range, RangeId(2));
        assert_eq!(decoded[0].frame.from, ServerId(1));
        assert_eq!(decoded[1].range, RangeId(1));
        assert_eq!(decoded[1].frame.from, ServerId(2));
    }

    /// Framing that does not parse takes the frame with it: nothing in it has a
    /// known end.
    #[test]
    fn a_frame_whose_framing_does_not_parse_is_refused_whole() {
        let good = built(&[(RangeId(2), heartbeat(3)), (RangeId(9), response(3))]);
        for cut in 0..good.len() {
            assert!(
                decode(&good.slice(..cut)).is_err(),
                "a frame cut at {cut} decoded"
            );
        }
        let mut trailing = good.to_vec();
        trailing.push(0);
        assert!(decode(&Bytes::from(trailing)).is_err(), "trailing bytes");
        let mut version = good.to_vec();
        version[0] = VERSION + 1;
        assert!(decode(&Bytes::from(version)).is_err(), "another version");
        let mut count = good.to_vec();
        count[1] = 3;
        assert!(decode(&Bytes::from(count)).is_err(), "a count too high");
        let mut short_count = good.to_vec();
        short_count[1] = 1;
        assert!(
            decode(&Bytes::from(short_count)).is_err(),
            "a count too low"
        );
    }

    /// The framing is sound and one message is not: that message is dropped and the
    /// ones beside it are kept. §4's idle frame carries 222 responses of one node
    /// pair, so refusing the frame whole for one of them loses 222 messages of up to
    /// 222 ranges where one was written badly.
    #[test]
    fn a_message_the_codec_refuses_is_dropped_and_the_ones_beside_it_are_kept() {
        let mut builder = Builder::new(ananke_env::MAX_FRAME_LEN);
        builder.push(RangeId(2), &heartbeat(3).encode());
        builder.push(RangeId(9), b"not a message");
        builder.push(RangeId(7), &response(3).encode());
        let frame = builder.finish();
        assert_eq!(slices(&frame).expect("the framing is sound").len(), 3);
        let decoded = decode(&frame).expect("the framing is sound");
        assert_eq!(decoded.malformed, 1, "one message dropped, counted");
        assert_eq!(
            decoded.messages.iter().map(|t| t.range).collect::<Vec<_>>(),
            vec![RangeId(2), RangeId(7)],
            "the two beside it, each still under its own range"
        );
        assert_eq!(decoded.messages[0].frame, heartbeat(3));
        assert_eq!(decoded.messages[1].frame, response(3));
        // At §4's largest idle frame the difference is the whole frame.
        let mut many = Builder::new(ananke_env::MAX_FRAME_LEN);
        for range in 0..222u64 {
            many.push(RangeId(range), &response(3).encode());
        }
        many.push(RangeId(999), b"not a message");
        let decoded = decode(&many.finish()).expect("the framing is sound");
        assert_eq!(decoded.messages.len(), 222, "222 kept, not 0");
        assert_eq!(decoded.malformed, 1, "1 lost, not 223");
    }

    /// An empty message is 12 bytes of frame that no reader can turn back into a
    /// message: the caller has lost it before the frame is cut, and is told so.
    #[test]
    #[should_panic(expected = "an empty message is one no reader of the frame can decode")]
    fn an_empty_message_is_refused_by_the_builder() {
        Builder::new(ananke_env::MAX_FRAME_LEN).push(RangeId(2), b"");
    }

    /// The studio shows every message a frame carries, each with its own range, and
    /// labels a message it cannot read without losing the ones beside it.
    #[test]
    fn the_studio_sees_six_messages_of_three_ranges_and_not_one() {
        let frame = built(&[
            (RangeId(2), heartbeat(3)),
            (RangeId(9), heartbeat(4)),
            (RangeId(2), response(3)),
            (RangeId(7), heartbeat(5)),
            (RangeId(9), response(4)),
            (RangeId(7), response(5)),
        ]);
        let Json::Object(fields) = studio(&frame) else {
            panic!("an object")
        };
        let get = |name: &str| {
            fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("type"), Some(Json::str("shard.batch")));
        assert_eq!(get("count"), Some(Json::Int(6)));
        assert_eq!(get("ranges"), Some(Json::Int(3)));
        let Some(Json::Array(msgs)) = get("msgs") else {
            panic!("an array of messages")
        };
        assert_eq!(msgs.len(), 6, "six messages, not one");
        let field = |msg: &Json, name: &str| {
            let Json::Object(fields) = msg else {
                panic!("an object")
            };
            fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(
            field(&msgs[0], "type"),
            Some(Json::str("raft.append-entries"))
        );
        assert_eq!(field(&msgs[0], "range"), Some(Json::Int(2)));
        assert_eq!(field(&msgs[1], "range"), Some(Json::Int(9)));
        assert_eq!(field(&msgs[2], "range"), Some(Json::Int(2)));
        assert_eq!(
            field(&msgs[2], "type"),
            Some(Json::str("raft.append-entries-response"))
        );
        assert_eq!(field(&msgs[5], "range"), Some(Json::Int(7)));
        // `range` sits immediately after `type`, as the trace writes it immediately
        // after `server` (D-069).
        let Json::Object(first) = &msgs[0] else {
            panic!("an object")
        };
        assert_eq!(first[1].0, "range");
        let Json::Object(fields) = studio(b"junk") else {
            panic!("an object")
        };
        assert_eq!(fields[0].1, Json::str("shard.malformed"));
    }

    #[test]
    fn the_studio_labels_a_message_it_cannot_read_and_keeps_the_ones_beside_it() {
        let mut builder = Builder::new(ananke_env::MAX_FRAME_LEN);
        builder.push(RangeId(2), &heartbeat(3).encode());
        builder.push(RangeId(9), b"not a message");
        builder.push(RangeId(7), &response(3).encode());
        let Json::Object(fields) = studio(&builder.finish()) else {
            panic!("an object")
        };
        let Some((_, Json::Array(msgs))) = fields.iter().find(|(k, _)| k == "msgs") else {
            panic!("an array of messages")
        };
        assert_eq!(msgs.len(), 3);
        let type_of = |msg: &Json| {
            let Json::Object(fields) = msg else {
                panic!("an object")
            };
            fields
                .iter()
                .find(|(k, _)| k == "type")
                .map(|(_, v)| v.clone())
        };
        assert_eq!(type_of(&msgs[0]), Some(Json::str("raft.append-entries")));
        assert_eq!(type_of(&msgs[1]), Some(Json::str("raft.malformed")));
        assert_eq!(
            type_of(&msgs[2]),
            Some(Json::str("raft.append-entries-response"))
        );
    }

    #[test]
    fn a_builder_says_what_fits_and_an_empty_frame_is_a_frame_of_no_messages() {
        let encoded = heartbeat(3).encode();
        let cap = encoded_len(encoded.len());
        let mut builder = Builder::new(cap);
        assert!(builder.is_empty());
        assert!(builder.fits(encoded.len()));
        builder.push(RangeId(2), &encoded);
        assert!(!builder.fits(1), "one message is all this cap holds");
        assert_eq!(builder.count(), 1);
        assert_eq!(builder.len(), cap);
        let frame = builder.finish();
        assert_eq!(decode(&frame).expect("a frame").messages.len(), 1);
        assert_eq!(
            decode(&Builder::new(cap).finish())
                .expect("an empty frame")
                .messages
                .len(),
            0
        );
        // `encoded_len` saturates where `fits` does: a length no frame could hold
        // compares as one and does not wrap to a small one that fits.
        assert_eq!(encoded_len(usize::MAX), usize::MAX);
        assert!(!Builder::new(cap).fits(usize::MAX));
    }

    /// A range id is a `u64` and the studio's numbers are JSON's, which are `i64`:
    /// a range above `i64::MAX` exports as a string, exactly as every other `u64` the
    /// trace carries does (`ananke_raft::message::int`). Pinned so that the two
    /// cannot drift apart.
    #[test]
    fn a_range_id_above_i64_max_exports_as_ananke_raft_exports_one() {
        let big = u64::MAX;
        let frame = built(&[(RangeId(big), heartbeat(3)), (RangeId(7), heartbeat(3))]);
        let Json::Object(fields) = studio(&frame) else {
            panic!("an object")
        };
        let Some((_, Json::Array(msgs))) = fields.iter().find(|(k, _)| k == "msgs") else {
            panic!("an array of messages")
        };
        let range_of = |msg: &Json| {
            let Json::Object(fields) = msg else {
                panic!("an object")
            };
            fields
                .iter()
                .find(|(k, _)| k == "range")
                .map(|(_, v)| v.clone())
                .expect("a range")
        };
        assert_eq!(range_of(&msgs[0]), Json::Str(big.to_string()));
        assert_eq!(range_of(&msgs[1]), Json::Int(7));
        // And the same `u64` in `ananke-raft`'s own export reads the same way: an
        // index over `i64::MAX` on the message beside it.
        let over = Frame {
            from: ServerId(1),
            message: Message::AppendEntries {
                term: 3,
                prev_index: 7,
                prev_term: 2,
                entries: Vec::new(),
                commit: big,
                sent: 123_456_789,
            },
        };
        let Json::Object(raft) = ananke_raft::message::studio(&over.encode()) else {
            panic!("an object")
        };
        assert!(
            raft.iter().any(|(_, v)| *v == Json::Str(big.to_string())),
            "ananke-raft writes a u64 over i64::MAX as a string too: {raft:?}"
        );
    }

    /// What a byte of the inbox's bound can pin (D-072). A heartbeat holds no
    /// `Bytes`, so it pins nothing at all; the message that pins a frame is the
    /// smallest one that carries a payload, and the ratio is bounded by
    /// `MAX_FRAME_LEN` over its cost.
    #[test]
    fn the_smallest_message_that_pins_a_frame_is_the_smallest_carrier_and_not_a_heartbeat() {
        let carrier = |command: &'static [u8]| Frame {
            from: ServerId(1),
            message: Message::AppendEntries {
                term: 3,
                prev_index: 7,
                prev_term: 2,
                entries: vec![ananke_raft::types::Entry {
                    term: 3,
                    index: 8,
                    payload: ananke_raft::types::Payload::Command(Bytes::from_static(command)),
                }],
                commit: 6,
                sent: 123_456_789,
            },
        };
        let smallest = TAG_LEN + carrier(b"").encode().len();
        println!("the smallest entry-carrier costs {smallest} bytes of the bound");
        // A heartbeat borrows nothing from its frame: dropping the frame beside it
        // costs it nothing, so it cannot pin one.
        let Message::AppendEntries { entries, .. } = &decode(&built(&[(RangeId(2), heartbeat(3))]))
            .expect("a frame")
            .messages[0]
            .frame
            .message
        else {
            panic!("a heartbeat")
        };
        assert!(entries.is_empty(), "a heartbeat holds no Bytes at all");
        // The carrier does borrow: its command is a slice of the frame it arrived in.
        let mut builder = Builder::new(ananke_env::MAX_FRAME_LEN);
        builder.push(RangeId(2), &carrier(b"x").encode());
        let filler = vec![7u8; 780 * 1024];
        builder.push(
            RangeId(9),
            &Frame {
                from: ServerId(1),
                message: Message::InstallSnapshot {
                    term: 3,
                    last_index: 8,
                    last_term: 3,
                    file: Bytes::from_static(b"f"),
                    offset: 0,
                    total: filler.len() as u64,
                    done: true,
                    data: Bytes::from(filler),
                },
            }
            .encode(),
        );
        let frame = builder.finish();
        let held = frame.len();
        let decoded = decode(&frame).expect("a frame this crate wrote");
        let pin = &decoded.messages[0];
        let Message::AppendEntries { entries, .. } = &pin.frame.message else {
            panic!("a carrier")
        };
        let Payload::Command(command) = &entries[0].payload else {
            panic!("a command")
        };
        // The command is a slice of the frame, so holding the one message holds all
        // of it. That the bytes are equal is not the test — where they live is.
        let base = frame.as_ptr() as usize;
        let at = command.as_ptr() as usize;
        assert!(
            (base..base + frame.len()).contains(&at),
            "the command is a slice of the frame it arrived in"
        );
        println!(
            "an entry-carrier of {} bytes of the bound pins the {held}-byte frame it arrived in, {}x",
            pin.bytes,
            held / pin.bytes
        );
        assert!(
            held / pin.bytes > 8_000,
            "{} bytes pinning {held} is {}x",
            pin.bytes,
            held / pin.bytes
        );
        // And the worst of it is the cap over the smallest carrier.
        let worst = ananke_env::MAX_FRAME_LEN / smallest;
        println!("the bound on the ratio is MAX_FRAME_LEN / {smallest} = {worst}x");
        assert!((150_000..250_000).contains(&worst), "{worst}");
    }
}
