//! Messages between servers and their wire form (RAFT.md §3), and the decoder that
//! shows them in the moirae studio.
//!
//! One frame is `kind: u8 | from: u64 | term: u64 | fields`, everything little-endian,
//! entries as `count: u32` then per entry `term: u64 | index: u64 | payload`, a payload
//! as `tag: u8` then, for a command, `len: u32 | bytes`, and for a configuration the
//! voter, new-voter and learner lists as `count: u32 | ids`. A frame that does not
//! decode is a dropped message, never a panic.

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use moirae_trace::Json;

use crate::types::{Configuration, Entry, Index, Payload, ServerId, Term};

/// A message of the protocol. Every one carries the sender's current term, or for a
/// pre-vote the term the sender would start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// "Would you vote for me in `term`?", asked without leaving the current term
    /// (thesis §9.6).
    PreVote {
        /// The term the sender would start.
        term: Term,
        /// The sender's last log index.
        last_index: Index,
        /// The sender's last log term.
        last_term: Term,
    },
    /// The answer to a pre-vote: `term` is the prospective term when granted, the
    /// responder's current term when refused.
    PreVoteResponse {
        /// See above.
        term: Term,
        /// Whether the sender would vote.
        granted: bool,
    },
    /// Figure 2's RequestVote.
    RequestVote {
        /// The candidate's term.
        term: Term,
        /// The candidate's last log index.
        last_index: Index,
        /// The candidate's last log term.
        last_term: Term,
    },
    /// The answer to a vote request.
    RequestVoteResponse {
        /// The responder's current term.
        term: Term,
        /// Whether the vote was granted.
        granted: bool,
    },
    /// Figure 2's AppendEntries, a heartbeat when `entries` is empty.
    AppendEntries {
        /// The leader's term.
        term: Term,
        /// The index of the entry before `entries`.
        prev_index: Index,
        /// Its term.
        prev_term: Term,
        /// The entries to store.
        entries: Vec<Entry>,
        /// The leader's commit index.
        commit: Index,
    },
    /// The answer to AppendEntries. `match_index` is `prev_index` plus the entries
    /// stored, moirae's deviation D1; `hint` is where the leader should resume
    /// after a rejection: the follower's last index plus one, or the first index of
    /// the term that conflicted.
    AppendEntriesResponse {
        /// The responder's current term.
        term: Term,
        /// Whether the entries were stored.
        success: bool,
        /// See above.
        match_index: Index,
        /// See above.
        hint: Index,
    },
}

impl Message {
    /// The term the message carries.
    #[must_use]
    pub fn term(&self) -> Term {
        match self {
            Message::PreVote { term, .. }
            | Message::PreVoteResponse { term, .. }
            | Message::RequestVote { term, .. }
            | Message::RequestVoteResponse { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResponse { term, .. } => *term,
        }
    }

    /// The message's kind, as the studio labels it.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Message::PreVote { .. } => "pre-vote",
            Message::PreVoteResponse { .. } => "pre-vote-response",
            Message::RequestVote { .. } => "request-vote",
            Message::RequestVoteResponse { .. } => "request-vote-response",
            Message::AppendEntries { .. } => "append-entries",
            Message::AppendEntriesResponse { .. } => "append-entries-response",
        }
    }

    fn tag(&self) -> u8 {
        match self {
            Message::PreVote { .. } => 1,
            Message::PreVoteResponse { .. } => 2,
            Message::RequestVote { .. } => 3,
            Message::RequestVoteResponse { .. } => 4,
            Message::AppendEntries { .. } => 5,
            Message::AppendEntriesResponse { .. } => 6,
        }
    }
}

/// A message with its sender: what goes on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// The sender.
    pub from: ServerId,
    /// The message.
    pub message: Message,
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

fn put_ids(out: &mut BytesMut, ids: &[ServerId]) {
    out.put_u32_le(u32::try_from(ids.len()).expect("member count fits u32"));
    for id in ids {
        out.put_u64_le(id.0);
    }
}

fn get_ids(rest: &mut Bytes) -> io::Result<Vec<ServerId>> {
    if rest.len() < 4 {
        return Err(bad("member list torn"));
    }
    let count = rest.get_u32_le() as usize;
    if rest.len() < count * 8 {
        return Err(bad("member list torn"));
    }
    Ok((0..count).map(|_| ServerId(rest.get_u64_le())).collect())
}

/// Appends a payload's wire form: a tag, then a command's bytes or a configuration's
/// lists. Shared with the store, which keeps entries under the same form.
pub(crate) fn put_payload(out: &mut BytesMut, payload: &Payload) {
    match payload {
        Payload::Noop => out.put_u8(0),
        Payload::Command(bytes) => {
            out.put_u8(1);
            out.put_u32_le(u32::try_from(bytes.len()).expect("command fits u32"));
            out.put_slice(bytes);
        }
        Payload::Config(config) => {
            out.put_u8(2);
            put_ids(out, &config.voters);
            match &config.new_voters {
                Some(new) => {
                    out.put_u8(1);
                    put_ids(out, new);
                }
                None => out.put_u8(0),
            }
            put_ids(out, &config.learners);
        }
    }
}

/// Parses a payload written by [`put_payload`].
pub(crate) fn get_payload(rest: &mut Bytes) -> io::Result<Payload> {
    if rest.is_empty() {
        return Err(bad("payload torn"));
    }
    Ok(match rest.get_u8() {
        0 => Payload::Noop,
        1 => {
            if rest.len() < 4 {
                return Err(bad("command torn"));
            }
            let len = rest.get_u32_le() as usize;
            if rest.len() < len {
                return Err(bad("command torn"));
            }
            Payload::Command(rest.split_to(len))
        }
        2 => {
            let voters = get_ids(rest)?;
            if rest.is_empty() {
                return Err(bad("configuration torn"));
            }
            let new_voters = match rest.get_u8() {
                0 => None,
                1 => Some(get_ids(rest)?),
                _ => return Err(bad("configuration malformed")),
            };
            let learners = get_ids(rest)?;
            Payload::Config(Configuration {
                voters,
                new_voters,
                learners,
            })
        }
        _ => return Err(bad("payload malformed")),
    })
}

fn put_entries(out: &mut BytesMut, entries: &[Entry]) {
    out.put_u32_le(u32::try_from(entries.len()).expect("entry count fits u32"));
    for entry in entries {
        out.put_u64_le(entry.term);
        out.put_u64_le(entry.index);
        put_payload(out, &entry.payload);
    }
}

fn get_entries(rest: &mut Bytes) -> io::Result<Vec<Entry>> {
    if rest.len() < 4 {
        return Err(bad("entry list torn"));
    }
    let count = rest.get_u32_le() as usize;
    let mut entries = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if rest.len() < 16 {
            return Err(bad("entry torn"));
        }
        let term = rest.get_u64_le();
        let index = rest.get_u64_le();
        let payload = get_payload(rest)?;
        entries.push(Entry {
            term,
            index,
            payload,
        });
    }
    Ok(entries)
}

impl Frame {
    /// The wire form.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(64);
        out.put_u8(self.message.tag());
        out.put_u64_le(self.from.0);
        out.put_u64_le(self.message.term());
        match &self.message {
            Message::PreVote {
                last_index,
                last_term,
                ..
            }
            | Message::RequestVote {
                last_index,
                last_term,
                ..
            } => {
                out.put_u64_le(*last_index);
                out.put_u64_le(*last_term);
            }
            Message::PreVoteResponse { granted, .. }
            | Message::RequestVoteResponse { granted, .. } => {
                out.put_u8(u8::from(*granted));
            }
            Message::AppendEntries {
                prev_index,
                prev_term,
                entries,
                commit,
                ..
            } => {
                out.put_u64_le(*prev_index);
                out.put_u64_le(*prev_term);
                out.put_u64_le(*commit);
                put_entries(&mut out, entries);
            }
            Message::AppendEntriesResponse {
                success,
                match_index,
                hint,
                ..
            } => {
                out.put_u8(u8::from(*success));
                out.put_u64_le(*match_index);
                out.put_u64_le(*hint);
            }
        }
        out.freeze()
    }

    /// Parses a frame.
    ///
    /// # Errors
    ///
    /// `InvalidData` for anything [`encode`](Self::encode) did not produce.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        if bytes.len() < 17 {
            return Err(bad("frame too short"));
        }
        let tag = bytes.get_u8();
        let from = ServerId(bytes.get_u64_le());
        let term = bytes.get_u64_le();
        let u64_field = |bytes: &mut Bytes| -> io::Result<u64> {
            if bytes.len() < 8 {
                return Err(bad("frame torn"));
            }
            Ok(bytes.get_u64_le())
        };
        let message = match tag {
            1 | 3 => {
                let last_index = u64_field(&mut bytes)?;
                let last_term = u64_field(&mut bytes)?;
                if tag == 1 {
                    Message::PreVote {
                        term,
                        last_index,
                        last_term,
                    }
                } else {
                    Message::RequestVote {
                        term,
                        last_index,
                        last_term,
                    }
                }
            }
            2 | 4 => {
                if bytes.is_empty() {
                    return Err(bad("frame torn"));
                }
                let granted = match bytes.get_u8() {
                    0 => false,
                    1 => true,
                    _ => return Err(bad("frame malformed")),
                };
                if tag == 2 {
                    Message::PreVoteResponse { term, granted }
                } else {
                    Message::RequestVoteResponse { term, granted }
                }
            }
            5 => {
                let prev_index = u64_field(&mut bytes)?;
                let prev_term = u64_field(&mut bytes)?;
                let commit = u64_field(&mut bytes)?;
                let entries = get_entries(&mut bytes)?;
                Message::AppendEntries {
                    term,
                    prev_index,
                    prev_term,
                    entries,
                    commit,
                }
            }
            6 => {
                if bytes.is_empty() {
                    return Err(bad("frame torn"));
                }
                let success = match bytes.get_u8() {
                    0 => false,
                    1 => true,
                    _ => return Err(bad("frame malformed")),
                };
                let match_index = u64_field(&mut bytes)?;
                let hint = u64_field(&mut bytes)?;
                Message::AppendEntriesResponse {
                    term,
                    success,
                    match_index,
                    hint,
                }
            }
            _ => return Err(bad("unknown message kind")),
        };
        if !bytes.is_empty() {
            return Err(bad("frame has trailing bytes"));
        }
        Ok(Self { from, message })
    }
}

fn int(v: u64) -> Json {
    i64::try_from(v).map_or_else(|_| Json::Str(v.to_string()), Json::Int)
}

/// The studio's view of a frame: an object whose `type` is `raft.<kind>`, with the
/// sender, the term and the fields the studio filters by, and for AppendEntries the
/// entry range rather than the entries. A frame that does not decode is
/// `raft.malformed` with its length. Pass it to `ananke_env::moirae::Export`.
#[must_use]
pub fn studio(payload: &[u8]) -> Json {
    let Ok(frame) = Frame::decode(Bytes::copy_from_slice(payload)) else {
        return Json::obj(vec![
            ("type", Json::str("raft.malformed")),
            ("len", int(payload.len() as u64)),
        ]);
    };
    let mut fields = vec![
        ("type", Json::str(&format!("raft.{}", frame.message.kind()))),
        ("from", int(frame.from.0)),
        ("term", int(frame.message.term())),
    ];
    match &frame.message {
        Message::PreVote {
            last_index,
            last_term,
            ..
        }
        | Message::RequestVote {
            last_index,
            last_term,
            ..
        } => {
            fields.push(("lastIndex", int(*last_index)));
            fields.push(("lastTerm", int(*last_term)));
        }
        Message::PreVoteResponse { granted, .. } | Message::RequestVoteResponse { granted, .. } => {
            fields.push(("granted", Json::Bool(*granted)));
        }
        Message::AppendEntries {
            prev_index,
            entries,
            commit,
            ..
        } => {
            fields.push(("prevIndex", int(*prev_index)));
            fields.push(("entries", int(entries.len() as u64)));
            if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
                fields.push(("firstIndex", int(first.index)));
                fields.push(("lastIndex", int(last.index)));
            }
            fields.push(("commit", int(*commit)));
        }
        Message::AppendEntriesResponse {
            success,
            match_index,
            hint,
            ..
        } => {
            fields.push(("success", Json::Bool(*success)));
            fields.push(("matchIndex", int(*match_index)));
            fields.push(("hint", int(*hint)));
        }
    }
    Json::obj(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_kind() -> Vec<Message> {
        vec![
            Message::PreVote {
                term: 3,
                last_index: 7,
                last_term: 2,
            },
            Message::PreVoteResponse {
                term: 3,
                granted: true,
            },
            Message::RequestVote {
                term: 3,
                last_index: 7,
                last_term: 2,
            },
            Message::RequestVoteResponse {
                term: 3,
                granted: false,
            },
            Message::AppendEntries {
                term: 3,
                prev_index: 7,
                prev_term: 2,
                entries: vec![
                    Entry {
                        term: 3,
                        index: 8,
                        payload: Payload::Noop,
                    },
                    Entry {
                        term: 3,
                        index: 9,
                        payload: Payload::Command(Bytes::from_static(b"put k v")),
                    },
                    Entry {
                        term: 3,
                        index: 10,
                        payload: Payload::Config(Configuration {
                            voters: vec![ServerId(1), ServerId(2), ServerId(3)],
                            new_voters: Some(vec![ServerId(2), ServerId(3), ServerId(4)]),
                            learners: vec![ServerId(4)],
                        }),
                    },
                ],
                commit: 6,
            },
            Message::AppendEntriesResponse {
                term: 3,
                success: false,
                match_index: 7,
                hint: 5,
            },
        ]
    }

    #[test]
    fn every_kind_round_trips_and_a_torn_frame_is_refused() {
        for message in every_kind() {
            let frame = Frame {
                from: ServerId(2),
                message,
            };
            let bytes = frame.encode();
            assert_eq!(Frame::decode(bytes.clone()).unwrap(), frame);
            for cut in 1..bytes.len() {
                assert!(
                    Frame::decode(bytes.slice(..cut)).is_err(),
                    "{} cut at {cut}",
                    frame.message.kind()
                );
            }
            let mut long = bytes.to_vec();
            long.push(0);
            assert!(Frame::decode(Bytes::from(long)).is_err());
        }
        assert!(Frame::decode(Bytes::from_static(b"")).is_err());
        let mut unknown = every_kind()[0].clone();
        let _ = &mut unknown;
        let mut bytes = Frame {
            from: ServerId(1),
            message: unknown,
        }
        .encode()
        .to_vec();
        bytes[0] = 9;
        assert!(Frame::decode(Bytes::from(bytes)).is_err());
    }

    #[test]
    fn the_studio_sees_the_kind_the_term_and_the_range() {
        let frame = Frame {
            from: ServerId(2),
            message: every_kind().remove(4),
        };
        let json = studio(&frame.encode());
        let Json::Object(fields) = json else {
            panic!("an object")
        };
        let get = |name: &str| {
            fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get("type"), Some(Json::str("raft.append-entries")));
        assert_eq!(get("term"), Some(Json::Int(3)));
        assert_eq!(get("firstIndex"), Some(Json::Int(8)));
        assert_eq!(get("lastIndex"), Some(Json::Int(10)));
        assert_eq!(get("entries"), Some(Json::Int(3)));
        let Json::Object(fields) = studio(b"junk") else {
            panic!("an object")
        };
        assert_eq!(fields[0].1, Json::str("raft.malformed"));
    }
}
