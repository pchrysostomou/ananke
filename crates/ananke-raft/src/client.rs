//! What clients and servers say to each other (RAFT.md §3): a request carries a
//! [`Command`] and a response its outcome, or the leader to try instead. Requests
//! and responses share the servers' socket with the protocol's frames and are told
//! apart by the first byte: [`REQUEST_TAG`] and [`RESPONSE_TAG`] sit above every
//! [`Message`](crate::message::Message) tag.
//!
//! A request is `tag | client: u64 | seq: u64 | command`, a response `tag | client |
//! seq | reply: u8 | fields`, everything little-endian. The client and sequence
//! number identify the operation, so a late or duplicated response for an earlier
//! operation is recognised and ignored.
//!
//! A server answers a request in one of three ways: with the command's outcome once
//! the entry it became is applied; with [`Reply::NotLeader`] at once when it is not
//! the leader; or never, when the entry it proposed did not commit under its
//! leadership and no leader after it carried the entry to commit. The client that
//! hears nothing does not resend a write: the entry may yet commit, and a second copy
//! would be a second write. It abandons the operation as pending (RAFT.md §4) and
//! continues as a new process. Exactly-once retries need client sessions (thesis
//! §6.3), which are deferred (issue #21).

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use moirae_trace::Json;

use crate::apply::{Command, Outcome};
use crate::types::ServerId;

/// The first byte of a request.
pub const REQUEST_TAG: u8 = 0x40;
/// The first byte of a response.
pub const RESPONSE_TAG: u8 = 0x41;

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

/// A client's request: apply `command` and tell me the outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// The client process.
    pub client: u64,
    /// The operation's number within the process.
    pub seq: u64,
    /// The command.
    pub command: Command,
}

/// What a server answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// The command was applied with this outcome.
    Outcome(Outcome),
    /// This server is not the leader; try `leader` if it knows one.
    NotLeader {
        /// The leader, if known.
        leader: Option<ServerId>,
    },
}

/// A server's response to a [`Request`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// The client process the request named.
    pub client: u64,
    /// The operation's number the request named.
    pub seq: u64,
    /// The answer.
    pub reply: Reply,
}

/// Whether a payload is a client request or response rather than a protocol frame.
#[must_use]
pub fn is_client(payload: &[u8]) -> bool {
    matches!(payload.first(), Some(&REQUEST_TAG | &RESPONSE_TAG))
}

fn header(bytes: &mut Bytes, tag: u8) -> io::Result<(u64, u64)> {
    if bytes.len() < 17 {
        return Err(bad("client packet too short"));
    }
    if bytes.get_u8() != tag {
        return Err(bad("client packet tag"));
    }
    Ok((bytes.get_u64_le(), bytes.get_u64_le()))
}

fn put_bytes(out: &mut BytesMut, bytes: &[u8]) {
    out.put_u32_le(u32::try_from(bytes.len()).expect("length fits u32"));
    out.put_slice(bytes);
}

fn get_bytes(rest: &mut Bytes) -> io::Result<Bytes> {
    if rest.len() < 4 {
        return Err(bad("client packet torn"));
    }
    let len = rest.get_u32_le() as usize;
    if rest.len() < len {
        return Err(bad("client packet torn"));
    }
    Ok(rest.split_to(len))
}

impl Request {
    /// The wire form.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(48);
        out.put_u8(REQUEST_TAG);
        out.put_u64_le(self.client);
        out.put_u64_le(self.seq);
        out.put_slice(&self.command.encode());
        out.freeze()
    }

    /// Parses a request.
    ///
    /// # Errors
    ///
    /// `InvalidData` for anything [`encode`](Self::encode) did not produce.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        let (client, seq) = header(&mut bytes, REQUEST_TAG)?;
        let command = Command::decode(bytes)?;
        Ok(Self {
            client,
            seq,
            command,
        })
    }
}

impl Response {
    /// The wire form.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(32);
        out.put_u8(RESPONSE_TAG);
        out.put_u64_le(self.client);
        out.put_u64_le(self.seq);
        match &self.reply {
            Reply::Outcome(Outcome::Done) => out.put_u8(0),
            Reply::Outcome(Outcome::Swapped(swapped)) => {
                out.put_u8(1);
                out.put_u8(u8::from(*swapped));
            }
            Reply::Outcome(Outcome::Value(value)) => {
                out.put_u8(2);
                match value {
                    Some(value) => {
                        out.put_u8(1);
                        put_bytes(&mut out, value);
                    }
                    None => out.put_u8(0),
                }
            }
            Reply::NotLeader { leader } => {
                out.put_u8(3);
                out.put_u64_le(leader.map_or(u64::MAX, |l| l.0));
            }
        }
        out.freeze()
    }

    /// Parses a response.
    ///
    /// # Errors
    ///
    /// `InvalidData` for anything [`encode`](Self::encode) did not produce.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        let (client, seq) = header(&mut bytes, RESPONSE_TAG)?;
        if bytes.is_empty() {
            return Err(bad("client packet torn"));
        }
        let reply = match bytes.get_u8() {
            0 => Reply::Outcome(Outcome::Done),
            1 => {
                if bytes.is_empty() {
                    return Err(bad("client packet torn"));
                }
                Reply::Outcome(Outcome::Swapped(bytes.get_u8() != 0))
            }
            2 => {
                if bytes.is_empty() {
                    return Err(bad("client packet torn"));
                }
                let value = match bytes.get_u8() {
                    0 => None,
                    1 => Some(get_bytes(&mut bytes)?),
                    _ => return Err(bad("client packet malformed")),
                };
                Reply::Outcome(Outcome::Value(value))
            }
            3 => {
                if bytes.len() < 8 {
                    return Err(bad("client packet torn"));
                }
                let leader = match bytes.get_u64_le() {
                    u64::MAX => None,
                    id => Some(ServerId(id)),
                };
                Reply::NotLeader { leader }
            }
            _ => return Err(bad("client packet malformed")),
        };
        if !bytes.is_empty() {
            return Err(bad("client packet has trailing bytes"));
        }
        Ok(Self { client, seq, reply })
    }
}

fn int(v: u64) -> Json {
    i64::try_from(v).map_or_else(|_| Json::Str(v.to_string()), Json::Int)
}

fn text(bytes: &[u8]) -> Json {
    Json::str(&String::from_utf8_lossy(bytes))
}

/// The studio's view of a client packet, `client.request` or `client.response`
/// with the client, the sequence number and the operation or the reply; `None` for
/// a payload that is not one, and `client.malformed` for one that does not decode.
#[must_use]
pub fn studio(payload: &[u8]) -> Option<Json> {
    if !is_client(payload) {
        return None;
    }
    let bytes = Bytes::copy_from_slice(payload);
    let malformed = || {
        Json::obj(vec![
            ("type", Json::str("client.malformed")),
            ("len", int(payload.len() as u64)),
        ])
    };
    Some(if payload[0] == REQUEST_TAG {
        let Ok(request) = Request::decode(bytes) else {
            return Some(malformed());
        };
        let op = match &request.command {
            Command::Put { .. } => "put",
            Command::Delete { .. } => "delete",
            Command::Cas { .. } => "cas",
            Command::Get { .. } => "get",
        };
        Json::obj(vec![
            ("type", Json::str("client.request")),
            ("client", int(request.client)),
            ("seq", int(request.seq)),
            ("op", Json::str(op)),
            ("key", text(request.command.key())),
        ])
    } else {
        let Ok(response) = Response::decode(bytes) else {
            return Some(malformed());
        };
        let mut fields = vec![
            ("type", Json::str("client.response")),
            ("client", int(response.client)),
            ("seq", int(response.seq)),
        ];
        match &response.reply {
            Reply::Outcome(Outcome::Done) => fields.push(("reply", Json::str("done"))),
            Reply::Outcome(Outcome::Swapped(swapped)) => {
                fields.push(("reply", Json::str("swapped")));
                fields.push(("swapped", Json::Bool(*swapped)));
            }
            Reply::Outcome(Outcome::Value(value)) => {
                fields.push(("reply", Json::str("value")));
                fields.push(("value", value.as_ref().map_or(Json::Null, |v| text(v))));
            }
            Reply::NotLeader { leader } => {
                fields.push(("reply", Json::str("not-leader")));
                fields.push(("leader", leader.map_or(Json::Null, |l| int(l.0))));
            }
        }
        Json::obj(fields)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_responses_round_trip_and_torn_ones_are_refused() {
        let request = Request {
            client: 7 << 32 | 2,
            seq: 9,
            command: Command::Cas {
                key: Bytes::from_static(b"k"),
                expect: None,
                value: Bytes::from_static(b"v"),
            },
        };
        let bytes = request.encode();
        assert!(is_client(&bytes));
        assert_eq!(Request::decode(bytes.clone()).unwrap(), request);
        for cut in 0..bytes.len() {
            assert!(Request::decode(bytes.slice(..cut)).is_err(), "cut at {cut}");
        }
        let replies = [
            Reply::Outcome(Outcome::Done),
            Reply::Outcome(Outcome::Swapped(true)),
            Reply::Outcome(Outcome::Value(None)),
            Reply::Outcome(Outcome::Value(Some(Bytes::from_static(b"v")))),
            Reply::NotLeader { leader: None },
            Reply::NotLeader {
                leader: Some(ServerId(3)),
            },
        ];
        for reply in replies {
            let response = Response {
                client: 1,
                seq: 2,
                reply,
            };
            let bytes = response.encode();
            assert!(is_client(&bytes));
            assert_eq!(Response::decode(bytes.clone()).unwrap(), response);
            for cut in 0..bytes.len() {
                assert!(
                    Response::decode(bytes.slice(..cut)).is_err(),
                    "{response:?} cut at {cut}"
                );
            }
        }
        assert!(Request::decode(Bytes::from_static(b"\x01garbage")).is_err());
    }

    #[test]
    fn the_studio_sees_client_packets_and_nothing_else() {
        let request = Request {
            client: 1,
            seq: 2,
            command: Command::Get {
                key: Bytes::from_static(b"key"),
            },
        };
        let json = studio(&request.encode()).unwrap().to_json().unwrap();
        assert!(json.contains("client.request"), "{json}");
        assert!(json.contains("\"get\""), "{json}");
        let response = Response {
            client: 1,
            seq: 2,
            reply: Reply::NotLeader {
                leader: Some(ServerId(2)),
            },
        };
        let json = studio(&response.encode()).unwrap().to_json().unwrap();
        assert!(json.contains("not-leader"), "{json}");
        assert!(studio(b"\x01not a client packet").is_none());
        let json = studio(&[REQUEST_TAG, 1, 2]).unwrap().to_json().unwrap();
        assert!(json.contains("client.malformed"), "{json}");
    }
}
