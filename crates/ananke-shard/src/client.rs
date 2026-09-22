//! A range on every client message (SHARD.md §4, §12 Stage B).
//!
//! A client of a node says which range its key belongs to, because the node hosts
//! many and the key alone does not say: this stage routes nothing, and a client takes
//! its key's range from the scenario's fixed map (SHARD.md, Stage B). The envelope is
//! [`ananke_raft::client`]'s request and response with the range in front of them:
//! `tag | range: u64 | the raft packet`, little-endian.
//!
//! It is an envelope and not a field inside [`ananke_raft::client::Request`] on
//! purpose. The range layer's boundary (Q40) is that `ananke-raft` names a range only
//! where the trace needs one; the client protocol of a *group* is the same protocol
//! whether or not a node hosts several, and the node is what has to tell them apart.
//!
//! Stage C gives a client the descriptor cache that computes this range from the key;
//! until then the envelope carries what the scenario decided.

use std::io;

use ananke_raft::client::{Request, Response};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::range::RangeId;

/// The first byte of a ranged request: above [`ananke_raft::client::RESPONSE_TAG`]
/// and above every message tag, so a node tells the three apart by the first byte as
/// a server does today.
pub const RANGED_REQUEST_TAG: u8 = 0x50;
/// The first byte of a ranged response.
pub const RANGED_RESPONSE_TAG: u8 = 0x51;

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

/// Whether a payload is a ranged client packet rather than a protocol frame or a
/// bare `ananke-raft` client packet.
#[must_use]
pub fn is_ranged(payload: &[u8]) -> bool {
    matches!(
        payload.first(),
        Some(&RANGED_REQUEST_TAG | &RANGED_RESPONSE_TAG)
    )
}

/// A client's request, with the range its key belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangedRequest {
    /// The range.
    pub range: RangeId,
    /// The request.
    pub request: Request,
}

/// A node's response, with the range it was answered for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangedResponse {
    /// The range.
    pub range: RangeId,
    /// The response.
    pub response: Response,
}

fn header(bytes: &mut Bytes, tag: u8) -> io::Result<RangeId> {
    if bytes.len() < 9 {
        return Err(bad("ranged client packet too short"));
    }
    if bytes.get_u8() != tag {
        return Err(bad("ranged client packet tag"));
    }
    Ok(RangeId(bytes.get_u64_le()))
}

impl RangedRequest {
    /// The wire form.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let inner = self.request.encode();
        let mut out = BytesMut::with_capacity(9 + inner.len());
        out.put_u8(RANGED_REQUEST_TAG);
        out.put_u64_le(self.range.get());
        out.put_slice(&inner);
        out.freeze()
    }

    /// Parses a ranged request.
    ///
    /// # Errors
    ///
    /// `InvalidData` for anything [`encode`](Self::encode) did not produce.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        let range = header(&mut bytes, RANGED_REQUEST_TAG)?;
        Ok(Self {
            range,
            request: Request::decode(bytes)?,
        })
    }
}

impl RangedResponse {
    /// The wire form.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let inner = self.response.encode();
        let mut out = BytesMut::with_capacity(9 + inner.len());
        out.put_u8(RANGED_RESPONSE_TAG);
        out.put_u64_le(self.range.get());
        out.put_slice(&inner);
        out.freeze()
    }

    /// Parses a ranged response.
    ///
    /// # Errors
    ///
    /// `InvalidData` for anything [`encode`](Self::encode) did not produce.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        let range = header(&mut bytes, RANGED_RESPONSE_TAG)?;
        Ok(Self {
            range,
            response: Response::decode(bytes)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use ananke_raft::apply::{Command, Outcome};
    use ananke_raft::client::{Reply, is_client};

    use super::*;

    fn request() -> Request {
        Request {
            client: 3,
            seq: 9,
            command: Command::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
            },
        }
    }

    #[test]
    fn a_ranged_request_carries_its_range_and_the_raft_request() {
        let ranged = RangedRequest {
            range: RangeId(7),
            request: request(),
        };
        let wire = ranged.encode();
        assert!(is_ranged(&wire));
        // Told apart from a bare raft client packet and from a protocol frame by
        // the first byte, which is what the node's `net` task reads.
        assert!(!is_client(&wire));
        assert_eq!(RangedRequest::decode(wire).unwrap(), ranged);
    }

    #[test]
    fn a_ranged_response_carries_the_range_it_was_answered_for() {
        let ranged = RangedResponse {
            range: RangeId(4),
            response: Response {
                client: 3,
                seq: 9,
                reply: Reply::Outcome(Outcome::Done),
            },
        };
        let wire = ranged.encode();
        assert!(is_ranged(&wire));
        assert_eq!(RangedResponse::decode(wire).unwrap(), ranged);
    }

    #[test]
    fn a_torn_or_mistagged_packet_is_refused() {
        let wire = RangedRequest {
            range: RangeId(1),
            request: request(),
        }
        .encode();
        assert!(RangedRequest::decode(wire.slice(0..5)).is_err());
        assert!(RangedResponse::decode(wire.clone()).is_err());
        assert!(RangedRequest::decode(Bytes::new()).is_err());
    }
}
