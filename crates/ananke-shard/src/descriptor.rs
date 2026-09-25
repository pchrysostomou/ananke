//! A range's descriptor (SHARD.md §1; Q3, Q5, Q6): the range-local copy, the
//! authority, kept under the replica's own key prefix beside its Raft state
//! (`PURPOSE_DESCRIPTOR`, D-060) and written in the same batch as the apply that
//! changes it. In Stage C's first slice only a bootstrap writes one (§2); a split, a
//! subsume, a merge and a `C_new` write theirs in the slices that build them.
//!
//! A span is an interval of the engine's key order over **encoded** keys,
//! `tenant | table | user_key` (SPEC §2.6), so the system ranges' spans in tenant 1
//! and a user range's in tenant 2 are intervals of one order and check 19's tiling
//! is one question. The last range's end is the keyspace's end, which is
//! [`keyspace_end`].

use std::io;

use ananke_env::RangeState;
use ananke_raft::types::ServerId;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::range::RangeId;

/// The key past every key: one tenant past the last, which no encoded key reaches.
/// The last range's end (SHARD.md §1).
#[must_use]
pub fn keyspace_end() -> Bytes {
    Bytes::copy_from_slice(&u64::MAX.to_be_bytes())
}

/// A range's descriptor as every replica holds it (SHARD.md §1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeDescriptor {
    /// The range.
    pub range: RangeId,
    /// The span's first key, encoded.
    pub start: Bytes,
    /// The key past the span's last, encoded.
    pub end: Bytes,
    /// Rises at every split, merge and configuration change of the range (Q6).
    pub generation: u64,
    /// The plain configuration the range last committed: never joint, never a
    /// learner (SHARD.md §1).
    pub voters: Vec<ServerId>,
    /// `Live`, or the merge states §6 builds.
    pub state: RangeState,
}

const TAG_LIVE: u8 = 0;
const TAG_MERGING: u8 = 1;
const TAG_SUBSUMED: u8 = 2;

impl RangeDescriptor {
    /// Whether `key`, encoded, lies in the span.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start[..] <= *key && *key < self.end[..]
    }

    /// The descriptor's bytes: `range | start_len | start | end_len | end | generation
    /// | voters_len | voters | state`, every integer little-endian.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(64 + self.start.len() + self.end.len());
        out.put_u64_le(self.range.get());
        out.put_u32_le(u32::try_from(self.start.len()).expect("a key fits u32"));
        out.put_slice(&self.start);
        out.put_u32_le(u32::try_from(self.end.len()).expect("a key fits u32"));
        out.put_slice(&self.end);
        out.put_u64_le(self.generation);
        out.put_u32_le(u32::try_from(self.voters.len()).expect("voters fit u32"));
        for voter in &self.voters {
            out.put_u64_le(voter.0);
        }
        out.put_u8(match self.state {
            RangeState::Live => TAG_LIVE,
            RangeState::Merging => TAG_MERGING,
            RangeState::Subsumed => TAG_SUBSUMED,
            _ => TAG_LIVE,
        });
        out.freeze()
    }

    /// The descriptor `encode` wrote.
    ///
    /// # Errors
    ///
    /// Bytes that are not one.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        let bad = || io::Error::new(io::ErrorKind::InvalidData, "a range descriptor");
        let take = |bytes: &mut Bytes, n: usize| -> io::Result<Bytes> {
            if bytes.len() < n {
                return Err(bad());
            }
            Ok(bytes.split_to(n))
        };
        if bytes.len() < 8 {
            return Err(bad());
        }
        let range = RangeId(bytes.get_u64_le());
        if bytes.len() < 4 {
            return Err(bad());
        }
        let n = bytes.get_u32_le() as usize;
        let start = take(&mut bytes, n)?;
        if bytes.len() < 4 {
            return Err(bad());
        }
        let n = bytes.get_u32_le() as usize;
        let end = take(&mut bytes, n)?;
        if bytes.len() < 12 {
            return Err(bad());
        }
        let generation = bytes.get_u64_le();
        let n = bytes.get_u32_le() as usize;
        if bytes.len() < n * 8 + 1 {
            return Err(bad());
        }
        let voters = (0..n).map(|_| ServerId(bytes.get_u64_le())).collect();
        let state = match bytes.get_u8() {
            TAG_LIVE => RangeState::Live,
            TAG_MERGING => RangeState::Merging,
            TAG_SUBSUMED => RangeState::Subsumed,
            _ => return Err(bad()),
        };
        if !bytes.is_empty() {
            return Err(bad());
        }
        Ok(Self {
            range,
            start,
            end,
            generation,
            voters,
            state,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_descriptor_survives_its_encoding_and_knows_its_span() {
        let descriptor = RangeDescriptor {
            range: RangeId(7),
            start: Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x02k2"),
            end: Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x02k4"),
            generation: 3,
            voters: vec![ServerId(1), ServerId(2), ServerId(3)],
            state: RangeState::Live,
        };
        assert_eq!(
            RangeDescriptor::decode(descriptor.encode()).unwrap(),
            descriptor
        );
        assert!(descriptor.contains(b"\x00\x00\x00\x00\x00\x00\x00\x02k3"));
        assert!(!descriptor.contains(b"\x00\x00\x00\x00\x00\x00\x00\x02k4"));
        assert!(!descriptor.contains(b"\x00\x00\x00\x00\x00\x00\x00\x02k1"));
        assert!(RangeDescriptor::decode(Bytes::from_static(b"\x01\x02")).is_err());
        let mut torn = descriptor.encode().to_vec();
        torn.push(0);
        assert!(RangeDescriptor::decode(Bytes::from(torn)).is_err());
    }
}
