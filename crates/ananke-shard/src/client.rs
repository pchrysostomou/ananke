//! A range and a generation on every client message, and the client's descriptor
//! cache (SHARD.md §3, §4; §12 Stage B and Stage C's routing).
//!
//! A client of a node says which range its key belongs to, because the node hosts
//! many and the key alone does not say, and the generation of the descriptor it
//! routed by (Q10). The envelope is [`ananke_raft::client`]'s request and response
//! with the range in front of them: `tag | range: u64 | generation: u64 | the raft
//! packet` for a request and `tag | range: u64 | the raft packet` for a response,
//! little-endian.
//!
//! It is an envelope and not a field inside [`ananke_raft::client::Request`] on
//! purpose. The range layer's boundary (Q40) is that `ananke-raft` names a range only
//! where the trace needs one; the client protocol of a *group* is the same protocol
//! whether or not a node hosts several, and the node is what has to tell them apart.
//!
//! [`Cache`] is where a client keeps what it knows of the ranges: an ordered map from
//! end key to descriptor with a leader hint per range, merged by §1's generation rule.
//! It is advisory — a server checks every key against its own descriptor, never the
//! client's — so a stale cache costs a round trip and never a wrong answer (§3).
// PROPOSED(D-097): the generation on every request, and the client's cache.

use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Unbounded};

use ananke_raft::client::{Request, Response};
use ananke_raft::types::ServerId;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::descriptor::RangeDescriptor;
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

/// A client's request, with the range its key belongs to and the generation of the
/// descriptor it routed by (SHARD.md §3, Q10).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangedRequest {
    /// The range.
    pub range: RangeId,
    /// The generation the client believes that range is at: what the trace's
    /// `ClientSend` carries, and what check 17 reads a resend against (§8).
    // PROPOSED(D-097)
    pub generation: u64,
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
        let mut out = BytesMut::with_capacity(17 + inner.len());
        out.put_u8(RANGED_REQUEST_TAG);
        out.put_u64_le(self.range.get());
        out.put_u64_le(self.generation);
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
        if bytes.len() < 8 {
            return Err(bad("ranged client packet too short"));
        }
        let generation = bytes.get_u64_le();
        Ok(Self {
            range,
            generation,
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

/// One entry of a [`Cache`]: a span the client believes a range owns, at the
/// generation it learned it at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cached {
    /// The range.
    pub range: RangeId,
    /// The span's first key, encoded.
    pub start: Bytes,
    /// The key past the span's last, encoded.
    pub end: Bytes,
    /// The descriptor's generation (SHARD.md §1).
    pub generation: u64,
    /// The range's voters as the descriptor named them.
    pub voters: Vec<ServerId>,
}

impl Cached {
    /// Whether `key`, encoded, lies in the entry's span.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start[..] <= *key && *key < self.end[..]
    }
}

/// A client's descriptor cache (SHARD.md §3): an ordered map from end key to the
/// span a range owns, with a leader hint per range, merged by §1's generation rule.
///
/// **The merge.** A descriptor learned replaces the cached entries it overlaps that
/// carry a lower generation, and where an entry of a higher generation overlaps it,
/// the entry stays and the learned descriptor is dropped for that part. An entry of
/// a lower generation is replaced for the part the learned descriptor overlaps and
/// kept for the rest: what the client believed of the keys outside the learned span
/// is still its best knowledge of them, at the generation it had. An entry at the
/// learned descriptor's own generation stays too — two owners of one key at one
/// generation is a contradiction the rule of §1 excludes, and the first learned
/// keeps its place. So the entries never overlap, and a lookup finds at most one.
///
/// Descriptors are learned from meta lookups and from `RangeMismatch` (§3); a
/// `RangeMismatch` that carries nothing evicts the entry, and the client looks the
/// key up again. The cache is advisory: with the server's three checks a stale
/// cache costs a round trip and never a wrong answer, which is the rule
/// `TrustStaleDescriptor` breaks on the server (§10, Q38).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cache {
    /// The entries, keyed by end key: no two overlap.
    entries: BTreeMap<Bytes, Cached>,
    /// The leader last heard of, per range.
    hints: BTreeMap<RangeId, ServerId>,
}

impl Cache {
    /// An empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A cache holding `descriptors`, learned in order.
    #[must_use]
    pub fn seeded<'a>(descriptors: impl IntoIterator<Item = &'a RangeDescriptor>) -> Self {
        let mut cache = Self::new();
        for descriptor in descriptors {
            cache.learn(descriptor);
        }
        cache
    }

    /// The entry whose span contains `key`, encoded, if the cache holds one.
    #[must_use]
    pub fn lookup(&self, key: &[u8]) -> Option<&Cached> {
        self.entries
            .range::<[u8], _>((Excluded(key), Unbounded))
            .next()
            .map(|(_, entry)| entry)
            .filter(|entry| entry.contains(key))
    }

    /// Every entry, in key order.
    pub fn entries(&self) -> impl Iterator<Item = &Cached> {
        self.entries.values()
    }

    /// Merges `descriptor` by the generation rule, and says whether the cache
    /// changed.
    pub fn learn(&mut self, descriptor: &RangeDescriptor) -> bool {
        if descriptor.start >= descriptor.end {
            return false;
        }
        let (start, end, generation) = (
            descriptor.start.clone(),
            descriptor.end.clone(),
            descriptor.generation,
        );
        // The entries the learned span overlaps: every entry whose end lies past the
        // span's start, up to the first whose start lies at or past the span's end.
        let overlapping: Vec<Bytes> = self
            .entries
            .range::<[u8], _>((Excluded(&start[..]), Unbounded))
            .take_while(|(_, entry)| entry.start < end)
            .map(|(end, _)| end.clone())
            .collect();
        let mut changed = false;
        // The parts of the learned span not held at its generation or above.
        let mut uncovered: Vec<(Bytes, Bytes)> = vec![(start.clone(), end.clone())];
        for key in overlapping {
            let entry = self.entries.remove(&key).expect("listed");
            if entry.generation >= generation {
                // The entry stays, and the learned descriptor is dropped for the
                // part it covers.
                uncovered = uncovered
                    .into_iter()
                    .flat_map(|(s, e)| {
                        let mut parts = Vec::with_capacity(2);
                        if s < entry.start {
                            parts.push((s.clone(), e.clone().min(entry.start.clone())));
                        }
                        if e > entry.end {
                            parts.push((s.max(entry.end.clone()), e));
                        }
                        parts
                    })
                    .filter(|(s, e)| s < e)
                    .collect();
                self.entries.insert(key, entry);
                continue;
            }
            // A lower generation: replaced for the overlapped part, kept for the rest.
            changed = true;
            if entry.start < start {
                let left = Cached {
                    end: start.clone(),
                    ..entry.clone()
                };
                self.entries.insert(left.end.clone(), left);
            }
            if entry.end > end {
                let right = Cached {
                    start: end.clone(),
                    ..entry.clone()
                };
                self.entries.insert(right.end.clone(), right);
            }
        }
        for (s, e) in uncovered {
            changed = true;
            self.entries.insert(
                e.clone(),
                Cached {
                    range: descriptor.range,
                    start: s,
                    end: e,
                    generation,
                    voters: descriptor.voters.clone(),
                },
            );
        }
        changed
    }

    /// Forgets the entry that contains `key`: a `RangeMismatch` that carried nothing
    /// (SHARD.md §3).
    pub fn evict(&mut self, key: &[u8]) -> Option<Cached> {
        let end = self.lookup(key).map(|entry| entry.end.clone())?;
        self.entries.remove(&end)
    }

    /// The leader last heard of for `range`.
    #[must_use]
    pub fn hint(&self, range: RangeId) -> Option<ServerId> {
        self.hints.get(&range).copied()
    }

    /// Records the leader last heard of for `range`.
    pub fn set_hint(&mut self, range: RangeId, leader: ServerId) {
        self.hints.insert(range, leader);
    }

    /// Forgets the leader hint for `range`.
    pub fn clear_hint(&mut self, range: RangeId) {
        self.hints.remove(&range);
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
            generation: 3,
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
            generation: 1,
            request: request(),
        }
        .encode();
        assert!(RangedRequest::decode(wire.slice(0..5)).is_err());
        assert!(RangedRequest::decode(wire.slice(0..12)).is_err());
        assert!(RangedResponse::decode(wire.clone()).is_err());
        assert!(RangedRequest::decode(Bytes::new()).is_err());
    }

    fn descriptor(
        range: u64,
        start: &'static str,
        end: &'static str,
        generation: u64,
    ) -> RangeDescriptor {
        RangeDescriptor {
            range: RangeId(range),
            start: Bytes::from_static(start.as_bytes()),
            end: Bytes::from_static(end.as_bytes()),
            generation,
            voters: vec![ServerId(1), ServerId(2), ServerId(3)],
            state: ananke_env::RangeState::Live,
        }
    }

    fn spans(cache: &Cache) -> Vec<(u64, &str, &str, u64)> {
        cache
            .entries()
            .map(|entry| {
                (
                    entry.range.get(),
                    std::str::from_utf8(&entry.start).unwrap(),
                    std::str::from_utf8(&entry.end).unwrap(),
                    entry.generation,
                )
            })
            .collect()
    }

    #[test]
    fn a_seeded_cache_answers_a_lookup_by_span() {
        let cache = Cache::seeded(&[descriptor(2, "a", "k", 1), descriptor(3, "k", "z", 1)]);
        assert_eq!(cache.lookup(b"a").map(|e| e.range), Some(RangeId(2)));
        assert_eq!(cache.lookup(b"j").map(|e| e.range), Some(RangeId(2)));
        assert_eq!(cache.lookup(b"k").map(|e| e.range), Some(RangeId(3)));
        assert_eq!(cache.lookup(b"y").map(|e| e.range), Some(RangeId(3)));
        assert_eq!(cache.lookup(b"z"), None);
        assert_eq!(cache.lookup(b"0"), None);
    }

    #[test]
    fn a_higher_generation_replaces_the_part_it_overlaps_and_keeps_the_rest() {
        // A client that knew one range over everything, at generation 0, learns the
        // right half's descriptor from a `RangeMismatch`: the stale entry is
        // replaced where the learned span overlaps it and kept on either side.
        let mut cache = Cache::seeded(&[descriptor(2, "a", "z", 0)]);
        assert!(cache.learn(&descriptor(3, "k", "p", 1)));
        assert_eq!(
            spans(&cache),
            vec![(2, "a", "k", 0), (3, "k", "p", 1), (2, "p", "z", 0)]
        );
        assert_eq!(cache.lookup(b"m").map(|e| e.range), Some(RangeId(3)));
        assert_eq!(cache.lookup(b"q").map(|e| e.range), Some(RangeId(2)));
        // And learning it again changes nothing.
        assert!(!cache.learn(&descriptor(3, "k", "p", 1)));
    }

    #[test]
    fn a_lower_or_equal_generation_is_dropped_where_a_higher_one_stands() {
        let mut cache = Cache::seeded(&[descriptor(3, "k", "p", 2)]);
        // Lower: dropped for the overlapped part, kept for the uncovered parts.
        assert!(cache.learn(&descriptor(2, "a", "z", 1)));
        assert_eq!(
            spans(&cache),
            vec![(2, "a", "k", 1), (3, "k", "p", 2), (2, "p", "z", 1)]
        );
        // Equal, a different range over the same keys: the entry stays.
        assert!(!cache.learn(&descriptor(4, "k", "p", 2)));
        assert_eq!(cache.lookup(b"m").map(|e| e.range), Some(RangeId(3)));
        // Wholly inside a higher one: nothing to learn.
        assert!(!cache.learn(&descriptor(5, "l", "m", 1)));
        assert_eq!(spans(&cache).len(), 3);
    }

    #[test]
    fn an_eviction_forgets_the_entry_and_a_hint_is_kept_per_range() {
        let mut cache = Cache::seeded(&[descriptor(2, "a", "k", 1), descriptor(3, "k", "z", 1)]);
        cache.set_hint(RangeId(3), ServerId(2));
        assert_eq!(cache.hint(RangeId(3)), Some(ServerId(2)));
        assert_eq!(cache.hint(RangeId(2)), None);
        assert_eq!(cache.evict(b"m").map(|e| e.range), Some(RangeId(3)));
        assert_eq!(cache.lookup(b"m"), None);
        assert_eq!(cache.lookup(b"b").map(|e| e.range), Some(RangeId(2)));
        assert_eq!(cache.evict(b"m"), None);
        cache.clear_hint(RangeId(3));
        assert_eq!(cache.hint(RangeId(3)), None);
        // An empty span is not a descriptor the cache takes.
        assert!(!cache.learn(&descriptor(9, "q", "q", 5)));
    }
}
