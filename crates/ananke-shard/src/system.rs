//! The system tenant's layout (SHARD.md §1, §2; Q4, Q5): what range 0, the root,
//! and range 1, the meta range, hold, and the keys they hold it under.
//!
//! Tenant 1 is the system tenant (`SYSTEM_TENANT`, D-060). Its table 0 is the root
//! span, range 0's, and its table 1 the meta span, range 1's; Phase 5's catalog comes
//! later. Range 0 holds the meta range's descriptor, the range-id counter and each
//! node's lease of a block of ids (§5, Q17), the node records (§2, Q8) and the digest
//! of the bootstrap configuration (Q7). Range 1 holds a record per descriptor it has
//! been told, keyed by end key, each carrying its start, range, generation and voters
//! (§1, Q4). Neither splits nor merges in Phase 3.
//!
//! This slice writes both at bootstrap and reads neither: the lookups, the refill
//! and `MetaUpdate` are later slices' (§12, Stage C).

use std::io;
use std::net::SocketAddr;
use std::ops::Range;

use ananke_raft::apply::SYSTEM_TENANT;
use ananke_raft::store::key;
use ananke_raft::types::ServerId;
use ananke_storage::crc32c::crc32c;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::range::RangeId;

/// Range 0, the root (SHARD.md §1).
pub const ROOT_RANGE: RangeId = RangeId(0);
/// Range 1, the meta range (SHARD.md §1).
pub const META_RANGE: RangeId = RangeId(1);
/// The first id a user range takes: SPEC's "the rest of the keyspace" is range 2 (§2).
pub const FIRST_USER_RANGE: u64 = 2;

const ROOT_TABLE: u64 = 0;
const META_TABLE: u64 = 1;

/// Range 0's span: the root table of the system tenant.
#[must_use]
pub fn root_span() -> Range<Bytes> {
    key(SYSTEM_TENANT, ROOT_TABLE, &[])..key(SYSTEM_TENANT, META_TABLE, &[])
}

/// Range 1's span: the meta table of the system tenant.
#[must_use]
pub fn meta_span() -> Range<Bytes> {
    key(SYSTEM_TENANT, META_TABLE, &[])..key(SYSTEM_TENANT, META_TABLE + 1, &[])
}

/// Where range 0 keeps the meta range's descriptor (§1: found through range 0).
#[must_use]
pub fn meta_descriptor_key() -> Bytes {
    key(SYSTEM_TENANT, ROOT_TABLE, b"meta")
}

/// Where range 0 keeps the range-id counter (§5, Q17): the next id never granted.
#[must_use]
pub fn counter_key() -> Bytes {
    key(SYSTEM_TENANT, ROOT_TABLE, b"counter")
}

/// Where range 0 keeps node `id`'s record (§2, Q8): its address.
#[must_use]
pub fn node_key(id: ServerId) -> Bytes {
    let mut name = BytesMut::with_capacity(13);
    name.put_slice(b"node/");
    name.put_u64(id.0);
    key(SYSTEM_TENANT, ROOT_TABLE, &name)
}

/// Where range 0 keeps the digest of the bootstrap configuration (§2, Q7), which is
/// also what says a store was bootstrapped: a fresh bootstrap node writes it once.
#[must_use]
pub fn digest_key() -> Bytes {
    key(SYSTEM_TENANT, ROOT_TABLE, b"bootstrap")
}

/// Where range 1 keeps the record of the descriptor whose span ends at `end` (§1,
/// Q4): a lookup of `k` reads the first record whose end key is above `k`.
#[must_use]
pub fn meta_record_key(end: &[u8]) -> Bytes {
    key(SYSTEM_TENANT, META_TABLE, end)
}

/// One record of the meta range (§1): the descriptor's start, range, generation and
/// voters; the end is its key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaRecord {
    /// The span's first key.
    pub start: Bytes,
    /// The range.
    pub range: RangeId,
    /// The descriptor's generation.
    pub generation: u64,
    /// Its voters.
    pub voters: Vec<ServerId>,
}

impl MetaRecord {
    /// The record's bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(32 + self.start.len());
        out.put_u32_le(u32::try_from(self.start.len()).expect("a key fits u32"));
        out.put_slice(&self.start);
        out.put_u64_le(self.range.get());
        out.put_u64_le(self.generation);
        out.put_u32_le(u32::try_from(self.voters.len()).expect("voters fit u32"));
        for voter in &self.voters {
            out.put_u64_le(voter.0);
        }
        out.freeze()
    }

    /// The record `encode` wrote.
    ///
    /// # Errors
    ///
    /// Bytes that are not one.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        let bad = || io::Error::new(io::ErrorKind::InvalidData, "a meta record");
        if bytes.len() < 4 {
            return Err(bad());
        }
        let n = bytes.get_u32_le() as usize;
        if bytes.len() < n + 20 {
            return Err(bad());
        }
        let start = bytes.split_to(n);
        let range = RangeId(bytes.get_u64_le());
        let generation = bytes.get_u64_le();
        let n = bytes.get_u32_le() as usize;
        if bytes.len() != n * 8 {
            return Err(bad());
        }
        let voters = (0..n).map(|_| ServerId(bytes.get_u64_le())).collect();
        Ok(Self {
            start,
            range,
            generation,
            voters,
        })
    }
}

/// The digest of the bootstrap configuration (§2, Q7): what every bootstrap node
/// must share, over the bootstrap nodes, the address book and the user ranges
/// configuration fixes, as a CRC-32C of their encoding. A digest that differs
/// between two bootstrap nodes is two clusters, which nothing in this slice tells
/// apart yet; issue #43 is the one that asks for the rule.
#[must_use]
pub fn digest(
    bootstrap: &[ServerId],
    servers: &[(ServerId, SocketAddr)],
    ranges: &[(RangeId, Bytes, Bytes)],
) -> u32 {
    let mut out = BytesMut::new();
    for node in bootstrap {
        out.put_u64_le(node.0);
    }
    for (id, addr) in servers {
        out.put_u64_le(id.0);
        out.put_slice(addr.to_string().as_bytes());
        out.put_u8(0);
    }
    for (id, start, end) in ranges {
        out.put_u64_le(id.get());
        out.put_u32_le(u32::try_from(start.len()).expect("a key fits u32"));
        out.put_slice(start);
        out.put_u32_le(u32::try_from(end.len()).expect("a key fits u32"));
        out.put_slice(end);
    }
    crc32c(&out)
}

/// The digest's bytes as range 0 keeps them.
#[must_use]
pub fn encode_digest(digest: u32) -> Bytes {
    Bytes::copy_from_slice(&digest.to_le_bytes())
}

/// The counter's bytes as range 0 keeps them.
#[must_use]
pub fn encode_counter(next: u64) -> Bytes {
    Bytes::copy_from_slice(&next.to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_system_spans_are_the_two_tables_of_tenant_one_in_order() {
        let root = root_span();
        let meta = meta_span();
        assert_eq!(
            root.end, meta.start,
            "the root span ends where the meta span starts"
        );
        assert!(root.start < root.end && meta.start < meta.end);
        assert!(root.start <= meta_descriptor_key() && meta_descriptor_key() < root.end);
        assert!(root.start <= counter_key() && counter_key() < root.end);
        assert!(root.start <= node_key(ServerId(3)) && node_key(ServerId(3)) < root.end);
        assert!(root.start <= digest_key() && digest_key() < root.end);
        let record = meta_record_key(b"\x00\x00\x00\x00\x00\x00\x00\x02k2");
        assert!(meta.start <= record && record < meta.end);
        assert_eq!(&root.start[..8], &SYSTEM_TENANT.to_be_bytes());
    }

    #[test]
    fn a_meta_record_survives_its_encoding() {
        let record = MetaRecord {
            start: Bytes::from_static(b"\x00\x00\x00\x00\x00\x00\x00\x02k0"),
            range: RangeId(2),
            generation: 1,
            voters: vec![ServerId(1), ServerId(2), ServerId(3)],
        };
        assert_eq!(MetaRecord::decode(record.encode()).unwrap(), record);
        assert!(MetaRecord::decode(Bytes::from_static(b"\x09")).is_err());
    }

    #[test]
    fn the_digest_reads_every_field_it_is_over() {
        let addr = |n: u16| SocketAddr::from(([10, 0, 0, 1], n));
        let bootstrap = [ServerId(1), ServerId(2), ServerId(3)];
        let servers = [
            (ServerId(1), addr(1)),
            (ServerId(2), addr(2)),
            (ServerId(3), addr(3)),
        ];
        let ranges = [(
            RangeId(2),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"z"),
        )];
        let base = digest(&bootstrap, &servers, &ranges);
        assert_eq!(base, digest(&bootstrap, &servers, &ranges), "deterministic");
        assert_ne!(
            base,
            digest(&bootstrap[..2], &servers, &ranges),
            "the bootstrap nodes"
        );
        let moved = [
            (ServerId(1), addr(1)),
            (ServerId(2), addr(2)),
            (ServerId(3), addr(9)),
        ];
        assert_ne!(base, digest(&bootstrap, &moved, &ranges), "an address");
        let split = [(
            RangeId(2),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"m"),
        )];
        assert_ne!(base, digest(&bootstrap, &servers, &split), "a span");
    }
}
