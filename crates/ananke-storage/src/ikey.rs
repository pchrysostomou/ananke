//! Internal keys (D-023): a user key with the sequence number of the write, encoded
//! so that plain byte order is user key ascending, then newest write first. The
//! memtable's skiplist and the tables' blocks are ordered by these bytes, so a lookup
//! at a snapshot is one seek: the first entry at or after `(key, snapshot)` whose
//! user key is `key` is the newest write the snapshot can see.
//!
//! ```text
//! escaped user key | 0x00 0x00 | !seq as u64 BE
//! ```
//!
//! The user key is escaped so that a shorter key sorts before every longer key it is
//! a prefix of: each `0x00` byte becomes `0x00 0xFF`, and the terminator `0x00 0x00`
//! is less than any escaped byte pair. The sequence number is inverted so that a
//! higher number sorts first.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};

use crate::Seq;

/// The sequence number a lookup passes to see every write.
pub const LATEST: Seq = u64::MAX;

/// Encodes `user` written at `seq`.
#[must_use]
pub fn encode(user: &[u8], seq: Seq) -> Bytes {
    let mut out = BytesMut::with_capacity(user.len() + 10);
    for &byte in user {
        out.put_u8(byte);
        if byte == 0 {
            out.put_u8(0xff);
        }
    }
    out.put_u8(0);
    out.put_u8(0);
    out.put_u64(!seq);
    out.freeze()
}

/// The smallest internal key of `user`: what a seek to the start of a range passes.
#[must_use]
pub fn lower_bound(user: &[u8]) -> Bytes {
    encode(user, LATEST)
}

/// Decodes an internal key into its user key and sequence number.
///
/// # Errors
///
/// `InvalidData` for bytes no [`encode`] produced.
pub fn decode(ikey: &[u8]) -> io::Result<(Bytes, Seq)> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed internal key");
    let (user, seq) = split(ikey).ok_or_else(bad)?;
    let mut out = BytesMut::with_capacity(user.len());
    let mut bytes = user.iter();
    while let Some(&byte) = bytes.next() {
        out.put_u8(byte);
        if byte == 0 && bytes.next() != Some(&0xff) {
            return Err(bad());
        }
    }
    Ok((out.freeze(), seq))
}

/// The escaped user key and the sequence number, without unescaping: enough to
/// compare user keys, since the escaping preserves equality.
fn split(ikey: &[u8]) -> Option<(&[u8], Seq)> {
    if ikey.len() < 10 {
        return None;
    }
    let (body, seq) = ikey.split_at(ikey.len() - 8);
    let body = body.strip_suffix(&[0, 0])?;
    let seq = !u64::from_be_bytes(seq.try_into().ok()?);
    Some((body, seq))
}

/// The sequence number of an internal key.
#[must_use]
pub fn seq_of(ikey: &[u8]) -> Option<Seq> {
    split(ikey).map(|(_, seq)| seq)
}

/// Whether `ikey` is a write of `user`.
#[must_use]
pub fn is_user(ikey: &[u8], user: &[u8]) -> bool {
    let escaped = lower_bound(user);
    ikey.len() == escaped.len() && ikey[..ikey.len() - 8] == escaped[..escaped.len() - 8]
}

/// Whether two internal keys are writes of the same user key.
#[must_use]
pub fn same_user(a: &[u8], b: &[u8]) -> bool {
    match (split(a), split(b)) {
        (Some((ua, _)), Some((ub, _))) => ua == ub,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_is_user_key_then_newest_first() {
        let keys = [
            encode(b"a", 5),
            encode(b"a", 2),
            encode(b"a\0", 9),
            encode(b"a\0b", 1),
            encode(b"ab", 7),
            encode(b"b", 1),
        ];
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1], "{:?} < {:?}", pair[0], pair[1]);
        }
        assert!(lower_bound(b"a") < encode(b"a", u64::MAX - 1));
        assert!(lower_bound(b"a") <= encode(b"a", LATEST));
    }

    #[test]
    fn keys_round_trip_and_garbage_is_refused() {
        for (user, seq) in [
            (&b""[..], 0),
            (b"k", 1),
            (b"a\0b\0\0", 42),
            (b"\xff\x00\xff", u64::MAX),
        ] {
            let ikey = encode(user, seq);
            assert_eq!(decode(&ikey).unwrap(), (Bytes::copy_from_slice(user), seq));
            assert_eq!(seq_of(&ikey), Some(seq));
            assert!(is_user(&ikey, user));
            assert!(same_user(&ikey, &encode(user, 7)));
        }
        assert!(!is_user(&encode(b"a", 1), b"ab"));
        assert!(!same_user(&encode(b"a", 1), &encode(b"b", 1)));
        assert!(decode(b"").is_err());
        assert!(decode(b"abc").is_err());
        assert!(decode(b"a\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00").is_err());
        assert!(decode(b"a\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00").is_err());
    }
}
