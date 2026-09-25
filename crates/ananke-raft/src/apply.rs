//! The state machine adapter (RAFT.md §3): the key-value commands a Raft entry
//! carries, applied to the engine under the user tenant with the applied index in the
//! same batch.
//!
//! A command is `tag: u8 | key_len: u32 | key | ...`: a put with `value_len: u32 |
//! value`, a delete with nothing more, a compare-and-set with `has_expect: u8 |
//! [expect_len: u32 | expect] | value_len: u32 | value`, a get with nothing more.
//! Compare-and-set exists so that an entry applied twice, or a lost write, shows as
//! a wrong boolean in the linearizability check and not only as a stale value later.
//! A get goes through the log until read-index reads arrive (RAFT.md §1, stage C):
//! it is applied like any entry, reads the key at its place in the order, and is
//! linearizable by construction.

use std::io;

use ananke_env::Environment;
use ananke_storage::WriteBatch;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::store::{RaftStore, key};
use crate::types::Index;

/// The tenant user data lives under (SHARD.md §1): tenant 0 is the protocol's
/// own, tenant 1 the system tenant, and the user's data starts at tenant 2.
// PROPOSED(D-060): user data moves from tenant 1 to tenant 2.
pub const USER_TENANT: u64 = 2;

/// The system tenant (SHARD.md §1): the catalogue and the tables the database
/// keeps about itself. Nothing in Stage A writes a key under it; it is named
/// here so that nothing else takes it.
// PROPOSED(D-060): tenant 1 is the system tenant.
pub const SYSTEM_TENANT: u64 = 1;

const USER_TABLE: u64 = 0;

/// A key of the user's key-value store, as the engine sees it.
#[must_use]
pub fn user_key(user: &[u8]) -> Bytes {
    key(USER_TENANT, USER_TABLE, user)
}

/// The user key an encoded key of the user's store carries: the inverse of
/// [`user_key`], `None` for a key of another tenant or table.
// PROPOSED(D-100): a restart reads a split's right half back from its descriptor.
#[must_use]
pub fn user_key_of(encoded: &[u8]) -> Option<&[u8]> {
    let prefix = user_key(&[]);
    encoded.strip_prefix(&prefix[..])
}

/// A command the state machine applies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    /// Set `key` to `value`.
    Put {
        /// The key.
        key: Bytes,
        /// The value.
        value: Bytes,
    },
    /// Remove `key`.
    Delete {
        /// The key.
        key: Bytes,
    },
    /// Set `key` to `value` if it holds `expect` (none for absent); the result says
    /// whether it did.
    Cas {
        /// The key.
        key: Bytes,
        /// The value it must hold, or none for absent.
        expect: Option<Bytes>,
        /// The value to set.
        value: Bytes,
    },
    /// Read `key`: served by the leader's lease or a heartbeat round, never
    /// through the log (RAFT.md §1).
    Get {
        /// The key.
        key: Bytes,
    },
    /// An operator's request that the leader hand leadership to server `to`
    /// (thesis §3.10). Never an entry: the server acts on it directly.
    Transfer {
        /// The server to lead next.
        to: u64,
    },
    /// An operator's request that the group's voters become `voters` (RAFT.md
    /// §1, joint consensus). The command is only the trigger: the leader
    /// catches new servers up as learners and drives the joint and `C_new`
    /// configuration ENTRIES; the command itself is never one.
    Change {
        /// The servers that are to be the voters.
        voters: Vec<u64>,
    },
    /// The meta range's update (SHARD.md §1): descriptors, each encoded by the range
    /// layer (`ananke_shard::descriptor::RangeDescriptor`), which the meta range's
    /// state machine applies as a maximum by generation. An entry, applied by the
    /// range layer before [`apply_command`] sees it; this crate reads nothing of it
    /// (SHARD.md, Q40), and a one-group server never receives one.
    // PROPOSED(D-098)
    MetaUpdate {
        /// The descriptors, encoded.
        descriptors: Vec<Bytes>,
    },
    /// A lookup (SHARD.md §1, §3): asked of the root, the meta range's descriptor;
    /// asked of the meta range, the descriptor of the range whose span holds `key`.
    /// A read, served as a get is and never an entry, and not a command that touches
    /// `key`: it asks *about* it (Q36), so [`Command::key`] is `None`.
    // PROPOSED(D-098)
    Lookup {
        /// The key asked about.
        key: Bytes,
    },
    /// A node's refill of its block of range ids (SHARD.md §5, Q17): asked of range 0,
    /// whose apply grants the block at the counter to `node`, records it with `run`,
    /// the node's run nonce, and advances the counter past it. An entry, applied by
    /// the range layer before [`apply_command`] sees it, as a `MetaUpdate` is; this
    /// crate reads nothing of it, and a one-group server never receives one. It
    /// touches no key of the command's own, so [`Command::key`] is `None`.
    // PROPOSED(D-099)
    Refill {
        /// The asking node.
        node: u64,
        /// The nonce of the node's current run.
        run: u64,
    },
    /// A split of the range at `key` (SHARD.md §5, Q18, Q19): asked by an operator
    /// with `right` zero, and proposed by the leader with `right` the id it took from
    /// its node's block for the right half. An entry the range layer applies —
    /// every replica at the same index re-checks it and writes both halves'
    /// descriptors and the right half's Raft state in one batch — which this crate
    /// reads nothing of, and a one-group server never receives. It touches no key of
    /// its own: `key` is where the span is cut, so [`Command::key`] is `None` and
    /// the range layer checks it against the span itself.
    // PROPOSED(D-100)
    Split {
        /// The right half's first key.
        key: Bytes,
        /// The right half's range id; zero until the leader takes one.
        right: u64,
    },
}

impl Command {
    /// The key the command touches, if it touches one.
    #[must_use]
    pub fn key(&self) -> Option<&Bytes> {
        match self {
            Command::Put { key, .. }
            | Command::Delete { key }
            | Command::Cas { key, .. }
            | Command::Get { key } => Some(key),
            Command::Transfer { .. }
            | Command::Change { .. }
            | Command::MetaUpdate { .. }
            | Command::Lookup { .. }
            | Command::Refill { .. }
            | Command::Split { .. } => None,
        }
    }

    /// Whether the command is a read: served at an applied index, never an entry.
    // PROPOSED(D-098)
    #[must_use]
    pub fn is_read(&self) -> bool {
        matches!(self, Command::Get { .. } | Command::Lookup { .. })
    }
}

/// What applying a command produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A put or delete took effect.
    Done,
    /// Whether a compare-and-set took effect.
    Swapped(bool),
    /// What a get found.
    Value(Option<Bytes>),
    /// A split was refused, at its proposal or at its apply (SHARD.md §5, Q23;
    /// PROPOSED D-100), with the reason.
    Refused(SplitRefusal),
}

/// Why a split was refused (SHARD.md §5): the leader's checks at proposal, for
/// liveness, and every replica's re-check at apply, against shared state.
// PROPOSED(D-100)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitRefusal {
    /// The key is not strictly inside the range's span.
    KeyOutsideSpan,
    /// The range's descriptor is not `Live`.
    NotLive,
    /// The configuration in force at the split is joint, or a change is in flight.
    ConfigurationChanging,
    /// The node's block of range ids holds none and range 0 has not refilled it
    /// (Stage C's question 1, PROPOSED D-092).
    NoRangeId,
    /// The range is a system range, which neither splits nor merges in Phase 3
    /// (SHARD.md §1).
    SystemRange,
}

impl SplitRefusal {
    /// The reason's wire byte.
    #[must_use]
    pub fn code(self) -> u8 {
        match self {
            SplitRefusal::KeyOutsideSpan => 1,
            SplitRefusal::NotLive => 2,
            SplitRefusal::ConfigurationChanging => 3,
            SplitRefusal::NoRangeId => 4,
            SplitRefusal::SystemRange => 5,
        }
    }

    /// The reason a wire byte names.
    #[must_use]
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(SplitRefusal::KeyOutsideSpan),
            2 => Some(SplitRefusal::NotLive),
            3 => Some(SplitRefusal::ConfigurationChanging),
            4 => Some(SplitRefusal::NoRangeId),
            5 => Some(SplitRefusal::SystemRange),
            _ => None,
        }
    }

    /// The reason's name, for the studio.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            SplitRefusal::KeyOutsideSpan => "key-outside-span",
            SplitRefusal::NotLive => "not-live",
            SplitRefusal::ConfigurationChanging => "configuration-changing",
            SplitRefusal::NoRangeId => "no-range-id",
            SplitRefusal::SystemRange => "system-range",
        }
    }
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

fn put_bytes(out: &mut BytesMut, bytes: &[u8]) {
    out.put_u32_le(u32::try_from(bytes.len()).expect("length fits u32"));
    out.put_slice(bytes);
}

fn get_bytes(rest: &mut Bytes) -> io::Result<Bytes> {
    if rest.len() < 4 {
        return Err(bad("command torn"));
    }
    let len = rest.get_u32_le() as usize;
    if rest.len() < len {
        return Err(bad("command torn"));
    }
    Ok(rest.split_to(len))
}

impl Command {
    /// The command's bytes, what an entry's payload carries.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(32);
        match self {
            Command::Put { key, value } => {
                out.put_u8(0);
                put_bytes(&mut out, key);
                put_bytes(&mut out, value);
            }
            Command::Delete { key } => {
                out.put_u8(1);
                put_bytes(&mut out, key);
            }
            Command::Cas { key, expect, value } => {
                out.put_u8(2);
                put_bytes(&mut out, key);
                match expect {
                    Some(expect) => {
                        out.put_u8(1);
                        put_bytes(&mut out, expect);
                    }
                    None => out.put_u8(0),
                }
                put_bytes(&mut out, value);
            }
            Command::Get { key } => {
                out.put_u8(3);
                put_bytes(&mut out, key);
            }
            Command::Transfer { to } => {
                out.put_u8(4);
                out.put_u64_le(*to);
            }
            Command::Change { voters } => {
                out.put_u8(5);
                out.put_u32_le(u32::try_from(voters.len()).expect("voter count fits u32"));
                for voter in voters {
                    out.put_u64_le(*voter);
                }
            }
            Command::MetaUpdate { descriptors } => {
                out.put_u8(6);
                out.put_u16_le(u16::try_from(descriptors.len()).expect("descriptors fit u16"));
                for descriptor in descriptors {
                    put_bytes(&mut out, descriptor);
                }
            }
            Command::Lookup { key } => {
                out.put_u8(7);
                put_bytes(&mut out, key);
            }
            Command::Refill { node, run } => {
                out.put_u8(8);
                out.put_u64_le(*node);
                out.put_u64_le(*run);
            }
            Command::Split { key, right } => {
                out.put_u8(9);
                put_bytes(&mut out, key);
                out.put_u64_le(*right);
            }
        }
        out.freeze()
    }

    /// Parses a command.
    ///
    /// # Errors
    ///
    /// `InvalidData` for anything [`encode`](Self::encode) did not produce.
    pub fn decode(mut bytes: Bytes) -> io::Result<Self> {
        if bytes.is_empty() {
            return Err(bad("command empty"));
        }
        let command = match bytes.get_u8() {
            0 => Command::Put {
                key: get_bytes(&mut bytes)?,
                value: get_bytes(&mut bytes)?,
            },
            1 => Command::Delete {
                key: get_bytes(&mut bytes)?,
            },
            2 => {
                let key = get_bytes(&mut bytes)?;
                if bytes.is_empty() {
                    return Err(bad("command torn"));
                }
                let expect = match bytes.get_u8() {
                    0 => None,
                    1 => Some(get_bytes(&mut bytes)?),
                    _ => return Err(bad("command malformed")),
                };
                Command::Cas {
                    key,
                    expect,
                    value: get_bytes(&mut bytes)?,
                }
            }
            3 => Command::Get {
                key: get_bytes(&mut bytes)?,
            },
            4 => {
                if bytes.len() < 8 {
                    return Err(bad("command torn"));
                }
                Command::Transfer {
                    to: bytes.get_u64_le(),
                }
            }
            5 => {
                if bytes.len() < 4 {
                    return Err(bad("command torn"));
                }
                let count = bytes.get_u32_le() as usize;
                if bytes.len() < count * 8 {
                    return Err(bad("command torn"));
                }
                Command::Change {
                    voters: (0..count).map(|_| bytes.get_u64_le()).collect(),
                }
            }
            6 => {
                if bytes.len() < 2 {
                    return Err(bad("command torn"));
                }
                let count = bytes.get_u16_le();
                let mut descriptors = Vec::with_capacity(usize::from(count));
                for _ in 0..count {
                    descriptors.push(get_bytes(&mut bytes)?);
                }
                Command::MetaUpdate { descriptors }
            }
            7 => Command::Lookup {
                key: get_bytes(&mut bytes)?,
            },
            8 => {
                if bytes.len() < 16 {
                    return Err(bad("command torn"));
                }
                Command::Refill {
                    node: bytes.get_u64_le(),
                    run: bytes.get_u64_le(),
                }
            }
            9 => {
                let key = get_bytes(&mut bytes)?;
                if bytes.len() < 8 {
                    return Err(bad("command torn"));
                }
                Command::Split {
                    key,
                    right: bytes.get_u64_le(),
                }
            }
            _ => return Err(bad("command malformed")),
        };
        if !bytes.is_empty() {
            return Err(bad("command has trailing bytes"));
        }
        Ok(command)
    }
}

/// Applies `command` as entry `index`: reads what a compare-and-set needs, then
/// writes the command's effect and the applied index in one synced batch through
/// the store. A no-op entry applies as an empty batch, so the applied index still
/// advances durably.
///
/// # Errors
///
/// The engine's.
pub async fn apply_command<E: Environment>(
    store: &RaftStore<E>,
    index: Index,
    command: Option<&Command>,
) -> io::Result<Outcome> {
    let mut batch = WriteBatch::new();
    let outcome = match command {
        None => Outcome::Done,
        Some(Command::Put { key, value }) => {
            batch.put(user_key(key), value.clone());
            Outcome::Done
        }
        Some(Command::Delete { key }) => {
            batch.delete(user_key(key));
            Outcome::Done
        }
        Some(Command::Cas { key, expect, value }) => {
            let held = store.engine().get(&user_key(key)).await?;
            if held == *expect {
                batch.put(user_key(key), value.clone());
                Outcome::Swapped(true)
            } else {
                Outcome::Swapped(false)
            }
        }
        Some(Command::Get { key }) => Outcome::Value(store.engine().get(&user_key(key)).await?),
        // A meta update is the range layer's, applied by the node's applier before
        // this is called; a lookup is a read and never an entry (PROPOSED D-098).
        Some(Command::Transfer { .. })
        | Some(Command::Change { .. })
        | Some(Command::MetaUpdate { .. })
        | Some(Command::Lookup { .. })
        | Some(Command::Refill { .. })
        | Some(Command::Split { .. }) => Outcome::Done,
    };
    store.apply(index, batch).await?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_round_trip_and_torn_ones_are_refused() {
        let commands = [
            Command::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
            },
            Command::Delete {
                key: Bytes::from_static(b""),
            },
            Command::Cas {
                key: Bytes::from_static(b"k"),
                expect: None,
                value: Bytes::from_static(b"1"),
            },
            Command::Cas {
                key: Bytes::from_static(b"k"),
                expect: Some(Bytes::from_static(b"1")),
                value: Bytes::from_static(b"2"),
            },
            Command::Get {
                key: Bytes::from_static(b"k"),
            },
            Command::Transfer { to: 3 },
            Command::Change {
                voters: vec![1, 2, 3, 4, 5],
            },
            Command::MetaUpdate {
                descriptors: vec![Bytes::from_static(b"d1"), Bytes::from_static(b"")],
            },
            Command::MetaUpdate {
                descriptors: Vec::new(),
            },
            Command::Lookup {
                key: Bytes::from_static(b"k"),
            },
            Command::Refill {
                node: 3,
                run: 0x1122_3344_5566_7788,
            },
            Command::Split {
                key: Bytes::from_static(b"m"),
                right: 7,
            },
            Command::Split {
                key: Bytes::new(),
                right: 0,
            },
        ];
        for command in commands {
            let bytes = command.encode();
            assert_eq!(Command::decode(bytes.clone()).unwrap(), command);
            for cut in 0..bytes.len() {
                assert!(
                    Command::decode(bytes.slice(..cut)).is_err(),
                    "{command:?} cut at {cut}"
                );
            }
        }
    }
}
