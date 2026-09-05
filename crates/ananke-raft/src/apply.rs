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

/// The tenant user data lives under.
pub const USER_TENANT: u64 = 1;
const USER_TABLE: u64 = 0;

/// A key of the user's key-value store, as the engine sees it.
#[must_use]
pub fn user_key(user: &[u8]) -> Bytes {
    key(USER_TENANT, USER_TABLE, user)
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
    /// Read `key` at this entry's place in the log.
    Get {
        /// The key.
        key: Bytes,
    },
}

impl Command {
    /// The key the command touches.
    #[must_use]
    pub fn key(&self) -> &Bytes {
        match self {
            Command::Put { key, .. }
            | Command::Delete { key }
            | Command::Cas { key, .. }
            | Command::Get { key } => key,
        }
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
