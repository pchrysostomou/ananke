//! The manifest (SPEC.md §2.1, D-022): which SSTables make up the engine's state and
//! how much of the log they cover, in a numbered file `MANIFEST-<n>` that `CURRENT`
//! names. A flush writes the next manifest whole, syncs it, then points `CURRENT` at
//! it by writing `CURRENT.tmp` and renaming, and syncs the directory: the switch is
//! atomic, and a crash before it leaves the old manifest in force and the new one an
//! orphan. Manifests are never modified in place and older ones are kept, so recovery
//! can fall back when the one `CURRENT` names cannot be read.
//!
//! ```text
//! magic: u64 | format_version: u32 | number: u64 | next_sst: u64 | flushed_seq: u64 |
//! count: u32 | per table: number: u64 | first_seq: u64 | max_seq: u64 | entries: u64 |
//! crc32c: u32 over everything before it
//! ```
//!
//! `CURRENT` holds one line, `MANIFEST-<n> <crc32c of the name in hex>`.

use std::io;
use std::path::{Path, PathBuf};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Seq;
use crate::crc32c;

/// The on-disk format version.
pub const FORMAT_VERSION: u32 = 1;
const MAGIC: u64 = u64::from_le_bytes(*b"ANANKMAN");

/// One table the manifest lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SstMeta {
    /// The table's number, which names its file.
    pub number: u64,
    /// The lowest log sequence number of the writes it holds.
    pub first_seq: Seq,
    /// The highest.
    pub max_seq: Seq,
    /// Keys it holds.
    pub entries: u64,
}

/// The engine's durable state description.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    /// This manifest's number; 0 is the empty state no file holds.
    pub number: u64,
    /// The number the next table gets.
    pub next_sst: u64,
    /// Every log record numbered this or below is in a listed table.
    pub flushed_seq: Seq,
    /// The tables, oldest first.
    pub ssts: Vec<SstMeta>,
}

impl Manifest {
    /// The empty state: no tables, nothing flushed, the first table will be 1.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            number: 0,
            next_sst: 1,
            flushed_seq: 0,
            ssts: Vec::new(),
        }
    }

    /// The file's bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(48 + 32 * self.ssts.len());
        out.put_u64_le(MAGIC);
        out.put_u32_le(FORMAT_VERSION);
        out.put_u64_le(self.number);
        out.put_u64_le(self.next_sst);
        out.put_u64_le(self.flushed_seq);
        out.put_u32_le(u32::try_from(self.ssts.len()).expect("table count fits u32"));
        for sst in &self.ssts {
            out.put_u64_le(sst.number);
            out.put_u64_le(sst.first_seq);
            out.put_u64_le(sst.max_seq);
            out.put_u64_le(sst.entries);
        }
        let crc = crc32c::crc32c(&out);
        out.put_u32_le(crc);
        out.freeze()
    }

    /// Parses a file's bytes.
    ///
    /// # Errors
    ///
    /// `InvalidData` for anything that is not a complete manifest with a matching crc.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_owned());
        if bytes.len() < 44 {
            return Err(bad("manifest too short"));
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if u32::from_le_bytes(crc.try_into().expect("4 bytes")) != crc32c::crc32c(body) {
            return Err(bad("manifest crc mismatch"));
        }
        let mut body = body;
        if body.get_u64_le() != MAGIC {
            return Err(bad("not a manifest"));
        }
        if body.get_u32_le() != FORMAT_VERSION {
            return Err(bad("unsupported manifest format version"));
        }
        let number = body.get_u64_le();
        let next_sst = body.get_u64_le();
        let flushed_seq = body.get_u64_le();
        let count = body.get_u32_le() as usize;
        if body.len() != count * 32 {
            return Err(bad("manifest table list malformed"));
        }
        let mut ssts = Vec::with_capacity(count);
        for _ in 0..count {
            ssts.push(SstMeta {
                number: body.get_u64_le(),
                first_seq: body.get_u64_le(),
                max_seq: body.get_u64_le(),
                entries: body.get_u64_le(),
            });
        }
        Ok(Self {
            number,
            next_sst,
            flushed_seq,
            ssts,
        })
    }
}

/// The name of manifest `n`.
#[must_use]
pub fn manifest_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("MANIFEST-{number:06}"))
}

/// The manifest number a path names, if it names one.
#[must_use]
pub fn manifest_of(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_prefix("MANIFEST-")?
        .parse()
        .ok()
}

/// The name of table `n`.
#[must_use]
pub fn sst_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{number:06}.sst"))
}

/// The table number a path names, if it names one.
#[must_use]
pub fn sst_of(path: &Path) -> Option<u64> {
    path.file_name()?
        .to_str()?
        .strip_suffix(".sst")?
        .parse()
        .ok()
}

/// The `CURRENT` file, whose one line names the manifest in force.
#[must_use]
pub fn current_path(dir: &Path) -> PathBuf {
    dir.join("CURRENT")
}

/// Where `CURRENT` is written before being renamed into place.
#[must_use]
pub fn current_tmp_path(dir: &Path) -> PathBuf {
    dir.join("CURRENT.tmp")
}

/// The contents of `CURRENT` naming manifest `number`: the name, a space, the crc32c
/// of the name as eight hex digits, a newline. One flipped bit turns `000007` into
/// `000003`, a manifest that exists; the checksum is what stops recovery believing it.
#[must_use]
pub fn encode_current(number: u64) -> Bytes {
    let name = format!("MANIFEST-{number:06}");
    Bytes::from(format!("{name} {:08x}\n", crc32c::crc32c(name.as_bytes())))
}

/// The manifest number `CURRENT`'s contents name: exactly what [`encode_current`]
/// writes, with `n` at least 1 and the checksum matching. Anything else, a torn tail
/// or a flipped bit included, names nothing.
#[must_use]
pub fn parse_current(bytes: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(bytes).ok()?;
    let line = text.strip_suffix('\n')?;
    let (name, crc) = line.split_once(' ')?;
    let digits = name.strip_prefix("MANIFEST-")?;
    if digits.len() != 6 || !digits.bytes().all(|b| b.is_ascii_digit()) || crc.len() != 8 {
        return None;
    }
    if u32::from_str_radix(crc, 16).ok()? != crc32c::crc32c(name.as_bytes()) {
        return None;
    }
    let number: u64 = digits.parse().ok()?;
    (number >= 1).then_some(number)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manifest_round_trips_and_a_flipped_bit_is_caught() {
        let manifest = Manifest {
            number: 7,
            next_sst: 12,
            flushed_seq: 4096,
            ssts: vec![
                SstMeta {
                    number: 3,
                    first_seq: 1,
                    max_seq: 900,
                    entries: 40,
                },
                SstMeta {
                    number: 11,
                    first_seq: 901,
                    max_seq: 4096,
                    entries: 48,
                },
            ],
        };
        let bytes = manifest.encode();
        assert_eq!(Manifest::decode(&bytes).unwrap(), manifest);
        assert_eq!(
            Manifest::decode(&Manifest::empty().encode()).unwrap(),
            Manifest::empty()
        );
        for i in 0..bytes.len() {
            let mut flipped = bytes.to_vec();
            flipped[i] ^= 1;
            assert!(Manifest::decode(&flipped).is_err(), "byte {i}");
        }
        assert!(Manifest::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn names_parse() {
        let dir = Path::new("/db");
        assert_eq!(manifest_of(&manifest_path(dir, 42)), Some(42));
        assert_eq!(sst_of(&sst_path(dir, 42)), Some(42));
        assert_eq!(manifest_of(Path::new("/db/000042.sst")), None);
        assert_eq!(sst_of(Path::new("/db/MANIFEST-000042")), None);
        assert_eq!(parse_current(&encode_current(9)), Some(9));
        assert_eq!(parse_current(b"garbage"), None);
        // A torn tail names nothing, 0 is not a manifest, and a flipped bit anywhere
        // fails the checksum rather than naming another manifest.
        let current = encode_current(7);
        assert_eq!(parse_current(&current[..current.len() - 1]), None);
        assert_eq!(parse_current(&current[..12]), None);
        assert_eq!(parse_current(&encode_current(0)), None);
        assert_eq!(parse_current(b""), None);
        for i in 0..current.len() - 1 {
            let mut flipped = current.to_vec();
            flipped[i] ^= 4;
            assert_eq!(parse_current(&flipped), None, "byte {i}");
        }
    }
}
