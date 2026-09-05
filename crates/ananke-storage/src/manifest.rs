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
//! count: u32 | per table: number: u64 | level: u8 | first_seq: u64 | max_seq: u64 |
//! entries: u64 | bytes: u64 | first_key_len: u16 | first_key | last_key_len: u16 |
//! last_key | crc32c: u32 over everything before it
//! ```
//!
//! A table's level and key range are what leveled compaction (D-023) picks and
//! overlaps by: level 0 tables may overlap one another, tables of a deeper level
//! never do.
//!
//! `CURRENT` holds one line, `MANIFEST-<n> <crc32c of the name in hex>`.

use std::io;
use std::path::{Path, PathBuf};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::Seq;
use crate::crc32c;

/// The on-disk format version: 2 since tables carry a level, a key range and a size.
pub const FORMAT_VERSION: u32 = 2;
const MAGIC: u64 = u64::from_le_bytes(*b"ANANKMAN");

/// The deepest level.
pub const BOTTOM_LEVEL: u8 = 6;
/// How many levels there are.
pub const LEVELS: usize = BOTTOM_LEVEL as usize + 1;

/// One table the manifest lists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SstMeta {
    /// The table's number, which names its file.
    pub number: u64,
    /// The level it sits at: 0 for a flushed memtable, deeper for compaction output.
    pub level: u8,
    /// The lowest log sequence number of the writes it holds.
    pub first_seq: Seq,
    /// The highest.
    pub max_seq: Seq,
    /// Writes it holds.
    pub entries: u64,
    /// The file's size.
    pub bytes: u64,
    /// The smallest user key it holds.
    pub first_key: Bytes,
    /// The largest.
    pub last_key: Bytes,
}

impl SstMeta {
    /// Whether the table's key range overlaps `[first, last]`.
    #[must_use]
    pub fn overlaps(&self, first: &[u8], last: &[u8]) -> bool {
        self.first_key[..] <= *last && *first <= self.last_key[..]
    }

    /// Whether the table's key range contains `key`.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.first_key[..] <= *key && *key <= self.last_key[..]
    }
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

    /// The tables at `level`, in key order for a deeper level and by number, oldest
    /// first, at level 0.
    #[must_use]
    pub fn level(&self, level: u8) -> Vec<SstMeta> {
        let mut tables: Vec<SstMeta> = self
            .ssts
            .iter()
            .filter(|t| t.level == level)
            .cloned()
            .collect();
        if level == 0 {
            tables.sort_by_key(|t| t.number);
        } else {
            tables.sort_by(|a, b| a.first_key.cmp(&b.first_key));
        }
        tables
    }

    /// The file's bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(48 + 64 * self.ssts.len());
        out.put_u64_le(MAGIC);
        out.put_u32_le(FORMAT_VERSION);
        out.put_u64_le(self.number);
        out.put_u64_le(self.next_sst);
        out.put_u64_le(self.flushed_seq);
        out.put_u32_le(u32::try_from(self.ssts.len()).expect("table count fits u32"));
        for sst in &self.ssts {
            out.put_u64_le(sst.number);
            out.put_u8(sst.level);
            out.put_u64_le(sst.first_seq);
            out.put_u64_le(sst.max_seq);
            out.put_u64_le(sst.entries);
            out.put_u64_le(sst.bytes);
            out.put_u16_le(u16::try_from(sst.first_key.len()).expect("key fits u16"));
            out.put_slice(&sst.first_key);
            out.put_u16_le(u16::try_from(sst.last_key.len()).expect("key fits u16"));
            out.put_slice(&sst.last_key);
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
        let mut ssts = Vec::with_capacity(count);
        for _ in 0..count {
            if body.len() < 41 {
                return Err(bad("manifest table list malformed"));
            }
            let number = body.get_u64_le();
            let level = body.get_u8();
            let first_seq = body.get_u64_le();
            let max_seq = body.get_u64_le();
            let entries = body.get_u64_le();
            let bytes = body.get_u64_le();
            let key = |body: &mut &[u8]| -> io::Result<Bytes> {
                if body.len() < 2 {
                    return Err(bad("manifest table list malformed"));
                }
                let len = body.get_u16_le() as usize;
                if body.len() < len {
                    return Err(bad("manifest table list malformed"));
                }
                let key = Bytes::copy_from_slice(&body[..len]);
                body.advance(len);
                Ok(key)
            };
            let first_key = key(&mut body)?;
            let last_key = key(&mut body)?;
            if level > BOTTOM_LEVEL {
                return Err(bad("manifest table level out of range"));
            }
            ssts.push(SstMeta {
                number,
                level,
                first_seq,
                max_seq,
                entries,
                bytes,
                first_key,
                last_key,
            });
        }
        if !body.is_empty() {
            return Err(bad("manifest table list malformed"));
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
                    level: 1,
                    first_seq: 1,
                    max_seq: 900,
                    entries: 40,
                    bytes: 2000,
                    first_key: Bytes::from("a"),
                    last_key: Bytes::from("m"),
                },
                SstMeta {
                    number: 11,
                    level: 0,
                    first_seq: 901,
                    max_seq: 4096,
                    entries: 48,
                    bytes: 2500,
                    first_key: Bytes::from(""),
                    last_key: Bytes::from("zz\x00"),
                },
            ],
        };
        assert_eq!(manifest.level(0).len(), 1);
        assert_eq!(manifest.level(1)[0].number, 3);
        assert!(manifest.ssts[0].overlaps(b"m", b"z"));
        assert!(!manifest.ssts[0].overlaps(b"n", b"z"));
        assert!(manifest.ssts[0].contains(b"c") && !manifest.ssts[0].contains(b"n"));
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
