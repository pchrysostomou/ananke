//! SSTables (SPEC.md §2.4, D-022): a sorted, immutable table of the newest write per
//! key, written once by a flush and read by point lookups.
//!
//! The file is data blocks, a bloom block, an index block and a footer:
//!
//! ```text
//! data block   entries: shared: u16 | unshared: u16 | value_len: u32 | key suffix | value
//!              then crc32c: u32 over the entries. Keys are prefix-compressed against
//!              the previous key in the block; a block's first key is stored whole.
//!              A value_len of u32::MAX is a tombstone with no value bytes.
//! bloom block  bits: u32 | k: u8 | bit bytes, then crc32c. Ten bits per key, seven
//!              hashes from one FNV-1a hash and a mix of it.
//! index block  count: u32, then per data block: key_len: u16 | first key |
//!              offset: u64 | len: u32 (the block with its crc), then crc32c.
//! footer       index_offset: u64 | index_len: u32 | bloom_offset: u64 | bloom_len: u32 |
//!              entries: u64 | format_version: u32 | magic: u64 | crc32c: u32
//!              (48 bytes; the crc covers the 44 before it)
//! ```
//!
//! A data block is sealed when the next entry would carry it past [`BLOCK_BYTES`]. A
//! lookup tests the bloom filter, binary-searches the index for the last block whose
//! first key is not greater than the key, reads that block, checks its crc and walks
//! it. [`SstReader::verify`] reads and checks every block; recovery runs it on every
//! table the manifest lists, so bit rot is found at open rather than at the read
//! that happens to hit it.

use std::io;

use ananke_env::File;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::crc32c;
use crate::memtable::Value;

/// The size a data block is sealed at.
pub const BLOCK_BYTES: usize = 4096;
/// The on-disk format version in the footer.
pub const FORMAT_VERSION: u32 = 1;
/// The footer's size.
pub const FOOTER_LEN: usize = 48;
/// Bits per key in the bloom filter.
pub const BLOOM_BITS_PER_KEY: u64 = 10;
/// Hash functions in the bloom filter.
pub const BLOOM_HASHES: u8 = 7;

const MAGIC: u64 = u64::from_le_bytes(*b"ANANKSST");
const TOMBSTONE: u32 = u32::MAX;

fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_owned())
}

/// FNV-1a over `bytes`, the bloom filter's hash.
#[must_use]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The two hashes every probe is derived from.
fn bloom_hashes(key: &[u8]) -> (u64, u64) {
    let h1 = fnv1a64(key);
    let h2 = (h1 ^ (h1 >> 29)).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (h1, h2)
}

/// A bloom filter: no false negatives, about one false positive in a hundred.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bloom {
    bits: Vec<u8>,
    num_bits: u32,
    k: u8,
}

impl Bloom {
    fn with_keys(hashes: &[(u64, u64)]) -> Self {
        let num_bits = (hashes.len() as u64 * BLOOM_BITS_PER_KEY)
            .max(64)
            .div_ceil(8)
            * 8;
        let num_bits = u32::try_from(num_bits).expect("bloom filter fits u32 bits");
        let mut bloom = Self {
            bits: vec![0; num_bits as usize / 8],
            num_bits,
            k: BLOOM_HASHES,
        };
        for &(h1, h2) in hashes {
            for i in 0..u64::from(bloom.k) {
                let bit = h1.wrapping_add(i.wrapping_mul(h2)) % u64::from(num_bits);
                bloom.bits[(bit / 8) as usize] |= 1 << (bit % 8);
            }
        }
        bloom
    }

    /// Whether `key` may be in the table; `false` means it certainly is not.
    #[must_use]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = bloom_hashes(key);
        (0..u64::from(self.k)).all(|i| {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % u64::from(self.num_bits);
            self.bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0
        })
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.put_u32_le(self.num_bits);
        out.put_u8(self.k);
        out.put_slice(&self.bits);
    }

    fn decode(mut bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < 5 {
            return Err(invalid("bloom block too short"));
        }
        let num_bits = bytes.get_u32_le();
        let k = bytes.get_u8();
        if num_bits == 0
            || !num_bits.is_multiple_of(8)
            || k == 0
            || bytes.len() != num_bits as usize / 8
        {
            return Err(invalid("bloom block malformed"));
        }
        Ok(Self {
            bits: bytes.to_vec(),
            num_bits,
            k,
        })
    }
}

/// Appends `block` to `out` followed by its crc; returns (offset, len including crc).
fn seal(out: &mut Vec<u8>, block: &[u8]) -> (u64, u32) {
    let offset = out.len() as u64;
    out.extend_from_slice(block);
    out.put_u32_le(crc32c::crc32c(block));
    (offset, (block.len() + 4) as u32)
}

/// Checks a sealed block's crc and returns its contents.
fn unseal(block: &[u8]) -> io::Result<&[u8]> {
    if block.len() < 4 {
        return Err(invalid("block too short for its crc"));
    }
    let (body, crc) = block.split_at(block.len() - 4);
    if u32::from_le_bytes(crc.try_into().expect("4 bytes")) != crc32c::crc32c(body) {
        return Err(invalid("block crc mismatch"));
    }
    Ok(body)
}

/// One data block's location: its first key, offset and sealed length.
#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexEntry {
    first_key: Bytes,
    offset: u64,
    len: u32,
}

/// Builds a table in memory from keys added in strictly increasing order.
#[derive(Debug, Default)]
pub struct SstWriter {
    file: Vec<u8>,
    block: Vec<u8>,
    block_first_key: Option<Bytes>,
    last_key: Option<Bytes>,
    index: Vec<IndexEntry>,
    hashes: Vec<(u64, u64)>,
    entries: u64,
}

impl SstWriter {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `key` with `value`. Keys must arrive in strictly increasing order.
    ///
    /// # Panics
    ///
    /// If `key` is not greater than the previous one, or longer than `u16::MAX`.
    pub fn add(&mut self, key: &[u8], value: &Value) {
        assert!(
            self.last_key.as_deref().is_none_or(|last| key > last),
            "SSTable keys must be added in strictly increasing order"
        );
        let key_len = u16::try_from(key.len()).expect("SSTable key exceeds u16");
        let shared = match (&self.block_first_key, &self.last_key) {
            (Some(_), Some(last)) => last.iter().zip(key).take_while(|(a, b)| a == b).count(),
            _ => 0,
        };
        let (value_len, value_bytes): (u32, &[u8]) = match value {
            Value::Live(bytes) => (
                u32::try_from(bytes.len()).expect("SSTable value exceeds u32"),
                bytes,
            ),
            Value::Tombstone => (TOMBSTONE, &[]),
        };
        let entry_len = 8 + (key.len() - shared) + value_bytes.len();
        if !self.block.is_empty() && self.block.len() + entry_len + 4 > BLOCK_BYTES {
            self.seal_block();
            // A block's first key is stored whole.
            return self.add(key, value);
        }
        if self.block.is_empty() {
            self.block_first_key = Some(Bytes::copy_from_slice(key));
        }
        self.block.put_u16_le(shared as u16);
        self.block.put_u16_le(key_len - shared as u16);
        self.block.put_u32_le(value_len);
        self.block.put_slice(&key[shared..]);
        self.block.put_slice(value_bytes);
        self.last_key = Some(Bytes::copy_from_slice(key));
        self.hashes.push(bloom_hashes(key));
        self.entries += 1;
    }

    fn seal_block(&mut self) {
        let (offset, len) = seal(&mut self.file, &self.block);
        self.index.push(IndexEntry {
            first_key: self
                .block_first_key
                .take()
                .expect("a sealed block has a first key"),
            offset,
            len,
        });
        self.block.clear();
    }

    /// Keys added so far.
    #[must_use]
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// The table's bytes, ready to be written whole.
    #[must_use]
    pub fn finish(mut self) -> Bytes {
        if !self.block.is_empty() {
            self.seal_block();
        }
        let bloom = Bloom::with_keys(&self.hashes);
        let mut block = Vec::new();
        bloom.encode(&mut block);
        let (bloom_offset, bloom_len) = seal(&mut self.file, &block);
        block.clear();
        block.put_u32_le(self.index.len() as u32);
        for entry in &self.index {
            block.put_u16_le(entry.first_key.len() as u16);
            block.put_slice(&entry.first_key);
            block.put_u64_le(entry.offset);
            block.put_u32_le(entry.len);
        }
        let (index_offset, index_len) = seal(&mut self.file, &block);
        let mut footer = BytesMut::with_capacity(FOOTER_LEN);
        footer.put_u64_le(index_offset);
        footer.put_u32_le(index_len);
        footer.put_u64_le(bloom_offset);
        footer.put_u32_le(bloom_len);
        footer.put_u64_le(self.entries);
        footer.put_u32_le(FORMAT_VERSION);
        footer.put_u64_le(MAGIC);
        let crc = crc32c::crc32c(&footer);
        footer.put_u32_le(crc);
        self.file.extend_from_slice(&footer);
        Bytes::from(self.file)
    }
}

/// An open table: the index and bloom filter in memory, data blocks read on demand.
pub struct SstReader<F: File> {
    file: F,
    index: Vec<IndexEntry>,
    bloom: Bloom,
    entries: u64,
}

impl<F: File> std::fmt::Debug for SstReader<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstReader")
            .field("blocks", &self.index.len())
            .field("entries", &self.entries)
            .finish_non_exhaustive()
    }
}

impl<F: File> SstReader<F> {
    /// Reads the footer, index and bloom filter of `file`.
    ///
    /// # Errors
    ///
    /// `InvalidData` for a file that is not a table: too short, wrong magic or version,
    /// a crc mismatch in the footer, index or bloom block, or offsets out of range.
    pub async fn open(file: F) -> io::Result<Self> {
        let size = file.size().await?;
        if size < FOOTER_LEN as u64 {
            return Err(invalid("file too short for an SSTable footer"));
        }
        let footer = file.read_at(size - FOOTER_LEN as u64, FOOTER_LEN).await?;
        if footer.len() != FOOTER_LEN {
            return Err(invalid("footer torn"));
        }
        let (body, crc) = footer.split_at(FOOTER_LEN - 4);
        if u32::from_le_bytes(crc.try_into().expect("4 bytes")) != crc32c::crc32c(body) {
            return Err(invalid("footer crc mismatch"));
        }
        let mut body = body;
        let index_offset = body.get_u64_le();
        let index_len = body.get_u32_le();
        let bloom_offset = body.get_u64_le();
        let bloom_len = body.get_u32_le();
        let entries = body.get_u64_le();
        let version = body.get_u32_le();
        let magic = body.get_u64_le();
        if magic != MAGIC {
            return Err(invalid("not an SSTable"));
        }
        if version != FORMAT_VERSION {
            return Err(invalid("unsupported SSTable format version"));
        }
        let data_end = size - FOOTER_LEN as u64;
        if bloom_offset + u64::from(bloom_len) > data_end
            || index_offset + u64::from(index_len) > data_end
        {
            return Err(invalid("footer offsets out of range"));
        }
        let bloom = Self::read_sealed(&file, bloom_offset, bloom_len).await?;
        let bloom = Bloom::decode(&bloom)?;
        let index = Self::read_sealed(&file, index_offset, index_len).await?;
        let index = Self::decode_index(&index, bloom_offset)?;
        Ok(Self {
            file,
            index,
            bloom,
            entries,
        })
    }

    async fn read_sealed(file: &F, offset: u64, len: u32) -> io::Result<Bytes> {
        let block = file.read_at(offset, len as usize).await?;
        if block.len() != len as usize {
            return Err(invalid("block torn"));
        }
        let body = unseal(&block)?;
        Ok(block.slice(..body.len()))
    }

    fn decode_index(mut bytes: &[u8], data_end: u64) -> io::Result<Vec<IndexEntry>> {
        if bytes.len() < 4 {
            return Err(invalid("index block too short"));
        }
        let count = bytes.get_u32_le() as usize;
        let mut index = Vec::with_capacity(count);
        let mut previous_end = 0;
        for _ in 0..count {
            if bytes.len() < 2 {
                return Err(invalid("index entry torn"));
            }
            let key_len = bytes.get_u16_le() as usize;
            if bytes.len() < key_len + 12 {
                return Err(invalid("index entry torn"));
            }
            let first_key = Bytes::copy_from_slice(&bytes[..key_len]);
            bytes.advance(key_len);
            let offset = bytes.get_u64_le();
            let len = bytes.get_u32_le();
            if offset != previous_end || offset + u64::from(len) > data_end {
                return Err(invalid("index entry out of range"));
            }
            previous_end = offset + u64::from(len);
            index.push(IndexEntry {
                first_key,
                offset,
                len,
            });
        }
        if !bytes.is_empty() {
            return Err(invalid("index block has trailing bytes"));
        }
        Ok(index)
    }

    /// Keys in the table.
    #[must_use]
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// Data blocks in the table.
    #[must_use]
    pub fn blocks(&self) -> usize {
        self.index.len()
    }

    /// The bloom filter.
    #[must_use]
    pub fn bloom(&self) -> &Bloom {
        &self.bloom
    }

    /// Reads and checks every data block.
    ///
    /// # Errors
    ///
    /// `InvalidData` at the first block that is torn, fails its crc, or does not
    /// decode to keys in increasing order.
    pub async fn verify(&self) -> io::Result<()> {
        let mut previous: Option<Vec<u8>> = None;
        let mut count = 0;
        for entry in &self.index {
            let block = Self::read_sealed(&self.file, entry.offset, entry.len).await?;
            let mut key = Vec::new();
            let mut rest = &block[..];
            let mut block_first: Option<Vec<u8>> = None;
            while !rest.is_empty() {
                let (shared, suffix, _) = decode_entry(&mut rest)?;
                if block_first.is_none() && shared != 0 {
                    return Err(invalid("a block's first key shares a prefix"));
                }
                if shared > key.len() {
                    return Err(invalid("shared prefix longer than the previous key"));
                }
                key.truncate(shared);
                key.extend_from_slice(suffix);
                if previous
                    .as_ref()
                    .is_some_and(|p| p.as_slice() >= key.as_slice())
                {
                    return Err(invalid("keys out of order"));
                }
                if block_first.is_none() {
                    block_first = Some(key.clone());
                }
                previous = Some(key.clone());
                count += 1;
            }
            if block_first.as_deref() != Some(entry.first_key.as_ref()) {
                return Err(invalid("index first key does not match its block"));
            }
        }
        if count != self.entries {
            return Err(invalid("entry count does not match the footer"));
        }
        Ok(())
    }

    /// The newest write to `key` in this table, if the table has one.
    ///
    /// # Errors
    ///
    /// `InvalidData` if the block the key would be in is torn or fails its crc.
    pub async fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        if !self.bloom.might_contain(key) {
            return Ok(None);
        }
        let candidate = self.index.partition_point(|e| e.first_key.as_ref() <= key);
        if candidate == 0 {
            return Ok(None);
        }
        let entry = &self.index[candidate - 1];
        let block = Self::read_sealed(&self.file, entry.offset, entry.len).await?;
        let mut current = Vec::new();
        let mut rest = &block[..];
        while !rest.is_empty() {
            let (shared, suffix, value) = decode_entry(&mut rest)?;
            if shared > current.len() {
                return Err(invalid("shared prefix longer than the previous key"));
            }
            current.truncate(shared);
            current.extend_from_slice(suffix);
            match current.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return Ok(Some(value)),
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }
}

/// Decodes one entry, advancing `rest`: (shared, key suffix, value).
fn decode_entry<'a>(rest: &mut &'a [u8]) -> io::Result<(usize, &'a [u8], Value)> {
    if rest.len() < 8 {
        return Err(invalid("entry header torn"));
    }
    let shared = rest.get_u16_le() as usize;
    let unshared = rest.get_u16_le() as usize;
    let value_len = rest.get_u32_le();
    if rest.len() < unshared {
        return Err(invalid("entry key torn"));
    }
    let (suffix, after) = rest.split_at(unshared);
    *rest = after;
    let value = if value_len == TOMBSTONE {
        Value::Tombstone
    } else {
        let value_len = value_len as usize;
        if rest.len() < value_len {
            return Err(invalid("entry value torn"));
        }
        let (value, after) = rest.split_at(value_len);
        *rest = after;
        Value::Live(Bytes::copy_from_slice(value))
    };
    Ok((shared, suffix, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bloom_filter_has_no_false_negatives_and_few_false_positives() {
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("key-{i:05}").into_bytes())
            .collect();
        let hashes: Vec<(u64, u64)> = keys.iter().map(|k| bloom_hashes(k)).collect();
        let bloom = Bloom::with_keys(&hashes);
        assert!(keys.iter().all(|k| bloom.might_contain(k)));
        let false_positives = (0..10_000u32)
            .filter(|i| bloom.might_contain(format!("absent-{i}").as_bytes()))
            .count();
        assert!(
            false_positives < 300,
            "{false_positives} false positives in 10 000"
        );
        let mut encoded = Vec::new();
        bloom.encode(&mut encoded);
        assert_eq!(Bloom::decode(&encoded).unwrap(), bloom);
    }

    #[test]
    fn the_writer_seals_blocks_near_the_target_size() {
        let mut writer = SstWriter::new();
        for i in 0..2000u32 {
            writer.add(
                format!("k{i:06}").as_bytes(),
                &Value::Live(Bytes::from(vec![b'v'; 50])),
            );
        }
        let bytes = writer.finish();
        assert!(bytes.len() > 2000 * 50, "values are all there");
        // 2000 entries of ~60 bytes each is about 30 blocks of 4 KiB.
        assert!(
            bytes.len() < 2000 * 70,
            "prefix compression keeps the keys short"
        );
    }

    #[test]
    #[should_panic(expected = "strictly increasing")]
    fn keys_must_increase() {
        let mut writer = SstWriter::new();
        writer.add(b"b", &Value::Tombstone);
        writer.add(b"a", &Value::Tombstone);
    }
}
