//! CRC-32C (Castagnoli), the checksum SPEC.md §2.2 names for log records. Table
//! driven, reflected, polynomial `0x82F63B78`, as in iSCSI, ext4 and every other
//! CRC-32C implementation, so a record written here verifies anywhere. No dependency:
//! the table is built at compile time and the whole thing is checked against the
//! standard check value below (D-018).

/// The reflected polynomial.
const POLY: u32 = 0x82F6_3B78;

/// One table entry per byte value.
const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// An incremental CRC-32C: feed it pieces, then [`finish`](Hasher::finish).
#[derive(Clone, Copy, Debug)]
pub struct Hasher {
    state: u32,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    /// Nothing fed yet.
    #[must_use]
    pub fn new() -> Self {
        Self { state: !0 }
    }

    /// Feeds `bytes`.
    pub fn update(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            let index = (self.state ^ u32::from(byte)) & 0xff;
            self.state = TABLE[index as usize] ^ (self.state >> 8);
        }
    }

    /// The checksum of everything fed so far.
    #[must_use]
    pub fn finish(self) -> u32 {
        !self.state
    }
}

/// The CRC-32C of `bytes` in one call.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The check value every CRC catalogue lists for CRC-32C.
    #[test]
    fn matches_the_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        // 32 zero bytes, another published vector.
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    }

    #[test]
    fn incremental_equals_one_shot() {
        let data: Vec<u8> = (0..=255).collect();
        let mut hasher = Hasher::new();
        for chunk in data.chunks(7) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finish(), crc32c(&data));
    }

    #[test]
    fn every_single_bit_flip_changes_it() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let reference = crc32c(data);
        for byte in 0..data.len() {
            for bit in 0..8 {
                let mut flipped = data.to_vec();
                flipped[byte] ^= 1 << bit;
                assert_ne!(crc32c(&flipped), reference, "byte {byte} bit {bit}");
            }
        }
    }
}
