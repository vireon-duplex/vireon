//! CRC32C-based hasher for connection-ID routing tables.
//!
//! FxHash is fast but has poor distribution for certain key patterns.
//! CRC32C (via SSE4.2 `_mm_crc32_u64`, 8 bytes/instruction) provides
//! excellent distribution at the same latency, reducing collision rates
//! in the DCID → connection-index routing table.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use frame::crc32c;
use profiler::profile;

/// Hasher backed by hardware-accelerated CRC32C.
///
/// For a `[u8; N]` key the `Hash` impl calls `write()` once with the
/// full byte sequence, so this is a single CRC32C pass.
#[derive(Default)]
pub struct Crc32cHasher {
    state: u64,
}

impl Hasher for Crc32cHasher {
    #[profile]
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // CRC32C produces a u32; fold into 64-bit with a golden-ratio
        // multiply for good avalanche in the HashMap table indices.
        let crc = crc32c::crc32c(bytes);
        self.state = (self.state.rotate_left(32) ^ crc as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.state
    }
}

/// [`BuildHasher`] that creates [`Crc32cHasher`] instances.
pub type Crc32cBuildHasher = BuildHasherDefault<Crc32cHasher>;

/// [`HashMap`] keyed by CRC32C — use for byte-array keys (e.g. connection IDs).
pub type CrcHashMap<K, V> = HashMap<K, V, Crc32cBuildHasher>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn different_keys_produce_different_hashes() {
        let mut h1 = Crc32cHasher::default();
        h1.write(b"connection-id-aaa");
        let hash1 = h1.finish();

        let mut h2 = Crc32cHasher::default();
        h2.write(b"connection-id-bbb");
        let hash2 = h2.finish();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn same_key_produces_same_hash() {
        let mut h1 = Crc32cHasher::default();
        h1.write(b"some-connection-id");
        let hash1 = h1.finish();

        let mut h2 = Crc32cHasher::default();
        h2.write(b"some-connection-id");
        let hash2 = h2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn hashmap_lookups_work() {
        let mut map: CrcHashMap<[u8; 4], usize> = CrcHashMap::default();
        map.insert([1, 2, 3, 4], 42);
        map.insert([5, 6, 7, 8], 99);

        assert_eq!(map.get(&[1, 2, 3, 4]), Some(&42));
        assert_eq!(map.get(&[5, 6, 7, 8]), Some(&99));
        assert_eq!(map.get(&[9, 10, 11, 12]), None);
    }
}
