//! CRC32C (Castagnoli) checksum — hardware-accelerated via SSE4.2.
//!
//! Uses `_mm_crc32_u64` (8 bytes/cycle) on x86_64 when SSE4.2 is
//! available, falling back to a table-based software implementation
//! on other architectures or older CPUs.
//!
//! CRC32C is the polynomial used by iSCSI, ext4, and SCTP — chosen
//! here because the hardware instruction is universally available
//! on modern x86_64 (since ~2008) and processes 8 bytes per cycle.

/// Computes CRC32C of `data` using the Castagnoli polynomial.
///
/// Returns the checksum in the standard iSCSI convention:
/// init `0xFFFFFFFF`, final XOR `0xFFFFFFFF`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("sse4.2") {
            // SAFETY: SSE4.2 feature is verified at runtime.
            return unsafe { crc32c_hw(data) };
        }
    }
    crc32c_sw(data)
}

// ---------------------------------------------------------------------------
// Hardware path (SSE4.2)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_hw(data: &[u8]) -> u32 {
    use std::arch::x86_64::*;

    let mut crc: u64 = 0xFFFF_FFFF;

    // Process 8-byte chunks.
    let chunks = data.chunks_exact(8);
    let remainder = chunks.remainder();
    for chunk in chunks {
        // SAFETY: chunk is exactly 8 bytes; read_unaligned handles
        // any alignment.
        let v = std::ptr::read_unaligned(chunk.as_ptr() as *const u64);
        crc = _mm_crc32_u64(crc, v);
    }

    // Handle the remaining bytes: 4 → 2 → 1 for best coverage.
    let mut crc32 = crc as u32;
    let mut rem = remainder;
    if rem.len() >= 4 {
        let v = std::ptr::read_unaligned(rem.as_ptr() as *const u32);
        crc32 = _mm_crc32_u32(crc32, v);
        rem = &rem[4..];
    }
    if rem.len() >= 2 {
        let v = std::ptr::read_unaligned(rem.as_ptr() as *const u16);
        crc32 = _mm_crc32_u16(crc32, v);
        rem = &rem[2..];
    }
    if !rem.is_empty() {
        crc32 = _mm_crc32_u8(crc32, rem[0]);
    }

    !crc32
}

// ---------------------------------------------------------------------------
// Software fallback (table-based, 1 byte/iteration)
// ---------------------------------------------------------------------------

/// CRC32C reflected polynomial (Castagnoli).
const CRC32C_POLY: u32 = 0x82F6_3B78;

/// Lookup table generated at compile time from the Castagnoli polynomial.
static TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC32C_POLY
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

#[inline]
fn crc32c_sw(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = TABLE[idx] ^ (crc >> 8);
    }
    !crc
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Known CRC32C test vectors from RFC 3720 (iSCSI) Appendix B.
    // CRC32C of "123456789" (ASCII) = 0xE3069283.
    #[test]
    fn known_vector_ascii_digits() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn empty_input() {
        assert_eq!(crc32c(b""), 0x0000_0000);
    }

    #[test]
    fn single_byte() {
        assert_eq!(crc32c(b"\x00"), 0x527D_5351);
    }

    #[test]
    fn hardware_and_software_agree() {
        for size in [1, 7, 8, 15, 16, 31, 64, 100, 255, 256, 1024] {
            let data: Vec<u8> = (0..size).map(|i| i as u8).collect();
            let hw = crc32c(&data);
            let sw = crc32c_sw(&data);
            assert_eq!(hw, sw, "size {size}: hw=0x{hw:08X} sw=0x{sw:08X}");
        }
    }

    #[test]
    fn detects_single_bit_flip() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let good = crc32c(data);
        let mut corrupted = data.to_vec();
        corrupted[10] ^= 1;
        let bad = crc32c(&corrupted);
        assert_ne!(good, bad);
    }
}
