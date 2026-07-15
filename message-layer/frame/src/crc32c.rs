//! CRC32C (Castagnoli) checksum — hardware-accelerated.
//!
//! **Fast path (≥ 512 B):** 3-way parallel CRC using three independent
//! `_mm_crc32_u64` chains. Each chain has 3-cycle latency; with 3 chains
//! the CPU keeps port 0 fully utilized (1 instruction/cycle), achieving
//! **3× throughput** vs a single serial chain. The three partial CRCs
//! are combined at the end using PCLMULQDQ (carryless multiplication).
//!
//! **Small data (< 512 B):** single serial chain — CLMUL overhead exceeds
//! the parallelism benefit.
//!
//! **No SSE4.2 / PCLMULQDQ:** table-based software fallback.

#![allow(unsafe_op_in_unsafe_fn)]

/// Computes CRC32C of `data`.
///
/// Returns the checksum in the standard iSCSI convention:
/// init `0xFFFFFFFF`, final XOR `0xFFFFFFFF`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("sse4.2") {
            if data.len() >= 512 && std::is_x86_feature_detected!("pclmulqdq") {
                // SAFETY: SSE4.2 + PCLMULQDQ verified at runtime.
                return unsafe { crc32c_parallel(data) };
            }
            // SAFETY: SSE4.2 verified at runtime.
            return unsafe { crc32c_serial(data) };
        }
    }
    crc32c_sw(data)
}

// ---------------------------------------------------------------------------
// Hardware path — serial (SSE4.2 only)
// ---------------------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_serial(data: &[u8]) -> u32 {
    use std::arch::x86_64::*;

    let mut crc: u64 = 0xFFFF_FFFF;

    let n = data.len();
    let ptr = data.as_ptr();

    // Process 8-byte chunks via raw pointer loop.
    let main = n & !7;
    let mut i = 0;
    while i < main {
        let v = std::ptr::read_unaligned(ptr.add(i) as *const u64);
        crc = _mm_crc32_u64(crc, v);
        i += 8;
    }

    // Handle remaining bytes: 4 → 2 → 1.
    let mut crc32 = crc as u32;
    if i + 4 <= n {
        let v = std::ptr::read_unaligned(ptr.add(i) as *const u32);
        crc32 = _mm_crc32_u32(crc32, v);
        i += 4;
    }
    if i + 2 <= n {
        let v = std::ptr::read_unaligned(ptr.add(i) as *const u16);
        crc32 = _mm_crc32_u16(crc32, v);
        i += 2;
    }
    if i < n {
        crc32 = _mm_crc32_u8(crc32, *ptr.add(i));
    }

    !crc32
}

// ---------------------------------------------------------------------------
// Hardware path — 3-way parallel (SSE4.2 + PCLMULQDQ)
// ---------------------------------------------------------------------------

/// CRC32C generator polynomial in CLMUL convention (bit i = x^i).
/// Determined empirically from the hardware CRC32C instruction via
/// polynomial GCD of its transfer constants. The CRC instruction's
/// internal representation differs from the standard algebraic form
/// due to the reflected processing convention.
const P: u32 = 0x05EC_76F1;

/// Transfer constant M₁ = T₁(1): the CRC instruction's state after
/// processing 1 zero byte from initial state 1. This is the base for
/// computing M_k = M₁^k mod G via repeated squaring.
const M1: u32 = 0xF26B_8303;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
#[target_feature(enable = "pclmulqdq")]
unsafe fn crc32c_parallel(data: &[u8]) -> u32 {
    use std::arch::x86_64::*;

    let n = data.len();
    let ptr = data.as_ptr();

    // Split into three contiguous chunks, each a multiple of 8 bytes.
    let chunk_qw = n / 24; // qwords per chunk
    let chunk_bytes = chunk_qw * 8;
    let c0_start = 0;
    let c1_start = chunk_bytes;
    let c2_start = 2 * chunk_bytes;
    let c2_qw = chunk_qw;

    // ── Phase 1: 3-way interleaved CRC (port-0 fully pipelined) ──────
    //
    // Chain 0 starts from standard init 0xFFFFFFFF.
    // Chains 1 and 2 start from 0 (raw).
    let mut crc0: u64 = 0xFFFF_FFFF;
    let mut crc1: u64 = 0;
    let mut crc2: u64 = 0;

    for i in 0..chunk_qw {
        let v0 = std::ptr::read_unaligned(ptr.add(c0_start + i * 8) as *const u64);
        let v1 = std::ptr::read_unaligned(ptr.add(c1_start + i * 8) as *const u64);
        let v2 = std::ptr::read_unaligned(ptr.add(c2_start + i * 8) as *const u64);
        // Three independent dependency chains → port 0 keeps issuing 1/cycle.
        crc0 = _mm_crc32_u64(crc0, v0);
        crc1 = _mm_crc32_u64(crc1, v1);
        crc2 = _mm_crc32_u64(crc2, v2);
    }

    // ── Phase 2: Combine the three partial CRCs via CLMUL ────────────
    //
    // CRC_process(seed, A || B) = shift(CRC_process(seed, A), |B|) XOR raw(B)
    // where shift(crc, n_bytes) = crc ⊗ x^(8n) mod P.
    //
    // For three chunks:
    //   final = shift(s0, |C1|+|C2|) ⊕ shift(r1, |C2|) ⊕ r2
    //
    // then CRC32C = final ⊕ 0xFFFFFFFF.

    let len1 = chunk_bytes; // |C1|
    let len2 = c2_qw * 8; // |C2| (qword-aligned part)

    let m12 = transfer_constant(len1 + len2); // M_{|C1|+|C2|}
    let m2 = transfer_constant(len2); // M_{|C2|}

    let s0 = gf_mul(crc0 as u32, m12);
    let s1 = gf_mul(crc1 as u32, m2);
    let mut combined: u32 = s0 ^ s1 ^ (crc2 as u32);

    // ── Phase 3: Remainder bytes (after the 3 qword-aligned chunks) ──
    // Remainder can be 0..23 bytes. Process qwords first, then sub-qword tail.
    let rem_start = c2_start + len2;
    let mut i = rem_start;
    while i + 8 <= n {
        let v = std::ptr::read_unaligned(ptr.add(i) as *const u64);
        combined = _mm_crc32_u64(combined as u64, v) as u32;
        i += 8;
    }
    if i + 4 <= n {
        let v = std::ptr::read_unaligned(ptr.add(i) as *const u32);
        combined = _mm_crc32_u32(combined, v);
        i += 4;
    }
    if i + 2 <= n {
        let v = std::ptr::read_unaligned(ptr.add(i) as *const u16);
        combined = _mm_crc32_u16(combined, v);
        i += 2;
    }
    if i < n {
        combined = _mm_crc32_u8(combined, *ptr.add(i));
    }

    !combined
}

/// Carryless multiply of two 32-bit values → up to 63-bit result.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "pclmulqdq")]
unsafe fn clmul32(a: u32, b: u32) -> u64 {
    use std::arch::x86_64::*;
    let va = _mm_cvtsi64_si128(a as i64);
    let vb = _mm_cvtsi64_si128(b as i64);
    let r = _mm_clmulepi64_si128(va, vb, 0x00);
    _mm_cvtsi128_si64(r) as u64
}

/// Multiply two values in GF(2)[x] mod the CRC32C polynomial.
///
/// The product of two 32-bit values has up to 63 bits. We reduce by
/// repeatedly folding the high part: `hi ⊗ P_R` until no high bits remain.
/// Each fold reduces the degree by 4, so ≤ 8 iterations suffice.
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "pclmulqdq")]
unsafe fn gf_mul(a: u32, b: u32) -> u32 {
    let mut val = clmul32(a, b);
    // val has up to 63 bits: lo (0-31) + hi (32-62).
    let mut lo = val as u32;
    let mut hi = (val >> 32) as u32;

    // Fold: lo ^= clmul(hi, P); extract new hi from result.
    // P has degree 28, so each fold reduces degree(hi) by ≥ 4.
    for _ in 0..8 {
        if hi == 0 {
            break;
        }
        val = clmul32(hi, P);
        lo ^= val as u32;
        hi = (val >> 32) as u32;
    }
    lo
}

/// Compute the CRC transfer constant M_n = M₁^n mod G, where M₁ is the
/// hardware instruction's transfer constant for 1 zero byte.
///
/// Used for combining parallel CRC chains: processing k bytes from state
/// crc gives crc * M_k mod G (for zero data). M_k = M₁^k mod G.
///
/// O(log n) GF-multiplies — negligible vs the CRC loop.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "pclmulqdq")]
unsafe fn transfer_constant(n: usize) -> u32 {
    if n == 0 {
        return 1;
    }
    let mut result: u32 = 1;
    let mut base: u32 = M1;
    let mut e = n;
    while e > 0 {
        if e & 1 != 0 {
            result = gf_mul(result, base);
        }
        base = gf_mul(base, base);
        e >>= 1;
    }
    result
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
        for size in [
            1, 7, 8, 15, 16, 31, 64, 100, 255, 256, 511, 512, 513, 1024, 4096, 65536,
        ] {
            let data: Vec<u8> = (0..size).map(|i| i as u8).collect();
            let hw = crc32c(&data);
            let sw = crc32c_sw(&data);
            assert_eq!(hw, sw, "size {size}: hw=0x{hw:08X} sw=0x{sw:08X}");
        }
    }

    #[test]
    fn parallel_matches_serial_large() {
        // Test various sizes around the parallel threshold and beyond.
        for size in [512, 513, 1023, 1024, 1025, 2048, 4096, 8192, 65536, 100003] {
            let data: Vec<u8> = (0..size).map(|i| (i * 7 + 13) as u8).collect();
            let fast = crc32c(&data);
            let slow = crc32c_sw(&data);
            assert_eq!(
                fast, slow,
                "size {size}: parallel=0x{fast:08X} sw=0x{slow:08X}"
            );
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

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn gf_mul_identity() {
        unsafe {
            assert_eq!(gf_mul(1, 1), 1); // multiplicative identity
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn transfer_constant_basics() {
        unsafe {
            // M_0 = 1 (processing 0 bytes = identity)
            assert_eq!(transfer_constant(0), 1);
            // M_1 must match the hardware CRC instruction's transfer constant
            assert_eq!(transfer_constant(1), M1);
        }
    }
}
