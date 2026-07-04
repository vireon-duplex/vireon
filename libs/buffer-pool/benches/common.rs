//! Shared helpers for all buffer-pool benchmark binaries.
//!
//! Each `[[bench]]` file starts with:
//!
//! ```rust,ignore
//! #![allow(clippy::expect_used, clippy::unwrap_used)]
//! #[path = "common.rs"]
//! mod common;
//! ```
//!
//! This keeps workload data, the PRNG, and helper routines in one place.

// Not every bench binary uses every constant/function in this module.
#![allow(dead_code)]
// Bench helper code uses expect/unwrap for brevity (allowed in benches).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::hint::black_box;

// ============================================================================
// Workload sizes
// ============================================================================

/// Realistic FluxIO session traffic: 11 sizes spanning 7 size classes.
pub const WORKLOAD: &[(usize, &str)] = &[
    (64, "heartbeat"),
    (256, "stream_reset"),
    (1024, "stream_open"),
    (2048, "error"),
    (4096, "tool_call"),
    (80_000, "audio_frame"),
    (100_000, "text_token"),
    (120_000, "fragment"),
    (148_000, "tool_result"),
    (256_000, "image_frame"),
    (320_000, "video_frame"),
];

/// All 15 power-of-2 size-class slot sizes (64 B – 1 MiB).
pub const ALL_CLASS_SIZES: [usize; 15] = [
    64, 128, 256, 512, 1024, 2048, 4096, 8192, 16_384, 32_768, 65_536, 131_072, 262_144, 524_288,
    1_048_576,
];

/// Human-readable labels for the 15 classes.
pub const ALL_CLASS_LABELS: [&str; 15] = [
    "64B", "128B", "256B", "512B", "1KiB", "2KiB", "4KiB", "8KiB", "16KiB", "32KiB", "64KiB",
    "128KiB", "256KiB", "512KiB", "1MiB",
];

/// Three representative sizes: small, medium, large.
pub const REPRESENTATIVE: &[(usize, &str)] = &[(64, "64B"), (4096, "4KiB"), (131_072, "128KiB")];

/// Huge allocation sizes (above the 1 MiB class ceiling).
pub const HUGE_SIZES: &[(usize, &str)] = &[
    (2 * 1024 * 1024, "2MiB"),
    (4 * 1024 * 1024, "4MiB"),
    (8 * 1024 * 1024, "8MiB"),
    (16 * 1024 * 1024, "16MiB"),
];

/// QUIC / networking payload sizes.
pub const QUIC_SIZES: &[(usize, &str)] = &[
    (64, "64B"),
    (512, "512B"),
    (1200, "MTU"),
    (4096, "4KiB"),
    (16_384, "16KiB"),
    (65_536, "64KiB"),
];

/// Thread counts for scalability sweeps.
pub const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16, 32, 64];

/// Batch sizes to sweep.
pub const BATCH_SIZES: &[usize] = &[1, 2, 4, 8, 16, 32, 64];

// ============================================================================
// xorshift64 PRNG (deterministic, no external deps)
// ============================================================================

/// xorshift64 — fast, deterministic, sufficient for benchmark workload generation.
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// Creates a PRNG with a fixed seed for reproducibility.
    pub fn new(seed: u64) -> Self {
        // Ensure non-zero state.
        Self {
            state: if seed == 0 {
                0xDEAD_BEEF_CAFE_BABE
            } else {
                seed
            },
        }
    }

    /// Advances the generator and returns the next `u64`.
    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Returns a value in `[min, max)`.
    #[inline]
    pub fn gen_range(&mut self, min: usize, max: usize) -> usize {
        let range = max - min;
        min + (self.next_u64() as usize % range)
    }

    /// Picks an element from `slice`.
    #[inline]
    pub fn pick<'a, T>(&mut self, slice: &'a [T]) -> &'a T {
        let idx = self.next_u64() as usize % slice.len();
        &slice[idx]
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Writes a deterministic pattern to every byte of `buf` so the compiler
/// cannot elide the allocation as dead stores.
#[inline]
pub fn fill_pattern(buf: &mut [u8], seed: u8) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u8);
    }
    black_box(buf.as_ptr());
}

/// Touches one byte per 64-byte cache line — models a realistic "header scan"
/// without writing the full payload.
#[inline]
pub fn touch_cache_lines(buf: &[u8]) {
    let mut i = 0;
    while i < buf.len() {
        black_box(buf[i]);
        i += 64;
    }
}

/// Sum of all workload sizes (for throughput calculations).
pub fn total_workload_bytes() -> usize {
    WORKLOAD.iter().map(|(s, _)| *s).sum()
}

/// Pre-populates the global cache for `pool` and `size` by allocating `count`
/// buffers, dropping them, and flushing the thread-local cache. After this
/// call, the global shard has slots available for refill benchmarks.
pub fn seed_global_cache(pool: &buffer_pool::BufferPool, size: usize, count: usize) {
    let bufs: Vec<_> = (0..count).map(|_| pool.get(size).expect("get")).collect();
    drop(bufs);
    pool.flush();
}
