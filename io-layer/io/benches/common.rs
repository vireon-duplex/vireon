//! Shared helpers for the `io` crate benchmark suite.
//!
//! Mirrors `runtime-layer/timer-wheel/benches/common.rs`: deterministic PRNG,
//! fixture builders, and packet-size constants so every bench binary
//! uses the same workload parameters.

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use std::hint::black_box;

use io::buffer_pool::BufferPool;
use io::connection::Connection;
use io_uring::IoUring;

/// Packet sizes used across benches (bytes). Covers the typical UDP MTU
/// range from tiny ACKs to full 2048-byte provided buffers.
pub const PKT_SIZES: &[usize] = &[64, 256, 1024, 2048];

/// Ring-entry counts swept in ring-init and buffer-pool benches.
pub const RING_ENTRIES: &[u32] = &[4, 16, 64, 256, 1024];

/// Buffer counts swept in buffer-pool init benches.
pub const BUF_COUNTS: &[u32] = &[16, 64, 256, 1024];

/// Buffer sizes swept in buffer-pool init benches.
pub const BUF_SIZES: &[usize] = &[64, 256, 1024, 2048];

// ─── Fixture builders ─────────────────────────────────────────────

/// Creates a fresh io_uring ring with Vireon's standard flags.
pub fn fresh_ring(entries: u32) -> IoUring {
    IoUring::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .setup_coop_taskrun()
        .build(entries)
        .expect("ring init")
}

/// Binds a UDP socket on `0.0.0.0:{port}` (port 0 = ephemeral).
pub fn fresh_connection(port: u16) -> Connection {
    Connection::setup_socket(port).expect("socket setup")
}

/// Creates a provided-buffer ring with the given parameters.
pub fn fresh_pool(ring: &IoUring, bgid: u16, count: u32, size: usize) -> BufferPool {
    BufferPool::init_buffer_ring(&ring.submitter(), bgid, count, size).expect("buffer pool")
}

// ─── PRNG ─────────────────────────────────────────────────────────

/// Deterministic XorShift64 PRNG for reproducible workloads.
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// Creates a new PRNG with a fixed seed (deterministic across runs).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: 0x9E37_79B9_7F4A_7C15,
        }
    }

    /// Returns the next pseudo-random `u64`.
    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Returns a pseudo-random `u64` in `[0, bound)`.
    #[must_use]
    pub fn next_bounded(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }
}

impl Default for XorShift64 {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Utilities ────────────────────────────────────────────────────

/// Sink that prevents the compiler from optimising away a value.
#[inline]
pub fn sink<T>(v: T) {
    let _ = black_box(v);
}
