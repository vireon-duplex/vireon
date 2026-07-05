//! Shared helpers for the `send-queue` crate benchmark suite.
//!
//! Mirrors the `io` crate's bench helper pattern: deterministic PRNG,
//! fixture builders, and workload constants so every bench binary uses
//! the same parameters.

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use std::hint::black_box;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use criterion::BatchSize;
use send_queue::batch::BatchEntry;
use send_queue::{PacketMeta, SendQueue, SendQueueBuilder, SendQueueError, SubmissionSink};

// ── Workload constants ────────────────────────────────────────────

/// Packet payload sizes swept across benches (bytes).
pub const PKT_SIZES: &[usize] = &[64, 256, 1024];

/// Extended packet-size sweep including 4 KiB and 16 KiB (cache-line
/// crossing territory) for `cache` benches.
pub const PKT_SIZES_EXT: &[usize] = &[64, 256, 1024, 4096, 16_384];

/// Queue depths swept in flush / lifecycle benches.
pub const DEPTHS: &[usize] = &[16, 64, 256, 1024];

/// Capacities swept in submit benches (power-of-two).
pub const CAPACITIES: &[usize] = &[256, 1024, 4096];

/// Ring capacities for cache-locality sweep — chosen to cross typical
/// L1 / L2 / L3 boundaries at 64 B per slot.
pub const CACHE_CAPACITIES: &[usize] = &[256, 1024, 4096, 16_384];

/// Producer-thread counts for MPSC scaling benches.
pub const PRODUCER_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

/// Fill-level percentages for saturation benches.
pub const FILL_PERCENTS: &[u8] = &[50, 75, 90, 95, 98, 99];

/// Destination address used by every bench (no allocation, parse-free).
pub fn dest_addr() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 8080))
}

// ── Fixture builders ──────────────────────────────────────────────

/// Builds a fresh queue with `capacity` using the default builder path.
pub fn fresh_queue(capacity: usize) -> SendQueue {
    SendQueueBuilder::new()
        .capacity(capacity)
        .build()
        .expect("queue build")
}

/// Submits `n` packets into `sq` and returns their [`PacketId`]s.
pub fn fill_queue(sq: &mut SendQueue, n: usize) -> Vec<send_queue::PacketId> {
    let dest = dest_addr();
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        ids.push(
            sq.submit(i as u16, 64, dest, PacketMeta::new())
                .expect("submit"),
        );
    }
    ids
}

/// Runs the full CQE pair for each id (send + reclaim), freeing slots.
pub fn complete_all(sq: &mut SendQueue, ids: &[send_queue::PacketId]) {
    for id in ids {
        sq.on_send_cqe(id.raw());
        sq.on_reclaim_cqe(id.raw());
    }
}

// ── NullSink: zero-overhead SubmissionSink ────────────────────────

/// [`SubmissionSink`] that accepts every entry and discards it.
///
/// Used to measure the queue's hot path without IO-Layer noise. For
/// partial-submission (SQ-full) scenarios see [`PartialSink`].
pub struct NullSink;

impl SubmissionSink for NullSink {
    #[inline]
    fn submit_batch(&mut self, entries: &[BatchEntry]) -> Result<usize, SendQueueError> {
        Ok(entries.len())
    }
}

/// [`SubmissionSink`] that accepts at most `cap` entries per call,
/// simulating SQ backpressure.
pub struct PartialSink {
    cap: usize,
}

impl PartialSink {
    #[must_use]
    pub const fn new(cap: usize) -> Self {
        Self { cap }
    }
}

impl SubmissionSink for PartialSink {
    #[inline]
    fn submit_batch(&mut self, entries: &[BatchEntry]) -> Result<usize, SendQueueError> {
        Ok(self.cap.min(entries.len()))
    }
}

// ── Batch size helper ─────────────────────────────────────────────

/// Re-export so bench files don't import criterion directly for the
/// `BatchSize` used by `iter_batched`.
pub const SMALL_BATCH: BatchSize = BatchSize::SmallInput;

/// Larger batch size for multi-threaded or long-running benches.
pub const LARGE_BATCH: BatchSize = BatchSize::LargeInput;

// ── PRNG ──────────────────────────────────────────────────────────

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
}

impl Default for XorShift64 {
    fn default() -> Self {
        Self::new()
    }
}

// ── Utilities ─────────────────────────────────────────────────────

/// Sink that prevents the compiler from optimising away a value.
#[inline]
pub fn sink<T>(v: T) {
    let _ = black_box(v);
}
