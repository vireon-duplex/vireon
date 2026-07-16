//! Cache-aligned scratch + counters (Layer 5).
//!
//! All hot mutable state lives in [`HotState`], which is `#[repr(align(128))]`
//! so it occupies two cache lines and never shares a line with adjacent
//! fields. This prevents false sharing with other per-reactor state.

use std::sync::atomic::AtomicU64;

/// Cache-aligned hot counters. Updated on every CQE.
///
/// Layout: two cache lines (128 bytes).
/// - Line 1: `cqes_total`, `drain_cycles`, `last_drain_size`.
/// - Line 2: dispatch histogram (16 slots — one per OpKind).
#[repr(align(128))]
pub struct HotState {
    // Line 0 (64 bytes)
    /// Total CQEs processed across all drains.
    pub cqes_total: AtomicU64,
    /// Total `drain()` invocations.
    pub drain_cycles: AtomicU64,
    /// Size of the most recent drain batch.
    pub last_drain_size: AtomicU64,
    /// Total buffers returned via the recycle pipeline.
    pub buffers_recycled: AtomicU64,
    _pad0: [u8; 64 - 4 * 8],

    // Line 1 (128 bytes)
    /// Per-OpKind dispatch histogram.
    pub cqes_per_opkind: [AtomicU64; 16],
}

impl HotState {
    /// Construct a zeroed hot-state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cqes_total: AtomicU64::new(0),
            drain_cycles: AtomicU64::new(0),
            last_drain_size: AtomicU64::new(0),
            buffers_recycled: AtomicU64::new(0),
            _pad0: [0; 32],
            cqes_per_opkind: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    /// Record a single CQE dispatched under `opkind_index`.
    #[inline]
    pub fn record_cqe(&self, opkind_index: usize) {
        self.cqes_total
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        debug_assert!(opkind_index < 16);
        // SAFETY: `opkind_index` is derived from an OpKind discriminant
        // which is guaranteed to be in [0, 16).
        unsafe {
            (*self.cqes_per_opkind.as_ptr().add(opkind_index))
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Record the completion of a drain batch.
    #[inline]
    pub fn record_drain(&self, batch_size: u64) {
        self.drain_cycles
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        self.last_drain_size
            .store(batch_size, core::sync::atomic::Ordering::Relaxed);
    }

    /// Record `n` recycled buffers.
    #[inline]
    pub fn record_recycle(&self, n: u64) {
        self.buffers_recycled
            .fetch_add(n, core::sync::atomic::Ordering::Relaxed);
    }
}

impl Default for HotState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn alignment_is_128() {
        assert!(core::mem::align_of::<HotState>() >= 64);
    }

    #[test]
    fn record_cqe_increments_total_and_histogram() {
        let h = HotState::new();
        h.record_cqe(0);
        h.record_cqe(0);
        h.record_cqe(1);
        assert_eq!(h.cqes_total.load(Ordering::Relaxed), 3);
        assert_eq!(h.cqes_per_opkind[0].load(Ordering::Relaxed), 2);
        assert_eq!(h.cqes_per_opkind[1].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_cqe_increments_correct_histogram_slot() {
        let h = HotState::new();
        // All valid OpKind discriminants (0..=3) must hit the right slot.
        for i in 0..4u32 {
            h.record_cqe(i as usize);
        }
        assert_eq!(h.cqes_total.load(Ordering::Relaxed), 4);
        for i in 0..4 {
            assert_eq!(h.cqes_per_opkind[i].load(Ordering::Relaxed), 1);
        }
    }
}
