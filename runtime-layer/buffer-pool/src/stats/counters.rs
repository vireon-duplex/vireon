//! Atomic counters (Layer 13).
//!
//! [`Counters`] holds a set of [`AtomicU64`]
//! fields updated on every acquire / release. All updates use `Relaxed`
//! ordering — they are independent statistics, not publication markers — so
//! `Relaxed` is correct and fastest.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::stats::snapshot::CountersSnapshot;

/// Monotonic counters for the pool.
///
/// All fields use `Relaxed` ordering; callers that need to observe a
/// consistent view across fields must take a [`snapshot`](Counters::snapshot)
/// (still only cross-field consistent, not a transaction).
pub struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    reuses: AtomicU64,
    allocations: AtomicU64,
    frees: AtomicU64,
    outstanding: AtomicU64,
    peak_outstanding: AtomicU64,
    bytes_allocated: AtomicU64,
    bytes_freed: AtomicU64,
    bytes_outstanding: AtomicU64,
}

impl Counters {
    /// Creates zeroed counters.
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
            allocations: AtomicU64::new(0),
            frees: AtomicU64::new(0),
            outstanding: AtomicU64::new(0),
            peak_outstanding: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
            bytes_freed: AtomicU64::new(0),
            bytes_outstanding: AtomicU64::new(0),
        }
    }

    /// Thread-local cache hit.
    #[inline]
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Thread-local cache miss (may still be served from global/arena).
    #[inline]
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Served from the global shard cache.
    #[inline]
    pub fn record_reuse(&self) {
        self.reuses.fetch_add(1, Ordering::Relaxed);
    }

    /// New allocation from the arena or huge allocator.
    ///
    /// Does **not** touch `outstanding` — call [`record_acquire`] once per
    /// successful acquire regardless of the source.
    ///
    /// [`record_acquire`]: Counters::record_acquire
    #[inline]
    pub fn record_alloc(&self, bytes: usize) {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Marks one live buffer after a successful acquire (hit, reuse, or alloc).
    ///
    /// Updates `outstanding`, `peak_outstanding`, and `bytes_outstanding`.
    #[inline]
    pub fn record_acquire(&self, bytes: usize) {
        let now = self.outstanding.fetch_add(1, Ordering::Relaxed) + 1;
        let _prev = self.peak_outstanding.fetch_max(now, Ordering::Relaxed);
        self.bytes_outstanding
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Buffer released back to the pool.
    #[inline]
    pub fn record_free(&self, bytes: usize) {
        self.frees.fetch_add(1, Ordering::Relaxed);
        self.bytes_freed.fetch_add(bytes as u64, Ordering::Relaxed);
        self.outstanding.fetch_sub(1, Ordering::Relaxed);
        self.bytes_outstanding
            .fetch_sub(bytes as u64, Ordering::Relaxed);
    }

    /// Current outstanding byte count (used by the backpressure gate).
    #[inline]
    pub fn bytes_outstanding(&self) -> u64 {
        self.bytes_outstanding.load(Ordering::Relaxed)
    }

    /// Reads all counters into a snapshot.
    pub fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            reuses: self.reuses.load(Ordering::Relaxed),
            allocations: self.allocations.load(Ordering::Relaxed),
            frees: self.frees.load(Ordering::Relaxed),
            outstanding: self.outstanding.load(Ordering::Relaxed),
            peak_outstanding: self.peak_outstanding.load(Ordering::Relaxed),
            bytes_allocated: self.bytes_allocated.load(Ordering::Relaxed),
            bytes_freed: self.bytes_freed.load(Ordering::Relaxed),
            bytes_outstanding: self.bytes_outstanding.load(Ordering::Relaxed),
        }
    }

    /// Resets every counter to zero.
    ///
    /// Useful for benchmarks and test isolation.
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.reuses.store(0, Ordering::Relaxed);
        self.allocations.store(0, Ordering::Relaxed);
        self.frees.store(0, Ordering::Relaxed);
        self.outstanding.store(0, Ordering::Relaxed);
        self.peak_outstanding.store(0, Ordering::Relaxed);
        self.bytes_allocated.store(0, Ordering::Relaxed);
        self.bytes_freed.store(0, Ordering::Relaxed);
        self.bytes_outstanding.store(0, Ordering::Relaxed);
    }
}

impl Default for Counters {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_hit_increments_only_hits() {
        let c = Counters::new();
        c.record_hit();
        c.record_hit();
        let s = c.snapshot();
        assert_eq!(s.hits, 2);
        assert_eq!(s.misses, 0);
        assert_eq!(s.reuses, 0);
    }

    #[test]
    fn record_alloc_tracks_bytes_and_count() {
        let c = Counters::new();
        c.record_alloc(64);
        c.record_alloc(128);
        let s = c.snapshot();
        assert_eq!(s.allocations, 2);
        assert_eq!(s.bytes_allocated, 192);
    }

    #[test]
    fn record_acquire_updates_outstanding_and_peak() {
        let c = Counters::new();
        c.record_acquire(64);
        c.record_acquire(128);
        let s = c.snapshot();
        assert_eq!(s.outstanding, 2);
        assert_eq!(s.peak_outstanding, 2);
        assert_eq!(s.bytes_outstanding, 192);
    }

    #[test]
    fn record_free_decrements_outstanding() {
        let c = Counters::new();
        c.record_acquire(64);
        c.record_acquire(64);
        c.record_free(64);
        let s = c.snapshot();
        assert_eq!(s.outstanding, 1);
        assert_eq!(s.frees, 1);
        assert_eq!(s.peak_outstanding, 2);
        assert_eq!(s.bytes_outstanding, 64);
    }

    #[test]
    fn peak_is_monotone_across_rise_and_fall() {
        let c = Counters::new();
        for _ in 0..5 {
            c.record_acquire(64);
        }
        for _ in 0..3 {
            c.record_free(64);
        }
        c.record_acquire(64);
        let s = c.snapshot();
        assert_eq!(s.outstanding, 3); // 5 - 3 + 1
        assert_eq!(s.peak_outstanding, 5);
    }

    #[test]
    fn reset_zeroes_all_fields() {
        let c = Counters::new();
        c.record_alloc(64);
        c.record_hit();
        c.record_free(64);
        c.reset();
        let s = c.snapshot();
        assert_eq!(s, CountersSnapshot::default());
    }

    #[test]
    fn counters_are_thread_safe_under_stress() {
        let c = Counters::new();
        let c_ref = &c;
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(move || {
                    for _ in 0..1000 {
                        c_ref.record_alloc(64);
                        c_ref.record_acquire(64);
                        c_ref.record_hit();
                        c_ref.record_free(64);
                    }
                });
            }
        });
        let s = c.snapshot();
        assert_eq!(s.allocations, 4000);
        assert_eq!(s.hits, 4000);
        assert_eq!(s.frees, 4000);
        assert_eq!(s.outstanding, 0);
    }
}
