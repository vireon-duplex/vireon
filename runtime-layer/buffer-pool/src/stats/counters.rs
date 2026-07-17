//! Single-threaded counters (Layer 13).
//!
//! [`Counters`] holds a set of [`Cell<u64>`] fields updated on every
//! acquire / release. Since the pool is single-threaded (`Rc`-based),
//! `Cell` is sufficient — no atomic overhead.

use std::cell::Cell;

use crate::stats::snapshot::CountersSnapshot;

/// Monotonic counters for the pool (single-threaded).
pub struct Counters {
    hits: Cell<u64>,
    misses: Cell<u64>,
    reuses: Cell<u64>,
    allocations: Cell<u64>,
    frees: Cell<u64>,
    outstanding: Cell<u64>,
    peak_outstanding: Cell<u64>,
    bytes_allocated: Cell<u64>,
    bytes_freed: Cell<u64>,
    bytes_outstanding: Cell<u64>,
}

impl Counters {
    /// Creates zeroed counters.
    pub fn new() -> Self {
        Self {
            hits: Cell::new(0),
            misses: Cell::new(0),
            reuses: Cell::new(0),
            allocations: Cell::new(0),
            frees: Cell::new(0),
            outstanding: Cell::new(0),
            peak_outstanding: Cell::new(0),
            bytes_allocated: Cell::new(0),
            bytes_freed: Cell::new(0),
            bytes_outstanding: Cell::new(0),
        }
    }

    /// Free-list hit.
    #[inline]
    pub fn record_hit(&self) {
        self.hits.set(self.hits.get() + 1);
    }

    /// Free-list miss (served from arena).
    #[inline]
    pub fn record_miss(&self) {
        self.misses.set(self.misses.get() + 1);
    }

    /// Served from the free-list (kept for API compatibility).
    #[inline]
    pub fn record_reuse(&self) {
        self.reuses.set(self.reuses.get() + 1);
    }

    /// New allocation from the arena or huge allocator.
    ///
    /// Does **not** touch `outstanding` — call [`record_acquire`] once per
    /// successful acquire regardless of the source.
    ///
    /// [`record_acquire`]: Counters::record_acquire
    #[inline]
    pub fn record_alloc(&self, bytes: usize) {
        self.allocations.set(self.allocations.get() + 1);
        self.bytes_allocated
            .set(self.bytes_allocated.get() + bytes as u64);
    }

    /// Marks one live buffer after a successful acquire.
    ///
    /// Updates `outstanding`, `peak_outstanding`, and `bytes_outstanding`.
    #[inline]
    pub fn record_acquire(&self, bytes: usize) {
        let now = self.outstanding.get() + 1;
        self.outstanding.set(now);
        if now > self.peak_outstanding.get() {
            self.peak_outstanding.set(now);
        }
        self.bytes_outstanding
            .set(self.bytes_outstanding.get() + bytes as u64);
    }

    /// Buffer released back to the pool.
    #[inline]
    pub fn record_free(&self, bytes: usize) {
        self.frees.set(self.frees.get() + 1);
        self.bytes_freed
            .set(self.bytes_freed.get() + bytes as u64);
        self.outstanding
            .set(self.outstanding.get().saturating_sub(1));
        self.bytes_outstanding
            .set(self.bytes_outstanding.get().saturating_sub(bytes as u64));
    }

    /// Current outstanding byte count (used by the backpressure gate).
    #[inline]
    pub fn bytes_outstanding(&self) -> u64 {
        self.bytes_outstanding.get()
    }

    /// Reads all counters into a snapshot.
    pub fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            hits: self.hits.get(),
            misses: self.misses.get(),
            reuses: self.reuses.get(),
            allocations: self.allocations.get(),
            frees: self.frees.get(),
            outstanding: self.outstanding.get(),
            peak_outstanding: self.peak_outstanding.get(),
            bytes_allocated: self.bytes_allocated.get(),
            bytes_freed: self.bytes_freed.get(),
            bytes_outstanding: self.bytes_outstanding.get(),
        }
    }

    /// Resets every counter to zero.
    ///
    /// Useful for benchmarks and test isolation.
    pub fn reset(&self) {
        self.hits.set(0);
        self.misses.set(0);
        self.reuses.set(0);
        self.allocations.set(0);
        self.frees.set(0);
        self.outstanding.set(0);
        self.peak_outstanding.set(0);
        self.bytes_allocated.set(0);
        self.bytes_freed.set(0);
        self.bytes_outstanding.set(0);
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
}
