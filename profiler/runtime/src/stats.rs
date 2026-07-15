//! Per-function statistics with atomic counters and lock-free histogram.
//!
//! [`FunctionStats`] is the **global** per-function accumulator. Each
//! field is an [`AtomicU64`] or an embedded [`LatencyHistogram`], so
//! updates from multiple threads never block.
//!
//! [`LocalEntry`] is the **thread-local** per-function accumulator.
//! It uses plain `u64` counters (no atomics) for the fastest possible
//! hot path, then flushes into [`FunctionStats`] atomically.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::histogram::{LatencyHistogram, NUM_BUCKETS, bucket_for};

// ── Atomic min/max helpers ────────────────────────────────────────

/// Atomically update `target` to `min(target, new)` using a CAS loop.
#[inline]
fn atomic_fetch_min(target: &AtomicU64, new: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while new < current {
        match target.compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

/// Atomically update `target` to `max(target, new)` using a CAS loop.
#[inline]
fn atomic_fetch_max(target: &AtomicU64, new: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while new > current {
        match target.compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

// ── Global per-function stats ─────────────────────────────────────

/// Global atomic statistics for a single function.
///
/// One instance per instrumented function name. Stored in the global
/// registry behind a `Box`. All updates go through relaxed atomics.
pub struct FunctionStats {
    /// Function name (compile-time `&'static str`).
    pub name: &'static str,
    call_count: AtomicU64,
    total_ns: AtomicU64,
    min_ns: AtomicU64,
    max_ns: AtomicU64,
    last_ns: AtomicU64,
    histogram: LatencyHistogram,
}

impl FunctionStats {
    /// Creates a new zero-initialized stats block.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            call_count: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            min_ns: AtomicU64::new(u64::MAX),
            max_ns: AtomicU64::new(0),
            last_ns: AtomicU64::new(0),
            histogram: LatencyHistogram::new(),
        }
    }

    /// Records a single timing observation (nanoseconds).
    #[inline]
    pub fn observe(&self, ns: u64) {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.total_ns.fetch_add(ns, Ordering::Relaxed);
        atomic_fetch_min(&self.min_ns, ns);
        atomic_fetch_max(&self.max_ns, ns);
        self.last_ns.store(ns, Ordering::Relaxed);
        self.histogram.observe(ns);
    }

    /// Merges a thread-local [`LocalEntry`] into this global stats.
    ///
    /// Called during periodic flush (every ~1024 calls per thread).
    pub fn merge_local(&self, local: &LocalEntry) {
        self.call_count
            .fetch_add(local.call_count, Ordering::Relaxed);
        self.total_ns.fetch_add(local.total_ns, Ordering::Relaxed);
        if local.call_count > 0 {
            atomic_fetch_min(&self.min_ns, local.min_ns);
            atomic_fetch_max(&self.max_ns, local.max_ns);
            self.last_ns.store(local.last_ns, Ordering::Relaxed);
            self.histogram.merge_counts(&local.buckets);
        }
    }

    /// Takes a consistent snapshot of all metrics.
    #[must_use]
    pub fn snapshot(&self) -> FunctionSnapshot {
        let calls = self.call_count.load(Ordering::Relaxed);
        let total = self.total_ns.load(Ordering::Relaxed);
        let avg = if calls > 0 { total / calls } else { 0 };
        let min = if calls > 0 {
            self.min_ns.load(Ordering::Relaxed)
        } else {
            0
        };
        FunctionSnapshot {
            name: self.name,
            calls,
            total_ns: total,
            avg_ns: avg,
            min_ns: min,
            max_ns: self.max_ns.load(Ordering::Relaxed),
            p50: self.histogram.percentile(50.0),
            p95: self.histogram.percentile(95.0),
            p99: self.histogram.percentile(99.0),
            last_ns: self.last_ns.load(Ordering::Relaxed),
        }
    }

    /// Resets every counter to zero.
    pub fn reset(&self) {
        self.call_count.store(0, Ordering::Relaxed);
        self.total_ns.store(0, Ordering::Relaxed);
        self.min_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_ns.store(0, Ordering::Relaxed);
        self.last_ns.store(0, Ordering::Relaxed);
        self.histogram.reset();
    }
}

impl std::fmt::Debug for FunctionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionStats")
            .field("name", &self.name)
            .field("calls", &self.call_count.load(Ordering::Relaxed))
            .finish()
    }
}

// ── Thread-local per-function entry ───────────────────────────────

/// Thread-local per-function accumulator (no atomics — single-thread).
///
/// Lives inside the thread-local [`LocalCollector`]'s `HashMap`. Flushed
/// into the global [`FunctionStats`] periodically via `merge_local`.
pub struct LocalEntry {
    pub call_count: u64,
    pub total_ns: u64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub last_ns: u64,
    pub buckets: Vec<u64>,
}

impl LocalEntry {
    /// Creates a new zero-initialized entry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            call_count: 0,
            total_ns: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            last_ns: 0,
            buckets: vec![0; NUM_BUCKETS],
        }
    }

    /// Records a single timing observation.
    #[inline]
    pub fn record(&mut self, ns: u64) {
        self.call_count += 1;
        self.total_ns += ns;
        if ns < self.min_ns {
            self.min_ns = ns;
        }
        if ns > self.max_ns {
            self.max_ns = ns;
        }
        self.last_ns = ns;
        // Increment the local histogram bucket.
        let idx = bucket_for(ns);
        self.buckets[idx] += 1;
    }

    /// Resets the entry to zero (after a flush).
    pub fn reset(&mut self) {
        self.call_count = 0;
        self.total_ns = 0;
        self.min_ns = u64::MAX;
        self.max_ns = 0;
        self.last_ns = 0;
        for b in &mut self.buckets {
            *b = 0;
        }
    }

    /// Whether this entry has accumulated data.
    #[must_use]
    pub fn has_data(&self) -> bool {
        self.call_count > 0
    }
}

impl Default for LocalEntry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Snapshot (immutable, for reports / export) ────────────────────

/// Immutable snapshot of a function's metrics at a point in time.
#[derive(Clone, Debug)]
pub struct FunctionSnapshot {
    /// Function name.
    pub name: &'static str,
    /// Total call count.
    pub calls: u64,
    /// Total time spent (ns).
    pub total_ns: u64,
    /// Average duration (ns).
    pub avg_ns: u64,
    /// Minimum observed duration (ns).
    pub min_ns: u64,
    /// Maximum observed duration (ns).
    pub max_ns: u64,
    /// P50 (median) duration (ns).
    pub p50: u64,
    /// P95 duration (ns).
    pub p95: u64,
    /// P99 duration (ns).
    pub p99: u64,
    /// Last recorded duration (ns).
    pub last_ns: u64,
}

impl FunctionSnapshot {
    /// Formats a duration in nanoseconds as a human-readable string.
    #[must_use]
    pub fn fmt_duration(ns: u64) -> String {
        if ns < 1_000 {
            format!("{ns} ns")
        } else if ns < 1_000_000 {
            format!("{:.2} µs", ns as f64 / 1_000.0)
        } else if ns < 1_000_000_000 {
            format!("{:.2} ms", ns as f64 / 1_000_000.0)
        } else {
            format!("{:.2} s", ns as f64 / 1_000_000_000.0)
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn function_stats_observe() {
        let stats = FunctionStats::new("test_fn");
        stats.observe(100);
        stats.observe(200);
        stats.observe(300);
        let snap = stats.snapshot();
        assert_eq!(snap.name, "test_fn");
        assert_eq!(snap.calls, 3);
        assert_eq!(snap.total_ns, 600);
        assert_eq!(snap.avg_ns, 200);
        assert_eq!(snap.min_ns, 100);
        assert_eq!(snap.max_ns, 300);
        assert_eq!(snap.last_ns, 300);
    }

    #[test]
    fn function_stats_empty_snapshot() {
        let stats = FunctionStats::new("empty");
        let snap = stats.snapshot();
        assert_eq!(snap.calls, 0);
        assert_eq!(snap.avg_ns, 0);
        assert_eq!(snap.min_ns, 0);
        assert_eq!(snap.max_ns, 0);
    }

    #[test]
    fn function_stats_reset() {
        let stats = FunctionStats::new("reset_fn");
        stats.observe(100);
        stats.observe(200);
        stats.reset();
        let snap = stats.snapshot();
        assert_eq!(snap.calls, 0);
    }

    #[test]
    fn local_entry_record_and_reset() {
        let mut entry = LocalEntry::new();
        entry.record(10);
        entry.record(20);
        entry.record(30);
        assert_eq!(entry.call_count, 3);
        assert_eq!(entry.total_ns, 60);
        assert_eq!(entry.min_ns, 10);
        assert_eq!(entry.max_ns, 30);
        assert_eq!(entry.last_ns, 30);
        assert!(entry.has_data());

        entry.reset();
        assert_eq!(entry.call_count, 0);
        assert!(!entry.has_data());
    }

    #[test]
    fn merge_local_into_global() {
        let global = FunctionStats::new("merged");
        let mut local = LocalEntry::new();
        local.record(10);
        local.record(20);
        local.record(30);
        global.merge_local(&local);

        let snap = global.snapshot();
        assert_eq!(snap.calls, 3);
        assert_eq!(snap.total_ns, 60);
        assert_eq!(snap.min_ns, 10);
        assert_eq!(snap.max_ns, 30);
    }

    #[test]
    fn merge_multiple_locals() {
        let global = FunctionStats::new("multi");

        let mut local1 = LocalEntry::new();
        local1.record(10);
        local1.record(20);
        global.merge_local(&local1);
        local1.reset();

        let mut local2 = LocalEntry::new();
        local2.record(30);
        local2.record(40);
        global.merge_local(&local2);

        let snap = global.snapshot();
        assert_eq!(snap.calls, 4);
        assert_eq!(snap.total_ns, 100);
        assert_eq!(snap.min_ns, 10);
        assert_eq!(snap.max_ns, 40);
    }

    #[test]
    fn atomic_min_max_threaded() {
        use std::sync::Arc;
        use std::thread;
        let stats = Arc::new(FunctionStats::new("threaded"));
        let mut handles = Vec::new();
        for t in 0..4 {
            let stats = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    stats.observe(t * 1000 + i);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let snap = stats.snapshot();
        assert_eq!(snap.calls, 4000);
        assert!(snap.min_ns <= snap.max_ns);
    }

    #[test]
    fn fmt_duration_formats_correctly() {
        assert_eq!(FunctionSnapshot::fmt_duration(0), "0 ns");
        assert_eq!(FunctionSnapshot::fmt_duration(500), "500 ns");
        assert!(FunctionSnapshot::fmt_duration(1_500).contains("µs"));
        assert!(FunctionSnapshot::fmt_duration(1_500_000).contains("ms"));
        assert!(FunctionSnapshot::fmt_duration(1_500_000_000).contains("s"));
    }
}
