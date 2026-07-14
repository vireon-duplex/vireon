//! Thread-local collector — the hot-path accumulator.
//!
//! Each thread owns a [`LocalCollector`] via `thread_local!`. Every
//! instrumented function call records its duration here without any
//! locks or atomics. When the accumulated call count exceeds the flush
//! threshold (default 1024), the collector merges its data into the
//! global registry and resets.
//!
//! # Hot-path cost
//!
//! `record()` does:
//! 1. One `thread_local!` borrow (compiler-optimized TLS access)
//! 2. One `HashMap` lookup by `&'static str` (FxHash, no allocation
//!    after the first call to each function)
//! 3. Plain `u64` arithmetic + one array increment
//!
//! No locks, no atomics, no heap allocation. Amortized O(1).

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use rustc_hash::FxHashMap;

use crate::context;
use crate::registry;
use crate::stats::LocalEntry;

/// Default flush threshold (calls before a thread flushes to global).
const DEFAULT_FLUSH_THRESHOLD: usize = 1024;

/// Global enable/disable flag.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Global sampling rate (1 = every call, N = every Nth call).
static SAMPLING: AtomicU32 = AtomicU32::new(1);

/// Flush threshold (configurable via [`set_flush_threshold`]).
static FLUSH_THRESHOLD: AtomicUsize = AtomicUsize::new(DEFAULT_FLUSH_THRESHOLD);

/// Enables or disables profiling globally.
#[inline]
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// Returns whether profiling is enabled.
#[inline]
#[must_use]
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Sets the sampling rate (1 = every call, N = every Nth call).
#[inline]
pub fn set_sampling(rate: u32) {
    SAMPLING.store(rate.max(1), Ordering::Relaxed);
}

/// Sets the flush threshold (calls before a thread flushes).
#[inline]
pub fn set_flush_threshold(threshold: usize) {
    FLUSH_THRESHOLD.store(threshold.max(1), Ordering::Relaxed);
}

/// Background tick generation. Incremented by the background thread
/// every `flush_interval_ms`. Each thread-local collector checks this
/// on every call and flushes if the generation changed.
static TICK_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Increments the global tick generation, signalling all threads to
/// flush their local data on the next call.
#[inline]
pub fn bump_tick() {
    TICK_GENERATION.fetch_add(1, Ordering::Relaxed);
}

// ── Thread-local collector ────────────────────────────────────────

thread_local! {
    static LOCAL: RefCell<LocalCollector> = RefCell::new(LocalCollector::new());
}

/// Per-thread accumulator.
pub struct LocalCollector {
    entries: FxHashMap<&'static str, LocalEntry>,
    calls_since_flush: u64,
    /// Local sampling counter (for skip logic).
    sample_counter: u64,
    /// Last seen tick generation (for background-triggered flush).
    last_tick: u64,
}

impl LocalCollector {
    #[must_use]
    fn new() -> Self {
        Self {
            entries: FxHashMap::default(),
            calls_since_flush: 0,
            sample_counter: 0,
            last_tick: 0,
        }
    }

    /// Records a single observation.
    #[inline]
    fn record(&mut self, name: &'static str, ns: u64) {
        // Background tick check: flush if the generation changed.
        let current_tick = TICK_GENERATION.load(Ordering::Relaxed);
        if current_tick != self.last_tick {
            self.last_tick = current_tick;
            self.flush();
        }

        // Sampling: skip if this call is not sampled.
        let sampling = SAMPLING.load(Ordering::Relaxed);
        if sampling > 1 {
            self.sample_counter = self.sample_counter.wrapping_add(1);
            if self.sample_counter % sampling as u64 != 0 {
                return;
            }
        }

        self.entries
            .entry(name)
            .or_insert_with(LocalEntry::new)
            .record(ns);

        self.calls_since_flush += 1;
        let threshold = FLUSH_THRESHOLD.load(Ordering::Relaxed) as u64;
        if self.calls_since_flush >= threshold {
            self.flush();
        }
    }

    /// Merges all local entries into the global registry, then resets.
    fn flush(&mut self) {
        if self.entries.is_empty() {
            self.calls_since_flush = 0;
            return;
        }

        for (name, entry) in &mut self.entries {
            if entry.has_data() {
                registry::merge(name, entry);
                entry.reset();
            }
        }

        // Also flush call-tree edges.
        let edges = context::drain_edges();
        crate::flamegraph::merge_edges(edges);

        self.calls_since_flush = 0;
    }

    /// Forces a flush (called on thread exit or explicit `Profiler::stop()`).
    fn force_flush(&mut self) {
        self.flush();
    }
}

// ── Public API for the guard ──────────────────────────────────────

/// Records a timing observation.
///
/// This is the function called by `Guard::drop` on every instrumented
/// function return. It must be as fast as possible.
#[inline]
pub fn record(name: &'static str, ns: u64) {
    LOCAL.with(|local| {
        local.borrow_mut().record(name, ns);
    });
}

/// Flushes the current thread's local collector.
pub fn flush_local() {
    LOCAL.with(|local| {
        local.borrow_mut().force_flush();
    });
}

// ── Thread-exit hook ──────────────────────────────────────────────

thread_local! {
    static FLUSH_ON_EXIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Marks the current thread for auto-flush on exit.
pub fn enable_auto_flush() {
    FLUSH_ON_EXIT.with(|f| f.set(true));
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn reset_all() {
        registry::reset_all();
        set_enabled(true);
        set_sampling(1);
        set_flush_threshold(DEFAULT_FLUSH_THRESHOLD);
    }

    /// Sync the local collector's tick to prevent background-triggered flushes during tests.
    fn sync_tick(c: &mut LocalCollector) {
        c.last_tick = TICK_GENERATION.load(Ordering::Relaxed);
    }

    #[test]
    fn record_accumulates_locally() {
        reset_all();
        // Directly test the collector.
        LOCAL.with(|local| {
            let mut c = local.borrow_mut();
            c.entries.clear();
            c.calls_since_flush = 0;
            sync_tick(&mut c);
            c.record("test_record_accumulates", 100);
            c.record("test_record_accumulates", 200);
            let entry = c.entries.get("test_record_accumulates").unwrap();
            assert_eq!(entry.call_count, 2);
            assert_eq!(entry.total_ns, 300);
        });
    }

    #[test]
    fn flush_merges_to_global() {
        reset_all();
        LOCAL.with(|local| {
            let mut c = local.borrow_mut();
            c.entries.clear();
            c.calls_since_flush = 0;
            sync_tick(&mut c);
            c.record("test_flush_merges", 100);
            c.record("test_flush_merges", 200);
            c.flush();
            // After flush, local entries are reset.
            let entry = c.entries.get("test_flush_merges").unwrap();
            assert_eq!(entry.call_count, 0);
        });
    }

    #[test]
    fn flush_threshold_triggers_flush() {
        reset_all();
        set_flush_threshold(5);
        LOCAL.with(|local| {
            let mut c = local.borrow_mut();
            c.entries.clear();
            c.calls_since_flush = 0;
            sync_tick(&mut c);
            for _ in 0..5 {
                c.record("test_threshold", 10);
            }
            // After 5 calls (== threshold), flush should have triggered.
            assert_eq!(c.calls_since_flush, 0);
        });
        set_flush_threshold(DEFAULT_FLUSH_THRESHOLD);
    }

    #[test]
    fn sampling_skips_calls() {
        reset_all();
        set_sampling(3);
        LOCAL.with(|local| {
            let mut c = local.borrow_mut();
            c.entries.clear();
            c.calls_since_flush = 0;
            c.sample_counter = 0;
            sync_tick(&mut c);
            for _ in 0..9 {
                c.record("test_sampling", 10);
            }
            // With sampling=3, only 3 of 9 calls are recorded.
            let entry = c.entries.get("test_sampling").unwrap();
            assert_eq!(entry.call_count, 3);
        });
        set_sampling(1);
    }

    #[test]
    fn enable_disable_flag() {
        set_enabled(false);
        assert!(!is_enabled());
        set_enabled(true);
        assert!(is_enabled());
    }

    #[test]
    fn force_flush_works() {
        reset_all();
        LOCAL.with(|local| {
            let mut c = local.borrow_mut();
            c.entries.clear();
            c.calls_since_flush = 0;
            sync_tick(&mut c);
            c.record("test_force_flush", 50);
            c.force_flush();
            let entry = c.entries.get("test_force_flush").unwrap();
            assert_eq!(entry.call_count, 0);
        });
    }
}
