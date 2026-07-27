//! Allocation-site leak tracking (Layer 20).
//!
//! [`LeakTracker`] records every outstanding allocation in a side table keyed
//! by slot address. When the `leak` cargo feature is enabled each entry also
//! captures a backtrace via `std::backtrace`; without the feature the
//! backtrace string is empty to avoid the capture cost.
//!
//! Integration with [`BufferPool`] is automatic: `PoolInner` owns a tracker
//! and calls [`record_alloc`](LeakTracker::record_alloc) /
//! [`record_free`](LeakTracker::record_free) on every acquire / release.
//!
//! ## Performance
//!
//! The `leak` feature is **off by default**. When off, `LeakTracker` is a
//! zero-sized struct with `#[inline(always)]` no-op methods — the compiler
//! eliminates every call site, so there is no Mutex acquisition, no
//! `HashMap` insertion, and no `String` allocation per buffer op.
//!
//! Flamegraph analysis (2026-07-26) showed the always-on tracker consumed
//! ~0.73% of CPU in single-worker mode and contributed to per-acquire
//! allocator pressure. The feature gate recovers that cost without changing
//! the public API.
//!
//! [`BufferPool`]: crate::buffer::BufferPool

#[cfg(feature = "leak")]
use std::backtrace::Backtrace;

#[cfg(feature = "leak")]
use std::collections::HashMap;
#[cfg(feature = "leak")]
use std::sync::Mutex;
#[cfg(feature = "leak")]
use std::time::Instant;

// ── active implementation (feature = "leak") ───────────────────────

/// A single outstanding allocation recorded by [`LeakTracker`].
#[derive(Debug, Clone)]
pub struct LeakEntry {
    /// Slot address.
    pub addr: usize,
    /// Capacity in bytes.
    pub size: usize,
    /// When the allocation occurred.
    #[cfg(feature = "leak")]
    pub allocated_at: Instant,
    /// Name of the allocating thread (`<unnamed>` if not set).
    #[cfg(feature = "leak")]
    pub thread: String,
    /// Backtrace at allocation time (empty unless the `leak` feature is on).
    #[cfg(feature = "leak")]
    pub backtrace: String,
}

/// Side-table tracker for outstanding allocations.
///
/// Every successful acquire calls [`record_alloc`](Self::record_alloc); every
/// release calls [`record_free`](Self::record_free). After all expected
/// releases, [`report`](Self::report) should return an empty list — any
/// remaining entries are leaks.
///
/// When the `leak` cargo feature is **off** (the default), this struct is
/// zero-sized and all methods are no-ops — see the module-level docs.
pub struct LeakTracker {
    #[cfg(feature = "leak")]
    entries: Mutex<HashMap<usize, LeakEntry>>,
}

impl LeakTracker {
    /// Creates an empty tracker.
    #[cfg(feature = "leak")]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Creates a zero-sized no-op tracker.
    #[cfg(not(feature = "leak"))]
    #[inline(always)]
    pub fn new() -> Self {
        Self {}
    }

    /// Records a fresh allocation.
    #[cfg(feature = "leak")]
    pub fn record_alloc(&self, addr: usize, size: usize) {
        let entry = LeakEntry {
            addr,
            size,
            allocated_at: Instant::now(),
            thread: current_thread_name(),
            backtrace: capture_backtrace(),
        };
        lock(&self.entries).insert(addr, entry);
    }

    /// No-op (feature disabled).
    #[cfg(not(feature = "leak"))]
    #[inline(always)]
    pub fn record_alloc(&self, _addr: usize, _size: usize) {}

    /// Marks the allocation at `addr` as released.
    #[cfg(feature = "leak")]
    pub fn record_free(&self, addr: usize) {
        lock(&self.entries).remove(&addr);
    }

    /// No-op (feature disabled).
    #[cfg(not(feature = "leak"))]
    #[inline(always)]
    pub fn record_free(&self, _addr: usize) {}

    /// Returns all currently-outstanding allocations.
    ///
    /// Always empty when the `leak` feature is off.
    pub fn report(&self) -> Vec<LeakEntry> {
        #[cfg(feature = "leak")]
        {
            lock(&self.entries).values().cloned().collect()
        }
        #[cfg(not(feature = "leak"))]
        {
            Vec::new()
        }
    }

    /// Number of outstanding allocations.
    ///
    /// Always zero when the `leak` feature is off.
    pub fn count(&self) -> usize {
        #[cfg(feature = "leak")]
        {
            lock(&self.entries).len()
        }
        #[cfg(not(feature = "leak"))]
        {
            0
        }
    }

    /// Emits each outstanding allocation via `tracing::warn!`.
    pub fn dump(&self) {
        let entries = self.report();
        if entries.is_empty() {
            tracing::info!("leak tracker: no outstanding allocations");
            return;
        }
        tracing::warn!("leak tracker: {} outstanding allocation(s)", entries.len());
        #[cfg(feature = "leak")]
        for e in &entries {
            tracing::warn!(
                addr = e.addr,
                size = e.size,
                thread = %e.thread,
                "outstanding buffer"
            );
        }
    }
}

#[cfg(feature = "leak")]
impl Default for LeakTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "leak")]
fn capture_backtrace() -> String {
    format!("{}", Backtrace::force_capture())
}

#[cfg(not(feature = "leak"))]
fn capture_backtrace() -> String {
    String::new()
}

#[cfg(feature = "leak")]
fn current_thread_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("<unnamed>")
        .to_owned()
}

#[cfg(not(feature = "leak"))]
#[inline(always)]
fn current_thread_name() -> String {
    String::new()
}

/// Locks a mutex, recovering from poison.
#[cfg(feature = "leak")]
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!("leak tracker mutex poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_alloc_then_free_yields_empty_report() {
        let t = LeakTracker::new();
        t.record_alloc(0x1000, 64);
        // Without the `leak` feature the tracker is a no-op — count()
        // stays 0. Only assert state when the tracker actually records.
        #[cfg(feature = "leak")]
        {
            assert_eq!(t.count(), 1);
            t.record_free(0x1000);
            assert_eq!(t.count(), 0);
            assert!(t.report().is_empty());
        }
        #[cfg(not(feature = "leak"))]
        {
            // Sanity: no-op path returns 0 even after "alloc".
            assert_eq!(t.count(), 0);
            t.record_free(0x1000); // must not panic
        }
    }

    #[test]
    fn untracked_free_is_silent() {
        let t = LeakTracker::new();
        t.record_free(0xDEAD_BEEF);
        assert_eq!(t.count(), 0);
    }

    #[test]
    fn multiple_outstanding_entries() {
        let t = LeakTracker::new();
        t.record_alloc(0x1000, 64);
        t.record_alloc(0x2000, 128);
        t.record_alloc(0x3000, 256);
        #[cfg(feature = "leak")]
        {
            assert_eq!(t.count(), 3);
            let report = t.report();
            assert_eq!(report.len(), 3);
            t.record_free(0x2000);
            assert_eq!(t.count(), 2);
        }
        #[cfg(not(feature = "leak"))]
        {
            assert_eq!(t.count(), 0);
            assert!(t.report().is_empty());
        }
    }

    #[test]
    fn dump_does_not_panic_with_entries() {
        let t = LeakTracker::new();
        t.record_alloc(0x1000, 64);
        t.dump(); // should log, not panic
    }

    #[test]
    fn dump_does_not_panic_when_empty() {
        let t = LeakTracker::new();
        t.dump();
    }
}
