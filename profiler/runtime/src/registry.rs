//! Global registry of per-function statistics.
//!
//! [`GLOBAL_REGISTRY`] is a `LazyLock<RwLock<FxHashMap>>`. The write
//! lock is taken only when a new function name is registered for the
//! first time. All subsequent flushes take a read lock and merge via
//! atomic operations — no contention.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use crate::stats::{FunctionSnapshot, FunctionStats, LocalEntry};

type StatsMap = HashMap<&'static str, Box<FunctionStats>>;

static GLOBAL_REGISTRY: LazyLock<RwLock<StatsMap>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Merges a thread-local entry into the global stats for `name`.
///
/// Uses a read lock on the fast path (function already registered).
/// Falls back to a write lock only for the very first observation of
/// a new function name.
pub fn merge(name: &'static str, local: &LocalEntry) {
    // Fast path: function already registered.
    {
        let read = GLOBAL_REGISTRY
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(stats) = read.get(name) {
            stats.merge_local(local);
            return;
        }
    }
    // Slow path: register new function.
    {
        let mut write = GLOBAL_REGISTRY
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let stats = write
            .entry(name)
            .or_insert_with(|| Box::new(FunctionStats::new(name)));
        stats.merge_local(local);
    }
}

/// Takes a snapshot of every registered function.
///
/// Returns a `Vec` sorted by total time descending (most expensive first).
#[must_use]
pub fn snapshot_all() -> Vec<FunctionSnapshot> {
    let read = GLOBAL_REGISTRY
        .read()
        .unwrap_or_else(|e| e.into_inner());
    let mut snapshots: Vec<FunctionSnapshot> =
        read.values().map(|s| s.snapshot()).collect();
    snapshots.sort_by(|a, b| b.total_ns.cmp(&a.total_ns));
    snapshots
}

/// Resets every function's stats to zero.
pub fn reset_all() {
    let read = GLOBAL_REGISTRY
        .read()
        .unwrap_or_else(|e| e.into_inner());
    for stats in read.values() {
        stats.reset();
    }
}

/// Removes all function entries from the registry.
pub fn clear_all() {
    GLOBAL_REGISTRY
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
}

/// Number of registered functions.
#[must_use]
pub fn count() -> usize {
    GLOBAL_REGISTRY
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .len()
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::Mutex;

    // Serialize registry tests — they share global state.
    static TEST_LOCK: Mutex<()> = Mutex::new(());
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn merge_registers_new_function() {
        let _g = lock();
        clear_all();
        let mut local = LocalEntry::new();
        local.record(100);
        merge("test_merge_registers", &local);
        assert_eq!(count(), 1);
    }

    #[test]
    fn merge_accumulates_multiple_calls() {
        let _g = lock();
        clear_all();
        for i in 0..10 {
            let mut local = LocalEntry::new();
            local.record(i * 10 + 10);
            merge("test_merge_accumulate", &local);
        }
        let snaps = snapshot_all();
        let snap = snaps
            .iter()
            .find(|s| s.name == "test_merge_accumulate")
            .unwrap();
        assert_eq!(snap.calls, 10);
        assert_eq!(snap.min_ns, 10);
        assert_eq!(snap.max_ns, 100);
    }

    #[test]
    fn snapshot_all_sorted_by_total() {
        let _g = lock();
        clear_all();

        let mut local_a = LocalEntry::new();
        local_a.record(1000);
        merge("func_a", &local_a);

        let mut local_b = LocalEntry::new();
        local_b.record(10);
        merge("func_b", &local_b);

        let snaps = snapshot_all();
        assert_eq!(snaps.len(), 2);
        // func_a should be first (higher total_ns).
        assert_eq!(snaps[0].name, "func_a");
    }

    #[test]
    fn reset_all_clears() {
        let _g = lock();
        let mut local = LocalEntry::new();
        local.record(100);
        merge("test_reset_all", &local);
        assert!(count() > 0);

        clear_all();
        let snaps = snapshot_all();
        for s in &snaps {
            assert_eq!(s.calls, 0);
        }
    }

    #[test]
    fn merge_multiple_functions() {
        let _g = lock();
        clear_all();
        let mut local1 = LocalEntry::new();
        local1.record(10);
        merge("merge_fn1", &local1);

        let mut local2 = LocalEntry::new();
        local2.record(20);
        merge("merge_fn2", &local2);

        let snaps = snapshot_all();
        assert!(snaps.iter().any(|s| s.name == "merge_fn1"));
        assert!(snaps.iter().any(|s| s.name == "merge_fn2"));
    }
}
