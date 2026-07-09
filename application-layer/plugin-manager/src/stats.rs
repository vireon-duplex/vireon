//! [`PluginManagerStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand and the snapshot is what gets exported to metrics.

use std::sync::atomic::{AtomicU64, Ordering};

// ── PluginManagerStats ─────────────────────────────────────────────

/// Atomic counters for the plugin manager.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`PluginManagerStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct PluginManagerStats {
    // ── Plugin lifecycle ───────────────────────────────────────
    /// Plugins currently loaded (gauge).
    pub plugins_loaded: AtomicU64,
    /// Plugins unloaded since startup (counter).
    pub plugins_unloaded: AtomicU64,
    /// Plugin load failures since startup (counter).
    pub load_failures: AtomicU64,
    /// Plugin reloads (hot reload) since startup (counter).
    pub reloads_total: AtomicU64,

    // ── Compatibility & dependencies ────────────────────────────
    /// API version mismatches detected (counter).
    pub api_mismatches: AtomicU64,
    /// Dependency resolution failures (counter).
    pub dependency_errors: AtomicU64,
}

impl Default for PluginManagerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginManagerStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            plugins_loaded: AtomicU64::new(0),
            plugins_unloaded: AtomicU64::new(0),
            load_failures: AtomicU64::new(0),
            reloads_total: AtomicU64::new(0),
            api_mismatches: AtomicU64::new(0),
            dependency_errors: AtomicU64::new(0),
        }
    }

    /// Record a plugin load.
    pub fn record_plugin_loaded(&self) {
        self.plugins_loaded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a plugin unload.
    pub fn record_plugin_unloaded(&self) {
        self.plugins_unloaded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a plugin load failure.
    pub fn record_load_failure(&self) {
        self.load_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a plugin reload (hot reload).
    pub fn record_reload(&self) {
        self.reloads_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an API version mismatch.
    pub fn record_api_mismatch(&self) {
        self.api_mismatches.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a dependency resolution error.
    pub fn record_dependency_error(&self) {
        self.dependency_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            plugins_loaded: self.plugins_loaded.load(Ordering::Relaxed),
            plugins_unloaded: self.plugins_unloaded.load(Ordering::Relaxed),
            load_failures: self.load_failures.load(Ordering::Relaxed),
            reloads_total: self.reloads_total.load(Ordering::Relaxed),
            api_mismatches: self.api_mismatches.load(Ordering::Relaxed),
            dependency_errors: self.dependency_errors.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of plugin manager counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Plugins currently loaded.
    pub plugins_loaded: u64,
    /// Plugins unloaded since startup.
    pub plugins_unloaded: u64,
    /// Plugin load failures since startup.
    pub load_failures: u64,
    /// Plugin reloads since startup.
    pub reloads_total: u64,
    /// API version mismatches detected.
    pub api_mismatches: u64,
    /// Dependency resolution failures.
    pub dependency_errors: u64,
}

impl StatsSnapshot {
    /// Load failure rate in basis points (0..=10_000).
    /// `load_failures / (plugins_loaded + load_failures)`.
    /// Returns 0 if no load attempts yet.
    #[must_use]
    pub const fn load_failure_rate_bps(&self) -> u16 {
        let attempts = self.plugins_loaded.saturating_add(self.load_failures);
        if attempts == 0 {
            return 0;
        }
        let rate = (self.load_failures * 10_000) / attempts;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = PluginManagerStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_plugin_loaded_increments_counter() {
        let s = PluginManagerStats::new();
        s.record_plugin_loaded();
        s.record_plugin_loaded();
        let snap = s.snapshot();
        assert_eq!(snap.plugins_loaded, 2);
    }

    #[test]
    fn record_plugin_unloaded_increments_counter() {
        let s = PluginManagerStats::new();
        s.record_plugin_unloaded();
        let snap = s.snapshot();
        assert_eq!(snap.plugins_unloaded, 1);
    }

    #[test]
    fn record_load_failure_increments_counter() {
        let s = PluginManagerStats::new();
        s.record_load_failure();
        s.record_load_failure();
        s.record_load_failure();
        let snap = s.snapshot();
        assert_eq!(snap.load_failures, 3);
    }

    #[test]
    fn record_reload_increments_counter() {
        let s = PluginManagerStats::new();
        s.record_reload();
        let snap = s.snapshot();
        assert_eq!(snap.reloads_total, 1);
    }

    #[test]
    fn record_api_mismatch_increments_counter() {
        let s = PluginManagerStats::new();
        s.record_api_mismatch();
        let snap = s.snapshot();
        assert_eq!(snap.api_mismatches, 1);
    }

    #[test]
    fn record_dependency_error_increments_counter() {
        let s = PluginManagerStats::new();
        s.record_dependency_error();
        let snap = s.snapshot();
        assert_eq!(snap.dependency_errors, 1);
    }

    #[test]
    fn load_failure_rate_with_no_attempts_is_zero() {
        let s = PluginManagerStats::new();
        assert_eq!(s.snapshot().load_failure_rate_bps(), 0);
    }

    #[test]
    fn load_failure_rate_with_all_successes() {
        let s = PluginManagerStats::new();
        s.record_plugin_loaded();
        s.record_plugin_loaded();
        s.record_plugin_loaded();
        // 0 failures / 3 attempts = 0%.
        assert_eq!(s.snapshot().load_failure_rate_bps(), 0);
    }

    #[test]
    fn load_failure_rate_with_failures() {
        let s = PluginManagerStats::new();
        s.record_plugin_loaded();
        s.record_plugin_loaded();
        s.record_load_failure(); // 1 failure / 3 attempts = 33.33%
        let rate = s.snapshot().load_failure_rate_bps();
        // 3333 bps ≈ 33.33%
        assert!(rate >= 3300 && rate <= 3400);
    }

    #[test]
    fn load_failure_rate_with_all_failures() {
        let s = PluginManagerStats::new();
        s.record_load_failure();
        s.record_load_failure();
        // 2 failures / 2 attempts = 100% = 10_000 bps.
        assert_eq!(s.snapshot().load_failure_rate_bps(), 10_000);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = PluginManagerStats::new();
        s.record_plugin_loaded();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }
}
