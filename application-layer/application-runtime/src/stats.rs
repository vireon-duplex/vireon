//! [`AppRuntimeStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for the application runtime.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand and the snapshot is what gets exported to metrics.
//!
//! # Mapping to metrics
//!
//! Each counter maps 1:1 to a Prometheus counter or gauge in
//! Phase 2+:
//!
//! | Counter | Prometheus type | Name |
//! |---------|-----------------|------|
//! | `modules_loaded` | gauge | `app_runtime_modules_loaded` |
//! | `modules_started` | gauge | `app_runtime_modules_started` |
//! | `modules_failed` | counter | `app_runtime_modules_failed_total` |
//! | `startups_total` | counter | `app_runtime_startups_total` |
//! | `shutdowns_total` | counter | `app_runtime_shutdowns_total` |
//! | `health_checks_total` | counter | `app_runtime_health_checks_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── AppRuntimeStats ─────────────────────────────────────────────

/// Atomic counters for the application runtime.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`AppRuntimeStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct AppRuntimeStats {
    /// Modules currently loaded (gauge).
    pub modules_loaded: AtomicU64,
    /// Modules currently started (gauge).
    pub modules_started: AtomicU64,
    /// Modules that failed since startup (counter).
    pub modules_failed: AtomicU64,
    /// Total startup attempts since process start (counter).
    pub startups_total: AtomicU64,
    /// Total shutdown attempts since process start (counter).
    pub shutdowns_total: AtomicU64,
    /// Total health checks performed (counter).
    pub health_checks_total: AtomicU64,
}

impl Default for AppRuntimeStats {
    fn default() -> Self {
        Self::new()
    }
}

impl AppRuntimeStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            modules_loaded: AtomicU64::new(0),
            modules_started: AtomicU64::new(0),
            modules_failed: AtomicU64::new(0),
            startups_total: AtomicU64::new(0),
            shutdowns_total: AtomicU64::new(0),
            health_checks_total: AtomicU64::new(0),
        }
    }

    /// Record a module being loaded.
    pub fn record_module_loaded(&self) {
        self.modules_loaded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a module being started.
    pub fn record_module_started(&self) {
        self.modules_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a module failure.
    pub fn record_module_failure(&self) {
        self.modules_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a startup attempt.
    pub fn record_startup(&self) {
        self.startups_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a shutdown attempt.
    pub fn record_shutdown(&self) {
        self.shutdowns_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a health check.
    pub fn record_health_check(&self) {
        self.health_checks_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            modules_loaded: self.modules_loaded.load(Ordering::Relaxed),
            modules_started: self.modules_started.load(Ordering::Relaxed),
            modules_failed: self.modules_failed.load(Ordering::Relaxed),
            startups_total: self.startups_total.load(Ordering::Relaxed),
            shutdowns_total: self.shutdowns_total.load(Ordering::Relaxed),
            health_checks_total: self.health_checks_total.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of application runtime counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Modules currently loaded.
    pub modules_loaded: u64,
    /// Modules currently started.
    pub modules_started: u64,
    /// Modules that failed since startup.
    pub modules_failed: u64,
    /// Total startup attempts since process start.
    pub startups_total: u64,
    /// Total shutdown attempts since process start.
    pub shutdowns_total: u64,
    /// Total health checks performed.
    pub health_checks_total: u64,
}

impl StatsSnapshot {
    /// Startup success rate in basis points (0..=10_000).
    ///
    /// Returns 10_000 (100%) when there are no startups yet, since
    /// an idle system is considered healthy.
    ///
    /// Success rate = `startups_total - modules_failed` / `startups_total`.
    #[must_use]
    pub const fn startup_success_rate_bps(&self) -> u16 {
        if self.startups_total == 0 {
            return 10_000;
        }
        let succeeded = self.startups_total.saturating_sub(self.modules_failed);
        let rate = succeeded.saturating_mul(10_000) / self.startups_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Module failure rate in basis points (0..=10_000).
    ///
    /// Returns 0 when there are no modules loaded yet.
    #[must_use]
    pub const fn module_failure_rate_bps(&self) -> u16 {
        if self.modules_loaded == 0 {
            return 0;
        }
        let rate = self.modules_failed.saturating_mul(10_000) / self.modules_loaded;
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
        let s = AppRuntimeStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_module_loaded_increments_counter() {
        let s = AppRuntimeStats::new();
        s.record_module_loaded();
        s.record_module_loaded();
        assert_eq!(s.snapshot().modules_loaded, 2);
    }

    #[test]
    fn record_module_started_increments_counter() {
        let s = AppRuntimeStats::new();
        s.record_module_started();
        s.record_module_started();
        s.record_module_started();
        assert_eq!(s.snapshot().modules_started, 3);
    }

    #[test]
    fn record_module_failure_increments_counter() {
        let s = AppRuntimeStats::new();
        s.record_module_failure();
        assert_eq!(s.snapshot().modules_failed, 1);
    }

    #[test]
    fn record_startup_increments_counter() {
        let s = AppRuntimeStats::new();
        s.record_startup();
        s.record_startup();
        assert_eq!(s.snapshot().startups_total, 2);
    }

    #[test]
    fn record_shutdown_increments_counter() {
        let s = AppRuntimeStats::new();
        s.record_shutdown();
        assert_eq!(s.snapshot().shutdowns_total, 1);
    }

    #[test]
    fn record_health_check_increments_counter() {
        let s = AppRuntimeStats::new();
        s.record_health_check();
        s.record_health_check();
        s.record_health_check();
        assert_eq!(s.snapshot().health_checks_total, 3);
    }

    #[test]
    fn success_rate_with_no_startups_is_max() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.startup_success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_all_succeeded() {
        let s = AppRuntimeStats::new();
        for _ in 0..100 {
            s.record_startup();
        }
        assert_eq!(s.snapshot().startup_success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_mixed_results() {
        // 97 succeeded out of 100 startups = 97% = 9700 bps.
        let s = AppRuntimeStats::new();
        for _ in 0..100 {
            s.record_startup();
        }
        for _ in 0..3 {
            s.record_module_failure();
        }
        // (100 - 3) * 10000 / 100 = 9700
        assert_eq!(s.snapshot().startup_success_rate_bps(), 9700);
    }

    #[test]
    fn success_rate_truncates_not_rounds() {
        // 2 succeeded out of 3 startups = 6666 bps (truncated).
        let s = AppRuntimeStats::new();
        s.record_startup();
        s.record_startup();
        s.record_startup();
        s.record_module_failure();
        // (3 - 1) * 10000 / 3 = 20000 / 3 = 6666
        assert_eq!(s.snapshot().startup_success_rate_bps(), 6666);
    }

    #[test]
    fn failure_rate_with_no_modules_is_zero() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.module_failure_rate_bps(), 0);
    }

    #[test]
    fn failure_rate_mixed_results() {
        let snap = StatsSnapshot {
            modules_loaded: 100,
            modules_failed: 5,
            ..StatsSnapshot::default()
        };
        // 5 * 10000 / 100 = 500
        assert_eq!(snap.module_failure_rate_bps(), 500);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = AppRuntimeStats::new();
        s.record_startup();
        s.record_module_loaded();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }
}
