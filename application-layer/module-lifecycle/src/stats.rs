//! [`ModuleLifecycleStats`], [`StatsSnapshot`] — atomic counters and a
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

// ── ModuleLifecycleStats ─────────────────────────────────────────────

/// Atomic counters for the module lifecycle system.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`ModuleLifecycleStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct ModuleLifecycleStats {
    /// Modules currently registered (gauge).
    pub modules_registered: AtomicU64,
    /// Modules that have been initialized (counter).
    pub modules_initialized: AtomicU64,
    /// Modules that have been started (counter).
    pub modules_started: AtomicU64,
    /// Modules that have been stopped (counter).
    pub modules_stopped: AtomicU64,
    /// Modules that have failed (counter).
    pub modules_failed: AtomicU64,
    /// Total time spent in init across all modules (ns).
    pub init_time_ns_total: AtomicU64,
    /// Total time spent in shutdown across all modules (ns).
    pub shutdown_time_ns_total: AtomicU64,
}

impl Default for ModuleLifecycleStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ModuleLifecycleStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            modules_registered: AtomicU64::new(0),
            modules_initialized: AtomicU64::new(0),
            modules_started: AtomicU64::new(0),
            modules_stopped: AtomicU64::new(0),
            modules_failed: AtomicU64::new(0),
            init_time_ns_total: AtomicU64::new(0),
            shutdown_time_ns_total: AtomicU64::new(0),
        }
    }

    /// Record a module registration.
    pub fn record_registered(&self) {
        self.modules_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a module initialization.
    pub fn record_initialized(&self) {
        self.modules_initialized.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a module startup.
    pub fn record_started(&self) {
        self.modules_started.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a module shutdown.
    pub fn record_stopped(&self) {
        self.modules_stopped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a module failure.
    pub fn record_failed(&self) {
        self.modules_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record initialization time (ns).
    pub fn record_init_time(&self, ns: u64) {
        self.init_time_ns_total.fetch_add(ns, Ordering::Relaxed);
    }

    /// Record shutdown time (ns).
    pub fn record_shutdown_time(&self, ns: u64) {
        self.shutdown_time_ns_total.fetch_add(ns, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            modules_registered: self.modules_registered.load(Ordering::Relaxed),
            modules_initialized: self.modules_initialized.load(Ordering::Relaxed),
            modules_started: self.modules_started.load(Ordering::Relaxed),
            modules_stopped: self.modules_stopped.load(Ordering::Relaxed),
            modules_failed: self.modules_failed.load(Ordering::Relaxed),
            init_time_ns_total: self.init_time_ns_total.load(Ordering::Relaxed),
            shutdown_time_ns_total: self.shutdown_time_ns_total.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────

/// Point-in-time snapshot of lifecycle counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Modules currently registered.
    pub modules_registered: u64,
    /// Modules that have been initialized.
    pub modules_initialized: u64,
    /// Modules that have been started.
    pub modules_started: u64,
    /// Modules that have been stopped.
    pub modules_stopped: u64,
    /// Modules that have failed.
    pub modules_failed: u64,
    /// Total initialization time (ns).
    pub init_time_ns_total: u64,
    /// Total shutdown time (ns).
    pub shutdown_time_ns_total: u64,
}

impl StatsSnapshot {
    /// Failure rate in basis points (0..=10_000).
    /// Returns 0 when there are no completed operations.
    #[must_use]
    pub const fn failure_rate_bps(&self) -> u16 {
        let completed = self.modules_initialized + self.modules_started + self.modules_stopped;
        if completed == 0 {
            return 0;
        }
        let rate = (self.modules_failed * 10_000) / completed;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Average initialization time in ns.
    /// Returns 0 when no modules have been initialized.
    #[must_use]
    pub const fn avg_init_time_ns(&self) -> u64 {
        if self.modules_initialized == 0 {
            return 0;
        }
        self.init_time_ns_total / self.modules_initialized
    }

    /// Average shutdown time in ns.
    /// Returns 0 when no modules have been stopped.
    #[must_use]
    pub const fn avg_shutdown_time_ns(&self) -> u64 {
        if self.modules_stopped == 0 {
            return 0;
        }
        self.shutdown_time_ns_total / self.modules_stopped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = ModuleLifecycleStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_registered_increments_counter() {
        let s = ModuleLifecycleStats::new();
        s.record_registered();
        s.record_registered();
        let snap = s.snapshot();
        assert_eq!(snap.modules_registered, 2);
    }

    #[test]
    fn record_initialized_increments_counter() {
        let s = ModuleLifecycleStats::new();
        s.record_initialized();
        s.record_initialized();
        s.record_initialized();
        let snap = s.snapshot();
        assert_eq!(snap.modules_initialized, 3);
    }

    #[test]
    fn record_started_increments_counter() {
        let s = ModuleLifecycleStats::new();
        s.record_started();
        let snap = s.snapshot();
        assert_eq!(snap.modules_started, 1);
    }

    #[test]
    fn record_stopped_increments_counter() {
        let s = ModuleLifecycleStats::new();
        s.record_stopped();
        s.record_stopped();
        let snap = s.snapshot();
        assert_eq!(snap.modules_stopped, 2);
    }

    #[test]
    fn record_failed_increments_counter() {
        let s = ModuleLifecycleStats::new();
        s.record_failed();
        let snap = s.snapshot();
        assert_eq!(snap.modules_failed, 1);
    }

    #[test]
    fn record_init_time_accumulates() {
        let s = ModuleLifecycleStats::new();
        s.record_init_time(100);
        s.record_init_time(200);
        s.record_init_time(300);
        assert_eq!(s.snapshot().init_time_ns_total, 600);
    }

    #[test]
    fn record_shutdown_time_accumulates() {
        let s = ModuleLifecycleStats::new();
        s.record_shutdown_time(1000);
        s.record_shutdown_time(2000);
        assert_eq!(s.snapshot().shutdown_time_ns_total, 3000);
    }

    #[test]
    fn failure_rate_is_zero_with_no_completions() {
        let s = ModuleLifecycleStats::new();
        assert_eq!(s.snapshot().failure_rate_bps(), 0);
    }

    #[test]
    fn failure_rate_calculates_correctly() {
        let mut snap = StatsSnapshot::default();
        snap.modules_initialized = 90;
        snap.modules_started = 8;
        snap.modules_stopped = 2;
        snap.modules_failed = 5; // 5 / 100 = 5% = 500 bps.
        assert_eq!(snap.failure_rate_bps(), 500);
    }

    #[test]
    fn failure_rate_clamps_at_max() {
        let mut snap = StatsSnapshot::default();
        snap.modules_initialized = 1;
        snap.modules_failed = 200_000; // Would overflow u16.
        assert_eq!(snap.failure_rate_bps(), u16::MAX);
    }

    #[test]
    fn avg_init_time_is_zero_with_no_inits() {
        let s = ModuleLifecycleStats::new();
        assert_eq!(s.snapshot().avg_init_time_ns(), 0);
    }

    #[test]
    fn avg_init_time_calculates_correctly() {
        let mut snap = StatsSnapshot::default();
        snap.modules_initialized = 3;
        snap.init_time_ns_total = 6000; // 6000 / 3 = 2000.
        assert_eq!(snap.avg_init_time_ns(), 2000);
    }

    #[test]
    fn avg_shutdown_time_is_zero_with_no_stops() {
        let s = ModuleLifecycleStats::new();
        assert_eq!(s.snapshot().avg_shutdown_time_ns(), 0);
    }

    #[test]
    fn avg_shutdown_time_calculates_correctly() {
        let mut snap = StatsSnapshot::default();
        snap.modules_stopped = 4;
        snap.shutdown_time_ns_total = 8000; // 8000 / 4 = 2000.
        assert_eq!(snap.avg_shutdown_time_ns(), 2000);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = ModuleLifecycleStats::new();
        s.record_initialized();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_is_copy() {
        let s = ModuleLifecycleStats::new();
        let snap1 = s.snapshot();
        let snap2 = snap1;
        assert_eq!(snap1.modules_initialized, snap2.modules_initialized);
    }
}
