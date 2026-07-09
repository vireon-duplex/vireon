//! [`GracefulStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput.

use std::sync::atomic::{AtomicU64, Ordering};

// ── GracefulStats ───────────────────────────────────────────────

/// Atomic counters for the graceful startup and shutdown system.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`GracefulStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct GracefulStats {
    // ── Startup lifecycle ───────────────────────────────────────
    /// Total startup attempts (counter).
    pub startups_total: AtomicU64,
    /// Startups that completed successfully (counter).
    pub startups_completed: AtomicU64,
    /// Startups that failed (counter).
    pub startups_failed: AtomicU64,

    // ── Shutdown lifecycle ──────────────────────────────────────
    /// Total shutdown attempts (counter).
    pub shutdowns_total: AtomicU64,
    /// Shutdowns that completed successfully (counter).
    pub shutdowns_completed: AtomicU64,
    /// Shutdowns that were forced (counter).
    pub shutdowns_forced: AtomicU64,

    // ── Connection draining ──────────────────────────────────────
    /// Connections drained during shutdown (counter).
    pub connections_drained: AtomicU64,

    // ── Timing ─────────────────────────────────────────────────
    /// Total startup time across all startups (counter).
    pub total_startup_time_ns: AtomicU64,
    /// Total shutdown time across all shutdowns (counter).
    pub total_shutdown_time_ns: AtomicU64,
}

impl Default for GracefulStats {
    fn default() -> Self {
        Self::new()
    }
}

impl GracefulStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            startups_total: AtomicU64::new(0),
            startups_completed: AtomicU64::new(0),
            startups_failed: AtomicU64::new(0),
            shutdowns_total: AtomicU64::new(0),
            shutdowns_completed: AtomicU64::new(0),
            shutdowns_forced: AtomicU64::new(0),
            connections_drained: AtomicU64::new(0),
            total_startup_time_ns: AtomicU64::new(0),
            total_shutdown_time_ns: AtomicU64::new(0),
        }
    }

    /// Record a startup attempt.
    pub fn record_startup_attempt(&self) {
        self.startups_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful startup completion.
    ///
    /// # Arguments
    /// * `duration_ns` - Duration of the startup in nanoseconds.
    pub fn record_startup_completed(&self, duration_ns: u64) {
        self.startups_completed.fetch_add(1, Ordering::Relaxed);
        self.total_startup_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    /// Record a startup failure.
    pub fn record_startup_failed(&self) {
        self.startups_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a shutdown attempt.
    pub fn record_shutdown_attempt(&self) {
        self.shutdowns_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful shutdown completion.
    ///
    /// # Arguments
    /// * `duration_ns` - Duration of the shutdown in nanoseconds.
    pub fn record_shutdown_completed(&self, duration_ns: u64) {
        self.shutdowns_completed.fetch_add(1, Ordering::Relaxed);
        self.total_shutdown_time_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    /// Record a forced shutdown.
    pub fn record_shutdown_forced(&self) {
        self.shutdowns_forced.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connections drained.
    ///
    /// # Arguments
    /// * `count` - Number of connections drained.
    pub fn record_connections_drained(&self, count: u64) {
        self.connections_drained.fetch_add(count, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            startups_total: self.startups_total.load(Ordering::Relaxed),
            startups_completed: self.startups_completed.load(Ordering::Relaxed),
            startups_failed: self.startups_failed.load(Ordering::Relaxed),
            shutdowns_total: self.shutdowns_total.load(Ordering::Relaxed),
            shutdowns_completed: self.shutdowns_completed.load(Ordering::Relaxed),
            shutdowns_forced: self.shutdowns_forced.load(Ordering::Relaxed),
            connections_drained: self.connections_drained.load(Ordering::Relaxed),
            total_startup_time_ns: self.total_startup_time_ns.load(Ordering::Relaxed),
            total_shutdown_time_ns: self.total_shutdown_time_ns.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of graceful startup/shutdown counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total startup attempts.
    pub startups_total: u64,
    /// Startups that completed successfully.
    pub startups_completed: u64,
    /// Startups that failed.
    pub startups_failed: u64,
    /// Total shutdown attempts.
    pub shutdowns_total: u64,
    /// Shutdowns that completed successfully.
    pub shutdowns_completed: u64,
    /// Shutdowns that were forced.
    pub shutdowns_forced: u64,
    /// Connections drained during shutdown.
    pub connections_drained: u64,
    /// Total startup time across all startups.
    pub total_startup_time_ns: u64,
    /// Total shutdown time across all shutdowns.
    pub total_shutdown_time_ns: u64,
}

impl StatsSnapshot {
    /// Startup failure rate in basis points (0..=10_000).
    /// Returns 0 if there were no startups.
    #[must_use]
    pub const fn startup_failure_rate_bps(&self) -> u16 {
        if self.startups_total == 0 {
            return 0;
        }
        let rate = (self.startups_failed * 10_000) / self.startups_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Forced shutdown rate in basis points (0..=10_000).
    /// Returns 0 if there were no shutdowns.
    #[must_use]
    pub const fn forced_shutdown_rate_bps(&self) -> u16 {
        if self.shutdowns_total == 0 {
            return 0;
        }
        let rate = (self.shutdowns_forced * 10_000) / self.shutdowns_total;
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
        let s = GracefulStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_startup_attempt_increments_total() {
        let s = GracefulStats::new();
        s.record_startup_attempt();
        s.record_startup_attempt();
        let snap = s.snapshot();
        assert_eq!(snap.startups_total, 2);
        assert_eq!(snap.startups_completed, 0);
        assert_eq!(snap.startups_failed, 0);
    }

    #[test]
    fn record_startup_completed_increments_completed_and_time() {
        let s = GracefulStats::new();
        s.record_startup_completed(1_500_000);
        let snap = s.snapshot();
        assert_eq!(snap.startups_completed, 1);
        assert_eq!(snap.total_startup_time_ns, 1_500_000);
    }

    #[test]
    fn record_startup_failed_increments_failed() {
        let s = GracefulStats::new();
        s.record_startup_failed();
        s.record_startup_failed();
        let snap = s.snapshot();
        assert_eq!(snap.startups_failed, 2);
    }

    #[test]
    fn record_shutdown_attempt_increments_total() {
        let s = GracefulStats::new();
        s.record_shutdown_attempt();
        let snap = s.snapshot();
        assert_eq!(snap.shutdowns_total, 1);
    }

    #[test]
    fn record_shutdown_completed_increments_completed_and_time() {
        let s = GracefulStats::new();
        s.record_shutdown_completed(2_000_000);
        let snap = s.snapshot();
        assert_eq!(snap.shutdowns_completed, 1);
        assert_eq!(snap.total_shutdown_time_ns, 2_000_000);
    }

    #[test]
    fn record_shutdown_forced_increments_forced() {
        let s = GracefulStats::new();
        s.record_shutdown_forced();
        let snap = s.snapshot();
        assert_eq!(snap.shutdowns_forced, 1);
    }

    #[test]
    fn record_connections_drained_adds_to_counter() {
        let s = GracefulStats::new();
        s.record_connections_drained(10);
        s.record_connections_drained(20);
        let snap = s.snapshot();
        assert_eq!(snap.connections_drained, 30);
    }

    #[test]
    fn startup_failure_rate_with_no_startups_is_zero() {
        let s = GracefulStats::new();
        assert_eq!(s.snapshot().startup_failure_rate_bps(), 0);
    }

    #[test]
    fn startup_failure_rate_with_failures() {
        let s = GracefulStats::new();
        s.startups_total.store(100, Ordering::Relaxed);
        s.startups_failed.store(5, Ordering::Relaxed);
        // 5/100 = 5% = 500 bps.
        assert_eq!(s.snapshot().startup_failure_rate_bps(), 500);
    }

    #[test]
    fn forced_shutdown_rate_with_no_shutdowns_is_zero() {
        let s = GracefulStats::new();
        assert_eq!(s.snapshot().forced_shutdown_rate_bps(), 0);
    }

    #[test]
    fn forced_shutdown_rate_with_forced() {
        let s = GracefulStats::new();
        s.shutdowns_total.store(50, Ordering::Relaxed);
        s.shutdowns_forced.store(2, Ordering::Relaxed);
        // 2/50 = 4% = 400 bps.
        assert_eq!(s.snapshot().forced_shutdown_rate_bps(), 400);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = GracefulStats::new();
        s.record_startup_attempt();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn failure_rate_clamps_to_u16_max() {
        let s = GracefulStats::new();
        s.startups_total.store(1, Ordering::Relaxed);
        s.startups_failed
            .store(u16::MAX as u64 + 1000, Ordering::Relaxed);
        assert_eq!(s.snapshot().startup_failure_rate_bps(), u16::MAX);
    }

    #[test]
    fn default_snapshot_is_zero() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.startups_total, 0);
        assert_eq!(snap.startups_completed, 0);
        assert_eq!(snap.startups_failed, 0);
        assert_eq!(snap.shutdowns_total, 0);
        assert_eq!(snap.shutdowns_completed, 0);
        assert_eq!(snap.shutdowns_forced, 0);
        assert_eq!(snap.connections_drained, 0);
        assert_eq!(snap.total_startup_time_ns, 0);
        assert_eq!(snap.total_shutdown_time_ns, 0);
    }
}
