//! [`ContainerStats`], [`StatsSnapshot`] — atomic counters and a
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

// ── ContainerStats ───────────────────────────────────────────────

/// Atomic counters for the dependency injection container.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`ContainerStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct ContainerStats {
    /// Total services ever registered (counter; never decreases).
    pub registrations_total: AtomicU64,
    /// Total resolution attempts (counter).
    pub resolutions_total: AtomicU64,
    /// Resolutions that succeeded (counter).
    pub resolutions_succeeded: AtomicU64,
    /// Resolutions that failed (counter).
    pub resolutions_failed: AtomicU64,
    /// Singleton services currently active (gauge).
    pub singletons_active: AtomicU64,
    /// Scoped services currently active (gauge).
    pub scopes_active: AtomicU64,
    /// Circular dependency errors detected (counter).
    pub circular_errors: AtomicU64,
}

impl Default for ContainerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainerStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            registrations_total: AtomicU64::new(0),
            resolutions_total: AtomicU64::new(0),
            resolutions_succeeded: AtomicU64::new(0),
            resolutions_failed: AtomicU64::new(0),
            singletons_active: AtomicU64::new(0),
            scopes_active: AtomicU64::new(0),
            circular_errors: AtomicU64::new(0),
        }
    }

    /// Record a service registration.
    pub fn record_registration(&self) {
        self.registrations_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a resolution attempt.
    pub fn record_resolution_attempt(&self) {
        self.resolutions_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful resolution.
    pub fn record_resolution_success(&self) {
        self.resolutions_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed resolution.
    pub fn record_resolution_failure(&self) {
        self.resolutions_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a singleton becoming active.
    pub fn record_singleton_activated(&self) {
        self.singletons_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a singleton being disposed.
    pub fn record_singleton_disposed(&self) {
        self.singletons_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a scope being created.
    pub fn record_scope_created(&self) {
        self.scopes_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a scope being disposed.
    pub fn record_scope_disposed(&self) {
        self.scopes_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a circular dependency error.
    pub fn record_circular_error(&self) {
        self.circular_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields (e.g. `resolutions_total`
    /// could exceed `resolutions_succeeded + resolutions_failed` by 1
    /// if a resolution is in flight). This is acceptable for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            registrations_total: self.registrations_total.load(Ordering::Relaxed),
            resolutions_total: self.resolutions_total.load(Ordering::Relaxed),
            resolutions_succeeded: self.resolutions_succeeded.load(Ordering::Relaxed),
            resolutions_failed: self.resolutions_failed.load(Ordering::Relaxed),
            singletons_active: self.singletons_active.load(Ordering::Relaxed),
            scopes_active: self.scopes_active.load(Ordering::Relaxed),
            circular_errors: self.circular_errors.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of container counters.
///
/// Rates are precomputed as basis-points (bps, 1/100 of a percent)
/// so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total services ever registered.
    pub registrations_total: u64,
    /// Total resolution attempts.
    pub resolutions_total: u64,
    /// Resolutions that succeeded.
    pub resolutions_succeeded: u64,
    /// Resolutions that failed.
    pub resolutions_failed: u64,
    /// Singleton services currently active.
    pub singletons_active: u64,
    /// Scoped services currently active.
    pub scopes_active: u64,
    /// Circular dependency errors detected.
    pub circular_errors: u64,
}

impl StatsSnapshot {
    /// Resolution failure rate in basis points (0..=10_000).
    /// Returns 0 when there are no resolutions yet.
    #[must_use]
    pub const fn resolution_failure_rate_bps(&self) -> u16 {
        if self.resolutions_total == 0 {
            return 0;
        }
        // failed * 10_000 / total, clamped to u16.
        let rate = (self.resolutions_failed * 10_000) / self.resolutions_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Resolution success rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no resolutions yet.
    #[must_use]
    pub const fn resolution_success_rate_bps(&self) -> u16 {
        if self.resolutions_total == 0 {
            return 10_000;
        }
        let rate = (self.resolutions_succeeded * 10_000) / self.resolutions_total;
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
        let s = ContainerStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_registration_increments_counter() {
        let s = ContainerStats::new();
        s.record_registration();
        s.record_registration();
        let snap = s.snapshot();
        assert_eq!(snap.registrations_total, 2);
    }

    #[test]
    fn record_resolution_attempt_increments_total() {
        let s = ContainerStats::new();
        s.record_resolution_attempt();
        s.record_resolution_attempt();
        let snap = s.snapshot();
        assert_eq!(snap.resolutions_total, 2);
    }

    #[test]
    fn record_resolution_success() {
        let s = ContainerStats::new();
        s.record_resolution_attempt();
        s.record_resolution_success();
        let snap = s.snapshot();
        assert_eq!(snap.resolutions_succeeded, 1);
    }

    #[test]
    fn record_resolution_failure() {
        let s = ContainerStats::new();
        s.record_resolution_attempt();
        s.record_resolution_failure();
        let snap = s.snapshot();
        assert_eq!(snap.resolutions_failed, 1);
    }

    #[test]
    fn singleton_activation_disposal() {
        let s = ContainerStats::new();
        s.record_singleton_activated();
        s.record_singleton_activated();
        s.record_singleton_disposed();
        let snap = s.snapshot();
        assert_eq!(snap.singletons_active, 1);
    }

    #[test]
    fn scope_creation_disposal() {
        let s = ContainerStats::new();
        s.record_scope_created();
        s.record_scope_created();
        s.record_scope_disposed();
        let snap = s.snapshot();
        assert_eq!(snap.scopes_active, 1);
    }

    #[test]
    fn circular_error_counter() {
        let s = ContainerStats::new();
        s.record_circular_error();
        s.record_circular_error();
        assert_eq!(s.snapshot().circular_errors, 2);
    }

    #[test]
    fn failure_rate_is_zero_with_no_resolutions() {
        let s = ContainerStats::new();
        assert_eq!(s.snapshot().resolution_failure_rate_bps(), 0);
    }

    #[test]
    fn failure_rate_reflects_mixed_results() {
        let s = ContainerStats::new();
        for _ in 0..7 {
            s.record_resolution_attempt();
            s.record_resolution_success();
        }
        for _ in 0..3 {
            s.record_resolution_attempt();
            s.record_resolution_failure();
        }
        // 3/10 = 30% = 3000 bps.
        assert_eq!(s.snapshot().resolution_failure_rate_bps(), 3_000);
    }

    #[test]
    fn success_rate_is_100_pct_with_no_resolutions() {
        let s = ContainerStats::new();
        // No resolutions → 100% by convention.
        assert_eq!(s.snapshot().resolution_success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_reflects_mixed_results() {
        let s = ContainerStats::new();
        for _ in 0..7 {
            s.record_resolution_attempt();
            s.record_resolution_success();
        }
        for _ in 0..3 {
            s.record_resolution_attempt();
            s.record_resolution_failure();
        }
        // 7/10 = 70% = 7000 bps.
        assert_eq!(s.snapshot().resolution_success_rate_bps(), 7_000);
    }

    #[test]
    fn rates_sum_to_100_percent() {
        let s = ContainerStats::new();
        for _ in 0..85 {
            s.record_resolution_attempt();
            s.record_resolution_success();
        }
        for _ in 0..15 {
            s.record_resolution_attempt();
            s.record_resolution_failure();
        }

        let snap = s.snapshot();
        let total =
            snap.resolution_success_rate_bps() as u32 + snap.resolution_failure_rate_bps() as u32;
        assert_eq!(total, 10_000);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = ContainerStats::new();
        s.record_registration();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_captures_all_counters() {
        let s = ContainerStats::new();
        s.record_registration();
        s.record_resolution_attempt();
        s.record_resolution_success();
        s.record_singleton_activated();
        s.record_scope_created();
        s.record_circular_error();

        let snap = s.snapshot();
        assert_eq!(snap.registrations_total, 1);
        assert_eq!(snap.resolutions_total, 1);
        assert_eq!(snap.resolutions_succeeded, 1);
        assert_eq!(snap.singletons_active, 1);
        assert_eq!(snap.scopes_active, 1);
        assert_eq!(snap.circular_errors, 1);
    }
}
