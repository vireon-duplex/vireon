//! [`SessionStats`] + [`StatsSnapshot`] — atomic counters for the
//! session manager.
//!
//! See `DESIGN.md` Part 15 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of session classes for per-class tracking.
pub const SESSION_CLASSES: usize = 4;

/// Session-manager-wide atomic counters.
#[derive(Debug)]
pub struct SessionStats {
    /// Total sessions created since manager start.
    pub sessions_created_total: AtomicU64,
    /// Total sessions destroyed since manager start.
    pub sessions_destroyed_total: AtomicU64,
    /// Currently-active session count.
    pub sessions_active: AtomicU64,
    /// Currently-idle session count.
    pub sessions_idle: AtomicU64,
    /// Sessions currently migrating.
    pub sessions_migrating: AtomicU64,
    /// Total authentication failures.
    pub auth_failures_total: AtomicU64,
    /// Total authorization denials.
    pub authz_denials_total: AtomicU64,
    /// Total quota rejections (stream/memory/bandwidth).
    pub quota_rejections_total: AtomicU64,
    /// Total connection migrations (successful + failed).
    pub migrations_total: AtomicU64,
    /// Total session resumptions (0-RTT reconnects).
    pub resumptions_total: AtomicU64,
    /// Per-class distribution: index = SessionClass discriminant.
    pub class_distribution: [AtomicU64; SESSION_CLASSES],
}

impl Default for SessionStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStats {
    /// Construct zeroed stats.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            sessions_created_total: AtomicU64::new(0),
            sessions_destroyed_total: AtomicU64::new(0),
            sessions_active: AtomicU64::new(0),
            sessions_idle: AtomicU64::new(0),
            sessions_migrating: AtomicU64::new(0),
            auth_failures_total: AtomicU64::new(0),
            authz_denials_total: AtomicU64::new(0),
            quota_rejections_total: AtomicU64::new(0),
            migrations_total: AtomicU64::new(0),
            resumptions_total: AtomicU64::new(0),
            class_distribution: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    /// Record a session creation.
    pub fn record_created(&self) {
        self.sessions_created_total.fetch_add(1, Ordering::Relaxed);
        self.sessions_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a session destruction.
    pub fn record_destroyed(&self) {
        self.sessions_destroyed_total
            .fetch_add(1, Ordering::Relaxed);
        self.sessions_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a session transitioning to Idle.
    pub fn record_idle(&self) {
        self.sessions_active.fetch_sub(1, Ordering::Relaxed);
        self.sessions_idle.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a session waking from Idle to Active.
    pub fn record_wake(&self) {
        self.sessions_idle.fetch_sub(1, Ordering::Relaxed);
        self.sessions_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an authentication failure.
    pub fn record_auth_failure(&self) {
        self.auth_failures_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an authorization denial.
    pub fn record_authz_denial(&self) {
        self.authz_denials_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a quota rejection.
    pub fn record_quota_rejection(&self) {
        self.quota_rejections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a migration attempt (success or failure).
    pub fn record_migration(&self) {
        self.migrations_total.fetch_add(1, Ordering::Relaxed);
        self.sessions_migrating.fetch_add(1, Ordering::Relaxed);
    }

    /// Record migration completion.
    pub fn record_migration_complete(&self) {
        self.sessions_migrating.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a session resumption (0-RTT).
    pub fn record_resumption(&self) {
        self.resumptions_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the per-class counter.
    pub fn record_class(&self, class_index: usize) {
        if class_index < SESSION_CLASSES {
            self.class_distribution[class_index].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Snapshot all counters atomically (per-field).
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            sessions_created_total: self.sessions_created_total.load(Ordering::Relaxed),
            sessions_destroyed_total: self.sessions_destroyed_total.load(Ordering::Relaxed),
            sessions_active: self.sessions_active.load(Ordering::Relaxed),
            sessions_idle: self.sessions_idle.load(Ordering::Relaxed),
            sessions_migrating: self.sessions_migrating.load(Ordering::Relaxed),
            auth_failures_total: self.auth_failures_total.load(Ordering::Relaxed),
            authz_denials_total: self.authz_denials_total.load(Ordering::Relaxed),
            quota_rejections_total: self.quota_rejections_total.load(Ordering::Relaxed),
            migrations_total: self.migrations_total.load(Ordering::Relaxed),
            resumptions_total: self.resumptions_total.load(Ordering::Relaxed),
            class_distribution: [
                self.class_distribution[0].load(Ordering::Relaxed),
                self.class_distribution[1].load(Ordering::Relaxed),
                self.class_distribution[2].load(Ordering::Relaxed),
                self.class_distribution[3].load(Ordering::Relaxed),
            ],
        }
    }
}

/// `Copy` view of [`SessionStats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Cumulative created count.
    pub sessions_created_total: u64,
    /// Cumulative destroyed count.
    pub sessions_destroyed_total: u64,
    /// Currently-active count.
    pub sessions_active: u64,
    /// Currently-idle count.
    pub sessions_idle: u64,
    /// Currently-migrating count.
    pub sessions_migrating: u64,
    /// Cumulative auth failures.
    pub auth_failures_total: u64,
    /// Cumulative authz denials.
    pub authz_denials_total: u64,
    /// Cumulative quota rejections.
    pub quota_rejections_total: u64,
    /// Cumulative migrations.
    pub migrations_total: u64,
    /// Cumulative resumptions (0-RTT).
    pub resumptions_total: u64,
    /// Per-class session counts.
    pub class_distribution: [u64; SESSION_CLASSES],
}

impl StatsSnapshot {
    /// Authentication failure rate as a fraction of total created
    /// sessions (0.0..=1.0).
    #[must_use]
    pub fn auth_failure_rate(&self) -> f64 {
        if self.sessions_created_total == 0 {
            0.0
        } else {
            self.auth_failures_total as f64 / self.sessions_created_total as f64
        }
    }

    /// Quota rejection rate as a fraction of total created.
    #[must_use]
    pub fn quota_rejection_rate(&self) -> f64 {
        if self.sessions_created_total == 0 {
            0.0
        } else {
            self.quota_rejections_total as f64 / self.sessions_created_total as f64
        }
    }

    /// Session churn rate = destroyed / created.
    #[must_use]
    pub fn churn_rate(&self) -> f64 {
        if self.sessions_created_total == 0 {
            0.0
        } else {
            self.sessions_destroyed_total as f64 / self.sessions_created_total as f64
        }
    }

    /// Sessions in a non-active state (idle + migrating).
    #[must_use]
    pub fn sessions_inactive(&self) -> u64 {
        self.sessions_idle.saturating_add(self.sessions_migrating)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_created_increments_active() {
        let s = SessionStats::new();
        s.record_created();
        s.record_created();
        let snap = s.snapshot();
        assert_eq!(snap.sessions_created_total, 2);
        assert_eq!(snap.sessions_active, 2);
    }

    #[test]
    fn record_destroyed_decrements_active() {
        let s = SessionStats::new();
        s.record_created();
        s.record_created();
        s.record_destroyed();
        let snap = s.snapshot();
        assert_eq!(snap.sessions_active, 1);
        assert_eq!(snap.sessions_destroyed_total, 1);
    }

    #[test]
    fn idle_wake_cycle() {
        let s = SessionStats::new();
        s.record_created();
        s.record_idle();
        assert_eq!(s.snapshot().sessions_active, 0);
        assert_eq!(s.snapshot().sessions_idle, 1);
        s.record_wake();
        assert_eq!(s.snapshot().sessions_active, 1);
        assert_eq!(s.snapshot().sessions_idle, 0);
    }

    #[test]
    fn auth_failure_rate() {
        let s = SessionStats::new();
        s.record_created();
        s.record_created();
        s.record_created();
        s.record_auth_failure();
        s.record_auth_failure();
        let snap = s.snapshot();
        assert!((snap.auth_failure_rate() - (2.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn churn_rate() {
        let snap = StatsSnapshot {
            sessions_created_total: 100,
            sessions_destroyed_total: 30,
            ..StatsSnapshot::default()
        };
        assert!((snap.churn_rate() - 0.3).abs() < 1e-9);
    }

    #[test]
    fn migration_tracking() {
        let s = SessionStats::new();
        s.record_migration();
        s.record_migration();
        s.record_migration_complete();
        let snap = s.snapshot();
        assert_eq!(snap.migrations_total, 2);
        assert_eq!(snap.sessions_migrating, 1);
    }

    #[test]
    fn class_distribution() {
        let s = SessionStats::new();
        s.record_class(0); // LatencySensitive
        s.record_class(2); // Streaming
        let snap = s.snapshot();
        assert_eq!(snap.class_distribution[0], 1);
        assert_eq!(snap.class_distribution[2], 1);
    }
}
