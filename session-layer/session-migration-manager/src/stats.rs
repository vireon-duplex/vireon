//! [`MigrationStats`], [`StatsSnapshot`] — atomic counters and a
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
//!
//! # Mapping to metrics
//!
//! Each counter maps 1:1 to a Prometheus counter or gauge in
//! Phase 2+:
//!
//! | Counter | Prometheus type | Name |
//! |---------|-----------------|------|
//! | `migrations_started` | counter | `migration_started_total` |
//! | `migrations_completed` | counter | `migration_completed_total` |
//! | `migrations_failed` | counter | `migration_failed_total` |
//! | `migrations_in_progress` | gauge | `migration_in_progress` |
//! | `snapshots_captured` | counter | `migration_snapshots_captured_total` |
//! | `bytes_transferred` | counter | `migration_bytes_transferred_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── MigrationStats ──────────────────────────────────────────────

/// Atomic counters for the session migration manager.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`MigrationStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct MigrationStats {
    // ── Migration lifecycle ───────────────────────────────────
    /// Total migrations ever initiated (counter; never decreases).
    pub migrations_started: AtomicU64,
    /// Total migrations completed successfully (counter).
    pub migrations_completed: AtomicU64,
    /// Total migrations that failed (counter).
    pub migrations_failed: AtomicU64,
    /// Total migrations rolled back (counter).
    pub migrations_rolled_back: AtomicU64,
    /// Migrations currently in progress (gauge).
    pub migrations_in_progress: AtomicU64,

    // ── Duration ──────────────────────────────────────────────
    /// Total nanoseconds spent in completed migrations (for average).
    pub total_duration_ns: AtomicU64,

    // ── Snapshot / transfer ───────────────────────────────────
    /// Snapshots captured (counter).
    pub snapshots_captured: AtomicU64,
    /// State transfers performed (counter).
    pub transfers_total: AtomicU64,
    /// Total bytes transferred (counter).
    pub bytes_transferred: AtomicU64,
}

impl Default for MigrationStats {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            migrations_started: AtomicU64::new(0),
            migrations_completed: AtomicU64::new(0),
            migrations_failed: AtomicU64::new(0),
            migrations_rolled_back: AtomicU64::new(0),
            migrations_in_progress: AtomicU64::new(0),
            total_duration_ns: AtomicU64::new(0),
            snapshots_captured: AtomicU64::new(0),
            transfers_total: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
        }
    }

    /// Record a migration initiation.
    pub fn record_started(&self) {
        self.migrations_started.fetch_add(1, Ordering::Relaxed);
        self.migrations_in_progress.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful migration completion.
    pub fn record_completed(&self, duration_ns: u64) {
        self.migrations_completed.fetch_add(1, Ordering::Relaxed);
        self.migrations_in_progress.fetch_sub(1, Ordering::Relaxed);
        self.total_duration_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
    }

    /// Record a failed migration.
    pub fn record_failed(&self) {
        self.migrations_failed.fetch_add(1, Ordering::Relaxed);
        self.migrations_in_progress.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a rolled-back migration.
    pub fn record_rolled_back(&self) {
        self.migrations_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a snapshot capture.
    pub fn record_snapshot(&self) {
        self.snapshots_captured.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a state transfer of `bytes` bytes.
    pub fn record_transfer(&self, bytes: u64) {
        self.transfers_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            migrations_started: self.migrations_started.load(Ordering::Relaxed),
            migrations_completed: self.migrations_completed.load(Ordering::Relaxed),
            migrations_failed: self.migrations_failed.load(Ordering::Relaxed),
            migrations_rolled_back: self.migrations_rolled_back.load(Ordering::Relaxed),
            migrations_in_progress: self.migrations_in_progress.load(Ordering::Relaxed),
            total_duration_ns: self.total_duration_ns.load(Ordering::Relaxed),
            snapshots_captured: self.snapshots_captured.load(Ordering::Relaxed),
            transfers_total: self.transfers_total.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of migration counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total migrations ever initiated.
    pub migrations_started: u64,
    /// Total migrations completed successfully.
    pub migrations_completed: u64,
    /// Total migrations that failed.
    pub migrations_failed: u64,
    /// Total migrations rolled back.
    pub migrations_rolled_back: u64,
    /// Migrations currently in progress.
    pub migrations_in_progress: u64,
    /// Total nanoseconds in completed migrations.
    pub total_duration_ns: u64,
    /// Snapshots captured.
    pub snapshots_captured: u64,
    /// State transfers performed.
    pub transfers_total: u64,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
}

impl StatsSnapshot {
    /// Success rate in basis points (0..=10_000).
    /// `completed / (completed + failed)`. Returns 10_000 (100%)
    /// when no migrations have terminated yet.
    #[must_use]
    pub const fn success_rate_bps(&self) -> u16 {
        let denom = self
            .migrations_completed
            .saturating_add(self.migrations_failed);
        if denom == 0 {
            return 10_000;
        }
        let rate = (self.migrations_completed * 10_000) / denom;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Average migration duration in nanoseconds.
    /// Returns 0 when no migrations have completed.
    #[must_use]
    pub const fn avg_duration_ns(&self) -> u64 {
        if self.migrations_completed == 0 {
            return 0;
        }
        self.total_duration_ns / self.migrations_completed
    }

    /// Failure rate in basis points (0..=10_000).
    /// `failed / (completed + failed)`. Returns 0 when no migrations
    /// have terminated.
    #[must_use]
    pub const fn failure_rate_bps(&self) -> u16 {
        let denom = self
            .migrations_completed
            .saturating_add(self.migrations_failed);
        if denom == 0 {
            return 0;
        }
        let rate = (self.migrations_failed * 10_000) / denom;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Average bytes transferred per transfer.
    /// Returns 0 when no transfers have occurred.
    #[must_use]
    pub const fn avg_bytes_per_transfer(&self) -> u64 {
        if self.transfers_total == 0 {
            return 0;
        }
        self.bytes_transferred / self.transfers_total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = MigrationStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_started_increments_started_and_in_progress() {
        let s = MigrationStats::new();
        s.record_started();
        s.record_started();
        let snap = s.snapshot();
        assert_eq!(snap.migrations_started, 2);
        assert_eq!(snap.migrations_in_progress, 2);
    }

    #[test]
    fn record_completed_decrements_in_progress() {
        let s = MigrationStats::new();
        s.record_started();
        s.record_started();
        s.record_completed(5_000_000_000);
        let snap = s.snapshot();
        assert_eq!(snap.migrations_started, 2);
        assert_eq!(snap.migrations_completed, 1);
        assert_eq!(snap.migrations_in_progress, 1);
        assert_eq!(snap.total_duration_ns, 5_000_000_000);
    }

    #[test]
    fn record_failed_decrements_in_progress() {
        let s = MigrationStats::new();
        s.record_started();
        s.record_failed();
        let snap = s.snapshot();
        assert_eq!(snap.migrations_failed, 1);
        assert_eq!(snap.migrations_in_progress, 0);
    }

    #[test]
    fn record_rolled_back() {
        let s = MigrationStats::new();
        s.record_rolled_back();
        s.record_rolled_back();
        assert_eq!(s.snapshot().migrations_rolled_back, 2);
    }

    #[test]
    fn record_snapshot_and_transfer() {
        let s = MigrationStats::new();
        s.record_snapshot();
        s.record_transfer(4_096);
        s.record_transfer(8_192);
        let snap = s.snapshot();
        assert_eq!(snap.snapshots_captured, 1);
        assert_eq!(snap.transfers_total, 2);
        assert_eq!(snap.bytes_transferred, 12_288);
    }

    #[test]
    fn success_rate_with_no_terminations() {
        let s = StatsSnapshot::default();
        assert_eq!(s.success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_with_mixed_results() {
        let s = StatsSnapshot {
            migrations_completed: 9,
            migrations_failed: 1,
            ..StatsSnapshot::default()
        };
        // 9/10 = 90% = 9000 bps.
        assert_eq!(s.success_rate_bps(), 9_000);
        assert_eq!(s.failure_rate_bps(), 1_000);
    }

    #[test]
    fn avg_duration_ns() {
        let s = StatsSnapshot {
            migrations_completed: 3,
            total_duration_ns: 15_000_000_000, // 15s total
            ..StatsSnapshot::default()
        };
        assert_eq!(s.avg_duration_ns(), 5_000_000_000); // 5s avg
    }

    #[test]
    fn avg_duration_zero_when_no_completions() {
        let s = StatsSnapshot::default();
        assert_eq!(s.avg_duration_ns(), 0);
    }
}
