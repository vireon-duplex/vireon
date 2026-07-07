//! [`ReplicationStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for replication observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering: stats
//! are advisory, never load-bearing, so we trade synchronization
//! cost for throughput. Snapshots are taken on demand and the
//! snapshot is what gets exported to metrics.
//!
//! # Mapping to metrics
//!
//! Each counter maps 1:1 to a Prometheus counter or gauge:
//!
//! | Counter | Prometheus type | Name |
//! |---------|-----------------|------|
//! | `ops_total` | counter | `replication_ops_total` |
//! | `ops_succeeded` | counter | `replication_ops_succeeded_total` |
//! | `ops_failed` | counter | `replication_ops_failed_total` |
//! | `reads_total` | counter | `replication_reads_total` |
//! | `writes_total` | counter | `replication_writes_total` |
//! | `conflicts_total` | counter | `replication_conflicts_total` |
//! | `bytes_replicated` | counter | `replication_bytes_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── ReplicationStats ────────────────────────────────────────────

/// Atomic counters for the session replication service.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`ReplicationStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct ReplicationStats {
    // ── Operation counters ─────────────────────────────────────
    /// Total replication operations attempted (counter).
    pub ops_total: AtomicU64,
    /// Operations that succeeded (counter).
    pub ops_succeeded: AtomicU64,
    /// Operations that failed (counter).
    pub ops_failed: AtomicU64,
    /// Total read operations (counter).
    pub reads_total: AtomicU64,
    /// Total write operations (counter).
    pub writes_total: AtomicU64,

    // ── Conflict counters ──────────────────────────────────────
    /// Total conflicts detected (counter).
    pub conflicts_total: AtomicU64,
    /// Total conflicts resolved (counter).
    pub resolutions_total: AtomicU64,

    // ── Replication health ─────────────────────────────────────
    /// Current replication lag in nanoseconds (gauge).
    pub lag_ns: AtomicU64,
    /// Gossip protocol rounds completed (counter).
    pub gossip_rounds: AtomicU64,
    /// Raft terms seen (counter).
    pub raft_terms: AtomicU64,
    /// Total bytes replicated (counter).
    pub bytes_replicated: AtomicU64,
}

impl Default for ReplicationStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ops_total: AtomicU64::new(0),
            ops_succeeded: AtomicU64::new(0),
            ops_failed: AtomicU64::new(0),
            reads_total: AtomicU64::new(0),
            writes_total: AtomicU64::new(0),
            conflicts_total: AtomicU64::new(0),
            resolutions_total: AtomicU64::new(0),
            lag_ns: AtomicU64::new(0),
            gossip_rounds: AtomicU64::new(0),
            raft_terms: AtomicU64::new(0),
            bytes_replicated: AtomicU64::new(0),
        }
    }

    /// Record a successful replication operation.
    pub fn record_success(&self) {
        self.ops_total.fetch_add(1, Ordering::Relaxed);
        self.ops_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed replication operation.
    pub fn record_failure(&self) {
        self.ops_total.fetch_add(1, Ordering::Relaxed);
        self.ops_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a read operation (independent of success/failure).
    pub fn record_read(&self) {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write operation (independent of success/failure).
    pub fn record_write(&self) {
        self.writes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a conflict detection.
    pub fn record_conflict(&self) {
        self.ops_total.fetch_add(1, Ordering::Relaxed);
        self.conflicts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a conflict resolution.
    pub fn record_resolution(&self) {
        self.resolutions_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a gossip round completion.
    pub fn record_gossip_round(&self) {
        self.gossip_rounds.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a Raft term change.
    pub fn record_raft_term(&self) {
        self.raft_terms.fetch_add(1, Ordering::Relaxed);
    }

    /// Record bytes replicated.
    pub fn record_bytes(&self, n: u64) {
        self.bytes_replicated.fetch_add(n, Ordering::Relaxed);
    }

    /// Update the current replication lag (nanoseconds).
    pub fn set_lag_ns(&self, lag_ns: u64) {
        self.lag_ns.store(lag_ns, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            ops_total: self.ops_total.load(Ordering::Relaxed),
            ops_succeeded: self.ops_succeeded.load(Ordering::Relaxed),
            ops_failed: self.ops_failed.load(Ordering::Relaxed),
            reads_total: self.reads_total.load(Ordering::Relaxed),
            writes_total: self.writes_total.load(Ordering::Relaxed),
            conflicts_total: self.conflicts_total.load(Ordering::Relaxed),
            resolutions_total: self.resolutions_total.load(Ordering::Relaxed),
            lag_ns: self.lag_ns.load(Ordering::Relaxed),
            gossip_rounds: self.gossip_rounds.load(Ordering::Relaxed),
            raft_terms: self.raft_terms.load(Ordering::Relaxed),
            bytes_replicated: self.bytes_replicated.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of replication counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total replication operations attempted.
    pub ops_total: u64,
    /// Operations that succeeded.
    pub ops_succeeded: u64,
    /// Operations that failed.
    pub ops_failed: u64,
    /// Total read operations.
    pub reads_total: u64,
    /// Total write operations.
    pub writes_total: u64,
    /// Total conflicts detected.
    pub conflicts_total: u64,
    /// Total conflicts resolved.
    pub resolutions_total: u64,
    /// Current replication lag (nanoseconds).
    pub lag_ns: u64,
    /// Gossip protocol rounds completed.
    pub gossip_rounds: u64,
    /// Raft terms seen.
    pub raft_terms: u64,
    /// Total bytes replicated.
    pub bytes_replicated: u64,
}

impl StatsSnapshot {
    /// Success rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no operations yet.
    #[must_use]
    pub const fn success_rate_bps(&self) -> u16 {
        if self.ops_total == 0 {
            return 10_000;
        }
        let rate = (self.ops_succeeded * 10_000) / self.ops_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Conflict rate in basis points (0..=10_000).
    /// Returns 0 when there are no operations yet.
    #[must_use]
    pub const fn conflict_rate_bps(&self) -> u16 {
        if self.ops_total == 0 {
            return 0;
        }
        let rate = (self.conflicts_total * 10_000) / self.ops_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Current replication lag in nanoseconds.
    #[must_use]
    pub const fn current_lag_ns(&self) -> u64 {
        self.lag_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = ReplicationStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_success_increments_counters() {
        let s = ReplicationStats::new();
        s.record_success();
        s.record_success();
        let snap = s.snapshot();
        assert_eq!(snap.ops_total, 2);
        assert_eq!(snap.ops_succeeded, 2);
        assert_eq!(snap.ops_failed, 0);
    }

    #[test]
    fn record_failure_increments_counters() {
        let s = ReplicationStats::new();
        s.record_failure();
        let snap = s.snapshot();
        assert_eq!(snap.ops_total, 1);
        assert_eq!(snap.ops_failed, 1);
        assert_eq!(snap.ops_succeeded, 0);
    }

    #[test]
    fn record_read_and_write() {
        let s = ReplicationStats::new();
        s.record_read();
        s.record_read();
        s.record_write();
        let snap = s.snapshot();
        assert_eq!(snap.reads_total, 2);
        assert_eq!(snap.writes_total, 1);
    }

    #[test]
    fn success_rate_with_no_ops_is_100_pct() {
        let s = ReplicationStats::new();
        assert_eq!(s.snapshot().success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_reflects_mixed_ops() {
        let s = ReplicationStats::new();
        for _ in 0..9 {
            s.record_success();
        }
        for _ in 0..1 {
            s.record_failure();
        }
        // 9/10 = 90% = 9000 bps.
        assert_eq!(s.snapshot().success_rate_bps(), 9_000);
    }

    #[test]
    fn conflict_rate_reflects_conflicts() {
        let s = ReplicationStats::new();
        for _ in 0..95 {
            s.record_success();
        }
        for _ in 0..5 {
            s.record_conflict();
        }
        // 5/100 = 5% = 500 bps.
        assert_eq!(s.snapshot().conflict_rate_bps(), 500);
    }

    #[test]
    fn lag_is_settable() {
        let s = ReplicationStats::new();
        s.set_lag_ns(42_000_000);
        assert_eq!(s.snapshot().current_lag_ns(), 42_000_000);
    }

    #[test]
    fn bytes_replicated_accumulate() {
        let s = ReplicationStats::new();
        s.record_bytes(1_000);
        s.record_bytes(2_500);
        assert_eq!(s.snapshot().bytes_replicated, 3_500);
    }

    #[test]
    fn gossip_and_raft_counters() {
        let s = ReplicationStats::new();
        s.record_gossip_round();
        s.record_gossip_round();
        s.record_raft_term();
        let snap = s.snapshot();
        assert_eq!(snap.gossip_rounds, 2);
        assert_eq!(snap.raft_terms, 1);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = ReplicationStats::new();
        s.record_success();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }
}
