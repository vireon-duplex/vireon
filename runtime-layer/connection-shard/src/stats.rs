//! Lock-free statistics collector (Layer 12).
//!
//! [`StatsCollector`] holds `AtomicU64` counters that are incremented on
//! the fast path (single-threaded per shard, so `Relaxed` ordering is
//! sufficient) and read from monitoring threads via [`StatsSnapshot`].
//!
//! Follows the cqe-processor and task-queue patterns exactly.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free statistics collector for a [`ConnectionShard`](crate::shard::ConnectionShard).
///
/// All counters use `Ordering::Relaxed` because each shard is mutated by
/// exactly one thread. Cross-thread reads (from a monitoring thread) are
/// eventually consistent — sufficient for observability.
#[derive(Debug)]
pub struct StatsCollector {
    connections_created: AtomicU64,
    connections_destroyed: AtomicU64,
    migrations_in: AtomicU64,
    migrations_out: AtomicU64,
    lookups_total: AtomicU64,
    lookups_missed: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    backpressure_soft_events: AtomicU64,
    backpressure_hard_events: AtomicU64,
    state_transitions: AtomicU64,
    peak_active: AtomicU64,
}

impl StatsCollector {
    /// Creates a zeroed collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections_created: AtomicU64::new(0),
            connections_destroyed: AtomicU64::new(0),
            migrations_in: AtomicU64::new(0),
            migrations_out: AtomicU64::new(0),
            lookups_total: AtomicU64::new(0),
            lookups_missed: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
            backpressure_soft_events: AtomicU64::new(0),
            backpressure_hard_events: AtomicU64::new(0),
            state_transitions: AtomicU64::new(0),
            peak_active: AtomicU64::new(0),
        }
    }

    // ── Recorders ──────────────────────────────────────────────

    /// Records a new connection creation.
    #[inline]
    pub fn record_created(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a connection destruction.
    #[inline]
    pub fn record_destroyed(&self) {
        self.connections_destroyed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful migration into this shard.
    #[inline]
    pub fn record_migration_in(&self) {
        self.migrations_in.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a migration out of this shard.
    #[inline]
    pub fn record_migration_out(&self) {
        self.migrations_out.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a stream-id lookup (hit or miss).
    #[inline]
    pub fn record_lookup(&self, hit: bool) {
        self.lookups_total.fetch_add(1, Ordering::Relaxed);
        if !hit {
            self.lookups_missed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records bytes received on a connection.
    #[inline]
    pub fn record_recv(&self, bytes: u64) {
        self.bytes_recv.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records bytes sent on a connection.
    #[inline]
    pub fn record_send(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a transition to soft backpressure.
    #[inline]
    pub fn record_soft_bp(&self) {
        self.backpressure_soft_events
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a transition to hard backpressure.
    #[inline]
    pub fn record_hard_bp(&self) {
        self.backpressure_hard_events
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a state transition.
    #[inline]
    pub fn record_transition(&self) {
        self.state_transitions.fetch_add(1, Ordering::Relaxed);
    }

    /// Updates the peak active-connection watermark.
    ///
    /// Uses a CAS loop so the peak is monotonically increasing.
    #[inline]
    pub fn update_peak_active(&self, current_active: u64) {
        loop {
            let prev = self.peak_active.load(Ordering::Relaxed);
            if current_active <= prev {
                break;
            }
            if self
                .peak_active
                .compare_exchange_weak(prev, current_active, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    // ── Accessors ──────────────────────────────────────────────

    /// Total connections ever created.
    #[inline]
    #[must_use]
    pub fn created_total(&self) -> u64 {
        self.connections_created.load(Ordering::Relaxed)
    }

    /// Total connections ever destroyed.
    #[inline]
    #[must_use]
    pub fn destroyed_total(&self) -> u64 {
        self.connections_destroyed.load(Ordering::Relaxed)
    }

    /// Peak active-connection watermark.
    #[inline]
    #[must_use]
    pub fn peak_active(&self) -> u64 {
        self.peak_active.load(Ordering::Relaxed)
    }

    /// Captures a point-in-time snapshot of all counters.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_destroyed: self.connections_destroyed.load(Ordering::Relaxed),
            migrations_in: self.migrations_in.load(Ordering::Relaxed),
            migrations_out: self.migrations_out.load(Ordering::Relaxed),
            lookups_total: self.lookups_total.load(Ordering::Relaxed),
            lookups_missed: self.lookups_missed.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_recv: self.bytes_recv.load(Ordering::Relaxed),
            backpressure_soft_events: self.backpressure_soft_events.load(Ordering::Relaxed),
            backpressure_hard_events: self.backpressure_hard_events.load(Ordering::Relaxed),
            state_transitions: self.state_transitions.load(Ordering::Relaxed),
            peak_active: self.peak_active.load(Ordering::Relaxed),
        }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// A `Copy` snapshot of all shard statistics at a point in time.
///
/// Returned by [`StatsCollector::snapshot`]. Safe to hold without
/// borrowing from the collector.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total connections ever created in this shard.
    pub connections_created: u64,
    /// Total connections ever destroyed in this shard.
    pub connections_destroyed: u64,
    /// Total migrations received from other shards.
    pub migrations_in: u64,
    /// Total migrations sent to other shards.
    pub migrations_out: u64,
    /// Total stream-id lookups (hits + misses).
    pub lookups_total: u64,
    /// Stream-id lookups that missed (unknown stream).
    pub lookups_missed: u64,
    /// Total bytes sent across all connections.
    pub bytes_sent: u64,
    /// Total bytes received across all connections.
    pub bytes_recv: u64,
    /// Number of times soft backpressure was entered.
    pub backpressure_soft_events: u64,
    /// Number of times hard backpressure was entered.
    pub backpressure_hard_events: u64,
    /// Total state transitions applied.
    pub state_transitions: u64,
    /// Peak active-connection count (watermark).
    pub peak_active: u64,
}

impl StatsSnapshot {
    /// Lookup hit rate in `[0.0, 1.0]`. Returns `1.0` if no lookups.
    #[must_use]
    pub fn lookup_hit_rate(&self) -> f64 {
        if self.lookups_total == 0 {
            return 1.0;
        }
        let hits = self.lookups_total - self.lookups_missed;
        hits as f64 / self.lookups_total as f64
    }

    /// Currently active connections (created − destroyed − migrated out).
    ///
    /// This is approximate — migrations in are counted as created on the
    /// target, so this formula gives the net active count.
    #[must_use]
    pub fn net_active(&self) -> u64 {
        self.connections_created
            .saturating_sub(self.connections_destroyed)
            .saturating_sub(self.migrations_out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_starts_zeroed() {
        let s = StatsCollector::new().snapshot();
        assert_eq!(s, StatsSnapshot::default());
    }

    #[test]
    fn record_created_destroyed() {
        let c = StatsCollector::new();
        c.record_created();
        c.record_created();
        c.record_destroyed();
        let s = c.snapshot();
        assert_eq!(s.connections_created, 2);
        assert_eq!(s.connections_destroyed, 1);
    }

    #[test]
    fn record_lookup_hit_miss() {
        let c = StatsCollector::new();
        c.record_lookup(true);
        c.record_lookup(true);
        c.record_lookup(false);
        let s = c.snapshot();
        assert_eq!(s.lookups_total, 3);
        assert_eq!(s.lookups_missed, 1);
    }

    #[test]
    fn record_bytes() {
        let c = StatsCollector::new();
        c.record_recv(1024);
        c.record_send(2048);
        let s = c.snapshot();
        assert_eq!(s.bytes_recv, 1024);
        assert_eq!(s.bytes_sent, 2048);
    }

    #[test]
    fn peak_active_is_monotonic() {
        let c = StatsCollector::new();
        c.update_peak_active(10);
        c.update_peak_active(5); // should not lower
        c.update_peak_active(20);
        assert_eq!(c.peak_active(), 20);
    }

    #[test]
    fn lookup_hit_rate_no_lookups() {
        let s = StatsSnapshot::default();
        assert_eq!(s.lookup_hit_rate(), 1.0);
    }

    #[test]
    fn lookup_hit_rate_with_misses() {
        let s = StatsSnapshot {
            lookups_total: 100,
            lookups_missed: 5,
            ..StatsSnapshot::default()
        };
        assert_eq!(s.lookup_hit_rate(), 0.95);
    }

    #[test]
    fn migrations_tracked() {
        let c = StatsCollector::new();
        c.record_migration_in();
        c.record_migration_in();
        c.record_migration_out();
        let s = c.snapshot();
        assert_eq!(s.migrations_in, 2);
        assert_eq!(s.migrations_out, 1);
    }
}
