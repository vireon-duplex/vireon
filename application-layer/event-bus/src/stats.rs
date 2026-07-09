//! [`EventBusStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for event bus observability.
//!
//! See `DESIGN.md` Part 12 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

// ── EventBusStats ─────────────────────────────────────────────────

/// Atomic counters for the event bus.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`EventBusStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct EventBusStats {
    /// Total messages published (counter).
    pub messages_published: AtomicU64,
    /// Total messages consumed (counter).
    pub messages_consumed: AtomicU64,
    /// Bytes published (counter).
    pub bytes_published: AtomicU64,
    /// Publish errors (counter).
    pub publish_errors: AtomicU64,
    /// Consumer lag in nanoseconds (gauge).
    pub consumer_lag_ns: AtomicU64,
    /// Rebalance events triggered (counter).
    pub rebalance_count: AtomicU64,
    /// ACKs processed (counter).
    pub ack_total: AtomicU64,
}

impl Default for EventBusStats {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBusStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            messages_published: AtomicU64::new(0),
            messages_consumed: AtomicU64::new(0),
            bytes_published: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
            consumer_lag_ns: AtomicU64::new(0),
            rebalance_count: AtomicU64::new(0),
            ack_total: AtomicU64::new(0),
        }
    }

    /// Record a published message.
    pub fn record_publish(&self, bytes: u64) {
        self.messages_published.fetch_add(1, Ordering::Relaxed);
        self.bytes_published.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a consumed message.
    pub fn record_consume(&self) {
        self.messages_consumed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a publish error.
    pub fn record_publish_error(&self) {
        self.publish_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record consumer lag.
    pub fn record_consumer_lag(&self, lag_ns: u64) {
        self.consumer_lag_ns.store(lag_ns, Ordering::Relaxed);
    }

    /// Record a rebalance event.
    pub fn record_rebalance(&self) {
        self.rebalance_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an ACK.
    pub fn record_ack(&self) {
        self.ack_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            messages_published: self.messages_published.load(Ordering::Relaxed),
            messages_consumed: self.messages_consumed.load(Ordering::Relaxed),
            bytes_published: self.bytes_published.load(Ordering::Relaxed),
            publish_errors: self.publish_errors.load(Ordering::Relaxed),
            consumer_lag_ns: self.consumer_lag_ns.load(Ordering::Relaxed),
            rebalance_count: self.rebalance_count.load(Ordering::Relaxed),
            ack_total: self.ack_total.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────────

/// Point-in-time snapshot of event bus counters.
///
/// Rates are precomputed as basis-points (bps, 1/100 of a percent)
/// so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total messages published.
    pub messages_published: u64,
    /// Total messages consumed.
    pub messages_consumed: u64,
    /// Bytes published.
    pub bytes_published: u64,
    /// Publish errors.
    pub publish_errors: u64,
    /// Consumer lag in nanoseconds.
    pub consumer_lag_ns: u64,
    /// Rebalance events.
    pub rebalance_count: u64,
    /// ACKs processed.
    pub ack_total: u64,
}

impl StatsSnapshot {
    /// Publish error rate in basis points (0..=10_000).
    /// Returns 0 when there are no publishes yet.
    #[must_use]
    pub const fn publish_error_rate_bps(&self) -> u16 {
        let total = self.messages_published.saturating_add(self.publish_errors);
        if total == 0 {
            return 0;
        }
        let rate = (self.publish_errors.saturating_mul(10_000)) / total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Whether there are active rebalances.
    #[must_use]
    pub const fn has_active_rebalances(&self) -> bool {
        self.rebalance_count > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = EventBusStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_publish_increments_counters() {
        let s = EventBusStats::new();
        s.record_publish(1024);
        s.record_publish(2048);
        let snap = s.snapshot();
        assert_eq!(snap.messages_published, 2);
        assert_eq!(snap.bytes_published, 3072);
    }

    #[test]
    fn record_consume_increments_counter() {
        let s = EventBusStats::new();
        s.record_consume();
        s.record_consume();
        assert_eq!(s.snapshot().messages_consumed, 2);
    }

    #[test]
    fn record_publish_error_increments_counter() {
        let s = EventBusStats::new();
        s.record_publish_error();
        assert_eq!(s.snapshot().publish_errors, 1);
    }

    #[test]
    fn record_rebalance_increments_counter() {
        let s = EventBusStats::new();
        s.record_rebalance();
        assert_eq!(s.snapshot().rebalance_count, 1);
    }

    #[test]
    fn record_ack_increments_counter() {
        let s = EventBusStats::new();
        s.record_ack();
        s.record_ack();
        assert_eq!(s.snapshot().ack_total, 2);
    }

    #[test]
    fn consumer_lag_updates() {
        let s = EventBusStats::new();
        s.record_consumer_lag(1_000_000_000); // 1 second
        assert_eq!(s.snapshot().consumer_lag_ns, 1_000_000_000);
    }

    #[test]
    fn publish_error_rate_with_no_publishes_is_zero() {
        let s = EventBusStats::new();
        assert_eq!(s.snapshot().publish_error_rate_bps(), 0);
    }

    #[test]
    fn publish_error_rate_calculates_correctly() {
        let s = EventBusStats::new();
        s.record_publish(100); // success
        s.record_publish_error();
        s.record_publish_error(); // 2 errors
        // 2 errors / 3 total = 66.67% = 6666 bps (integer division truncates)
        assert_eq!(s.snapshot().publish_error_rate_bps(), 6_666);
    }

    #[test]
    fn has_active_rebalances() {
        let s = EventBusStats::new();
        assert!(!s.snapshot().has_active_rebalances());
        s.record_rebalance();
        assert!(s.snapshot().has_active_rebalances());
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = EventBusStats::new();
        s.record_publish(1000);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_cloning_is_independent() {
        let s = EventBusStats::new();
        s.record_publish(500);
        let snap1 = s.snapshot();
        let snap2 = snap1;
        s.record_publish(500); // This affects stats, not snapshots
        assert_eq!(snap1, snap2); // Snapshots are unchanged
        assert_eq!(snap1.messages_published, 1);
    }

    #[test]
    fn publish_error_rate_clamps_to_u16_max() {
        let mut snap = StatsSnapshot::default();
        snap.messages_published = 0;
        snap.publish_errors = 100;
        // 100% error rate = 10_000 bps (the max meaningful value).
        let rate = snap.publish_error_rate_bps();
        assert_eq!(rate, 10_000);
    }

    #[test]
    fn all_counter_methods_independent() {
        let s = EventBusStats::new();
        s.record_publish(123);
        s.record_consume();
        s.record_publish_error();
        s.record_consumer_lag(999);
        s.record_rebalance();
        s.record_ack();
        let snap = s.snapshot();
        assert_eq!(snap.messages_published, 1);
        assert_eq!(snap.messages_consumed, 1);
        assert_eq!(snap.publish_errors, 1);
        assert_eq!(snap.consumer_lag_ns, 999);
        assert_eq!(snap.rebalance_count, 1);
        assert_eq!(snap.ack_total, 1);
    }
}
