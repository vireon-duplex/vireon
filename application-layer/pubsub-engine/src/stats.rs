//! [`PubSubStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for pub/sub engine observability.
//!
//! See `DESIGN.md` Part 12 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

// ── PubSubStats ─────────────────────────────────────────────────

/// Atomic counters for the pub/sub engine.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`PubSubStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct PubSubStats {
    /// Total messages published (counter).
    pub messages_published: AtomicU64,
    /// Total messages delivered (counter).
    pub messages_delivered: AtomicU64,
    /// Delivery failures (counter).
    pub delivery_failures: AtomicU64,
    /// Currently active subscriptions (gauge).
    pub subscriptions_active: AtomicU64,
    /// Total subscriptions created (counter).
    pub subscriptions_created: AtomicU64,
}

impl Default for PubSubStats {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            messages_published: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            delivery_failures: AtomicU64::new(0),
            subscriptions_active: AtomicU64::new(0),
            subscriptions_created: AtomicU64::new(0),
        }
    }

    /// Record a published message.
    pub fn record_publish(&self) {
        self.messages_published.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delivered message.
    pub fn record_deliver(&self) {
        self.messages_delivered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delivery failure.
    pub fn record_delivery_failure(&self) {
        self.delivery_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a subscription creation.
    pub fn record_subscription_created(&self) {
        self.subscriptions_created.fetch_add(1, Ordering::Relaxed);
        self.subscriptions_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a subscription removal.
    pub fn record_subscription_removed(&self) {
        self.subscriptions_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Set the active subscription count directly.
    pub fn set_active_subscriptions(&self, count: u64) {
        self.subscriptions_active.store(count, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            messages_published: self.messages_published.load(Ordering::Relaxed),
            messages_delivered: self.messages_delivered.load(Ordering::Relaxed),
            delivery_failures: self.delivery_failures.load(Ordering::Relaxed),
            subscriptions_active: self.subscriptions_active.load(Ordering::Relaxed),
            subscriptions_created: self.subscriptions_created.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of pub/sub engine counters.
///
/// Rates are precomputed as basis-points (bps, 1/100 of a percent)
/// so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total messages published.
    pub messages_published: u64,
    /// Total messages delivered.
    pub messages_delivered: u64,
    /// Delivery failures.
    pub delivery_failures: u64,
    /// Currently active subscriptions.
    pub subscriptions_active: u64,
    /// Total subscriptions created.
    pub subscriptions_created: u64,
}

impl StatsSnapshot {
    /// Delivery success rate in basis points (0..=10_000).
    ///
    /// Returns 10,000 (100%) when there are no deliveries yet,
    /// since no failures have occurred.
    #[must_use]
    pub const fn delivery_success_rate_bps(&self) -> u16 {
        let total = self
            .messages_delivered
            .saturating_add(self.delivery_failures);
        if total == 0 {
            return 10_000; // 100% — no failures observed
        }
        let rate = (self.messages_delivered.saturating_mul(10_000)) / total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Delivery failure rate in basis points (0..=10_000).
    ///
    /// Returns 0 when there are no deliveries yet.
    #[must_use]
    pub const fn delivery_failure_rate_bps(&self) -> u16 {
        let total = self
            .messages_delivered
            .saturating_add(self.delivery_failures);
        if total == 0 {
            return 0;
        }
        let rate = (self.delivery_failures.saturating_mul(10_000)) / total;
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
        let s = PubSubStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_publish_increments_counter() {
        let s = PubSubStats::new();
        s.record_publish();
        s.record_publish();
        assert_eq!(s.snapshot().messages_published, 2);
    }

    #[test]
    fn record_deliver_increments_counter() {
        let s = PubSubStats::new();
        s.record_deliver();
        s.record_deliver();
        assert_eq!(s.snapshot().messages_delivered, 2);
    }

    #[test]
    fn record_delivery_failure_increments_counter() {
        let s = PubSubStats::new();
        s.record_delivery_failure();
        assert_eq!(s.snapshot().delivery_failures, 1);
    }

    #[test]
    fn record_subscription_created_increments_counters() {
        let s = PubSubStats::new();
        s.record_subscription_created();
        s.record_subscription_created();
        let snap = s.snapshot();
        assert_eq!(snap.subscriptions_created, 2);
        assert_eq!(snap.subscriptions_active, 2);
    }

    #[test]
    fn record_subscription_removed_decrements_active() {
        let s = PubSubStats::new();
        s.record_subscription_created();
        s.record_subscription_created();
        s.record_subscription_removed();
        let snap = s.snapshot();
        assert_eq!(snap.subscriptions_created, 2);
        assert_eq!(snap.subscriptions_active, 1);
    }

    #[test]
    fn set_active_subscriptions() {
        let s = PubSubStats::new();
        s.set_active_subscriptions(42);
        assert_eq!(s.snapshot().subscriptions_active, 42);
    }

    #[test]
    fn delivery_success_rate_with_no_deliveries_is_100_percent() {
        let s = PubSubStats::new();
        assert_eq!(s.snapshot().delivery_success_rate_bps(), 10_000);
    }

    #[test]
    fn delivery_success_rate_calculates_correctly() {
        let s = PubSubStats::new();
        s.record_deliver(); // success
        s.record_deliver(); // success
        s.record_delivery_failure(); // 1 failure
        // 2 successes / 3 total = 66.67% = 6666 bps (integer division truncates)
        assert_eq!(s.snapshot().delivery_success_rate_bps(), 6_666);
    }

    #[test]
    fn delivery_failure_rate_with_no_deliveries_is_zero() {
        let s = PubSubStats::new();
        assert_eq!(s.snapshot().delivery_failure_rate_bps(), 0);
    }

    #[test]
    fn delivery_failure_rate_calculates_correctly() {
        let s = PubSubStats::new();
        s.record_deliver();
        s.record_delivery_failure();
        s.record_delivery_failure();
        // 2 failures / 3 total = 66.67% = 6666 bps
        assert_eq!(s.snapshot().delivery_failure_rate_bps(), 6_666);
    }

    #[test]
    fn delivery_success_rate_100_percent() {
        let s = PubSubStats::new();
        s.record_deliver();
        s.record_deliver();
        assert_eq!(s.snapshot().delivery_success_rate_bps(), 10_000);
    }

    #[test]
    fn delivery_success_rate_0_percent() {
        let s = PubSubStats::new();
        s.record_delivery_failure();
        s.record_delivery_failure();
        assert_eq!(s.snapshot().delivery_success_rate_bps(), 0);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = PubSubStats::new();
        s.record_publish();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_cloning_is_independent() {
        let s = PubSubStats::new();
        s.record_publish();
        let snap1 = s.snapshot();
        let snap2 = snap1;
        s.record_publish(); // This affects stats, not snapshots
        assert_eq!(snap1, snap2); // Snapshots are unchanged
        assert_eq!(snap1.messages_published, 1);
    }

    #[test]
    fn all_counter_methods_independent() {
        let s = PubSubStats::new();
        s.record_publish();
        s.record_deliver();
        s.record_delivery_failure();
        s.record_subscription_created();
        let snap = s.snapshot();
        assert_eq!(snap.messages_published, 1);
        assert_eq!(snap.messages_delivered, 1);
        assert_eq!(snap.delivery_failures, 1);
        assert_eq!(snap.subscriptions_active, 1);
        assert_eq!(snap.subscriptions_created, 1);
    }

    #[test]
    fn delivery_rates_are_complementary_with_exact_division() {
        // Use numbers that divide evenly so rates sum to exactly 10_000.
        // 800 delivered + 200 failures = 1000 total → 8000 bps + 2000 bps.
        let s = PubSubStats::new();
        for _ in 0..800 {
            s.record_deliver();
        }
        for _ in 0..200 {
            s.record_delivery_failure();
        }
        let snap = s.snapshot();
        let sum = snap.delivery_success_rate_bps() as u32 + snap.delivery_failure_rate_bps() as u32;
        assert_eq!(sum, 10_000);
    }

    #[test]
    fn delivery_rates_truncate_with_inexact_division() {
        // 2 delivered + 1 failure = 3 total.
        // success = 2*10000/3 = 6666 bps (truncated).
        // failure = 1*10000/3 = 3333 bps (truncated).
        // Sum = 9999, not 10000, due to integer truncation.
        let s = PubSubStats::new();
        s.record_deliver();
        s.record_deliver();
        s.record_delivery_failure();
        let snap = s.snapshot();
        assert_eq!(snap.delivery_success_rate_bps(), 6_666);
        assert_eq!(snap.delivery_failure_rate_bps(), 3_333);
    }
}
