//! [`RpcStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for RPC observability.
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

// ── RpcStats ─────────────────────────────────────────────────────

/// Atomic counters for the RPC framework.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`RpcStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct RpcStats {
    /// Total RPC calls attempted (counter).
    pub calls_total: AtomicU64,
    /// RPC calls that succeeded (counter).
    pub calls_succeeded: AtomicU64,
    /// RPC calls that failed (counter).
    pub calls_failed: AtomicU64,
    /// QUIC streams opened (counter).
    pub streams_opened: AtomicU64,
    /// QUIC streams closed (counter).
    pub streams_closed: AtomicU64,
    /// Calls that timed out (counter).
    pub timeouts: AtomicU64,
    /// Calls that were cancelled (counter).
    pub cancellations: AtomicU64,
    /// Bytes sent (counter).
    pub bytes_sent: AtomicU64,
    /// Bytes received (counter).
    pub bytes_received: AtomicU64,
}

impl Default for RpcStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            calls_total: AtomicU64::new(0),
            calls_succeeded: AtomicU64::new(0),
            calls_failed: AtomicU64::new(0),
            streams_opened: AtomicU64::new(0),
            streams_closed: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }

    /// Record a successful RPC call.
    pub fn record_call_success(&self) {
        self.calls_total.fetch_add(1, Ordering::Relaxed);
        self.calls_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed RPC call.
    pub fn record_call_failure(&self) {
        self.calls_total.fetch_add(1, Ordering::Relaxed);
        self.calls_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a stream being opened.
    pub fn record_stream_opened(&self) {
        self.streams_opened.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a stream being closed.
    pub fn record_stream_closed(&self) {
        self.streams_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timeout.
    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cancellation.
    pub fn record_cancellation(&self) {
        self.cancellations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record bytes sent.
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes received.
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields (e.g. `calls_total`
    /// could exceed `calls_succeeded + calls_failed` by 1 if a call
    /// is in flight). This is acceptable for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            calls_total: self.calls_total.load(Ordering::Relaxed),
            calls_succeeded: self.calls_succeeded.load(Ordering::Relaxed),
            calls_failed: self.calls_failed.load(Ordering::Relaxed),
            streams_opened: self.streams_opened.load(Ordering::Relaxed),
            streams_closed: self.streams_closed.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────────

/// Point-in-time snapshot of RPC counters.
///
/// Rates are precomputed as basis-points (bps, 1/100 of a percent)
/// so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total RPC calls attempted.
    pub calls_total: u64,
    /// RPC calls that succeeded.
    pub calls_succeeded: u64,
    /// RPC calls that failed.
    pub calls_failed: u64,
    /// QUIC streams opened.
    pub streams_opened: u64,
    /// QUIC streams closed.
    pub streams_closed: u64,
    /// Calls that timed out.
    pub timeouts: u64,
    /// Calls that were cancelled.
    pub cancellations: u64,
    /// Bytes sent.
    pub bytes_sent: u64,
    /// Bytes received.
    pub bytes_received: u64,
}

impl StatsSnapshot {
    /// Success rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no calls yet.
    #[must_use]
    pub const fn success_rate_bps(&self) -> u16 {
        if self.calls_total == 0 {
            return 10_000;
        }
        let rate = (self.calls_succeeded.saturating_mul(10_000)) / self.calls_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Failure rate in basis points (0..=10_000).
    /// Returns 0 when there are no calls yet.
    #[must_use]
    pub const fn failure_rate_bps(&self) -> u16 {
        if self.calls_total == 0 {
            return 0;
        }
        let rate = (self.calls_failed.saturating_mul(10_000)) / self.calls_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Whether there are currently active streams (opened > closed).
    #[must_use]
    pub const fn has_active_streams(&self) -> bool {
        self.streams_opened > self.streams_closed
    }

    /// Count of currently active streams.
    #[must_use]
    pub const fn active_streams(&self) -> u64 {
        self.streams_opened.saturating_sub(self.streams_closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = RpcStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_call_success_increments_counters() {
        let s = RpcStats::new();
        s.record_call_success();
        s.record_call_success();
        let snap = s.snapshot();
        assert_eq!(snap.calls_total, 2);
        assert_eq!(snap.calls_succeeded, 2);
        assert_eq!(snap.calls_failed, 0);
    }

    #[test]
    fn record_call_failure_increments_counters() {
        let s = RpcStats::new();
        s.record_call_failure();
        let snap = s.snapshot();
        assert_eq!(snap.calls_total, 1);
        assert_eq!(snap.calls_succeeded, 0);
        assert_eq!(snap.calls_failed, 1);
    }

    #[test]
    fn mixed_calls_counters() {
        let s = RpcStats::new();
        s.record_call_success();
        s.record_call_success();
        s.record_call_failure();
        let snap = s.snapshot();
        assert_eq!(snap.calls_total, 3);
        assert_eq!(snap.calls_succeeded, 2);
        assert_eq!(snap.calls_failed, 1);
    }

    #[test]
    fn stream_counters() {
        let s = RpcStats::new();
        s.record_stream_opened();
        s.record_stream_opened();
        s.record_stream_closed();
        let snap = s.snapshot();
        assert_eq!(snap.streams_opened, 2);
        assert_eq!(snap.streams_closed, 1);
        assert!(snap.has_active_streams());
        assert_eq!(snap.active_streams(), 1);
    }

    #[test]
    fn timeout_counter() {
        let s = RpcStats::new();
        s.record_timeout();
        s.record_timeout();
        assert_eq!(s.snapshot().timeouts, 2);
    }

    #[test]
    fn cancellation_counter() {
        let s = RpcStats::new();
        s.record_cancellation();
        assert_eq!(s.snapshot().cancellations, 1);
    }

    #[test]
    fn bytes_counters() {
        let s = RpcStats::new();
        s.record_bytes_sent(1000);
        s.record_bytes_received(2000);
        s.record_bytes_sent(500);
        let snap = s.snapshot();
        assert_eq!(snap.bytes_sent, 1500);
        assert_eq!(snap.bytes_received, 2000);
    }

    #[test]
    fn success_rate_with_no_calls_is_100_percent() {
        let s = RpcStats::new();
        assert_eq!(s.snapshot().success_rate_bps(), 10_000);
    }

    #[test]
    fn success_rate_calculates_correctly() {
        let s = RpcStats::new();
        s.record_call_success();
        s.record_call_success();
        s.record_call_success();
        s.record_call_failure();
        // 3/4 = 75% = 7500 bps
        assert_eq!(s.snapshot().success_rate_bps(), 7_500);
    }

    #[test]
    fn failure_rate_with_no_calls_is_0() {
        let s = RpcStats::new();
        assert_eq!(s.snapshot().failure_rate_bps(), 0);
    }

    #[test]
    fn failure_rate_calculates_correctly() {
        let s = RpcStats::new();
        s.record_call_success();
        s.record_call_failure();
        s.record_call_failure();
        // 2/3 = 66.67% = 6666 bps (integer division truncates)
        assert_eq!(s.snapshot().failure_rate_bps(), 6_666);
    }

    #[test]
    fn active_streams_calculation() {
        let s = RpcStats::new();
        s.record_stream_opened();
        s.record_stream_opened();
        s.record_stream_opened();
        s.record_stream_closed();
        assert_eq!(s.snapshot().active_streams(), 2);
        assert!(s.snapshot().has_active_streams());
    }

    #[test]
    fn no_active_streams_when_equal() {
        let s = RpcStats::new();
        s.record_stream_opened();
        s.record_stream_closed();
        assert!(!s.snapshot().has_active_streams());
        assert_eq!(s.snapshot().active_streams(), 0);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = RpcStats::new();
        s.record_call_success();
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_cloning_is_independent() {
        let s = RpcStats::new();
        s.record_call_success();
        let snap1 = s.snapshot();
        let snap2 = snap1;
        s.record_call_failure(); // This affects the stats, not the snapshots
        assert_eq!(snap1, snap2); // Snapshots are unchanged
        assert_eq!(snap1.calls_succeeded, 1);
        assert_eq!(snap1.calls_failed, 0);
    }

    #[test]
    fn success_rate_clamps_to_u16_max() {
        let mut snap = StatsSnapshot::default();
        snap.calls_total = 1;
        snap.calls_succeeded = u64::MAX;
        // Should clamp to u16::MAX, not overflow
        let rate = snap.success_rate_bps();
        assert_eq!(rate, u16::MAX);
    }

    #[test]
    fn failure_rate_clamps_to_u16_max() {
        let mut snap = StatsSnapshot::default();
        snap.calls_total = 1;
        snap.calls_failed = u64::MAX;
        // Should clamp to u16::MAX, not overflow
        let rate = snap.failure_rate_bps();
        assert_eq!(rate, u16::MAX);
    }

    #[test]
    fn all_counter_methods_independent() {
        let s = RpcStats::new();
        s.record_call_success();
        s.record_stream_opened();
        s.record_timeout();
        s.record_cancellation();
        s.record_bytes_sent(123);
        s.record_bytes_received(456);
        let snap = s.snapshot();
        assert_eq!(snap.calls_total, 1);
        assert_eq!(snap.streams_opened, 1);
        assert_eq!(snap.timeouts, 1);
        assert_eq!(snap.cancellations, 1);
        assert_eq!(snap.bytes_sent, 123);
        assert_eq!(snap.bytes_received, 456);
    }
}
