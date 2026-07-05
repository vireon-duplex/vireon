//! Stats counters and snapshot (Layer 10).
//!
//! [`StatsCollector`] holds atomic counters updated on every state
//! transition. [`StatsSnapshot`] is a `Copy` struct for cheap point-in-time
//! reads.

use std::sync::atomic::{AtomicU64, Ordering};

/// Point-in-time snapshot of send-queue metrics.
#[derive(Clone, Copy, Debug, Default)]
pub struct StatsSnapshot {
    // ── Counters (monotonically increasing) ──
    pub submit_total: u64,
    pub submit_success_total: u64,
    pub submit_rejected_total: u64,
    pub cancel_total: u64,
    pub flush_total: u64,
    pub packets_submitted_total: u64,
    pub send_cqe_total: u64,
    pub reclaim_cqe_total: u64,
    pub retry_total: u64,
    pub drop_total: u64,

    // ── Gauges (instantaneous) ──
    pub queue_depth: usize,
    pub inflight_count: usize,
    pub batch_pending: usize,
    pub pressure_level: u8,
}

/// Atomic counter collection for the send queue.
#[derive(Debug)]
pub struct StatsCollector {
    submit_total: AtomicU64,
    submit_success_total: AtomicU64,
    submit_rejected_total: AtomicU64,
    cancel_total: AtomicU64,
    flush_total: AtomicU64,
    packets_submitted_total: AtomicU64,
    send_cqe_total: AtomicU64,
    reclaim_cqe_total: AtomicU64,
    retry_total: AtomicU64,
    drop_total: AtomicU64,
}

impl StatsCollector {
    /// Creates a zeroed collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            submit_total: AtomicU64::new(0),
            submit_success_total: AtomicU64::new(0),
            submit_rejected_total: AtomicU64::new(0),
            cancel_total: AtomicU64::new(0),
            flush_total: AtomicU64::new(0),
            packets_submitted_total: AtomicU64::new(0),
            send_cqe_total: AtomicU64::new(0),
            reclaim_cqe_total: AtomicU64::new(0),
            retry_total: AtomicU64::new(0),
            drop_total: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record_submit(&self) {
        self.submit_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_submit_success(&self) {
        self.submit_success_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_submit_rejected(&self) {
        self.submit_rejected_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cancel(&self) {
        self.cancel_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_flush(&self, packets: u64) {
        self.flush_total.fetch_add(1, Ordering::Relaxed);
        self.packets_submitted_total
            .fetch_add(packets, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_send_cqe(&self) {
        self.send_cqe_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_reclaim_cqe(&self) {
        self.reclaim_cqe_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_retry(&self) {
        self.retry_total.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_drop(&self) {
        self.drop_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Captures a point-in-time snapshot.
    ///
    /// `queue_depth`, `inflight_count`, `batch_pending`, and
    /// `pressure_level` are provided by the caller (they are not atomic
    /// counters — they are instantaneous values from the queue state).
    #[must_use]
    pub fn snapshot(
        &self,
        queue_depth: usize,
        inflight_count: usize,
        batch_pending: usize,
        pressure_level: u8,
    ) -> StatsSnapshot {
        StatsSnapshot {
            submit_total: self.submit_total.load(Ordering::Relaxed),
            submit_success_total: self.submit_success_total.load(Ordering::Relaxed),
            submit_rejected_total: self.submit_rejected_total.load(Ordering::Relaxed),
            cancel_total: self.cancel_total.load(Ordering::Relaxed),
            flush_total: self.flush_total.load(Ordering::Relaxed),
            packets_submitted_total: self.packets_submitted_total.load(Ordering::Relaxed),
            send_cqe_total: self.send_cqe_total.load(Ordering::Relaxed),
            reclaim_cqe_total: self.reclaim_cqe_total.load(Ordering::Relaxed),
            retry_total: self.retry_total.load(Ordering::Relaxed),
            drop_total: self.drop_total.load(Ordering::Relaxed),
            queue_depth,
            inflight_count,
            batch_pending,
            pressure_level,
        }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_collector_is_zeroed() {
        let s = StatsCollector::new();
        let snap = s.snapshot(0, 0, 0, 0);
        assert_eq!(snap.submit_total, 0);
        assert_eq!(snap.flush_total, 0);
        assert_eq!(snap.drop_total, 0);
    }

    #[test]
    fn record_submit_increments() {
        let s = StatsCollector::new();
        s.record_submit();
        s.record_submit_success();
        s.record_submit();
        s.record_submit_rejected();
        let snap = s.snapshot(0, 0, 0, 0);
        assert_eq!(snap.submit_total, 2);
        assert_eq!(snap.submit_success_total, 1);
        assert_eq!(snap.submit_rejected_total, 1);
    }

    #[test]
    fn record_flush_increments_packets() {
        let s = StatsCollector::new();
        s.record_flush(32);
        s.record_flush(16);
        let snap = s.snapshot(0, 0, 0, 0);
        assert_eq!(snap.flush_total, 2);
        assert_eq!(snap.packets_submitted_total, 48);
    }

    #[test]
    fn snapshot_captures_gauges() {
        let s = StatsCollector::new();
        let snap = s.snapshot(100, 50, 8, 1);
        assert_eq!(snap.queue_depth, 100);
        assert_eq!(snap.inflight_count, 50);
        assert_eq!(snap.batch_pending, 8);
        assert_eq!(snap.pressure_level, 1);
    }
}
