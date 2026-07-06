//! [`StreamStats`] + [`StatsSnapshot`] — atomic counters for the
//! stream manager.
//!
//! See `DESIGN.md` Part 15 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

/// Stream-manager-wide atomic counters.
#[derive(Debug, Default)]
pub struct StreamStats {
    /// Total streams created since manager start.
    pub streams_created_total: AtomicU64,
    /// Total streams destroyed since manager start.
    pub streams_destroyed_total: AtomicU64,
    /// Currently-active stream count.
    pub streams_active: AtomicU64,
    /// Count of times a send path blocked on flow-control credit.
    pub send_blocked_total: AtomicU64,
    /// Count of times a recv path blocked waiting for reordering.
    pub recv_blocked_total: AtomicU64,
    /// Count of RESET_STREAM sent to the peer.
    pub resets_sent_total: AtomicU64,
    /// Count of RESET_STREAM received from the peer.
    pub resets_received_total: AtomicU64,
    /// Total ns the manager has spent in flow-control stalls.
    pub fc_stall_ns_total: AtomicU64,
}

impl StreamStats {
    /// Construct zeroed stats.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            streams_created_total: AtomicU64::new(0),
            streams_destroyed_total: AtomicU64::new(0),
            streams_active: AtomicU64::new(0),
            send_blocked_total: AtomicU64::new(0),
            recv_blocked_total: AtomicU64::new(0),
            resets_sent_total: AtomicU64::new(0),
            resets_received_total: AtomicU64::new(0),
            fc_stall_ns_total: AtomicU64::new(0),
        }
    }

    /// Record that a stream was created.
    pub fn record_created(&self) {
        self.streams_created_total.fetch_add(1, Ordering::Relaxed);
        self.streams_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a stream was destroyed.
    pub fn record_destroyed(&self) {
        self.streams_destroyed_total.fetch_add(1, Ordering::Relaxed);
        self.streams_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a send-side flow-control block.
    pub fn record_send_blocked(&self, stall_ns: u64) {
        self.send_blocked_total.fetch_add(1, Ordering::Relaxed);
        self.fc_stall_ns_total
            .fetch_add(stall_ns, Ordering::Relaxed);
    }

    /// Record a recv-side reorder stall.
    pub fn record_recv_blocked(&self) {
        self.recv_blocked_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an outbound RESET_STREAM.
    pub fn record_reset_sent(&self) {
        self.resets_sent_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an inbound RESET_STREAM.
    pub fn record_reset_received(&self) {
        self.resets_received_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot all counters atomically (per-field, not
    /// cross-field consistent).
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            streams_created_total: self.streams_created_total.load(Ordering::Relaxed),
            streams_destroyed_total: self.streams_destroyed_total.load(Ordering::Relaxed),
            streams_active: self.streams_active.load(Ordering::Relaxed),
            send_blocked_total: self.send_blocked_total.load(Ordering::Relaxed),
            recv_blocked_total: self.recv_blocked_total.load(Ordering::Relaxed),
            resets_sent_total: self.resets_sent_total.load(Ordering::Relaxed),
            resets_received_total: self.resets_received_total.load(Ordering::Relaxed),
            fc_stall_ns_total: self.fc_stall_ns_total.load(Ordering::Relaxed),
        }
    }
}

/// `Copy` view of [`StreamStats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Cumulative created count.
    pub streams_created_total: u64,
    /// Cumulative destroyed count.
    pub streams_destroyed_total: u64,
    /// Currently-active count.
    pub streams_active: u64,
    /// Send-side block count.
    pub send_blocked_total: u64,
    /// Recv-side block count.
    pub recv_blocked_total: u64,
    /// Outbound resets.
    pub resets_sent_total: u64,
    /// Inbound resets.
    pub resets_received_total: u64,
    /// Total ns in flow-control stalls.
    pub fc_stall_ns_total: u64,
}

impl StatsSnapshot {
    /// Stream creation rate per second, given an elapsed window in
    /// nanoseconds.
    #[must_use]
    pub fn creation_rate_per_sec(&self, elapsed_ns: u64) -> u64 {
        if elapsed_ns == 0 {
            0
        } else {
            self.streams_created_total.saturating_mul(1_000_000_000) / elapsed_ns
        }
    }

    /// Average flow-control stall duration in nanoseconds.
    #[must_use]
    pub fn mean_fc_stall_ns(&self) -> u64 {
        if self.send_blocked_total == 0 {
            0
        } else {
            self.fc_stall_ns_total / self.send_blocked_total
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_created_increments_active() {
        let s = StreamStats::new();
        s.record_created();
        s.record_created();
        let snap = s.snapshot();
        assert_eq!(snap.streams_created_total, 2);
        assert_eq!(snap.streams_active, 2);
    }

    #[test]
    fn record_destroyed_decrements_active() {
        let s = StreamStats::new();
        s.record_created();
        s.record_created();
        s.record_destroyed();
        let snap = s.snapshot();
        assert_eq!(snap.streams_active, 1);
        assert_eq!(snap.streams_destroyed_total, 1);
    }

    #[test]
    fn record_send_blocked_aggregates_stall() {
        let s = StreamStats::new();
        s.record_send_blocked(1_000);
        s.record_send_blocked(3_000);
        let snap = s.snapshot();
        assert_eq!(snap.send_blocked_total, 2);
        assert_eq!(snap.fc_stall_ns_total, 4_000);
        assert_eq!(snap.mean_fc_stall_ns(), 2_000);
    }

    #[test]
    fn creation_rate_handles_zero_elapsed() {
        let s = StatsSnapshot {
            streams_created_total: 5,
            ..StatsSnapshot::default()
        };
        assert_eq!(s.creation_rate_per_sec(0), 0);
    }

    #[test]
    fn creation_rate_computes_per_second() {
        let s = StatsSnapshot {
            streams_created_total: 100,
            ..StatsSnapshot::default()
        };
        // 100 streams in 1 second (1_000_000_000 ns)
        assert_eq!(s.creation_rate_per_sec(1_000_000_000), 100);
    }

    #[test]
    fn resets_tracked_separately() {
        let s = StreamStats::new();
        s.record_reset_sent();
        s.record_reset_received();
        let snap = s.snapshot();
        assert_eq!(snap.resets_sent_total, 1);
        assert_eq!(snap.resets_received_total, 1);
    }
}
