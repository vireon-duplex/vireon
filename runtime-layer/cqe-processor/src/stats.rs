//! Statistics (Layer 13/14).
//!
//! All counters are `AtomicU64` with `Ordering::Relaxed` — single-thread
//! access by the reactor, but the snapshot reader may run on a different
//! thread (e.g. metrics scraper). `StatsSnapshot` is `Copy` so readers
//! never see a torn snapshot.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free statistics collector. Updated by the dispatch handlers on the
/// reactor thread; snapshots may be taken from any thread.
#[derive(Default)]
pub struct StatsCollector {
    /// Total CQEs processed (mirrors `HotState::cqes_total`).
    pub cqes_total: AtomicU64,
    /// Per-OpKind dispatch histogram (mirrors `HotState::cqes_per_opkind`).
    pub cqes_per_opkind: [AtomicU64; crate::user_data::OP_KIND_COUNT],
    /// Total `drain()` invocations.
    pub drain_cycles: AtomicU64,
    /// Last drain batch size.
    pub last_drain_size: AtomicU64,
    /// CQ overflow events.
    pub cq_overflow_events: AtomicU64,
    /// CQ starvation events.
    pub cq_starvation_events: AtomicU64,
    /// Successful multishot Receive handlers invoked.
    pub recv_buffers_refilled: AtomicU64,
    /// SEND_ZC second-CQE notifications.
    pub send_buffers_recycled: AtomicU64,
    /// Multishot re-arm requests scheduled.
    pub multishot_rearms: AtomicU64,
    /// `-ENOBUFS` events (buffer group dry).
    pub buffer_exhaustion_events: AtomicU64,
    /// First CQE of SEND_ZC observed.
    pub zc_first_cqes: AtomicU64,
    /// Second CQE of SEND_ZC observed.
    pub zc_notifications: AtomicU64,
    /// Timeout completions.
    pub timer_fires: AtomicU64,
    /// Cancel completions.
    pub timer_cancels: AtomicU64,
    /// Generic error count.
    pub errors_total: AtomicU64,
}

impl StatsCollector {
    /// Construct a zeroed collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot all counters into a `Copy` struct.
    ///
    /// Counters are read with `Relaxed` ordering; the snapshot is
    /// point-in-time but not transactionally consistent across counters
    /// (which is acceptable for observability).
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        let cqes_per_opkind = self
            .cqes_per_opkind
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap_or([0; crate::user_data::OP_KIND_COUNT]);
        StatsSnapshot {
            cqes_total: self.cqes_total.load(Ordering::Relaxed),
            cqes_per_opkind,
            drain_cycles: self.drain_cycles.load(Ordering::Relaxed),
            last_drain_size: self.last_drain_size.load(Ordering::Relaxed),
            cq_overflow_events: self.cq_overflow_events.load(Ordering::Relaxed),
            cq_starvation_events: self.cq_starvation_events.load(Ordering::Relaxed),
            recv_buffers_refilled: self.recv_buffers_refilled.load(Ordering::Relaxed),
            send_buffers_recycled: self.send_buffers_recycled.load(Ordering::Relaxed),
            multishot_rearms: self.multishot_rearms.load(Ordering::Relaxed),
            buffer_exhaustion_events: self.buffer_exhaustion_events.load(Ordering::Relaxed),
            zc_first_cqes: self.zc_first_cqes.load(Ordering::Relaxed),
            zc_notifications: self.zc_notifications.load(Ordering::Relaxed),
            timer_fires: self.timer_fires.load(Ordering::Relaxed),
            timer_cancels: self.timer_cancels.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
        }
    }
}

impl std::fmt::Debug for StatsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsCollector")
            .field("cqes_total", &self.cqes_total.load(Ordering::Relaxed))
            .field("drain_cycles", &self.drain_cycles.load(Ordering::Relaxed))
            .finish()
    }
}

/// Consistent point-in-time snapshot of all counters. `Copy` so readers
/// never see a torn view.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub cqes_total: u64,
    pub cqes_per_opkind: [u64; crate::user_data::OP_KIND_COUNT],
    pub drain_cycles: u64,
    pub last_drain_size: u64,
    pub cq_overflow_events: u64,
    pub cq_starvation_events: u64,
    pub recv_buffers_refilled: u64,
    pub send_buffers_recycled: u64,
    pub multishot_rearms: u64,
    pub buffer_exhaustion_events: u64,
    pub zc_first_cqes: u64,
    pub zc_notifications: u64,
    pub timer_fires: u64,
    pub timer_cancels: u64,
    pub errors_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_starts_zero() {
        let s = StatsCollector::new();
        let snap = s.snapshot();
        assert_eq!(snap.cqes_total, 0);
        assert_eq!(snap.drain_cycles, 0);
    }

    #[test]
    fn snapshot_reflects_increments() {
        let s = StatsCollector::new();
        s.cqes_total.store(42, Ordering::Relaxed);
        s.zc_notifications.store(7, Ordering::Relaxed);
        let snap = s.snapshot();
        assert_eq!(snap.cqes_total, 42);
        assert_eq!(snap.zc_notifications, 7);
    }
}
