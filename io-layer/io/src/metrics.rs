//! Atomic counters for the io_uring reactor.
//!
//! Every counter is lock-free (`AtomicU64` with `Relaxed` ordering — the
//! values are advisory and need no cross-thread synchronization beyond
//! atomicity itself). Snapshots are point-in-time and may move the moment
//! they are read; that is acceptable for observability.
//!
//! A future Prometheus exporter can call [`MetricsCollector::snapshot`]
//! on a timer; wiring that is deferred to a feature flag.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free metrics for one reactor.
#[derive(Debug, Default)]
pub struct MetricsCollector {
    cq_completions: AtomicU64,
    cq_overflows: AtomicU64,
    buffer_exhaustions: AtomicU64,
    bytes_rx: AtomicU64,
    bytes_tx: AtomicU64,
    submit_backpressures: AtomicU64,
    timer_cancels: AtomicU64,
}

/// Point-in-time copy of every counter.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Total CQEs reaped (success + failure).
    pub cq_completions: u64,
    /// Times the kernel reported CQ overflow.
    pub cq_overflows: u64,
    /// Times a buffer group ran dry mid-RX.
    pub buffer_exhaustions: u64,
    /// Aggregate bytes received.
    pub bytes_rx: u64,
    /// Aggregate bytes transmitted.
    pub bytes_tx: u64,
    /// Times the reactor observed `-EBUSY` on submit.
    pub submit_backpressures: u64,
    /// Timer cancellations issued.
    pub timer_cancels: u64,
}

impl MetricsCollector {
    /// Construct a fresh collector with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a CQE completion. On positive `res`, the value is added to
    /// the appropriate byte counter (`bytes_rx` for `is_rx`, else `bytes_tx`).
    pub fn record_cqe_completion(&self, res: i32, is_rx: bool) {
        self.cq_completions.fetch_add(1, Ordering::Relaxed);
        if res > 0 {
            let bytes = u64::try_from(res).unwrap_or(0);
            if is_rx {
                self.bytes_rx.fetch_add(bytes, Ordering::Relaxed);
            } else {
                self.bytes_tx.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// Record that the CQ overflowed (`cq.koverflow > 0`).
    pub fn record_cq_overflow(&self) {
        self.cq_overflows.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that buffer-group `bgid` ran out of spare buffers.
    pub fn track_buffer_exhaustion(&self, _bgid: u16) {
        self.buffer_exhaustions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record `n` bytes received out-of-band (e.g. via a non-CQE path).
    pub fn record_bytes_rx(&self, n: u64) {
        self.bytes_rx.fetch_add(n, Ordering::Relaxed);
    }

    /// Record `n` bytes transmitted out-of-band.
    pub fn record_bytes_tx(&self, n: u64) {
        self.bytes_tx.fetch_add(n, Ordering::Relaxed);
    }

    /// Record a backpressure event on submit.
    pub fn record_submit_backpressure(&self) {
        self.submit_backpressures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a timer was cancelled.
    pub fn record_timer_cancel(&self) {
        self.timer_cancels.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent snapshot of every counter.
    ///
    /// Counters are read in declared order; the snapshot is not atomic
    /// across counters, but each individual read is.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            cq_completions: self.cq_completions.load(Ordering::Relaxed),
            cq_overflows: self.cq_overflows.load(Ordering::Relaxed),
            buffer_exhaustions: self.buffer_exhaustions.load(Ordering::Relaxed),
            bytes_rx: self.bytes_rx.load(Ordering::Relaxed),
            bytes_tx: self.bytes_tx.load(Ordering::Relaxed),
            submit_backpressures: self.submit_backpressures.load(Ordering::Relaxed),
            timer_cancels: self.timer_cancels.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_matches_recorded_ops() {
        let m = MetricsCollector::new();
        m.record_cqe_completion(1024, true);
        m.record_cqe_completion(512, false);
        m.record_cqe_completion(-libc::EBUSY, true);
        m.record_cq_overflow();
        m.track_buffer_exhaustion(3);
        m.record_timer_cancel();
        m.record_submit_backpressure();
        m.record_submit_backpressure();

        let snap = m.snapshot();
        assert_eq!(snap.cq_completions, 3);
        assert_eq!(snap.bytes_rx, 1024);
        assert_eq!(snap.bytes_tx, 512);
        assert_eq!(snap.cq_overflows, 1);
        assert_eq!(snap.buffer_exhaustions, 1);
        assert_eq!(snap.timer_cancels, 1);
        assert_eq!(snap.submit_backpressures, 2);
    }

    #[test]
    fn negative_res_does_not_bump_bytes() {
        let m = MetricsCollector::new();
        m.record_cqe_completion(-5, true);
        m.record_cqe_completion(-5, false);
        let snap = m.snapshot();
        assert_eq!(snap.bytes_rx, 0);
        assert_eq!(snap.bytes_tx, 0);
        assert_eq!(snap.cq_completions, 2);
    }

    #[test]
    fn fresh_collector_is_all_zero() {
        let snap = MetricsCollector::new().snapshot();
        assert_eq!(snap, MetricsSnapshot::default());
    }
}
