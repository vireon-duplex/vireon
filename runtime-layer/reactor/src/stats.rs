//! [`ReactorStats`] — atomic counters maintained by the reactor,
//! and [`StatsSnapshot`] — a `Copy` view used by health derivation
//! and external metrics exporters.
//!
//! See `DESIGN.md` §17 (observability) for the full metric
//! inventory and §20 (production review) for the "stats are
//! hot cache lines" caveat.

use std::sync::atomic::{AtomicU64, Ordering};

/// Reactor-wide atomic counters. Each field is on its own cache
/// line conceptually (Phase 5 will add `#[repr(C, align(64))]` to
/// prevent false sharing between writers).
#[derive(Debug, Default)]
pub struct ReactorStats {
    /// Total ticks since reactor start.
    pub ticks_total: AtomicU64,
    /// Total datagrams received.
    pub packets_recv: AtomicU64,
    /// Total datagrams submitted (SEND_ZC / SENDMMSG).
    pub packets_sent: AtomicU64,
    /// Total tasks executed from the task queue.
    pub tasks_executed: AtomicU64,
    /// Total timers that fired.
    pub timers_fired: AtomicU64,
    /// Number of CQ overflow events (kernel dropped CQEs).
    pub overflow_events: AtomicU64,
    /// Number of cross-thread wakeups received via eventfd.
    pub wakeups: AtomicU64,
    /// Sum of per-tick latency in nanoseconds (divide by
    /// `ticks_total` for mean tick latency).
    pub drain_latency_ns_sum: AtomicU64,
}

impl ReactorStats {
    /// Construct a zeroed stats collector.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ticks_total: AtomicU64::new(0),
            packets_recv: AtomicU64::new(0),
            packets_sent: AtomicU64::new(0),
            tasks_executed: AtomicU64::new(0),
            timers_fired: AtomicU64::new(0),
            overflow_events: AtomicU64::new(0),
            wakeups: AtomicU64::new(0),
            drain_latency_ns_sum: AtomicU64::new(0),
        }
    }

    /// Record one tick's outcome.
    pub fn record_tick(&self, r: &crate::tick::TickResult) {
        self.ticks_total.fetch_add(1, Ordering::Relaxed);
        self.packets_recv
            .fetch_add(u64::from(r.packets_recv), Ordering::Relaxed);
        self.packets_sent
            .fetch_add(u64::from(r.packets_sent), Ordering::Relaxed);
        self.tasks_executed
            .fetch_add(u64::from(r.tasks_run), Ordering::Relaxed);
        self.timers_fired
            .fetch_add(u64::from(r.timers_fired), Ordering::Relaxed);
        self.drain_latency_ns_sum
            .fetch_add(r.latency_ns, Ordering::Relaxed);
        if r.overflow {
            self.overflow_events.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an eventfd wake.
    pub fn record_wakeup(&self) {
        self.wakeups.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot all counters atomically (each field independently,
    /// not cross-field consistent).
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            ticks_total: self.ticks_total.load(Ordering::Relaxed),
            packets_recv: self.packets_recv.load(Ordering::Relaxed),
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            tasks_executed: self.tasks_executed.load(Ordering::Relaxed),
            timers_fired: self.timers_fired.load(Ordering::Relaxed),
            overflow_events: self.overflow_events.load(Ordering::Relaxed),
            wakeups: self.wakeups.load(Ordering::Relaxed),
            drain_latency_ns_sum: self.drain_latency_ns_sum.load(Ordering::Relaxed),
        }
    }
}

/// `Copy` view of [`ReactorStats`]. Used by
/// [`HealthReport::derive`](crate::health::HealthReport::derive)
/// and (Phase 17) Prometheus exporters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total ticks since reactor start.
    pub ticks_total: u64,
    /// Total datagrams received.
    pub packets_recv: u64,
    /// Total datagrams submitted.
    pub packets_sent: u64,
    /// Total tasks executed.
    pub tasks_executed: u64,
    /// Total timers fired.
    pub timers_fired: u64,
    /// CQ overflow events.
    pub overflow_events: u64,
    /// Cross-thread wakeups.
    pub wakeups: u64,
    /// Sum of per-tick latency (ns).
    pub drain_latency_ns_sum: u64,
}

impl StatsSnapshot {
    /// Mean tick latency in nanoseconds (0 if no ticks yet).
    #[must_use]
    pub fn mean_tick_latency_ns(&self) -> u64 {
        if self.ticks_total == 0 {
            0
        } else {
            self.drain_latency_ns_sum / self.ticks_total
        }
    }

    /// Packets-per-second estimate, given an elapsed time in nanoseconds.
    #[must_use]
    pub fn packets_per_sec(&self, elapsed_ns: u64) -> u64 {
        if elapsed_ns == 0 {
            0
        } else {
            self.packets_recv.saturating_mul(1_000_000_000) / elapsed_ns
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tick::TickResult;

    #[test]
    fn record_tick_aggregates() {
        let s = ReactorStats::new();
        s.record_tick(&TickResult {
            drained: 4,
            packets_recv: 3,
            packets_sent: 2,
            tasks_run: 1,
            timers_fired: 1,
            latency_ns: 1000,
            overflow: false,
            dispatched: 4,
        });
        s.record_tick(&TickResult {
            drained: 1,
            packets_recv: 1,
            packets_sent: 0,
            tasks_run: 0,
            timers_fired: 0,
            latency_ns: 3000,
            overflow: true,
            dispatched: 1,
        });
        let snap = s.snapshot();
        assert_eq!(snap.ticks_total, 2);
        assert_eq!(snap.packets_recv, 4);
        assert_eq!(snap.packets_sent, 2);
        assert_eq!(snap.tasks_executed, 1);
        assert_eq!(snap.timers_fired, 1);
        assert_eq!(snap.overflow_events, 1);
        assert_eq!(snap.drain_latency_ns_sum, 4000);
    }

    #[test]
    fn mean_tick_latency_handles_zero() {
        let s = StatsSnapshot::default();
        assert_eq!(s.mean_tick_latency_ns(), 0);
    }

    #[test]
    fn mean_tick_latency_divides() {
        let s = StatsSnapshot {
            ticks_total: 4,
            drain_latency_ns_sum: 4000,
            ..StatsSnapshot::default()
        };
        assert_eq!(s.mean_tick_latency_ns(), 1000);
    }

    #[test]
    fn wakeup_counter_increments() {
        let s = ReactorStats::new();
        s.record_wakeup();
        s.record_wakeup();
        assert_eq!(s.snapshot().wakeups, 2);
    }
}
