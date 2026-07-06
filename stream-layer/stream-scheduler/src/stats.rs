//! [`SchedulerStats`] + [`StatsSnapshot`] — atomic counters for the
//! stream scheduler.
//!
//! See `DESIGN.md` Part 15 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of urgency bands (RFC 9218: 0..=7).
pub const URGENCY_BANDS: usize = 8;

/// Scheduler-wide atomic counters.
#[derive(Debug)]
pub struct SchedulerStats {
    /// Total number of successful stream selections.
    pub selection_count: AtomicU64,
    /// Total bytes scheduled across all ticks.
    pub bytes_scheduled: AtomicU64,
    /// Total number of starvation events (aging promotions).
    pub starvation_count: AtomicU64,
    /// Total backpressure stalls (no ready, unblocked stream).
    pub backpressure_stalls: AtomicU64,
    /// Sum of queue depths observed at tick start (for averaging).
    pub queue_depth_sum: AtomicU64,
    /// Number of ticks observed (for averaging queue_depth_sum).
    pub tick_count: AtomicU64,
    /// Total number of fairness checks performed.
    pub fairness_check_count: AtomicU64,
    /// Per-band selection counts. Indexed by urgency (0..7).
    pub priority_distribution: [AtomicU64; URGENCY_BANDS],
}

impl Default for SchedulerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerStats {
    /// Construct zeroed stats.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            selection_count: AtomicU64::new(0),
            bytes_scheduled: AtomicU64::new(0),
            starvation_count: AtomicU64::new(0),
            backpressure_stalls: AtomicU64::new(0),
            queue_depth_sum: AtomicU64::new(0),
            tick_count: AtomicU64::new(0),
            fairness_check_count: AtomicU64::new(0),
            priority_distribution: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    /// Record a successful stream selection.
    pub fn record_selection(&self, urgency: u8, bytes: u64) {
        self.selection_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_scheduled.fetch_add(bytes, Ordering::Relaxed);
        if (urgency as usize) < URGENCY_BANDS {
            self.priority_distribution[urgency as usize].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a starvation event (aging promotion).
    pub fn record_starvation(&self) {
        self.starvation_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a backpressure stall.
    pub fn record_backpressure_stall(&self) {
        self.backpressure_stalls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the queue depth at the start of a tick.
    pub fn record_tick(&self, queue_depth: u64) {
        self.queue_depth_sum
            .fetch_add(queue_depth, Ordering::Relaxed);
        self.tick_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a fairness check was performed.
    pub fn record_fairness_check(&self) {
        self.fairness_check_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot all counters atomically (per-field, not
    /// cross-field consistent).
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        let priority_distribution: [u64; URGENCY_BANDS] = [
            self.priority_distribution[0].load(Ordering::Relaxed),
            self.priority_distribution[1].load(Ordering::Relaxed),
            self.priority_distribution[2].load(Ordering::Relaxed),
            self.priority_distribution[3].load(Ordering::Relaxed),
            self.priority_distribution[4].load(Ordering::Relaxed),
            self.priority_distribution[5].load(Ordering::Relaxed),
            self.priority_distribution[6].load(Ordering::Relaxed),
            self.priority_distribution[7].load(Ordering::Relaxed),
        ];
        StatsSnapshot {
            selection_count: self.selection_count.load(Ordering::Relaxed),
            bytes_scheduled: self.bytes_scheduled.load(Ordering::Relaxed),
            starvation_count: self.starvation_count.load(Ordering::Relaxed),
            backpressure_stalls: self.backpressure_stalls.load(Ordering::Relaxed),
            queue_depth_sum: self.queue_depth_sum.load(Ordering::Relaxed),
            tick_count: self.tick_count.load(Ordering::Relaxed),
            fairness_check_count: self.fairness_check_count.load(Ordering::Relaxed),
            priority_distribution,
        }
    }
}

/// `Copy` view of [`SchedulerStats`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total successful selections.
    pub selection_count: u64,
    /// Total bytes scheduled.
    pub bytes_scheduled: u64,
    /// Total starvation events.
    pub starvation_count: u64,
    /// Total backpressure stalls.
    pub backpressure_stalls: u64,
    /// Sum of queue depths observed.
    pub queue_depth_sum: u64,
    /// Number of ticks observed.
    pub tick_count: u64,
    /// Total fairness checks performed.
    pub fairness_check_count: u64,
    /// Per-band selection counts (index = urgency 0..7).
    pub priority_distribution: [u64; URGENCY_BANDS],
}

impl StatsSnapshot {
    /// Average queue depth over all observed ticks.
    #[must_use]
    pub fn mean_queue_depth(&self) -> f64 {
        if self.tick_count == 0 {
            0.0
        } else {
            self.queue_depth_sum as f64 / self.tick_count as f64
        }
    }

    /// Average bytes per selection.
    #[must_use]
    pub fn mean_bytes_per_selection(&self) -> f64 {
        if self.selection_count == 0 {
            0.0
        } else {
            self.bytes_scheduled as f64 / self.selection_count as f64
        }
    }

    /// Total selections in the given urgency band.
    #[must_use]
    pub const fn selections_in_band(&self, urgency: u8) -> u64 {
        if (urgency as usize) < URGENCY_BANDS {
            self.priority_distribution[urgency as usize]
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_selection_updates_band() {
        let s = SchedulerStats::new();
        s.record_selection(0, 1000);
        s.record_selection(0, 500);
        s.record_selection(3, 2000);
        let snap = s.snapshot();
        assert_eq!(snap.selection_count, 3);
        assert_eq!(snap.bytes_scheduled, 3500);
        assert_eq!(snap.selections_in_band(0), 2);
        assert_eq!(snap.selections_in_band(3), 1);
    }

    #[test]
    fn mean_queue_depth_handles_zero() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.mean_queue_depth(), 0.0);
    }

    #[test]
    fn mean_queue_depth_computes() {
        let snap = StatsSnapshot {
            queue_depth_sum: 1000,
            tick_count: 10,
            ..StatsSnapshot::default()
        };
        assert!((snap.mean_queue_depth() - 100.0).abs() < 1e-9);
    }

    #[test]
    fn starvation_and_stalls_tracked() {
        let s = SchedulerStats::new();
        s.record_starvation();
        s.record_starvation();
        s.record_backpressure_stall();
        let snap = s.snapshot();
        assert_eq!(snap.starvation_count, 2);
        assert_eq!(snap.backpressure_stalls, 1);
    }

    #[test]
    fn mean_bytes_per_selection() {
        let snap = StatsSnapshot {
            bytes_scheduled: 5000,
            selection_count: 100,
            ..StatsSnapshot::default()
        };
        assert!((snap.mean_bytes_per_selection() - 50.0).abs() < 1e-9);
    }
}
