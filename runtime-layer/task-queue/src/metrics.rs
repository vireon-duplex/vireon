//! MetricsCollector — atomic counters + snapshot (Layer 21).

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counter collection. All updates use `Relaxed` ordering
/// (we don't need cross-field consistency, only eventual visibility).
pub struct MetricsCollector {
    spawned: AtomicU64,
    executed: AtomicU64,
    cancelled: AtomicU64,
    expired: AtomicU64,
    rejected: AtomicU64,
    rescheduled: AtomicU64,
    failed: AtomicU64,
    ticks: AtomicU64,
    overruns: AtomicU64,
    inbox_pushes: AtomicU64,
}

impl MetricsCollector {
    /// Creates a zeroed collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            spawned: AtomicU64::new(0),
            executed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            rescheduled: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
            overruns: AtomicU64::new(0),
            inbox_pushes: AtomicU64::new(0),
        }
    }

    // ── Recorders ──────────────────────────────────────────────────

    #[inline]
    pub fn record_spawned(&self) {
        self.spawned.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_executed(&self) {
        self.executed.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_cancelled(&self) {
        self.cancelled.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_expired(&self) {
        self.expired.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_rescheduled(&self) {
        self.rescheduled.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_tick(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_overrun(&self) {
        self.overruns.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    pub fn record_inbox_push(&self) {
        self.inbox_pushes.fetch_add(1, Ordering::Relaxed);
    }

    // ── Accessors (for tests + snapshot) ────────────────────────────

    #[must_use]
    pub fn spawned_total(&self) -> u64 {
        self.spawned.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn executed_total(&self) -> u64 {
        self.executed.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn cancelled_total(&self) -> u64 {
        self.cancelled.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn expired_total(&self) -> u64 {
        self.expired.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn rejected_total(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn rescheduled_total(&self) -> u64 {
        self.rescheduled.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn failed_total(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn ticks_total(&self) -> u64 {
        self.ticks.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn overruns_total(&self) -> u64 {
        self.overruns.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn inbox_pushes_total(&self) -> u64 {
        self.inbox_pushes.load(Ordering::Relaxed)
    }

    /// Captures all counters into a [`MetricsSnapshot`].
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            spawned: self.spawned_total(),
            executed: self.executed_total(),
            cancelled: self.cancelled_total(),
            expired: self.expired_total(),
            rejected: self.rejected_total(),
            rescheduled: self.rescheduled_total(),
            failed: self.failed_total(),
            ticks: self.ticks_total(),
            overruns: self.overruns_total(),
            inbox_pushes: self.inbox_pushes_total(),
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.snapshot();
        f.debug_struct("MetricsCollector")
            .field("spawned", &s.spawned)
            .field("executed", &s.executed)
            .field("ticks", &s.ticks)
            .finish()
    }
}

/// Point-in-time copy of all counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub spawned: u64,
    pub executed: u64,
    pub cancelled: u64,
    pub expired: u64,
    pub rejected: u64,
    pub rescheduled: u64,
    pub failed: u64,
    pub ticks: u64,
    pub overruns: u64,
    pub inbox_pushes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        let m = MetricsCollector::new();
        m.record_spawned();
        m.record_spawned();
        m.record_executed();
        m.record_tick();
        assert_eq!(m.spawned_total(), 2);
        assert_eq!(m.executed_total(), 1);
        assert_eq!(m.ticks_total(), 1);
    }

    #[test]
    fn snapshot_reflects_state() {
        let m = MetricsCollector::new();
        for _ in 0..10 {
            m.record_spawned();
        }
        for _ in 0..7 {
            m.record_executed();
        }
        let s = m.snapshot();
        assert_eq!(s.spawned, 10);
        assert_eq!(s.executed, 7);
        assert_eq!(s.cancelled, 0);
    }

    #[test]
    fn default_is_zeroed() {
        let m = MetricsCollector::default();
        let s = m.snapshot();
        assert_eq!(s, MetricsSnapshot::default());
    }
}
