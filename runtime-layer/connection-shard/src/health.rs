//! [`HealthReport`] — derived health status (Layer 13).
//!
//! Health is derived from the shard's current state (active count,
//! backpressure level, free slots, stats). It is a read-only summary
//! for monitoring — the shard does not make decisions based on it.

use crate::backpressure::BackpressureLevel;
use crate::stats::StatsSnapshot;

/// Top-level health state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthState {
    /// Shard is operating normally.
    Healthy,
    /// Approaching capacity or under soft backpressure.
    Degraded,
    /// Under hard backpressure or critically loaded.
    Critical,
}

impl HealthState {
    /// Whether this state requires immediate attention.
    #[inline]
    #[must_use]
    pub const fn is_actionable(self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }
}

/// A point-in-time health summary for a [`ConnectionShard`](crate::shard::ConnectionShard).
#[derive(Clone, Copy, Debug)]
pub struct HealthReport {
    /// Derived health state.
    pub state: HealthState,
    /// Current active connection count.
    pub active_connections: usize,
    /// Free slots remaining.
    pub free_slots: usize,
    /// Slab capacity.
    pub capacity: usize,
    /// Current backpressure level.
    pub backpressure: BackpressureLevel,
    /// Fill ratio in `[0.0, 1.0]`.
    pub fill_ratio: f64,
    /// Lookup miss rate in `[0.0, 1.0]`.
    pub lookup_miss_rate: f64,
}

impl HealthReport {
    /// Derives a health report from current shard metrics.
    #[must_use]
    pub fn derive(
        active: usize,
        capacity: usize,
        backpressure: BackpressureLevel,
        stats: &StatsSnapshot,
    ) -> Self {
        let free_slots = capacity.saturating_sub(active);
        let fill_ratio = if capacity == 0 {
            1.0
        } else {
            active as f64 / capacity as f64
        };
        let lookup_miss_rate = if stats.lookups_total == 0 {
            0.0
        } else {
            stats.lookups_missed as f64 / stats.lookups_total as f64
        };

        let state = if backpressure == BackpressureLevel::Hard || fill_ratio >= 0.99 {
            HealthState::Critical
        } else if backpressure == BackpressureLevel::Soft
            || fill_ratio >= 0.85
            || lookup_miss_rate > 0.1
        {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            active_connections: active,
            free_slots,
            capacity,
            backpressure,
            fill_ratio,
            lookup_miss_rate,
        }
    }

    /// Convenience: returns `true` if the shard is healthy.
    #[inline]
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.state == HealthState::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_shard_is_healthy() {
        let stats = StatsSnapshot::default();
        let h = HealthReport::derive(0, 65_536, BackpressureLevel::None, &stats);
        assert_eq!(h.state, HealthState::Healthy);
        assert!(h.is_healthy());
        assert_eq!(h.fill_ratio, 0.0);
    }

    #[test]
    fn soft_bp_is_degraded() {
        let stats = StatsSnapshot::default();
        let h = HealthReport::derive(1000, 65_536, BackpressureLevel::Soft, &stats);
        assert_eq!(h.state, HealthState::Degraded);
        assert!(h.state.is_actionable());
    }

    #[test]
    fn hard_bp_is_critical() {
        let stats = StatsSnapshot::default();
        let h = HealthReport::derive(1000, 65_536, BackpressureLevel::Hard, &stats);
        assert_eq!(h.state, HealthState::Critical);
        assert!(h.state.is_actionable());
    }

    #[test]
    fn high_fill_ratio_is_degraded() {
        let stats = StatsSnapshot::default();
        // 90% full → degraded
        let h = HealthReport::derive(90, 100, BackpressureLevel::None, &stats);
        assert_eq!(h.state, HealthState::Degraded);
    }

    #[test]
    fn near_full_is_critical() {
        let stats = StatsSnapshot::default();
        let h = HealthReport::derive(99, 100, BackpressureLevel::None, &stats);
        assert_eq!(h.state, HealthState::Critical);
    }

    #[test]
    fn high_lookup_miss_rate_degraded() {
        let stats = StatsSnapshot {
            lookups_total: 1000,
            lookups_missed: 150, // 15% miss rate
            ..StatsSnapshot::default()
        };
        let h = HealthReport::derive(100, 65_536, BackpressureLevel::None, &stats);
        assert_eq!(h.state, HealthState::Degraded);
    }

    #[test]
    fn zero_capacity_shown_as_full() {
        let stats = StatsSnapshot::default();
        let h = HealthReport::derive(0, 0, BackpressureLevel::None, &stats);
        assert_eq!(h.fill_ratio, 1.0);
    }
}
