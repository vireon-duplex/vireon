//! [`HealthReport`] + [`HealthState`] — derived from
//! [`StatsSnapshot`] and reactor state.
//!
//! See `DESIGN.md` §17 (observability).
//!
//! Mirrors the pattern of `connection_shard::health::HealthReport`
//! and `quic_engine::health::HealthReport`.
//!
//! # States
//!
//! - **Healthy** — no overflow, mean tick latency < 1 ms.
//! - **Degraded** — any overflow event in the last window, or mean
//!   tick latency in 1–10 ms.
//! - **Critical** — mean tick latency > 10 ms (the reactor is
//!   saturated and missing deadlines).

use crate::stats::StatsSnapshot;

/// Tri-state classification of reactor health.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HealthState {
    /// All metrics nominal.
    #[default]
    Healthy,
    /// Some metric above soft threshold (latency 1–10 ms, overflow > 0).
    Degraded,
    /// Some metric above hard threshold (latency > 10 ms).
    Critical,
}

impl HealthState {
    /// Whether the state requires operator action.
    #[must_use]
    pub const fn is_actionable(self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }
}

/// Health report derived from a [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall state.
    pub state: HealthState,
    /// Mean tick latency in nanoseconds (copied from snapshot).
    pub mean_tick_latency_ns: u64,
    /// Number of overflow events observed.
    pub overflow_events: u64,
}

/// Soft threshold for mean tick latency (1 ms in ns).
pub const DEGRADED_LATENCY_NS: u64 = 1_000_000;
/// Hard threshold for mean tick latency (10 ms in ns).
pub const CRITICAL_LATENCY_NS: u64 = 10_000_000;

impl HealthReport {
    /// Derive a health report from the current stats snapshot.
    ///
    /// Phase-0 heuristic:
    /// - Critical if mean tick latency > 10 ms.
    /// - Degraded if mean tick latency > 1 ms OR overflow_events > 0.
    /// - Healthy otherwise.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let mean = stats.mean_tick_latency_ns();
        let state = if mean > CRITICAL_LATENCY_NS {
            HealthState::Critical
        } else if mean > DEGRADED_LATENCY_NS || stats.overflow_events > 0 {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            mean_tick_latency_ns: mean,
            overflow_events: stats.overflow_events,
        }
    }

    /// Convenience predicate.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self.state, HealthState::Healthy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(latency_sum: u64, ticks: u64, overflow: u64) -> StatsSnapshot {
        StatsSnapshot {
            ticks_total: ticks,
            drain_latency_ns_sum: latency_sum,
            overflow_events: overflow,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn derive_healthy_on_default() {
        let r = HealthReport::derive(&StatsSnapshot::default());
        assert!(r.is_healthy());
    }

    #[test]
    fn derive_degraded_on_overflow() {
        let r = HealthReport::derive(&snap(0, 1, 1));
        assert_eq!(r.state, HealthState::Degraded);
    }

    #[test]
    fn derive_degraded_on_soft_latency() {
        // mean = 1.5 ms > 1 ms soft threshold
        let r = HealthReport::derive(&snap(1_500_000, 1, 0));
        assert_eq!(r.state, HealthState::Degraded);
    }

    #[test]
    fn derive_critical_on_hard_latency() {
        // mean = 11 ms > 10 ms hard threshold
        let r = HealthReport::derive(&snap(11_000_000, 1, 0));
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn derive_critical_wins_over_overflow() {
        // latency is critical, overflow is also set — Critical wins.
        let r = HealthReport::derive(&snap(11_000_000, 1, 5));
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn actionable_states() {
        assert!(!HealthState::Healthy.is_actionable());
        assert!(HealthState::Degraded.is_actionable());
        assert!(HealthState::Critical.is_actionable());
    }
}
