//! [`HealthReport`] + [`HealthState`] — derived from
//! [`StatsSnapshot`](crate::stats::StatsSnapshot).
//!
//! See `DESIGN.md` Part 15 (observability).

use crate::stats::StatsSnapshot;

/// Tri-state classification of scheduler health.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HealthState {
    /// All metrics nominal; fairness within tolerance.
    #[default]
    Healthy,
    /// Fairness below soft threshold or sustained backpressure.
    Degraded,
    /// Active starvation detected or fairness critically low.
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
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HealthReport {
    /// Overall state.
    pub state: HealthState,
    /// Latest Jain fairness index (0.0..=1.0) if available.
    pub fairness_index: f64,
    /// Total starvation events observed.
    pub starvation_count: u64,
    /// Total backpressure stalls observed.
    pub backpressure_stalls: u64,
    /// Mean queue depth over the observed window.
    pub mean_queue_depth: f64,
}

/// Jain fairness index below which health enters `Degraded`.
pub const DEGRADED_FAIRNESS: f64 = 0.7;
/// Jain fairness index below which health enters `Critical`.
pub const CRITICAL_FAIRNESS: f64 = 0.4;

impl HealthReport {
    /// Derive a health report from the current stats snapshot and
    /// the latest fairness index.
    ///
    /// Phase-0 heuristic (see `DESIGN.md` Part 15):
    /// - Critical if `starvation_count > 0` or `fairness < 0.4`.
    /// - Degraded if `fairness < 0.7` or `backpressure_stalls > 0`.
    /// - Healthy otherwise.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot, fairness_index: f64) -> Self {
        let state = if stats.starvation_count > 0 || fairness_index < CRITICAL_FAIRNESS {
            HealthState::Critical
        } else if fairness_index < DEGRADED_FAIRNESS || stats.backpressure_stalls > 0 {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            fairness_index,
            starvation_count: stats.starvation_count,
            backpressure_stalls: stats.backpressure_stalls,
            mean_queue_depth: stats.mean_queue_depth(),
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

    fn snap(starvation: u64, stalls: u64) -> StatsSnapshot {
        StatsSnapshot {
            starvation_count: starvation,
            backpressure_stalls: stalls,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn derive_healthy_on_default() {
        let r = HealthReport::derive(&StatsSnapshot::default(), 1.0);
        assert!(r.is_healthy());
    }

    #[test]
    fn derive_degraded_on_low_fairness() {
        let r = HealthReport::derive(&StatsSnapshot::default(), 0.5);
        assert_eq!(r.state, HealthState::Degraded);
    }

    #[test]
    fn derive_critical_on_very_low_fairness() {
        let r = HealthReport::derive(&StatsSnapshot::default(), 0.2);
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn derive_critical_on_starvation() {
        let r = HealthReport::derive(&snap(1, 0), 1.0);
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn derive_degraded_on_backpressure() {
        let r = HealthReport::derive(&snap(0, 5), 0.9);
        assert_eq!(r.state, HealthState::Degraded);
    }

    #[test]
    fn actionable_states() {
        assert!(!HealthState::Healthy.is_actionable());
        assert!(HealthState::Degraded.is_actionable());
        assert!(HealthState::Critical.is_actionable());
    }
}
