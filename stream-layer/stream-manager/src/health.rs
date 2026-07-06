//! [`HealthReport`] + [`HealthState`] — derived from
//! [`StatsSnapshot`](crate::stats::StatsSnapshot).
//!
//! See `DESIGN.md` Part 15 (observability).

use crate::stats::StatsSnapshot;

/// Tri-state classification of stream-manager health.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HealthState {
    /// All metrics nominal.
    #[default]
    Healthy,
    /// Active-stream count above soft cap, or sustained
    /// flow-control stalls.
    Degraded,
    /// Active-stream count above hard cap, or excessive stalls.
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
    /// Active stream count (copied from snapshot).
    pub streams_active: u64,
    /// Mean flow-control stall (ns).
    pub mean_fc_stall_ns: u64,
    /// Reset ratio = `(resets_received + resets_sent) / created_total`.
    /// Saturates at `u8::MAX` for compact reporting.
    pub reset_ratio_per_milli: u16,
}

/// Soft cap on active streams before the manager enters `Degraded`.
pub const SOFT_CAP_STREAMS: u64 = 100_000;
/// Hard cap on active streams before the manager enters `Critical`.
pub const HARD_CAP_STREAMS: u64 = 1_000_000;
/// Mean flow-control stall (ns) that triggers `Degraded`.
pub const DEGRADED_STALL_NS: u64 = 1_000_000;
/// Mean flow-control stall (ns) that triggers `Critical`.
pub const CRITICAL_STALL_NS: u64 = 10_000_000;

impl HealthReport {
    /// Derive a health report from the current stats snapshot.
    ///
    /// Phase-0 heuristic (see `DESIGN.md` Part 15):
    /// - Critical if `streams_active > HARD_CAP_STREAMS`
    ///   or `mean_fc_stall_ns > CRITICAL_STALL_NS`.
    /// - Degraded if `streams_active > SOFT_CAP_STREAMS`
    ///   or `mean_fc_stall_ns > DEGRADED_STALL_NS`.
    /// - Healthy otherwise.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let mean_stall = stats.mean_fc_stall_ns();
        let reset_ratio_per_milli = if stats.streams_created_total == 0 {
            0
        } else {
            let resets = stats
                .resets_sent_total
                .saturating_add(stats.resets_received_total);
            ((resets * 1_000) / stats.streams_created_total)
                .try_into()
                .unwrap_or(u16::MAX)
        };

        let state = if stats.streams_active > HARD_CAP_STREAMS || mean_stall > CRITICAL_STALL_NS {
            HealthState::Critical
        } else if stats.streams_active > SOFT_CAP_STREAMS || mean_stall > DEGRADED_STALL_NS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            streams_active: stats.streams_active,
            mean_fc_stall_ns: mean_stall,
            reset_ratio_per_milli,
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

    fn snap(active: u64, blocked: u64, stall_ns: u64) -> StatsSnapshot {
        StatsSnapshot {
            streams_active: active,
            send_blocked_total: blocked,
            fc_stall_ns_total: stall_ns,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn derive_healthy_on_default() {
        let r = HealthReport::derive(&StatsSnapshot::default());
        assert!(r.is_healthy());
        assert_eq!(r.reset_ratio_per_milli, 0);
    }

    #[test]
    fn derive_degraded_on_soft_cap_breach() {
        let r = HealthReport::derive(&snap(SOFT_CAP_STREAMS + 1, 0, 0));
        assert_eq!(r.state, HealthState::Degraded);
    }

    #[test]
    fn derive_critical_on_hard_cap_breach() {
        let r = HealthReport::derive(&snap(HARD_CAP_STREAMS + 1, 0, 0));
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn derive_degraded_on_stall() {
        // mean = 1_500_000 ns > 1 ms soft threshold
        let r = HealthReport::derive(&snap(0, 1, 1_500_000));
        assert_eq!(r.state, HealthState::Degraded);
    }

    #[test]
    fn derive_critical_on_long_stall() {
        // mean = 11 ms > 10 ms hard threshold
        let r = HealthReport::derive(&snap(0, 1, 11_000_000));
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn reset_ratio_per_mille_computed() {
        // 100 streams created, 5 resets → 50 per mille
        let stats = StatsSnapshot {
            streams_created_total: 100,
            resets_sent_total: 3,
            resets_received_total: 2,
            ..StatsSnapshot::default()
        };
        let r = HealthReport::derive(&stats);
        assert_eq!(r.reset_ratio_per_milli, 50);
    }

    #[test]
    fn actionable_states() {
        assert!(!HealthState::Healthy.is_actionable());
        assert!(HealthState::Degraded.is_actionable());
        assert!(HealthState::Critical.is_actionable());
    }
}
