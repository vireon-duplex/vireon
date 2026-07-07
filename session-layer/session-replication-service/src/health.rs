//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from replication stats.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Tri-state classification
//!
//! Health is classified into three states, each with a clear
//! operator action:
//!
//! | State | Meaning | Operator action |
//! |-------|---------|-----------------|
//! | `Healthy` | All systems nominal | None |
//! | `Degraded` | Performance or availability impact | Investigate |
//! | `Critical` | Severe impact; data loss risk | Page on-call |
//!
//! # Thresholds
//!
//! - Success rate ≥ 99% (`9_900` bps): healthy.
//! - Success rate < 95% (`9_500` bps): degraded.
//! - Success rate < 90% (`9_000` bps): critical.
//! - Conflict rate > 5% (`500` bps): degraded.
//! - Conflict rate > 20% (`2_000` bps): critical.
//! - Replication lag > 500 ms (`500_000_000` ns): degraded.
//! - Replication lag > 5 s (`5_000_000_000` ns): critical.
//! - Quorum not healthy: critical.

use crate::stats::StatsSnapshot;

// ── HealthState ─────────────────────────────────────────────────

/// Tri-state health classification for the replication service.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum HealthState {
    /// All systems nominal.
    #[default]
    Healthy,
    /// Performance or availability degraded; investigate.
    Degraded,
    /// Severe impact; page on-call.
    Critical,
}

impl HealthState {
    /// Whether the state is `Healthy`.
    #[must_use]
    pub const fn is_healthy(self) -> bool {
        matches!(self, Self::Healthy)
    }

    /// Whether the state requires operator attention.
    #[must_use]
    pub const fn needs_attention(self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }

    /// Whether the state is critical (page on-call).
    #[must_use]
    pub const fn is_critical(self) -> bool {
        matches!(self, Self::Critical)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Critical => "critical",
        }
    }
}

// ── HealthReport ────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Number of replicas in `Alive` state.
    pub alive_replicas: u64,
    /// Total number of replicas configured.
    pub total_replicas: u64,
    /// Current replication lag (nanoseconds).
    pub replication_lag_ns: u64,
    /// Conflict rate in basis points (0..=10_000).
    pub conflict_rate_bps: u16,
    /// Whether a quorum of replicas is healthy.
    pub quorum_healthy: bool,
}

impl HealthReport {
    /// Threshold: success rate ≥ this → healthy (in bps).
    pub const SUCCESS_HEALTHY_BPS: u16 = 9_900;
    /// Threshold: success rate < this → degraded (in bps).
    pub const SUCCESS_DEGRADED_BPS: u16 = 9_500;
    /// Threshold: success rate < this → critical (in bps).
    pub const SUCCESS_CRITICAL_BPS: u16 = 9_000;

    /// Threshold: conflict rate > this → degraded (in bps).
    pub const CONFLICT_DEGRADED_BPS: u16 = 500;
    /// Threshold: conflict rate > this → critical (in bps).
    pub const CONFLICT_CRITICAL_BPS: u16 = 2_000;

    /// Threshold: lag > this → degraded (in ns).
    pub const LAG_DEGRADED_NS: u64 = 500_000_000;
    /// Threshold: lag > this → critical (in ns).
    pub const LAG_CRITICAL_NS: u64 = 5_000_000_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let success_rate_bps = stats.success_rate_bps();
        let conflict_rate_bps = stats.conflict_rate_bps();
        let lag_ns = stats.current_lag_ns();

        // Determine quorum health: if we have stats, quorum is healthy
        // when ops are mostly succeeding.
        let quorum_healthy = success_rate_bps >= Self::SUCCESS_DEGRADED_BPS;

        // Compute per-signal state, then take the worst.
        let state = Self::classify(success_rate_bps, conflict_rate_bps, lag_ns, quorum_healthy);

        Self {
            state,
            alive_replicas: 0,
            total_replicas: 0,
            replication_lag_ns: lag_ns,
            conflict_rate_bps,
            quorum_healthy,
        }
    }

    /// Combine multiple health signals into one overall state
    /// using a "worst signal wins" policy.
    fn classify(
        success_bps: u16,
        conflict_bps: u16,
        lag_ns: u64,
        quorum_healthy: bool,
    ) -> HealthState {
        // Success rate signal.
        let success_state = if success_bps >= Self::SUCCESS_HEALTHY_BPS {
            HealthState::Healthy
        } else if success_bps < Self::SUCCESS_CRITICAL_BPS {
            HealthState::Critical
        } else {
            HealthState::Degraded
        };

        // Conflict rate signal.
        let conflict_state = if conflict_bps > Self::CONFLICT_CRITICAL_BPS {
            HealthState::Critical
        } else if conflict_bps > Self::CONFLICT_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Lag signal.
        let lag_state = if lag_ns > Self::LAG_CRITICAL_NS {
            HealthState::Critical
        } else if lag_ns > Self::LAG_DEGRADED_NS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Quorum signal.
        let quorum_state = if quorum_healthy {
            HealthState::Healthy
        } else {
            HealthState::Critical
        };

        // Worst signal wins.
        worst_of([success_state, conflict_state, lag_state, quorum_state])
    }

    /// Whether the report indicates a healthy system.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.state.is_healthy()
    }

    /// Whether the report requires operator attention.
    #[must_use]
    pub const fn needs_attention(&self) -> bool {
        self.state.needs_attention()
    }
}

/// Pick the worst (most severe) of four health states.
const fn worst_of(states: [HealthState; 4]) -> HealthState {
    let mut i = 0;
    let mut worst = HealthState::Healthy;
    while i < 4 {
        let s = states[i];
        if matches!(s, HealthState::Critical) {
            return HealthState::Critical;
        }
        if matches!(s, HealthState::Degraded) {
            worst = HealthState::Degraded;
        }
        i += 1;
    }
    worst
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_stats() -> StatsSnapshot {
        StatsSnapshot::default()
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = empty_stats();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
        assert!(report.quorum_healthy);
    }

    #[test]
    fn high_success_rate_is_healthy() {
        let mut stats = empty_stats();
        // 9_999 / 10_000 = 99.99% success.
        stats.ops_total = 10_000;
        stats.ops_succeeded = 9_999;
        stats.ops_failed = 1;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn degraded_success_rate_is_degraded() {
        let mut stats = empty_stats();
        // 97% = 9700 bps → between 9500 and 9900.
        stats.ops_total = 10_000;
        stats.ops_succeeded = 9_700;
        stats.ops_failed = 300;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn high_conflict_rate_is_critical() {
        let mut stats = empty_stats();
        stats.ops_total = 100;
        stats.ops_succeeded = 100;
        stats.conflicts_total = 25; // 25% > 20%.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn high_lag_is_critical() {
        let mut stats = empty_stats();
        stats.lag_ns = 6_000_000_000; // 6s > 5s.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn worst_signal_wins() {
        // If any signal is critical, overall is critical.
        let mut stats = empty_stats();
        stats.ops_total = 100;
        stats.ops_succeeded = 100; // healthy success
        stats.lag_ns = 6_000_000_000; // critical lag
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }
}
