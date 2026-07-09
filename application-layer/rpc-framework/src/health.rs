//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from RPC stats.
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
//! | `Degraded` | Performance or availability impact | Investigate during business hours |
//! | `Critical` | Severe impact; data loss risk possible | Page on-call immediately |
//!
//! # Thresholds
//!
//! Thresholds are encoded in [`HealthReport::derive`]:
//!
//! - Success rate ≥ 99% (`9_900` bps): healthy
//! - Success rate < 95% (`9_500` bps): degraded
//! - Success rate < 90% (`9_000` bps): critical
//! - Timeout rate > 5% (`500` bps): degraded
//! - Timeout rate > 20% (`2_000` bps): critical

use crate::stats::StatsSnapshot;

// ── HealthState ───────────────────────────────────────────────────

/// Tri-state health classification.
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
    pub const fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    /// Whether the state requires operator attention.
    #[must_use]
    pub const fn needs_attention(&self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }

    /// Whether the state is critical (page on-call).
    #[must_use]
    pub const fn is_critical(&self) -> bool {
        matches!(self, Self::Critical)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Critical => "critical",
        }
    }
}

// ── HealthReport ──────────────────────────────────────────────────

/// Operational health snapshot derived from [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall health state.
    pub state: HealthState,
    /// Total RPC calls processed.
    pub calls_total: u64,
    /// Success rate in basis points (0..=10_000).
    pub success_rate_bps: u16,
    /// Timeout rate in basis points (0..=10_000).
    pub timeout_rate_bps: u16,
    /// Currently active streams.
    pub streams_active: u64,
}

impl HealthReport {
    /// Threshold: success rate ≥ this → healthy (in bps).
    pub const SUCCESS_HEALTHY_BPS: u16 = 9_900;
    /// Threshold: success rate < this → degraded (in bps).
    pub const SUCCESS_DEGRADED_BPS: u16 = 9_500;
    /// Threshold: success rate < this → critical (in bps).
    pub const SUCCESS_CRITICAL_BPS: u16 = 9_000;

    /// Threshold: timeout rate > this → degraded (in bps).
    pub const TIMEOUT_DEGRADED_BPS: u16 = 500;
    /// Threshold: timeout rate > this → critical (in bps).
    pub const TIMEOUT_CRITICAL_BPS: u16 = 2_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let success_rate_bps = stats.success_rate_bps();
        let timeout_rate_bps = Self::compute_timeout_rate_bps(stats);

        // Compute per-signal state, then take the worst.
        let state = Self::classify(success_rate_bps, timeout_rate_bps);

        Self {
            state,
            calls_total: stats.calls_total,
            success_rate_bps,
            timeout_rate_bps,
            streams_active: stats.active_streams(),
        }
    }

    /// Compute timeout rate from stats snapshot.
    fn compute_timeout_rate_bps(stats: &StatsSnapshot) -> u16 {
        let total = stats.calls_total;
        if total == 0 {
            return 0;
        }
        let rate = (stats.timeouts.saturating_mul(10_000)) / total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Combine multiple health signals into one overall state
    /// using a "worst signal wins" policy.
    fn classify(success_bps: u16, timeout_bps: u16) -> HealthState {
        // Success rate signal.
        // >= SUCCESS_HEALTHY_BPS → Healthy
        // < SUCCESS_CRITICAL_BPS → Critical
        // In between → Degraded
        let success_state = if success_bps >= Self::SUCCESS_HEALTHY_BPS {
            HealthState::Healthy
        } else if success_bps < Self::SUCCESS_CRITICAL_BPS {
            HealthState::Critical
        } else {
            HealthState::Degraded
        };

        // Timeout rate signal.
        let timeout_state = if timeout_bps > Self::TIMEOUT_CRITICAL_BPS {
            HealthState::Critical
        } else if timeout_bps > Self::TIMEOUT_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Worst signal wins.
        worst_of([success_state, timeout_state])
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

/// Pick the worst (most severe) of two health states.
const fn worst_of(states: [HealthState; 2]) -> HealthState {
    let mut i = 0;
    let mut worst = HealthState::Healthy;
    while i < 2 {
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

    fn stats_with_rates(success: u64, failed: u64, timeouts: u64) -> StatsSnapshot {
        StatsSnapshot {
            calls_total: success + failed,
            calls_succeeded: success,
            calls_failed: failed,
            timeouts,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
        assert_eq!(report.calls_total, 0);
        assert_eq!(report.streams_active, 0);
    }

    #[test]
    fn high_success_rate_is_healthy() {
        // 9999 / 10000 = 99.99%.
        let stats = stats_with_rates(9_999, 1, 0);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert_eq!(report.success_rate_bps, 9_999);
    }

    #[test]
    fn degraded_success_rate_is_degraded() {
        // 9700 / 10000 = 97% → between 9500 and 9900.
        let stats = stats_with_rates(9_700, 300, 0);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn low_success_rate_is_critical() {
        // 8000 / 10000 = 80% < 9000.
        let stats = stats_with_rates(8_000, 2_000, 0);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn high_timeout_rate_is_degraded() {
        // 95% success, 6% timeouts > 5% threshold.
        let stats = stats_with_rates(95, 5, 6);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert_eq!(report.timeout_rate_bps, 600); // 6% = 600 bps
    }

    #[test]
    fn very_high_timeout_rate_is_critical() {
        // 76% success, 24% timeouts > 20% threshold.
        let stats = stats_with_rates(76, 24, 24);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
        assert_eq!(report.timeout_rate_bps, 2_400); // 24% = 2400 bps
    }

    #[test]
    fn health_state_predicates() {
        assert!(HealthState::Healthy.is_healthy());
        assert!(!HealthState::Healthy.needs_attention());
        assert!(!HealthState::Healthy.is_critical());

        assert!(!HealthState::Degraded.is_healthy());
        assert!(HealthState::Degraded.needs_attention());
        assert!(!HealthState::Degraded.is_critical());

        assert!(HealthState::Critical.is_critical());
        assert!(HealthState::Critical.needs_attention());
    }

    #[test]
    fn health_state_as_str() {
        assert_eq!(HealthState::Healthy.as_str(), "healthy");
        assert_eq!(HealthState::Degraded.as_str(), "degraded");
        assert_eq!(HealthState::Critical.as_str(), "critical");
    }

    #[test]
    fn worst_signal_wins() {
        // If any signal is critical, overall is critical.
        // 9900/10000 = 99% success (healthy), 2500/10000 = 25% timeouts (critical).
        let stats = stats_with_rates(9_900, 100, 2_500);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn timeout_rate_with_no_calls_is_zero() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.timeout_rate_bps, 0);
    }

    #[test]
    fn timeout_rate_calculates_correctly() {
        let stats = stats_with_rates(90, 10, 5); // 5 timeouts out of 100 calls
        let report = HealthReport::derive(&stats);
        assert_eq!(report.timeout_rate_bps, 500); // 5%
    }

    #[test]
    fn active_streams_reflected_in_report() {
        let mut stats = StatsSnapshot::default();
        stats.streams_opened = 10;
        stats.streams_closed = 7;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.streams_active, 3);
    }

    #[test]
    fn healthy_report_needs_no_attention() {
        let stats = stats_with_rates(10_000, 0, 0);
        let report = HealthReport::derive(&stats);
        assert!(report.is_healthy());
        assert!(!report.needs_attention());
    }

    #[test]
    fn degraded_report_needs_attention() {
        let stats = stats_with_rates(9_600, 400, 0);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }

    #[test]
    fn critical_report_needs_attention() {
        let stats = stats_with_rates(8_000, 2_000, 0);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }

    #[test]
    fn both_signals_degraded() {
        let stats = stats_with_rates(9_200, 800, 10); // both signals degraded
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn timeout_rate_clamps_to_u16_max() {
        let mut stats = StatsSnapshot::default();
        stats.calls_total = 1;
        stats.timeouts = u64::MAX;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.timeout_rate_bps, u16::MAX);
    }
}
