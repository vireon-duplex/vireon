//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from agent runtime stats.
//!
//! See `DESIGN.md` Part 12 (observability).

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
    /// Tool call success rate in basis points (0..=10_000).
    pub tool_success_rate_bps: u16,
    /// Number of currently active agents.
    pub active_agents: u64,
    /// Total context tokens used.
    pub context_tokens_used: u64,
}

impl HealthReport {
    /// Threshold: tool success rate below this -> degraded (in bps).
    pub const TOOL_SUCCESS_DEGRADED_BPS: u16 = 9_000; // 90%
    /// Threshold: tool success rate below this -> critical (in bps).
    pub const TOOL_SUCCESS_CRITICAL_BPS: u16 = 8_000; // 80%

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let tool_success_rate_bps = stats.tool_success_rate_bps();
        let active_agents = stats.active_agents();

        // Compute per-signal state, then take the worst.
        let state = Self::classify(tool_success_rate_bps);

        Self {
            state,
            tool_success_rate_bps,
            active_agents,
            context_tokens_used: stats.context_tokens_used,
        }
    }

    /// Classify health based on tool success rate.
    fn classify(tool_success_bps: u16) -> HealthState {
        // Tool success rate signal (lower is worse).
        if tool_success_bps < Self::TOOL_SUCCESS_CRITICAL_BPS {
            HealthState::Critical
        } else if tool_success_bps < Self::TOOL_SUCCESS_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_with_tool_calls(initiated: u64, completed: u64, failed: u64) -> StatsSnapshot {
        StatsSnapshot {
            agents_created: initiated,
            agents_stopped: 0,
            tool_calls_initiated: initiated,
            tool_calls_completed: completed,
            tool_calls_failed: failed,
            context_tokens_used: 0,
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
        assert_eq!(report.context_tokens_used, 0);
        assert_eq!(report.tool_success_rate_bps, 10_000);
    }

    #[test]
    fn all_success_is_healthy() {
        let stats = stats_with_tool_calls(10, 10, 0);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert_eq!(report.tool_success_rate_bps, 10_000);
    }

    #[test]
    fn moderate_failure_rate_is_degraded() {
        // 3 out of 20 failed = 17/20 = 85% = 8500 bps -> degraded
        let stats = stats_with_tool_calls(20, 17, 3);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn high_failure_rate_is_critical() {
        // 5 out of 10 failed = 50% success = 5000 bps -> critical
        let stats = stats_with_tool_calls(10, 5, 5);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
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
    fn healthy_report_needs_no_attention() {
        let stats = stats_with_tool_calls(10, 10, 0);
        let report = HealthReport::derive(&stats);
        assert!(report.is_healthy());
        assert!(!report.needs_attention());
    }

    #[test]
    fn degraded_report_needs_attention() {
        let stats = stats_with_tool_calls(20, 17, 3);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }

    #[test]
    fn critical_report_needs_attention() {
        let stats = stats_with_tool_calls(10, 5, 5);
        let report = HealthReport::derive(&stats);
        assert!(!report.is_healthy());
        assert!(report.needs_attention());
    }

    #[test]
    fn active_agents_in_report() {
        let mut stats = StatsSnapshot::default();
        stats.agents_created = 5;
        stats.agents_stopped = 2;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.active_agents, 3);
    }

    #[test]
    fn context_tokens_in_report() {
        let mut stats = StatsSnapshot::default();
        stats.context_tokens_used = 42_000;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.context_tokens_used, 42_000);
    }

    #[test]
    fn boundary_degraded_threshold() {
        // Exactly 90% success rate = 9000 bps, which is NOT < 9000, so healthy
        let stats = stats_with_tool_calls(10, 9, 1);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert_eq!(report.tool_success_rate_bps, 9_000);
    }

    #[test]
    fn boundary_critical_threshold() {
        // Exactly 80% success rate = 8000 bps, which is NOT < 8000, so degraded
        let stats = stats_with_tool_calls(10, 8, 2);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert_eq!(report.tool_success_rate_bps, 8_000);
    }
}
