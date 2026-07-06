//! [`HealthReport`] + [`HealthState`] — derived from
//! [`StatsSnapshot`](crate::stats::StatsSnapshot).
//!
//! See `DESIGN.md` Part 15 (observability) and Part 14 (security).
//!
//! # Health heuristic
//!
//! The session-manager reports a tri-state health classification so
//! that operators can spot capacity and security problems before
//! they become incidents:
//!
//! - **Healthy** — every counter within nominal range.
//! - **Degraded** — soft cap breach or auth-failure rate > 5%.
//!   Actionable but not urgent.
//! - **Critical** — hard cap breach or auth-failure rate > 20%.
//!   Likely DoS or capacity exhaustion; page on-call.

use crate::stats::StatsSnapshot;

/// Tri-state classification of session-manager health.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HealthState {
    /// All metrics nominal.
    #[default]
    Healthy,
    /// Active-session count above soft cap, or sustained
    /// auth-failure rate.
    Degraded,
    /// Active-session count above hard cap, or excessive
    /// auth failures / quota rejections.
    Critical,
}

impl HealthState {
    /// Whether the state requires operator action.
    #[must_use]
    pub const fn is_actionable(self) -> bool {
        matches!(self, Self::Degraded | Self::Critical)
    }

    /// Whether the state is terminal (page on-call).
    #[must_use]
    pub const fn is_critical(self) -> bool {
        matches!(self, Self::Critical)
    }
}

/// Health report derived from a [`StatsSnapshot`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    /// Overall state.
    pub state: HealthState,
    /// Active session count (copied from snapshot).
    pub sessions_active: u64,
    /// Idle session count (copied from snapshot).
    pub sessions_idle: u64,
    /// Auth failure rate as basis points (0..10_000). 5% = 500 bps.
    pub auth_failure_bps: u16,
    /// Quota rejection rate as basis points (0..10_000).
    pub quota_rejection_bps: u16,
    /// Session churn rate as basis points (0..10_000).
    pub churn_bps: u16,
}

// ── Thresholds ───────────────────────────────────────────────────

/// Soft cap on active sessions before the manager enters `Degraded`.
pub const SOFT_CAP_SESSIONS: u64 = 100_000;
/// Hard cap on active sessions before the manager enters `Critical`.
pub const HARD_CAP_SESSIONS: u64 = 1_000_000;
/// Auth-failure rate (as a fraction) above which the manager enters
/// `Degraded`. 5%.
pub const DEGRADED_AUTH_FAILURE_RATE: f64 = 0.05;
/// Auth-failure rate (as a fraction) above which the manager enters
/// `Critical`. 20%.
pub const CRITICAL_AUTH_FAILURE_RATE: f64 = 0.20;
/// Quota-rejection rate above which the manager enters `Degraded`.
/// 10%.
pub const DEGRADED_QUOTA_REJECTION_RATE: f64 = 0.10;
/// Quota-rejection rate above which the manager enters `Critical`.
/// 40%.
pub const CRITICAL_QUOTA_REJECTION_RATE: f64 = 0.40;

/// Convert a fraction (0.0..=1.0) to basis points (0..10_000),
/// saturating at `u16::MAX`.
fn to_bps(frac: f64) -> u16 {
    let bps = (frac.clamp(0.0, 1.0) * 10_000.0).round();
    if bps > u16::MAX as f64 {
        u16::MAX
    } else {
        bps as u16
    }
}

impl HealthReport {
    /// Derive a health report from the current stats snapshot.
    ///
    /// Phase-0 heuristic (see `DESIGN.md` Part 15):
    /// - **Critical** if `sessions_active > HARD_CAP_SESSIONS`,
    ///   `auth_failure_rate > CRITICAL_AUTH_FAILURE_RATE`, or
    ///   `quota_rejection_rate > CRITICAL_QUOTA_REJECTION_RATE`.
    /// - **Degraded** if `sessions_active > SOFT_CAP_SESSIONS`,
    ///   `auth_failure_rate > DEGRADED_AUTH_FAILURE_RATE`, or
    ///   `quota_rejection_rate > DEGRADED_QUOTA_REJECTION_RATE`.
    /// - **Healthy** otherwise.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let auth_failure_bps = to_bps(stats.auth_failure_rate());
        let quota_rejection_bps = to_bps(stats.quota_rejection_rate());
        let churn_bps = to_bps(stats.churn_rate());

        let auth_failure_frac = stats.auth_failure_rate();
        let quota_rejection_frac = stats.quota_rejection_rate();

        let state = if stats.sessions_active > HARD_CAP_SESSIONS
            || auth_failure_frac > CRITICAL_AUTH_FAILURE_RATE
            || quota_rejection_frac > CRITICAL_QUOTA_REJECTION_RATE
        {
            HealthState::Critical
        } else if stats.sessions_active > SOFT_CAP_SESSIONS
            || auth_failure_frac > DEGRADED_AUTH_FAILURE_RATE
            || quota_rejection_frac > DEGRADED_QUOTA_REJECTION_RATE
        {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        Self {
            state,
            sessions_active: stats.sessions_active,
            sessions_idle: stats.sessions_idle,
            auth_failure_bps,
            quota_rejection_bps,
            churn_bps,
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

    fn snap(active: u64, auth_failures: u64, created: u64) -> StatsSnapshot {
        StatsSnapshot {
            sessions_active: active,
            auth_failures_total: auth_failures,
            sessions_created_total: created,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn derive_healthy_on_default() {
        let r = HealthReport::derive(&StatsSnapshot::default());
        assert!(r.is_healthy());
        assert_eq!(r.auth_failure_bps, 0);
    }

    #[test]
    fn derive_degraded_on_soft_cap_breach() {
        let r = HealthReport::derive(&snap(SOFT_CAP_SESSIONS + 1, 0, SOFT_CAP_SESSIONS + 1));
        assert_eq!(r.state, HealthState::Degraded);
    }

    #[test]
    fn derive_critical_on_hard_cap_breach() {
        let r = HealthReport::derive(&snap(HARD_CAP_SESSIONS + 1, 0, HARD_CAP_SESSIONS + 1));
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn derive_degraded_on_auth_failures() {
        // 100 sessions, 6 failures → 6% > 5%
        let r = HealthReport::derive(&snap(0, 6, 100));
        assert_eq!(r.state, HealthState::Degraded);
        assert_eq!(r.auth_failure_bps, 600);
    }

    #[test]
    fn derive_critical_on_auth_failures() {
        // 100 sessions, 25 failures → 25% > 20%
        let r = HealthReport::derive(&snap(0, 25, 100));
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn derive_degraded_on_quota_rejections() {
        let stats = StatsSnapshot {
            sessions_created_total: 100,
            quota_rejections_total: 11, // 11% > 10%
            ..StatsSnapshot::default()
        };
        let r = HealthReport::derive(&stats);
        assert_eq!(r.state, HealthState::Degraded);
        assert_eq!(r.quota_rejection_bps, 1_100);
    }

    #[test]
    fn derive_critical_on_quota_rejections() {
        let stats = StatsSnapshot {
            sessions_created_total: 100,
            quota_rejections_total: 45, // 45% > 40%
            ..StatsSnapshot::default()
        };
        let r = HealthReport::derive(&stats);
        assert_eq!(r.state, HealthState::Critical);
    }

    #[test]
    fn churn_bps_computed() {
        let stats = StatsSnapshot {
            sessions_created_total: 1_000,
            sessions_destroyed_total: 70,
            ..StatsSnapshot::default()
        };
        let r = HealthReport::derive(&stats);
        assert_eq!(r.churn_bps, 700);
    }

    #[test]
    fn actionable_and_critical_states() {
        assert!(!HealthState::Healthy.is_actionable());
        assert!(HealthState::Degraded.is_actionable());
        assert!(HealthState::Critical.is_actionable());
        assert!(HealthState::Critical.is_critical());
        assert!(!HealthState::Degraded.is_critical());
    }
}
