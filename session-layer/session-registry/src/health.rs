//! [`HealthReport`], [`HealthState`] — operational health summary
//! derived from stats.
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
//! Thresholds are encoded in [`HealthReport::derive`]. The numbers
//! are derived from typical SLO targets for session systems:
//!
//! - Lookup hit rate ≥ 99% (`9_900` bps): healthy.
//! - Lookup hit rate < 95% (`9_500` bps): degraded.
//! - Lookup hit rate < 90% (`9_000` bps): critical.
//! - Lease expiry rate > 5% (`500` bps): degraded.
//! - Lease expiry rate > 20% (`2_000` bps): critical.
//! - Suspect + Dead nodes > 1/3 of total: critical.

use crate::stats::StatsSnapshot;

// ── HealthState ─────────────────────────────────────────────────

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
    /// Number of nodes in `Alive` state.
    pub nodes_alive: u64,
    /// Number of nodes in `Suspect` state.
    pub nodes_suspect: u64,
    /// Number of nodes in `Dead` state.
    pub nodes_dead: u64,
    /// Sessions currently registered.
    pub sessions_registered: u64,
    /// Lookup hit rate in basis points (0..=10_000).
    pub lookup_hit_rate_bps: u16,
    /// Lease expiry rate in basis points (0..=10_000).
    pub lease_expiry_rate_bps: u16,
}

impl HealthReport {
    /// Threshold: lookup hit rate ≥ this → healthy (in bps).
    pub const LOOKUP_HEALTHY_BPS: u16 = 9_900;
    /// Threshold: lookup hit rate < this → degraded (in bps).
    pub const LOOKUP_DEGRADED_BPS: u16 = 9_500;
    /// Threshold: lookup hit rate < this → critical (in bps).
    pub const LOOKUP_CRITICAL_BPS: u16 = 9_000;

    /// Threshold: lease expiry rate > this → degraded (in bps).
    pub const LEASE_EXPIRY_DEGRADED_BPS: u16 = 500;
    /// Threshold: lease expiry rate > this → critical (in bps).
    pub const LEASE_EXPIRY_CRITICAL_BPS: u16 = 2_000;

    /// Derive a health report from a stats snapshot.
    ///
    /// The classification follows a "worst signal wins" policy: if
    /// any individual signal is critical, the overall state is
    /// critical; otherwise the worst degraded signal wins.
    #[must_use]
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let lookup_hit_rate_bps = stats.lookup_hit_rate_bps();
        let lease_expiry_rate_bps = stats.lease_expiry_rate_bps();

        // Compute per-signal state, then take the worst.
        let state = Self::classify(
            lookup_hit_rate_bps,
            lease_expiry_rate_bps,
            stats.nodes_alive,
            stats.nodes_suspect,
            stats.nodes_dead,
        );

        Self {
            state,
            nodes_alive: stats.nodes_alive,
            nodes_suspect: stats.nodes_suspect,
            nodes_dead: stats.nodes_dead,
            sessions_registered: stats.sessions_registered,
            lookup_hit_rate_bps,
            lease_expiry_rate_bps,
        }
    }

    /// Combine multiple health signals into one overall state
    /// using a "worst signal wins" policy.
    fn classify(
        lookup_bps: u16,
        lease_expiry_bps: u16,
        nodes_alive: u64,
        nodes_suspect: u64,
        nodes_dead: u64,
    ) -> HealthState {
        // Lookup hit rate signal.
        // >= LOOKUP_HEALTHY_BPS → Healthy
        // < LOOKUP_CRITICAL_BPS → Critical
        // In between → Degraded
        let lookup_state = if lookup_bps >= Self::LOOKUP_HEALTHY_BPS {
            HealthState::Healthy
        } else if lookup_bps < Self::LOOKUP_CRITICAL_BPS {
            HealthState::Critical
        } else {
            HealthState::Degraded
        };

        // Lease expiry rate signal.
        let lease_state = if lease_expiry_bps > Self::LEASE_EXPIRY_CRITICAL_BPS {
            HealthState::Critical
        } else if lease_expiry_bps > Self::LEASE_EXPIRY_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        };

        // Node health signal.
        let unhealthy = nodes_suspect + nodes_dead;
        let node_state = if nodes_alive == 0 && unhealthy == 0 {
            // Empty cluster — treat as healthy (startup).
            HealthState::Healthy
        } else if nodes_alive == 0 {
            HealthState::Critical
        } else {
            let total = nodes_alive + unhealthy;
            // > 1/3 unhealthy → critical.
            if unhealthy * 3 > total {
                HealthState::Critical
            } else if unhealthy * 10 > total {
                // > 10% unhealthy → degraded.
                HealthState::Degraded
            } else {
                HealthState::Healthy
            }
        };

        // Worst signal wins.
        worst_of([lookup_state, lease_state, node_state])
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

/// Pick the worst (most severe) of three health states.
const fn worst_of(states: [HealthState; 3]) -> HealthState {
    let mut i = 0;
    let mut worst = HealthState::Healthy;
    while i < 3 {
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

    fn stats_with_lookup(hit: u64, miss: u64) -> StatsSnapshot {
        StatsSnapshot {
            lookups_total: hit + miss,
            lookups_hit: hit,
            lookups_miss: miss,
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn empty_stats_derive_healthy() {
        let stats = StatsSnapshot::default();
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
        assert!(report.is_healthy());
    }

    #[test]
    fn high_hit_rate_is_healthy() {
        // 9999 / 10000 = 99.99%.
        let stats = stats_with_lookup(9_999, 1);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);
    }

    #[test]
    fn degraded_hit_rate_is_degraded() {
        // 9700 / 10000 = 97% → between 9500 and 9900.
        let stats = stats_with_lookup(9_700, 300);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
        assert!(report.needs_attention());
        assert!(!report.is_healthy());
    }

    #[test]
    fn low_hit_rate_is_critical() {
        // 8000 / 10000 = 80% < 9000.
        let stats = stats_with_lookup(8_000, 2_000);
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn high_lease_expiry_is_degraded() {
        let mut stats = stats_with_lookup(10_000, 0);
        stats.leases_active = 95;
        stats.leases_expired = 6; // ~6% > 5%.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn very_high_lease_expiry_is_critical() {
        let mut stats = stats_with_lookup(10_000, 0);
        stats.leases_active = 80;
        stats.leases_expired = 25; // ~24% > 20%.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn many_dead_nodes_is_critical() {
        let mut stats = stats_with_lookup(10_000, 0);
        stats.nodes_alive = 6;
        stats.nodes_dead = 4; // 4/10 > 1/3.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }

    #[test]
    fn some_dead_nodes_is_degraded() {
        let mut stats = stats_with_lookup(10_000, 0);
        stats.nodes_alive = 95;
        stats.nodes_dead = 5; // 5/100 = 5%, >10% threshold check.
        // Actually 5*10 = 50 ≤ 100, so still healthy. Need 11 dead.
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Healthy);

        // Try with 11% dead.
        stats.nodes_alive = 89;
        stats.nodes_dead = 11;
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Degraded);
    }

    #[test]
    fn zero_alive_nodes_is_critical() {
        let mut stats = stats_with_lookup(10_000, 0);
        stats.nodes_alive = 0;
        stats.nodes_dead = 3;
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
    fn worst_signal_wins() {
        // If any signal is critical, overall is critical.
        let mut stats = stats_with_lookup(10_000, 0); // healthy lookup
        stats.leases_active = 80;
        stats.leases_expired = 25; // critical lease
        let report = HealthReport::derive(&stats);
        assert_eq!(report.state, HealthState::Critical);
    }
}
