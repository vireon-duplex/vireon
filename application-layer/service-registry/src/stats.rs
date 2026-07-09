//! [`ServiceRegistryStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for observability.
//!
//! See `DESIGN.md` Part 12 (observability).
//!
//! # Design
//!
//! Counters are stored as `AtomicU64` with `Relaxed` ordering:
//! stats are advisory, never load-bearing, so we trade
//! synchronization cost for throughput. Snapshots are taken on
//! demand and the snapshot is what gets exported to metrics.
//!
//! # Mapping to metrics
//!
//! Each counter maps 1:1 to a Prometheus counter or gauge in
//! Phase 2+:
//!
//! | Counter | Prometheus type | Name |
//! |---------|-----------------|------|
//! | `services_registered` | gauge | `service_registry_services_total` |
//! | `services_active` | gauge | `service_registry_services_active` |
//! | `services_draining` | gauge | `service_registry_services_draining` |
//! | `endpoints_total` | gauge | `service_registry_endpoints_total` |
//! | `endpoints_healthy` | gauge | `service_registry_endpoints_healthy` |
//! | `lookups_total` | counter | `service_registry_lookups_total` |
//! | `lookups_hit` | counter | `service_registry_lookups_hit_total` |
//! | `load_balance_decisions` | counter | `service_registry_load_balance_decisions_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── ServiceRegistryStats ───────────────────────────────────────────

/// Atomic counters for the service registry.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`ServiceRegistryStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct ServiceRegistryStats {
    // ── Service lifecycle ───────────────────────────────────────
    /// Services currently registered (gauge).
    pub services_registered: AtomicU64,
    /// Services in `Active` state (gauge).
    pub services_active: AtomicU64,
    /// Services in `Draining` state (gauge).
    pub services_draining: AtomicU64,

    // ── Endpoint lifecycle ───────────────────────────────────────
    /// Total endpoints registered (gauge).
    pub endpoints_total: AtomicU64,
    /// Endpoints currently healthy (gauge).
    pub endpoints_healthy: AtomicU64,

    // ── Lookup hot path ──────────────────────────────────────────
    /// Total lookup operations (counter).
    pub lookups_total: AtomicU64,
    /// Lookups that hit (counter).
    pub lookups_hit: AtomicU64,

    // ── Load balancing ──────────────────────────────────────────
    /// Load balancer decisions made (counter).
    pub load_balance_decisions: AtomicU64,
}

impl Default for ServiceRegistryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceRegistryStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            services_registered: AtomicU64::new(0),
            services_active: AtomicU64::new(0),
            services_draining: AtomicU64::new(0),
            endpoints_total: AtomicU64::new(0),
            endpoints_healthy: AtomicU64::new(0),
            lookups_total: AtomicU64::new(0),
            lookups_hit: AtomicU64::new(0),
            load_balance_decisions: AtomicU64::new(0),
        }
    }

    /// Record a service registration.
    pub fn record_service_registered(&self) {
        self.services_registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a service activation.
    pub fn record_service_activated(&self) {
        self.services_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a service entering drain state.
    pub fn record_service_draining(&self) {
        self.services_draining.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a service unregistration.
    pub fn record_service_unregistered(&self, was_active: bool, was_draining: bool) {
        self.services_registered.fetch_sub(1, Ordering::Relaxed);
        if was_active {
            self.services_active.fetch_sub(1, Ordering::Relaxed);
        }
        if was_draining {
            self.services_draining.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record an endpoint addition.
    pub fn record_endpoint_added(&self) {
        self.endpoints_total.fetch_add(1, Ordering::Relaxed);
        self.endpoints_healthy.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an endpoint removal.
    pub fn record_endpoint_removed(&self, was_healthy: bool) {
        self.endpoints_total.fetch_sub(1, Ordering::Relaxed);
        if was_healthy {
            self.endpoints_healthy.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record an endpoint health change.
    pub fn record_endpoint_health_change(&self, now_healthy: bool) {
        if now_healthy {
            self.endpoints_healthy.fetch_add(1, Ordering::Relaxed);
        } else {
            self.endpoints_healthy.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record a lookup result.
    pub fn record_lookup(&self, hit: bool) {
        self.lookups_total.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.lookups_hit.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a load balancer decision.
    pub fn record_load_balance_decision(&self) {
        self.load_balance_decisions.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields (e.g. `lookups_total`
    /// could exceed `lookups_hit + misses` by 1 if a lookup
    /// is in flight). This is acceptable for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            services_registered: self.services_registered.load(Ordering::Relaxed),
            services_active: self.services_active.load(Ordering::Relaxed),
            services_draining: self.services_draining.load(Ordering::Relaxed),
            endpoints_total: self.endpoints_total.load(Ordering::Relaxed),
            endpoints_healthy: self.endpoints_healthy.load(Ordering::Relaxed),
            lookups_total: self.lookups_total.load(Ordering::Relaxed),
            lookups_hit: self.lookups_hit.load(Ordering::Relaxed),
            load_balance_decisions: self.load_balance_decisions.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ─────────────────────────────────────────────────

/// Point-in-time snapshot of registry counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Services currently registered.
    pub services_registered: u64,
    /// Services currently active.
    pub services_active: u64,
    /// Services currently draining.
    pub services_draining: u64,
    /// Total endpoints registered.
    pub endpoints_total: u64,
    /// Endpoints currently healthy.
    pub endpoints_healthy: u64,
    /// Total lookup operations.
    pub lookups_total: u64,
    /// Lookups that hit.
    pub lookups_hit: u64,
    /// Load balancer decisions made.
    pub load_balance_decisions: u64,
}

impl StatsSnapshot {
    /// Lookup hit rate in basis points (0..=10_000).
    /// Returns 10_000 (100%) when there are no lookups yet.
    #[must_use]
    pub const fn lookup_hit_rate_bps(&self) -> u16 {
        if self.lookups_total == 0 {
            return 10_000;
        }
        // hit * 10_000 / total, clamped to u16.
        let rate = (self.lookups_hit * 10_000) / self.lookups_total;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Endpoint health ratio in basis points (0..=10_000).
    /// Returns 0 when there are no endpoints.
    #[must_use]
    pub const fn endpoint_health_ratio_bps(&self) -> u16 {
        if self.endpoints_total == 0 {
            return 0;
        }
        let ratio = (self.endpoints_healthy * 10_000) / self.endpoints_total;
        if ratio > u16::MAX as u64 {
            u16::MAX
        } else {
            ratio as u16
        }
    }

    /// Count of unhealthy endpoints.
    #[must_use]
    pub const fn endpoints_unhealthy(&self) -> u64 {
        self.endpoints_total.saturating_sub(self.endpoints_healthy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = ServiceRegistryStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_service_registered_increments_counter() {
        let s = ServiceRegistryStats::new();
        s.record_service_registered();
        s.record_service_registered();
        let snap = s.snapshot();
        assert_eq!(snap.services_registered, 2);
    }

    #[test]
    fn record_service_activated_increments_active() {
        let s = ServiceRegistryStats::new();
        s.record_service_activated();
        s.record_service_activated();
        let snap = s.snapshot();
        assert_eq!(snap.services_active, 2);
    }

    #[test]
    fn record_service_draining_increments_draining() {
        let s = ServiceRegistryStats::new();
        s.record_service_draining();
        let snap = s.snapshot();
        assert_eq!(snap.services_draining, 1);
    }

    #[test]
    fn record_service_unregistered_decrements_counters() {
        let s = ServiceRegistryStats::new();
        s.record_service_registered();
        s.record_service_activated();
        s.record_service_unregistered(true, false);
        let snap = s.snapshot();
        assert_eq!(snap.services_registered, 0);
        assert_eq!(snap.services_active, 0);
    }

    #[test]
    fn record_endpoint_added_increments_counters() {
        let s = ServiceRegistryStats::new();
        s.record_endpoint_added();
        s.record_endpoint_added();
        let snap = s.snapshot();
        assert_eq!(snap.endpoints_total, 2);
        assert_eq!(snap.endpoints_healthy, 2);
    }

    #[test]
    fn record_endpoint_removed_decrements_counters() {
        let s = ServiceRegistryStats::new();
        s.record_endpoint_added();
        s.record_endpoint_removed(true);
        let snap = s.snapshot();
        assert_eq!(snap.endpoints_total, 0);
        assert_eq!(snap.endpoints_healthy, 0);
    }

    #[test]
    fn record_endpoint_health_change() {
        let s = ServiceRegistryStats::new();
        s.record_endpoint_added();
        s.record_endpoint_health_change(false); // becomes unhealthy
        let snap = s.snapshot();
        assert_eq!(snap.endpoints_healthy, 0);

        s.record_endpoint_health_change(true); // becomes healthy
        let snap = s.snapshot();
        assert_eq!(snap.endpoints_healthy, 1);
    }

    #[test]
    fn record_lookup_hit_increments_hit_counter() {
        let s = ServiceRegistryStats::new();
        s.record_lookup(true);
        s.record_lookup(true);
        let snap = s.snapshot();
        assert_eq!(snap.lookups_total, 2);
        assert_eq!(snap.lookups_hit, 2);
    }

    #[test]
    fn record_lookup_miss_increments_total_only() {
        let s = ServiceRegistryStats::new();
        s.record_lookup(false);
        let snap = s.snapshot();
        assert_eq!(snap.lookups_total, 1);
        assert_eq!(snap.lookups_hit, 0);
    }

    #[test]
    fn hit_rate_is_100_pct_with_no_lookups() {
        let s = ServiceRegistryStats::new();
        // No lookups → 100% by convention.
        assert_eq!(s.snapshot().lookup_hit_rate_bps(), 10_000);
    }

    #[test]
    fn hit_rate_reflects_mixed_hits_and_misses() {
        let s = ServiceRegistryStats::new();
        for _ in 0..7 {
            s.record_lookup(true);
        }
        for _ in 0..3 {
            s.record_lookup(false);
        }
        // 7/10 = 70% = 7000 bps.
        assert_eq!(s.snapshot().lookup_hit_rate_bps(), 7_000);
    }

    #[test]
    fn endpoint_health_ratio() {
        let s = ServiceRegistryStats::new();
        for _ in 0..10 {
            s.record_endpoint_added();
        }
        s.record_endpoint_health_change(false);
        s.record_endpoint_health_change(false);
        let snap = s.snapshot();
        // 8/10 = 80% = 8000 bps.
        assert_eq!(snap.endpoint_health_ratio_bps(), 8_000);
        assert_eq!(snap.endpoints_unhealthy(), 2);
    }

    #[test]
    fn endpoint_health_ratio_with_no_endpoints() {
        let s = ServiceRegistryStats::new();
        assert_eq!(s.snapshot().endpoint_health_ratio_bps(), 0);
    }

    #[test]
    fn record_load_balance_decision() {
        let s = ServiceRegistryStats::new();
        s.record_load_balance_decision();
        s.record_load_balance_decision();
        let snap = s.snapshot();
        assert_eq!(snap.load_balance_decisions, 2);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = ServiceRegistryStats::new();
        s.record_lookup(true);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_const_methods() {
        const SNAP: StatsSnapshot = StatsSnapshot {
            lookups_total: 100,
            lookups_hit: 75,
            endpoints_total: 10,
            endpoints_healthy: 8,
            services_registered: 0,
            services_active: 0,
            services_draining: 0,
            load_balance_decisions: 0,
        };
        assert_eq!(SNAP.lookup_hit_rate_bps(), 7_500); // 75%
        assert_eq!(SNAP.endpoint_health_ratio_bps(), 8_000); // 80%
        assert_eq!(SNAP.endpoints_unhealthy(), 2);
    }
}
