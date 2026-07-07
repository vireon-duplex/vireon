//! [`RegistryStats`], [`StatsSnapshot`] — atomic counters and a
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
//! | `lookups_total` | counter | `registry_lookups_total` |
//! | `lookups_hit` | counter | `registry_lookups_hit_total` |
//! | `lookups_miss` | counter | `registry_lookups_miss_total` |
//! | `leases_active` | gauge | `registry_leases_active` |
//! | `ownership_transfers` | counter | `registry_ownership_transfers_total` |

use std::sync::atomic::{AtomicU64, Ordering};

// ── RegistryStats ───────────────────────────────────────────────

/// Atomic counters for the session registry.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`RegistryStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct RegistryStats {
    // ── Cluster topology ───────────────────────────────────────
    /// Total nodes ever registered (counter; never decreases).
    pub nodes_total: AtomicU64,
    /// Nodes currently in `Alive` state (gauge).
    pub nodes_alive: AtomicU64,
    /// Nodes currently in `Suspect` state (gauge).
    pub nodes_suspect: AtomicU64,
    /// Nodes currently in `Dead` state (gauge).
    pub nodes_dead: AtomicU64,

    // ── Session lifecycle ──────────────────────────────────────
    /// Sessions currently registered (gauge).
    pub sessions_registered: AtomicU64,
    /// Sessions that have expired since startup (counter).
    pub sessions_expired: AtomicU64,

    // ── Lookup hot path ────────────────────────────────────────
    /// Total lookup operations (counter).
    pub lookups_total: AtomicU64,
    /// Lookups that hit the ownership table (counter).
    pub lookups_hit: AtomicU64,
    /// Lookups that missed (counter).
    pub lookups_miss: AtomicU64,

    // ── Leases ─────────────────────────────────────────────────
    /// Leases currently active (gauge).
    pub leases_active: AtomicU64,
    /// Leases that have expired since startup (counter).
    pub leases_expired: AtomicU64,
    /// Lease renewals performed (counter).
    pub leases_renewed: AtomicU64,

    // ── Ownership ──────────────────────────────────────────────
    /// Ownership transfers completed (counter).
    pub ownership_transfers: AtomicU64,
    /// Rebalance events triggered (counter).
    pub rebalance_events: AtomicU64,
}

impl Default for RegistryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RegistryStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            nodes_total: AtomicU64::new(0),
            nodes_alive: AtomicU64::new(0),
            nodes_suspect: AtomicU64::new(0),
            nodes_dead: AtomicU64::new(0),
            sessions_registered: AtomicU64::new(0),
            sessions_expired: AtomicU64::new(0),
            lookups_total: AtomicU64::new(0),
            lookups_hit: AtomicU64::new(0),
            lookups_miss: AtomicU64::new(0),
            leases_active: AtomicU64::new(0),
            leases_expired: AtomicU64::new(0),
            leases_renewed: AtomicU64::new(0),
            ownership_transfers: AtomicU64::new(0),
            rebalance_events: AtomicU64::new(0),
        }
    }

    /// Record a lookup result.
    pub fn record_lookup(&self, hit: bool) {
        self.lookups_total.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.lookups_hit.fetch_add(1, Ordering::Relaxed);
        } else {
            self.lookups_miss.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a successful lease renewal.
    pub fn record_lease_renewed(&self) {
        self.leases_renewed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed ownership transfer.
    pub fn record_ownership_transfer(&self) {
        self.ownership_transfers.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rebalance event.
    pub fn record_rebalance(&self) {
        self.rebalance_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a node joining the cluster.
    pub fn record_node_added(&self) {
        self.nodes_total.fetch_add(1, Ordering::Relaxed);
        self.nodes_alive.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields (e.g. `lookups_total`
    /// could exceed `lookups_hit + lookups_miss` by 1 if a lookup
    /// is in flight). This is acceptable for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            nodes_total: self.nodes_total.load(Ordering::Relaxed),
            nodes_alive: self.nodes_alive.load(Ordering::Relaxed),
            nodes_suspect: self.nodes_suspect.load(Ordering::Relaxed),
            nodes_dead: self.nodes_dead.load(Ordering::Relaxed),
            sessions_registered: self.sessions_registered.load(Ordering::Relaxed),
            sessions_expired: self.sessions_expired.load(Ordering::Relaxed),
            lookups_total: self.lookups_total.load(Ordering::Relaxed),
            lookups_hit: self.lookups_hit.load(Ordering::Relaxed),
            lookups_miss: self.lookups_miss.load(Ordering::Relaxed),
            leases_active: self.leases_active.load(Ordering::Relaxed),
            leases_expired: self.leases_expired.load(Ordering::Relaxed),
            leases_renewed: self.leases_renewed.load(Ordering::Relaxed),
            ownership_transfers: self.ownership_transfers.load(Ordering::Relaxed),
            rebalance_events: self.rebalance_events.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ───────────────────────────────────────────────

/// Point-in-time snapshot of registry counters.
///
/// All rates are precomputed as basis-points (bps, 1/100 of a
/// percent) so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total nodes ever registered.
    pub nodes_total: u64,
    /// Nodes currently `Alive`.
    pub nodes_alive: u64,
    /// Nodes currently `Suspect`.
    pub nodes_suspect: u64,
    /// Nodes currently `Dead`.
    pub nodes_dead: u64,
    /// Sessions currently registered.
    pub sessions_registered: u64,
    /// Sessions that have expired since startup.
    pub sessions_expired: u64,
    /// Total lookup operations.
    pub lookups_total: u64,
    /// Lookups that hit.
    pub lookups_hit: u64,
    /// Lookups that missed.
    pub lookups_miss: u64,
    /// Leases currently active.
    pub leases_active: u64,
    /// Leases that have expired since startup.
    pub leases_expired: u64,
    /// Lease renewals performed.
    pub leases_renewed: u64,
    /// Ownership transfers completed.
    pub ownership_transfers: u64,
    /// Rebalance events triggered.
    pub rebalance_events: u64,
}

impl StatsSnapshot {
    /// Cache hit rate in basis points (0..=10_000).
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

    /// Lease expiry rate in basis points (0..=10_000).
    /// `expired / (active + expired)`. Returns 0 if no leases yet.
    #[must_use]
    pub const fn lease_expiry_rate_bps(&self) -> u16 {
        let denom = self.leases_active.saturating_add(self.leases_expired);
        if denom == 0 {
            return 0;
        }
        let rate = (self.leases_expired * 10_000) / denom;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = RegistryStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_lookup_hit_increments_hit_counter() {
        let s = RegistryStats::new();
        s.record_lookup(true);
        s.record_lookup(true);
        let snap = s.snapshot();
        assert_eq!(snap.lookups_total, 2);
        assert_eq!(snap.lookups_hit, 2);
        assert_eq!(snap.lookups_miss, 0);
    }

    #[test]
    fn record_lookup_miss_increments_miss_counter() {
        let s = RegistryStats::new();
        s.record_lookup(false);
        let snap = s.snapshot();
        assert_eq!(snap.lookups_total, 1);
        assert_eq!(snap.lookups_hit, 0);
        assert_eq!(snap.lookups_miss, 1);
    }

    #[test]
    fn hit_rate_is_100_pct_with_no_lookups() {
        let s = RegistryStats::new();
        // No lookups → 100% by convention.
        assert_eq!(s.snapshot().lookup_hit_rate_bps(), 10_000);
    }

    #[test]
    fn hit_rate_reflects_mixed_hits_and_misses() {
        let s = RegistryStats::new();
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
    fn node_added_increments_total_and_alive() {
        let s = RegistryStats::new();
        s.record_node_added();
        s.record_node_added();
        let snap = s.snapshot();
        assert_eq!(snap.nodes_total, 2);
        assert_eq!(snap.nodes_alive, 2);
    }

    #[test]
    fn lease_renewal_counter() {
        let s = RegistryStats::new();
        s.record_lease_renewed();
        s.record_lease_renewed();
        s.record_lease_renewed();
        assert_eq!(s.snapshot().leases_renewed, 3);
    }

    #[test]
    fn ownership_transfer_counter() {
        let s = RegistryStats::new();
        s.record_ownership_transfer();
        assert_eq!(s.snapshot().ownership_transfers, 1);
    }

    #[test]
    fn rebalance_counter() {
        let s = RegistryStats::new();
        s.record_rebalance();
        assert_eq!(s.snapshot().rebalance_events, 1);
    }

    #[test]
    fn lease_expiry_rate_with_no_leases_is_zero() {
        let s = RegistryStats::new();
        assert_eq!(s.snapshot().lease_expiry_rate_bps(), 0);
    }

    #[test]
    fn lease_expiry_rate_with_mixed_leases() {
        let s = RegistryStats::new();
        s.leases_active.store(80, Ordering::Relaxed);
        s.leases_expired.store(20, Ordering::Relaxed);
        // 20/(80+20) = 20% = 2000 bps.
        assert_eq!(s.snapshot().lease_expiry_rate_bps(), 2_000);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = RegistryStats::new();
        s.record_lookup(true);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }
}
