//! [`RegistryConfig`], [`Preset`] — declarative configuration for
//! the session registry.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The four presets encode deployment archetypes studied in the
//! Dynamo / Spanner / Cassandra literature:
//!
//! | Preset | Topology | TTL | Ring | Sessions |
//! |--------|----------|-----|------|----------|
//! | `SingleRegion` | 1 region, single zone | 30s | 150 vnodes | 1 M |
//! | `MultiRegion` | 3 regions, 3 zones each | 45s | 200 vnodes | 10 M |
//! | `Hyperscale` | 5 regions, 5 zones each | 20s | 256 vnodes | 100 M |
//! | `Edge` | 12 regions, 1 zone each | 60s | 100 vnodes | 1 M |

use crate::error::RegistryError;
use crate::lease::{DEFAULT_LEASE_TTL_MS, MAX_LEASE_TTL_MS, MIN_LEASE_TTL_MS};
use crate::node::{NodeId, Region, Zone};
use crate::ring::{DEFAULT_VNODES, MAX_VNODES, MIN_VNODES};

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of nodes the registry will accept at construction
/// (the local node must always exist).
pub const MIN_NODES: u32 = 1;

/// Maximum number of nodes the registry will accept (hard cap from
/// the u32 NodeId space; in practice cluster topology is far
/// smaller).
pub const MAX_NODES: u32 = 1_000_000;

/// Minimum number of sessions the registry must support.
pub const MIN_SESSIONS: u64 = 1_000;

/// Maximum number of sessions the registry will accept (hard cap;
/// 1 B sessions is the Vireon design ceiling).
pub const MAX_SESSIONS: u64 = 1_000_000_000;

// ── RegistryConfig ──────────────────────────────────────────────

/// Declarative configuration for the session registry.
///
/// Constructed via [`RegistryConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegistryConfig {
    /// The local node's identifier.
    pub self_node_id: NodeId,
    /// Region the local node resides in.
    pub region: Region,
    /// Zone within the region.
    pub zone: Zone,
    /// Virtual nodes per physical node on the consistent-hash ring.
    pub ring_vnodes: u32,
    /// Lease TTL in milliseconds.
    pub lease_ttl_ms: u64,
    /// Maximum nodes the cluster may grow to.
    pub max_nodes: u32,
    /// Maximum sessions the registry may track.
    pub max_sessions: u64,
    /// Whether ownership rebalancing is enabled.
    pub enable_rebalancing: bool,
    /// Whether lease renewal is enabled.
    pub enable_lease_renewal: bool,
}

impl RegistryConfig {
    /// Construct a minimal config with the local node id.
    ///
    /// Defaults: region `GLOBAL`, zone `DEFAULT`, 150 vnodes,
    /// 30 s lease TTL, 1 M nodes, 100 M sessions, rebalancing and
    /// lease renewal enabled.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidConfig`] only if `self_node`
    /// is nil.
    pub fn new(self_node: NodeId) -> Result<Self, RegistryError> {
        if self_node.is_nil() {
            return Err(RegistryError::InvalidConfig(
                "self_node_id must not be nil".to_string(),
            ));
        }
        Ok(Self {
            self_node_id: self_node,
            region: Region::GLOBAL,
            zone: Zone::DEFAULT,
            ring_vnodes: DEFAULT_VNODES,
            lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
            max_nodes: 100_000,
            max_sessions: 100_000_000,
            enable_rebalancing: true,
            enable_lease_renewal: true,
        })
    }

    /// Builder-style: set the region.
    #[must_use]
    pub const fn with_region(mut self, region: Region) -> Self {
        self.region = region;
        self
    }

    /// Builder-style: set the zone.
    #[must_use]
    pub const fn with_zone(mut self, zone: Zone) -> Self {
        self.zone = zone;
        self
    }

    /// Builder-style: set the vnode count.
    #[must_use]
    pub const fn with_ring_vnodes(mut self, vnodes: u32) -> Self {
        self.ring_vnodes = vnodes;
        self
    }

    /// Builder-style: set the lease TTL (ms).
    #[must_use]
    pub const fn with_lease_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.lease_ttl_ms = ttl_ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidConfig`] if any field is out
    /// of range or contradictory.
    pub fn validate(&self) -> Result<(), RegistryError> {
        if self.self_node_id.is_nil() {
            return Err(RegistryError::InvalidConfig(
                "self_node_id must not be nil".to_string(),
            ));
        }
        if self.ring_vnodes < MIN_VNODES || self.ring_vnodes > MAX_VNODES {
            return Err(RegistryError::InvalidConfig(format!(
                "ring_vnodes {range} out of range [{MIN_VNODES}, {MAX_VNODES}]",
                range = self.ring_vnodes
            )));
        }
        if self.lease_ttl_ms < MIN_LEASE_TTL_MS || self.lease_ttl_ms > MAX_LEASE_TTL_MS {
            return Err(RegistryError::InvalidConfig(format!(
                "lease_ttl_ms {ttl} out of range [{MIN_LEASE_TTL_MS}, {MAX_LEASE_TTL_MS}]",
                ttl = self.lease_ttl_ms
            )));
        }
        if self.max_nodes < MIN_NODES || self.max_nodes > MAX_NODES {
            return Err(RegistryError::InvalidConfig(format!(
                "max_nodes {n} out of range [{MIN_NODES}, {MAX_NODES}]",
                n = self.max_nodes
            )));
        }
        if self.max_sessions < MIN_SESSIONS || self.max_sessions > MAX_SESSIONS {
            return Err(RegistryError::InvalidConfig(format!(
                "max_sessions {n} out of range [{MIN_SESSIONS}, {MAX_SESSIONS}]",
                n = self.max_sessions
            )));
        }
        Ok(())
    }
}

impl Default for RegistryConfig {
    fn default() -> Self {
        // NodeId::new(1) is always non-nil; construct directly to
        // avoid going through `new()` which returns Result.
        Self {
            self_node_id: NodeId::new(1),
            region: Region::GLOBAL,
            zone: Zone::DEFAULT,
            ring_vnodes: DEFAULT_VNODES,
            lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
            max_nodes: 100_000,
            max_sessions: 100_000_000,
            enable_rebalancing: true,
            enable_lease_renewal: true,
        }
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// topology and load profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Single-region, single-zone cluster. Typical for dev / staging
    /// and small production deployments.
    SingleRegion,
    /// Multi-region, multi-zone cluster with relaxed lease TTL
    /// for cross-region latency tolerance.
    MultiRegion,
    /// Hyperscale: many regions, more vnodes per node, shorter
    /// leases for faster failover.
    Hyperscale,
    /// Edge: many small regions, long leases (limited connectivity),
    /// fewer vnodes (memory-constrained nodes).
    Edge,
}

impl Preset {
    /// Materialize the preset into a concrete config for the given
    /// local node.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidConfig`] only if `self_node`
    /// is nil.
    pub fn to_config(self, self_node: NodeId) -> Result<RegistryConfig, RegistryError> {
        let mut cfg = RegistryConfig::new(self_node)?;
        match self {
            Self::SingleRegion => {
                // Defaults are already single-region.
            }
            Self::MultiRegion => {
                cfg = cfg
                    .with_region(Region::new(1))
                    .with_zone(Zone::new(1))
                    .with_ring_vnodes(200)
                    .with_lease_ttl_ms(45_000);
                cfg.max_sessions = 10_000_000;
            }
            Self::Hyperscale => {
                cfg = cfg
                    .with_region(Region::new(1))
                    .with_zone(Zone::new(1))
                    .with_ring_vnodes(256)
                    .with_lease_ttl_ms(20_000);
                cfg.max_sessions = 100_000_000;
                cfg.max_nodes = 10_000;
            }
            Self::Edge => {
                cfg = cfg
                    .with_region(Region::new(1))
                    .with_zone(Zone::DEFAULT)
                    .with_ring_vnodes(100)
                    .with_lease_ttl_ms(60_000);
                cfg.max_sessions = 1_000_000;
            }
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = RegistryConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.self_node_id, NodeId::new(1));
        assert_eq!(cfg.ring_vnodes, DEFAULT_VNODES);
        assert_eq!(cfg.lease_ttl_ms, DEFAULT_LEASE_TTL_MS);
    }

    #[test]
    fn new_rejects_nil_node() {
        assert!(RegistryConfig::new(NodeId::NIL).is_err());
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = RegistryConfig::new(NodeId::new(5))
            .unwrap()
            .with_region(Region::new(7))
            .with_zone(Zone::new(2))
            .with_ring_vnodes(200)
            .with_lease_ttl_ms(45_000);
        assert_eq!(cfg.region, Region::new(7));
        assert_eq!(cfg.zone, Zone::new(2));
        assert_eq!(cfg.ring_vnodes, 200);
        assert_eq!(cfg.lease_ttl_ms, 45_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_vnodes() {
        let mut cfg = RegistryConfig::default();
        cfg.ring_vnodes = 0;
        assert!(cfg.validate().is_err());
        cfg.ring_vnodes = MAX_VNODES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_lease_ttl() {
        let mut cfg = RegistryConfig::default();
        cfg.lease_ttl_ms = MIN_LEASE_TTL_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.lease_ttl_ms = MAX_LEASE_TTL_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_node_count() {
        let mut cfg = RegistryConfig::default();
        cfg.max_nodes = 0;
        assert!(cfg.validate().is_err());
        cfg.max_nodes = MAX_NODES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_session_count() {
        let mut cfg = RegistryConfig::default();
        cfg.max_sessions = 100;
        assert!(cfg.validate().is_err());
        cfg.max_sessions = MAX_SESSIONS * 10;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_nil_node() {
        let mut cfg = RegistryConfig::default();
        cfg.self_node_id = NodeId::NIL;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn single_region_preset_matches_defaults() {
        let preset_cfg = Preset::SingleRegion.to_config(NodeId::new(1)).unwrap();
        let default_cfg = RegistryConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn multi_region_preset_sets_extended_lease() {
        let cfg = Preset::MultiRegion.to_config(NodeId::new(1)).unwrap();
        assert_eq!(cfg.lease_ttl_ms, 45_000);
        assert_eq!(cfg.ring_vnodes, 200);
        assert_eq!(cfg.max_sessions, 10_000_000);
    }

    #[test]
    fn hyperscale_preset_uses_short_lease_many_vnodes() {
        let cfg = Preset::Hyperscale.to_config(NodeId::new(1)).unwrap();
        assert_eq!(cfg.lease_ttl_ms, 20_000);
        assert_eq!(cfg.ring_vnodes, 256);
        assert_eq!(cfg.max_nodes, 10_000);
    }

    #[test]
    fn edge_preset_uses_long_lease() {
        let cfg = Preset::Edge.to_config(NodeId::new(1)).unwrap();
        assert_eq!(cfg.lease_ttl_ms, 60_000);
        assert_eq!(cfg.ring_vnodes, 100);
    }

    #[test]
    fn preset_rejects_nil_node() {
        assert!(Preset::SingleRegion.to_config(NodeId::NIL).is_err());
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [
            Preset::SingleRegion,
            Preset::MultiRegion,
            Preset::Hyperscale,
            Preset::Edge,
        ] {
            let cfg = preset.to_config(NodeId::new(1)).unwrap();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }
}
