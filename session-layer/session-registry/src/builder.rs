//! [`RegistryBuilder`] — fluent builder for [`RegistryConfig`] and
//! the directory itself.
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `RegistryConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{Preset, RegistryConfig};
use crate::error::RegistryError;
use crate::node::{NodeId, Region, Zone};

/// Fluent builder for [`RegistryConfig`].
#[derive(Debug, Clone)]
pub struct RegistryBuilder {
    cfg: RegistryConfig,
}

impl RegistryBuilder {
    /// Start a new builder for the given local node.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidConfig`] if `self_node` is nil.
    pub fn new(self_node: NodeId) -> Result<Self, RegistryError> {
        Ok(Self {
            cfg: RegistryConfig::new(self_node)?,
        })
    }

    /// Start from a preset.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidConfig`] if `self_node` is nil.
    pub fn from_preset(preset: Preset, self_node: NodeId) -> Result<Self, RegistryError> {
        Ok(Self {
            cfg: preset.to_config(self_node)?,
        })
    }

    /// Override the region.
    #[must_use]
    pub fn region(mut self, region: Region) -> Self {
        self.cfg = self.cfg.with_region(region);
        self
    }

    /// Override the zone.
    #[must_use]
    pub fn zone(mut self, zone: Zone) -> Self {
        self.cfg = self.cfg.with_zone(zone);
        self
    }

    /// Override the lease TTL (ms).
    #[must_use]
    pub fn lease_ttl_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_lease_ttl_ms(ms);
        self
    }

    /// Override the vnode count.
    #[must_use]
    pub fn ring_vnodes(mut self, n: u32) -> Self {
        self.cfg = self.cfg.with_ring_vnodes(n);
        self
    }

    /// Override the max node count.
    #[must_use]
    pub const fn max_nodes(mut self, n: u32) -> Self {
        self.cfg.max_nodes = n;
        self
    }

    /// Override the max session count.
    #[must_use]
    pub const fn max_sessions(mut self, n: u64) -> Self {
        self.cfg.max_sessions = n;
        self
    }

    /// Enable or disable rebalancing.
    #[must_use]
    pub const fn enable_rebalancing(mut self, on: bool) -> Self {
        self.cfg.enable_rebalancing = on;
        self
    }

    /// Enable or disable lease renewal.
    #[must_use]
    pub const fn enable_lease_renewal(mut self, on: bool) -> Self {
        self.cfg.enable_lease_renewal = on;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidConfig`] if any field is out
    /// of range.
    pub fn build(self) -> Result<RegistryConfig, RegistryError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::{MAX_LEASE_TTL_MS, MIN_LEASE_TTL_MS};
    use crate::ring::{MAX_VNODES, MIN_VNODES};

    #[test]
    fn new_rejects_nil_node() {
        assert!(RegistryBuilder::new(NodeId::NIL).is_err());
    }

    #[test]
    fn from_preset_rejects_nil_node() {
        assert!(RegistryBuilder::from_preset(Preset::SingleRegion, NodeId::NIL).is_err());
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = RegistryBuilder::new(NodeId::new(1))
            .unwrap()
            .region(Region::new(7))
            .zone(Zone::new(3))
            .lease_ttl_ms(45_000)
            .ring_vnodes(200)
            .build()
            .unwrap();
        assert_eq!(cfg.region, Region::new(7));
        assert_eq!(cfg.zone, Zone::new(3));
        assert_eq!(cfg.lease_ttl_ms, 45_000);
        assert_eq!(cfg.ring_vnodes, 200);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = RegistryBuilder::new(NodeId::new(1))
            .unwrap()
            .lease_ttl_ms(MIN_LEASE_TTL_MS / 2) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = RegistryBuilder::new(NodeId::new(1))
            .unwrap()
            .lease_ttl_ms(MIN_LEASE_TTL_MS)
            .ring_vnodes(MIN_VNODES)
            .build()
            .unwrap();
        assert_eq!(cfg.lease_ttl_ms, MIN_LEASE_TTL_MS);

        let cfg = RegistryBuilder::new(NodeId::new(1))
            .unwrap()
            .lease_ttl_ms(MAX_LEASE_TTL_MS)
            .ring_vnodes(MAX_VNODES)
            .build()
            .unwrap();
        assert_eq!(cfg.lease_ttl_ms, MAX_LEASE_TTL_MS);
        assert_eq!(cfg.ring_vnodes, MAX_VNODES);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = RegistryBuilder::from_preset(Preset::Hyperscale, NodeId::new(1))
            .unwrap()
            .lease_ttl_ms(15_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.lease_ttl_ms, 15_000);
        assert_eq!(cfg.ring_vnodes, 256); // from preset
    }

    #[test]
    fn builder_disables_flags() {
        let cfg = RegistryBuilder::new(NodeId::new(1))
            .unwrap()
            .enable_rebalancing(false)
            .enable_lease_renewal(false)
            .build()
            .unwrap();
        assert!(!cfg.enable_rebalancing);
        assert!(!cfg.enable_lease_renewal);
    }
}
