//! [`ReplicationServiceBuilder`] — fluent builder for
//! [`ReplicationConfig`] and the replication service.
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `ReplicationConfig` has many fields and most callers only need
//! to override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides.

use crate::config::{Preset, ReplicationConfig};
use crate::error::ReplicationError;
use crate::strategy::{ConsistencyLevel, ReplicationStrategy};

/// Fluent builder for [`ReplicationConfig`].
#[derive(Debug, Clone)]
pub struct ReplicationServiceBuilder {
    cfg: ReplicationConfig,
}

impl ReplicationServiceBuilder {
    /// Start a new builder with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: ReplicationConfig::new(),
        }
    }

    /// Start from a preset.
    ///
    /// # Errors
    /// Returns [`ReplicationError`] if the preset produces an invalid
    /// config (shouldn't happen for built-in presets).
    pub fn from_preset(preset: Preset) -> Result<Self, ReplicationError> {
        Ok(Self {
            cfg: preset.to_config()?,
        })
    }

    /// Override the replication strategy.
    #[must_use]
    pub const fn strategy(mut self, strategy: ReplicationStrategy) -> Self {
        self.cfg = self.cfg.with_strategy(strategy);
        self
    }

    /// Override the consistency level.
    #[must_use]
    pub const fn consistency(mut self, consistency: ConsistencyLevel) -> Self {
        self.cfg = self.cfg.with_consistency(consistency);
        self
    }

    /// Override the replica count.
    #[must_use]
    pub const fn replica_count(mut self, count: u8) -> Self {
        self.cfg = self.cfg.with_replica_count(count);
        self
    }

    /// Override the write timeout (ms).
    #[must_use]
    pub const fn write_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_write_timeout_ms(ms);
        self
    }

    /// Override the read timeout (ms).
    #[must_use]
    pub const fn read_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_read_timeout_ms(ms);
        self
    }

    /// Override the max lag (ms).
    #[must_use]
    pub const fn max_lag_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_max_lag_ms(ms);
        self
    }

    /// Enable or disable conflict resolution.
    #[must_use]
    pub const fn conflict_resolution(mut self, enabled: bool) -> Self {
        self.cfg = self.cfg.with_conflict_resolution(enabled);
        self
    }

    /// Override the gossip interval (ms).
    #[must_use]
    pub const fn gossip_interval_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_gossip_interval_ms(ms);
        self
    }

    /// Override the Raft election timeout (ms).
    #[must_use]
    pub const fn raft_election_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_raft_election_timeout_ms(ms);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`ReplicationError::InvalidConfig`] if any field is
    /// out of range.
    pub fn build(self) -> Result<ReplicationConfig, ReplicationError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for ReplicationServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_new_produces_valid_config() {
        let cfg = ReplicationServiceBuilder::new().build().unwrap();
        assert_eq!(cfg.strategy, ReplicationStrategy::Raft);
        assert_eq!(cfg.consistency, ConsistencyLevel::Quorum);
        assert_eq!(cfg.replica_count, 3);
    }

    #[test]
    fn builder_from_preset() {
        let cfg = ReplicationServiceBuilder::from_preset(Preset::Hybrid)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(cfg.strategy, ReplicationStrategy::Crdt);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = ReplicationServiceBuilder::new()
            .strategy(ReplicationStrategy::Gossip)
            .consistency(ConsistencyLevel::One)
            .replica_count(5)
            .write_timeout_ms(10_000)
            .max_lag_ms(3_000)
            .build()
            .unwrap();
        assert_eq!(cfg.strategy, ReplicationStrategy::Gossip);
        assert_eq!(cfg.consistency, ConsistencyLevel::One);
        assert_eq!(cfg.replica_count, 5);
        assert_eq!(cfg.write_timeout_ms, 10_000);
        assert_eq!(cfg.max_lag_ms, 3_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = ReplicationServiceBuilder::new()
            .replica_count(0) // invalid
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_disables_conflict_resolution() {
        let cfg = ReplicationServiceBuilder::new()
            .conflict_resolution(false)
            .build()
            .unwrap();
        assert!(!cfg.conflict_resolution_enabled);
    }
}
