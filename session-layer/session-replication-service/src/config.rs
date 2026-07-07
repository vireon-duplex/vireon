//! [`ReplicationConfig`], [`Preset`] — declarative configuration for
//! the replication service.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The four presets encode replication archetypes:
//!
//! | Preset | Strategy | Consistency | Replicas | Timeout |
//! |--------|----------|-------------|----------|---------|
//! | `StrongConsistency` | Raft | Quorum | 3 | 5 s |
//! | `EventuallyConsistent` | Gossip | One | 5 | 10 s |
//! | `Hybrid` | Crdt | Quorum | 3 | 5 s |
//! | `EdgeReplication` | Gossip | One | 7 | 15 s |

use crate::error::ReplicationError;
use crate::strategy::{ConsistencyLevel, ReplicationStrategy};

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of replicas (a single replica is local-only).
pub const MIN_REPLICAS: u8 = 1;

/// Maximum number of replicas (hard cap from u8 space).
pub const MAX_REPLICAS: u8 = 255;

/// Minimum write timeout (ms).
pub const MIN_WRITE_TIMEOUT_MS: u64 = 100;

/// Maximum write timeout (ms) — 60 seconds.
pub const MAX_WRITE_TIMEOUT_MS: u64 = 60_000;

/// Minimum read timeout (ms).
pub const MIN_READ_TIMEOUT_MS: u64 = 100;

/// Maximum read timeout (ms) — 60 seconds.
pub const MAX_READ_TIMEOUT_MS: u64 = 60_000;

/// Minimum gossip interval (ms).
pub const MIN_GOSSIP_INTERVAL_MS: u64 = 100;

/// Minimum Raft election timeout (ms).
pub const MIN_RAFT_ELECTION_TIMEOUT_MS: u64 = 500;

// ── ReplicationConfig ───────────────────────────────────────────

/// Declarative configuration for the session replication service.
///
/// Constructed via [`ReplicationConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// The replication strategy to use.
    pub strategy: ReplicationStrategy,
    /// The consistency level for reads and writes.
    pub consistency: ConsistencyLevel,
    /// Number of replicas to maintain for each key.
    pub replica_count: u8,
    /// Write operation timeout (ms).
    pub write_timeout_ms: u64,
    /// Read operation timeout (ms).
    pub read_timeout_ms: u64,
    /// Maximum acceptable replication lag (ms) before alerting.
    pub max_lag_ms: u64,
    /// Whether automatic conflict detection and resolution is enabled.
    pub conflict_resolution_enabled: bool,
    /// Gossip protocol broadcast interval (ms).
    pub gossip_interval_ms: u64,
    /// Raft election timeout (ms).
    pub raft_election_timeout_ms: u64,
}

impl ReplicationConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: Raft strategy, Quorum consistency, 3 replicas,
    /// 5 s write timeout, 5 s read timeout, 1 s max lag,
    /// conflict resolution enabled, 1 s gossip interval,
    /// 1.5 s Raft election timeout.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            strategy: ReplicationStrategy::Raft,
            consistency: ConsistencyLevel::Quorum,
            replica_count: 3,
            write_timeout_ms: 5_000,
            read_timeout_ms: 5_000,
            max_lag_ms: 1_000,
            conflict_resolution_enabled: true,
            gossip_interval_ms: 1_000,
            raft_election_timeout_ms: 1_500,
        }
    }

    /// Builder-style: set the strategy.
    #[must_use]
    pub const fn with_strategy(mut self, strategy: ReplicationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Builder-style: set the consistency level.
    #[must_use]
    pub const fn with_consistency(mut self, consistency: ConsistencyLevel) -> Self {
        self.consistency = consistency;
        self
    }

    /// Builder-style: set the replica count.
    #[must_use]
    pub const fn with_replica_count(mut self, count: u8) -> Self {
        self.replica_count = count;
        self
    }

    /// Builder-style: set the write timeout (ms).
    #[must_use]
    pub const fn with_write_timeout_ms(mut self, ms: u64) -> Self {
        self.write_timeout_ms = ms;
        self
    }

    /// Builder-style: set the read timeout (ms).
    #[must_use]
    pub const fn with_read_timeout_ms(mut self, ms: u64) -> Self {
        self.read_timeout_ms = ms;
        self
    }

    /// Builder-style: set the max lag (ms).
    #[must_use]
    pub const fn with_max_lag_ms(mut self, ms: u64) -> Self {
        self.max_lag_ms = ms;
        self
    }

    /// Builder-style: enable or disable conflict resolution.
    #[must_use]
    pub const fn with_conflict_resolution(mut self, enabled: bool) -> Self {
        self.conflict_resolution_enabled = enabled;
        self
    }

    /// Builder-style: set the gossip interval (ms).
    #[must_use]
    pub const fn with_gossip_interval_ms(mut self, ms: u64) -> Self {
        self.gossip_interval_ms = ms;
        self
    }

    /// Builder-style: set the Raft election timeout (ms).
    #[must_use]
    pub const fn with_raft_election_timeout_ms(mut self, ms: u64) -> Self {
        self.raft_election_timeout_ms = ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`ReplicationError::InvalidConfig`] if any field is out
    /// of range or contradictory.
    pub fn validate(&self) -> Result<(), ReplicationError> {
        if self.replica_count < MIN_REPLICAS {
            return Err(ReplicationError::InvalidConfig(format!(
                "replica_count {n} below minimum {MIN_REPLICAS}",
                n = self.replica_count
            )));
        }
        if self.write_timeout_ms < MIN_WRITE_TIMEOUT_MS
            || self.write_timeout_ms > MAX_WRITE_TIMEOUT_MS
        {
            return Err(ReplicationError::InvalidConfig(format!(
                "write_timeout_ms {ms} out of range [{MIN_WRITE_TIMEOUT_MS}, {MAX_WRITE_TIMEOUT_MS}]",
                ms = self.write_timeout_ms
            )));
        }
        if self.read_timeout_ms < MIN_READ_TIMEOUT_MS || self.read_timeout_ms > MAX_READ_TIMEOUT_MS
        {
            return Err(ReplicationError::InvalidConfig(format!(
                "read_timeout_ms {ms} out of range [{MIN_READ_TIMEOUT_MS}, {MAX_READ_TIMEOUT_MS}]",
                ms = self.read_timeout_ms
            )));
        }
        if self.max_lag_ms == 0 {
            return Err(ReplicationError::InvalidConfig(
                "max_lag_ms must be > 0".to_string(),
            ));
        }
        if self.gossip_interval_ms < MIN_GOSSIP_INTERVAL_MS {
            return Err(ReplicationError::InvalidConfig(format!(
                "gossip_interval_ms {ms} below minimum {MIN_GOSSIP_INTERVAL_MS}",
                ms = self.gossip_interval_ms
            )));
        }
        if self.raft_election_timeout_ms < MIN_RAFT_ELECTION_TIMEOUT_MS {
            return Err(ReplicationError::InvalidConfig(format!(
                "raft_election_timeout_ms {ms} below minimum {MIN_RAFT_ELECTION_TIMEOUT_MS}",
                ms = self.raft_election_timeout_ms
            )));
        }
        // Consistency All requires at least 2 replicas for replication
        // to be meaningful (1 replica = no replication).
        if matches!(self.consistency, ConsistencyLevel::All) && self.replica_count < 2 {
            return Err(ReplicationError::InvalidConfig(
                "consistency=All requires replica_count >= 2".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as replication configurations.
///
/// Each preset encodes a well-studied point on the CAP-latency
/// trade-off curve. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Strong consistency via Raft consensus. Prioritizes correctness
    /// over availability. Best for financial or critical metadata.
    StrongConsistency,
    /// Eventual consistency via Gossip. Prioritizes availability
    /// and partition tolerance. Best for edge / IoT deployments.
    EventuallyConsistent,
    /// Hybrid approach using CRDTs for conflict-free convergence.
    /// Good balance for multi-region deployments.
    Hybrid,
    /// Edge replication: many small replicas, long timeouts,
    /// gossip-based. For constrained-network environments.
    EdgeReplication,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    ///
    /// # Errors
    /// This function always returns a valid config; `Result` is
    /// reserved for future extensibility.
    pub fn to_config(self) -> Result<ReplicationConfig, ReplicationError> {
        let cfg = match self {
            Self::StrongConsistency => ReplicationConfig::new()
                .with_strategy(ReplicationStrategy::Raft)
                .with_consistency(ConsistencyLevel::Quorum)
                .with_replica_count(3)
                .with_write_timeout_ms(5_000)
                .with_read_timeout_ms(5_000)
                .with_max_lag_ms(500)
                .with_conflict_resolution(true)
                .with_gossip_interval_ms(1_000)
                .with_raft_election_timeout_ms(1_500),
            Self::EventuallyConsistent => ReplicationConfig::new()
                .with_strategy(ReplicationStrategy::Gossip)
                .with_consistency(ConsistencyLevel::One)
                .with_replica_count(5)
                .with_write_timeout_ms(10_000)
                .with_read_timeout_ms(10_000)
                .with_max_lag_ms(5_000)
                .with_conflict_resolution(true)
                .with_gossip_interval_ms(500)
                .with_raft_election_timeout_ms(2_000),
            Self::Hybrid => ReplicationConfig::new()
                .with_strategy(ReplicationStrategy::Crdt)
                .with_consistency(ConsistencyLevel::Quorum)
                .with_replica_count(3)
                .with_write_timeout_ms(5_000)
                .with_read_timeout_ms(3_000)
                .with_max_lag_ms(2_000)
                .with_conflict_resolution(false) // CRDTs are conflict-free
                .with_gossip_interval_ms(2_000)
                .with_raft_election_timeout_ms(1_500),
            Self::EdgeReplication => ReplicationConfig::new()
                .with_strategy(ReplicationStrategy::Gossip)
                .with_consistency(ConsistencyLevel::One)
                .with_replica_count(7)
                .with_write_timeout_ms(15_000)
                .with_read_timeout_ms(15_000)
                .with_max_lag_ms(10_000)
                .with_conflict_resolution(true)
                .with_gossip_interval_ms(3_000)
                .with_raft_election_timeout_ms(5_000),
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = ReplicationConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.strategy, ReplicationStrategy::Raft);
        assert_eq!(cfg.consistency, ConsistencyLevel::Quorum);
        assert_eq!(cfg.replica_count, 3);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = ReplicationConfig::new()
            .with_strategy(ReplicationStrategy::Gossip)
            .with_consistency(ConsistencyLevel::One)
            .with_replica_count(5)
            .with_write_timeout_ms(10_000)
            .with_read_timeout_ms(8_000)
            .with_max_lag_ms(3_000)
            .with_conflict_resolution(false)
            .with_gossip_interval_ms(500)
            .with_raft_election_timeout_ms(2_000);
        assert_eq!(cfg.strategy, ReplicationStrategy::Gossip);
        assert_eq!(cfg.consistency, ConsistencyLevel::One);
        assert_eq!(cfg.replica_count, 5);
        assert_eq!(cfg.write_timeout_ms, 10_000);
        assert!(!cfg.conflict_resolution_enabled);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_replicas() {
        let cfg = ReplicationConfig::new().with_replica_count(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_write_timeout() {
        let mut cfg = ReplicationConfig::new();
        cfg.write_timeout_ms = 10;
        assert!(cfg.validate().is_err());
        cfg.write_timeout_ms = MAX_WRITE_TIMEOUT_MS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_read_timeout() {
        let mut cfg = ReplicationConfig::new();
        cfg.read_timeout_ms = 10;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_lag() {
        let mut cfg = ReplicationConfig::new();
        cfg.max_lag_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_all_consistency_with_one_replica() {
        let cfg = ReplicationConfig::new()
            .with_consistency(ConsistencyLevel::All)
            .with_replica_count(1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [
            Preset::StrongConsistency,
            Preset::EventuallyConsistent,
            Preset::Hybrid,
            Preset::EdgeReplication,
        ] {
            let cfg = preset.to_config().unwrap_or_else(|e| {
                let _ = format!("preset {preset:?} failed: {e}");
                ReplicationConfig::default()
            });
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn preset_strong_consistency_uses_raft() {
        let cfg = Preset::StrongConsistency.to_config().unwrap();
        assert_eq!(cfg.strategy, ReplicationStrategy::Raft);
        assert_eq!(cfg.consistency, ConsistencyLevel::Quorum);
        assert_eq!(cfg.replica_count, 3);
    }

    #[test]
    fn preset_eventually_consistent_uses_gossip() {
        let cfg = Preset::EventuallyConsistent.to_config().unwrap();
        assert_eq!(cfg.strategy, ReplicationStrategy::Gossip);
        assert_eq!(cfg.consistency, ConsistencyLevel::One);
        assert_eq!(cfg.replica_count, 5);
    }

    #[test]
    fn preset_hybrid_uses_crdt() {
        let cfg = Preset::Hybrid.to_config().unwrap();
        assert_eq!(cfg.strategy, ReplicationStrategy::Crdt);
        assert!(!cfg.conflict_resolution_enabled);
    }

    #[test]
    fn preset_edge_replication_uses_many_replicas() {
        let cfg = Preset::EdgeReplication.to_config().unwrap();
        assert_eq!(cfg.replica_count, 7);
        assert_eq!(cfg.strategy, ReplicationStrategy::Gossip);
    }
}
