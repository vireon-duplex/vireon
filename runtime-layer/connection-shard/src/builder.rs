//! [`ConnectionShardBuilder`] — fluent builder (Layer 15).
//!
//! Follows the workspace pattern: fluent setters with `#[must_use]`,
//! consuming `build()` that validates and constructs the shard.

use crate::config::{Preset, ShardConfig};
use crate::error::ShardError;
use crate::shard::ConnectionShard;
use crate::strategy::ShardingStrategy;

/// Fluent builder for [`ConnectionShard`].
///
/// ```
/// use connection_shard::ConnectionShardBuilder;
///
/// let shard = ConnectionShardBuilder::new()
///     .reactor_id(2)
///     .capacity(8192)
///     .soft_bp_pct(75)
///     .build()
///     .expect("shard build");
/// ```
#[derive(Debug)]
pub struct ConnectionShardBuilder {
    config: ShardConfig,
    strategy: Option<Box<dyn ShardingStrategy>>,
}

impl Default for ConnectionShardBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionShardBuilder {
    /// Creates a builder with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: ShardConfig::default(),
            strategy: None,
        }
    }

    /// Creates a builder pre-populated from a named preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            config: preset.to_config(),
            strategy: None,
        }
    }

    // ── Setters ─────────────────────────────────────────────────

    /// Sets the reactor / core id that will own this shard.
    #[must_use]
    pub fn reactor_id(mut self, reactor_id: u32) -> Self {
        self.config.reactor_id = reactor_id;
        self
    }

    /// Sets the NUMA node hint.
    #[must_use]
    pub fn numa_node(mut self, numa_node: u32) -> Self {
        self.config.numa_node = numa_node;
        self
    }

    /// Sets the slab capacity (number of connection slots).
    #[must_use]
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.config.capacity = capacity;
        self
    }

    /// Sets the soft-backpressure threshold percentage.
    #[must_use]
    pub fn soft_bp_pct(mut self, pct: u8) -> Self {
        self.config.soft_bp_pct = pct;
        self
    }

    /// Sets the hard-backpressure threshold percentage.
    #[must_use]
    pub fn hard_bp_pct(mut self, pct: u8) -> Self {
        self.config.hard_bp_pct = pct;
        self
    }

    /// Marks the shard as pre-shutdown (for testing).
    #[must_use]
    pub fn shutting_down(mut self) -> Self {
        self.config.shutting_down = true;
        self
    }

    /// Sets a custom sharding strategy.
    ///
    /// If not called, [`LocalSharding`](crate::strategy::LocalSharding)
    /// bound to the reactor id is used.
    #[must_use]
    pub fn strategy(mut self, strategy: Box<dyn ShardingStrategy>) -> Self {
        self.strategy = Some(strategy);
        self
    }

    // ── Build ───────────────────────────────────────────────────

    /// Validates the configuration and builds the shard.
    ///
    /// Returns [`ShardError::InvalidConfig`] if any constraint is
    /// violated.
    pub fn build(self) -> Result<ConnectionShard, ShardError> {
        let mut shard = ConnectionShard::from_config(self.config)?;
        if let Some(strategy) = self.strategy {
            shard = shard.with_strategy(strategy);
        }
        Ok(shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_CAPACITY, DEFAULT_HARD_BP_PCT, DEFAULT_SOFT_BP_PCT};
    use crate::strategy::ModuloSharding;

    #[test]
    fn default_builds() {
        let shard = ConnectionShardBuilder::new().build();
        assert!(shard.is_ok());
    }

    #[test]
    fn invalid_capacity_rejected() {
        let result = ConnectionShardBuilder::new().capacity(1).build();
        assert!(matches!(result, Err(ShardError::InvalidConfig(_))));
    }

    #[test]
    fn invalid_bp_rejected() {
        let result = ConnectionShardBuilder::new()
            .soft_bp_pct(90)
            .hard_bp_pct(80)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn custom_strategy_applied() {
        let shard = ConnectionShardBuilder::new()
            .strategy(Box::new(ModuloSharding))
            .build()
            .expect("build");
        // With ModuloSharding, stream_id 5 % 1 = 0 (single shard).
        assert_eq!(shard.shard_for_stream(5), 0);
    }

    #[test]
    fn preset_builds() {
        let shard = ConnectionShardBuilder::from_preset(Preset::Cloud).build();
        assert!(shard.is_ok());
    }

    #[test]
    fn reactor_id_set() {
        let shard = ConnectionShardBuilder::new()
            .reactor_id(7)
            .build()
            .expect("build");
        assert_eq!(shard.reactor_id(), 7);
    }

    #[test]
    fn capacity_set() {
        let shard = ConnectionShardBuilder::new()
            .capacity(8192)
            .build()
            .expect("build");
        assert_eq!(shard.capacity(), 8192);
    }

    #[test]
    fn default_capacity_matches_constant() {
        let shard = ConnectionShardBuilder::new().build().expect("build");
        assert_eq!(shard.capacity(), DEFAULT_CAPACITY);
    }

    #[test]
    fn default_bp_pcts_match_constants() {
        let config = ConnectionShardBuilder::new().build().expect("build");
        assert_eq!(config.config().soft_bp_pct, DEFAULT_SOFT_BP_PCT);
        assert_eq!(config.config().hard_bp_pct, DEFAULT_HARD_BP_PCT);
    }
}
