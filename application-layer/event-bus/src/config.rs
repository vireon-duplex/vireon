//! [`EventBusConfig`], [`Preset`] — declarative configuration for
//! the partitioned event bus.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes studied in the
//! Kafka/Pulsar literature:
//!
//! | Preset | Topics | Partitions | Retention | Message Size | Group Timeout |
//! |--------|--------|------------|-----------|--------------|---------------|
//! | `Local` | 10 | 8 | 1 day | 1 MB | 30 s |
//! | `Clustered` | 100 | 32 | 7 days | 4 MB | 60 s |
//! | `HighThroughput` | 1000 | 128 | 30 days | 16 MB | 120 s |

use crate::error::EventError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of topics the bus will accept.
pub const MIN_TOPICS: u32 = 1;

/// Maximum number of topics the bus will accept.
pub const MAX_TOPICS: u32 = 100_000;

/// Minimum number of partitions per topic.
pub const MIN_PARTITIONS: u8 = 1;

/// Maximum number of partitions per topic.
pub const MAX_PARTITIONS: u8 = 255;

/// Minimum retention time in milliseconds (1 hour).
pub const MIN_RETENTION_MS: u64 = 3_600_000;

/// Maximum retention time in milliseconds (365 days).
pub const MAX_RETENTION_MS: u64 = 31_536_000_000;

/// Minimum message size in bytes.
pub const MIN_MESSAGE_BYTES: u32 = 512;

/// Maximum message size in bytes.
pub const MAX_MESSAGE_BYTES: u32 = 50_000_000;

/// Minimum consumer group timeout in milliseconds.
pub const MIN_GROUP_TIMEOUT_MS: u64 = 5_000;

/// Maximum consumer group timeout in milliseconds (1 hour).
pub const MAX_GROUP_TIMEOUT_MS: u64 = 3_600_000;

// ── EventBusConfig ───────────────────────────────────────────────

/// Declarative configuration for the partitioned event bus.
///
/// Constructed via [`EventBusConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventBusConfig {
    /// Maximum number of topics.
    pub max_topics: u32,
    /// Default number of partitions per topic.
    pub default_partitions: u8,
    /// Message retention time in milliseconds.
    pub retention_ms: u64,
    /// Maximum message size in bytes.
    pub max_message_bytes: u32,
    /// Consumer group timeout in milliseconds.
    pub consumer_group_timeout_ms: u64,
}

impl EventBusConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 100 topics, 8 partitions, 7-day retention, 1 MB messages,
    /// 60-second group timeout.
    pub fn new() -> Self {
        Self {
            max_topics: 100,
            default_partitions: 8,
            retention_ms: 604_800_000, // 7 days
            max_message_bytes: 1_000_000,
            consumer_group_timeout_ms: 60_000,
        }
    }

    /// Builder-style: set the max topics.
    #[must_use]
    pub const fn with_max_topics(mut self, topics: u32) -> Self {
        self.max_topics = topics;
        self
    }

    /// Builder-style: set the default partitions.
    #[must_use]
    pub const fn with_default_partitions(mut self, partitions: u8) -> Self {
        self.default_partitions = partitions;
        self
    }

    /// Builder-style: set the retention time (ms).
    #[must_use]
    pub const fn with_retention_ms(mut self, ms: u64) -> Self {
        self.retention_ms = ms;
        self
    }

    /// Builder-style: set the max message bytes.
    #[must_use]
    pub const fn with_max_message_bytes(mut self, bytes: u32) -> Self {
        self.max_message_bytes = bytes;
        self
    }

    /// Builder-style: set the consumer group timeout (ms).
    #[must_use]
    pub const fn with_consumer_group_timeout_ms(mut self, ms: u64) -> Self {
        self.consumer_group_timeout_ms = ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`EventError::PublishFailed`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), EventError> {
        if self.max_topics < MIN_TOPICS || self.max_topics > MAX_TOPICS {
            return Err(EventError::PublishFailed("max_topics out of range"));
        }
        if self.default_partitions < MIN_PARTITIONS {
            return Err(EventError::PublishFailed("default_partitions out of range"));
        }
        if self.retention_ms < MIN_RETENTION_MS || self.retention_ms > MAX_RETENTION_MS {
            return Err(EventError::PublishFailed("retention_ms out of range"));
        }
        if self.max_message_bytes < MIN_MESSAGE_BYTES || self.max_message_bytes > MAX_MESSAGE_BYTES
        {
            return Err(EventError::PublishFailed("max_message_bytes out of range"));
        }
        if self.consumer_group_timeout_ms < MIN_GROUP_TIMEOUT_MS
            || self.consumer_group_timeout_ms > MAX_GROUP_TIMEOUT_MS
        {
            return Err(EventError::PublishFailed(
                "consumer_group_timeout_ms out of range",
            ));
        }
        Ok(())
    }
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// workload profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Local deployment: fewer topics, fewer partitions, short retention.
    Local,
    /// Clustered deployment: moderate settings for production clusters.
    Clustered,
    /// High-throughput deployment: many topics, many partitions, long retention.
    HighThroughput,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub fn to_config(self) -> EventBusConfig {
        match self {
            Self::Local => EventBusConfig::new()
                .with_max_topics(10)
                .with_default_partitions(8)
                .with_retention_ms(86_400_000) // 1 day
                .with_max_message_bytes(1_000_000)
                .with_consumer_group_timeout_ms(30_000),
            Self::Clustered => EventBusConfig::new(), // Defaults match clustered
            Self::HighThroughput => EventBusConfig::new()
                .with_max_topics(1000)
                .with_default_partitions(128)
                .with_retention_ms(2_592_000_000) // 30 days
                .with_max_message_bytes(16_000_000)
                .with_consumer_group_timeout_ms(120_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = EventBusConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_topics, 100);
        assert_eq!(cfg.default_partitions, 8);
        assert_eq!(cfg.retention_ms, 604_800_000);
        assert_eq!(cfg.max_message_bytes, 1_000_000);
        assert_eq!(cfg.consumer_group_timeout_ms, 60_000);
    }

    #[test]
    fn new_matches_default() {
        let cfg = EventBusConfig::new();
        let default = EventBusConfig::default();
        assert_eq!(cfg, default);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = EventBusConfig::new()
            .with_max_topics(500)
            .with_default_partitions(16)
            .with_retention_ms(1_209_600_000)
            .with_max_message_bytes(8_000_000)
            .with_consumer_group_timeout_ms(90_000);
        assert_eq!(cfg.max_topics, 500);
        assert_eq!(cfg.default_partitions, 16);
        assert_eq!(cfg.retention_ms, 1_209_600_000);
        assert_eq!(cfg.max_message_bytes, 8_000_000);
        assert_eq!(cfg.consumer_group_timeout_ms, 90_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_topic_count() {
        let mut cfg = EventBusConfig::default();
        cfg.max_topics = 0;
        assert!(cfg.validate().is_err());
        cfg.max_topics = MAX_TOPICS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_partition_count() {
        let mut cfg = EventBusConfig::default();
        cfg.default_partitions = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_retention() {
        let mut cfg = EventBusConfig::default();
        cfg.retention_ms = MIN_RETENTION_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.retention_ms = MAX_RETENTION_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_message_size() {
        let mut cfg = EventBusConfig::default();
        cfg.max_message_bytes = MIN_MESSAGE_BYTES / 2;
        assert!(cfg.validate().is_err());
        cfg.max_message_bytes = MAX_MESSAGE_BYTES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_group_timeout() {
        let mut cfg = EventBusConfig::default();
        cfg.consumer_group_timeout_ms = MIN_GROUP_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.consumer_group_timeout_ms = MAX_GROUP_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn local_preset_config() {
        let cfg = Preset::Local.to_config();
        assert_eq!(cfg.max_topics, 10);
        assert_eq!(cfg.default_partitions, 8);
        assert_eq!(cfg.retention_ms, 86_400_000);
        assert_eq!(cfg.max_message_bytes, 1_000_000);
        assert_eq!(cfg.consumer_group_timeout_ms, 30_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn clustered_preset_matches_default() {
        let preset_cfg = Preset::Clustered.to_config();
        let default_cfg = EventBusConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn high_throughput_preset_config() {
        let cfg = Preset::HighThroughput.to_config();
        assert_eq!(cfg.max_topics, 1000);
        assert_eq!(cfg.default_partitions, 128);
        assert_eq!(cfg.retention_ms, 2_592_000_000);
        assert_eq!(cfg.max_message_bytes, 16_000_000);
        assert_eq!(cfg.consumer_group_timeout_ms, 120_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Local, Preset::Clustered, Preset::HighThroughput] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn builder_overrides_preset() {
        let cfg = Preset::Local
            .to_config()
            .with_max_topics(50)
            .with_retention_ms(604_800_000);
        assert_eq!(cfg.max_topics, 50);
        assert_eq!(cfg.retention_ms, 604_800_000);
        assert_eq!(cfg.default_partitions, 8); // from preset
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = EventBusConfig::new()
            .with_max_topics(MIN_TOPICS)
            .with_default_partitions(MIN_PARTITIONS)
            .with_retention_ms(MIN_RETENTION_MS)
            .with_max_message_bytes(MIN_MESSAGE_BYTES)
            .with_consumer_group_timeout_ms(MIN_GROUP_TIMEOUT_MS);
        assert!(cfg.validate().is_ok());

        let cfg = EventBusConfig::new()
            .with_max_topics(MAX_TOPICS)
            .with_default_partitions(MAX_PARTITIONS)
            .with_retention_ms(MAX_RETENTION_MS)
            .with_max_message_bytes(MAX_MESSAGE_BYTES)
            .with_consumer_group_timeout_ms(MAX_GROUP_TIMEOUT_MS);
        assert!(cfg.validate().is_ok());
    }
}
