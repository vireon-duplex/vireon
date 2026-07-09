//! [`PubSubConfig`], [`Preset`] — declarative configuration for
//! the pub/sub engine.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes studied in the
//! MQTT/Pulsar/NATS literature:
//!
//! | Preset | Max Subs | Max Patterns | Delivery Timeout | Backlog Limit |
//! |--------|----------|--------------|------------------|---------------|
//! | `Local` | 128 | 64 | 5 s | 1,024 |
//! | `Clustered` | 1,024 | 256 | 10 s | 10,000 |
//! | `HighThroughput` | 10,000 | 1,024 | 30 s | 100,000 |

use crate::error::PubSubError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of subscriptions the engine will accept.
pub const MIN_MAX_SUBSCRIPTIONS: u32 = 1;

/// Maximum number of subscriptions the engine will accept.
pub const MAX_MAX_SUBSCRIPTIONS: u32 = 1_000_000;

/// Minimum number of topic patterns the engine will accept.
pub const MIN_MAX_TOPIC_PATTERNS: u32 = 1;

/// Maximum number of topic patterns the engine will accept.
pub const MAX_MAX_TOPIC_PATTERNS: u32 = 100_000;

/// Minimum delivery timeout in milliseconds (100 ms).
pub const MIN_DELIVERY_TIMEOUT_MS: u64 = 100;

/// Maximum delivery timeout in milliseconds (5 minutes).
pub const MAX_DELIVERY_TIMEOUT_MS: u64 = 300_000;

/// Minimum backlog limit (number of queued messages).
pub const MIN_BACKLOG_LIMIT: u32 = 16;

/// Maximum backlog limit (number of queued messages).
pub const MAX_BACKLOG_LIMIT: u32 = 1_000_000;

// ── PubSubConfig ────────────────────────────────────────────────

/// Declarative configuration for the pub/sub engine.
///
/// Constructed via [`PubSubConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PubSubConfig {
    /// Maximum number of concurrent subscriptions.
    pub max_subscriptions: u32,
    /// Maximum number of topic patterns the engine tracks.
    pub max_topic_patterns: u32,
    /// Delivery timeout in milliseconds.
    pub delivery_timeout_ms: u64,
    /// Maximum number of messages queued per subscription (backlog).
    pub backlog_limit: u32,
}

impl PubSubConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 1024 subscriptions, 256 patterns, 10-second delivery
    /// timeout, 10,000 backlog limit.
    pub fn new() -> Self {
        Self {
            max_subscriptions: 1_024,
            max_topic_patterns: 256,
            delivery_timeout_ms: 10_000,
            backlog_limit: 10_000,
        }
    }

    /// Builder-style: set the max subscriptions.
    #[must_use]
    pub const fn with_max_subscriptions(mut self, max: u32) -> Self {
        self.max_subscriptions = max;
        self
    }

    /// Builder-style: set the max topic patterns.
    #[must_use]
    pub const fn with_max_topic_patterns(mut self, max: u32) -> Self {
        self.max_topic_patterns = max;
        self
    }

    /// Builder-style: set the delivery timeout (ms).
    #[must_use]
    pub const fn with_delivery_timeout_ms(mut self, ms: u64) -> Self {
        self.delivery_timeout_ms = ms;
        self
    }

    /// Builder-style: set the backlog limit.
    #[must_use]
    pub const fn with_backlog_limit(mut self, limit: u32) -> Self {
        self.backlog_limit = limit;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`PubSubError::PublishFailed`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), PubSubError> {
        if self.max_subscriptions < MIN_MAX_SUBSCRIPTIONS
            || self.max_subscriptions > MAX_MAX_SUBSCRIPTIONS
        {
            return Err(PubSubError::PublishFailed("max_subscriptions out of range"));
        }
        if self.max_topic_patterns < MIN_MAX_TOPIC_PATTERNS
            || self.max_topic_patterns > MAX_MAX_TOPIC_PATTERNS
        {
            return Err(PubSubError::PublishFailed(
                "max_topic_patterns out of range",
            ));
        }
        if self.delivery_timeout_ms < MIN_DELIVERY_TIMEOUT_MS
            || self.delivery_timeout_ms > MAX_DELIVERY_TIMEOUT_MS
        {
            return Err(PubSubError::PublishFailed(
                "delivery_timeout_ms out of range",
            ));
        }
        if self.backlog_limit < MIN_BACKLOG_LIMIT || self.backlog_limit > MAX_BACKLOG_LIMIT {
            return Err(PubSubError::PublishFailed("backlog_limit out of range"));
        }
        Ok(())
    }
}

impl Default for PubSubConfig {
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
    /// Local deployment: fewer subscriptions, short delivery timeout.
    Local,
    /// Clustered deployment: moderate settings for production clusters.
    Clustered,
    /// High-throughput deployment: many subscriptions, large backlog.
    HighThroughput,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub fn to_config(self) -> PubSubConfig {
        match self {
            Self::Local => PubSubConfig::new()
                .with_max_subscriptions(128)
                .with_max_topic_patterns(64)
                .with_delivery_timeout_ms(5_000)
                .with_backlog_limit(1_024),
            Self::Clustered => PubSubConfig::new(), // Defaults match clustered
            Self::HighThroughput => PubSubConfig::new()
                .with_max_subscriptions(10_000)
                .with_max_topic_patterns(1_024)
                .with_delivery_timeout_ms(30_000)
                .with_backlog_limit(100_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = PubSubConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_subscriptions, 1_024);
        assert_eq!(cfg.max_topic_patterns, 256);
        assert_eq!(cfg.delivery_timeout_ms, 10_000);
        assert_eq!(cfg.backlog_limit, 10_000);
    }

    #[test]
    fn new_matches_default() {
        let cfg = PubSubConfig::new();
        let default = PubSubConfig::default();
        assert_eq!(cfg, default);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = PubSubConfig::new()
            .with_max_subscriptions(500)
            .with_max_topic_patterns(128)
            .with_delivery_timeout_ms(15_000)
            .with_backlog_limit(5_000);
        assert_eq!(cfg.max_subscriptions, 500);
        assert_eq!(cfg.max_topic_patterns, 128);
        assert_eq!(cfg.delivery_timeout_ms, 15_000);
        assert_eq!(cfg.backlog_limit, 5_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_subscription_count() {
        let mut cfg = PubSubConfig::default();
        cfg.max_subscriptions = 0;
        assert!(cfg.validate().is_err());
        cfg.max_subscriptions = MAX_MAX_SUBSCRIPTIONS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_pattern_count() {
        let mut cfg = PubSubConfig::default();
        cfg.max_topic_patterns = 0;
        assert!(cfg.validate().is_err());
        cfg.max_topic_patterns = MAX_MAX_TOPIC_PATTERNS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_delivery_timeout() {
        let mut cfg = PubSubConfig::default();
        cfg.delivery_timeout_ms = MIN_DELIVERY_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.delivery_timeout_ms = MAX_DELIVERY_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_backlog_limit() {
        let mut cfg = PubSubConfig::default();
        cfg.backlog_limit = 0;
        assert!(cfg.validate().is_err());
        cfg.backlog_limit = MAX_BACKLOG_LIMIT + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn local_preset_config() {
        let cfg = Preset::Local.to_config();
        assert_eq!(cfg.max_subscriptions, 128);
        assert_eq!(cfg.max_topic_patterns, 64);
        assert_eq!(cfg.delivery_timeout_ms, 5_000);
        assert_eq!(cfg.backlog_limit, 1_024);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn clustered_preset_matches_default() {
        let preset_cfg = Preset::Clustered.to_config();
        let default_cfg = PubSubConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn high_throughput_preset_config() {
        let cfg = Preset::HighThroughput.to_config();
        assert_eq!(cfg.max_subscriptions, 10_000);
        assert_eq!(cfg.max_topic_patterns, 1_024);
        assert_eq!(cfg.delivery_timeout_ms, 30_000);
        assert_eq!(cfg.backlog_limit, 100_000);
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
            .with_max_subscriptions(50)
            .with_delivery_timeout_ms(8_000);
        assert_eq!(cfg.max_subscriptions, 50);
        assert_eq!(cfg.delivery_timeout_ms, 8_000);
        assert_eq!(cfg.max_topic_patterns, 64); // from preset
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = PubSubConfig::new()
            .with_max_subscriptions(MIN_MAX_SUBSCRIPTIONS)
            .with_max_topic_patterns(MIN_MAX_TOPIC_PATTERNS)
            .with_delivery_timeout_ms(MIN_DELIVERY_TIMEOUT_MS)
            .with_backlog_limit(MIN_BACKLOG_LIMIT);
        assert!(cfg.validate().is_ok());

        let cfg = PubSubConfig::new()
            .with_max_subscriptions(MAX_MAX_SUBSCRIPTIONS)
            .with_max_topic_patterns(MAX_MAX_TOPIC_PATTERNS)
            .with_delivery_timeout_ms(MAX_DELIVERY_TIMEOUT_MS)
            .with_backlog_limit(MAX_BACKLOG_LIMIT);
        assert!(cfg.validate().is_ok());
    }
}
