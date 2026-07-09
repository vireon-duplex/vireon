//! [`PubSubBuilder`] — fluent builder for [`PubSubConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).

use crate::config::{Preset, PubSubConfig};

/// Fluent builder for [`PubSubConfig`].
#[derive(Debug, Clone)]
pub struct PubSubBuilder {
    cfg: PubSubConfig,
}

impl PubSubBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: PubSubConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max subscriptions.
    #[must_use]
    pub fn max_subscriptions(mut self, max: u32) -> Self {
        self.cfg = self.cfg.with_max_subscriptions(max);
        self
    }

    /// Override the max topic patterns.
    #[must_use]
    pub fn max_topic_patterns(mut self, max: u32) -> Self {
        self.cfg = self.cfg.with_max_topic_patterns(max);
        self
    }

    /// Override the delivery timeout (ms).
    #[must_use]
    pub fn delivery_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_delivery_timeout_ms(ms);
        self
    }

    /// Override the backlog limit.
    #[must_use]
    pub fn backlog_limit(mut self, limit: u32) -> Self {
        self.cfg = self.cfg.with_backlog_limit(limit);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`crate::PubSubError::PublishFailed`] if any field is out
    /// of range.
    pub fn build(self) -> Result<PubSubConfig, crate::PubSubError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for PubSubBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_BACKLOG_LIMIT, MAX_DELIVERY_TIMEOUT_MS, MAX_MAX_SUBSCRIPTIONS, MAX_MAX_TOPIC_PATTERNS,
        MIN_BACKLOG_LIMIT, MIN_DELIVERY_TIMEOUT_MS, MIN_MAX_SUBSCRIPTIONS, MIN_MAX_TOPIC_PATTERNS,
    };

    #[test]
    fn builder_new_has_defaults() {
        let builder = PubSubBuilder::new();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_subscriptions, 1_024);
        assert_eq!(cfg.max_topic_patterns, 256);
        assert_eq!(cfg.delivery_timeout_ms, 10_000);
        assert_eq!(cfg.backlog_limit, 10_000);
    }

    #[test]
    fn builder_from_preset() {
        let builder = PubSubBuilder::from_preset(Preset::Local);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_subscriptions, 128);
        assert_eq!(cfg.max_topic_patterns, 64);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = PubSubBuilder::new()
            .max_subscriptions(500)
            .max_topic_patterns(128)
            .delivery_timeout_ms(15_000)
            .backlog_limit(5_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_subscriptions, 500);
        assert_eq!(cfg.max_topic_patterns, 128);
        assert_eq!(cfg.delivery_timeout_ms, 15_000);
        assert_eq!(cfg.backlog_limit, 5_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = PubSubBuilder::new()
            .max_subscriptions(0) // invalid
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = PubSubBuilder::new()
            .max_subscriptions(MIN_MAX_SUBSCRIPTIONS)
            .max_topic_patterns(MIN_MAX_TOPIC_PATTERNS)
            .delivery_timeout_ms(MIN_DELIVERY_TIMEOUT_MS)
            .backlog_limit(MIN_BACKLOG_LIMIT)
            .build()
            .unwrap();
        assert_eq!(cfg.max_subscriptions, MIN_MAX_SUBSCRIPTIONS);

        let cfg = PubSubBuilder::new()
            .max_subscriptions(MAX_MAX_SUBSCRIPTIONS)
            .max_topic_patterns(MAX_MAX_TOPIC_PATTERNS)
            .delivery_timeout_ms(MAX_DELIVERY_TIMEOUT_MS)
            .backlog_limit(MAX_BACKLOG_LIMIT)
            .build()
            .unwrap();
        assert_eq!(cfg.max_subscriptions, MAX_MAX_SUBSCRIPTIONS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = PubSubBuilder::from_preset(Preset::HighThroughput)
            .max_subscriptions(500) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_subscriptions, 500);
        assert_eq!(cfg.max_topic_patterns, 1_024); // from preset
    }

    #[test]
    fn builder_default_is_default_trait() {
        let builder = PubSubBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg, PubSubConfig::default());
    }
}
