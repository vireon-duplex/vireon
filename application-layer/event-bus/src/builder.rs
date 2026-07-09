//! [`EventBusBuilder`] — fluent builder for [`EventBusConfig`]
//! and the event bus itself.
//!
//! See `DESIGN.md` Part 13 (configuration).

use crate::config::{EventBusConfig, Preset};

/// Fluent builder for [`EventBusConfig`].
#[derive(Debug, Clone)]
pub struct EventBusBuilder {
    cfg: EventBusConfig,
}

impl EventBusBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: EventBusConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max topics.
    #[must_use]
    pub fn max_topics(mut self, topics: u32) -> Self {
        self.cfg = self.cfg.with_max_topics(topics);
        self
    }

    /// Override the default partitions.
    #[must_use]
    pub fn default_partitions(mut self, partitions: u8) -> Self {
        self.cfg = self.cfg.with_default_partitions(partitions);
        self
    }

    /// Override the retention time (ms).
    #[must_use]
    pub fn retention_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_retention_ms(ms);
        self
    }

    /// Override the max message bytes.
    #[must_use]
    pub fn max_message_bytes(mut self, bytes: u32) -> Self {
        self.cfg = self.cfg.with_max_message_bytes(bytes);
        self
    }

    /// Override the consumer group timeout (ms).
    #[must_use]
    pub fn consumer_group_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_consumer_group_timeout_ms(ms);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`crate::EventError::PublishFailed`] if any field is out
    /// of range.
    pub fn build(self) -> Result<EventBusConfig, crate::EventError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for EventBusBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_GROUP_TIMEOUT_MS, MAX_MESSAGE_BYTES, MAX_PARTITIONS, MAX_RETENTION_MS, MAX_TOPICS,
        MIN_GROUP_TIMEOUT_MS, MIN_MESSAGE_BYTES, MIN_PARTITIONS, MIN_RETENTION_MS, MIN_TOPICS,
    };

    #[test]
    fn builder_new_has_defaults() {
        let builder = EventBusBuilder::new();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_topics, 100);
        assert_eq!(cfg.default_partitions, 8);
        assert_eq!(cfg.retention_ms, 604_800_000);
        assert_eq!(cfg.max_message_bytes, 1_000_000);
        assert_eq!(cfg.consumer_group_timeout_ms, 60_000);
    }

    #[test]
    fn builder_from_preset() {
        let builder = EventBusBuilder::from_preset(Preset::Local);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_topics, 10);
        assert_eq!(cfg.default_partitions, 8);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = EventBusBuilder::new()
            .max_topics(500)
            .default_partitions(16)
            .retention_ms(1_209_600_000)
            .max_message_bytes(8_000_000)
            .consumer_group_timeout_ms(90_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_topics, 500);
        assert_eq!(cfg.default_partitions, 16);
        assert_eq!(cfg.retention_ms, 1_209_600_000);
        assert_eq!(cfg.max_message_bytes, 8_000_000);
        assert_eq!(cfg.consumer_group_timeout_ms, 90_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = EventBusBuilder::new()
            .max_topics(0) // invalid
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = EventBusBuilder::new()
            .max_topics(MIN_TOPICS)
            .default_partitions(MIN_PARTITIONS)
            .retention_ms(MIN_RETENTION_MS)
            .max_message_bytes(MIN_MESSAGE_BYTES)
            .consumer_group_timeout_ms(MIN_GROUP_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_topics, MIN_TOPICS);

        let cfg = EventBusBuilder::new()
            .max_topics(MAX_TOPICS)
            .default_partitions(MAX_PARTITIONS)
            .retention_ms(MAX_RETENTION_MS)
            .max_message_bytes(MAX_MESSAGE_BYTES)
            .consumer_group_timeout_ms(MAX_GROUP_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_topics, MAX_TOPICS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = EventBusBuilder::from_preset(Preset::HighThroughput)
            .max_topics(500) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_topics, 500);
        assert_eq!(cfg.default_partitions, 128); // from preset
    }

    #[test]
    fn builder_default_is_default_trait() {
        let builder = EventBusBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg, EventBusConfig::default());
    }
}
