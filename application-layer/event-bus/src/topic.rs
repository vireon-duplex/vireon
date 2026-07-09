//! [`TopicId`], [`TopicConfig`] — topic identifiers and configuration.
//!
//! See `DESIGN.md` Part 4 (topics).

use crate::error::EventError;

// ── TopicId ─────────────────────────────────────────────────────

/// Unique identifier for a topic.
///
/// Topics are named entities in the event bus. Each topic has a
/// unique ID and is partitioned for parallel consumption.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TopicId(u64);

impl TopicId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new topic ID.
    ///
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil topic ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil topic ID.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for TopicId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for TopicId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "topic#{}", self.0)
    }
}

// ── TopicConfig ──────────────────────────────────────────────────

/// Configuration for a single topic.
///
/// Topics are created with a specific partition count, retention
/// policy, and message size limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicConfig {
    /// Number of partitions for this topic.
    pub partition_count: u8,
    /// Retention time in milliseconds.
    pub retention_ms: u64,
    /// Maximum message size in bytes.
    pub max_message_bytes: u32,
}

impl TopicConfig {
    /// Create a new topic config with the given partition count.
    ///
    /// Defaults: 7-day retention, 1 MB message size.
    #[must_use]
    pub const fn new(partition_count: u8) -> Self {
        Self {
            partition_count,
            retention_ms: 604_800_000, // 7 days
            max_message_bytes: 1_000_000,
        }
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

    /// Validate the config.
    ///
    /// # Errors
    /// Returns [`EventError::PublishFailed`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), EventError> {
        if self.partition_count == 0 {
            return Err(EventError::PublishFailed("partition_count cannot be 0"));
        }
        if self.retention_ms == 0 {
            return Err(EventError::PublishFailed("retention_ms cannot be 0"));
        }
        if self.max_message_bytes == 0 {
            return Err(EventError::PublishFailed("max_message_bytes cannot be 0"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_id_new() {
        let id = TopicId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn topic_id_nil() {
        let id = TopicId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn topic_id_nil_constant() {
        assert_eq!(TopicId::NIL, TopicId::nil());
        assert!(TopicId::NIL.is_nil());
    }

    #[test]
    fn topic_id_default_is_nil() {
        let id = TopicId::default();
        assert_eq!(id, TopicId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn topic_id_equality() {
        let id1 = TopicId::new(42);
        let id2 = TopicId::new(42);
        let id3 = TopicId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn topic_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TopicId::new(1));
        set.insert(TopicId::new(2));
        set.insert(TopicId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn topic_config_new() {
        let cfg = TopicConfig::new(8);
        assert_eq!(cfg.partition_count, 8);
        assert_eq!(cfg.retention_ms, 604_800_000);
        assert_eq!(cfg.max_message_bytes, 1_000_000);
    }

    #[test]
    fn topic_config_builder() {
        let cfg = TopicConfig::new(16)
            .with_retention_ms(86_400_000)
            .with_max_message_bytes(4_000_000);
        assert_eq!(cfg.partition_count, 16);
        assert_eq!(cfg.retention_ms, 86_400_000);
        assert_eq!(cfg.max_message_bytes, 4_000_000);
    }

    #[test]
    fn topic_config_validate_rejects_zero_partitions() {
        let cfg = TopicConfig::new(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn topic_config_validate_rejects_zero_retention() {
        let cfg = TopicConfig::new(8).with_retention_ms(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn topic_config_validate_rejects_zero_message_size() {
        let cfg = TopicConfig::new(8).with_max_message_bytes(0);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn topic_config_validate_accepts_valid() {
        let cfg = TopicConfig::new(8)
            .with_retention_ms(1000)
            .with_max_message_bytes(1024);
        assert!(cfg.validate().is_ok());
    }
}
