//! [`EventBus`] — partitioned event bus facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 9 (event bus).

use crate::config::EventBusConfig;
use crate::consumer::{ConsumerGroupId, SubscriptionMode};
use crate::error::EventError;
use crate::health::HealthReport;
use crate::partition::{Offset, PartitionId};
use crate::stats::EventBusStats;
use crate::topic::{TopicConfig, TopicId};
use std::fmt;

/// Partitioned event bus facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Topic creation and management
/// - Message publishing
/// - Consumer group management
/// - Offset tracking
/// - Partition rebalancing
pub struct EventBus {
    /// Event bus configuration.
    #[allow(dead_code)]
    cfg: EventBusConfig,
    /// Event bus statistics.
    #[allow(dead_code)]
    stats: EventBusStats,
}

impl fmt::Debug for EventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventBus")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl EventBus {
    /// Create a new event bus with the given config.
    #[must_use]
    pub fn new(cfg: EventBusConfig) -> Self {
        Self {
            cfg,
            stats: EventBusStats::new(),
        }
    }

    /// Create a new topic.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the bus is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn create_topic(&mut self, _config: TopicConfig) -> Result<TopicId, EventError> {
        Err(EventError::ShuttingDown)
    }

    /// Publish a message to a topic partition.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the bus is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn publish(
        &self,
        _topic: TopicId,
        _partition: PartitionId,
        _msg_hash: u64,
    ) -> Result<(), EventError> {
        Err(EventError::ShuttingDown)
    }

    /// Subscribe a consumer group to a topic.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the bus is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn subscribe(
        &self,
        _group: ConsumerGroupId,
        _topic: TopicId,
        _mode: SubscriptionMode,
    ) -> Result<(), EventError> {
        Err(EventError::ShuttingDown)
    }

    /// Acknowledge consumption of a message.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the bus is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn ack(
        &self,
        _group: ConsumerGroupId,
        _partition: PartitionId,
        _offset: Offset,
    ) -> Result<(), EventError> {
        Err(EventError::ShuttingDown)
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bus_creation() {
        let cfg = EventBusConfig::new();
        let bus = EventBus::new(cfg);
        assert_eq!(bus.stats.snapshot().messages_published, 0);
    }

    #[test]
    fn create_topic_returns_shutting_down() {
        let mut bus = EventBus::new(EventBusConfig::new());
        let result = bus.create_topic(TopicConfig::new(8));
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn publish_returns_shutting_down() {
        let bus = EventBus::new(EventBusConfig::new());
        let result = bus.publish(TopicId::new(1), PartitionId::new(0), 123);
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn subscribe_returns_shutting_down() {
        let bus = EventBus::new(EventBusConfig::new());
        let result = bus.subscribe(
            ConsumerGroupId::new(1),
            TopicId::new(1),
            SubscriptionMode::Latest,
        );
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn ack_returns_shutting_down() {
        let bus = EventBus::new(EventBusConfig::new());
        let result = bus.ack(ConsumerGroupId::new(1), PartitionId::new(0), Offset::new(0));
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let bus = EventBus::new(EventBusConfig::new());
        let health = bus.health();
        assert!(health.is_healthy());
        assert_eq!(health.messages_published, 0);
    }
}
