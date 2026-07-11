//! [`EventBus`] — partitioned event bus facade.
//!
//! Phase 1: in-memory topic registry with per-partition message logs
//! and consumer-group offset tracking.

use std::collections::HashMap;
use std::fmt;

use crate::config::EventBusConfig;
use crate::consumer::{ConsumerGroupId, SubscriptionMode};
use crate::error::EventError;
use crate::health::HealthReport;
use crate::partition::{Offset, PartitionId};
use crate::stats::EventBusStats;
use crate::topic::{TopicConfig, TopicId};

/// Per-topic in-memory state.
struct TopicState {
    #[allow(dead_code)]
    config: TopicConfig,
    /// Partition id → ordered list of message hashes.
    partitions: HashMap<PartitionId, Vec<u64>>,
}

/// Partitioned event bus facade.
///
/// Owns the in-memory topic/partition registry and consumer-group offsets.
pub struct EventBus {
    /// Event bus configuration.
    #[allow(dead_code)]
    cfg: EventBusConfig,
    /// Event bus statistics.
    stats: EventBusStats,
    /// Topic registry.
    topics: HashMap<TopicId, TopicState>,
    /// Monotonic topic-id allocator (starts at 1; 0 is reserved nil).
    next_topic_id: u64,
    /// Consumer offsets keyed by (group, partition).
    consumer_offsets: HashMap<(ConsumerGroupId, PartitionId), Offset>,
}

impl fmt::Debug for EventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventBus")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .field("topics", &self.topics.len())
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
            topics: HashMap::new(),
            next_topic_id: 1,
            consumer_offsets: HashMap::new(),
        }
    }

    /// Create a new topic.
    ///
    /// # Errors
    /// Returns [`EventError::TopicExists`] only on id-allocator collision
    /// (practically never).
    pub fn create_topic(&mut self, config: TopicConfig) -> Result<TopicId, EventError> {
        config.validate()?;
        let id = TopicId::new(self.next_topic_id);
        self.next_topic_id = self.next_topic_id.wrapping_add(1);
        if self.topics.contains_key(&id) {
            return Err(EventError::TopicExists(id));
        }
        // Pre-create partitions up to partition_count for fast publish.
        let mut partitions = HashMap::new();
        for p in 0..config.partition_count {
            partitions.insert(PartitionId::new(p as u32), Vec::new());
        }
        self.topics.insert(id, TopicState { config, partitions });
        Ok(id)
    }

    /// Publish a message hash to a topic partition.
    ///
    /// # Errors
    /// Returns [`EventError::TopicNotFound`] if the topic does not exist.
    pub fn publish(
        &mut self,
        topic: TopicId,
        partition: PartitionId,
        msg_hash: u64,
    ) -> Result<(), EventError> {
        let state = self
            .topics
            .get_mut(&topic)
            .ok_or(EventError::TopicNotFound(topic))?;
        state
            .partitions
            .entry(partition)
            .or_default()
            .push(msg_hash);
        self.stats.record_publish(8); // 8 bytes ≈ size of u64 hash
        Ok(())
    }

    /// Subscribe a consumer group to a topic.
    ///
    /// Records the consumer group's starting offset. The `mode` controls
    /// the initial offset: [`SubscriptionMode::Earliest`] → 0,
    /// [`SubscriptionMode::Latest`] → current partition tail (0 for
    /// in-memory since we don't track per-partition consumer position
    /// at subscribe time in Phase 1).
    ///
    /// # Errors
    /// Returns [`EventError::TopicNotFound`] if the topic does not exist.
    pub fn subscribe(
        &mut self,
        group: ConsumerGroupId,
        topic: TopicId,
        mode: SubscriptionMode,
    ) -> Result<(), EventError> {
        if !self.topics.contains_key(&topic) {
            return Err(EventError::TopicNotFound(topic));
        }
        // Phase 1: record subscription for all existing partitions at the
        // mode's initial offset.
        let initial = match mode {
            SubscriptionMode::Earliest | SubscriptionMode::FromOffset(0) => Offset::ZERO,
            SubscriptionMode::Latest => Offset::ZERO,
            SubscriptionMode::FromOffset(n) => Offset::new(n),
        };
        let state = self
            .topics
            .get(&topic)
            .ok_or(EventError::TopicNotFound(topic))?;
        for &pid in state.partitions.keys() {
            self.consumer_offsets.insert((group, pid), initial);
        }
        Ok(())
    }

    /// Acknowledge consumption of a message, advancing the consumer
    /// group's offset for `partition`.
    ///
    /// # Errors
    /// Returns [`EventError::ConsumerGroupNotFound`] if the group has no
    /// recorded offset for the partition.
    pub fn ack(
        &mut self,
        group: ConsumerGroupId,
        partition: PartitionId,
        offset: Offset,
    ) -> Result<(), EventError> {
        match self.consumer_offsets.get_mut(&(group, partition)) {
            Some(current) => {
                if offset.raw() > current.raw() {
                    *current = offset;
                }
                self.stats.record_ack();
                Ok(())
            }
            None => Err(EventError::ConsumerGroupNotFound(group)),
        }
    }

    /// Returns the number of registered topics.
    #[must_use]
    pub fn topic_count(&self) -> usize {
        self.topics.len()
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
        let bus = EventBus::new(EventBusConfig::new());
        assert_eq!(bus.stats.snapshot().messages_published, 0);
        assert_eq!(bus.topic_count(), 0);
    }

    #[test]
    fn create_topic_succeeds() {
        let mut bus = EventBus::new(EventBusConfig::new());
        let id = bus.create_topic(TopicConfig::new(8)).unwrap();
        assert!(!id.is_nil());
        assert_eq!(bus.topic_count(), 1);
    }

    #[test]
    fn publish_appends_to_partition() {
        let mut bus = EventBus::new(EventBusConfig::new());
        let tid = bus.create_topic(TopicConfig::new(4)).unwrap();
        bus.publish(tid, PartitionId::new(0), 123).unwrap();
        bus.publish(tid, PartitionId::new(0), 456).unwrap();
        bus.publish(tid, PartitionId::new(1), 789).unwrap();
        assert_eq!(bus.stats.snapshot().messages_published, 3);
    }

    #[test]
    fn publish_unknown_topic_errors() {
        let mut bus = EventBus::new(EventBusConfig::new());
        assert!(matches!(
            bus.publish(TopicId::new(99), PartitionId::new(0), 1),
            Err(EventError::TopicNotFound(_))
        ));
    }

    #[test]
    fn subscribe_records_offsets() {
        let mut bus = EventBus::new(EventBusConfig::new());
        let tid = bus.create_topic(TopicConfig::new(2)).unwrap();
        bus.subscribe(ConsumerGroupId::new(1), tid, SubscriptionMode::Earliest)
            .unwrap();
        // Ack should now succeed for the recorded partitions.
        bus.ack(ConsumerGroupId::new(1), PartitionId::new(0), Offset::new(5))
            .unwrap();
    }

    #[test]
    fn subscribe_unknown_topic_errors() {
        let mut bus = EventBus::new(EventBusConfig::new());
        assert!(matches!(
            bus.subscribe(
                ConsumerGroupId::new(1),
                TopicId::new(99),
                SubscriptionMode::Latest,
            ),
            Err(EventError::TopicNotFound(_))
        ));
    }

    #[test]
    fn ack_unknown_group_errors() {
        let mut bus = EventBus::new(EventBusConfig::new());
        assert!(matches!(
            bus.ack(
                ConsumerGroupId::new(42),
                PartitionId::new(0),
                Offset::new(1),
            ),
            Err(EventError::ConsumerGroupNotFound(_))
        ));
    }

    #[test]
    fn health_report_reflects_publishes() {
        let mut bus = EventBus::new(EventBusConfig::new());
        let tid = bus.create_topic(TopicConfig::new(1)).unwrap();
        bus.publish(tid, PartitionId::new(0), 1).unwrap();
        let health = bus.health();
        assert_eq!(health.messages_published, 1);
    }
}
