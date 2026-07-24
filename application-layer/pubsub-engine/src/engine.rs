//! [`PubSubEngine`] — pub/sub engine facade.
//!
//! Phase 1: in-memory subscription registry with pattern-based routing.

use std::collections::HashMap;
use std::fmt;

use crate::config::PubSubConfig;
use crate::error::PubSubError;
use crate::health::HealthReport;
use crate::message::PubSubMessage;
use crate::stats::PubSubStats;
use crate::subscription::{SubscriptionId, TopicPattern};

/// Pub/sub engine facade.
///
/// Owns the in-memory subscription registry and atomic statistics.
pub struct PubSubEngine {
    /// Engine configuration.
    #[allow(dead_code)]
    cfg: PubSubConfig,
    /// Engine statistics.
    stats: PubSubStats,
    /// Active subscriptions keyed by id.
    subscriptions: HashMap<SubscriptionId, TopicPattern>,
    /// Monotonic subscription-id allocator (starts at 1; 0 is reserved nil).
    next_sub_id: u64,
}

impl fmt::Debug for PubSubEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubEngine")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .field("subscriptions", &self.subscriptions.len())
            .finish()
    }
}

impl PubSubEngine {
    /// Create a new pub/sub engine with the given config.
    #[must_use]
    pub fn new(cfg: PubSubConfig) -> Self {
        Self {
            cfg,
            stats: PubSubStats::new(),
            subscriptions: HashMap::new(),
            next_sub_id: 1,
        }
    }

    /// Publish a message to all matching subscriptions.
    ///
    /// Counts how many subscription patterns match the message topic and
    /// records one delivery per match.
    pub fn publish(&mut self, msg: PubSubMessage) -> Result<(), PubSubError> {
        self.stats.record_publish();
        let mut deliveries = 0u64;
        for pattern in self.subscriptions.values() {
            if pattern.matches(msg.topic()) {
                deliveries += 1;
                self.stats.record_deliver();
            }
        }
        let _ = deliveries; // observability via stats
        Ok(())
    }

    /// Create a new subscription for a topic pattern.
    pub fn subscribe(&mut self, pattern: TopicPattern) -> Result<SubscriptionId, PubSubError> {
        let id = SubscriptionId::new(self.next_sub_id);
        self.next_sub_id = self.next_sub_id.wrapping_add(1);
        self.subscriptions.insert(id, pattern);
        self.stats.record_subscription_created();
        self.stats
            .set_active_subscriptions(self.subscriptions.len() as u64);
        Ok(id)
    }

    /// Remove an existing subscription.
    ///
    /// # Errors
    /// Returns [`PubSubError::SubscriptionNotFound`] if `id` is not active.
    pub fn unsubscribe(&mut self, id: SubscriptionId) -> Result<(), PubSubError> {
        if self.subscriptions.remove(&id).is_none() {
            return Err(PubSubError::SubscriptionNotFound(id));
        }
        self.stats.record_subscription_removed();
        self.stats
            .set_active_subscriptions(self.subscriptions.len() as u64);
        Ok(())
    }

    /// Returns the number of active subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Take a snapshot of the engine's atomic counters. Useful for
    /// metric collectors that need access to fields not present on
    /// [`HealthReport`] (e.g. `messages_delivered`).
    #[must_use]
    pub fn snapshot(&self) -> crate::stats::StatsSnapshot {
        self.stats.snapshot()
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::Topic;

    #[test]
    fn engine_creation() {
        let engine = PubSubEngine::new(PubSubConfig::new());
        assert_eq!(engine.stats.snapshot().messages_published, 0);
        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn subscribe_succeeds() {
        let mut engine = PubSubEngine::new(PubSubConfig::new());
        let pattern = TopicPattern::new("sensor.*").unwrap();
        let id = engine.subscribe(pattern).unwrap();
        assert!(!id.is_nil());
        assert_eq!(engine.subscription_count(), 1);
    }

    #[test]
    fn unsubscribe_succeeds() {
        let mut engine = PubSubEngine::new(PubSubConfig::new());
        let id = engine
            .subscribe(TopicPattern::new("topic").unwrap())
            .unwrap();
        engine.unsubscribe(id).unwrap();
        assert_eq!(engine.subscription_count(), 0);
    }

    #[test]
    fn unsubscribe_unknown_errors() {
        let mut engine = PubSubEngine::new(PubSubConfig::new());
        assert!(matches!(
            engine.unsubscribe(SubscriptionId::new(99)),
            Err(PubSubError::SubscriptionNotFound(_))
        ));
    }

    #[test]
    fn publish_matches_subscriptions() {
        let mut engine = PubSubEngine::new(PubSubConfig::new());
        engine
            .subscribe(TopicPattern::new("sensor.*").unwrap())
            .unwrap();
        engine
            .subscribe(TopicPattern::new("alert.*").unwrap())
            .unwrap();
        engine
            .publish(PubSubMessage::new(
                Topic::new("sensor.temp").unwrap(),
                vec![1, 2, 3],
            ))
            .unwrap();
        assert_eq!(engine.stats.snapshot().messages_published, 1);
    }

    #[test]
    fn health_report_reflects_publishes() {
        let mut engine = PubSubEngine::new(PubSubConfig::new());
        engine
            .publish(PubSubMessage::new(Topic::new("t").unwrap(), vec![]))
            .unwrap();
        let health = engine.health();
        assert_eq!(health.messages_published, 1);
    }
}
