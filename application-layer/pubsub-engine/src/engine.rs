//! [`PubSubEngine`] — pub/sub engine facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 9 (engine).

use crate::config::PubSubConfig;
use crate::error::PubSubError;
use crate::health::HealthReport;
use crate::message::PubSubMessage;
use crate::stats::PubSubStats;
use crate::subscription::{SubscriptionId, TopicPattern};
use std::fmt;

/// Pub/sub engine facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Subscription management (create, remove, list)
/// - Message publishing and routing
/// - Topic pattern matching via `TopicTrie`
/// - Delivery with backpressure and backlog management
pub struct PubSubEngine {
    /// Engine configuration.
    #[allow(dead_code)]
    cfg: PubSubConfig,
    /// Engine statistics.
    stats: PubSubStats,
}

impl fmt::Debug for PubSubEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubEngine")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
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
        }
    }

    /// Publish a message to all matching subscriptions.
    ///
    /// # Errors
    /// Returns [`PubSubError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation. // TODO(phase-1)
    pub fn publish(&self, _msg: PubSubMessage) -> Result<(), PubSubError> {
        // TODO(phase-1): route message to matching subscriptions via TopicTrie
        Err(PubSubError::ShuttingDown)
    }

    /// Create a new subscription for a topic pattern.
    ///
    /// # Errors
    /// Returns [`PubSubError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation. // TODO(phase-1)
    pub fn subscribe(&self, _pattern: TopicPattern) -> Result<SubscriptionId, PubSubError> {
        // TODO(phase-1): create subscription, insert pattern into TopicTrie
        Err(PubSubError::ShuttingDown)
    }

    /// Remove an existing subscription.
    ///
    /// # Errors
    /// Returns [`PubSubError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation. // TODO(phase-1)
    pub fn unsubscribe(&self, _id: SubscriptionId) -> Result<(), PubSubError> {
        // TODO(phase-1): remove subscription, clean up from TopicTrie
        Err(PubSubError::ShuttingDown)
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
    fn engine_creation() {
        let engine = PubSubEngine::new(PubSubConfig::new());
        assert_eq!(engine.stats.snapshot().messages_published, 0);
    }

    #[test]
    fn publish_returns_shutting_down() {
        let engine = PubSubEngine::new(PubSubConfig::new());
        let msg = PubSubMessage::new("sensor.temp", vec![1, 2, 3]);
        let result = engine.publish(msg);
        assert!(matches!(result, Err(PubSubError::ShuttingDown)));
    }

    #[test]
    fn subscribe_returns_shutting_down() {
        let engine = PubSubEngine::new(PubSubConfig::new());
        let pattern = TopicPattern::new("sensor.*").unwrap();
        let result = engine.subscribe(pattern);
        assert!(matches!(result, Err(PubSubError::ShuttingDown)));
    }

    #[test]
    fn unsubscribe_returns_shutting_down() {
        let engine = PubSubEngine::new(PubSubConfig::new());
        let result = engine.unsubscribe(SubscriptionId::new(1));
        assert!(matches!(result, Err(PubSubError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let engine = PubSubEngine::new(PubSubConfig::new());
        let health = engine.health();
        assert!(health.is_healthy());
        assert_eq!(health.messages_published, 0);
    }
}
