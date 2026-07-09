//! [`TopicTrie`] — trie-based topic pattern matching (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 10 (topic trie).

use crate::config::PubSubConfig;
use crate::error::PubSubError;
use crate::stats::PubSubStats;
use crate::subscription::SubscriptionId;
use std::fmt;

/// Trie-based topic pattern matcher.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - O(k) pattern insertion and lookup (k = segment count)
/// - Wildcard `*` segment support
/// - Subscription ID storage at leaf nodes
/// - Concurrent read access
pub struct TopicTrie {
    /// Engine configuration.
    #[allow(dead_code)]
    cfg: PubSubConfig,
    /// Trie statistics.
    #[allow(dead_code)]
    stats: PubSubStats,
}

impl fmt::Debug for TopicTrie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicTrie")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl TopicTrie {
    /// Create a new topic trie with the given config.
    #[must_use]
    pub fn new(cfg: PubSubConfig) -> Self {
        Self {
            cfg,
            stats: PubSubStats::new(),
        }
    }

    /// Insert a topic pattern into the trie, associated with a subscription ID.
    ///
    /// # Errors
    /// Returns [`PubSubError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation. // TODO(phase-1)
    pub fn insert(&mut self, _pattern: &str, _sub_id: SubscriptionId) -> Result<(), PubSubError> {
        // TODO(phase-1): insert pattern segments into trie nodes
        Err(PubSubError::ShuttingDown)
    }

    /// Look up all subscription IDs matching a given topic.
    ///
    /// # Errors
    /// Returns [`PubSubError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation. // TODO(phase-1)
    pub fn lookup(&self, _topic: &str) -> Result<Vec<SubscriptionId>, PubSubError> {
        // TODO(phase-1): traverse trie with topic segments, collect matches
        Err(PubSubError::ShuttingDown)
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> crate::health::HealthReport {
        crate::health::HealthReport::derive(&self.stats.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie_creation() {
        let trie = TopicTrie::new(PubSubConfig::new());
        // Just ensure it constructs.
        let _ = trie.health();
    }

    #[test]
    fn insert_returns_shutting_down() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        let result = trie.insert("sensor.*", SubscriptionId::new(1));
        assert!(matches!(result, Err(PubSubError::ShuttingDown)));
    }

    #[test]
    fn lookup_returns_shutting_down() {
        let trie = TopicTrie::new(PubSubConfig::new());
        let result = trie.lookup("sensor.temp");
        assert!(matches!(result, Err(PubSubError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let trie = TopicTrie::new(PubSubConfig::new());
        let health = trie.health();
        assert!(health.is_healthy());
    }
}
