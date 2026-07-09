//! [`OffsetStore`] — consumer offset tracking facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 10 (offset store).

use crate::config::EventBusConfig;
use crate::consumer::ConsumerGroupId;
use crate::error::EventError;
use crate::health::HealthReport;
use crate::partition::{Offset, PartitionId};
use crate::stats::EventBusStats;
use std::fmt;

/// Consumer offset tracking store.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Offset commit API
/// - Offset fetch API
/// - Offset validation
/// - Offset compaction
pub struct OffsetStore {
    /// Offset store configuration.
    #[allow(dead_code)]
    cfg: EventBusConfig,
    /// Offset store statistics.
    stats: EventBusStats,
}

impl fmt::Debug for OffsetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OffsetStore")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl OffsetStore {
    /// Create a new offset store with the given config.
    #[must_use]
    pub fn new(cfg: EventBusConfig) -> Self {
        Self {
            cfg,
            stats: EventBusStats::new(),
        }
    }

    /// Commit an offset for a consumer group partition.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the store is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn commit(
        &self,
        _group: ConsumerGroupId,
        _partition: PartitionId,
        _offset: Offset,
    ) -> Result<(), EventError> {
        Err(EventError::ShuttingDown)
    }

    /// Fetch the committed offset for a consumer group partition.
    ///
    /// # Errors
    /// Returns [`EventError::ShuttingDown`] if the store is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn fetch(
        &self,
        _group: ConsumerGroupId,
        _partition: PartitionId,
    ) -> Result<Offset, EventError> {
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
    fn offset_store_creation() {
        let cfg = EventBusConfig::new();
        let store = OffsetStore::new(cfg);
        assert_eq!(store.stats.snapshot().ack_total, 0);
    }

    #[test]
    fn commit_returns_shutting_down() {
        let store = OffsetStore::new(EventBusConfig::new());
        let result = store.commit(ConsumerGroupId::new(1), PartitionId::new(0), Offset::new(0));
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn fetch_returns_shutting_down() {
        let store = OffsetStore::new(EventBusConfig::new());
        let result = store.fetch(ConsumerGroupId::new(1), PartitionId::new(0));
        assert!(matches!(result, Err(EventError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let store = OffsetStore::new(EventBusConfig::new());
        let health = store.health();
        assert!(health.is_healthy());
    }
}
