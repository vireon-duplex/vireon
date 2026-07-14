//! [`OffsetStore`] — consumer offset tracking with in-memory persistence.
//!
//! Stores `(ConsumerGroupId, PartitionId) → Offset` pairs in memory.
//! Supports commit (write) and fetch (read) operations with validation.
//!
//! ```text
//! commit(group=1, partition=0, offset=42)
//!   → stores (1, 0) → 42
//!
//! fetch(group=1, partition=0)
//!   → returns 42
//!
//! fetch(group=2, partition=0)
//!   → returns Offset::ZERO (never committed)
//! ```

use crate::config::EventBusConfig;
use crate::consumer::ConsumerGroupId;
use crate::error::EventError;
use crate::health::HealthReport;
use crate::partition::{Offset, PartitionId};
use crate::stats::EventBusStats;
use rustc_hash::FxHashMap;
use std::fmt;

/// Consumer offset tracking store.
///
/// Maintains an in-memory map of committed offsets per consumer group
/// and partition. Monotonic offset advancement is enforced: committing
/// an offset lower than the current one is silently ignored.
pub struct OffsetStore {
    #[allow(dead_code)]
    cfg: EventBusConfig,
    stats: EventBusStats,
    offsets: FxHashMap<(ConsumerGroupId, PartitionId), Offset>,
}

impl fmt::Debug for OffsetStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OffsetStore")
            .field("cfg", &self.cfg)
            .field("tracked_offsets", &self.offsets.len())
            .finish()
    }
}

impl OffsetStore {
    /// Creates a new offset store with the given config.
    #[must_use]
    pub fn new(cfg: EventBusConfig) -> Self {
        Self {
            cfg,
            stats: EventBusStats::new(),
            offsets: FxHashMap::default(),
        }
    }

    /// Commits (advances) the offset for a consumer group + partition.
    ///
    /// Offsets are monotonic: if the new offset is lower than the
    /// currently committed one, the call is silently ignored (no error).
    /// This matches at-least-once semantics where retries may re-deliver.
    ///
    /// # Errors
    /// Currently never errors; reserved for future persistence failures.
    pub fn commit(
        &mut self,
        group: ConsumerGroupId,
        partition: PartitionId,
        offset: Offset,
    ) -> Result<(), EventError> {
        let entry = self.offsets.entry((group, partition)).or_insert(Offset::ZERO);
        if offset.raw() > entry.raw() {
            *entry = offset;
        }
        self.stats.record_ack();
        Ok(())
    }

    /// Fetches the committed offset for a consumer group + partition.
    ///
    /// Returns [`Offset::ZERO`] if no offset has been committed for
    /// the given `(group, partition)` pair.
    ///
    /// # Errors
    /// Currently never errors; reserved for future persistence failures.
    pub fn fetch(
        &self,
        group: ConsumerGroupId,
        partition: PartitionId,
    ) -> Result<Offset, EventError> {
        Ok(self
            .offsets
            .get(&(group, partition))
            .copied()
            .unwrap_or(Offset::ZERO))
    }

    /// Returns the number of tracked `(group, partition)` pairs.
    #[must_use]
    pub fn tracked_count(&self) -> usize {
        self.offsets.len()
    }

    /// Resets all offsets (for testing or admin reset).
    pub fn reset(&mut self) {
        self.offsets.clear();
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_creation() {
        let store = OffsetStore::new(EventBusConfig::new());
        assert_eq!(store.tracked_count(), 0);
    }

    #[test]
    fn commit_then_fetch() {
        let mut store = OffsetStore::new(EventBusConfig::new());
        store
            .commit(ConsumerGroupId::new(1), PartitionId::new(0), Offset::new(42))
            .unwrap();

        let offset = store
            .fetch(ConsumerGroupId::new(1), PartitionId::new(0))
            .unwrap();
        assert_eq!(offset, Offset::new(42));
    }

    #[test]
    fn fetch_uncommitted_returns_zero() {
        let store = OffsetStore::new(EventBusConfig::new());
        let offset = store
            .fetch(ConsumerGroupId::new(99), PartitionId::new(99))
            .unwrap();
        assert_eq!(offset, Offset::ZERO);
    }

    #[test]
    fn commit_is_monotonic() {
        let mut store = OffsetStore::new(EventBusConfig::new());
        let g = ConsumerGroupId::new(1);
        let p = PartitionId::new(0);

        store.commit(g, p, Offset::new(10)).unwrap();
        store.commit(g, p, Offset::new(5)).unwrap(); // lower — ignored
        store.commit(g, p, Offset::new(20)).unwrap();

        assert_eq!(store.fetch(g, p).unwrap(), Offset::new(20));
    }

    #[test]
    fn multiple_groups_and_partitions() {
        let mut store = OffsetStore::new(EventBusConfig::new());

        store.commit(ConsumerGroupId::new(1), PartitionId::new(0), Offset::new(10)).unwrap();
        store.commit(ConsumerGroupId::new(1), PartitionId::new(1), Offset::new(20)).unwrap();
        store.commit(ConsumerGroupId::new(2), PartitionId::new(0), Offset::new(30)).unwrap();

        assert_eq!(store.fetch(ConsumerGroupId::new(1), PartitionId::new(0)).unwrap(), Offset::new(10));
        assert_eq!(store.fetch(ConsumerGroupId::new(1), PartitionId::new(1)).unwrap(), Offset::new(20));
        assert_eq!(store.fetch(ConsumerGroupId::new(2), PartitionId::new(0)).unwrap(), Offset::new(30));
        assert_eq!(store.tracked_count(), 3);
    }

    #[test]
    fn reset_clears_all() {
        let mut store = OffsetStore::new(EventBusConfig::new());
        store
            .commit(ConsumerGroupId::new(1), PartitionId::new(0), Offset::new(10))
            .unwrap();
        assert_eq!(store.tracked_count(), 1);

        store.reset();
        assert_eq!(store.tracked_count(), 0);
    }

    #[test]
    fn health_report_initially_healthy() {
        let store = OffsetStore::new(EventBusConfig::new());
        let health = store.health();
        assert!(health.is_healthy());
    }
}
