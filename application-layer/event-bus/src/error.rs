//! [`EventError`] — the typed failure surface of the event bus.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`EventError::is_fatal`] / [`EventError::is_retryable`] /
//! [`EventError::is_transient`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `TopicExists`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `PublishFailed`, `ConsumerLag`, `PartitionRebalance` | Back off + retry |
//! | Capacity | `OffsetOutOfRange` | Wait or reset offset |

use crate::consumer::ConsumerGroupId;
use crate::partition::PartitionId;
use crate::topic::TopicId;
use thiserror::Error;

// ── EventError ───────────────────────────────────────────────────

/// Typed failure surface for the partitioned event bus.
///
/// Every fallible event bus API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EventError {
    /// The requested topic does not exist.
    #[error("topic not found: {0}")]
    TopicNotFound(TopicId),

    /// A topic with the given ID already exists.
    #[error("topic already exists: {0}")]
    TopicExists(TopicId),

    /// Publishing an event failed.
    #[error("publish failed: {0}")]
    PublishFailed(&'static str),

    /// Consumer is lagging behind producer.
    #[error("consumer lag: {0} messages behind")]
    ConsumerLag(u64),

    /// Consumer offset is out of valid range.
    #[error("offset out of range: {0}")]
    OffsetOutOfRange(u64),

    /// Partition is being reassigned to another consumer.
    #[error("partition rebalance in progress: {0}")]
    PartitionRebalance(PartitionId),

    /// Consumer group does not exist.
    #[error("consumer group not found: {0}")]
    ConsumerGroupNotFound(ConsumerGroupId),

    /// Event bus is shutting down.
    #[error("event bus is shutting down")]
    ShuttingDown,
}

impl EventError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`TopicExists`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::TopicExists(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (publish failed,
    /// consumer lag, partition rebalance).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::PublishFailed(_) | Self::ConsumerLag(_) | Self::PartitionRebalance(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(EventError::TopicExists(TopicId::new(1)).is_fatal());
        assert!(EventError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!EventError::TopicNotFound(TopicId::new(1)).is_fatal());
        assert!(!EventError::PublishFailed("temp").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(EventError::PublishFailed("temp overload").is_retryable());
        assert!(EventError::ConsumerLag(1000).is_retryable());
        assert!(EventError::PartitionRebalance(PartitionId::new(1)).is_retryable());
        // Non-retryable examples.
        assert!(!EventError::ShuttingDown.is_retryable());
        assert!(!EventError::TopicExists(TopicId::new(1)).is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = EventError::TopicNotFound(TopicId::new(42));
        let s = format!("{e}");
        assert!(s.contains("topic not found"));
        assert!(s.contains("42"));

        let e = EventError::PublishFailed("buffer full");
        assert!(format!("{e}").contains("publish failed"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = EventError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = EventError::PublishFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
    }

    #[test]
    fn topic_not_found_with_valid_id() {
        let e = EventError::TopicNotFound(TopicId::new(123));
        assert!(format!("{e}").contains("not found"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn topic_exists_is_fatal() {
        let e = EventError::TopicExists(TopicId::new(1));
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn publish_failed_is_retryable() {
        let e = EventError::PublishFailed("network error");
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn consumer_lag_is_retryable() {
        let e = EventError::ConsumerLag(5000);
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn partition_rebalance_is_retryable() {
        let e = EventError::PartitionRebalance(PartitionId::new(3));
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn consumer_group_not_found() {
        let e = EventError::ConsumerGroupNotFound(ConsumerGroupId::new(999));
        assert!(format!("{e}").contains("consumer group not found"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }
}
