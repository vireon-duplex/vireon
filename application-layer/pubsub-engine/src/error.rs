//! [`PubSubError`] — the typed failure surface of the pub/sub engine.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`PubSubError::is_fatal`] / [`PubSubError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `SubscriptionExists`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `PublishFailed` | Back off + retry |
//! | Not-found | `SubscriptionNotFound` | Check subscription ID |
//! | Mismatch | `TopicMismatch` | Verify topic/pattern alignment |

use crate::subscription::SubscriptionId;
use thiserror::Error;

// ── PubSubError ──────────────────────────────────────────────────

/// Typed failure surface for the pub/sub engine.
///
/// Every fallible pub/sub API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum PubSubError {
    /// The requested subscription does not exist.
    #[error("subscription not found: {0}")]
    SubscriptionNotFound(SubscriptionId),

    /// A subscription with the given ID already exists.
    #[error("subscription already exists: {0}")]
    SubscriptionExists(SubscriptionId),

    /// Publishing a message failed.
    #[error("publish failed: {0}")]
    PublishFailed(&'static str),

    /// The message topic does not match the subscription pattern.
    #[error("topic mismatch: expected pattern does not match topic")]
    TopicMismatch,

    /// Pub/sub engine is shutting down.
    #[error("pub/sub engine is shutting down")]
    ShuttingDown,
}

impl PubSubError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`SubscriptionExists`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::SubscriptionExists(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (publish failed).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::PublishFailed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(PubSubError::SubscriptionExists(SubscriptionId::new(1)).is_fatal());
        assert!(PubSubError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!PubSubError::SubscriptionNotFound(SubscriptionId::new(1)).is_fatal());
        assert!(!PubSubError::PublishFailed("temp").is_fatal());
        assert!(!PubSubError::TopicMismatch.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(PubSubError::PublishFailed("temp overload").is_retryable());
        // Non-retryable examples.
        assert!(!PubSubError::ShuttingDown.is_retryable());
        assert!(!PubSubError::SubscriptionExists(SubscriptionId::new(1)).is_retryable());
        assert!(!PubSubError::SubscriptionNotFound(SubscriptionId::new(1)).is_retryable());
        assert!(!PubSubError::TopicMismatch.is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = PubSubError::SubscriptionNotFound(SubscriptionId::new(42));
        let s = format!("{e}");
        assert!(s.contains("subscription not found"));
        assert!(s.contains("42"));

        let e = PubSubError::PublishFailed("buffer full");
        assert!(format!("{e}").contains("publish failed"));

        let e = PubSubError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));

        let e = PubSubError::TopicMismatch;
        assert!(format!("{e}").contains("topic mismatch"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = PubSubError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = PubSubError::PublishFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
    }

    #[test]
    fn subscription_not_found_with_valid_id() {
        let e = PubSubError::SubscriptionNotFound(SubscriptionId::new(123));
        assert!(format!("{e}").contains("not found"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn subscription_exists_is_fatal() {
        let e = PubSubError::SubscriptionExists(SubscriptionId::new(1));
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn publish_failed_is_retryable() {
        let e = PubSubError::PublishFailed("network error");
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn topic_mismatch_is_neither_fatal_nor_retryable() {
        let e = PubSubError::TopicMismatch;
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }
}
