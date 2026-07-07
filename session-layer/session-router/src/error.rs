//! [`RouterError`] — the typed failure surface of the session router.
//!
//! See `DESIGN.md` Part 10 (failure handling) and Part 11
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`RouterError::is_fatal`] / [`RouterError::is_retryable`] /
//! [`RouterError::is_transient`] predicates:
//!
//! | Category    | Example                  | Caller action          |
//! |-------------|--------------------------|------------------------|
//! | Fatal       | `InvalidConfig`          | Abort; don't retry     |
//! | Retryable   | `NodeUnavailable`        | Back off + retry       |
//! | Transient   | `Timeout`                | Retry immediately      |

use crate::decision::MulticastGroup;
use session_manager::SessionId;
use session_registry::NodeId;
use thiserror::Error;

// ── RouterError ─────────────────────────────────────────────────

/// Typed failure surface for the session router.
///
/// Every fallible router API returns one of these variants so that
/// callers can branch on a small, exhaustive set of failure modes
/// instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Clone, Debug, Error)]
pub enum RouterError {
    /// A configuration field is out of range or contradictory.
    #[error("invalid router configuration: {0}")]
    InvalidConfig(String),

    /// No route exists for the given session.
    #[error("no route to session: {0}")]
    NoRoute(SessionId),

    /// A route lookup operation failed (e.g. ring walk, cache scan).
    #[error("route lookup failed: {0}")]
    LookupFailed(&'static str),

    /// The target node was found but is unavailable (down, suspect).
    #[error("target node unavailable: {0:?}")]
    NodeUnavailable(NodeId),

    /// Cache miss and the registry was unreachable to refill.
    #[error("cache miss and registry unreachable: {0}")]
    RegistryUnreachable(&'static str),

    /// The routing operation did not complete within the timeout.
    #[error("routing timeout")]
    Timeout,

    /// Anycast selection found no healthy candidate nodes.
    #[error("anycast: no healthy nodes")]
    NoHealthyNodes,

    /// The multicast group was not found.
    #[error("multicast group not found: {0}")]
    GroupNotFound(MulticastGroup),

    /// The router is shutting down; reject new operations.
    #[error("router is shutting down")]
    ShuttingDown,
}

impl RouterError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes
    /// (`InvalidConfig`) or terminal lifecycle states
    /// (`ShuttingDown`) that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidConfig(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically caused by temporary node
    /// unavailability or registry connectivity issues.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::NodeUnavailable(_)
                | Self::RegistryUnreachable(_)
                | Self::NoHealthyNodes
                | Self::LookupFailed(_)
        )
    }

    /// Whether the error is transient — the caller may retry
    /// immediately (no backoff needed).
    ///
    /// Transient errors include timeouts and anycast failures
    /// that may self-correct within a millisecond.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Timeout | Self::NoHealthyNodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(RouterError::InvalidConfig("bad".into()).is_fatal());
        assert!(RouterError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!RouterError::Timeout.is_fatal());
        assert!(!RouterError::NoRoute(SessionId::nil()).is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(RouterError::NodeUnavailable(NodeId::new(1)).is_retryable());
        assert!(RouterError::RegistryUnreachable("ring").is_retryable());
        assert!(RouterError::NoHealthyNodes.is_retryable());
        assert!(RouterError::LookupFailed("scan").is_retryable());
        // Non-retryable examples.
        assert!(!RouterError::InvalidConfig("bad".into()).is_retryable());
        assert!(!RouterError::ShuttingDown.is_retryable());
    }

    #[test]
    fn transient_classification() {
        assert!(RouterError::Timeout.is_transient());
        assert!(RouterError::NoHealthyNodes.is_transient());
        // Non-transient examples.
        assert!(!RouterError::NodeUnavailable(NodeId::new(1)).is_transient());
        assert!(!RouterError::ShuttingDown.is_transient());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = RouterError::NodeUnavailable(NodeId::new(42));
        let s = format!("{e}");
        assert!(s.contains("target node unavailable"));

        let e = RouterError::Timeout;
        assert!(format!("{e}").contains("timeout"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = RouterError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_transient());

        let retry = RouterError::NodeUnavailable(NodeId::new(1));
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_transient());

        let transient = RouterError::Timeout;
        assert!(!transient.is_fatal());
        assert!(transient.is_transient());
    }
}
