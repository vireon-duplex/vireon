//! [`RegistryError`] — the typed failure surface of the session
//! registry.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`RegistryError::is_fatal`] / [`RegistryError::is_retryable`] /
//! [`RegistryError::is_capacity`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidConfig`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `NodeUnavailable`, `LeaseRejected` | Back off + retry |
//! | Capacity | `CapacityExceeded`, `RingEmpty` | Shed load / wait |

use crate::node::NodeId;
use session_manager::SessionId;
use thiserror::Error;

// ── RegistryError ───────────────────────────────────────────────

/// Typed failure surface for the distributed session registry.
///
/// Every fallible registry API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RegistryError {
    /// A configuration field is out of range or contradictory.
    #[error("invalid registry configuration: {0}")]
    InvalidConfig(String),

    /// The named node is not on the ring.
    #[error("node not found: {0}")]
    NodeNotFound(NodeId),

    /// A node with the given id is already registered.
    #[error("node already exists: {0}")]
    NodeExists(NodeId),

    /// No ownership entry exists for the session.
    #[error("session not in registry: {0}")]
    SessionNotFound(SessionId),

    /// The session is already registered (active generation).
    #[error("session already registered: {0}")]
    SessionExists(SessionId),

    /// The owning lease has expired; ownership can be reclaimed.
    #[error("lease expired for session {0}")]
    LeaseExpired(SessionId),

    /// The lessor rejected the renewal or grant (quorum, policy).
    #[error("lease rejected: {0}")]
    LeaseRejected(&'static str),

    /// An ownership transfer could not complete.
    #[error("ownership transfer failed: {0}")]
    OwnershipTransferFailed(&'static str),

    /// The target node is not in a state that can serve sessions.
    #[error("node is not alive: {0}")]
    NodeUnavailable(NodeId),

    /// The consistent-hash ring has no nodes; lookups cannot resolve.
    #[error("ring is empty — no nodes registered")]
    RingEmpty,

    /// A hard capacity limit was reached (sessions, nodes, bytes).
    #[error("capacity exceeded: {0}")]
    CapacityExceeded(&'static str),

    /// Registry is shutting down; reject new operations.
    #[error("registry is shutting down")]
    ShuttingDown,
}

impl RegistryError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidConfig`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidConfig(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (node temporarily
    /// unavailable, lease quorum not met, ownership transfer
    /// racing with another writer).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::NodeUnavailable(_)
                | Self::LeaseRejected(_)
                | Self::OwnershipTransferFailed(_)
                | Self::LeaseExpired(_)
        )
    }

    /// Whether the error reflects a capacity condition.
    ///
    /// Capacity errors signal load-shedding: callers should either
    /// reject new work or wait for existing work to drain.
    #[must_use]
    pub const fn is_capacity(&self) -> bool {
        matches!(self, Self::CapacityExceeded(_) | Self::RingEmpty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(RegistryError::InvalidConfig("bad".into()).is_fatal());
        assert!(RegistryError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!RegistryError::NodeNotFound(NodeId::new(1)).is_fatal());
        assert!(!RegistryError::RingEmpty.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(RegistryError::NodeUnavailable(NodeId::new(1)).is_retryable());
        assert!(RegistryError::LeaseRejected("quorum").is_retryable());
        assert!(RegistryError::OwnershipTransferFailed("race").is_retryable());
        assert!(RegistryError::LeaseExpired(SessionId::nil()).is_retryable());
        // Non-retryable examples.
        assert!(!RegistryError::RingEmpty.is_retryable());
        assert!(!RegistryError::InvalidConfig("bad".into()).is_retryable());
    }

    #[test]
    fn capacity_classification() {
        assert!(RegistryError::CapacityExceeded("max sessions").is_capacity());
        assert!(RegistryError::RingEmpty.is_capacity());
        // Non-capacity examples.
        assert!(!RegistryError::ShuttingDown.is_capacity());
        assert!(!RegistryError::NodeExists(NodeId::new(1)).is_capacity());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = RegistryError::NodeNotFound(NodeId::new(42));
        let s = format!("{e}");
        assert!(s.contains("node not found"));
        assert!(s.contains("42"));

        let e = RegistryError::RingEmpty;
        assert!(format!("{e}").contains("ring is empty"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = RegistryError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_capacity());

        let retry = RegistryError::NodeUnavailable(NodeId::new(1));
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_capacity());

        let cap = RegistryError::CapacityExceeded("limit");
        assert!(!cap.is_fatal());
        assert!(!cap.is_retryable());
        assert!(cap.is_capacity());
    }
}
