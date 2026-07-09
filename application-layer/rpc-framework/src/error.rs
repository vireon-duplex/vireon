//! [`RpcError`] — the typed failure surface of the RPC framework.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`RpcError::is_fatal`] / [`RpcError::is_retryable`] /
//! [`RpcError::is_transient`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidRequest`, `SerializationError`, `PermissionDenied`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `Timeout`, `Unavailable`, `StreamClosed` | Back off + retry |
//! | Cancelled | `Cancelled` | Operation cancelled by caller |

use crate::handle::StreamHandle;
use crate::method::RpcMethodId;
use thiserror::Error;

// ── RpcError ───────────────────────────────────────────────────

/// Typed failure surface for the RPC framework.
///
/// Every fallible RPC API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RpcError {
    /// The requested RPC method does not exist.
    #[error("RPC method not found: {0}")]
    MethodNotFound(RpcMethodId),

    /// The request is malformed or violates protocol constraints.
    #[error("invalid RPC request: {0}")]
    InvalidRequest(&'static str),

    /// Serialization or deserialization failed.
    #[error("serialization error: {0}")]
    SerializationError(&'static str),

    /// The operation timed out.
    #[error("RPC operation timed out")]
    Timeout,

    /// The underlying QUIC stream was closed unexpectedly.
    #[error("stream closed: {0}")]
    StreamClosed(StreamHandle),

    /// The RPC service is temporarily unavailable.
    #[error("RPC service unavailable: {0}")]
    Unavailable(&'static str),

    /// The operation was cancelled by the caller.
    #[error("operation cancelled: {0}")]
    Cancelled(StreamHandle),

    /// The caller lacks permission to invoke this method.
    #[error("permission denied")]
    PermissionDenied,

    /// The RPC framework is shutting down.
    #[error("RPC framework is shutting down")]
    ShuttingDown,
}

impl RpcError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidRequest`),
    /// serialization bugs, permission issues, or terminal lifecycle
    /// states (`ShuttingDown`) that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidRequest(_)
                | Self::SerializationError(_)
                | Self::PermissionDenied
                | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (timeout, service
    /// temporarily unavailable, stream closed).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Unavailable(_) | Self::StreamClosed(_)
        )
    }

    /// Whether the error represents a cancellation.
    ///
    /// Cancelled operations should not be retried unless explicitly
    /// requested by the caller.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(RpcError::InvalidRequest("bad payload").is_fatal());
        assert!(RpcError::SerializationError("parse error").is_fatal());
        assert!(RpcError::PermissionDenied.is_fatal());
        assert!(RpcError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!RpcError::MethodNotFound(RpcMethodId::new(1)).is_fatal());
        assert!(!RpcError::Timeout.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(RpcError::Timeout.is_retryable());
        assert!(RpcError::Unavailable("temp overload").is_retryable());
        assert!(RpcError::StreamClosed(StreamHandle::new(42)).is_retryable());
        // Non-retryable examples.
        assert!(!RpcError::InvalidRequest("bad").is_retryable());
        assert!(!RpcError::ShuttingDown.is_retryable());
    }

    #[test]
    fn cancelled_classification() {
        assert!(RpcError::Cancelled(StreamHandle::new(1)).is_cancelled());
        // Non-cancelled examples.
        assert!(!RpcError::Timeout.is_cancelled());
        assert!(!RpcError::PermissionDenied.is_cancelled());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = RpcError::MethodNotFound(RpcMethodId::new(42));
        let s = format!("{e}");
        assert!(s.contains("method not found"));
        assert!(s.contains("42"));

        let e = RpcError::Timeout;
        assert!(format!("{e}").contains("timed out"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = RpcError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_cancelled());

        let retry = RpcError::Timeout;
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_cancelled());

        let cancel = RpcError::Cancelled(StreamHandle::new(1));
        assert!(!cancel.is_fatal());
        assert!(!cancel.is_retryable());
        assert!(cancel.is_cancelled());
    }

    #[test]
    fn method_not_found_with_nil_id() {
        let e = RpcError::MethodNotFound(RpcMethodId::NIL);
        assert!(format!("{e}").contains("not found"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn serialization_error_is_fatal() {
        let e = RpcError::SerializationError("JSON parse error");
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
        assert!(!e.is_cancelled());
    }

    #[test]
    fn unavailable_is_retryable() {
        let e = RpcError::Unavailable("service overloaded");
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
        assert!(!e.is_cancelled());
    }

    #[test]
    fn permission_denied_is_fatal() {
        let e = RpcError::PermissionDenied;
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
        assert!(!e.is_cancelled());
    }

    #[test]
    fn stream_closed_is_retryable() {
        let handle = StreamHandle::new(123);
        let e = RpcError::StreamClosed(handle);
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
        assert!(!e.is_cancelled());
    }
}
