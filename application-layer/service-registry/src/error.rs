//! [`ServiceRegistryError`] — the typed failure surface of the service
//! registry.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`ServiceRegistryError::is_fatal`] / [`ServiceRegistryError::is_retryable`] /
//! [`ServiceRegistryError::is_capacity`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidEndpoint`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `NoHealthyInstance`, `ServiceDraining` | Back off + retry |
//! | Capacity | `CapacityExceeded` | Shed load / wait |

use crate::endpoint::ServiceEndpoint;
use crate::service::ServiceId;
use thiserror::Error;

// ── ServiceRegistryError ─────────────────────────────────────────────

/// Typed failure surface for the service registry.
///
/// Every fallible registry API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ServiceRegistryError {
    /// The requested service does not exist in the registry.
    #[error("service not found: {0}")]
    ServiceNotFound(ServiceId),

    /// A service with the given id is already registered.
    #[error("service already exists: {0}")]
    ServiceExists(ServiceId),

    /// No healthy instances available for the service.
    #[error("no healthy instances for service: {0}")]
    NoHealthyInstance(ServiceId),

    /// Invalid endpoint configuration (malformed address, bad port, etc.).
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(&'static str),

    /// The endpoint already exists for this service.
    #[error("endpoint already exists: {0:?}")]
    EndpointExists(ServiceEndpoint),

    /// Service is draining and not accepting new requests.
    #[error("service is draining: {0}")]
    ServiceDraining(ServiceId),

    /// A hard capacity limit was reached (services, instances, endpoints).
    #[error("capacity exceeded: {0}")]
    CapacityExceeded(&'static str),

    /// Registry is shutting down; reject new operations.
    #[error("registry is shutting down")]
    ShuttingDown,
}

impl ServiceRegistryError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidEndpoint`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidEndpoint(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (no healthy instances
    /// right now, service temporarily draining).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::NoHealthyInstance(_) | Self::ServiceDraining(_))
    }

    /// Whether the error reflects a capacity condition.
    ///
    /// Capacity errors signal load-shedding: callers should either
    /// reject new work or wait for existing work to drain.
    #[must_use]
    pub const fn is_capacity(&self) -> bool {
        matches!(self, Self::CapacityExceeded(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(ServiceRegistryError::InvalidEndpoint("bad address").is_fatal());
        assert!(ServiceRegistryError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!ServiceRegistryError::ServiceNotFound(ServiceId::nil()).is_fatal());
        assert!(!ServiceRegistryError::CapacityExceeded("limit").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(ServiceRegistryError::NoHealthyInstance(ServiceId::nil()).is_retryable());
        assert!(ServiceRegistryError::ServiceDraining(ServiceId::nil()).is_retryable());
        // Non-retryable examples.
        assert!(!ServiceRegistryError::ShuttingDown.is_retryable());
        assert!(!ServiceRegistryError::InvalidEndpoint("bad").is_retryable());
    }

    #[test]
    fn capacity_classification() {
        assert!(ServiceRegistryError::CapacityExceeded("max instances").is_capacity());
        // Non-capacity examples.
        assert!(!ServiceRegistryError::ShuttingDown.is_capacity());
        assert!(!ServiceRegistryError::ServiceExists(ServiceId::nil()).is_capacity());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = ServiceRegistryError::ServiceNotFound(ServiceId::nil());
        let s = format!("{e}");
        assert!(s.contains("service not found"));

        let e = ServiceRegistryError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = ServiceRegistryError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_capacity());

        let retry = ServiceRegistryError::NoHealthyInstance(ServiceId::nil());
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_capacity());

        let cap = ServiceRegistryError::CapacityExceeded("limit");
        assert!(!cap.is_fatal());
        assert!(!cap.is_retryable());
        assert!(cap.is_capacity());
    }
}
