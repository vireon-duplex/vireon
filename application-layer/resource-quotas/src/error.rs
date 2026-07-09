//! [`QuotaError`] — the typed failure surface of the resource quotas
//! system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`QuotaError::is_fatal`] / [`QuotaError::is_retryable`] /
//! [`QuotaError::is_capacity`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidQuota`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `LimitExceeded` | Back off + retry |
//! | Capacity | `LimitExceeded` | Shed load / wait |

use crate::tenant::TenantId;
use thiserror::Error;

// ── QuotaError ───────────────────────────────────────────────────

/// Typed failure surface for the resource quotas system.
///
/// Every fallible API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum QuotaError {
    /// A quota limit was exceeded.
    #[error("quota limit exceeded: {0}")]
    LimitExceeded(&'static str),

    /// A tenant was not found in the quota manager.
    #[error("tenant not found: {0}")]
    TenantNotFound(TenantId),

    /// A quota configuration is invalid.
    #[error("invalid quota: {0}")]
    InvalidQuota(&'static str),

    /// A tenant with the given id is already registered.
    #[error("tenant already exists: {0}")]
    DuplicateTenant(TenantId),

    /// Quota manager is shutting down; reject new operations.
    #[error("quota manager is shutting down")]
    ShuttingDown,
}

impl QuotaError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidQuota`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidQuota(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (limit exceeded
    /// due to a temporary spike).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::LimitExceeded(_))
    }

    /// Whether the error reflects a capacity condition.
    ///
    /// Capacity errors signal load-shedding: callers should either
    /// reject new work or wait for existing work to drain.
    #[must_use]
    pub const fn is_capacity(&self) -> bool {
        matches!(self, Self::LimitExceeded(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(QuotaError::InvalidQuota("bad").is_fatal());
        assert!(QuotaError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!QuotaError::LimitExceeded("sessions").is_fatal());
        assert!(!QuotaError::TenantNotFound(TenantId::new(1)).is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(QuotaError::LimitExceeded("sessions").is_retryable());
        // Non-retryable examples.
        assert!(!QuotaError::ShuttingDown.is_retryable());
        assert!(!QuotaError::InvalidQuota("bad").is_retryable());
    }

    #[test]
    fn capacity_classification() {
        assert!(QuotaError::LimitExceeded("sessions").is_capacity());
        // Non-capacity examples.
        assert!(!QuotaError::ShuttingDown.is_capacity());
        assert!(!QuotaError::DuplicateTenant(TenantId::new(1)).is_capacity());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = QuotaError::LimitExceeded("max sessions");
        let s = format!("{e}");
        assert!(s.contains("quota limit exceeded"));
        assert!(s.contains("max sessions"));

        let e = QuotaError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = QuotaError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_capacity());

        let retry = QuotaError::LimitExceeded("sessions");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(retry.is_capacity());
    }
}
