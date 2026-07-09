//! [`VersionNegotiationError`] — the typed failure surface of the
//! version negotiation system.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`VersionNegotiationError::is_fatal`] /
//! [`VersionNegotiationError::is_retryable`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidVersion`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `NoOverlap`, `Deprecated` | Back off + retry |
//! | Incompatible | `IncompatibleVersions` | Fix version range |

use thiserror::Error;

// ── VersionNegotiationError ───────────────────────────────────────

/// Typed failure surface for the version negotiation system.
///
/// Every fallible negotiation API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum VersionNegotiationError {
    /// No compatible version overlap between local and remote.
    #[error("no version overlap between local and remote")]
    NoOverlap,

    /// A version number is invalid (out of range, malformed).
    #[error("invalid version: {0}")]
    InvalidVersion(u32),

    /// A version is deprecated and should not be used.
    #[error("version {0} is deprecated")]
    Deprecated(u32),

    /// Versions are incompatible per the compatibility matrix.
    #[error("incompatible versions")]
    IncompatibleVersions,

    /// The negotiation system is shutting down.
    #[error("version negotiation is shutting down")]
    ShuttingDown,
}

impl VersionNegotiationError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidVersion`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidVersion(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (version temporarily
    /// unavailable, deprecated version still supported).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::NoOverlap | Self::Deprecated(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(VersionNegotiationError::InvalidVersion(0).is_fatal());
        assert!(VersionNegotiationError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!VersionNegotiationError::NoOverlap.is_fatal());
        assert!(!VersionNegotiationError::Deprecated(1).is_fatal());
        assert!(!VersionNegotiationError::IncompatibleVersions.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(VersionNegotiationError::NoOverlap.is_retryable());
        assert!(VersionNegotiationError::Deprecated(1).is_retryable());
        // Non-retryable examples.
        assert!(!VersionNegotiationError::InvalidVersion(0).is_retryable());
        assert!(!VersionNegotiationError::ShuttingDown.is_retryable());
        assert!(!VersionNegotiationError::IncompatibleVersions.is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = VersionNegotiationError::InvalidVersion(999);
        let s = format!("{e}");
        assert!(s.contains("invalid version"));
        assert!(s.contains("999"));

        let e = VersionNegotiationError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = VersionNegotiationError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = VersionNegotiationError::NoOverlap;
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());

        let incompat = VersionNegotiationError::IncompatibleVersions;
        assert!(!incompat.is_fatal());
        assert!(!incompat.is_retryable());
    }

    #[test]
    fn deprecated_is_retryable() {
        // Deprecated versions may still work temporarily.
        let e = VersionNegotiationError::Deprecated(1);
        assert!(e.is_retryable());
        assert!(!e.is_fatal());
    }
}
