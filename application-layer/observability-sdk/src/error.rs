//! [`ObservabilityError`] — the typed failure surface of the
//! observability SDK.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`ObservabilityError::is_fatal`] / [`ObservabilityError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `InvalidTraceId`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `ExportFailed` | Back off + retry |
//! | Capacity | (none yet) | Shed load / wait |

use thiserror::Error;

// ── ObservabilityError ───────────────────────────────────────────

/// Typed failure surface for the observability SDK.
///
/// Every fallible API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ObservabilityError {
    /// A metric export operation failed (backend unavailable).
    #[error("export failed: {0}")]
    ExportFailed(&'static str),

    /// A metric key was not found in the registry.
    #[error("metric not found: {0}")]
    MetricNotFound(String),

    /// A metric label is invalid (empty, too long, bad characters).
    #[error("invalid label: {0}")]
    InvalidLabel(&'static str),

    /// A span ID was not found in the tracer.
    #[error("span not found: {0}")]
    SpanNotFound(u64),

    /// A trace ID is invalid (nil, malformed).
    #[error("invalid trace ID")]
    InvalidTraceId,

    /// SDK is shutting down; reject new operations.
    #[error("observability SDK is shutting down")]
    ShuttingDown,
}

impl ObservabilityError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`InvalidTraceId`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::InvalidTraceId | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (backend temporarily
    /// unavailable, network hiccup).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::ExportFailed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(ObservabilityError::InvalidTraceId.is_fatal());
        assert!(ObservabilityError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!ObservabilityError::ExportFailed("backend").is_fatal());
        assert!(!ObservabilityError::MetricNotFound("key".to_string()).is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(ObservabilityError::ExportFailed("backend").is_retryable());
        // Non-retryable examples.
        assert!(!ObservabilityError::ShuttingDown.is_retryable());
        assert!(!ObservabilityError::InvalidTraceId.is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = ObservabilityError::ExportFailed("backend unavailable");
        let s = format!("{e}");
        assert!(s.contains("export failed"));
        assert!(s.contains("backend unavailable"));

        let e = ObservabilityError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = ObservabilityError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = ObservabilityError::ExportFailed("backend");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
    }
}
