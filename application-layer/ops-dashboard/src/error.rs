//! [`OpsDashboardError`] — the typed failure surface of the
//! ops-dashboard crate.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`OpsDashboardError::is_fatal`] / [`OpsDashboardError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `WidgetNotFound`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `QueryFailed`, `CollectorUnavailable` | Back off + retry |
//! | Capacity | (none yet) | Shed load / wait |

use thiserror::Error;

// ── OpsDashboardError ───────────────────────────────────────────

/// Typed failure surface for the ops-dashboard crate.
///
/// Every fallible API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum OpsDashboardError {
    /// The requested widget ID was not found in the dashboard registry.
    #[error("widget not found")]
    WidgetNotFound,

    /// A query operation failed (timeout, backend error, etc.).
    #[error("query failed: {0}")]
    QueryFailed(&'static str),

    /// The metrics collector is unavailable (not started, crashed, etc.).
    #[error("collector unavailable")]
    CollectorUnavailable,

    /// The dashboard is shutting down; reject new operations.
    #[error("ops dashboard is shutting down")]
    ShuttingDown,
}

impl OpsDashboardError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`WidgetNotFound`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::WidgetNotFound | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (backend temporarily
    /// unavailable, query timeout).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::QueryFailed(_) | Self::CollectorUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(OpsDashboardError::WidgetNotFound.is_fatal());
        assert!(OpsDashboardError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!OpsDashboardError::QueryFailed("timeout").is_fatal());
        assert!(!OpsDashboardError::CollectorUnavailable.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(OpsDashboardError::QueryFailed("timeout").is_retryable());
        assert!(OpsDashboardError::CollectorUnavailable.is_retryable());
        // Non-retryable examples.
        assert!(!OpsDashboardError::WidgetNotFound.is_retryable());
        assert!(!OpsDashboardError::ShuttingDown.is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = OpsDashboardError::QueryFailed("connection refused");
        let s = format!("{e}");
        assert!(s.contains("query failed"));
        assert!(s.contains("connection refused"));

        let e = OpsDashboardError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));

        let e = OpsDashboardError::WidgetNotFound;
        assert!(format!("{e}").contains("widget not found"));

        let e = OpsDashboardError::CollectorUnavailable;
        assert!(format!("{e}").contains("collector unavailable"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = OpsDashboardError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = OpsDashboardError::QueryFailed("backend");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());

        let retry2 = OpsDashboardError::CollectorUnavailable;
        assert!(!retry2.is_fatal());
        assert!(retry2.is_retryable());

        let fatal2 = OpsDashboardError::WidgetNotFound;
        assert!(fatal2.is_fatal());
        assert!(!fatal2.is_retryable());
    }
}
