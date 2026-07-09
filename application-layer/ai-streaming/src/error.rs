//! [`AiStreamError`] — the typed failure surface of the AI streaming
//! framework.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into categories, mirroring the
//! [`AiStreamError::is_fatal`] / [`AiStreamError::is_retryable`]
//! predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `StreamExists`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `TokenFailed`, `ToolCallFailed` | Back off + retry |
//! | Not found | `StreamNotFound` | Check stream ID |

use crate::stream::AiStreamId;
use thiserror::Error;

// ── AiStreamError ────────────────────────────────────────────────

/// Typed failure surface for the AI streaming framework.
///
/// Every fallible AI streaming API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AiStreamError {
    /// The requested stream does not exist.
    #[error("stream not found: {0}")]
    StreamNotFound(AiStreamId),

    /// A stream with the given ID already exists.
    #[error("stream already exists: {0}")]
    StreamExists(AiStreamId),

    /// Token generation or streaming failed.
    #[error("token failed: {0}")]
    TokenFailed(&'static str),

    /// A tool call failed during execution.
    #[error("tool call failed")]
    ToolCallFailed,

    /// The AI streaming framework is shutting down.
    #[error("ai streaming is shutting down")]
    ShuttingDown,
}

impl AiStreamError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (`StreamExists`)
    /// or terminal lifecycle states (`ShuttingDown`) that no amount
    /// of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::StreamExists(_) | Self::ShuttingDown)
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (token generation
    /// failures, tool call failures).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::TokenFailed(_) | Self::ToolCallFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(AiStreamError::StreamExists(AiStreamId::new(1)).is_fatal());
        assert!(AiStreamError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!AiStreamError::StreamNotFound(AiStreamId::new(1)).is_fatal());
        assert!(!AiStreamError::TokenFailed("temp").is_fatal());
        assert!(!AiStreamError::ToolCallFailed.is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(AiStreamError::TokenFailed("temp overload").is_retryable());
        assert!(AiStreamError::ToolCallFailed.is_retryable());
        // Non-retryable examples.
        assert!(!AiStreamError::ShuttingDown.is_retryable());
        assert!(!AiStreamError::StreamExists(AiStreamId::new(1)).is_retryable());
        assert!(!AiStreamError::StreamNotFound(AiStreamId::new(1)).is_retryable());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let e = AiStreamError::StreamNotFound(AiStreamId::new(42));
        let s = format!("{e}");
        assert!(s.contains("stream not found"));
        assert!(s.contains("42"));

        let e = AiStreamError::TokenFailed("buffer full");
        assert!(format!("{e}").contains("token failed"));

        let e = AiStreamError::ToolCallFailed;
        assert!(format!("{e}").contains("tool call failed"));

        let e = AiStreamError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = AiStreamError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());

        let retry = AiStreamError::TokenFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
    }

    #[test]
    fn stream_not_found_with_valid_id() {
        let e = AiStreamError::StreamNotFound(AiStreamId::new(123));
        assert!(format!("{e}").contains("not found"));
        assert!(!e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn stream_exists_is_fatal() {
        let e = AiStreamError::StreamExists(AiStreamId::new(1));
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
    }

    #[test]
    fn token_failed_is_retryable() {
        let e = AiStreamError::TokenFailed("network error");
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn tool_call_failed_is_retryable() {
        let e = AiStreamError::ToolCallFailed;
        assert!(!e.is_fatal());
        assert!(e.is_retryable());
    }

    #[test]
    fn shutting_down_is_fatal() {
        let e = AiStreamError::ShuttingDown;
        assert!(e.is_fatal());
        assert!(!e.is_retryable());
    }
}
