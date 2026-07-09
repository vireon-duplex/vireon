//! [`PluginError`] — the typed failure surface of the plugin manager.
//!
//! See `DESIGN.md` Part 11 (failure modes) and Part 12
//! (observability).
//!
//! # Variants
//!
//! Errors are grouped into three categories, mirroring the
//! [`PluginError::is_fatal`] / [`PluginError::is_retryable`] /
//! [`PluginError::is_validation`] predicates:
//!
//! | Category | Example | Caller action |
//! |----------|---------|---------------|
//! | Fatal | `ApiMismatch`, `VersionConflict`, `ShuttingDown` | Abort; don't retry |
//! | Retryable | `LoadFailed` (transient I/O) | Back off + retry |
//! | Validation | `NotFound`, `AlreadyLoaded`, `CapabilityMissing`, `DependencyMissing` | Fix input |

use crate::id::{PluginId, PluginVersion};
use thiserror::Error;

// ── PluginError ───────────────────────────────────────────────────

/// Typed failure surface for the plugin manager.
///
/// Every fallible plugin API returns one of these variants so
/// that callers can branch on a small, exhaustive set of failure
/// modes instead of string-matching on error messages.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum PluginError {
    /// The plugin with the given id was not found.
    #[error("plugin not found: {0}")]
    NotFound(PluginId),

    /// Two versions of the same plugin conflict.
    #[error("version conflict: plugin {0} version {1} conflicts with loaded version")]
    VersionConflict(PluginId, PluginVersion),

    /// A plugin failed to load (transient I/O error).
    #[error("plugin load failed: {0}")]
    LoadFailed(&'static str),

    /// The plugin's API version is incompatible with the runtime.
    #[error("API version mismatch: plugin requires {0}, runtime supports {1}")]
    ApiMismatch(u16, u16),

    /// The plugin is already loaded.
    #[error("plugin already loaded: {0}")]
    AlreadyLoaded(PluginId),

    /// A required dependency is missing.
    #[error("dependency missing: {0}")]
    DependencyMissing(PluginId),

    /// A required capability is not provided.
    #[error("capability missing: {0}")]
    CapabilityMissing(&'static str),

    /// The manager is shutting down; reject new operations.
    #[error("plugin manager is shutting down")]
    ShuttingDown,
}

impl PluginError {
    /// Whether the error is fatal — the caller must NOT retry.
    ///
    /// Fatal errors indicate programming mistakes (API mismatch)
    /// or terminal lifecycle states (shutting down, version conflict)
    /// that no amount of retrying will fix.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::ApiMismatch(_, _) | Self::VersionConflict(_, _) | Self::ShuttingDown
        )
    }

    /// Whether the error is retryable — the caller may retry after
    /// a backoff.
    ///
    /// Retryable errors are typically transient (I/O failures during
    /// load).
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::LoadFailed(_))
    }

    /// Whether the error reflects a validation problem.
    ///
    /// Validation errors signal incorrect caller input: missing
    /// dependencies, duplicate loads, or capability checks.
    #[must_use]
    pub const fn is_validation(&self) -> bool {
        matches!(
            self,
            Self::NotFound(_)
                | Self::AlreadyLoaded(_)
                | Self::CapabilityMissing(_)
                | Self::DependencyMissing(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_classification() {
        assert!(PluginError::ApiMismatch(2, 1).is_fatal());
        assert!(
            PluginError::VersionConflict(PluginId::nil(), PluginVersion::new(1, 0, 0)).is_fatal()
        );
        assert!(PluginError::ShuttingDown.is_fatal());
        // Non-fatal examples.
        assert!(!PluginError::NotFound(PluginId::nil()).is_fatal());
        assert!(!PluginError::LoadFailed("temp").is_fatal());
    }

    #[test]
    fn retryable_classification() {
        assert!(PluginError::LoadFailed("EAGAIN").is_retryable());
        // Non-retryable examples.
        assert!(!PluginError::ShuttingDown.is_retryable());
        assert!(!PluginError::ApiMismatch(2, 1).is_retryable());
        assert!(!PluginError::NotFound(PluginId::nil()).is_retryable());
    }

    #[test]
    fn validation_classification() {
        assert!(PluginError::NotFound(PluginId::nil()).is_validation());
        assert!(PluginError::AlreadyLoaded(PluginId::nil()).is_validation());
        assert!(PluginError::CapabilityMissing("auth").is_validation());
        assert!(PluginError::DependencyMissing(PluginId::nil()).is_validation());
        // Non-validation examples.
        assert!(!PluginError::ShuttingDown.is_validation());
        assert!(!PluginError::LoadFailed("temp").is_validation());
    }

    #[test]
    fn error_messages_are_human_readable() {
        let id = PluginId::nil();
        let e = PluginError::NotFound(id);
        let s = format!("{e}");
        assert!(s.contains("plugin not found"));

        let e = PluginError::ApiMismatch(2, 1);
        assert!(format!("{e}").contains("API version mismatch"));
        assert!(format!("{e}").contains("2"));
        assert!(format!("{e}").contains("1"));

        let e = PluginError::ShuttingDown;
        assert!(format!("{e}").contains("shutting down"));
    }

    #[test]
    fn variants_are_distinct() {
        // Ensure predicates do not overlap on the same variant.
        let fatal = PluginError::ShuttingDown;
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
        assert!(!fatal.is_validation());

        let retry = PluginError::LoadFailed("temp");
        assert!(!retry.is_fatal());
        assert!(retry.is_retryable());
        assert!(!retry.is_validation());

        let validation = PluginError::NotFound(PluginId::nil());
        assert!(!validation.is_fatal());
        assert!(!validation.is_retryable());
        assert!(validation.is_validation());
    }
}
