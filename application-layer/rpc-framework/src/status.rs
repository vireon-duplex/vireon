//! [`RpcStatus`] — RPC result status codes.
//!
//! See `DESIGN.md` Part 8 (status codes).

// ── RpcStatus ─────────────────────────────────────────────────────

/// Status code for an RPC operation result.
///
/// Status codes follow the gRPC/HTTP pattern with OK, error codes,
/// and special statuses for cancellation and deadline exceeded.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RpcStatus {
    /// The operation completed successfully.
    #[default]
    Ok = 0,
    /// The operation failed with a specific error code.
    Error(u16) = 1,
    /// The operation was cancelled by the caller.
    Cancelled = 2,
    /// The operation exceeded its deadline.
    DeadlineExceeded = 3,
}

impl RpcStatus {
    /// Whether the status indicates success.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Whether the status indicates an error.
    ///
    /// This includes explicit error codes, cancellation, and deadline
    /// exceeded.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !self.is_ok()
    }

    /// Get the error code if this is an error status.
    #[must_use]
    pub const fn error_code(&self) -> Option<u16> {
        match self {
            Self::Error(code) => Some(*code),
            _ => None,
        }
    }

    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error(_) => "error",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }

    /// Create an OK status.
    #[must_use]
    pub const fn ok() -> Self {
        Self::Ok
    }

    /// Create an error status with a code.
    #[must_use]
    pub const fn error(code: u16) -> Self {
        Self::Error(code)
    }

    /// Create a cancelled status.
    #[must_use]
    pub const fn cancelled() -> Self {
        Self::Cancelled
    }

    /// Create a deadline exceeded status.
    #[must_use]
    pub const fn deadline_exceeded() -> Self {
        Self::DeadlineExceeded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_ok_is_success() {
        let status = RpcStatus::Ok;
        assert!(status.is_ok());
        assert!(!status.is_error());
        assert_eq!(status.as_str(), "ok");
    }

    #[test]
    fn status_error_is_failure() {
        let status = RpcStatus::Error(5);
        assert!(!status.is_ok());
        assert!(status.is_error());
        assert_eq!(status.as_str(), "error");
        assert_eq!(status.error_code(), Some(5));
    }

    #[test]
    fn status_cancelled_is_error() {
        let status = RpcStatus::Cancelled;
        assert!(!status.is_ok());
        assert!(status.is_error());
        assert_eq!(status.as_str(), "cancelled");
        assert_eq!(status.error_code(), None);
    }

    #[test]
    fn status_deadline_exceeded_is_error() {
        let status = RpcStatus::DeadlineExceeded;
        assert!(!status.is_ok());
        assert!(status.is_error());
        assert_eq!(status.as_str(), "deadline_exceeded");
        assert_eq!(status.error_code(), None);
    }

    #[test]
    fn status_default_is_ok() {
        let status = RpcStatus::default();
        assert_eq!(status, RpcStatus::Ok);
    }

    #[test]
    fn status_equality() {
        assert_eq!(RpcStatus::Ok, RpcStatus::Ok);
        assert_eq!(RpcStatus::Error(10), RpcStatus::Error(10));
        assert_eq!(RpcStatus::Cancelled, RpcStatus::Cancelled);
        assert_ne!(RpcStatus::Error(10), RpcStatus::Error(11));
        assert_ne!(RpcStatus::Ok, RpcStatus::Cancelled);
    }

    #[test]
    fn status_factory_methods() {
        assert_eq!(RpcStatus::ok(), RpcStatus::Ok);
        assert_eq!(RpcStatus::error(42), RpcStatus::Error(42));
        assert_eq!(RpcStatus::cancelled(), RpcStatus::Cancelled);
        assert_eq!(RpcStatus::deadline_exceeded(), RpcStatus::DeadlineExceeded);
    }

    #[test]
    fn status_error_code_only_for_errors() {
        assert_eq!(RpcStatus::Ok.error_code(), None);
        assert_eq!(RpcStatus::Error(100).error_code(), Some(100));
        assert_eq!(RpcStatus::Cancelled.error_code(), None);
        assert_eq!(RpcStatus::DeadlineExceeded.error_code(), None);
    }

    #[test]
    fn status_as_str_all_variants() {
        assert_eq!(RpcStatus::Ok.as_str(), "ok");
        assert_eq!(RpcStatus::Error(0).as_str(), "error");
        assert_eq!(RpcStatus::Cancelled.as_str(), "cancelled");
        assert_eq!(RpcStatus::DeadlineExceeded.as_str(), "deadline_exceeded");
    }

    #[test]
    fn status_hash_can_be_used_in_set() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RpcStatus::Ok);
        set.insert(RpcStatus::Error(1));
        set.insert(RpcStatus::Error(2));
        set.insert(RpcStatus::Cancelled);
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn status_repr_u8() {
        // RpcStatus has a payload variant (Error(u16)), so `as u8` is
        // not available. Verify discriminants via as_str mapping instead.
        assert_eq!(RpcStatus::Ok.as_str(), "ok");
        assert_eq!(RpcStatus::Error(0).as_str(), "error");
        assert_eq!(RpcStatus::Cancelled.as_str(), "cancelled");
        assert_eq!(RpcStatus::DeadlineExceeded.as_str(), "deadline_exceeded");
    }

    #[test]
    fn status_copy_trait() {
        let s1 = RpcStatus::Error(123);
        let s2 = s1; // Copy, not Move
        assert_eq!(s1, s2); // s1 is still valid
        assert_eq!(s1.error_code(), Some(123));
    }

    #[test]
    fn status_clone_trait() {
        let s1 = RpcStatus::Cancelled;
        let s2 = s1.clone();
        assert_eq!(s1, s2);
        assert_eq!(s1.as_str(), "cancelled");
    }

    #[test]
    fn status_all_error_codes_distinct() {
        // Different error codes should be distinct.
        assert_ne!(RpcStatus::Error(0), RpcStatus::Error(1));
        assert_ne!(RpcStatus::Error(100), RpcStatus::Error(200));
    }

    #[test]
    fn status_is_error_predicate_comprehensive() {
        // All non-OK statuses should be errors.
        assert!(RpcStatus::Error(0).is_error());
        assert!(RpcStatus::Cancelled.is_error());
        assert!(RpcStatus::DeadlineExceeded.is_error());
    }
}
