//! [`MiddlewareStage`] — execution stage for middleware components.
//!
//! See `DESIGN.md` Part 6 (stage dispatch).
//!
//! # Stages
//!
//! Middleware executes in discrete stages during request processing:
//!
//! ```text
//!   Request ──► PreAuth ──► AuthN ──► AuthZ ──► PreDispatch ──► [Handler]
//!      │                                                                │
//!      ▼                                                                ▼
//!   Response ◄─── Response ◄─── PostDispatch ◄─────────────────────────┘
//!
//!   Error ◄─── Error (from any stage)
//! ```

// ── MiddlewareStage ─────────────────────────────────────────────

/// Execution stage for middleware components.
///
/// Stages define when middleware executes during request processing
/// and what operations are permitted.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MiddlewareStage {
    /// Before authentication (logging, metrics).
    #[default]
    PreAuth = 0,
    /// Authentication (verify credentials).
    AuthN = 1,
    /// Authorization (check permissions).
    AuthZ = 2,
    /// Before request dispatch (routing, headers).
    PreDispatch = 3,
    /// After request dispatch (response transformation).
    PostDispatch = 4,
    /// Response processing (compression, caching).
    Response = 5,
    /// Error handling (error transformation, logging).
    Error = 6,
}

impl MiddlewareStage {
    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreAuth => "pre_auth",
            Self::AuthN => "authn",
            Self::AuthZ => "authz",
            Self::PreDispatch => "pre_dispatch",
            Self::PostDispatch => "post_dispatch",
            Self::Response => "response",
            Self::Error => "error",
        }
    }

    /// Execution order for sorting middlewares.
    #[must_use]
    pub const fn order(self) -> u8 {
        self as u8
    }

    /// Whether this is an authentication stage.
    #[must_use]
    pub const fn is_auth_stage(self) -> bool {
        matches!(self, Self::AuthN | Self::AuthZ)
    }

    /// Whether this is a dispatch stage (around handler).
    #[must_use]
    pub const fn is_dispatch_stage(self) -> bool {
        matches!(self, Self::PreDispatch | Self::PostDispatch)
    }

    /// Whether this is a response stage.
    #[must_use]
    pub const fn is_response_stage(self) -> bool {
        matches!(self, Self::Response | Self::PostDispatch)
    }

    /// Get the raw u8 value for serialization.
    #[must_use]
    pub const fn raw(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_pre_auth() {
        assert_eq!(MiddlewareStage::default(), MiddlewareStage::PreAuth);
    }

    #[test]
    fn as_str_returns_correct_name() {
        assert_eq!(MiddlewareStage::PreAuth.as_str(), "pre_auth");
        assert_eq!(MiddlewareStage::AuthN.as_str(), "authn");
        assert_eq!(MiddlewareStage::AuthZ.as_str(), "authz");
        assert_eq!(MiddlewareStage::PreDispatch.as_str(), "pre_dispatch");
        assert_eq!(MiddlewareStage::PostDispatch.as_str(), "post_dispatch");
        assert_eq!(MiddlewareStage::Response.as_str(), "response");
        assert_eq!(MiddlewareStage::Error.as_str(), "error");
    }

    #[test]
    fn order_returns_discriminant() {
        assert_eq!(MiddlewareStage::PreAuth.order(), 0);
        assert_eq!(MiddlewareStage::AuthN.order(), 1);
        assert_eq!(MiddlewareStage::AuthZ.order(), 2);
        assert_eq!(MiddlewareStage::PreDispatch.order(), 3);
        assert_eq!(MiddlewareStage::PostDispatch.order(), 4);
        assert_eq!(MiddlewareStage::Response.order(), 5);
        assert_eq!(MiddlewareStage::Error.order(), 6);
    }

    #[test]
    fn is_auth_stage_for_auth_stages() {
        assert!(MiddlewareStage::AuthN.is_auth_stage());
        assert!(MiddlewareStage::AuthZ.is_auth_stage());
        assert!(!MiddlewareStage::PreAuth.is_auth_stage());
        assert!(!MiddlewareStage::Response.is_auth_stage());
    }

    #[test]
    fn is_dispatch_stage_for_dispatch_stages() {
        assert!(MiddlewareStage::PreDispatch.is_dispatch_stage());
        assert!(MiddlewareStage::PostDispatch.is_dispatch_stage());
        assert!(!MiddlewareStage::AuthN.is_dispatch_stage());
        assert!(!MiddlewareStage::Response.is_dispatch_stage());
    }

    #[test]
    fn is_response_stage_for_response_stages() {
        assert!(MiddlewareStage::Response.is_response_stage());
        assert!(MiddlewareStage::PostDispatch.is_response_stage());
        assert!(!MiddlewareStage::PreDispatch.is_response_stage());
        assert!(!MiddlewareStage::AuthN.is_response_stage());
    }

    #[test]
    fn raw_returns_discriminant_value() {
        assert_eq!(MiddlewareStage::PreAuth.raw(), 0);
        assert_eq!(MiddlewareStage::AuthN.raw(), 1);
        assert_eq!(MiddlewareStage::AuthZ.raw(), 2);
        assert_eq!(MiddlewareStage::PreDispatch.raw(), 3);
        assert_eq!(MiddlewareStage::PostDispatch.raw(), 4);
        assert_eq!(MiddlewareStage::Response.raw(), 5);
        assert_eq!(MiddlewareStage::Error.raw(), 6);
    }

    #[test]
    fn stage_can_be_hashed() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(MiddlewareStage::AuthN);
        set.insert(MiddlewareStage::AuthZ);
        assert_eq!(set.len(), 2);
        set.insert(MiddlewareStage::AuthN); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn stages_are_ordered() {
        assert!(MiddlewareStage::PreAuth < MiddlewareStage::AuthN);
        assert!(MiddlewareStage::AuthN < MiddlewareStage::AuthZ);
        assert!(MiddlewareStage::AuthZ < MiddlewareStage::PreDispatch);
        assert!(MiddlewareStage::PreDispatch < MiddlewareStage::PostDispatch);
        assert!(MiddlewareStage::PostDispatch < MiddlewareStage::Response);
        assert!(MiddlewareStage::Response < MiddlewareStage::Error);
    }
}
