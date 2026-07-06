//! [`Authorizer`] trait + [`PolicyCache`] + [`Permission`] enum +
//! [`TenantId`] / [`Namespace`].
//!
//! See `DESIGN.md` Part 8 (authorization).
//!
//! # AuthZ model
//!
//! Vireon supports both RBAC (Role-Based Access Control) and ABAC
//! (Attribute-Based Access Control). The `Authorizer` trait
//! abstracts over policy engines; the default `PolicyCache` is a
//! per-session in-memory cache of resolved permissions.
//!
//! ## Scoping
//!
//! Permissions are scoped to:
//!
//! - **Tenant** — multi-tenant isolation. A session in tenant A
//!   cannot see tenant B's sessions.
//! - **Namespace** — sub-tenant scoping (e.g. one team's streams
//!   within a larger org).
//!
//! ## Phase 0 status
//!
//! Phase 0 ships the type surface + the `Permission` taxonomy.
//! Functional policy enforcement lands in Phase 2+.

use crate::auth::AuthContext;
use crate::error::SessionManagerError;

/// Tenant identifier (RBAC scope). Zero is reserved for the
/// "no tenant" sentinel.
pub type TenantId = u64;

/// Namespace within a tenant. Empty string is reserved for the root
/// namespace.
pub type Namespace = str;

/// Permission enum covering all session-layer actions.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Create a new session.
    SessionCreate,
    /// Destroy a session.
    SessionDestroy,
    /// Open a stream within a session.
    StreamOpen,
    /// Send data on a stream.
    StreamSend,
    /// Receive data on a stream.
    StreamRecv,
    /// Migrate a session to a new connection.
    SessionMigrate,
    /// Resume a session (0-RTT).
    SessionResume,
    /// Inspect session stats / metadata.
    SessionInspect,
    /// Administrative action (eviction, quota override).
    SessionAdmin,
}

/// Authorization decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthzDecision {
    /// Action permitted.
    Allow,
    /// Action denied (insufficient permissions).
    Deny,
}

/// Authorizer trait. Phase 0 ships only the trait; concrete policy
/// engines land in Phase 2+.
pub trait Authorizer: Send + Sync {
    /// Check whether `ctx` is permitted to perform `action` in the
    /// given namespace.
    fn check(&self, ctx: &AuthContext, action: Permission, namespace: &str) -> AuthzDecision;
}

/// Per-session permission cache. Avoids re-running the policy
/// engine on every stream open.
///
/// Phase 0 ships only the type signature; Phase 2+ adds a real
/// cache (LRU + TTL).
// TODO(phase-2): LRU + TTL cache.
#[derive(Clone, Debug, Default)]
pub struct PolicyCache;

impl PolicyCache {
    /// Construct a new empty cache.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Phase-2 `check` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::AuthorizationDenied`]
    /// in Phase 0.
    pub fn check(
        &self,
        _ctx: &AuthContext,
        _action: Permission,
        _namespace: &str,
    ) -> Result<AuthzDecision, SessionManagerError> {
        // TODO(phase-2): cache lookup + policy engine call.
        Err(SessionManagerError::AuthorizationDenied(
            "policy cache phase-2 stub",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_variants_distinct() {
        assert_ne!(Permission::SessionCreate, Permission::SessionDestroy);
        assert_ne!(Permission::StreamOpen, Permission::StreamSend);
        assert_ne!(Permission::SessionMigrate, Permission::SessionResume);
    }

    #[test]
    fn decision_equality() {
        assert_eq!(AuthzDecision::Allow, AuthzDecision::Allow);
        assert_ne!(AuthzDecision::Allow, AuthzDecision::Deny);
    }

    #[test]
    fn policy_cache_phase0_stub() {
        let cache = PolicyCache::new();
        let ctx = AuthContext::anonymous();
        let r = cache.check(&ctx, Permission::SessionCreate, "default");
        assert!(matches!(
            r,
            Err(SessionManagerError::AuthorizationDenied(_))
        ));
    }
}
