//! [`Authenticator`] trait + [`AuthenticationPipeline`] +
//! [`AuthContext`].
//!
//! See `DESIGN.md` Part 7 (authentication).
//!
//! # Auth model
//!
//! Vireon's session manager supports pluggable authentication. The
//! `Authenticator` trait abstracts over:
//!
//! - **TLS 1.3** (RFC 9001) — certificate-based, handshake-embedded.
//! - **JWT** (RFC 7519) — bearer token in the first application
//!   message.
//! - **OAuth 2.0** (RFC 6749) — token exchange for short-lived
//!   access tokens.
//! - **mTLS** (mutual TLS) — both peers present certificates.
//!
//! The pipeline runs after the QUIC handshake completes and before
//! the session transitions to `Ready`. 0-RTT (RFC 9001 §2.3) is
//! gated by replay protection (Phase 2+).
//!
//! # Phase 0 status
//!
//! Phase 0 ships the trait surface + `AuthContext`. Concrete
//! authenticators (TLS/JWT/OAuth/mTLS) and the pipeline executor
//! land in Phase 2+.

use crate::error::SessionManagerError;
use crate::id::SessionId;

/// Authentication method used to validate a session.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// TLS 1.3 certificate (RFC 9001).
    Tls,
    /// JWT bearer token (RFC 7519).
    Jwt,
    /// OAuth 2.0 access token (RFC 6749).
    OAuth2,
    /// Mutual TLS (both peers authenticated).
    Mtls,
    /// Anonymous (no authentication; for testing only).
    Anonymous,
}

/// Identity established after successful authentication. Used by
/// the authorization layer to scope permissions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    /// Subject identifier (e.g. user id, service id).
    pub subject: String,
    /// Tenant id (RBAC scope).
    pub tenant_id: Option<u64>,
    /// Authentication method used.
    pub method: AuthMethod,
    /// SHA-256 fingerprint of the client certificate (if mTLS/TLS).
    pub certificate_fingerprint: Option<[u8; 32]>,
    /// Hash of the bearer token (if JWT/OAuth). Stored as a hash to
    /// avoid keeping the raw token in memory.
    pub token_hash: Option<[u8; 32]>,
}

impl AuthContext {
    /// Construct an anonymous context (testing only).
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            subject: "anonymous".to_string(),
            tenant_id: None,
            method: AuthMethod::Anonymous,
            certificate_fingerprint: None,
            token_hash: None,
        }
    }
}

/// Authenticator trait. Phase 0 ships only the trait; concrete
/// implementations land in Phase 2+.
pub trait Authenticator: Send + Sync {
    /// Authenticate a session using method-specific evidence
    /// (certificate chain, JWT, OAuth token, …).
    ///
    /// # Errors
    /// Returns [`SessionManagerError::AuthenticationFailed`] on any
    /// validation failure.
    fn authenticate(
        &self,
        session_id: SessionId,
        evidence: &[u8],
    ) -> Result<AuthContext, SessionManagerError>;
}

/// Authentication pipeline. Runs authenticators in order; first
/// success wins.
///
/// Phase 0 ships only the type signature; Phase 2+ adds the real
/// pipeline executor.
// TODO(phase-2): real pipeline executor.
#[derive(Clone, Debug, Default)]
pub struct AuthenticationPipeline;

impl AuthenticationPipeline {
    /// Construct a new empty pipeline.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Phase-2 `run` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::AuthenticationFailed`]
    /// in Phase 0.
    pub fn run(
        &self,
        _session_id: SessionId,
        _evidence: &[u8],
    ) -> Result<AuthContext, SessionManagerError> {
        // TODO(phase-2): run authenticators in order.
        Err(SessionManagerError::AuthenticationFailed(
            "pipeline phase-2 stub",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_context() {
        let ctx = AuthContext::anonymous();
        assert_eq!(ctx.method, AuthMethod::Anonymous);
        assert_eq!(ctx.subject, "anonymous");
        assert!(ctx.tenant_id.is_none());
    }

    #[test]
    fn pipeline_phase0_stub() {
        let p = AuthenticationPipeline::new();
        assert!(matches!(
            p.run(SessionId::nil(), &[]),
            Err(SessionManagerError::AuthenticationFailed(_))
        ));
    }

    #[test]
    fn auth_method_variants_distinct() {
        assert_ne!(AuthMethod::Tls, AuthMethod::Jwt);
        assert_ne!(AuthMethod::Jwt, AuthMethod::OAuth2);
        assert_ne!(AuthMethod::OAuth2, AuthMethod::Mtls);
    }
}
