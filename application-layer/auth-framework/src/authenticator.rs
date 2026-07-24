//! [`AuthenticationPipeline`] — authentication with principal registry
//! and per-principal rate limiting.
//!
//! Provides an in-process principal registry backed by a
//! `HashMap<PrincipalId, Credential>`. Each `authenticate()` call is
//! rate-limited per principal using a sliding-window counter.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::AuthConfig;
use crate::credential::Credential;
use crate::error::AuthError;
use crate::health::HealthReport;
use crate::principal::PrincipalId;
use crate::stats::AuthStats;

/// Sliding window for rate limiting (nanoseconds).
const RATE_WINDOW_NS: u64 = 60 * 1_000_000_000; // 1 minute

/// Authentication pipeline — owns principal registry, rate limiter,
/// and stats.
#[derive(Debug)]
pub struct AuthenticationPipeline {
    cfg: AuthConfig,
    stats: AuthStats,
    principals: HashMap<PrincipalId, Credential>,
    /// Per-principal attempt timestamps (nanoseconds since epoch).
    rate_tracker: HashMap<PrincipalId, Vec<u64>>,
}

/// Returns the current time in nanoseconds since UNIX_EPOCH.
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl AuthenticationPipeline {
    /// Construct from validated configuration.
    ///
    /// # Errors
    /// Returns [`AuthError::InvalidCredential`] if config validation fails.
    pub fn new(cfg: AuthConfig) -> Result<Self, AuthError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: AuthStats::new(),
            principals: HashMap::new(),
            rate_tracker: HashMap::new(),
        })
    }

    /// Register a principal with a credential.
    ///
    /// Allows subsequent `authenticate()` calls to succeed for this
    /// principal. Overwrites if already registered.
    pub fn register_principal(&mut self, principal: PrincipalId, credential: Credential) {
        self.principals.insert(principal, credential);
    }

    /// Authenticate a principal against stored credentials.
    ///
    /// Compares the `data_hash` of the provided credential against the
    /// stored one. Rate-limited per principal: if `rate_limit_per_minute`
    /// attempts have been made in the last 60 seconds, returns
    /// [`AuthError::RateLimited`].
    ///
    /// # Errors
    /// - [`AuthError::RateLimited`] — too many attempts.
    /// - [`AuthError::UnknownPrincipal`] — principal not registered.
    /// - [`AuthError::AuthenticationFailed`] — credential hash mismatch.
    pub fn authenticate(
        &mut self,
        principal: PrincipalId,
        credential: Credential,
    ) -> Result<u64, AuthError> {
        self.stats.record_auth_attempt();

        // Rate limit check.
        let now = now_ns();
        let cutoff = now.saturating_sub(RATE_WINDOW_NS);
        let timestamps = self.rate_tracker.entry(principal).or_default();
        timestamps.retain(|&t| t > cutoff);
        if timestamps.len() >= self.cfg.rate_limit_per_minute as usize {
            self.stats.record_rate_limited();
            return Err(AuthError::RateLimited);
        }
        timestamps.push(now);

        let stored = self
            .principals
            .get(&principal)
            .ok_or(AuthError::UnknownPrincipal(principal))?;

        if stored.data_hash() == credential.data_hash() {
            self.stats.record_auth_success();
            Ok(principal.raw())
        } else {
            self.stats.record_auth_failure();
            Err(AuthError::AuthenticationFailed("credential hash mismatch"))
        }
    }

    /// Snapshot the pipeline's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats.
    #[must_use]
    pub fn stats(&self) -> &AuthStats {
        &self.stats
    }

    /// Number of registered principals.
    #[must_use]
    pub fn principal_count(&self) -> usize {
        self.principals.len()
    }

    /// Returns the rate limit config (attempts per minute).
    #[must_use]
    pub const fn rate_limit_per_minute(&self) -> u32 {
        self.cfg.rate_limit_per_minute
    }
}

impl Default for AuthenticationPipeline {
    /// Equivalent to `AuthenticationPipeline::new(AuthConfig::new())`.
    /// `AuthConfig::new()` is a `const fn` whose default values always
    /// pass `validate()`, so this never panics.
    fn default() -> Self {
        Self::new(AuthConfig::new())
            // SAFETY: AuthConfig::new() always validates Ok (token_ttl_ms
            // and min_password_length are inside the valid ranges).
            // Keeping a fallback rather than `unwrap()` so callers in
            // deny-panic workspaces don't have to suppress the lint.
            .unwrap_or_else(|_| Self {
                cfg: AuthConfig::new(),
                stats: AuthStats::new(),
                principals: HashMap::new(),
                rate_tracker: HashMap::new(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;
    use crate::credential::CredentialType;

    fn fresh() -> AuthenticationPipeline {
        let cfg = Preset::Standard.to_config();
        AuthenticationPipeline::new(cfg).unwrap()
    }

    #[test]
    fn authenticator_constructs() {
        let auth = fresh();
        assert!(auth.health().is_healthy());
        assert_eq!(auth.principal_count(), 0);
    }

    #[test]
    fn register_and_authenticate_succeeds() {
        let mut auth = fresh();
        let principal = PrincipalId::new(1);
        let cred = Credential::new(CredentialType::Password, 12345, 0);
        auth.register_principal(principal, cred);

        let result = auth.authenticate(principal, cred);
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn authenticate_unknown_principal_fails() {
        let mut auth = fresh();
        let cred = Credential::new(CredentialType::Password, 12345, 0);
        let result = auth.authenticate(PrincipalId::new(99), cred);
        assert!(matches!(result, Err(AuthError::UnknownPrincipal(_))));
    }

    #[test]
    fn authenticate_wrong_credential_fails() {
        let mut auth = fresh();
        let principal = PrincipalId::new(1);
        let stored_cred = Credential::new(CredentialType::Password, 11111, 0);
        auth.register_principal(principal, stored_cred);

        let wrong_cred = Credential::new(CredentialType::Password, 99999, 0);
        let result = auth.authenticate(principal, wrong_cred);
        assert!(matches!(result, Err(AuthError::AuthenticationFailed(_))));
    }

    #[test]
    fn register_overwrites_existing() {
        let mut auth = fresh();
        let principal = PrincipalId::new(1);
        let old_cred = Credential::new(CredentialType::Password, 11111, 0);
        auth.register_principal(principal, old_cred);

        let new_cred = Credential::new(CredentialType::Token, 22222, 0);
        auth.register_principal(principal, new_cred);

        assert!(auth.authenticate(principal, old_cred).is_err());
        assert!(auth.authenticate(principal, new_cred).is_ok());
    }

    #[test]
    fn principal_count_tracks_registrations() {
        let mut auth = fresh();
        assert_eq!(auth.principal_count(), 0);
        auth.register_principal(
            PrincipalId::new(1),
            Credential::new(CredentialType::Password, 1, 0),
        );
        assert_eq!(auth.principal_count(), 1);
        auth.register_principal(
            PrincipalId::new(2),
            Credential::new(CredentialType::Token, 2, 0),
        );
        assert_eq!(auth.principal_count(), 2);
    }

    #[test]
    fn rate_limiting_kicks_in() {
        let cfg = AuthConfig {
            rate_limit_per_minute: 3,
            ..Preset::Standard.to_config()
        };
        let mut auth = AuthenticationPipeline::new(cfg).unwrap();
        let principal = PrincipalId::new(1);
        let cred = Credential::new(CredentialType::Password, 12345, 0);
        auth.register_principal(principal, cred);

        let wrong = Credential::new(CredentialType::Password, 99999, 0);

        // 3 attempts allowed.
        assert!(auth.authenticate(principal, wrong.clone()).is_err()); // 1
        assert!(auth.authenticate(principal, wrong.clone()).is_err()); // 2
        assert!(auth.authenticate(principal, wrong.clone()).is_err()); // 3
        // 4th should be rate limited.
        let result = auth.authenticate(principal, wrong);
        assert!(matches!(result, Err(AuthError::RateLimited)));
    }

    #[test]
    fn rate_limit_per_minute_exposed() {
        let auth = fresh();
        assert_eq!(
            auth.rate_limit_per_minute(),
            Preset::Standard.to_config().rate_limit_per_minute
        );
    }
}
