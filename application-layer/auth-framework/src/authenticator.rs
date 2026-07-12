//! [`AuthenticationPipeline`] — authentication facade.
//!
//! Provides an in-process principal registry backed by a
//! `HashMap<PrincipalId, Credential>`. Single-threaded (reactor-local).

use std::collections::HashMap;

use crate::config::AuthConfig;
use crate::credential::Credential;
use crate::error::AuthError;
use crate::health::HealthReport;
use crate::principal::PrincipalId;
use crate::stats::AuthStats;

/// Authentication pipeline — owns principal registry and stats.
#[derive(Debug)]
pub struct AuthenticationPipeline {
    #[allow(dead_code)]
    cfg: AuthConfig,
    stats: AuthStats,
    principals: HashMap<PrincipalId, Credential>,
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
    /// stored one. On success returns the principal's raw ID. On failure
    /// returns an appropriate error variant.
    ///
    /// # Errors
    /// - [`AuthError::UnknownPrincipal`] — principal not registered.
    /// - [`AuthError::AuthenticationFailed`] — credential hash mismatch.
    pub fn authenticate(
        &mut self,
        principal: PrincipalId,
        credential: Credential,
    ) -> Result<u64, AuthError> {
        self.stats.record_auth_attempt();

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

        // Old credential should now fail.
        assert!(auth.authenticate(principal, old_cred).is_err());
        // New credential should succeed.
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
}
