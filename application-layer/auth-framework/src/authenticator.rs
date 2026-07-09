//! [`AuthenticationPipeline`] — authentication facade.

use crate::config::AuthConfig;
use crate::credential::Credential;
use crate::error::AuthError;
use crate::health::HealthReport;
use crate::principal::PrincipalId;
use crate::stats::AuthStats;

#[derive(Debug)]
pub struct AuthenticationPipeline {
    #[allow(dead_code)]
    cfg: AuthConfig,
    stats: AuthStats,
}

impl AuthenticationPipeline {
    pub fn new(cfg: AuthConfig) -> Result<Self, AuthError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: AuthStats::new(),
        })
    }

    pub fn authenticate(
        &mut self,
        _principal: PrincipalId,
        _credential: Credential,
    ) -> Result<u64, AuthError> {
        // TODO(phase-1): implement credential verification
        self.stats.record_auth_attempt();
        Err(AuthError::ShuttingDown)
    }

    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    #[must_use]
    pub fn stats(&self) -> &AuthStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Preset;
    use crate::credential::CredentialType;

    #[test]
    fn authenticator_constructs() {
        let cfg = Preset::Standard.to_config();
        let auth = AuthenticationPipeline::new(cfg).unwrap();
        assert!(auth.health().is_healthy());
    }

    #[test]
    fn authenticate_phase0_stub() {
        let cfg = Preset::Standard.to_config();
        let mut auth = AuthenticationPipeline::new(cfg).unwrap();
        let cred = Credential::new(CredentialType::Password, 123, 0);
        assert!(auth.authenticate(PrincipalId::new(1), cred).is_err());
    }
}
