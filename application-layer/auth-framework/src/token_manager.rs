//! [`TokenManager`] — token lifecycle management.

use crate::config::AuthConfig;
use crate::error::AuthError;
use crate::health::HealthReport;
use crate::principal::PrincipalId;
use crate::stats::AuthStats;

#[derive(Debug)]
pub struct TokenManager {
    #[allow(dead_code)]
    cfg: AuthConfig,
    stats: AuthStats,
    #[allow(dead_code)]
    token_count: u64,
}

impl TokenManager {
    #[must_use]
    pub const fn new(cfg: AuthConfig) -> Self {
        Self {
            cfg,
            stats: AuthStats::new(),
            token_count: 0,
        }
    }

    pub fn issue(&mut self, _principal: PrincipalId) -> Result<u64, AuthError> {
        // TODO(phase-1): implement token issuance
        Err(AuthError::ShuttingDown)
    }

    pub fn validate(&mut self, _token_hash: u64) -> Result<bool, AuthError> {
        // TODO(phase-1): implement token validation
        Err(AuthError::ShuttingDown)
    }

    pub fn revoke(&mut self, _token_hash: u64) -> Result<(), AuthError> {
        // TODO(phase-1): implement token revocation
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

    #[test]
    fn token_manager_constructs() {
        let cfg = Preset::Standard.to_config();
        let tm = TokenManager::new(cfg);
        assert!(tm.health().is_healthy());
    }

    #[test]
    fn issue_phase0_stub() {
        let cfg = Preset::Standard.to_config();
        let mut tm = TokenManager::new(cfg);
        assert!(tm.issue(PrincipalId::new(1)).is_err());
    }

    #[test]
    fn validate_phase0_stub() {
        let cfg = Preset::Standard.to_config();
        let mut tm = TokenManager::new(cfg);
        assert!(tm.validate(123).is_err());
    }

    #[test]
    fn revoke_phase0_stub() {
        let cfg = Preset::Standard.to_config();
        let mut tm = TokenManager::new(cfg);
        assert!(tm.revoke(123).is_err());
    }
}
