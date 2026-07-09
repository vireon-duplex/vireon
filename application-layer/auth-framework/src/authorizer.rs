//! [`AuthorizationEngine`] — authorization facade.

use crate::config::AuthConfig;
use crate::error::AuthError;
use crate::health::HealthReport;
use crate::permission::Permission;
use crate::principal::PrincipalId;
use crate::stats::AuthStats;

#[derive(Debug)]
pub struct AuthorizationEngine {
    #[allow(dead_code)]
    cfg: AuthConfig,
    stats: AuthStats,
}

impl AuthorizationEngine {
    #[must_use]
    pub const fn new(cfg: AuthConfig) -> Self {
        Self {
            cfg,
            stats: AuthStats::new(),
        }
    }

    pub fn authorize(
        &mut self,
        _principal: PrincipalId,
        _permission: Permission,
    ) -> Result<(), AuthError> {
        // TODO(phase-1): implement RBAC/ABAC check
        self.stats.record_authz_check(false);
        Err(AuthError::PermissionDenied("access denied"))
    }

    pub fn check_role(
        &mut self,
        _principal: PrincipalId,
        _role_id: u16,
    ) -> Result<bool, AuthError> {
        // TODO(phase-1): implement role check
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
    fn authorizer_constructs() {
        let cfg = Preset::Standard.to_config();
        let authz = AuthorizationEngine::new(cfg);
        assert!(authz.health().is_healthy());
    }

    #[test]
    fn authorize_phase0_stub() {
        let cfg = Preset::Standard.to_config();
        let mut authz = AuthorizationEngine::new(cfg);
        let perm = Permission::new(1, 2);
        assert!(authz.authorize(PrincipalId::new(1), perm).is_err());
    }
}
