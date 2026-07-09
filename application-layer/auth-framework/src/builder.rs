//! [`AuthFrameworkBuilder`] — fluent builder for auth configuration.

use crate::config::{AuthConfig, Preset};
use crate::error::AuthError;

#[derive(Debug, Clone)]
pub struct AuthFrameworkBuilder {
    cfg: AuthConfig,
}

impl AuthFrameworkBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: AuthConfig::new(),
        }
    }

    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    #[must_use]
    pub const fn token_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.cfg.token_ttl_ms = ttl_ms;
        self
    }

    #[must_use]
    pub const fn max_auth_attempts(mut self, max: u8) -> Self {
        self.cfg.max_auth_attempts = max;
        self
    }

    #[must_use]
    pub const fn rate_limit_per_minute(mut self, limit: u32) -> Self {
        self.cfg.rate_limit_per_minute = limit;
        self
    }

    #[must_use]
    pub const fn enable_rbac(mut self, enable: bool) -> Self {
        self.cfg.enable_rbac = enable;
        self
    }

    #[must_use]
    pub const fn enable_abac(mut self, enable: bool) -> Self {
        self.cfg.enable_abac = enable;
        self
    }

    pub fn build(self) -> Result<AuthConfig, AuthError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for AuthFrameworkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_from_preset() {
        let cfg = AuthFrameworkBuilder::from_preset(Preset::Strict)
            .build()
            .unwrap();
        assert!(cfg.enable_rbac);
        assert!(cfg.enable_abac);
    }

    #[test]
    fn builder_chains() {
        let cfg = AuthFrameworkBuilder::new()
            .token_ttl_ms(7_200_000)
            .max_auth_attempts(5)
            .build()
            .unwrap();
        assert_eq!(cfg.token_ttl_ms, 7_200_000);
        assert_eq!(cfg.max_auth_attempts, 5);
    }
}
