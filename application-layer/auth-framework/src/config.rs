//! [`AuthConfig`], [`Preset`] — declarative configuration for the auth
//! framework.

use crate::error::AuthError;

pub const MIN_PASSWORD_LENGTH: u8 = 8;
pub const MAX_PASSWORD_LENGTH: u8 = 128;
pub const MIN_RATE_LIMIT: u32 = 1;
pub const MAX_RATE_LIMIT: u32 = 10_000;
pub const MIN_TTL_MS: u64 = 60_000; // 1 minute
pub const MAX_TTL_MS: u64 = 86_400_000; // 24 hours

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthConfig {
    pub token_ttl_ms: u64,
    pub max_auth_attempts: u8,
    pub rate_limit_per_minute: u32,
    pub enable_rbac: bool,
    pub enable_abac: bool,
    pub min_password_length: u8,
}

impl AuthConfig {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            token_ttl_ms: 3_600_000, // 1 hour
            max_auth_attempts: 3,
            rate_limit_per_minute: 10,
            enable_rbac: true,
            enable_abac: false,
            min_password_length: MIN_PASSWORD_LENGTH,
        }
    }

    pub fn validate(&self) -> Result<(), AuthError> {
        if self.token_ttl_ms < MIN_TTL_MS || self.token_ttl_ms > MAX_TTL_MS {
            return Err(AuthError::ShuttingDown);
        }
        if self.min_password_length < MIN_PASSWORD_LENGTH {
            return Err(AuthError::ShuttingDown);
        }
        Ok(())
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    Permissive,
    Standard,
    Strict,
    HighSecurity,
}

impl Preset {
    #[must_use]
    pub const fn to_config(self) -> AuthConfig {
        match self {
            Self::Permissive => AuthConfig {
                token_ttl_ms: 24 * 3_600_000,
                max_auth_attempts: 10,
                rate_limit_per_minute: 100,
                enable_rbac: false,
                enable_abac: false,
                min_password_length: 8,
            },
            Self::Standard => AuthConfig::new(),
            Self::Strict => AuthConfig {
                token_ttl_ms: 3_600_000,
                max_auth_attempts: 3,
                rate_limit_per_minute: 10,
                enable_rbac: true,
                enable_abac: true,
                min_password_length: 12,
            },
            Self::HighSecurity => AuthConfig {
                token_ttl_ms: 1_800_000,
                max_auth_attempts: 3,
                rate_limit_per_minute: 5,
                enable_rbac: true,
                enable_abac: true,
                min_password_length: 16,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = AuthConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn preset_to_config() {
        let cfg = Preset::Strict.to_config();
        assert!(cfg.enable_rbac);
        assert!(cfg.enable_abac);
        assert_eq!(cfg.min_password_length, 12);
    }

    #[test]
    fn validate_rejects_bad_ttl() {
        let mut cfg = AuthConfig::default();
        cfg.token_ttl_ms = 10;
        assert!(cfg.validate().is_err());
    }
}
