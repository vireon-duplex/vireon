//! [`ContainerConfig`], [`Preset`] — declarative configuration for
//! the dependency injection container.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The two presets encode deployment archetypes for DI containers:
//!
//! | Preset | Validation | Resolution | Use case |
//!--------|------------|------------|----------|
//! | `Strict` | On registration | Eager | High-reliability systems |
//! | `Lazy` | Deferred | Lazy | High-throughput systems |

use crate::error::DependencyError;
use crate::lifetime::ServiceLifetime;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of services the container will accept.
pub const MIN_MAX_SERVICES: u32 = 1;

/// Maximum number of services the container will accept.
pub const MAX_MAX_SERVICES: u32 = 512;

/// Minimum scope depth (nesting level).
pub const MIN_SCOPE_DEPTH: u8 = 1;

/// Maximum scope depth (nesting level).
pub const MAX_SCOPE_DEPTH: u8 = 32;

// ── ContainerConfig ──────────────────────────────────────────────

/// Declarative configuration for the dependency injection container.
///
/// Constructed via [`ContainerConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContainerConfig {
    /// Maximum number of services that can be registered.
    pub max_services: u32,
    /// Maximum nesting depth for scoped services.
    pub max_scope_depth: u8,
    /// Whether to enable lazy resolution.
    pub enable_lazy_resolution: bool,
    /// Default lifetime for services without explicit lifetime.
    pub default_lifetime: ServiceLifetime,
    /// Whether to validate bindings on registration.
    pub validate_on_register: bool,
}

impl ContainerConfig {
    /// Construct a minimal config with defaults.
    ///
    /// Defaults: max_services=256, max_scope_depth=16, lazy resolution
    /// enabled, transient lifetime, validation on register disabled.
    pub const fn new() -> Self {
        Self {
            max_services: 256,
            max_scope_depth: 16,
            enable_lazy_resolution: true,
            default_lifetime: ServiceLifetime::Transient,
            validate_on_register: false,
        }
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`DependencyError::TypeMismatch`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), DependencyError> {
        if self.max_services < MIN_MAX_SERVICES || self.max_services > MAX_MAX_SERVICES {
            return Err(DependencyError::TypeMismatch("max_services out of range"));
        }
        if self.max_scope_depth < MIN_SCOPE_DEPTH || self.max_scope_depth > MAX_SCOPE_DEPTH {
            return Err(DependencyError::TypeMismatch(
                "max_scope_depth out of range",
            ));
        }
        Ok(())
    }
}

impl Default for ContainerConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// operational profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Strict validation on registration, eager resolution.
    /// High-reliability systems where errors should be caught early.
    Strict,
    /// Lazy resolution, deferred validation.
    /// High-throughput systems where startup time matters.
    Lazy,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> ContainerConfig {
        match self {
            Self::Strict => ContainerConfig {
                max_services: 256,
                max_scope_depth: 16,
                enable_lazy_resolution: false,
                default_lifetime: ServiceLifetime::Singleton,
                validate_on_register: true,
            },
            Self::Lazy => ContainerConfig {
                max_services: 512,
                max_scope_depth: 32,
                enable_lazy_resolution: true,
                default_lifetime: ServiceLifetime::Transient,
                validate_on_register: false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = ContainerConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_services, 256);
        assert_eq!(cfg.max_scope_depth, 16);
        assert!(cfg.enable_lazy_resolution);
    }

    #[test]
    fn validate_rejects_bad_max_services() {
        let mut cfg = ContainerConfig::default();
        cfg.max_services = 0;
        assert!(cfg.validate().is_err());
        cfg.max_services = MAX_MAX_SERVICES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_scope_depth() {
        let mut cfg = ContainerConfig::default();
        cfg.max_scope_depth = 0;
        assert!(cfg.validate().is_err());
        cfg.max_scope_depth = MAX_SCOPE_DEPTH + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let mut cfg = ContainerConfig::default();
        cfg.max_services = MIN_MAX_SERVICES;
        cfg.max_scope_depth = MIN_SCOPE_DEPTH;
        assert!(cfg.validate().is_ok());

        cfg.max_services = MAX_MAX_SERVICES;
        cfg.max_scope_depth = MAX_SCOPE_DEPTH;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn strict_preset_disables_lazy_resolution() {
        let cfg = Preset::Strict.to_config();
        assert!(!cfg.enable_lazy_resolution);
        assert!(cfg.validate_on_register);
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Singleton);
    }

    #[test]
    fn lazy_preset_enables_lazy_resolution() {
        let cfg = Preset::Lazy.to_config();
        assert!(cfg.enable_lazy_resolution);
        assert!(!cfg.validate_on_register);
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Transient);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Strict, Preset::Lazy] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn default_lifetime_matches_transient() {
        let cfg = ContainerConfig::default();
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Transient);
    }
}
