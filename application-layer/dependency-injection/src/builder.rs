//! [`ContainerBuilder`] — fluent builder for [`ContainerConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `ContainerConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{ContainerConfig, Preset};
use crate::error::DependencyError;
use crate::lifetime::ServiceLifetime;

/// Fluent builder for [`ContainerConfig`].
#[derive(Debug, Clone)]
pub struct ContainerBuilder {
    cfg: ContainerConfig,
}

impl ContainerBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: ContainerConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the maximum number of services.
    #[must_use]
    pub const fn max_services(mut self, n: u32) -> Self {
        self.cfg.max_services = n;
        self
    }

    /// Override the maximum scope depth.
    #[must_use]
    pub const fn max_scope_depth(mut self, depth: u8) -> Self {
        self.cfg.max_scope_depth = depth;
        self
    }

    /// Enable or disable lazy resolution.
    #[must_use]
    pub const fn enable_lazy(mut self, on: bool) -> Self {
        self.cfg.enable_lazy_resolution = on;
        self
    }

    /// Set the default lifetime.
    #[must_use]
    pub const fn default_lifetime(mut self, lifetime: ServiceLifetime) -> Self {
        self.cfg.default_lifetime = lifetime;
        self
    }

    /// Enable or disable validation on registration.
    #[must_use]
    pub const fn validate_on_register(mut self, on: bool) -> Self {
        self.cfg.validate_on_register = on;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`DependencyError::TypeMismatch`] if any field is out
    /// of range.
    pub fn build(self) -> Result<ContainerConfig, DependencyError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for ContainerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_MAX_SERVICES, MAX_SCOPE_DEPTH, MIN_MAX_SERVICES, MIN_SCOPE_DEPTH};

    #[test]
    fn new_builder_has_defaults() {
        let builder = ContainerBuilder::new();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_services, 256);
        assert_eq!(cfg.max_scope_depth, 16);
        assert!(cfg.enable_lazy_resolution);
    }

    #[test]
    fn builder_from_preset() {
        let builder = ContainerBuilder::from_preset(Preset::Strict);
        let cfg = builder.build().unwrap();
        assert!(!cfg.enable_lazy_resolution);
        assert!(cfg.validate_on_register);
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Singleton);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = ContainerBuilder::new()
            .max_services(128)
            .max_scope_depth(8)
            .enable_lazy(false)
            .default_lifetime(ServiceLifetime::Scoped)
            .build()
            .unwrap();

        assert_eq!(cfg.max_services, 128);
        assert_eq!(cfg.max_scope_depth, 8);
        assert!(!cfg.enable_lazy_resolution);
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Scoped);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = ContainerBuilder::new().max_services(0).build();
        assert!(res.is_err());

        let res = ContainerBuilder::new().max_scope_depth(0).build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = ContainerBuilder::new()
            .max_services(MIN_MAX_SERVICES)
            .max_scope_depth(MIN_SCOPE_DEPTH)
            .build()
            .unwrap();
        assert_eq!(cfg.max_services, MIN_MAX_SERVICES);
        assert_eq!(cfg.max_scope_depth, MIN_SCOPE_DEPTH);

        let cfg = ContainerBuilder::new()
            .max_services(MAX_MAX_SERVICES)
            .max_scope_depth(MAX_SCOPE_DEPTH)
            .build()
            .unwrap();
        assert_eq!(cfg.max_services, MAX_MAX_SERVICES);
        assert_eq!(cfg.max_scope_depth, MAX_SCOPE_DEPTH);
    }

    #[test]
    fn builder_preserves_preset_overrides() {
        let cfg = ContainerBuilder::from_preset(Preset::Lazy)
            .max_services(100)
            .build()
            .unwrap();

        assert_eq!(cfg.max_services, 100);
        assert!(cfg.enable_lazy_resolution); // from preset
        assert!(!cfg.validate_on_register); // from preset
    }

    #[test]
    fn builder_default_impl() {
        let builder = ContainerBuilder::default();
        assert_eq!(builder.cfg.max_services, 256);
    }

    #[test]
    fn builder_from_lazy_preset() {
        let builder = ContainerBuilder::from_preset(Preset::Lazy);
        let cfg = builder.build().unwrap();
        assert!(cfg.enable_lazy_resolution);
        assert!(!cfg.validate_on_register);
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Transient);
    }

    #[test]
    fn builder_enables_validation() {
        let cfg = ContainerBuilder::new()
            .validate_on_register(true)
            .build()
            .unwrap();
        assert!(cfg.validate_on_register);
    }

    #[test]
    fn builder_disables_validation() {
        let cfg = ContainerBuilder::new()
            .validate_on_register(false)
            .build()
            .unwrap();
        assert!(!cfg.validate_on_register);
    }

    #[test]
    fn builder_sets_singleton_default() {
        let cfg = ContainerBuilder::new()
            .default_lifetime(ServiceLifetime::Singleton)
            .build()
            .unwrap();
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Singleton);
    }

    #[test]
    fn builder_sets_scoped_default() {
        let cfg = ContainerBuilder::new()
            .default_lifetime(ServiceLifetime::Scoped)
            .build()
            .unwrap();
        assert_eq!(cfg.default_lifetime, ServiceLifetime::Scoped);
    }

    #[test]
    fn builder_rejects_invalid_max_services() {
        let res = ContainerBuilder::new()
            .max_services(MAX_MAX_SERVICES + 1)
            .build();
        assert!(res.is_err());
        assert!(matches!(res, Err(DependencyError::TypeMismatch(_))));
    }

    #[test]
    fn builder_rejects_invalid_scope_depth() {
        let res = ContainerBuilder::new()
            .max_scope_depth(MAX_SCOPE_DEPTH + 1)
            .build();
        assert!(res.is_err());
        assert!(matches!(res, Err(DependencyError::TypeMismatch(_))));
    }
}
