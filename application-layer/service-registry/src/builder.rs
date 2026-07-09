//! [`ServiceRegistryBuilder`] — fluent builder for [`ServiceRegistryConfig`]
//! and the registry itself.
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `ServiceRegistryConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{Preset, ServiceRegistryConfig};
use crate::error::ServiceRegistryError;

/// Fluent builder for [`ServiceRegistryConfig`].
#[derive(Debug, Clone)]
pub struct ServiceRegistryBuilder {
    cfg: ServiceRegistryConfig,
}

impl ServiceRegistryBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: ServiceRegistryConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max services.
    #[must_use]
    pub const fn max_services(mut self, max: u32) -> Self {
        self.cfg.max_services = max;
        self
    }

    /// Override the max instances per service.
    #[must_use]
    pub const fn max_instances_per_service(mut self, max: u8) -> Self {
        self.cfg.max_instances_per_service = max;
        self
    }

    /// Override the health check interval.
    #[must_use]
    pub const fn health_check_interval_ms(mut self, interval_ms: u64) -> Self {
        self.cfg.health_check_interval_ms = interval_ms;
        self
    }

    /// Enable or disable load balancing.
    #[must_use]
    pub const fn load_balancing(mut self, enable: bool) -> Self {
        self.cfg.enable_load_balancing = enable;
        self
    }

    /// Override the drain timeout.
    #[must_use]
    pub const fn drain_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.cfg.drain_timeout_ms = timeout_ms;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ShuttingDown`] if any field is out
    /// of range (mapped to ShuttingDown since InvalidEndpoint/InvalidConfig aren't available).
    pub fn build(self) -> Result<ServiceRegistryConfig, ServiceRegistryError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }

    /// Get the current config (for testing).
    #[must_use]
    pub const fn config(&self) -> &ServiceRegistryConfig {
        &self.cfg
    }
}

impl Default for ServiceRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_starts_with_defaults() {
        let builder = ServiceRegistryBuilder::new();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_services, 10_000);
        assert_eq!(cfg.max_instances_per_service, 50);
    }

    #[test]
    fn builder_from_preset() {
        let builder = ServiceRegistryBuilder::from_preset(Preset::Local);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_services, 100);
        assert_eq!(cfg.max_instances_per_service, 10);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = ServiceRegistryBuilder::new()
            .max_services(1000)
            .max_instances_per_service(20)
            .health_check_interval_ms(15_000)
            .load_balancing(false)
            .drain_timeout_ms(45_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_services, 1000);
        assert_eq!(cfg.max_instances_per_service, 20);
        assert_eq!(cfg.health_check_interval_ms, 15_000);
        assert!(!cfg.enable_load_balancing);
        assert_eq!(cfg.drain_timeout_ms, 45_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = ServiceRegistryBuilder::new()
            .health_check_interval_ms(50) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = ServiceRegistryBuilder::from_preset(Preset::Cluster)
            .health_check_interval_ms(20_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.health_check_interval_ms, 20_000);
        assert_eq!(cfg.max_instances_per_service, 50); // from preset
    }

    #[test]
    fn builder_default() {
        let builder = ServiceRegistryBuilder::default();
        assert_eq!(builder.config().max_services, 10_000);
    }

    #[test]
    fn builder_accepts_boundary_values() {
        use crate::config::{
            MAX_HEALTH_CHECK_MS, MAX_INSTANCES, MAX_SERVICES, MIN_HEALTH_CHECK_MS, MIN_INSTANCES,
            MIN_SERVICES,
        };
        let cfg = ServiceRegistryBuilder::new()
            .max_services(MIN_SERVICES)
            .max_instances_per_service(MIN_INSTANCES)
            .health_check_interval_ms(MIN_HEALTH_CHECK_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_services, MIN_SERVICES);
        assert_eq!(cfg.max_instances_per_service, MIN_INSTANCES);
        assert_eq!(cfg.health_check_interval_ms, MIN_HEALTH_CHECK_MS);

        let cfg = ServiceRegistryBuilder::new()
            .max_services(MAX_SERVICES)
            .max_instances_per_service(MAX_INSTANCES)
            .health_check_interval_ms(MAX_HEALTH_CHECK_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_services, MAX_SERVICES);
        assert_eq!(cfg.max_instances_per_service, MAX_INSTANCES);
        assert_eq!(cfg.health_check_interval_ms, MAX_HEALTH_CHECK_MS);
    }

    #[test]
    fn builder_const_fns() {
        // Ensure builder methods are const-friendly.
        const BUILDER: ServiceRegistryBuilder = ServiceRegistryBuilder::new()
            .max_services(500)
            .max_instances_per_service(25);
        assert_eq!(BUILDER.config().max_services, 500);
    }

    #[test]
    fn all_presets_build_valid_configs() {
        for preset in [
            Preset::Local,
            Preset::Cluster,
            Preset::Federation,
            Preset::Edge,
        ] {
            let cfg = ServiceRegistryBuilder::from_preset(preset).build().unwrap();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }
}
