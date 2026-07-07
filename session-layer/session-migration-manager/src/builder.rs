//! [`MigrationServiceBuilder`] — fluent builder for the migration
//! service.
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `MigrationConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching every
//! field.

use crate::config::{MigrationConfig, Preset};
use crate::error::MigrationError;

/// Fluent builder for [`MigrationConfig`].
#[derive(Debug, Clone, Copy)]
pub struct MigrationServiceBuilder {
    cfg: MigrationConfig,
}

impl MigrationServiceBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: MigrationConfig::new(),
        }
    }

    /// Start from a preset.
    ///
    /// # Errors
    /// Returns [`MigrationError::InvalidConfig`] if the preset
    /// produces an invalid config.
    pub fn from_preset(preset: Preset) -> Result<Self, MigrationError> {
        Ok(Self {
            cfg: preset.to_config()?,
        })
    }

    /// Override the default timeout (ms).
    #[must_use]
    pub const fn timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_timeout_ms(ms);
        self
    }

    /// Override the max concurrent migrations.
    #[must_use]
    pub const fn max_concurrent(mut self, n: u32) -> Self {
        self.cfg = self.cfg.with_max_concurrent(n);
        self
    }

    /// Override the max retries.
    #[must_use]
    pub const fn max_retries(mut self, n: u8) -> Self {
        self.cfg = self.cfg.with_max_retries(n);
        self
    }

    /// Enable or disable snapshot compression.
    #[must_use]
    pub const fn compression(mut self, on: bool) -> Self {
        self.cfg = self.cfg.with_compression(on);
        self
    }

    /// Enable or disable ordering preservation.
    #[must_use]
    pub const fn ordering(mut self, on: bool) -> Self {
        self.cfg = self.cfg.with_ordering(on);
        self
    }

    /// Override the checkpoint interval (ms).
    #[must_use]
    pub const fn checkpoint_interval_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_checkpoint_interval(ms);
        self
    }

    /// Enable or disable zero-downtime migration.
    #[must_use]
    pub const fn zero_downtime(mut self, on: bool) -> Self {
        self.cfg = self.cfg.with_zero_downtime(on);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`MigrationError::InvalidConfig`] if any field is out
    /// of range.
    pub fn build(self) -> Result<MigrationConfig, MigrationError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for MigrationServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_CONCURRENT, MAX_TIMEOUT_MS, MIN_CONCURRENT, MIN_TIMEOUT_MS};

    #[test]
    fn default_builder_is_valid() {
        let cfg = MigrationServiceBuilder::new().build().unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = MigrationServiceBuilder::new()
            .timeout_ms(45_000)
            .max_concurrent(128)
            .max_retries(4)
            .compression(false)
            .ordering(false)
            .checkpoint_interval_ms(3_000)
            .zero_downtime(true)
            .build()
            .unwrap();
        assert_eq!(cfg.default_timeout_ms, 45_000);
        assert_eq!(cfg.max_concurrent_migrations, 128);
        assert!(!cfg.snapshot_compression);
        assert!(cfg.enable_zero_downtime);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = MigrationServiceBuilder::new()
            .timeout_ms(MIN_TIMEOUT_MS / 2) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = MigrationServiceBuilder::from_preset(Preset::Aggressive)
            .unwrap()
            .timeout_ms(15_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.default_timeout_ms, 15_000);
        assert!(cfg.enable_zero_downtime); // from preset
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = MigrationServiceBuilder::new()
            .timeout_ms(MIN_TIMEOUT_MS)
            .max_concurrent(MIN_CONCURRENT)
            .build()
            .unwrap();
        assert_eq!(cfg.default_timeout_ms, MIN_TIMEOUT_MS);

        let cfg = MigrationServiceBuilder::new()
            .timeout_ms(MAX_TIMEOUT_MS)
            .max_concurrent(MAX_CONCURRENT)
            .build()
            .unwrap();
        assert_eq!(cfg.default_timeout_ms, MAX_TIMEOUT_MS);
    }
}
