//! [`GracefulBuilder`] — fluent builder for [`GracefulConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `GracefulConfig` has several fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{GracefulConfig, Preset};
use crate::error::ShutdownError;
use crate::phase::StartupPhase;

/// Fluent builder for [`GracefulConfig`].
#[derive(Debug, Clone)]
pub struct GracefulBuilder {
    cfg: GracefulConfig,
}

impl GracefulBuilder {
    /// Start a new builder with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: GracefulConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the startup timeout (milliseconds).
    #[must_use]
    pub const fn startup_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.startup_timeout_ms = ms;
        self
    }

    /// Override the shutdown timeout (milliseconds).
    #[must_use]
    pub const fn shutdown_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.shutdown_timeout_ms = ms;
        self
    }

    /// Override the drain timeout (milliseconds).
    #[must_use]
    pub const fn drain_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.drain_timeout_ms = ms;
        self
    }

    /// Override whether to force shutdown on timeout.
    #[must_use]
    pub const fn force_on_timeout(mut self, force: bool) -> Self {
        self.cfg.force_on_timeout = force;
        self
    }

    /// Override whether to enable health checks.
    #[must_use]
    pub const fn enable_health_checks(mut self, enable: bool) -> Self {
        self.cfg.enable_health_checks = enable;
        self
    }

    /// Override the phase at which to accept connections.
    #[must_use]
    pub const fn accept_connections_at_phase(mut self, phase: StartupPhase) -> Self {
        self.cfg.accept_connections_at_phase = phase;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`ShutdownError::InvalidPhase`] if any field is out
    /// of range.
    pub fn build(self) -> Result<GracefulConfig, ShutdownError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for GracefulBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_TIMEOUT_MS, MIN_TIMEOUT_MS};

    #[test]
    fn new_builder_has_defaults() {
        let builder = GracefulBuilder::new();
        let cfg = builder.cfg;
        assert_eq!(cfg.startup_timeout_ms, 30_000);
        assert_eq!(cfg.shutdown_timeout_ms, 30_000);
        assert_eq!(cfg.drain_timeout_ms, 10_000);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = GracefulBuilder::new()
            .startup_timeout_ms(45_000)
            .shutdown_timeout_ms(60_000)
            .drain_timeout_ms(15_000)
            .force_on_timeout(true)
            .build()
            .unwrap();
        assert_eq!(cfg.startup_timeout_ms, 45_000);
        assert_eq!(cfg.shutdown_timeout_ms, 60_000);
        assert_eq!(cfg.drain_timeout_ms, 15_000);
        assert!(cfg.force_on_timeout);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = GracefulBuilder::new()
            .startup_timeout_ms(500) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = GracefulBuilder::new()
            .startup_timeout_ms(MIN_TIMEOUT_MS)
            .shutdown_timeout_ms(MIN_TIMEOUT_MS)
            .drain_timeout_ms(MIN_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.startup_timeout_ms, MIN_TIMEOUT_MS);

        let cfg = GracefulBuilder::new()
            .startup_timeout_ms(MAX_TIMEOUT_MS)
            .shutdown_timeout_ms(MAX_TIMEOUT_MS)
            .drain_timeout_ms(MAX_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.startup_timeout_ms, MAX_TIMEOUT_MS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = GracefulBuilder::from_preset(Preset::Fast)
            .startup_timeout_ms(15_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.startup_timeout_ms, 15_000);
        assert!(cfg.force_on_timeout); // from preset
    }

    #[test]
    fn builder_enables_health_checks() {
        let cfg = GracefulBuilder::new()
            .enable_health_checks(true)
            .build()
            .unwrap();
        assert!(cfg.enable_health_checks);
    }

    #[test]
    fn builder_sets_accept_connections_phase() {
        let cfg = GracefulBuilder::new()
            .accept_connections_at_phase(StartupPhase::BindPorts)
            .build()
            .unwrap();
        assert_eq!(cfg.accept_connections_at_phase, StartupPhase::BindPorts);
    }

    #[test]
    fn builder_default_is_default() {
        let builder1 = GracefulBuilder::new();
        let builder2 = GracefulBuilder::default();
        assert_eq!(builder1.cfg, builder2.cfg);
    }

    #[test]
    fn from_preset_fast() {
        let cfg = GracefulBuilder::from_preset(Preset::Fast).build().unwrap();
        assert_eq!(cfg.startup_timeout_ms, 10_000);
        assert_eq!(cfg.shutdown_timeout_ms, 10_000);
        assert!(cfg.force_on_timeout);
    }

    #[test]
    fn from_preset_standard() {
        let cfg = GracefulBuilder::from_preset(Preset::Standard)
            .build()
            .unwrap();
        assert_eq!(cfg.startup_timeout_ms, 30_000);
        assert_eq!(cfg.shutdown_timeout_ms, 30_000);
        assert!(!cfg.force_on_timeout);
    }

    #[test]
    fn from_preset_safe() {
        let cfg = GracefulBuilder::from_preset(Preset::Safe).build().unwrap();
        assert_eq!(cfg.startup_timeout_ms, 60_000);
        assert_eq!(cfg.shutdown_timeout_ms, 60_000);
        assert!(cfg.enable_health_checks);
        assert!(!cfg.force_on_timeout);
    }

    #[test]
    fn all_presets_build_successfully() {
        for preset in [Preset::Fast, Preset::Standard, Preset::Safe] {
            let cfg = GracefulBuilder::from_preset(preset).build();
            assert!(cfg.is_ok(), "preset {preset:?} failed to build: {cfg:?}");
        }
    }
}
