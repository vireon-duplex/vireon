//! [`GracefulConfig`], [`Preset`] — declarative configuration for
//! the graceful startup and shutdown system.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for different
//! operational requirements:
//!
//! | Preset | Timeout | Force | Health Checks | Use Case |
//!--------|---------|-------|----------------|----------|
//! | `Fast` | Short | Yes | No | Development / Testing |
//! | `Standard` | Moderate | No | No | Production default |
//! | `Safe` | Long | No | Yes | Critical production |

use crate::error::ShutdownError;
use crate::phase::StartupPhase;

// ── Constants ───────────────────────────────────────────────────

/// Minimum timeout for any phase (milliseconds).
pub const MIN_TIMEOUT_MS: u64 = 1_000;

/// Maximum timeout for any phase (milliseconds).
pub const MAX_TIMEOUT_MS: u64 = 300_000;

// ── GracefulConfig ──────────────────────────────────────────────

/// Declarative configuration for the graceful startup and shutdown system.
///
/// Constructed via [`GracefulConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GracefulConfig {
    /// Startup timeout in milliseconds.
    pub startup_timeout_ms: u64,
    /// Shutdown timeout in milliseconds.
    pub shutdown_timeout_ms: u64,
    /// Connection draining timeout in milliseconds.
    pub drain_timeout_ms: u64,
    /// Phase at which to begin accepting connections.
    pub accept_connections_at_phase: StartupPhase,
    /// Whether health checks are enabled during startup.
    pub enable_health_checks: bool,
    /// Whether to force shutdown on timeout.
    pub force_on_timeout: bool,
}

impl GracefulConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 30s startup, 30s shutdown, 10s drain, accept at
    /// `Ready`, health checks disabled, no force on timeout.
    pub fn new() -> Self {
        Self {
            startup_timeout_ms: 30_000,
            shutdown_timeout_ms: 30_000,
            drain_timeout_ms: 10_000,
            accept_connections_at_phase: StartupPhase::Ready,
            enable_health_checks: false,
            force_on_timeout: false,
        }
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`ShutdownError::InvalidConfig`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), ShutdownError> {
        if self.startup_timeout_ms < MIN_TIMEOUT_MS || self.startup_timeout_ms > MAX_TIMEOUT_MS {
            return Err(ShutdownError::InvalidPhase(
                "startup_timeout_ms out of range",
            ));
        }
        if self.shutdown_timeout_ms < MIN_TIMEOUT_MS || self.shutdown_timeout_ms > MAX_TIMEOUT_MS {
            return Err(ShutdownError::InvalidPhase(
                "shutdown_timeout_ms out of range",
            ));
        }
        if self.drain_timeout_ms < MIN_TIMEOUT_MS || self.drain_timeout_ms > MAX_TIMEOUT_MS {
            return Err(ShutdownError::InvalidPhase("drain_timeout_ms out of range"));
        }
        Ok(())
    }

    /// Builder-style: set the startup timeout.
    #[must_use]
    pub const fn with_startup_timeout_ms(mut self, ms: u64) -> Self {
        self.startup_timeout_ms = ms;
        self
    }

    /// Builder-style: set the shutdown timeout.
    #[must_use]
    pub const fn with_shutdown_timeout_ms(mut self, ms: u64) -> Self {
        self.shutdown_timeout_ms = ms;
        self
    }

    /// Builder-style: set the drain timeout.
    #[must_use]
    pub const fn with_drain_timeout_ms(mut self, ms: u64) -> Self {
        self.drain_timeout_ms = ms;
        self
    }

    /// Builder-style: set the connection acceptance phase.
    #[must_use]
    pub const fn with_accept_connections_at_phase(mut self, phase: StartupPhase) -> Self {
        self.accept_connections_at_phase = phase;
        self
    }

    /// Builder-style: enable or disable health checks.
    #[must_use]
    pub const fn with_enable_health_checks(mut self, enable: bool) -> Self {
        self.enable_health_checks = enable;
        self
    }

    /// Builder-style: enable or disable force on timeout.
    #[must_use]
    pub const fn with_force_on_timeout(mut self, force: bool) -> Self {
        self.force_on_timeout = force;
        self
    }
}

impl Default for GracefulConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ─────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// operational profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Fast startup/shutdown for development and testing.
    Fast,
    /// Standard production configuration.
    Standard,
    /// Safe mode with long timeouts and health checks.
    Safe,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> GracefulConfig {
        match self {
            Self::Fast => GracefulConfig {
                startup_timeout_ms: 10_000,
                shutdown_timeout_ms: 10_000,
                drain_timeout_ms: 5_000,
                accept_connections_at_phase: StartupPhase::Ready,
                enable_health_checks: false,
                force_on_timeout: true,
            },
            Self::Standard => GracefulConfig {
                startup_timeout_ms: 30_000,
                shutdown_timeout_ms: 30_000,
                drain_timeout_ms: 10_000,
                accept_connections_at_phase: StartupPhase::Ready,
                enable_health_checks: false,
                force_on_timeout: false,
            },
            Self::Safe => GracefulConfig {
                startup_timeout_ms: 60_000,
                shutdown_timeout_ms: 60_000,
                drain_timeout_ms: 30_000,
                accept_connections_at_phase: StartupPhase::Ready,
                enable_health_checks: true,
                force_on_timeout: false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = GracefulConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.startup_timeout_ms, 30_000);
        assert_eq!(cfg.shutdown_timeout_ms, 30_000);
        assert_eq!(cfg.drain_timeout_ms, 10_000);
        assert!(!cfg.enable_health_checks);
        assert!(!cfg.force_on_timeout);
    }

    #[test]
    fn new_matches_default() {
        let new_cfg = GracefulConfig::new();
        let default_cfg = GracefulConfig::default();
        assert_eq!(new_cfg, default_cfg);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = GracefulConfig::new()
            .with_startup_timeout_ms(45_000)
            .with_shutdown_timeout_ms(60_000)
            .with_drain_timeout_ms(15_000)
            .with_enable_health_checks(true)
            .with_force_on_timeout(true);
        assert_eq!(cfg.startup_timeout_ms, 45_000);
        assert_eq!(cfg.shutdown_timeout_ms, 60_000);
        assert_eq!(cfg.drain_timeout_ms, 15_000);
        assert!(cfg.enable_health_checks);
        assert!(cfg.force_on_timeout);
    }

    #[test]
    fn validate_rejects_startup_too_small() {
        let cfg = GracefulConfig::new().with_startup_timeout_ms(500);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_startup_too_large() {
        let cfg = GracefulConfig::new().with_startup_timeout_ms(400_000);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_shutdown_too_small() {
        let cfg = GracefulConfig::new().with_shutdown_timeout_ms(500);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_drain_too_small() {
        let cfg = GracefulConfig::new().with_drain_timeout_ms(500);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = GracefulConfig::new()
            .with_startup_timeout_ms(MIN_TIMEOUT_MS)
            .with_shutdown_timeout_ms(MIN_TIMEOUT_MS)
            .with_drain_timeout_ms(MIN_TIMEOUT_MS);
        assert!(cfg.validate().is_ok());

        let cfg = GracefulConfig::new()
            .with_startup_timeout_ms(MAX_TIMEOUT_MS)
            .with_shutdown_timeout_ms(MAX_TIMEOUT_MS)
            .with_drain_timeout_ms(MAX_TIMEOUT_MS);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn fast_preset_uses_short_timeouts() {
        let cfg = Preset::Fast.to_config();
        assert_eq!(cfg.startup_timeout_ms, 10_000);
        assert_eq!(cfg.shutdown_timeout_ms, 10_000);
        assert_eq!(cfg.drain_timeout_ms, 5_000);
        assert!(cfg.force_on_timeout);
        assert!(!cfg.enable_health_checks);
    }

    #[test]
    fn standard_preset_uses_moderate_timeouts() {
        let cfg = Preset::Standard.to_config();
        assert_eq!(cfg.startup_timeout_ms, 30_000);
        assert_eq!(cfg.shutdown_timeout_ms, 30_000);
        assert_eq!(cfg.drain_timeout_ms, 10_000);
        assert!(!cfg.force_on_timeout);
        assert!(!cfg.enable_health_checks);
    }

    #[test]
    fn safe_preset_uses_long_timeouts_and_health_checks() {
        let cfg = Preset::Safe.to_config();
        assert_eq!(cfg.startup_timeout_ms, 60_000);
        assert_eq!(cfg.shutdown_timeout_ms, 60_000);
        assert_eq!(cfg.drain_timeout_ms, 30_000);
        assert!(!cfg.force_on_timeout);
        assert!(cfg.enable_health_checks);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Fast, Preset::Standard, Preset::Safe] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn accept_connections_at_phase_defaults_to_ready() {
        let cfg = GracefulConfig::default();
        assert_eq!(cfg.accept_connections_at_phase, StartupPhase::Ready);
    }

    #[test]
    fn builder_sets_accept_connections_phase() {
        let cfg = GracefulConfig::new().with_accept_connections_at_phase(StartupPhase::BindPorts);
        assert_eq!(cfg.accept_connections_at_phase, StartupPhase::BindPorts);
    }
}
