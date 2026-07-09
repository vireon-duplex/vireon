//! [`SchedulerHooksConfig`], [`Preset`] — configuration for scheduler
//! hooks.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The presets encode common deployment patterns:
//!
//! | Preset | max_hooks | enable_pre_tick | enable_post_tick | enable_on_idle | tick_budget_ns |
//! |--------|-----------|-----------------|------------------|----------------|----------------|
//! | `Minimal` | 128 | true | true | false | 1_000_000 |
//! | `Full` | 1024 | true | true | true | 500_000 |

use crate::error::SchedulerHookError;

// ── Constants ─────────────────────────────────────────────────────

/// Minimum number of hooks the registry can store.
pub const MIN_MAX_HOOKS: u32 = 1;

/// Maximum number of hooks the registry can store.
pub const MAX_MAX_HOOKS: u32 = 1024;

/// Minimum tick budget in nanoseconds.
pub const MIN_TICK_BUDGET_NS: u64 = 100;

/// Maximum tick budget in nanoseconds.
pub const MAX_TICK_BUDGET_NS: u64 = 1_000_000;

// ── SchedulerHooksConfig ───────────────────────────────────────────

/// Configuration for the scheduler hooks system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulerHooksConfig {
    /// Maximum number of hooks in the registry.
    pub max_hooks: u32,
    /// Enable pre-tick hooks.
    pub enable_pre_tick: bool,
    /// Enable post-tick hooks.
    pub enable_post_tick: bool,
    /// Enable on-idle hooks.
    pub enable_on_idle: bool,
    /// Tick budget in nanoseconds (time allocated for hooks).
    pub tick_budget_ns: u64,
}

impl SchedulerHooksConfig {
    /// Construct a default config.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_hooks: 128,
            enable_pre_tick: true,
            enable_post_tick: true,
            enable_on_idle: false,
            tick_budget_ns: 1_000_000,
        }
    }

    /// Validate the config.
    ///
    /// # Errors
    /// Returns `SchedulerHookError::InvalidPhase` if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), SchedulerHookError> {
        if self.max_hooks < MIN_MAX_HOOKS || self.max_hooks > MAX_MAX_HOOKS {
            return Err(SchedulerHookError::InvalidPhase("max_hooks out of range"));
        }
        if self.tick_budget_ns < MIN_TICK_BUDGET_NS || self.tick_budget_ns > MAX_TICK_BUDGET_NS {
            return Err(SchedulerHookError::InvalidPhase(
                "tick_budget_ns out of range",
            ));
        }
        Ok(())
    }

    /// Builder-style: set max hooks.
    #[must_use]
    pub const fn with_max_hooks(mut self, n: u32) -> Self {
        self.max_hooks = n;
        self
    }

    /// Builder-style: set enable pre-tick.
    #[must_use]
    pub const fn with_enable_pre_tick(mut self, enable: bool) -> Self {
        self.enable_pre_tick = enable;
        self
    }

    /// Builder-style: set enable post-tick.
    #[must_use]
    pub const fn with_enable_post_tick(mut self, enable: bool) -> Self {
        self.enable_post_tick = enable;
        self
    }

    /// Builder-style: set enable on-idle.
    #[must_use]
    pub const fn with_enable_on_idle(mut self, enable: bool) -> Self {
        self.enable_on_idle = enable;
        self
    }

    /// Builder-style: set tick budget.
    #[must_use]
    pub const fn with_tick_budget_ns(mut self, ns: u64) -> Self {
        self.tick_budget_ns = ns;
        self
    }
}

impl Default for SchedulerHooksConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ───────────────────────────────────────────────────────

/// Deployment presets for scheduler hook configuration.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Minimal configuration: only pre/post tick hooks.
    Minimal,
    /// Full configuration: all hooks enabled.
    Full,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub fn to_config(self) -> SchedulerHooksConfig {
        let base = SchedulerHooksConfig::new();
        match self {
            Self::Minimal => base,
            Self::Full => base
                .with_max_hooks(1024)
                .with_enable_on_idle(true)
                .with_tick_budget_ns(500_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = SchedulerHooksConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_hooks, 128);
        assert!(cfg.enable_pre_tick);
        assert!(cfg.enable_post_tick);
        assert!(!cfg.enable_on_idle);
        assert_eq!(cfg.tick_budget_ns, 1_000_000);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = SchedulerHooksConfig::new()
            .with_max_hooks(256)
            .with_enable_pre_tick(false)
            .with_enable_post_tick(false)
            .with_enable_on_idle(true)
            .with_tick_budget_ns(500_000);
        assert_eq!(cfg.max_hooks, 256);
        assert!(!cfg.enable_pre_tick);
        assert!(!cfg.enable_post_tick);
        assert!(cfg.enable_on_idle);
        assert_eq!(cfg.tick_budget_ns, 500_000);
    }

    #[test]
    fn validate_rejects_bad_max_hooks() {
        let cfg = SchedulerHooksConfig::new().with_max_hooks(0);
        assert!(cfg.validate().is_err());

        let cfg = SchedulerHooksConfig::new().with_max_hooks(MAX_MAX_HOOKS + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_tick_budget() {
        let cfg = SchedulerHooksConfig::new().with_tick_budget_ns(50);
        assert!(cfg.validate().is_err());

        let cfg = SchedulerHooksConfig::new().with_tick_budget_ns(MAX_TICK_BUDGET_NS * 2);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = SchedulerHooksConfig::new()
            .with_max_hooks(MIN_MAX_HOOKS)
            .with_tick_budget_ns(MIN_TICK_BUDGET_NS);
        assert!(cfg.validate().is_ok());

        let cfg = SchedulerHooksConfig::new()
            .with_max_hooks(MAX_MAX_HOOKS)
            .with_tick_budget_ns(MAX_TICK_BUDGET_NS);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn minimal_preset_matches_default() {
        let preset_cfg = Preset::Minimal.to_config();
        let default_cfg = SchedulerHooksConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn full_preset() {
        let cfg = Preset::Full.to_config();
        assert_eq!(cfg.max_hooks, 1024);
        assert!(cfg.enable_on_idle);
        assert_eq!(cfg.tick_budget_ns, 500_000);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Minimal, Preset::Full] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {:?} produced invalid config",
                preset
            );
        }
    }

    #[test]
    fn config_copy_and_clone() {
        let cfg1 = SchedulerHooksConfig::new();
        let cfg2 = cfg1;
        assert_eq!(cfg1, cfg2);
    }

    #[test]
    fn preset_hash() {
        // Ensure Preset derives Hash.
        let _ = std::collections::HashSet::from([Preset::Minimal, Preset::Full]);
    }
}
