//! [`SchedulerHooksBuilder`] — fluent builder for scheduler hook config.
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `SchedulerHooksConfig` has multiple fields and most callers only need
//! to override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching every
//! field.

use crate::config::{Preset, SchedulerHooksConfig};

/// Fluent builder for [`SchedulerHooksConfig`].
#[derive(Clone, Debug)]
pub struct SchedulerHooksBuilder {
    cfg: SchedulerHooksConfig,
}

impl SchedulerHooksBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: SchedulerHooksConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max hooks.
    #[must_use]
    pub const fn max_hooks(mut self, n: u32) -> Self {
        self.cfg.max_hooks = n;
        self
    }

    /// Enable or disable pre-tick hooks.
    #[must_use]
    pub const fn enable_pre_tick(mut self, enable: bool) -> Self {
        self.cfg.enable_pre_tick = enable;
        self
    }

    /// Enable or disable post-tick hooks.
    #[must_use]
    pub const fn enable_post_tick(mut self, enable: bool) -> Self {
        self.cfg.enable_post_tick = enable;
        self
    }

    /// Enable or disable on-idle hooks.
    #[must_use]
    pub const fn enable_on_idle(mut self, enable: bool) -> Self {
        self.cfg.enable_on_idle = enable;
        self
    }

    /// Set the tick budget (nanoseconds).
    #[must_use]
    pub const fn tick_budget_ns(mut self, ns: u64) -> Self {
        self.cfg.tick_budget_ns = ns;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns `SchedulerHookError::InvalidPhase` if any field is out
    /// of range.
    pub fn build(self) -> Result<SchedulerHooksConfig, crate::error::SchedulerHookError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for SchedulerHooksBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MAX_MAX_HOOKS, MAX_TICK_BUDGET_NS, MIN_MAX_HOOKS, MIN_TICK_BUDGET_NS};

    #[test]
    fn builder_from_default() {
        let builder = SchedulerHooksBuilder::new();
        let cfg = builder.build().unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn builder_from_preset() {
        let builder = SchedulerHooksBuilder::from_preset(Preset::Full);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_hooks, 1024);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = SchedulerHooksBuilder::from_preset(Preset::Minimal)
            .max_hooks(512)
            .enable_pre_tick(false)
            .enable_post_tick(false)
            .enable_on_idle(true)
            .tick_budget_ns(250_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_hooks, 512);
        assert!(!cfg.enable_pre_tick);
        assert!(!cfg.enable_post_tick);
        assert!(cfg.enable_on_idle);
        assert_eq!(cfg.tick_budget_ns, 250_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = SchedulerHooksBuilder::new()
            .max_hooks(0) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = SchedulerHooksBuilder::new()
            .max_hooks(MIN_MAX_HOOKS)
            .tick_budget_ns(MIN_TICK_BUDGET_NS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_hooks, MIN_MAX_HOOKS);
        assert_eq!(cfg.tick_budget_ns, MIN_TICK_BUDGET_NS);

        let cfg = SchedulerHooksBuilder::new()
            .max_hooks(MAX_MAX_HOOKS)
            .tick_budget_ns(MAX_TICK_BUDGET_NS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_hooks, MAX_MAX_HOOKS);
        assert_eq!(cfg.tick_budget_ns, MAX_TICK_BUDGET_NS);
    }

    #[test]
    fn builder_preserves_preset_overrides() {
        let cfg = SchedulerHooksBuilder::from_preset(Preset::Full)
            .max_hooks(256) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_hooks, 256);
        assert!(cfg.enable_on_idle); // from preset
        assert_eq!(cfg.tick_budget_ns, 500_000); // from preset
    }

    #[test]
    fn builder_default_impl() {
        let builder = SchedulerHooksBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_hooks, 128);
    }

    #[test]
    fn builder_clone() {
        let builder1 = SchedulerHooksBuilder::new();
        let builder2 = builder1.clone();
        let cfg1 = builder1.build().unwrap();
        let cfg2 = builder2.build().unwrap();
        assert_eq!(cfg1, cfg2);
    }
}
