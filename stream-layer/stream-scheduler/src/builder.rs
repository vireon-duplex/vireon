//! [`SchedulerBuilder`] — fluent construction for
//! [`StreamScheduler`](crate::scheduler::StreamScheduler).
//!
//! See `DESIGN.md` Part 1 (responsibilities).

use crate::config::{Preset, SchedulerConfig};
use crate::error::SchedulerError;
use crate::policy::SchedulingPolicy;
use crate::scheduler::StreamScheduler;

/// Fluent builder for the scheduler.
#[derive(Clone, Debug)]
pub struct SchedulerBuilder {
    cfg: SchedulerConfig,
}

impl Default for SchedulerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: SchedulerConfig::default(),
        }
    }

    /// Start a new builder from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Set the scheduling policy.
    #[must_use]
    pub const fn policy(mut self, p: SchedulingPolicy) -> Self {
        self.cfg.policy = p;
        self
    }

    /// Set the maximum streams per tick.
    #[must_use]
    pub const fn max_streams_per_tick(mut self, n: u32) -> Self {
        self.cfg.max_streams_per_tick = n;
        self
    }

    /// Set the maximum batch size.
    #[must_use]
    pub const fn batch_size_max(mut self, n: u32) -> Self {
        self.cfg.batch_size_max = n;
        self
    }

    /// Set the aging granularity (microseconds).
    #[must_use]
    pub const fn aging_granularity_us(mut self, us: u64) -> Self {
        self.cfg.aging_granularity_us = us;
        self
    }

    /// Set the starvation threshold (microseconds).
    #[must_use]
    pub const fn starvation_threshold_us(mut self, us: u64) -> Self {
        self.cfg.starvation_threshold_us = us;
        self
    }

    /// Set the incremental share percentage.
    #[must_use]
    pub const fn incremental_share_pct(mut self, pct: u8) -> Self {
        self.cfg.incremental_share_pct = pct;
        self
    }

    /// Inspect the config the builder has assembled.
    #[must_use]
    pub const fn config(&self) -> SchedulerConfig {
        self.cfg
    }

    /// Phase-1 `build()` placeholder.
    ///
    /// # Errors
    /// - [`SchedulerError::InvalidConfig`] if validation fails.
    /// - Phase 0 always returns `Err` after validation (no
    ///   scheduler implementation yet).
    pub fn build(self) -> Result<StreamScheduler, SchedulerError> {
        self.cfg.validate()?;
        // TODO(phase-1): construct StreamScheduler { queue, selector, ... }.
        StreamScheduler::new(self.cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_uses_default_config() {
        let b = SchedulerBuilder::new();
        assert_eq!(b.config(), SchedulerConfig::default());
    }

    #[test]
    fn preset_builder_materialises() {
        let b = SchedulerBuilder::from_preset(Preset::LatencyFirst);
        assert_eq!(b.config().policy, SchedulingPolicy::Hybrid);
    }

    #[test]
    fn setters_take_effect() {
        let b = SchedulerBuilder::new()
            .policy(SchedulingPolicy::StrictPriority)
            .max_streams_per_tick(100);
        let cfg = b.config();
        assert_eq!(cfg.policy, SchedulingPolicy::StrictPriority);
        assert_eq!(cfg.max_streams_per_tick, 100);
    }

    #[test]
    fn build_validates_before_succeeding() {
        let r = SchedulerBuilder::new().build();
        assert!(r.is_ok());
    }

    #[test]
    fn build_rejects_invalid_config() {
        let r = SchedulerBuilder::new().max_streams_per_tick(0).build();
        assert!(matches!(r, Err(SchedulerError::InvalidConfig(_))));
    }
}
