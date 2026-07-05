//! [`ReactorBuilder`] — fluent construction.
//!
//! See `DESIGN.md` §1 and the builder pattern used in every other
//! Vireon crate (e.g. `connection_shard::ConnectionShardBuilder`).

use crate::budget::BudgetConfig;
use crate::config::{ReactorConfig, ReactorPreset};
use crate::error::ReactorError;
use crate::idle::IdleStrategy;
use crate::reactor::Reactor;

/// Fluent builder for [`Reactor`].
///
/// ```no_run
/// # use reactor::{ReactorBuilder, ReactorPreset};
/// let _reactor = ReactorBuilder::from_preset(ReactorPreset::BareMetal, 0)
///     .build()
///     .expect("build reactor");
/// ```
// TODO(phase-1): accept a ReactorDriver impl and wire the event loop.
#[derive(Debug)]
pub struct ReactorBuilder {
    config: ReactorConfig,
}

impl ReactorBuilder {
    /// Start from default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: ReactorConfig::default(),
        }
    }

    /// Start from a tuned preset.
    #[must_use]
    pub fn from_preset(preset: ReactorPreset, cpu_id: u32) -> Self {
        Self {
            config: preset.to_config(cpu_id),
        }
    }

    /// Set the pinned CPU core.
    #[must_use]
    pub fn cpu_id(mut self, cpu_id: u32) -> Self {
        self.config.cpu_id = cpu_id;
        self
    }

    /// Set the NUMA node hint.
    #[must_use]
    pub fn numa_node(mut self, numa_node: u32) -> Self {
        self.config.numa_node = numa_node;
        self
    }

    /// Set the io_uring ring depth.
    #[must_use]
    pub fn ring_entries(mut self, entries: u32) -> Self {
        self.config.ring_entries = entries;
        self
    }

    /// Set the provided-buffer ring depth.
    #[must_use]
    pub fn buf_ring_entries(mut self, entries: u32) -> Self {
        self.config.buf_ring_entries = entries;
        self
    }

    /// Enable or disable `IORING_SETUP_SQPOLL`.
    #[must_use]
    pub fn sqpoll(mut self, enabled: bool) -> Self {
        self.config.sqpoll_enabled = enabled;
        self
    }

    /// Enable or disable zero-copy send (`SEND_ZC`).
    #[must_use]
    pub fn zerocopy_send(mut self, enabled: bool) -> Self {
        self.config.enable_zerocopy_send = enabled;
        self
    }

    /// Set the idle strategy.
    #[must_use]
    pub fn idle_strategy(mut self, strategy: IdleStrategy) -> Self {
        self.config.idle_strategy = strategy;
        self
    }

    /// Set the per-tick subsystem budget.
    #[must_use]
    pub fn budget(mut self, budget: BudgetConfig) -> Self {
        self.config.budget = budget;
        self
    }

    /// Consume the builder, validate, and construct the reactor.
    ///
    /// Phase 0 returns a placeholder reactor (no ring, no driver);
    /// Phase 1 will accept a [`ReactorDriver`](crate::driver::ReactorDriver)
    /// and start the real event loop.
    ///
    /// # Errors
    /// Returns [`ReactorError::InvalidConfig`] if any field fails
    /// validation.
    pub fn build(self) -> Result<Reactor, ReactorError> {
        self.config.validate()?;
        Ok(Reactor::placeholder(self.config))
    }
}

impl Default for ReactorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_builds_default() -> Result<(), ReactorError> {
        let r = ReactorBuilder::new().build()?;
        assert_eq!(r.config().ring_entries, crate::config::DEFAULT_RING_ENTRIES);
        Ok(())
    }

    #[test]
    fn preset_builds_valid() -> Result<(), ReactorError> {
        let r = ReactorBuilder::from_preset(ReactorPreset::HundredGbe, 0).build()?;
        assert!(r.config().sqpoll_enabled);
        Ok(())
    }

    #[test]
    fn fluent_setters_apply() -> Result<(), ReactorError> {
        let r = ReactorBuilder::new()
            .cpu_id(3)
            .ring_entries(1024)
            .buf_ring_entries(2048)
            .idle_strategy(IdleStrategy::Spin)
            .build()?;
        assert_eq!(r.config().cpu_id, 3);
        assert_eq!(r.config().ring_entries, 1024);
        assert_eq!(r.config().idle_strategy, IdleStrategy::Spin);
        Ok(())
    }

    #[test]
    fn invalid_config_rejected() {
        let err = ReactorBuilder::new()
            .ring_entries(crate::config::MIN_RING_ENTRIES / 2)
            .build()
            .expect_err("should reject tiny ring");
        assert!(matches!(err, ReactorError::InvalidConfig(_)));
    }
}
