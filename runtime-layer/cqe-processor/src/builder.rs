//! Fluent builder (Layer 16).

use crate::config::{CqeProcessorConfig, OverflowPolicy};
use crate::error::CqeError;
use crate::processor::CqeProcessor;

/// Fluent builder for [`CqeProcessor`].
#[derive(Default)]
pub struct CqeProcessorBuilder {
    config: CqeProcessorConfig,
}

impl CqeProcessorBuilder {
    /// Creates a builder populated with defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum CQEs drained per `drain_cq()` call. Default: 256.
    #[must_use]
    pub fn batch_max_cqes(mut self, n: usize) -> Self {
        self.config.batch_max_cqes = n;
        self
    }

    /// Consecutive empty-drain threshold for starvation detection.
    /// Default: 1024.
    #[must_use]
    pub fn starvation_threshold_ticks(mut self, n: u32) -> Self {
        self.config.starvation_threshold_ticks = n;
        self
    }

    /// CQ overflow policy. Default: [`OverflowPolicy::Log`].
    #[must_use]
    pub fn on_overflow(mut self, p: OverflowPolicy) -> Self {
        self.config.on_overflow = p;
        self
    }

    /// Buffer-recycle coalesce threshold. Default: 32.
    #[must_use]
    pub fn recycle_coalesce_threshold(mut self, n: usize) -> Self {
        self.config.recycle_coalesce_threshold = n;
        self
    }

    /// Multishot re-arm cooldown in ticks. Default: 4.
    #[must_use]
    pub fn multishot_rearm_cooldown(mut self, n: u32) -> Self {
        self.config.multishot_rearm_cooldown = n;
        self
    }

    /// Reactor / core ID for diagnostics. Default: 0.
    #[must_use]
    pub fn reactor_id(mut self, id: u32) -> Self {
        self.config.reactor_id = id;
        self
    }

    /// Validates the configuration and constructs the [`CqeProcessor`].
    pub fn build(self) -> Result<CqeProcessor, CqeError> {
        CqeProcessor::from_config(self.config)
    }
}

impl std::fmt::Debug for CqeProcessorBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqeProcessorBuilder")
            .field("batch_max_cqes", &self.config.batch_max_cqes)
            .field(
                "starvation_threshold_ticks",
                &self.config.starvation_threshold_ticks,
            )
            .field("on_overflow", &self.config.on_overflow)
            .field("reactor_id", &self.config.reactor_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        DEFAULT_BATCH_MAX_CQES, DEFAULT_MULTISHOT_REARM_COOLDOWN, DEFAULT_RECYCLE_THRESHOLD,
        DEFAULT_STARVATION_THRESHOLD,
    };

    #[test]
    fn default_build_succeeds() {
        assert!(CqeProcessorBuilder::new().build().is_ok());
    }

    #[test]
    fn rejects_too_small_batch() {
        let err = CqeProcessorBuilder::new()
            .batch_max_cqes(4)
            .build()
            .unwrap_err();
        assert!(matches!(err, CqeError::InvalidConfig(_)));
    }

    #[test]
    fn rejects_too_large_batch() {
        let err = CqeProcessorBuilder::new()
            .batch_max_cqes(8192)
            .build()
            .unwrap_err();
        assert!(matches!(err, CqeError::InvalidConfig(_)));
    }

    #[test]
    fn custom_settings_apply() {
        let p = CqeProcessorBuilder::new()
            .batch_max_cqes(128)
            .reactor_id(7)
            .on_overflow(OverflowPolicy::Shutdown)
            .build()
            .expect("build");
        assert_eq!(p.batch_max_cqes(), 128);
        assert_eq!(p.reactor_id(), 7);
    }

    #[test]
    fn defaults_match_constants() {
        let cfg = CqeProcessorConfig::default();
        assert_eq!(cfg.batch_max_cqes, DEFAULT_BATCH_MAX_CQES);
        assert_eq!(cfg.starvation_threshold_ticks, DEFAULT_STARVATION_THRESHOLD);
        assert_eq!(cfg.recycle_coalesce_threshold, DEFAULT_RECYCLE_THRESHOLD);
        assert_eq!(
            cfg.multishot_rearm_cooldown,
            DEFAULT_MULTISHOT_REARM_COOLDOWN
        );
    }
}
