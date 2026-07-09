//! [`Pipeline`] — the main pipeline facade.
//!
//! See `DESIGN.md` Part 1 (architecture), Part 8 (pipeline).
//!
//! # Role
//!
//! `Pipeline` is the single entry point that callers use to
//! execute middleware chains. It composes:
//!
//! - a [`MiddlewareChain`] (registration),
//! - execution context tracking,
//! - a [`MiddlewareStats`] (observability).
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub error so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::config::MiddlewareConfig;
use crate::context::MiddlewareContext;
use crate::error::MiddlewareError;
use crate::health::HealthReport;
use crate::stats::MiddlewareStats;

/// The middleware pipeline facade.
///
/// Composes the middleware chain and stats. Phase 0 ships a typed
/// skeleton; Phase 1+ populates real logic.
#[derive(Debug)]
pub struct Pipeline {
    /// Static configuration.
    #[allow(dead_code)]
    cfg: MiddlewareConfig,
    /// Atomic observability counters.
    stats: MiddlewareStats,
}

impl Pipeline {
    /// Construct a pipeline from validated configuration.
    ///
    /// # Errors
    /// Returns [`MiddlewareError::InvalidOrder`] if the config fails
    /// validation.
    pub fn new(cfg: MiddlewareConfig) -> Result<Self, MiddlewareError> {
        cfg.validate()?;
        Ok(Self {
            cfg,
            stats: MiddlewareStats::new(),
        })
    }

    /// Execute the middleware chain with a context.
    ///
    /// # Errors
    /// Always returns an error in Phase 0.
    pub fn execute(
        &mut self,
        _ctx: MiddlewareContext,
    ) -> Result<MiddlewareContext, MiddlewareError> {
        // TODO(phase-1): execute chain through stages.
        self.stats.record_execution();
        Err(MiddlewareError::ShuttingDown)
    }

    /// Snapshot the pipeline's health.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }

    /// Reference to the underlying stats (for testing / metrics).
    #[must_use]
    pub fn stats(&self) -> &MiddlewareStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MiddlewareStage;
    use crate::config::Preset;

    fn make_pipeline() -> Pipeline {
        let cfg = Preset::Standard.to_config();
        Pipeline::new(cfg).unwrap()
    }

    #[test]
    fn pipeline_constructs_from_valid_config() {
        let pipeline = make_pipeline();
        let h = pipeline.health();
        assert!(h.is_healthy());
    }

    #[test]
    fn pipeline_rejects_invalid_config() {
        let mut cfg = Preset::Standard.to_config();
        cfg.max_middlewares = 0; // invalid
        assert!(Pipeline::new(cfg).is_err());
    }

    #[test]
    fn execute_phase0_stub() {
        let mut pipeline = make_pipeline();
        let ctx = MiddlewareContext::new(MiddlewareStage::PreAuth);
        assert!(pipeline.execute(ctx).is_err());
    }
}
