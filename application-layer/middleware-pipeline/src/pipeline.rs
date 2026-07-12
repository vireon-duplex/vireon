//! [`Pipeline`] — the main pipeline facade.
//!
//! Phase 1: passthrough chain execution with registration tracking.
//! Real middleware callbacks (with trait objects) are Phase 2.
//!
//! The pipeline validates `MiddlewareContext` abort flags and records
//! stats. It serves as the integration point for cross-cutting concerns
//! (logging, validation, rate-limiting) that will be populated as the
//! middleware trait ecosystem matures.

use crate::config::MiddlewareConfig;
use crate::context::MiddlewareContext;
use crate::error::MiddlewareError;
use crate::health::HealthReport;
use crate::stats::MiddlewareStats;

/// The middleware pipeline facade.
///
/// Composes the middleware chain and stats. Phase 1 implements
/// passthrough execution with abort detection. Phase 2+ will add
/// real middleware callbacks.
#[derive(Debug)]
pub struct Pipeline {
    #[allow(dead_code)]
    cfg: MiddlewareConfig,
    stats: MiddlewareStats,
    /// Number of registered middleware entries (Phase 1: count only).
    registered_count: u32,
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
            registered_count: 0,
        })
    }

    /// Execute the middleware chain with a context.
    ///
    /// Phase 1 behavior:
    /// - If `ctx.is_aborted()`, returns `Err(Rejected)` with stats.
    /// - Otherwise, passes through with stats recording.
    ///
    /// Phase 2+ will execute real middleware callbacks in order.
    ///
    /// # Errors
    /// Returns [`MiddlewareError::Rejected`] if the context was
    /// pre-aborted.
    pub fn execute(
        &mut self,
        ctx: MiddlewareContext,
    ) -> Result<MiddlewareContext, MiddlewareError> {
        self.stats.record_execution();

        if ctx.is_aborted() {
            self.stats.record_rejected();
            return Err(MiddlewareError::Rejected("context pre-aborted"));
        }

        // Phase 1: passthrough — no real middleware to execute.
        // Phase 2: iterate registered callbacks here, checking abort
        // after each step.
        self.stats.record_success();
        Ok(ctx)
    }

    /// Increment the registered middleware count.
    ///
    /// Phase 2 will replace this with real `MiddlewareEntry` storage.
    pub fn register(&mut self) {
        self.registered_count = self.registered_count.saturating_add(1);
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

    /// Number of registered middleware entries.
    #[must_use]
    pub fn registered_count(&self) -> u32 {
        self.registered_count
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
        assert!(pipeline.health().is_healthy());
    }

    #[test]
    fn pipeline_rejects_invalid_config() {
        let mut cfg = Preset::Standard.to_config();
        cfg.max_middlewares = 0; // invalid
        assert!(Pipeline::new(cfg).is_err());
    }

    #[test]
    fn execute_passthrough_returns_ctx() {
        let mut pipeline = make_pipeline();
        let ctx = MiddlewareContext::new(MiddlewareStage::PreAuth);
        let result = pipeline.execute(ctx).unwrap();
        assert!(!result.is_aborted());
    }

    #[test]
    fn execute_aborted_context_returns_rejected() {
        let mut pipeline = make_pipeline();
        let ctx = MiddlewareContext::new(MiddlewareStage::PreAuth).with_abort();
        assert!(matches!(
            pipeline.execute(ctx),
            Err(MiddlewareError::Rejected(_))
        ));
    }

    #[test]
    fn register_increments_count() {
        let mut pipeline = make_pipeline();
        assert_eq!(pipeline.registered_count(), 0);
        pipeline.register();
        pipeline.register();
        assert_eq!(pipeline.registered_count(), 2);
    }
}
