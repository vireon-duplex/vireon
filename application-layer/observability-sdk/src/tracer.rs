//! [`Tracer`] — skeleton for distributed tracing functionality.
//!
//! See `DESIGN.md` Part 11 (tracer).
//!
//! # Phase 1
//!
//! This module ships as a typed skeleton. Functional tracer lands in
//! Phase 1.

use crate::SpanKind;
use crate::config::ObservabilityConfig;
use crate::error::ObservabilityError;
use crate::stats::ObservabilityStats;
use crate::trace::SpanId;

/// Distributed tracer (skeleton).
///
/// In Phase 1, this will manage span lifecycle and trace propagation.
/// Currently returns placeholder errors.
#[derive(Debug)]
pub struct Tracer {
    #[allow(dead_code)]
    cfg: ObservabilityConfig,
    #[allow(dead_code)]
    stats: ObservabilityStats,
}

impl Tracer {
    /// Create a new tracer.
    pub fn new(cfg: ObservabilityConfig, stats: ObservabilityStats) -> Self {
        Self { cfg, stats }
    }

    /// Open a new span.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn span_open(
        &mut self,
        _name_hash: u64,
        _kind: SpanKind,
    ) -> Result<SpanId, ObservabilityError> {
        Err(ObservabilityError::ShuttingDown)
    }

    /// Close an existing span.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::ShuttingDown`] unconditionally
    /// in this skeleton.
    pub fn span_close(&mut self, _id: SpanId) -> Result<(), ObservabilityError> {
        Err(ObservabilityError::ShuttingDown)
    }

    /// Get the current health state.
    #[must_use]
    pub fn health(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tracer_created() {
        let cfg = ObservabilityConfig::new();
        let stats = ObservabilityStats::new();
        let tracer = Tracer::new(cfg, stats);
        assert!(tracer.health());
    }

    #[test]
    fn span_open_returns_shutting_down() {
        let mut tracer = Tracer::new(ObservabilityConfig::new(), ObservabilityStats::new());
        assert!(matches!(
            tracer.span_open(123, SpanKind::Server),
            Err(ObservabilityError::ShuttingDown)
        ));
    }

    #[test]
    fn span_close_returns_shutting_down() {
        let mut tracer = Tracer::new(ObservabilityConfig::new(), ObservabilityStats::new());
        assert!(matches!(
            tracer.span_close(SpanId::new(1)),
            Err(ObservabilityError::ShuttingDown)
        ));
    }
}
