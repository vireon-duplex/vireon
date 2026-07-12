//! [`TokenAggregator`] — token aggregation facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 10 (aggregator).

use crate::config::AiStreamConfig;
use crate::error::AiStreamError;
use crate::health::HealthReport;
use crate::stats::AiStreamStats;
use crate::stream::AiStreamId;
use crate::token::TokenChunk;
use std::fmt;

/// Token aggregator facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Token accumulation and reassembly
/// - Tool call argument aggregation
/// - Buffered delivery with flush semantics
/// - Stream-level completion detection
pub struct TokenAggregator {
    /// Aggregator configuration.
    #[allow(dead_code)]
    cfg: AiStreamConfig,
    /// Aggregator statistics.
    stats: AiStreamStats,
}

impl fmt::Debug for TokenAggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenAggregator")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl TokenAggregator {
    /// Create a new token aggregator with the given config.
    #[must_use]
    pub fn new(cfg: AiStreamConfig) -> Self {
        Self {
            cfg,
            stats: AiStreamStats::new(),
        }
    }

    /// Aggregate a token chunk into a stream's buffer.
    ///
    /// # Errors
    /// Returns [`AiStreamError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement token aggregation.
    pub fn aggregate(
        &self,
        _stream: AiStreamId,
        _chunk: TokenChunk<'_>,
    ) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement token aggregation.
        Err(AiStreamError::ShuttingDown)
    }

    /// Flush aggregated tokens for a stream.
    ///
    /// # Errors
    /// Returns [`AiStreamError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement flush.
    pub fn flush(&self, _stream: AiStreamId) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement flush.
        Err(AiStreamError::ShuttingDown)
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregator_creation() {
        let cfg = AiStreamConfig::new();
        let agg = TokenAggregator::new(cfg);
        assert_eq!(agg.stats.snapshot().tokens_generated, 0);
    }

    #[test]
    fn aggregate_returns_shutting_down() {
        let agg = TokenAggregator::new(AiStreamConfig::new());
        let chunk = TokenChunk::new(1, String::from("hello"));
        let result = agg.aggregate(AiStreamId::new(1), chunk);
        assert!(matches!(result, Err(AiStreamError::ShuttingDown)));
    }

    #[test]
    fn flush_returns_shutting_down() {
        let agg = TokenAggregator::new(AiStreamConfig::new());
        let result = agg.flush(AiStreamId::new(1));
        assert!(matches!(result, Err(AiStreamError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let agg = TokenAggregator::new(AiStreamConfig::new());
        let health = agg.health();
        assert!(health.is_healthy());
        assert_eq!(health.tokens_generated, 0);
    }
}
