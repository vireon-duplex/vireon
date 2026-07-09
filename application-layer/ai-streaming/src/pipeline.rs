//! [`AiStreamPipeline`] — AI streaming pipeline facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 9 (pipeline).

use crate::config::AiStreamConfig;
use crate::error::AiStreamError;
use crate::health::HealthReport;
use crate::stats::AiStreamStats;
use crate::stream::{AiStreamId, StreamMode};
use crate::token::TokenChunk;
use std::fmt;

/// AI streaming pipeline facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Stream lifecycle management (open/close)
/// - Token chunk routing and delivery
/// - Tool call dispatch
/// - Backpressure and flow control
pub struct AiStreamPipeline {
    /// Pipeline configuration.
    #[allow(dead_code)]
    cfg: AiStreamConfig,
    /// Pipeline statistics.
    stats: AiStreamStats,
}

impl fmt::Debug for AiStreamPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AiStreamPipeline")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl AiStreamPipeline {
    /// Create a new pipeline with the given config.
    #[must_use]
    pub fn new(cfg: AiStreamConfig) -> Self {
        Self {
            cfg,
            stats: AiStreamStats::new(),
        }
    }

    /// Open a new AI stream.
    ///
    /// # Errors
    /// Returns [`AiStreamError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement stream opening with backpressure.
    pub fn open_stream(&self, _mode: StreamMode) -> Result<AiStreamId, AiStreamError> {
        // TODO(phase-1): Implement stream opening.
        Err(AiStreamError::ShuttingDown)
    }

    /// Send a token chunk to a stream.
    ///
    /// # Errors
    /// Returns [`AiStreamError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement token chunk delivery.
    pub fn send_token_chunk(
        &self,
        _stream: AiStreamId,
        _chunk: TokenChunk,
    ) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement token chunk delivery.
        Err(AiStreamError::ShuttingDown)
    }

    /// Close an AI stream.
    ///
    /// # Errors
    /// Returns [`AiStreamError::ShuttingDown`] — skeleton implementation.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    // TODO(phase-1): Implement stream closing.
    pub fn close_stream(&self, _stream: AiStreamId) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement stream closing.
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
    fn pipeline_creation() {
        let cfg = AiStreamConfig::new();
        let pipeline = AiStreamPipeline::new(cfg);
        assert_eq!(pipeline.stats.snapshot().streams_opened, 0);
    }

    #[test]
    fn open_stream_returns_shutting_down() {
        let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let result = pipeline.open_stream(StreamMode::Text);
        assert!(matches!(result, Err(AiStreamError::ShuttingDown)));
    }

    #[test]
    fn send_token_chunk_returns_shutting_down() {
        let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let chunk = TokenChunk::new(1, String::from("hello"));
        let result = pipeline.send_token_chunk(AiStreamId::new(1), chunk);
        assert!(matches!(result, Err(AiStreamError::ShuttingDown)));
    }

    #[test]
    fn close_stream_returns_shutting_down() {
        let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let result = pipeline.close_stream(AiStreamId::new(1));
        assert!(matches!(result, Err(AiStreamError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let health = pipeline.health();
        assert!(health.is_healthy());
        assert_eq!(health.tokens_generated, 0);
    }
}
