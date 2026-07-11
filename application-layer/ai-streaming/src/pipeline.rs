//! [`AiStreamPipeline`] — AI streaming pipeline facade.
//!
//! Phase 1: in-memory stream registry with per-stream token accounting.
//! Production-grade backpressure / flow control lands in a later phase.

use std::collections::HashMap;
use std::fmt;

use crate::config::AiStreamConfig;
use crate::error::AiStreamError;
use crate::health::HealthReport;
use crate::stats::AiStreamStats;
use crate::stream::{AiStreamId, StreamMode};
use crate::token::TokenChunk;

/// Per-stream in-memory state.
struct StreamEntry {
    #[allow(dead_code)]
    mode: StreamMode,
    token_count: u64,
}

/// AI streaming pipeline facade.
///
/// Owns the in-memory stream registry and atomic statistics. All mutating
/// methods take `&mut self` (single-threaded reactor — no interior
/// mutability needed for the registry).
pub struct AiStreamPipeline {
    /// Pipeline configuration.
    #[allow(dead_code)]
    cfg: AiStreamConfig,
    /// Pipeline statistics (atomic — snapshot via `health()`).
    stats: AiStreamStats,
    /// Open streams keyed by id.
    streams: HashMap<AiStreamId, StreamEntry>,
    /// Monotonic stream-id allocator (starts at 1; 0 is reserved nil).
    next_id: u64,
}

impl fmt::Debug for AiStreamPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AiStreamPipeline")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .field("open_streams", &self.streams.len())
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
            streams: HashMap::new(),
            next_id: 1,
        }
    }

    /// Open a new AI stream, allocating an id and registering it.
    ///
    /// # Errors
    /// Returns [`AiStreamError::StreamExists`] only if the id allocator
    /// wraps into a live id (practically never — `u64` ≈ 584 years at
    /// 1 M streams/s).
    pub fn open_stream(&mut self, mode: StreamMode) -> Result<AiStreamId, AiStreamError> {
        let id = AiStreamId::new(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        if self
            .streams
            .insert(
                id,
                StreamEntry {
                    mode,
                    token_count: 0,
                },
            )
            .is_some()
        {
            return Err(AiStreamError::StreamExists(id));
        }
        self.stats.record_stream_opened();
        Ok(id)
    }

    /// Deliver a token chunk to an open stream.
    ///
    /// # Errors
    /// Returns [`AiStreamError::StreamNotFound`] if `stream` is not open.
    pub fn send_token_chunk(
        &mut self,
        stream: AiStreamId,
        chunk: TokenChunk,
    ) -> Result<(), AiStreamError> {
        let entry = self
            .streams
            .get_mut(&stream)
            .ok_or(AiStreamError::StreamNotFound(stream))?;
        entry.token_count += 1;
        self.stats.record_tokens(1);
        if chunk.is_final {
            // Final chunk implicitly closes the stream.
            self.streams.remove(&stream);
            self.stats.record_stream_closed();
        }
        Ok(())
    }

    /// Close an open AI stream.
    ///
    /// # Errors
    /// Returns [`AiStreamError::StreamNotFound`] if `stream` is not open.
    pub fn close_stream(&mut self, stream: AiStreamId) -> Result<(), AiStreamError> {
        if self.streams.remove(&stream).is_none() {
            return Err(AiStreamError::StreamNotFound(stream));
        }
        self.stats.record_stream_closed();
        Ok(())
    }

    /// Returns the number of currently open streams.
    #[must_use]
    pub fn open_stream_count(&self) -> usize {
        self.streams.len()
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
        let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        assert_eq!(pipeline.stats.snapshot().streams_opened, 0);
        assert_eq!(pipeline.open_stream_count(), 0);
    }

    #[test]
    fn open_stream_succeeds_and_allocates_unique_ids() {
        let mut pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let a = pipeline.open_stream(StreamMode::Text).unwrap();
        let b = pipeline.open_stream(StreamMode::Mixed).unwrap();
        assert_ne!(a, b);
        assert_eq!(pipeline.open_stream_count(), 2);
        assert_eq!(pipeline.stats.snapshot().streams_opened, 2);
    }

    #[test]
    fn send_token_chunk_increments_counter() {
        let mut pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let id = pipeline.open_stream(StreamMode::Text).unwrap();
        pipeline
            .send_token_chunk(id, TokenChunk::new(1, String::from("hi")))
            .unwrap();
        pipeline
            .send_token_chunk(id, TokenChunk::new(2, String::from("world")))
            .unwrap();
        assert_eq!(pipeline.stats.snapshot().tokens_generated, 2);
    }

    #[test]
    fn send_token_chunk_unknown_stream_errors() {
        let mut pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let err = pipeline
            .send_token_chunk(AiStreamId::new(99), TokenChunk::new(1, String::from("x")))
            .unwrap_err();
        assert!(matches!(err, AiStreamError::StreamNotFound(_)));
    }

    #[test]
    fn final_token_closes_stream() {
        let mut pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let id = pipeline.open_stream(StreamMode::Text).unwrap();
        pipeline
            .send_token_chunk(id, TokenChunk::new_final(1, String::from("done")))
            .unwrap();
        assert_eq!(pipeline.open_stream_count(), 0);
        assert_eq!(pipeline.stats.snapshot().streams_closed, 1);
    }

    #[test]
    fn close_stream_succeeds() {
        let mut pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let id = pipeline.open_stream(StreamMode::Text).unwrap();
        pipeline.close_stream(id).unwrap();
        assert_eq!(pipeline.open_stream_count(), 0);
        assert_eq!(pipeline.stats.snapshot().streams_closed, 1);
    }

    #[test]
    fn close_stream_unknown_errors() {
        let mut pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        assert!(matches!(
            pipeline.close_stream(AiStreamId::new(42)),
            Err(AiStreamError::StreamNotFound(_))
        ));
    }

    #[test]
    fn health_report_reflects_activity() {
        let mut pipeline = AiStreamPipeline::new(AiStreamConfig::new());
        let id = pipeline.open_stream(StreamMode::Text).unwrap();
        pipeline
            .send_token_chunk(id, TokenChunk::new(1, String::from("tok")))
            .unwrap();
        let health = pipeline.health();
        assert_eq!(health.active_streams, 1);
        assert_eq!(health.tokens_generated, 1);
    }
}
