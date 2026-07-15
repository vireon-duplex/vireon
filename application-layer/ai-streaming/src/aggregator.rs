//! [`TokenAggregator`] — token accumulation and reassembly.
//!
//! Buffers incoming token chunks per stream and provides flush semantics
//! to drain accumulated text. When a chunk arrives with `is_final = true`,
//! the aggregator marks the stream as complete.
//!
//! ```text
//! aggregate(stream=1, chunk="Hello ")  → buffer = "Hello "
//! aggregate(stream=1, chunk="World")   → buffer = "Hello World"
//! flush(stream=1)                      → returns "Hello World", clears buffer
//! ```

use crate::config::AiStreamConfig;
use crate::error::AiStreamError;
use crate::health::HealthReport;
use crate::stats::AiStreamStats;
use crate::stream::AiStreamId;
use crate::token::TokenChunk;
use rustc_hash::FxHashMap;
use std::fmt;

/// Per-stream aggregation state.
struct StreamBuffer {
    text: String,
    final_received: bool,
}

impl StreamBuffer {
    fn new() -> Self {
        Self {
            text: String::new(),
            final_received: false,
        }
    }
}

/// Token aggregator with per-stream buffering.
///
/// Accumulates token chunks into a per-stream `String` buffer.
/// `flush` drains and returns the accumulated text.
/// `take` is an alias that also returns the text.
pub struct TokenAggregator {
    #[allow(dead_code)]
    cfg: AiStreamConfig,
    stats: AiStreamStats,
    buffers: FxHashMap<AiStreamId, StreamBuffer>,
}

impl fmt::Debug for TokenAggregator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenAggregator")
            .field("cfg", &self.cfg)
            .field("active_buffers", &self.buffers.len())
            .finish()
    }
}

impl TokenAggregator {
    /// Creates a new token aggregator with the given config.
    #[must_use]
    pub fn new(cfg: AiStreamConfig) -> Self {
        Self {
            cfg,
            stats: AiStreamStats::new(),
            buffers: FxHashMap::default(),
        }
    }

    /// Aggregates a token chunk into a stream's buffer.
    ///
    /// If the chunk has `is_final = true`, the stream is marked as
    /// complete. Subsequent chunks to a completed stream are silently
    /// ignored.
    ///
    /// # Errors
    /// Returns [`AiStreamError::TokenFailed`] if `stream` is nil.
    pub fn aggregate(
        &mut self,
        stream: AiStreamId,
        chunk: TokenChunk<'_>,
    ) -> Result<(), AiStreamError> {
        if stream.is_nil() {
            return Err(AiStreamError::TokenFailed("nil stream id"));
        }

        let buf = self.buffers.entry(stream).or_insert_with(StreamBuffer::new);

        if buf.final_received {
            // Stream already completed — ignore late chunks.
            return Ok(());
        }

        buf.text.push_str(&chunk.text);
        if chunk.is_final {
            buf.final_received = true;
        }

        self.stats.record_tokens(1);
        Ok(())
    }

    /// Flushes aggregated tokens for a stream.
    ///
    /// Returns the accumulated text and clears the buffer. If the stream
    /// has no buffered data, returns an empty `String`.
    ///
    /// # Errors
    /// Returns [`AiStreamError::StreamNotFound`] if `stream` was never
    /// aggregated to.
    pub fn flush(&mut self, stream: AiStreamId) -> Result<String, AiStreamError> {
        match self.buffers.get_mut(&stream) {
            Some(buf) => {
                let text = std::mem::take(&mut buf.text);
                Ok(text)
            }
            None => Err(AiStreamError::StreamNotFound(stream)),
        }
    }

    /// Returns `true` if the stream has received its final chunk.
    #[must_use]
    pub fn is_complete(&self, stream: AiStreamId) -> bool {
        self.buffers.get(&stream).is_some_and(|b| b.final_received)
    }

    /// Returns the number of active stream buffers.
    #[must_use]
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Removes a stream's buffer entirely (e.g. on stream close).
    pub fn remove(&mut self, stream: AiStreamId) {
        self.buffers.remove(&stream);
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregator_creation() {
        let agg = TokenAggregator::new(AiStreamConfig::new());
        assert_eq!(agg.buffer_count(), 0);
    }

    #[test]
    fn aggregate_and_flush() {
        let mut agg = TokenAggregator::new(AiStreamConfig::new());
        let id = AiStreamId::new(1);

        agg.aggregate(id, TokenChunk::new(1, "Hello ")).unwrap();
        agg.aggregate(id, TokenChunk::new(2, "World")).unwrap();

        let text = agg.flush(id).unwrap();
        assert_eq!(text, "Hello World");

        // Buffer is cleared after flush.
        let text2 = agg.flush(id).unwrap();
        assert_eq!(text2, "");
    }

    #[test]
    fn aggregate_with_final_marks_complete() {
        let mut agg = TokenAggregator::new(AiStreamConfig::new());
        let id = AiStreamId::new(1);

        agg.aggregate(id, TokenChunk::new_final(1, "done")).unwrap();
        assert!(agg.is_complete(id));
    }

    #[test]
    fn late_chunks_after_final_ignored() {
        let mut agg = TokenAggregator::new(AiStreamConfig::new());
        let id = AiStreamId::new(1);

        agg.aggregate(id, TokenChunk::new_final(1, "done")).unwrap();
        agg.aggregate(id, TokenChunk::new(2, "late")).unwrap();

        let text = agg.flush(id).unwrap();
        assert_eq!(text, "done");
    }

    #[test]
    fn flush_unknown_stream_errors() {
        let mut agg = TokenAggregator::new(AiStreamConfig::new());
        let result = agg.flush(AiStreamId::new(99));
        assert!(matches!(result, Err(AiStreamError::StreamNotFound(_))));
    }

    #[test]
    fn multiple_streams_independent() {
        let mut agg = TokenAggregator::new(AiStreamConfig::new());
        let a = AiStreamId::new(1);
        let b = AiStreamId::new(2);

        agg.aggregate(a, TokenChunk::new(1, "AAA")).unwrap();
        agg.aggregate(b, TokenChunk::new(1, "BBB")).unwrap();
        agg.aggregate(a, TokenChunk::new(2, "aaa")).unwrap();

        assert_eq!(agg.flush(a).unwrap(), "AAAaaa");
        assert_eq!(agg.flush(b).unwrap(), "BBB");
    }

    #[test]
    fn nil_stream_rejected() {
        let mut agg = TokenAggregator::new(AiStreamConfig::new());
        let result = agg.aggregate(AiStreamId::NIL, TokenChunk::new(1, "x"));
        assert!(matches!(result, Err(AiStreamError::TokenFailed(_))));
    }

    #[test]
    fn remove_clears_stream() {
        let mut agg = TokenAggregator::new(AiStreamConfig::new());
        let id = AiStreamId::new(1);
        agg.aggregate(id, TokenChunk::new(1, "data")).unwrap();
        assert_eq!(agg.buffer_count(), 1);

        agg.remove(id);
        assert_eq!(agg.buffer_count(), 0);
    }

    #[test]
    fn health_report_initially_healthy() {
        let agg = TokenAggregator::new(AiStreamConfig::new());
        let health = agg.health();
        assert!(health.is_healthy());
    }
}
