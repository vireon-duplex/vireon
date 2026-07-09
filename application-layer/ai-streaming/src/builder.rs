//! [`AiStreamBuilder`] — fluent builder for [`AiStreamConfig`]
//! and the AI streaming framework.
//!
//! See `DESIGN.md` Part 13 (configuration).

use crate::config::{AiStreamConfig, Preset};
use crate::error::AiStreamError;

/// Fluent builder for [`AiStreamConfig`].
#[derive(Debug, Clone)]
pub struct AiStreamBuilder {
    cfg: AiStreamConfig,
}

impl AiStreamBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: AiStreamConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max concurrent streams.
    #[must_use]
    pub fn max_concurrent_streams(mut self, streams: u32) -> Self {
        self.cfg = self.cfg.with_max_concurrent_streams(streams);
        self
    }

    /// Override the max tokens per stream.
    #[must_use]
    pub fn max_tokens_per_stream(mut self, tokens: u64) -> Self {
        self.cfg = self.cfg.with_max_tokens_per_stream(tokens);
        self
    }

    /// Override the token chunk size.
    #[must_use]
    pub fn token_chunk_size(mut self, size: u32) -> Self {
        self.cfg = self.cfg.with_token_chunk_size(size);
        self
    }

    /// Override the tool call timeout (ms).
    #[must_use]
    pub fn tool_call_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg = self.cfg.with_tool_call_timeout_ms(ms);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`AiStreamError::TokenFailed`] if any field is out
    /// of range.
    pub fn build(self) -> Result<AiStreamConfig, AiStreamError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for AiStreamBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_MAX_CONCURRENT_STREAMS, MAX_MAX_TOKENS_PER_STREAM, MAX_TOKEN_CHUNK_SIZE,
        MAX_TOOL_CALL_TIMEOUT_MS, MIN_MAX_CONCURRENT_STREAMS, MIN_MAX_TOKENS_PER_STREAM,
        MIN_TOKEN_CHUNK_SIZE, MIN_TOOL_CALL_TIMEOUT_MS,
    };

    #[test]
    fn builder_new_has_defaults() {
        let builder = AiStreamBuilder::new();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_concurrent_streams, 128);
        assert_eq!(cfg.max_tokens_per_stream, 4_096);
        assert_eq!(cfg.token_chunk_size, 64);
        assert_eq!(cfg.tool_call_timeout_ms, 5_000);
    }

    #[test]
    fn builder_from_preset() {
        let builder = AiStreamBuilder::from_preset(Preset::Agent);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_concurrent_streams, 32);
        assert_eq!(cfg.max_tokens_per_stream, 32_768);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = AiStreamBuilder::new()
            .max_concurrent_streams(256)
            .max_tokens_per_stream(8_192)
            .token_chunk_size(32)
            .tool_call_timeout_ms(10_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_streams, 256);
        assert_eq!(cfg.max_tokens_per_stream, 8_192);
        assert_eq!(cfg.token_chunk_size, 32);
        assert_eq!(cfg.tool_call_timeout_ms, 10_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = AiStreamBuilder::new()
            .max_concurrent_streams(0) // invalid
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = AiStreamBuilder::new()
            .max_concurrent_streams(MIN_MAX_CONCURRENT_STREAMS)
            .max_tokens_per_stream(MIN_MAX_TOKENS_PER_STREAM)
            .token_chunk_size(MIN_TOKEN_CHUNK_SIZE)
            .tool_call_timeout_ms(MIN_TOOL_CALL_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_streams, MIN_MAX_CONCURRENT_STREAMS);
        assert_eq!(cfg.max_tokens_per_stream, MIN_MAX_TOKENS_PER_STREAM);
        assert_eq!(cfg.token_chunk_size, MIN_TOKEN_CHUNK_SIZE);
        assert_eq!(cfg.tool_call_timeout_ms, MIN_TOOL_CALL_TIMEOUT_MS);

        let cfg = AiStreamBuilder::new()
            .max_concurrent_streams(MAX_MAX_CONCURRENT_STREAMS)
            .max_tokens_per_stream(MAX_MAX_TOKENS_PER_STREAM)
            .token_chunk_size(MAX_TOKEN_CHUNK_SIZE)
            .tool_call_timeout_ms(MAX_TOOL_CALL_TIMEOUT_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_streams, MAX_MAX_CONCURRENT_STREAMS);
        assert_eq!(cfg.max_tokens_per_stream, MAX_MAX_TOKENS_PER_STREAM);
        assert_eq!(cfg.token_chunk_size, MAX_TOKEN_CHUNK_SIZE);
        assert_eq!(cfg.tool_call_timeout_ms, MAX_TOOL_CALL_TIMEOUT_MS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = AiStreamBuilder::from_preset(Preset::CodeAssistant)
            .max_concurrent_streams(100) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_streams, 100);
        assert_eq!(cfg.max_tokens_per_stream, 16_384); // from preset
    }

    #[test]
    fn builder_default_is_default_trait() {
        let builder = AiStreamBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg, AiStreamConfig::default());
    }

    #[test]
    fn builder_clone_produces_independent() {
        let b1 = AiStreamBuilder::new().max_concurrent_streams(100);
        let b2 = b1.clone();
        let cfg1 = b1.build().unwrap();
        let cfg2 = b2.max_concurrent_streams(200).build().unwrap();
        assert_eq!(cfg1.max_concurrent_streams, 100);
        assert_eq!(cfg2.max_concurrent_streams, 200);
    }
}
