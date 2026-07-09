//! [`AiStreamConfig`], [`Preset`] — declarative configuration for
//! the AI streaming framework.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes studied in the
//! LLM streaming literature:
//!
//! | Preset | Max Streams | Max Tokens | Chunk Size | Tool Timeout |
//! |--------|-------------|------------|------------|--------------|
//! | `Chat` | 128 | 4_096 | 64 | 5_000 ms |
//! | `CodeAssistant` | 64 | 16_384 | 128 | 15_000 ms |
//! | `Agent` | 32 | 32_768 | 256 | 60_000 ms |

use crate::error::AiStreamError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of concurrent streams.
pub const MIN_MAX_CONCURRENT_STREAMS: u32 = 1;

/// Maximum number of concurrent streams.
pub const MAX_MAX_CONCURRENT_STREAMS: u32 = 10_000;

/// Minimum tokens per stream.
pub const MIN_MAX_TOKENS_PER_STREAM: u64 = 128;

/// Maximum tokens per stream.
pub const MAX_MAX_TOKENS_PER_STREAM: u64 = 1_000_000;

/// Minimum token chunk size.
pub const MIN_TOKEN_CHUNK_SIZE: u32 = 1;

/// Maximum token chunk size.
pub const MAX_TOKEN_CHUNK_SIZE: u32 = 10_000;

/// Minimum tool call timeout in milliseconds.
pub const MIN_TOOL_CALL_TIMEOUT_MS: u64 = 100;

/// Maximum tool call timeout in milliseconds (10 minutes).
pub const MAX_TOOL_CALL_TIMEOUT_MS: u64 = 600_000;

// ── AiStreamConfig ──────────────────────────────────────────────

/// Declarative configuration for the AI streaming framework.
///
/// Constructed via [`AiStreamConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiStreamConfig {
    /// Maximum number of concurrent AI streams.
    pub max_concurrent_streams: u32,
    /// Maximum tokens per stream.
    pub max_tokens_per_stream: u64,
    /// Number of tokens per chunk for streaming.
    pub token_chunk_size: u32,
    /// Timeout for tool calls in milliseconds.
    pub tool_call_timeout_ms: u64,
}

impl AiStreamConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 128 streams, 4_096 tokens, chunk size 64,
    /// 5_000 ms tool timeout.
    pub fn new() -> Self {
        Self {
            max_concurrent_streams: 128,
            max_tokens_per_stream: 4_096,
            token_chunk_size: 64,
            tool_call_timeout_ms: 5_000,
        }
    }

    /// Builder-style: set the max concurrent streams.
    #[must_use]
    pub const fn with_max_concurrent_streams(mut self, streams: u32) -> Self {
        self.max_concurrent_streams = streams;
        self
    }

    /// Builder-style: set the max tokens per stream.
    #[must_use]
    pub const fn with_max_tokens_per_stream(mut self, tokens: u64) -> Self {
        self.max_tokens_per_stream = tokens;
        self
    }

    /// Builder-style: set the token chunk size.
    #[must_use]
    pub const fn with_token_chunk_size(mut self, size: u32) -> Self {
        self.token_chunk_size = size;
        self
    }

    /// Builder-style: set the tool call timeout (ms).
    #[must_use]
    pub const fn with_tool_call_timeout_ms(mut self, ms: u64) -> Self {
        self.tool_call_timeout_ms = ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`AiStreamError::TokenFailed`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), AiStreamError> {
        if self.max_concurrent_streams < MIN_MAX_CONCURRENT_STREAMS
            || self.max_concurrent_streams > MAX_MAX_CONCURRENT_STREAMS
        {
            return Err(AiStreamError::TokenFailed(
                "max_concurrent_streams out of range",
            ));
        }
        if self.max_tokens_per_stream < MIN_MAX_TOKENS_PER_STREAM
            || self.max_tokens_per_stream > MAX_MAX_TOKENS_PER_STREAM
        {
            return Err(AiStreamError::TokenFailed(
                "max_tokens_per_stream out of range",
            ));
        }
        if self.token_chunk_size < MIN_TOKEN_CHUNK_SIZE
            || self.token_chunk_size > MAX_TOKEN_CHUNK_SIZE
        {
            return Err(AiStreamError::TokenFailed("token_chunk_size out of range"));
        }
        if self.tool_call_timeout_ms < MIN_TOOL_CALL_TIMEOUT_MS
            || self.tool_call_timeout_ms > MAX_TOOL_CALL_TIMEOUT_MS
        {
            return Err(AiStreamError::TokenFailed(
                "tool_call_timeout_ms out of range",
            ));
        }
        Ok(())
    }
}

impl Default for AiStreamConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// AI workload profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Chat: moderate concurrency, short responses, fast tool calls.
    Chat,
    /// Code assistant: fewer streams, longer responses, larger chunks.
    CodeAssistant,
    /// Agent: fewest streams, longest responses, generous tool timeouts.
    Agent,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub fn to_config(self) -> AiStreamConfig {
        match self {
            Self::Chat => AiStreamConfig::new(), // Defaults match Chat
            Self::CodeAssistant => AiStreamConfig::new()
                .with_max_concurrent_streams(64)
                .with_max_tokens_per_stream(16_384)
                .with_token_chunk_size(128)
                .with_tool_call_timeout_ms(15_000),
            Self::Agent => AiStreamConfig::new()
                .with_max_concurrent_streams(32)
                .with_max_tokens_per_stream(32_768)
                .with_token_chunk_size(256)
                .with_tool_call_timeout_ms(60_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = AiStreamConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_concurrent_streams, 128);
        assert_eq!(cfg.max_tokens_per_stream, 4_096);
        assert_eq!(cfg.token_chunk_size, 64);
        assert_eq!(cfg.tool_call_timeout_ms, 5_000);
    }

    #[test]
    fn new_matches_default() {
        let cfg = AiStreamConfig::new();
        let default = AiStreamConfig::default();
        assert_eq!(cfg, default);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = AiStreamConfig::new()
            .with_max_concurrent_streams(256)
            .with_max_tokens_per_stream(8_192)
            .with_token_chunk_size(32)
            .with_tool_call_timeout_ms(10_000);
        assert_eq!(cfg.max_concurrent_streams, 256);
        assert_eq!(cfg.max_tokens_per_stream, 8_192);
        assert_eq!(cfg.token_chunk_size, 32);
        assert_eq!(cfg.tool_call_timeout_ms, 10_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_concurrent_streams() {
        let mut cfg = AiStreamConfig::default();
        cfg.max_concurrent_streams = 0;
        assert!(cfg.validate().is_err());
        cfg.max_concurrent_streams = MAX_MAX_CONCURRENT_STREAMS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_tokens() {
        let mut cfg = AiStreamConfig::default();
        cfg.max_tokens_per_stream = MIN_MAX_TOKENS_PER_STREAM / 2;
        assert!(cfg.validate().is_err());
        cfg.max_tokens_per_stream = MAX_MAX_TOKENS_PER_STREAM + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_chunk_size() {
        let mut cfg = AiStreamConfig::default();
        cfg.token_chunk_size = 0;
        assert!(cfg.validate().is_err());
        cfg.token_chunk_size = MAX_TOKEN_CHUNK_SIZE + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_tool_timeout() {
        let mut cfg = AiStreamConfig::default();
        cfg.tool_call_timeout_ms = MIN_TOOL_CALL_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.tool_call_timeout_ms = MAX_TOOL_CALL_TIMEOUT_MS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn chat_preset_matches_default() {
        let preset_cfg = Preset::Chat.to_config();
        let default_cfg = AiStreamConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn code_assistant_preset_config() {
        let cfg = Preset::CodeAssistant.to_config();
        assert_eq!(cfg.max_concurrent_streams, 64);
        assert_eq!(cfg.max_tokens_per_stream, 16_384);
        assert_eq!(cfg.token_chunk_size, 128);
        assert_eq!(cfg.tool_call_timeout_ms, 15_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn agent_preset_config() {
        let cfg = Preset::Agent.to_config();
        assert_eq!(cfg.max_concurrent_streams, 32);
        assert_eq!(cfg.max_tokens_per_stream, 32_768);
        assert_eq!(cfg.token_chunk_size, 256);
        assert_eq!(cfg.tool_call_timeout_ms, 60_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Chat, Preset::CodeAssistant, Preset::Agent] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn builder_overrides_preset() {
        let cfg = Preset::Chat
            .to_config()
            .with_max_concurrent_streams(50)
            .with_tool_call_timeout_ms(30_000);
        assert_eq!(cfg.max_concurrent_streams, 50);
        assert_eq!(cfg.tool_call_timeout_ms, 30_000);
        assert_eq!(cfg.max_tokens_per_stream, 4_096); // from preset
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = AiStreamConfig::new()
            .with_max_concurrent_streams(MIN_MAX_CONCURRENT_STREAMS)
            .with_max_tokens_per_stream(MIN_MAX_TOKENS_PER_STREAM)
            .with_token_chunk_size(MIN_TOKEN_CHUNK_SIZE)
            .with_tool_call_timeout_ms(MIN_TOOL_CALL_TIMEOUT_MS);
        assert!(cfg.validate().is_ok());

        let cfg = AiStreamConfig::new()
            .with_max_concurrent_streams(MAX_MAX_CONCURRENT_STREAMS)
            .with_max_tokens_per_stream(MAX_MAX_TOKENS_PER_STREAM)
            .with_token_chunk_size(MAX_TOKEN_CHUNK_SIZE)
            .with_tool_call_timeout_ms(MAX_TOOL_CALL_TIMEOUT_MS);
        assert!(cfg.validate().is_ok());
    }
}
