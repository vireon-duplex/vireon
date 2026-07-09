//! [`RpcConfig`], [`Preset`] — declarative configuration for
//! the RPC framework.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes studied in the
//! gRPC / HTTP/2 literature:
//!
//! | Preset | Streams | Timeout | Message Size | Retries |
//! |--------|---------|---------|--------------|---------|
//! | `LowLatency` | 32 | 5s | 1 MB | 2 |
//! | `Balanced` | 64 | 30s | 4 MB | 3 |
//! | `Throughput` | 256 | 60s | 16 MB | 5 |

use crate::error::RpcError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of concurrent streams the framework will accept.
pub const MIN_CONCURRENT_STREAMS: u32 = 1;

/// Maximum number of concurrent streams the framework will accept.
pub const MAX_CONCURRENT_STREAMS: u32 = 10_000;

/// Minimum default timeout in milliseconds.
pub const MIN_TIMEOUT_MS: u64 = 100;

/// Maximum default timeout in milliseconds.
pub const MAX_TIMEOUT_MS: u64 = 600_000; // 10 minutes

/// Minimum maximum message size in bytes.
pub const MIN_MESSAGE_BYTES: u64 = 1_024; // 1 KB

/// Maximum maximum message size in bytes.
pub const MAX_MESSAGE_BYTES: u64 = 100_000_000; // 100 MB

/// Minimum number of retries for retryable errors.
pub const MIN_RETRIES: u8 = 0;

/// Maximum number of retries for retryable errors.
pub const MAX_RETRIES: u8 = 10;

// ── RpcConfig ────────────────────────────────────────────────────

/// Declarative configuration for the RPC framework.
///
/// Constructed via [`RpcConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcConfig {
    /// Maximum concurrent streams per connection.
    pub max_concurrent_streams: u32,
    /// Default timeout for RPC calls in milliseconds.
    pub default_timeout_ms: u64,
    /// Maximum message size in bytes.
    pub max_message_bytes: u64,
    /// Whether to enable compression for payloads.
    pub enable_compression: bool,
    /// Maximum number of retries for retryable errors.
    pub max_retries: u8,
}

impl RpcConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 64 concurrent streams, 30s timeout, 4 MB message size,
    /// compression enabled, 3 retries.
    pub fn new() -> Self {
        Self {
            max_concurrent_streams: 64,
            default_timeout_ms: 30_000,
            max_message_bytes: 4_000_000,
            enable_compression: true,
            max_retries: 3,
        }
    }

    /// Builder-style: set the max concurrent streams.
    #[must_use]
    pub const fn with_max_concurrent_streams(mut self, streams: u32) -> Self {
        self.max_concurrent_streams = streams;
        self
    }

    /// Builder-style: set the default timeout (ms).
    #[must_use]
    pub const fn with_default_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.default_timeout_ms = timeout_ms;
        self
    }

    /// Builder-style: set the max message bytes.
    #[must_use]
    pub const fn with_max_message_bytes(mut self, bytes: u64) -> Self {
        self.max_message_bytes = bytes;
        self
    }

    /// Builder-style: enable or disable compression.
    #[must_use]
    pub const fn with_compression(mut self, enable: bool) -> Self {
        self.enable_compression = enable;
        self
    }

    /// Builder-style: set the max retries.
    #[must_use]
    pub const fn with_max_retries(mut self, retries: u8) -> Self {
        self.max_retries = retries;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`RpcError::InvalidRequest`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), RpcError> {
        if self.max_concurrent_streams < MIN_CONCURRENT_STREAMS
            || self.max_concurrent_streams > MAX_CONCURRENT_STREAMS
        {
            return Err(RpcError::InvalidRequest(
                "max_concurrent_streams out of range",
            ));
        }
        if self.default_timeout_ms < MIN_TIMEOUT_MS || self.default_timeout_ms > MAX_TIMEOUT_MS {
            return Err(RpcError::InvalidRequest("default_timeout_ms out of range"));
        }
        if self.max_message_bytes < MIN_MESSAGE_BYTES || self.max_message_bytes > MAX_MESSAGE_BYTES
        {
            return Err(RpcError::InvalidRequest("max_message_bytes out of range"));
        }
        if self.max_retries > MAX_RETRIES {
            return Err(RpcError::InvalidRequest("max_retries out of range"));
        }
        Ok(())
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// workload profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Low-latency configuration: fewer streams, short timeout, small messages.
    LowLatency,
    /// Balanced configuration: moderate settings for general use.
    Balanced,
    /// High-throughput configuration: many streams, long timeout, large messages.
    Throughput,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub fn to_config(self) -> RpcConfig {
        match self {
            Self::LowLatency => RpcConfig::new()
                .with_max_concurrent_streams(32)
                .with_default_timeout_ms(5_000)
                .with_max_message_bytes(1_000_000)
                .with_max_retries(2),
            Self::Balanced => RpcConfig::new(), // Defaults match balanced
            Self::Throughput => RpcConfig::new()
                .with_max_concurrent_streams(256)
                .with_default_timeout_ms(60_000)
                .with_max_message_bytes(16_000_000)
                .with_max_retries(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = RpcConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_concurrent_streams, 64);
        assert_eq!(cfg.default_timeout_ms, 30_000);
        assert_eq!(cfg.max_message_bytes, 4_000_000);
        assert!(cfg.enable_compression);
        assert_eq!(cfg.max_retries, 3);
    }

    #[test]
    fn new_matches_default() {
        let cfg = RpcConfig::new();
        let default = RpcConfig::default();
        assert_eq!(cfg, default);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = RpcConfig::new()
            .with_max_concurrent_streams(128)
            .with_default_timeout_ms(10_000)
            .with_max_message_bytes(8_000_000)
            .with_compression(false)
            .with_max_retries(5);
        assert_eq!(cfg.max_concurrent_streams, 128);
        assert_eq!(cfg.default_timeout_ms, 10_000);
        assert_eq!(cfg.max_message_bytes, 8_000_000);
        assert!(!cfg.enable_compression);
        assert_eq!(cfg.max_retries, 5);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_stream_count() {
        let mut cfg = RpcConfig::default();
        cfg.max_concurrent_streams = 0;
        assert!(cfg.validate().is_err());
        cfg.max_concurrent_streams = MAX_CONCURRENT_STREAMS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_timeout() {
        let mut cfg = RpcConfig::default();
        cfg.default_timeout_ms = MIN_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.default_timeout_ms = MAX_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_message_size() {
        let mut cfg = RpcConfig::default();
        cfg.max_message_bytes = MIN_MESSAGE_BYTES / 2;
        assert!(cfg.validate().is_err());
        cfg.max_message_bytes = MAX_MESSAGE_BYTES * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_retries() {
        let mut cfg = RpcConfig::default();
        cfg.max_retries = MAX_RETRIES + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn low_latency_preset_config() {
        let cfg = Preset::LowLatency.to_config();
        assert_eq!(cfg.max_concurrent_streams, 32);
        assert_eq!(cfg.default_timeout_ms, 5_000);
        assert_eq!(cfg.max_message_bytes, 1_000_000);
        assert_eq!(cfg.max_retries, 2);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn balanced_preset_matches_default() {
        let preset_cfg = Preset::Balanced.to_config();
        let default_cfg = RpcConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn throughput_preset_config() {
        let cfg = Preset::Throughput.to_config();
        assert_eq!(cfg.max_concurrent_streams, 256);
        assert_eq!(cfg.default_timeout_ms, 60_000);
        assert_eq!(cfg.max_message_bytes, 16_000_000);
        assert_eq!(cfg.max_retries, 5);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::LowLatency, Preset::Balanced, Preset::Throughput] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn builder_overrides_preset() {
        let cfg = Preset::LowLatency
            .to_config()
            .with_default_timeout_ms(15_000)
            .with_max_retries(4);
        assert_eq!(cfg.default_timeout_ms, 15_000);
        assert_eq!(cfg.max_retries, 4);
        assert_eq!(cfg.max_concurrent_streams, 32); // from preset
    }

    #[test]
    fn compression_toggle() {
        let cfg = RpcConfig::new().with_compression(false);
        assert!(!cfg.enable_compression);
        assert!(cfg.validate().is_ok());

        let cfg = cfg.with_compression(true);
        assert!(cfg.enable_compression);
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = RpcConfig::new()
            .with_max_concurrent_streams(MIN_CONCURRENT_STREAMS)
            .with_default_timeout_ms(MIN_TIMEOUT_MS)
            .with_max_message_bytes(MIN_MESSAGE_BYTES)
            .with_max_retries(MIN_RETRIES);
        assert!(cfg.validate().is_ok());

        let cfg = RpcConfig::new()
            .with_max_concurrent_streams(MAX_CONCURRENT_STREAMS)
            .with_default_timeout_ms(MAX_TIMEOUT_MS)
            .with_max_message_bytes(MAX_MESSAGE_BYTES)
            .with_max_retries(MAX_RETRIES);
        assert!(cfg.validate().is_ok());
    }
}
