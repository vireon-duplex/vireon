//! [`RpcFrameworkBuilder`] — fluent builder for [`crate::RpcConfig`]
//! and the RPC framework itself.
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `RpcConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{Preset, RpcConfig};
use crate::error::RpcError;

/// Fluent builder for [`RpcConfig`].
#[derive(Debug, Clone)]
pub struct RpcFrameworkBuilder {
    cfg: RpcConfig,
}

impl RpcFrameworkBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: RpcConfig::new(),
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

    /// Override the default timeout (ms).
    #[must_use]
    pub fn default_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.cfg = self.cfg.with_default_timeout_ms(timeout_ms);
        self
    }

    /// Override the max message bytes.
    #[must_use]
    pub fn max_message_bytes(mut self, bytes: u64) -> Self {
        self.cfg = self.cfg.with_max_message_bytes(bytes);
        self
    }

    /// Enable or disable compression.
    #[must_use]
    pub fn compression(mut self, enable: bool) -> Self {
        self.cfg = self.cfg.with_compression(enable);
        self
    }

    /// Override the max retries.
    #[must_use]
    pub fn max_retries(mut self, retries: u8) -> Self {
        self.cfg = self.cfg.with_max_retries(retries);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`RpcError::InvalidRequest`] if any field is out
    /// of range.
    pub fn build(self) -> Result<RpcConfig, RpcError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for RpcFrameworkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_CONCURRENT_STREAMS, MAX_MESSAGE_BYTES, MAX_RETRIES, MAX_TIMEOUT_MS,
        MIN_CONCURRENT_STREAMS, MIN_MESSAGE_BYTES, MIN_RETRIES, MIN_TIMEOUT_MS,
    };

    #[test]
    fn builder_new_has_defaults() {
        let builder = RpcFrameworkBuilder::new();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_concurrent_streams, 64);
        assert_eq!(cfg.default_timeout_ms, 30_000);
        assert_eq!(cfg.max_message_bytes, 4_000_000);
        assert!(cfg.enable_compression);
        assert_eq!(cfg.max_retries, 3);
    }

    #[test]
    fn builder_from_preset() {
        let builder = RpcFrameworkBuilder::from_preset(Preset::LowLatency);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_concurrent_streams, 32);
        assert_eq!(cfg.default_timeout_ms, 5_000);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = RpcFrameworkBuilder::new()
            .max_concurrent_streams(128)
            .default_timeout_ms(10_000)
            .max_message_bytes(8_000_000)
            .compression(false)
            .max_retries(5)
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_streams, 128);
        assert_eq!(cfg.default_timeout_ms, 10_000);
        assert_eq!(cfg.max_message_bytes, 8_000_000);
        assert!(!cfg.enable_compression);
        assert_eq!(cfg.max_retries, 5);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = RpcFrameworkBuilder::new()
            .default_timeout_ms(MIN_TIMEOUT_MS / 2) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = RpcFrameworkBuilder::new()
            .max_concurrent_streams(MIN_CONCURRENT_STREAMS)
            .default_timeout_ms(MIN_TIMEOUT_MS)
            .max_message_bytes(MIN_MESSAGE_BYTES)
            .max_retries(MIN_RETRIES)
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_streams, MIN_CONCURRENT_STREAMS);
        assert_eq!(cfg.default_timeout_ms, MIN_TIMEOUT_MS);
        assert_eq!(cfg.max_message_bytes, MIN_MESSAGE_BYTES);
        assert_eq!(cfg.max_retries, MIN_RETRIES);

        let cfg = RpcFrameworkBuilder::new()
            .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
            .default_timeout_ms(MAX_TIMEOUT_MS)
            .max_message_bytes(MAX_MESSAGE_BYTES)
            .max_retries(MAX_RETRIES)
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_streams, MAX_CONCURRENT_STREAMS);
        assert_eq!(cfg.default_timeout_ms, MAX_TIMEOUT_MS);
        assert_eq!(cfg.max_message_bytes, MAX_MESSAGE_BYTES);
        assert_eq!(cfg.max_retries, MAX_RETRIES);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = RpcFrameworkBuilder::from_preset(Preset::Throughput)
            .default_timeout_ms(90_000) // override
            .build()
            .unwrap();
        assert_eq!(cfg.default_timeout_ms, 90_000);
        assert_eq!(cfg.max_concurrent_streams, 256); // from preset
    }

    #[test]
    fn builder_compression_toggle() {
        let cfg = RpcFrameworkBuilder::new()
            .compression(false)
            .build()
            .unwrap();
        assert!(!cfg.enable_compression);

        let cfg = RpcFrameworkBuilder::new()
            .compression(true)
            .build()
            .unwrap();
        assert!(cfg.enable_compression);
    }

    #[test]
    fn builder_default_is_default_trait() {
        let builder = RpcFrameworkBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg, RpcConfig::default());
    }

    #[test]
    fn builder_clone_produces_independent() {
        let b1 = RpcFrameworkBuilder::new().max_concurrent_streams(100);
        let b2 = b1.clone();
        let cfg1 = b1.build().unwrap();
        let cfg2 = b2.max_concurrent_streams(200).build().unwrap();
        assert_eq!(cfg1.max_concurrent_streams, 100);
        assert_eq!(cfg2.max_concurrent_streams, 200);
    }
}
