//! [`StreamManagerBuilder`] — fluent construction for
//! [`StreamManager`](crate::manager::StreamManager).
//!
//! See `DESIGN.md` Part 1 (responsibilities) and Part 9 (lifecycle).
//!
//! # Phase 0 status
//!
//! Phase 0 ships the builder shape; functional `build()` (which
//! validates config, allocates the slab, and constructs the
//! manager) lands in Phase 1 once
//! [`crate::manager::StreamManager`] is real.

use crate::config::{Preset, StreamManagerConfig};
use crate::error::StreamManagerError;

/// Fluent builder for the stream manager.
#[derive(Clone, Debug)]
pub struct StreamManagerBuilder {
    cfg: StreamManagerConfig,
}

impl Default for StreamManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamManagerBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: StreamManagerConfig::default(),
        }
    }

    /// Start a new builder from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Set the maximum concurrent bidirectional streams.
    #[must_use]
    pub const fn max_streams_bidi(mut self, n: u64) -> Self {
        self.cfg.max_streams_bidi = n;
        self
    }

    /// Set the maximum concurrent unidirectional streams.
    #[must_use]
    pub const fn max_streams_uni(mut self, n: u64) -> Self {
        self.cfg.max_streams_uni = n;
        self
    }

    /// Set the initial bidi-local window.
    #[must_use]
    pub const fn initial_max_stream_data_bidi_local(mut self, n: u64) -> Self {
        self.cfg.initial_max_stream_data_bidi_local = n;
        self
    }

    /// Set the initial bidi-remote window.
    #[must_use]
    pub const fn initial_max_stream_data_bidi_remote(mut self, n: u64) -> Self {
        self.cfg.initial_max_stream_data_bidi_remote = n;
        self
    }

    /// Set the initial uni window.
    #[must_use]
    pub const fn initial_max_stream_data_uni(mut self, n: u64) -> Self {
        self.cfg.initial_max_stream_data_uni = n;
        self
    }

    /// Set the default priority for new streams.
    #[must_use]
    pub const fn default_priority(mut self, p: crate::priority::StreamPriority) -> Self {
        self.cfg.default_priority = p;
        self
    }

    /// Set the per-stream idle timeout in ms.
    #[must_use]
    pub const fn stream_idle_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.stream_idle_timeout_ms = ms;
        self
    }

    /// Set the slab chunk size (streams per chunk).
    #[must_use]
    pub const fn slab_chunk_streams(mut self, n: u32) -> Self {
        self.cfg.slab_chunk_streams = n;
        self
    }

    /// Inspect the config the builder has assembled.
    #[must_use]
    pub const fn config(&self) -> StreamManagerConfig {
        self.cfg
    }

    /// Phase-1 `build()` placeholder.
    ///
    /// # Errors
    /// - [`StreamManagerError::InvalidConfig`] if validation fails.
    /// - Phase 0 always returns `Err(InvalidConfig)` after
    ///   validation (no manager implementation yet).
    pub fn build(self) -> Result<(), StreamManagerError> {
        self.cfg.validate()?;
        // TODO(phase-1): construct StreamManager { table, allocator, ... }.
        Err(StreamManagerError::ResourceExhausted(
            "StreamManager phase-1 stub",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_uses_default_config() {
        let b = StreamManagerBuilder::new();
        assert_eq!(b.config(), StreamManagerConfig::default());
    }

    #[test]
    fn preset_builder_materialises() {
        let b = StreamManagerBuilder::from_preset(Preset::PubSub);
        assert_eq!(b.config().max_streams_bidi, 10_000);
    }

    #[test]
    fn setters_take_effect() {
        let b = StreamManagerBuilder::new()
            .max_streams_bidi(500)
            .slab_chunk_streams(2048);
        let cfg = b.config();
        assert_eq!(cfg.max_streams_bidi, 500);
        assert_eq!(cfg.slab_chunk_streams, 2048);
    }

    #[test]
    fn build_validates_before_failing() {
        // valid config — should pass validate(), then return the
        // phase-1 stub error.
        let r = StreamManagerBuilder::new().build();
        assert!(matches!(r, Err(StreamManagerError::ResourceExhausted(_))));
    }

    #[test]
    fn build_rejects_invalid_config() {
        let r = StreamManagerBuilder::new().max_streams_bidi(0).build();
        assert!(matches!(r, Err(StreamManagerError::InvalidConfig(_))));
    }
}
