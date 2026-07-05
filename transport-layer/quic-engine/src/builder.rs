//! [`QuicEngineBuilder`] — fluent builder + `from_preset`.
//!
//! Phase 13 — Runtime Integration. See `DESIGN.md` §4.13.
//!
//! Mirrors the pattern of
//! [`connection_shard::ConnectionShardBuilder`](../../connection-shard/src/builder.rs).
//!
//! ## Target API (Phase 13+)
//!
//! ```no_run
//! use quic_engine::{QuicEngineBuilder, QuicPreset};
//!
//! let mut engine = QuicEngineBuilder::from_preset(QuicPreset::BareMetal)
//!     .reactor_id(0)
//!     .max_connections(8192)
//!     .build()
//!     .expect("engine");
//! ```

use crate::config::{Preset, QuicConfig};
use crate::engine::QuicEngine;
use crate::error::QuicError;

/// Phase-0 skeleton. Phase 13 will add the full fluent surface
/// (numa_node, cert_chain, key, alpn, etc.).
#[derive(Debug, Default)]
pub struct QuicEngineBuilder {
    reactor_id: Option<u32>,
    config: QuicConfig,
}

impl QuicEngineBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            reactor_id: None,
            config: QuicConfig {
                // Use `Default::default()` semantics via `new` to
                // keep the const-eval happy; the actual `Default`
                // implementation fills in the rest.
                max_connections: 0, // replaced by `default()` call in `from_preset`
                max_streams_bidi: 0,
                max_streams_uni: 0,
                initial_max_data: 0,
                initial_max_stream_data: 0,
                idle_timeout: std::time::Duration::ZERO,
                handshake_timeout: std::time::Duration::ZERO,
                congestion: crate::config::CongestionAlgorithm::Bbr,
                initial_congestion_window_packets: 0,
                extensions: crate::config::ExtensionFlags {
                    datagram: false,
                    ack_frequency: false,
                    multipath: false,
                    bbr2: false,
                    grease_bit: false,
                },
                shutting_down: false,
            },
        }
    }

    /// Construct from a preset — typically the entry point.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            reactor_id: None,
            config: preset.to_config(),
        }
    }

    /// Set the Reactor (shard) id. Mandatory before `build()`.
    #[must_use]
    pub const fn reactor_id(mut self, id: u32) -> Self {
        self.reactor_id = Some(id);
        self
    }

    /// Override `max_connections`.
    #[must_use]
    pub const fn max_connections(mut self, n: usize) -> Self {
        self.config.max_connections = n;
        self
    }

    /// Build the engine. Validates config and returns the engine
    /// or the first validation error.
    ///
    /// TODO(phase-13): will accept `&mut connection_shard::ConnectionShard`
    /// (or build one) and wire all subsystems. For Phase 0 it
    /// constructs a placeholder.
    pub fn build(self) -> Result<QuicEngine, QuicError> {
        self.config.validate()?;
        let reactor_id = self
            .reactor_id
            .ok_or_else(|| QuicError::InvalidConfig("reactor_id must be set".into()))?;
        Ok(QuicEngine::placeholder(reactor_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_preset_then_reactor_id_then_build() {
        let engine = QuicEngineBuilder::from_preset(Preset::BareMetal)
            .reactor_id(0)
            .build()
            .expect("build");
        assert_eq!(engine.reactor_id, 0);
    }

    #[test]
    fn build_without_reactor_id_fails() {
        let err = QuicEngineBuilder::from_preset(Preset::BareMetal)
            .build()
            .expect_err("should require reactor_id");
        assert!(matches!(err, QuicError::InvalidConfig(_)));
    }

    #[test]
    fn invalid_max_connections_rejected() {
        let err = QuicEngineBuilder::from_preset(Preset::BareMetal)
            .reactor_id(0)
            .max_connections(0)
            .build()
            .expect_err("0 should fail");
        assert!(matches!(err, QuicError::InvalidConfig(_)));
    }
}
