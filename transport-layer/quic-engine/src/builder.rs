//! [`QuicEngineBuilder`] — fluent builder + `from_preset`.
//!
//! Constructs a real [`QuicEngine`] backed by quiche. The builder
//! accepts a [`QuicConfig`] (or one materialized from a [`Preset`]),
//! a TLS certificate / key path, and the local socket address the
//! engine's reactor is bound to.
//!
//! ## Example
//!
//! ```no_run
//! use quic_engine::{QuicEngineBuilder, QuicPreset};
//! use std::net::SocketAddr;
//!
//! let local_addr: SocketAddr = "127.0.0.1:4433".parse().unwrap();
//! let mut engine = QuicEngineBuilder::from_preset(QuicPreset::BareMetal)
//!     .reactor_id(0)
//!     .cert_path("cert.pem")
//!     .key_path("key.pem")
//!     .local_addr(local_addr)
//!     .build()
//!     .expect("engine");
//! ```

use std::net::SocketAddr;

use buffer_pool::BufferPool;

use crate::config::{Preset, QuicConfig};
use crate::crypto::build_quiche_config;
use crate::engine::QuicEngine;
use crate::error::QuicError;

/// Fluent builder for [`QuicEngine`].
///
/// Start with [`QuicEngineBuilder::new`] or
/// [`QuicEngineBuilder::from_preset`], then set the mandatory
/// `reactor_id`, `cert_path`, `key_path`, and `local_addr` before
/// calling [`build`](Self::build).
#[derive(Default)]
pub struct QuicEngineBuilder {
    reactor_id: Option<u32>,
    local_addr: Option<SocketAddr>,
    cert_path: Option<String>,
    key_path: Option<String>,
    config: QuicConfig,
    pool: Option<BufferPool>,
}

impl QuicEngineBuilder {
    /// Create an empty builder seeded with [`QuicConfig::default`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            reactor_id: None,
            local_addr: None,
            cert_path: None,
            key_path: None,
            config: QuicConfig::default(),
            pool: None,
        }
    }

    /// Construct from a preset — typically the entry point.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            reactor_id: None,
            local_addr: None,
            cert_path: None,
            key_path: None,
            config: preset.to_config(),
            pool: None,
        }
    }

    /// Construct from an explicit [`QuicConfig`].
    #[must_use]
    pub fn from_config(config: QuicConfig) -> Self {
        Self {
            reactor_id: None,
            local_addr: None,
            cert_path: None,
            key_path: None,
            config,
            pool: None,
        }
    }

    /// Set the Reactor (shard) id. Mandatory before `build()`.
    #[must_use]
    pub const fn reactor_id(mut self, id: u32) -> Self {
        self.reactor_id = Some(id);
        self
    }

    /// Set the local socket address the engine's reactor is bound to.
    /// Mandatory before `build()`.
    #[must_use]
    pub const fn local_addr(mut self, addr: SocketAddr) -> Self {
        self.local_addr = Some(addr);
        self
    }

    /// Set the PEM certificate chain path. Mandatory before `build()`.
    #[must_use]
    pub fn cert_path(mut self, path: impl Into<String>) -> Self {
        self.cert_path = Some(path.into());
        self
    }

    /// Set the PEM private key path. Mandatory before `build()`.
    #[must_use]
    pub fn key_path(mut self, path: impl Into<String>) -> Self {
        self.key_path = Some(path.into());
        self
    }

    /// Override `max_connections`.
    #[must_use]
    pub const fn max_connections(mut self, n: usize) -> Self {
        self.config.max_connections = n;
        self
    }

    /// Set the buffer pool used for zero-alloc frame payloads. If unset,
    /// a default pool is created.
    #[must_use]
    pub fn buffer_pool(mut self, pool: BufferPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Build the engine. Validates config, constructs the
    /// `quiche::Config` from cert/key paths, and returns the engine.
    ///
    /// # Errors
    ///
    /// Returns [`QuicError`] if:
    /// - config validation fails (see [`QuicConfig::validate`])
    /// - `reactor_id`, `local_addr`, `cert_path`, or `key_path` is unset
    /// - `quiche::Config` construction fails (cert/key load, ALPN, etc.)
    pub fn build(self) -> Result<QuicEngine, QuicError> {
        self.config.validate()?;

        let reactor_id = self
            .reactor_id
            .ok_or_else(|| QuicError::InvalidConfig("reactor_id must be set".into()))?;
        let local_addr = self
            .local_addr
            .ok_or_else(|| QuicError::InvalidConfig("local_addr must be set".into()))?;
        let cert_path = self
            .cert_path
            .ok_or_else(|| QuicError::InvalidConfig("cert_path must be set".into()))?;
        let key_path = self
            .key_path
            .ok_or_else(|| QuicError::InvalidConfig("key_path must be set".into()))?;

        let quiche_config = build_quiche_config(&self.config, &cert_path, &key_path)?;
        let pool = self.pool.unwrap_or_default();

        Ok(QuicEngine::new(
            self.config,
            quiche_config,
            reactor_id,
            local_addr,
            pool,
        ))
    }
}

impl std::fmt::Debug for QuicEngineBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicEngineBuilder")
            .field("reactor_id", &self.reactor_id)
            .field("local_addr", &self.local_addr)
            .field("cert_path", &self.cert_path.as_deref().unwrap_or("(unset)"))
            .field("key_path", &self.key_path.as_deref().unwrap_or("(unset)"))
            .field("config", &self.config)
            .field("pool", &self.pool.as_ref().map(|_| "<set>"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_without_reactor_id_fails() {
        let err = QuicEngineBuilder::from_preset(Preset::BareMetal)
            .cert_path("cert.pem")
            .key_path("key.pem")
            .local_addr("127.0.0.1:0".parse().unwrap())
            .build()
            .expect_err("should require reactor_id");
        assert!(matches!(err, QuicError::InvalidConfig(_)));
    }

    #[test]
    fn build_without_cert_path_fails() {
        let err = QuicEngineBuilder::from_preset(Preset::BareMetal)
            .reactor_id(0)
            .key_path("key.pem")
            .local_addr("127.0.0.1:0".parse().unwrap())
            .build()
            .expect_err("should require cert_path");
        assert!(matches!(err, QuicError::InvalidConfig(_)));
    }

    #[test]
    fn invalid_max_connections_rejected() {
        let err = QuicEngineBuilder::from_preset(Preset::BareMetal)
            .reactor_id(0)
            .cert_path("cert.pem")
            .key_path("key.pem")
            .local_addr("127.0.0.1:0".parse().unwrap())
            .max_connections(0)
            .build()
            .expect_err("0 should fail");
        assert!(matches!(err, QuicError::InvalidConfig(_)));
    }
}
