//! [`SessionManagerBuilder`] — fluent construction for
//! [`SessionManager`](crate::manager::SessionManager).
//!
//! See `DESIGN.md` Part 1 (architecture goals) and Part 4
//! (lifecycle).
//!
//! # Phase 0 status
//!
//! Phase 0 ships the builder shape; functional `build()` (which
//! validates config, allocates the slab, and constructs the
//! manager) lands in Phase 1+.

use crate::config::{Preset, SessionManagerConfig};
use crate::error::SessionManagerError;
use crate::manager::SessionManager;
use crate::priority::SessionClass;

/// Fluent builder for the session manager.
#[derive(Clone, Debug)]
pub struct SessionManagerBuilder {
    cfg: SessionManagerConfig,
}

impl Default for SessionManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionManagerBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: SessionManagerConfig::default(),
        }
    }

    /// Start a new builder from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Set the maximum concurrent sessions.
    #[must_use]
    pub const fn max_sessions(mut self, n: u64) -> Self {
        self.cfg.max_sessions = n;
        self
    }

    /// Set the maximum sessions per tenant.
    #[must_use]
    pub const fn max_sessions_per_tenant(mut self, n: u64) -> Self {
        self.cfg.max_sessions_per_tenant = n;
        self
    }

    /// Set the session idle timeout (ms).
    #[must_use]
    pub const fn session_idle_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.session_idle_timeout_ms = ms;
        self
    }

    /// Set the handshake timeout (ms).
    #[must_use]
    pub const fn handshake_timeout_ms(mut self, ms: u64) -> Self {
        self.cfg.handshake_timeout_ms = ms;
        self
    }

    /// Set the max streams per session.
    #[must_use]
    pub const fn max_streams_per_session(mut self, n: u32) -> Self {
        self.cfg.max_streams_per_session = n;
        self
    }

    /// Set the max memory per session (bytes).
    #[must_use]
    pub const fn max_memory_per_session(mut self, bytes: u64) -> Self {
        self.cfg.max_memory_per_session = bytes;
        self
    }

    /// Set the max bandwidth per session (bytes/sec).
    #[must_use]
    pub const fn max_bandwidth_per_session(mut self, bps: u64) -> Self {
        self.cfg.max_bandwidth_per_session = bps;
        self
    }

    /// Set the default session class.
    #[must_use]
    pub const fn default_class(mut self, class: SessionClass) -> Self {
        self.cfg.default_class = class;
        self
    }

    /// Enable / disable connection migration.
    #[must_use]
    pub const fn enable_migration(mut self, enabled: bool) -> Self {
        self.cfg.enable_migration = enabled;
        self
    }

    /// Enable / disable session resume (0-RTT).
    #[must_use]
    pub const fn enable_resume(mut self, enabled: bool) -> Self {
        self.cfg.enable_resume = enabled;
        self
    }

    /// Set the slab chunk size (sessions per chunk).
    #[must_use]
    pub const fn slab_chunk_sessions(mut self, n: u32) -> Self {
        self.cfg.slab_chunk_sessions = n;
        self
    }

    /// Inspect the config the builder has assembled.
    #[must_use]
    pub const fn config(&self) -> SessionManagerConfig {
        self.cfg
    }

    /// Build and return a [`SessionManager`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::InvalidConfig`] if validation fails.
    pub fn build(self) -> Result<SessionManager, SessionManagerError> {
        self.cfg.validate()?;
        SessionManager::new(self.cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_uses_default_config() {
        let b = SessionManagerBuilder::new();
        assert_eq!(b.config(), SessionManagerConfig::default());
    }

    #[test]
    fn preset_builder_materialises() {
        let b = SessionManagerBuilder::from_preset(Preset::Hyperscale);
        assert_eq!(b.config().max_sessions, 1_000_000);
    }

    #[test]
    fn setters_take_effect() {
        let b = SessionManagerBuilder::new()
            .max_sessions(500)
            .slab_chunk_sessions(2_048)
            .default_class(SessionClass::Streaming);
        let cfg = b.config();
        assert_eq!(cfg.max_sessions, 500);
        assert_eq!(cfg.slab_chunk_sessions, 2_048);
        assert_eq!(cfg.default_class, SessionClass::Streaming);
    }

    #[test]
    fn build_succeeds() {
        let m = SessionManagerBuilder::new().build();
        assert!(m.is_ok());
    }

    #[test]
    fn build_rejects_invalid_config() {
        let r = SessionManagerBuilder::new().max_sessions(0).build();
        assert!(matches!(r, Err(SessionManagerError::InvalidConfig(_))));
    }

    #[test]
    fn disable_flags_take_effect() {
        let b = SessionManagerBuilder::new()
            .enable_migration(false)
            .enable_resume(false);
        let cfg = b.config();
        assert!(!cfg.enable_migration);
        assert!(!cfg.enable_resume);
    }
}
