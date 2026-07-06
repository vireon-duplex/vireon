//! [`SessionManagerConfig`] + [`Preset`] — construction-time
//! configuration for the session manager.
//!
//! See `DESIGN.md` Part 1 (architecture goals), Part 4 (lifecycle),
//! and Part 11 (resource management) for the rationale behind each
//! field.

use crate::error::SessionManagerError;
use crate::priority::SessionClass;

// ── Defaults ────────────────────────────────────────────────────

/// Default maximum concurrent sessions per manager instance.
pub const DEFAULT_MAX_SESSIONS: u64 = 100_000;
/// Default maximum concurrent sessions per tenant.
pub const DEFAULT_MAX_SESSIONS_PER_TENANT: u64 = 10_000;
/// Default session idle timeout (ms). 5 minutes.
pub const DEFAULT_SESSION_IDLE_TIMEOUT_MS: u64 = 300_000;
/// Default handshake timeout (ms). 30 seconds (RFC 9000 §10.2).
pub const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 30_000;
/// Default max concurrent streams per session.
pub const DEFAULT_MAX_STREAMS_PER_SESSION: u32 = 100;
/// Default max memory per session (1 MiB).
pub const DEFAULT_MAX_MEMORY_PER_SESSION: u64 = 1024 * 1024;
/// Default max bandwidth per session (10 Mbps).
pub const DEFAULT_MAX_BANDWIDTH_PER_SESSION: u64 = 10 * 1024 * 1024 / 8;
/// Default slab chunk size (sessions allocated per chunk).
pub const DEFAULT_SLAB_CHUNK_SESSIONS: u32 = 1024;

// ── Bounds ──────────────────────────────────────────────────────

/// Minimum cap on sessions (must allow at least one session).
pub const MIN_MAX_SESSIONS: u64 = 1;
/// Hard cap on sessions (1 M). Beyond this the SessionId space
/// itself becomes stressed.
pub const MAX_MAX_SESSIONS: u64 = 1_000_000;
/// Minimum handshake timeout (1 second).
pub const MIN_HANDSHAKE_TIMEOUT_MS: u64 = 1_000;
/// Maximum handshake timeout (5 minutes).
pub const MAX_HANDSHAKE_TIMEOUT_MS: u64 = 300_000;
/// Minimum idle timeout (1 second).
pub const MIN_IDLE_TIMEOUT_MS: u64 = 1_000;
/// Maximum idle timeout (24 hours).
pub const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

/// Construction-time configuration. Built directly or via
/// [`Preset`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionManagerConfig {
    /// Maximum concurrent sessions this manager will accept.
    pub max_sessions: u64,
    /// Maximum concurrent sessions per tenant (RBAC scope).
    pub max_sessions_per_tenant: u64,
    /// Session idle timeout (ms). Sessions with no activity are
    /// transitioned to `Idle` and eventually destroyed.
    pub session_idle_timeout_ms: u64,
    /// Handshake timeout (ms). Sessions stuck in `Creating` are
    /// destroyed.
    pub handshake_timeout_ms: u64,
    /// Default cap on streams per session.
    pub max_streams_per_session: u32,
    /// Default cap on memory per session (bytes).
    pub max_memory_per_session: u64,
    /// Default cap on bandwidth per session (bytes/sec).
    pub max_bandwidth_per_session: u64,
    /// Default session class assigned to new sessions.
    pub default_class: SessionClass,
    /// Whether connection migration is enabled (QUIC path
    /// migration).
    pub enable_migration: bool,
    /// Whether session resume (0-RTT) is enabled.
    pub enable_resume: bool,
    /// Slab chunk size (sessions allocated per chunk).
    pub slab_chunk_sessions: u32,
}

impl Default for SessionManagerConfig {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_MAX_SESSIONS,
            max_sessions_per_tenant: DEFAULT_MAX_SESSIONS_PER_TENANT,
            session_idle_timeout_ms: DEFAULT_SESSION_IDLE_TIMEOUT_MS,
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
            max_streams_per_session: DEFAULT_MAX_STREAMS_PER_SESSION,
            max_memory_per_session: DEFAULT_MAX_MEMORY_PER_SESSION,
            max_bandwidth_per_session: DEFAULT_MAX_BANDWIDTH_PER_SESSION,
            default_class: SessionClass::default(),
            enable_migration: true,
            enable_resume: true,
            slab_chunk_sessions: DEFAULT_SLAB_CHUNK_SESSIONS,
        }
    }
}

impl SessionManagerConfig {
    /// Validate every field.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::InvalidConfig`] on any
    /// out-of-range field.
    pub fn validate(&self) -> Result<(), SessionManagerError> {
        if !(MIN_MAX_SESSIONS..=MAX_MAX_SESSIONS).contains(&self.max_sessions) {
            return Err(SessionManagerError::InvalidConfig(format!(
                "max_sessions {} out of range [{}..{}]",
                self.max_sessions, MIN_MAX_SESSIONS, MAX_MAX_SESSIONS
            )));
        }
        if self.max_sessions_per_tenant == 0 || self.max_sessions_per_tenant > self.max_sessions {
            return Err(SessionManagerError::InvalidConfig(format!(
                "max_sessions_per_tenant {} must be in 1..={}",
                self.max_sessions_per_tenant, self.max_sessions
            )));
        }
        if !(MIN_IDLE_TIMEOUT_MS..=MAX_IDLE_TIMEOUT_MS).contains(&self.session_idle_timeout_ms) {
            return Err(SessionManagerError::InvalidConfig(format!(
                "session_idle_timeout_ms {} out of range [{}..{}]",
                self.session_idle_timeout_ms, MIN_IDLE_TIMEOUT_MS, MAX_IDLE_TIMEOUT_MS
            )));
        }
        if !(MIN_HANDSHAKE_TIMEOUT_MS..=MAX_HANDSHAKE_TIMEOUT_MS)
            .contains(&self.handshake_timeout_ms)
        {
            return Err(SessionManagerError::InvalidConfig(format!(
                "handshake_timeout_ms {} out of range [{}..{}]",
                self.handshake_timeout_ms, MIN_HANDSHAKE_TIMEOUT_MS, MAX_HANDSHAKE_TIMEOUT_MS
            )));
        }
        if self.max_streams_per_session == 0 {
            return Err(SessionManagerError::InvalidConfig(
                "max_streams_per_session must be > 0".into(),
            ));
        }
        if self.max_memory_per_session == 0 {
            return Err(SessionManagerError::InvalidConfig(
                "max_memory_per_session must be > 0".into(),
            ));
        }
        if self.slab_chunk_sessions == 0 {
            return Err(SessionManagerError::InvalidConfig(
                "slab_chunk_sessions must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// Tuned configuration presets.
///
/// See `DESIGN.md` Part 20 (production review) for the workload
/// assumptions behind each preset.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    /// Cloud RPC (gRPC-over-QUIC, microservices): moderate sessions,
    /// interactive class, short idle timeout.
    CloudRpc,
    /// Messaging gateway (chat, notifications): many sessions,
    /// interactive class, long idle timeout.
    MessagingGateway,
    /// Real-time (gaming, trading): few latency-sensitive sessions,
    /// small per-session memory, short handshake timeout.
    RealTime,
    /// Hyperscale (CDN, 1M+ sessions): max session count, large
    /// slabs, bulk streaming default.
    Hyperscale,
    /// Edge (IoT, embedded): tiny footprint, low memory, small
    /// slabs.
    Edge,
}

impl Preset {
    /// Materialise the preset into a [`SessionManagerConfig`].
    #[must_use]
    pub const fn to_config(self) -> SessionManagerConfig {
        match self {
            Self::CloudRpc => SessionManagerConfig {
                max_sessions: 100_000,
                max_sessions_per_tenant: 10_000,
                session_idle_timeout_ms: 300_000,
                handshake_timeout_ms: 30_000,
                max_streams_per_session: 100,
                max_memory_per_session: 1024 * 1024,
                max_bandwidth_per_session: 10 * 1024 * 1024 / 8,
                default_class: SessionClass::Interactive,
                enable_migration: true,
                enable_resume: true,
                slab_chunk_sessions: 1_024,
            },
            Self::MessagingGateway => SessionManagerConfig {
                max_sessions: 500_000,
                max_sessions_per_tenant: 50_000,
                session_idle_timeout_ms: 3_600_000, // 1 hour
                handshake_timeout_ms: 30_000,
                max_streams_per_session: 50,
                max_memory_per_session: 512 * 1024,
                max_bandwidth_per_session: 5 * 1024 * 1024 / 8,
                default_class: SessionClass::Interactive,
                enable_migration: true,
                enable_resume: true,
                slab_chunk_sessions: 4_096,
            },
            Self::RealTime => SessionManagerConfig {
                max_sessions: 10_000,
                max_sessions_per_tenant: 1_000,
                session_idle_timeout_ms: 60_000,
                handshake_timeout_ms: 5_000,
                max_streams_per_session: 10,
                max_memory_per_session: 256 * 1024,
                max_bandwidth_per_session: 50 * 1024 * 1024 / 8,
                default_class: SessionClass::LatencySensitive,
                enable_migration: true,
                enable_resume: false, // 0-RTT disabled for anti-replay
                slab_chunk_sessions: 512,
            },
            Self::Hyperscale => SessionManagerConfig {
                max_sessions: 1_000_000,
                max_sessions_per_tenant: 100_000,
                session_idle_timeout_ms: 120_000,
                handshake_timeout_ms: 30_000,
                max_streams_per_session: 200,
                max_memory_per_session: 2 * 1024 * 1024,
                max_bandwidth_per_session: 100 * 1024 * 1024 / 8,
                default_class: SessionClass::Streaming,
                enable_migration: true,
                enable_resume: true,
                slab_chunk_sessions: 16_384,
            },
            Self::Edge => SessionManagerConfig {
                max_sessions: 1_000,
                max_sessions_per_tenant: 100,
                session_idle_timeout_ms: 60_000,
                handshake_timeout_ms: 10_000,
                max_streams_per_session: 10,
                max_memory_per_session: 64 * 1024,
                max_bandwidth_per_session: 1024 * 1024 / 8,
                default_class: SessionClass::Interactive,
                enable_migration: false, // edge devices rarely migrate
                enable_resume: true,
                slab_chunk_sessions: 64,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        SessionManagerConfig::default()
            .validate()
            .expect("default config should validate");
    }

    #[test]
    fn validate_rejects_zero_max_sessions() {
        let cfg = SessionManagerConfig {
            max_sessions: 0,
            ..SessionManagerConfig::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(SessionManagerError::InvalidConfig(_))
        ));
    }

    #[test]
    fn validate_rejects_too_many_sessions() {
        let cfg = SessionManagerConfig {
            max_sessions: MAX_MAX_SESSIONS + 1,
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_tenant_exceeding_global() {
        let cfg = SessionManagerConfig {
            max_sessions: 1_000,
            max_sessions_per_tenant: 2_000, // > max_sessions
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_tenant() {
        let cfg = SessionManagerConfig {
            max_sessions_per_tenant: 0,
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_short_idle_timeout() {
        let cfg = SessionManagerConfig {
            session_idle_timeout_ms: MIN_IDLE_TIMEOUT_MS - 1,
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_short_handshake_timeout() {
        let cfg = SessionManagerConfig {
            handshake_timeout_ms: MIN_HANDSHAKE_TIMEOUT_MS - 1,
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_streams_per_session() {
        let cfg = SessionManagerConfig {
            max_streams_per_session: 0,
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_memory() {
        let cfg = SessionManagerConfig {
            max_memory_per_session: 0,
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_slab_chunk() {
        let cfg = SessionManagerConfig {
            slab_chunk_sessions: 0,
            ..SessionManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn all_presets_validate() -> Result<(), SessionManagerError> {
        for p in [
            Preset::CloudRpc,
            Preset::MessagingGateway,
            Preset::RealTime,
            Preset::Hyperscale,
            Preset::Edge,
        ] {
            p.to_config().validate()?;
        }
        Ok(())
    }

    #[test]
    fn realtime_disables_resume() {
        let cfg = Preset::RealTime.to_config();
        assert!(!cfg.enable_resume);
    }

    #[test]
    fn realtime_uses_latency_sensitive_class() {
        let cfg = Preset::RealTime.to_config();
        assert_eq!(cfg.default_class, SessionClass::LatencySensitive);
    }

    #[test]
    fn hyperscale_uses_large_slabs() {
        let cfg = Preset::Hyperscale.to_config();
        assert!(cfg.slab_chunk_sessions >= 16_384);
    }

    #[test]
    fn edge_disables_migration() {
        let cfg = Preset::Edge.to_config();
        assert!(!cfg.enable_migration);
    }

    #[test]
    fn hyperscale_uses_streaming_default() {
        let cfg = Preset::Hyperscale.to_config();
        assert_eq!(cfg.default_class, SessionClass::Streaming);
    }

    #[test]
    fn messaging_gateway_uses_long_idle() {
        let cfg = Preset::MessagingGateway.to_config();
        assert!(cfg.session_idle_timeout_ms >= 3_600_000);
    }
}
