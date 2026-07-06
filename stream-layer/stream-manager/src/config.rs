//! [`StreamManagerConfig`] + [`Preset`] — construction-time
//! configuration for the stream manager.
//!
//! See `DESIGN.md` Part 1 (responsibilities) and Part 9
//! (lifecycle) for the rationale behind each field.

use crate::error::StreamManagerError;
use crate::priority::{StreamPriority, URGENCY_DEFAULT};

// ── RFC 9000 defaults ────────────────────────────────────────────

/// Default cap on bidirectional streams (RFC 9000 §4.6).
pub const DEFAULT_MAX_STREAMS_BIDI: u64 = 100;
/// Default cap on unidirectional streams (RFC 9000 §4.6).
pub const DEFAULT_MAX_STREAMS_UNI: u64 = 100;
/// Default initial `max_stream_data` for bidi streams (RFC 9000 §4.1).
/// 256 KiB matches common HTTP/3 deployments.
pub const DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI: u64 = 256 * 1024;
/// Default initial `max_stream_data` for uni streams (RFC 9000 §4.1).
pub const DEFAULT_INITIAL_MAX_STREAM_DATA_UNI: u64 = 256 * 1024;
/// Default idle timeout for an individual stream (ms).
pub const DEFAULT_STREAM_IDLE_TIMEOUT_MS: u64 = 60_000;
/// Default slab chunk size (streams allocated per chunk).
pub const DEFAULT_SLAB_CHUNK_STREAMS: u32 = 1024;

/// Minimum cap on stream counts (must allow at least one stream).
pub const MIN_MAX_STREAMS: u64 = 1;
/// Maximum cap on stream counts. 2^60 is far above any deployment;
/// beyond this the id space itself is exhausted.
pub const MAX_MAX_STREAMS: u64 = 1 << 60;

/// Minimum initial window (RFC 9000 §4.1: must be ≥ 0).
pub const MIN_INITIAL_WINDOW: u64 = 0;
/// Hard cap on initial window (8 MiB).
pub const MAX_INITIAL_WINDOW: u64 = 8 * 1024 * 1024;

/// Construction-time configuration. Built directly or via
/// [`Preset`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamManagerConfig {
    /// Maximum concurrent bidirectional streams (MAX_STREAMS frame).
    pub max_streams_bidi: u64,
    /// Maximum concurrent unidirectional streams.
    pub max_streams_uni: u64,
    /// Initial `max_stream_data` for locally-initiated bidi streams.
    pub initial_max_stream_data_bidi_local: u64,
    /// Initial `max_stream_data` for peer-initiated bidi streams.
    pub initial_max_stream_data_bidi_remote: u64,
    /// Initial `max_stream_data` for uni streams.
    pub initial_max_stream_data_uni: u64,
    /// Default priority assigned to new streams.
    pub default_priority: StreamPriority,
    /// Per-stream idle timeout (ms).
    pub stream_idle_timeout_ms: u64,
    /// Slab chunk size (streams allocated per chunk).
    pub slab_chunk_streams: u32,
}

impl Default for StreamManagerConfig {
    fn default() -> Self {
        Self {
            max_streams_bidi: DEFAULT_MAX_STREAMS_BIDI,
            max_streams_uni: DEFAULT_MAX_STREAMS_UNI,
            initial_max_stream_data_bidi_local: DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI,
            initial_max_stream_data_bidi_remote: DEFAULT_INITIAL_MAX_STREAM_DATA_BIDI,
            initial_max_stream_data_uni: DEFAULT_INITIAL_MAX_STREAM_DATA_UNI,
            default_priority: StreamPriority::default(),
            stream_idle_timeout_ms: DEFAULT_STREAM_IDLE_TIMEOUT_MS,
            slab_chunk_streams: DEFAULT_SLAB_CHUNK_STREAMS,
        }
    }
}

impl StreamManagerConfig {
    /// Validate every field.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::InvalidConfig`] on any
    /// out-of-range field.
    pub fn validate(&self) -> Result<(), StreamManagerError> {
        if !(MIN_MAX_STREAMS..=MAX_MAX_STREAMS).contains(&self.max_streams_bidi) {
            return Err(StreamManagerError::InvalidConfig(format!(
                "max_streams_bidi {} out of range",
                self.max_streams_bidi
            )));
        }
        if !(MIN_MAX_STREAMS..=MAX_MAX_STREAMS).contains(&self.max_streams_uni) {
            return Err(StreamManagerError::InvalidConfig(format!(
                "max_streams_uni {} out of range",
                self.max_streams_uni
            )));
        }
        if self.initial_max_stream_data_bidi_local > MAX_INITIAL_WINDOW {
            return Err(StreamManagerError::InvalidConfig(format!(
                "initial_max_stream_data_bidi_local {} exceeds {}",
                self.initial_max_stream_data_bidi_local, MAX_INITIAL_WINDOW
            )));
        }
        if self.initial_max_stream_data_bidi_remote > MAX_INITIAL_WINDOW {
            return Err(StreamManagerError::InvalidConfig(format!(
                "initial_max_stream_data_bidi_remote {} exceeds {}",
                self.initial_max_stream_data_bidi_remote, MAX_INITIAL_WINDOW
            )));
        }
        if self.initial_max_stream_data_uni > MAX_INITIAL_WINDOW {
            return Err(StreamManagerError::InvalidConfig(format!(
                "initial_max_stream_data_uni {} exceeds {}",
                self.initial_max_stream_data_uni, MAX_INITIAL_WINDOW
            )));
        }
        self.default_priority.validate()?;
        if self.slab_chunk_streams == 0 {
            return Err(StreamManagerError::InvalidConfig(
                "slab_chunk_streams must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// Tuned configuration presets.
///
/// See `DESIGN.md` Part 14 (scalability) for the workload
/// assumptions behind each preset.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    /// HTTP/3-style RPC: moderate bidi stream count, modest
    /// windows, default priority.
    WebRpc,
    /// Media streaming (e.g. video on demand): few long-lived
    /// streams, large uni window, incremental priority.
    MediaStream,
    /// Pub/Sub fan-out: many short-lived streams, default windows.
    PubSub,
    /// Hyperscale (e.g. 1 M+ concurrent streams): max stream count,
    /// large slabs.
    Hyperscale,
    /// Latency-sensitive (trading, gaming): small windows, low
    /// default urgency.
    LowLatency,
}

impl Preset {
    /// Materialise the preset into a [`StreamManagerConfig`].
    #[must_use]
    #[allow(clippy::too_many_lines)] // preset table
    pub fn to_config(self) -> StreamManagerConfig {
        use crate::priority::StreamPriority;

        match self {
            Self::WebRpc => StreamManagerConfig {
                max_streams_bidi: 1_000,
                max_streams_uni: 100,
                initial_max_stream_data_bidi_local: 256 * 1024,
                initial_max_stream_data_bidi_remote: 256 * 1024,
                initial_max_stream_data_uni: 64 * 1024,
                default_priority: StreamPriority::default(),
                stream_idle_timeout_ms: 60_000,
                slab_chunk_streams: 1_024,
            },
            Self::MediaStream => StreamManagerConfig {
                max_streams_bidi: 100,
                max_streams_uni: 1_000,
                initial_max_stream_data_bidi_local: 64 * 1024,
                initial_max_stream_data_bidi_remote: 64 * 1024,
                initial_max_stream_data_uni: 1024 * 1024,
                default_priority: StreamPriority::incremental(URGENCY_DEFAULT),
                stream_idle_timeout_ms: 300_000, // 5 min
                slab_chunk_streams: 256,
            },
            Self::PubSub => StreamManagerConfig {
                max_streams_bidi: 10_000,
                max_streams_uni: 10_000,
                initial_max_stream_data_bidi_local: 8 * 1024,
                initial_max_stream_data_bidi_remote: 8 * 1024,
                initial_max_stream_data_uni: 8 * 1024,
                default_priority: StreamPriority::default(),
                stream_idle_timeout_ms: 30_000,
                slab_chunk_streams: 4_096,
            },
            Self::Hyperscale => StreamManagerConfig {
                max_streams_bidi: 1_000_000,
                max_streams_uni: 1_000_000,
                initial_max_stream_data_bidi_local: 128 * 1024,
                initial_max_stream_data_bidi_remote: 128 * 1024,
                initial_max_stream_data_uni: 128 * 1024,
                default_priority: StreamPriority::default(),
                stream_idle_timeout_ms: 60_000,
                slab_chunk_streams: 16_384,
            },
            Self::LowLatency => StreamManagerConfig {
                max_streams_bidi: 1_000,
                max_streams_uni: 100,
                initial_max_stream_data_bidi_local: 16 * 1024,
                initial_max_stream_data_bidi_remote: 16 * 1024,
                initial_max_stream_data_uni: 16 * 1024,
                default_priority: StreamPriority::with_urgency(1),
                stream_idle_timeout_ms: 5_000,
                slab_chunk_streams: 512,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        StreamManagerConfig::default().validate().expect("default");
    }

    #[test]
    fn validate_rejects_zero_max_streams() {
        let cfg = StreamManagerConfig {
            max_streams_bidi: 0,
            ..StreamManagerConfig::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(StreamManagerError::InvalidConfig(_))
        ));
    }

    #[test]
    fn validate_rejects_huge_window() {
        let cfg = StreamManagerConfig {
            initial_max_stream_data_uni: MAX_INITIAL_WINDOW + 1,
            ..StreamManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_slab_chunk() {
        let cfg = StreamManagerConfig {
            slab_chunk_streams: 0,
            ..StreamManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_invalid_priority() {
        let cfg = StreamManagerConfig {
            default_priority: StreamPriority::with_urgency(8),
            ..StreamManagerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn all_presets_validate() -> Result<(), StreamManagerError> {
        for p in [
            Preset::WebRpc,
            Preset::MediaStream,
            Preset::PubSub,
            Preset::Hyperscale,
            Preset::LowLatency,
        ] {
            p.to_config().validate()?;
        }
        Ok(())
    }

    #[test]
    fn media_stream_is_incremental() {
        let cfg = Preset::MediaStream.to_config();
        assert!(cfg.default_priority.incremental);
    }

    #[test]
    fn low_latency_uses_urgency_one() {
        let cfg = Preset::LowLatency.to_config();
        assert_eq!(cfg.default_priority.urgency, 1);
    }

    #[test]
    fn hyperscale_uses_large_slabs() {
        let cfg = Preset::Hyperscale.to_config();
        assert!(cfg.slab_chunk_streams >= 16_384);
    }
}
