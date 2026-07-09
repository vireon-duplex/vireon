//! # media-streaming
//!
//! **Media Streaming** for the Vireon QUIC runtime. Provides audio/video/screen
//! track management with adaptive bitrate, codec negotiation, and simulcast
//! layer management.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   MediaTrack ──► [MediaStreaming] ──► RTP Stream
//!                   │
//!                   ├─ MediaSession   (track container)
//!                   ├─ CodecNegotiator (codec compatibility)
//!                   ├─ BitrateLadder  (ABR layers)
//!                   ├─ AdaptiveBitrateController (dynamic adjustment)
//!                   └─ SimulcastManager (spatial/temporal layers)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module      | Phase | Responsibility |
//! |----|-------------|-------|----------------|
//! | 1  | [`error`]   | 0 ✅  | `MediaError` thiserror enum |
//! | 2  | [`config`]  | 0 ✅  | `MediaStreamingConfig`, `Preset` |
//! | 3  | [`track`]   | 0 ✅  | `MediaTrackId`, `TrackKind`, `TrackDirection` |
//! | 4  | [`codec`]   | 0 ✅  | `CodecType`, `CodecConfig` |
//! | 5  | [`bitrate`] | 0 ✅  | `BitrateLadder`, `AdaptiveBitrateConfig` |
//! | 6  | [`stats`]   | 0 ✅  | `MediaStreamingStats`, `StatsSnapshot` |
//! | 7  | [`health`]  | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`] | 0 ✅  | `MediaStreamingBuilder` (fluent) |
//! | 9  | [`session`] | 1    | `MediaSession` skeleton |
//! | 10 | [`abr`]     | 1    | `AdaptiveBitrateController` skeleton |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–10 ship as typed
//! skeletons. Functional session + ABR logic land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod abr;
pub mod bitrate;
pub mod builder;
pub mod codec;
pub mod config;
pub mod error;
pub mod health;
pub mod session;
pub mod stats;
pub mod track;

// ── Re-exports ────────────────────────────────────────────────────

pub use abr::AdaptiveBitrateController;
pub use bitrate::{AdaptiveBitrateConfig, BitrateLadder};
pub use builder::MediaStreamingBuilder;
pub use codec::{CodecConfig, CodecType};
pub use config::{MediaStreamingConfig, Preset};
pub use error::MediaError;
pub use health::{HealthReport, HealthState};
pub use session::MediaSession;
pub use stats::{MediaStreamingStats, StatsSnapshot};
pub use track::{MediaTrackId, TrackDirection, TrackKind};

// Re-export key adjacent-layer types for convenience.
pub use session_manager::SessionId;
pub use stream_manager::StreamId;
