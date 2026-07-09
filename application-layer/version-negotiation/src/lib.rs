//! # version-negotiation
//!
//! **Protocol version negotiation** for the Vireon application layer.
//! Provides semver-encoded versions, compatibility matrices, and
//! ALPN-style selection.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   VersionRange ──► [CompatibilityMatrix] ──► BestVersion
//!                       │
//!                       ├─ LocalVersion (ProtocolVersion)
//!                       ├─ RemoteVersions (Vec<ProtocolVersion>)
//!                       └─ CompatibilityRules (CompatEntry)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module          | Phase | Responsibility |
//! |----|-----------------|-------|----------------|
//! | 1  | [`error`]       | 0 ✅  | `VersionNegotiationError` thiserror enum |
//! | 2  | [`config`]      | 0 ✅  | `VersionNegotiationConfig`, `Preset` |
//! | 3  | [`version`]     | 0 ✅  | `ProtocolVersion` (semver encoding) |
//! | 4  | [`range`]       | 0 ✅  | `VersionRange` (min/max) |
//! | 5  | [`matrix`]      | 0 ✅  | `CompatibilityMatrix`, `CompatEntry` |
//! | 6  | [`stats`]       | 0 ✅  | `VersionNegotiationStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]      | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]     | 0 ✅  | `VersionNegotiationBuilder` (fluent) |
//! | 9  | [`negotiator`]  | 1    | `VersionNegotiator` facade |
//! | 10 | [`advertised`]  | 1    | `AdvertisedVersions` (list) |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations (incl. compatibility matrix).
//! Modules 9–10 ship as typed skeletons. Functional negotiator +
//! advertised versions land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod advertised;
pub mod builder;
pub mod config;
pub mod error;
pub mod health;
pub mod matrix;
pub mod negotiator;
pub mod range;
pub mod stats;
pub mod version;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::VersionNegotiationBuilder;
pub use config::{Preset, VersionNegotiationConfig};
pub use error::VersionNegotiationError;
pub use health::{HealthReport, HealthState};
pub use matrix::{CompatEntry, CompatibilityMatrix};
pub use range::VersionRange;
pub use stats::{StatsSnapshot, VersionNegotiationStats};
pub use version::ProtocolVersion;
