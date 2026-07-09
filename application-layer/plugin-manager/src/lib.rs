//! # plugin-manager
//!
//! **Plugin manager** for the Vireon application layer. Provides
//! versioned plugin loading with API compatibility checking,
//! capability declarations, and dependency resolution.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   PluginId ──► [PluginManager] ──► LoadedPlugin
//!                     │
//!                     ├─ PluginLoader (discover + load)
//!                     ├─ ApiVersion (compatibility check)
//!                     ├─ PluginDescriptor (metadata)
//!                     └─ DependencyResolution
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module         | Phase | Responsibility |
//! |----|----------------|-------|----------------|
//! | 1  | [`error`]      | 0 ✅  | `PluginError` thiserror enum |
//! | 2  | [`config`]     | 0 ✅  | `PluginManagerConfig`, `Preset` |
//! | 3  | [`id`]         | 0 ✅  | `PluginId`, `PluginVersion` |
//! | 4  | [`api_version`]| 0 ✅  | `ApiVersion` (compatibility encoding) |
//! | 5  | [`descriptor`] | 0 ✅  | `PluginDescriptor` (metadata) |
//! | 6  | [`stats`]      | 0 ✅  | `PluginManagerStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]     | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]    | 0 ✅  | `PluginManagerBuilder` (fluent) |
//! | 9  | [`manager`]    | 1    | `PluginManager` facade |
//! | 10 | [`loader`]     | 1    | `PluginLoader` (discover + load) |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations (incl. API version checking).
//! Modules 9–10 ship as typed skeletons. Functional manager + loader
//! land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod api_version;
pub mod builder;
pub mod config;
pub mod descriptor;
pub mod error;
pub mod health;
pub mod id;
pub mod loader;
pub mod manager;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use api_version::{ApiVersion, CURRENT_API};
pub use builder::PluginManagerBuilder;
pub use config::{PluginManagerConfig, Preset};
pub use descriptor::{
    CAP_AUTH, CAP_LOGGING, CAP_METRICS, CAP_ROUTING, CAP_SCHEDULING, PluginDescriptor,
};
pub use error::PluginError;
pub use health::{HealthReport, HealthState};
pub use id::{PluginId, PluginVersion};
pub use loader::PluginLoader;
pub use manager::PluginManager;
pub use stats::{PluginManagerStats, StatsSnapshot};

// Re-export key adjacent-layer types for convenience.
pub use configuration_system::{ConfigError, ConfigKey, ConfigValue};
pub use version_negotiation::{ProtocolVersion, VersionRange};
