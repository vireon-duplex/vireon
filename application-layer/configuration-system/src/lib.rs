//! # configuration-system
//!
//! **Hierarchical configuration system** for the Vireon application
//! layer. Provides typed key-value access with layered sources (file,
//! environment, CLI flags, runtime).
//!
//! # Architecture (one-line)
//!
//! ```text
//!   ConfigKey ──► [ConfigLayer Stack] ──► ConfigValue
//!                     │
//!                     ├─ File (priority 70)
//!                     ├─ Environment (priority 80)
//!                     ├─ CLI Flags (priority 90)
//!                     └─ Runtime overrides (priority 100)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module       | Phase | Responsibility |
//! |----|--------------|-------|----------------|
//! | 1  | [`error`]    | 0 ✅  | `ConfigError` thiserror enum |
//! | 2  | [`config`]   | 0 ✅  | `ConfigurationSystemConfig`, `Preset` |
//! | 3  | [`key`]      | 0 ✅  | `ConfigKey` (fixed-size key) |
//! | 4  | [`value`]    | 0 ✅  | `ConfigValue` (typed values) |
//! | 5  | [`source`]   | 0 ✅  | `ConfigSource` enum + priority |
//! | 6  | [`layer`]    | 0 ✅  | `ConfigLayer` (layer metadata) |
//! | 7  | [`stats`]    | 0 ✅  | `ConfigStats` atomics, `StatsSnapshot` |
//! | 8  | [`health`]   | 0 ✅  | `HealthReport`, `HealthState` |
//! | 9  | [`builder`]  | 0 ✅  | `ConfigBuilder` (fluent) |
//! | 10 | [`manager`]  | 1    | `ConfigurationManager` facade |
//! | 11 | [`watcher`]  | 1    | `ConfigWatcher` (hot reload) |
//!
//! # Phase 0 status
//!
//! Modules 1–9 are real implementations. Modules 10–11 ship as typed
//! skeletons. Functional manager + watcher land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod error;
pub mod health;
pub mod key;
pub mod layer;
pub mod manager;
pub mod source;
pub mod stats;
pub mod value;
pub mod watcher;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::ConfigBuilder;
pub use config::{ConfigurationSystemConfig, Preset};
pub use error::ConfigError;
pub use health::{HealthReport, HealthState};
pub use key::ConfigKey;
pub use layer::ConfigLayer;
pub use source::ConfigSource;
pub use stats::{ConfigStats, StatsSnapshot};
pub use value::ConfigValue;
