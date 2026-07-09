//! # module-lifecycle
//!
//! **Module lifecycle management** for the Vireon application layer.
//! Provides dependency-ordered initialization, startup, and shutdown
//! with cycle detection and failure rollback.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   ModuleId ──► [Dependency Graph] ──► Topo Sort ──► Phase Transitions
//!                     │
//!                     ├─ DependencyCycle Detection
//!                     ├─ Phase State Machine
//!                     ├─ Failure Rollback
//!                     └─ LifecycleCoordinator
//! ```
//!
//! See `DESIGN.md` for the full 15-section specification.
//!
//! # Layer map
//!
//! | #  | Module       | Phase | Responsibility |
//! |----|--------------|-------|----------------|
//! | 1  | [`error`]    | 0 ✅  | `ModuleLifecycleError` thiserror enum |
//! | 2  | [`config`]   | 0 ✅  | `LifecycleConfig`, `Preset` |
//! | 3  | [`id`]       | 0 ✅  | `ModuleId` newtype |
//! | 4  | [`phase`]    | 0 ✅  | `ModulePhase` state machine |
//! | 5  | [`descriptor`] | 0 ✅ | `ModuleDescriptor` metadata |
//! | 6  | [`stats`]    | 0 ✅  | `ModuleLifecycleStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]   | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]  | 0 ✅  | `LifecycleBuilder` (fluent) |
//! | 9  | [`coordinator`] | 1  | `LifecycleCoordinator` facade |
//! | 10 | [`graph`]    | 1    | `DependencyGraph` with cycle detection |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–10 ship as typed
//! skeletons. Functional coordinator + graph logic land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod coordinator;
pub mod descriptor;
pub mod error;
pub mod graph;
pub mod health;
pub mod id;
pub mod phase;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::LifecycleBuilder;
pub use config::{LifecycleConfig, Preset};
pub use descriptor::ModuleDescriptor;
pub use error::ModuleLifecycleError;
pub use health::{HealthReport, HealthState};
pub use id::ModuleId;
pub use phase::ModulePhase;
pub use stats::{ModuleLifecycleStats, StatsSnapshot};

// Re-export key adjacent-layer types for convenience.
pub use configuration_system::{
    ConfigError, ConfigKey, ConfigValue, ConfigurationSystemConfig,
    HealthReport as ConfigHealthReport, HealthState as ConfigHealthState,
};
