//! # graceful-startup-shutdown
//!
//! **Orchestrated startup and shutdown** for the Vireon runtime.
//! Provides phase-ordered boot sequence, connection draining, and
//! crash-safe shutdown with rollback.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   StartupRequest ──► [Phase Machine] ──► GracefulShutdown
//!                       │
//!                       ├─ StartupShutdownCoordinator
//!                       ├─ ConnectionDrainer
//!                       ├─ PhaseBarrier
//!                       └─ Health & Stats
//! ```
//!
//! See `DESIGN.md` for the full 15-section specification.
//!
//! # Layer map
//!
//! | #  | Module       | Phase | Responsibility |
//! |----|--------------|-------|----------------|
//! | 1  | [`error`]    | 0 ✅  | `ShutdownError` thiserror enum |
//! | 2  | [`config`]   | 0 ✅  | `GracefulConfig`, `Preset` |
//! | 3  | [`phase`]    | 0 ✅  | `StartupPhase`, `ShutdownPhase` state machines |
//! | 4  | [`barrier`]  | 0 ✅  | `PhaseBarrier` synchronization primitive |
//! | 5  | [`stats`]    | 0 ✅  | `GracefulStats` atomics, `StatsSnapshot` |
//! | 6  | [`health`]   | 0 ✅  | `HealthReport`, `HealthState` |
//! | 7  | [`builder`]  | 0 ✅  | `GracefulBuilder` (fluent) |
//! | 8  | [`coordinator`] | 0  | `StartupShutdownCoordinator` skeleton |
//! | 9  | [`drain`]    | 0    | `ConnectionDrainer` skeleton |
//!
//! # Phase 0 status
//!
//! Modules 1–7 are real implementations. Modules 8–9 ship as typed
//! skeletons. Functional coordinator and drainer land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod barrier;
pub mod builder;
pub mod config;
pub mod coordinator;
pub mod drain;
pub mod error;
pub mod health;
pub mod phase;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::GracefulBuilder;
pub use config::{GracefulConfig, Preset};
pub use coordinator::StartupShutdownCoordinator;
pub use drain::ConnectionDrainer;
pub use error::ShutdownError;
pub use health::{HealthReport, HealthState};
pub use phase::{ShutdownPhase, StartupPhase};
pub use stats::{GracefulStats, StatsSnapshot};

// Re-export key adjacent-layer types for convenience.
pub use configuration_system::ConfigKey;
pub use module_lifecycle::ModuleId;
pub use scheduler_hooks::SchedulerHookId;
pub use service_registry::ServiceId;

// ── Barrier re-export ─────────────────────────────────────────────

pub use barrier::PhaseBarrier;
