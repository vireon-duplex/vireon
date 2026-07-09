//! # scheduler-hooks
//!
//! **Reactor scheduler hooks** for the Vireon runtime layer.
//!
//! Provides pre/post-tick, pre-submit, post-complete, on-idle, and
//! on-wakeup triggers with priority ordering for custom logic injection
//! into the reactor's event loop.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   SchedulerHookId ──► [HookRegistry] ──► HookTrigger
//!                         │
//!                         ├─ Priority (HIGH/NORMAL/LOW)
//!                         ├─ HookFunction (callback)
//!                         └─ Invocation Count
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Design goals
//!
//! - **Type-safe hook IDs**: 64-bit identifiers with nil sentinel
//! - **Priority ordering**: HIGHEST (0) to LOWEST (255) with explicit comparison
//! - **Trigger types**: Pre/post tick, submit, complete, idle, wakeup
//! - **Thread-safe**: Lock-free statistics with atomic counters
//! - **Observable**: Comprehensive metrics and health monitoring
//! - **Performant**: Minimal overhead on hot paths
//!
//! # Phase 0 status
//!
//! Real implementations for:
//! - `SchedulerHookId`, `HookTrigger`, `HookPriority` (types and predicates)
//! - `SchedulerHooksConfig`, `Preset` (validation and presets)
//! - `SchedulerHookStats`, `StatsSnapshot` (atomic counters)
//! - `HealthReport`, `HealthState` (health classification)
//! - `SchedulerHooksBuilder` (fluent config builder)
//!
//! Skeleton facades for:
//! - `HookRegistry` (hook registration and invocation)
//! - `HookCoordinator` (tick lifecycle coordination)
//!
//! Functional registry and coordinator land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod coordinator;
pub mod error;
pub mod health;
pub mod hook;
pub mod priority;
pub mod registry;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::SchedulerHooksBuilder;
pub use config::{Preset, SchedulerHooksConfig};
pub use error::SchedulerHookError;
pub use health::{HealthReport, HealthState};
pub use hook::{HookTrigger, SchedulerHookId};
pub use priority::HookPriority;
pub use stats::{SchedulerHookStats, StatsSnapshot};

// Re-export reactor types for convenience
pub use reactor::{ReactorConfig, ReactorPreset};
