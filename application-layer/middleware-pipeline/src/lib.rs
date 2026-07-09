//! # middleware-pipeline
//!
//! **Request/response middleware pipeline** for the Vireon application
//! layer. Provides ordered chain execution with stage-based dispatch
//! (pre-auth, auth, pre-dispatch, post-dispatch, response, error).
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Request ──► [Middleware Chain] ──► Staged Dispatch ──► Response
//!                     │
//!                     ├─ PreAuth Stage
//!                     ├─ AuthN/AuthZ Stage
//!                     ├─ PreDispatch Stage
//!                     ├─ PostDispatch Stage
//!                     ├─ Response Stage
//!                     └─ Error Stage
//! ```
//!
//! See `DESIGN.md` for the full 15-section specification.
//!
//! # Layer map
//!
//! | #  | Module       | Phase | Responsibility |
//! |----|--------------|-------|----------------|
//! | 1  | [`error`]    | 0 ✅  | `MiddlewareError` thiserror enum |
//! | 2  | [`config`]   | 0 ✅  | `MiddlewareConfig`, `Preset` |
//! | 3  | [`id`]       | 0 ✅  | `MiddlewareId` newtype |
//! | 4  | [`stage`]    | 0 ✅  | `MiddlewareStage` enum |
//! | 5  | [`context`]  | 0 ✅  | `MiddlewareContext` request context |
//! | 6  | [`stats`]    | 0 ✅  | `MiddlewareStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]   | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]  | 0 ✅  | `MiddlewarePipelineBuilder` (fluent) |
//! | 9  | [`pipeline`] | 1    | `Pipeline` facade |
//! | 10 | [`chain`]    | 1    | `MiddlewareChain` registry |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–10 ship as typed
//! skeletons. Functional pipeline + chain logic land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod chain;
pub mod config;
pub mod context;
pub mod error;
pub mod health;
pub mod id;
pub mod pipeline;
pub mod stage;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::MiddlewarePipelineBuilder;
pub use config::{MiddlewareConfig, Preset};
pub use context::MiddlewareContext;
pub use error::MiddlewareError;
pub use health::{HealthReport, HealthState};
pub use id::MiddlewareId;
pub use stage::MiddlewareStage;
pub use stats::{MiddlewareStats, StatsSnapshot};
