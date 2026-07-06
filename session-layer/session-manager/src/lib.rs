//! # session-manager
//!
//! The **Session Orchestrator** for the Vireon QUIC runtime. Binds
//! application-level sessions to QUIC connections, streams, and
//! reactor cores. Not just CRUD — this crate coordinates
//! authentication, authorization, resource quotas, connection
//! migration, and end-to-end backpressure.
//!
//! # Architecture (one-line)
//!
//! ```text
//! Application ─► [Session Manager] ─► Stream Manager / Transport
//!                      │
//!                      ├─ SessionTable     (slab + index)
//!                      ├─ SessionRegistry   (local lookup)
//!                      ├─ SessionRouter     (SessionId → Connection → Core)
//!                      ├─ AuthenticationPipeline (TLS/JWT/OAuth)
//!                      ├─ AuthorizationSystem (RBAC/ABAC)
//!                      ├─ MigrationManager  (QUIC path migration)
//!                      ├─ SessionScheduler  (per-class scheduling)
//!                      ├─ ResourceManager   (quotas + token bucket)
//!                      ├─ BackpressureTracker (end-to-end)
//!                      └─ SessionLifecycle  (10-state FSM)
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Layer map
//!
//! | # | Module | Phase | Responsibility |
//! |---|--------|-------|----------------|
//! | 1 | [`error`]    | 0 ✅ | `SessionManagerError` thiserror enum |
//! | 2 | [`config`]   | 0 ✅ | `SessionManagerConfig`, `Preset` |
//! | 3 | [`id`]       | 0 ✅ | `SessionId` (128-bit application-level) |
//! | 4 | [`state`]    | 0 ✅ | `SessionState` (10-state FSM) |
//! | 5 | [`priority`] | 0 ✅ | `SessionClass` (Latency/Interactive/Streaming/Bulk) |
//! | 6 | [`quota`]    | 0 ✅ | `ResourceQuota` (memory/buffer/stream/bandwidth) |
//! | 7 | [`stats`]    | 0 ✅ | `SessionStats` atomics, `StatsSnapshot` |
//! | 8 | [`health`]   | 0 ✅ | `HealthReport`, `HealthState` |
//! | 9 | [`session`]     | 1 | `Session` struct (metadata + state) |
//! | 10| [`table`]      | 1 | `SessionTable` (slab + index) |
//! | 11| [`registry`]   | 1 | `SessionRegistry` (local lookup) |
//! | 12| [`routing`]    | 1 | `SessionRouter` (SessionId → Connection → Core) |
//! | 13| [`auth`]       | 2 | `AuthenticationPipeline` (TLS/JWT/OAuth) |
//! | 14| [`authz`]      | 2 | `AuthorizationSystem` (RBAC/ABAC) |
//! | 15| [`migration`]  | 3 | `MigrationManager` (QUIC migration) |
//! | 16| [`scheduler`]  | 2 | `SessionScheduler` (per-class) |
//! | 17| [`resource`]   | 2 | `ResourceManager` (token bucket) |
//! | 18| [`backpressure`]| 3 | `BackpressureTracker` (end-to-end) |
//! | 19| [`lifecycle`]  | 3 | `SessionLifecycle` (FSM controller) |
//! | 20| [`event`]      | 1 | `SessionEvent` enum |
//! | 21| [`builder`]    | 1 | `SessionManagerBuilder` (fluent) |
//! | 22| [`manager`]    | 1 | `SessionManager` facade |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations; modules 9–22 ship as
//! typed skeletons. The functional table + auth + migration land
//! in Phases 1–3+.

#![allow(clippy::module_inception)]

pub mod auth;
pub mod authz;
pub mod backpressure;
pub mod builder;
pub mod config;
pub mod error;
pub mod event;
pub mod health;
pub mod id;
pub mod lifecycle;
pub mod manager;
pub mod migration;
pub mod priority;
pub mod quota;
pub mod registry;
pub mod resource;
pub mod routing;
pub mod scheduler;
pub mod session;
pub mod state;
pub mod stats;
pub mod table;

// ── Re-exports ────────────────────────────────────────────────────

pub use config::{Preset, SessionManagerConfig};
pub use error::SessionManagerError;
pub use health::{HealthReport, HealthState};
pub use id::SessionId;
pub use priority::SessionClass;
pub use quota::{QuotaSnapshot, ResourceQuota};
pub use state::SessionState;
pub use stats::{SessionStats, StatsSnapshot};

// Re-export key adjacent-layer types for convenience.
pub use connection_shard::{ConnectionId, ConnectionPriority, ConnectionState};
pub use stream_manager::{StreamId, StreamPriority};
