//! # session-migration-manager
//!
//! The **Cross-Node Session Migration Coordinator** for the Vireon
//! QUIC runtime. Coordinates the migration of active sessions between
//! cluster nodes with zero message loss, using a four-phase protocol:
//!
//! ```text
//!   Initiate → Snapshot → Transfer → Restore → Finalize
//! ```
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Operator / Rebalancer
//!           │
//!           ▼
//!   ┌─────────────────────┐
//!   │ MigrationCoordinator │
//!   └───────┬─────────────┘
//!           │
//!           ├─ MigrationPlan      (source → target + deadline + flags)
//!           ├─ MigrationPhase     (state machine)
//!           ├─ SnapshotMetadata   (session state descriptor)
//!           ├─ StateTransfer      (chunked/streaming transfer)
//!           ├─ StateRestore       (reconstruct on target)
//!           ├─ OrderingPreserver  (sequence + replay buffer)
//!           ├─ CheckpointManager  (crash recovery)
//!           └─ MigrationStats     (observability)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module        | Phase | Responsibility |
//! |----|---------------|-------|----------------|
//! | 1  | [`error`]     | 0     | `MigrationError` thiserror enum |
//! | 2  | [`config`]    | 0     | `MigrationConfig`, `Preset` |
//! | 3  | [`phase`]     | 0     | `MigrationPhase` state machine |
//! | 4  | [`plan`]      | 0     | `MigrationPlan`, `MigrationFlags` |
//! | 5  | [`snapshot`]  | 0     | `SnapshotMetadata`, `MigrationSnapshotRef` |
//! | 6  | [`coordinator`]| 0    | `MigrationRecord`, `MigrationCoordinator` skeleton |
//! | 7  | [`stats`]     | 0     | `MigrationStats` atomics, `StatsSnapshot` |
//! | 8  | [`health`]    | 0     | `HealthReport`, `HealthState` |
//! | 9  | [`event`]     | 0     | `MigrationEvent` enum |
//! | 10 | [`builder`]   | 0     | `MigrationServiceBuilder` (fluent) |
//! | 11 | [`transfer`]  | 1     | `StateTransfer` (chunked/streaming) |
//! | 12 | [`restore`]   | 1     | `StateRestore` (reconstruct) |
//! | 13 | [`ordering`]  | 1     | `OrderingPreserver` (sequence + replay) |
//! | 14 | [`checkpoint`]| 1     | `CheckpointManager` (crash recovery) |
//!
//! # Phase 0 status
//!
//! Modules 1–10 are real implementations (incl. state machine and
//! atomic stats). Modules 11–14 ship as typed skeletons. Functional
//! coordinator, transfer, and restore land in Phase 1+.

pub mod builder;
pub mod checkpoint;
pub mod config;
pub mod coordinator;
pub mod error;
pub mod event;
pub mod health;
pub mod ordering;
pub mod phase;
pub mod plan;
pub mod restore;
pub mod snapshot;
pub mod stats;
pub mod transfer;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::MigrationServiceBuilder;
pub use config::{MigrationConfig, Preset};
pub use coordinator::{MigrationCoordinator, MigrationRecord};
pub use error::MigrationError;
pub use event::MigrationEvent;
pub use health::{HealthReport, HealthState};
pub use phase::MigrationPhase;
pub use plan::{MigrationFlags, MigrationPlan};
pub use restore::StateRestore;
pub use snapshot::{MigrationSnapshotRef, SnapshotMetadata};
pub use stats::{MigrationStats, StatsSnapshot};
pub use transfer::StateTransfer;

// Re-export key adjacent-layer types for convenience.
pub use session_manager::SessionId;
pub use session_registry::{Epoch, Generation, NodeId, Region, Zone};
