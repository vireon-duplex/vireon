//! # session-replication-service
//!
//! The **Metadata Replication Service** for the Vireon QUIC runtime.
//! Provides high-availability replication of session-layer metadata
//! using Gossip, Raft, CRDT, or Sync strategies with conflict
//! detection and quorum management.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   SessionMetadata ──► [ReplicationStrategy] ──► Replicas
//!                              │
//!                              ├─ GossipEngine   (eventual consistency)
//!                              ├─ RaftEngine     (strong consistency)
//!                              ├─ CrdtEngine     (conflict-free)
//!                              └─ SyncEngine     (synchronous)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module         | Phase | Responsibility |
//! |----|----------------|-------|----------------|
//! | 1  | [`error`]      | 0     | `ReplicationError` thiserror enum |
//! | 2  | [`config`]     | 0     | `ReplicationConfig`, `Preset` |
//! | 3  | [`strategy`]   | 0     | `ReplicationStrategy`, `ConsistencyLevel` |
//! | 4  | [`replica`]    | 0     | `ReplicaId`, `ReplicaInfo` |
//! | 5  | [`liveness`]   | 0     | `ReplicaLiveness`, `ReplicaHealth` |
//! | 6  | [`stats`]      | 0     | `ReplicationStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]     | 0     | `HealthReport`, `HealthState` |
//! | 8  | [`event`]      | 0     | `ReplicationEvent` enum |
//! | 9  | [`builder`]    | 0     | `ReplicationServiceBuilder` (fluent) |
//! | 10 | [`gossip`]     | 1     | `GossipEngine` skeleton |
//! | 11 | [`raft`]       | 1     | `RaftEngine` skeleton |
//! | 12 | [`crdt`]       | 1     | `CrdtEngine` skeleton |
//! | 13 | [`sync`]       | 1     | `SyncEngine` skeleton |
//! | 14 | [`conflict`]   | 1     | `ConflictResolver` skeleton |
//!
//! # Phase 0 status
//!
//! Modules 1–9 are real implementations. Modules 10–14 ship as
//! typed skeletons. Functional engines land in Phase 1+.

#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod builder;
pub mod config;
pub mod conflict;
pub mod crdt;
pub mod error;
pub mod event;
pub mod gossip;
pub mod health;
pub mod liveness;
pub mod raft;
pub mod replica;
pub mod stats;
pub mod strategy;
pub mod sync;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::ReplicationServiceBuilder;
pub use config::{Preset, ReplicationConfig};
pub use conflict::{ConflictResolver, ConflictStrategy};
pub use crdt::CrdtEngine;
pub use error::ReplicationError;
pub use event::ReplicationEvent;
pub use gossip::GossipEngine;
pub use health::{HealthReport, HealthState};
pub use liveness::{ReplicaHealth, ReplicaLiveness};
pub use raft::RaftEngine;
pub use replica::{ReplicaId, ReplicaInfo};
pub use stats::{ReplicationStats, StatsSnapshot};
pub use strategy::{ConsistencyLevel, ReplicationStrategy};
pub use sync::SyncEngine;

// Re-export key adjacent-layer types for convenience.
pub use session_registry::node::{NodeId, Region, Zone};
