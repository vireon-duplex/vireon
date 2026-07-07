//! # session-registry
//!
//! The **Distributed Session Directory** for the Vireon QUIC runtime.
//! Maps application-level `SessionId` to the cluster node that owns
//! it, using consistent hashing for uniform distribution and minimal
//! data movement on topology changes.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   SessionID ──► [Consistent Hash Ring] ──► Owner Node
//!                       │
//!                       ├─ MembershipList   (node join/leave/suspicion)
//!                       ├─ OwnershipTable   (SessionId → NodeId + generation)
//!                       ├─ LeaseManager     (per-session lease with TTL)
//!                       └─ SessionDirectory (facade)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module         | Phase | Responsibility |
//! |----|----------------|-------|----------------|
//! | 1  | [`error`]      | 0 ✅  | `RegistryError` thiserror enum |
//! | 2  | [`config`]     | 0 ✅  | `RegistryConfig`, `Preset` |
//! | 3  | [`node`]       | 0 ✅  | `NodeId`, `NodeInfo`, `NodeState`, `Region`, `Zone` |
//! | 4  | [`version`]    | 0 ✅  | `Generation`, `Epoch`, `MonotonicVersion` |
//! | 5  | [`lease`]      | 0 ✅  | `LeaseInfo`, `LeaseState` |
//! | 6  | [`locator`]    | 0 ✅  | `SessionLocator` (resolved location) |
//! | 7  | [`ownership`]  | 0 ✅  | `OwnershipEntry`, `OwnershipTable` skeleton |
//! | 8  | [`ring`]       | 0 ✅  | `ConsistentHash` ring (real impl) |
//! | 9  | [`stats`]      | 0 ✅  | `RegistryStats` atomics, `StatsSnapshot` |
//! | 10 | [`health`]     | 0 ✅  | `HealthReport`, `HealthState` |
//! | 11 | [`directory`]  | 1    | `SessionDirectory` facade |
//! | 12 | [`membership`] | 1    | `MembershipList` (cluster tracker) |
//! | 13 | [`rebalance`]  | 1    | `RebalancePlanner` |
//! | 14 | [`gc`]         | 1    | `RegistryGc` (expired lease cleanup) |
//! | 15 | [`builder`]    | 1    | `RegistryBuilder` (fluent) |
//! | 16 | [`event`]      | 1    | `RegistryEvent` enum |
//!
//! # Phase 0 status
//!
//! Modules 1–10 are real implementations (incl. the consistent hash
//! ring). Modules 11–16 ship as typed skeletons. Functional
//! directory + membership + lease renewal land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod directory;
pub mod error;
pub mod event;
pub mod gc;
pub mod health;
pub mod lease;
pub mod locator;
pub mod membership;
pub mod node;
pub mod ownership;
pub mod rebalance;
pub mod ring;
pub mod stats;
pub mod version;

// ── Re-exports ────────────────────────────────────────────────────

pub use config::{Preset, RegistryConfig};
pub use error::RegistryError;
pub use health::{HealthReport, HealthState};
pub use lease::{LeaseInfo, LeaseState};
pub use locator::SessionLocator;
pub use node::{NodeAddress, NodeCapacity, NodeId, NodeInfo, NodeState, Region, Zone};
pub use ownership::{OwnershipEntry, OwnershipTable};
pub use ring::{ConsistentHash, DEFAULT_VNODES, RingEntry};
pub use stats::{RegistryStats, StatsSnapshot};
pub use version::{Epoch, Generation, MonotonicVersion};

// Re-export key adjacent-layer types for convenience.
pub use connection_shard::ConnectionId;
pub use session_manager::SessionId;
