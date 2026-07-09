//! # event-bus
//!
//! **Partitioned event bus** for the Vireon application layer. Provides
//! topics, partitions, consumer groups, offset tracking, and partition
//! rebalancing.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Event ──► [Event Bus] ──► Consumer Group
//!                 │
//!                 ├─ TopicRegistry    (TopicId → Topic)
//!                 ├─ PartitionManager (partition assignment)
//!                 ├─ OffsetStore      (consumer progress)
//!                 ├─ Rebalancer       (partition distribution)
//!                 └─ ConsumerGroups  (group membership)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module        | Phase | Responsibility |
//! |----|---------------|-------|----------------|
//! | 1  | [`error`]     | 0 ✅  | `EventError` thiserror enum |
//! | 2  | [`config`]    | 0 ✅  | `EventBusConfig`, `Preset` |
//! | 3  | [`topic`]     | 0 ✅  | `TopicId`, `TopicConfig` |
//! | 4  | [`partition`] | 0 ✅  | `PartitionId`, `Offset`, `PartitionState` |
//! | 5  | [`consumer`]  | 0 ✅  | `ConsumerGroupId`, `ConsumerId`, `SubscriptionMode` |
//! | 6  | [`stats`]     | 0 ✅  | `EventBusStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]    | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]   | 0 ✅  | `EventBusBuilder` (fluent) |
//! | 9  | [`bus`]       | 1    | `EventBus` facade |
//! | 10 | [`offset_store`] | 1  | `OffsetStore` (consumer progress) |
//! | 11 | [`rebalancer`] | 1   | `PartitionRebalancer` (distribution) |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–11 ship as typed
//! skeletons. Functional bus + offset store + rebalancer land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod bus;
pub mod config;
pub mod consumer;
pub mod error;
pub mod health;
pub mod offset_store;
pub mod partition;
pub mod rebalancer;
pub mod stats;
pub mod topic;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::EventBusBuilder;
pub use bus::EventBus;
pub use config::{EventBusConfig, Preset};
pub use consumer::{ConsumerGroupId, ConsumerId, SubscriptionMode};
pub use error::EventError;
pub use health::{HealthReport, HealthState};
pub use offset_store::OffsetStore;
pub use partition::{Offset, PartitionId, PartitionState};
pub use rebalancer::PartitionRebalancer;
pub use stats::{EventBusStats, StatsSnapshot};
pub use topic::{TopicConfig, TopicId};

// Re-export key adjacent-layer types for convenience.
pub use session_manager::SessionId;
