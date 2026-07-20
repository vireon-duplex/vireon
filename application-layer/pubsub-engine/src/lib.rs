//! # pubsub-engine
//!
//! **Pub/Sub engine** for the Vireon application layer. Provides topic
//! pattern matching, subscription management, and message delivery.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Message ──► [PubSubEngine] ──► Matching Subscriptions
//!                    │
//!                    ├─ PubSubConfig       (limits & timeouts)
//!                    ├─ TopicTrie           (pattern → subscription set)
//!                    ├─ PubSubStats         (atomic counters)
//!                    ├─ HealthReport        (derive from stats)
//!                    └─ PubSubBuilder       (fluent config)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module           | Phase | Responsibility |
//! |----|------------------|-------|----------------|
//! | 1  | [`error`]        | 0 ✅  | `PubSubError` thiserror enum |
//! | 2  | [`config`]       | 0 ✅  | `PubSubConfig`, `Preset` |
//! | 3  | [`subscription`] | 0 ✅  | `SubscriptionId`, `TopicPattern` |
//! | 4  | [`message`]      | 0 ✅  | `PubSubMessage` |
//! | 5  | [`stats`]        | 0 ✅  | `PubSubStats` atomics, `StatsSnapshot` |
//! | 6  | [`health`]       | 0 ✅  | `HealthReport`, `HealthState` |
//! | 7  | [`builder`]      | 0 ✅  | `PubSubBuilder` (fluent) |
//! | 8  | [`engine`]       | 1    | `PubSubEngine` facade |
//! | 9  | [`trie`]         | 1    | `TopicTrie` (pattern matching) |
//! | 10 | [`topic`]        | 1    | `Topic` (validated concrete topic name) |
//!
//! # Phase 0 status
//!
//! Modules 1–7 are real implementations. Modules 8–9 ship as typed
//! skeletons. Functional engine + trie land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod engine;
pub mod error;
pub mod health;
pub mod message;
pub mod registry;
pub mod stats;
pub mod subscription;
pub mod topic;
pub mod trie;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::PubSubBuilder;
pub use config::{Preset, PubSubConfig};
pub use engine::PubSubEngine;
pub use error::PubSubError;
pub use health::{HealthReport, HealthState};
pub use message::PubSubMessage;
pub use registry::{Subscriber, TopicCatalog, TopicConfig, TopicRegistry};
pub use stats::{PubSubStats, StatsSnapshot};
pub use subscription::{SubscriptionId, TopicPattern};
pub use topic::Topic;
pub use trie::TopicTrie;

// Re-export key adjacent-layer types for convenience.
pub use event_bus::TopicId;
