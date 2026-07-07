//! # session-router
//!
//! The **Session Routing Layer** for the Vireon QUIC runtime.
//! Resolves a `SessionId` to a [`RoutingDecision`] — the next hop
//! for a datagram: local delivery, remote forward, anycast,
//! broadcast, or multicast group fan-out.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   SessionID ──► [RouteResolver] ──► RoutingDecision
//!                    │
//!                    ├─ RoutingCache     (TTL, hit/miss, capacity)
//!                    ├─ LocalRouter      (is this session ours?)
//!                    ├─ RemoteRouter     (forward to target node)
//!                    ├─ AnycastRouter    (pick healthiest node)
//!                    ├─ BroadcastRouter  (fan-out to all)
//!                    ├─ MulticastRouter  (fan-out to group)
//!                    └─ RouterStats      (observability)
//! ```
//!
//! See `DESIGN.md` for the full 17-section specification.
//!
//! # Layer map
//!
//! | #  | Module        | Phase | Responsibility |
//! |----|---------------|-------|----------------|
//! | 1  | [`error`]     | 0     | `RouterError` thiserror enum |
//! | 2  | [`config`]    | 0     | `RouterConfig`, `Preset` |
//! | 3  | [`decision`]  | 0     | `RoutingDecision`, `RoutingMode`, `MulticastGroup` |
//! | 4  | [`endpoint`]  | 0     | `NodeEndpoint`, `EndpointProtocol` |
//! | 5  | [`cache`]     | 0     | `RoutingCacheStats`, `RoutingCache` skeleton |
//! | 6  | [`stats`]     | 0     | `RouterStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]    | 0     | `HealthReport`, `HealthState` |
//! | 8  | [`local`]     | 1     | `LocalRouter` |
//! | 9  | [`remote`]    | 1     | `RemoteRouter` |
//! | 10 | [`anycast`]   | 1     | `AnycastRouter` |
//! | 11 | [`broadcast`] | 1     | `BroadcastRouter` |
//! | 12 | [`multicast`] | 1     | `MulticastRouter` |
//! | 13 | [`resolver`]  | 1     | `RouteResolver` facade |
//! | 14 | [`builder`]   | 1     | `RouterBuilder` |
//! | 15 | [`event`]     | 1     | `RouterEvent` enum |
//!
//! # Phase 0 status
//!
//! Modules 1-7 are real implementations. Modules 8-15 ship as typed
//! skeletons returning stub errors. Functional routing lands in Phase 1+.

#![allow(clippy::module_inception)]

pub mod anycast;
pub mod broadcast;
pub mod builder;
pub mod cache;
pub mod config;
pub mod decision;
pub mod endpoint;
pub mod error;
pub mod event;
pub mod health;
pub mod local;
pub mod multicast;
pub mod remote;
pub mod resolver;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use cache::{CacheStatsSnapshot, RoutingCache, RoutingCacheStats};
pub use config::{Preset, RouterConfig};
pub use decision::{MulticastGroup, RoutingDecision, RoutingMode};
pub use endpoint::{EndpointProtocol, NodeEndpoint};
pub use error::RouterError;
pub use health::{HealthReport, HealthState};
pub use stats::{RouterStats, StatsSnapshot};

// Re-export key adjacent-layer types for convenience.
pub use session_manager::SessionId;
pub use session_registry::NodeId;
