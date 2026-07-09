//! # service-registry
//!
//! **Application-level Service Discovery** for the Vireon runtime.
//! Provides service registration, health tracking, endpoint management,
//! and load balancing for microservices architectures.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   ServiceRequest ──► [Service Registry] ──► ServiceEndpoint
//!                       │
//!                       ├─ ServiceRegistry   (registration & lookup)
//!                       ├─ LoadBalancer      (round-robin / least-loaded)
//!                       ├─ HealthTracker     (per-instance health scores)
//!                       ├─ EndpointManager   (address + port + weight)
//!                       └─ WatchSystem       (subscription & notification)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module         | Phase | Responsibility |
//! |----|----------------|-------|----------------|
//! | 1  | [`error`]      | 0 ✅  | `ServiceRegistryError` thiserror enum |
//! | 2  | [`config`]     | 0 ✅  | `ServiceRegistryConfig`, `Preset` |
//! | 3  | [`service`]    | 0 ✅  | `ServiceId`, `ServiceState`, `ServiceInfo` |
//! | 4  | [`endpoint`]   | 0 ✅  | `ServiceEndpoint` (network address + weight) |
//! | 5  | [`instance`]   | 0 ✅  | `ServiceInstance` (endpoint + health + load) |
//! | 6  | [`stats`]      | 0 ✅  | `ServiceRegistryStats`, `StatsSnapshot` |
//! | 7  | [`health`]     | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]    | 0 ✅  | `ServiceRegistryBuilder` (fluent) |
//! | 9  | [`registry`]   | 1    | `ServiceRegistry` facade |
//! | 10 | [`loadbalancer`] | 1  | `LoadBalancer` (round-robin / least-loaded) |
//! | 11 | [`watch`]      | 1    | `WatchHandle` (subscription system) |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–11 ship as typed
//! skeletons. Functional registry + load balancer + watch system land
//! in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod endpoint;
pub mod error;
pub mod health;
pub mod instance;
pub mod loadbalancer;
pub mod registry;
pub mod service;
pub mod stats;
pub mod watch;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::ServiceRegistryBuilder;
pub use config::{Preset, ServiceRegistryConfig};
pub use endpoint::ServiceEndpoint;
pub use error::ServiceRegistryError;
pub use health::{HealthReport, HealthState};
pub use instance::ServiceInstance;
pub use loadbalancer::LoadBalancer;
pub use registry::ServiceRegistry;
pub use service::{ServiceId, ServiceInfo, ServiceState};
pub use stats::{ServiceRegistryStats, StatsSnapshot};
pub use watch::WatchHandle;

// Re-export key adjacent-layer types for convenience.
pub use configuration_system::{ConfigKey, ConfigSource, ConfigValue};
pub use connection_shard::ConnectionId;
pub use session_manager::SessionId;
pub use session_registry::{NodeId, NodeInfo, NodeState, Region, Zone};
