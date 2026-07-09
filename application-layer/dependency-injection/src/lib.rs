//! # dependency-injection
//!
//! **Dependency Injection Container** for the Vireon application layer.
//! Provides typed service tokens, lifetime management (singleton/scoped/transient),
//! and dependency graph resolution.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   ServiceToken ──► [DI Container] ──► Service Instance
//!                       │
//!                       ├─ ServiceToken<T>     (type-safe service identifiers)
//!                       ├─ BindingDescriptor   (service metadata + dependencies)
//!                       ├─ ServiceLifetime     (singleton/scoped/transient)
//!                       ├─ DependencyResolver  (graph resolution + cycle detection)
//!                       └─ Container           (registration + resolution facade)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module         | Phase | Responsibility |
//! |----|----------------|-------|----------------|
//! | 1  | [`error`]      | 0 ✅  | `DependencyError` thiserror enum |
//! | 2  | [`config`]     | 0 ✅  | `ContainerConfig`, `Preset` |
//! | 3  | [`token`]      | 0 ✅  | `TokenId`, `ServiceToken<T>` |
//! | 4  | [`lifetime`]   | 0 ✅  | `ServiceLifetime` enum |
//! | 5  | [`binding`]    | 0 ✅  | `BindingDescriptor` |
//! | 6  | [`stats`]      | 0 ✅  | `ContainerStats`, `StatsSnapshot` |
//! | 7  | [`health`]     | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]    | 0 ✅  | `ContainerBuilder` (fluent) |
//! | 9  | [`container`]  | 0 ✅  | `Container` facade (skeleton) |
//! | 10 | [`resolver`]   | 0 ✅  | `DependencyResolver` (skeleton) |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–10 ship as typed
//! skeletons. Functional container + resolver land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod binding;
pub mod builder;
pub mod config;
pub mod container;
pub mod error;
pub mod health;
pub mod lifetime;
pub mod resolver;
pub mod stats;
pub mod token;

// ── Re-exports ────────────────────────────────────────────────────

pub use binding::BindingDescriptor;
pub use builder::ContainerBuilder;
pub use config::{ContainerConfig, Preset};
pub use container::Container;
pub use error::DependencyError;
pub use health::{HealthReport, HealthState};
pub use lifetime::ServiceLifetime;
pub use resolver::DependencyResolver;
pub use stats::{ContainerStats, StatsSnapshot};
pub use token::{ServiceToken, TokenId};

// Re-export key adjacent-layer types for convenience.
pub use configuration_system::ConfigKey;
pub use service_registry::ServiceId;
