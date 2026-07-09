//! # auth-framework
//!
//! **Authentication and Authorization Framework** for the Vireon runtime.
//! Provides pluggable auth methods, RBAC/ABAC, token management,
//! and permission checking.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Auth Request ──► [Auth Framework] ──► Auth Decision
//!                       │
//!                       ├─ AuthenticationPipeline (credential verification)
//!                       ├─ AuthorizationEngine (RBAC/ABAC)
//!                       ├─ TokenManager (issue/validate/revoke)
//!                       ├─ RoleStore (role definitions)
//!                       └─ PermissionStore (access control)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module             | Phase | Responsibility |
//! |----|--------------------|-------|----------------|
//! | 1  | [`error`]          | 0 ✅  | `AuthError` thiserror enum |
//! | 2  | [`config`]         | 0 ✅  | `AuthConfig`, `Preset` |
//! | 3  | [`principal`]      | 0 ✅  | `PrincipalId`, `PrincipalType` |
//! | 4  | [`credential`]     | 0 ✅  | `Credential`, `CredentialType` |
//! | 5  | [`permission`]     | 0 ✅  | `Permission`, `PermissionSet` |
//! | 6  | [`role`]           | 0 ✅  | `Role`, `RoleAssignment` |
//! | 7  | [`stats`]          | 0 ✅  | `AuthStats`, `StatsSnapshot` |
//! | 8  | [`health`]         | 0 ✅  | `HealthReport`, `HealthState` |
//! | 9  | [`builder`]        | 0 ✅  | `AuthFrameworkBuilder` (fluent) |
//! | 10 | [`authenticator`]  | 1    | `AuthenticationPipeline` facade |
//! | 11 | [`authorizer`]     | 1    | `AuthorizationEngine` facade |
//! | 12 | [`token_manager`]  | 1    | `TokenManager` facade |
//!
//! # Phase 0 status
//!
//! Modules 1–9 are real implementations. Modules 10–12 ship as typed
//! skeletons. Functional auth + token manager land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod authenticator;
pub mod authorizer;
pub mod builder;
pub mod config;
pub mod credential;
pub mod error;
pub mod health;
pub mod permission;
pub mod principal;
pub mod role;
pub mod stats;
pub mod token_manager;

// ── Re-exports ────────────────────────────────────────────────────

pub use authenticator::AuthenticationPipeline;
pub use authorizer::AuthorizationEngine;
pub use builder::AuthFrameworkBuilder;
pub use config::{AuthConfig, Preset};
pub use credential::{Credential, CredentialType};
pub use error::AuthError;
pub use health::{HealthReport, HealthState};
pub use permission::{Permission, PermissionSet};
pub use principal::{PrincipalId, PrincipalType};
pub use role::{Role, RoleAssignment};
pub use stats::{AuthStats, StatsSnapshot};
pub use token_manager::TokenManager;

// Re-export key adjacent-layer types for convenience.
pub use configuration_system::{ConfigKey, ConfigSource, ConfigValue};
pub use session_manager::SessionId;
