//! # resource-quotas
//!
//! The **Resource Quotas** system for the Vireon QUIC runtime. Provides
//! per-tenant resource quotas and enforcement: session/stream/bandwidth/
//! memory/request limits with atomic usage tracking.
//!
//! # Architecture (one-line)
//!
//! ```text
//! Tenant ─► [QuotaManager] ─► [QuotaEnforcer] ─► Resource Usage
//!             │                  │
//!             ├─ TenantId        ├─ Session Limits
//!             ├─ TenantTier      ├─ Bandwidth Limits
//!             ├─ QuotaLimit      ├─ Memory Limits
//!             └─ QuotaUsage      └─ Request Rate Limits
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Layer map
//!
//! | #  | Module          | Phase | Responsibility |
//! |----|-----------------|-------|----------------|
//! | 1  | [`error`]       | 0 ✅  | `QuotaError` thiserror enum |
//! | 2  | [`config`]      | 0 ✅  | `ResourceQuotasConfig`, `Preset` |
//! | 3  | [`tenant`]      | 0 ✅  | `TenantId`, `TenantTier` |
//! | 4  | [`limit`]       | 0 ✅  | `QuotaLimit` (Copy + bounds) |
//! | 5  | [`usage`]       | 0 ✅  | `QuotaUsage` (atomics + snapshot) |
//! | 6  | [`stats`]       | 0 ✅  | `ResourceQuotaStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]      | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]     | 0 ✅  | `ResourceQuotasBuilder` (fluent) |
//! | 9  | [`manager`]     | 1    | `QuotaManager` skeleton |
//! | 10 | [`enforcer`]    | 1    | `QuotaEnforcer` skeleton |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–10 ship as
//! typed skeletons. Functional manager + enforcer land in Phase 1.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod enforcer;
pub mod error;
pub mod health;
pub mod limit;
pub mod manager;
pub mod stats;
pub mod tenant;
pub mod usage;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::ResourceQuotasBuilder;
pub use config::{Preset, ResourceQuotasConfig};
pub use error::QuotaError;
pub use health::{HealthReport, HealthState};
pub use limit::QuotaLimit;
pub use stats::{ResourceQuotaStats, StatsSnapshot};
pub use tenant::{TenantId, TenantTier};
pub use usage::{QuotaUsage, UsageSnapshot};

// Re-export key adjacent-layer types for convenience.
pub use session_manager::{QuotaSnapshot, ResourceQuota, SessionClass};
