//! # feature-flags
//!
//! **Feature flag system** for the Vireon application layer.
//!
//! Provides typed flag keys, typed values (Bool/Percentage/String),
//! and rule-based evaluation with tenant/user context.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   FlagKey ──► [FlagManager] ──► FlagValue
//!                 │
//!                 ├─ FlagRule (conditions: Always, Never, Percentage, TenantIn)
//!                 ├─ EvaluationContext (tenant_id, user_id, metadata)
//!                 └─ Evaluation cache with TTL
//! ```
//!
//! # Design goals
//!
//! - **Type-safe flag keys**: 63-byte ASCII identifiers with compile-time guarantees.
//! - **Typed flag values**: Bool, Percentage (0..=100), and String (up to 127 bytes).
//! - **Rule-based evaluation**: Percentage rollout, tenant whitelists, always/never toggles.
//! - **Evaluation context**: Tenant-scoped and user-scoped flag checks with consistent hashing.
//! - **Performance**: Atomic stats, cache-friendly layout, bounded evaluation time.
//! - **Observability**: StatsSnapshots, health reports, and per-flag metrics.
//!
//! # Phase 0 status
//!
//! Real implementations for:
//! - `FlagKey`, `FlagValue`, `FlagState` (types and predicates)
//! - `FlagRule`, `EvaluationContext` (rule matching with percentage hash)
//! - `FeatureFlagsConfig`, `Preset` (validation and presets)
//! - `FeatureFlagsStats`, `StatsSnapshot` (atomic counters)
//! - `HealthReport`, `HealthState` (health classification)
//! - `FeatureFlagsBuilder` (fluent config builder)
//!
//! Skeleton facades for:
//! - `FeatureFlagManager` (registry + evaluation with cache)
//! - `FlagProvider` (trait for flag state sources)
//!
//! Functional manager and provider land in Phase 1+.
//!
//! # Example
//!
//! ```rust
//! use feature_flags::{FlagKey, FlagValue, FlagRule, EvaluationContext, FeatureFlagsConfig, Preset};
//!
//! // Define a flag key
//! let key = FlagKey::new("new_api_enabled").unwrap();
//!
//! // Create a rule: 20% rollout for tenant 42
//! let rule = FlagRule::new(
//!     key,
//!     feature_flags::RuleCondition::Percentage(20),
//!     FlagValue::Bool(true)
//! );
//!
//! // Evaluate in context
//! let ctx = EvaluationContext::new(42, 1001, 0, 0);
//! let enabled = rule.matches(ctx);
//!
//! // Create a manager from config
//! let cfg = Preset::Default.to_config();
//! let mut manager = feature_flags::FeatureFlagManager::new(cfg).unwrap();
//! ```

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod error;
pub mod eval;
pub mod flag;
pub mod health;
pub mod manager;
pub mod provider;
pub mod rule;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::FeatureFlagsBuilder;
pub use config::{FeatureFlagsConfig, Preset};
pub use error::FlagError;
pub use eval::{EvaluationContext, FlagRule, RuleCondition};
pub use flag::{FlagKey, FlagState, FlagValue};
pub use health::{HealthReport, HealthState};
pub use manager::FeatureFlagManager;
pub use provider::{FlagProvider, StaticProvider};
pub use stats::{FeatureFlagsStats, StatsSnapshot};
