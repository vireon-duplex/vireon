//! # topic-acl
//!
//! Topic-pattern based access control for Vireon pub/sub.
//!
//! Phase 7 introduces per-topic authorization. The model:
//!
//! * Rules are tuples of `(pattern, principal?, actions, effect)`.
//! * `pattern` uses single-segment `*` wildcards (same semantics as
//!   `pubsub_engine::TopicPattern`).
//! * `principal = None` means the wildcard principal (matches anyone).
//! * `effect` is `Allow` or `Deny`. **Explicit deny wins** — any
//!   matching Deny rule short-circuits the check.
//! * Default when no rule matches is `Deny` (deny-by-default).
//!
//! # Example
//!
//! ```
//! use topic_acl::{AclTableBuilder, Action, ActionSet, AclDecision};
//! use auth_framework::PrincipalId;
//!
//! let mut pub_sub = ActionSet::NONE;
//! pub_sub.add(Action::Publish).add(Action::Subscribe);
//!
//! let table = AclTableBuilder::new()
//!     .allow("sensor.*", None, pub_sub)              // anyone can pub+sub on sensor.*
//!     .allow("admin.*", Some(PrincipalId::new(1)), ActionSet::ALL) // p#1 full control
//!     .deny("system.*", None, ActionSet::ALL)        // nobody touches system.*
//!     .build()
//!     .unwrap();
//!
//! // Anonymous user publishes to sensor.temp — allowed.
//! assert_eq!(table.check(PrincipalId::new(99), Action::Publish, "sensor.temp"),
//!            AclDecision::Allow);
//!
//! // Anyone trying to delete system.logs — explicitly denied.
//! assert!(matches!(table.check(PrincipalId::new(1), Action::TopicDelete, "system.logs"),
//!                  AclDecision::Deny { .. }));
//! ```
//!
//! # Algorithm
//!
//! `check` walks every rule in insertion order:
//! 1. If any rule matches with `effect == Deny` → `Deny { ExplicitDeny }`.
//! 2. Else if any rule matches with `effect == Allow` → `Allow`.
//! 3. Else if any rule partially matched (pattern+principal but not
//!    action) → `Deny { ActionNotInAllowSet }`.
//! 4. Else → `Deny { NoMatch }`.
//!
//! A rule "matches" iff `pattern_matches(rule.pattern, topic)` AND
//! (`rule.principal.is_none()` OR `rule.principal == Some(principal)`)
//! AND `rule.actions.contains(action)`.

pub mod action;
pub mod builder;
pub mod error;
pub mod pattern;
pub mod rule;
pub mod table;

// ── Re-exports ────────────────────────────────────────────────────

pub use action::{Action, ActionSet};
pub use builder::AclTableBuilder;
pub use error::AclError;
pub use pattern::pattern_matches;
pub use rule::{AclEffect, AclRule, MAX_PATTERN_LEN};
pub use table::{AclDecision, AclTable, DenyReason};

// Re-export PrincipalId so callers can build ACLs without depending
// on auth-framework directly.
pub use auth_framework::PrincipalId;
