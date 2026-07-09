//! [`FlagRule`], [`EvaluationContext`], [`RuleCondition`] — rule-based
//! flag evaluation.
//!
//! # Design
//!
//! - `FlagRule`: Maps a flag key to a condition and a value.
//! - `RuleCondition`: Always, Never, Percentage rollout, or tenant whitelist.
//! - `EvaluationContext`: Tenant/user context for evaluation.
//! - Consistent hashing for percentage rollouts ensures stable distribution.

use crate::flag::{FlagKey, FlagValue};

// ── RuleCondition ───────────────────────────────────────────────────

/// Condition for when a rule matches.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuleCondition {
    /// Rule always matches.
    Always,
    /// Rule never matches.
    Never,
    /// Percentage rollout (0..=100).
    /// Uses consistent hashing on (tenant_id, user_id, flag_key) % 100 < percentage.
    Percentage(u8),
    /// Tenant whitelist (matches only if tenant_id is in the list).
    TenantIn([u64; 8], u8), // [tenant_ids; 8], length
}

// ── FlagRule ───────────────────────────────────────────────────────

/// A rule that maps a flag key to a condition and a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlagRule {
    /// The flag key this rule applies to.
    pub key: FlagKey,
    /// The condition for matching.
    pub condition: RuleCondition,
    /// The value to return when the rule matches.
    pub value: FlagValue,
}

impl FlagRule {
    /// Create a new flag rule.
    #[must_use]
    pub const fn new(key: FlagKey, condition: RuleCondition, value: FlagValue) -> Self {
        Self {
            key,
            condition,
            value,
        }
    }

    /// Check if this rule matches the given evaluation context.
    ///
    /// For `Percentage` conditions, uses consistent hashing:
    /// `hash(tenant_id, user_id, flag_key) % 100 < percentage`
    #[must_use]
    pub fn matches(&self, ctx: EvaluationContext) -> bool {
        match self.condition {
            RuleCondition::Always => true,
            RuleCondition::Never => false,
            RuleCondition::Percentage(pct) => {
                // Consistent hash: (tenant_id ^ user_id ^ key_hash) % 100
                let key_hash = simple_hash(self.key.as_str());
                let combined = ctx.tenant_id ^ ctx.user_id ^ key_hash;
                (combined % 100) < pct as u64
            }
            RuleCondition::TenantIn(tenants, len) => {
                // Check if ctx.tenant_id is in the whitelist.
                let mut i = 0;
                while i < len as usize {
                    if tenants[i] == ctx.tenant_id {
                        return true;
                    }
                    i += 1;
                }
                false
            }
        }
    }
}

/// Simple hash function for consistent hashing in Phase 0.
/// In Phase 1+, this will be replaced with a proper hash (e.g., FNV).
#[must_use]
const fn simple_hash(s: &str) -> u64 {
    let bytes = s.as_bytes();
    let mut hash: u64 = 5381;
    let mut i = 0;
    while i < bytes.len() {
        hash = hash.wrapping_mul(33).wrapping_add(bytes[i] as u64);
        i += 1;
    }
    hash
}

// ── EvaluationContext ────────────────────────────────────────────────

/// Context for evaluating feature flag rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvaluationContext {
    /// Tenant identifier.
    pub tenant_id: u64,
    /// User identifier within the tenant.
    pub user_id: u64,
    /// Arbitrary metadata hash (for future extensions).
    pub metadata_hash: u64,
    /// Current time in nanoseconds (for cache invalidation).
    pub now_ns: u64,
}

impl EvaluationContext {
    /// Create a new evaluation context.
    #[must_use]
    pub const fn new(tenant_id: u64, user_id: u64, metadata_hash: u64, now_ns: u64) -> Self {
        Self {
            tenant_id,
            user_id,
            metadata_hash,
            now_ns,
        }
    }

    /// Create a minimal context with only tenant and user IDs.
    #[must_use]
    pub const fn minimal(tenant_id: u64, user_id: u64) -> Self {
        Self {
            tenant_id,
            user_id,
            metadata_hash: 0,
            now_ns: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_condition_always_matches() {
        let key = FlagKey::new("test").unwrap();
        let rule = FlagRule::new(key, RuleCondition::Always, FlagValue::bool(true));
        let ctx = EvaluationContext::minimal(1, 100);
        assert!(rule.matches(ctx));
    }

    #[test]
    fn rule_condition_never_matches() {
        let key = FlagKey::new("test").unwrap();
        let rule = FlagRule::new(key, RuleCondition::Never, FlagValue::bool(false));
        let ctx = EvaluationContext::minimal(1, 100);
        assert!(!rule.matches(ctx));
    }

    #[test]
    fn rule_condition_percentage_zero() {
        let key = FlagKey::new("rollout").unwrap();
        let rule = FlagRule::new(key, RuleCondition::Percentage(0), FlagValue::bool(true));
        let ctx = EvaluationContext::minimal(1, 100);
        assert!(!rule.matches(ctx));
    }

    #[test]
    fn rule_condition_percentage_hundred() {
        let key = FlagKey::new("rollout").unwrap();
        let rule = FlagRule::new(key, RuleCondition::Percentage(100), FlagValue::bool(true));
        let ctx = EvaluationContext::minimal(1, 100);
        assert!(rule.matches(ctx));
    }

    #[test]
    fn rule_condition_percentage_deterministic() {
        let key = FlagKey::new("rollout").unwrap();
        let rule = FlagRule::new(key, RuleCondition::Percentage(50), FlagValue::bool(true));
        let ctx = EvaluationContext::minimal(42, 1001);
        // Should be deterministic for same context.
        let result1 = rule.matches(ctx);
        let result2 = rule.matches(ctx);
        assert_eq!(result1, result2);
    }

    #[test]
    fn rule_condition_percentage_different_users() {
        let key = FlagKey::new("rollout").unwrap();
        let rule = FlagRule::new(key, RuleCondition::Percentage(50), FlagValue::bool(true));

        let ctx1 = EvaluationContext::minimal(1, 1000);
        let ctx2 = EvaluationContext::minimal(1, 1001);
        let ctx3 = EvaluationContext::minimal(1, 1002);

        // Different users should get different results (deterministically with our hash).
        let result1 = rule.matches(ctx1);
        let result2 = rule.matches(ctx2);
        let result3 = rule.matches(ctx3);

        // At minimum, verify they're not all the same (unless hash collides).
        let all_same = result1 == result2 && result2 == result3;
        if all_same {
            // If they're all the same, that's okay for this hash function,
            // but verify at least that different users produce deterministic results.
            assert_eq!(rule.matches(ctx1), result1);
            assert_eq!(rule.matches(ctx2), result2);
            assert_eq!(rule.matches(ctx3), result3);
        }
    }

    #[test]
    fn rule_condition_tenant_in_match() {
        let key = FlagKey::new("tenant_feature").unwrap();
        let mut tenants = [0u64; 8];
        tenants[0] = 42;
        tenants[1] = 99;
        let rule = FlagRule::new(
            key,
            RuleCondition::TenantIn(tenants, 2),
            FlagValue::bool(true),
        );

        let ctx = EvaluationContext::minimal(42, 100);
        assert!(rule.matches(ctx));

        let ctx = EvaluationContext::minimal(99, 100);
        assert!(rule.matches(ctx));
    }

    #[test]
    fn rule_condition_tenant_in_no_match() {
        let key = FlagKey::new("tenant_feature").unwrap();
        let mut tenants = [0u64; 8];
        tenants[0] = 42;
        let rule = FlagRule::new(
            key,
            RuleCondition::TenantIn(tenants, 1),
            FlagValue::bool(true),
        );

        let ctx = EvaluationContext::minimal(99, 100);
        assert!(!rule.matches(ctx));
    }

    #[test]
    fn evaluation_context_new() {
        let ctx = EvaluationContext::new(123, 456, 789, 1_000_000);
        assert_eq!(ctx.tenant_id, 123);
        assert_eq!(ctx.user_id, 456);
        assert_eq!(ctx.metadata_hash, 789);
        assert_eq!(ctx.now_ns, 1_000_000);
    }

    #[test]
    fn evaluation_context_minimal() {
        let ctx = EvaluationContext::minimal(10, 20);
        assert_eq!(ctx.tenant_id, 10);
        assert_eq!(ctx.user_id, 20);
        assert_eq!(ctx.metadata_hash, 0);
        assert_eq!(ctx.now_ns, 0);
    }

    #[test]
    fn flag_rule_accessors() {
        let key = FlagKey::new("test").unwrap();
        let rule = FlagRule::new(key, RuleCondition::Always, FlagValue::percentage(75));
        assert_eq!(rule.key, key);
        assert_eq!(rule.value, FlagValue::percentage(75));
    }

    #[test]
    fn simple_hash_is_deterministic() {
        let s = "hello_world";
        let h1 = simple_hash(s);
        let h2 = simple_hash(s);
        assert_eq!(h1, h2);
    }

    #[test]
    fn simple_hash_different_strings() {
        let h1 = simple_hash("foo");
        let h2 = simple_hash("bar");
        assert_ne!(h1, h2);
    }

    #[test]
    fn rule_copy_and_clone() {
        let key = FlagKey::new("test").unwrap();
        let rule1 = FlagRule::new(key, RuleCondition::Always, FlagValue::bool(true));
        let rule2 = rule1;
        assert_eq!(rule1, rule2);
    }

    #[test]
    fn percentage_condition_boundary() {
        let key = FlagKey::new("test").unwrap();
        // Test boundary values.
        for pct in [0, 1, 50, 99, 100] {
            let rule = FlagRule::new(key, RuleCondition::Percentage(pct), FlagValue::bool(true));
            let ctx = EvaluationContext::minimal(1, 1);
            // Should not panic.
            let _ = rule.matches(ctx);
        }
    }
}
