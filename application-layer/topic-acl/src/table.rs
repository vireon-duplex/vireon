//! [`AclTable`] — the table of rules + the [`AclTable::check`] algorithm.

use auth_framework::PrincipalId;

use crate::action::Action;
use crate::pattern::pattern_matches;
use crate::rule::AclRule;

/// Decision returned by [`AclTable::check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclDecision {
    /// The operation is permitted.
    Allow,
    /// The operation is forbidden. `reason` is useful for audit logs.
    Deny {
        /// Why the check denied.
        reason: DenyReason,
    },
}

impl AclDecision {
    /// `true` if this is `Allow`.
    #[must_use]
    pub const fn is_allow(self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// Why an [`AclDecision::Deny`] was reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenyReason {
    /// No rule matched the (principal, action, topic) triple at all —
    /// deny-by-default.
    NoMatch,
    /// A `Deny` rule matched. Explicit denies override allows.
    ExplicitDeny,
    /// At least one Allow rule matched the (principal, topic) but
    /// none of them included the requested action in their
    /// [`crate::ActionSet`].
    ActionNotInAllowSet,
}

/// In-memory topic-ACL table.
///
/// ## Algorithm
///
/// `check` walks every rule in insertion order. The decision is:
/// 1. If any matching **Deny** rule is found → `Deny { ExplicitDeny }`.
/// 2. Else if any matching **Allow** rule is found → `Allow`.
/// 3. Else if any rule matched the (principal, topic) but not the
///    action → `Deny { ActionNotInAllowSet }`.
/// 4. Else → `Deny { NoMatch }` (deny-by-default).
///
/// A rule "matches" iff
/// * `pattern_matches(rule.pattern, topic)`, AND
/// * `rule.principal.is_none() || rule.principal == Some(principal)`, AND
/// * `rule.actions.contains(action)` (for steps 1–2).
#[derive(Clone, Default, Debug)]
pub struct AclTable {
    rules: Vec<AclRule>,
}

impl AclTable {
    /// Construct empty. Every check returns `Deny { NoMatch }`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a rule. Validates the rule's pattern + actions (these
    /// are also validated by [`AclRule::allow`] / [`AclRule::deny`],
    /// but a defensive re-check here catches hand-constructed rules).
    ///
    /// # Errors
    /// Propagates [`crate::AclError`] if the rule is malformed.
    pub fn add(&mut self, rule: AclRule) -> Result<(), crate::AclError> {
        if rule.pattern.is_empty() {
            return Err(crate::AclError::EmptyPattern);
        }
        if rule.pattern.len() > crate::rule::MAX_PATTERN_LEN {
            return Err(crate::AclError::OverlongPattern);
        }
        if rule.actions.is_empty() {
            return Err(crate::AclError::EmptyActionSet);
        }
        self.rules.push(rule);
        Ok(())
    }

    /// Remove every rule with the given `(pattern, principal)` pair.
    /// Returns the count removed. `principal = None` matches rules
    /// whose principal is the wildcard (`None`).
    pub fn remove(&mut self, pattern: &str, principal: Option<PrincipalId>) -> usize {
        let before = self.rules.len();
        self.rules
            .retain(|r| !(r.pattern == pattern && r.principal == principal));
        before - self.rules.len()
    }

    /// Decision for `(principal, action, topic)`.
    #[must_use]
    pub fn check(&self, principal: PrincipalId, action: Action, topic: &str) -> AclDecision {
        let mut any_allow_match = false;
        let mut any_principal_topic_match = false;
        for rule in &self.rules {
            if !pattern_matches(&rule.pattern, topic) {
                continue;
            }
            // Principal match: rule.principal = None means wildcard.
            if rule.principal.is_some() && rule.principal != Some(principal) {
                continue;
            }
            // From here, (pattern, principal) matched.
            if !rule.actions.contains(action) {
                // Action wasn't in this rule's set, but the rule
                // still partially matched. Remember this for the
                // ActionNotInAllowSet reason.
                any_principal_topic_match = true;
                continue;
            }
            // Full match.
            match rule.effect {
                crate::rule::AclEffect::Deny => {
                    return AclDecision::Deny {
                        reason: DenyReason::ExplicitDeny,
                    };
                }
                crate::rule::AclEffect::Allow => any_allow_match = true,
            }
        }
        if any_allow_match {
            AclDecision::Allow
        } else if any_principal_topic_match {
            AclDecision::Deny {
                reason: DenyReason::ActionNotInAllowSet,
            }
        } else {
            AclDecision::Deny {
                reason: DenyReason::NoMatch,
            }
        }
    }

    /// Convenience wrapper: `check(...).is_allow()`.
    #[must_use]
    pub fn is_allowed(&self, principal: PrincipalId, action: Action, topic: &str) -> bool {
        self.check(principal, action, topic).is_allow()
    }

    /// Total rule count.
    #[must_use]
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Read-only view of all rules.
    #[must_use]
    pub fn rules(&self) -> &[AclRule] {
        &self.rules
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::action::{Action, ActionSet};
    use crate::rule::AclRule;

    fn p(id: u64) -> PrincipalId {
        PrincipalId::new(id)
    }

    // ── Empty table ──────────────────────────────────────────────

    #[test]
    fn empty_table_denies_everything() {
        let t = AclTable::new();
        assert_eq!(t.rule_count(), 0);
        for a in Action::ALL {
            assert_eq!(
                t.check(p(1), *a, "any.topic"),
                AclDecision::Deny {
                    reason: DenyReason::NoMatch
                }
            );
        }
    }

    // ── Single allow rule ────────────────────────────────────────

    #[test]
    fn allow_rule_matches_when_everything_aligns() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        assert_eq!(
            t.check(p(1), Action::Publish, "sensor.temp"),
            AclDecision::Allow
        );
    }

    #[test]
    fn mismatched_action_returns_action_not_in_allow_set() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        // Same pattern + principal, but Subscribe not in set.
        assert_eq!(
            t.check(p(1), Action::Subscribe, "sensor.temp"),
            AclDecision::Deny {
                reason: DenyReason::ActionNotInAllowSet
            }
        );
    }

    #[test]
    fn mismatched_principal_returns_no_match() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        assert_eq!(
            t.check(p(2), Action::Publish, "sensor.temp"),
            AclDecision::Deny {
                reason: DenyReason::NoMatch
            }
        );
    }

    #[test]
    fn mismatched_topic_returns_no_match() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        assert_eq!(
            t.check(p(1), Action::Publish, "alerts.fire"),
            AclDecision::Deny {
                reason: DenyReason::NoMatch
            }
        );
    }

    // ── Wildcards ────────────────────────────────────────────────

    #[test]
    fn wildcard_principal_matches_any_principal() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", None, ActionSet::single(Action::Subscribe)).unwrap())
            .unwrap();
        for id in 1..=5 {
            assert_eq!(
                t.check(p(id), Action::Subscribe, "sensor.temp"),
                AclDecision::Allow,
                "principal {id} should be allowed via wildcard"
            );
        }
    }

    #[test]
    fn wildcard_pattern_matches_topics() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        for topic in &["sensor.temp", "sensor.humidity", "sensor.pressure"] {
            assert_eq!(t.check(p(1), Action::Publish, topic), AclDecision::Allow);
        }
        // But not multi-segment.
        assert_eq!(
            t.check(p(1), Action::Publish, "sensor.temp.room1"),
            AclDecision::Deny {
                reason: DenyReason::NoMatch
            }
        );
    }

    // ── Explicit deny wins ───────────────────────────────────────

    #[test]
    fn explicit_deny_overrides_allow_even_when_allow_comes_first() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        t.add(
            AclRule::deny(
                "sensor.critical",
                Some(p(1)),
                ActionSet::single(Action::Publish),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            t.check(p(1), Action::Publish, "sensor.temp"),
            AclDecision::Allow
        );
        assert_eq!(
            t.check(p(1), Action::Publish, "sensor.critical"),
            AclDecision::Deny {
                reason: DenyReason::ExplicitDeny
            }
        );
    }

    #[test]
    fn explicit_deny_via_wildcard_principal() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("*.*", None, ActionSet::ALL).unwrap())
            .unwrap();
        t.add(AclRule::deny("system.*", None, ActionSet::single(Action::TopicDelete)).unwrap())
            .unwrap();
        // system.<x> deny TopicDelete for everyone (wildcard principal).
        assert_eq!(
            t.check(p(99), Action::TopicDelete, "system.logs"),
            AclDecision::Deny {
                reason: DenyReason::ExplicitDeny
            }
        );
        // Other actions on system.* are allowed by the broad allow.
        assert_eq!(
            t.check(p(99), Action::Publish, "system.logs"),
            AclDecision::Allow
        );
        // TopicDelete on other two-segment topics is allowed.
        assert_eq!(
            t.check(p(99), Action::TopicDelete, "user.data"),
            AclDecision::Allow
        );
    }

    // ── Multiple allow rules ─────────────────────────────────────

    #[test]
    fn multiple_allow_rules_any_match_permits() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        t.add(
            AclRule::allow("alerts.*", Some(p(1)), ActionSet::single(Action::Subscribe)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            t.check(p(1), Action::Publish, "sensor.temp"),
            AclDecision::Allow
        );
        assert_eq!(
            t.check(p(1), Action::Subscribe, "alerts.fire"),
            AclDecision::Allow
        );
        // Cross-action-topic mismatch.
        assert_eq!(
            t.check(p(1), Action::Subscribe, "sensor.temp"),
            AclDecision::Deny {
                reason: DenyReason::ActionNotInAllowSet
            }
        );
    }

    // ── Action set composition ───────────────────────────────────

    #[test]
    fn multi_action_set_in_one_rule() {
        let mut t = AclTable::new();
        let mut set = ActionSet::NONE;
        set.add(Action::Publish).add(Action::Subscribe);
        t.add(AclRule::allow("t.*", Some(p(1)), set).unwrap())
            .unwrap();
        assert_eq!(t.check(p(1), Action::Publish, "t.x"), AclDecision::Allow);
        assert_eq!(t.check(p(1), Action::Subscribe, "t.x"), AclDecision::Allow);
        assert_eq!(
            t.check(p(1), Action::TopicCreate, "t.x"),
            AclDecision::Deny {
                reason: DenyReason::ActionNotInAllowSet
            }
        );
    }

    // ── is_allowed ───────────────────────────────────────────────

    #[test]
    fn is_allowed_agrees_with_check() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        assert!(t.is_allowed(p(1), Action::Publish, "sensor.temp"));
        assert!(!t.is_allowed(p(1), Action::Subscribe, "sensor.temp"));
        assert!(!t.is_allowed(p(2), Action::Publish, "sensor.temp"));
    }

    // ── remove ───────────────────────────────────────────────────

    #[test]
    fn remove_returns_count_and_updates_table() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::ALL).unwrap())
            .unwrap();
        t.add(AclRule::allow("sensor.*", Some(p(1)), ActionSet::ALL).unwrap())
            .unwrap();
        t.add(AclRule::allow("sensor.*", Some(p(2)), ActionSet::ALL).unwrap())
            .unwrap();
        assert_eq!(t.rule_count(), 3);

        let removed = t.remove("sensor.*", Some(p(1)));
        assert_eq!(removed, 2);
        assert_eq!(t.rule_count(), 1);
        // p(2)'s rule still present.
        assert_eq!(
            t.check(p(2), Action::Publish, "sensor.temp"),
            AclDecision::Allow
        );
    }

    #[test]
    fn remove_unknown_returns_zero() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("t", Some(p(1)), ActionSet::ALL).unwrap())
            .unwrap();
        assert_eq!(t.remove("other", Some(p(1))), 0);
        assert_eq!(t.remove("t", Some(p(99))), 0);
    }

    #[test]
    fn remove_wildcard_principal_only_matches_wildcard_rules() {
        let mut t = AclTable::new();
        t.add(AclRule::allow("t", None, ActionSet::ALL).unwrap())
            .unwrap();
        t.add(AclRule::allow("t", Some(p(1)), ActionSet::ALL).unwrap())
            .unwrap();
        // remove(principal=None) should only drop the wildcard rule.
        assert_eq!(t.remove("t", None), 1);
        assert_eq!(t.rule_count(), 1);
        assert_eq!(t.rules()[0].principal, Some(p(1)));
    }

    // ── add validation ───────────────────────────────────────────

    #[test]
    fn add_rejects_malformed_rule() {
        let mut t = AclTable::new();
        // Construct a rule with an empty pattern via internal new
        // (the public AclRule::allow would have rejected already).
        let bad = AclRule {
            pattern: String::new(),
            principal: None,
            actions: ActionSet::ALL,
            effect: crate::rule::AclEffect::Allow,
        };
        assert!(t.add(bad).is_err());
    }

    #[test]
    fn rules_view_round_trip() {
        let mut t = AclTable::new();
        let r = AclRule::allow("t", Some(p(1)), ActionSet::ALL).unwrap();
        t.add(r.clone()).unwrap();
        assert_eq!(t.rules().len(), 1);
        assert_eq!(t.rules()[0], r);
    }
}
