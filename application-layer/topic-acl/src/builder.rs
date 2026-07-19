//! [`AclTableBuilder`] — fluent construction of an [`AclTable`].

use auth_framework::PrincipalId;

use crate::AclError;
use crate::action::ActionSet;
use crate::rule::AclRule;
use crate::table::AclTable;

/// Fluent builder. Accumulates rules and validates on [`Self::build`].
///
/// # Example
/// ```
/// use topic_acl::{AclTableBuilder, Action, ActionSet, AclDecision};
/// use auth_framework::PrincipalId;
///
/// let mut pub_only = ActionSet::NONE;
/// pub_only.add(Action::Publish);
///
/// let table = AclTableBuilder::new()
///     .allow("sensor.*", None, pub_only)            // anyone can publish
///     .allow("alerts.*", Some(PrincipalId::new(1)), ActionSet::ALL) // p1 full control
///     .deny("system.*", None, ActionSet::ALL)       // nobody touches system.*
///     .build()
/// .unwrap();
///
/// assert_eq!(table.rule_count(), 3);
/// ```
#[derive(Debug, Default)]
pub struct AclTableBuilder {
    table: AclTable,
}

impl AclTableBuilder {
    /// Start with no rules.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Push an Allow rule.
    pub fn allow(
        mut self,
        pattern: &str,
        principal: Option<PrincipalId>,
        actions: impl Into<ActionSet>,
    ) -> Self {
        let rule = match AclRule::allow(pattern, principal, actions.into()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(pattern = pattern, error = ?e, "skipping malformed ACL rule");
                return self;
            }
        };
        match self.table.add(rule) {
            Ok(()) => self,
            Err(e) => {
                tracing::warn!(pattern = pattern, error = ?e, "skipping malformed ACL rule");
                self
            }
        }
    }

    /// Push a Deny rule.
    pub fn deny(
        mut self,
        pattern: &str,
        principal: Option<PrincipalId>,
        actions: impl Into<ActionSet>,
    ) -> Self {
        let rule = match AclRule::deny(pattern, principal, actions.into()) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(pattern = pattern, error = ?e, "skipping malformed ACL rule");
                return self;
            }
        };
        match self.table.add(rule) {
            Ok(()) => self,
            Err(e) => {
                tracing::warn!(pattern = pattern, error = ?e, "skipping malformed ACL rule");
                self
            }
        }
    }

    /// Finalize. Returns the populated table. The result is
    /// `Result` for forward compatibility (future validation passes
    /// may want to surface accumulated errors); currently always
    /// returns `Ok` because individual rule errors are logged and
    /// skipped during construction.
    pub fn build(self) -> Result<AclTable, AclError> {
        Ok(self.table)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::action::{Action, ActionSet};
    use crate::table::AclDecision;

    fn p(id: u64) -> PrincipalId {
        PrincipalId::new(id)
    }

    #[test]
    fn empty_builder_produces_empty_table() {
        let t = AclTableBuilder::new().build().unwrap();
        assert_eq!(t.rule_count(), 0);
    }

    #[test]
    fn chained_rules_preserve_insertion_order() {
        let t = AclTableBuilder::new()
            .allow("a.*", None, ActionSet::single(Action::Publish))
            .deny("a.critical", None, ActionSet::ALL)
            .allow("b.*", Some(p(1)), ActionSet::ALL)
            .build()
            .unwrap();
        assert_eq!(t.rule_count(), 3);
        let patterns: Vec<&str> = t.rules().iter().map(|r| r.pattern.as_str()).collect();
        assert_eq!(patterns, vec!["a.*", "a.critical", "b.*"]);
    }

    #[test]
    fn builder_produces_equivalent_table_to_manual_add() {
        let mut manual = AclTable::new();
        manual
            .add(AclRule::allow("a.*", None, ActionSet::single(Action::Publish)).unwrap())
            .unwrap();
        manual
            .add(AclRule::deny("a.critical", None, ActionSet::ALL).unwrap())
            .unwrap();

        let built = AclTableBuilder::new()
            .allow("a.*", None, ActionSet::single(Action::Publish))
            .deny("a.critical", None, ActionSet::ALL)
            .build()
            .unwrap();

        assert_eq!(manual.rules(), built.rules());
    }

    #[test]
    fn builder_skips_malformed_rule_silently() {
        // Empty pattern — should be skipped, not panic.
        let t = AclTableBuilder::new()
            .allow("", None, ActionSet::ALL)
            .allow("ok.*", None, ActionSet::ALL)
            .build()
            .unwrap();
        assert_eq!(t.rule_count(), 1);
        assert_eq!(t.rules()[0].pattern, "ok.*");
    }

    #[test]
    fn builder_accepts_single_action_via_into() {
        let t = AclTableBuilder::new()
            .allow("t", None, Action::Publish) // Action: Into<ActionSet>
            .build()
            .unwrap();
        assert_eq!(t.check(p(1), Action::Publish, "t"), AclDecision::Allow);
        assert_eq!(
            t.check(p(1), Action::Subscribe, "t"),
            AclDecision::Deny {
                reason: crate::table::DenyReason::ActionNotInAllowSet
            }
        );
    }
}
