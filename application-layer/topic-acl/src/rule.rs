//! [`AclRule`] + [`AclEffect`] — one entry in the ACL table.

use auth_framework::PrincipalId;

use crate::action::ActionSet;
use crate::error::AclError;

/// Maximum pattern length (fits a `u16` wire prefix).
pub const MAX_PATTERN_LEN: usize = u16::MAX as usize; // 65_535

/// Whether a matching rule permits or denies the action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclEffect {
    /// Permit the action.
    Allow,
    /// Forbid the action. Any matching `Deny` rule overrides all
    /// matching `Allow` rules.
    Deny,
}

impl AclEffect {
    /// Lowercase name for logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// One ACL entry: "principal X (or anyone, if `None`) may (or may not)
/// perform actions in `actions` on topics matching `pattern`".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclRule {
    /// Topic pattern. Single-segment `*` wildcards allowed.
    pub pattern: String,
    /// `None` = wildcard (matches any principal).
    pub principal: Option<PrincipalId>,
    /// Actions covered by this rule.
    pub actions: ActionSet,
    /// Allow or Deny.
    pub effect: AclEffect,
}

impl AclRule {
    /// Construct an Allow rule.
    ///
    /// # Errors
    /// * [`AclError::EmptyPattern`] if `pattern` is empty.
    /// * [`AclError::OverlongPattern`] if `pattern` exceeds `MAX_PATTERN_LEN`.
    /// * [`AclError::EmptyActionSet`] if `actions` is empty.
    pub fn allow(
        pattern: &str,
        principal: Option<PrincipalId>,
        actions: ActionSet,
    ) -> Result<Self, AclError> {
        Self::new(pattern, principal, actions, AclEffect::Allow)
    }

    /// Construct a Deny rule.
    ///
    /// # Errors
    /// Same as [`Self::allow`].
    pub fn deny(
        pattern: &str,
        principal: Option<PrincipalId>,
        actions: ActionSet,
    ) -> Result<Self, AclError> {
        Self::new(pattern, principal, actions, AclEffect::Deny)
    }

    /// Construct with explicit effect.
    fn new(
        pattern: &str,
        principal: Option<PrincipalId>,
        actions: ActionSet,
        effect: AclEffect,
    ) -> Result<Self, AclError> {
        if pattern.is_empty() {
            return Err(AclError::EmptyPattern);
        }
        if pattern.len() > MAX_PATTERN_LEN {
            return Err(AclError::OverlongPattern);
        }
        if actions.is_empty() {
            return Err(AclError::EmptyActionSet);
        }
        Ok(Self {
            pattern: pattern.to_owned(),
            principal,
            actions,
            effect,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::action::Action;

    fn p(id: u64) -> PrincipalId {
        PrincipalId::new(id)
    }

    #[test]
    fn allow_constructor() {
        let r = AclRule::allow("sensor.*", Some(p(1)), ActionSet::single(Action::Publish)).unwrap();
        assert_eq!(r.effect, AclEffect::Allow);
        assert_eq!(r.pattern, "sensor.*");
        assert_eq!(r.principal, Some(p(1)));
    }

    #[test]
    fn deny_constructor() {
        let r = AclRule::deny("system.*", None, ActionSet::ALL).unwrap();
        assert_eq!(r.effect, AclEffect::Deny);
        assert!(r.principal.is_none());
    }

    #[test]
    fn empty_pattern_rejected() {
        assert!(matches!(
            AclRule::allow("", None, ActionSet::ALL),
            Err(AclError::EmptyPattern)
        ));
    }

    #[test]
    fn overlong_pattern_rejected() {
        let big = "x".repeat(MAX_PATTERN_LEN + 1);
        assert!(matches!(
            AclRule::allow(&big, None, ActionSet::ALL),
            Err(AclError::OverlongPattern)
        ));
    }

    #[test]
    fn empty_action_set_rejected() {
        assert!(matches!(
            AclRule::allow("t", None, ActionSet::NONE),
            Err(AclError::EmptyActionSet)
        ));
    }

    #[test]
    fn max_pattern_length_accepted() {
        let pat = "x".repeat(MAX_PATTERN_LEN);
        assert!(AclRule::allow(&pat, None, ActionSet::ALL).is_ok());
    }

    #[test]
    fn effect_as_str() {
        assert_eq!(AclEffect::Allow.as_str(), "allow");
        assert_eq!(AclEffect::Deny.as_str(), "deny");
    }
}
