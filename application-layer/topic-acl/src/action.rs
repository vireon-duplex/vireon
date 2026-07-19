//! [`Action`] + [`ActionSet`] — what a principal wants to do to a topic.

/// Operation a principal attempts on a topic. The set is closed for
/// Phase 7; new actions (`Describe`, `ReadOffset`, `CommitOffset`)
/// will arrive in follow-up phases via additions to the enum.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// Publish a message to the topic.
    Publish = 0x01,
    /// Subscribe to the topic / pattern.
    Subscribe = 0x02,
    /// Create the topic (`MessageType::TopicCreate`).
    TopicCreate = 0x03,
    /// Delete the topic (`MessageType::TopicDelete`).
    TopicDelete = 0x04,
}

impl Action {
    /// All current actions. Used by `ActionSet::ALL` and tests.
    pub const ALL: &'static [Action] = &[
        Action::Publish,
        Action::Subscribe,
        Action::TopicCreate,
        Action::TopicDelete,
    ];

    /// Inverse of the `as u8` cast.
    #[must_use]
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Publish),
            0x02 => Some(Self::Subscribe),
            0x03 => Some(Self::TopicCreate),
            0x04 => Some(Self::TopicDelete),
            _ => None,
        }
    }

    /// Lowercase name for metrics labels / logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
            Self::TopicCreate => "topic_create",
            Self::TopicDelete => "topic_delete",
        }
    }
}

/// Compact bitmask of [`Action`] values. Backed by `u8`; supports up
/// to 8 actions (4 currently used, 4 reserved for forward-compat).
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct ActionSet(u8);

impl ActionSet {
    /// Empty set — matches no actions.
    pub const NONE: Self = Self(0);

    /// Set of every currently-defined action. Bit positions are
    /// `(Action::X as u8) - 1` — see [`Self::single`].
    pub const ALL: Self = Self(0x01 | 0x02 | 0x04 | 0x08);

    /// Construct containing a single action.
    #[must_use]
    pub const fn single(a: Action) -> Self {
        Self(1 << (a as u8 - 1))
    }

    /// Add an action. No-op if already present.
    pub fn add(&mut self, a: Action) -> &mut Self {
        self.0 |= 1 << (a as u8 - 1);
        self
    }

    /// Remove an action. No-op if absent.
    pub fn remove(&mut self, a: Action) -> &mut Self {
        self.0 &= !(1 << (a as u8 - 1));
        self
    }

    /// Whether `a` is in the set.
    #[must_use]
    pub fn contains(&self, a: Action) -> bool {
        (self.0 & (1 << (a as u8 - 1))) != 0
    }

    /// Whether the set is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Iterate the actions present, in canonical order (Publish,
    /// Subscribe, TopicCreate, TopicDelete).
    pub fn iter(self) -> impl Iterator<Item = Action> {
        Action::ALL
            .iter()
            .copied()
            .filter(move |a| self.contains(*a))
    }

    /// Build from an iterator. Duplicates collapse silently.
    #[must_use]
    pub fn from_actions<I: IntoIterator<Item = Action>>(it: I) -> Self {
        let mut s = Self::NONE;
        for a in it {
            s.add(a);
        }
        s
    }
}

impl std::iter::FromIterator<Action> for ActionSet {
    fn from_iter<I: IntoIterator<Item = Action>>(it: I) -> Self {
        let mut s = Self::NONE;
        for a in it {
            s.add(a);
        }
        s
    }
}

impl Extend<Action> for ActionSet {
    fn extend<T: IntoIterator<Item = Action>>(&mut self, iter: T) {
        for a in iter {
            self.add(a);
        }
    }
}

impl From<Action> for ActionSet {
    fn from(a: Action) -> Self {
        Self::single(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_u8_round_trips() {
        for a in Action::ALL {
            assert_eq!(Action::from_u8(*a as u8), Some(*a));
        }
    }

    #[test]
    fn from_u8_rejects_unknown() {
        assert_eq!(Action::from_u8(0x00), None);
        assert_eq!(Action::from_u8(0xFF), None);
        assert_eq!(Action::from_u8(0x05), None);
    }

    #[test]
    fn as_str_lowercase() {
        assert_eq!(Action::Publish.as_str(), "publish");
        assert_eq!(Action::TopicDelete.as_str(), "topic_delete");
    }

    #[test]
    fn all_covers_every_variant() {
        assert_eq!(Action::ALL.len(), 4);
    }

    #[test]
    fn set_starts_empty() {
        assert!(ActionSet::NONE.is_empty());
    }

    #[test]
    fn single_action_round_trip() {
        for a in Action::ALL {
            let s = ActionSet::single(*a);
            assert!(!s.is_empty());
            assert!(s.contains(*a));
            // Other actions absent.
            for other in Action::ALL {
                if other != a {
                    assert!(
                        !s.contains(*other),
                        "{other:?} should not be in single-{a:?} set"
                    );
                }
            }
        }
    }

    #[test]
    fn add_remove_idempotent() {
        let mut s = ActionSet::NONE;
        s.add(Action::Publish);
        s.add(Action::Publish);
        assert!(s.contains(Action::Publish));
        s.remove(Action::Publish);
        s.remove(Action::Publish);
        assert!(!s.contains(Action::Publish));
    }

    #[test]
    fn iter_yields_in_canonical_order() {
        let mut s = ActionSet::NONE;
        s.add(Action::TopicDelete);
        s.add(Action::Publish);
        s.add(Action::Subscribe);
        let collected: Vec<_> = s.iter().collect();
        assert_eq!(
            collected,
            vec![Action::Publish, Action::Subscribe, Action::TopicDelete]
        );
    }

    #[test]
    fn all_constant_includes_everything() {
        for a in Action::ALL {
            assert!(ActionSet::ALL.contains(*a));
        }
    }

    #[test]
    fn from_actions_collapses_duplicates() {
        let s = ActionSet::from_actions([Action::Publish, Action::Publish, Action::Subscribe]);
        let count = s.iter().count();
        assert_eq!(count, 2);
    }

    #[test]
    fn collect_into_action_set() {
        let s: ActionSet = [Action::Publish, Action::Subscribe].into_iter().collect();
        assert!(s.contains(Action::Publish));
        assert!(s.contains(Action::Subscribe));
        assert!(!s.contains(Action::TopicCreate));
    }

    #[test]
    fn from_action_conversion() {
        let s: ActionSet = Action::Publish.into();
        assert!(s.contains(Action::Publish));
        assert!(!s.contains(Action::Subscribe));
    }
}
