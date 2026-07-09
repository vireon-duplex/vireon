//! [`PrincipalId`], [`PrincipalType`] — principal identification and type.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PrincipalId(u64);

impl PrincipalId {
    pub const NIL: Self = Self(0);
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for PrincipalId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "principal#{}", self.0)
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PrincipalType {
    #[default]
    User,
    Service,
    Agent,
    Anonymous,
}

impl PrincipalType {
    #[must_use]
    pub const fn is_authenticated(self) -> bool {
        !matches!(self, Self::Anonymous)
    }
    #[must_use]
    pub const fn is_machine(self) -> bool {
        matches!(self, Self::Service | Self::Agent)
    }
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Service => "service",
            Self::Agent => "agent",
            Self::Anonymous => "anonymous",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_id_nil() {
        assert!(PrincipalId::NIL.is_nil());
        assert!(!PrincipalId::new(1).is_nil());
    }

    #[test]
    fn principal_type_predicates() {
        assert!(PrincipalType::User.is_authenticated());
        assert!(!PrincipalType::Anonymous.is_authenticated());
        assert!(PrincipalType::Service.is_machine());
    }
}
