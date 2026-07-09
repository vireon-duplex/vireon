//! [`ServiceLifetime`] — lifetime management for services in the
//! dependency injection container.
//!
//! See `DESIGN.md` Part 5 (lifetime management).
//!
//! # Lifetimes
//!
//! The container supports three service lifetimes:
//!
//! - `Singleton`: One instance for the entire container lifetime.
//! - `Scoped`: One instance per scope (request, transaction, etc.).
//! - `Transient`: New instance created on each resolution.

// ── ServiceLifetime ───────────────────────────────────────────────

/// Lifetime policy for a service in the container.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ServiceLifetime {
    /// One instance for the entire container lifetime.
    Singleton = 0,
    /// One instance per scope (created when scope enters, disposed when scope exits).
    Scoped = 1,
    /// New instance on each resolution (no caching).
    #[default]
    Transient = 2,
}

impl ServiceLifetime {
    /// Whether this lifetime is singleton.
    #[must_use]
    pub const fn is_singleton(self) -> bool {
        matches!(self, Self::Singleton)
    }

    /// Whether this lifetime is scoped.
    #[must_use]
    pub const fn is_scoped(self) -> bool {
        matches!(self, Self::Scoped)
    }

    /// Whether this lifetime is transient.
    #[must_use]
    pub const fn is_transient(self) -> bool {
        matches!(self, Self::Transient)
    }

    /// String representation for metrics and logging.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Singleton => "singleton",
            Self::Scoped => "scoped",
            Self::Transient => "transient",
        }
    }

    /// Whether this lifetime keeps instances alive across resolutions.
    ///
    /// Singletons are kept alive for the container lifetime.
    /// Scoped instances are kept alive for the scope lifetime.
    /// Transient instances are not cached.
    #[must_use]
    pub const fn keeps_instance_alive(self) -> bool {
        matches!(self, Self::Singleton | Self::Scoped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_transient() {
        assert_eq!(ServiceLifetime::default(), ServiceLifetime::Transient);
    }

    #[test]
    fn singleton_predicates() {
        let lt = ServiceLifetime::Singleton;
        assert!(lt.is_singleton());
        assert!(!lt.is_scoped());
        assert!(!lt.is_transient());
        assert!(lt.keeps_instance_alive());
    }

    #[test]
    fn scoped_predicates() {
        let lt = ServiceLifetime::Scoped;
        assert!(!lt.is_singleton());
        assert!(lt.is_scoped());
        assert!(!lt.is_transient());
        assert!(lt.keeps_instance_alive());
    }

    #[test]
    fn transient_predicates() {
        let lt = ServiceLifetime::Transient;
        assert!(!lt.is_singleton());
        assert!(!lt.is_scoped());
        assert!(lt.is_transient());
        assert!(!lt.keeps_instance_alive());
    }

    #[test]
    fn as_str_matches_variant() {
        assert_eq!(ServiceLifetime::Singleton.as_str(), "singleton");
        assert_eq!(ServiceLifetime::Scoped.as_str(), "scoped");
        assert_eq!(ServiceLifetime::Transient.as_str(), "transient");
    }

    #[test]
    fn keeps_instance_alive_behavior() {
        assert!(ServiceLifetime::Singleton.keeps_instance_alive());
        assert!(ServiceLifetime::Scoped.keeps_instance_alive());
        assert!(!ServiceLifetime::Transient.keeps_instance_alive());
    }

    #[test]
    fn lifetime_is_copy() {
        let lt = ServiceLifetime::Scoped;
        let copy = lt;
        assert_eq!(lt, ServiceLifetime::Scoped); // lt is still valid
        assert_eq!(copy, ServiceLifetime::Scoped);
    }

    #[test]
    fn lifetime_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();

        set.insert(ServiceLifetime::Singleton);
        set.insert(ServiceLifetime::Transient);
        set.insert(ServiceLifetime::Singleton); // duplicate

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn lifetime_equality() {
        assert_eq!(ServiceLifetime::Singleton, ServiceLifetime::Singleton);
        assert_ne!(ServiceLifetime::Singleton, ServiceLifetime::Scoped);
        assert_ne!(ServiceLifetime::Scoped, ServiceLifetime::Transient);
    }

    #[test]
    fn repr_is_u8() {
        assert_eq!(ServiceLifetime::Singleton as u8, 0);
        assert_eq!(ServiceLifetime::Scoped as u8, 1);
        assert_eq!(ServiceLifetime::Transient as u8, 2);
    }
}
