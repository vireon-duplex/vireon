//! [`BindingDescriptor`] — service binding metadata for the dependency
//! injection container.
//!
//! See `DESIGN.md` Part 6 (service bindings).
//!
//! # Design
//!
//! A binding descriptor stores:
//! - The token ID of the service
//! - The lifetime policy (singleton/scoped/transient)
//! - A hash of the factory function for identification
//! - Up to 8 dependency tokens
//! - Whether the binding has been initialized

use crate::lifetime::ServiceLifetime;
use crate::token::TokenId;

// ── BindingDescriptor ─────────────────────────────────────────────

/// Metadata for a service binding in the container.
///
/// Stores all information needed to resolve and instantiate a service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingDescriptor {
    /// Token ID of this service.
    pub token: TokenId,
    /// Lifetime policy for the service.
    pub lifetime: ServiceLifetime,
    /// Hash of the factory function (identifies the implementation).
    pub factory_hash: u64,
    /// Dependency tokens (up to 8).
    pub dependencies: [TokenId; 8],
    /// Number of valid dependencies in the array.
    pub dep_count: u8,
    /// Whether the binding has been initialized.
    pub is_initialized: bool,
}

impl BindingDescriptor {
    /// Maximum number of dependencies per service.
    pub const MAX_DEPS: usize = 8;

    /// Create a new binding descriptor for a service.
    #[must_use]
    pub const fn new(token: TokenId, lifetime: ServiceLifetime) -> Self {
        Self {
            token,
            lifetime,
            factory_hash: 0,
            dependencies: [TokenId::NIL; Self::MAX_DEPS],
            dep_count: 0,
            is_initialized: false,
        }
    }

    /// Set the factory hash.
    #[must_use]
    pub const fn with_factory(mut self, hash: u64) -> Self {
        self.factory_hash = hash;
        self
    }

    /// Add a dependency token.
    ///
    /// # Errors
    /// Returns an error if the dependency array is full.
    pub fn add_dependency(mut self, dep: TokenId) -> Result<Self, &'static str> {
        if self.dep_count >= Self::MAX_DEPS as u8 {
            return Err("dependency array full");
        }
        self.dependencies[self.dep_count as usize] = dep;
        self.dep_count += 1;
        Ok(self)
    }

    /// Get the number of dependencies.
    #[must_use]
    pub const fn dependency_count(&self) -> usize {
        self.dep_count as usize
    }

    /// Check if this binding depends on the given token.
    #[must_use]
    pub fn has_dependency(&self, token: TokenId) -> bool {
        for i in 0..self.dep_count as usize {
            if self.dependencies[i].raw() == token.raw() {
                return true;
            }
        }
        false
    }

    /// Get an iterator over the dependencies.
    pub fn dependencies_iter(&self) -> impl Iterator<Item = TokenId> + '_ {
        (0..self.dep_count as usize).map(move |i| self.dependencies[i])
    }
}

impl Default for BindingDescriptor {
    fn default() -> Self {
        Self::new(TokenId::nil(), ServiceLifetime::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_binding_has_no_dependencies() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Singleton);
        assert_eq!(binding.dependency_count(), 0);
        assert!(!binding.is_initialized);
        assert_eq!(binding.factory_hash, 0);
    }

    #[test]
    fn with_factory_sets_hash() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Scoped)
            .with_factory(0xDEAD_BEEF);

        assert_eq!(binding.factory_hash, 0xDEAD_BEEF);
    }

    #[test]
    fn add_dependency_increases_count() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Transient)
            .add_dependency(TokenId::new(2))
            .unwrap();

        assert_eq!(binding.dependency_count(), 1);
        assert!(binding.has_dependency(TokenId::new(2)));
    }

    #[test]
    fn add_multiple_dependencies() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Singleton)
            .add_dependency(TokenId::new(2))
            .unwrap()
            .add_dependency(TokenId::new(3))
            .unwrap()
            .add_dependency(TokenId::new(4))
            .unwrap();

        assert_eq!(binding.dependency_count(), 3);
        assert!(binding.has_dependency(TokenId::new(2)));
        assert!(binding.has_dependency(TokenId::new(3)));
        assert!(binding.has_dependency(TokenId::new(4)));
        assert!(!binding.has_dependency(TokenId::new(5)));
    }

    #[test]
    fn add_dependency_fails_when_full() {
        let mut binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Transient);

        // Fill all 8 slots
        for i in 2..=9 {
            binding = binding.add_dependency(TokenId::new(i)).unwrap();
        }

        // 9th dependency should fail
        let result = binding.add_dependency(TokenId::new(10));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "dependency array full");
    }

    #[test]
    fn dependencies_iter_yields_all_deps() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Scoped)
            .add_dependency(TokenId::new(2))
            .unwrap()
            .add_dependency(TokenId::new(3))
            .unwrap();

        let deps: Vec<TokenId> = binding.dependencies_iter().collect();
        assert_eq!(deps.len(), 2);
        assert_eq!(deps[0], TokenId::new(2));
        assert_eq!(deps[1], TokenId::new(3));
    }

    #[test]
    fn has_dependency_returns_false_for_non_dep() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Singleton)
            .add_dependency(TokenId::new(2))
            .unwrap();

        assert!(!binding.has_dependency(TokenId::new(99)));
    }

    #[test]
    fn binding_is_copy() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Scoped)
            .add_dependency(TokenId::new(2))
            .unwrap();

        let copy = binding;
        assert_eq!(binding.dependency_count(), 1); // binding is still valid
        assert_eq!(copy.dependency_count(), 1);
    }

    #[test]
    fn binding_default() {
        let binding = BindingDescriptor::default();
        assert_eq!(binding.token, TokenId::nil());
        assert_eq!(binding.lifetime, ServiceLifetime::Transient);
        assert_eq!(binding.dependency_count(), 0);
    }

    #[test]
    fn max_deps_is_eight() {
        assert_eq!(BindingDescriptor::MAX_DEPS, 8);
    }

    #[test]
    fn binding_lifetime_variants() {
        let singleton = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Singleton);
        assert!(singleton.lifetime.is_singleton());

        let scoped = BindingDescriptor::new(TokenId::new(2), ServiceLifetime::Scoped);
        assert!(scoped.lifetime.is_scoped());

        let transient = BindingDescriptor::new(TokenId::new(3), ServiceLifetime::Transient);
        assert!(transient.lifetime.is_transient());
    }

    #[test]
    fn binding_equality() {
        let binding1 =
            BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Singleton).with_factory(123);

        let binding2 =
            BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Singleton).with_factory(123);

        let binding3 =
            BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Singleton).with_factory(456);

        assert_eq!(binding1, binding2);
        assert_ne!(binding1, binding3);
    }

    #[test]
    fn binding_can_be_modified_after_creation() {
        let binding = BindingDescriptor::new(TokenId::new(1), ServiceLifetime::Scoped);
        assert_eq!(binding.dependency_count(), 0);

        let with_deps = binding
            .add_dependency(TokenId::new(2))
            .unwrap()
            .add_dependency(TokenId::new(3))
            .unwrap();

        assert_eq!(with_deps.dependency_count(), 2);
    }
}
