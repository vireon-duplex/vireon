//! [`TokenId`], [`ServiceToken`] — typed identifiers for services in the
//! dependency injection container.
//!
//! See `DESIGN.md` Part 4 (service tokens).
//!
//! # Design
//!
//! - `TokenId` is a raw `u64` newtype used internally.
//! - `ServiceToken<T>` is a type-safe wrapper that ties a token to a
//!   specific service type `T`.
//! - The NIL token (id 0) represents the absence of a service.

use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

// ── TokenId ─────────────────────────────────────────────────────

/// Raw identifier for a service in the container.
///
/// `TokenId` is the internal representation used by the container
/// for indexing and lookups.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TokenId(u64);

impl TokenId {
    /// The NIL token (represents absence of a service).
    pub const NIL: Self = Self(0);

    /// Create a new token ID from a raw value.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Create the NIL token.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this token is NIL.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        // Derived PartialEq is NOT const in Rust 2024
        self.raw() == Self::NIL.raw()
    }

    /// Get the raw `u64` value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TokenId({})", self.0)
    }
}

// ── ServiceToken ────────────────────────────────────────────────

/// Type-safe token for a service of type `T`.
///
/// `ServiceToken<T>` ties a token ID to a specific service type,
/// providing compile-time type safety for service lookups.
///
/// # Type parameters
///
/// - `T`: The service type. Stored as PhantomData to make the token
///   Send + Sync while maintaining variance safety.
pub struct ServiceToken<T> {
    id: TokenId,
    _phantom: PhantomData<T>, // Makes the token type-safe over T
}

impl<T> Clone for ServiceToken<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ServiceToken<T> {}

impl<T> fmt::Debug for ServiceToken<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceToken")
            .field("id", &self.id)
            .field("_phantom", &self._phantom)
            .finish()
    }
}

impl<T> PartialEq for ServiceToken<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for ServiceToken<T> {}

impl<T> Hash for ServiceToken<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<T> ServiceToken<T> {
    /// Create a new service token from a token ID.
    #[must_use]
    pub const fn new(id: TokenId) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }

    /// Get the underlying token ID.
    #[must_use]
    pub const fn id(self) -> TokenId {
        self.id
    }

    /// Whether this token is NIL.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.id.is_nil()
    }
}

// Implement Default for any service token type
impl<T> Default for ServiceToken<T> {
    fn default() -> Self {
        Self::new(TokenId::nil())
    }
}

// SAFETY: ServiceToken only contains a TokenId (Copy) and PhantomData.
// TokenId is Send + Sync, and PhantomData<T> is Send + Sync for all T.
unsafe impl<T> Send for ServiceToken<T> {}
unsafe impl<T> Sync for ServiceToken<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_token_is_zero() {
        assert_eq!(TokenId::NIL.raw(), 0);
        assert!(TokenId::NIL.is_nil());
    }

    #[test]
    fn new_token_has_correct_id() {
        let token = TokenId::new(42);
        assert_eq!(token.raw(), 42);
        assert!(!token.is_nil());
    }

    #[test]
    fn token_nil_method() {
        let nil = TokenId::nil();
        assert_eq!(nil.raw(), 0);
        assert!(nil.is_nil());
    }

    #[test]
    fn service_token_stores_id() {
        let token = ServiceToken::<String>::new(TokenId::new(7));
        assert_eq!(token.id().raw(), 7);
        assert!(!token.is_nil());
    }

    #[test]
    fn service_token_nil() {
        let token = ServiceToken::<Vec<u8>>::new(TokenId::nil());
        assert!(token.is_nil());
    }

    #[test]
    fn service_token_is_copy() {
        let token = ServiceToken::<i32>::new(TokenId::new(1));
        let copy = token;
        assert_eq!(token.id().raw(), 1); // token is still valid
        assert_eq!(copy.id().raw(), 1);
    }

    #[test]
    fn service_token_equality() {
        let token1 = ServiceToken::<String>::new(TokenId::new(5));
        let token2 = ServiceToken::<String>::new(TokenId::new(5));
        let token3 = ServiceToken::<String>::new(TokenId::new(7));

        assert_eq!(token1, token2);
        assert_ne!(token1, token3);
    }

    #[test]
    fn service_token_default_is_nil() {
        let token: ServiceToken<i32> = ServiceToken::default();
        assert!(token.is_nil());
    }

    #[test]
    fn token_id_ordering() {
        let a = TokenId::new(1);
        let b = TokenId::new(5);
        let c = TokenId::new(10);

        assert!(a < b);
        assert!(b < c);
        assert!(a <= b);
        assert!(b >= a);
    }

    #[test]
    fn token_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();

        set.insert(TokenId::new(1));
        set.insert(TokenId::new(2));
        set.insert(TokenId::new(1)); // duplicate

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn service_token_type_distinction() {
        // ServiceToken<i32> and ServiceToken<String> are different types
        // even with the same ID
        let token_i32 = ServiceToken::<i32>::new(TokenId::new(1));
        let token_string = ServiceToken::<String>::new(TokenId::new(1));

        // They have the same ID but are different types
        assert_eq!(token_i32.id(), token_string.id());
        // Can't directly compare different ServiceToken types
    }

    #[test]
    fn service_token_send_sync() {
        // Ensure ServiceToken is Send + Sync for any T
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ServiceToken<i32>>();
        assert_send_sync::<ServiceToken<String>>();
    }

    #[test]
    fn token_display() {
        let token = TokenId::new(42);
        assert_eq!(format!("{token}"), "TokenId(42)");
    }
}
