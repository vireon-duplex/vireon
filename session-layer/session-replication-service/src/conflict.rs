//! [`ConflictResolver`] — Phase 1+ skeleton for conflict detection
//! and resolution.
//!
//! See `DESIGN.md` Part 8 (conflict detection & resolution).
//!
//! # Conflict Resolution Strategies
//!
//! When concurrent writes arrive at different replicas, conflicts
//! can arise. Vireon supports multiple resolution strategies:
//!
//! 1. **Last-Writer-Wins (LWW)**: The write with the latest
//!    timestamp wins. Simple but loses data.
//! 2. **Vector Clocks**: Track causal history to detect true
//!    conflicts vs. concurrent writes.
//! 3. **Custom Application Logic**: Application-defined resolver
//!    callbacks.
//!
//! # Phase 0
//!
//! Type + stub only. Phase 1+ adds real resolution logic.

use crate::error::ReplicationError;

/// Phase 1+ conflict resolver.
///
/// Detects and resolves conflicts between concurrent writes.
#[allow(dead_code)]
pub struct ConflictResolver {
    strategy: ConflictStrategy,
    resolved_count: u64,
}

/// The strategy used to resolve conflicts.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ConflictStrategy {
    /// Last-writer-wins based on wall-clock timestamp.
    #[default]
    LastWriterWins,
    /// Vector clock comparison for causal consistency.
    VectorClocks,
    /// Application-defined custom resolver.
    Custom,
}

impl ConflictResolver {
    /// Construct a new resolver with the default strategy (LWW).
    #[must_use]
    pub fn new() -> Self {
        Self {
            strategy: ConflictStrategy::default(),
            resolved_count: 0,
        }
    }

    /// Construct a new resolver with a specific strategy.
    #[must_use]
    pub const fn with_strategy(strategy: ConflictStrategy) -> Self {
        Self {
            strategy,
            resolved_count: 0,
        }
    }

    /// Resolve a conflict for the given key.
    ///
    /// The `conflicting` parameter is the raw bytes of the
    /// conflicting values. The resolver picks one according to
    /// its strategy.
    ///
    /// # Errors
    /// Returns `ReplicationError::ConflictResolutionFailed` if
    /// resolution fails. In Phase 0, always returns `Ok`.
    pub fn resolve(
        &mut self,
        key: &str,
        conflicting: &[(&[u8], &[u8])],
    ) -> Result<Vec<u8>, ReplicationError> {
        // Phase 0: stub — always pick the first value.
        let _ = key;
        let _ = conflicting;
        self.resolved_count = self.resolved_count.saturating_add(1);
        Ok(Vec::new())
    }

    /// The configured conflict resolution strategy.
    #[must_use]
    pub const fn strategy(&self) -> ConflictStrategy {
        self.strategy
    }

    /// Number of conflicts resolved since startup.
    #[must_use]
    pub const fn resolved_count(&self) -> u64 {
        self.resolved_count
    }
}

impl Default for ConflictResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_resolver_new_defaults_to_lww() {
        let resolver = ConflictResolver::new();
        assert_eq!(resolver.strategy(), ConflictStrategy::LastWriterWins);
        assert_eq!(resolver.resolved_count(), 0);
    }

    #[test]
    fn conflict_resolver_with_vector_clocks() {
        let resolver = ConflictResolver::with_strategy(ConflictStrategy::VectorClocks);
        assert_eq!(resolver.strategy(), ConflictStrategy::VectorClocks);
    }

    #[test]
    fn conflict_resolve_is_stub_ok() {
        let mut resolver = ConflictResolver::new();
        let v1 = b"value1";
        let v2 = b"value2";
        let conflicting: [(&[u8], &[u8]); 1] = [(&v1[..], &v2[..])];
        let result = resolver.resolve("key1", &conflicting);
        assert!(result.is_ok());
        assert_eq!(resolver.resolved_count(), 1);
    }
}
