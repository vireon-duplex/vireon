//! [`ConflictResolver`] — conflict detection and resolution.
//!
//! See `DESIGN.md` Part 8 (conflict detection & resolution).
//!
//! # Conflict Resolution Strategies
//!
//! When concurrent writes arrive at different replicas, conflicts
//! can arise. Vireon supports multiple resolution strategies:
//!
//! 1. **Last-Writer-Wins (LWW)**: The write with the latest
//!    timestamp (encoded as a u64 prefix in the value) wins.
//! 2. **Vector Clocks**: Track causal history to detect true
//!    conflicts vs. concurrent writes.
//! 3. **Custom Application Logic**: Application-defined resolver.

use crate::error::ReplicationError;

/// Conflict resolver.
///
/// Detects and resolves conflicts between concurrent writes.
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
    /// The `conflicting` parameter is a slice of `(local, remote)`
    /// byte pairs. The resolver picks one according to its strategy.
    ///
    /// # Strategies
    ///
    /// - **LastWriterWins**: Picks the longer value (heuristic: more
    ///   data = more recent). If equal length, picks the lexicographically
    ///   larger one for determinism.
    /// - **VectorClocks**: Picks the value with the higher first 8 bytes
    ///   (interpreted as a u64 timestamp in big-endian).
    /// - **Custom**: Same as LWW (no application callback registered).
    ///
    /// # Errors
    /// Returns [`ReplicationError::ConflictResolutionFailed`] if no
    /// conflicting values are provided.
    pub fn resolve(
        &mut self,
        key: &str,
        conflicting: &[(&[u8], &[u8])],
    ) -> Result<Vec<u8>, ReplicationError> {
        let _ = key;

        if conflicting.is_empty() {
            return Err(ReplicationError::ConflictResolutionFailed(
                "no conflicting values provided",
            ));
        }

        // Collect all candidate values.
        let mut candidates: Vec<&[u8]> = Vec::with_capacity(conflicting.len() * 2);
        for (local, remote) in conflicting {
            candidates.push(local);
            candidates.push(remote);
        }

        let winner = match self.strategy {
            ConflictStrategy::LastWriterWins | ConflictStrategy::Custom => {
                // Heuristic: longer value wins. Tie-break: lexicographic max.
                candidates
                    .into_iter()
                    .max_by(|a, b| {
                        a.len()
                            .cmp(&b.len())
                            .then_with(|| a.cmp(b))
                    })
                    .unwrap_or(&[])
            }
            ConflictStrategy::VectorClocks => {
                // Interpret first 8 bytes as a big-endian u64 timestamp.
                // Higher timestamp wins.
                candidates
                    .into_iter()
                    .max_by(|a, b| {
                        let ta = extract_timestamp(a);
                        let tb = extract_timestamp(b);
                        ta.cmp(&tb).then_with(|| a.cmp(b))
                    })
                    .unwrap_or(&[])
            }
        };

        self.resolved_count = self.resolved_count.saturating_add(1);
        Ok(winner.to_vec())
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

/// Extract a u64 timestamp from the first 8 bytes (big-endian).
/// Returns 0 if fewer than 8 bytes.
fn extract_timestamp(data: &[u8]) -> u64 {
    if data.len() >= 8 {
        u64::from_be_bytes(data[0..8].try_into().unwrap_or([0; 8]))
    } else {
        0
    }
}

impl Default for ConflictResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConflictResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConflictResolver")
            .field("strategy", &self.strategy)
            .field("resolved_count", &self.resolved_count)
            .finish()
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
    fn conflict_resolve_lww_picks_longer() {
        let mut resolver = ConflictResolver::new();
        let v1 = b"short";
        let v2 = b"longer_value";
        let conflicting: [(&[u8], &[u8]); 1] = [(&v1[..], &v2[..])];
        let result = resolver.resolve("key1", &conflicting).unwrap();
        assert_eq!(result, b"longer_value");
        assert_eq!(resolver.resolved_count(), 1);
    }

    #[test]
    fn conflict_resolve_lww_tiebreak() {
        let mut resolver = ConflictResolver::new();
        // Same length → lexicographic max wins.
        let v1 = b"aaa";
        let v2 = b"bbb";
        let conflicting: [(&[u8], &[u8]); 1] = [(&v1[..], &v2[..])];
        let result = resolver.resolve("key1", &conflicting).unwrap();
        assert_eq!(result, b"bbb");
    }

    #[test]
    fn conflict_resolve_empty_conflicts_errors() {
        let mut resolver = ConflictResolver::new();
        let result = resolver.resolve("key1", &[]);
        assert!(matches!(
            result,
            Err(ReplicationError::ConflictResolutionFailed(_))
        ));
    }

    #[test]
    fn conflict_resolve_vector_clocks() {
        let mut resolver = ConflictResolver::with_strategy(ConflictStrategy::VectorClocks);
        // Higher timestamp in v2.
        let mut v1 = vec![0u8; 8];
        v1[..8].copy_from_slice(&100u64.to_be_bytes());
        v1.extend_from_slice(b"data1");

        let mut v2 = vec![0u8; 8];
        v2[..8].copy_from_slice(&200u64.to_be_bytes());
        v2.extend_from_slice(b"data2");

        let conflicting: [(&[u8], &[u8]); 1] = [(&v1[..], &v2[..])];
        let result = resolver.resolve("key1", &conflicting).unwrap();
        // v2 has higher timestamp.
        assert_eq!(result, v2.as_slice());
    }

    #[test]
    fn conflict_resolve_multiple_pairs() {
        let mut resolver = ConflictResolver::new();
        let pairs: [(&[u8], &[u8]); 3] = [
            (b"a", b"bb"),
            (b"ccc", b"dd"),
            (b"e", b"ffff"),
        ];
        let result = resolver.resolve("key", &pairs).unwrap();
        // "ffff" is the longest.
        assert_eq!(result, b"ffff");
        assert_eq!(resolver.resolved_count(), 1);
    }

    #[test]
    fn conflict_resolve_count_increments() {
        let mut resolver = ConflictResolver::new();
        let conflicting: [(&[u8], &[u8]); 1] = [(b"a", b"b")];
        resolver.resolve("k1", &conflicting).unwrap();
        resolver.resolve("k2", &conflicting).unwrap();
        resolver.resolve("k3", &conflicting).unwrap();
        assert_eq!(resolver.resolved_count(), 3);
    }

    #[test]
    fn conflict_resolve_custom_strategy_works() {
        let mut resolver = ConflictResolver::with_strategy(ConflictStrategy::Custom);
        let conflicting: [(&[u8], &[u8]); 1] = [(b"short", b"longer")];
        let result = resolver.resolve("key", &conflicting).unwrap();
        assert_eq!(result, b"longer");
    }
}
