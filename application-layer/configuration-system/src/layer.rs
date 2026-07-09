//! [`ConfigLayer`] — configuration layer metadata.
//!
//! See `DESIGN.md` Part 6 (sources & priority).

use crate::source::ConfigSource;

// ── ConfigLayer ──────────────────────────────────────────────────

/// Configuration layer metadata.
///
/// Each layer represents a configuration source loaded at a specific
/// time. Layers are stacked by priority and merged on lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigLayer {
    source: ConfigSource,
    priority: u8,
    path_hash: u64,
    loaded_ns: u64,
}

impl ConfigLayer {
    /// Construct a new layer from a source and load timestamp.
    #[must_use]
    pub const fn new(source: ConfigSource, loaded_ns: u64) -> Self {
        Self {
            priority: source.priority(),
            source,
            path_hash: 0,
            loaded_ns,
        }
    }

    /// Get the source of this layer.
    #[must_use]
    pub const fn source(&self) -> ConfigSource {
        self.source
    }

    /// Get the priority of this layer.
    #[must_use]
    pub const fn priority(&self) -> u8 {
        self.priority
    }

    /// Get the load timestamp (nanoseconds since epoch).
    #[must_use]
    pub const fn loaded_ns(&self) -> u64 {
        self.loaded_ns
    }

    /// Get the path hash of this layer.
    #[must_use]
    pub const fn path_hash(&self) -> u64 {
        self.path_hash
    }
}

impl Default for ConfigLayer {
    fn default() -> Self {
        Self::new(ConfigSource::default(), 0)
    }
}

impl PartialOrd for ConfigLayer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConfigLayer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by priority first (higher priority = greater).
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => {
                // If priorities are equal, compare by load time (more recent = greater).
                self.loaded_ns.cmp(&other.loaded_ns)
            }
            ord => ord,
        }
    }
}

impl std::hash::Hash for ConfigLayer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.source.hash(state);
        self.priority.hash(state);
        self.path_hash.hash(state);
        self.loaded_ns.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layer_constructs_from_source() {
        let layer = ConfigLayer::new(ConfigSource::File, 1000);
        assert_eq!(layer.source(), ConfigSource::File);
        assert_eq!(layer.priority(), ConfigSource::File.priority());
        assert_eq!(layer.loaded_ns(), 1000);
    }

    #[test]
    fn layer_priority_matches_source() {
        let layer = ConfigLayer::new(ConfigSource::Runtime, 0);
        assert_eq!(layer.priority(), 100);
    }

    #[test]
    fn layer_load_time_preserved() {
        let layer = ConfigLayer::new(ConfigSource::Env, 1234567890);
        assert_eq!(layer.loaded_ns(), 1234567890);
    }

    #[test]
    fn layers_order_by_priority_then_time() {
        let file_old = ConfigLayer::new(ConfigSource::File, 1000);
        let file_new = ConfigLayer::new(ConfigSource::File, 2000);
        let env = ConfigLayer::new(ConfigSource::Env, 1000);

        // Same priority, newer time comes first.
        assert!(file_new > file_old);
        // Higher priority wins regardless of time.
        assert!(env > file_new);
    }

    #[test]
    fn default_layer_is_default_source() {
        let layer = ConfigLayer::default();
        assert_eq!(layer.source(), ConfigSource::Default);
        assert_eq!(layer.loaded_ns(), 0);
    }

    #[test]
    fn layer_hash_is_deterministic() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let layer = ConfigLayer::new(ConfigSource::File, 1000);

        let mut h1 = DefaultHasher::new();
        layer.hash(&mut h1);
        let hash1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        layer.hash(&mut h2);
        let hash2 = h2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn layer_equality() {
        let a = ConfigLayer::new(ConfigSource::File, 1000);
        let b = ConfigLayer::new(ConfigSource::File, 1000);
        let c = ConfigLayer::new(ConfigSource::File, 2000);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn layer_is_copy() {
        let a = ConfigLayer::new(ConfigSource::Env, 100);
        let b = a;
        // a is still valid
        assert_eq!(a.source(), ConfigSource::Env);
        assert_eq!(b.source(), ConfigSource::Env);
    }
}
