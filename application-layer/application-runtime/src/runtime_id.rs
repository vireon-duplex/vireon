//! [`RuntimeId`], [`RuntimeRole`] — identity and role for the
//! application runtime.
//!
//! See `DESIGN.md` Part 4 (identity).
//!
//! # Design
//!
//! `RuntimeId` is a `u64` newtype with a nil sentinel (0). Each
//! runtime instance gets a unique id assigned at startup. The id
//! is used in logs, metrics, and distributed coordination.
//!
//! `RuntimeRole` classifies the runtime's deployment archetype:
//! standalone, cluster member, edge node, or cluster controller.

// ── RuntimeId ───────────────────────────────────────────────────

/// Unique identifier for an application runtime instance.
///
/// `RuntimeId` is a newtype around `u64` with a reserved nil value
/// (0) representing "no runtime". All real runtimes have ids in the
/// range `1..=u64::MAX`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeId(u64);

impl RuntimeId {
    /// The nil runtime id — represents "no runtime".
    pub const NIL: Self = Self(0);

    /// Construct a runtime id from a raw u64.
    ///
    /// # Panics
    /// Panics if `id` is 0 (use [`RuntimeId::nil`] instead).
    #[must_use]
    pub const fn new(id: u64) -> Self {
        assert!(
            id != 0,
            "RuntimeId::new called with 0; use RuntimeId::nil instead"
        );
        Self(id)
    }

    /// Construct the nil runtime id.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil runtime id.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }

    /// Get the raw u64 value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RuntimeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── RuntimeRole ─────────────────────────────────────────────────

/// Deployment archetype for the runtime.
///
/// Encoded as `#[repr(u8)]` for efficient FFI and compact storage.
#[non_exhaustive]
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeRole {
    /// Standalone: single-node deployment, no clustering.
    Standalone = 0,
    /// Cluster: member of a multi-node cluster.
    Cluster = 1,
    /// Edge: edge node with limited resources.
    Edge = 2,
    /// Controller: cluster controller/coordinator.
    Controller = 3,
}

impl RuntimeRole {
    /// Whether this role participates in clustering.
    ///
    /// `Cluster` and `Controller` roles are clustered; `Standalone`
    /// and `Edge` are not.
    #[must_use]
    pub const fn is_clustered(&self) -> bool {
        matches!(self, Self::Cluster | Self::Controller)
    }

    /// Human-readable name for metrics labels and logs.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Cluster => "cluster",
            Self::Edge => "edge",
            Self::Controller => "controller",
        }
    }
}

impl Default for RuntimeRole {
    fn default() -> Self {
        Self::Standalone
    }
}

impl std::fmt::Display for RuntimeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_id_is_zero() {
        assert_eq!(RuntimeId::nil().raw(), 0);
        assert!(RuntimeId::nil().is_nil());
    }

    #[test]
    fn new_id_is_non_zero() {
        let id = RuntimeId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn nil_constant_equals_nil_method() {
        assert_eq!(RuntimeId::NIL, RuntimeId::nil());
    }

    #[test]
    fn ids_are_ordered() {
        let id1 = RuntimeId::new(1);
        let id2 = RuntimeId::new(2);
        assert!(id1 < id2);
        assert!(id2 > id1);
    }

    #[test]
    fn ids_can_be_hashed() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RuntimeId::new(1));
        set.insert(RuntimeId::new(2));
        assert_eq!(set.len(), 2);
        set.insert(RuntimeId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn nil_is_not_equal_to_real_id() {
        assert_ne!(RuntimeId::nil(), RuntimeId::new(1));
    }

    #[test]
    fn raw_returns_constructed_value() {
        let id = RuntimeId::new(42);
        assert_eq!(id.raw(), 42);
    }

    #[test]
    fn copy_trait_works() {
        let id1 = RuntimeId::new(7);
        let id2 = id1;
        assert_eq!(id1.raw(), 7);
        assert_eq!(id2.raw(), 7);
    }

    #[test]
    fn default_is_nil() {
        assert_eq!(RuntimeId::default(), RuntimeId::nil());
    }

    #[test]
    fn display_formats_correctly() {
        let id = RuntimeId::new(42);
        assert_eq!(format!("{id}"), "42");
        assert_eq!(format!("{}", RuntimeId::nil()), "0");
    }

    #[test]
    fn standalone_is_not_clustered() {
        assert!(!RuntimeRole::Standalone.is_clustered());
    }

    #[test]
    fn cluster_is_clustered() {
        assert!(RuntimeRole::Cluster.is_clustered());
    }

    #[test]
    fn edge_is_not_clustered() {
        assert!(!RuntimeRole::Edge.is_clustered());
    }

    #[test]
    fn controller_is_clustered() {
        assert!(RuntimeRole::Controller.is_clustered());
    }

    #[test]
    fn role_as_str() {
        assert_eq!(RuntimeRole::Standalone.as_str(), "standalone");
        assert_eq!(RuntimeRole::Cluster.as_str(), "cluster");
        assert_eq!(RuntimeRole::Edge.as_str(), "edge");
        assert_eq!(RuntimeRole::Controller.as_str(), "controller");
    }

    #[test]
    fn role_display() {
        assert_eq!(format!("{}", RuntimeRole::Standalone), "standalone");
        assert_eq!(format!("{}", RuntimeRole::Cluster), "cluster");
        assert_eq!(format!("{}", RuntimeRole::Edge), "edge");
        assert_eq!(format!("{}", RuntimeRole::Controller), "controller");
    }

    #[test]
    fn role_default_is_standalone() {
        assert_eq!(RuntimeRole::default(), RuntimeRole::Standalone);
    }
}
