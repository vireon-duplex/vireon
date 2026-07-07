//! [`Generation`], [`Epoch`], [`MonotonicVersion`] — versioning
//! primitives for optimistic concurrency and ownership tracking.
//!
//! See `DESIGN.md` Part 13 (version control).
//!
//! # Three layers of versioning
//!
//! | Type | Scope | When bumped |
//! |------|-------|-------------|
//! | `Generation` | Per-session ownership | Each ownership transfer |
//! | `Epoch` | Cluster-wide topology | Membership change (node join/leave) |
//! | `MonotonicVersion` | Per-record | Each metadata mutation |

// ── Generation ──────────────────────────────────────────────────

/// Ownership generation. Per-session, incremented on each
/// ownership transfer. Wrapping u64; practically never overflows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Generation(u64);

impl Generation {
    /// The initial generation for a new session.
    pub const INITIAL: Self = Self(1);

    /// Construct from a raw u64.
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw u64 value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Advance to the next generation (wrapping).
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    /// Whether `self` is strictly newer than `other`.
    #[must_use]
    pub const fn is_newer_than(self, other: Self) -> bool {
        self.0 > other.0
    }
}

impl std::fmt::Display for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gen#{}", self.0)
    }
}

// ── Epoch ───────────────────────────────────────────────────────

/// Cluster epoch. Bumped on every membership change (node join or
/// leave). Used to detect stale ownership decisions across topology
/// changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Epoch(u64);

impl Epoch {
    /// The initial cluster epoch.
    pub const INITIAL: Self = Self(1);

    /// Construct from a raw u64.
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw u64 value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Bump the epoch (wrapping).
    #[must_use]
    pub const fn bump(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    /// Whether `self` happened-after `other`.
    #[must_use]
    pub const fn is_after(self, other: Self) -> bool {
        self.0 > other.0
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "epoch#{}", self.0)
    }
}

// ── MonotonicVersion ────────────────────────────────────────────

/// Monotonically increasing version for optimistic concurrency
/// control on individual records (e.g. lease metadata, capacity
/// counters).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MonotonicVersion(u64);

impl MonotonicVersion {
    /// The zero version (no writes yet).
    pub const ZERO: Self = Self(0);

    /// Construct from a raw u64.
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// The raw u64 value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Increment the version (wrapping).
    #[must_use]
    pub const fn increment(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    /// Whether `self` is strictly after `other`.
    #[must_use]
    pub const fn is_after(self, other: Self) -> bool {
        self.0 > other.0
    }

    /// Whether `self` is strictly before `other`.
    #[must_use]
    pub const fn is_before(self, other: Self) -> bool {
        self.0 < other.0
    }
}

impl std::fmt::Display for MonotonicVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v#{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_next_advances() {
        let g = Generation::INITIAL;
        assert_eq!(g.next().raw(), 2);
        assert!(g.next().is_newer_than(g));
    }

    #[test]
    fn generation_wraps() {
        let g = Generation::new(u64::MAX);
        assert_eq!(g.next().raw(), 0);
    }

    #[test]
    fn generation_ordering() {
        assert!(Generation::new(5).is_newer_than(Generation::new(3)));
        assert!(!Generation::new(3).is_newer_than(Generation::new(5)));
    }

    #[test]
    fn epoch_bump() {
        assert_eq!(Epoch::INITIAL.bump().raw(), 2);
        assert!(Epoch::new(5).is_after(Epoch::new(3)));
    }

    #[test]
    fn monotonic_version_increment() {
        let v = MonotonicVersion::ZERO;
        let v1 = v.increment();
        assert!(v1.is_after(v));
        assert!(v.is_before(v1));
    }

    #[test]
    fn monotonic_version_wraps() {
        let v = MonotonicVersion::new(u64::MAX);
        assert_eq!(v.increment().raw(), 0);
    }

    #[test]
    fn display_formats() {
        assert_eq!(format!("{}", Generation::new(7)), "gen#7");
        assert_eq!(format!("{}", Epoch::new(3)), "epoch#3");
        assert_eq!(format!("{}", MonotonicVersion::new(11)), "v#11");
    }

    #[test]
    fn equality_and_hashing() {
        let g1 = Generation::new(42);
        let g2 = Generation::new(42);
        assert_eq!(g1, g2);
        let mut set = std::collections::HashSet::new();
        set.insert(g1);
        assert!(set.contains(&g2));
    }
}
