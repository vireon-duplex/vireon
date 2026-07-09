//! [`PartitionId`], [`Offset`], [`PartitionState`] — partition identifiers,
//! offsets, and states.
//!
//! See `DESIGN.md` Part 5 (partitions).

// ── PartitionId ───────────────────────────────────────────────────

/// Unique identifier for a partition within a topic.
///
/// Partitions are numbered 0..N for each topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId(u32);

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partition#{}", self.0)
    }
}

impl PartitionId {
    /// Construct a new partition ID.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

// ── Offset ───────────────────────────────────────────────────────

/// Offset within a partition.
///
/// Offsets are monotonically increasing identifiers for events within
/// a partition. Each new event gets the next offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Offset(u64);

impl Offset {
    /// The starting offset (0).
    pub const ZERO: Self = Self(0);

    /// Construct a new offset.
    #[must_use]
    pub const fn new(offset: u64) -> Self {
        Self(offset)
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Whether this offset is valid (non-negative).
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 != u64::MAX // Reserve u64::MAX as invalid sentinel
    }

    /// Get the next offset (wraps on overflow).
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl Default for Offset {
    fn default() -> Self {
        Self::ZERO
    }
}

// ── PartitionState ────────────────────────────────────────────────

/// State of a partition.
///
/// Partitions can be active, being reassigned, or sealed.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PartitionState {
    /// Partition is active and accepting writes.
    #[default]
    Active = 0,
    /// Partition is being reassigned to another consumer.
    Reassigning = 1,
    /// Partition is sealed (read-only).
    Sealed = 2,
}

impl PartitionState {
    /// Whether the partition is active.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether the partition accepts writes.
    #[must_use]
    pub const fn accepts_writes(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Reassigning => "reassigning",
            Self::Sealed => "sealed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_id_new() {
        let id = PartitionId::new(5);
        assert_eq!(id.raw(), 5);
    }

    #[test]
    fn partition_id_equality() {
        let id1 = PartitionId::new(10);
        let id2 = PartitionId::new(10);
        let id3 = PartitionId::new(11);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn partition_id_ordering() {
        assert!(PartitionId::new(1) < PartitionId::new(2));
        assert!(PartitionId::new(5) > PartitionId::new(3));
    }

    #[test]
    fn offset_new() {
        let offset = Offset::new(100);
        assert_eq!(offset.raw(), 100);
    }

    #[test]
    fn offset_zero() {
        assert_eq!(Offset::ZERO, Offset::new(0));
        assert_eq!(Offset::ZERO.raw(), 0);
    }

    #[test]
    fn offset_default_is_zero() {
        assert_eq!(Offset::default(), Offset::ZERO);
    }

    #[test]
    fn offset_is_valid() {
        assert!(Offset::new(0).is_valid());
        assert!(Offset::new(100).is_valid());
        assert!(!Offset::new(u64::MAX).is_valid());
    }

    #[test]
    fn offset_next() {
        assert_eq!(Offset::new(0).next(), Offset::new(1));
        assert_eq!(Offset::new(100).next(), Offset::new(101));
    }

    #[test]
    fn offset_next_wraps() {
        let offset = Offset::new(u64::MAX);
        let next = offset.next();
        assert_eq!(next.raw(), 0); // wraps to 0
    }

    #[test]
    fn offset_ordering() {
        assert!(Offset::new(1) < Offset::new(2));
        assert!(Offset::new(100) > Offset::new(50));
    }

    #[test]
    fn partition_state_default_is_active() {
        assert_eq!(PartitionState::default(), PartitionState::Active);
    }

    #[test]
    fn partition_state_is_active() {
        assert!(PartitionState::Active.is_active());
        assert!(!PartitionState::Reassigning.is_active());
        assert!(!PartitionState::Sealed.is_active());
    }

    #[test]
    fn partition_state_accepts_writes() {
        assert!(PartitionState::Active.accepts_writes());
        assert!(!PartitionState::Reassigning.accepts_writes());
        assert!(!PartitionState::Sealed.accepts_writes());
    }

    #[test]
    fn partition_state_as_str() {
        assert_eq!(PartitionState::Active.as_str(), "active");
        assert_eq!(PartitionState::Reassigning.as_str(), "reassigning");
        assert_eq!(PartitionState::Sealed.as_str(), "sealed");
    }

    #[test]
    fn partition_state_repr_u8() {
        assert_eq!(PartitionState::Active as u8, 0);
        assert_eq!(PartitionState::Reassigning as u8, 1);
        assert_eq!(PartitionState::Sealed as u8, 2);
    }
}
