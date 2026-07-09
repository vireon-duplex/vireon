//! [`ConsumerGroupId`], [`ConsumerId`], [`SubscriptionMode`] — consumer
//! identifiers and subscription modes.
//!
//! See `DESIGN.md` Part 6 (consumers).

// ── ConsumerGroupId ───────────────────────────────────────────────

/// Unique identifier for a consumer group.
///
/// Consumer groups allow parallel consumption of topics with offset
/// tracking per group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConsumerGroupId(u64);

impl ConsumerGroupId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new consumer group ID.
    ///
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil consumer group ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil consumer group ID.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for ConsumerGroupId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for ConsumerGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group#{}", self.0)
    }
}

// ── ConsumerId ────────────────────────────────────────────────────

/// Unique identifier for a consumer within a group.
///
/// Each consumer in a group has a unique ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConsumerId(u64);

impl ConsumerId {
    /// Construct a new consumer ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

// ── SubscriptionMode ─────────────────────────────────────────────

/// Mode for consuming events from a partition.
///
/// Consumers can start from the earliest offset, the latest offset,
/// or a specific offset.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SubscriptionMode {
    /// Start from the earliest available offset.
    #[default]
    Earliest = 0,
    /// Start from the latest offset (only new messages).
    Latest = 1,
    /// Start from a specific offset.
    FromOffset(u64) = 2,
}

impl SubscriptionMode {
    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::FromOffset(_) => "from_offset",
        }
    }

    /// Get the starting offset if this is a `FromOffset` mode.
    #[must_use]
    pub const fn offset(&self) -> Option<u64> {
        match self {
            Self::FromOffset(offset) => Some(*offset),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_group_id_new() {
        let id = ConsumerGroupId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn consumer_group_id_nil() {
        let id = ConsumerGroupId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn consumer_group_id_nil_constant() {
        assert_eq!(ConsumerGroupId::NIL, ConsumerGroupId::nil());
        assert!(ConsumerGroupId::NIL.is_nil());
    }

    #[test]
    fn consumer_group_id_default_is_nil() {
        let id = ConsumerGroupId::default();
        assert_eq!(id, ConsumerGroupId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn consumer_id_new() {
        let id = ConsumerId::new(42);
        assert_eq!(id.raw(), 42);
    }

    #[test]
    fn subscription_mode_default_is_earliest() {
        assert_eq!(SubscriptionMode::default(), SubscriptionMode::Earliest);
    }

    #[test]
    fn subscription_mode_as_str() {
        assert_eq!(SubscriptionMode::Earliest.as_str(), "earliest");
        assert_eq!(SubscriptionMode::Latest.as_str(), "latest");
        assert_eq!(SubscriptionMode::FromOffset(100).as_str(), "from_offset");
    }

    #[test]
    fn subscription_mode_offset() {
        assert_eq!(SubscriptionMode::Earliest.offset(), None);
        assert_eq!(SubscriptionMode::Latest.offset(), None);
        assert_eq!(SubscriptionMode::FromOffset(500).offset(), Some(500));
    }

    #[test]
    fn subscription_mode_repr_u8() {
        // SubscriptionMode has a payload variant (FromOffset(u64)),
        // so `as u8` is not available. Verify via as_str mapping instead.
        assert_eq!(SubscriptionMode::Earliest.as_str(), "earliest");
        assert_eq!(SubscriptionMode::Latest.as_str(), "latest");
        assert_eq!(SubscriptionMode::FromOffset(0).as_str(), "from_offset");
    }

    #[test]
    fn subscription_mode_from_offset_variants_distinct() {
        let mode1 = SubscriptionMode::FromOffset(100);
        let mode2 = SubscriptionMode::FromOffset(200);
        assert_ne!(mode1, mode2);
    }

    #[test]
    fn consumer_group_id_equality() {
        let id1 = ConsumerGroupId::new(42);
        let id2 = ConsumerGroupId::new(42);
        let id3 = ConsumerGroupId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn consumer_id_equality() {
        let id1 = ConsumerId::new(10);
        let id2 = ConsumerId::new(10);
        let id3 = ConsumerId::new(11);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn subscription_mode_all_variants_distinct() {
        let modes = [
            SubscriptionMode::Earliest,
            SubscriptionMode::Latest,
            SubscriptionMode::FromOffset(0),
            SubscriptionMode::FromOffset(100),
        ];
        let mut seen = std::collections::HashSet::new();
        for mode in modes {
            assert!(seen.insert(mode), "duplicate mode variant");
        }
    }
}
