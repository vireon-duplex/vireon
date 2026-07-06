//! [`SessionClass`] — session classification for scheduling.
//!
//! See `DESIGN.md` Part 10 (session scheduling) and Part 16
//! (performance).
//!
//! # Classes
//!
//! Each session is classified into one of four traffic profiles.
//! The [`SessionScheduler`](crate::scheduler::SessionScheduler) uses
//! this to apply per-class scheduling policies, window sizes, and
//! CPU budgets:
//!
//! | Class | Urgency | Weight | Incremental | Example |
//! |-------|---------|--------|-------------|---------|
//! | LatencySensitive | 0–1 | 8 | ✗ | Trading, gaming, RPC |
//! | Interactive | 2–3 | 4 | ✗ | Chat, web, APIs |
//! | Streaming | 4–5 | 2 | ✓ | Video, bulk file transfer |
//! | Bulk | 6–7 | 1 | ✗ | Backups, replication |

/// Session traffic classification.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SessionClass {
    /// Ultra-low-latency: trading, gaming, real-time RPC.
    /// Smallest windows, highest priority, no incremental.
    LatencySensitive,
    /// Interactive: chat, web, API calls. Default class.
    #[default]
    Interactive,
    /// Streaming: video on demand, large file transfer. Large
    /// windows, incremental delivery.
    Streaming,
    /// Bulk: backups, replication, background sync. Lowest
    /// priority, uses spare bandwidth.
    Bulk,
}

impl SessionClass {
    /// Base RFC 9218 urgency for this class (lower = higher
    /// priority). Individual sessions can deviate ±1.
    #[must_use]
    pub const fn urgency_base(self) -> u8 {
        match self {
            Self::LatencySensitive => 0,
            Self::Interactive => 2,
            Self::Streaming => 4,
            Self::Bulk => 6,
        }
    }

    /// WFQ (Weighted Fair Queuing) weight. Higher = more bandwidth
    /// share.
    #[must_use]
    pub const fn weight(self) -> u32 {
        match self {
            Self::LatencySensitive => 8,
            Self::Interactive => 4,
            Self::Streaming => 2,
            Self::Bulk => 1,
        }
    }

    /// Whether this class benefits from incremental (partial)
    /// delivery per RFC 9218.
    #[must_use]
    pub const fn incremental(self) -> bool {
        matches!(self, Self::Streaming)
    }

    /// Urgency range for this class: `[urgency_base, urgency_base+1]`.
    #[must_use]
    pub const fn urgency_range(self) -> core::ops::RangeInclusive<u8> {
        self.urgency_base()..=(self.urgency_base() + 1)
    }

    /// Whether a given urgency value is valid for this class.
    #[must_use]
    pub const fn accepts_urgency(self, urgency: u8) -> bool {
        let base = self.urgency_base();
        urgency >= base && urgency <= base.saturating_add(1)
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LatencySensitive => "latency_sensitive",
            Self::Interactive => "interactive",
            Self::Streaming => "streaming",
            Self::Bulk => "bulk",
        }
    }

    /// All classes, ordered from highest to lowest priority.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [
            Self::LatencySensitive,
            Self::Interactive,
            Self::Streaming,
            Self::Bulk,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_interactive() {
        assert_eq!(SessionClass::default(), SessionClass::Interactive);
    }

    #[test]
    fn urgency_base_ordering() {
        assert!(
            SessionClass::LatencySensitive.urgency_base()
                < SessionClass::Interactive.urgency_base()
        );
        assert!(SessionClass::Interactive.urgency_base() < SessionClass::Streaming.urgency_base());
        assert!(SessionClass::Streaming.urgency_base() < SessionClass::Bulk.urgency_base());
    }

    #[test]
    fn weight_ordering() {
        assert!(SessionClass::LatencySensitive.weight() > SessionClass::Interactive.weight());
        assert!(SessionClass::Interactive.weight() > SessionClass::Streaming.weight());
        assert!(SessionClass::Streaming.weight() > SessionClass::Bulk.weight());
    }

    #[test]
    fn streaming_is_incremental() {
        assert!(SessionClass::Streaming.incremental());
        assert!(!SessionClass::Interactive.incremental());
        assert!(!SessionClass::LatencySensitive.incremental());
    }

    #[test]
    fn urgency_range_accepts_valid() {
        assert!(SessionClass::LatencySensitive.accepts_urgency(0));
        assert!(SessionClass::LatencySensitive.accepts_urgency(1));
        assert!(!SessionClass::LatencySensitive.accepts_urgency(2));
        assert!(SessionClass::Bulk.accepts_urgency(6));
        assert!(SessionClass::Bulk.accepts_urgency(7));
        assert!(!SessionClass::Bulk.accepts_urgency(5));
    }

    #[test]
    fn as_str_all_variants() {
        for c in SessionClass::all() {
            assert!(!c.as_str().is_empty());
        }
    }
}
