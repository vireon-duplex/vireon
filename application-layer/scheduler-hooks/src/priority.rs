//! [`HookPriority`] — priority levels for scheduler hooks.
//!
//! # Design
//!
//! - `HookPriority`: u8-based priority (0 = highest, 255 = lowest)
//! - Predefined constants for common priorities
//! - Explicit comparison methods for clarity

use std::fmt;

// ── HookPriority ──────────────────────────────────────────────────

/// Priority level for hook execution.
///
/// Lower values are more important (executed first).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HookPriority(u8);

impl HookPriority {
    /// Highest priority (executed first).
    pub const HIGHEST: Self = Self(0);

    /// High priority.
    pub const HIGH: Self = Self(32);

    /// Normal (default) priority.
    pub const NORMAL: Self = Self(64);

    /// Low priority.
    pub const LOW: Self = Self(128);

    /// Lowest priority (executed last).
    pub const LOWEST: Self = Self(255);

    /// Construct a new priority from a raw value.
    #[must_use]
    pub const fn new(prio: u8) -> Self {
        Self(prio)
    }

    /// Raw priority value.
    #[must_use]
    pub const fn raw(&self) -> u8 {
        self.0
    }

    /// Whether this is the highest priority.
    #[must_use]
    pub const fn is_highest(&self) -> bool {
        self.raw() == Self::HIGHEST.raw()
    }

    /// Whether this is the lowest priority.
    #[must_use]
    pub const fn is_lowest(&self) -> bool {
        self.raw() == Self::LOWEST.raw()
    }

    /// Whether this priority is more important than another.
    ///
    /// Lower values are more important.
    #[must_use]
    pub const fn is_more_important_than(&self, other: &Self) -> bool {
        self.raw() <= other.raw()
    }
}

impl fmt::Display for HookPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Priority({})", self.0)
    }
}

impl Default for HookPriority {
    fn default() -> Self {
        Self::NORMAL
    }
}

impl PartialOrd for HookPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HookPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse: lower value = higher priority
        other.0.cmp(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_new() {
        let prio = HookPriority::new(100);
        assert_eq!(prio.raw(), 100);
    }

    #[test]
    fn priority_constants() {
        assert_eq!(HookPriority::HIGHEST.raw(), 0);
        assert_eq!(HookPriority::HIGH.raw(), 32);
        assert_eq!(HookPriority::NORMAL.raw(), 64);
        assert_eq!(HookPriority::LOW.raw(), 128);
        assert_eq!(HookPriority::LOWEST.raw(), 255);
    }

    #[test]
    fn priority_is_highest() {
        assert!(HookPriority::HIGHEST.is_highest());
        assert!(!HookPriority::HIGH.is_highest());
        assert!(!HookPriority::LOWEST.is_highest());
    }

    #[test]
    fn priority_is_lowest() {
        assert!(HookPriority::LOWEST.is_lowest());
        assert!(!HookPriority::LOW.is_lowest());
        assert!(!HookPriority::HIGHEST.is_lowest());
    }

    #[test]
    fn priority_is_more_important_than() {
        assert!(HookPriority::HIGHEST.is_more_important_than(&HookPriority::HIGH));
        assert!(HookPriority::HIGH.is_more_important_than(&HookPriority::NORMAL));
        assert!(HookPriority::NORMAL.is_more_important_than(&HookPriority::LOW));
        assert!(HookPriority::LOW.is_more_important_than(&HookPriority::LOWEST));

        assert!(!HookPriority::LOWEST.is_more_important_than(&HookPriority::HIGHEST));
        assert!(!HookPriority::NORMAL.is_more_important_than(&HookPriority::HIGH));
    }

    #[test]
    fn priority_copy_and_clone() {
        let prio1 = HookPriority::new(50);
        let prio2 = prio1;
        assert_eq!(prio1, prio2);
    }

    #[test]
    fn priority_display() {
        let prio = HookPriority::new(42);
        assert_eq!(format!("{}", prio), "Priority(42)");
    }

    #[test]
    fn priority_default() {
        let prio = HookPriority::default();
        assert_eq!(prio, HookPriority::NORMAL);
    }

    #[test]
    fn priority_ordering() {
        assert!(HookPriority::HIGHEST > HookPriority::HIGH);
        assert!(HookPriority::HIGH > HookPriority::NORMAL);
        assert!(HookPriority::NORMAL > HookPriority::LOW);
        assert!(HookPriority::LOW > HookPriority::LOWEST);
    }

    #[test]
    fn priority_hash() {
        // Ensure HookPriority derives Hash.
        let _ = std::collections::HashSet::from([
            HookPriority::HIGHEST,
            HookPriority::NORMAL,
            HookPriority::LOWEST,
        ]);
    }

    #[test]
    fn priority_boundary_values() {
        let highest = HookPriority::new(0);
        let lowest = HookPriority::new(255);
        assert!(highest.is_more_important_than(&lowest));
        assert!(!lowest.is_more_important_than(&highest));
    }
}
