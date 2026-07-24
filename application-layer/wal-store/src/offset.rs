//! [`Offset`] — monotonic per-topic sequence number.

use std::fmt;
use std::ops::{Add, Sub};

/// Monotonic per-topic offset.
///
/// Offsets are `u64` under the hood and dense: the first append on a
/// topic returns `Offset(0)`, the next returns `Offset(1)`, and so on.
/// Truncation does not renumber — it advances [`Offset::OLDEST`] past
/// the deleted entries but the surviving entries keep their original
/// offsets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

impl Offset {
    /// Reserved "no offset" sentinel. Distinct from any valid offset
    /// returned by [`crate::WalStore::append`]. Used as the default for
    /// [`crate::ConsumerOffsets`] when a consumer has not acked yet.
    pub const NONE: Self = Self(u64::MAX);

    /// The offset of the very first entry on a fresh topic. Equal to
    /// `Offset(0)`. Returned by `head()` when no entries have been
    /// appended yet (consumers starting fresh ask for "from OLDEST").
    pub const OLDEST: Self = Self(0);

    /// Construct from a raw `u64`. The caller is responsible for the
    /// value's provenance — this is intentionally not `pub(crate)` so
    /// the store backends can build offsets from disk-loaded indices.
    #[inline]
    #[must_use]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// Raw underlying value.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advance by `n`. Saturating — never overflows past `u64::MAX`.
    #[inline]
    #[must_use]
    pub const fn add(self, n: u64) -> Self {
        Self(self.0.saturating_add(n))
    }

    /// Retreat by `n`. Saturating — never underflows below 0.
    #[inline]
    #[must_use]
    pub const fn sub(self, n: u64) -> Self {
        Self(self.0.saturating_sub(n))
    }

    /// `true` when this is the reserved "none" sentinel.
    #[inline]
    #[must_use]
    pub const fn is_none(self) -> bool {
        self.0 == Self::NONE.0
    }

    /// Next contiguous offset. Used internally by the store when it
    /// allocates the offset for an `append`.
    #[inline]
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_none() {
            write!(f, "off:none")
        } else {
            write!(f, "off:{}", self.0)
        }
    }
}

impl Add<u64> for Offset {
    type Output = Self;
    #[inline]
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.wrapping_add(rhs))
    }
}

impl Sub<u64> for Offset {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.saturating_sub(rhs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_round_trip() {
        let o = Offset::new(42);
        assert_eq!(o.get(), 42);
        assert_eq!(o, Offset(42));
    }

    #[test]
    fn offset_next() {
        assert_eq!(Offset::new(0).next(), Offset::new(1));
        assert_eq!(Offset::new(99).next(), Offset::new(100));
    }

    #[test]
    fn offset_ordering() {
        assert!(Offset::new(1) < Offset::new(2));
        assert!(Offset::OLDEST < Offset::new(5));
    }

    #[test]
    fn offset_none_is_distinguished() {
        assert!(Offset::NONE.is_none());
        assert!(!Offset::new(0).is_none());
        assert!(!Offset::new(1).is_none());
    }

    #[test]
    fn offset_add_sub() {
        assert_eq!(Offset::new(5).add(3), Offset::new(8));
        assert_eq!(Offset::new(5).sub(3), Offset::new(2));
        assert_eq!(Offset::new(5).sub(99), Offset::new(0), "saturates at zero");
    }

    #[test]
    fn offset_display() {
        assert_eq!(format!("{}", Offset::new(7)), "off:7");
        assert_eq!(format!("{}", Offset::NONE), "off:none");
    }
}
