//! [`BitmapScheduler`] — O(1) urgency-band lookup via bitmask.
//!
//! See `DESIGN.md` Part 9 (data structures) and Part 13
//! (performance targets).
//!
//! # Why a bitmap?
//!
//! The [`ReadyQueue`](crate::queue::ReadyQueue) has 8 urgency
//! bands. To find the highest-priority non-empty band, a linear scan
//! costs 8 comparisons. A bitmask reduces this to a single
//! `trailing_ones()` (one instruction on x86/ARM).
//!
//! Each bit in the `u8` mask represents one band: bit 0 = urgency 0,
//! bit 7 = urgency 7. A set bit means "this band has at least one
//! ready stream".
//!
//! # Phase 0 status
//!
//! Phase 0 ships the mask + bit operations. Integration with the
//! `ReadyQueue` lands in Phase 1.

/// 8-bit mask: one bit per urgency band (0..7).
pub type BandMask = u8;

/// All-bits-set mask (every band non-empty).
pub const MASK_ALL_FULL: BandMask = 0xFF;
/// All-bits-clear mask (every band empty).
pub const MASK_ALL_EMPTY: BandMask = 0x00;

/// Bitmap tracker for urgency bands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BitmapScheduler {
    mask: BandMask,
}

impl BitmapScheduler {
    /// Construct an empty bitmap.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            mask: MASK_ALL_EMPTY,
        }
    }

    /// The raw mask value.
    #[must_use]
    pub const fn mask(&self) -> BandMask {
        self.mask
    }

    /// Whether all bands are empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.mask == MASK_ALL_EMPTY
    }

    /// Mark a band as non-empty.
    pub const fn mark_non_empty(&mut self, urgency: u8) {
        if (urgency as u32) < 8 {
            self.mask |= 1 << urgency;
        }
    }

    /// Mark a band as empty.
    pub const fn mark_empty(&mut self, urgency: u8) {
        if (urgency as u32) < 8 {
            self.mask &= !(1 << urgency);
        }
    }

    /// Lowest (most urgent) non-empty band, or `None` if all empty.
    ///
    /// This is `trailing_ones()` on the mask — one CPU instruction.
    #[must_use]
    pub const fn first_non_empty_band(&self) -> Option<u8> {
        if self.mask == MASK_ALL_EMPTY {
            None
        } else {
            // trailing_zeros gives the index of the lowest set bit
            Some(self.mask.trailing_zeros() as u8)
        }
    }

    /// Whether a specific band is non-empty.
    #[must_use]
    pub const fn band_is_non_empty(&self, urgency: u8) -> bool {
        if (urgency as u32) < 8 {
            (self.mask & (1 << urgency)) != 0
        } else {
            false
        }
    }

    /// Count of non-empty bands.
    #[must_use]
    pub const fn non_empty_count(&self) -> u32 {
        self.mask.count_ones()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bitmap_has_no_bands() {
        let b = BitmapScheduler::new();
        assert!(b.is_empty());
        assert_eq!(b.first_non_empty_band(), None);
        assert_eq!(b.non_empty_count(), 0);
    }

    #[test]
    fn mark_and_lookup() {
        let mut b = BitmapScheduler::new();
        b.mark_non_empty(3);
        b.mark_non_empty(5);
        assert!(!b.is_empty());
        assert_eq!(b.first_non_empty_band(), Some(3));
        assert_eq!(b.non_empty_count(), 2);
        assert!(b.band_is_non_empty(3));
        assert!(b.band_is_non_empty(5));
        assert!(!b.band_is_non_empty(4));
    }

    #[test]
    fn lowest_band_wins() {
        let mut b = BitmapScheduler::new();
        b.mark_non_empty(7);
        b.mark_non_empty(0);
        b.mark_non_empty(4);
        assert_eq!(b.first_non_empty_band(), Some(0));
    }

    #[test]
    fn mark_empty_clears() {
        let mut b = BitmapScheduler::new();
        b.mark_non_empty(2);
        b.mark_non_empty(4);
        b.mark_empty(2);
        assert_eq!(b.first_non_empty_band(), Some(4));
    }

    #[test]
    fn out_of_range_urgency_ignored() {
        let mut b = BitmapScheduler::new();
        b.mark_non_empty(99);
        assert!(b.is_empty());
    }
}
