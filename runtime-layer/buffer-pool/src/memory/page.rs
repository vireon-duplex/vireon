//! Page-size constants and helpers.

use crate::memory::alignment::align_up;

/// Standard page size on most Linux/x86-64 systems (4 KiB).
pub const PAGE_SIZE: usize = 4096;

/// Rounds `n` up to a whole number of [`PAGE_SIZE`] pages.
///
/// Returns `None` on overflow.
#[inline]
pub fn page_align_up(n: usize) -> Option<usize> {
    align_up(n, PAGE_SIZE)
}

/// Number of [`PAGE_SIZE`] pages needed to hold `n` bytes (at least 1).
#[inline]
pub fn pages_for(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    n.div_ceil(PAGE_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_align_up_rounds() {
        assert_eq!(page_align_up(0), Some(0));
        assert_eq!(page_align_up(1), Some(PAGE_SIZE));
        assert_eq!(page_align_up(PAGE_SIZE), Some(PAGE_SIZE));
        assert_eq!(page_align_up(PAGE_SIZE + 1), Some(PAGE_SIZE * 2));
    }

    #[test]
    fn pages_for_nonzero_minimum() {
        assert_eq!(pages_for(0), 1);
        assert_eq!(pages_for(1), 1);
        assert_eq!(pages_for(PAGE_SIZE), 1);
        assert_eq!(pages_for(PAGE_SIZE + 1), 2);
        assert_eq!(pages_for(PAGE_SIZE * 5), 5);
    }
}
