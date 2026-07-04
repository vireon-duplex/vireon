//! Alignment helpers.
//!
//! Every slot handed out by the pool is aligned to at least [`MIN_ALIGN`] (one
//! cache line) so SIMD and atomic accesses on buffer contents are sound.

/// Minimum alignment guaranteed by the pool: one x86/ARM cache line.
pub const MIN_ALIGN: usize = 64;

/// Rounds `n` up to the next multiple of `align`.
///
/// Returns `None` when `align` is not a positive power of two or when the
/// result would overflow `usize`.
#[inline]
pub fn align_up(n: usize, align: usize) -> Option<usize> {
    if align == 0 || (align & align.wrapping_sub(1)) != 0 {
        return None;
    }
    let mask = align - 1;
    n.checked_add(mask).map(|v| v & !mask)
}

/// Rounds `n` down to the previous multiple of `align` (power of two).
///
/// Returns `n` unchanged when `align` is not a positive power of two.
#[inline]
pub fn align_down(n: usize, align: usize) -> usize {
    if align == 0 || (align & align.wrapping_sub(1)) != 0 {
        return n;
    }
    n & !(align - 1)
}

/// True when `ptr` is aligned to `align`.
#[inline]
pub fn is_aligned(ptr: usize, align: usize) -> bool {
    align != 0 && (align & align.wrapping_sub(1)) == 0 && (ptr & (align - 1)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_rounds_to_multiple() {
        assert_eq!(align_up(1, 64), Some(64));
        assert_eq!(align_up(64, 64), Some(64));
        assert_eq!(align_up(65, 64), Some(128));
        assert_eq!(align_up(100, 128), Some(128));
        assert_eq!(align_up(4097, 4096), Some(8192));
    }

    #[test]
    fn align_up_rejects_non_power_of_two() {
        assert_eq!(align_up(10, 0), None);
        assert_eq!(align_up(10, 3), None);
        assert_eq!(align_up(10, 96), None); // 96 is not a power of two
    }

    #[test]
    fn align_up_detects_overflow() {
        assert_eq!(align_up(usize::MAX, 64), None);
        assert_eq!(align_up(usize::MAX - 10, 64), None);
    }

    #[test]
    fn align_down_floors() {
        assert_eq!(align_down(65, 64), 64);
        assert_eq!(align_down(63, 64), 0);
        assert_eq!(align_down(4096, 4096), 4096);
    }

    #[test]
    fn align_down_ignores_bad_align() {
        assert_eq!(align_down(65, 0), 65);
        assert_eq!(align_down(65, 3), 65);
    }

    #[test]
    fn is_aligned_checks_pointer() {
        assert!(is_aligned(0, 64));
        assert!(is_aligned(64, 64));
        assert!(is_aligned(128, 64));
        assert!(!is_aligned(1, 64));
        assert!(!is_aligned(63, 64));
        assert!(!is_aligned(64, 0));
    }
}
