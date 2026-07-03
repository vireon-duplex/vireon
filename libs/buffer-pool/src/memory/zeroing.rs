//! Buffer zeroing strategies (Layer 10).
//!
//! [`zero`] is the single entry point called on release. [`ZeroingPolicy::None`]
//! and [`ZeroingPolicy::Lazy`] are no-ops here; the lazy strategy defers
//! clearing to the next acquire (handled by the pool, not this module).

use core::ptr;

use crate::buffer::policy::ZeroingPolicy;

/// Zeroes `len` bytes starting at `data` according to `policy`.
///
/// # Safety
///
/// `data` must be valid for writes of `len` bytes. With
/// [`ZeroingPolicy::None`] or [`ZeroingPolicy::Lazy`] this function performs no
/// writes, but the validity contract still holds.
#[inline]
pub unsafe fn zero(data: *mut u8, len: usize, policy: ZeroingPolicy) {
    // SAFETY: the caller of `zero` upholds the validity contract that this
    // function itself declares; the delegated calls inherit it.
    unsafe {
        match policy {
            ZeroingPolicy::None | ZeroingPolicy::Lazy => {}
            ZeroingPolicy::Clear => zero_clear(data, len),
            ZeroingPolicy::Secure => zero_secure(data, len),
        }
    }
}

#[inline]
unsafe fn zero_clear(data: *mut u8, len: usize) {
    if len == 0 {
        return;
    }
    // SAFETY: caller guarantees `data` is valid for `len` writes.
    unsafe { ptr::write_bytes(data, 0, len) };
}

#[inline]
unsafe fn zero_secure(data: *mut u8, len: usize) {
    // Volatile writes cannot be elided by the optimiser even when the store
    // appears dead — required for wiping secrets before recycling a slot.
    for i in 0..len {
        // SAFETY: caller guarantees the full `[0, len)` range is writable.
        unsafe { ptr::write_volatile(data.add(i), 0) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_zeroes_all_bytes() {
        let mut buf = [0xAAu8; 128];
        // SAFETY: `buf` is a valid 128-byte write target for its lifetime.
        unsafe { zero(buf.as_mut_ptr(), buf.len(), ZeroingPolicy::Clear) };
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn secure_zeroes_all_bytes() {
        let mut buf = [0x55u8; 64];
        // SAFETY: `buf` is a valid 64-byte write target for its lifetime.
        unsafe { zero(buf.as_mut_ptr(), buf.len(), ZeroingPolicy::Secure) };
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn none_leaves_bytes_untouched() {
        let mut buf = [0x77u8; 32];
        // SAFETY: no writes occur for `None`; validity contract still holds.
        unsafe { zero(buf.as_mut_ptr(), buf.len(), ZeroingPolicy::None) };
        assert!(buf.iter().all(|&b| b == 0x77));
    }

    #[test]
    fn lazy_is_a_noop_at_release_time() {
        let mut buf = [0x11u8; 16];
        // SAFETY: no writes occur for `Lazy`; validity contract still holds.
        unsafe { zero(buf.as_mut_ptr(), buf.len(), ZeroingPolicy::Lazy) };
        assert!(buf.iter().all(|&b| b == 0x11));
    }

    #[test]
    fn zero_len_is_safe() {
        let mut buf = [0u8; 0];
        // SAFETY: zero-length slice is valid for zero writes.
        unsafe { zero(buf.as_mut_ptr(), 0, ZeroingPolicy::Secure) };
    }
}
