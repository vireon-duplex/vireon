//! Slot recycling helpers (Layer 11).
//!
//! On release the pool performs three steps uniformly, whether the slot came
//! from a slab or the huge allocator:
//!
//! 1. [`validate`] — in debug builds, check the head/tail magic to detect
//!    overrun or use-after-free.
//! 2. [`prepare`] — zero the slot according to the configured [`ZeroingPolicy`].
//! 3. The caller then hands the slot to the cache (see `cache::thread_local`).
//!
//! Fresh allocations are stamped by [`stamp_magic`] so that [`validate`] has
//! something to check.

#[cfg(debug_assertions)]
use core::ptr;

use crate::buffer::policy::ZeroingPolicy;

/// Sentinel written at the head and tail of every slot in debug builds.
#[cfg(debug_assertions)]
pub const SLOT_MAGIC: u64 = 0xDEAD_BEEF_CAFE_BABE;

/// Number of bytes occupied by the magic header/footer.
pub const MAGIC_BYTES: usize = core::mem::size_of::<u64>();

/// Minimum slot size that can carry head/tail magic.
pub const MAGIC_MIN_SLOT: usize = MAGIC_BYTES * 2;

/// Stamps the slot's head and tail with a magic value (debug only).
///
/// No-op in release builds.
///
/// # Safety
///
/// `data` must be valid for writes of `len` bytes, and when `len >= MAGIC_MIN_SLOT`
/// the first and last [`MAGIC_BYTES`] of the slot must be writable.
#[inline]
pub unsafe fn stamp_magic(data: *mut u8, len: usize) {
    #[cfg(debug_assertions)]
    {
        // SAFETY: forwarded under the same contract this function declares.
        unsafe { stamp_magic_debug(data, len) };
    }
    // Release builds: intentionally no-op; params are unused.
    let _ = (data, len);
}

#[cfg(debug_assertions)]
#[inline]
unsafe fn stamp_magic_debug(data: *mut u8, len: usize) {
    if len < MAGIC_MIN_SLOT {
        return;
    }
    // SAFETY: caller guarantees head/tail ranges are valid for writes.
    unsafe {
        let last = data.add(len - MAGIC_BYTES);
        ptr::write_unaligned(data as *mut u64, SLOT_MAGIC);
        ptr::write_unaligned(last as *mut u64, SLOT_MAGIC);
    }
}

/// Verifies the magic stamped by [`stamp_magic`] (debug only).
///
/// Always returns `true` in release builds.
///
/// # Safety
///
/// Same preconditions as [`stamp_magic`]: the first and last [`MAGIC_BYTES`]
/// of the slot must be readable when `len >= MAGIC_MIN_SLOT`.
#[inline]
pub unsafe fn validate(data: *mut u8, len: usize) -> bool {
    #[cfg(debug_assertions)]
    {
        // SAFETY: forwarded under the same contract this function declares.
        unsafe { validate_debug(data, len) }
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = (data, len);
        true
    }
}

#[cfg(debug_assertions)]
#[inline]
unsafe fn validate_debug(data: *mut u8, len: usize) -> bool {
    if len < MAGIC_MIN_SLOT {
        return true;
    }
    // SAFETY: caller guarantees head/tail ranges are readable.
    unsafe {
        let last = data.add(len - MAGIC_BYTES);
        let head = ptr::read_unaligned(data as *const u64);
        let tail = ptr::read_unaligned(last as *const u64);
        head == SLOT_MAGIC && tail == SLOT_MAGIC
    }
}

/// Prepares a slot for reuse by zeroing it according to `policy`.
///
/// # Safety
///
/// `data` must be valid for writes of `len` bytes.
#[inline]
pub unsafe fn prepare(data: *mut u8, len: usize, policy: ZeroingPolicy) {
    // SAFETY: forwarded to `zeroing::zero` under the same validity contract.
    unsafe { crate::memory::zeroing::zero(data, len, policy) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_zeroes_a_slot() {
        let mut slot = [0xFFu8; 64];
        // SAFETY: slot is a valid 64-byte write target.
        unsafe { prepare(slot.as_mut_ptr(), slot.len(), ZeroingPolicy::Clear) };
        assert!(slot.iter().all(|&b| b == 0));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn magic_round_trip_detects_corruption_in_debug() {
        let mut slot = vec![0u8; 128];
        // SAFETY: slot owns 128 writable bytes.
        unsafe { stamp_magic(slot.as_mut_ptr(), slot.len()) };
        assert!(unsafe { validate(slot.as_mut_ptr(), slot.len()) });

        // Corrupt the head magic.
        slot[0] = 0x00;
        assert!(!unsafe { validate(slot.as_mut_ptr(), slot.len()) });
    }

    #[cfg(debug_assertions)]
    #[test]
    fn small_slots_skip_magic_safely() {
        let mut slot = [0u8; 4];
        // SAFETY: len < MAGIC_MIN_SLOT; stamp/validate are no-ops on the body.
        unsafe { stamp_magic(slot.as_mut_ptr(), slot.len()) };
        assert!(unsafe { validate(slot.as_mut_ptr(), slot.len()) });
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn validate_returns_true_in_release() {
        let mut slot = [0u8; 64];
        // SAFETY: validate is unconditionally true in release builds.
        assert!(unsafe { validate(slot.as_mut_ptr(), slot.len()) });
    }
}
