//! Slab allocator (Layer 3).
//!
//! [`SlabAllocator<T>`] pre-allocates a fixed backing of `capacity` slots and
//! hands out [`NonZeroU32`] indices from an O(1) free-list stack. Slot `0` is
//! reserved as the null sentinel used by [`crate::core::bucket`] link fields, so
//! usable indices are `1..=capacity`.
//!
//! ## Concurrency
//!
//! The slab is **single-threaded by construction** — only the owning worker
//! touches it. There is therefore no atomics or locking on the fast path.

use std::mem::MaybeUninit;
use std::num::NonZeroU32;

/// Pre-allocated, free-list-backed slot container.
///
/// `T` must be `Copy` so freed slots can be returned to the free-list without
/// running drop glue on potentially-uninitialised memory.
pub struct SlabAllocator<T: Copy> {
    backing: Box<[MaybeUninit<T>]>,
    free_list: Vec<u32>,
    capacity: usize,
}

impl<T: Copy> SlabAllocator<T> {
    /// Allocates a slab with `capacity` usable slots (plus the reserved
    /// null slot at index 0).
    ///
    /// Returns `None` if `capacity` is zero or would overflow `u32 - 1`.
    #[must_use]
    pub fn new(capacity: usize) -> Option<Self> {
        if capacity == 0 || capacity >= u32::MAX as usize {
            return None;
        }
        // total slots = capacity + 1 (slot 0 reserved as null sentinel)
        let total = capacity.checked_add(1)?;
        let mut backing: Box<[MaybeUninit<T>]> =
            (0..total).map(|_| MaybeUninit::uninit()).collect();
        let _ = &mut backing; // ensure assigned
        // free-list seeded with usable indices 1..=capacity, largest first so
        // the smallest indices are handed out first (cache-friendlier).
        let mut free_list = Vec::with_capacity(capacity);
        for idx in (1..=capacity).rev() {
            free_list.push(idx as u32);
        }
        Some(Self {
            backing,
            free_list,
            capacity,
        })
    }

    /// Number of usable slots (excluding the null sentinel).
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of slots currently handed out.
    #[inline]
    #[must_use]
    pub fn used(&self) -> usize {
        self.capacity - self.free_list.len()
    }

    /// Number of slots immediately available.
    #[inline]
    #[must_use]
    pub fn free_slots(&self) -> usize {
        self.free_list.len()
    }

    /// Pops the next free slot and returns its non-zero index.
    ///
    /// Returns `None` when the slab is exhausted.
    #[inline]
    pub fn allocate(&mut self) -> Option<NonZeroU32> {
        let idx = self.free_list.pop()?;
        // SAFETY: free_list only ever holds values in [1, capacity], which are
        // non-zero by construction, so `idx` is a valid NonZeroU32.
        NonZeroU32::new(idx)
    }

    /// Writes `value` into `idx`. The slot must have been returned by
    /// [`allocate`](Self::allocate) and not yet freed.
    #[inline]
    pub fn write(&mut self, idx: NonZeroU32, value: T) {
        let i = idx.get() as usize;
        debug_assert!(i != 0 && i < self.backing.len(), "slab write out of range");
        // SAFETY: `i` is in `[1, capacity]` (caller obligation + debug_assert),
        // and `backing` has `capacity + 1` slots, so `i < backing.len()`.
        unsafe {
            self.backing.get_unchecked_mut(i).write(value);
        }
    }

    /// Borrows the value at `idx` (read-only).
    ///
    /// # Safety obligation (caller)
    ///
    /// `idx` must reference a slot that has been allocated **and** written,
    /// and not yet freed.
    #[inline]
    pub fn get(&self, idx: u32) -> &T {
        let i = idx as usize;
        debug_assert!(i != 0 && i < self.backing.len(), "slab read out of range");
        // SAFETY: see `write`; caller guarantees the slot is initialised.
        unsafe { self.backing.get_unchecked(i).assume_init_ref() }
    }

    /// Mutably borrows the value at `idx`.
    ///
    /// # Safety obligation (caller)
    ///
    /// Same as [`get`](Self::get).
    #[inline]
    pub fn get_mut(&mut self, idx: u32) -> &mut T {
        let i = idx as usize;
        debug_assert!(
            i != 0 && i < self.backing.len(),
            "slab mut read out of range"
        );
        // SAFETY: see `write`; caller guarantees the slot is initialised.
        unsafe { self.backing.get_unchecked_mut(i).assume_init_mut() }
    }

    /// Returns `idx` to the free-list.
    ///
    /// Silently ignores the null index (0) and out-of-range indices to make
    /// cancellation of already-fired handles a no-op rather than a hard error.
    #[inline]
    pub fn free(&mut self, idx: NonZeroU32) {
        let i = idx.get();
        if i as usize > self.capacity {
            return;
        }
        self.free_list.push(i);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_zero_and_overflowing_capacity() {
        assert!(SlabAllocator::<u64>::new(0).is_none());
        assert!(SlabAllocator::<u64>::new(u32::MAX as usize).is_none());
    }

    #[test]
    fn fresh_slab_reports_full_free_list() {
        let slab: SlabAllocator<u64> = SlabAllocator::new(8).expect("some slab");
        assert_eq!(slab.capacity(), 8);
        assert_eq!(slab.used(), 0);
        assert_eq!(slab.free_slots(), 8);
    }

    #[test]
    fn allocate_write_get_round_trip() {
        let mut slab: SlabAllocator<u64> = SlabAllocator::new(4).expect("some slab");
        let a = slab.allocate().expect("slot");
        slab.write(a, 0xAABB);
        assert_eq!(slab.get(a.get()), &0xAABB);
        assert_eq!(slab.used(), 1);
        assert_eq!(slab.free_slots(), 3);
    }

    #[test]
    fn allocate_returns_none_when_exhausted() {
        let mut slab: SlabAllocator<u64> = SlabAllocator::new(2).expect("some slab");
        let _ = slab.allocate().expect("slot");
        let _ = slab.allocate().expect("slot");
        assert!(slab.allocate().is_none());
        assert_eq!(slab.free_slots(), 0);
    }

    #[test]
    fn free_returns_slot_for_reuse() {
        let mut slab: SlabAllocator<u64> = SlabAllocator::new(2).expect("some slab");
        let a = slab.allocate().expect("slot");
        slab.write(a, 1);
        slab.free(a);
        assert_eq!(slab.used(), 0);
        // re-allocating gives back a usable slot
        let b = slab.allocate().expect("slot");
        slab.write(b, 2);
        assert_eq!(slab.get(b.get()), &2);
    }

    #[test]
    fn free_ignores_out_of_range() {
        let mut slab: SlabAllocator<u64> = SlabAllocator::new(2).expect("some slab");
        let _ = slab.allocate();
        let bogus = NonZeroU32::new(u32::MAX).expect("nz");
        slab.free(bogus); // no-op, no panic
        assert_eq!(slab.used(), 1);
    }

    #[test]
    fn slot_zero_is_never_handed_out() {
        let mut slab: SlabAllocator<u64> = SlabAllocator::new(16).expect("some slab");
        for _ in 0..16 {
            let idx = slab.allocate().expect("slot");
            assert_ne!(idx.get(), 0);
        }
    }
}
