//! Slot container (Layer 5).
//!
//! A [`Slab`] owns one contiguous aligned backing allocation and divides it
//! into `slot_count` equal-sized slots. Free slots are tracked by a stack
//! ( [`Vec<u32>`] ) so `alloc_slot` and `mark_free_slot` are O(1).
//!
//! ## Fast path
//!
//! The pool's per-buffer fast path never touches the slab — it pushes/pops the
//! thread-local cache. Slab methods run only on cache refill (allocate_slot)
//! and during maintenance (mark_free / purge).

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

use crate::allocator::size_class::ClassId;
use crate::error::BufferPoolError;
use crate::memory::alignment::MIN_ALIGN;

/// Geometry of a slab, derived from the size class.
#[derive(Clone, Copy, Debug)]
pub struct SlabGeometry {
    /// Number of equal-sized slots in the slab.
    pub slot_count: usize,
    /// Total backing size in bytes.
    pub slab_bytes: usize,
}

/// Computes the slab geometry for a slot size.
///
/// Targets 512 slots per slab, clamped to `[4 KiB, 2 MiB]` and rounded up to a
/// whole number of slots so the backing is fully utilised.
pub fn slab_geometry_for(slot_size: usize) -> SlabGeometry {
    const TARGET_SLOTS: usize = 512;
    const MAX_SLAB_BYTES: usize = 1 << 21; // 2 MiB
    const MIN_SLAB_BYTES: usize = 4096;

    let raw = slot_size.saturating_mul(TARGET_SLOTS);
    let bounded = raw.clamp(MIN_SLAB_BYTES, MAX_SLAB_BYTES).max(slot_size);
    let slab_bytes = crate::memory::alignment::align_up(bounded, slot_size).unwrap_or(bounded);
    let slot_count = slab_bytes / slot_size;
    SlabGeometry {
        slot_count,
        slab_bytes,
    }
}

/// Owns a contiguous aligned backing and hands out fixed-size slots.
pub struct Slab {
    backing_ptr: NonNull<u8>,
    layout: Layout,
    base: usize,
    slot_size: usize,
    slot_count: usize,
    class: ClassId,
    free_list: Vec<u32>,
}

// SAFETY: the slab owns its backing exclusively. Outstanding pointers into the
// backing are raw `NonNull`s held by caches/buffers; they do not borrow from
// the `Slab` struct, so moving the slab across threads is sound.
unsafe impl Send for Slab {}

impl Slab {
    /// Allocates a fresh slab with `geometry.slot_count` slots of
    /// `slot_size` bytes, backed by an allocation aligned to `align`.
    pub fn new(
        class: ClassId,
        slot_size: usize,
        align: usize,
        geometry: SlabGeometry,
    ) -> Result<Self, BufferPoolError> {
        if slot_size == 0 || geometry.slot_count == 0 {
            return Err(BufferPoolError::InvalidConfig(
                "slot_size and slot_count must be non-zero".into(),
            ));
        }
        let layout =
            Layout::from_size_align(geometry.slab_bytes, align.max(MIN_ALIGN)).map_err(|_| {
                BufferPoolError::Oversized {
                    size: geometry.slab_bytes,
                    max: isize::MAX as usize,
                }
            })?;
        // SAFETY: `layout` is valid (non-zero size, power-of-two align).
        let ptr = unsafe { alloc(layout) };
        let backing_ptr = NonNull::new(ptr).ok_or(BufferPoolError::OutOfMemory {
            size: geometry.slab_bytes,
        })?;
        let base = backing_ptr.as_ptr() as usize;
        let free_list = (0..geometry.slot_count as u32).collect();
        Ok(Self {
            backing_ptr,
            layout,
            base,
            slot_size,
            slot_count: geometry.slot_count,
            class,
            free_list,
        })
    }

    /// Slot size in bytes.
    #[inline]
    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    /// Total number of slots.
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.slot_count
    }

    /// Size class this slab belongs to.
    #[inline]
    pub fn class(&self) -> ClassId {
        self.class
    }

    /// Total backing size in bytes.
    #[inline]
    pub fn slab_bytes(&self) -> usize {
        self.layout.size()
    }

    /// Number of slots currently free (available for immediate allocation).
    #[inline]
    pub fn free_slots(&self) -> usize {
        self.free_list.len()
    }

    /// Number of slots currently handed out or sitting in a cache.
    #[inline]
    pub fn used_slots(&self) -> usize {
        self.slot_count - self.free_list.len()
    }

    /// True when every slot is free — the slab is eligible for purging.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.free_list.len() == self.slot_count
    }

    /// True when no slot can be allocated without growing.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.free_list.is_empty()
    }

    /// True when `ptr` lies within this slab's backing.
    #[inline]
    pub fn contains(&self, ptr: NonNull<u8>) -> bool {
        let addr = ptr.as_ptr() as usize;
        let end = self.base + self.slot_count * self.slot_size;
        addr >= self.base && addr < end
    }

    /// Pops the next free slot and returns its pointer.
    ///
    /// Returns `None` when the slab is full.
    #[inline]
    pub fn alloc_slot(&mut self) -> Option<NonNull<u8>> {
        let idx = self.free_list.pop()?;
        let offset = idx as usize * self.slot_size;
        // SAFETY: `idx < slot_count` (it came from the free_list which only
        // holds values in `[0, slot_count)`), so `offset < slot_count *
        // slot_size = slab_bytes`. The backing is a valid allocation of
        // `slab_bytes` starting at `backing_ptr`, so the derived pointer is
        // non-null and in-bounds.
        unsafe {
            let ptr = self.backing_ptr.as_ptr().add(offset);
            Some(NonNull::new_unchecked(ptr))
        }
    }

    /// Returns the slot owning `ptr` to the free list.
    ///
    /// Caller must have verified [`contains`](Self::contains) first.
    #[inline]
    pub fn mark_free_slot(&mut self, ptr: NonNull<u8>) {
        let offset = (ptr.as_ptr() as usize) - self.base;
        // Defensive: only push when the offset is slot-aligned and in range.
        if offset <= self.slot_count * self.slot_size && offset % self.slot_size == 0 {
            self.free_list.push((offset / self.slot_size) as u32);
        }
    }
}

impl Drop for Slab {
    fn drop(&mut self) {
        // SAFETY: `backing_ptr`/`layout` came from the matching `alloc` call
        // in `Slab::new`, and the slab owns them exclusively.
        unsafe {
            dealloc(self.backing_ptr.as_ptr(), self.layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_slab(slot_size: usize, align: usize) -> Slab {
        let geometry = slab_geometry_for(slot_size);
        Slab::new(ClassId(0), slot_size, align, geometry).unwrap()
    }

    #[test]
    fn geometry_targets_512_slots_for_small_classes() {
        let g = slab_geometry_for(64);
        assert_eq!(g.slot_count, 512);
        assert_eq!(g.slab_bytes, 512 * 64);
    }

    #[test]
    fn geometry_caps_at_two_mib() {
        let g = slab_geometry_for(1 << 20); // 1 MiB class
        assert!(g.slab_bytes <= 1 << 21);
        assert!(g.slot_count >= 1);
    }

    #[test]
    fn fresh_slab_is_empty_and_not_full() {
        let slab = make_slab(64, 64);
        assert!(slab.is_empty());
        assert!(!slab.is_full());
        assert_eq!(slab.free_slots(), slab.slot_count());
    }

    #[test]
    fn alloc_then_free_round_trip() {
        let mut slab = make_slab(64, 64);
        let before = slab.free_slots();
        let ptr = slab.alloc_slot().unwrap();
        assert_eq!(slab.free_slots(), before - 1);
        assert!(slab.contains(ptr));
        slab.mark_free_slot(ptr);
        assert_eq!(slab.free_slots(), before);
        assert!(slab.is_empty());
    }

    #[test]
    fn alloc_exhausts_then_refills() {
        let mut slab = make_slab(128, 64);
        let total = slab.slot_count();
        let mut ptrs = Vec::with_capacity(total);
        for _ in 0..total {
            ptrs.push(slab.alloc_slot().expect("slot"));
        }
        assert!(slab.is_full());
        assert!(slab.alloc_slot().is_none());
        for ptr in ptrs {
            slab.mark_free_slot(ptr);
        }
        assert!(slab.is_empty());
    }

    #[test]
    fn contains_distinguishes_own_from_foreign_pointer() {
        let mut slab_a = make_slab(64, 64);
        let mut slab_b = make_slab(64, 64);
        let ptr_a = slab_a.alloc_slot().unwrap();
        let ptr_b = slab_b.alloc_slot().unwrap();
        assert!(slab_a.contains(ptr_a));
        assert!(!slab_a.contains(ptr_b));
        slab_a.mark_free_slot(ptr_a);
        slab_b.mark_free_slot(ptr_b);
    }

    #[test]
    fn zero_slot_size_rejected() {
        let geometry = SlabGeometry {
            slot_count: 0,
            slab_bytes: 0,
        };
        assert!(Slab::new(ClassId(0), 0, 64, geometry).is_err());
    }
}
