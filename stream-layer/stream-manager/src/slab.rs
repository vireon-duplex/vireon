//! [`StreamSlab`] + [`SlotIndex`] + [`SlotGeneration`] —
//! generation-tracked slab allocator for stream-owned objects.
//!
//! See `DESIGN.md` Part 10 (memory management) and Part 11
//! (lock-free design).
//!
//! # Why generation tracking?
//!
//! A slot index alone is unsafe if slots are recycled: a stale
//! handle to a freed stream could resolve to a newly-allocated
//! stream of the same index. We pair every index with a
//! generation counter; the handle is the pair `(index, generation)`.
//! A handle matches the slot only when both index and generation
//! agree.
//!
//! # Phase 0 status
//!
//! This module ships the index + generation types and a minimal
//! storage-shape placeholder. The actual `StreamSlab` allocation
//! / free-list lands in Phase 1; Phase 0 exposes the typed
//! handles so other modules can reference them in skeletons.

/// Slot index — position within the slab. `u32` is sufficient for
/// up to 4 G slots per slab.
pub type SlotIndex = u32;

/// Generation counter — incremented every time a slot is freed.
/// Wraps after `u32::MAX` frees (considered acceptable: ~4 G
/// stream churn per slot).
pub type SlotGeneration = u32;

/// Reserved index signalling "no slot".
pub const NIL_SLOT: SlotIndex = u32::MAX;

/// A slab handle is the pair `(index, generation)`. Compact at
/// 64 bits when packed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlotHandle {
    /// Position within the slab.
    pub index: SlotIndex,
    /// Generation at allocation time; mismatch on free.
    pub generation: SlotGeneration,
}

impl SlotHandle {
    /// Construct a nil handle (no slot).
    #[must_use]
    pub const fn nil() -> Self {
        Self {
            index: NIL_SLOT,
            generation: 0,
        }
    }

    /// Whether this handle is the nil sentinel.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.index == NIL_SLOT
    }
}

impl Default for SlotHandle {
    fn default() -> Self {
        Self::nil()
    }
}

/// A generation-tracked slab. Phase-0 holds only capacity + an
/// occupancy counter; the actual storage (`Vec<T>` or unsafe
/// `Box<[UnsafeCell<T>]>`) lands in Phase 1.
// TODO(phase-1): replace `len` / `capacity` with a real allocator.
pub struct StreamSlab {
    capacity: SlotIndex,
    occupancy: SlotIndex,
    next_generation: SlotGeneration,
}

impl StreamSlab {
    /// Construct an empty slab with the given capacity.
    #[must_use]
    pub const fn with_capacity(capacity: SlotIndex) -> Self {
        Self {
            capacity,
            occupancy: 0,
            next_generation: 1,
        }
    }

    /// Maximum number of slots.
    #[must_use]
    pub const fn capacity(&self) -> SlotIndex {
        self.capacity
    }

    /// Currently-occupied slot count.
    #[must_use]
    pub const fn occupancy(&self) -> SlotIndex {
        self.occupancy
    }

    /// Free slots available.
    #[must_use]
    pub const fn free(&self) -> SlotIndex {
        self.capacity.saturating_sub(self.occupancy)
    }

    /// Whether the slab is at capacity.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.occupancy >= self.capacity
    }

    /// Whether the slab holds any live object.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.occupancy == 0
    }

    /// Phase-0 placeholder for `allocate()`. Phase 1 will return a
    /// `SlotHandle` after finding a free slot and bumping its
    /// generation. Phase 0 only bumps the counters for testing.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::ResourceExhausted`](crate::error::StreamManagerError::ResourceExhausted)
    /// when the slab is full.
    pub fn try_reserve(&mut self) -> Result<SlotGeneration, crate::error::StreamManagerError> {
        if self.is_full() {
            return Err(crate::error::StreamManagerError::ResourceExhausted(
                "stream slab full",
            ));
        }
        self.occupancy += 1;
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        Ok(generation)
    }

    /// Phase-0 placeholder for `free(handle)`. Phase 1 will
    /// validate the generation and mark the slot reusable.
    pub fn release(&mut self) {
        if self.occupancy > 0 {
            self.occupancy -= 1;
        }
    }
}

impl Default for StreamSlab {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_handle_predicates() {
        let n = SlotHandle::nil();
        assert!(n.is_nil());
        assert_eq!(SlotHandle::default(), n);
    }

    #[test]
    fn slab_capacity_predicates() {
        let s = StreamSlab::with_capacity(4);
        assert_eq!(s.capacity(), 4);
        assert_eq!(s.free(), 4);
        assert!(s.is_empty());
        assert!(!s.is_full());
    }

    #[test]
    fn try_reserve_increments_occupancy() {
        let mut s = StreamSlab::with_capacity(2);
        let g0 = s.try_reserve().expect("first reserve");
        let g1 = s.try_reserve().expect("second reserve");
        assert_eq!(s.occupancy(), 2);
        assert!(s.is_full());
        assert_ne!(g0, g1, "generation must advance per reserve");
    }

    #[test]
    fn try_reserve_fails_when_full() {
        let mut s = StreamSlab::with_capacity(1);
        s.try_reserve().expect("first");
        let r = s.try_reserve();
        assert!(matches!(
            r,
            Err(crate::error::StreamManagerError::ResourceExhausted(_))
        ));
    }

    #[test]
    fn release_decrements_occupancy() {
        let mut s = StreamSlab::with_capacity(2);
        s.try_reserve().expect("first");
        s.release();
        assert_eq!(s.occupancy(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn release_at_zero_is_noop() {
        let mut s = StreamSlab::with_capacity(2);
        s.release();
        assert_eq!(s.occupancy(), 0);
    }

    #[test]
    fn generation_increments_per_reserve() {
        let mut s = StreamSlab::with_capacity(8);
        let gens: Vec<_> = (0..5).map(|_| s.try_reserve().expect("reserve")).collect();
        for w in gens.windows(2) {
            assert!(w[1] > w[0], "generation must strictly increase");
        }
    }
}
