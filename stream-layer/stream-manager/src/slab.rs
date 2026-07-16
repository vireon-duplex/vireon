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

/// Default initial slab capacity.
const DEFAULT_SLAB_CAPACITY: SlotIndex = 1024;

/// Default hard ceiling — the slab will not grow beyond this.
const DEFAULT_SLAB_MAX_CAPACITY: SlotIndex = 1_048_576; // 1 M streams

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
    /// Hard ceiling — the slab will not grow beyond this.
    max_capacity: SlotIndex,
    /// Growth granularity (slots added per grow).
    chunk_size: SlotIndex,
    occupancy: SlotIndex,
    next_generation: SlotGeneration,
}

impl StreamSlab {
    /// Construct an empty slab with the given initial capacity and
    /// a default hard ceiling (`DEFAULT_SLAB_MAX_CAPACITY`).
    #[must_use]
    pub const fn with_capacity(capacity: SlotIndex) -> Self {
        let chunk = if capacity == 0 { 1 } else { capacity };
        Self::with_limits(capacity, DEFAULT_SLAB_MAX_CAPACITY, chunk)
    }

    /// Construct an empty slab with explicit initial capacity, hard
    /// ceiling, and growth granularity. The slab will auto-grow in
    /// increments of `chunk_size` until `max_capacity` is reached.
    #[must_use]
    pub const fn with_limits(
        initial: SlotIndex,
        max_capacity: SlotIndex,
        chunk_size: SlotIndex,
    ) -> Self {
        let capacity = if initial > max_capacity {
            max_capacity
        } else {
            initial
        };
        let chunk_size = if chunk_size == 0 { 1 } else { chunk_size };
        Self {
            capacity,
            max_capacity,
            chunk_size,
            occupancy: 0,
            next_generation: 1,
        }
    }

    /// Maximum number of slots the slab currently holds.
    #[must_use]
    pub const fn capacity(&self) -> SlotIndex {
        self.capacity
    }

    /// Hard ceiling — the slab will not grow beyond this.
    #[must_use]
    pub const fn max_capacity(&self) -> SlotIndex {
        self.max_capacity
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
    /// generation.
    ///
    /// When the current capacity is exhausted the slab auto-grows by
    /// one `chunk_size` increment, up to `max_capacity`. Only when
    /// the hard ceiling is reached does this return an error.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::ResourceExhausted`](crate::error::StreamManagerError::ResourceExhausted)
    /// when the slab has reached its `max_capacity` ceiling.
    pub fn try_reserve(&mut self) -> Result<SlotGeneration, crate::error::StreamManagerError> {
        if self.is_full() && !self.try_grow() {
            return Err(crate::error::StreamManagerError::ResourceExhausted(
                "stream slab full",
            ));
        }
        self.occupancy += 1;
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);

        // High-watermark warning at 80 % of max capacity.
        if self.max_capacity > 0 && self.occupancy as u64 * 100 >= self.max_capacity as u64 * 80 {
            tracing::warn!(
                occupancy = self.occupancy,
                capacity = self.capacity,
                max_capacity = self.max_capacity,
                "stream slab approaching capacity"
            );
        }

        Ok(generation)
    }

    /// Attempt to grow the slab by one chunk. Returns `false` when
    /// already at `max_capacity`.
    fn try_grow(&mut self) -> bool {
        if self.capacity >= self.max_capacity {
            return false;
        }
        let grow_by = self.chunk_size.min(self.max_capacity - self.capacity);
        self.capacity += grow_by;
        tracing::debug!(
            capacity = self.capacity,
            max_capacity = self.max_capacity,
            occupancy = self.occupancy,
            chunk = grow_by,
            "stream slab grew"
        );
        true
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
        Self::with_limits(
            DEFAULT_SLAB_CAPACITY,
            DEFAULT_SLAB_MAX_CAPACITY,
            DEFAULT_SLAB_CAPACITY,
        )
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
        // Use with_limits to pin max_capacity = 1 so no growth is possible.
        let mut s = StreamSlab::with_limits(1, 1, 1);
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

    // ── Growable-capacity tests ────────────────────────────────

    #[test]
    fn default_capacity_below_max() {
        let s = StreamSlab::default();
        assert_eq!(s.capacity(), DEFAULT_SLAB_CAPACITY);
        assert_eq!(s.max_capacity(), DEFAULT_SLAB_MAX_CAPACITY);
        assert!(!s.is_full());
    }

    #[test]
    fn grows_when_full_within_max() {
        let mut s = StreamSlab::with_limits(4, 100, 4);
        assert_eq!(s.capacity(), 4);

        // Fill the initial chunk.
        for _ in 0..4 {
            s.try_reserve().expect("initial chunk");
        }
        assert!(s.is_full());
        assert_eq!(s.capacity(), 4);

        // Next reserve should auto-grow.
        s.try_reserve().expect("auto-grow");
        assert_eq!(s.capacity(), 8, "should have grown by one chunk");
        assert_eq!(s.occupancy(), 5);
    }

    #[test]
    fn rejects_at_max_capacity() {
        let mut s = StreamSlab::with_limits(4, 8, 4);
        for _ in 0..8 {
            s.try_reserve().expect("within max");
        }
        assert_eq!(s.capacity(), 8);

        let err = s.try_reserve().unwrap_err();
        assert!(
            matches!(err, crate::error::StreamManagerError::ResourceExhausted(_)),
            "expected ResourceExhausted, got {err:?}"
        );
    }

    #[test]
    fn grow_respects_chunk_granularity() {
        let mut s = StreamSlab::with_limits(5, 12, 5);
        assert_eq!(s.capacity(), 5);

        // Fill first chunk (5 slots).
        for _ in 0..5 {
            s.try_reserve().unwrap();
        }
        assert_eq!(s.capacity(), 5);
        assert!(s.is_full());

        // 6th reserve triggers auto-grow by 5 → capacity 10.
        s.try_reserve().unwrap();
        assert_eq!(s.capacity(), 10);

        // Fill to 10.
        for _ in 0..4 {
            s.try_reserve().unwrap();
        }
        assert!(s.is_full());

        // 11th reserve triggers partial grow by min(5, 2) → capacity 12.
        s.try_reserve().unwrap();
        assert_eq!(s.capacity(), 12, "partial chunk at ceiling");

        // Fill the last slot.
        s.try_reserve().unwrap();
        assert_eq!(s.occupancy(), 12);

        // Table is now at max — one more should fail.
        assert!(s.try_reserve().is_err());
    }

    #[test]
    fn high_watermark_does_not_panic() {
        let mut s = StreamSlab::with_limits(1, 1, 1);
        assert!(s.try_reserve().is_ok());
        assert!(s.try_reserve().is_err());
    }

    #[test]
    fn release_then_reserve_reuses_capacity() {
        let mut s = StreamSlab::with_limits(2, 2, 2);
        s.try_reserve().unwrap();
        s.try_reserve().unwrap();
        assert!(s.is_full());

        s.release();
        assert!(!s.is_full());
        assert!(s.try_reserve().is_ok(), "freed slot is reusable");
    }
}
