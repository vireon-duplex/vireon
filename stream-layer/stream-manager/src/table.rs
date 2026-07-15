//! [`StreamTable`] — slab + index holding every live [`Stream`].
//!
//! See `DESIGN.md` Part 3 (stream table) and Part 10 (memory
//! management).
//!
//! # Design sketch
//!
//! Two structures back the table:
//!
//! 1. **Slab** — a dense `Vec<Stream>` (or `Box<[UnsafeCell<Stream>]>`
//!    under lock-free access) indexed by [`SlotIndex`](crate::slab::SlotIndex).
//!    Free slots form an intrusive free-list.
//! 2. **Index** — a hashmap `StreamId → SlotHandle` for O(1) lookup
//!    by id (the slab alone can't find a stream by id without a scan).
//!
//! Generations on the slot handle (see [`crate::slab`]) protect against
//! ABA: a stale `&Stream` reference resolves only when both index and
//! generation agree.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the type signature and the capacity / occupancy
//! accessors. The functional allocator + hashmap integration land in
//! Phase 1, after [`StreamSlab`](crate::slab::StreamSlab) gains real
//! storage.

use crate::config::StreamManagerConfig;
use crate::error::StreamManagerError;
use crate::id::StreamId;
use crate::slab::{SlotGeneration, SlotIndex};
use profiler::profile;

/// Maximum number of slots a single [`StreamTable`] can hold. The
/// slab uses `u32` indices, so this is the practical cap. In
/// practice the connection's `MAX_STREAMS` frames cap us far below
/// this.
pub const TABLE_CAPACITY_MAX: usize = u32::MAX as usize;

/// The stream table.
///
/// Phase 0 holds only the config + counters; Phase 1 will add the
/// `Vec<Stream>` storage, the `FxHashMap<StreamId, SlotHandle>`
/// index, and the intrusive free-list.
// TODO(phase-1): add `Vec<Stream>` storage and `FxHashMap` index.
#[allow(dead_code)]
pub struct StreamTable {
    cfg: StreamManagerConfig,
    capacity: u32,
    occupancy: u32,
    /// Monotonic generation counter; bumped on every free.
    next_generation: SlotGeneration,
}

impl StreamTable {
    /// Construct an empty table sized for `cfg.slab_chunk_streams`.
    #[must_use]
    pub fn new(cfg: StreamManagerConfig) -> Self {
        let capacity = cfg.slab_chunk_streams;
        Self {
            cfg,
            capacity,
            occupancy: 0,
            next_generation: 1,
        }
    }

    /// Configured maximum stream count.
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

    /// Whether the table has reached its configured capacity.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.occupancy >= self.capacity
    }

    /// Whether the table holds any live stream.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.occupancy == 0
    }

    /// Phase-0 reservation placeholder. Phase 1 will find a free
    /// slot, install the [`Stream`](crate::stream::Stream), and
    /// return the generation-stamped [`SlotHandle`](crate::slab::SlotHandle).
    ///
    /// # Errors
    /// Returns [`StreamManagerError::ResourceExhausted`] when the
    /// table is full.
    #[profile]
    pub fn try_reserve_slot(&mut self) -> Result<SlotGeneration, StreamManagerError> {
        if self.is_full() {
            return Err(StreamManagerError::ResourceExhausted("stream table full"));
        }
        self.occupancy += 1;
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        Ok(generation)
    }

    /// Phase-0 release placeholder.
    pub fn release_slot(&mut self) {
        if self.occupancy > 0 {
            self.occupancy -= 1;
        }
    }

    /// Phase-1 lookup-by-id placeholder.
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::StreamNotFound`] in
    /// Phase 0 (no storage yet).
    pub fn get(&self, _id: StreamId) -> Result<(), StreamManagerError> {
        // TODO(phase-1): index lookup
        Err(StreamManagerError::StreamNotFound(_id.raw()))
    }
}

impl Default for StreamTable {
    fn default() -> Self {
        Self::new(StreamManagerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table_predicates() {
        let t = StreamTable::default();
        assert!(t.is_empty());
        assert!(!t.is_full());
        assert!(t.free() > 0);
    }

    #[test]
    fn reserve_increments_occupancy() {
        let mut t = StreamTable::default();
        let g0 = t.try_reserve_slot().expect("first");
        let g1 = t.try_reserve_slot().expect("second");
        assert_eq!(t.occupancy(), 2);
        assert_ne!(g0, g1, "generation must advance");
    }

    #[test]
    fn get_returns_not_found_in_phase0() {
        let t = StreamTable::default();
        assert!(matches!(
            t.get(StreamId::default()),
            Err(StreamManagerError::StreamNotFound(_))
        ));
    }
}
