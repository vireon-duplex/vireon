//! [`SessionTable`] — slab + index holding every live [`Session`].
//!
//! See `DESIGN.md` Part 18 (data structures) and Part 6 (session
//! registry).
//!
//! # Design sketch
//!
//! Two structures back the table:
//!
//! 1. **Slab** — a dense `Vec<Session>` indexed by `SlotIndex`. Free
//!    slots form an intrusive free-list.
//! 2. **Index** — a hashmap `SessionId → SlotHandle` for O(1) lookup
//!    by id (the slab alone can't find a session by id without a
//!    scan).
//!
//! Generations on the slot handle protect against ABA: a stale
//! `&Session` reference resolves only when both index and generation
//! agree.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the type signature and the capacity / occupancy
//! accessors. The functional allocator + hashmap integration land in
//! Phase 1.

use crate::config::SessionManagerConfig;
use crate::error::SessionManagerError;
use crate::id::SessionId;
use profiler::profile;

/// Generation counter type stamped on every slot handle.
pub type SlotGeneration = u32;

/// Slot index into the slab storage.
pub type SlotIndex = u32;

/// Opaque handle returned by the table. Combines slot + generation
/// so a stale `&Session` reference cannot resolve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlotHandle {
    /// Slot index in the slab.
    pub slot: SlotIndex,
    /// Generation counter for ABA protection.
    pub generation: SlotGeneration,
}

impl SlotHandle {
    /// The nil sentinel.
    pub const NIL: Self = Self {
        slot: 0,
        generation: 0,
    };

    /// Construct a new handle.
    #[must_use]
    pub const fn new(slot: SlotIndex, generation: SlotGeneration) -> Self {
        Self { slot, generation }
    }

    /// Whether this is the nil sentinel.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.slot == 0 && self.generation == 0
    }
}

impl Default for SlotHandle {
    fn default() -> Self {
        Self::NIL
    }
}

/// Maximum number of slots a single [`SessionTable`] can hold. The
/// slab uses `u32` indices, so this is the practical cap. In
/// practice the manager's `max_sessions` caps us far below this.
pub const TABLE_CAPACITY_MAX: usize = u32::MAX as usize;

/// The session table.
///
/// Phase 0 holds only the config + counters; Phase 1 will add the
/// `Vec<Session>` storage, the `FxHashMap<SessionId, SlotHandle>`
/// index, and the intrusive free-list.
// TODO(phase-1): add `Vec<Session>` storage and `FxHashMap` index.
#[allow(dead_code)]
pub struct SessionTable {
    cfg: SessionManagerConfig,
    capacity: u32,
    occupancy: u32,
    /// Monotonic generation counter; bumped on every free.
    next_generation: SlotGeneration,
}

impl SessionTable {
    /// Construct an empty table sized for `cfg.slab_chunk_sessions`.
    #[must_use]
    pub fn new(cfg: SessionManagerConfig) -> Self {
        let capacity = cfg.slab_chunk_sessions;
        Self {
            cfg,
            capacity,
            occupancy: 0,
            next_generation: 1,
        }
    }

    /// Configured maximum session count.
    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Currently-occupied slot count.
    #[must_use]
    pub const fn occupancy(&self) -> u32 {
        self.occupancy
    }

    /// Free slots available.
    #[must_use]
    pub const fn free(&self) -> u32 {
        self.capacity.saturating_sub(self.occupancy)
    }

    /// Whether the table has reached its configured capacity.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.occupancy >= self.capacity
    }

    /// Whether the table holds any live session.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.occupancy == 0
    }

    /// Phase-0 reservation placeholder. Phase 1 will find a free
    /// slot, install the [`Session`](crate::session::Session), and
    /// return the generation-stamped [`SlotHandle`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] when the
    /// table is full.
    #[profile]
    pub fn try_reserve_slot(&mut self) -> Result<SlotGeneration, SessionManagerError> {
        if self.is_full() {
            return Err(SessionManagerError::ResourceExhausted("session table full"));
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
    /// Always returns [`SessionManagerError::SessionNotFound`] in
    /// Phase 0 (no storage yet).
    pub fn get(&self, id: SessionId) -> Result<SlotHandle, SessionManagerError> {
        // TODO(phase-1): index lookup
        Err(SessionManagerError::SessionNotFound(id))
    }
}

impl Default for SessionTable {
    fn default() -> Self {
        Self::new(SessionManagerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table_predicates() {
        let t = SessionTable::default();
        assert!(t.is_empty());
        assert!(!t.is_full());
        assert!(t.free() > 0);
    }

    #[test]
    fn reserve_increments_occupancy() {
        let mut t = SessionTable::default();
        let g0 = t.try_reserve_slot().expect("first");
        let g1 = t.try_reserve_slot().expect("second");
        assert_eq!(t.occupancy(), 2);
        assert_ne!(g0, g1, "generation must advance");
    }

    #[test]
    fn get_returns_not_found_in_phase0() {
        let t = SessionTable::default();
        assert!(matches!(
            t.get(SessionId::nil()),
            Err(SessionManagerError::SessionNotFound(_))
        ));
    }

    #[test]
    fn slot_handle_nil() {
        assert!(SlotHandle::NIL.is_nil());
        assert!(!SlotHandle::new(1, 1).is_nil());
    }
}
