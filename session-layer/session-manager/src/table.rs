//! [`SessionTable`] — slab + index holding every live [`Session`].
//!
//! See `DESIGN.md` Part 18 (data structures) and Part 6 (session
//! registry).
//!
//! # Design
//!
//! Two structures back the table:
//!
//! 1. **Slab** — a dense `Vec<Slot>` indexed by `SlotIndex`. Free
//!    slots form an intrusive free-list via the `Slot::Free` variant.
//! 2. **Index** — a hashmap `SessionId → SlotHandle` for O(1) lookup
//!    by id (the slab alone can't find a session by id without a
//!    scan).
//!
//! Generations on the slot handle protect against ABA: a stale
//! `&Session` reference resolves only when both index and generation
//! agree.

use rustc_hash::FxHashMap;

use crate::config::SessionManagerConfig;
use crate::error::SessionManagerError;
use crate::id::SessionId;
use crate::session::Session;
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

/// A slot in the slab — either occupied by a live [`Session`] or on
/// the free-list.
enum Slot {
    /// Holds a live session.
    Occupied {
        session: Session,
        /// Generation stamped when this slot was last freed.
        /// Must match the `SlotHandle.generation` for a lookup to succeed.
        generation: SlotGeneration,
    },
    /// On the free-list — available for reuse.
    Free {
        /// Generation to stamp on the next allocation from this slot.
        generation: SlotGeneration,
        /// Next free slot in the intrusive linked list.
        next_free: Option<SlotIndex>,
    },
}

/// The session table.
///
/// Combines a dense slab (`Vec<Slot>`) with a `SessionId → SlotHandle`
/// index for O(1) lookup by id. Free slots form an intrusive free-list.
pub struct SessionTable {
    cfg: SessionManagerConfig,
    capacity: u32,
    /// Hard ceiling — the table will not grow beyond this.
    max_capacity: u32,
    occupancy: u32,
    /// Monotonic generation counter; bumped on every free.
    next_generation: SlotGeneration,
    /// Slab storage — indexed by `SlotIndex`.
    slab: Vec<Slot>,
    /// `SessionId → SlotHandle` index for O(1) lookup.
    index: FxHashMap<SessionId, SlotHandle>,
    /// Head of the intrusive free-list (`None` = list empty).
    free_head: Option<SlotIndex>,
}

impl SessionTable {
    /// Construct an empty table sized for `cfg.slab_chunk_sessions`,
    /// with a hard ceiling of `cfg.max_sessions`.
    #[must_use]
    pub fn new(cfg: SessionManagerConfig) -> Self {
        let capacity = cfg.slab_chunk_sessions;
        let max_capacity = cfg.max_sessions.min(u32::MAX as u64) as u32;
        // Ensure the initial capacity does not exceed the ceiling.
        let capacity = capacity.min(max_capacity);

        // Pre-allocate the slab with Free slots linked into a free-list.
        let mut slab: Vec<Slot> = Vec::with_capacity(capacity as usize);
        for i in 0..capacity {
            let next_free = if i + 1 < capacity {
                Some(i + 1)
            } else {
                None
            };
            slab.push(Slot::Free {
                generation: 1,
                next_free,
            });
        }

        Self {
            cfg,
            capacity,
            max_capacity,
            occupancy: 0,
            next_generation: 1,
            slab,
            index: FxHashMap::default(),
            free_head: if capacity > 0 { Some(0) } else { None },
        }
    }

    /// Configured maximum session count.
    #[must_use]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Hard ceiling — the table will not grow beyond this.
    #[must_use]
    pub const fn max_capacity(&self) -> u32 {
        self.max_capacity
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

    /// Insert a [`Session`] into the table. Allocates a slot from the
    /// free-list (auto-growing the slab if necessary), stores the
    /// session, and indexes it by `session.id`.
    ///
    /// Returns the generation-stamped [`SlotHandle`] for later access.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] when the
    /// table has reached its `max_capacity` ceiling.
    /// Returns [`SessionManagerError::InvalidConfig`] if the session's
    /// id is nil.
    #[profile]
    pub fn insert(&mut self, session: Session) -> Result<SlotHandle, SessionManagerError> {
        if session.id.is_nil() {
            return Err(SessionManagerError::InvalidConfig(
                "cannot insert session with nil id".to_string(),
            ));
        }
        if self.index.contains_key(&session.id) {
            return Err(SessionManagerError::InvalidConfig(
                "duplicate session id".to_string(),
            ));
        }
        if self.is_full() && !self.try_grow() {
            return Err(SessionManagerError::ResourceExhausted("session table full"));
        }

        // Capture id before the session is moved into the slab.
        let id = session.id;

        let slot_idx = self
            .pop_free_slot()
            .ok_or(SessionManagerError::ResourceExhausted("free-list empty"))?;

        let generation = match &self.slab[slot_idx as usize] {
            Slot::Free { generation, .. } => *generation,
            // Defensive: pop_free_slot should only return Free slots.
            _ => return Err(SessionManagerError::ResourceExhausted("slot not free")),
        };

        self.slab[slot_idx as usize] = Slot::Occupied { session, generation };
        let handle = SlotHandle {
            slot: slot_idx,
            generation,
        };

        self.index.insert(id, handle);

        self.occupancy += 1;

        // High-watermark warning at 80 % of max capacity.
        if self.max_capacity > 0 && self.occupancy as u64 * 100 >= self.max_capacity as u64 * 80 {
            tracing::warn!(
                occupancy = self.occupancy,
                capacity = self.capacity,
                max_capacity = self.max_capacity,
                "session table approaching capacity"
            );
        }

        Ok(handle)
    }

    /// Look up a session by id and return its [`SlotHandle`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if no session
    /// with the given id exists.
    pub fn get(&self, id: SessionId) -> Result<SlotHandle, SessionManagerError> {
        self.index
            .get(&id)
            .copied()
            .ok_or(SessionManagerError::SessionNotFound(id))
    }

    /// Borrow the [`Session`] stored at `handle`.
    ///
    /// Returns `None` if the slot is free or the generation doesn't
    /// match (stale handle).
    #[must_use]
    pub fn get_session(&self, handle: SlotHandle) -> Option<&Session> {
        let slot = self.slab.get(handle.slot as usize)?;
        match slot {
            Slot::Occupied {
                session,
                generation,
            } if *generation == handle.generation => Some(session),
            _ => None,
        }
    }

    /// Mutably borrow the [`Session`] stored at `handle`.
    ///
    /// Returns `None` if the slot is free or the generation doesn't
    /// match (stale handle).
    pub fn get_session_mut(&mut self, handle: SlotHandle) -> Option<&mut Session> {
        let slot = self.slab.get_mut(handle.slot as usize)?;
        match slot {
            Slot::Occupied {
                session,
                generation,
            } if *generation == handle.generation => Some(session),
            _ => None,
        }
    }

    /// Remove a session by id. Returns the removed [`Session`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionNotFound`] if no session
    /// with the given id exists.
    pub fn remove(&mut self, id: SessionId) -> Result<Session, SessionManagerError> {
        let handle = self.index.remove(&id).ok_or(SessionManagerError::SessionNotFound(id))?;
        let slot_idx = handle.slot;

        // Bump generation BEFORE stamping so the next occupant of this
        // slot gets a strictly newer generation than the one being freed.
        self.next_generation = self.next_generation.wrapping_add(1);
        let new_gen = self.next_generation;

        let old = std::mem::replace(
            &mut self.slab[slot_idx as usize],
            Slot::Free {
                generation: new_gen,
                next_free: self.free_head,
            },
        );
        self.free_head = Some(slot_idx);

        self.occupancy = self.occupancy.saturating_sub(1);

        match old {
            Slot::Occupied { session, .. } => Ok(session),
            // Defensive: index referenced a Free slot (shouldn't happen).
            Slot::Free { .. } => Err(SessionManagerError::SessionNotFound(id)),
        }
    }

    /// Phase-0 reservation placeholder — allocates a slot from the
    /// free-list without storing a [`Session`]. Returns the generation
    /// of the allocated slot.
    ///
    /// **Prefer [`Self::insert`]** for new code. This method exists
    /// for backward compatibility with the server's `SessionLayer`,
    /// which reserves a slot before generating a `SessionId`.
    ///
    /// When the current capacity is exhausted the table auto-grows by
    /// one `slab_chunk_sessions` chunk, up to `max_capacity`.
    ///
    /// # Errors
    /// Returns [`SessionManagerError::ResourceExhausted`] when the
    /// table has reached its `max_capacity` ceiling.
    #[profile]
    pub fn try_reserve_slot(&mut self) -> Result<SlotGeneration, SessionManagerError> {
        if self.is_full() && !self.try_grow() {
            return Err(SessionManagerError::ResourceExhausted("session table full"));
        }

        let slot_idx = self
            .pop_free_slot()
            .ok_or(SessionManagerError::ResourceExhausted("free-list empty"))?;

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);

        // Mark as Occupied with a placeholder nil-id Session so the
        // slot is accounted for in the occupancy count.
        self.slab[slot_idx as usize] = Slot::Occupied {
            session: Session::default(),
            generation,
        };
        self.occupancy += 1;

        // High-watermark warning at 80 % of max capacity.
        if self.max_capacity > 0 && self.occupancy as u64 * 100 >= self.max_capacity as u64 * 80 {
            tracing::warn!(
                occupancy = self.occupancy,
                capacity = self.capacity,
                max_capacity = self.max_capacity,
                "session table approaching capacity"
            );
        }

        Ok(generation)
    }

    /// Attempt to grow the table by one slab chunk. Returns `false`
    /// when already at `max_capacity`.
    fn try_grow(&mut self) -> bool {
        if self.capacity >= self.max_capacity {
            return false;
        }
        let grow_by = self
            .cfg
            .slab_chunk_sessions
            .min(self.max_capacity - self.capacity);
        let old_capacity = self.capacity;
        let new_capacity = self.capacity + grow_by;

        // Extend the slab with new Free slots.
        for i in old_capacity..new_capacity {
            let next_free = if i + 1 < new_capacity {
                Some(i + 1)
            } else {
                self.free_head
            };
            self.slab.push(Slot::Free {
                generation: 1,
                next_free,
            });
        }
        // Point free_head at the first new slot.
        self.free_head = Some(old_capacity);
        self.capacity = new_capacity;

        tracing::debug!(
            capacity = self.capacity,
            max_capacity = self.max_capacity,
            occupancy = self.occupancy,
            chunk = grow_by,
            "session table grew"
        );
        true
    }

    /// Pop a slot from the free-list. Returns the slot index.
    fn pop_free_slot(&mut self) -> Option<SlotIndex> {
        let slot_idx = self.free_head?;
        let next = match &self.slab[slot_idx as usize] {
            Slot::Free { next_free, .. } => *next_free,
            // Defensive: free_head is stale — reset and return None.
            _ => {
                self.free_head = None;
                return None;
            }
        };
        self.free_head = next;
        Some(slot_idx)
    }

    /// Release a reserved slot (decrements occupancy and frees the
    /// most recently reserved nil-id slot). For slots reserved via
    /// [`Self::try_reserve_slot`] that don't have a `SessionId` for
    /// [`Self::remove`].
    ///
    /// **Prefer [`Self::remove`]** for sessions inserted via
    /// [`Self::insert`].
    pub fn release_slot(&mut self) {
        // Scan for a slot reserved via try_reserve_slot (Occupied with
        // nil-id Session) and free it. This is O(n) but only called on
        // connection close, not a hot path.
        for i in 0..self.slab.len() {
            if let Slot::Occupied { session, .. } = &self.slab[i] {
                if session.id.is_nil() {
                    let idx = i as SlotIndex;
                    self.next_generation = self.next_generation.wrapping_add(1);
                    let new_gen = self.next_generation;
                    self.slab[idx as usize] = Slot::Free {
                        generation: new_gen,
                        next_free: self.free_head,
                    };
                    self.free_head = Some(idx);
                    self.occupancy = self.occupancy.saturating_sub(1);
                    return;
                }
            }
        }
        // Fallback: just decrement occupancy (old Phase 0 behavior).
        if self.occupancy > 0 {
            self.occupancy -= 1;
        }
    }
}

impl Default for SessionTable {
    fn default() -> Self {
        Self::new(SessionManagerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::priority::SessionClass;

    fn make_session(hi: u64) -> Session {
        Session::new(
            SessionId::from_pair(hi, hi * 2),
            SessionClass::default(),
            0,
        )
    }

    // ── Basic predicates ─────────────────────────────────────────

    #[test]
    fn empty_table_predicates() {
        let t = SessionTable::new(cfg_with(4, 100));
        assert!(t.is_empty());
        assert!(!t.is_full());
        assert!(t.free() > 0);
    }

    #[test]
    fn default_table_has_capacity() {
        let t = SessionTable::default();
        assert!(t.free() > 0);
        assert!(!t.is_full());
    }

    // ── Insert / get / remove ────────────────────────────────────

    #[test]
    fn insert_then_get_by_id() {
        let mut t = SessionTable::default();
        let session = make_session(1);
        let id = session.id;

        let handle = t.insert(session).unwrap();
        assert_eq!(t.occupancy(), 1);

        let looked_up = t.get(id).unwrap();
        assert_eq!(looked_up, handle);

        // Can borrow the session.
        let s = t.get_session(handle).unwrap();
        assert_eq!(s.id, id);
    }

    #[test]
    fn insert_rejects_nil_id() {
        let mut t = SessionTable::default();
        let session = Session::new(SessionId::nil(), SessionClass::default(), 0);
        assert!(t.insert(session).is_err());
    }

    #[test]
    fn insert_rejects_duplicate_id() {
        let mut t = SessionTable::default();
        let s1 = make_session(1);
        let s2 = make_session(1); // same id
        assert!(t.insert(s1).is_ok());
        assert!(t.insert(s2).is_err());
    }

    #[test]
    fn remove_returns_session() {
        let mut t = SessionTable::default();
        let session = make_session(42);
        let id = session.id;
        let _ = t.insert(session).unwrap();
        assert_eq!(t.occupancy(), 1);

        let removed = t.remove(id).unwrap();
        assert_eq!(removed.id, id);
        assert_eq!(t.occupancy(), 0);
        assert!(t.get(id).is_err());
    }

    #[test]
    fn remove_unknown_returns_not_found() {
        let mut t = SessionTable::default();
        assert!(matches!(
            t.remove(SessionId::from_pair(1, 1)),
            Err(SessionManagerError::SessionNotFound(_))
        ));
    }

    #[test]
    fn get_unknown_returns_not_found() {
        let t = SessionTable::default();
        assert!(matches!(
            t.get(SessionId::from_pair(1, 1)),
            Err(SessionManagerError::SessionNotFound(_))
        ));
    }

    #[test]
    fn stale_handle_does_not_resolve() {
        let mut t = SessionTable::default();
        let session = make_session(1);
        let id = session.id;
        let handle = t.insert(session).unwrap();

        // Remove + reinsert at the same slot — generation bumps.
        let _ = t.remove(id).unwrap();
        let _ = t.insert(make_session(2)).unwrap();

        // Old handle should not resolve (generation mismatch).
        assert!(t.get_session(handle).is_none());
    }

    #[test]
    fn get_session_mut_allows_mutation() {
        let mut t = SessionTable::default();
        let session = make_session(1);
        let handle = t.insert(session).unwrap();

        let s = t.get_session_mut(handle).unwrap();
        s.mark_authenticated();
        assert!(t.get_session(handle).unwrap().authenticated);
    }

    // ── SlotHandle ───────────────────────────────────────────────

    #[test]
    fn slot_handle_nil() {
        assert!(SlotHandle::NIL.is_nil());
        assert!(!SlotHandle::new(1, 1).is_nil());
    }

    // ── Growable-capacity tests ──────────────────────────────────

    fn cfg_with(chunk: u32, max: u64) -> SessionManagerConfig {
        SessionManagerConfig {
            slab_chunk_sessions: chunk,
            max_sessions: max,
            ..SessionManagerConfig::default()
        }
    }

    #[test]
    fn default_capacity_below_max() {
        let t = SessionTable::new(SessionManagerConfig::default());
        assert_eq!(t.capacity(), 1024);
        assert_eq!(t.max_capacity(), 100_000);
        assert!(!t.is_full());
    }

    #[test]
    fn insert_fills_and_grows() {
        let mut t = SessionTable::new(cfg_with(4, 100));
        assert_eq!(t.capacity(), 4);

        // Fill the initial chunk.
        for i in 1..=4 {
            t.insert(make_session(i as u64)).unwrap();
        }
        assert!(t.is_full());
        assert_eq!(t.capacity(), 4);

        // Next insert should auto-grow.
        t.insert(make_session(5)).unwrap();
        assert_eq!(t.capacity(), 8, "should have grown by one chunk");
        assert_eq!(t.occupancy(), 5);
    }

    #[test]
    fn insert_rejects_at_max_capacity() {
        let mut t = SessionTable::new(cfg_with(4, 8));
        for i in 1..=8u64 {
            t.insert(make_session(i)).unwrap();
        }
        assert_eq!(t.capacity(), 8);
        assert!(t.is_full());

        let err = t.insert(make_session(9)).unwrap_err();
        assert!(
            matches!(err, SessionManagerError::ResourceExhausted(_)),
            "expected ResourceExhausted, got {err:?}"
        );
    }

    #[test]
    fn grow_respects_chunk_granularity() {
        let mut t = SessionTable::new(cfg_with(5, 12));
        assert_eq!(t.capacity(), 5);

        // Fill first chunk (5 slots).
        for i in 1..=5u64 {
            t.insert(make_session(i)).unwrap();
        }
        assert!(t.is_full());

        // 6th insert triggers auto-grow by 5 → capacity 10.
        t.insert(make_session(6)).unwrap();
        assert_eq!(t.capacity(), 10);

        // Fill to 10.
        for i in 7..=10u64 {
            t.insert(make_session(i)).unwrap();
        }
        assert!(t.is_full());

        // 11th insert triggers partial grow by min(5, 2) → capacity 12.
        t.insert(make_session(11)).unwrap();
        assert_eq!(t.capacity(), 12, "partial chunk at ceiling");
        assert_eq!(t.occupancy(), 11);

        // Fill the last slot.
        t.insert(make_session(12)).unwrap();
        assert_eq!(t.occupancy(), 12);

        // Table is now at max — one more should fail.
        assert!(t.insert(make_session(13)).is_err());
    }

    #[test]
    fn high_watermark_does_not_panic() {
        let mut t = SessionTable::new(cfg_with(1, 1));
        assert!(t.insert(make_session(1)).is_ok());
        assert!(t.insert(make_session(2)).is_err());
    }

    // ── Backward-compat: try_reserve_slot / release_slot ─────────

    #[test]
    fn reserve_increments_occupancy() {
        let mut t = SessionTable::default();
        let g0 = t.try_reserve_slot().expect("first");
        let g1 = t.try_reserve_slot().expect("second");
        assert_eq!(t.occupancy(), 2);
        assert_ne!(g0, g1, "generation must advance");
    }

    #[test]
    fn release_then_reserve_reuses_capacity() {
        let mut t = SessionTable::new(cfg_with(2, 2));
        t.try_reserve_slot().unwrap();
        t.try_reserve_slot().unwrap();
        assert!(t.is_full());

        t.release_slot(); // occupancy → 1
        assert!(!t.is_full());
        assert!(t.try_reserve_slot().is_ok(), "freed slot is reusable");
    }
}
