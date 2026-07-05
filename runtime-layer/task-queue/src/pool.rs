//! TaskPool — slab-based task descriptor allocator (Layer 10).
//!
//! Owns a contiguous `Vec<TaskSlot>` of fixed-size entries plus a LIFO
//! free-list of slot indices. Each slot has a generation counter that
//! bumps on every free, so stale [`TaskId`] handles from a previous
//! generation are rejected.
//!
//! ## Why a dedicated slab (not Buffer Pool)?
//!
//! The Buffer Pool manages variable-size byte buffers (64 B – 1 MiB,
//! power-of-two classes) with RAII `Buffer` guards. Task descriptors
//! are fixed-size metadata structs with generation tracking — a
//! different allocation pattern. The slab gives direct `&mut TaskSlot`
//! access by index, which is essential for O(1) cancellation queries.

use std::boxed::Box;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use crate::TaskResult;
use crate::error::TaskError;
use crate::handle::TaskHandle;
use crate::id::{GEN_MASK, SLOT_MASK, decode_slot, make_id};
use crate::metadata::TaskMetadata;
use crate::priority::TaskPriority;
use crate::state::TaskState;
use crate::task::{Task, TaskKind};

/// Inline storage for task-specific data within a slot.
///
/// 64 bytes covers typical closure captures. Larger tasks store a
/// pointer to heap data (allocated lazily by the task's own code).
pub const INLINE_DATA_SIZE: usize = 64;

/// A slab slot holding a task descriptor and its metadata.
///
/// Layout target: 2 cache lines (128 B). The hot fields (`task`,
/// `state`, `priority`) are on the first line; cold metadata is on the
/// second.
pub struct TaskSlot {
    /// The task itself (boxed so we can swap it in/out without moving).
    /// `None` when the slot is free.
    pub(crate) task: Option<Box<dyn Task>>,
    /// Lifecycle state.
    pub(crate) state: TaskState,
    /// Priority band the task was spawned at.
    pub(crate) priority: TaskPriority,
    /// Coarse kind tag for metrics.
    pub(crate) kind: TaskKind,
    /// Per-task metadata.
    pub(crate) metadata: TaskMetadata,
    /// Shared cancellation flag, handed out in the `TaskHandle`.
    pub(crate) cancel_flag: Arc<AtomicBool>,
}

impl std::fmt::Debug for TaskSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskSlot")
            .field("state", &self.state)
            .field("priority", &self.priority)
            .field("kind", &self.kind)
            .field("metadata", &self.metadata)
            .field(
                "cancelled",
                &self.cancel_flag.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field("has_task", &self.task.is_some())
            .finish()
    }
}

/// Slab-based task allocator.
///
/// Allocating a slot returns a generation-stamped [`TaskId`] and a
/// [`TaskHandle`] tied to the slot's shared cancellation flag. The
/// queue stores the task and metadata, then pushes the slot index into
/// a ready queue or timer wheel.
pub struct TaskPool {
    slots: Vec<TaskSlot>,
    /// LIFO free-list of slot indices.
    free_slots: Vec<u32>,
    /// Per-slot generation counters.
    gens: Vec<u16>,
}

impl TaskPool {
    /// Creates a pool with `capacity` slots (all free).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        let free_slots = (0..capacity as u32).rev().collect();
        let gens = vec![0u16; capacity];
        for _ in 0..capacity {
            slots.push(TaskSlot {
                task: None,
                state: TaskState::Idle,
                priority: TaskPriority::Normal,
                kind: TaskKind::Generic,
                metadata: TaskMetadata::default(),
                cancel_flag: Arc::new(AtomicBool::new(false)),
            });
        }
        Self {
            slots,
            free_slots,
            gens,
        }
    }

    /// Number of slots currently in the free-list.
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.free_slots.len()
    }

    /// Total capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Number of slots currently holding a live task.
    #[must_use]
    pub fn live_count(&self) -> usize {
        self.capacity() - self.free_count()
    }

    /// Allocates a slot, installs `task` + `metadata`, and returns
    /// `(TaskId, TaskHandle)`.
    ///
    /// Returns [`TaskError::PoolExhausted`] if no free slot is available.
    pub fn alloc(
        &mut self,
        task: Box<dyn Task>,
        priority: TaskPriority,
        metadata: TaskMetadata,
        now_ns: u64,
    ) -> Result<(TaskHandle, u32), TaskError> {
        let slot_idx = self.free_slots.pop().ok_or(TaskError::PoolExhausted {
            capacity: self.capacity(),
        })?;
        // Reset the cancellation flag for the new occupant.
        let slot = &mut self.slots[slot_idx as usize];
        debug_assert!(slot.task.is_none(), "allocating non-free slot");
        slot.cancel_flag
            .store(false, std::sync::atomic::Ordering::Release);
        let kind = task.kind();
        slot.task = Some(task);
        slot.state = TaskState::Queued;
        slot.priority = priority;
        slot.kind = kind;
        slot.metadata = metadata;
        slot.metadata.spawned_at_ns = now_ns;

        let generation = self.gens[slot_idx as usize];
        let id = make_id(slot_idx, generation);
        let handle = TaskHandle::new(id, Arc::clone(&slot.cancel_flag));
        Ok((handle, slot_idx))
    }

    /// Returns a slot to the free-list and bumps its generation. The
    /// task is dropped (its `Drop` runs here).
    pub fn free(&mut self, slot_idx: u32) {
        if (slot_idx as usize) >= self.slots.len() {
            return;
        }
        let slot = &mut self.slots[slot_idx as usize];
        slot.task = None;
        slot.state = TaskState::Idle;
        slot.metadata = TaskMetadata::default();
        self.gens[slot_idx as usize] =
            (self.gens[slot_idx as usize].wrapping_add(1)) & GEN_MASK as u16;
        self.free_slots.push(slot_idx);
    }

    /// Borrows a slot by index (immutable).
    #[must_use]
    pub fn get(&self, slot_idx: u32) -> Option<&TaskSlot> {
        self.slots.get(slot_idx as usize)
    }

    /// Borrows a slot by index (mutable).
    #[must_use]
    pub fn get_mut(&mut self, slot_idx: u32) -> Option<&mut TaskSlot> {
        self.slots.get_mut(slot_idx as usize)
    }

    /// Returns the generation counter for a slot.
    #[must_use]
    pub fn generation(&self, slot_idx: u32) -> u16 {
        self.gens.get(slot_idx as usize).copied().unwrap_or(0)
    }

    /// Verifies that `id`'s generation matches the slot's current
    /// generation — i.e. the id is not stale.
    #[must_use]
    pub fn is_live(&self, id: crate::id::TaskId) -> bool {
        let slot_idx = decode_slot(id.raw()) as usize;
        match self.slots.get(slot_idx) {
            Some(s) => {
                !s.state.is_recyclable()
                    && s.state != TaskState::Idle
                    && self.gens[slot_idx] == crate::id::decode_generation(id.raw())
            }
            None => false,
        }
    }

    /// Reschedules a task by resetting its priority / metadata and
    /// returning the slot to `Queued` state. Used by the executor when
    /// a task returns `Reschedule { delay: ZERO }`.
    pub fn reschedule_in_place(&mut self, slot_idx: u32, priority: TaskPriority, now_ns: u64) {
        if let Some(slot) = self.slots.get_mut(slot_idx as usize) {
            slot.state = TaskState::Queued;
            slot.priority = priority;
            slot.metadata.spawned_at_ns = now_ns;
            // deadline is cleared on reschedule
            slot.metadata.deadline_ns = 0;
        }
    }
}

// ── Validate the masks used in id encoding ──────────────────────────
//
// SLOT_MASK and GEN_MASK are used implicitly through the id module's
// encode/decode functions. We sanity-check here that the masks align
// with the pool's slot-count expectations at construction time.
const _: () = {
    assert!(SLOT_MASK > 0);
    assert!(GEN_MASK > 0);
};

impl std::fmt::Debug for TaskPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskPool")
            .field("capacity", &self.capacity())
            .field("free", &self.free_count())
            .field("live", &self.live_count())
            .finish()
    }
}

// Re-export the small helpers some tests rely on.
pub(crate) fn _used(duration_zero: Duration, _tr: TaskResult) -> Duration {
    duration_zero
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::TaskContext;
    use crate::task::TaskResult;

    struct Noop;
    impl Task for Noop {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }

    fn boxed() -> Box<dyn Task> {
        Box::new(Noop)
    }

    #[test]
    fn alloc_free_round_trip() {
        let mut pool = TaskPool::new(16);
        assert_eq!(pool.free_count(), 16);
        let (handle, slot) = pool
            .alloc(boxed(), TaskPriority::Normal, TaskMetadata::default(), 0)
            .expect("alloc");
        assert_eq!(pool.free_count(), 15);
        assert!(pool.is_live(handle.id()));
        pool.free(slot);
        assert_eq!(pool.free_count(), 16);
        assert!(!pool.is_live(handle.id()));
    }

    #[test]
    fn alloc_exhausts_pool() {
        let mut pool = TaskPool::new(4);
        for _ in 0..4 {
            pool.alloc(boxed(), TaskPriority::Normal, TaskMetadata::default(), 0)
                .expect("alloc");
        }
        let err = pool
            .alloc(boxed(), TaskPriority::Normal, TaskMetadata::default(), 0)
            .unwrap_err();
        assert!(matches!(err, TaskError::PoolExhausted { capacity: 4 }));
    }

    #[test]
    fn generation_bumps_on_free() {
        let mut pool = TaskPool::new(4);
        let (_, slot) = pool
            .alloc(boxed(), TaskPriority::Normal, TaskMetadata::default(), 0)
            .expect("alloc");
        let g0 = pool.generation(slot);
        pool.free(slot);
        let g1 = pool.generation(slot);
        assert_eq!(g1, g0 + 1);
    }

    #[test]
    fn stale_id_detected_after_reuse() {
        let mut pool = TaskPool::new(4);
        let (handle, slot) = pool
            .alloc(boxed(), TaskPriority::Normal, TaskMetadata::default(), 0)
            .expect("alloc");
        pool.free(slot);
        // Allocate the same slot again (LIFO free-list pops it next).
        let (_h2, _slot2) = pool
            .alloc(boxed(), TaskPriority::Normal, TaskMetadata::default(), 0)
            .expect("alloc");
        // The original handle's id should be considered stale.
        assert!(!pool.is_live(handle.id()));
    }

    #[test]
    fn reschedule_in_place_resets_metadata() {
        let mut pool = TaskPool::new(4);
        let (_, slot) = pool
            .alloc(boxed(), TaskPriority::High, TaskMetadata::default(), 100)
            .expect("alloc");
        pool.reschedule_in_place(slot, TaskPriority::Low, 200);
        let s = pool.get(slot).expect("slot");
        assert_eq!(s.priority, TaskPriority::Low);
        assert_eq!(s.metadata.spawned_at_ns, 200);
        assert_eq!(s.metadata.deadline_ns, 0);
        assert_eq!(s.state, TaskState::Queued);
    }

    #[test]
    fn debug_format_has_counts() {
        let pool = TaskPool::new(16);
        let s = format!("{pool:?}");
        assert!(s.contains("capacity"));
        assert!(s.contains("16"));
    }
}
