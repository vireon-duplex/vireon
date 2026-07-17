//! Slab owner and address-range index (Layer 4).
//!
//! The [`Arena`] owns every [`Slab`] for every size class. It is the single
//! place where new backing memory is created and the single place where it is
//! returned to the operating system.
//!
//! ## Concurrency
//!
//! Single-threaded (`RefCell` interior mutability). The arena is owned by
//! one reactor thread exclusively — no locks needed.

use std::cell::RefCell;
use std::ptr::NonNull;

use crate::allocator::size_class::{CLASS_TABLE, ClassId, NUM_CLASSES};
use crate::allocator::slab::{Slab, SlabGeometry, slab_geometry_for};
use crate::error::BufferPoolError;
use crate::numa::NodeId;

/// Internal mutable state of the arena.
#[derive(Default)]
struct ArenaState {
    /// `slabs[class_index]` is the list of slabs for that class.
    slabs: Vec<Vec<Slab>>,
}

impl ArenaState {
    fn new() -> Self {
        Self {
            slabs: (0..NUM_CLASSES).map(|_| Vec::new()).collect(),
        }
    }
}

/// Owns all slabs for every size class.
pub struct Arena {
    state: RefCell<ArenaState>,
    geometry: Vec<SlabGeometry>,
    /// Preferred NUMA node passed to each new [`Slab`] (None = default policy).
    numa_node: Option<NodeId>,
}

impl Arena {
    /// Creates an empty arena with no pre-allocated slabs and the kernel's
    /// default memory policy.
    pub fn new() -> Self {
        Self::with_numa_node(None)
    }

    /// Creates an empty arena that binds each new slab's backing memory to
    /// `node` via `mbind(MPOL_PREFERRED)`. Best-effort.
    #[must_use]
    pub fn with_numa_node(numa_node: Option<NodeId>) -> Self {
        let geometry: Vec<SlabGeometry> = CLASS_TABLE
            .iter()
            .map(|c| slab_geometry_for(c.size))
            .collect();
        Self {
            state: RefCell::new(ArenaState::new()),
            geometry,
            numa_node,
        }
    }

    /// Allocates one slot of `class`, growing the arena by one slab when every
    /// existing slab of that class is full.
    pub fn allocate_slot(&self, class: ClassId) -> Result<NonNull<u8>, BufferPoolError> {
        let idx = class_index(class)?;
        let mut state = self.state.borrow_mut();
        let slabs = &mut state.slabs[idx];
        for slab in slabs.iter_mut() {
            if let Some(ptr) = slab.alloc_slot() {
                return Ok(ptr);
            }
        }
        let geometry = self.geometry[idx];
        let class_info = CLASS_TABLE[idx];
        let mut new_slab =
            Slab::new(class, class_info.size, class_info.align, geometry, self.numa_node)?;
        let ptr = new_slab.alloc_slot().ok_or(BufferPoolError::OutOfMemory {
            size: class_info.size,
        })?;
        slabs.push(new_slab);
        Ok(ptr)
    }

    /// Returns a slot to its owning slab.
    ///
    /// Used by the release path when the free-list overflows, and by the
    /// shrinker when draining caches.
    pub fn mark_free(&self, ptr: NonNull<u8>, class: ClassId) -> Result<(), BufferPoolError> {
        let idx = class_index(class)?;
        let mut state = self.state.borrow_mut();
        for slab in state.slabs[idx].iter_mut() {
            if slab.contains(ptr) {
                slab.mark_free_slot(ptr);
                return Ok(());
            }
        }
        tracing::warn!("arena: pointer not found in any slab of class {class:?}");
        Err(BufferPoolError::WrongPool { class })
    }

    /// Drops every fully-free slab and returns the total bytes reclaimed.
    ///
    /// A slab is fully free only when none of its slots are outstanding — i.e.
    /// not held by a live buffer and not sitting in any free-list.
    pub fn purge(&self) -> usize {
        let mut freed = 0;
        let mut state = self.state.borrow_mut();
        for class_slabs in state.slabs.iter_mut() {
            class_slabs.retain(|slab| {
                if slab.is_empty() {
                    freed += slab.slab_bytes();
                    false
                } else {
                    true
                }
            });
        }
        freed
    }

    /// Pre-allocates enough slabs for `class` to hold at least `count` free
    /// slots without triggering a grow on the next refills.
    pub fn reserve(&self, class: ClassId, count: usize) -> Result<(), BufferPoolError> {
        let idx = class_index(class)?;
        let geometry = self.geometry[idx];
        let class_info = CLASS_TABLE[idx];
        let mut state = self.state.borrow_mut();
        while free_slots_in(&state.slabs[idx]) < count {
            let slab = Slab::new(class, class_info.size, class_info.align, geometry, self.numa_node)?;
            state.slabs[idx].push(slab);
        }
        Ok(())
    }

    /// Number of slabs currently held for `class`.
    pub fn slab_count(&self, class: ClassId) -> usize {
        let Ok(idx) = class_index(class) else {
            return 0;
        };
        self.state.borrow().slabs[idx].len()
    }

    /// Total bytes of backing memory currently held.
    pub fn memory_usage(&self) -> usize {
        let state = self.state.borrow();
        state.slabs.iter().flatten().map(|s| s.slab_bytes()).sum()
    }

    /// Total free slots across all slabs of `class`.
    pub fn free_slots(&self, class: ClassId) -> usize {
        let Ok(idx) = class_index(class) else {
            return 0;
        };
        free_slots_in(&self.state.borrow().slabs[idx])
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn free_slots_in(slabs: &[Slab]) -> usize {
    slabs.iter().map(|s| s.free_slots()).sum()
}

/// Validates `class` and returns its index into [`CLASS_TABLE`].
fn class_index(class: ClassId) -> Result<usize, BufferPoolError> {
    if class == ClassId::HUGE {
        return Err(BufferPoolError::InvalidConfig(
            "arena does not serve the huge class".into(),
        ));
    }
    let idx = usize::from(class.0);
    if idx < NUM_CLASSES {
        Ok(idx)
    } else {
        Err(BufferPoolError::WrongPool { class })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_arena_has_no_memory() {
        let arena = Arena::new();
        assert_eq!(arena.memory_usage(), 0);
    }

    #[test]
    fn allocate_then_return_round_trip() {
        let arena = Arena::new();
        let ptr = arena.allocate_slot(ClassId(0)).unwrap();
        assert!(arena.memory_usage() > 0);
        assert_eq!(
            arena.free_slots(ClassId(0)),
            arena.slab_count(ClassId(0)) * 512 - 1
        );
        arena.mark_free(ptr, ClassId(0)).unwrap();
        assert_eq!(
            arena.free_slots(ClassId(0)),
            arena.slab_count(ClassId(0)) * 512
        );
    }

    #[test]
    fn allocate_grows_slabs_on_exhaustion() {
        let arena = Arena::new();
        // Exhaust the first slab (512 slots for the 64-byte class).
        let first_slab_slots = 512;
        let mut ptrs = Vec::with_capacity(first_slab_slots + 1);
        for _ in 0..first_slab_slots {
            ptrs.push(arena.allocate_slot(ClassId(0)).unwrap());
        }
        assert_eq!(arena.slab_count(ClassId(0)), 1);
        // Next allocate triggers a second slab.
        ptrs.push(arena.allocate_slot(ClassId(0)).unwrap());
        assert_eq!(arena.slab_count(ClassId(0)), 2);
        for ptr in ptrs {
            arena.mark_free(ptr, ClassId(0)).unwrap();
        }
    }

    #[test]
    fn purge_drops_fully_free_slabs() {
        let arena = Arena::new();
        let ptrs: Vec<_> = (0..3)
            .map(|_| arena.allocate_slot(ClassId(1)).unwrap())
            .collect();
        assert!(arena.slab_count(ClassId(1)) >= 1);
        for ptr in ptrs {
            arena.mark_free(ptr, ClassId(1)).unwrap();
        }
        let freed = arena.purge();
        assert!(freed > 0);
        assert_eq!(arena.slab_count(ClassId(1)), 0);
    }

    #[test]
    fn purge_keeps_slabs_with_outstanding_slots() {
        let arena = Arena::new();
        let _held = arena.allocate_slot(ClassId(2)).unwrap();
        // Allocate and return some siblings so the slab is partially used.
        let transient: Vec<_> = (0..5)
            .map(|_| arena.allocate_slot(ClassId(2)).unwrap())
            .collect();
        for ptr in transient {
            arena.mark_free(ptr, ClassId(2)).unwrap();
        }
        let freed = arena.purge();
        assert_eq!(freed, 0, "slab with an outstanding slot must not be purged");
        assert!(arena.slab_count(ClassId(2)) >= 1);
    }

    #[test]
    fn reserve_preallocates_capacity() {
        let arena = Arena::new();
        arena.reserve(ClassId(3), 600).unwrap();
        // 512 slots per slab for the 512-byte class; need 2 slabs for 600.
        assert!(arena.slab_count(ClassId(3)) >= 2);
        assert!(arena.free_slots(ClassId(3)) >= 600);
    }

    #[test]
    fn huge_class_rejected() {
        let arena = Arena::new();
        assert!(arena.allocate_slot(ClassId::HUGE).is_err());
        assert!(arena.mark_free(NonNull::dangling(), ClassId::HUGE).is_err());
    }

    #[test]
    fn mark_free_unknown_pointer_fails() {
        let arena = Arena::new();
        // A dangling pointer is not inside any slab.
        let result = arena.mark_free(NonNull::dangling(), ClassId(0));
        assert!(matches!(result, Err(BufferPoolError::WrongPool { .. })));
    }
}
