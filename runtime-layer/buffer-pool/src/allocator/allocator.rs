//! Top-level allocator orchestrating arena and huge paths (Layer 6 glue).
//!
//! [`Allocator`] is the boundary the pool talks to. It routes a request to the
//! [`Arena`] when it fits a size class, or to the [`HugeAllocator`] when it
//! does not, and exposes matching release entry points.

use std::alloc::Layout;
use std::ptr::NonNull;

use crate::allocator::arena::Arena;
use crate::allocator::classifier::classify_request;
use crate::allocator::huge::{HugeAllocation, HugeAllocator, HugeStatsSnapshot};
use crate::allocator::size_class::ClassId;
use crate::error::BufferPoolError;
use crate::numa::NodeId;

/// The outcome of a successful allocation.
///
/// Carries everything the pool needs to construct a [`Buffer`](crate::guard)
/// guard and, later, to release the memory through the right path.
#[derive(Debug)]
pub struct Allocation {
    /// Owned pointer to the slot's start.
    pub ptr: NonNull<u8>,
    /// Capacity in bytes (slot size for classed; requested size for huge).
    pub capacity: usize,
    /// Size class (`ClassId::HUGE` for the huge path).
    pub class: ClassId,
    /// Layout for huge allocations; `None` for slab slots (their backing is
    /// owned by the arena and freed via [`Allocator::return_slot`]).
    pub huge_layout: Option<Layout>,
}

impl Allocation {
    /// True when this allocation came from the huge path.
    #[inline]
    pub fn is_huge(&self) -> bool {
        self.class == ClassId::HUGE
    }
}

/// Routes allocation requests between the arena (size classes) and the huge
/// allocator (oversized requests).
pub struct Allocator {
    arena: Arena,
    huge: HugeAllocator,
}

impl Allocator {
    /// Creates a new allocator with empty arena and zeroed huge counters,
    /// using the kernel's default memory policy.
    pub fn new() -> Self {
        Self::with_numa_node(None)
    }

    /// Creates a new allocator whose arena binds each new slab's backing
    /// memory to `numa_node` via `mbind(MPOL_PREFERRED)`. Best-effort.
    #[must_use]
    pub fn with_numa_node(numa_node: Option<NodeId>) -> Self {
        Self {
            arena: Arena::with_numa_node(numa_node),
            huge: HugeAllocator::new(),
        }
    }

    /// Allocates memory for `size` bytes.
    ///
    /// Requests up to the class-table maximum are served from the arena;
    /// larger requests go through the huge allocator.
    pub fn allocate(&self, size: usize) -> Result<Allocation, BufferPoolError> {
        match classify_request(size) {
            crate::allocator::classifier::Classification::Classed { id, class } => {
                let ptr = self.arena.allocate_slot(id)?;
                Ok(Allocation {
                    ptr,
                    capacity: class.size,
                    class: id,
                    huge_layout: None,
                })
            }
            crate::allocator::classifier::Classification::Huge { .. } => {
                let huge_alloc = self.huge.allocate(size)?;
                let layout = huge_alloc.layout;
                Ok(Allocation {
                    ptr: huge_alloc.ptr,
                    capacity: size,
                    class: ClassId::HUGE,
                    huge_layout: Some(layout),
                })
            }
        }
    }

    /// Releases a huge allocation back to the operating system.
    ///
    /// Panics-free: an `Allocation` without a layout is a caller bug surfaced
    /// as [`BufferPoolError::WrongPool`].
    pub fn deallocate_huge(&self, alloc: Allocation) -> Result<(), BufferPoolError> {
        let layout = alloc
            .huge_layout
            .ok_or(BufferPoolError::WrongPool { class: alloc.class })?;
        self.huge.deallocate(HugeAllocation {
            ptr: alloc.ptr,
            layout,
            size: alloc.capacity,
        });
        Ok(())
    }

    /// Returns a slab slot to its owning slab via the arena.
    pub fn return_slot(&self, ptr: NonNull<u8>, class: ClassId) -> Result<(), BufferPoolError> {
        self.arena.mark_free(ptr, class)
    }

    /// Drops fully-free slabs and returns the bytes reclaimed.
    pub fn purge(&self) -> usize {
        self.arena.purge()
    }

    /// Pre-allocates arena capacity for `class`.
    pub fn reserve(&self, class: ClassId, count: usize) -> Result<(), BufferPoolError> {
        self.arena.reserve(class, count)
    }

    /// Total backing bytes held by the arena.
    pub fn arena_memory(&self) -> usize {
        self.arena.memory_usage()
    }

    /// Huge-path counters.
    pub fn huge_stats(&self) -> HugeStatsSnapshot {
        self.huge.stats()
    }
}

impl Default for Allocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classed_allocation_round_trip() {
        let allocator = Allocator::new();
        let alloc = allocator.allocate(100).unwrap();
        assert!(!alloc.is_huge());
        assert_eq!(alloc.capacity, 128);
        assert!(alloc.huge_layout.is_none());
        allocator.return_slot(alloc.ptr, alloc.class).unwrap();
    }

    #[test]
    fn huge_allocation_round_trip() {
        let allocator = Allocator::new();
        let alloc = allocator.allocate(2 << 20).unwrap();
        assert!(alloc.is_huge());
        assert_eq!(alloc.capacity, 2 << 20);
        assert!(alloc.huge_layout.is_some());
        allocator.deallocate_huge(alloc).unwrap();
    }

    #[test]
    fn deallocate_huge_rejects_classed_allocation() {
        let allocator = Allocator::new();
        let alloc = allocator.allocate(100).unwrap();
        assert!(matches!(
            allocator.deallocate_huge(alloc),
            Err(BufferPoolError::WrongPool { .. })
        ));
    }

    #[test]
    fn purge_frees_returned_arena_memory() {
        let allocator = Allocator::new();
        let ptr = allocator.allocate(64).unwrap();
        let class = ptr.class;
        allocator.return_slot(ptr.ptr, class).unwrap();
        let freed = allocator.purge();
        assert!(freed > 0);
    }
}
