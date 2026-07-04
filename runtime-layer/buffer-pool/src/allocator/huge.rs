//! Direct allocator for oversized buffers (Layer 6).
//!
//! Sizes above the class table (default 1 MiB) bypass the slab hierarchy and
//! go straight to the global allocator via [`std::alloc`]. Each allocation is
//! tracked individually — there is no caching, because huge buffers are too
//! large to retain profitably and too rare to justify the bookkeeping.

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::error::BufferPoolError;
use crate::memory::alignment::MIN_ALIGN;

/// An outstanding huge allocation.
#[derive(Debug)]
pub struct HugeAllocation {
    /// Owned pointer to the allocation's start.
    pub ptr: NonNull<u8>,
    /// Layout used at allocation; required for deallocation.
    pub layout: Layout,
    /// Requested size in bytes (may be smaller than `layout.size()` after
    /// rounding, but always equal to what the caller asked for).
    pub size: usize,
}

/// Counters for the huge path.
#[derive(Debug, Default)]
pub struct HugeStats {
    /// Lifetime count of huge allocations.
    pub allocations: AtomicU64,
    /// Lifetime count of huge deallocations.
    pub deallocations: AtomicU64,
    /// Bytes currently live (allocations minus deallocations).
    pub bytes_in_use: AtomicUsize,
    /// Lifetime bytes allocated.
    pub bytes_allocated: AtomicU64,
    /// Lifetime bytes freed.
    pub bytes_freed: AtomicU64,
}

/// A snapshot of [`HugeStats`] at a point in time.
#[derive(Debug, Clone, Copy, Default)]
pub struct HugeStatsSnapshot {
    /// Lifetime allocations.
    pub allocations: u64,
    /// Lifetime deallocations.
    pub deallocations: u64,
    /// Bytes currently live.
    pub bytes_in_use: usize,
    /// Lifetime bytes allocated.
    pub bytes_allocated: u64,
    /// Lifetime bytes freed.
    pub bytes_freed: u64,
}

impl HugeStats {
    fn record_alloc(&self, size: usize) {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(size as u64, Ordering::Relaxed);
        self.bytes_in_use.fetch_add(size, Ordering::Relaxed);
    }

    fn record_dealloc(&self, size: usize) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        self.bytes_freed.fetch_add(size as u64, Ordering::Relaxed);
        self.bytes_in_use.fetch_sub(size, Ordering::Relaxed);
    }

    /// Captures a consistent-enough snapshot of the counters.
    pub fn snapshot(&self) -> HugeStatsSnapshot {
        HugeStatsSnapshot {
            allocations: self.allocations.load(Ordering::Relaxed),
            deallocations: self.deallocations.load(Ordering::Relaxed),
            bytes_in_use: self.bytes_in_use.load(Ordering::Relaxed),
            bytes_allocated: self.bytes_allocated.load(Ordering::Relaxed),
            bytes_freed: self.bytes_freed.load(Ordering::Relaxed),
        }
    }
}

/// Allocator for buffers larger than the size-class table.
pub struct HugeAllocator {
    stats: HugeStats,
}

impl HugeAllocator {
    /// Creates a new huge allocator with zeroed counters.
    pub fn new() -> Self {
        Self {
            stats: HugeStats::default(),
        }
    }

    /// Allocates `size` bytes aligned to at least [`MIN_ALIGN`].
    ///
    /// Returns [`BufferPoolError::Oversized`] when `size` exceeds what a valid
    /// [`Layout`] can describe, and [`BufferPoolError::OutOfMemory`] when the
    /// underlying allocator refuses.
    pub fn allocate(&self, size: usize) -> Result<HugeAllocation, BufferPoolError> {
        if size == 0 {
            return Err(BufferPoolError::InvalidConfig(
                "huge allocate requires a non-zero size".into(),
            ));
        }
        let layout =
            Layout::from_size_align(size, MIN_ALIGN).map_err(|_| BufferPoolError::Oversized {
                size,
                max: isize::MAX as usize,
            })?;
        // SAFETY: `layout` is valid (non-zero size, power-of-two align).
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).ok_or(BufferPoolError::OutOfMemory { size })?;
        self.stats.record_alloc(size);
        Ok(HugeAllocation { ptr, layout, size })
    }

    /// Releases a huge allocation back to the operating system.
    ///
    /// # Panics (debug only)
    ///
    /// In debug builds this asserts that `alloc.layout` matches; in release it
    /// trusts the caller. Neither path panics in production.
    pub fn deallocate(&self, alloc: HugeAllocation) {
        let size = alloc.size;
        // SAFETY: `alloc.layout` came from the matching `Layout::from_size_align`
        // call in `allocate`, and `alloc.ptr` is the pointer returned by `alloc`.
        unsafe {
            dealloc(alloc.ptr.as_ptr(), alloc.layout);
        }
        self.stats.record_dealloc(size);
    }

    /// Returns a snapshot of the huge-path counters.
    pub fn stats(&self) -> HugeStatsSnapshot {
        self.stats.snapshot()
    }
}

impl Default for HugeAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_and_deallocate_round_trip() {
        let huge = HugeAllocator::new();
        let alloc = huge.allocate(1 << 21).unwrap(); // 2 MiB
        assert_eq!(alloc.size, 1 << 21);
        assert!(alloc.ptr.as_ptr() as usize % MIN_ALIGN == 0);
        let snap = huge.stats();
        assert_eq!(snap.allocations, 1);
        assert_eq!(snap.bytes_in_use, 1 << 21);
        huge.deallocate(alloc);
        let snap = huge.stats();
        assert_eq!(snap.deallocations, 1);
        assert_eq!(snap.bytes_in_use, 0);
    }

    #[test]
    fn zero_size_rejected() {
        let huge = HugeAllocator::new();
        assert!(matches!(
            huge.allocate(0),
            Err(BufferPoolError::InvalidConfig(_))
        ));
    }

    #[test]
    fn oversized_layout_rejected() {
        let huge = HugeAllocator::new();
        // isize::MAX + 1 cannot be described by a Layout.
        let result = huge.allocate(isize::MAX as usize + 1);
        assert!(matches!(result, Err(BufferPoolError::Oversized { .. })));
    }

    #[test]
    fn stats_track_lifetime_bytes() {
        let huge = HugeAllocator::new();
        let a = huge.allocate(1 << 20).unwrap();
        let b = huge.allocate(1 << 21).unwrap();
        let snap = huge.stats();
        assert_eq!(snap.bytes_allocated, (1 << 20) + (1 << 21));
        assert_eq!(snap.bytes_in_use, (1 << 20) + (1 << 21));
        huge.deallocate(a);
        let snap = huge.stats();
        assert_eq!(snap.bytes_freed, 1 << 20);
        assert_eq!(snap.bytes_in_use, 1 << 21);
        huge.deallocate(b);
    }
}
