//! Memory abstractions for the IO layer.
//!
//! [`MemoryRegion`] owns a contiguous allocation that backs a buffer
//! pool, fixed-buffer registration, or any kernel-facing memory. It
//! frees memory on `Drop` so callers never need to call `dealloc`
//! manually.
//!
//! # Future expansion
//!
//! The struct currently uses `std::alloc` (heap). When HugePage /
//! Pinned / DMA support is needed, a `MemoryKind` enum or
//! `BufferAllocator` trait can be added without changing the public
//! surface — only the internal allocation path changes.

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

use crate::error::IoError;

pub mod buffer_pool;

pub use buffer_pool::BufferPool;

/// Owned, page-aligned memory region.
///
/// Allocates with `std::alloc` on construction and frees on `Drop`.
/// The pointer is guaranteed non-null and aligned to `align`.
pub struct MemoryRegion {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl MemoryRegion {
    /// Allocates `size` bytes aligned to `align`.
    ///
    /// `align` must be a power of two and a valid Rust allocation
    /// alignment. `size` must be non-zero.
    pub fn alloc(size: usize, align: usize) -> Result<Self, IoError> {
        if size == 0 {
            return Err(IoError::InvalidConfig("size must be > 0".into()));
        }
        let layout = Layout::from_size_align(size, align)
            .map_err(|e| IoError::InvalidConfig(format!("memory layout: {e}")))?;
        // SAFETY: `layout` is valid (checked above). On failure returns null.
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr)
            .ok_or_else(|| IoError::InvalidConfig(format!("allocation of {size} bytes failed")))?;
        // Zero the region so callers start from a known state.
        // SAFETY: `ptr` points to `size` freshly-allocated bytes.
        unsafe { std::ptr::write_bytes(ptr.as_ptr(), 0, size) };
        Ok(Self { ptr, layout })
    }

    /// Returns the base pointer.
    #[must_use]
    pub const fn as_ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    /// Returns the allocation size in bytes.
    #[allow(clippy::len_without_is_empty)]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.layout.size()
    }

    /// Returns the alignment.
    #[must_use]
    pub const fn align(&self) -> usize {
        self.layout.align()
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        // SAFETY: `ptr` was allocated with `layout` in `alloc`.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

impl std::fmt::Debug for MemoryRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryRegion")
            .field("ptr", &self.ptr)
            .field("size", &self.layout.size())
            .field("align", &self.layout.align())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_returns_zeroed_region() {
        let region = MemoryRegion::alloc(256, 64).expect("alloc");
        assert_eq!(region.len(), 256);
        assert_eq!(region.align(), 64);
        // Verify zeroed.
        let ptr = region.as_ptr().as_ptr();
        for i in 0..256 {
            assert_eq!(unsafe { *ptr.add(i) }, 0, "byte {i} not zeroed");
        }
    }

    #[test]
    fn alloc_is_aligned() {
        for &align in &[1, 2, 4, 8, 16, 32, 64, 4096] {
            let region = MemoryRegion::alloc(128, align).expect("alloc");
            let addr = region.as_ptr().as_ptr() as usize;
            assert_eq!(addr % align, 0, "not aligned to {align}");
        }
    }

    #[test]
    fn alloc_rejects_zero_size() {
        let err = MemoryRegion::alloc(0, 8).expect_err("zero size");
        assert!(matches!(err, IoError::InvalidConfig(_)));
    }

    #[test]
    fn drop_frees_without_leak() {
        // Just verify Drop doesn't panic. A real leak check would use
        // valgrind or a custom allocator.
        for _ in 0..100 {
            let _ = MemoryRegion::alloc(4096, 4096).expect("alloc");
        }
    }
}
