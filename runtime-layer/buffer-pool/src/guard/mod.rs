//! RAII guard for a pooled slot (Layer 12).
//!
//! [`Buffer`] is the user-facing handle returned by [`BufferPool::get`]. It
//! [`Deref`](core::ops::Deref)s to `&[u8]` / `&mut [u8]` over the owned slot
//! and, on `Drop`, hands the slot back to the pool through a [`ReleaseSink`].
//!
//! The sink is held as a [`Weak`](std::sync::Weak) so a buffer that outlives its
//! pool degrades gracefully instead of keeping the pool alive.
//!
//! [`BufferPool::get`]: crate::buffer::BufferPool::get

pub mod buffer;

pub use buffer::Buffer;

use core::alloc::Layout;
use core::ptr::NonNull;

use crate::allocator::size_class::ClassId;

/// Receiver for a released buffer.
///
/// Implemented by the pool's inner state. The guard calls
/// [`release_buffer`](ReleaseSink::release_buffer) exactly once on `Drop`; the
/// sink routes the pointer back through zeroing → thread-local cache → global
/// cache → arena/huge as appropriate.
///
/// The trait is object-safe so the guard can store `Weak<dyn ReleaseSink>`.
pub trait ReleaseSink {
    /// Receives ownership of a slot back from a dropping [`Buffer`].
    ///
    /// # Parameters
    ///
    /// - `ptr` — start of the slot (same value returned by the allocator).
    /// - `len` — number of bytes the caller wrote; drives zeroing.
    /// - `class` — size class ([`ClassId::HUGE`] for the huge path).
    /// - `huge_layout` — `Layout` for huge allocations; `None` for slab slots.
    fn release_buffer(
        &self,
        ptr: NonNull<u8>,
        len: usize,
        class: ClassId,
        huge_layout: Option<Layout>,
    );
}
