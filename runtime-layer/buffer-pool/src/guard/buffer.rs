//! The [`Buffer`] guard (Layer 12).
//!
//! Owns a slot leased from the pool. [`Deref`] / [`DerefMut`] expose the slot
//! as a byte slice; `Drop` returns it through the [`ReleaseSink`].

use core::alloc::Layout;
use core::fmt;
use core::ops::{Deref, DerefMut};
use core::ptr::NonNull;
use std::rc::Weak;

use bytes::Bytes;

use super::ReleaseSink;
use crate::allocator::size_class::ClassId;

/// An owned, pooled byte buffer.
///
/// Constructed by [`BufferPool::get`](crate::buffer::BufferPool::get); released
/// automatically on `Drop`. Derefs to the caller-requested length (`len`); the
/// underlying slot capacity is available via [`capacity`](Buffer::capacity).
///
/// # Huge allocations
///
/// When the pool serves a request above its class-table maximum, the buffer is
/// backed by a direct [`std::alloc`] allocation. If such a buffer outlives its
/// pool, `Drop` deallocates the raw allocation directly to avoid leaking large
/// regions. Classed buffers that outlive the pool leak their slot — the slab
/// backing has already been freed by the arena.
pub struct Buffer {
    /// Slot start.
    ptr: NonNull<u8>,
    /// Caller-visible length (the requested size).
    len: usize,
    /// Slot capacity (the class size or huge size).
    cap: usize,
    /// Size class.
    class: ClassId,
    /// Layout for huge allocations; `None` for classed.
    huge_layout: Option<Layout>,
    /// Weak ref to the pool's release sink.
    sink: Weak<dyn ReleaseSink>,
}

impl Buffer {
    /// Creates a new guard wrapping `ptr`.
    ///
    /// This is `pub(crate)` — only the pool constructs buffers.
    #[inline]
    #[allow(dead_code)] // wired up in Phase 5 (BufferPool::get)
    pub(crate) fn new(
        ptr: NonNull<u8>,
        len: usize,
        cap: usize,
        class: ClassId,
        huge_layout: Option<Layout>,
        sink: Weak<dyn ReleaseSink>,
    ) -> Self {
        Self {
            ptr,
            len,
            cap,
            class,
            huge_layout,
            sink,
        }
    }

    /// Caller-visible length (the size passed to `get`).
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Capacity of the underlying slot.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// `true` when `len` is zero.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Size class of this buffer.
    #[inline]
    pub fn class(&self) -> ClassId {
        self.class
    }

    /// `true` when this buffer was served from the huge allocator.
    #[inline]
    pub fn is_huge(&self) -> bool {
        self.class == ClassId::HUGE
    }

    /// Copies the visible bytes into a refcounted [`Bytes`].
    ///
    /// This always copies — the pool's slot is released back for recycling
    /// after the copy completes. Prefer `&buf[..]` when refcounted ownership
    /// is not required.
    pub fn into_bytes(self) -> Bytes {
        // `self` drops after the expression evaluates, so the copy completes
        // before the slot is released.
        Bytes::copy_from_slice(&self[..])
    }
}

impl Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `len` bytes for the lifetime of `self`.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for Buffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is valid for `len` bytes; `&mut self` ensures
        // exclusivity.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buffer")
            .field("ptr", &format_args!("{:p}", self.ptr.as_ptr()))
            .field("len", &self.len)
            .field("cap", &self.cap)
            .field("class", &self.class)
            .field("is_huge", &self.is_huge())
            .finish()
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(sink) = self.sink.upgrade() {
            sink.release_buffer(self.ptr, self.len, self.class, self.huge_layout);
            return;
        }
        // Pool is gone.
        if let Some(layout) = self.huge_layout {
            // SAFETY: `ptr` was allocated via `std::alloc::alloc` with exactly
            // `layout`. We still own it and have not freed it.
            unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) };
        }
        // Classed slot: the arena already dropped the slab backing. Touching
        // `ptr` would be use-after-free. Leak the dangling handle.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{Layout, alloc, dealloc};
    use std::cell::RefCell;
    use std::rc::Rc;

    /// Test sink that records every release.
    struct RecordingSink {
        releases: RefCell<Vec<(usize, usize, ClassId, bool)>>,
    }

    impl RecordingSink {
        fn new() -> Rc<Self> {
            Rc::new(Self {
                releases: RefCell::new(Vec::new()),
            })
        }

        fn count(&self) -> usize {
            self.releases.borrow().len()
        }

        fn last(&self) -> (usize, usize, ClassId, bool) {
            *self.releases.borrow().last().expect("at least one release")
        }
    }

    impl ReleaseSink for RecordingSink {
        fn release_buffer(
            &self,
            ptr: NonNull<u8>,
            len: usize,
            class: ClassId,
            huge_layout: Option<Layout>,
        ) {
            self.releases.borrow_mut().push((
                ptr.as_ptr() as usize,
                len,
                class,
                huge_layout.is_some(),
            ));
        }
    }

    /// Allocates `cap` bytes with a 64-byte alignment (matching [`MIN_ALIGN`]).
    ///
    /// [`MIN_ALIGN`]: crate::memory::alignment::MIN_ALIGN
    fn alloc_slot(cap: usize) -> (NonNull<u8>, Layout) {
        let layout = Layout::from_size_align(cap, 64).expect("layout");
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).expect("alloc");
        (ptr, layout)
    }

    fn make_buffer(len: usize, cap: usize) -> (Buffer, Rc<RecordingSink>) {
        let (ptr, _) = alloc_slot(cap);
        let sink = RecordingSink::new();
        let buf = Buffer::new(
            ptr,
            len,
            cap,
            ClassId(1),
            None,
            Rc::downgrade(&sink) as Weak<dyn ReleaseSink>,
        );
        (buf, sink)
    }

    #[test]
    fn deref_and_deref_mut_round_trip() {
        let (mut buf, _sink) = make_buffer(16, 64);
        assert_eq!(buf.len(), 16);
        assert_eq!(buf.capacity(), 64);
        for b in buf.iter_mut() {
            *b = 0xAA;
        }
        assert!(buf.iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn deref_slice_is_len_bytes() {
        let (buf, _sink) = make_buffer(8, 64);
        assert_eq!(buf.len(), 8);
        // Deref yields exactly len bytes, not cap.
        assert_eq!(buf.deref().len(), 8);
    }

    #[test]
    fn drop_releases_exactly_once() {
        let (buf, sink) = make_buffer(16, 64);
        assert_eq!(sink.count(), 0);
        drop(buf);
        assert_eq!(sink.count(), 1);
        let (_, len, class, is_huge) = sink.last();
        assert_eq!(len, 16);
        assert_eq!(class, ClassId(1));
        assert!(!is_huge);
    }

    #[test]
    fn into_bytes_copies_then_releases() {
        let (mut buf, sink) = make_buffer(16, 64);
        buf[..5].copy_from_slice(b"hello");
        let bytes = buf.into_bytes();
        assert_eq!(&bytes[..5], b"hello");
        assert_eq!(bytes.len(), 16);
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn debug_format_includes_len_and_cap() {
        let (buf, _sink) = make_buffer(16, 64);
        let s = format!("{buf:?}");
        assert!(s.contains("len: 16"));
        assert!(s.contains("cap: 64"));
    }

    #[test]
    fn is_empty_tracks_len() {
        let (buf, _sink) = make_buffer(0, 64);
        assert!(buf.is_empty());
        let (buf2, _sink2) = make_buffer(1, 64);
        assert!(!buf2.is_empty());
    }

    // --- Pool-gone behaviour ------------------------------------------------

    /// A no-op sink used to construct a [`Weak<dyn ReleaseSink>`] that will
    /// never upgrade — the `Rc` is dropped immediately after downgrade.
    struct DeadSink;

    impl ReleaseSink for DeadSink {
        fn release_buffer(&self, _: NonNull<u8>, _: usize, _: ClassId, _: Option<Layout>) {}
    }

    /// Creates a weak that never upgrades (simulates a dropped pool).
    fn dead_weak() -> Weak<dyn ReleaseSink> {
        let rc: Rc<dyn ReleaseSink> = Rc::new(DeadSink);
        let weak = Rc::downgrade(&rc);
        drop(rc);
        weak
    }

    #[test]
    fn drop_classed_when_pool_gone_does_not_panic() {
        let weak = dead_weak();
        let (ptr, layout) = alloc_slot(128);
        let buf = Buffer::new(ptr, 128, 128, ClassId(0), None, weak);
        // Must not panic. Slot leaks (documented).
        drop(buf);
        // Manually free the backing so the test is leak-clean.
        unsafe { dealloc(ptr.as_ptr(), layout) };
    }

    #[test]
    fn drop_huge_when_pool_gone_deallocs_directly() {
        let size = 2 * 1024 * 1024;
        let layout = Layout::from_size_align(size, 4096).expect("layout");
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).expect("alloc");
        let weak = dead_weak();
        let buf = Buffer::new(ptr, size, size, ClassId::HUGE, Some(layout), weak);
        // Drop must dealloc directly — no leak, no panic.
        drop(buf);
        // If we reach here the dealloc path ran cleanly.
    }

    #[test]
    fn is_huge_flag_is_set() {
        let weak = dead_weak();
        let layout = Layout::from_size_align(2 * 1024 * 1024, 4096).expect("layout");
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).expect("alloc");
        let buf = Buffer::new(
            ptr,
            2 * 1024 * 1024,
            2 * 1024 * 1024,
            ClassId::HUGE,
            Some(layout),
            weak,
        );
        assert!(buf.is_huge());
        // Pool is gone (dead_weak) → huge path deallocs in Drop.
    }
}
