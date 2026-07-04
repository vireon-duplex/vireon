//! Buffer caches (Layers 7-9).
//!
//! - [`shard`] — one shard of the global cache.
//! - [`global`] — the `[class][shard]` matrix shared across threads.
//! - [`thread_local`] — per-thread buckets (fast path).
//! - [`batch`] — adaptive batch sizing for refill/drain.
//! - [`refill`] / [`drain`] — coordinated transfers between the tiers.
//!
//! [`thread_local`]: mod@thread_local

pub mod batch;
pub mod drain;
pub mod global;
pub mod refill;
pub mod shard;
pub mod thread_local;

use core::ptr::NonNull;

/// A recycled slab-slot pointer.
///
/// Wraps a [`NonNull<u8>`] so it is [`Send`] + [`Sync`]. This is sound because
/// a cached slot is unaliased — no live buffer references it while it sits in
/// a cache — and the cache only ever moves the pointer, never dereferences it.
#[repr(transparent)]
pub struct SlotPtr(pub NonNull<u8>);

// SAFETY: see the type-level doc comment — the pointer denotes exclusive
// ownership of a heap slot that is not concurrently accessed while cached.
unsafe impl Send for SlotPtr {}
unsafe impl Sync for SlotPtr {}

impl SlotPtr {
    /// Wraps a non-null pointer.
    #[inline]
    pub fn new(ptr: NonNull<u8>) -> Self {
        Self(ptr)
    }

    /// Returns the underlying pointer.
    #[inline]
    pub fn as_nonnull(self) -> NonNull<u8> {
        self.0
    }

    /// Returns the raw address.
    #[inline]
    pub fn as_usize(self) -> usize {
        self.0.as_ptr() as usize
    }
}

impl From<NonNull<u8>> for SlotPtr {
    #[inline]
    fn from(ptr: NonNull<u8>) -> Self {
        Self(ptr)
    }
}

impl Clone for SlotPtr {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl Copy for SlotPtr {}

impl core::fmt::Debug for SlotPtr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "SlotPtr({:#x})", self.0.as_ptr() as usize)
    }
}

impl PartialEq for SlotPtr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for SlotPtr {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_ptr_wraps_and_unwraps() {
        let raw = NonNull::<u8>::dangling();
        let slot = SlotPtr::new(raw);
        assert_eq!(slot.as_nonnull(), raw);
        let cloned = slot;
        assert_eq!(cloned.as_nonnull(), raw);
    }
}
