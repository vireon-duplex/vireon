//! Thread-local cache (Layer 7).
//!
//! Each thread holds `NUM_CLASSES` buckets of recycled slots; the per-buffer
//! fast path is a `Vec::pop` on the matching bucket. A thread "claims" the
//! cache on first use for a given pool id; a second pool on the same thread
//! falls through to the global cache (documented limitation — revisit with the
//! `thread_local` crate if a real workload needs per-pool caching).

use std::cell::RefCell;

use crate::cache::SlotPtr;

/// Per-thread collection of recycling buckets.
pub struct ThreadCache {
    buckets: Vec<Vec<SlotPtr>>,
    cap: usize,
}

impl ThreadCache {
    /// Creates an empty cache with `num_classes` buckets, each holding up to
    /// `cap` slots.
    pub fn new(num_classes: usize, cap: usize) -> Self {
        let buckets = (0..num_classes)
            .map(|_| Vec::with_capacity(cap.min(64)))
            .collect();
        Self { buckets, cap }
    }

    /// Pops one slot from bucket `class_idx`.
    #[inline]
    pub fn pop(&mut self, class_idx: usize) -> Option<SlotPtr> {
        self.buckets[class_idx].pop()
    }

    /// Pushes one slot. Returns `true` if accepted, `false` if the bucket is
    /// full (caller should drain to the global cache).
    #[inline]
    pub fn push(&mut self, class_idx: usize, ptr: SlotPtr) -> bool {
        if self.buckets[class_idx].len() >= self.cap {
            return false;
        }
        self.buckets[class_idx].push(ptr);
        true
    }

    /// Number of slots currently cached for `class_idx`.
    #[inline]
    pub fn len(&self, class_idx: usize) -> usize {
        self.buckets[class_idx].len()
    }

    /// Per-bucket capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Total slots across all buckets.
    pub fn total(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    /// Drains bucket `class_idx`, returning its contents for return to the
    /// global cache or arena.
    pub fn drain_bucket(&mut self, class_idx: usize) -> Vec<SlotPtr> {
        self.buckets[class_idx].drain(..).collect()
    }

    /// Drains every bucket, grouped by class.
    pub fn drain_all(&mut self) -> Vec<Vec<SlotPtr>> {
        self.buckets.iter_mut().map(std::mem::take).collect()
    }
}

struct ThreadCacheEntry {
    pool_id: usize,
    cache: ThreadCache,
}

thread_local! {
    static THREAD_CACHE: RefCell<Option<ThreadCacheEntry>> = const { RefCell::new(None) };
}

/// Runs `f` against this thread's cache for `pool_id` if this thread is
/// claimed by that pool (or not yet claimed).
///
/// Returns `None` when another pool has already claimed this thread's cache —
/// in that case the caller should fall through to the global cache.
pub fn try_with_local_cache<R>(
    pool_id: usize,
    num_classes: usize,
    cap: usize,
    f: impl FnOnce(&mut ThreadCache) -> R,
) -> Option<R> {
    THREAD_CACHE.with(|cell| {
        let mut guard = cell.borrow_mut();
        match guard.as_mut() {
            Some(entry) if entry.pool_id == pool_id => Some(f(&mut entry.cache)),
            Some(_) => None,
            None => {
                let mut cache = ThreadCache::new(num_classes, cap);
                let result = f(&mut cache);
                *guard = Some(ThreadCacheEntry { pool_id, cache });
                Some(result)
            }
        }
    })
}

/// Drains this thread's cache for `pool_id` if claimed, returning the slots
/// grouped by class. Used by [`BufferPool::flush`] and on close.
///
/// [`BufferPool::flush`]: crate::buffer::BufferPool::flush
pub fn flush_current_thread(pool_id: usize) -> Vec<Vec<SlotPtr>> {
    THREAD_CACHE.with(|cell| {
        let mut guard = cell.borrow_mut();
        match guard.as_mut() {
            Some(entry) if entry.pool_id == pool_id => entry.cache.drain_all(),
            _ => Vec::new(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr::NonNull;

    fn ptr(addr: usize) -> SlotPtr {
        SlotPtr::new(NonNull::new(addr as *mut u8).unwrap())
    }

    #[test]
    fn push_then_pop_round_trip() {
        let mut cache = ThreadCache::new(4, 8);
        assert!(cache.push(0, ptr(0x10)));
        assert_eq!(cache.pop(0).map(|s| s.as_usize()), Some(0x10));
        assert!(cache.pop(0).is_none());
    }

    #[test]
    fn push_respects_capacity() {
        let mut cache = ThreadCache::new(2, 2);
        assert!(cache.push(0, ptr(1)));
        assert!(cache.push(0, ptr(2)));
        assert!(!cache.push(0, ptr(3)));
        assert_eq!(cache.len(0), 2);
    }

    #[test]
    fn drain_all_empties_every_bucket() {
        let mut cache = ThreadCache::new(3, 8);
        cache.push(0, ptr(1));
        cache.push(1, ptr(2));
        cache.push(1, ptr(3));
        let drained = cache.drain_all();
        assert_eq!(drained[0].len(), 1);
        assert_eq!(drained[1].len(), 2);
        assert_eq!(drained[2].len(), 0);
        assert_eq!(cache.total(), 0);
    }

    #[test]
    fn try_with_local_cache_claims_for_first_pool() {
        let r1 = try_with_local_cache(100, 4, 8, |c| {
            c.push(0, ptr(0xAA));
            c.pop(0).map(|s| s.as_usize())
        });
        assert_eq!(r1, Some(Some(0xAA)));

        // A second pool on the same thread must fall through.
        let r2 = try_with_local_cache(200, 4, 8, |c| c.pop(0).map(|s| s.as_usize()));
        assert_eq!(r2, None);

        // The first pool still owns the cache.
        let r3 = try_with_local_cache(100, 4, 8, |c| c.total());
        assert_eq!(r3, Some(0));
    }
}
