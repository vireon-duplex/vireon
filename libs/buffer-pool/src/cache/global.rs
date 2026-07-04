//! Sharded global cache (Layer 8).
//!
//! [`GlobalCache`] holds a `[class][shard]` matrix of [`Shard`]s. Each thread
//! maps to one shard per class via [`thread_key`], so releasing and refilling
//! threads touch the same lock only when they hash to the same shard.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cache::SlotPtr;
use crate::cache::shard::Shard;

/// Lazily-assigned per-thread identifier used to pick a shard.
///
/// Returns a stable non-zero `usize` per thread, assigned on first call.
pub fn thread_key() -> usize {
    thread_local! {
        static KEY: Cell<usize> = const { Cell::new(0) };
    }
    KEY.with(|c| {
        let v = c.get();
        if v != 0 {
            v
        } else {
            let next = THREAD_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
            c.set(next);
            next
        }
    })
}

static THREAD_SEQ: AtomicUsize = AtomicUsize::new(0);

/// The shared cache sitting above the arena.
pub struct GlobalCache {
    /// `shards[class_index][shard_index]`.
    shards: Vec<Vec<Shard>>,
    shards_per_class: usize,
    cap_per_shard: usize,
}

impl GlobalCache {
    /// Creates an empty cache with `shards_per_class` shards of `cap_per_shard`
    /// slots each, for every size class.
    pub fn new(num_classes: usize, shards_per_class: usize, cap_per_shard: usize) -> Self {
        let shards = (0..num_classes)
            .map(|_| {
                (0..shards_per_class)
                    .map(|_| Shard::new(cap_per_shard))
                    .collect()
            })
            .collect();
        Self {
            shards,
            shards_per_class,
            cap_per_shard,
        }
    }

    /// Picks the shard index for the calling thread and `class_idx`.
    #[inline]
    fn shard_index(&self, class_idx: usize) -> usize {
        if self.shards_per_class == 1 {
            return 0;
        }
        (class_idx.wrapping_add(thread_key())) % self.shards_per_class
    }

    /// Pops one slot for `class_idx` from the calling thread's shard.
    pub fn pop(&self, class_idx: usize) -> Option<SlotPtr> {
        let s = self.shard_index(class_idx);
        self.shards[class_idx][s].pop()
    }

    /// Pops up to `n` slots into `out`. Returns the count moved.
    pub fn pop_batch(&self, class_idx: usize, n: usize, out: &mut Vec<SlotPtr>) -> usize {
        let s = self.shard_index(class_idx);
        self.shards[class_idx][s].pop_batch(n, out)
    }

    /// Pushes one slot to the calling thread's shard. Returns `true` if accepted.
    pub fn push(&self, class_idx: usize, ptr: SlotPtr) -> bool {
        let s = self.shard_index(class_idx);
        self.shards[class_idx][s].push(ptr)
    }

    /// Pushes a batch; returns the count accepted (remainder stays in `batch`).
    pub fn push_batch(&self, class_idx: usize, batch: &mut Vec<SlotPtr>) -> usize {
        let s = self.shard_index(class_idx);
        self.shards[class_idx][s].push_batch(batch)
    }

    /// Total cached slots across all shards and classes.
    pub fn len(&self) -> usize {
        self.shards.iter().flatten().map(|s| s.len()).sum()
    }

    /// True when no slots are cached anywhere.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate bytes retained (`count * per_class_size`) — caller passes a
    /// function mapping class index to slot size.
    pub fn memory_usage(&self, slot_size: impl Fn(usize) -> usize) -> usize {
        self.shards
            .iter()
            .enumerate()
            .map(|(class_idx, shards)| {
                let size = slot_size(class_idx);
                size * shards.iter().map(|s| s.len()).sum::<usize>()
            })
            .sum()
    }

    /// Drops slots from every shard until each is at most `max` slots.
    /// Returns the drained pointers grouped by class.
    pub fn trim(&self, max_per_shard: usize) -> Vec<Vec<SlotPtr>> {
        self.shards
            .iter()
            .map(|class_shards| {
                let mut drained = Vec::new();
                for shard in class_shards {
                    drained.extend(shard.trim_to(max_per_shard));
                }
                drained
            })
            .collect()
    }

    /// Drains every shard, grouped by class. Used by the shrinker.
    pub fn drain_all(&self) -> Vec<Vec<SlotPtr>> {
        self.shards
            .iter()
            .map(|class_shards| {
                let mut drained = Vec::new();
                for shard in class_shards {
                    drained.extend(shard.drain_all());
                }
                drained
            })
            .collect()
    }

    /// Number of shards per class.
    #[inline]
    pub fn shards_per_class(&self) -> usize {
        self.shards_per_class
    }

    /// Capacity of each shard.
    #[inline]
    pub fn cap_per_shard(&self) -> usize {
        self.cap_per_shard
    }
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
        let cache = GlobalCache::new(NUM_CLASSES, 4, 8);
        assert!(cache.push(0, ptr(0x10)));
        assert_eq!(cache.pop(0).map(|s| s.as_usize()), Some(0x10));
        assert_eq!(cache.pop(0), None);
    }

    #[test]
    fn sharding_distributes_across_shards() {
        // Multiple threads should not all collide on one shard.
        let cache = GlobalCache::new(NUM_CLASSES, 8, 1024);
        let cache_ref = &cache;
        std::thread::scope(|s| {
            for t in 0..4u32 {
                s.spawn(move || {
                    for i in 0..16 {
                        let _ = cache_ref.push(0, ptr(0x1000 + t as usize * 0x100 + i));
                    }
                });
            }
        });
        // Total across all shards should be 64.
        let total: usize = cache.shards[0].iter().map(|s| s.len()).sum();
        assert_eq!(total, 64);
        // At least two shards received data (probabilistic but reliable with 4 threads).
        let nonempty = cache.shards[0].iter().filter(|s| !s.is_empty()).count();
        assert!(
            nonempty >= 2,
            "expected sharding to spread load, got {nonempty}"
        );
    }

    #[test]
    fn memory_usage_sums_class_sizes() {
        let cache = GlobalCache::new(NUM_CLASSES, 2, 8);
        for _ in 0..3 {
            cache.push(0, ptr(0x1));
        }
        for _ in 0..2 {
            cache.push(1, ptr(0x2));
        }
        let bytes = cache.memory_usage(|idx| match idx {
            0 => 64,
            1 => 128,
            _ => 0,
        });
        assert_eq!(bytes, 3 * 64 + 2 * 128);
    }

    #[test]
    fn trim_returns_excess_grouped_by_class() {
        let cache = GlobalCache::new(NUM_CLASSES, 1, 16);
        for i in 0..10 {
            cache.push(0, ptr(0x100 + i));
        }
        let drained = cache.trim(2);
        assert_eq!(drained[0].len(), 8);
    }

    #[test]
    fn drain_all_empties_the_cache() {
        let cache = GlobalCache::new(NUM_CLASSES, 2, 8);
        cache.push(0, ptr(1));
        cache.push(1, ptr(2));
        let drained = cache.drain_all();
        assert_eq!(drained[0].len() + drained[1].len(), 2);
        assert_eq!(cache.len(), 0);
    }

    use crate::allocator::size_class::NUM_CLASSES;
}
