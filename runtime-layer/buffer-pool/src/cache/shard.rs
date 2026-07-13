//! One shard of the global cache (Layer 8).
//!
//! A [`Shard`] is a [`Mutex`]-guarded FIFO of recycled slots. Many shards per
//! class let releasing threads avoid contending on a single lock.

use std::collections::VecDeque;

use parking_lot::Mutex;

use crate::cache::SlotPtr;

/// A lock-protected queue of recycled slot pointers with a soft capacity.
pub struct Shard {
    buffers: Mutex<VecDeque<SlotPtr>>,
    cap: usize,
}

impl Shard {
    /// Creates an empty shard with the given capacity.
    pub fn new(cap: usize) -> Self {
        Self {
            buffers: Mutex::new(VecDeque::with_capacity(cap.min(64))),
            cap,
        }
    }

    /// Pops one slot from the front.
    pub fn pop(&self) -> Option<SlotPtr> {
        self.buffers.lock().pop_front()
    }

    /// Pops up to `n` slots, appending them to `out`. Returns the count moved.
    pub fn pop_batch(&self, n: usize, out: &mut Vec<SlotPtr>) -> usize {
        let mut q = self.buffers.lock();
        let take = n.min(q.len());
        for _ in 0..take {
            if let Some(p) = q.pop_front() {
                out.push(p);
            }
        }
        take
    }

    /// Pushes one slot if below capacity. Returns `true` when accepted.
    pub fn push(&self, ptr: SlotPtr) -> bool {
        let mut q = self.buffers.lock();
        if q.len() >= self.cap {
            return false;
        }
        q.push_back(ptr);
        true
    }

    /// Pushes as many slots from `batch` as fit. The accepted slots are
    /// removed from the front of `batch`; the remainder stay for the caller to
    /// return to the arena. Returns the count accepted.
    pub fn push_batch(&self, batch: &mut Vec<SlotPtr>) -> usize {
        let mut q = self.buffers.lock();
        let mut accepted = 0;
        while q.len() < self.cap {
            let Some(ptr) = batch.pop() else {
                break;
            };
            q.push_back(ptr);
            accepted += 1;
        }
        accepted
    }

    /// Current number of cached slots.
    pub fn len(&self) -> usize {
        self.buffers.lock().len()
    }

    /// Capacity (soft cap on retained slots).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Drops slots until at most `max` remain, returning the drained pointers.
    pub fn trim_to(&self, max: usize) -> Vec<SlotPtr> {
        let mut q = self.buffers.lock();
        let mut drained = Vec::new();
        while q.len() > max {
            if let Some(p) = q.pop_front() {
                drained.push(p);
            }
        }
        drained
    }

    /// Drains every slot. Used by the shrinker.
    pub fn drain_all(&self) -> Vec<SlotPtr> {
        self.buffers.lock().drain(..).collect()
    }

    /// True when no slots are cached.
    pub fn is_empty(&self) -> bool {
        self.buffers.lock().is_empty()
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
    fn push_then_pop_preserves_order() {
        let shard = Shard::new(8);
        assert!(shard.push(ptr(1)));
        assert!(shard.push(ptr(2)));
        assert_eq!(shard.len(), 2);
        assert_eq!(shard.pop().map(|s| s.as_usize()), Some(1));
        assert_eq!(shard.pop().map(|s| s.as_usize()), Some(2));
        assert!(shard.pop().is_none());
    }

    #[test]
    fn push_respects_capacity() {
        let shard = Shard::new(2);
        assert!(shard.push(ptr(1)));
        assert!(shard.push(ptr(2)));
        assert!(!shard.push(ptr(3)));
        assert_eq!(shard.len(), 2);
    }

    #[test]
    fn pop_batch_moves_into_out() {
        let shard = Shard::new(8);
        for a in 1..=4 {
            assert!(shard.push(ptr(a)));
        }
        let mut out = Vec::new();
        let moved = shard.pop_batch(3, &mut out);
        assert_eq!(moved, 3);
        assert_eq!(out.len(), 3);
        assert_eq!(shard.len(), 1);
    }

    #[test]
    fn push_batch_accepts_until_full() {
        let shard = Shard::new(3);
        let mut batch = vec![ptr(10), ptr(20), ptr(30), ptr(40)];
        let accepted = shard.push_batch(&mut batch);
        assert_eq!(accepted, 3);
        assert_eq!(batch.len(), 1);
        assert_eq!(shard.len(), 3);
    }

    #[test]
    fn trim_to_returns_excess() {
        let shard = Shard::new(8);
        for a in 1..=5 {
            shard.push(ptr(a));
        }
        let drained = shard.trim_to(2);
        assert_eq!(drained.len(), 3);
        assert_eq!(shard.len(), 2);
    }
}
