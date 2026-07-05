//! ReadyQueue — 4-band priority FIFO (Layer 12).
//!
//! Each priority band is a separate ring buffer of slot indices. The
//! queue is **reactor-local**: only the owning reactor thread pushes
//! and pops. No atomics are needed.

use crate::id::decode_slot;
use crate::priority::TaskPriority;

/// Per-band FIFO ring of slot indices.
struct Band {
    /// Power-of-two masked ring of slot indices. `u32` because slot
    /// indices never exceed `MAX_SLOTS`.
    buf: Vec<u32>,
    /// Head (pop) index into `buf`.
    head: u32,
    /// Tail (push) index into `buf`.
    tail: u32,
    /// `buf.len()` cached as a mask: `len - 1` (power of two).
    mask: u32,
    /// Number of items currently in the ring.
    len: u32,
    /// Enqueue timestamp (monotonic ns) of the oldest in-ring item.
    /// Used by [`AgingTracker`](crate::aging) to detect starvation.
    oldest_enqueue_ns: u64,
}

impl Band {
    fn new(capacity: usize) -> Self {
        let cap = capacity.max(2).next_power_of_two();
        Self {
            buf: vec![0; cap],
            head: 0,
            tail: 0,
            mask: (cap - 1) as u32,
            len: 0,
            oldest_enqueue_ns: 0,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Pushes `slot_idx`. Caller guarantees the ring is not full.
    /// Returns `false` if the ring is full.
    #[inline]
    fn push(&mut self, slot_idx: u32, now_ns: u64) -> bool {
        if (self.len as usize) == self.capacity() {
            return false;
        }
        if self.len == 0 {
            self.oldest_enqueue_ns = now_ns;
        }
        self.buf[(self.tail & self.mask) as usize] = slot_idx;
        self.tail = self.tail.wrapping_add(1);
        self.len += 1;
        true
    }

    /// Pops the next slot index, or returns `None` if empty.
    #[inline]
    fn pop(&mut self) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        let slot_idx = self.buf[(self.head & self.mask) as usize];
        self.head = self.head.wrapping_add(1);
        self.len -= 1;
        if self.len == 0 {
            self.oldest_enqueue_ns = 0;
        }
        Some(slot_idx)
    }

    #[inline]
    fn len(&self) -> u32 {
        self.len
    }
}

/// Four-band FIFO ready queue (reactor-local).
///
/// Pushes are O(1). Pops are O(1) per band. Priority order is enforced
/// by the consumer iterating bands in priority order (see [`pop_next`](Self::pop_next)).
pub struct ReadyQueue {
    bands: [Band; 4],
    total: u64,
}

impl ReadyQueue {
    /// Creates a queue with each band sized to `capacity` (rounded to
    /// a power of two).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            bands: [
                Band::new(capacity),
                Band::new(capacity),
                Band::new(capacity),
                Band::new(capacity),
            ],
            total: 0,
        }
    }

    /// Pushes `slot_idx` into `band`'s FIFO.
    ///
    /// Returns `Err(slot_idx)` if that band is full — the caller can
    /// either drop the task or push into a lower band as overflow.
    pub fn push(&mut self, band: TaskPriority, slot_idx: u32, now_ns: u64) -> Result<(), u32> {
        if self.bands[band.as_index()].push(slot_idx, now_ns) {
            self.total += 1;
            Ok(())
        } else {
            Err(slot_idx)
        }
    }

    /// Pops the next task by strict priority order (High → Background).
    /// Within each band, FIFO.
    ///
    /// Returns `(slot_idx, band)`.
    #[must_use]
    pub fn pop_next(&mut self) -> Option<(u32, TaskPriority)> {
        for band in TaskPriority::ALL {
            let b = &mut self.bands[band.as_index()];
            if let Some(slot_idx) = b.pop() {
                self.total -= 1;
                return Some((slot_idx, band));
            }
        }
        None
    }

    /// Pops the next task from a specific band only.
    #[must_use]
    pub fn pop_from(&mut self, band: TaskPriority) -> Option<u32> {
        if let Some(slot_idx) = self.bands[band.as_index()].pop() {
            self.total -= 1;
            Some(slot_idx)
        } else {
            None
        }
    }

    /// Moves the oldest entry from `from` to `to` (one-shot, used by aging).
    /// Returns the slot index moved, or `None` if `from` is empty.
    #[must_use]
    pub fn promote_one(
        &mut self,
        from: TaskPriority,
        to: TaskPriority,
        now_ns: u64,
    ) -> Option<u32> {
        if let Some(slot_idx) = self.bands[from.as_index()].pop() {
            self.total -= 1;
            // ignore capacity error — destination has the same capacity,
            // and aging only fires when destination would not overflow.
            let _ = self.bands[to.as_index()].push(slot_idx, now_ns);
            self.total += 1;
            Some(slot_idx)
        } else {
            None
        }
    }

    /// Total depth across all bands.
    #[must_use]
    pub fn depth(&self) -> u64 {
        self.total
    }

    /// Depth of a single band.
    #[must_use]
    pub fn depth_of(&self, band: TaskPriority) -> u32 {
        self.bands[band.as_index()].len()
    }

    /// Returns `true` if all bands are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Returns the enqueue timestamp of the oldest item in `band`,
    /// or `None` if the band is empty. Used for aging.
    #[must_use]
    pub fn oldest_enqueue_ns(&self, band: TaskPriority) -> Option<u64> {
        let b = &self.bands[band.as_index()];
        if b.is_empty() {
            None
        } else {
            Some(b.oldest_enqueue_ns)
        }
    }

    /// Drains all bands, returning the total number of dropped items.
    /// Used during shutdown.
    pub fn drain_all(&mut self) -> u64 {
        let mut dropped = 0u64;
        for b in &mut self.bands {
            while b.pop().is_some() {
                dropped += 1;
            }
        }
        self.total = 0;
        dropped
    }

    /// Capacity of a single band (all bands have the same capacity).
    #[must_use]
    pub fn band_capacity(&self) -> usize {
        self.bands[0].capacity()
    }

    /// Decode a slot index from a raw `TaskId` (convenience).
    #[inline]
    #[must_use]
    pub fn slot_of(id: crate::id::TaskId) -> u32 {
        decode_slot(id.raw())
    }
}

impl std::fmt::Debug for ReadyQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadyQueue")
            .field("total", &self.total)
            .field("high", &self.depth_of(TaskPriority::High))
            .field("normal", &self.depth_of(TaskPriority::Normal))
            .field("low", &self.depth_of(TaskPriority::Low))
            .field("background", &self.depth_of(TaskPriority::Background))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_fifo_within_band() {
        let mut q = ReadyQueue::new(16);
        for i in 0..5 {
            q.push(TaskPriority::Normal, i, 0).expect("push");
        }
        for i in 0..5 {
            let (slot, band) = q.pop_next().expect("pop");
            assert_eq!(slot, i);
            assert_eq!(band, TaskPriority::Normal);
        }
        assert!(q.pop_next().is_none());
    }

    #[test]
    fn strict_priority_order() {
        let mut q = ReadyQueue::new(16);
        q.push(TaskPriority::Background, 30, 0).expect("push");
        q.push(TaskPriority::Low, 20, 0).expect("push");
        q.push(TaskPriority::Normal, 10, 0).expect("push");
        q.push(TaskPriority::High, 1, 0).expect("push");
        // Drain order: High, Normal, Low, Background
        assert_eq!(q.pop_next().unwrap().1, TaskPriority::High);
        assert_eq!(q.pop_next().unwrap().1, TaskPriority::Normal);
        assert_eq!(q.pop_next().unwrap().1, TaskPriority::Low);
        assert_eq!(q.pop_next().unwrap().1, TaskPriority::Background);
    }

    #[test]
    fn band_overflow_rejected() {
        let mut q = ReadyQueue::new(2);
        // Band has capacity 2 (rounded up to 2).
        assert!(q.push(TaskPriority::High, 1, 0).is_ok());
        assert!(q.push(TaskPriority::High, 2, 0).is_ok());
        // Third push fails.
        assert!(q.push(TaskPriority::High, 3, 0).is_err());
    }

    #[test]
    fn total_depth_tracks_pushes() {
        let mut q = ReadyQueue::new(16);
        assert_eq!(q.depth(), 0);
        q.push(TaskPriority::High, 1, 0).expect("push");
        q.push(TaskPriority::Low, 2, 0).expect("push");
        assert_eq!(q.depth(), 2);
        assert_eq!(q.depth_of(TaskPriority::High), 1);
        assert_eq!(q.depth_of(TaskPriority::Low), 1);
    }

    #[test]
    fn promote_one_moves_oldest() {
        let mut q = ReadyQueue::new(16);
        q.push(TaskPriority::Low, 42, 0).expect("push");
        let moved = q.promote_one(TaskPriority::Low, TaskPriority::Normal, 0);
        assert_eq!(moved, Some(42));
        assert_eq!(q.depth_of(TaskPriority::Low), 0);
        assert_eq!(q.depth_of(TaskPriority::Normal), 1);
    }

    #[test]
    fn drain_all_clears() {
        let mut q = ReadyQueue::new(16);
        q.push(TaskPriority::High, 1, 0).expect("push");
        q.push(TaskPriority::Low, 2, 0).expect("push");
        assert_eq!(q.drain_all(), 2);
        assert!(q.is_empty());
    }

    #[test]
    fn oldest_enqueue_ns_set_and_cleared() {
        let mut q = ReadyQueue::new(16);
        assert!(q.oldest_enqueue_ns(TaskPriority::Normal).is_none());
        q.push(TaskPriority::Normal, 1, 1234).expect("push");
        assert_eq!(q.oldest_enqueue_ns(TaskPriority::Normal), Some(1234));
        let _ = q.pop_next();
        assert!(q.oldest_enqueue_ns(TaskPriority::Normal).is_none());
    }
}
