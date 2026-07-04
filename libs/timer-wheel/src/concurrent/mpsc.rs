//! Bounded lock-free MPSC queue (Layer 6).
//!
//! Implementation of Dmitry Vyukov's bounded MPMC queue, used here in the
//! MPSC configuration (many application threads push `Start`/`Cancel`
//! commands; a single worker thread drains them).
//!
//! ## Algorithm
//!
//! Each cell carries a [`AtomicUsize`] sequence number. The producer claims a
//! cell at `enqueue_pos`, waits until its sequence signals "empty"
//! (`seq == enqueue_pos`), writes the payload, then publishes by storing
//! `seq = enqueue_pos + 1`. The consumer waits for `seq == dequeue_pos + 1`,
//! reads the payload, then stores `seq = dequeue_pos + capacity` to mark the
//! cell empty for the next lap. Because capacity is a power of two, the lap
//! math is exact.
//!
//! ## Memory ordering
//!
//! * `enqueue_pos` / `dequeue_pos`: `Relaxed` — they are advisory cursors;
//!   the sequence counter is the real handoff.
//! * Sequence `load`: `Acquire` — pairs with the other side's `Release` store
//!   so the payload is visible after the sequence is observed.
//! * Sequence `store`: `Release` — publishes the payload.
//!
//! ## Safety
//!
//! `T: Send` is required: ownership of a value moves from a producer thread to
//! the consumer thread through the cell. The single-producer-per-cell
//! invariant (at any moment, at most one producer has claimed a given cell) is
//! enforced by the `enqueue_pos` CAS, so each cell write is exclusive.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A cell in the queue ring buffer.
struct Cell<T> {
    sequence: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

/// Bounded, lock-free MPSC queue (Vyukov algorithm).
pub struct BoundedMpsc<T> {
    buffer: Box<[Cell<T>]>,
    capacity: usize,
    mask: usize,
    enqueue_pos: AtomicUsize,
    dequeue_pos: AtomicUsize,
}

// SAFETY: the queue hands ownership of `T` from a producer thread to the
// consumer thread. That transfer requires `T: Send`. Concurrent access through
// `&BoundedMpsc` is the intended usage (many producers, one consumer), so the
// type is `Sync` whenever `T: Send`.
unsafe impl<T: Send> Send for BoundedMpsc<T> {}
unsafe impl<T: Send> Sync for BoundedMpsc<T> {}

impl<T> BoundedMpsc<T> {
    /// Creates a queue with at least `min_capacity` slots (rounded up to the
    /// next power of two so the mask is exact).
    ///
    /// Returns `None` if `min_capacity` is zero or the rounded capacity would
    /// overflow `isize::MAX`.
    #[must_use]
    pub fn new(min_capacity: usize) -> Option<Self> {
        if min_capacity == 0 {
            return None;
        }
        let capacity = min_capacity.next_power_of_two();
        if capacity > (isize::MAX as usize) {
            return None;
        }
        let mut buffer: Vec<Cell<T>> = Vec::with_capacity(capacity);
        for i in 0..capacity {
            buffer.push(Cell {
                sequence: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        Some(Self {
            buffer: buffer.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(0),
        })
    }

    /// Actual capacity (power of two, `>=` the requested minimum).
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Attempts to enqueue `value`. Returns `Err(value)` if the queue is full.
    pub fn push(&self, value: T) -> Result<(), T> {
        let mut pos = self.enqueue_pos.load(Ordering::Relaxed);
        loop {
            let idx = pos & self.mask;
            let cell = unsafe { self.buffer.get_unchecked(idx) };
            let seq = cell.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos) as isize;
            if diff == 0 {
                // Cell is empty and belongs to this slot. Try to advance the
                // producer cursor; on success we own the cell exclusively.
                match self.enqueue_pos.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: we exclusively own this cell until we publish
                        // the new sequence; no other producer or the consumer
                        // can touch `data` in this window.
                        unsafe {
                            (*cell.data.get()).write(value);
                        }
                        cell.sequence.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                // Queue full.
                return Err(value);
            } else {
                // Another producer raced ahead past this cell; reload.
                pos = self.enqueue_pos.load(Ordering::Relaxed);
            }
        }
    }

    /// Dequeues one value, or `None` if the queue is empty.
    pub fn pop(&self) -> Option<T> {
        let mut pos = self.dequeue_pos.load(Ordering::Relaxed);
        loop {
            let idx = pos & self.mask;
            let cell = unsafe { self.buffer.get_unchecked(idx) };
            let seq = cell.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos.wrapping_add(1)) as isize;
            if diff == 0 {
                // Cell holds data and belongs to this consumer slot.
                match self.dequeue_pos.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: the producer published this value (Release on
                        // sequence) and we observed it via Acquire, so the
                        // payload is fully initialised and ours exclusively.
                        let value = unsafe { (*cell.data.get()).assume_init_read() };
                        cell.sequence
                            .store(pos.wrapping_add(self.capacity), Ordering::Release);
                        return Some(value);
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                // Queue empty.
                return None;
            } else {
                // Consumer cursor lagged; reload.
                pos = self.dequeue_pos.load(Ordering::Relaxed);
            }
        }
    }

    /// Drains every immediately-available value into `out`.
    pub fn drain_into(&self, out: &mut Vec<T>) {
        while let Some(v) = self.pop() {
            out.push(v);
        }
    }
}

impl<T> Drop for BoundedMpsc<T> {
    fn drop(&mut self) {
        // Drop any values still queued at shutdown. `pop` is sound here: drop
        // runs with `&mut self`, so there is no concurrent access.
        while self.pop().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn new_rejects_zero_capacity() {
        assert!(BoundedMpsc::<u64>::new(0).is_none());
    }

    #[test]
    fn capacity_rounds_up_to_power_of_two() {
        let q = BoundedMpsc::<u64>::new(1000).expect("some queue");
        assert_eq!(q.capacity(), 1024);
        let q = BoundedMpsc::<u64>::new(64).expect("some queue");
        assert_eq!(q.capacity(), 64);
    }

    #[test]
    fn push_then_pop_round_trip() {
        let q = BoundedMpsc::new(8).expect("some queue");
        q.push(11u64).expect("push ok");
        q.push(22u64).expect("push ok");
        assert_eq!(q.pop(), Some(11));
        assert_eq!(q.pop(), Some(22));
        assert!(q.pop().is_none());
    }

    #[test]
    fn full_queue_rejects_push() {
        let q = BoundedMpsc::new(2).expect("some queue");
        assert!(q.push(1u64).is_ok());
        assert!(q.push(2u64).is_ok());
        assert!(q.push(3u64).is_err());
        // drain and refill across laps
        assert_eq!(q.pop(), Some(1));
        assert!(q.push(4u64).is_ok());
    }

    #[test]
    fn fifo_order_preserved_across_many_laps() {
        let q = BoundedMpsc::new(4).expect("some queue");
        for lap in 0..100u64 {
            for i in 0..3u64 {
                q.push(lap * 100 + i).expect("push");
            }
            for i in 0..3u64 {
                assert_eq!(q.pop(), Some(lap * 100 + i));
            }
        }
    }

    #[test]
    fn drain_into_collects_all_pending() {
        let q = BoundedMpsc::new(8).expect("some queue");
        for v in 0..5u64 {
            q.push(v).expect("push");
        }
        let mut out = Vec::new();
        q.drain_into(&mut out);
        assert_eq!(out, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn drops_unconsumed_values_on_drop() {
        // Verify Drop does not leak for owned heap values.
        static COUNT: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct Counter;
        impl Drop for Counter {
            fn drop(&mut self) {
                COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        COUNT.store(0, Ordering::SeqCst);
        {
            let q = BoundedMpsc::new(8).expect("some queue");
            for _ in 0..5 {
                q.push(Counter).expect("push");
            }
            // leave 3 unconsumed
            let _ = q.pop();
            let _ = q.pop();
        }
        assert_eq!(COUNT.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn multi_producer_stress() {
        // Correctness gate: N producers each push M unique tokens; a single
        // consumer drains them all. Asserts no losses or duplicates.
        const PRODUCERS: usize = 8;
        const PER_PRODUCER: usize = 4096;
        let q = Arc::new(BoundedMpsc::new(256).expect("some queue"));
        let mut handles = Vec::new();
        for p in 0..PRODUCERS {
            let q = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    let token = (p * PER_PRODUCER + i) as u64 + 1; // 1-based, 0 reserved
                    while q.push(token).is_err() {
                        // back off briefly under backpressure
                        thread::yield_now();
                    }
                }
            }));
        }
        let total = PRODUCERS * PER_PRODUCER;
        let mut seen = vec![false; total + 1];
        let mut got = 0usize;
        while got < total {
            if let Some(v) = q.pop() {
                let idx = v as usize;
                assert!(!seen[idx], "duplicate token {v}");
                seen[idx] = true;
                got += 1;
            } else {
                thread::yield_now();
            }
        }
        for h in handles {
            h.join().expect("producer panicked");
        }
        assert_eq!(got, total);
        // every expected token observed exactly once
        assert!(seen[1..].iter().all(|&b| b));
    }
}
