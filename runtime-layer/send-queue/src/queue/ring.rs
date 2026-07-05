//! Bounded single-producer single-consumer ring buffer (Layer 3).
//!
//! [`IngressRing`] stores [`PacketSlot`]s in a fixed-size, power-of-two
//! array. Exactly one producer thread calls [`push`](IngressRing::push) and
//! exactly one consumer thread calls [`pop`](IngressRing::pop). All
//! synchronisation is via acquire/release ordering on the `head` and `tail`
//! atomic indices — no locks, no allocations on the hot path.
//!
//! # Memory ordering
//!
//! ```text
//! Producer                          Consumer
//! ────────                          ────────
//! write slot[tail]                  head_acq = head.load(Acquire)
//! tail.store(tail+1, Release)       ─── slot[tail-1] visible ───
//!                                   read slot[head_acq]
//!                                   head.store(head+1, Release)
//! ```

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::SendQueueError;
use crate::queue::slot::PacketSlot;

/// Cache-line padded wrapper to prevent false sharing between `head` and `tail`.
#[repr(C, align(64))]
struct CachePadded<T>(T);

/// Bounded SPSC ring of [`PacketSlot`]s.
///
/// Created with a power-of-two `capacity`. The producer fills slots at the
/// `tail` index; the consumer drains slots at the `head` index. When
/// `tail - head == capacity`, the ring is full and [`push`](Self::push)
/// returns [`QueueFull`](SendQueueError::QueueFull).
///
/// # Safety
///
/// The caller must ensure that **at most one thread** calls `push` and
/// **at most one (other) thread** calls `pop`. The type is `Send + Sync`
/// because `&self` methods use `UnsafeCell` with disciplined access, but
/// violating the SPSC contract is undefined behaviour.
pub struct IngressRing {
    slots: Box<[UnsafeCell<Option<PacketSlot>>]>,
    mask: u64,
    capacity: usize,
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
}

// SAFETY: See the type-level safety comment. The ring is sound when used
// as single-producer / single-consumer.
unsafe impl Send for IngressRing {}
unsafe impl Sync for IngressRing {}

impl IngressRing {
    /// Creates a ring with `capacity` slots.
    ///
    /// `capacity` must be a power of two and within
    /// `[MIN_CAPACITY, MAX_CAPACITY]`.
    pub fn new(capacity: usize) -> Result<Self, SendQueueError> {
        if !capacity.is_power_of_two() {
            return Err(SendQueueError::InvalidConfig(format!(
                "capacity must be a power of two (is {capacity})"
            )));
        }
        let slots = (0..capacity)
            .map(|_| UnsafeCell::new(None))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            slots,
            mask: (capacity - 1) as u64,
            capacity,
            head: CachePadded(AtomicU64::new(0)),
            tail: CachePadded(AtomicU64::new(0)),
        })
    }

    /// Returns the ring capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Approximate depth (relaxed loads). Useful for observability.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Relaxed);
        tail.wrapping_sub(head) as usize
    }

    /// Whether the ring is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the ring is full.
    #[inline]
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Attempts to push `slot` into the ring.
    ///
    /// Returns [`QueueFull`](SendQueueError::QueueFull) when the ring is at
    /// capacity.
    ///
    /// # Safety contract
    ///
    /// Must only be called from one thread (the producer). The consumer
    /// must not call `pop` until `tail` has been published with `Release`.
    pub fn push(&self, slot: PacketSlot) -> Result<(), SendQueueError> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        if tail.wrapping_sub(head) >= self.capacity as u64 {
            return Err(SendQueueError::QueueFull {
                capacity: self.capacity,
            });
        }
        let idx = (tail & self.mask) as usize;
        // SAFETY: Only the producer writes to slots[tail & mask]. The
        // consumer will not read this position until we store tail+1 with
        // Release ordering below.
        unsafe {
            *self.slots[idx].get() = Some(slot);
        }
        self.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pops the next slot in FIFO order.
    ///
    /// Returns `None` when the ring is empty.
    ///
    /// # Safety contract
    ///
    /// Must only be called from one thread (the consumer).
    #[must_use]
    pub fn pop(&self) -> Option<PacketSlot> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = (head & self.mask) as usize;
        // SAFETY: Only the consumer reads/takes from slots[head & mask].
        // The producer filled this position before publishing tail with
        // Release (which we acquired above).
        let slot = unsafe { (*self.slots[idx].get()).take() };
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        slot
    }
}

impl std::fmt::Debug for IngressRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngressRing")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_slot(id: u16) -> PacketSlot {
        let mut s = PacketSlot::empty();
        s.fill(
            id as u64, // tracking_id
            id,        // buf_index
            64,        // len
            "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            0,         // priority
            0,         // deadline_ns
            id as u64, // stream_id
        );
        s
    }

    #[test]
    fn new_creates_empty_ring() {
        let ring = IngressRing::new(64).expect("create");
        assert_eq!(ring.capacity(), 64);
        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn rejects_non_power_of_two() {
        let err = IngressRing::new(100).unwrap_err();
        assert!(matches!(err, SendQueueError::InvalidConfig(_)));
    }

    #[test]
    fn push_then_pop_round_trips() {
        let ring = IngressRing::new(16).expect("create");
        ring.push(make_slot(1)).expect("push");
        assert_eq!(ring.len(), 1);
        let popped = ring.pop().expect("pop");
        assert_eq!(popped.stream_id, 1);
        assert!(ring.is_empty());
    }

    #[test]
    fn pop_on_empty_returns_none() {
        let ring = IngressRing::new(16).expect("create");
        assert!(ring.pop().is_none());
    }

    #[test]
    fn push_until_full_returns_queue_full() {
        let ring = IngressRing::new(16).expect("create");
        for i in 0..16 {
            ring.push(make_slot(i as u16)).expect("push");
        }
        assert!(ring.is_full());
        let err = ring.push(make_slot(99)).unwrap_err();
        assert!(matches!(err, SendQueueError::QueueFull { capacity: 16 }));
    }

    #[test]
    fn fifo_order_preserved() {
        let ring = IngressRing::new(64).expect("create");
        for i in 0..32 {
            ring.push(make_slot(i as u16)).expect("push");
        }
        for i in 0..32 {
            let slot = ring.pop().expect("pop");
            assert_eq!(slot.stream_id, i as u64, "FIFO order violated at {i}");
        }
    }

    #[test]
    fn wrap_around_works() {
        let ring = IngressRing::new(4).expect("create");
        // Fill and drain multiple times to exercise wrap-around.
        for cycle in 0..10 {
            for i in 0..4 {
                let mut s = make_slot((cycle * 4 + i) as u16);
                s.stream_id = (cycle * 4 + i) as u64;
                ring.push(s).expect("push");
            }
            for i in 0..4 {
                let slot = ring.pop().expect("pop");
                assert_eq!(slot.stream_id, (cycle * 4 + i) as u64);
            }
            assert!(ring.is_empty());
        }
    }

    #[test]
    fn interleaved_push_pop() {
        let ring = IngressRing::new(8).expect("create");
        ring.push(make_slot(1)).expect("push");
        ring.push(make_slot(2)).expect("push");
        let s = ring.pop().expect("pop");
        assert_eq!(s.stream_id, 1);
        ring.push(make_slot(3)).expect("push");
        let s = ring.pop().expect("pop");
        assert_eq!(s.stream_id, 2);
        let s = ring.pop().expect("pop");
        assert_eq!(s.stream_id, 3);
        assert!(ring.is_empty());
    }

    #[test]
    fn cross_thread_spsc() {
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(IngressRing::new(256).expect("create"));
        let producer = Arc::clone(&ring);
        let consumer = Arc::clone(&ring);

        let n: u64 = 1000;
        let h1 = thread::spawn(move || {
            for i in 0..n {
                let mut s = PacketSlot::empty();
                s.fill(i, 0, 4, "127.0.0.1:1".parse().unwrap(), 0, 0, i);
                loop {
                    match producer.push(s.clone()) {
                        Ok(()) => break,
                        Err(SendQueueError::QueueFull { .. }) => continue,
                        Err(e) => panic!("push error: {e:?}"),
                    }
                }
            }
        });

        let h2 = thread::spawn(move || {
            let mut next: u64 = 0;
            while next < n {
                if let Some(slot) = consumer.pop() {
                    assert_eq!(slot.stream_id, next, "order violation at {next}");
                    next += 1;
                }
            }
        });

        h1.join().expect("producer");
        h2.join().expect("consumer");
    }
}
