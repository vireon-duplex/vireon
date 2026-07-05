//! MpscInbox — bounded multi-producer single-consumer queue (Layer 11).
//!
//! Cross-thread task submissions go through this inbox. The inbox is
//! drained once per reactor tick (by the owning reactor thread) and
//! its contents moved into the [`ReadyQueue`](crate::ready::ReadyQueue).
//!
//! ## Implementation
//!
//! Backed by [`crossbeam_queue::ArrayQueue`], a lock-free bounded MPMC
//! ring. The Task Queue is single-consumer, but ArrayQueue's MPMC
//! semantics are a strict superset and the implementation is
//! well-audited. We wrap it to (a) hide the dependency behind a
//! domain-named type, and (b) carry the [`InboxEntry`] type plus
//! domain error mapping.

use crossbeam_queue::ArrayQueue;

use crate::error::TaskError;
use crate::priority::TaskPriority;

/// One inbox entry: a boxed task plus the priority at which to enqueue.
pub(crate) struct InboxEntry {
    pub task: Box<dyn crate::task::Task>,
    pub priority: TaskPriority,
    pub spawned_at_ns: u64,
}

/// Bounded MPSC inbox for cross-thread task delivery.
pub struct MpscInbox {
    q: ArrayQueue<InboxEntry>,
}

impl MpscInbox {
    /// Creates an inbox with `capacity` slots.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            q: ArrayQueue::new(capacity.max(1)),
        }
    }

    /// Capacity of the inbox.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.q.capacity()
    }

    /// Current number of pending entries. Approximate.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.q.len()
    }

    /// Pushes an entry into the inbox (multi-producer safe).
    ///
    /// Returns [`TaskError::InboxFull`] if the ring is full.
    pub(crate) fn push(&self, entry: InboxEntry) -> Result<(), TaskError> {
        self.q.push(entry).map_err(|_| TaskError::InboxFull {
            capacity: self.q.capacity(),
        })
    }

    /// Drains all pending entries and returns them in submission order.
    /// Consumer-only (reactor thread). Allocation is one `Vec` of size
    /// `pending`; the boxed entries themselves are moved, not cloned.
    pub(crate) fn drain(&mut self) -> Vec<InboxEntry> {
        let n = self.q.len();
        let mut out = Vec::with_capacity(n);
        while let Some(entry) = self.q.pop() {
            out.push(entry);
        }
        out
    }
}

impl std::fmt::Debug for MpscInbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MpscInbox")
            .field("capacity", &self.capacity())
            .field("pending", &self.pending())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::TaskContext;
    use crate::task::{Task, TaskResult};

    #[allow(dead_code)]
    struct Marker(u32);
    impl Task for Marker {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }

    fn entry(mark: u32, prio: TaskPriority) -> InboxEntry {
        InboxEntry {
            task: Box::new(Marker(mark)),
            priority: prio,
            spawned_at_ns: 0,
        }
    }

    #[test]
    fn push_drain_round_trip() {
        let mut inbox = MpscInbox::new(16);
        inbox.push(entry(1, TaskPriority::Normal)).expect("push");
        inbox.push(entry(2, TaskPriority::High)).expect("push");
        let drained = inbox.drain();
        assert_eq!(drained.len(), 2);
    }

    #[test]
    fn push_full_returns_err() {
        let inbox = MpscInbox::new(2);
        inbox.push(entry(1, TaskPriority::Normal)).expect("push");
        inbox.push(entry(2, TaskPriority::Normal)).expect("push");
        let err = inbox.push(entry(3, TaskPriority::Normal)).unwrap_err();
        assert!(matches!(err, TaskError::InboxFull { capacity: 2 }));
    }

    #[test]
    fn drain_after_empty_is_noop() {
        let mut inbox = MpscInbox::new(4);
        let drained = inbox.drain();
        assert!(drained.is_empty());
    }

    #[test]
    fn drain_advances_tail() {
        let inbox = MpscInbox::new(4);
        for i in 0..3 {
            inbox.push(entry(i, TaskPriority::Normal)).expect("push");
        }
        let mut inbox = inbox;
        let _ = inbox.drain();
        assert_eq!(inbox.pending(), 0);
        inbox.push(entry(99, TaskPriority::Normal)).expect("push");
        assert_eq!(inbox.pending(), 1);
    }

    #[test]
    fn pending_reflects_pushes() {
        let inbox = MpscInbox::new(8);
        inbox.push(entry(1, TaskPriority::Normal)).expect("push");
        inbox.push(entry(2, TaskPriority::Normal)).expect("push");
        assert_eq!(inbox.pending(), 2);
    }
}
