//! Task handle / cancellation token (Layer 8).
//!
//! A [`TaskHandle`] is a cheap `Copy` token returned by
//! [`TaskQueue::spawn`](crate::queue::TaskQueue::spawn). It carries
//! the [`TaskId`] and enough information to request cancellation.
//!
//! The handle does **not** keep the task alive — dropping it is a
//! no-op. Cancellation is best-effort and lazy: setting the bit is
//! O(1); the executor checks the bit before running and skips
//! cancelled tasks on dequeue.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::id::TaskId;

/// Cancellation handle for a spawned task.
///
/// Cheap to copy (two words). The `cancel()` method sets a shared
/// atomic flag that the reactor's executor checks before running the
/// task. Cancellation is cooperative — the task is skipped on next
/// dequeue, not preempted mid-execution.
#[derive(Clone)]
pub struct TaskHandle {
    id: TaskId,
    /// Shared with the TaskQueue's CancellationRegistry. Setting this
    /// to `true` requests cancellation; the executor polls it before
    /// running the task.
    cancel_flag: CancelFlag,
}

/// Internal shared cancellation flag.
///
/// Wrapping in `Arc` allows the handle to outlive the queue's borrow.
/// The cost is one allocation per task; for cancellation-heavy
/// workloads a bitmap-based path can be added later (Phase 2).
pub(crate) type CancelFlag = std::sync::Arc<AtomicBool>;

impl TaskHandle {
    /// Creates a new handle bound to a shared cancel flag.
    #[inline]
    pub(crate) fn new(id: TaskId, cancel_flag: CancelFlag) -> Self {
        Self { id, cancel_flag }
    }

    /// Returns the task's identifier.
    #[inline]
    #[must_use]
    pub const fn id(&self) -> TaskId {
        self.id
    }

    /// Requests cancellation. Best-effort — the executor will skip the
    /// task on next dequeue if it has not started yet.
    ///
    /// Calling `cancel` after the task has started executing is a
    /// no-op (the task runs to completion).
    #[inline]
    pub fn cancel(&self) {
        self.cancel_flag.store(true, Ordering::Release);
    }

    /// Returns `true` if cancellation has been requested.
    #[inline]
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel_flag.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("id", &self.id)
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

// Manual Copy is not possible because of `Arc`, but Clone is cheap
// (one Arc bump). Provide a clear doc on Clone.
impl std::fmt::Display for TaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskHandle({:#x})", self.id.raw())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::make_id;

    fn fresh_flag() -> CancelFlag {
        std::sync::Arc::new(AtomicBool::new(false))
    }

    #[test]
    fn cancel_sets_flag() {
        let h = TaskHandle::new(make_id(1, 0), fresh_flag());
        assert!(!h.is_cancelled());
        h.cancel();
        assert!(h.is_cancelled());
    }

    #[test]
    fn clone_shares_flag() {
        let h = TaskHandle::new(make_id(1, 0), fresh_flag());
        let h2 = h.clone();
        h.cancel();
        assert!(h2.is_cancelled()); // shared state
    }

    #[test]
    fn id_accessor() {
        let id = make_id(42, 7);
        let h = TaskHandle::new(id, fresh_flag());
        assert_eq!(h.id(), id);
    }

    #[test]
    fn display_format() {
        let h = TaskHandle::new(make_id(42, 0), fresh_flag());
        let s = format!("{h}");
        assert!(s.starts_with("TaskHandle("));
    }
}
