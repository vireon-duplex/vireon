//! Deferred-execution adapter (Layer — Timer Wheel integration).
//!
//! Phase 1: the Task Queue does not link against the Timer Wheel crate
//! (keep dependencies minimal). The full Timer Wheel integration is a
//! thin adapter implemented here as a placeholder that explains the
//! intended wiring; concrete integration happens at the reactor layer
//! when both modules are linked.
//!
//! ## Intended wiring
//!
//! ```text
//! task_queue.schedule_after(task, delay, priority)
//!   ├── pool.alloc(task, priority, metadata)
//!   └── timer_wheel.start(task_id=slot_idx, duration=delay)
//!         │
//!         │  [delay elapses]
//!         ▼
//! timer_wheel.drain_expired() → Vec<TaskId>
//!   │
//!   ▼
//! reactor: for each fired id:
//!   └── task_queue.on_timer_fire(id) → ready.push(band)
//! ```

use std::time::Duration;

/// Type alias for a timer-wheel task id (the slot index).
pub type DeferredId = u64;

/// A pending deferred task recorded by the Task Queue so it can
/// re-enqueue the slot when the timer fires.
///
/// In the full runtime integration this struct is replaced by an
/// actual `TimerHandle` from the `timer-wheel` crate.
#[derive(Clone, Copy, Debug)]
pub struct DeferredHandle {
    /// Slot index in the TaskPool.
    pub slot_idx: u32,
    /// Generation when the deferred task was registered.
    pub generation: u16,
    /// Original priority band — used when the task is moved back to
    /// the ReadyQueue on fire.
    pub priority: crate::priority::TaskPriority,
    /// Requested delay (informational).
    pub delay: Duration,
}

impl DeferredHandle {
    /// Creates a new deferred handle.
    #[must_use]
    pub const fn new(
        slot_idx: u32,
        generation: u16,
        priority: crate::priority::TaskPriority,
        delay: Duration,
    ) -> Self {
        Self {
            slot_idx,
            generation,
            priority,
            delay,
        }
    }
}

/// Stub deferred registry. The real implementation lives in the
/// Timer Wheel; this exists so the Task Queue's `schedule_after`
/// API surface compiles standalone and tests can verify the call
/// path records the deferred task.
#[derive(Default)]
pub struct DeferredRegistry {
    entries: Vec<DeferredHandle>,
}

impl DeferredRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Records a deferred task. In the full runtime integration this
    /// would call `timer_wheel.start()`.
    pub fn register(&mut self, handle: DeferredHandle) {
        self.entries.push(handle);
    }

    /// Drains and returns all registered deferred tasks. The reactor
    /// calls this when wiring up the real Timer Wheel.
    pub fn drain(&mut self) -> Vec<DeferredHandle> {
        std::mem::take(&mut self.entries)
    }

    /// Number of pending deferred tasks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if no deferred tasks are pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl std::fmt::Debug for DeferredRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredRegistry")
            .field("pending", &self.entries.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::priority::TaskPriority;

    #[test]
    fn register_and_drain() {
        let mut reg = DeferredRegistry::new();
        assert!(reg.is_empty());
        reg.register(DeferredHandle::new(
            1,
            0,
            TaskPriority::Normal,
            Duration::from_millis(10),
        ));
        reg.register(DeferredHandle::new(
            2,
            0,
            TaskPriority::High,
            Duration::from_millis(5),
        ));
        assert_eq!(reg.len(), 2);
        let drained = reg.drain();
        assert_eq!(drained.len(), 2);
        assert!(reg.is_empty());
    }
}
