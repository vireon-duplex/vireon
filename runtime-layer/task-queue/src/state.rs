//! Task state machine (Layer 5).
//!
//! See [`DESIGN.md`] §15 for the full state-transition diagram.

use std::fmt;

/// Lifecycle state of a tracked task.
///
/// Only `Idle`, `Queued`, `Deferred`, `Ready`, and `Executing` are
/// stored on a slot; the terminal states (`Completed`, `Failed`,
/// `Cancelled`, `Expired`) are transient and immediately transition
/// to slot recycling.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TaskState {
    /// Slot is in the free-list. No task is associated.
    #[default]
    Idle = 0,
    /// Task is in the [`ReadyQueue`](crate::ready::ReadyQueue), waiting
    /// for execution.
    Queued = 1,
    /// Task is registered with the Timer Wheel, waiting for fire.
    Deferred = 2,
    /// Timer fired; task moved back to the ReadyQueue.
    Ready = 3,
    /// Task's `run()` method is in progress.
    Executing = 4,
    /// Task finished successfully (transient → recycled).
    Completed = 5,
    /// Task returned an error (transient → recycled).
    Failed = 6,
    /// Task was cancelled before execution (transient → recycled).
    Cancelled = 7,
    /// Task's deadline passed before execution (transient → recycled).
    Expired = 8,
    /// Slot returned to the free-list (transient → `Idle`).
    Recycled = 9,
}

impl TaskState {
    /// Returns `true` if the task is currently stored in a queue
    /// (`Queued`, `Ready`) or external wait (`Deferred`).
    #[inline]
    #[must_use]
    pub const fn is_pending(self) -> bool {
        matches!(self, Self::Queued | Self::Deferred | Self::Ready)
    }

    /// Returns `true` if the task is being or has been worked on
    /// (`Executing` or any terminal state).
    #[inline]
    #[must_use]
    pub const fn is_terminal_or_executing(self) -> bool {
        matches!(
            self,
            Self::Executing
                | Self::Completed
                | Self::Failed
                | Self::Cancelled
                | Self::Expired
                | Self::Recycled
        )
    }

    /// Returns `true` if this is a final state after which the slot
    /// can be recycled.
    #[inline]
    #[must_use]
    pub const fn is_recyclable(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Expired | Self::Recycled
        )
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Idle => "idle",
            Self::Queued => "queued",
            Self::Deferred => "deferred",
            Self::Ready => "ready",
            Self::Executing => "executing",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::Recycled => "recycled",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_idle() {
        assert_eq!(TaskState::default(), TaskState::Idle);
    }

    #[test]
    fn pending_classification() {
        assert!(TaskState::Queued.is_pending());
        assert!(TaskState::Deferred.is_pending());
        assert!(TaskState::Ready.is_pending());
        assert!(!TaskState::Idle.is_pending());
        assert!(!TaskState::Executing.is_pending());
        assert!(!TaskState::Completed.is_pending());
    }

    #[test]
    fn recyclable_classification() {
        assert!(TaskState::Completed.is_recyclable());
        assert!(TaskState::Failed.is_recyclable());
        assert!(TaskState::Cancelled.is_recyclable());
        assert!(TaskState::Expired.is_recyclable());
        assert!(!TaskState::Queued.is_recyclable());
        assert!(!TaskState::Executing.is_recyclable());
    }

    #[test]
    fn display() {
        assert_eq!(TaskState::Idle.to_string(), "idle");
        assert_eq!(TaskState::Executing.to_string(), "executing");
    }
}
