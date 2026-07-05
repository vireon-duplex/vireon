//! Task trait, TaskResult, TaskKind (Layer 7).

use std::time::Duration;

use crate::context::TaskContext;
use crate::error::TaskError;

/// The unit of deferred work.
///
/// Implementations store their state inline within the
/// [`TaskSlot`](crate::pool) data region. The `run` method receives a
/// mutable [`TaskContext`] providing access to runtime services.
///
/// `Task::run` **must be non-blocking** and should complete in under
/// 50 µs. Long-running work must be split into sub-tasks or yield via
/// [`TaskResult::Reschedule`].
pub trait Task: Send {
    /// Executes the task.
    ///
    /// Returning [`TaskResult::Reschedule`] re-enqueues the task for a
    /// future tick (immediate or delayed).
    fn run(&mut self, ctx: &mut TaskContext) -> TaskResult;

    /// Optional kind tag for metrics bucketing. The default is
    /// [`TaskKind::Generic`].
    fn kind(&self) -> TaskKind {
        TaskKind::Generic
    }
}

/// Result of running a task.
#[derive(Debug)]
pub enum TaskResult {
    /// Task completed successfully. The slot is recycled.
    Done,
    /// Task encountered an error. The slot is recycled; the error is
    /// counted in metrics. The task is **not** retried automatically.
    Error(TaskError),
    /// Task requests re-scheduling after `delay` at the given priority.
    ///
    /// - `delay == Duration::ZERO`: re-enqueue on the next tick.
    /// - `delay != Duration::ZERO`: register with the Timer Wheel.
    ///
    /// The slot is recycled for the immediate case; for the deferred
    /// case the slot stays alive (registered with the Timer Wheel).
    Reschedule {
        delay: Duration,
        priority: crate::priority::TaskPriority,
    },
}

impl TaskResult {
    /// Returns `true` if the task is fully done (no reschedule).
    #[inline]
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Error(..))
    }

    /// Returns `true` if the task requested rescheduling.
    #[inline]
    #[must_use]
    pub fn is_reschedule(&self) -> bool {
        matches!(self, Self::Reschedule { .. })
    }

    /// Convenience: returns a successful result.
    #[inline]
    #[must_use]
    pub const fn done() -> Self {
        Self::Done
    }

    /// Convenience: returns a re-enqueue-next-tick result.
    #[inline]
    #[must_use]
    pub fn yield_next(priority: crate::priority::TaskPriority) -> Self {
        Self::Reschedule {
            delay: Duration::ZERO,
            priority,
        }
    }
}

impl Default for TaskResult {
    fn default() -> Self {
        Self::Done
    }
}

/// Coarse task classification used for metrics bucketing.
///
/// Tasks report a `TaskKind` from [`Task::kind`] so the executor can
/// break down execution latency and counts by category without
/// inspecting the task's internals.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TaskKind {
    /// Default — uncategorised callback.
    #[default]
    Generic = 0,
    /// IO-completion-driven callback (e.g. CQE handler).
    IoCompletion = 1,
    /// Timer-fire callback (e.g. timeout, retry).
    TimerFire = 2,
    /// Application-initiated work.
    Application = 3,
    /// Stream / connection state transition.
    StreamEvent = 4,
    /// Periodic maintenance (cleanup, stats flush).
    Maintenance = 5,
    /// Shutdown drain or final cleanup.
    Shutdown = 6,
}

impl TaskKind {
    /// Returns a short static name for logging.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::IoCompletion => "io_completion",
            Self::TimerFire => "timer_fire",
            Self::Application => "application",
            Self::StreamEvent => "stream_event",
            Self::Maintenance => "maintenance",
            Self::Shutdown => "shutdown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Noop;
    impl Task for Noop {
        fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }

    #[test]
    fn task_kind_default_is_generic() {
        assert_eq!(Noop.kind(), TaskKind::Generic);
    }

    #[test]
    fn result_terminal_vs_reschedule() {
        assert!(TaskResult::Done.is_terminal());
        assert!(TaskResult::Error(TaskError::ShuttingDown).is_terminal());
        assert!(!TaskResult::Done.is_reschedule());

        let r = TaskResult::yield_next(crate::priority::TaskPriority::Low);
        assert!(r.is_reschedule());
        assert!(!r.is_terminal());
    }

    #[test]
    fn kind_as_str() {
        assert_eq!(TaskKind::IoCompletion.as_str(), "io_completion");
        assert_eq!(TaskKind::Shutdown.as_str(), "shutdown");
    }
}
