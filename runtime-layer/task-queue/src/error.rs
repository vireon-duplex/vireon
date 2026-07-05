//! Error types for the Task Queue (Layer 1).
//!
//! All public fallible operations return [`TaskError`]. The enum is
//! `Clone`, `Copy` is not possible (one variant carries a string), so
//! cheap-clone `Arc<str>` would be used if the error were cloned often —
//! in practice it is matched and dropped immediately.

use thiserror::Error;

/// Result alias for Task Queue operations.
pub type TaskResultStd<T> = Result<T, TaskError>;

/// Errors returned by Task Queue operations.
#[derive(Debug, Error)]
pub enum TaskError {
    /// The task pool is exhausted — no free slots are available.
    ///
    /// The caller should back off or drop work. The `capacity` field
    /// reports the configured pool size for diagnostics.
    #[error("task pool exhausted (capacity {capacity})")]
    PoolExhausted { capacity: usize },

    /// The queue is under backpressure and rejected a low-priority task.
    ///
    /// Only `Low` and `Background` tasks can be rejected this way;
    /// `High` and `Normal` are always admitted up to the hard pool cap.
    #[error("task rejected under backpressure (priority {priority:?})")]
    Rejected {
        priority: crate::priority::TaskPriority,
    },

    /// The MPSC inbox is full — too many cross-thread submissions are
    /// pending. The producer should retry or route differently.
    #[error("mpsc inbox full (capacity {capacity})")]
    InboxFull { capacity: usize },

    /// The queue has been shut down and no longer accepts new tasks.
    #[error("task queue is shutting down")]
    ShuttingDown,

    /// Configuration validation failed.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// A deferred-task registration was rejected by the Timer Wheel.
    ///
    /// The Task Queue does not implement its own timers; all deferred
    /// work is delegated. If the Timer Wheel rejects (e.g. full), the
    /// original `spawn_after` call surfaces the error to the caller.
    #[error("deferred scheduling failed: {0}")]
    DeferredFailed(&'static str),

    /// A task ran to completion but returned an error result.
    ///
    /// This is used internally by the executor to count failures; it is
    /// not returned from public APIs.
    #[error("task error: {0}")]
    TaskFailed(String),
}

impl TaskError {
    /// Returns `true` if this error indicates a transient condition
    /// that the caller may retry.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::PoolExhausted { .. } | Self::InboxFull { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::priority::TaskPriority;

    #[test]
    fn display_pool_exhausted() {
        let e = TaskError::PoolExhausted { capacity: 4096 };
        assert_eq!(e.to_string(), "task pool exhausted (capacity 4096)");
    }

    #[test]
    fn display_rejected() {
        let e = TaskError::Rejected {
            priority: TaskPriority::Low,
        };
        assert!(e.to_string().contains("Low"));
    }

    #[test]
    fn is_retryable_classification() {
        assert!(TaskError::PoolExhausted { capacity: 1 }.is_retryable());
        assert!(TaskError::InboxFull { capacity: 1 }.is_retryable());
        assert!(!TaskError::ShuttingDown.is_retryable());
        assert!(!TaskError::InvalidConfig("x".into()).is_retryable());
    }
}
