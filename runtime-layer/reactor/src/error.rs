//! [`ReactorError`] — unified error surface for the reactor.
//!
//! Foreign errors from each owned subsystem are folded into
//! [`ReactorError`] via `#[from]`, so reactor code can use `?`
//! against any primitive. Fatal-vs-retryable classification drives
//! the shutdown coordinator (Phase 5): fatal errors take the
//! reactor straight to `Forced`; retryable errors are logged and
//! retried on the next tick.

use thiserror::Error;

/// Unified reactor error.
///
/// `#[non_exhaustive]` reserves the right to add new variants in
/// future phases without breaking match arms.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ReactorError {
    /// Configuration validation failed (e.g. ring_entries out of range).
    #[error("invalid reactor configuration: {0}")]
    InvalidConfig(String),

    /// `io-layer` ring / socket / completion error.
    #[error("io layer: {0}")]
    Io(#[from] io::IoError),

    /// Buffer pool could not satisfy an acquire / refill.
    #[error("buffer pool: {0}")]
    BufferPool(#[from] buffer_pool::BufferPoolError),

    /// Send queue rejected a submission (full / cancelled).
    #[error("send queue: {0}")]
    SendQueue(#[from] send_queue::SendQueueError),

    /// Timer wheel rejected a start / cancel.
    #[error("timer wheel: {0}")]
    TimerWheel(#[from] timer_wheel::TimerError),

    /// Task queue rejected a spawn (capacity / shutdown).
    #[error("task queue: {0}")]
    TaskQueue(#[from] task_queue::TaskError),

    /// Connection shard lookup / state transition failed.
    #[error("connection shard: {0}")]
    ShardError(#[from] connection_shard::ShardError),

    /// CQE processor dispatch / drain error.
    #[error("cqe processor: {0}")]
    CqeProcessor(#[from] cqe_processor::CqeError),

    /// Reactor has entered shutdown and refuses new work.
    #[error("reactor is shutting down")]
    Shutdown,

    /// Completion queue overflowed — kernel dropped CQEs.
    /// Recovery requires ring re-arming (Phase 14).
    #[error("completion queue overflow")]
    RingOverflow,

    /// `sched_setaffinity` or NUMA bind failed.
    #[error("cpu pinning failed: {0}")]
    PinFailed(String),
}

impl ReactorError {
    /// Fatal errors abort the reactor thread. The shutdown
    /// coordinator (Phase 5) propagates these to the supervisor.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidConfig(_) | Self::Shutdown | Self::RingOverflow | Self::PinFailed(_)
        )
    }

    /// Retryable errors are logged and dropped; the next tick
    /// re-attempts the operation. Includes transient foreign errors.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        !self.is_fatal()
    }

    /// Whether the reactor should silently drop the event (no log).
    /// Currently nothing is silent — reserved for future use.
    #[must_use]
    pub const fn is_silent_drop(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_fatal() {
        assert!(ReactorError::Shutdown.is_fatal());
        assert!(ReactorError::RingOverflow.is_fatal());
        assert!(ReactorError::PinFailed("x".into()).is_fatal());
        assert!(ReactorError::InvalidConfig("x".into()).is_fatal());
    }

    #[test]
    fn classify_retryable() {
        // Foreign errors are retryable.
        assert!(
            ReactorError::SendQueue(send_queue::SendQueueError::QueueFull { capacity: 0 })
                .is_retryable()
        );
        assert!(!ReactorError::Shutdown.is_retryable());
    }

    #[test]
    fn silent_drop_is_false_for_now() {
        assert!(!ReactorError::Shutdown.is_silent_drop());
    }

    #[test]
    fn display_includes_context() {
        let e = ReactorError::InvalidConfig("ring too small".into());
        let s = format!("{e}");
        assert!(s.contains("ring too small"));
    }
}
