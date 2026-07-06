//! [`SchedulerError`] — unified error surface for the stream
//! scheduler.
//!
//! See `DESIGN.md` Part 12 (failure handling).

use thiserror::Error;

/// Unified scheduler error.
///
/// `#[non_exhaustive]` reserves the right to add new variants in
/// future phases without breaking match arms.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// Configuration validation failed.
    #[error("invalid scheduler configuration: {0}")]
    InvalidConfig(String),

    /// No streams are ready for scheduling this tick.
    #[error("no streams ready")]
    NoStreamsReady,

    /// The per-tick budget (bytes / frames / streams) is exhausted.
    #[error("budget exhausted")]
    BudgetExhausted,

    /// A stream has been starved beyond the configured threshold.
    #[error("starvation detected for stream {0}")]
    StarvationDetected(u64),

    /// Backpressure stall — all ready streams are flow-control
    /// blocked.
    #[error("backpressure stall")]
    BackpressureStall,

    /// Invalid priority value supplied (e.g. urgency > 7).
    #[error("invalid priority: {0}")]
    InvalidPriority(String),

    /// The scheduler is shutting down.
    #[error("stream-scheduler is shutting down")]
    ShuttingDown,
}

impl SchedulerError {
    /// Fatal errors abort the connection. The scheduler treats
    /// config errors and shutdown as fatal; everything else is
    /// recoverable (return an empty tick).
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::InvalidConfig(_) | Self::InvalidPriority(_) | Self::ShuttingDown
        )
    }

    /// Retryable errors are logged and the tick returns an empty
    /// selection. Examples: `NoStreamsReady`, `BudgetExhausted`,
    /// `BackpressureStall`.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        !self.is_fatal()
    }

    /// Whether this error indicates a fairness / liveness problem
    /// that should trip an alarm.
    #[must_use]
    pub const fn is_liveness_issue(&self) -> bool {
        matches!(self, Self::StarvationDetected(_) | Self::BackpressureStall)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_fatal() {
        assert!(SchedulerError::ShuttingDown.is_fatal());
        assert!(SchedulerError::InvalidConfig("x".into()).is_fatal());
        assert!(SchedulerError::InvalidPriority("bad".into()).is_fatal());
    }

    #[test]
    fn classify_retryable() {
        assert!(SchedulerError::NoStreamsReady.is_retryable());
        assert!(SchedulerError::BudgetExhausted.is_retryable());
        assert!(SchedulerError::BackpressureStall.is_retryable());
    }

    #[test]
    fn liveness_flagging() {
        assert!(SchedulerError::StarvationDetected(7).is_liveness_issue());
        assert!(SchedulerError::BackpressureStall.is_liveness_issue());
        assert!(!SchedulerError::NoStreamsReady.is_liveness_issue());
    }

    #[test]
    fn display_includes_context() {
        let e = SchedulerError::StarvationDetected(42);
        let s = format!("{e}");
        assert!(s.contains('4') && s.contains('2'));
    }
}
