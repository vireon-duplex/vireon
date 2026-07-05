//! Error types (Layer 1).
//!
//! [`CqeError`] is intentionally narrow: most per-CQE anomalies are recorded
//! as metrics rather than returned as `Result` variants, because the reactor
//! must keep draining regardless. Only structural/configuration failures
//! surface here.

use thiserror::Error;

/// Errors emitted by the CQE Processor.
#[derive(Error, Debug)]
pub enum CqeError {
    /// Configuration validation failed.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// The CQ ring overflowed — the kernel dropped CQEs.
    ///
    /// Recorded as a metric; surfaced only when the configured
    /// [`OverflowPolicy`](crate::config::OverflowPolicy) is `Shutdown` or
    /// `Panic`.
    #[error("CQ overflow detected ({dropped} entries dropped)")]
    CqOverflow { dropped: u64 },

    /// Consecutive empty drains exceeded the configured threshold.
    ///
    /// Informational; the reactor is making progress but is starved for
    /// completions (typically indicates the application is SQ-bound).
    #[error("CQ starvation: {consecutive_empty} consecutive empty drains")]
    CqStarvation { consecutive_empty: u32 },

    /// A kernel-reported errno that the processor cannot classify.
    #[error("kernel errno from completion: {errno}")]
    KernelErrno { errno: i32 },
}

impl CqeError {
    /// Returns `true` if the error indicates a structurally fatal ring state.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::CqOverflow { .. })
    }
}
