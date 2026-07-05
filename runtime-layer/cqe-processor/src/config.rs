//! Configuration (Layer 2).
//!
//! Plain-data configuration plus the [`OverflowPolicy`] that controls how
//! the processor reacts to CQ ring overflow.

use crate::error::CqeError;

// ── Defaults ──────────────────────────────────────────────────────────

/// Default maximum CQEs drained per `drain()` call.
pub const DEFAULT_BATCH_MAX_CQES: usize = 256;
/// Minimum batch size.
pub const MIN_BATCH_CQES: usize = 16;
/// Maximum batch size.
pub const MAX_BATCH_CQES: usize = 4096;

/// Default consecutive-empty threshold before flagging starvation.
pub const DEFAULT_STARVATION_THRESHOLD: u32 = 1024;

/// Default buffer-recycle coalesce threshold.
pub const DEFAULT_RECYCLE_THRESHOLD: usize = 32;
/// Maximum recycle threshold.
pub const MAX_RECYCLE_THRESHOLD: usize = 1024;

/// Default multishot re-arm cooldown (ticks).
pub const DEFAULT_MULTISHOT_REARM_COOLDOWN: u32 = 4;

// ── Overflow policy ───────────────────────────────────────────────────

/// Action to take when the kernel reports CQ overflow.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Record a metric and continue draining. Default.
    #[default]
    Log,
    /// Record a metric and signal reactor shutdown via the returned
    /// [`TickResult`](crate::processor::TickResult).
    Shutdown,
    /// Record a metric and panic. CI / fuzzing only.
    Panic,
}

// ── Config ────────────────────────────────────────────────────────────

/// Configuration for a [`CqeProcessor`](crate::processor::CqeProcessor).
#[derive(Clone, Debug)]
pub struct CqeProcessorConfig {
    /// Maximum CQEs drained per `drain()` call.
    pub batch_max_cqes: usize,

    /// Consecutive empty drains before flagging starvation.
    pub starvation_threshold_ticks: u32,

    /// CQ overflow policy.
    pub on_overflow: OverflowPolicy,

    /// Buffer-recycle coalesce threshold. The processor flushes pending
    /// buffer returns when the count reaches this value or the drain ends.
    pub recycle_coalesce_threshold: usize,

    /// Multishot re-arm cooldown in ticks. Prevents re-arm storms when
    /// the kernel persistently reports `-ENOBUFS`.
    pub multishot_rearm_cooldown: u32,

    /// Reactor / core ID for diagnostics.
    pub reactor_id: u32,
}

impl CqeProcessorConfig {
    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), CqeError> {
        if self.batch_max_cqes < MIN_BATCH_CQES {
            return Err(CqeError::InvalidConfig(format!(
                "batch_max_cqes must be >= {MIN_BATCH_CQES} (is {})",
                self.batch_max_cqes
            )));
        }
        if self.batch_max_cqes > MAX_BATCH_CQES {
            return Err(CqeError::InvalidConfig(format!(
                "batch_max_cqes must be <= {MAX_BATCH_CQES} (is {})",
                self.batch_max_cqes
            )));
        }
        if self.starvation_threshold_ticks == 0 {
            return Err(CqeError::InvalidConfig(
                "starvation_threshold_ticks must be > 0".into(),
            ));
        }
        if self.recycle_coalesce_threshold == 0 {
            return Err(CqeError::InvalidConfig(
                "recycle_coalesce_threshold must be > 0".into(),
            ));
        }
        if self.recycle_coalesce_threshold > MAX_RECYCLE_THRESHOLD {
            return Err(CqeError::InvalidConfig(format!(
                "recycle_coalesce_threshold must be <= {MAX_RECYCLE_THRESHOLD} (is {})",
                self.recycle_coalesce_threshold
            )));
        }
        Ok(())
    }
}

impl Default for CqeProcessorConfig {
    fn default() -> Self {
        Self {
            batch_max_cqes: DEFAULT_BATCH_MAX_CQES,
            starvation_threshold_ticks: DEFAULT_STARVATION_THRESHOLD,
            on_overflow: OverflowPolicy::Log,
            recycle_coalesce_threshold: DEFAULT_RECYCLE_THRESHOLD,
            multishot_rearm_cooldown: DEFAULT_MULTISHOT_REARM_COOLDOWN,
            reactor_id: 0,
        }
    }
}
