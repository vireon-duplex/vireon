//! Configuration types (Layer 2).

use crate::error::TaskError;

// ── Defaults ──────────────────────────────────────────────────────────

/// Default slab capacity (must be a power of two).
pub const DEFAULT_CAPACITY: usize = 4096;
/// Absolute minimum slab capacity.
pub const MIN_CAPACITY: usize = 64;
/// Absolute maximum slab capacity (`2^20` slots — matches ID encoding).
pub const MAX_CAPACITY: usize = 1 << crate::id::SLOT_BITS;

/// Default maximum tasks executed per tick batch.
pub const DEFAULT_BATCH_MAX_TASKS: u16 = 128;
/// Minimum batch size.
pub const MIN_BATCH_TASKS: u16 = 1;
/// Maximum batch size.
pub const MAX_BATCH_TASKS: u16 = 1024;

/// Default per-tick execution time budget (100 µs).
pub const DEFAULT_BATCH_MAX_TIME_NS: u64 = 100_000;

/// Default MPSC inbox capacity for cross-thread submissions.
pub const DEFAULT_INBOX_CAPACITY: usize = 256;

/// Default aging threshold (10 ms) — tasks waiting longer are promoted.
pub const DEFAULT_AGING_THRESHOLD_NS: u64 = 10_000_000;

/// Default shutdown grace period (5 s).
pub const DEFAULT_SHUTDOWN_GRACE_NS: u64 = 5_000_000_000;

// ── Policy kind ───────────────────────────────────────────────────────

/// Selectable scheduling policies.
///
/// Phase 1 ships `Fifo`. `PriorityBandedFifo` and `Deadline` are
/// reserved for Phase 2 — they construct at build time but currently
/// fall back to FIFO internally.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PolicyKind {
    /// Single-band strict FIFO (Phase 1 default).
    #[default]
    Fifo,
    /// 4-band priority with aging (Phase 2).
    PriorityBandedFifo,
    /// Earliest-deadline-first within band (Phase 2).
    Deadline,
}

// ── Main config ───────────────────────────────────────────────────────

/// Plain-data configuration for a [`TaskQueue`](crate::queue::TaskQueue).
#[derive(Clone, Debug)]
pub struct TaskQueueConfig {
    /// Slab capacity (rounded up to a power of two).
    pub capacity: usize,

    /// Scheduling policy.
    pub policy: PolicyKind,

    /// Maximum tasks executed per tick batch.
    pub batch_max_tasks: u16,
    /// Maximum execution time per tick (nanoseconds).
    pub batch_max_time_ns: u64,

    /// MPSC inbox capacity for cross-thread submissions.
    pub inbox_capacity: usize,

    /// Aging threshold (nanoseconds). Tasks waiting longer are promoted.
    pub aging_threshold_ns: u64,

    /// Shutdown grace period (nanoseconds).
    pub shutdown_grace_ns: u64,

    /// Reactor / core ID for diagnostics.
    pub reactor_id: u32,
}

impl TaskQueueConfig {
    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), TaskError> {
        if self.capacity < MIN_CAPACITY {
            return Err(TaskError::InvalidConfig(format!(
                "capacity must be >= {MIN_CAPACITY} (is {})",
                self.capacity
            )));
        }
        if self.capacity > MAX_CAPACITY {
            return Err(TaskError::InvalidConfig(format!(
                "capacity must be <= {MAX_CAPACITY} (is {})",
                self.capacity
            )));
        }
        if !self.capacity.is_power_of_two() {
            return Err(TaskError::InvalidConfig(format!(
                "capacity must be a power of two (is {})",
                self.capacity
            )));
        }

        if self.batch_max_tasks < MIN_BATCH_TASKS {
            return Err(TaskError::InvalidConfig(format!(
                "batch_max_tasks must be >= {MIN_BATCH_TASKS} (is {})",
                self.batch_max_tasks
            )));
        }
        if self.batch_max_tasks > MAX_BATCH_TASKS {
            return Err(TaskError::InvalidConfig(format!(
                "batch_max_tasks must be <= {MAX_BATCH_TASKS} (is {})",
                self.batch_max_tasks
            )));
        }
        if (self.batch_max_tasks as usize) > self.capacity {
            return Err(TaskError::InvalidConfig(format!(
                "batch_max_tasks ({}) must be <= capacity ({})",
                self.batch_max_tasks, self.capacity
            )));
        }

        if self.batch_max_time_ns == 0 {
            return Err(TaskError::InvalidConfig(
                "batch_max_time_ns must be > 0".into(),
            ));
        }
        if self.inbox_capacity < 16 {
            return Err(TaskError::InvalidConfig(format!(
                "inbox_capacity must be >= 16 (is {})",
                self.inbox_capacity
            )));
        }
        if self.aging_threshold_ns == 0 {
            return Err(TaskError::InvalidConfig(
                "aging_threshold_ns must be > 0".into(),
            ));
        }
        Ok(())
    }

    /// Rounds `capacity` up to the next power of two (in place).
    pub fn round_capacity_up(&mut self) {
        self.capacity = self.capacity.next_power_of_two();
    }
}

impl Default for TaskQueueConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CAPACITY,
            policy: PolicyKind::default(),
            batch_max_tasks: DEFAULT_BATCH_MAX_TASKS,
            batch_max_time_ns: DEFAULT_BATCH_MAX_TIME_NS,
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            aging_threshold_ns: DEFAULT_AGING_THRESHOLD_NS,
            shutdown_grace_ns: DEFAULT_SHUTDOWN_GRACE_NS,
            reactor_id: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        TaskQueueConfig::default()
            .validate()
            .expect("default validates");
    }

    #[test]
    fn rejects_non_power_of_two() {
        let cfg = TaskQueueConfig {
            capacity: 1000,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_too_small() {
        let cfg = TaskQueueConfig {
            capacity: 8,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_batch_larger_than_capacity() {
        let cfg = TaskQueueConfig {
            capacity: 64,
            batch_max_tasks: 128,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_time_budget() {
        let cfg = TaskQueueConfig {
            batch_max_time_ns: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn round_capacity_up_works() {
        let mut cfg = TaskQueueConfig {
            capacity: 1000,
            ..Default::default()
        };
        cfg.round_capacity_up();
        assert_eq!(cfg.capacity, 1024);
    }
}
