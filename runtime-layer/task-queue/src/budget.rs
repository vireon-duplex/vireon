//! Per-tick execution budget (Layer 18).
//!
//! [`Budget`] bounds how much work a single reactor tick can spend on
//! task execution. Without it, a flood of high-priority tasks could
//! starve IO processing indefinitely.

/// Bounds for a single tick's task execution.
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    /// Maximum number of tasks to execute per tick.
    pub max_tasks: u16,
    /// Maximum wall-clock time for task execution per tick (nanoseconds).
    pub max_time_ns: u64,
}

impl Budget {
    /// Creates a budget from the config values.
    #[must_use]
    pub const fn new(max_tasks: u16, max_time_ns: u64) -> Self {
        Self {
            max_tasks,
            max_time_ns,
        }
    }

    /// Returns `true` if `executed` tasks or `elapsed_ns` time has
    /// exhausted the budget.
    #[inline]
    #[must_use]
    pub const fn is_exhausted(self, executed: u16, elapsed_ns: u64) -> bool {
        executed >= self.max_tasks || elapsed_ns >= self.max_time_ns
    }

    /// Remaining tasks before exhaustion.
    #[inline]
    #[must_use]
    pub const fn remaining_tasks(self, executed: u16) -> u16 {
        self.max_tasks.saturating_sub(executed)
    }
}

impl Default for Budget {
    fn default() -> Self {
        Self::new(
            crate::config::DEFAULT_BATCH_MAX_TASKS,
            crate::config::DEFAULT_BATCH_MAX_TIME_NS,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exhaustion_by_count() {
        let b = Budget::new(10, 1_000_000);
        assert!(!b.is_exhausted(5, 0));
        assert!(b.is_exhausted(10, 0));
        assert!(b.is_exhausted(11, 0));
    }

    #[test]
    fn exhaustion_by_time() {
        let b = Budget::new(10, 1_000);
        assert!(!b.is_exhausted(5, 999));
        assert!(b.is_exhausted(5, 1_000));
        assert!(b.is_exhausted(5, 2_000));
    }

    #[test]
    fn remaining_tasks() {
        let b = Budget::new(10, 1_000_000);
        assert_eq!(b.remaining_tasks(0), 10);
        assert_eq!(b.remaining_tasks(7), 3);
        assert_eq!(b.remaining_tasks(10), 0);
        assert_eq!(b.remaining_tasks(20), 0);
    }
}
