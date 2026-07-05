//! TaskQueueBuilder — fluent builder (Layer 23).

use crate::budget::Budget;
use crate::config::{PolicyKind, TaskQueueConfig};
use crate::error::TaskError;
use crate::queue::TaskQueue;

/// Fluent builder for [`TaskQueue`].
#[derive(Default)]
pub struct TaskQueueBuilder {
    config: TaskQueueConfig,
}

impl TaskQueueBuilder {
    /// Creates a builder populated with [`TaskQueueConfig::default`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Slab capacity (rounded up to a power of two). Default: 4096.
    ///
    /// Calling this recomputes derived defaults that scale with
    /// capacity. To override explicitly, call the relevant setter
    /// **after** `.capacity()`.
    #[must_use]
    pub fn capacity(mut self, cap: usize) -> Self {
        self.config.capacity = cap;
        self
    }

    /// Scheduling policy. Default: [`PolicyKind::Fifo`].
    #[must_use]
    pub fn policy(mut self, p: PolicyKind) -> Self {
        self.config.policy = p;
        self
    }

    /// Maximum tasks executed per tick. Default: 128.
    #[must_use]
    pub fn batch_max_tasks(mut self, n: u16) -> Self {
        self.config.batch_max_tasks = n;
        self
    }

    /// Maximum execution time per tick (ns). Default: 100 000 (100 µs).
    #[must_use]
    pub fn batch_max_time_ns(mut self, ns: u64) -> Self {
        self.config.batch_max_time_ns = ns;
        self
    }

    /// MPSC inbox capacity. Default: 256.
    #[must_use]
    pub fn inbox_capacity(mut self, n: usize) -> Self {
        self.config.inbox_capacity = n;
        self
    }

    /// Aging threshold (ns). Default: 10 000 000 (10 ms).
    #[must_use]
    pub fn aging_threshold_ns(mut self, ns: u64) -> Self {
        self.config.aging_threshold_ns = ns;
        self
    }

    /// Shutdown grace period (ns). Default: 5 000 000 000 (5 s).
    #[must_use]
    pub fn shutdown_grace_ns(mut self, ns: u64) -> Self {
        self.config.shutdown_grace_ns = ns;
        self
    }

    /// Reactor / core ID for diagnostics. Default: 0.
    #[must_use]
    pub fn reactor_id(mut self, id: u32) -> Self {
        self.config.reactor_id = id;
        self
    }

    /// Validates the configuration and constructs the [`TaskQueue`].
    pub fn build(self) -> Result<TaskQueue, TaskError> {
        TaskQueue::from_config(self.config)
    }
}

impl TaskQueueBuilder {
    /// Returns the configured batch budget (convenience).
    #[must_use]
    pub const fn planned_budget(&self) -> Budget {
        Budget::new(self.config.batch_max_tasks, self.config.batch_max_time_ns)
    }
}

// Explicit Default impl to keep field docs accurate.
impl std::fmt::Debug for TaskQueueBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueueBuilder")
            .field("capacity", &self.config.capacity)
            .field("policy", &self.config.policy)
            .field("batch_max_tasks", &self.config.batch_max_tasks)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        DEFAULT_AGING_THRESHOLD_NS, DEFAULT_BATCH_MAX_TASKS, DEFAULT_BATCH_MAX_TIME_NS,
        DEFAULT_CAPACITY, DEFAULT_INBOX_CAPACITY, DEFAULT_SHUTDOWN_GRACE_NS,
    };

    #[test]
    fn default_build_succeeds() {
        assert!(TaskQueueBuilder::new().build().is_ok());
    }

    #[test]
    fn capacity_rounded_to_power_of_two() {
        let q = TaskQueueBuilder::new()
            .capacity(1000)
            .build()
            .expect("build");
        assert_eq!(q.capacity(), 1024);
    }

    #[test]
    fn rejects_too_small() {
        let err = TaskQueueBuilder::new().capacity(4).build().unwrap_err();
        assert!(matches!(err, TaskError::InvalidConfig(_)));
    }

    #[test]
    fn custom_batch_and_aging() {
        let q = TaskQueueBuilder::new()
            .capacity(256)
            .batch_max_tasks(32)
            .aging_threshold_ns(5_000_000)
            .build()
            .expect("build");
        assert_eq!(q.capacity(), 256);
    }

    #[test]
    fn planned_budget_reflects_config() {
        let b = TaskQueueBuilder::new()
            .batch_max_tasks(64)
            .batch_max_time_ns(50_000);
        let budget = b.planned_budget();
        assert_eq!(budget.max_tasks, 64);
        assert_eq!(budget.max_time_ns, 50_000);
    }

    #[test]
    fn default_constants_match_config_defaults() {
        let cfg = TaskQueueConfig::default();
        assert_eq!(cfg.capacity, DEFAULT_CAPACITY);
        assert_eq!(cfg.batch_max_tasks, DEFAULT_BATCH_MAX_TASKS);
        assert_eq!(cfg.batch_max_time_ns, DEFAULT_BATCH_MAX_TIME_NS);
        assert_eq!(cfg.inbox_capacity, DEFAULT_INBOX_CAPACITY);
        assert_eq!(cfg.aging_threshold_ns, DEFAULT_AGING_THRESHOLD_NS);
        assert_eq!(cfg.shutdown_grace_ns, DEFAULT_SHUTDOWN_GRACE_NS);
    }
}
