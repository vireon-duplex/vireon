//! [`TickResult`] — the output of one reactor tick — and
//! [`TickBudget`] — the per-tick limit envelope.
//!
//! See `DESIGN.md` §3 (event loop ordering) and §17 (observability
//! for tick-level metrics).

/// Per-tick runtime budget. The event loop consults these caps
/// every iteration; overruns are observable in [`TickResult`] and
/// aggregated in [`ReactorStats`](crate::stats::ReactorStats).
///
/// `TickBudget` is computed once at reactor start from
/// [`BudgetConfig`](crate::budget::BudgetConfig) and re-used
/// for every tick unless a backpressure signal shrinks it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickBudget {
    /// Max CQEs to drain per tick (prevents CQ drain from
    /// monopolising the loop under burst).
    pub max_cqes: u32,
    /// Max tasks to execute per tick (cooperative fairness).
    pub max_tasks: u32,
    /// Max packets to flush from the send queue per tick.
    pub max_send_flush: u32,
    /// Soft wall-clock deadline for one tick, in nanoseconds.
    /// The loop checks `deadline_ns` between phases.
    pub deadline_ns: u64,
}

impl TickBudget {
    /// Construct from the four independent caps.
    #[must_use]
    pub const fn new(max_cqes: u32, max_tasks: u32, max_send_flush: u32, deadline_ns: u64) -> Self {
        Self {
            max_cqes,
            max_tasks,
            max_send_flush,
            deadline_ns,
        }
    }

    /// `true` if the budget is the trivial all-zero (used as a
    /// sentinel by skeletons).
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.max_cqes == 0
            && self.max_tasks == 0
            && self.max_send_flush == 0
            && self.deadline_ns == 0
    }
}

impl Default for TickBudget {
    fn default() -> Self {
        // Conservative defaults; overridden by BudgetConfig at build.
        Self::new(256, 64, 128, 200_000)
    }
}

/// Output of one reactor tick — what happened during this
/// iteration of the event loop. Returned by
/// [`EventLoop::run_one_tick`](crate::event_loop::EventLoop::run_one_tick)
/// (Phase 1) and aggregated into
/// [`ReactorStats`](crate::stats::ReactorStats).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickResult {
    /// Number of CQEs drained this tick.
    pub drained: u32,
    /// Number of outcomes dispatched to subsystems.
    pub dispatched: u32,
    /// Datagrams received (any RECVMSG CQE).
    pub packets_recv: u32,
    /// Datagrams submitted to the ring (SEND_ZC / SENDMMSG).
    pub packets_sent: u32,
    /// Tasks executed from the task queue.
    pub tasks_run: u32,
    /// Timers that fired this tick.
    pub timers_fired: u32,
    /// Wall-clock time spent in this tick, in nanoseconds.
    pub latency_ns: u64,
    /// `true` if any subsystem exceeded its budget.
    pub overflow: bool,
}

impl TickResult {
    /// Aggregate another tick's counters into `self` (used by the
    /// stats aggregator).
    pub fn merge(&mut self, other: &Self) {
        self.drained = self.drained.saturating_add(other.drained);
        self.dispatched = self.dispatched.saturating_add(other.dispatched);
        self.packets_recv = self.packets_recv.saturating_add(other.packets_recv);
        self.packets_sent = self.packets_sent.saturating_add(other.packets_sent);
        self.tasks_run = self.tasks_run.saturating_add(other.tasks_run);
        self.timers_fired = self.timers_fired.saturating_add(other.timers_fired);
        self.latency_ns = self.latency_ns.max(other.latency_ns);
        self.overflow = self.overflow || other.overflow;
    }

    /// `true` if the tick did no work at all — the reactor was
    /// idle. Used by the idle-strategy selector.
    #[must_use]
    pub const fn was_idle(&self) -> bool {
        self.drained == 0 && self.packets_sent == 0 && self.tasks_run == 0 && self.timers_fired == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_budget_default_nonzero() {
        let b = TickBudget::default();
        assert!(!b.is_zero());
        assert_eq!(b.max_cqes, 256);
    }

    #[test]
    fn tick_budget_zero_sentinel() {
        let z = TickBudget::new(0, 0, 0, 0);
        assert!(z.is_zero());
    }

    #[test]
    fn tick_result_merge_saturates() {
        let mut a = TickResult {
            drained: u32::MAX - 1,
            ..TickResult::default()
        };
        let b = TickResult {
            drained: 10,
            ..TickResult::default()
        };
        a.merge(&b);
        assert_eq!(a.drained, u32::MAX);
    }

    #[test]
    fn tick_result_was_idle() {
        let idle = TickResult::default();
        assert!(idle.was_idle());

        let busy = TickResult {
            drained: 1,
            ..TickResult::default()
        };
        assert!(!busy.was_idle());
    }
}
