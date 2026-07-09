//! [`AgentRuntimeStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for agent runtime observability.
//!
//! See `DESIGN.md` Part 12 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

// ── AgentRuntimeStats ────────────────────────────────────────────

/// Atomic counters for the agent runtime.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`AgentRuntimeStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct AgentRuntimeStats {
    /// Total agents created (counter).
    pub agents_created: AtomicU64,
    /// Total agents stopped (counter).
    pub agents_stopped: AtomicU64,
    /// Total tool calls initiated (counter).
    pub tool_calls_initiated: AtomicU64,
    /// Total tool calls completed successfully (counter).
    pub tool_calls_completed: AtomicU64,
    /// Total tool calls failed (counter).
    pub tool_calls_failed: AtomicU64,
    /// Total context tokens used (counter).
    pub context_tokens_used: AtomicU64,
}

impl Default for AgentRuntimeStats {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentRuntimeStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            agents_created: AtomicU64::new(0),
            agents_stopped: AtomicU64::new(0),
            tool_calls_initiated: AtomicU64::new(0),
            tool_calls_completed: AtomicU64::new(0),
            tool_calls_failed: AtomicU64::new(0),
            context_tokens_used: AtomicU64::new(0),
        }
    }

    /// Record an agent created.
    pub fn record_agent_created(&self) {
        self.agents_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an agent stopped.
    pub fn record_agent_stopped(&self) {
        self.agents_stopped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tool call initiated.
    pub fn record_tool_call_initiated(&self) {
        self.tool_calls_initiated.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tool call completed successfully.
    pub fn record_tool_call_completed(&self) {
        self.tool_calls_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tool call failed.
    pub fn record_tool_call_failed(&self) {
        self.tool_calls_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record context tokens used.
    pub fn record_context_tokens(&self, count: u64) {
        self.context_tokens_used.fetch_add(count, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            agents_created: self.agents_created.load(Ordering::Relaxed),
            agents_stopped: self.agents_stopped.load(Ordering::Relaxed),
            tool_calls_initiated: self.tool_calls_initiated.load(Ordering::Relaxed),
            tool_calls_completed: self.tool_calls_completed.load(Ordering::Relaxed),
            tool_calls_failed: self.tool_calls_failed.load(Ordering::Relaxed),
            context_tokens_used: self.context_tokens_used.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ────────────────────────────────────────────────

/// Point-in-time snapshot of agent runtime counters.
///
/// Rates are precomputed as basis-points (bps, 1/100 of a percent)
/// so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total agents created.
    pub agents_created: u64,
    /// Total agents stopped.
    pub agents_stopped: u64,
    /// Total tool calls initiated.
    pub tool_calls_initiated: u64,
    /// Total tool calls completed successfully.
    pub tool_calls_completed: u64,
    /// Total tool calls failed.
    pub tool_calls_failed: u64,
    /// Total context tokens used.
    pub context_tokens_used: u64,
}

impl StatsSnapshot {
    /// Tool call success rate in basis points (0..=10_000).
    ///
    /// Returns 10_000 (100%) when no tool calls have been initiated.
    #[must_use]
    pub const fn tool_success_rate_bps(&self) -> u16 {
        if self.tool_calls_initiated == 0 {
            return 10_000;
        }
        let rate = (self.tool_calls_completed.saturating_mul(10_000)) / self.tool_calls_initiated;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Number of currently active agents (created minus stopped).
    #[must_use]
    pub const fn active_agents(&self) -> u64 {
        self.agents_created.saturating_sub(self.agents_stopped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = AgentRuntimeStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_agent_created_increments_counter() {
        let s = AgentRuntimeStats::new();
        s.record_agent_created();
        s.record_agent_created();
        assert_eq!(s.snapshot().agents_created, 2);
    }

    #[test]
    fn record_agent_stopped_increments_counter() {
        let s = AgentRuntimeStats::new();
        s.record_agent_stopped();
        assert_eq!(s.snapshot().agents_stopped, 1);
    }

    #[test]
    fn record_tool_call_initiated_increments_counter() {
        let s = AgentRuntimeStats::new();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        assert_eq!(s.snapshot().tool_calls_initiated, 3);
    }

    #[test]
    fn record_tool_call_completed_increments_counter() {
        let s = AgentRuntimeStats::new();
        s.record_tool_call_completed();
        assert_eq!(s.snapshot().tool_calls_completed, 1);
    }

    #[test]
    fn record_tool_call_failed_increments_counter() {
        let s = AgentRuntimeStats::new();
        s.record_tool_call_failed();
        s.record_tool_call_failed();
        assert_eq!(s.snapshot().tool_calls_failed, 2);
    }

    #[test]
    fn record_context_tokens_increments_counter() {
        let s = AgentRuntimeStats::new();
        s.record_context_tokens(100);
        s.record_context_tokens(50);
        assert_eq!(s.snapshot().context_tokens_used, 150);
    }

    #[test]
    fn tool_success_rate_with_no_calls_is_max() {
        let s = AgentRuntimeStats::new();
        assert_eq!(s.snapshot().tool_success_rate_bps(), 10_000);
    }

    #[test]
    fn tool_success_rate_all_success() {
        let s = AgentRuntimeStats::new();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_completed();
        s.record_tool_call_completed();
        assert_eq!(s.snapshot().tool_success_rate_bps(), 10_000);
    }

    #[test]
    fn tool_success_rate_all_failed() {
        let s = AgentRuntimeStats::new();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_failed();
        s.record_tool_call_failed();
        assert_eq!(s.snapshot().tool_success_rate_bps(), 0);
    }

    #[test]
    fn tool_success_rate_calculates_correctly() {
        let s = AgentRuntimeStats::new();
        // 3 initiated, 2 completed = 2/3 = 66.67% = 6666 bps (integer division truncates)
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_completed();
        s.record_tool_call_completed();
        s.record_tool_call_failed();
        assert_eq!(s.snapshot().tool_success_rate_bps(), 6_666);
    }

    #[test]
    fn active_agents_calculation() {
        let s = AgentRuntimeStats::new();
        assert_eq!(s.snapshot().active_agents(), 0);
        s.record_agent_created();
        s.record_agent_created();
        s.record_agent_created();
        assert_eq!(s.snapshot().active_agents(), 3);
        s.record_agent_stopped();
        assert_eq!(s.snapshot().active_agents(), 2);
    }

    #[test]
    fn active_agents_never_underflows() {
        let mut snap = StatsSnapshot::default();
        snap.agents_created = 1;
        snap.agents_stopped = 5;
        assert_eq!(snap.active_agents(), 0);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = AgentRuntimeStats::new();
        s.record_context_tokens(1000);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_cloning_is_independent() {
        let s = AgentRuntimeStats::new();
        s.record_agent_created();
        let snap1 = s.snapshot();
        let snap2 = snap1;
        s.record_agent_created(); // This affects stats, not snapshots
        assert_eq!(snap1, snap2); // Snapshots are unchanged
        assert_eq!(snap1.agents_created, 1);
    }

    #[test]
    fn all_counter_methods_independent() {
        let s = AgentRuntimeStats::new();
        s.record_agent_created();
        s.record_agent_stopped();
        s.record_tool_call_initiated();
        s.record_tool_call_completed();
        s.record_tool_call_failed();
        s.record_context_tokens(42);
        let snap = s.snapshot();
        assert_eq!(snap.agents_created, 1);
        assert_eq!(snap.agents_stopped, 1);
        assert_eq!(snap.tool_calls_initiated, 1);
        assert_eq!(snap.tool_calls_completed, 1);
        assert_eq!(snap.tool_calls_failed, 1);
        assert_eq!(snap.context_tokens_used, 42);
    }
}
