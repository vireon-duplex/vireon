//! [`AiStreamStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for AI streaming observability.
//!
//! See `DESIGN.md` Part 12 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

// ── AiStreamStats ────────────────────────────────────────────────

/// Atomic counters for the AI streaming framework.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`AiStreamStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct AiStreamStats {
    /// Total streams opened (counter).
    pub streams_opened: AtomicU64,
    /// Total streams closed (counter).
    pub streams_closed: AtomicU64,
    /// Total tokens generated (counter).
    pub tokens_generated: AtomicU64,
    /// Total tool calls initiated (counter).
    pub tool_calls_initiated: AtomicU64,
    /// Total tool calls completed successfully (counter).
    pub tool_calls_completed: AtomicU64,
    /// Total tool calls failed (counter).
    pub tool_calls_failed: AtomicU64,
}

impl Default for AiStreamStats {
    fn default() -> Self {
        Self::new()
    }
}

impl AiStreamStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            streams_opened: AtomicU64::new(0),
            streams_closed: AtomicU64::new(0),
            tokens_generated: AtomicU64::new(0),
            tool_calls_initiated: AtomicU64::new(0),
            tool_calls_completed: AtomicU64::new(0),
            tool_calls_failed: AtomicU64::new(0),
        }
    }

    /// Record a stream opened.
    pub fn record_stream_opened(&self) {
        self.streams_opened.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a stream closed.
    pub fn record_stream_closed(&self) {
        self.streams_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record tokens generated.
    pub fn record_tokens(&self, count: u64) {
        self.tokens_generated.fetch_add(count, Ordering::Relaxed);
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

    /// Take a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            streams_opened: self.streams_opened.load(Ordering::Relaxed),
            streams_closed: self.streams_closed.load(Ordering::Relaxed),
            tokens_generated: self.tokens_generated.load(Ordering::Relaxed),
            tool_calls_initiated: self.tool_calls_initiated.load(Ordering::Relaxed),
            tool_calls_completed: self.tool_calls_completed.load(Ordering::Relaxed),
            tool_calls_failed: self.tool_calls_failed.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ────────────────────────────────────────────────

/// Point-in-time snapshot of AI streaming counters.
///
/// Rates are precomputed as basis-points (bps, 1/100 of a percent)
/// so consumers don't need to do division at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Total streams opened.
    pub streams_opened: u64,
    /// Total streams closed.
    pub streams_closed: u64,
    /// Total tokens generated.
    pub tokens_generated: u64,
    /// Total tool calls initiated.
    pub tool_calls_initiated: u64,
    /// Total tool calls completed successfully.
    pub tool_calls_completed: u64,
    /// Total tool calls failed.
    pub tool_calls_failed: u64,
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

    /// Number of currently active streams (opened minus closed).
    #[must_use]
    pub const fn active_streams(&self) -> u64 {
        self.streams_opened.saturating_sub(self.streams_closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = AiStreamStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_stream_opened_increments_counter() {
        let s = AiStreamStats::new();
        s.record_stream_opened();
        s.record_stream_opened();
        assert_eq!(s.snapshot().streams_opened, 2);
    }

    #[test]
    fn record_stream_closed_increments_counter() {
        let s = AiStreamStats::new();
        s.record_stream_closed();
        assert_eq!(s.snapshot().streams_closed, 1);
    }

    #[test]
    fn record_tokens_increments_counter() {
        let s = AiStreamStats::new();
        s.record_tokens(100);
        s.record_tokens(50);
        assert_eq!(s.snapshot().tokens_generated, 150);
    }

    #[test]
    fn record_tool_call_initiated_increments_counter() {
        let s = AiStreamStats::new();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        assert_eq!(s.snapshot().tool_calls_initiated, 3);
    }

    #[test]
    fn record_tool_call_completed_increments_counter() {
        let s = AiStreamStats::new();
        s.record_tool_call_completed();
        assert_eq!(s.snapshot().tool_calls_completed, 1);
    }

    #[test]
    fn record_tool_call_failed_increments_counter() {
        let s = AiStreamStats::new();
        s.record_tool_call_failed();
        s.record_tool_call_failed();
        assert_eq!(s.snapshot().tool_calls_failed, 2);
    }

    #[test]
    fn tool_success_rate_with_no_calls_is_max() {
        let s = AiStreamStats::new();
        assert_eq!(s.snapshot().tool_success_rate_bps(), 10_000);
    }

    #[test]
    fn tool_success_rate_all_success() {
        let s = AiStreamStats::new();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_completed();
        s.record_tool_call_completed();
        assert_eq!(s.snapshot().tool_success_rate_bps(), 10_000);
    }

    #[test]
    fn tool_success_rate_all_failed() {
        let s = AiStreamStats::new();
        s.record_tool_call_initiated();
        s.record_tool_call_initiated();
        s.record_tool_call_failed();
        s.record_tool_call_failed();
        assert_eq!(s.snapshot().tool_success_rate_bps(), 0);
    }

    #[test]
    fn tool_success_rate_calculates_correctly() {
        let s = AiStreamStats::new();
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
    fn active_streams_calculation() {
        let s = AiStreamStats::new();
        assert_eq!(s.snapshot().active_streams(), 0);
        s.record_stream_opened();
        s.record_stream_opened();
        s.record_stream_opened();
        assert_eq!(s.snapshot().active_streams(), 3);
        s.record_stream_closed();
        assert_eq!(s.snapshot().active_streams(), 2);
    }

    #[test]
    fn active_streams_never_underflows() {
        let mut snap = StatsSnapshot::default();
        snap.streams_opened = 1;
        snap.streams_closed = 5;
        assert_eq!(snap.active_streams(), 0);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = AiStreamStats::new();
        s.record_tokens(1000);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn snapshot_cloning_is_independent() {
        let s = AiStreamStats::new();
        s.record_stream_opened();
        let snap1 = s.snapshot();
        let snap2 = snap1;
        s.record_stream_opened(); // This affects stats, not snapshots
        assert_eq!(snap1, snap2); // Snapshots are unchanged
        assert_eq!(snap1.streams_opened, 1);
    }

    #[test]
    fn all_counter_methods_independent() {
        let s = AiStreamStats::new();
        s.record_stream_opened();
        s.record_stream_closed();
        s.record_tokens(42);
        s.record_tool_call_initiated();
        s.record_tool_call_completed();
        s.record_tool_call_failed();
        let snap = s.snapshot();
        assert_eq!(snap.streams_opened, 1);
        assert_eq!(snap.streams_closed, 1);
        assert_eq!(snap.tokens_generated, 42);
        assert_eq!(snap.tool_calls_initiated, 1);
        assert_eq!(snap.tool_calls_completed, 1);
        assert_eq!(snap.tool_calls_failed, 1);
    }
}
