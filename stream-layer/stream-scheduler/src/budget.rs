//! [`SchedulingBudget`] — per-tick resource caps.
//!
//! See `DESIGN.md` Part 3 (scheduling model) and Part 13
//! (performance targets).
//!
//! # Why a budget?
//!
//! Each reactor tick must be time-bounded. If the scheduler
//! selected every ready stream, a single tick could overrun the
//! reactor's latency target. The budget caps the work per tick:
//!
//! - `max_streams` — streams considered (selection loop).
//! - `max_bytes` — total bytes scheduled (congestion-window aware).
//! - `max_frames` — STREAM frames emitted (packet-budget aware).
//! - `deadline_ns` — wall-clock deadline for the tick.

/// Default per-tick byte cap (matches a typical CWND slice).
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024;
/// Default per-tick frame cap.
pub const DEFAULT_MAX_FRAMES: u32 = 128;
/// Default per-tick deadline (1 ms).
pub const DEFAULT_DEADLINE_NS: u64 = 1_000_000;

/// Per-tick scheduling budget. Consumed as the scheduler selects
/// streams; the tick ends when the budget is exhausted.
#[derive(Clone, Copy, Debug)]
pub struct SchedulingBudget {
    max_streams: u32,
    max_bytes: u64,
    max_frames: u32,
    deadline_ns: u64,
    // Consumed so far:
    streams_consumed: u32,
    bytes_consumed: u64,
    frames_consumed: u32,
}

impl SchedulingBudget {
    /// Construct a full budget.
    #[must_use]
    pub const fn new(max_streams: u32, max_bytes: u64, max_frames: u32, deadline_ns: u64) -> Self {
        Self {
            max_streams,
            max_bytes,
            max_frames,
            deadline_ns,
            streams_consumed: 0,
            bytes_consumed: 0,
            frames_consumed: 0,
        }
    }

    /// Construct a budget from defaults.
    #[must_use]
    pub const fn default_budget(max_streams: u32) -> Self {
        Self::new(
            max_streams,
            DEFAULT_MAX_BYTES,
            DEFAULT_MAX_FRAMES,
            DEFAULT_DEADLINE_NS,
        )
    }

    /// Maximum streams the budget allows.
    #[must_use]
    pub const fn max_streams(&self) -> u32 {
        self.max_streams
    }

    /// The configured tick deadline (ns).
    #[must_use]
    pub const fn deadline_ns(&self) -> u64 {
        self.deadline_ns
    }

    /// Remaining stream slots.
    #[must_use]
    pub const fn remaining_streams(&self) -> u32 {
        self.max_streams.saturating_sub(self.streams_consumed)
    }

    /// Remaining byte budget.
    #[must_use]
    pub const fn remaining_bytes(&self) -> u64 {
        self.max_bytes.saturating_sub(self.bytes_consumed)
    }

    /// Remaining frame budget.
    #[must_use]
    pub const fn remaining_frames(&self) -> u32 {
        self.max_frames.saturating_sub(self.frames_consumed)
    }

    /// Whether the budget is fully consumed.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.streams_consumed >= self.max_streams
            || self.bytes_consumed >= self.max_bytes
            || self.frames_consumed >= self.max_frames
    }

    /// Consume `bytes` worth of data (one STREAM frame). Returns
    /// `true` if the consumption succeeded (budget available),
    /// `false` if the budget would be exceeded (no change made).
    pub fn consume(&mut self, bytes: u64) -> bool {
        if self.is_exhausted() {
            return false;
        }
        let new_bytes = self.bytes_consumed + bytes;
        if new_bytes > self.max_bytes {
            return false;
        }
        self.bytes_consumed = new_bytes;
        self.streams_consumed += 1;
        self.frames_consumed += 1;
        true
    }

    /// Consume one stream selection without byte accounting (used
    /// for control frames or zero-byte selections).
    pub fn consume_slot(&mut self) -> bool {
        if self.streams_consumed >= self.max_streams {
            return false;
        }
        self.streams_consumed += 1;
        true
    }

    /// Bytes consumed so far this tick.
    #[must_use]
    pub const fn bytes_consumed(&self) -> u64 {
        self.bytes_consumed
    }

    /// Streams selected so far this tick.
    #[must_use]
    pub const fn streams_consumed(&self) -> u32 {
        self.streams_consumed
    }
}

impl Default for SchedulingBudget {
    fn default() -> Self {
        Self::default_budget(crate::config::DEFAULT_MAX_STREAMS_PER_TICK)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_budget_not_exhausted() {
        let b = SchedulingBudget::new(10, 1024, 32, 1_000_000);
        assert!(!b.is_exhausted());
        assert_eq!(b.remaining_streams(), 10);
        assert_eq!(b.remaining_bytes(), 1024);
    }

    #[test]
    fn consume_decrements_budget() {
        let mut b = SchedulingBudget::new(2, 1000, 10, 1_000_000);
        assert!(b.consume(400));
        assert!(b.consume(500));
        assert!(!b.consume(200)); // bytes would exceed
        assert_eq!(b.bytes_consumed(), 900);
        assert_eq!(b.streams_consumed(), 2);
        assert!(b.is_exhausted()); // streams exhausted
    }

    #[test]
    fn consume_slot_ignores_bytes() {
        let mut b = SchedulingBudget::new(3, 0, 10, 1_000_000);
        assert!(b.consume_slot()); // bytes already 0 but slot counts
        assert_eq!(b.streams_consumed(), 1);
    }

    #[test]
    fn exhaustion_on_streams() {
        let mut b = SchedulingBudget::new(1, 1_000_000, 1_000_000, 1_000_000);
        assert!(b.consume(100));
        assert!(b.is_exhausted());
        assert!(!b.consume(100));
    }

    #[test]
    fn exhaustion_on_bytes() {
        let mut b = SchedulingBudget::new(1_000_000, 500, 1_000_000, 1_000_000);
        assert!(b.consume(500));
        assert!(b.is_exhausted());
    }
}
