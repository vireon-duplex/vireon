//! [`PhaseBarrier`] — synchronization primitive for phase completion.
//!
//! See `DESIGN.md` Part 6 (barrier synchronization).
//!
//! # Design
//!
//! A phase barrier tracks the lifecycle of a single startup or shutdown
//! phase. It records when the phase started, tracks completion progress,
//! and marks when the phase is complete.

/// Synchronization primitive for tracking phase completion.
///
/// Barriers are created when a phase begins and marked complete when
/// all work for that phase is finished.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseBarrier {
    /// Phase identifier (raw u8 from StartupPhase or ShutdownPhase).
    pub phase_id: u8,
    /// Number of completion steps recorded.
    pub completion_count: u32,
    /// Whether the phase is complete.
    pub is_complete: bool,
    /// Nanosecond timestamp when the phase started.
    pub started_ns: u64,
    /// Nanosecond timestamp when the phase completed (0 if not complete).
    pub completed_ns: u64,
}

impl PhaseBarrier {
    /// Create a new barrier for the given phase.
    ///
    /// # Arguments
    /// * `phase_id` - Raw phase identifier (u8 representation).
    /// * `started_ns` - Nanosecond timestamp when the phase started.
    #[must_use]
    pub const fn new(phase_id: u8, started_ns: u64) -> Self {
        Self {
            phase_id,
            completion_count: 0,
            is_complete: false,
            started_ns,
            completed_ns: 0,
        }
    }

    /// Record progress within the phase.
    ///
    /// Increments the completion counter.
    pub fn mark_progress(&mut self) {
        self.completion_count = self.completion_count.saturating_add(1);
    }

    /// Mark the phase as complete.
    ///
    /// # Arguments
    /// * `now_ns` - Current nanosecond timestamp.
    pub fn mark_complete(&mut self, now_ns: u64) {
        self.is_complete = true;
        self.completed_ns = now_ns;
    }

    /// Whether the phase is complete.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.is_complete
    }

    /// Duration of the phase in nanoseconds.
    ///
    /// Returns the elapsed time from start to completion, or from
    /// start to now if not complete.
    ///
    /// # Arguments
    /// * `now_ns` - Current nanosecond timestamp.
    #[must_use]
    pub const fn duration_ns(&self, now_ns: u64) -> u64 {
        if self.is_complete {
            self.completed_ns.saturating_sub(self.started_ns)
        } else if now_ns >= self.started_ns {
            now_ns.saturating_sub(self.started_ns)
        } else {
            0 // Invalid timestamps
        }
    }

    /// Get the phase ID.
    #[must_use]
    pub const fn phase(&self) -> u8 {
        self.phase_id
    }

    /// Get the completion count.
    #[must_use]
    pub const fn completions(&self) -> u32 {
        self.completion_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_barrier_is_not_complete() {
        let barrier = PhaseBarrier::new(0, 1_000_000);
        assert!(!barrier.is_done());
        assert_eq!(barrier.phase_id, 0);
        assert_eq!(barrier.started_ns, 1_000_000);
        assert_eq!(barrier.completed_ns, 0);
        assert_eq!(barrier.completion_count, 0);
    }

    #[test]
    fn mark_progress_increments_count() {
        let mut barrier = PhaseBarrier::new(1, 1_000_000);
        barrier.mark_progress();
        assert_eq!(barrier.completion_count, 1);
        barrier.mark_progress();
        assert_eq!(barrier.completion_count, 2);
        assert!(!barrier.is_done());
    }

    #[test]
    fn mark_complete_sets_flag_and_timestamp() {
        let mut barrier = PhaseBarrier::new(2, 1_000_000);
        barrier.mark_complete(2_500_000);
        assert!(barrier.is_done());
        assert_eq!(barrier.completed_ns, 2_500_000);
    }

    #[test]
    fn duration_ns_when_complete() {
        let mut barrier = PhaseBarrier::new(3, 1_000_000);
        barrier.mark_complete(3_500_000);
        assert_eq!(barrier.duration_ns(4_000_000), 2_500_000);
    }

    #[test]
    fn duration_ns_when_not_complete() {
        let barrier = PhaseBarrier::new(4, 1_000_000);
        assert_eq!(barrier.duration_ns(2_000_000), 1_000_000);
    }

    #[test]
    fn duration_ns_handles_invalid_timestamps() {
        let mut barrier = PhaseBarrier::new(5, 5_000_000);
        // Complete before start (invalid)
        barrier.mark_complete(1_000_000);
        assert_eq!(barrier.duration_ns(6_000_000), 0);

        let barrier2 = PhaseBarrier::new(6, 5_000_000);
        // Now before start (invalid)
        assert_eq!(barrier2.duration_ns(1_000_000), 0);
    }

    #[test]
    fn phase_getter() {
        let barrier = PhaseBarrier::new(7, 1_000_000);
        assert_eq!(barrier.phase(), 7);
    }

    #[test]
    fn completions_getter() {
        let mut barrier = PhaseBarrier::new(8, 1_000_000);
        assert_eq!(barrier.completions(), 0);
        barrier.mark_progress();
        barrier.mark_progress();
        barrier.mark_progress();
        assert_eq!(barrier.completions(), 3);
    }

    #[test]
    fn barrier_is_copy() {
        let barrier1 = PhaseBarrier::new(9, 1_000_000);
        let barrier2 = barrier1; // Copy
        assert_eq!(barrier1.phase_id, barrier2.phase_id);
        assert_eq!(barrier1, barrier2);
    }

    #[test]
    fn barrier_is_clone() {
        let barrier1 = PhaseBarrier::new(10, 1_000_000);
        let barrier2 = barrier1.clone();
        assert_eq!(barrier1, barrier2);
    }

    #[test]
    fn barrier_equality() {
        let barrier1 = PhaseBarrier::new(11, 1_000_000);
        let mut barrier2 = PhaseBarrier::new(11, 1_000_000);
        assert_eq!(barrier1, barrier2);

        barrier2.mark_progress();
        assert_ne!(barrier1, barrier2);
    }

    #[test]
    fn default_barrier() {
        let barrier = PhaseBarrier::default();
        assert_eq!(barrier.phase_id, 0);
        assert_eq!(barrier.completion_count, 0);
        assert!(!barrier.is_complete);
        assert_eq!(barrier.started_ns, 0);
        assert_eq!(barrier.completed_ns, 0);
    }

    #[test]
    fn mark_progress_saturates() {
        let mut barrier = PhaseBarrier::new(12, 1_000_000);
        barrier.completion_count = u32::MAX;
        barrier.mark_progress();
        assert_eq!(barrier.completion_count, u32::MAX);
    }

    #[test]
    fn mark_complete_can_be_called_multiple_times() {
        let mut barrier = PhaseBarrier::new(13, 1_000_000);
        barrier.mark_complete(2_000_000);
        barrier.mark_complete(3_000_000);
        assert!(barrier.is_done());
        assert_eq!(barrier.completed_ns, 3_000_000);
    }
}
