//! [`SessionScheduler`] — per-class scheduling and admission control.
//!
//! See `DESIGN.md` Part 10 (session scheduling).
//!
//! # Scheduling model
//!
//! Sessions are classified into one of four
//! [`SessionClass`](crate::priority::SessionClass)es. The scheduler
//! uses these classes to:
//!
//! - **Admission control** — reject new sessions when capacity is
//!   low, prioritising high-urgency classes.
//! - **WFQ (Weighted Fair Queuing)** — allocate reactor ticks per
//!   class according to weight.
//! - **Preemption** — latency-sensitive sessions can preempt bulk
//!   sessions on the same reactor core.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the type signature + admission decision shape.
//! Functional scheduling lands in Phase 2+.

use crate::error::SessionManagerError;
use crate::priority::SessionClass;

/// Admission decision returned by the scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Session admitted; reactor core assigned.
    Admitted { core: u8 },
    /// Session queued; will be admitted when capacity frees.
    Queued,
    /// Session rejected (capacity / DoS protection).
    Rejected,
}

/// Per-class counters used by the scheduler.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SchedulerClassStats {
    /// Currently-admitted sessions in this class.
    pub active: u64,
    /// Cumulative admitted sessions.
    pub admitted_total: u64,
    /// Cumulative rejected sessions.
    pub rejected_total: u64,
}

/// Session scheduler.
///
/// Phase 0 holds no state; Phase 2+ will add the per-class queues
/// and the WFQ state.
// TODO(phase-2): per-class queues + WFQ state.
#[derive(Clone, Debug, Default)]
pub struct SessionScheduler {
    class_stats: [SchedulerClassStats; 4],
}

impl SessionScheduler {
    /// Construct a new scheduler.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            class_stats: [SchedulerClassStats::new(); 4],
        }
    }

    /// Per-class stats (index = `SessionClass` discriminant).
    #[must_use]
    pub const fn class_stats(&self, class_index: usize) -> SchedulerClassStats {
        if class_index < self.class_stats.len() {
            self.class_stats[class_index]
        } else {
            SchedulerClassStats::new()
        }
    }

    /// Phase-2 `admit` placeholder.
    ///
    /// # Errors
    /// Always returns [`SessionManagerError::SessionLimitReached`]
    /// in Phase 0.
    pub fn admit(
        &mut self,
        _class: SessionClass,
    ) -> Result<AdmissionDecision, SessionManagerError> {
        // TODO(phase-2): WFQ + admission control.
        Err(SessionManagerError::SessionLimitReached)
    }

    /// Phase-2 `release` placeholder.
    pub fn release(&mut self, _class: SessionClass, _core: u8) {
        // TODO(phase-2): decrement per-class counter.
    }
}

impl SchedulerClassStats {
    /// Construct zeroed stats.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            active: 0,
            admitted_total: 0,
            rejected_total: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_scheduler_has_zero_stats() {
        let s = SessionScheduler::new();
        for i in 0..4 {
            assert_eq!(s.class_stats(i).active, 0);
        }
    }

    #[test]
    fn admit_phase0_stub() {
        let mut s = SessionScheduler::new();
        assert!(matches!(
            s.admit(SessionClass::default()),
            Err(SessionManagerError::SessionLimitReached)
        ));
    }

    #[test]
    fn class_index_out_of_range_returns_default() {
        let s = SessionScheduler::new();
        assert_eq!(s.class_stats(99), SchedulerClassStats::default());
    }
}
