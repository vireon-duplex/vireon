//! [`SessionScheduler`] — per-class admission control.
//!
//! See `DESIGN.md` Part 10 (session scheduling).
//!
//! # Scheduling model
//!
//! Sessions are classified into one of four
//! [`SessionClass`](crate::priority::SessionClass)es. The scheduler
//! uses these classes for **admission control** — reject new sessions
//! when a class is at capacity.
//!
//! # Phase 2
//!
//! Functional admission control with per-class limits derived from
//! `max_sessions`. WFQ, preemption, and queuing remain Phase 3+.

use crate::error::SessionManagerError;
use crate::priority::SessionClass;

/// Number of session classes (matches `SessionClass::all().len()`).
const NUM_CLASSES: usize = 4;

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

/// Session scheduler — per-class admission control.
///
/// Each class has an independent session cap (`class_limits`). When a
/// class is at capacity, [`admit`](Self::admit) returns
/// [`AdmissionDecision::Rejected`]. The caller is responsible for
/// translating that into an error or a queue action.
#[derive(Clone, Debug)]
pub struct SessionScheduler {
    /// Active session count per class.
    class_counts: [u32; NUM_CLASSES],
    /// Maximum sessions per class.
    class_limits: [u32; NUM_CLASSES],
    /// Per-class cumulative stats.
    class_stats: [SchedulerClassStats; NUM_CLASSES],
}

impl Default for SessionScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionScheduler {
    /// Construct a scheduler with generous default limits.
    ///
    /// Each class defaults to `u32::MAX / 4` so admission control is
    /// effectively disabled unless the caller tightens the limits via
    /// [`set_class_limit`](Self::set_class_limit).
    #[must_use]
    pub const fn new() -> Self {
        let limit = u32::MAX / 4;
        Self {
            class_counts: [0; NUM_CLASSES],
            class_limits: [limit; NUM_CLASSES],
            class_stats: [SchedulerClassStats::new(); NUM_CLASSES],
        }
    }

    /// Construct a scheduler with per-class limits derived from
    /// `max_sessions`. The total is split evenly across the four
    /// classes.
    #[must_use]
    pub fn with_max_sessions(max_sessions: u64) -> Self {
        let per_class = (max_sessions / NUM_CLASSES as u64).min(u32::MAX as u64) as u32;
        Self {
            class_counts: [0; NUM_CLASSES],
            class_limits: [per_class; NUM_CLASSES],
            class_stats: [SchedulerClassStats::new(); NUM_CLASSES],
        }
    }

    /// Sets the session cap for a specific class.
    pub fn set_class_limit(&mut self, class: SessionClass, limit: u32) {
        self.class_limits[class as usize] = limit;
    }

    /// Current active count for a class.
    #[must_use]
    pub const fn active_for_class(&self, class: SessionClass) -> u32 {
        self.class_counts[class as usize]
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

    /// Attempts to admit a session of the given class.
    ///
    /// On success, increments the class counter and returns
    /// [`AdmissionDecision::Admitted`]. On capacity exhaustion returns
    /// [`AdmissionDecision::Rejected`].
    ///
    /// # Errors
    /// Returns [`SessionManagerError::SessionLimitReached`] only if the
    /// caller translates `Rejected` into an error. The method itself
    /// returns `Ok(AdmissionDecision)` so the caller can choose to
    /// queue rather than reject.
    pub fn admit(&mut self, class: SessionClass) -> Result<AdmissionDecision, SessionManagerError> {
        let idx = class as usize;
        let count = self.class_counts[idx];
        if count >= self.class_limits[idx] {
            self.class_stats[idx].rejected_total += 1;
            return Ok(AdmissionDecision::Rejected);
        }
        self.class_counts[idx] = count + 1;
        let stats = &mut self.class_stats[idx];
        stats.active += 1;
        stats.admitted_total += 1;
        Ok(AdmissionDecision::Admitted { core: 0 })
    }

    /// Releases a session of the given class (decrements the counter).
    pub fn release(&mut self, class: SessionClass) {
        let idx = class as usize;
        if self.class_counts[idx] > 0 {
            self.class_counts[idx] -= 1;
        }
        if self.class_stats[idx].active > 0 {
            self.class_stats[idx].active -= 1;
        }
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
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn new_scheduler_has_zero_stats() {
        let s = SessionScheduler::new();
        for i in 0..NUM_CLASSES {
            assert_eq!(s.class_stats(i).active, 0);
        }
    }

    #[test]
    fn admit_increments_active() {
        let mut s = SessionScheduler::new();
        let d = s.admit(SessionClass::Interactive).unwrap();
        assert!(matches!(d, AdmissionDecision::Admitted { .. }));
        assert_eq!(s.active_for_class(SessionClass::Interactive), 1);
        assert_eq!(
            s.class_stats(SessionClass::Interactive as usize)
                .admitted_total,
            1
        );
    }

    #[test]
    fn release_decrements_active() {
        let mut s = SessionScheduler::new();
        s.admit(SessionClass::Interactive).unwrap();
        s.admit(SessionClass::Interactive).unwrap();
        assert_eq!(s.active_for_class(SessionClass::Interactive), 2);
        s.release(SessionClass::Interactive);
        assert_eq!(s.active_for_class(SessionClass::Interactive), 1);
    }

    #[test]
    fn admit_rejects_at_limit() {
        let mut s = SessionScheduler::new();
        s.set_class_limit(SessionClass::Bulk, 2);
        assert!(s.admit(SessionClass::Bulk).unwrap().is_admitted());
        assert!(s.admit(SessionClass::Bulk).unwrap().is_admitted());
        assert_eq!(
            s.admit(SessionClass::Bulk).unwrap(),
            AdmissionDecision::Rejected
        );
        assert_eq!(s.class_stats(SessionClass::Bulk as usize).rejected_total, 1);
    }

    #[test]
    fn with_max_sessions_derives_limits() {
        let s = SessionScheduler::with_max_sessions(4_000);
        // 4000 / 4 = 1000 per class.
        for class in SessionClass::all() {
            assert_eq!(s.active_for_class(class), 0);
        }
        // Verify limit by admitting until rejection.
        let mut s = s;
        s.set_class_limit(SessionClass::Interactive, 3);
        for _ in 0..3 {
            assert!(s.admit(SessionClass::Interactive).unwrap().is_admitted());
        }
        assert_eq!(
            s.admit(SessionClass::Interactive).unwrap(),
            AdmissionDecision::Rejected
        );
    }

    #[test]
    fn class_index_out_of_range_returns_default() {
        let s = SessionScheduler::new();
        assert_eq!(s.class_stats(99), SchedulerClassStats::default());
    }

    #[test]
    fn release_below_zero_clamps() {
        let mut s = SessionScheduler::new();
        s.release(SessionClass::Streaming); // should not underflow
        assert_eq!(s.active_for_class(SessionClass::Streaming), 0);
    }
}

/// Convenience predicate on [`AdmissionDecision`].
impl AdmissionDecision {
    /// `true` when the decision is [`Admitted`](Self::Admitted).
    #[must_use]
    pub const fn is_admitted(self) -> bool {
        matches!(self, Self::Admitted { .. })
    }
}
