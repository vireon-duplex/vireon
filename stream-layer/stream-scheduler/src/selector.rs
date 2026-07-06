//! [`StreamSelector`] — picks the next stream per the active policy.
//!
//! See `DESIGN.md` Part 4 (RFC 9218) and Part 5 (algorithm).
//!
//! # Selection algorithm (Hybrid policy, default)
//!
//! ```text
//! 1. bitmap.first_non_empty_band()    ── highest urgency with data
//! 2. aging.effective_priority(entry)  ── apply starvation boost
//! 3. within band: round-robin          ── fairness within urgency
//! 4. budget.consume(bytes)             ── debit the tick budget
//! 5. stats.record_selection(...)       ── atomic counters
//! ```
//!
//! # Phase 0 status
//!
//! Phase 0 ships the selection-result type. Functional `select()`
//! lands in Phase 1 alongside the `ReadyQueue`.

use crate::budget::SchedulingBudget;
use crate::error::SchedulerError;
use crate::policy::SchedulingPolicy;
use stream_manager::StreamId;

/// The result of a single stream selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Selection {
    /// The selected stream.
    pub id: StreamId,
    /// Effective urgency used for this selection (post-aging).
    pub effective_urgency: u8,
    /// Bytes scheduled for this stream this tick.
    pub bytes: u64,
}

impl Selection {
    /// Construct a new selection record.
    #[must_use]
    pub const fn new(id: StreamId, effective_urgency: u8, bytes: u64) -> Self {
        Self {
            id,
            effective_urgency,
            bytes,
        }
    }
}

/// Per-tick stream selector.
///
/// Phase 0 ships the type; Phase 1 will add the `select()` method
/// that consults the [`ReadyQueue`](crate::queue::ReadyQueue) and
/// applies the active [`SchedulingPolicy`].
// TODO(phase-1): functional select() per policy.
#[derive(Clone, Debug)]
pub struct StreamSelector {
    policy: SchedulingPolicy,
}

impl StreamSelector {
    /// Construct a selector bound to a policy.
    #[must_use]
    pub const fn new(policy: SchedulingPolicy) -> Self {
        Self { policy }
    }

    /// The active policy.
    #[must_use]
    pub const fn policy(&self) -> SchedulingPolicy {
        self.policy
    }

    /// Phase-1 `select` placeholder. Phase 1 will return the next
    /// `Selection` per the active policy.
    ///
    /// # Errors
    /// - [`SchedulerError::NoStreamsReady`] when the queue is empty.
    /// - [`SchedulerError::BudgetExhausted`] when the tick budget
    ///   is spent.
    pub fn select(&self, _budget: &mut SchedulingBudget) -> Result<Selection, SchedulerError> {
        // TODO(phase-1): dequeue from ReadyQueue, apply aging, debit budget.
        Err(SchedulerError::NoStreamsReady)
    }
}

impl Default for StreamSelector {
    fn default() -> Self {
        Self::new(SchedulingPolicy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_uses_hybrid_policy() {
        let s = StreamSelector::default();
        assert_eq!(s.policy(), SchedulingPolicy::Hybrid);
    }

    #[test]
    fn select_returns_no_streams_in_phase0() {
        let sel = StreamSelector::default();
        let mut budget = SchedulingBudget::default();
        assert!(matches!(
            sel.select(&mut budget),
            Err(SchedulerError::NoStreamsReady)
        ));
    }

    #[test]
    fn selection_carries_metadata() {
        let sel = Selection::new(StreamId::new_unchecked(4), 2, 1024);
        assert_eq!(sel.id.raw(), 4);
        assert_eq!(sel.effective_urgency, 2);
        assert_eq!(sel.bytes, 1024);
    }
}
