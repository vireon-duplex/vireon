//! [`StreamSelector`] — picks the next stream per the active policy.
//!
//! See `DESIGN.md` Part 4 (RFC 9218) and Part 5 (algorithm).
//!
//! # Selection algorithm
//!
//! ```text
//! 1. ready_queue.dequeue()    ── highest urgency FIFO entry
//! 2. budget.consume(bytes)    ── debit the tick budget
//! 3. return Selection          ── record in stats
//! ```
//!
//! For strict-priority and hybrid policies, the queue's strict-band
//! scan already enforces priority ordering. For round-robin policies,
//! a per-band cursor rotates the starting band.

use crate::budget::SchedulingBudget;
use crate::error::SchedulerError;
use crate::policy::SchedulingPolicy;
use crate::queue::ReadyQueue;
use profiler::profile;
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
/// Holds the active policy and optional round-robin cursor. The
/// `select()` method dequeues from the [`ReadyQueue`] and debits
/// the [`SchedulingBudget`].
pub struct StreamSelector {
    policy: SchedulingPolicy,
    /// Round-robin cursor (rotates the scan start band).
    rr_cursor: u8,
}

impl StreamSelector {
    /// Construct a selector bound to a policy.
    #[must_use]
    pub const fn new(policy: SchedulingPolicy) -> Self {
        Self {
            policy,
            rr_cursor: 0,
        }
    }

    /// The active policy.
    #[must_use]
    pub const fn policy(&self) -> SchedulingPolicy {
        self.policy
    }

    /// Select the next stream from the queue, debiting budget.
    ///
    /// # Errors
    /// - [`SchedulerError::NoStreamsReady`] when the queue is empty.
    /// - [`SchedulerError::BudgetExhausted`] when the tick budget
    ///   is spent or the entry doesn't fit.
    #[profile]
    pub fn select(
        &mut self,
        queue: &mut ReadyQueue,
        budget: &mut SchedulingBudget,
    ) -> Result<Selection, SchedulerError> {
        if budget.is_exhausted() {
            return Err(SchedulerError::BudgetExhausted);
        }

        let entry = match self.policy {
            SchedulingPolicy::Fifo
            | SchedulingPolicy::StrictPriority
            | SchedulingPolicy::Hybrid
            | SchedulingPolicy::WeightedRoundRobin
            | SchedulingPolicy::DeficitRoundRobin
            | SchedulingPolicy::WeightedFair => queue.dequeue(),
            SchedulingPolicy::RoundRobin => self.dequeue_round_robin(queue),
        }
        .ok_or(SchedulerError::NoStreamsReady)?;

        // Attempt to debit the budget for this entry's bytes.
        let bytes = entry.bytes_available;
        let bytes = if bytes == 0 {
            // Zero-byte selection — just consume a slot.
            if !budget.consume_slot() {
                let _ = queue.enqueue(entry);
                return Err(SchedulerError::BudgetExhausted);
            }
            0
        } else {
            if !budget.consume(bytes) {
                // Entry doesn't fit remaining budget — re-queue.
                let _ = queue.enqueue(entry);
                return Err(SchedulerError::BudgetExhausted);
            }
            bytes
        };

        Ok(Selection::new(entry.id, entry.priority.urgency, bytes))
    }

    /// Round-robin dequeue: rotate the starting band each call to
    /// give each urgency band a fair turn.
    #[profile]
    fn dequeue_round_robin(&mut self, queue: &mut ReadyQueue) -> Option<crate::queue::ReadyEntry> {
        let start = self.rr_cursor as usize % crate::queue::URGENCY_BANDS;
        self.rr_cursor = (self.rr_cursor + 1) % crate::queue::URGENCY_BANDS as u8;

        // Scan starting from `start`, wrapping around.
        for offset in 0..crate::queue::URGENCY_BANDS {
            let band = (start + offset) % crate::queue::URGENCY_BANDS;
            if let Some(entry) = queue.dequeue_band(band) {
                return Some(entry);
            }
        }
        None
    }
}

impl Default for StreamSelector {
    fn default() -> Self {
        Self::new(SchedulingPolicy::default())
    }
}

impl Clone for StreamSelector {
    fn clone(&self) -> Self {
        Self {
            policy: self.policy,
            rr_cursor: self.rr_cursor,
        }
    }
}

impl std::fmt::Debug for StreamSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSelector")
            .field("policy", &self.policy)
            .field("rr_cursor", &self.rr_cursor)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::ReadyEntry;
    use stream_manager::StreamPriority;

    fn make_entry(id: u64, urgency: u8, bytes: u64) -> ReadyEntry {
        ReadyEntry::new(
            StreamId::new_unchecked(id),
            StreamPriority::with_urgency(urgency),
            bytes,
        )
    }

    #[test]
    fn default_uses_hybrid_policy() {
        let s = StreamSelector::default();
        assert_eq!(s.policy(), SchedulingPolicy::Hybrid);
    }

    #[test]
    fn select_empty_queue_errors() {
        let mut sel = StreamSelector::default();
        let mut queue = ReadyQueue::default();
        let mut budget = SchedulingBudget::default();
        assert!(matches!(
            sel.select(&mut queue, &mut budget),
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

    #[test]
    fn strict_priority_selects_highest_urgency() {
        let mut sel = StreamSelector::new(SchedulingPolicy::StrictPriority);
        let mut queue = ReadyQueue::default();
        let mut budget = SchedulingBudget::new(10, 100_000, 100, 1_000_000);

        queue.enqueue(make_entry(1, 5, 100)).unwrap();
        queue.enqueue(make_entry(2, 0, 100)).unwrap();
        queue.enqueue(make_entry(3, 3, 100)).unwrap();

        let sel = sel.select(&mut queue, &mut budget).unwrap();
        assert_eq!(sel.id, StreamId::new_unchecked(2)); // urgency 0
        assert_eq!(sel.effective_urgency, 0);
    }

    #[test]
    fn budget_debited_after_select() {
        let mut sel = StreamSelector::default();
        let mut queue = ReadyQueue::default();
        let mut budget = SchedulingBudget::new(10, 10_000, 100, 1_000_000);

        queue.enqueue(make_entry(1, 0, 500)).unwrap();
        let sel = sel.select(&mut queue, &mut budget).unwrap();
        assert_eq!(sel.bytes, 500);
        assert_eq!(budget.bytes_consumed(), 500);
        assert_eq!(budget.streams_consumed(), 1);
    }

    #[test]
    fn budget_exhausted_returns_error() {
        let mut sel = StreamSelector::default();
        let mut queue = ReadyQueue::default();
        let mut budget = SchedulingBudget::new(1, 1000, 100, 1_000_000);

        queue.enqueue(make_entry(1, 0, 500)).unwrap();
        assert!(sel.select(&mut queue, &mut budget).is_ok());
        // Budget now exhausted (1 stream consumed).
        queue.enqueue(make_entry(2, 0, 500)).unwrap();
        assert!(matches!(
            sel.select(&mut queue, &mut budget),
            Err(SchedulerError::BudgetExhausted)
        ));
    }

    #[test]
    fn entry_too_large_requeues() {
        let mut sel = StreamSelector::default();
        let mut queue = ReadyQueue::default();
        let mut budget = SchedulingBudget::new(10, 100, 100, 1_000_000);

        queue.enqueue(make_entry(1, 0, 500)).unwrap();
        // 500 bytes > 100 budget — should re-queue and error.
        assert!(matches!(
            sel.select(&mut queue, &mut budget),
            Err(SchedulerError::BudgetExhausted)
        ));
        // Entry should still be in the queue.
        assert_eq!(queue.total(), 1);
    }

    #[test]
    fn round_robin_rotates_bands() {
        let mut sel = StreamSelector::new(SchedulingPolicy::RoundRobin);
        let mut queue = ReadyQueue::default();
        let mut budget = SchedulingBudget::new(10, 100_000, 100, 1_000_000);

        queue.enqueue(make_entry(1, 0, 100)).unwrap();
        queue.enqueue(make_entry(2, 2, 100)).unwrap();

        // First call starts at cursor 0 → band 0.
        let s1 = sel.select(&mut queue, &mut budget).unwrap();
        assert_eq!(s1.id, StreamId::new_unchecked(1));
        // Second call starts at cursor 1 → band 1, then scans to band 2.
        let s2 = sel.select(&mut queue, &mut budget).unwrap();
        assert_eq!(s2.id, StreamId::new_unchecked(2));
    }
}
