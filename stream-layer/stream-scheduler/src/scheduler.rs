//! [`StreamScheduler`] — facade that owns the queue, selector,
//! aging tracker, and stats.
//!
//! See `DESIGN.md` Part 1 (responsibilities) and Part 5 (algorithm).
//!
//! # Role
//!
//! One `StreamScheduler` exists per connection (alongside the
//! [`StreamManager`](stream_manager)). Each reactor tick, the
//! scheduler:
//!
//! 1. Drains ready-stream notifications from the manager.
//! 2. Applies aging to detect starving streams.
//! 3. Selects the next batch per the active policy.
//! 4. Hands the batch to the send-queue for packetization.

use crate::budget::SchedulingBudget;
use crate::config::SchedulerConfig;
use crate::error::SchedulerError;
use crate::queue::{ReadyEntry, ReadyQueue};
use crate::selector::{Selection, StreamSelector};
use crate::stats::SchedulerStats;
use profiler::profile;
use stream_manager::StreamId;

/// Per-connection stream scheduler.
///
/// Owns the ready queue, selector, and stats. Callers submit ready
/// streams via [`submit`](Self::submit) and drain selections via
/// [`tick`](Self::tick).
pub struct StreamScheduler {
    cfg: SchedulerConfig,
    queue: ReadyQueue,
    selector: StreamSelector,
    stats: SchedulerStats,
}

impl StreamScheduler {
    /// Construct a scheduler from a validated config.
    ///
    /// # Errors
    /// Returns [`SchedulerError::InvalidConfig`] if validation fails.
    pub fn new(cfg: SchedulerConfig) -> Result<Self, SchedulerError> {
        cfg.validate()?;
        Ok(Self {
            queue: ReadyQueue::default(),
            selector: StreamSelector::new(cfg.policy),
            stats: SchedulerStats::new(),
            cfg,
        })
    }

    /// Borrow the config.
    #[must_use]
    pub const fn config(&self) -> &SchedulerConfig {
        &self.cfg
    }

    /// Borrow the live stats counters.
    #[must_use]
    pub const fn stats(&self) -> &SchedulerStats {
        &self.stats
    }

    /// Total ready-stream count.
    #[must_use]
    pub fn ready_count(&self) -> u64 {
        self.queue.total()
    }

    /// Submit a ready stream for scheduling.
    ///
    /// The stream is placed in its urgency band's FIFO queue and
    /// will be considered on the next [`tick`](Self::tick).
    ///
    /// # Errors
    /// Returns [`SchedulerError::BackpressureStall`] if the band is full.
    pub fn submit(&mut self, entry: ReadyEntry) -> Result<(), SchedulerError> {
        self.queue.enqueue(entry)
    }

    /// Submit a batch of ready streams.
    ///
    /// Stops on the first error and returns it. Entries before the
    /// error are already enqueued.
    ///
    /// # Errors
    /// Returns the first enqueue error.
    #[profile]
    pub fn submit_batch(
        &mut self,
        entries: impl IntoIterator<Item = ReadyEntry>,
    ) -> Result<(), SchedulerError> {
        for entry in entries {
            self.queue.enqueue(entry)?;
        }
        Ok(())
    }

    /// Remove a stream from the ready queue (e.g. on stream close).
    ///
    /// Returns `true` if the stream was found and removed.
    pub fn cancel(&mut self, id: StreamId) -> bool {
        self.queue.remove(id)
    }

    /// Execute one scheduling tick.
    ///
    /// Selects up to `batch_size_max` streams (bounded by budget),
    /// debits the budget, and records stats. Returns the selected
    /// batch.
    ///
    /// # Errors
    /// - [`SchedulerError::NoStreamsReady`] when the queue is empty.
    /// - [`SchedulerError::BudgetExhausted`] when the budget is
    ///   already spent at entry.
    #[profile]
    pub fn tick(
        &mut self,
        budget: &mut SchedulingBudget,
    ) -> Result<Vec<Selection>, SchedulerError> {
        self.stats.record_tick(self.queue.total());

        if budget.is_exhausted() {
            return Err(SchedulerError::BudgetExhausted);
        }

        if self.queue.is_empty() {
            return Err(SchedulerError::NoStreamsReady);
        }

        let max_selections = budget.remaining_streams().min(self.cfg.batch_size_max);

        let mut selections = Vec::with_capacity(max_selections as usize);

        for _ in 0..max_selections {
            match self.selector.select(&mut self.queue, budget) {
                Ok(sel) => {
                    self.stats
                        .record_selection(sel.effective_urgency, sel.bytes);
                    selections.push(sel);
                }
                Err(SchedulerError::NoStreamsReady) | Err(SchedulerError::BudgetExhausted) => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        if selections.is_empty() {
            return Err(SchedulerError::NoStreamsReady);
        }

        Ok(selections)
    }

    /// Clear all ready entries (e.g. on connection teardown).
    pub fn clear(&mut self) {
        self.queue.clear();
    }
}

impl std::fmt::Debug for StreamScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamScheduler")
            .field("cfg", &self.cfg)
            .field("ready_count", &self.queue.total())
            .field("policy", &self.cfg.policy)
            .finish()
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::SchedulingPolicy;
    use stream_manager::StreamPriority;

    fn make_entry(id: u64, urgency: u8, bytes: u64) -> ReadyEntry {
        ReadyEntry::new(
            StreamId::new_unchecked(id),
            StreamPriority::with_urgency(urgency),
            bytes,
        )
    }

    #[test]
    fn new_validates_config() {
        let s = StreamScheduler::new(SchedulerConfig::default()).expect("default ok");
        assert_eq!(s.ready_count(), 0);
    }

    #[test]
    fn new_rejects_invalid_config() {
        let bad = SchedulerConfig {
            max_streams_per_tick: 0,
            ..SchedulerConfig::default()
        };
        assert!(StreamScheduler::new(bad).is_err());
    }

    #[test]
    fn tick_empty_queue_errors() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).expect("ok");
        let mut budget = SchedulingBudget::default();
        assert!(matches!(
            s.tick(&mut budget),
            Err(SchedulerError::NoStreamsReady)
        ));
    }

    #[test]
    fn submit_and_tick() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).unwrap();
        let mut budget = SchedulingBudget::new(10, 100_000, 100, 1_000_000);

        s.submit(make_entry(1, 0, 500)).unwrap();
        s.submit(make_entry(2, 1, 300)).unwrap();

        let selections = s.tick(&mut budget).unwrap();
        assert_eq!(selections.len(), 2);
        // Strict priority: urgency 0 first.
        assert_eq!(selections[0].id, StreamId::new_unchecked(1));
        assert_eq!(selections[0].bytes, 500);
        assert_eq!(selections[1].id, StreamId::new_unchecked(2));
        assert_eq!(selections[1].bytes, 300);
    }

    #[test]
    fn tick_respects_batch_size_max() {
        let cfg = SchedulerConfig {
            batch_size_max: 2,
            ..SchedulerConfig::default()
        };
        let mut s = StreamScheduler::new(cfg).unwrap();
        let mut budget = SchedulingBudget::new(100, 1_000_000, 100, 1_000_000);

        for i in 0..5 {
            s.submit(make_entry(i, 0, 100)).unwrap();
        }

        let selections = s.tick(&mut budget).unwrap();
        assert_eq!(selections.len(), 2);
        // Remaining streams still in queue.
        assert_eq!(s.ready_count(), 3);
    }

    #[test]
    fn tick_respects_budget() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).unwrap();
        let mut budget = SchedulingBudget::new(100, 1000, 100, 1_000_000);

        // Each entry is 500 bytes; budget is 1000 → 2 fit.
        s.submit(make_entry(1, 0, 500)).unwrap();
        s.submit(make_entry(2, 0, 500)).unwrap();
        s.submit(make_entry(3, 0, 500)).unwrap();

        let selections = s.tick(&mut budget).unwrap();
        assert_eq!(selections.len(), 2);
        // Third stream remains queued (budget exhausted).
        assert_eq!(s.ready_count(), 1);
    }

    #[test]
    fn tick_updates_stats() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).unwrap();
        let mut budget = SchedulingBudget::new(10, 100_000, 100, 1_000_000);

        s.submit(make_entry(1, 0, 500)).unwrap();
        s.submit(make_entry(2, 3, 200)).unwrap();

        let _ = s.tick(&mut budget).unwrap();

        let snap = s.stats().snapshot();
        assert_eq!(snap.tick_count, 1);
        assert_eq!(snap.selection_count, 2);
        assert_eq!(snap.bytes_scheduled, 700);
        assert_eq!(snap.selections_in_band(0), 1);
        assert_eq!(snap.selections_in_band(3), 1);
    }

    #[test]
    fn cancel_removes_stream() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).unwrap();
        s.submit(make_entry(1, 0, 100)).unwrap();
        s.submit(make_entry(2, 0, 100)).unwrap();

        assert!(s.cancel(StreamId::new_unchecked(1)));
        assert_eq!(s.ready_count(), 1);
        assert!(!s.cancel(StreamId::new_unchecked(99)));
    }

    #[test]
    fn submit_batch_works() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).unwrap();
        let entries = vec![
            make_entry(1, 0, 100),
            make_entry(2, 1, 200),
            make_entry(3, 2, 300),
        ];
        s.submit_batch(entries).unwrap();
        assert_eq!(s.ready_count(), 3);
    }

    #[test]
    fn strict_priority_ordering_in_tick() {
        let cfg = SchedulerConfig {
            policy: SchedulingPolicy::StrictPriority,
            ..SchedulerConfig::default()
        };
        let mut s = StreamScheduler::new(cfg).unwrap();
        let mut budget = SchedulingBudget::new(10, 1_000_000, 100, 1_000_000);

        s.submit(make_entry(1, 5, 100)).unwrap();
        s.submit(make_entry(2, 0, 100)).unwrap();
        s.submit(make_entry(3, 2, 100)).unwrap();

        let selections = s.tick(&mut budget).unwrap();
        assert_eq!(selections[0].id, StreamId::new_unchecked(2)); // urgency 0
        assert_eq!(selections[1].id, StreamId::new_unchecked(3)); // urgency 2
        assert_eq!(selections[2].id, StreamId::new_unchecked(1)); // urgency 5
    }

    #[test]
    fn tick_exhausted_budget_at_entry() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).unwrap();
        let mut budget = SchedulingBudget::new(0, 1000, 100, 1_000_000);
        // max_streams=0 → exhausted immediately.
        assert!(matches!(
            s.tick(&mut budget),
            Err(SchedulerError::BudgetExhausted)
        ));
    }

    #[test]
    fn debug_formatter_works() {
        let s = StreamScheduler::new(SchedulerConfig::default()).unwrap();
        let dbg = format!("{s:?}");
        assert!(dbg.contains("StreamScheduler"));
        assert!(dbg.contains("ready_count"));
    }
}
