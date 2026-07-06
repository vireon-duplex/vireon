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
//!
//! # Phase 0 status
//!
//! Phase 0 ships the facade shape. Functional `tick()` lands in
//! Phase 1.

use crate::budget::SchedulingBudget;
use crate::config::SchedulerConfig;
use crate::error::SchedulerError;
use crate::queue::ReadyQueue;
use crate::selector::Selection;
use crate::stats::SchedulerStats;

/// Per-connection stream scheduler.
///
/// Phase 0 holds the component parts; Phase 1 adds the functional
/// `tick()` surface.
// TODO(phase-1): functional tick().
#[allow(dead_code)]
pub struct StreamScheduler {
    cfg: SchedulerConfig,
    queue: ReadyQueue,
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

    /// Phase-1 `tick` placeholder. Phase 1 will return the
    /// selections for this tick.
    ///
    /// # Errors
    /// - [`SchedulerError::NoStreamsReady`] when the queue is empty.
    /// - [`SchedulerError::BudgetExhausted`] when the budget is
    ///   spent.
    pub fn tick(
        &mut self,
        _budget: &mut SchedulingBudget,
    ) -> Result<Vec<Selection>, SchedulerError> {
        // TODO(phase-1): drain manager notifications, apply aging,
        // select per policy, compose batch, return selections.
        self.stats.record_tick(self.queue.total());
        Err(SchedulerError::NoStreamsReady)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn tick_phase0_stub() {
        let mut s = StreamScheduler::new(SchedulerConfig::default()).expect("ok");
        let mut budget = SchedulingBudget::default();
        assert!(s.tick(&mut budget).is_err());
    }
}
