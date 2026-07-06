//! [`AgingTracker`] — starvation prevention for low-priority streams.
//!
//! See `DESIGN.md` Part 6 (fairness) and Part 4 (RFC 9218).
//!
//! # Why aging?
//!
//! Strict-priority scheduling (RFC 9218 urgency bands) can starve
//! low-urgency (high numeric value) streams. Aging prevents this by
//! promoting a stream's urgency after it has waited longer than
//! `starvation_threshold_us`.
//!
//! # Algorithm
//!
//! ```text
//! for each ready stream:
//!     waited_ns = now_ns - stream.last_scheduled_ns
//!     if waited_ns > starvation_threshold:
//!         promote(stream)  // decrement urgency by `boost_delta`
//! ```
//!
//! Promotion is temporary (per-tick); the stream's canonical
//! priority is unchanged.

use stream_manager::{StreamId, StreamPriority, URGENCY_MIN};

/// Default urgency boost applied on promotion (e.g. urgency 5 → 3).
pub const DEFAULT_BOOST_DELTA: u8 = 2;

/// Per-stream aging record. The scheduler holds one per ready stream.
#[derive(Clone, Copy, Debug)]
pub struct AgingEntry {
    /// The stream being tracked.
    pub id: StreamId,
    /// Canonical priority (from the stream manager).
    pub base_priority: StreamPriority,
    /// Nanoseconds since manager start when the stream last had
    /// bytes scheduled.
    pub last_scheduled_ns: u64,
}

impl AgingEntry {
    /// Construct a new entry.
    #[must_use]
    pub const fn new(id: StreamId, base_priority: StreamPriority, now_ns: u64) -> Self {
        Self {
            id,
            base_priority,
            last_scheduled_ns: now_ns,
        }
    }

    /// Nanoseconds the stream has waited since its last scheduling.
    #[must_use]
    pub const fn waited_ns(&self, now_ns: u64) -> u64 {
        now_ns.saturating_sub(self.last_scheduled_ns)
    }

    /// Whether the stream should be promoted this tick.
    #[must_use]
    pub const fn should_promote(&self, now_ns: u64, threshold_ns: u64) -> bool {
        self.waited_ns(now_ns) >= threshold_ns
    }

    /// Effective priority after applying aging. The canonical
    /// priority is unchanged; only the effective urgency is reduced.
    #[must_use]
    pub const fn effective_priority(
        &self,
        now_ns: u64,
        threshold_ns: u64,
        boost: u8,
    ) -> StreamPriority {
        if self.should_promote(now_ns, threshold_ns) {
            let new_urgency = if self.base_priority.urgency >= boost {
                self.base_priority.urgency - boost
            } else {
                URGENCY_MIN
            };
            StreamPriority {
                urgency: new_urgency,
                incremental: self.base_priority.incremental,
            }
        } else {
            self.base_priority
        }
    }

    /// Mark that the stream was scheduled this tick (resets the
    /// wait timer).
    pub fn mark_scheduled(&mut self, now_ns: u64) {
        self.last_scheduled_ns = now_ns;
    }
}

/// Aging tracker — holds per-stream entries and applies promotion
/// logic.
///
/// Phase 0 ships the entry + promotion logic. Phase 1 will integrate
/// this with the `ReadyQueue` so the scheduler consults it every
/// tick.
#[derive(Clone, Debug, Default)]
pub struct AgingTracker {
    /// Promotion threshold in ns.
    threshold_ns: u64,
    /// Urgency reduction on promotion.
    boost_delta: u8,
}

impl AgingTracker {
    /// Construct a tracker with the given threshold and boost.
    #[must_use]
    pub const fn new(threshold_ns: u64, boost_delta: u8) -> Self {
        Self {
            threshold_ns,
            boost_delta,
        }
    }

    /// The configured promotion threshold (ns).
    #[must_use]
    pub const fn threshold_ns(&self) -> u64 {
        self.threshold_ns
    }

    /// The configured urgency boost on promotion.
    #[must_use]
    pub const fn boost_delta(&self) -> u8 {
        self.boost_delta
    }

    /// Check whether an entry should be promoted.
    #[must_use]
    pub const fn should_promote(&self, entry: &AgingEntry, now_ns: u64) -> bool {
        entry.should_promote(now_ns, self.threshold_ns)
    }

    /// Compute the effective priority for an entry this tick.
    #[must_use]
    pub const fn effective_priority(&self, entry: &AgingEntry, now_ns: u64) -> StreamPriority {
        entry.effective_priority(now_ns, self.threshold_ns, self.boost_delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(urgency: u8, last_ns: u64) -> AgingEntry {
        AgingEntry::new(
            StreamId::new_unchecked(0),
            StreamPriority::with_urgency(urgency),
            last_ns,
        )
    }

    #[test]
    fn no_promotion_under_threshold() {
        let t = AgingTracker::new(10_000, DEFAULT_BOOST_DELTA);
        let e = entry(5, 0);
        let eff = t.effective_priority(&e, 5_000);
        assert_eq!(eff.urgency, 5);
    }

    #[test]
    fn promotion_above_threshold() {
        let t = AgingTracker::new(10_000, DEFAULT_BOOST_DELTA);
        let e = entry(5, 0);
        let eff = t.effective_priority(&e, 15_000);
        assert_eq!(eff.urgency, 3); // 5 - 2
    }

    #[test]
    fn promotion_clamps_at_min() {
        let t = AgingTracker::new(10_000, 5);
        let e = entry(2, 0); // urgency 2, boost 5
        let eff = t.effective_priority(&e, 15_000);
        assert_eq!(eff.urgency, URGENCY_MIN);
    }

    #[test]
    fn mark_scheduled_resets_wait() {
        let mut e = entry(5, 0);
        e.mark_scheduled(10_000);
        assert_eq!(e.waited_ns(12_000), 2_000);
    }

    #[test]
    fn incremental_flag_preserved() {
        let t = AgingTracker::new(10_000, 2);
        let mut e = entry(5, 0);
        e.base_priority.incremental = true;
        let eff = t.effective_priority(&e, 15_000);
        assert!(eff.incremental);
        assert_eq!(eff.urgency, 3);
    }
}
