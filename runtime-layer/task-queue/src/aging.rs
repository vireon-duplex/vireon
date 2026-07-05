//! AgingTracker — starvation prevention (Layer 19).
//!
//! Tasks in lower-priority bands can be starved if higher-priority
//! bands are continuously fed at or above the drain rate. The
//! [`AgingTracker`] watches each band's oldest enqueue time and
//! promotes a starved task to the next-higher band.

use crate::priority::TaskPriority;
use crate::ready::ReadyQueue;

/// Per-band aging tracker. Records nothing per-task — it relies on the
/// [`ReadyQueue`] exposing the oldest enqueue timestamp per band.
pub struct AgingTracker {
    /// Threshold (ns) after which a band's oldest task is promoted.
    threshold_ns: u64,
}

impl AgingTracker {
    /// Creates a tracker with the given promotion threshold.
    #[must_use]
    pub const fn new(threshold_ns: u64) -> Self {
        Self { threshold_ns }
    }

    /// Returns the configured threshold.
    #[must_use]
    pub const fn threshold_ns(&self) -> u64 {
        self.threshold_ns
    }

    /// Inspects each lower band (`Background`, `Low`, `Normal`) and, if
    /// its oldest entry has waited past the threshold, promotes one
    /// entry to the next-higher band.
    ///
    /// At most one promotion per band per call — bounded O(3) work.
    /// The next call (next tick) handles the next starved entry.
    pub fn apply(&self, ready: &mut ReadyQueue, now_ns: u64) -> usize {
        let mut promoted = 0;
        // Walk bands lowest-first so a promoted entry can cascade
        // upward in subsequent ticks (not the same call).
        let ladder = [
            (TaskPriority::Background, TaskPriority::Low),
            (TaskPriority::Low, TaskPriority::Normal),
            (TaskPriority::Normal, TaskPriority::High),
        ];
        for (from, to) in ladder {
            if let Some(oldest) = ready.oldest_enqueue_ns(from) {
                let age = now_ns.saturating_sub(oldest);
                if age > self.threshold_ns && ready.promote_one(from, to, now_ns).is_some() {
                    promoted += 1;
                }
            }
        }
        promoted
    }
}

impl std::fmt::Debug for AgingTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgingTracker")
            .field("threshold_ns", &self.threshold_ns)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_set() {
        let t = AgingTracker::new(5_000);
        assert_eq!(t.threshold_ns(), 5_000);
    }

    #[test]
    fn apply_promotes_old_entries() {
        let mut ready = ReadyQueue::new(16);
        ready.push(TaskPriority::Low, 1, 100).expect("push");
        let tracker = AgingTracker::new(1_000);
        // At now=100, age 0 — no promotion.
        assert_eq!(tracker.apply(&mut ready, 100), 0);
        // At now=2000, age 1900 > 1000 — promote.
        let n = tracker.apply(&mut ready, 2000);
        assert_eq!(n, 1);
        // The Low band is now empty.
        assert_eq!(ready.depth_of(TaskPriority::Low), 0);
        assert_eq!(ready.depth_of(TaskPriority::Normal), 1);
    }

    #[test]
    fn apply_promotes_at_most_one_per_band() {
        let mut ready = ReadyQueue::new(16);
        ready.push(TaskPriority::Low, 1, 0).expect("push");
        ready.push(TaskPriority::Low, 2, 0).expect("push");
        let tracker = AgingTracker::new(100);
        let n = tracker.apply(&mut ready, 1000);
        assert_eq!(n, 1);
        assert_eq!(ready.depth_of(TaskPriority::Low), 1);
    }

    #[test]
    fn empty_bands_no_op() {
        let mut ready = ReadyQueue::new(16);
        let tracker = AgingTracker::new(100);
        assert_eq!(tracker.apply(&mut ready, 1_000_000), 0);
    }
}
