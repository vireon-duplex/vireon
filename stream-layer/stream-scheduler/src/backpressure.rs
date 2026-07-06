//! [`BackpressureTracker`] — flow-control integration.
//!
//! See `DESIGN.md` Part 8 (flow-control integration).
//!
//! # Role
//!
//! When a stream's send credit is exhausted, the scheduler must
//! skip it until credit arrives (`MAX_STREAM_DATA` from the peer).
//! The `BackpressureTracker` records which streams are blocked and
//! wakes them when credit arrives.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the tracker type. Functional block / wake land in
//! Phase 2 alongside flow-control integration.

use std::collections::HashSet;
use stream_manager::StreamId;

/// Per-tick backpressure tracker.
#[derive(Clone, Debug, Default)]
pub struct BackpressureTracker {
    /// Stream ids that are currently blocked (no send credit).
    blocked: HashSet<u64>,
}

impl BackpressureTracker {
    /// Construct an empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            blocked: HashSet::new(),
        }
    }

    /// Number of currently-blocked streams.
    #[must_use]
    pub fn blocked_count(&self) -> usize {
        self.blocked.len()
    }

    /// Whether any stream is blocked.
    #[must_use]
    pub fn any_blocked(&self) -> bool {
        !self.blocked.is_empty()
    }

    /// Mark a stream as blocked (no send credit).
    pub fn block(&mut self, id: StreamId) {
        self.blocked.insert(id.raw());
    }

    /// Mark a stream as unblocked (credit arrived).
    pub fn unblock(&mut self, id: StreamId) {
        self.blocked.remove(&id.raw());
    }

    /// Whether a stream is currently blocked.
    #[must_use]
    pub fn is_blocked(&self, id: StreamId) -> bool {
        self.blocked.contains(&id.raw())
    }

    /// Clear all blocked state (used on connection reset).
    pub fn clear(&mut self) {
        self.blocked.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tracker() {
        let t = BackpressureTracker::new();
        assert_eq!(t.blocked_count(), 0);
        assert!(!t.any_blocked());
    }

    #[test]
    fn block_and_query() {
        let mut t = BackpressureTracker::new();
        let id = StreamId::new_unchecked(4);
        t.block(id);
        assert!(t.is_blocked(id));
        assert_eq!(t.blocked_count(), 1);
        assert!(t.any_blocked());
    }

    #[test]
    fn unblock_removes() {
        let mut t = BackpressureTracker::new();
        let id = StreamId::new_unchecked(4);
        t.block(id);
        t.unblock(id);
        assert!(!t.is_blocked(id));
        assert!(!t.any_blocked());
    }

    #[test]
    fn clear_resets_all() {
        let mut t = BackpressureTracker::new();
        t.block(StreamId::new_unchecked(0));
        t.block(StreamId::new_unchecked(4));
        t.clear();
        assert_eq!(t.blocked_count(), 0);
    }
}
