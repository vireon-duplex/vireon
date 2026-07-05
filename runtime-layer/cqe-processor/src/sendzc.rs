//! SEND_ZC tracking (Layer 8).
//!
//! In production this module is a thin `id → buf_id` registry so the
//! `process_send` handler can return the correct `buf_id` to recycle
//! when the `SendBufferReclaim` CQE arrives. The application (or Reactor)
//! registers the mapping at submit time via [`SendZcTracker::register`].
//!
//! In debug builds, the tracker additionally pairs Send and Reclaim CQEs
//! by id and panics on out-of-order reclaim — surfacing application bugs
//! that would otherwise silently leak buffers.

use std::collections::HashMap;

/// Tracks `id → buf_id` mappings for SEND_ZC completions, plus (in debug)
/// the lifecycle phase of each in-flight send.
pub struct SendZcTracker {
    /// Maps the 60-bit id portion of user_data to the application-provided
    /// `buf_index`. Set on `register`, consumed on `SendBufferReclaim`.
    buf_map: HashMap<u64, u16>,
    #[cfg(debug_assertions)]
    phases: HashMap<u64, SendZcPhase>,
}

/// Lifecycle phase of a SEND_ZC operation (debug-only).
#[cfg(debug_assertions)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendZcPhase {
    /// Application registered the id (via `register`).
    Registered,
    /// First CQE observed (kernel accepted the send).
    Accepted,
    /// Second CQE observed (buffer reclaimable).
    Reclaimed,
}

impl SendZcTracker {
    /// Construct an empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf_map: HashMap::new(),
            #[cfg(debug_assertions)]
            phases: HashMap::new(),
        }
    }

    /// Construct with a pre-allocated capacity.
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf_map: HashMap::with_capacity(cap),
            #[cfg(debug_assertions)]
            phases: HashMap::with_capacity(cap),
        }
    }

    /// Registers an `id → buf_id` mapping. Called by the application when
    /// it submits a SEND_ZC SQE (typically via the Send Queue, which then
    /// notifies the CQE Processor out-of-band).
    pub fn register(&mut self, id: u64, buf_id: u16) {
        self.buf_map.insert(id, buf_id);
        #[cfg(debug_assertions)]
        self.phases.insert(id, SendZcPhase::Registered);
    }

    /// Returns the `buf_id` associated with `id` *without* removing it.
    /// Used by the dispatcher to fill in `Outcome::return_buffer`.
    #[must_use]
    pub fn peek_buf_for(&self, id: u64) -> Option<u16> {
        self.buf_map.get(&id).copied()
    }

    /// Consumes the `id → buf_id` mapping. Called when the reclaim CQE
    /// has been processed (the buffer is being recycled).
    pub fn retire(&mut self, id: u64) -> Option<u16> {
        let buf = self.buf_map.remove(&id);
        #[cfg(debug_assertions)]
        {
            if self.phases.insert(id, SendZcPhase::Reclaimed).is_none() && buf.is_some() {
                // Reclaim without a registered Send — invariant violation.
                // In debug mode we surface this loudly.
                debug_assert!(
                    false,
                    "SendZcTracker::retire({id}) — reclaim observed but no prior registration"
                );
            }
        }
        buf
    }

    /// Records the first-CQE observation for `id` (debug only).
    #[cfg(debug_assertions)]
    pub fn note_send(&mut self, id: u64) {
        let existing = self.phases.insert(id, SendZcPhase::Accepted);
        if existing == Some(SendZcPhase::Accepted) {
            debug_assert!(
                false,
                "SendZcTracker::note_send({id}) — duplicate Send CQE without Reclaim"
            );
        }
    }

    #[cfg(not(debug_assertions))]
    #[inline]
    pub fn note_send(&mut self, _id: u64) {}

    /// Records the second-CQE observation for `id` (debug only).
    #[cfg(debug_assertions)]
    pub fn note_reclaim(&mut self, id: u64) {
        let prev = self.phases.insert(id, SendZcPhase::Reclaimed);
        if prev.is_none() {
            debug_assert!(
                false,
                "SendZcTracker::note_reclaim({id}) — reclaim without Send"
            );
        }
    }

    #[cfg(not(debug_assertions))]
    #[inline]
    pub fn note_reclaim(&mut self, _id: u64) {}

    /// Number of in-flight SEND_ZC operations currently tracked.
    #[must_use]
    pub fn inflight_count(&self) -> usize {
        self.buf_map.len()
    }

    /// Clears all tracked mappings (used at shutdown).
    pub fn clear(&mut self) {
        self.buf_map.clear();
        #[cfg(debug_assertions)]
        self.phases.clear();
    }
}

impl Default for SendZcTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SendZcTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendZcTracker")
            .field("inflight", &self.buf_map.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_then_peek_then_retire() {
        let mut t = SendZcTracker::new();
        t.register(42, 7);
        assert_eq!(t.peek_buf_for(42), Some(7));
        assert_eq!(t.inflight_count(), 1);
        assert_eq!(t.retire(42), Some(7));
        assert_eq!(t.peek_buf_for(42), None);
        assert_eq!(t.inflight_count(), 0);
    }

    #[test]
    fn retire_unknown_id_returns_none() {
        let mut t = SendZcTracker::new();
        assert_eq!(t.retire(99), None);
    }

    #[test]
    fn clear_removes_all() {
        let mut t = SendZcTracker::new();
        t.register(1, 10);
        t.register(2, 20);
        t.clear();
        assert_eq!(t.inflight_count(), 0);
    }
}
