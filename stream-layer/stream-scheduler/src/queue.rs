//! [`ReadyQueue`] — 8-bucket urgency structure for ready streams.
//!
//! See `DESIGN.md` Part 4 (RFC 9218) and Part 9 (data structures).
//!
//! # Design sketch
//!
//! RFC 9218 defines 8 urgency levels (0..7). The `ReadyQueue` keeps
//! one FIFO queue per urgency band. Selection scans bands from
//! urgency 0 (highest) to urgency 7 (lowest), dequeuing from the
//! first non-empty band. This gives O(1) selection per band and
//! O(8) worst-case scan — fast enough for hot-path use.
//!
//! Within each band, streams are FIFO by default. Incremental
//! streams (RFC 9218 §2) cycle-share within their band instead of
//! strict FIFO — handled by [`crate::incremental`].
//!
//! # Phase 0 status
//!
//! Phase 0 ships the band type + capacity tracking. Functional
//! enqueue / dequeue land in Phase 1.

use stream_manager::{StreamId, StreamPriority};

/// Number of urgency bands (RFC 9218: 0..=7).
pub const URGENCY_BANDS: usize = 8;

/// A ready-stream entry in the queue. Carries the scheduling
/// metadata the selector needs without dereferencing the stream
/// table.
#[derive(Clone, Copy, Debug)]
pub struct ReadyEntry {
    /// The stream id.
    pub id: StreamId,
    /// Effective priority this tick (may differ from canonical due
    /// to aging — see [`crate::aging`]).
    pub priority: StreamPriority,
    /// Bytes available to send (bounded by flow control + backlog).
    pub bytes_available: u64,
}

impl ReadyEntry {
    /// Construct a new ready entry.
    #[must_use]
    pub const fn new(id: StreamId, priority: StreamPriority, bytes_available: u64) -> Self {
        Self {
            id,
            priority,
            bytes_available,
        }
    }

    /// Which urgency band this entry belongs in (0..7).
    #[must_use]
    pub const fn band(&self) -> usize {
        let max_band = (URGENCY_BANDS - 1) as u8;
        let u = if self.priority.urgency > max_band {
            max_band
        } else {
            self.priority.urgency
        };
        u as usize
    }
}

/// The 8-bucket urgency queue.
///
/// Phase 0 holds only the capacity + per-band counters; Phase 1
/// will add the per-band `VecDeque<ReadyEntry>` storage.
// TODO(phase-1): per-band VecDeque + scan-to-dequeue.
#[derive(Clone, Debug)]
pub struct ReadyQueue {
    capacity_per_band: usize,
    /// Per-band occupancy (Phase 1 will replace with real dequeues).
    band_occupancy: [u32; URGENCY_BANDS],
    total_occupancy: u64,
}

impl ReadyQueue {
    /// Construct a queue with the given per-band capacity.
    #[must_use]
    pub const fn new(capacity_per_band: usize) -> Self {
        Self {
            capacity_per_band,
            band_occupancy: [0; URGENCY_BANDS],
            total_occupancy: 0,
        }
    }

    /// Per-band capacity.
    #[must_use]
    pub const fn capacity_per_band(&self) -> usize {
        self.capacity_per_band
    }

    /// Total entries across all bands.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.total_occupancy
    }

    /// Whether the queue holds any entry.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.total_occupancy == 0
    }

    /// Occupancy of a specific band.
    #[must_use]
    pub const fn band_occupancy(&self, urgency: u8) -> u32 {
        if (urgency as usize) < URGENCY_BANDS {
            self.band_occupancy[urgency as usize]
        } else {
            0
        }
    }

    /// Lowest non-empty urgency band, or `None` if empty.
    #[must_use]
    pub fn first_non_empty_band(&self) -> Option<u8> {
        for (idx, &count) in self.band_occupancy.iter().enumerate() {
            if count > 0 {
                return Some(idx as u8);
            }
        }
        None
    }

    /// Phase-1 `enqueue` placeholder.
    ///
    /// # Errors
    /// Always returns [`crate::error::SchedulerError::ShuttingDown`]
    /// in Phase 0 (no storage).
    pub fn enqueue(&mut self, _entry: ReadyEntry) -> Result<(), crate::error::SchedulerError> {
        // TODO(phase-1): push to the appropriate band's VecDeque.
        Err(crate::error::SchedulerError::ShuttingDown)
    }
}

impl Default for ReadyQueue {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn band_classification() {
        let e = ReadyEntry::new(
            StreamId::new_unchecked(0),
            StreamPriority::with_urgency(3),
            1000,
        );
        assert_eq!(e.band(), 3);
    }

    #[test]
    fn band_clamps_at_max_urgency() {
        let e = ReadyEntry::new(
            StreamId::new_unchecked(0),
            StreamPriority::with_urgency(99),
            1000,
        );
        assert_eq!(e.band(), URGENCY_BANDS - 1);
    }

    #[test]
    fn empty_queue_predicates() {
        let q = ReadyQueue::default();
        assert!(q.is_empty());
        assert_eq!(q.total(), 0);
        assert_eq!(q.first_non_empty_band(), None);
    }
}
