//! [`ReadyQueue`] — 8-bucket urgency structure for ready streams.
//!
//! See `DESIGN.md` Part 4 (RFC 9218) and Part 9 (data structures).
//!
//! # Design
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

use std::collections::VecDeque;

use stream_manager::{StreamId, StreamPriority};

/// Number of urgency bands (RFC 9218: 0..=7).
pub const URGENCY_BANDS: usize = 8;

/// A ready-stream entry in the queue. Carries the scheduling
/// metadata the selector needs without dereferencing the stream
/// table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
/// Each band holds a FIFO `VecDeque<ReadyEntry>`. Dequeue scans from
/// band 0 (highest urgency) to band 7 (lowest), returning the first
/// available entry. This implements strict-priority across bands.
pub struct ReadyQueue {
    capacity_per_band: usize,
    bands: [VecDeque<ReadyEntry>; URGENCY_BANDS],
    total_occupancy: u64,
}

impl ReadyQueue {
    /// Construct a queue with the given per-band capacity.
    #[must_use]
    pub fn new(capacity_per_band: usize) -> Self {
        Self {
            capacity_per_band,
            bands: std::array::from_fn(|_| VecDeque::new()),
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
    pub fn band_occupancy(&self, urgency: u8) -> u32 {
        if (urgency as usize) < URGENCY_BANDS {
            self.bands[urgency as usize].len() as u32
        } else {
            0
        }
    }

    /// Lowest non-empty urgency band, or `None` if empty.
    #[must_use]
    pub fn first_non_empty_band(&self) -> Option<u8> {
        for i in 0..URGENCY_BANDS {
            if !self.bands[i].is_empty() {
                return Some(i as u8);
            }
        }
        None
    }

    /// Enqueue a ready stream into its urgency band.
    ///
    /// Entries are pushed to the back of their band's FIFO queue.
    /// Returns an error if the per-band capacity is exceeded.
    ///
    /// # Errors
    /// Returns [`crate::error::SchedulerError::BackpressureStall`] if
    /// the target band is full.
    pub fn enqueue(&mut self, entry: ReadyEntry) -> Result<(), crate::error::SchedulerError> {
        let band = entry.band();
        if self.bands[band].len() >= self.capacity_per_band {
            return Err(crate::error::SchedulerError::BackpressureStall);
        }
        self.bands[band].push_back(entry);
        self.total_occupancy += 1;
        Ok(())
    }

    /// Dequeue the highest-priority entry.
    ///
    /// Scans bands 0 → 7 and returns the first available entry.
    /// This is the strict-priority selection across bands.
    pub fn dequeue(&mut self) -> Option<ReadyEntry> {
        for band in 0..URGENCY_BANDS {
            if let Some(entry) = self.bands[band].pop_front() {
                self.total_occupancy = self.total_occupancy.saturating_sub(1);
                return Some(entry);
            }
        }
        None
    }

    /// Dequeue from a specific urgency band (used for testing and
    /// non-strict policies).
    pub fn dequeue_band(&mut self, band: usize) -> Option<ReadyEntry> {
        if band < URGENCY_BANDS {
            if let Some(entry) = self.bands[band].pop_front() {
                self.total_occupancy = self.total_occupancy.saturating_sub(1);
                return Some(entry);
            }
        }
        None
    }

    /// Remove a specific stream from the queue (for cancellation).
    ///
    /// Returns `true` if the stream was found and removed.
    pub fn remove(&mut self, id: StreamId) -> bool {
        for band in 0..URGENCY_BANDS {
            if let Some(pos) = self.bands[band].iter().position(|e| e.id == id) {
                self.bands[band].remove(pos);
                self.total_occupancy = self.total_occupancy.saturating_sub(1);
                return true;
            }
        }
        false
    }

    /// Drain all entries from all bands.
    pub fn clear(&mut self) {
        for band in 0..URGENCY_BANDS {
            self.bands[band].clear();
        }
        self.total_occupancy = 0;
    }
}

impl Default for ReadyQueue {
    fn default() -> Self {
        Self::new(1024)
    }
}

// ── Clone impl (VecDeque<ReadyEntry> is cloneable) ───────────────

impl Clone for ReadyQueue {
    fn clone(&self) -> Self {
        Self {
            capacity_per_band: self.capacity_per_band,
            bands: self.bands.clone(),
            total_occupancy: self.total_occupancy,
        }
    }
}

impl std::fmt::Debug for ReadyQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadyQueue")
            .field("capacity_per_band", &self.capacity_per_band)
            .field("total_occupancy", &self.total_occupancy)
            .field("band_occupancy", &{
                let mut occ = [0u32; URGENCY_BANDS];
                for i in 0..URGENCY_BANDS {
                    occ[i] = self.bands[i].len() as u32;
                }
                occ
            })
            .finish()
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
        let mut q = ReadyQueue::default();
        assert!(q.is_empty());
        assert_eq!(q.total(), 0);
        assert_eq!(q.first_non_empty_band(), None);
        assert_eq!(q.dequeue(), None);
    }

    #[test]
    fn enqueue_dequeue_single() {
        let mut q = ReadyQueue::default();
        let entry = ReadyEntry::new(
            StreamId::new_unchecked(5),
            StreamPriority::with_urgency(2),
            500,
        );
        assert!(q.enqueue(entry).is_ok());
        assert_eq!(q.total(), 1);
        assert!(!q.is_empty());
        assert_eq!(q.band_occupancy(2), 1);
        assert_eq!(q.first_non_empty_band(), Some(2));

        let dequeued = q.dequeue().unwrap();
        assert_eq!(dequeued.id, StreamId::new_unchecked(5));
        assert!(q.is_empty());
    }

    #[test]
    fn strict_priority_ordering() {
        let mut q = ReadyQueue::default();
        // Enqueue into bands 3, 0, 5 — dequeue should return band 0 first.
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(1),
            StreamPriority::with_urgency(3),
            100,
        ))
        .unwrap();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(2),
            StreamPriority::with_urgency(0),
            200,
        ))
        .unwrap();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(3),
            StreamPriority::with_urgency(5),
            300,
        ))
        .unwrap();

        // Dequeue: band 0 first, then band 3, then band 5.
        assert_eq!(q.dequeue().unwrap().id, StreamId::new_unchecked(2));
        assert_eq!(q.dequeue().unwrap().id, StreamId::new_unchecked(1));
        assert_eq!(q.dequeue().unwrap().id, StreamId::new_unchecked(3));
        assert_eq!(q.dequeue(), None);
    }

    #[test]
    fn fifo_within_band() {
        let mut q = ReadyQueue::default();
        for i in 0..5 {
            q.enqueue(ReadyEntry::new(
                StreamId::new_unchecked(i),
                StreamPriority::with_urgency(2),
                100,
            ))
            .unwrap();
        }
        for i in 0..5 {
            assert_eq!(q.dequeue().unwrap().id, StreamId::new_unchecked(i));
        }
    }

    #[test]
    fn capacity_rejects() {
        let mut q = ReadyQueue::new(2);
        let pri = StreamPriority::with_urgency(1);
        assert!(
            q.enqueue(ReadyEntry::new(StreamId::new_unchecked(1), pri, 0))
                .is_ok()
        );
        assert!(
            q.enqueue(ReadyEntry::new(StreamId::new_unchecked(2), pri, 0))
                .is_ok()
        );
        // Third should fail.
        assert!(
            q.enqueue(ReadyEntry::new(StreamId::new_unchecked(3), pri, 0))
                .is_err()
        );
        assert_eq!(q.total(), 2);
    }

    #[test]
    fn capacity_is_per_band() {
        let mut q = ReadyQueue::new(2);
        // Fill band 0 to capacity.
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(1),
            StreamPriority::with_urgency(0),
            0,
        ))
        .unwrap();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(2),
            StreamPriority::with_urgency(0),
            0,
        ))
        .unwrap();
        // Band 1 should still accept.
        assert!(
            q.enqueue(ReadyEntry::new(
                StreamId::new_unchecked(3),
                StreamPriority::with_urgency(1),
                0
            ))
            .is_ok()
        );
    }

    #[test]
    fn remove_cancels_stream() {
        let mut q = ReadyQueue::default();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(1),
            StreamPriority::with_urgency(0),
            0,
        ))
        .unwrap();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(2),
            StreamPriority::with_urgency(0),
            0,
        ))
        .unwrap();
        assert_eq!(q.total(), 2);
        assert!(q.remove(StreamId::new_unchecked(1)));
        assert_eq!(q.total(), 1);
        assert!(!q.remove(StreamId::new_unchecked(99)));
        assert_eq!(q.dequeue().unwrap().id, StreamId::new_unchecked(2));
    }

    #[test]
    fn clear_empties_all() {
        let mut q = ReadyQueue::default();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(1),
            StreamPriority::with_urgency(0),
            0,
        ))
        .unwrap();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(2),
            StreamPriority::with_urgency(5),
            0,
        ))
        .unwrap();
        q.clear();
        assert!(q.is_empty());
        assert_eq!(q.total(), 0);
    }

    #[test]
    fn dequeue_band_specific() {
        let mut q = ReadyQueue::default();
        q.enqueue(ReadyEntry::new(
            StreamId::new_unchecked(1),
            StreamPriority::with_urgency(3),
            100,
        ))
        .unwrap();
        assert_eq!(q.dequeue_band(0), None);
        assert_eq!(q.dequeue_band(3).unwrap().id, StreamId::new_unchecked(1));
        assert!(q.is_empty());
    }
}
