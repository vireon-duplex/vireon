//! [`BatchComposer`] — accumulates N stream selections into one
//! packet flush.
//!
//! See `DESIGN.md` Part 7 (transport integration) and Part 13
//! (performance targets).
//!
//! # Why batch?
//!
//! GSO (Generic Segmentation Offload) lets the kernel send multiple
//! UDP datagrams in one syscall, drastically reducing per-packet
//! overhead. To use GSO effectively, the scheduler should accumulate
//! several stream selections into a single batch before handing the
//! batch to the send-queue.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the batch type + capacity. Functional accumulation
//! and flush land in Phase 2 alongside GSO integration.

use crate::selector::Selection;

/// Default maximum batch size (streams per flush).
pub const DEFAULT_BATCH_CAPACITY: usize = 16;

/// A batch of stream selections ready for a single packet flush.
#[derive(Clone, Debug)]
pub struct BatchComposer {
    capacity: usize,
    selections: Vec<Selection>,
    total_bytes: u64,
}

impl BatchComposer {
    /// Construct a composer with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            selections: Vec::with_capacity(capacity),
            total_bytes: 0,
        }
    }

    /// Construct with default capacity.
    #[must_use]
    pub fn default_composer() -> Self {
        Self::new(DEFAULT_BATCH_CAPACITY)
    }

    /// Maximum selections per batch.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current number of selections in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.selections.len()
    }

    /// Whether the batch holds no selections.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.selections.is_empty()
    }

    /// Whether the batch is at capacity (ready to flush).
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.selections.len() >= self.capacity
    }

    /// Total bytes across all selections in the batch.
    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Append a selection. Returns `false` if the batch is full.
    pub fn push(&mut self, selection: Selection) -> bool {
        if self.is_full() {
            return false;
        }
        self.total_bytes = self.total_bytes.saturating_add(selection.bytes);
        self.selections.push(selection);
        true
    }

    /// Drain the batch, returning the selections for a packet flush.
    pub fn flush(&mut self) -> Vec<Selection> {
        self.total_bytes = 0;
        std::mem::take(&mut self.selections)
    }
}

impl Default for BatchComposer {
    fn default() -> Self {
        Self::default_composer()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_manager::StreamId;

    fn sel(id: u64, bytes: u64) -> Selection {
        Selection::new(StreamId::new_unchecked(id), 3, bytes)
    }

    #[test]
    fn empty_batch_predicates() {
        let b = BatchComposer::new(4);
        assert!(b.is_empty());
        assert!(!b.is_full());
        assert_eq!(b.len(), 0);
        assert_eq!(b.total_bytes(), 0);
    }

    #[test]
    fn push_accumulates() {
        let mut b = BatchComposer::new(3);
        assert!(b.push(sel(0, 100)));
        assert!(b.push(sel(4, 200)));
        assert_eq!(b.len(), 2);
        assert_eq!(b.total_bytes(), 300);
    }

    #[test]
    fn full_batch_rejects() {
        let mut b = BatchComposer::new(2);
        b.push(sel(0, 100));
        b.push(sel(4, 100));
        assert!(b.is_full());
        assert!(!b.push(sel(8, 100)));
    }

    #[test]
    fn flush_returns_and_clears() {
        let mut b = BatchComposer::new(4);
        b.push(sel(0, 100));
        b.push(sel(4, 200));
        let drained = b.flush();
        assert_eq!(drained.len(), 2);
        assert!(b.is_empty());
        assert_eq!(b.total_bytes(), 0);
    }
}
