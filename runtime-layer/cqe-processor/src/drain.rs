//! CQ drainer (Layer 7).
//!
//! Reads raw CQE entries from the io_uring completion ring into a
//! pre-allocated scratch [`Vec`]. Head advancement is performed via the
//! io-uring crate's `by_ref()` iterator, which advances the head by the
//! number of entries consumed when the iterator is dropped — exactly the
//! single-Release-store-per-batch semantics we want.

use io_uring::IoUring;
use io_uring::cqueue::Entry;
use profiler::profile;

/// Owns the drain scratch + head-advance logic. Stateless across calls
/// aside from the reused buffer.
pub struct CqDrainer {
    scratch: Vec<Entry>,
    batch_max: usize,
}

impl CqDrainer {
    /// Construct with a pre-allocated scratch sized for `batch_max` entries.
    #[must_use]
    pub fn new(batch_max: usize) -> Self {
        Self {
            scratch: Vec::with_capacity(batch_max),
            batch_max,
        }
    }

    /// Updates the batch cap. Grows the scratch if necessary so that
    /// `capacity >= batch_max`.
    pub fn set_batch_max(&mut self, batch_max: usize) {
        self.batch_max = batch_max;
        if self.scratch.capacity() < batch_max {
            // `Vec::reserve(additional)` guarantees
            // `capacity >= len + additional`. We want `capacity >= batch_max`,
            // so `additional = batch_max - len`.
            let additional = batch_max - self.scratch.len();
            self.scratch.reserve(additional);
        }
    }

    /// Drains up to `batch_max` entries from `ring`'s CQ into the internal
    /// scratch. The CQ head is advanced exactly once (Release store) by the
    /// io-uring crate's iterator Drop.
    ///
    /// Returns a slice of the drained entries.
    #[profile]
    pub fn drain<'s>(&'s mut self, ring: &mut IoUring) -> &'s [Entry] {
        self.scratch.clear();
        let cap = self.batch_max;

        // Borrow the CQ, take up to `cap` entries via the by_ref iterator,
        // then drop the borrow before we return the slice. The iterator's
        // Drop advances the head by exactly the number of items consumed.
        {
            let mut cq = ring.completion();
            let iter = cq.by_ref().take(cap);
            self.scratch.extend(iter);
        }

        self.scratch.as_slice()
    }

    /// Returns the current number of available CQEs (without draining).
    /// Uses the io-uring crate's `len()` which is the safe public accessor.
    #[must_use]
    pub fn available(ring: &mut IoUring) -> usize {
        ring.completion().len()
    }

    /// Detects CQ overflow (the kernel dropped CQEs because the ring was
    /// full). The counter is the kernel-reported `overflow()` value.
    ///
    /// Returns the dropped-CQE count (0 if no overflow).
    pub fn check_overflow(ring: &mut IoUring) -> u32 {
        ring.completion().overflow()
    }

    /// Returns the scratch capacity (for diagnostics).
    #[must_use]
    pub fn scratch_capacity(&self) -> usize {
        self.scratch.capacity()
    }
}

impl std::fmt::Debug for CqDrainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CqDrainer")
            .field("batch_max", &self.batch_max)
            .field("scratch_capacity", &self.scratch.capacity())
            .field("scratch_len", &self.scratch.len())
            .finish()
    }
}

// Suppress unused-import warning for `Entry` when only the type signature
// references it (the public scratch slice type).
#[allow(dead_code)]
const _: fn() = || {
    fn _types_referenced(_e: &Entry) {}
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_preallocated() {
        let d = CqDrainer::new(128);
        assert!(d.scratch_capacity() >= 128);
    }

    #[test]
    fn set_batch_max_grows_capacity() {
        let mut d = CqDrainer::new(32);
        d.set_batch_max(256);
        assert!(d.scratch_capacity() >= 256);
    }
}
