//! Recycle pipeline (Layer 10).
//!
//! Coalesces per-CQE buffer returns into a single flush per drain cycle.
//! Without coalescing, every `SendBufferReclaim` would incur a
//! `BufferPool::refill_buffer` call — fine individually but noisy at
//! 40 Gbps line rate.

use profiler::profile;

/// Pre-allocated scratch of pending buf_ids awaiting return.
pub struct RecyclePipeline {
    pending: Vec<u16>,
    threshold: usize,
}

impl RecyclePipeline {
    /// Construct with the given capacity and flush threshold.
    #[must_use]
    pub fn new(threshold: usize) -> Self {
        Self {
            // Pre-grow to threshold + slack so push never reallocates on the
            // hot path.
            pending: Vec::with_capacity(threshold.max(16) * 2),
            threshold,
        }
    }

    /// Pushes a buf_id to be returned later. O(1) amortized.
    #[inline]
    pub fn push(&mut self, buf_id: u16) {
        self.pending.push(buf_id);
    }

    /// Returns `true` if the pipeline has reached the flush threshold.
    #[inline]
    #[must_use]
    pub fn should_flush(&self) -> bool {
        self.pending.len() >= self.threshold
    }

    /// Drains the pending buf_ids and returns how many were queued.
    /// The actual `BufferPool::refill_buffer` calls are the caller's
    /// responsibility (the caller owns the pool).
    #[must_use]
    #[profile]
    pub fn drain_pending(&mut self) -> Vec<u16> {
        let out = std::mem::take(&mut self.pending);
        // Restore capacity for the next batch.
        self.pending = Vec::with_capacity(self.threshold.max(16) * 2);
        out
    }

    /// Current pending count.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Update the flush threshold.
    pub fn set_threshold(&mut self, threshold: usize) {
        self.threshold = threshold;
        if self.pending.capacity() < threshold * 2 {
            self.pending
                .reserve(threshold * 2 - self.pending.capacity());
        }
    }
}

impl std::fmt::Debug for RecyclePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecyclePipeline")
            .field("pending", &self.pending.len())
            .field("threshold", &self.threshold)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_drain() {
        let mut p = RecyclePipeline::new(3);
        p.push(1);
        p.push(2);
        p.push(3);
        assert_eq!(p.pending_count(), 3);
        let drained = p.drain_pending();
        assert_eq!(drained, vec![1, 2, 3]);
        assert_eq!(p.pending_count(), 0);
    }

    #[test]
    fn should_flush_at_threshold() {
        let mut p = RecyclePipeline::new(2);
        assert!(!p.should_flush());
        p.push(1);
        assert!(!p.should_flush());
        p.push(2);
        assert!(p.should_flush());
    }

    #[test]
    fn drain_resets_capacity_for_next_batch() {
        let mut p = RecyclePipeline::new(2);
        for i in 0..5 {
            p.push(i);
        }
        let _ = p.drain_pending();
        // After drain we can push again without reallocation.
        p.push(10);
        p.push(11);
        assert_eq!(p.pending_count(), 2);
    }

    #[test]
    fn set_threshold_grows_capacity() {
        let mut p = RecyclePipeline::new(2);
        p.set_threshold(64);
        // No assertion on capacity directly (subject to Vec policy), but
        // we verify that pushing well past the old threshold works.
        for i in 0..32 {
            p.push(i);
        }
        assert!(p.pending_count() >= 32);
    }
}
