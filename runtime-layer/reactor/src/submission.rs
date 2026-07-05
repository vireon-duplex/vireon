//! [`RingSubmissionSink`] — implements [`send_queue::SubmissionSink`]
//! against [`RingOwner`](crate::ring::RingOwner), bridging send-queue
//! batches to io_uring SEND_ZC / SENDMMSG SQEs.
//!
//! See `DESIGN.md` §13 (io_uring integration) and §7 (send-queue
//! integration).
//!
//! The send queue only knows about the [`SubmissionSink`] trait;
//! this adapter is what makes the abstraction real. Each
//! [`BatchEntry`] becomes one SQE; GSO-batched entries share a
//! single submission via `UDP_SEGMENT`.

use send_queue::{BatchEntry, SendQueueError, SubmissionSink};

/// Adapter that turns a `&mut RingOwner` into a `SubmissionSink`.
// TODO(phase-2): hold a `&mut RingOwner` reference and push real SQEs.
pub struct RingSubmissionSink {
    submitted: usize,
}

impl RingSubmissionSink {
    /// Construct an empty sink (Phase-0 placeholder).
    #[must_use]
    pub const fn new() -> Self {
        Self { submitted: 0 }
    }

    /// Number of entries accepted so far (test helper).
    #[must_use]
    pub const fn submitted(&self) -> usize {
        self.submitted
    }
}

impl Default for RingSubmissionSink {
    fn default() -> Self {
        Self::new()
    }
}

impl SubmissionSink for RingSubmissionSink {
    fn submit_batch(&mut self, _entries: &[BatchEntry]) -> Result<usize, SendQueueError> {
        // TODO(phase-2): push real SEND_ZC SQEs via RingOwner, one
        //   per entry (or one GSO-batched submission for entries
        //   that share a destination).
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let mut sink = RingSubmissionSink::new();
        // Phase-0 stub: submit_batch accepts anything and reports 0.
        let n = sink.submit_batch(&[]).expect("empty batch");
        assert_eq!(n, 0);
        assert_eq!(sink.submitted(), 0);
        // TODO(phase-2): real SEND_ZC SQE push.
    }
}
