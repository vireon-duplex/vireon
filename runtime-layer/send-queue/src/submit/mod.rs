//! Submission sink trait (Layer 9) — the IO Layer boundary.
//!
//! [`SubmissionSink`] is the **only** contact point between the Send
//! Queue and the IO Layer. The Send Queue never touches `io_uring`,
//! `RawFd`, or `Sender` directly — it calls this trait, and the IO Layer
//! (or an integration crate) provides the implementation that translates
//! to `Sender::send_zc` + SQE submission.

use crate::batch::BatchEntry;
use crate::error::SendQueueError;

/// Abstraction over the IO Layer's send path.
///
/// The Send Queue calls [`submit_batch`](Self::submit_batch) to hand a
/// batch of prepared packets to the kernel. The implementation pushes
/// `SEND_ZC` SQEs onto the io_uring submission queue.
///
/// # Return value
///
/// Returns the number of packets successfully queued to the SQ ring. A
/// return value less than `entries.len()` indicates the SQ was full; the
/// caller should retry the remainder on the next tick.
pub trait SubmissionSink: Send {
    /// Submits a batch of packets.
    ///
    /// Each entry references a registered fixed buffer by index and
    /// specifies the payload length and destination.
    fn submit_batch(&mut self, entries: &[BatchEntry]) -> Result<usize, SendQueueError>;
}

#[cfg(test)]
pub(crate) mod test_sink {
    //! Test-only sink that records submitted entries without touching the
    //! IO Layer.

    use super::*;
    use std::sync::Mutex;

    /// A [`SubmissionSink`] implementation that records every submission
    /// for test assertions.
    pub struct RecordingSink {
        submitted: Mutex<Vec<BatchEntry>>,
        /// How many entries to accept per call; `None` = accept all.
        accept_count: Option<usize>,
    }

    impl RecordingSink {
        #[must_use]
        pub fn new() -> Self {
            Self {
                submitted: Mutex::new(Vec::new()),
                accept_count: None,
            }
        }

        /// Configures the sink to accept only `n` entries per
        /// `submit_batch` call (simulates SQ backpressure).
        #[must_use]
        pub fn with_accept_count(mut self, n: usize) -> Self {
            self.accept_count = Some(n);
            self
        }

        /// Returns all submitted entries.
        pub fn submitted(&self) -> Vec<BatchEntry> {
            self.submitted.lock().expect("lock").clone()
        }

        /// Number of submitted entries.
        pub fn count(&self) -> usize {
            self.submitted.lock().expect("lock").len()
        }
    }

    impl Default for RecordingSink {
        fn default() -> Self {
            Self::new()
        }
    }

    impl SubmissionSink for RecordingSink {
        fn submit_batch(&mut self, entries: &[BatchEntry]) -> Result<usize, SendQueueError> {
            let n = self
                .accept_count
                .unwrap_or(entries.len())
                .min(entries.len());
            let mut guard = self.submitted.lock().expect("lock");
            guard.extend_from_slice(&entries[..n]);
            Ok(n)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn recording_sink_records_entries() {
        let mut sink = test_sink::RecordingSink::new();
        let entries = vec![BatchEntry {
            buf_index: 0,
            len: 64,
            dest: "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
            user_data: 0,
        }];
        let n = sink.submit_batch(&entries).expect("submit");
        assert_eq!(n, 1);
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn recording_sink_respects_accept_count() {
        let mut sink = test_sink::RecordingSink::new().with_accept_count(2);
        let entries: Vec<_> = (0..5_u16)
            .map(|i| BatchEntry {
                buf_index: i,
                len: 4,
                dest: "127.0.0.1:1".parse().unwrap(),
                user_data: u64::from(i),
            })
            .collect();
        let n = sink.submit_batch(&entries).expect("submit");
        assert_eq!(n, 2);
        assert_eq!(sink.count(), 2);
    }
}
