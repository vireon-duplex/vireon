//! Retry tracker (Layer 7).
//!
//! [`RetryTracker`] works in conjunction with [`InFlightTable`] to track
//! retransmission attempts and decide whether to retry or drop a packet.
//! RTO (retransmit timeout) computation is based on [`RetryConfig`].
//!
//! Phase 1 provides fixed-RTO computation; adaptive RTT estimation
//! (SRTT + RTTVAR, RFC 6298 style) is deferred to Phase 5.

use crate::config::RetryConfig;
use crate::lifecycle::InFlightTable;

/// Reason a packet was dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// Maximum retry attempts exceeded.
    MaxRetriesExceeded,
    /// Queue was full at flush time.
    QueueFullAtFlush,
    /// Explicitly cancelled by the caller.
    Cancelled,
}

/// Decides whether to retry or drop a timed-out packet.
#[derive(Debug)]
pub struct RetryTracker {
    config: RetryConfig,
}

impl RetryTracker {
    /// Creates a tracker with the given config.
    #[must_use]
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    /// Returns the RTO (nanoseconds) for the `attempt`-th transmission.
    ///
    /// Phase 1: returns the fixed `initial_rto_ns` for all attempts.
    /// Phase 5: will compute an adaptive estimate based on observed RTT.
    #[must_use]
    pub fn rto_for_attempt(&self, _attempt: u8) -> u64 {
        // Fixed RTO for now — adaptive estimation is a future extension.
        self.config.initial_rto_ns
    }

    /// Handles a timeout for packet `id`.
    ///
    /// Returns:
    /// - `Ok(rto_ns)` — the packet should be retried; schedule a timer
    ///   for `rto_ns`.
    /// - `Err(DropReason)` — the packet has exceeded `max_retries` and
    ///   must be dropped.
    pub fn on_timeout(&self, table: &mut InFlightTable, id: u64) -> Result<u64, DropReason> {
        let current_retries = table.retry_count(id).unwrap_or(0);
        if current_retries >= self.config.max_retries {
            // Drop: retire the entry and signal the caller.
            table.retire(id);
            return Err(DropReason::MaxRetriesExceeded);
        }
        // Bump the retry count and compute the next RTO.
        let new_count = table.bump_retry(id).ok_or(DropReason::MaxRetriesExceeded)?;
        let rto = self.rto_for_attempt(new_count);
        Ok(rto)
    }

    /// Maximum retry attempts from the config.
    #[must_use]
    pub fn max_retries(&self) -> u8 {
        self.config.max_retries
    }

    /// Whether adaptive RTT estimation is enabled.
    #[must_use]
    pub fn adaptive_rtt(&self) -> bool {
        self.config.adaptive_rtt
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::encode_id;
    use crate::queue::SlotState;

    #[test]
    fn first_timeout_returns_initial_rto() {
        let tracker = RetryTracker::new(RetryConfig::default());
        let mut table = InFlightTable::new(64);
        let id = encode_id(0, 1);
        table.track(id, 0);
        let rto = tracker.on_timeout(&mut table, id).expect("retry");
        assert_eq!(rto, RetryConfig::default().initial_rto_ns);
        assert_eq!(table.retry_count(id), Some(1));
    }

    #[test]
    fn drops_after_max_retries() {
        let cfg = RetryConfig {
            max_retries: 2,
            ..RetryConfig::default()
        };
        let tracker = RetryTracker::new(cfg);
        let mut table = InFlightTable::new(64);
        let id = encode_id(0, 1);
        table.track(id, 0);
        // Attempt 1
        tracker.on_timeout(&mut table, id).expect("retry 1");
        // Attempt 2
        tracker.on_timeout(&mut table, id).expect("retry 2");
        // Attempt 3 → should drop (max_retries = 2 means 2 retries beyond initial)
        let reason = tracker.on_timeout(&mut table, id).unwrap_err();
        assert_eq!(reason, DropReason::MaxRetriesExceeded);
        assert!(!table.is_tracked(id));
    }

    #[test]
    fn rto_is_clamped_to_config() {
        let cfg = RetryConfig {
            initial_rto_ns: 500_000,
            min_rto_ns: 100_000,
            max_rto_ns: 10_000_000,
            ..RetryConfig::default()
        };
        let tracker = RetryTracker::new(cfg);
        let rto = tracker.rto_for_attempt(0);
        assert_eq!(rto, 500_000);
    }

    #[test]
    fn on_timeout_untracked_packet_drops() {
        let tracker = RetryTracker::new(RetryConfig::default());
        let mut table = InFlightTable::new(64);
        let id = encode_id(99, 1); // never tracked
        let reason = tracker.on_timeout(&mut table, id).unwrap_err();
        assert_eq!(reason, DropReason::MaxRetriesExceeded);
    }

    #[test]
    fn retry_transitions_state() {
        let tracker = RetryTracker::new(RetryConfig::default());
        let mut table = InFlightTable::new(64);
        let id = encode_id(5, 1);
        table.track(id, 0);
        tracker.on_timeout(&mut table, id).expect("retry");
        assert_eq!(table.state(id), Some(SlotState::Retrying));
    }
}
