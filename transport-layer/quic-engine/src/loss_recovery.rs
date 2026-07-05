//! [`LossRecovery`] — RFC 9002 §6 wrapper around quiche's loss
//! detector.
//!
//! Phase 4 — Loss Recovery. See `DESIGN.md` §4.4.
//!
//! Vireon delegates the actual loss detection to quiche. This module
//! exists to:
//!
//! 1. **Observe** loss events so they can be reported via stats and
//!    surfaced to the application (e.g. for live media to react
//!    quickly to bandwidth drops).
//! 2. **Translate** quiche's `timeout()` into `timer-wheel` ticks so
//!    that loss detection is driven by Vireon's timer system, not
//!    quiche's internal clock.
//! 3. **Override** loss policy when explicitly instructed by the
//!    application (e.g. `RESET_STREAM` after tail loss).
//!
//! Implements:
//! - RTT estimator (RFC 9002 §5: SRTT, RTTVAR, min RTT, latest RTT)
//! - Loss detection algorithm (RFC 9002 §6: packet threshold + time
//!   threshold, PTO)
//! - Persistent congestion detection (RFC 9002 §7.8)
//! - Spurious loss recovery (RFC 9002 §6.5)

/// Phase-0 skeleton. Phase 4 populates RTT samples, loss detection
/// state, and exposes a snapshot API for stats.
#[derive(Default)]
pub struct LossRecovery {
    /// Smoothed RTT in microseconds (RFC 9002 §5.3).
    srtt_us: u32,
    /// RTT variance in microseconds.
    rttvar_us: u32,
    /// Minimum observed RTT, used as the loss-detection floor.
    min_rtt_us: u32,
    /// Probe Timeout (RFC 9002 §6.2) in microseconds.
    pto_us: u32,
}

impl LossRecovery {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            srtt_us: 0,
            rttvar_us: 0,
            min_rtt_us: 0,
            pto_us: 0,
        }
    }

    /// TODO(phase-4): `fn on_ack_received(&mut self, acked: u64, now_ns: u64)`
    /// to update RTT estimators per RFC 9002 §5.1.
    pub fn _placeholder(&self) {}
}

/// Phase-4 observer hook fired when a packet is declared lost.
#[derive(Debug)]
#[non_exhaustive]
pub struct LossEvent {
    /// Packet number declared lost.
    pub packet_number: u64,
    /// Whether the loss is suspected to be spurious (re-ordered).
    pub spurious: bool,
    /// Whether this triggers persistent congestion (RFC 9002 §7.8).
    pub persistent_congestion: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let lr = LossRecovery::new();
        lr._placeholder();
        // TODO(phase-4): wrap quiche loss detection, expose stats
        // (srtt, pto, lost_packets, spurious_losses).
    }
}
