//! [`AckManager`] — ACK generation, ACK aggregation, ACK delay,
//! ACK Frequency extension.
//!
//! Phase 6 — ACK Manager. See `DESIGN.md` §4.6.
//!
//! RFC 9000 §13 + RFC 9002 §?:
//! - ACK frames acknowledge received packets
//! - Delayed ACK reduces ACK rate (default: every 2 packets, max
//!   25 ms delay — see `quiche` defaults)
//! - ACK Frequency extension (draft-ietf-quic-ack-frequency) lets
//!   the receiver tell the sender how often to expect ACKs
//!
//! Vireon delegates ACK encoding to quiche. This module:
//! - Configures `max_ack_delay` and `ack_delay_exponent` (RFC 9000
//!   §18.2)
//! - Surfaces ACK-eliciting packet stats
//! - Drives the ACK Frequency extension negotiation
//! - Hooks immediate ACK generation on reordering / loss detection

/// Phase-0 skeleton. Phase 6 will populate policy fields and the
/// quiche-config wiring.
#[derive(Clone, Copy, Debug)]
pub struct AckManager {
    /// Maximum ACK delay the receiver will impose, in microseconds.
    /// Default 25_000 (RFC 9000 §18.2).
    pub max_ack_delay_us: u32,
    /// ACK delay exponent (RFC 9000 §18.2). Default 3.
    pub ack_delay_exponent: u8,
    /// Whether to negotiate the ACK Frequency extension.
    pub use_ack_frequency: bool,
}

impl Default for AckManager {
    fn default() -> Self {
        Self {
            max_ack_delay_us: 25_000,
            ack_delay_exponent: 3,
            use_ack_frequency: false,
        }
    }
}

impl AckManager {
    /// TODO(phase-6): `fn apply_to_quiche(&self, cfg: &mut quiche::Config)`.
    pub fn _placeholder(&self) {}
}

/// Phase-6 observation hook fired when an ACK frame is sent.
#[derive(Debug)]
#[non_exhaustive]
pub struct AckEvent {
    /// Number of packet ranges included in this ACK.
    pub ranges: u32,
    /// Largest packet number newly acknowledged.
    pub largest_acked: u64,
    /// Whether this ACK was sent immediately (no delay) due to
    /// reordering or peer request.
    pub immediate: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let m = AckManager::default();
        assert_eq!(m.ack_delay_exponent, 3);
        // TODO(phase-6): apply_to_quiche, ACK Frequency negotiation,
        // stats hooks.
    }
}
