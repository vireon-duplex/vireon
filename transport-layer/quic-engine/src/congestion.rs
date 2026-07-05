//! [`CongestionController`] — pluggable CC (Reno / CUBIC / BBR /
//! BBRv2) + ECN + HyStart++.
//!
//! Phase 5 — Congestion Control. See `DESIGN.md` §4.5.
//!
//! Vireon delegates CC to quiche (which exposes Reno, CUBIC, BBR).
//! BBRv2 and HyStart++ will be added via quiche's CC-plugin
//! interface once they land upstream.
//!
//! This module's role:
//! - Translate engine-wide policy ([`crate::config::CongestionAlgorithm`])
//!   into quiche's `set_cc_algorithm_name`
//! - Observe congestion state (cwnd, bytes-in-flight, recovery
//!   window, bandwidth estimate) for stats and for the pacer
//!   (Phase 10)
//! - Surface ECN feedback (RFC 9000 §13.4 / §19.3) so the engine
//!   can mark outgoing packets accordingly
//! - Hook the application-limited detection (RFC 9002 §7.8.2)
//!
//! RFC 9002 §7 (Congestion Control) compliance is quiche's
//! responsibility; we ensure the policy is applied uniformly
//! across all connections on the shard.

use crate::config::CongestionAlgorithm;

/// Phase-0 skeleton. Phase 5 will populate this with stat-snapshot
/// logic and the ECN policy.
#[derive(Default)]
pub struct CongestionController {
    /// Currently active CC algorithm.
    algorithm: CongestionAlgorithm,
    /// Congestion window in bytes (mirrors quiche's internal cwnd).
    cwnd_bytes: u64,
    /// Bytes currently in flight (sent, not yet acked or declared lost).
    bytes_in_flight: u64,
}

impl CongestionController {
    /// TODO(phase-5): construct from algorithm + initial cwnd.
    #[must_use]
    pub fn new(algorithm: CongestionAlgorithm) -> Self {
        Self {
            algorithm,
            cwnd_bytes: 0,
            bytes_in_flight: 0,
        }
    }

    /// TODO(phase-5): `fn snapshot(&self) -> CongestionSnapshot`
    /// returning a Copy-able snapshot for stats / pacing.
    pub fn _placeholder(&self) {}

    /// Returns the configured algorithm.
    #[must_use]
    pub const fn algorithm(&self) -> CongestionAlgorithm {
        self.algorithm
    }
}

/// Phase-5 ECN policy (RFC 9000 §13.4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum EcnPolicy {
    /// Disable ECN marking entirely.
    #[default]
    Disabled,
    /// Attempt ECN; fall back to non-ECN if not supported by the path.
    BestEffort,
    /// Require ECN; tear down if peer doesn't echo.
    Required,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let cc = CongestionController::new(CongestionAlgorithm::Bbr);
        assert_eq!(cc.algorithm(), CongestionAlgorithm::Bbr);
        // TODO(phase-5): observe quiche CC stats, expose cwnd/bif/
        // bandwidth-estimate, drive the pacer.
    }
}
