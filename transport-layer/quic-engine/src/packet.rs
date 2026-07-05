//! [`PacketRouter`] — long vs short header routing, packet
//! coalescing, anti-amplification enforcement.
//!
//! Phase 2 — Packet Engine. See `DESIGN.md` §4.2.
//!
//! Receives raw UDP datagrams from `cqe-processor` and routes by
//! the first byte's header form bit (RFC 9000 §17):
//!
//! ```text
//! incoming UDP datagram
//!         │
//!         ▼
//! ┌────────────────┐   bit 0x80 set
//! │ decode header  │──────────────► LONG HEADER path
//! │ form bit       │                ├─ Initial ─────────► accept_path
//! └────────┬───────┘                ├─ Handshake ───────► route_by_dcid
//!          │                        ├─ 0-RTT ───────────► route_by_dcid
//!          │ bit 0x80 clear         └─ Version Negot. ──► send_vn
//!          ▼
//!   SHORT HEADER path
//!          ├─ route_by_dcid (8-byte inline key)
//!          └─ stateless-reset check ──► tear down
//! ```
//!
//! Coalescing (RFC 9000 §12.2): multiple QUIC packets may share a
//! single UDP datagram; the router iterates until the datagram is
//! exhausted.
//!
//! Anti-amplification (RFC 9000 §8.1): bytes sent to an unvalidated
//! address must not exceed 3× bytes received from it. Enforced on
//! the egress path in Phase 10 (Packet Scheduler), but the router
//! must flag unvalidated connections.

/// Phase-0 type skeleton. Phase 2 will populate this with the
/// header-form decode, long-header dispatch table, and coalescing
/// iteration loop. It will NOT duplicate work quiche already does —
/// quiche parses the packet internals; the router only classifies
/// and dispatches.
#[derive(Default)]
pub struct PacketRouter {
    /// Count of packets routed since engine start (for stats).
    packets_routed: u64,
}

impl PacketRouter {
    #[must_use]
    pub const fn new() -> Self {
        Self { packets_routed: 0 }
    }

    /// TODO(phase-2):
    /// `route(&mut self, datagram: &mut [u8], now_ns: u64) -> RouteOutcome`
    #[must_use]
    pub const fn _placeholder_route(&self) -> RouteOutcome {
        RouteOutcome::Drop
    }
}

/// Phase-2 outcome enum — what the router did with this datagram.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum RouteOutcome {
    /// Packet dropped (malformed, unknown CID, version mismatch).
    Drop,
    /// Packet handed to a connection's `quiche::Connection::recv`.
    Dispatched,
    /// Router needs to emit a Version Negotiation packet.
    VersionNegotiation,
    /// Router needs to emit a Retry packet.
    Retry,
    /// Router detected a stateless reset for a known connection.
    StatelessReset,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let r = PacketRouter::new();
        assert_eq!(r._placeholder_route(), RouteOutcome::Drop);
        // TODO(phase-2): header-form decode, coalescing iteration,
        // anti-amplification accounting.
    }
}
