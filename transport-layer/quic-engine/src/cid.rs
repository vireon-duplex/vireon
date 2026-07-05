//! [`QuicCid`] — QUIC Connection ID.
//!
//! Phase 1 — Connection Manager. See `DESIGN.md` §4.1.
//!
//! RFC 9000 §5 defines Connection IDs (CIDs) as opaque byte strings
//! of length 0–20 chosen by each endpoint. Vireon uses fixed-size
//! 8-byte CIDs on the server side (enables inline hash keys and
//! SIMD-friendly table lookups, matching the
//! [Cloudflare quiche server pattern](https://blog.cloudflare.org/)).
//!
//! Responsibilities (Phase 1):
//! - CID generation via a thread-local CSPRNG
//! - CID rotation (`NEW_CONNECTION_ID` / `RETIRE_CONNECTION_ID`)
//! - Stateless-reset token derivation (`HMAC-SHA256` of the CID)
//! - Forwarding table for retired CIDs (grace period)

/// Maximum CID length permitted by RFC 9000 §17.2 / §17.3.
pub const MAX_CID_LEN: usize = 20;

/// Server-side CID length used by Vireon. Fixed size enables:
/// - Inline keys in the connection table (no heap allocation)
/// - 64-bit hash with no length-prefix branch
/// - Single-cache-line lookup structure
pub const SERVER_CID_LEN: usize = 8;

/// Opaque connection identifier. Stored inline where possible.
///
/// Phase 0 ships only the type and length constants; generation,
/// parsing, and token derivation land in Phase 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QuicCid {
    /// Significant bytes.
    bytes: [u8; MAX_CID_LEN],
    /// Logical length (1..=20).
    len: u8,
}

impl QuicCid {
    /// Placeholder constructor for Phase 0. Phase 1 will add
    /// `random()`, `from_slice()`, `as_bytes()`, `len()`.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            bytes: [0; MAX_CID_LEN],
            len: 0,
        }
    }
}

impl Default for QuicCid {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let cid = QuicCid::empty();
        assert_eq!(cid, QuicCid::default());
        // TODO(phase-1): full CID lifecycle — generation, rotation,
        // retirement, stateless-reset token derivation.
    }
}
