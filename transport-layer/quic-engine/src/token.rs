//! [`StatelessResetToken`] and [`RetryToken`].
//!
//! Phase 1 — Connection Manager. See `DESIGN.md` §4.1 and §8.
//!
//! - **Stateless Reset** (RFC 9000 §10.3): a 16-byte token carried
//!   in a short-header packet that allows a peer to recover after
//!   the local side has lost state (crash, restart, eviction).
//!   Derived deterministically from the CID + a per-engine secret
//!   via HMAC-SHA256, so the engine can verify a reset without
//!   retaining per-connection state.
//! - **Retry Token** (RFC 9000 §17.2.5.1): an opaque blob carried in
//!   a Retry packet, validated by the server on the subsequent
//!   Initial. Encodes the original DCID + peer address + an expiry,
//!   AEAD-encrypted with a server-only key. Defends against address
//!   spoofing prior to address validation.

use std::net::SocketAddr;

/// 16-byte stateless reset token (RFC 9000 §10.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StatelessResetToken(pub [u8; 16]);

impl StatelessResetToken {
    /// TODO(phase-1): compute `HMAC-SHA256(engine_secret, cid)[..16]`.
    #[must_use]
    pub const fn zeroed() -> Self {
        Self([0; 16])
    }
}

/// Address-validation token carried in Retry packets. Opaque to the
/// peer; decrypted server-side to recover the original DCID and peer
/// address.
///
/// Phase 1 will define the encrypted payload format.
#[derive(Clone, Copy, Debug)]
pub struct RetryToken {
    /// Encoded bytes — encrypted AEAD payload, including the
    /// original DCID and an expiry timestamp.
    bytes: [u8; 64],
    len: u8,
}

impl RetryToken {
    /// TODO(phase-1): `encode(odcid, peer, expiry, key)` and
    /// `decode(bytes, key, now) -> Result<(odcid, peer), QuicError>`.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            bytes: [0; 64],
            len: 0,
        }
    }
}

/// Argument bundle for Retry token decode (Phase 1).
#[derive(Clone, Copy, Debug)]
pub struct RetryTokenParams<'a> {
    /// Original DCID chosen by the client.
    pub odcid: &'a [u8],
    /// Peer transport address.
    pub peer: SocketAddr,
    /// Token expiry (monotonic nanos).
    pub expires_ns: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let _t = StatelessResetToken::zeroed();
        let _r = RetryToken::empty();
        // TODO(phase-1): HMAC derivation, AEAD encode/decode,
        // replay-window tests.
    }
}
