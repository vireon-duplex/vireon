//! The [`DeliveryPolicy`] enum and the [`StreamOpenMeta`] wire format.
//!
//! See the crate-level docs for the full table of policies and their use cases.

use bytes::Bytes;

use crate::error::DeliveryPolicyError;

/// Discriminants for the four supported delivery semantics.
///
/// Encoded as a single byte in the `MessageType::StreamOpen` payload —
/// see [`StreamOpenMeta`].
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeliveryPolicy {
    /// In-order delivery with a bounded reorder buffer. Never drops.
    ///
    /// Use for: file transfer, RPC, database replication, configuration
    /// updates — anything where order matters and loss is unacceptable.
    ReliableOrdered = 0x01,

    /// Deliver all frames immediately in arrival order; dedup by `Seq`.
    ///
    /// Use for: image chunks, AI token streams, partial aggregation —
    /// anything where loss is unacceptable but order does not matter.
    ReliableUnordered = 0x02,

    /// Drop any frame whose `Seq` is `≤ last_delivered`.
    ///
    /// Use for: audio, video — realtime media where stale samples are
    /// worthless and retransmits only add latency.
    RealtimeDropOld = 0x03,

    /// Keep only the most recent frame in a pending slot; older frames
    /// are discarded when a newer one arrives.
    ///
    /// Use for: cursor, mouse, GPS, presence — state where only the
    /// latest value matters and history is irrelevant.
    LatestOnly = 0x04,
}

impl DeliveryPolicy {
    /// Wire byte for this variant.
    #[inline]
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Decode a wire byte. Returns `None` for reserved / unknown values.
    #[inline]
    #[must_use]
    pub const fn from_byte(b: u8) -> Option<Self> {
        Some(match b {
            0x01 => Self::ReliableOrdered,
            0x02 => Self::ReliableUnordered,
            0x03 => Self::RealtimeDropOld,
            0x04 => Self::LatestOnly,
            _ => return None,
        })
    }

    /// Human-readable name for logging / metrics labels.
    #[inline]
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReliableOrdered => "reliable_ordered",
            Self::ReliableUnordered => "reliable_unordered",
            Self::RealtimeDropOld => "realtime_drop_old",
            Self::LatestOnly => "latest_only",
        }
    }
}

/// Wire payload of a `MessageType::StreamOpen` frame.
///
/// ```text
/// byte 0:   DeliveryPolicy discriminant (0x01..=0x04)
/// byte 1..: reserved (forward-compat for QoS, priority, topic binding).
///           Receivers MUST ignore unknown trailing bytes.
/// ```
///
/// `StreamOpen` payloads shorter than 1 byte are rejected as malformed —
/// a sender must always specify a policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamOpenMeta {
    /// The policy the sender has chosen for this stream.
    pub policy: DeliveryPolicy,
}

impl StreamOpenMeta {
    /// Construct from a known policy (sender side).
    #[inline]
    #[must_use]
    pub const fn new(policy: DeliveryPolicy) -> Self {
        Self { policy }
    }

    /// Encode to a single-byte [`Bytes`] for use as a `StreamOpen` payload.
    ///
    /// The returned allocation is one byte — cheap enough that we do not
    /// bother with a static singleton.
    #[inline]
    #[must_use]
    pub fn encode(&self) -> Bytes {
        Bytes::from(vec![self.policy.as_byte()])
    }

    /// Decode a `StreamOpen` payload. Trailing bytes (reserved extensions)
    /// are silently ignored to preserve forward compatibility.
    ///
    /// Returns [`DeliveryPolicyError::MalformedStreamOpenPayload`] if the
    /// payload is empty, or [`DeliveryPolicyError::UnknownPolicyByte`] if
    /// the discriminant is not in `0x01..=0x04`.
    #[inline]
    pub fn decode(payload: &[u8]) -> Result<Self, DeliveryPolicyError> {
        if payload.is_empty() {
            return Err(DeliveryPolicyError::MalformedStreamOpenPayload { len: 0 });
        }
        match DeliveryPolicy::from_byte(payload[0]) {
            Some(policy) => Ok(Self { policy }),
            None => Err(DeliveryPolicyError::UnknownPolicyByte { byte: payload[0] }),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn policy_round_trip_all_variants() {
        for p in [
            DeliveryPolicy::ReliableOrdered,
            DeliveryPolicy::ReliableUnordered,
            DeliveryPolicy::RealtimeDropOld,
            DeliveryPolicy::LatestOnly,
        ] {
            let b = p.as_byte();
            assert_eq!(DeliveryPolicy::from_byte(b), Some(p), "{p:?} byte {b:#x}");
        }
    }

    #[test]
    fn from_byte_rejects_unknown() {
        for b in [0x00u8, 0x05, 0x10, 0xFF] {
            assert_eq!(DeliveryPolicy::from_byte(b), None, "byte {b:#x}");
        }
    }

    #[test]
    fn meta_encode_decode() {
        let m = StreamOpenMeta::new(DeliveryPolicy::RealtimeDropOld);
        let bytes = m.encode();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], DeliveryPolicy::RealtimeDropOld.as_byte());
        let back = StreamOpenMeta::decode(&bytes).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn meta_decode_ignores_reserved_trailing_bytes() {
        let payload = [DeliveryPolicy::LatestOnly.as_byte(), 0xAA, 0xBB];
        let m = StreamOpenMeta::decode(&payload).unwrap();
        assert_eq!(m.policy, DeliveryPolicy::LatestOnly);
    }

    #[test]
    fn meta_decode_empty_payload_errors() {
        let err = StreamOpenMeta::decode(&[]).unwrap_err();
        assert!(matches!(
            err,
            DeliveryPolicyError::MalformedStreamOpenPayload { len: 0 }
        ));
    }

    #[test]
    fn meta_decode_unknown_discriminant_errors() {
        let err = StreamOpenMeta::decode(&[0x09]).unwrap_err();
        assert!(matches!(
            err,
            DeliveryPolicyError::UnknownPolicyByte { byte: 0x09 }
        ));
    }

    #[test]
    fn as_str_covers_all_variants() {
        for p in [
            DeliveryPolicy::ReliableOrdered,
            DeliveryPolicy::ReliableUnordered,
            DeliveryPolicy::RealtimeDropOld,
            DeliveryPolicy::LatestOnly,
        ] {
            // Smoke: non-empty, ASCII, lowercase.
            let s = p.as_str();
            assert!(!s.is_empty());
            assert!(s.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
        }
    }
}
