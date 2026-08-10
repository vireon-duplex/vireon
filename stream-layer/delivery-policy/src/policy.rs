//! The [`DeliveryPolicy`] enum, the [`StreamPriority`] enum, and the
//! [`StreamOpenMeta`] wire format.
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

/// Egress scheduling priority for a stream — decoupled from [`DeliveryPolicy`].
///
/// While `DeliveryPolicy` describes *how* frames are delivered (ordered, lossy,
/// latest-only), `StreamPriority` describes *how urgently* the server should
/// drain them relative to other streams on the same connection. A Critical
/// audio stream wins every egress tick against a Low file-sync stream even
/// though both might use different delivery policies.
///
/// Encoded as a single byte in the `MessageType::StreamOpen` payload byte 1
/// (byte 0 is [`DeliveryPolicy`]) — see [`StreamOpenMeta`].
///
/// ## Defaults
///
/// When a `StreamOpen` payload omits byte 1 (legacy 1-byte clients), the server
/// derives a priority from the policy via [`default_priority_for_policy`] so
/// existing realtime clients keep their old egress-lane behaviour.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamPriority {
    /// Wins every egress contention. Use for audio, tactile feedback, or any
    /// stream where latency dominates utility.
    Critical = 0x01,
    /// Drains after Critical but before Normal. Use for video, presence, and
    /// state mutations that should jump ahead of bulk traffic.
    High = 0x02,
    /// Default lane — chat, AI token streams, RPC. Most streams belong here.
    Normal = 0x03,
    /// Background / bulk — file sync, telemetry, snapshot transfer. Drains
    /// last; may be starved under sustained higher-lane load (by design).
    Low = 0x04,
}

impl StreamPriority {
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
            0x01 => Self::Critical,
            0x02 => Self::High,
            0x03 => Self::Normal,
            0x04 => Self::Low,
            _ => return None,
        })
    }

    /// Human-readable name for logging / metrics labels.
    #[inline]
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::High => "high",
            Self::Normal => "normal",
            Self::Low => "low",
        }
    }
}

impl Default for StreamPriority {
    /// The default priority for newly-constructed stream specs and for legacy
    /// 1-byte `StreamOpen` payloads whose policy maps to neither realtime
    /// class.
    #[inline]
    fn default() -> Self {
        Self::Normal
    }
}

/// Derive a [`StreamPriority`] from a [`DeliveryPolicy`] for backward
/// compatibility with legacy 1-byte `StreamOpen` payloads (which carry policy
/// but no explicit priority byte).
///
/// Preserves the pre-expansion 3-level egress behaviour:
/// - Realtime policies (`RealtimeDropOld`, `LatestOnly`) → [`High`] (formerly
///   the `Realtime` lane).
/// - Reliable policies (`ReliableOrdered`, `ReliableUnordered`) → [`Normal`]
///   (formerly the `Throughput` lane).
///
/// New clients that want [`Critical`] or [`Low`] must send byte 1 explicitly.
///
/// [`High`]: StreamPriority::High
/// [`Normal`]: StreamPriority::Normal
/// [`Critical`]: StreamPriority::Critical
/// [`Low`]: StreamPriority::Low
#[inline]
#[must_use]
pub const fn default_priority_for_policy(policy: DeliveryPolicy) -> StreamPriority {
    match policy {
        DeliveryPolicy::RealtimeDropOld | DeliveryPolicy::LatestOnly => StreamPriority::High,
        DeliveryPolicy::ReliableOrdered | DeliveryPolicy::ReliableUnordered => {
            StreamPriority::Normal
        }
    }
}

/// Wire payload of a `MessageType::StreamOpen` frame.
///
/// ```text
/// byte 0:   DeliveryPolicy discriminant (0x01..=0x04)
/// byte 1:   StreamPriority discriminant (0x01..=0x04)
/// byte 2..: reserved (forward-compat for future extensions).
///           Receivers MUST ignore unknown trailing bytes.
/// ```
///
/// ## Backward compatibility
///
/// Legacy 1-byte payloads (byte 0 only) are accepted: the priority is
/// derived from the policy via [`default_priority_for_policy`], preserving
/// the pre-expansion 3-level egress behaviour for already-deployed clients.
///
/// `StreamOpen` payloads shorter than 1 byte are rejected as malformed —
/// a sender must always specify a policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamOpenMeta {
    /// The policy the sender has chosen for this stream.
    pub policy: DeliveryPolicy,
    /// The egress scheduling priority the sender has chosen for this stream.
    pub priority: StreamPriority,
}

impl StreamOpenMeta {
    /// Construct from a known policy and priority (sender side).
    #[inline]
    #[must_use]
    pub const fn new(policy: DeliveryPolicy, priority: StreamPriority) -> Self {
        Self { policy, priority }
    }

    /// Encode to a 2-byte [`Bytes`] for use as a `StreamOpen` payload.
    ///
    /// The returned allocation is two bytes — `[policy, priority]`.
    #[inline]
    #[must_use]
    pub fn encode(&self) -> Bytes {
        Bytes::from(vec![self.policy.as_byte(), self.priority.as_byte()])
    }

    /// Decode a `StreamOpen` payload.
    ///
    /// ## Backward compatibility
    ///
    /// - 1-byte payload (legacy): priority is derived from the policy via
    ///   [`default_priority_for_policy`] so existing realtime clients keep
    ///   their old egress-lane behaviour.
    /// - 2+ byte payload: byte 1 is read as the priority; unknown values
    ///   fall back to [`StreamPriority::Normal`] (rather than erroring) so
    ///   that newer clients with an unknown future priority byte do not get
    ///   rejected outright.
    ///
    /// Returns [`DeliveryPolicyError::MalformedStreamOpenPayload`] if the
    /// payload is empty, or [`DeliveryPolicyError::UnknownPolicyByte`] if
    /// the policy discriminant (byte 0) is not in `0x01..=0x04`.
    #[inline]
    pub fn decode(payload: &[u8]) -> Result<Self, DeliveryPolicyError> {
        if payload.is_empty() {
            return Err(DeliveryPolicyError::MalformedStreamOpenPayload { len: 0 });
        }
        let policy = match DeliveryPolicy::from_byte(payload[0]) {
            Some(p) => p,
            None => return Err(DeliveryPolicyError::UnknownPolicyByte { byte: payload[0] }),
        };
        // 1-byte legacy payload → derive priority from policy for backward compat.
        // 2+ byte payload → read byte 1; unknown → Normal fallback.
        let priority = if payload.len() >= 2 {
            StreamPriority::from_byte(payload[1]).unwrap_or(StreamPriority::Normal)
        } else {
            default_priority_for_policy(policy)
        };
        Ok(Self { policy, priority })
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
    fn priority_round_trip_all_variants() {
        for p in [
            StreamPriority::Critical,
            StreamPriority::High,
            StreamPriority::Normal,
            StreamPriority::Low,
        ] {
            let b = p.as_byte();
            assert_eq!(StreamPriority::from_byte(b), Some(p), "{p:?} byte {b:#x}");
        }
    }

    #[test]
    fn priority_from_byte_rejects_unknown() {
        for b in [0x00u8, 0x05, 0x10, 0xFF] {
            assert_eq!(StreamPriority::from_byte(b), None, "byte {b:#x}");
        }
    }

    #[test]
    fn default_priority_for_policy_preserves_old_lanes() {
        // Realtime policies → High (old Realtime lane).
        assert_eq!(
            default_priority_for_policy(DeliveryPolicy::RealtimeDropOld),
            StreamPriority::High
        );
        assert_eq!(
            default_priority_for_policy(DeliveryPolicy::LatestOnly),
            StreamPriority::High
        );
        // Reliable policies → Normal (old Throughput lane).
        assert_eq!(
            default_priority_for_policy(DeliveryPolicy::ReliableOrdered),
            StreamPriority::Normal
        );
        assert_eq!(
            default_priority_for_policy(DeliveryPolicy::ReliableUnordered),
            StreamPriority::Normal
        );
    }

    #[test]
    fn meta_encode_decode_all_priority_variants() {
        for policy in [
            DeliveryPolicy::ReliableOrdered,
            DeliveryPolicy::RealtimeDropOld,
        ] {
            for priority in [
                StreamPriority::Critical,
                StreamPriority::High,
                StreamPriority::Normal,
                StreamPriority::Low,
            ] {
                let m = StreamOpenMeta::new(policy, priority);
                let bytes = m.encode();
                assert_eq!(bytes.len(), 2, "2-byte payload");
                assert_eq!(bytes[0], policy.as_byte());
                assert_eq!(bytes[1], priority.as_byte());
                let back = StreamOpenMeta::decode(&bytes).unwrap();
                assert_eq!(back, m, "{policy:?} + {priority:?}");
            }
        }
    }

    #[test]
    fn meta_decode_legacy_1_byte_derives_priority() {
        // Legacy 1-byte payload: policy only, priority derived.
        let bytes = [DeliveryPolicy::RealtimeDropOld.as_byte()];
        let m = StreamOpenMeta::decode(&bytes).unwrap();
        assert_eq!(m.policy, DeliveryPolicy::RealtimeDropOld);
        assert_eq!(m.priority, StreamPriority::High); // derived from RealtimeDropOld

        let bytes = [DeliveryPolicy::ReliableOrdered.as_byte()];
        let m = StreamOpenMeta::decode(&bytes).unwrap();
        assert_eq!(m.policy, DeliveryPolicy::ReliableOrdered);
        assert_eq!(m.priority, StreamPriority::Normal); // derived from ReliableOrdered
    }

    #[test]
    fn meta_decode_unknown_priority_byte_falls_back_to_normal() {
        // Byte 1 set to an unknown value → Normal, NOT an error.
        let payload = [DeliveryPolicy::LatestOnly.as_byte(), 0xFF];
        let m = StreamOpenMeta::decode(&payload).unwrap();
        assert_eq!(m.policy, DeliveryPolicy::LatestOnly);
        assert_eq!(m.priority, StreamPriority::Normal);
    }

    #[test]
    fn meta_decode_ignores_reserved_trailing_bytes() {
        // 2-byte meta + reserved trailing bytes — trailing ignored.
        let payload = [
            DeliveryPolicy::LatestOnly.as_byte(),
            StreamPriority::Critical.as_byte(),
            0xAA,
            0xBB,
        ];
        let m = StreamOpenMeta::decode(&payload).unwrap();
        assert_eq!(m.policy, DeliveryPolicy::LatestOnly);
        assert_eq!(m.priority, StreamPriority::Critical);
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
    fn meta_decode_unknown_policy_discriminant_errors() {
        // Unknown policy byte (byte 0) → still an error (not recoverable).
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
        for p in [
            StreamPriority::Critical,
            StreamPriority::High,
            StreamPriority::Normal,
            StreamPriority::Low,
        ] {
            let s = p.as_str();
            assert!(!s.is_empty());
            assert!(s.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
        }
    }
}
