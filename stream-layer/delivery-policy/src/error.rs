//! Errors returned by the delivery-policy crate.

use thiserror::Error;

/// All errors emitted by [`crate::DeliveryPolicyTable`] and
/// [`crate::StreamOpenMeta`] decode.
#[derive(Debug, Error)]
pub enum DeliveryPolicyError {
    /// `MessageType::StreamOpen` payload was shorter than the 1-byte
    /// discriminant required to read the policy.
    #[error("stream-open payload too short ({len} bytes, need ≥ 1)")]
    MalformedStreamOpenPayload {
        /// Number of payload bytes actually present.
        len: usize,
    },

    /// The discriminant byte did not map to a defined [`DeliveryPolicy`](crate::DeliveryPolicy).
    #[error("unknown delivery-policy discriminant 0x{byte:02X}")]
    UnknownPolicyByte {
        /// The offending wire byte.
        byte: u8,
    },

    /// [`DeliveryPolicyTable::bind`](crate::DeliveryPolicyTable::bind) was
    /// called past the configured [`MAX_TRACKED_STREAMS`](crate::table::MAX_TRACKED_STREAMS).
    ///
    /// Bounded to prevent a misbehaving or hostile peer from exhausting
    /// server memory by opening millions of streams.
    #[error("delivery-policy table full (cap={cap})")]
    TableFull {
        /// The capacity that was exceeded.
        cap: usize,
    },
}
