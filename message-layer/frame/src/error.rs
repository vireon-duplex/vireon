//! Typed errors for frame-layer encode / decode operations.

/// Errors returned by [`encode`](super::encode_into) and [`decode`](super::decode).
///
/// `IncompleteHeader` / `IncompletePayload` are returned by the one-shot
/// [`decode`] functions when the input buffer does not hold a whole frame. The
/// streaming [`FrameDecoder`](super::FrameDecoder) signals the same conditions
/// with `Ok(None)` — those variants never escape it.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    /// The input buffer was shorter than the 13-byte fixed header.
    #[error("incomplete frame header: need {needed} bytes, have {have}")]
    IncompleteHeader { needed: usize, have: usize },

    /// The header declared a payload length larger than the bytes that followed it.
    #[error("incomplete payload: declared {declared} bytes, have {have}")]
    IncompletePayload { declared: usize, have: usize },

    /// A payload length (or frame body) exceeded the configured upper bound.
    ///
    /// Returned by [`FrameDecoder`](super::FrameDecoder) when the wire-declared
    /// length surpasses `max_payload_len`, and by the one-shot helpers when the
    /// payload cannot fit in a `u32` length field.
    #[error("payload length {len} exceeds maximum {max}")]
    PayloadTooLarge { len: usize, max: usize },

    /// The destination slice was smaller than the encoded frame needed.
    ///
    /// Returned by [`encode_into_slice`](super::encode_into_slice) (and the
    /// pool-backed encoders that build on it) when the caller hands in a
    /// buffer shorter than `HEADER_SIZE + payload.len()`.
    #[error("destination buffer too small: need {needed} bytes, have {have}")]
    BufferTooSmall { needed: usize, have: usize },

    /// The message-type byte does not map to any known
    /// [`MessageType`](super::MessageType).
    #[error("unknown message type byte: 0x{value:02X}")]
    UnknownMessageType { value: u8 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_informative() {
        assert_eq!(
            FrameError::IncompleteHeader {
                needed: 13,
                have: 5
            }
            .to_string()
            .as_str(),
            "incomplete frame header: need 13 bytes, have 5"
        );
        assert_eq!(
            FrameError::IncompletePayload {
                declared: 100,
                have: 10
            }
            .to_string()
            .as_str(),
            "incomplete payload: declared 100 bytes, have 10"
        );
        assert_eq!(
            FrameError::PayloadTooLarge { len: 999, max: 256 }
                .to_string()
                .as_str(),
            "payload length 999 exceeds maximum 256"
        );
        assert_eq!(
            FrameError::BufferTooSmall {
                needed: 64,
                have: 32
            }
            .to_string()
            .as_str(),
            "destination buffer too small: need 64 bytes, have 32"
        );
        assert_eq!(
            FrameError::UnknownMessageType { value: 0xFF }
                .to_string()
                .as_str(),
            "unknown message type byte: 0xFF"
        );
    }
}
