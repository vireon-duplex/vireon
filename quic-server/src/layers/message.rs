//! Message layer — frame validation and routing classification.
//!
//! The frame is already decoded by the QUIC engine (transport-layer).
//! This layer classifies each frame and returns a routing decision
//! that the stream layer uses to decide whether to dispatch upward
//! to the application layer or consume it internally.

use frame::codec::FrameRef;

/// Routing action returned by the message layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameAction {
    /// Data-plane frame — should be dispatched to the application layer.
    DispatchToApp,
    /// Control-plane frame — consumed by a lower layer (stream/session).
    Consumed,
}

/// Message layer — stateless frame classifier.
///
/// Owns no mutable state. The frame codec lives in the `frame` crate
/// and is exercised by `QuicEngine` during decode. This struct adds
/// the classification step that sits between transport-layer decode
/// and stream-layer routing.
pub struct MessageLayer;

impl MessageLayer {
    /// Creates a new message layer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Classifies a decoded frame into a routing action.
    ///
    /// Data frames (`MessageType::is_data()`) are dispatched to the
    /// application layer. All other frames (StreamOpen, StreamClose,
    /// StreamReset, Handshake, Fragment, Ack) are consumed by lower
    /// layers.
    #[must_use]
    pub fn classify(&self, frame: &FrameRef<'_>) -> FrameAction {
        if frame.msg_type.is_data() {
            FrameAction::DispatchToApp
        } else {
            FrameAction::Consumed
        }
    }
}

impl Default for MessageLayer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frame::codec::{FrameHeader, decode, encode_into_slice};
    use frame::header::{MessageType, StreamId};

    fn classify_msg_type(ml: &MessageLayer, msg_type: MessageType) -> FrameAction {
        let header = FrameHeader::new(StreamId::new(1), msg_type);
        let payload = b"x";
        let mut buf = vec![0u8; frame::codec::HEADER_SIZE + payload.len() + frame::codec::CRC_SIZE];
        let _ = encode_into_slice(&mut buf, header, payload);
        let frame = decode(&buf).unwrap();
        ml.classify(&frame)
    }

    #[test]
    fn data_frames_dispatch_to_app() {
        let ml = MessageLayer::new();
        assert_eq!(
            classify_msg_type(&ml, MessageType::TextToken),
            FrameAction::DispatchToApp
        );
    }

    #[test]
    fn control_frames_are_consumed() {
        let ml = MessageLayer::new();
        for mt in [
            MessageType::StreamOpen,
            MessageType::StreamClose,
            MessageType::StreamReset,
            MessageType::Handshake,
            MessageType::Ack,
            MessageType::Fragment,
        ] {
            assert_eq!(classify_msg_type(&ml, mt), FrameAction::Consumed, "{mt:?}");
        }
    }
}
