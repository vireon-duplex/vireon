//! Message layer — frame validation and routing classification.
//!
//! Mirrors `quic-server/src/layers/message.rs`. Stateless classifier that
//! sits between transport-layer decode and stream-layer routing.

use frame::codec::FrameRef;

/// Routing action returned by the message layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameAction {
    /// Data-plane frame — dispatch to the application layer.
    DispatchToApp,
    /// Control-plane frame — consumed by a lower layer.
    Consumed,
}

/// Message layer — stateless frame classifier.
pub struct ClientMessageLayer;

impl ClientMessageLayer {
    /// Creates a new message layer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Classifies a decoded frame into a routing action.
    #[must_use]
    pub fn classify(&self, frame: &FrameRef<'_>) -> FrameAction {
        if frame.msg_type.is_data() {
            FrameAction::DispatchToApp
        } else {
            FrameAction::Consumed
        }
    }
}

impl Default for ClientMessageLayer {
    fn default() -> Self {
        Self::new()
    }
}
