//! Stream layer — stream lifecycle management + per-stream sequence tracking.
//!
//! Mirrors `quic-server/src/layers/stream.rs` but simplified for the
//! client's single connection. Adds a send-path `build_header` method
//! that the application layer uses to construct outgoing frame headers
//! with incrementing per-stream sequence numbers.

use std::collections::{HashMap, HashSet};

use frame::codec::{FrameHeader, FrameRef};
use frame::header::{MessageType, Seq, StreamId};
use stream_manager::StreamSlab;

use super::message::{ClientMessageLayer, FrameAction};

/// Stream layer — manages stream lifecycle and per-stream sequence numbers.
pub struct ClientStreamLayer {
    /// Message layer (owned — sits below stream in the stack).
    message: ClientMessageLayer,
    /// Stream slab — allocates/tracks stream slots.
    slab: StreamSlab,
    /// Open logical stream IDs (single connection — no per-conn map needed).
    open_streams: HashSet<u64>,
    /// Per-stream sequence counter for the send path.
    next_seq: HashMap<u64, Seq>,
}

impl ClientStreamLayer {
    /// Creates a new stream layer with default capacity.
    #[must_use]
    pub fn new() -> Self {
        Self {
            message: ClientMessageLayer::new(),
            slab: StreamSlab::default(),
            open_streams: HashSet::new(),
            next_seq: HashMap::new(),
        }
    }

    // ── Send path ──────────────────────────────────────────────────

    /// Builds a `FrameHeader` for an outgoing frame, advancing the
    /// per-stream sequence counter.
    ///
    /// If this is the first frame on `logical_stream`, the stream is
    /// implicitly opened in the slab.
    pub fn build_header(
        &mut self,
        logical_stream: u64,
        msg_type: MessageType,
        flags: frame::header::FrameFlags,
    ) -> FrameHeader {
        // Lazily open the stream slab slot on first use.
        if self.open_streams.insert(logical_stream) {
            let _ = self.slab.try_reserve();
        }

        let seq = self.next_seq.entry(logical_stream).or_insert(Seq::ZERO);
        let header = FrameHeader::new(StreamId::new(logical_stream), msg_type)
            .with_seq(*seq)
            .with_flags(flags);
        *seq = seq.next();
        header
    }

    // ── Receive path ───────────────────────────────────────────────

    /// Processes a received frame: classifies via message layer, then
    /// handles stream lifecycle for control frames.
    pub fn on_frame(&mut self, frame: &FrameRef<'_>) -> FrameAction {
        let action = self.message.classify(frame);

        if action == FrameAction::Consumed {
            let logical_stream = frame.stream_id.get();
            match frame.msg_type {
                MessageType::StreamOpen => {
                    if self.open_streams.insert(logical_stream) {
                        let _ = self.slab.try_reserve();
                    }
                }
                MessageType::StreamClose | MessageType::StreamReset => {
                    if self.open_streams.remove(&logical_stream) {
                        self.slab.release();
                    }
                }
                MessageType::Handshake | MessageType::Fragment | MessageType::Ack => {}
                _ => {}
            }
        }

        action
    }

    // ── Lifecycle ──────────────────────────────────────────────────

    /// Releases all open streams.
    ///
    /// Returns the number of streams released.
    pub fn on_close(&mut self) -> usize {
        let count = self.open_streams.len();
        for _ in 0..count {
            self.slab.release();
        }
        self.open_streams.clear();
        self.next_seq.clear();
        count
    }

    /// Returns the number of open streams.
    #[must_use]
    #[allow(dead_code)]
    pub fn stream_count(&self) -> usize {
        self.open_streams.len()
    }
}

impl Default for ClientStreamLayer {
    fn default() -> Self {
        Self::new()
    }
}
