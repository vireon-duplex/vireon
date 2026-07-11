//! Application layer — the outermost layer for the bench client.
//!
//! Mirrors `quic-server/src/layers/application.rs` but instead of
//! `LoggingDispatcher` + `AiStreamPipeline`, the client tracks
//! send/recv frame counts for benchmark statistics.
//!
//! ```text
//! ClientApplicationLayer
//!   ├─ ClientSessionLayer (owns StreamLayer → MessageLayer)
//!   └─ counters: frames_tx, frames_rx, bytes_rx
//! ```

use frame::codec::{FrameHeader, FrameRef};
use frame::header::{FrameFlags, MessageType};
use session_manager::SessionId;

use super::message::FrameAction;
use super::session::ClientSessionLayer;

/// Application layer — top-level entry point for the benchmark client.
pub struct ClientApplicationLayer {
    /// Session layer (owned).
    session: ClientSessionLayer,
    /// Total frames dispatched down the stack (send path).
    frames_tx: u64,
    /// Total data frames dispatched up to the application (recv path).
    frames_rx: u64,
    /// Total payload bytes received (recv path).
    bytes_rx: u64,
}

impl ClientApplicationLayer {
    /// Creates a new application layer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            session: ClientSessionLayer::new(),
            frames_tx: 0,
            frames_rx: 0,
            bytes_rx: 0,
        }
    }

    /// Creates a session.
    ///
    /// Returns `None` if the session table is full.
    pub fn on_connect(&mut self) -> Option<SessionId> {
        self.session.on_connect()
    }

    // ── Send path ──────────────────────────────────────────────────

    /// Builds a `FrameHeader` for an outgoing frame and records the send.
    ///
    /// This is the entry point for the send path: the caller passes the
    /// logical stream ID, message type, and flags; the application layer
    /// delegates header construction down through session → stream, where
    /// the per-stream sequence counter is advanced.
    pub fn prepare_frame(
        &mut self,
        logical_stream: u64,
        msg_type: MessageType,
        flags: FrameFlags,
    ) -> FrameHeader {
        self.frames_tx += 1;
        self.session.build_header(logical_stream, msg_type, flags)
    }

    // ── Receive path ───────────────────────────────────────────────

    /// Processes a decoded frame through the layer stack.
    ///
    /// If the frame is a data frame, counters are updated.
    pub fn on_frame(&mut self, frame: &FrameRef<'_>) {
        let action = self.session.on_frame(frame);
        if action == FrameAction::DispatchToApp {
            self.frames_rx += 1;
            self.bytes_rx += frame.payload.len() as u64;
        }
    }

    // ── Lifecycle ──────────────────────────────────────────────────

    /// Tears down the session.
    ///
    /// Returns the number of streams released.
    pub fn on_close(&mut self) -> usize {
        self.session.on_close()
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the total frames sent.
    #[must_use]
    pub fn frames_tx(&self) -> u64 {
        self.frames_tx
    }

    /// Returns the total data frames received.
    #[must_use]
    pub fn frames_rx(&self) -> u64 {
        self.frames_rx
    }

    /// Returns the total payload bytes received.
    #[must_use]
    pub fn bytes_rx(&self) -> u64 {
        self.bytes_rx
    }
}

impl Default for ClientApplicationLayer {
    fn default() -> Self {
        Self::new()
    }
}
