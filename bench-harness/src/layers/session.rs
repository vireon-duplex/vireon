//! Session layer — session lifecycle management.
//!
//! Mirrors `quic-server/src/layers/session.rs` but simplified for the
//! client's single connection.

use frame::codec::{FrameHeader, FrameRef};
use frame::header::{FrameFlags, MessageType};
use session_manager::table::SessionTable;
use session_manager::{SessionId, SessionManagerConfig};

use super::message::FrameAction;
use super::stream::ClientStreamLayer;

/// Session layer — manages the session slot and delegates to the stream layer.
pub struct ClientSessionLayer {
    /// Stream layer (owned — sits below session in the stack).
    stream: ClientStreamLayer,
    /// Session slab — allocates/tracks the session slot.
    table: SessionTable,
}

impl ClientSessionLayer {
    /// Creates a new session layer with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stream: ClientStreamLayer::new(),
            table: SessionTable::new(SessionManagerConfig::default()),
        }
    }

    /// Creates a session: reserves a slot.
    ///
    /// Returns the application-level `SessionId`, or `None` if the table is full.
    pub fn on_connect(&mut self) -> Option<SessionId> {
        match self.table.try_reserve_slot() {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = ?e, "session table full");
                return None;
            }
        }
        Some(SessionId::new())
    }

    // ── Send path ──────────────────────────────────────────────────

    /// Delegates header building to the stream layer.
    pub fn build_header(
        &mut self,
        logical_stream: u64,
        msg_type: MessageType,
        flags: FrameFlags,
    ) -> FrameHeader {
        self.stream.build_header(logical_stream, msg_type, flags)
    }

    // ── Receive path ───────────────────────────────────────────────

    /// Delegates frame processing to the stream layer.
    pub fn on_frame(&mut self, frame: &FrameRef<'_>) -> FrameAction {
        self.stream.on_frame(frame)
    }

    // ── Lifecycle ──────────────────────────────────────────────────

    /// Destroys the session: releases streams and the session slot.
    ///
    /// Returns the number of streams released.
    pub fn on_close(&mut self) -> usize {
        let released = self.stream.on_close();
        self.table.release_slot();
        released
    }
}

impl Default for ClientSessionLayer {
    fn default() -> Self {
        Self::new()
    }
}
