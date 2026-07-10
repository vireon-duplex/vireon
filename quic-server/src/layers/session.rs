//! Session layer — session lifecycle management.
//!
//! Owns the `StreamLayer` and the `SessionTable`. Session creation
//! and destruction delegate to the stream layer for per-connection
//! stream initialization and cleanup.

use frame::codec::Frame;
use session_manager::table::SessionTable;
use session_manager::{SessionId, SessionManagerConfig};

use super::message::FrameAction;
use super::stream::StreamLayer;

/// Session layer — manages session slots and delegates frame
/// processing to the stream layer.
pub struct SessionLayer {
    /// Stream layer (owned — sits below session in the stack).
    stream: StreamLayer,
    /// Session slab — allocates/tracks session slots.
    table: SessionTable,
}

impl SessionLayer {
    /// Creates a new session layer with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stream: StreamLayer::new(),
            table: SessionTable::new(SessionManagerConfig::default()),
        }
    }

    /// Creates a new session: reserves a slot and initializes stream
    /// state for the connection.
    ///
    /// Returns the application-level `SessionId`. If the session table
    /// is full, returns `None`.
    pub fn on_connect(&mut self, conn_idx: usize) -> Option<SessionId> {
        match self.table.try_reserve_slot() {
            Ok(generation) => {
                tracing::debug!(conn_idx, session_gen = generation, "session slot reserved");
            }
            Err(e) => {
                tracing::warn!(conn_idx, error = ?e, "session table full");
                return None;
            }
        }

        // Delegate to stream layer for per-connection stream init.
        self.stream.on_connect(conn_idx);

        let session_id = SessionId::new();
        Some(session_id)
    }

    /// Delegates frame processing to the stream layer.
    ///
    /// Returns the `FrameAction` from the stream layer so the
    /// application layer can decide whether to dispatch.
    pub fn on_frame(&mut self, conn_idx: usize, frame: &Frame) -> FrameAction {
        self.stream.on_frame(conn_idx, frame)
    }

    /// Destroys a session: releases streams and the session slot.
    ///
    /// Returns the number of streams released.
    pub fn on_close(&mut self, conn_idx: usize) -> usize {
        let released = self.stream.on_close(conn_idx);
        self.table.release_slot();
        released
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the session table occupancy.
    #[must_use]
    pub fn occupancy(&self) -> usize {
        self.table.occupancy() as usize
    }

    /// Returns a reference to the owned stream layer (for introspection).
    #[must_use]
    #[allow(dead_code)]
    pub const fn stream(&self) -> &StreamLayer {
        &self.stream
    }
}

impl Default for SessionLayer {
    fn default() -> Self {
        Self::new()
    }
}
