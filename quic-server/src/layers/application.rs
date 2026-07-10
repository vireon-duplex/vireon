//! Application layer — the outermost layer.
//!
//! Owns the `SessionLayer` and an `AppDispatcher` implementation.
//! This is where application services (AI streaming, RPC, media
//! streaming, etc.) plug into the data flow.
//!
//! ```text
//! ApplicationLayer
//!   ├─ SessionLayer (owns StreamLayer → MessageLayer)
//!   └─ dispatcher: LoggingDispatcher (owns AiStreamPipeline)
//! ```

use std::net::SocketAddr;

use ai_streaming::{AiStreamConfig, AiStreamPipeline, TokenChunk};
use frame::codec::Frame;
use frame::header::MessageType;
use session_manager::SessionId;

use super::message::FrameAction;
use super::session::SessionLayer;

// ── AppDispatcher trait ────────────────────────────────────────────

/// Trait implemented by application-layer services to receive
/// data-plane events from the pipeline.
///
/// All callbacks are synchronous and run on the reactor thread.
/// Implementations must be non-blocking — offload heavy work to a
/// separate task queue or thread pool.
pub trait AppDispatcher {
    /// Called when a QUIC connection completes handshake and a new
    /// session is created.
    fn on_connection_established(&mut self, session_id: SessionId, peer: SocketAddr);

    /// Called for every data-plane frame received.
    fn on_data_frame(&mut self, session_id: SessionId, conn_idx: usize, frame: &Frame);

    /// Called when a session is destroyed (connection closed).
    fn on_connection_closed(&mut self, session_id: SessionId, reason: Option<&str>);
}

// ── LoggingDispatcher ──────────────────────────────────────────────

/// Default dispatcher that logs all events and routes AI token frames
/// through the [`AiStreamPipeline`].
pub struct LoggingDispatcher {
    /// [application-layer] AI streaming pipeline for token routing.
    #[allow(dead_code)]
    ai_pipeline: AiStreamPipeline,
}

impl LoggingDispatcher {
    /// Creates a new dispatcher with a default AI streaming pipeline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ai_pipeline: AiStreamPipeline::new(AiStreamConfig::default()),
        }
    }
}

impl Default for LoggingDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl AppDispatcher for LoggingDispatcher {
    fn on_connection_established(&mut self, session_id: SessionId, peer: SocketAddr) {
        tracing::info!(session_id = ?session_id, %peer, "[app] connection established");
    }

    fn on_data_frame(&mut self, session_id: SessionId, conn_idx: usize, frame: &Frame) {
        // [application-layer] Route AI token frames through the pipeline.
        if frame.msg_type == MessageType::TextToken {
            let chunk = TokenChunk {
                token_id: frame.seq.get(),
                text: String::from_utf8_lossy(&frame.payload).into_owned(),
                is_final: frame.flags.contains(frame::header::FrameFlags::FIN),
            };
            tracing::info!(
                conn_idx,
                session_id = ?session_id,
                token_id = chunk.token_id,
                text_len = chunk.text.len(),
                is_final = chunk.is_final,
                "[app:ai-streaming] TextToken → TokenChunk"
            );
            return;
        }

        tracing::info!(
            conn_idx,
            session_id = ?session_id,
            msg_type = ?frame.msg_type,
            stream_id = frame.stream_id.get(),
            seq = frame.seq.get(),
            payload_len = frame.payload.len(),
            "[app] data frame"
        );
    }

    fn on_connection_closed(&mut self, session_id: SessionId, reason: Option<&str>) {
        tracing::info!(
            session_id = ?session_id,
            reason = reason.unwrap_or("none"),
            "[app] connection closed"
        );
    }
}

// ── ApplicationLayer ───────────────────────────────────────────────

/// Application layer — owns the session layer and the application
/// dispatcher.
pub struct ApplicationLayer {
    /// Session layer (owned — sits below application in the stack).
    session: SessionLayer,
    /// Application dispatch boundary.
    dispatcher: LoggingDispatcher,
}

impl ApplicationLayer {
    /// Creates a new application layer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            session: SessionLayer::new(),
            dispatcher: LoggingDispatcher::new(),
        }
    }

    /// Creates a session and notifies the application dispatcher.
    ///
    /// Returns `Some(SessionId)` on success, `None` if the session
    /// table or connection shard is full.
    pub fn on_connect(&mut self, conn_idx: usize, peer: SocketAddr) -> Option<SessionId> {
        let session_id = self.session.on_connect(conn_idx)?;

        // Notify application dispatcher.
        self.dispatcher.on_connection_established(session_id, peer);

        tracing::info!(
            conn_idx,
            session_id = ?session_id,
            session_occupancy = self.session.occupancy(),
            %peer,
            "session created"
        );

        Some(session_id)
    }

    /// Processes a frame through the session/stream layers, then
    /// dispatches to the application if it's a data frame.
    pub fn on_frame(&mut self, conn_idx: usize, session_id: SessionId, frame: &Frame) {
        let action = self.session.on_frame(conn_idx, frame);

        if action == FrameAction::DispatchToApp {
            self.dispatcher.on_data_frame(session_id, conn_idx, frame);
        }
    }

    /// Tears down the session and notifies the application dispatcher.
    pub fn on_close(&mut self, conn_idx: usize, session_id: SessionId, reason: Option<&str>) {
        let released = self.session.on_close(conn_idx);

        self.dispatcher.on_connection_closed(session_id, reason);

        tracing::info!(
            conn_idx,
            session_id = ?session_id,
            open_streams = released,
            reason = reason.unwrap_or("none"),
            "session destroyed"
        );
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the session table occupancy.
    #[must_use]
    pub fn session_occupancy(&self) -> usize {
        self.session.occupancy()
    }

    /// Returns a reference to the owned session layer.
    #[must_use]
    #[allow(dead_code)]
    pub const fn session(&self) -> &SessionLayer {
        &self.session
    }
}

impl Default for ApplicationLayer {
    fn default() -> Self {
        Self::new()
    }
}
