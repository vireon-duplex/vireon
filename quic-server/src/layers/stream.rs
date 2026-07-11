//! Stream layer — stream lifecycle management.
//!
//! Owns the `MessageLayer` (frame classification) and `StreamSlab`
//! (slot allocation). Tracks per-connection logical stream IDs so
//! that all streams can be released when a connection closes.

use std::collections::{HashMap, HashSet};

use frame::codec::FrameRef;
use frame::header::MessageType;
use stream_manager::StreamSlab;
use stream_scheduler::budget::SchedulingBudget;
use stream_scheduler::builder::SchedulerBuilder;
use stream_scheduler::scheduler::StreamScheduler;

use super::message::{FrameAction, MessageLayer};
use crate::error::ServerError;

/// Stream layer — manages stream lifecycle and delegates frame
/// classification to the message layer.
pub struct StreamLayer {
    /// Message layer (owned — sits below stream in the stack).
    message: MessageLayer,
    /// Stream slab — allocates/tracks stream slots.
    slab: StreamSlab,
    /// Per-connection logical stream IDs.
    conn_streams: HashMap<usize, HashSet<u64>>,
    /// Priority scheduling + flow control.
    pub scheduler: StreamScheduler,
}

impl StreamLayer {
    /// Creates a new stream layer with default capacity.
    pub fn new() -> Result<Self, ServerError> {
        let scheduler = SchedulerBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("stream-scheduler: {e:?}")))?;

        Ok(Self {
            message: MessageLayer::new(),
            slab: StreamSlab::default(),
            conn_streams: HashMap::new(),
            scheduler,
        })
    }

    /// Initializes stream-tracking state for a new connection.
    pub fn on_connect(&mut self, conn_idx: usize) {
        self.conn_streams.insert(conn_idx, HashSet::new());
    }

    /// Processes a frame: classifies via message layer, then handles
    /// stream lifecycle for control frames.
    ///
    /// Returns `FrameAction::DispatchToApp` for data frames so the
    /// caller (session → application) can dispatch them.
    pub fn on_frame(&mut self, conn_idx: usize, frame: &FrameRef<'_>) -> FrameAction {
        let action = self.message.classify(frame);

        // [stream-scheduler] Drive the scheduler each tick — records
        // stats and (in Phase 1+) returns priority-ordered selections.
        let mut budget = SchedulingBudget::default_budget(64);
        if let Err(e) = self.scheduler.tick(&mut budget) {
            // NoStreamsReady / BudgetExhausted are expected on idle ticks.
            tracing::trace!(error = ?e, "[stream-scheduler] tick");
        }

        if action == FrameAction::Consumed {
            // Control-plane frame — handle stream lifecycle.
            let logical_stream = frame.stream_id.get();
            match frame.msg_type {
                MessageType::StreamOpen => match self.slab.try_reserve() {
                    Ok(_) => {
                        self.conn_streams
                            .entry(conn_idx)
                            .or_default()
                            .insert(logical_stream);
                        tracing::info!(
                            conn_idx,
                            stream_id = logical_stream,
                            open_streams = self.conn_streams.get(&conn_idx).map_or(0, HashSet::len),
                            stream_occupancy = self.slab.occupancy(),
                            "stream opened"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            conn_idx,
                            stream_id = logical_stream,
                            error = ?e,
                            "stream slab full — cannot open stream"
                        );
                    }
                },
                MessageType::StreamClose | MessageType::StreamReset => {
                    if self
                        .conn_streams
                        .entry(conn_idx)
                        .or_default()
                        .remove(&logical_stream)
                    {
                        self.slab.release();
                        tracing::info!(
                            conn_idx,
                            stream_id = logical_stream,
                            msg_type = ?frame.msg_type,
                            "stream closed"
                        );
                    }
                }
                MessageType::Handshake => {
                    tracing::info!(
                        conn_idx,
                        len = frame.payload.len(),
                        "handshake control frame"
                    );
                }
                MessageType::Fragment => {
                    tracing::debug!(
                        conn_idx,
                        stream_id = logical_stream,
                        seq = frame.seq.get(),
                        "fragment"
                    );
                }
                MessageType::Ack => {
                    tracing::debug!(
                        conn_idx,
                        stream_id = logical_stream,
                        seq = frame.seq.get(),
                        "ack"
                    );
                }
                // Data-plane types are handled above.
                _ => {
                    tracing::warn!(
                        conn_idx,
                        msg_type = ?frame.msg_type,
                        "unexpected type in control dispatch"
                    );
                }
            }
        }

        action
    }

    /// Releases all streams for a closing connection.
    ///
    /// Returns the number of streams released.
    pub fn on_close(&mut self, conn_idx: usize) -> usize {
        if let Some(streams) = self.conn_streams.remove(&conn_idx) {
            let count = streams.len();
            for _ in &streams {
                self.slab.release();
            }
            count
        } else {
            0
        }
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the number of streams open for `conn_idx`.
    #[must_use]
    #[allow(dead_code)]
    pub fn stream_count(&self, conn_idx: usize) -> usize {
        self.conn_streams.get(&conn_idx).map_or(0, HashSet::len)
    }

    /// Returns the stream slab occupancy (total across all connections).
    #[must_use]
    #[allow(dead_code)]
    pub fn occupancy(&self) -> usize {
        self.slab.occupancy() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frame::codec::{FrameHeader, decode, encode_into_slice};
    use frame::header::{MessageType, StreamId};

    /// Helper: encode a frame and call `on_frame` in one step so the
    /// buffer lives long enough for the FrameRef borrow.
    fn send_frame(
        sl: &mut StreamLayer,
        conn_idx: usize,
        msg_type: MessageType,
        stream_id: u64,
    ) -> FrameAction {
        let header = FrameHeader::new(StreamId::new(stream_id), msg_type);
        let payload = b"x";
        let mut buf = vec![0u8; frame::codec::HEADER_SIZE + payload.len()];
        let _ = encode_into_slice(&mut buf, header, payload);
        let frame = decode(&buf).unwrap();
        sl.on_frame(conn_idx, &frame)
    }

    #[test]
    fn stream_open_close_lifecycle() {
        let mut sl = StreamLayer::new().unwrap();
        sl.on_connect(0);

        assert_eq!(
            send_frame(&mut sl, 0, MessageType::StreamOpen, 42),
            FrameAction::Consumed
        );
        assert_eq!(sl.stream_count(0), 1);

        assert_eq!(
            send_frame(&mut sl, 0, MessageType::StreamClose, 42),
            FrameAction::Consumed
        );
        assert_eq!(sl.stream_count(0), 0);
    }

    #[test]
    fn data_frame_dispatches_to_app() {
        let mut sl = StreamLayer::new().unwrap();
        sl.on_connect(0);
        assert_eq!(
            send_frame(&mut sl, 0, MessageType::TextToken, 1),
            FrameAction::DispatchToApp
        );
    }

    #[test]
    fn on_close_releases_all_streams() {
        let mut sl = StreamLayer::new().unwrap();
        sl.on_connect(0);
        send_frame(&mut sl, 0, MessageType::StreamOpen, 1);
        send_frame(&mut sl, 0, MessageType::StreamOpen, 2);
        send_frame(&mut sl, 0, MessageType::StreamOpen, 3);
        assert_eq!(sl.stream_count(0), 3);

        let released = sl.on_close(0);
        assert_eq!(released, 3);
        assert_eq!(sl.stream_count(0), 0);
    }
}
