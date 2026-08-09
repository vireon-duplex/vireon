//! Stream layer — stream lifecycle management.
//!
//! Owns the `MessageLayer` (frame classification) and `StreamSlab`
//! (slot allocation). Tracks per-connection logical stream IDs so
//! that all streams can be released when a connection closes.

use rustc_hash::FxHashMap;
use std::collections::HashSet;
use std::time::{Duration, Instant};

use frame::codec::FrameRef;
use frame::header::MessageType;
use frame::reassembly::FragmentReassembler;
use stream_manager::StreamSlab;
use stream_scheduler::budget::SchedulingBudget;
use stream_scheduler::builder::SchedulerBuilder;
use stream_scheduler::scheduler::StreamScheduler;

use super::message::{FrameAction, MessageLayer};
use crate::conf::LayerTuning;
use crate::error::ServerError;

/// Minimum interval between repeated warn logs for the same condition.
///
/// The first occurrence always logs immediately; subsequent identical
/// warnings within this interval are silently counted.
const WARN_THROTTLE: Duration = Duration::from_secs(10);

/// Rate-limits a repeating warning so it logs at most once per
/// [`WARN_THROTTLE`] interval, while tracking the total occurrence count.
#[derive(Debug)]
struct WarnThrottle {
    /// Total occurrences since startup.
    total: u64,
    /// When we last emitted the warn (`None` = never).
    last: Option<Instant>,
}

impl WarnThrottle {
    const fn new() -> Self {
        Self {
            total: 0,
            last: None,
        }
    }

    /// Increments the total count and returns `true` if the caller
    /// should emit a log line now (first occurrence or throttle
    /// interval elapsed).
    fn tick(&mut self) -> bool {
        self.total = self.total.wrapping_add(1);
        let now = Instant::now();
        match self.last {
            None => {
                self.last = Some(now);
                true
            }
            Some(t) => {
                if now.saturating_duration_since(t) >= WARN_THROTTLE {
                    self.last = Some(now);
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// Stream layer — manages stream lifecycle and delegates frame
/// classification to the message layer.
pub struct StreamLayer {
    /// Message layer (owned — sits below stream in the stack).
    message: MessageLayer,
    /// Stream slab — allocates/tracks stream slots.
    slab: StreamSlab,
    /// Per-connection logical stream IDs.
    conn_streams: FxHashMap<usize, HashSet<u64>>,
    /// Priority scheduling + flow control.
    pub scheduler: StreamScheduler,
    /// Fragment reassembler — collects Fragment frames per stream.
    reassembler: FragmentReassembler,
    // ── Warn throttles (prevent log spam under sustained error conditions) ──
    slab_full_throttle: WarnThrottle,
    frag_overflow_throttle: WarnThrottle,
    unexpected_type_throttle: WarnThrottle,
}

impl StreamLayer {
    /// Creates a new stream layer with tuned scheduler configuration.
    pub fn new(t: &LayerTuning) -> Result<Self, ServerError> {
        let scheduler = SchedulerBuilder::new()
            .max_streams_per_tick(t.scheduler_max_streams_per_tick)
            .batch_size_max(t.scheduler_max_streams_per_tick.min(64))
            .starvation_threshold_us(t.scheduler_starvation_threshold_us)
            .build()
            .map_err(|e| ServerError::EngineInit(format!("stream-scheduler: {e:?}")))?;

        Ok(Self {
            message: MessageLayer::new(),
            slab: StreamSlab::default(),
            conn_streams: FxHashMap::default(),
            scheduler,
            reassembler: FragmentReassembler::new(),
            slab_full_throttle: WarnThrottle::new(),
            frag_overflow_throttle: WarnThrottle::new(),
            unexpected_type_throttle: WarnThrottle::new(),
        })
    }

    /// Initializes stream-tracking state for a new connection.
    pub fn on_connect(&mut self, conn_idx: usize) {
        self.conn_streams.insert(conn_idx, HashSet::new());
    }

    /// Drives the stream scheduler once. Call this once per recv batch,
    /// not per frame — the scheduler records stats and returns
    /// priority-ordered selections.
    pub fn on_batch_tick(&mut self) {
        let mut budget = SchedulingBudget::default_budget(64);
        if let Err(e) = self.scheduler.tick(&mut budget) {
            tracing::trace!(error = ?e, "[stream-scheduler] tick");
        }
    }

    /// Processes a frame: classifies via message layer, then handles
    /// stream lifecycle for control frames.
    ///
    /// Returns `FrameAction::DispatchToApp` for data frames so the
    /// caller (session → application) can dispatch them.
    pub fn on_frame(&mut self, conn_idx: usize, frame: &FrameRef<'_>) -> FrameAction {
        let action = self.message.classify(frame);

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
                        tracing::trace!(
                            conn_idx,
                            stream_id = logical_stream,
                            open_streams = self.conn_streams.get(&conn_idx).map_or(0, HashSet::len),
                            stream_occupancy = self.slab.occupancy(),
                            "stream opened"
                        );
                    }
                    Err(e) => {
                        if self.slab_full_throttle.tick() {
                            tracing::warn!(
                                conn_idx,
                                stream_id = logical_stream,
                                total = self.slab_full_throttle.total,
                                error = ?e,
                                "stream slab full — cannot open stream"
                            );
                        }
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
                        tracing::trace!(
                            conn_idx,
                            stream_id = logical_stream,
                            msg_type = ?frame.msg_type,
                            "stream closed"
                        );
                    }
                }
                MessageType::Handshake => {
                    tracing::trace!(
                        conn_idx,
                        len = frame.payload.len(),
                        "handshake control frame"
                    );
                }
                MessageType::Fragment => match self.reassembler.on_fragment(frame) {
                    Ok(Some(reassembled)) => {
                        tracing::trace!(
                            conn_idx,
                            stream_id = logical_stream,
                            bytes = reassembled.len(),
                            "fragment chain reassembled"
                        );
                    }
                    Ok(None) => {
                        tracing::trace!(
                            conn_idx,
                            stream_id = logical_stream,
                            seq = frame.seq.get(),
                            pending_bytes = self.reassembler.pending_bytes(),
                            "fragment accumulating"
                        );
                    }
                    Err(e) => {
                        if self.frag_overflow_throttle.tick() {
                            tracing::warn!(
                                conn_idx,
                                stream_id = logical_stream,
                                total = self.frag_overflow_throttle.total,
                                error = %e,
                                "fragment reassembly error"
                            );
                        }
                    }
                },
                // Ack is now acted on at the application layer
                // (`VireonDispatcher::handle_ack`); the stream layer is
                // agnostic to it. Leaving a trace-only arm keeps the
                // control-dispatch match exhaustive without consuming the
                // payload here.
                MessageType::Ack => {
                    tracing::trace!(
                        conn_idx,
                        stream_id = logical_stream,
                        seq = frame.seq.get(),
                        "ack (handled at application layer)"
                    );
                }
                // Data-plane types are handled above.
                _ => {
                    if self.unexpected_type_throttle.tick() {
                        tracing::warn!(
                            conn_idx,
                            msg_type = ?frame.msg_type,
                            total = self.unexpected_type_throttle.total,
                            "unexpected type in control dispatch"
                        );
                    }
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
        let mut buf = vec![0u8; frame::codec::HEADER_SIZE + payload.len() + frame::codec::CRC_SIZE];
        let _ = encode_into_slice(&mut buf, header, payload);
        let frame = decode(&buf).unwrap();
        sl.on_frame(conn_idx, &frame)
    }

    #[test]
    fn stream_open_close_lifecycle() {
        let mut sl = StreamLayer::new(&LayerTuning::default()).unwrap();
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
        let mut sl = StreamLayer::new(&LayerTuning::default()).unwrap();
        sl.on_connect(0);
        assert_eq!(
            send_frame(&mut sl, 0, MessageType::TextToken, 1),
            FrameAction::DispatchToApp
        );
    }

    #[test]
    fn on_close_releases_all_streams() {
        let mut sl = StreamLayer::new(&LayerTuning::default()).unwrap();
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
