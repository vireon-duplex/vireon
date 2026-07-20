//! [`ClientStack`] — the client-side composition root.
//!
//! Wraps the application/session/stream/message layer chain and provides
//! the encode/coalesce/decode machinery that `worker.rs` drives.
//!
//! ```text
//!                     ClientStack
//!                         │
//!     ┌───────────────────┼───────────────────┐
//!     │                                       │
//!     ▼                                       ▼
//!  ClientAppLayer                       FrameDecoders
//!  (owns Session →                     (one per QUIC
//!   Stream → Message)                   stream, lazy)
//! ```
//!
//! SEND PATH:
//!   stack.encode_frame_coalesced_into(dst, stream, msg_type, flags, payload)
//!     → app.prepare_frame()          [builds header + increments seq]
//!       → session → stream           [layer chain]
//!     → encode_into_slice() × N      [write into caller-owned `dst`]
//!     → caller does stream_send(&dst)
//!
//! RECV PATH:
//!   stack.on_stream_data(quic_sid, &buf[..n])
//!     → FrameDecoder.decode_frame()  [parse 22-byte header + payload]
//!     → app.on_frame(&FrameRef)      [session → stream → message → count]
//!     → returns frame count

use std::collections::HashMap;

use frame::codec::{FrameDecoder, FrameRef, encode_into_slice};
use frame::header::{FrameFlags, MessageType};
use session_manager::SessionId;

use super::application::ClientApplicationLayer;

/// Pre-allocation size for each per-stream `FrameDecoder`.
const DECODER_CAPACITY: usize = 65_535;

/// The client-side layer stack — owns the full application chain
/// plus reusable decode buffers.
pub struct ClientStack {
    /// Application layer (owns session → stream → message).
    app: ClientApplicationLayer,
    /// One incremental decoder per QUIC stream (created lazily).
    decoders: HashMap<u64, FrameDecoder>,
}

impl ClientStack {
    /// Creates a new stack.
    ///
    /// The `coalesce_capacity` argument is retained for ABI stability
    /// with callers in `worker.rs` but no longer used — the encode
    /// path writes into a caller-owned buffer (`encode_frame_coalesced_into`).
    #[must_use]
    pub fn new(_coalesce_capacity: usize) -> Self {
        Self {
            app: ClientApplicationLayer::new(),
            decoders: HashMap::new(),
        }
    }

    /// Wires up the session/stream layers after handshake completes.
    ///
    /// Returns `None` if the session table is full.
    pub fn on_connect(&mut self) -> Option<SessionId> {
        self.app.on_connect()
    }

    // ── Send path ──────────────────────────────────────────────────

    /// Encodes `coalesce` copies of a single frame into `dst` (caller-owned).
    ///
    /// Each copy gets a unique incrementing `seq` from the stream layer.
    /// The caller feeds `dst` to `conn.stream_send`. Using a caller-owned
    /// buffer releases the borrow on `self` immediately — no `.to_vec()`
    /// needed in the caller, and no `memset` on the buffer since
    /// `encode_into_slice` writes every byte.
    pub fn encode_frame_coalesced_into(
        &mut self,
        dst: &mut Vec<u8>,
        logical_stream: u64,
        msg_type: MessageType,
        flags: FrameFlags,
        payload: &[u8],
        coalesce: usize,
    ) {
        let frame_wire_size = frame::codec::HEADER_SIZE + payload.len() + frame::codec::CRC_SIZE;
        let total = frame_wire_size * coalesce;
        dst.clear();
        // reserve ensures capacity >= len + additional. After clear,
        // len is 0, so reserve(total) guarantees capacity >= total.
        if dst.capacity() < total {
            dst.reserve(total);
        }
        // SAFETY: `encode_into_slice` writes every byte of each frame
        // segment, so the buffer is fully initialised after the loop.
        // We use set_len (not resize(n, 0)) to skip the zero-fill.
        unsafe { dst.set_len(total) }

        for i in 0..coalesce {
            let header = self.app.prepare_frame(logical_stream, msg_type, flags);
            let off = i * frame_wire_size;
            let _ = encode_into_slice(&mut dst[off..off + frame_wire_size], header, payload);
        }
    }

    // ── Receive path ───────────────────────────────────────────────

    /// Feeds raw bytes from `stream_recv` into the decoder for `quic_sid`,
    /// then drains all complete frames through the layer stack.
    ///
    /// Returns the number of frames decoded.
    pub fn on_stream_data(&mut self, quic_sid: u64, data: &[u8]) -> usize {
        let decoder = self
            .decoders
            .entry(quic_sid)
            .or_insert_with(|| FrameDecoder::with_capacity(DECODER_CAPACITY));
        decoder.push(data);

        let mut count = 0usize;
        loop {
            match decoder.decode_frame() {
                Ok(Some(frame)) => {
                    // Construct a zero-copy FrameRef borrowing from `frame.payload`.
                    // `frame` lives on the stack for the duration of `on_frame`.
                    let frame_ref = FrameRef {
                        stream_id: frame.stream_id,
                        seq: frame.seq,
                        msg_type: frame.msg_type,
                        flags: frame.flags,
                        payload: &frame.payload[..],
                    };
                    self.app.on_frame(&frame_ref);
                    count += 1;
                }
                Ok(None) => break,
                Err(_) => {
                    decoder.clear();
                    break;
                }
            }
        }
        count
    }

    // ── Lifecycle ──────────────────────────────────────────────────

    /// Tears down all layers.
    ///
    /// Returns the number of streams released.
    pub fn on_close(&mut self) -> usize {
        self.app.on_close()
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the total frames sent through the layer stack.
    #[must_use]
    pub fn frames_tx(&self) -> u64 {
        self.app.frames_tx()
    }

    /// Returns the total data frames received through the layer stack.
    #[must_use]
    pub fn frames_rx(&self) -> u64 {
        self.app.frames_rx()
    }

    /// Returns the total payload bytes received.
    #[must_use]
    pub fn bytes_rx(&self) -> u64 {
        self.app.bytes_rx()
    }
}
