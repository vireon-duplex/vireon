//! [`ClientStack`] — the client-side composition root.
//!
//! Wraps the application/session/stream/message layer chain and provides
//! the encode/coalesce/decode machinery that `worker.rs` drives. As of
//! the client-side egress policy wiring, every encoded frame flows
//! through a [`SendPolicyTable`] before reaching `dst` — for the default
//! `ReliableOrdered` policy this is pass-through, but a bench run with
//! `--egress-policy latest_only` (or similar) sees older copies
//! replaced/dropped here instead of at the server.
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
//!     │
//!     ▼
//!  SendPolicyTable
//!  (conn_idx=0 — single
//!   connection client)
//! ```
//!
//! SEND PATH:
//!   stack.encode_frame_coalesced_into(dst, stream, msg_type, flags, payload, n)
//!     → app.prepare_frame() × n         [build header + increment seq]
//!     → send_policy.enqueue() × n       [policy may drop / replace]
//!     → send_policy.dequeue() ready set [drains in policy order]
//!     → encode_into_slice() per frame   [write into caller-owned `dst`]
//!     → caller does stream_send(&dst)
//!
//! RECV PATH:
//!   stack.on_stream_data(quic_sid, &buf[..n])
//!     → FrameDecoder.decode_frame()  [parse 22-byte header + payload]
//!     → app.on_frame(&FrameRef)      [session → stream → message → count]
//!     → returns frame count

use std::collections::HashMap;

use bytes::Bytes;
use frame::codec::{FrameDecoder, FrameHeader, FrameRef, encode_into_slice};
use frame::header::{FrameFlags, MessageType, StreamId};
use send_policy::{DeliveryPolicy, DequeueOutcome, FramePayload, SendPolicyTable};
use session_manager::SessionId;

use super::application::ClientApplicationLayer;

/// Pre-allocation size for each per-stream `FrameDecoder`.
const DECODER_CAPACITY: usize = 65_535;

/// The single `conn_idx` the bench client uses. The server-side
/// `SendPolicyTable` is keyed by `(conn_idx, StreamId)` because a server
/// has many connections; a bench client has exactly one, so we pin to 0.
const CLIENT_CONN_IDX: usize = 0;

/// The client-side layer stack — owns the full application chain
/// plus reusable decode buffers and the send-policy table.
pub struct ClientStack {
    /// Application layer (owns session → stream → message).
    app: ClientApplicationLayer,
    /// One incremental decoder per QUIC stream (created lazily).
    decoders: HashMap<u64, FrameDecoder>,
    /// Per-stream send-side delivery policy. Lazy-defaults to
    /// `ReliableOrdered` for any stream that is enqueued before
    /// [`Self::bind_send_policy`] is called, so the default bench
    /// workload (which never binds) is fully pass-through.
    send_policy: SendPolicyTable,
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
            send_policy: SendPolicyTable::new(),
        }
    }

    /// Wires up the session/stream layers after handshake completes.
    ///
    /// Returns `None` if the session table is full.
    pub fn on_connect(&mut self) -> Option<SessionId> {
        self.app.on_connect()
    }

    /// Binds a send-side delivery policy for `logical_stream`.
    ///
    /// After this call, every frame enqueued via `encode_frame_coalesced_into`
    /// for this stream is routed through the policy. Streams that are
    /// never bound lazy-default to `ReliableOrdered` (the only policy
    /// that never drops) so the default bench workload is unaffected.
    ///
    /// Any frames queued by prior enqueues are flushed out by the bind
    /// (returned from [`SendPolicyTable::bind`]) and immediately
    /// re-enqueued under the new policy — they are not lost.
    pub fn bind_send_policy(&mut self, logical_stream: u64, policy: DeliveryPolicy) {
        let stream_id = StreamId::new(logical_stream);
        if let Ok(flushed) = self.send_policy.bind(CLIENT_CONN_IDX, stream_id, policy) {
            // Re-enqueue any frames the bind displaced so they are not
            // silently lost — they'll emerge from the next drain pass.
            for fp in flushed {
                let _ = self.send_policy.enqueue(CLIENT_CONN_IDX, stream_id, fp);
            }
        }
    }

    // ── Send path ──────────────────────────────────────────────────

    /// Encodes `coalesce` copies of a single frame into `dst` (caller-owned),
    /// routing each through the per-stream send policy first.
    ///
    /// Each copy gets a unique incrementing `seq` from the stream layer.
    /// The send-policy table may drop or replace copies according to the
    /// stream's bound policy (defaults to `ReliableOrdered` = pass-through
    /// when no policy is bound). The caller feeds `dst` to `conn.stream_send`.
    ///
    /// Note: for `LatestOnly`, only the newest of the `coalesce` copies
    /// will emerge in `dst` — older copies are dropped inside the policy.
    /// For `ReliableOrdered` / `ReliableUnordered` / fresh `RealtimeDropOld`,
    /// all `coalesce` copies emerge.
    pub fn encode_frame_coalesced_into(
        &mut self,
        dst: &mut Vec<u8>,
        logical_stream: u64,
        msg_type: MessageType,
        flags: FrameFlags,
        payload: &[u8],
        coalesce: usize,
    ) {
        // 1. Enqueue each coalesced copy through the policy table.
        //    `prepare_frame` increments the per-stream seq counter, so
        //    each copy carries a unique Seq — required by `RealtimeDropOld`
        //    and `LatestOnly` to identify the newest.
        let stream_id = StreamId::new(logical_stream);
        let payload_bytes = Bytes::copy_from_slice(payload);
        for _ in 0..coalesce {
            let header = self.app.prepare_frame(logical_stream, msg_type, flags);
            let fp = FramePayload::new(msg_type, flags, header.seq, payload_bytes.clone());
            let _ = self.send_policy.enqueue(CLIENT_CONN_IDX, stream_id, fp);
        }

        // 2. Drain ready frames into dst. We snapshot the ready set
        //    first so we don't hold a borrow over `dequeue`.
        dst.clear();
        let mut ready: Vec<(usize, StreamId)> = Vec::new();
        self.send_policy.ready_streams_all(&mut ready);

        for (conn_idx, sid) in ready {
            // Only drain streams we own (single-connection client).
            if conn_idx != CLIENT_CONN_IDX {
                continue;
            }
            while let DequeueOutcome::Some(fp) = self.send_policy.dequeue(conn_idx, sid) {
                self.encode_one_into(dst, sid, fp);
            }
        }
    }

    /// Encodes a single `FramePayload` (post-policy) into `dst`.
    /// Grows `dst` to fit; `encode_into_slice` writes every byte.
    #[inline]
    fn encode_one_into(&mut self, dst: &mut Vec<u8>, sid: StreamId, fp: FramePayload) {
        let header = FrameHeader::new(sid, fp.msg_type)
            .with_seq(fp.seq)
            .with_flags(fp.flags);
        let needed = frame::codec::HEADER_SIZE + fp.bytes.len() + frame::codec::CRC_SIZE;
        let off = dst.len();
        dst.resize(off + needed, 0);
        // SAFETY: encode_into_slice writes every byte of the frame
        // segment, so the resize() above is fully overwritten.
        let _ = encode_into_slice(&mut dst[off..off + needed], header, &fp.bytes);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stack() -> ClientStack {
        ClientStack::new(0)
    }

    #[test]
    fn default_reliable_ordered_passes_all_frames_through_policy() {
        // No bind — lazy-defaults to ReliableOrdered.
        let mut stack = make_stack();
        let mut dst = Vec::new();
        let payload = b"hello";

        stack.encode_frame_coalesced_into(
            &mut dst,
            0,
            MessageType::TextToken,
            FrameFlags::NONE,
            payload,
            10,
        );

        // Each TextToken frame: 22-byte header + 5-byte payload + 4-byte CRC = 31 bytes.
        let frame_wire = frame::codec::HEADER_SIZE + payload.len() + frame::codec::CRC_SIZE;
        assert_eq!(dst.len(), 10 * frame_wire);
        // All 10 copies accounted for at the app layer too.
        assert_eq!(stack.frames_tx(), 10);
    }

    #[test]
    fn latest_only_drops_all_but_newest_within_one_batch() {
        let mut stack = make_stack();
        stack.bind_send_policy(0, DeliveryPolicy::LatestOnly);

        let mut dst = Vec::new();
        let payload = b"hello";
        stack.encode_frame_coalesced_into(
            &mut dst,
            0,
            MessageType::TextToken,
            FrameFlags::NONE,
            payload,
            10,
        );

        let frame_wire = frame::codec::HEADER_SIZE + payload.len() + frame::codec::CRC_SIZE;
        // LatestOnly replaces older pending frames in-place, so at most
        // one frame emerges per drain pass.
        assert_eq!(dst.len(), frame_wire);
        assert_eq!(stack.frames_tx(), 10);
    }

    #[test]
    fn realtime_drop_old_keeps_newest_seq_in_queue() {
        let mut stack = make_stack();
        stack.bind_send_policy(0, DeliveryPolicy::RealtimeDropOld);

        let mut dst = Vec::new();
        let payload = b"abc";
        stack.encode_frame_coalesced_into(
            &mut dst,
            0,
            MessageType::AudioFrame,
            FrameFlags::NONE,
            payload,
            5,
        );

        let frame_wire = frame::codec::HEADER_SIZE + payload.len() + frame::codec::CRC_SIZE;
        // RealtimeDropOld appends frames whose seq is newer than the
        // currently queued newest. Within one batch each prepare_frame
        // monotonically advances seq, so all 5 are queued and drained.
        assert_eq!(dst.len(), 5 * frame_wire);
    }

    #[test]
    fn bind_then_rebind_flushes_displaced_frames_into_next_drain() {
        let mut stack = make_stack();
        stack.bind_send_policy(0, DeliveryPolicy::LatestOnly);

        // First batch: 1 frame queued (latest), drain emits 1.
        let mut dst1 = Vec::new();
        stack.encode_frame_coalesced_into(
            &mut dst1,
            0,
            MessageType::TextToken,
            FrameFlags::NONE,
            b"first",
            1,
        );
        assert!(!dst1.is_empty());

        // Rebind to ReliableOrdered — any pending frames re-enqueue.
        stack.bind_send_policy(0, DeliveryPolicy::ReliableOrdered);
        let mut dst2 = Vec::new();
        let second_payload = b"second"; // 6 bytes
        stack.encode_frame_coalesced_into(
            &mut dst2,
            0,
            MessageType::TextToken,
            FrameFlags::NONE,
            second_payload,
            3,
        );
        // Under ReliableOrdered every enqueued copy drains. Payload is
        // 6 bytes, so frame wire size = HEADER + 6 + CRC.
        let frame_wire = frame::codec::HEADER_SIZE + second_payload.len() + frame::codec::CRC_SIZE;
        assert_eq!(dst2.len(), 3 * frame_wire);
    }
}
