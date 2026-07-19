//! Bridge between quiche stream events and the frame codec.
//!
//! After `conn.recv()`, quiche marks streams as readable. This module
//! reads the stream data, reassembles complete frames (which may span
//! multiple `stream_recv` calls), and pushes decoded frames as
//! [`QuicEvent::FrameReceived`] into the engine's event queue.

use bytes::Bytes;
use rustc_hash::FxHashMap;

use buffer_pool::BufferPool;
use bytes::BytesMut;
use delivery_policy::{Decision, DeliveryPolicyTable, FramePayload, StreamOpenMeta};
use frame::codec;
use frame::header::MessageType;
use frame::header::{Seq, StreamId};

use crate::engine::QuicEvent;

/// Maximum bytes to read per `stream_recv` call.
const STREAM_READ_BUF_SIZE: usize = 65_535;

/// Maximum carryover buffer per stream. Prevents unbounded memory growth
/// from misbehaving peers. Matches the codec payload limit + header slack.
const MAX_CARRYOVER: usize = 16 * 1024 * 1024 + 64; // 16 MiB + header

/// Per-stream reassembly state — holds partial frame data plus warning
/// suppression flags to prevent log spam from misbehaving peers.
///
/// A malicious or buggy peer can flood a stream with thousands of oversized
/// frames in milliseconds. Without per-stream deduplication, each rejected
/// frame emits a `WARN` log — which itself becomes a DoS vector (disk I/O,
/// log aggregation costs). The flags below ensure each stream logs the
/// first occurrence, then stays silent until the stream finishes and the
/// entry is removed from the carryover map.
#[derive(Default)]
pub struct StreamCarry {
    /// Reassembly buffer for frames spanning multiple `stream_recv` calls.
    pub buf: BytesMut,
    /// `true` after the first "carryover exceeds limit" warning for this
    /// stream. Prevents repeated logs on every tick while the peer
    /// continues flooding.
    overflow_warned: bool,
    /// `true` after the first "oversized frame rejected" warning.
    /// Prevents thousands of log lines when a peer sends garbage
    /// headers with multi-gigabyte declared lengths.
    oversized_warned: bool,
}

/// Processes all readable streams on a quiche connection.
///
/// Called by the engine after `conn.recv()` — drains every stream that
/// quiche reports as readable, reassembles frames from the byte stream
/// (handling partial frames that span multiple reads), and pushes
/// successful decodes as `FrameReceived` events.
///
/// `readable_buf`, `pool`, and `carryover` are caller-owned reusable
/// buffers passed in from the engine to avoid per-datagram heap allocation.
///
/// `carryover` is a per-stream reassembly buffer keyed by QUIC stream ID.
/// It holds the tail of a partial frame that didn't fit in the previous
/// read batch, so the rest can be prepended on the next `process_readable`
/// call for that stream.
pub fn process_readable(
    conn: &mut quiche::Connection,
    reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    readable_buf: &mut Vec<u64>,
    pool: &BufferPool,
    carryover: &mut FxHashMap<(usize, u64), StreamCarry>,
    delivery_policies: &mut DeliveryPolicyTable,
    policy_drain_buf: &mut Vec<(StreamId, Seq, FramePayload)>,
    skip_crc: bool,
) {
    // Collect readable stream IDs into the caller-provided buffer to
    // avoid per-datagram Vec allocation.
    readable_buf.clear();
    readable_buf.extend(conn.readable());

    let readable_count = readable_buf.len();
    if readable_count > 0 {
        tracing::trace!(
            reactor_id,
            conn_idx,
            readable_count,
            "readable streams pending"
        );
    }

    // Index-based iteration: `readable_buf[i]` copies the u64 out, so
    // the shared borrow ends before we call `carryover.entry()` which
    // takes &mut FxHashMap (disjoint from the &Vec used for iteration).
    let n = readable_buf.len();
    for i in 0..n {
        let stream_id = readable_buf[i];
        process_single_stream(
            conn,
            stream_id,
            reactor_id,
            conn_idx,
            events,
            pool,
            carryover,
            delivery_policies,
            skip_crc,
        );
    }

    // Release frames that became deliverable due to gap-fill (ReliableOrdered)
    // or that have been held in a pending slot (LatestOnly). This runs once
    // per connection-tick, after all readable streams have been drained.
    policy_drain_buf.clear();
    delivery_policies.drain_ready_for_connection(conn_idx, policy_drain_buf);
    for (stream_id, seq, payload) in policy_drain_buf.drain(..) {
        if events.len() >= crate::engine::MAX_PENDING_EVENTS {
            tracing::warn!(
                conn_idx,
                pending = events.len(),
                "event queue full — dropping drained frame"
            );
            break;
        }
        // The drained `FramePayload` carries the original msg_type/flags
        // AND the quic_stream_id it arrived on, so the upper layer sees
        // the same `FrameReceived` event it would have seen for an
        // immediately-delivered frame.
        events.push(QuicEvent::FrameReceived {
            conn_idx,
            quic_stream_id: payload.quic_stream_id,
            header: codec::FrameHeader {
                stream_id,
                seq,
                msg_type: payload.msg_type,
                flags: payload.flags,
            },
            payload: payload.bytes,
        });
    }

    // Also process any pending DATAGRAM frames.
    process_datagrams(conn, reactor_id, conn_idx);
}

/// Reads all available data from a single stream, reassembles complete
/// frames, and emits `FrameReceived` events.
fn process_single_stream(
    conn: &mut quiche::Connection,
    stream_id: u64,
    reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    pool: &BufferPool,
    carryover: &mut FxHashMap<(usize, u64), StreamCarry>,
    delivery_policies: &mut DeliveryPolicyTable,
    skip_crc: bool,
) {
    let mut read_buf = [0u8; STREAM_READ_BUF_SIZE];
    let mut finished = false;
    let mut total_frames = 0u32;

    // Interleaved read + decode: after each stream_recv, append to
    // carryover and immediately decode complete frames. This avoids
    // accumulating megabytes of data before decoding anything.
    loop {
        match conn.stream_recv(stream_id, &mut read_buf) {
            Ok((0, fin)) => {
                if fin {
                    finished = true;
                }
                break;
            }
            Ok((len, fin)) => {
                if fin {
                    finished = true;
                }

                let cs = carryover.entry((conn_idx, stream_id)).or_default();
                cs.buf.extend_from_slice(&read_buf[..len]);

                // Defense-in-depth: cap carryover to prevent unbounded
                // memory growth if a peer floods a stream with data.
                if cs.buf.len() > MAX_CARRYOVER {
                    if !cs.overflow_warned {
                        cs.overflow_warned = true;
                        tracing::warn!(
                            reactor_id,
                            conn_idx,
                            stream_id,
                            carryover_len = cs.buf.len(),
                            max = MAX_CARRYOVER,
                            "carryover exceeds limit — draining (further occurrences suppressed)"
                        );
                    }
                    cs.buf.clear();
                }

                // Decode complete frames from carryover.
                let mut offset = 0usize;
                while offset + codec::HEADER_SIZE <= cs.buf.len() {
                    // P2: Peek payload_len from the header before calling
                    // the full decode function. This avoids the overhead of
                    // header parse + CRC setup + error-return for the last
                    // incomplete frame in each batch.
                    let remaining = cs.buf.len() - offset;
                    // SAFETY: offset + HEADER_SIZE <= buf.len() verified above.
                    let payload_len = unsafe {
                        let p = cs.buf.as_ptr().add(offset + 18);
                        u32::from_be(core::ptr::read_unaligned(p as *const u32)) as usize
                    };

                    if payload_len > codec::DEFAULT_MAX_PAYLOAD_LEN {
                        if !cs.oversized_warned {
                            cs.oversized_warned = true;
                            tracing::warn!(
                                reactor_id,
                                conn_idx,
                                stream_id,
                                declared_len = payload_len,
                                "oversized frame rejected — draining carryover (further occurrences suppressed)"
                            );
                        }
                        cs.buf.clear();
                        offset = 0;
                        break;
                    }

                    let frame_end = codec::HEADER_SIZE + payload_len + codec::CRC_SIZE;
                    if remaining < frame_end {
                        break; // incomplete frame — wait for more data
                    }

                    // P3: Select CRC-skipping variant when configured.
                    // The branch is constant for the engine's lifetime,
                    // so perfectly predicted by the CPU.
                    let result = if skip_crc {
                        codec::decode_with_len_unchecked(&cs.buf[offset..])
                    } else {
                        codec::decode_with_len(&cs.buf[offset..])
                    };
                    match result {
                        Ok((frame_ref, consumed)) => {
                            dispatch_decoded_frame(
                                &frame_ref,
                                stream_id,
                                reactor_id,
                                conn_idx,
                                events,
                                pool,
                                delivery_policies,
                            );
                            offset += consumed;
                            total_frames += 1;
                        }
                        // P1: On any decode error (CRC mismatch, unknown
                        // type), drain the carryover instead of skipping 1
                        // byte at a time. The old `offset += 1` approach
                        // could call decode_with_len once per byte on
                        // corrupted data — a call-count bomb.
                        Err(e) => {
                            tracing::warn!(
                                reactor_id,
                                conn_idx,
                                stream_id,
                                offset,
                                error = ?e,
                                "decode error — draining carryover to resync"
                            );
                            cs.buf.clear();
                            offset = 0;
                            break;
                        }
                    }
                }
                if offset > 0 {
                    // split_to is O(1) for uniquely-owned buffers — just
                    // advances the internal pointer, no memmove (unlike
                    // Vec::drain which shifts all remaining bytes forward).
                    let _ = cs.buf.split_to(offset);
                }

                if fin {
                    break;
                }
            }
            Err(quiche::Error::Done) => break,
            Err(e) => {
                tracing::warn!(
                    reactor_id, conn_idx, stream_id,
                    error = ?e,
                    "stream_recv error"
                );
                break;
            }
        }
    }

    // Clean up carryover for finished or empty streams.
    // IMPORTANT: keep the entry alive if warning flags are set — removing
    // it would reset `oversized_warned`/`overflow_warned`, causing the next
    // oversized frame to log again (defeating the per-stream suppression).
    let remove = match carryover.get(&(conn_idx, stream_id)) {
        Some(cs) => finished || (cs.buf.is_empty() && !cs.oversized_warned && !cs.overflow_warned),
        None => false,
    };
    if remove {
        carryover.remove(&(conn_idx, stream_id));
    }

    if finished {
        events.push(QuicEvent::StreamFinished {
            conn_idx,
            stream_id,
        });
    }

    if total_frames > 0 {
        let remaining = carryover
            .get(&(conn_idx, stream_id))
            .map_or(0, |cs| cs.buf.len());
        tracing::trace!(
            reactor_id,
            conn_idx,
            stream_id,
            total_frames,
            remaining,
            "decoded frames from stream"
        );
    }
}

/// Processes QUIC DATAGRAM frames (RFC 9221) if the extension is enabled.
fn process_datagrams(conn: &mut quiche::Connection, reactor_id: u32, conn_idx: usize) {
    let mut dgram_buf = [0u8; STREAM_READ_BUF_SIZE];
    loop {
        match conn.dgram_recv(&mut dgram_buf) {
            Ok(len) => {
                tracing::trace!(reactor_id, conn_idx, dgram_len = len, "datagram received");
            }
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }
}

/// Routes a decoded frame to the right handler based on its `MessageType`:
///
/// * **Control plane** (`StreamOpen`, `StreamReset`, `StreamClose`) —
///   mutates the delivery-policy table (register / remove stream state),
///   then emits a `FrameReceived` event so the upper layer sees the
///   control message.
/// * **Data plane** (everything else) — allocates a pool buffer, copies
///   the payload into it, converts to `Bytes`, and applies the policy.
///   The policy decides `Deliver` (emit now), `Buffer` (held for later
///   drain), or `Drop` (discard).
fn dispatch_decoded_frame(
    frame_ref: &codec::FrameRef<'_>,
    quic_stream_id: u64,
    _reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    pool: &BufferPool,
    delivery_policies: &mut DeliveryPolicyTable,
) {
    match frame_ref.msg_type {
        MessageType::StreamOpen => {
            match StreamOpenMeta::decode(frame_ref.payload) {
                Ok(meta) => {
                    if let Err(e) =
                        delivery_policies.bind(conn_idx, frame_ref.stream_id, meta.policy)
                    {
                        tracing::warn!(
                            conn_idx,
                            stream_id = frame_ref.stream_id.get(),
                            policy = ?meta.policy,
                            error = ?e,
                            "delivery policy bind failed"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        conn_idx,
                        stream_id = frame_ref.stream_id.get(),
                        error = ?e,
                        "malformed StreamOpen payload — stream will use default ReliableOrdered"
                    );
                }
            }
            // Still emit the StreamOpen event so the app layer can react.
            emit_control_frame_event(frame_ref, quic_stream_id, conn_idx, events, pool);
        }
        MessageType::StreamReset | MessageType::StreamClose => {
            delivery_policies.remove_stream(conn_idx, frame_ref.stream_id);
            emit_control_frame_event(frame_ref, quic_stream_id, conn_idx, events, pool);
        }
        _ => {
            // Data-plane frame — route through the policy.
            apply_and_emit(
                frame_ref,
                quic_stream_id,
                conn_idx,
                events,
                pool,
                delivery_policies,
            );
        }
    }
}

/// Allocates a pool buffer for the payload, copies it, converts to
/// `Bytes`, and applies the delivery policy. On `Deliver`, emits a
/// `FrameReceived` event; on `Buffer`/`Drop`, the bytes are owned by
/// the policy or dropped.
fn apply_and_emit(
    frame_ref: &codec::FrameRef<'_>,
    quic_stream_id: u64,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    pool: &BufferPool,
    delivery_policies: &mut DeliveryPolicyTable,
) {
    if events.len() >= crate::engine::MAX_PENDING_EVENTS {
        tracing::warn!(
            conn_idx,
            pending = events.len(),
            "event queue full — dropping frame before policy check"
        );
        return;
    }

    let payload_len = frame_ref.payload.len().max(1);
    let mut buf = match pool.try_get(payload_len) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                conn_idx,
                error = ?e,
                payload_len,
                "buffer pool exhausted — dropping frame"
            );
            return;
        }
    };
    let n = frame_ref.payload.len();
    if n > 0 {
        buf[..n].copy_from_slice(frame_ref.payload);
    }
    // Convert the pool buffer into refcounted Bytes — the policy may
    // retain the payload across the call (reorder window, pending slot).
    let bytes: Bytes = buf.into_bytes();

    let payload = delivery_policy::FramePayload::new(
        frame_ref.msg_type,
        frame_ref.flags,
        quic_stream_id,
        bytes,
    );
    let decision = delivery_policies.apply(conn_idx, frame_ref.stream_id, frame_ref.seq, payload);
    match decision {
        Decision::Deliver(p) => {
            events.push(QuicEvent::FrameReceived {
                conn_idx,
                quic_stream_id,
                header: codec::FrameHeader {
                    stream_id: frame_ref.stream_id,
                    seq: frame_ref.seq,
                    msg_type: p.msg_type,
                    flags: p.flags,
                },
                payload: p.bytes,
            });
        }
        Decision::Buffer => {
            // Policy owns the payload; it will resurface via
            // drain_ready_for_connection at the end of the batch.
            // The original `quic_stream_id` travels with the payload
            // and is restored when the drained frame is emitted.
        }
        Decision::Drop => {
            // Payload is released — no event emitted.
        }
    }
}

/// Creates a `FrameReceived` event for a **control-plane** frame
/// (`StreamOpen`, `StreamReset`, `StreamClose`, etc.). These bypass the
/// delivery policy entirely — control frames must always reach the upper
/// layer so the app sees stream lifecycle.
fn emit_control_frame_event(
    frame_ref: &codec::FrameRef<'_>,
    quic_stream_id: u64,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    pool: &BufferPool,
) {
    if events.len() >= crate::engine::MAX_PENDING_EVENTS {
        tracing::warn!(
            conn_idx,
            pending = events.len(),
            "event queue full — dropping control frame"
        );
        return;
    }

    let header = codec::FrameHeader {
        stream_id: frame_ref.stream_id,
        seq: frame_ref.seq,
        msg_type: frame_ref.msg_type,
        flags: frame_ref.flags,
    };
    let payload_len = frame_ref.payload.len().max(1);
    let mut buf = match pool.try_get(payload_len) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                conn_idx,
                error = ?e,
                payload_len,
                "buffer pool exhausted — dropping control frame"
            );
            return;
        }
    };
    let n = frame_ref.payload.len();
    if n > 0 {
        buf[..n].copy_from_slice(frame_ref.payload);
    }
    events.push(QuicEvent::FrameReceived {
        conn_idx,
        quic_stream_id,
        header,
        payload: buf.into_bytes(),
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn placeholder() {
        // process_readable requires a live quiche::Connection which
        // cannot be constructed without a full TLS handshake. The
        // function signature and event-push paths are exercised via
        // integration tests in the quic-server binary.
    }
}
