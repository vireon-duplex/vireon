//! Bridge between quiche stream events and the frame codec.
//!
//! After `conn.recv()`, quiche marks streams as readable. This module
//! reads the stream data, reassembles complete frames (which may span
//! multiple `stream_recv` calls), and pushes decoded frames as
//! [`QuicEvent::FrameReceived`] into the engine's event queue.

use std::collections::HashMap;

use buffer_pool::BufferPool;
use bytes::BytesMut;
use frame::codec;
use frame::error::FrameError;

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
pub(crate) struct StreamCarry {
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
    carryover: &mut HashMap<u64, StreamCarry>,
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
    // takes &mut HashMap (disjoint from the &Vec used for iteration).
    let n = readable_buf.len();
    for i in 0..n {
        let stream_id = readable_buf[i];
        process_single_stream(
            conn, stream_id, reactor_id, conn_idx, events, pool, carryover,
        );
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
    carryover: &mut HashMap<u64, StreamCarry>,
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

                let cs = carryover.entry(stream_id).or_default();
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
                while offset < cs.buf.len() {
                    match codec::decode_with_len(&cs.buf[offset..]) {
                        Ok((frame_ref, consumed)) => {
                            emit_frame_event(
                                &frame_ref, stream_id, reactor_id, conn_idx, events, pool,
                            );
                            offset += consumed;
                            total_frames += 1;
                        }
                        Err(FrameError::IncompleteHeader { .. })
                        | Err(FrameError::IncompletePayload { .. }) => break,
                        Err(FrameError::PayloadTooLarge { len, .. }) => {
                            if !cs.oversized_warned {
                                cs.oversized_warned = true;
                                tracing::warn!(
                                    reactor_id,
                                    conn_idx,
                                    stream_id,
                                    declared_len = len,
                                    "oversized frame rejected — draining carryover (further occurrences suppressed)"
                                );
                            }
                            cs.buf.clear();
                            offset = 0;
                            break;
                        }
                        Err(_) => {
                            offset += 1;
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
    let remove = match carryover.get(&stream_id) {
        Some(cs) => finished || (cs.buf.is_empty() && !cs.oversized_warned && !cs.overflow_warned),
        None => false,
    };
    if remove {
        carryover.remove(&stream_id);
    }

    if finished {
        events.push(QuicEvent::StreamFinished {
            conn_idx,
            stream_id,
        });
    }

    if total_frames > 0 {
        let remaining = carryover.get(&stream_id).map_or(0, |cs| cs.buf.len());
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

/// Creates a `FrameReceived` event from a decoded frame reference and
/// pushes it into the engine's event queue. Payload bytes are copied
/// into a pool buffer that is recycled when the event is consumed.
fn emit_frame_event(
    frame_ref: &codec::FrameRef<'_>,
    stream_id: u64,
    _reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    pool: &BufferPool,
) {
    // Backpressure: stop pushing when the upper layer hasn't drained.
    if events.len() >= crate::engine::MAX_PENDING_EVENTS {
        tracing::warn!(
            conn_idx,
            pending = events.len(),
            "event queue full — dropping frame"
        );
        return;
    }

    let header = codec::FrameHeader {
        stream_id: frame_ref.stream_id,
        seq: frame_ref.seq,
        msg_type: frame_ref.msg_type,
        flags: frame_ref.flags,
    };
    let payload_len = frame_ref.payload.len();

    if payload_len == 0 {
        match pool.try_get(1) {
            Ok(buf) => events.push(QuicEvent::FrameReceived {
                conn_idx,
                quic_stream_id: stream_id,
                header,
                payload: buf,
            }),
            Err(e) => {
                tracing::warn!(
                    conn_idx, error = ?e,
                    "buffer pool exhausted — dropping zero-len frame"
                );
            }
        }
    } else {
        match pool.try_get(payload_len) {
            Ok(mut buf) => {
                buf[..payload_len].copy_from_slice(frame_ref.payload);
                events.push(QuicEvent::FrameReceived {
                    conn_idx,
                    quic_stream_id: stream_id,
                    header,
                    payload: buf,
                });
            }
            Err(e) => {
                tracing::warn!(
                    conn_idx, error = ?e, payload_len,
                    "buffer pool exhausted — dropping frame"
                );
            }
        }
    }
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
