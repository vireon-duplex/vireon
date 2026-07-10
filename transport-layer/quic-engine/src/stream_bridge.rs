//! Bridge between quiche stream events and the frame codec.
//!
//! After `conn.recv()`, quiche marks streams as readable. This module
//! reads the stream data, attempts to decode it as a frame using the
//! 22-byte codec-layer wire format, and pushes decoded frames as
//! [`QuicEvent::FrameReceived`] into the engine's event queue.

use buffer_pool::BufferPool;
use frame::codec;

use crate::engine::QuicEvent;

/// Maximum bytes to read per `stream_recv` call.
const STREAM_READ_BUF_SIZE: usize = 65_535;

/// Processes all readable streams on a quiche connection.
///
/// Called by the engine after `conn.recv()` — drains every stream that
/// quiche reports as readable, attempts frame decode on each chunk, and
/// pushes successful decodes as `FrameReceived` events. Non-frame data
/// (e.g. HTTP/3, TLS handshake) is logged at debug level.
///
/// `readable_buf` and `pool` are caller-owned reusable buffers passed
/// in from the engine to avoid per-datagram heap allocation.
pub fn process_readable(
    conn: &mut quiche::Connection,
    reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    readable_buf: &mut Vec<u64>,
    pool: &BufferPool,
) {
    // Collect readable stream IDs into the caller-provided buffer to
    // avoid per-datagram Vec allocation.
    readable_buf.clear();
    readable_buf.extend(conn.readable());

    // `readable_buf.iter()` borrows the Vec immutably for the loop, but
    // `conn` is a separate parameter — no aliasing conflict with
    // `conn.stream_recv`.
    for &stream_id in readable_buf.iter() {
        let mut buf = [0u8; STREAM_READ_BUF_SIZE];
        loop {
            match conn.stream_recv(stream_id, &mut buf) {
                Ok((len, fin)) => {
                    if len > 0 {
                        decode_stream_data(
                            &buf[..len],
                            stream_id,
                            reactor_id,
                            conn_idx,
                            events,
                            pool,
                        );
                    }
                    if fin {
                        events.push(QuicEvent::StreamFinished {
                            conn_idx,
                            stream_id,
                        });
                        break;
                    }
                    if len == 0 {
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
    }

    // Also process any pending DATAGRAM frames.
    process_datagrams(conn, reactor_id, conn_idx);
}

/// Processes QUIC DATAGRAM frames (RFC 9221) if the extension is enabled.
fn process_datagrams(conn: &mut quiche::Connection, reactor_id: u32, conn_idx: usize) {
    let mut dgram_buf = [0u8; STREAM_READ_BUF_SIZE];
    loop {
        match conn.dgram_recv(&mut dgram_buf) {
            Ok(len) => {
                tracing::debug!(reactor_id, conn_idx, dgram_len = len, "datagram received");
            }
            Err(quiche::Error::Done) => break,
            Err(_) => break,
        }
    }
}

/// Attempts to decode stream data as a codec frame and pushes a
/// `FrameReceived` event on success. Payload bytes are copied into a
/// pool buffer that is recycled when the event is consumed.
fn decode_stream_data(
    data: &[u8],
    stream_id: u64,
    reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
    pool: &BufferPool,
) {
    match codec::decode(data) {
        Ok(frame_ref) => {
            let header = codec::FrameHeader {
                stream_id: frame_ref.stream_id,
                seq: frame_ref.seq,
                msg_type: frame_ref.msg_type,
                flags: frame_ref.flags,
            };
            let payload_len = frame_ref.payload.len();

            tracing::debug!(
                reactor_id, conn_idx, stream_id,
                msg_type = ?header.msg_type,
                seq = ?header.seq,
                payload_len,
                "decoded frame"
            );

            if payload_len == 0 {
                // Acquire minimum-class buffer for uniform event shape.
                match pool.try_get(1) {
                    Ok(buf) => events.push(QuicEvent::FrameReceived {
                        conn_idx,
                        header,
                        payload: buf,
                    }),
                    Err(e) => {
                        tracing::warn!(
                            reactor_id, conn_idx, error = ?e,
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
                            header,
                            payload: buf,
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            reactor_id, conn_idx, error = ?e, payload_len,
                            "buffer pool exhausted — dropping frame"
                        );
                    }
                }
            }
        }
        Err(_) => {
            // Non-frame data — could be HTTP/3 framing, TLS, or raw bytes.
            tracing::debug!(
                reactor_id,
                conn_idx,
                stream_id,
                len = data.len(),
                "non-frame stream data"
            );
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
