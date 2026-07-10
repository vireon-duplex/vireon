//! Bridge between quiche stream events and the frame codec.
//!
//! After `conn.recv()`, quiche marks streams as readable. This module
//! reads the stream data, attempts to decode it as a frame using the
//! 22-byte codec-layer wire format, and pushes decoded frames as
//! [`QuicEvent::FrameReceived`] into the engine's event queue.

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
pub fn process_readable(
    conn: &mut quiche::Connection,
    reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
) {
    // Collect readable stream IDs first to avoid holding &conn across
    // the mutable stream_recv calls.
    let streams: Vec<u64> = conn.readable().collect();

    for stream_id in streams {
        let mut buf = [0u8; STREAM_READ_BUF_SIZE];
        loop {
            match conn.stream_recv(stream_id, &mut buf) {
                Ok((len, fin)) => {
                    if len > 0 {
                        decode_stream_data(&buf[..len], stream_id, reactor_id, conn_idx, events);
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
/// `FrameReceived` event on success.
fn decode_stream_data(
    data: &[u8],
    stream_id: u64,
    reactor_id: u32,
    conn_idx: usize,
    events: &mut Vec<QuicEvent>,
) {
    match codec::decode(data) {
        Ok(frame_ref) => {
            let frame = frame_ref.to_owned();
            tracing::debug!(
                reactor_id, conn_idx, stream_id,
                msg_type = ?frame.msg_type,
                seq = ?frame.seq,
                payload_len = frame.payload.len(),
                "decoded frame"
            );
            events.push(QuicEvent::FrameReceived { conn_idx, frame });
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
