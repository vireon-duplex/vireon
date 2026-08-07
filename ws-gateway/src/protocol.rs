//! Binary wire protocol between the browser SDK and this gateway.
//!
//! All WebSocket frames are binary (opcode 2). The first byte is the op tag;
//! all multi-byte integers are little-endian (matches `DataView` defaults in
//! JS). Strings are UTF-8.
//!
//! Every C→S op (except `try_publish`, `stream_try_publish`, `close`) carries
//! a `req_id: u32` immediately after the tag so the gateway can echo it back
//! in the matching `ack` / `error` frame. This keeps correlation unambiguous
//! when the browser has multiple publishes in flight.
//!
//! See `bindings/browser/src/protocol.ts` for the TypeScript mirror — the two
//! MUST stay in sync byte-for-byte.

#![allow(clippy::expect_used)] // validation paths use TryFrom inputs

use bytes::Bytes;

// ── op tag constants ─────────────────────────────────────────────────

/// C→S ops.
pub mod c2s {
    pub const CONNECT: u8 = 0x01;
    pub const SUBSCRIBE: u8 = 0x02;
    pub const UNSUBSCRIBE: u8 = 0x03;
    pub const OPEN_STREAM: u8 = 0x04;
    pub const PUBLISH: u8 = 0x05;
    pub const TRY_PUBLISH: u8 = 0x0A;
    pub const CLOSE: u8 = 0x0B;
    pub const GROUP_JOIN: u8 = 0x0C;
    pub const GROUP_LEAVE: u8 = 0x0D;
    pub const STREAM_PUBLISH: u8 = 0x0E;
    pub const STREAM_TRY_PUBLISH: u8 = 0x0F;
}

/// S→C ops.
pub mod s2c {
    pub const MESSAGE: u8 = 0x06;
    pub const STREAM_MESSAGE: u8 = 0x07;
    pub const ACK: u8 = 0x08;
    pub const ERROR: u8 = 0x09;
}

/// DeliveryPolicy byte values (matches `stream-layer/delivery-policy/src/policy.rs`).
pub mod policy {
    pub const RELIABLE_ORDERED: u8 = 0x01;
    pub const RELIABLE_UNORDERED: u8 = 0x02;
    pub const REALTIME_DROP_OLD: u8 = 0x03;
    pub const LATEST_ONLY: u8 = 0x04;
}

/// Decode a `DeliveryPolicy` byte into the SDK enum. Returns `None` for
/// unknown values so the gateway can NACK with a clear error instead of
/// defaulting silently.
#[inline]
#[must_use]
pub fn decode_policy(byte: u8) -> Option<vireon_sdk::DeliveryPolicy> {
    use vireon_sdk::DeliveryPolicy;
    match byte {
        policy::RELIABLE_ORDERED => Some(DeliveryPolicy::ReliableOrdered),
        policy::RELIABLE_UNORDERED => Some(DeliveryPolicy::ReliableUnordered),
        policy::REALTIME_DROP_OLD => Some(DeliveryPolicy::RealtimeDropOld),
        policy::LATEST_ONLY => Some(DeliveryPolicy::LatestOnly),
        _ => None,
    }
}

// ── decode helpers (C→S frame parsing) ───────────────────────────────

/// A decoded C→S frame. Variants without `req_id` are the fire-and-forget
/// ops (`try_publish`, `stream_try_publish`, `close`).
#[derive(Debug)]
pub enum ClientOp {
    Connect { req_id: u32 },
    Subscribe { req_id: u32, sub_id: u32, topic: String },
    Unsubscribe { req_id: u32, sub_id: u32, topic: String },
    OpenStream {
        req_id: u32,
        stream_handle_id: u32,
        policy: u8,
        topic: Option<String>,
    },
    Publish { req_id: u32, topic: String, payload: Bytes },
    TryPublish { topic: String, payload: Bytes },
    Close,
    GroupJoin {
        req_id: u32,
        sub_id: u32,
        topic: String,
        group: String,
        consumer: String,
    },
    GroupLeave {
        req_id: u32,
        topic: String,
        group: String,
        consumer: String,
    },
    StreamPublish {
        req_id: u32,
        stream_handle_id: u32,
        topic: String,
        payload: Bytes,
    },
    StreamTryPublish {
        stream_handle_id: u32,
        topic: String,
        payload: Bytes,
    },
}

/// Parse error surfaced to the caller. `Short` means the buffer was smaller
/// than the layout requires; `BadTag` means the leading byte was not a known
/// op; `BadPolicy` means the policy byte did not decode.
#[derive(Debug)]
pub enum DecodeError {
    Short,
    BadTag(u8),
    BadPolicy(u8),
}

/// Decode a single C→S frame from a complete WS message. The caller is
/// responsible for ensuring the whole frame is in `buf` — the WebSocket
/// message boundary does that for us (each binary WS message is one op).
///
/// # Errors
/// Returns [`DecodeError::Short`] if `buf` is too short for the op layout,
/// [`DecodeError::BadTag`] for unknown op tags, or [`DecodeError::BadPolicy`]
/// for unknown policy bytes on `open_stream`.
pub fn decode_client_op(buf: &[u8]) -> Result<ClientOp, DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::Short);
    }
    let tag = buf[0];
    let rest = &buf[1..];
    match tag {
        c2s::CONNECT => {
            let req_id = read_u32(rest, 0)?;
            Ok(ClientOp::Connect { req_id })
        }
        c2s::SUBSCRIBE => {
            let req_id = read_u32(rest, 0)?;
            let sub_id = read_u32(rest, 4)?;
            let (topic, _) = read_string(rest, 8)?;
            Ok(ClientOp::Subscribe {
                req_id,
                sub_id,
                topic,
            })
        }
        c2s::UNSUBSCRIBE => {
            let req_id = read_u32(rest, 0)?;
            let sub_id = read_u32(rest, 4)?;
            let (topic, _) = read_string(rest, 8)?;
            Ok(ClientOp::Unsubscribe {
                req_id,
                sub_id,
                topic,
            })
        }
        c2s::OPEN_STREAM => {
            let req_id = read_u32(rest, 0)?;
            let stream_handle_id = read_u32(rest, 4)?;
            let policy = read_u8(rest, 8)?;
            // Validate policy byte here so the gateway can NACK immediately
            // with a clear error.
            if decode_policy(policy).is_none() {
                return Err(DecodeError::BadPolicy(policy));
            }
            let (topic_str, consumed) = read_string(rest, 9)?;
            let topic = if topic_str.is_empty() {
                None
            } else {
                Some(topic_str)
            };
            // `consumed` is the byte count past the tag of the parsed
            // (req_id + handle + policy + topic) fields. We don't use it here
            // because there is no payload to consume afterwards.
            let _ = consumed;
            Ok(ClientOp::OpenStream {
                req_id,
                stream_handle_id,
                policy,
                topic,
            })
        }
        c2s::PUBLISH => {
            let req_id = read_u32(rest, 0)?;
            let (topic, n) = read_string(rest, 4)?;
            let payload = Bytes::copy_from_slice(&rest[n..]);
            Ok(ClientOp::Publish {
                req_id,
                topic,
                payload,
            })
        }
        c2s::TRY_PUBLISH => {
            // No req_id — fire-and-forget. The browser never expects an ack.
            let (topic, n) = read_string(rest, 0)?;
            let payload = Bytes::copy_from_slice(&rest[n..]);
            Ok(ClientOp::TryPublish { topic, payload })
        }
        c2s::CLOSE => Ok(ClientOp::Close),
        c2s::GROUP_JOIN => {
            let req_id = read_u32(rest, 0)?;
            let sub_id = read_u32(rest, 4)?;
            let mut pos = 8;
            let (topic, n) = read_string(rest, pos)?;
            pos = n;
            let (group, n) = read_string(rest, pos)?;
            pos = n;
            let (consumer, _) = read_string(rest, pos)?;
            Ok(ClientOp::GroupJoin {
                req_id,
                sub_id,
                topic,
                group,
                consumer,
            })
        }
        c2s::GROUP_LEAVE => {
            let req_id = read_u32(rest, 0)?;
            let mut pos = 4;
            let (topic, n) = read_string(rest, pos)?;
            pos = n;
            let (group, n) = read_string(rest, pos)?;
            pos = n;
            let (consumer, _) = read_string(rest, pos)?;
            Ok(ClientOp::GroupLeave {
                req_id,
                topic,
                group,
                consumer,
            })
        }
        c2s::STREAM_PUBLISH => {
            let req_id = read_u32(rest, 0)?;
            let stream_handle_id = read_u32(rest, 4)?;
            let (topic, n) = read_string(rest, 8)?;
            let payload = Bytes::copy_from_slice(&rest[n..]);
            Ok(ClientOp::StreamPublish {
                req_id,
                stream_handle_id,
                topic,
                payload,
            })
        }
        c2s::STREAM_TRY_PUBLISH => {
            let stream_handle_id = read_u32(rest, 0)?;
            let (topic, n) = read_string(rest, 4)?;
            let payload = Bytes::copy_from_slice(&rest[n..]);
            Ok(ClientOp::StreamTryPublish {
                stream_handle_id,
                topic,
                payload,
            })
        }
        other => Err(DecodeError::BadTag(other)),
    }
}

#[inline]
fn read_u8(buf: &[u8], pos: usize) -> Result<u8, DecodeError> {
    buf.get(pos).copied().ok_or(DecodeError::Short)
}

#[inline]
fn read_u32(buf: &[u8], pos: usize) -> Result<u32, DecodeError> {
    let slice = buf.get(pos..pos + 4).ok_or(DecodeError::Short)?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

/// Read a `u16` length prefix followed by that many UTF-8 bytes.
/// Returns the parsed string and the total byte offset (relative to the
/// start of `buf`) of the position just past the string (so the caller can
/// continue parsing trailing fields like `payload`).
#[inline]
fn read_string(buf: &[u8], pos: usize) -> Result<(String, usize), DecodeError> {
    let len_slice = buf.get(pos..pos + 2).ok_or(DecodeError::Short)?;
    let len = u16::from_le_bytes([len_slice[0], len_slice[1]]) as usize;
    let start = pos + 2;
    let end = start + len;
    let bytes = buf.get(start..end).ok_or(DecodeError::Short)?;
    let s = std::str::from_utf8(bytes).map_err(|_| DecodeError::Short)?;
    Ok((s.to_string(), end))
}

// ── encode helpers (S→C frame construction) ──────────────────────────

/// Encode a `message` S→C frame (op 0x06) for a default-channel subscription
/// delivery. `sub_id` correlates to the browser's `Subscription` object.
pub fn encode_message(sub_id: u32, msg: &vireon_sdk::Message) -> Vec<u8> {
    let topic_len = u16::try_from(msg.topic.len()).unwrap_or(u16::MAX);
    let mut out =
        Vec::with_capacity(1 + 4 + 8 + 8 + 2 + msg.topic.len() + msg.payload.len());
    out.push(s2c::MESSAGE);
    out.extend_from_slice(&sub_id.to_le_bytes());
    out.extend_from_slice(&msg.seq.to_le_bytes());
    out.extend_from_slice(&msg.stream_id.to_le_bytes());
    out.extend_from_slice(&topic_len.to_le_bytes());
    out.extend_from_slice(&msg.topic);
    out.extend_from_slice(&msg.payload);
    out
}

/// Encode a `stream_message` S→C frame (op 0x07) for a dedicated-stream
/// delivery. `stream_handle_id` correlates to the browser's `StreamHandle`.
pub fn encode_stream_message(stream_handle_id: u32, msg: &vireon_sdk::Message) -> Vec<u8> {
    let topic_len = u16::try_from(msg.topic.len()).unwrap_or(u16::MAX);
    let mut out =
        Vec::with_capacity(1 + 4 + 8 + 8 + 2 + msg.topic.len() + msg.payload.len());
    out.push(s2c::STREAM_MESSAGE);
    out.extend_from_slice(&stream_handle_id.to_le_bytes());
    out.extend_from_slice(&msg.seq.to_le_bytes());
    out.extend_from_slice(&msg.stream_id.to_le_bytes());
    out.extend_from_slice(&topic_len.to_le_bytes());
    out.extend_from_slice(&msg.topic);
    out.extend_from_slice(&msg.payload);
    out
}

/// Encode an `ack` S→C frame (op 0x08) for a successful C→S op.
/// `op_tag_acked` is the tag byte of the operation being acknowledged
/// (e.g. `c2s::SUBSCRIBE`).
pub fn encode_ack(req_id: u32, op_tag_acked: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(7);
    out.push(s2c::ACK);
    out.extend_from_slice(&req_id.to_le_bytes());
    out.push(op_tag_acked);
    out.push(0); // status = OK
    out
}

/// Encode an `error` S→C frame (op 0x09) for a failed C→S op.
/// `op_tag_errored` is the tag byte of the operation that failed;
/// `err_code` is a stable numeric code the browser can switch on;
/// `msg` is a human-readable diagnostic.
pub fn encode_error(req_id: u32, op_tag_errored: u8, err_code: u16, msg: &str) -> Vec<u8> {
    let msg_bytes = msg.as_bytes();
    let msg_len = u16::try_from(msg_bytes.len()).unwrap_or(u16::MAX);
    let mut out = Vec::with_capacity(1 + 4 + 1 + 2 + 2 + msg_bytes.len());
    out.push(s2c::ERROR);
    out.extend_from_slice(&req_id.to_le_bytes());
    out.push(op_tag_errored);
    out.extend_from_slice(&err_code.to_le_bytes());
    out.extend_from_slice(&msg_len.to_le_bytes());
    out.extend_from_slice(msg_bytes);
    out
}

// ── tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(op: u8, body: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + body.len());
        v.push(op);
        v.extend_from_slice(body);
        v
    }

    #[test]
    fn decode_subscribe_roundtrip() {
        // req_id=42, sub_id=99, topic="foo.bar"
        let mut body = Vec::new();
        body.extend_from_slice(&42u32.to_le_bytes());
        body.extend_from_slice(&99u32.to_le_bytes());
        body.extend_from_slice(&7u16.to_le_bytes());
        body.extend_from_slice(b"foo.bar");
        let buf = frame(c2s::SUBSCRIBE, &body);
        match decode_client_op(&buf).unwrap() {
            ClientOp::Subscribe {
                req_id,
                sub_id,
                topic,
            } => {
                assert_eq!(req_id, 42);
                assert_eq!(sub_id, 99);
                assert_eq!(topic, "foo.bar");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_publish_with_payload() {
        let mut body = Vec::new();
        body.extend_from_slice(&7u32.to_le_bytes()); // req_id
        body.extend_from_slice(&3u16.to_le_bytes()); // topic len
        body.extend_from_slice(b"foo"); // topic
        body.extend_from_slice(b"payload bytes"); // payload
        let buf = frame(c2s::PUBLISH, &body);
        match decode_client_op(&buf).unwrap() {
            ClientOp::Publish {
                req_id,
                topic,
                payload,
            } => {
                assert_eq!(req_id, 7);
                assert_eq!(topic, "foo");
                assert_eq!(payload.as_ref(), b"payload bytes");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_open_stream_catchall() {
        // topic_len=0 ⇒ catch-all ⇒ None
        let mut body = Vec::new();
        body.extend_from_slice(&1u32.to_le_bytes()); // req_id
        body.extend_from_slice(&5u32.to_le_bytes()); // stream_handle_id
        body.push(policy::LATEST_ONLY);
        body.extend_from_slice(&0u16.to_le_bytes()); // empty topic
        let buf = frame(c2s::OPEN_STREAM, &body);
        match decode_client_op(&buf).unwrap() {
            ClientOp::OpenStream {
                policy,
                topic,
                ..
            } => {
                assert_eq!(policy, policy::LATEST_ONLY);
                assert!(topic.is_none());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_bad_policy_rejected() {
        let mut body = Vec::new();
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.push(0xFE); // bad policy
        body.extend_from_slice(&0u16.to_le_bytes());
        let buf = frame(c2s::OPEN_STREAM, &body);
        assert!(matches!(
            decode_client_op(&buf),
            Err(DecodeError::BadPolicy(0xFE))
        ));
    }

    #[test]
    fn decode_close_is_payloadless() {
        let buf = vec![c2s::CLOSE];
        assert!(matches!(
            decode_client_op(&buf),
            Ok(ClientOp::Close)
        ));
    }

    #[test]
    fn decode_empty_buffer_is_short() {
        assert!(matches!(decode_client_op(&[]), Err(DecodeError::Short)));
    }

    #[test]
    fn encode_message_layout() {
        let msg = vireon_sdk::Message {
            topic: Bytes::from_static(b"foo.bar"),
            payload: Bytes::from_static(b"hello"),
            seq: 42,
            stream_id: 4,
        };
        let out = encode_message(99, &msg);
        assert_eq!(out[0], s2c::MESSAGE);
        assert_eq!(u32::from_le_bytes(out[1..5].try_into().unwrap()), 99);
        assert_eq!(u64::from_le_bytes(out[5..13].try_into().unwrap()), 42);
        assert_eq!(u64::from_le_bytes(out[13..21].try_into().unwrap()), 4);
        assert_eq!(u16::from_le_bytes(out[21..23].try_into().unwrap()), 7);
        assert_eq!(&out[23..30], b"foo.bar");
        assert_eq!(&out[30..], b"hello");
    }

    #[test]
    fn encode_ack_minimal() {
        let out = encode_ack(7, c2s::SUBSCRIBE);
        assert_eq!(out.len(), 7);
        assert_eq!(out[0], s2c::ACK);
        assert_eq!(u32::from_le_bytes(out[1..5].try_into().unwrap()), 7);
        assert_eq!(out[5], c2s::SUBSCRIBE);
        assert_eq!(out[6], 0);
    }

    #[test]
    fn encode_error_with_msg() {
        let out = encode_error(3, c2s::PUBLISH, 404, "not found");
        assert_eq!(out[0], s2c::ERROR);
        assert_eq!(u32::from_le_bytes(out[1..5].try_into().unwrap()), 3);
        assert_eq!(out[5], c2s::PUBLISH);
        assert_eq!(u16::from_le_bytes(out[6..8].try_into().unwrap()), 404);
        assert_eq!(u16::from_le_bytes(out[8..10].try_into().unwrap()), 9);
        assert_eq!(&out[10..], b"not found");
    }
}
