//! [`InterNodeMessage`] — wire format for cluster-internal traffic.
//!
//! These messages ride on top of the existing QUIC frame codec
//! (`message-layer/frame/src/codec.rs`), which already adds a CRC32C
//! trailer and length prefix. So this layer concerns itself only with
//! the body encoding: a 19-byte fixed header followed by the topic
//! string and payload bytes.
//!
//! ## Layout
//!
//! ```text
//! Offset  Size  Field
//! 0       1     kind (InterNodeKind discriminant)
//! 1       4     origin_node (u32 LE)
//! 5       4     origin_conn_idx (u32 LE)
//! 9       8     origin_stream_id (u64 LE)
//! 17      2     topic_len (u16 LE, 1..=65535)
//! 19      var   topic_or_pattern bytes
//! 19+T    4     payload_len (u32 LE)
//! 23+T    var   payload bytes
//! ```
//!
//! Empty payload is legal (used by Subscribe/Unsubscribe). Empty
//! topic is rejected ([`crate::ClusterError::TopicEmpty`]) so the
//! topic can never be confused with a missing field.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use session_registry::NodeId;

use crate::error::ClusterError;

/// Fixed prefix size: kind(1) + origin_node(4) + origin_conn_idx(4)
/// + origin_stream_id(8) + topic_len(2).
const HEADER_LEN: usize = 1 + 4 + 4 + 8 + 2;
/// Size of the payload length prefix.
const PAYLOAD_LEN_SIZE: usize = 4;
/// Maximum topic length. Stays under the u16 prefix so it fits.
pub const MAX_TOPIC_LEN: usize = u16::MAX as usize; // 65_535

/// Discriminant for [`InterNodeMessage`].
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InterNodeKind {
    /// Origin → owner: "please publish this payload to this topic".
    Publish = 0x01,
    /// Owner → interested node: "deliver this to your local subscribers".
    Fanout = 0x02,
    /// Non-owner → owner: "I have local interest in this pattern".
    Subscribe = 0x03,
    /// Non-owner → owner: "stop sending fanouts for this pattern".
    Unsubscribe = 0x04,
}

impl InterNodeKind {
    /// Inverse of the `as u8` cast. Returns `None` for unknown
    /// discriminants so the decoder can fail cleanly.
    #[must_use]
    const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Publish),
            0x02 => Some(Self::Fanout),
            0x03 => Some(Self::Subscribe),
            0x04 => Some(Self::Unsubscribe),
            _ => None,
        }
    }
}

/// One inter-node message body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterNodeMessage {
    /// What to do with this message.
    pub kind: InterNodeKind,
    /// Id of the node that originated the publish/subscribe. For
    /// `Fanout` this is the **original publisher** (not the owner
    /// doing the fanout) so downstream nodes can back-route acks.
    pub origin_node: NodeId,
    /// Connection slab index on the origin node. Back-route target.
    pub origin_conn_idx: u32,
    /// QUIC stream id on the origin connection.
    pub origin_stream_id: u64,
    /// Topic (for Publish/Fanout) or pattern (for Subscribe/Unsubscribe).
    /// 1..=65535 bytes UTF-8 (caller validates encoding; wire is bytes).
    pub topic_or_pattern: String,
    /// Message payload. Empty for Subscribe/Unsubscribe.
    pub payload: Bytes,
}

impl InterNodeMessage {
    /// Construct a Publish message.
    #[must_use]
    pub fn publish(
        origin: NodeId,
        conn_idx: u32,
        stream_id: u64,
        topic: String,
        payload: Bytes,
    ) -> Self {
        Self {
            kind: InterNodeKind::Publish,
            origin_node: origin,
            origin_conn_idx: conn_idx,
            origin_stream_id: stream_id,
            topic_or_pattern: topic,
            payload,
        }
    }

    /// Construct a Fanout message.
    #[must_use]
    pub fn fanout(
        origin: NodeId,
        conn_idx: u32,
        stream_id: u64,
        topic: String,
        payload: Bytes,
    ) -> Self {
        Self {
            kind: InterNodeKind::Fanout,
            origin_node: origin,
            origin_conn_idx: conn_idx,
            origin_stream_id: stream_id,
            topic_or_pattern: topic,
            payload,
        }
    }

    /// Construct a Subscribe message (empty payload).
    #[must_use]
    pub fn subscribe(origin: NodeId, conn_idx: u32, stream_id: u64, pattern: String) -> Self {
        Self {
            kind: InterNodeKind::Subscribe,
            origin_node: origin,
            origin_conn_idx: conn_idx,
            origin_stream_id: stream_id,
            topic_or_pattern: pattern,
            payload: Bytes::new(),
        }
    }

    /// Construct an Unsubscribe message (empty payload).
    #[must_use]
    pub fn unsubscribe(origin: NodeId, conn_idx: u32, stream_id: u64, pattern: String) -> Self {
        Self {
            kind: InterNodeKind::Unsubscribe,
            origin_node: origin,
            origin_conn_idx: conn_idx,
            origin_stream_id: stream_id,
            topic_or_pattern: pattern,
            payload: Bytes::new(),
        }
    }

    /// Serialized byte length (header + topic + payload_len + payload).
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.topic_or_pattern.len() + PAYLOAD_LEN_SIZE + self.payload.len()
    }

    /// Encode to a fresh `Bytes`.
    ///
    /// # Errors
    /// [`ClusterError::TopicEmpty`] if `topic_or_pattern` is empty.
    /// [`ClusterError::InvalidConfig`] if the topic exceeds `MAX_TOPIC_LEN`.
    pub fn encode(&self) -> Result<Bytes, ClusterError> {
        let topic_bytes = self.topic_or_pattern.as_bytes();
        let topic_len = topic_bytes.len();
        if topic_len == 0 {
            return Err(ClusterError::TopicEmpty);
        }
        if topic_len > MAX_TOPIC_LEN {
            return Err(ClusterError::InvalidConfig {
                reason: "topic exceeds 65535 bytes",
            });
        }
        let total = self.encoded_len();
        let mut buf = BytesMut::with_capacity(total);
        buf.put_u8(self.kind as u8);
        buf.put_u32_le(self.origin_node.raw());
        buf.put_u32_le(self.origin_conn_idx);
        buf.put_u64_le(self.origin_stream_id);
        buf.put_u16_le(topic_len as u16);
        buf.put_slice(topic_bytes);
        buf.put_u32_le(self.payload.len() as u32);
        buf.put_slice(&self.payload);
        Ok(buf.freeze())
    }

    /// Encode into a caller-supplied scratch buffer, returning a borrow
    /// of the encoded bytes. The buffer is cleared (not shrunk) first so
    /// its allocation is reused across calls — avoids one `BytesMut`
    /// allocation per cluster forward on the transport hot path.
    ///
    /// # Errors
    /// Same as [`encode`](Self::encode).
    pub fn encode_into<'a>(
        &self,
        buf: &'a mut BytesMut,
    ) -> Result<&'a [u8], ClusterError> {
        let topic_bytes = self.topic_or_pattern.as_bytes();
        let topic_len = topic_bytes.len();
        if topic_len == 0 {
            return Err(ClusterError::TopicEmpty);
        }
        if topic_len > MAX_TOPIC_LEN {
            return Err(ClusterError::InvalidConfig {
                reason: "topic exceeds 65535 bytes",
            });
        }
        let total = self.encoded_len();
        buf.clear();
        buf.reserve(total);
        buf.put_u8(self.kind as u8);
        buf.put_u32_le(self.origin_node.raw());
        buf.put_u32_le(self.origin_conn_idx);
        buf.put_u64_le(self.origin_stream_id);
        buf.put_u16_le(topic_len as u16);
        buf.put_slice(topic_bytes);
        buf.put_u32_le(self.payload.len() as u32);
        buf.put_slice(&self.payload);
        Ok(buf)
    }

    /// Decode.
    ///
    /// # Errors
    /// [`ClusterError::DecodeFailed`] on truncation, bad discriminant,
    /// or over-length fields.
    /// [`ClusterError::TopicEmpty`] if topic length is zero.
    pub fn decode(buf: &[u8]) -> Result<Self, ClusterError> {
        if buf.len() < HEADER_LEN {
            return Err(ClusterError::DecodeFailed {
                reason: "buffer shorter than fixed header",
            });
        }
        let mut cur = buf;
        let kind_byte = cur.get_u8();
        let kind = InterNodeKind::from_u8(kind_byte).ok_or(ClusterError::DecodeFailed {
            reason: "unknown InterNodeKind discriminant",
        })?;
        let origin_node = NodeId::new(cur.get_u32_le());
        let origin_conn_idx = cur.get_u32_le();
        let origin_stream_id = cur.get_u64_le();
        let topic_len = cur.get_u16_le() as usize;
        if topic_len == 0 {
            return Err(ClusterError::TopicEmpty);
        }
        if cur.remaining() < topic_len + PAYLOAD_LEN_SIZE {
            return Err(ClusterError::DecodeFailed {
                reason: "buffer truncated before payload length",
            });
        }
        let topic_bytes = &cur[..topic_len];
        cur.advance(topic_len);
        let payload_len = cur.get_u32_le() as usize;
        if cur.remaining() < payload_len {
            return Err(ClusterError::DecodeFailed {
                reason: "buffer truncated inside payload",
            });
        }
        let payload_bytes = &cur[..payload_len];
        let topic_or_pattern =
            String::from_utf8(topic_bytes.to_vec()).map_err(|_| ClusterError::DecodeFailed {
                reason: "topic is not valid UTF-8",
            })?;
        let payload = Bytes::copy_from_slice(payload_bytes);
        Ok(Self {
            kind,
            origin_node,
            origin_conn_idx,
            origin_stream_id,
            topic_or_pattern,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn sample() -> InterNodeMessage {
        InterNodeMessage::publish(
            NodeId::new(7),
            42,
            0xDEAD_BEEF_CAFE,
            "sensor.temp".to_owned(),
            Bytes::from_static(b"\x01\x02\x03"),
        )
    }

    #[test]
    fn round_trip_basic() {
        let m = sample();
        let bytes = m.encode().unwrap();
        let back = InterNodeMessage::decode(&bytes).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn round_trip_empty_payload() {
        let m = InterNodeMessage::subscribe(NodeId::new(2), 5, 100, "alerts.*".to_owned());
        let bytes = m.encode().unwrap();
        let back = InterNodeMessage::decode(&bytes).unwrap();
        assert_eq!(m, back);
        assert!(back.payload.is_empty());
    }

    #[test]
    fn round_trip_subscribe_unsubscribe() {
        for kind in [InterNodeKind::Subscribe, InterNodeKind::Unsubscribe] {
            let m = InterNodeMessage {
                kind,
                origin_node: NodeId::new(3),
                origin_conn_idx: 9,
                origin_stream_id: 99,
                topic_or_pattern: "*.*".to_owned(),
                payload: Bytes::new(),
            };
            let bytes = m.encode().unwrap();
            assert_eq!(InterNodeMessage::decode(&bytes).unwrap(), m);
        }
    }

    #[test]
    fn encoded_len_matches_encode_output() {
        let m = sample();
        let bytes = m.encode().unwrap();
        assert_eq!(bytes.len(), m.encoded_len());
    }

    #[test]
    fn encode_rejects_empty_topic() {
        let m = InterNodeMessage::publish(
            NodeId::new(1),
            1,
            1,
            String::new(),
            Bytes::from_static(b"x"),
        );
        assert!(matches!(m.encode(), Err(ClusterError::TopicEmpty)));
    }

    #[test]
    fn encode_rejects_overlong_topic() {
        let m = InterNodeMessage::publish(
            NodeId::new(1),
            1,
            1,
            "x".repeat(MAX_TOPIC_LEN + 1),
            Bytes::new(),
        );
        assert!(matches!(
            m.encode(),
            Err(ClusterError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn max_topic_length_round_trips() {
        let topic = "x".repeat(MAX_TOPIC_LEN);
        let m =
            InterNodeMessage::publish(NodeId::new(1), 1, 1, topic, Bytes::from_static(b"payload"));
        let bytes = m.encode().unwrap();
        let back = InterNodeMessage::decode(&bytes).unwrap();
        assert_eq!(back.topic_or_pattern.len(), MAX_TOPIC_LEN);
    }

    #[test]
    fn decode_rejects_short_buffer() {
        // < HEADER_LEN
        assert!(matches!(
            InterNodeMessage::decode(&[0u8; 5]),
            Err(ClusterError::DecodeFailed { .. })
        ));
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        // Manually craft a buffer with kind=0xFF.
        let mut buf = Vec::new();
        buf.push(0xFF);
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&3u64.to_le_bytes());
        buf.extend_from_slice(&5u16.to_le_bytes());
        buf.extend_from_slice(b"hello");
        buf.extend_from_slice(&0u32.to_le_bytes());
        assert!(matches!(
            InterNodeMessage::decode(&buf),
            Err(ClusterError::DecodeFailed { .. })
        ));
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let m = sample();
        let mut bytes = m.encode().unwrap();
        // Truncate by one byte.
        let shortened = bytes.split_to(bytes.len() - 1);
        assert!(matches!(
            InterNodeMessage::decode(&shortened),
            Err(ClusterError::DecodeFailed { .. })
        ));
    }

    #[test]
    fn decode_rejects_truncated_topic() {
        let m = sample();
        let bytes = m.encode().unwrap();
        // Cut in the middle of the topic.
        let cut = bytes.len() - 5;
        assert!(matches!(
            InterNodeMessage::decode(&bytes[..cut]),
            Err(ClusterError::DecodeFailed { .. })
        ));
    }

    #[test]
    fn decode_rejects_zero_topic_len() {
        // Header with topic_len = 0.
        let mut buf = Vec::new();
        buf.push(InterNodeKind::Publish as u8);
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&3u64.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // topic_len = 0
        buf.extend_from_slice(&0u32.to_le_bytes()); // payload_len = 0
        assert!(matches!(
            InterNodeMessage::decode(&buf),
            Err(ClusterError::TopicEmpty)
        ));
    }

    #[test]
    fn kind_from_u8_round_trips() {
        for k in [
            InterNodeKind::Publish,
            InterNodeKind::Fanout,
            InterNodeKind::Subscribe,
            InterNodeKind::Unsubscribe,
        ] {
            assert_eq!(InterNodeKind::from_u8(k as u8), Some(k));
        }
        assert_eq!(InterNodeKind::from_u8(0x00), None);
        assert_eq!(InterNodeKind::from_u8(0xFF), None);
    }
}
