//! Typed payloads for control-plane frames and Error/Heartbeat.
//!
//! The frame layer treats data-plane payloads (TextToken, AudioFrame, etc.) as
//! opaque bytes — their interpretation is an application-layer concern. But
//! control-plane payloads and the Error/Heartbeat data-plane types carry
//! **structured** content that the frame layer can parse:
//!
//! | Type         | Wire layout (big-endian)                          |
//! |--------------|---------------------------------------------------|
//! | StreamOpen   | `direction(u8)`                                   |
//! | StreamClose  | `reason(u16)`                                     |
//! | StreamReset  | `error_code(u32)` + `reason_len(u16)` + `reason`  |
//! | Handshake    | `version(u8)` + `capabilities(u32)`               |
//! | Ack          | `ack_seq(u64)`                                    |
//! | Error        | `code(u32)` + `msg_len(u16)` + `msg`              |
//! | Heartbeat    | empty (0 bytes)                                   |
//! | Fragment     | no struct — raw bytes handled by `FragmentReassembler` |
//! | Subscribe    | `pattern_len(u16)` + `pattern` + `qos(u8)`        |
//! | Unsubscribe  | `pattern_len(u16)` + `pattern`                    |
//! | Publish      | `topic_len(u16)` + `topic` + payload bytes        |
//! | TopicCreate  | `topic_len(u16)` + `topic` + `partitions(u32)` + `retention_ms(u64)` |
//! | TopicDelete  | `topic_len(u16)` + `topic`                        |
//!
//! Decode returns borrowed `&str` / `&[u8]` views for variable-length fields
//! (zero-copy from the frame payload, matching the `FrameRef` philosophy).
//!
//! # Example
//!
//! ```
//! use frame::byte_writer::ByteWriter;
//! use frame::byte_reader::ByteReader;
//! use frame::payload::{Ack, parse_payload};
//! use frame::header::MessageType;
//!
//! // Encode an Ack payload
//! let ack = Ack { ack_seq: 42 };
//! let mut buf = vec![0u8; Ack::WIRE_SIZE];
//! let mut w = ByteWriter::new(&mut buf);
//! ack.encode(&mut w);
//!
//! // Parse it back via the top-level dispatcher
//! let parsed = parse_payload(MessageType::Ack, &buf).unwrap().unwrap();
//! assert!(matches!(parsed, frame::payload::ParsedPayload::Ack(Ack { ack_seq: 42 })));
//! ```

use crate::byte_reader::ByteReader;
use crate::byte_writer::ByteWriter;
use crate::header::{MessageType, Version};

// ── Error type ──────────────────────────────────────────────────────

/// Errors returned by payload decode functions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PayloadError {
    /// The payload buffer was shorter than the wire format requires.
    #[error("payload truncated: need {needed} bytes, have {have}")]
    Truncated {
        /// Minimum bytes the wire format needs.
        needed: usize,
        /// Bytes actually present in the payload.
        have: usize,
    },

    /// The stream-direction byte does not map to a known variant.
    #[error("invalid stream direction: 0x{value:02X}")]
    InvalidDirection {
        /// The raw byte that was not recognised.
        value: u8,
    },

    /// A variable-length text field contained invalid UTF-8.
    #[error("invalid UTF-8: {source}")]
    InvalidUtf8 {
        /// The underlying UTF-8 validation error.
        #[source]
        source: std::str::Utf8Error,
    },
}

// ── StreamDirection ─────────────────────────────────────────────────

/// Direction of a logical stream, carried in the StreamOpen payload.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamDirection {
    /// Data flows in both directions (client ↔ server).
    Bidirectional = 0,
    /// Data flows in one direction only.
    Unidirectional = 1,
}

impl StreamDirection {
    /// Decodes a wire byte. Returns `Err` for unknown values.
    pub const fn from_byte(b: u8) -> Result<Self, PayloadError> {
        match b {
            0 => Ok(Self::Bidirectional),
            1 => Ok(Self::Unidirectional),
            other => Err(PayloadError::InvalidDirection { value: other }),
        }
    }

    /// Returns the wire byte for this variant.
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

// ── StreamOpen ──────────────────────────────────────────────────────

/// Payload for [`MessageType::StreamOpen`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamOpen {
    /// Whether the new stream is bidirectional or unidirectional.
    pub direction: StreamDirection,
}

impl StreamOpen {
    /// Fixed wire size: 1 byte.
    pub const WIRE_SIZE: usize = 1;

    /// Encodes into `w` (writes exactly [`WIRE_SIZE`](Self::WIRE_SIZE) bytes).
    #[inline]
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u8(self.direction.as_byte());
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < Self::WIRE_SIZE {
            return Err(PayloadError::Truncated {
                needed: Self::WIRE_SIZE,
                have: r.remaining(),
            });
        }
        let direction = StreamDirection::from_byte(r.read_u8())?;
        Ok(Self { direction })
    }
}

// ── StreamClose ─────────────────────────────────────────────────────

/// Payload for [`MessageType::StreamClose`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamClose {
    /// Machine-readable close reason code (0 = normal close).
    pub reason: u16,
}

impl StreamClose {
    /// Fixed wire size: 2 bytes.
    pub const WIRE_SIZE: usize = 2;

    /// Normal graceful close — no error condition.
    pub const NORMAL: u16 = 0;

    /// Encodes into `w`.
    #[inline]
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u16(self.reason);
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < Self::WIRE_SIZE {
            return Err(PayloadError::Truncated {
                needed: Self::WIRE_SIZE,
                have: r.remaining(),
            });
        }
        Ok(Self {
            reason: r.read_u16(),
        })
    }
}

// ── StreamReset ─────────────────────────────────────────────────────

/// Payload for [`MessageType::StreamReset`].
///
/// The `reason` field borrows from the payload buffer — no allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamReset<'a> {
    /// Application-layer error code explaining the reset.
    pub error_code: u32,
    /// Human-readable reason string (UTF-8, borrowed from the payload).
    pub reason: &'a str,
}

/// Fixed header portion of StreamReset: `error_code(4) + reason_len(2)`.
const STREAM_RESET_MIN_SIZE: usize = 6;

impl<'a> StreamReset<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u32(self.error_code);
        w.write_u16(self.reason.len() as u16);
        w.write_bytes(self.reason.as_bytes());
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        STREAM_RESET_MIN_SIZE + self.reason.len()
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        if r.remaining() < STREAM_RESET_MIN_SIZE {
            return Err(PayloadError::Truncated {
                needed: STREAM_RESET_MIN_SIZE,
                have: r.remaining(),
            });
        }
        let error_code = r.read_u32();
        let reason_len = r.read_u16() as usize;
        if r.remaining() < reason_len {
            return Err(PayloadError::Truncated {
                needed: STREAM_RESET_MIN_SIZE + reason_len,
                have: STREAM_RESET_MIN_SIZE + r.remaining(),
            });
        }
        let reason_bytes = r.read_slice(reason_len);
        let reason = std::str::from_utf8(reason_bytes)
            .map_err(|source| PayloadError::InvalidUtf8 { source })?;
        Ok(Self { error_code, reason })
    }
}

// ── Handshake ───────────────────────────────────────────────────────

/// Payload for [`MessageType::Handshake`].
///
/// As of per-connection PrincipalId wiring, the handshake carries the
/// caller's identity claim. The server's `AuthenticationPipeline`
/// verifies `(principal_id, credential_token)` and binds the resolved
/// principal to the connection's `conn_idx`. A `principal_id` of 0
/// means anonymous (server falls back to its configured default).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Handshake {
    /// Protocol version the sender proposes or agrees to.
    pub version: Version,
    /// Capability bitfield (bit positions defined by the application).
    pub capabilities: u32,
    /// Principal the sender claims to be. 0 = anonymous.
    pub principal_id: u64,
    /// Opaque credential verified by `auth_framework::AuthenticationPipeline`.
    /// For static-key deployments this is a shared secret; for production
    /// deployments this will be a signed token (JWT, etc.) — but the
    /// wire shape does not change.
    pub credential_token: u64,
}

impl Handshake {
    /// Fixed wire size: `version(1) + capabilities(4) + principal_id(8)
    /// + credential_token(8)` = 21 bytes.
    pub const WIRE_SIZE: usize = 21;

    /// Encodes into `w`.
    #[inline]
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u8(self.version.get());
        w.write_u32(self.capabilities);
        w.write_u64(self.principal_id);
        w.write_u64(self.credential_token);
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < Self::WIRE_SIZE {
            return Err(PayloadError::Truncated {
                needed: Self::WIRE_SIZE,
                have: r.remaining(),
            });
        }
        Ok(Self {
            version: Version::new(r.read_u8()),
            capabilities: r.read_u32(),
            principal_id: r.read_u64(),
            credential_token: r.read_u64(),
        })
    }
}

// ── Ack ─────────────────────────────────────────────────────────────

/// Payload for [`MessageType::Ack`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Ack {
    /// The per-stream sequence number being acknowledged.
    pub ack_seq: u64,
}

impl Ack {
    /// Fixed wire size: 8 bytes.
    pub const WIRE_SIZE: usize = 8;

    /// Encodes into `w`.
    #[inline]
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u64(self.ack_seq);
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < Self::WIRE_SIZE {
            return Err(PayloadError::Truncated {
                needed: Self::WIRE_SIZE,
                have: r.remaining(),
            });
        }
        Ok(Self {
            ack_seq: r.read_u64(),
        })
    }
}

// ── ErrorPayload ────────────────────────────────────────────────────

/// Payload for [`MessageType::Error`].
///
/// The `message` field borrows from the payload buffer — no allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorPayload<'a> {
    /// Machine-readable error code.
    pub code: u32,
    /// Human-readable error message (UTF-8, borrowed from the payload).
    pub message: &'a str,
}

/// Fixed header portion of ErrorPayload: `code(4) + msg_len(2)`.
const ERROR_MIN_SIZE: usize = 6;

impl<'a> ErrorPayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u32(self.code);
        w.write_u16(self.message.len() as u16);
        w.write_bytes(self.message.as_bytes());
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        ERROR_MIN_SIZE + self.message.len()
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        if r.remaining() < ERROR_MIN_SIZE {
            return Err(PayloadError::Truncated {
                needed: ERROR_MIN_SIZE,
                have: r.remaining(),
            });
        }
        let code = r.read_u32();
        let msg_len = r.read_u16() as usize;
        if r.remaining() < msg_len {
            return Err(PayloadError::Truncated {
                needed: ERROR_MIN_SIZE + msg_len,
                have: ERROR_MIN_SIZE + r.remaining(),
            });
        }
        let msg_bytes = r.read_slice(msg_len);
        let message = std::str::from_utf8(msg_bytes)
            .map_err(|source| PayloadError::InvalidUtf8 { source })?;
        Ok(Self { code, message })
    }
}

// ── ParsedPayload enum ──────────────────────────────────────────────

/// A parsed control-plane / Error / Heartbeat payload.
///
/// Produced by [`parse_payload`]. The variable-length variants
/// (`StreamReset`, `Error`, pub/sub) borrow from the input payload buffer.
///
/// Note: this enum is `Clone` but not `Copy` because the resume-related
/// variants ([`Resume`](ResumePayload), [`ResumeOk`](ResumeOkPayload),
/// [`ResumeUnavailable`](ResumeUnavailablePayload)) own a `Vec` of slots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParsedPayload<'a> {
    /// [`MessageType::StreamOpen`].
    StreamOpen(StreamOpen),
    /// [`MessageType::StreamClose`].
    StreamClose(StreamClose),
    /// [`MessageType::StreamReset`].
    StreamReset(StreamReset<'a>),
    /// [`MessageType::Handshake`].
    Handshake(Handshake),
    /// [`MessageType::Ack`].
    Ack(Ack),
    /// [`MessageType::Error`].
    Error(ErrorPayload<'a>),
    /// [`MessageType::Heartbeat`] — no payload bytes.
    Heartbeat,
    /// [`MessageType::Subscribe`].
    Subscribe(SubscribePayload<'a>),
    /// [`MessageType::Unsubscribe`].
    Unsubscribe(UnsubscribePayload<'a>),
    /// [`MessageType::Publish`].
    Publish(PublishPayload<'a>),
    /// [`MessageType::TopicCreate`].
    TopicCreate(TopicCreatePayload<'a>),
    /// [`MessageType::TopicDelete`].
    TopicDelete(TopicDeletePayload<'a>),
    /// [`MessageType::Fetch`].
    Fetch(FetchPayload<'a>),
    /// [`MessageType::FetchReply`].
    FetchReply(FetchReplyPayload<'a>),
    /// [`MessageType::Resume`] — client resume / session-hello.
    Resume(ResumePayload),
    /// [`MessageType::ResumeOk`] — server resume accepted.
    ResumeOk(ResumeOkPayload),
    /// [`MessageType::ResumeUnavailable`] — replay window exceeded.
    ResumeUnavailable(ResumeUnavailablePayload),
}

// ── Pub/sub payloads ────────────────────────────────────────────────

/// Maximum topic / pattern length expressible on the wire (u16 field).
/// 64 KiB is far above any realistic topic name; the cap exists only to
/// give receivers a sane upper bound for the length-prefix read.
pub const MAX_TOPIC_LEN: usize = u16::MAX as usize;

/// Reads a `u16`-prefixed UTF-8 string from `r` and returns a borrowed view.
fn read_string<'a>(r: &mut ByteReader<'a>) -> Result<&'a str, PayloadError> {
    if r.remaining() < 2 {
        return Err(PayloadError::Truncated {
            needed: 2,
            have: r.remaining(),
        });
    }
    let len = r.read_u16() as usize;
    if r.remaining() < len {
        return Err(PayloadError::Truncated {
            needed: 2 + len,
            have: 2 + r.remaining(),
        });
    }
    let bytes = r.read_slice(len);
    std::str::from_utf8(bytes).map_err(|source| PayloadError::InvalidUtf8 { source })
}

/// Writes a `u16`-prefixed UTF-8 string into `w`.
fn write_string(w: &mut ByteWriter, s: &str) {
    w.write_u16(s.len() as u16);
    w.write_bytes(s.as_bytes());
}

/// Exact wire size a length-prefixed string will occupy: 2 + `s.len()`.
#[inline]
fn string_wire_size(s: &str) -> usize {
    2 + s.len()
}

/// Payload for [`MessageType::Subscribe`].
///
/// `pattern` borrows from the underlying payload buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscribePayload<'a> {
    /// Topic pattern (may include wildcards; semantics defined by the
    /// application's pub/sub matcher).
    pub pattern: &'a str,
    /// Quality-of-service hint (0 = default). Interpretation is
    /// application-defined — e.g. 0 = at-most-once, 1 = at-least-once.
    pub qos: u8,
}

impl<'a> SubscribePayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        write_string(w, self.pattern);
        w.write_u8(self.qos);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        string_wire_size(self.pattern) + 1
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        let pattern = read_string(r)?;
        if r.remaining() < 1 {
            return Err(PayloadError::Truncated {
                needed: 1,
                have: r.remaining(),
            });
        }
        let qos = r.read_u8();
        Ok(Self { pattern, qos })
    }
}

/// Payload for [`MessageType::Unsubscribe`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsubscribePayload<'a> {
    /// Topic pattern previously registered via [`SubscribePayload`].
    pub pattern: &'a str,
}

impl<'a> UnsubscribePayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        write_string(w, self.pattern);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        string_wire_size(self.pattern)
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        let pattern = read_string(r)?;
        Ok(Self { pattern })
    }
}

/// Payload for [`MessageType::Publish`].
///
/// `topic` and `payload` both borrow from the underlying payload buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishPayload<'a> {
    /// Destination topic name.
    pub topic: &'a str,
    /// Opaque message bytes carried after the topic. The pub/sub engine
    /// does not interpret these; delivery policy applies to the framed
    /// `MessageType::Publish` as a whole.
    pub payload: &'a [u8],
}

impl<'a> PublishPayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        write_string(w, self.topic);
        w.write_bytes(self.payload);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        string_wire_size(self.topic) + self.payload.len()
    }

    /// Decodes from `r`. The payload slice consumes all remaining bytes —
    /// a Publish frame is the last field on the wire by construction.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        let topic = read_string(r)?;
        let payload = r.read_slice(r.remaining());
        Ok(Self { topic, payload })
    }
}

/// Payload for [`MessageType::TopicCreate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicCreatePayload<'a> {
    /// Topic name to create.
    pub topic: &'a str,
    /// Partition count for the new topic.
    pub partitions: u32,
    /// Retention window in milliseconds (0 = unlimited while the topic lives).
    pub retention_ms: u64,
}

/// Fixed tail of a TopicCreate payload: `partitions(4) + retention_ms(8)`.
const TOPIC_CREATE_TAIL_SIZE: usize = 12;

impl<'a> TopicCreatePayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        write_string(w, self.topic);
        w.write_u32(self.partitions);
        w.write_u64(self.retention_ms);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        string_wire_size(self.topic) + TOPIC_CREATE_TAIL_SIZE
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        let topic = read_string(r)?;
        if r.remaining() < TOPIC_CREATE_TAIL_SIZE {
            return Err(PayloadError::Truncated {
                needed: TOPIC_CREATE_TAIL_SIZE,
                have: r.remaining(),
            });
        }
        let partitions = r.read_u32();
        let retention_ms = r.read_u64();
        Ok(Self {
            topic,
            partitions,
            retention_ms,
        })
    }
}

/// Payload for [`MessageType::TopicDelete`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicDeletePayload<'a> {
    /// Topic name to delete.
    pub topic: &'a str,
}

impl<'a> TopicDeletePayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        write_string(w, self.topic);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        string_wire_size(self.topic)
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        let topic = read_string(r)?;
        Ok(Self { topic })
    }
}

/// Payload for [`MessageType::Fetch`] (subscriber → server).
///
/// The caller asks the server to replay WAL entries for `topic` starting
/// at `from_offset`. The server responds with at most `max_count`
/// [`FetchReplyPayload`] frames (or fewer if the log has fewer entries
/// available).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchPayload<'a> {
    /// Topic name to replay from.
    pub topic: &'a str,
    /// Inclusive starting offset. `0` means "from the oldest retained
    /// entry". Offsets below the server's oldest retained offset are
    /// clamped to that oldest offset.
    pub from_offset: u64,
    /// Maximum number of `FetchReply` frames the subscriber will accept.
    /// The server caps this at [`FETCH_MAX_COUNT`] to bound fan-out.
    pub max_count: u32,
}

/// Fixed tail of a Fetch payload: `from_offset(8) + max_count(4)`.
const FETCH_TAIL_SIZE: usize = 12;

/// Hard upper bound on `FetchPayload::max_count`. A single Fetch request
/// can produce at most this many `FetchReply` frames — keeps a wild
/// request from saturating a subscriber's stream for seconds.
pub const FETCH_MAX_COUNT: u32 = 256;

impl<'a> FetchPayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        write_string(w, self.topic);
        w.write_u64(self.from_offset);
        w.write_u32(self.max_count);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        string_wire_size(self.topic) + FETCH_TAIL_SIZE
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        let topic = read_string(r)?;
        if r.remaining() < FETCH_TAIL_SIZE {
            return Err(PayloadError::Truncated {
                needed: FETCH_TAIL_SIZE,
                have: r.remaining(),
            });
        }
        let from_offset = r.read_u64();
        let max_count = r.read_u32();
        Ok(Self {
            topic,
            from_offset,
            max_count,
        })
    }
}

/// Payload for [`MessageType::FetchReply`] (server → subscriber).
///
/// One replayed WAL entry: the server emits these in ascending offset
/// order in response to a [`FetchPayload`] request. `payload` borrows
/// from the underlying wire buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchReplyPayload<'a> {
    /// Topic name the entry belongs to.
    pub topic: &'a str,
    /// WAL-assigned offset of this entry.
    pub offset: u64,
    /// Nanoseconds since UNIX epoch when the entry was appended.
    pub timestamp_ns: u64,
    /// Opaque message payload.
    pub payload: &'a [u8],
}

/// Fixed tail of a FetchReply payload: `offset(8) + timestamp_ns(8) +
/// payload_len(4)`.
const FETCH_REPLY_TAIL_SIZE: usize = 20;

impl<'a> FetchReplyPayload<'a> {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        write_string(w, self.topic);
        w.write_u64(self.offset);
        w.write_u64(self.timestamp_ns);
        w.write_u32(self.payload.len() as u32);
        w.write_bytes(self.payload);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        string_wire_size(self.topic) + FETCH_REPLY_TAIL_SIZE + self.payload.len()
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader<'a>) -> Result<Self, PayloadError> {
        let topic = read_string(r)?;
        if r.remaining() < FETCH_REPLY_TAIL_SIZE {
            return Err(PayloadError::Truncated {
                needed: FETCH_REPLY_TAIL_SIZE,
                have: r.remaining(),
            });
        }
        let offset = r.read_u64();
        let timestamp_ns = r.read_u64();
        let payload_len = r.read_u32() as usize;
        if r.remaining() < payload_len {
            return Err(PayloadError::Truncated {
                needed: FETCH_REPLY_TAIL_SIZE + payload_len,
                have: FETCH_REPLY_TAIL_SIZE + r.remaining(),
            });
        }
        let payload = r.read_slice(payload_len);
        Ok(Self {
            topic,
            offset,
            timestamp_ns,
            payload,
        })
    }
}

// ── Resume / reliability payloads ───────────────────────────────────

/// One per-stream resume slot carried in a [`ResumePayload`].
///
/// Fixed 16-byte record: `stream_id(8) + last_acked(8)`. Owned (`Copy`)
/// rather than borrowed because it is a pair of integers parsed out of the
/// wire buffer — there is no variable-length field to borrow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResumeSlot {
    /// Logical stream to resume.
    pub stream_id: u64,
    /// Highest sequence the client has already accepted/acked for this
    /// stream. The server replays `last_acked + 1 .. newest`.
    pub last_acked: u64,
}

impl ResumeSlot {
    /// Fixed wire size: 16 bytes.
    pub const WIRE_SIZE: usize = 16;

    /// Encodes into `w`.
    #[inline]
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u64(self.stream_id);
        w.write_u64(self.last_acked);
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < Self::WIRE_SIZE {
            return Err(PayloadError::Truncated {
                needed: Self::WIRE_SIZE,
                have: r.remaining(),
            });
        }
        Ok(Self {
            stream_id: r.read_u64(),
            last_acked: r.read_u64(),
        })
    }
}

/// Payload for [`MessageType::Resume`] (client → server).
///
/// Carries a client-stable `logical_session_id` (generated once, survives
/// reconnects — distinct from the per-connection [`crate::header::SessionId`]
/// the server reassigns each connect) and a list of per-stream resume slots.
/// An empty slot list is a "session hello" that registers the logical id on
/// first connect so the server begins retaining replay state for it.
///
/// Wire layout: `logical_session_id(8) + count(1) + count × ResumeSlot(16)`.
///
/// Unlike most payload types this owns a `Vec` rather than borrowing, because
/// the variable count of fixed-size records does not map cleanly to a single
/// borrowed slice. Resume frames are rare (once per reconnect), so the
/// allocation is negligible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumePayload {
    /// Client-stable logical session identifier.
    pub logical_session_id: u64,
    /// Per-stream `(stream_id, last_acked)` resume slots. Empty on first
    /// connect (session hello).
    pub streams: Vec<ResumeSlot>,
}

/// Fixed header portion of a Resume payload: `logical_session_id(8) + count(1)`.
const RESUME_HEAD_SIZE: usize = 9;

impl ResumePayload {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u64(self.logical_session_id);
        // count is capped at u8::MAX — a single Resume can resume at most 255
        // streams, which is well above any realistic per-client stream count.
        let count = u8::try_from(self.streams.len()).unwrap_or(u8::MAX);
        w.write_u8(count);
        for s in &self.streams {
            s.encode(w);
        }
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        RESUME_HEAD_SIZE + self.streams.len().min(u8::MAX as usize) * ResumeSlot::WIRE_SIZE
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < RESUME_HEAD_SIZE {
            return Err(PayloadError::Truncated {
                needed: RESUME_HEAD_SIZE,
                have: r.remaining(),
            });
        }
        let logical_session_id = r.read_u64();
        let count = r.read_u8() as usize;
        let needed = count * ResumeSlot::WIRE_SIZE;
        if r.remaining() < needed {
            return Err(PayloadError::Truncated {
                needed: RESUME_HEAD_SIZE + needed,
                have: RESUME_HEAD_SIZE + r.remaining(),
            });
        }
        let mut streams = Vec::with_capacity(count);
        for _ in 0..count {
            streams.push(ResumeSlot::decode(r)?);
        }
        Ok(Self {
            logical_session_id,
            streams,
        })
    }
}

/// One per-stream result slot carried in [`ResumeOkPayload`] /
/// [`ResumeUnavailablePayload`].
///
/// Fixed 16-byte record: `stream_id(8) + oldest_available(8)`. In a
/// `ResumeOk` the `oldest_available` tells the client the window floor; in a
/// `ResumeUnavailable` it tells the client the earliest seq the server could
/// have replayed, so the client can decide its fallback strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResumeOkSlot {
    /// Logical stream this result refers to.
    pub stream_id: u64,
    /// Oldest sequence the server still has retained for this stream.
    pub oldest_available: u64,
}

impl ResumeOkSlot {
    /// Fixed wire size: 16 bytes (same layout as [`ResumeSlot`]).
    pub const WIRE_SIZE: usize = 16;

    /// Encodes into `w`.
    #[inline]
    pub fn encode(&self, w: &mut ByteWriter) {
        w.write_u64(self.stream_id);
        w.write_u64(self.oldest_available);
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < Self::WIRE_SIZE {
            return Err(PayloadError::Truncated {
                needed: Self::WIRE_SIZE,
                have: r.remaining(),
            });
        }
        Ok(Self {
            stream_id: r.read_u64(),
            oldest_available: r.read_u64(),
        })
    }
}

/// Payload for [`MessageType::ResumeOk`] (server → client).
///
/// One result slot per requested stream. The server follows this frame with
/// the actual replayed `Publish` frames for each accepted stream.
///
/// Wire layout: `count(1) + count × ResumeOkSlot(16)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeOkPayload {
    /// One `(stream_id, oldest_available)` per requested stream.
    pub streams: Vec<ResumeOkSlot>,
}

/// Fixed header portion: `count(1)`.
const RESUME_OK_HEAD_SIZE: usize = 1;

impl ResumeOkPayload {
    /// Encodes into `w`.
    pub fn encode(&self, w: &mut ByteWriter) {
        let count = u8::try_from(self.streams.len()).unwrap_or(u8::MAX);
        w.write_u8(count);
        for s in &self.streams {
            s.encode(w);
        }
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        RESUME_OK_HEAD_SIZE + self.streams.len().min(u8::MAX as usize) * ResumeOkSlot::WIRE_SIZE
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        if r.remaining() < RESUME_OK_HEAD_SIZE {
            return Err(PayloadError::Truncated {
                needed: RESUME_OK_HEAD_SIZE,
                have: r.remaining(),
            });
        }
        let count = r.read_u8() as usize;
        let needed = count * ResumeOkSlot::WIRE_SIZE;
        if r.remaining() < needed {
            return Err(PayloadError::Truncated {
                needed: RESUME_OK_HEAD_SIZE + needed,
                have: RESUME_OK_HEAD_SIZE + r.remaining(),
            });
        }
        let mut streams = Vec::with_capacity(count);
        for _ in 0..count {
            streams.push(ResumeOkSlot::decode(r)?);
        }
        Ok(Self { streams })
    }
}

/// Payload for [`MessageType::ResumeUnavailable`] (server → client).
///
/// Same wire shape as [`ResumeOkPayload`] — reports the oldest retained
/// sequence per stream so the client knows its resume request was below the
/// window floor and must fall back to a fresh subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeUnavailablePayload(pub Vec<ResumeOkSlot>);

impl ResumeUnavailablePayload {
    /// Encodes into `w` (delegates to the [`ResumeOkPayload`] shape).
    pub fn encode(&self, w: &mut ByteWriter) {
        ResumeOkPayload {
            streams: self.0.clone(),
        }
        .encode(w);
    }

    /// Exact wire size this value will occupy.
    #[inline]
    pub fn wire_size(&self) -> usize {
        RESUME_OK_HEAD_SIZE + self.0.len().min(u8::MAX as usize) * ResumeOkSlot::WIRE_SIZE
    }

    /// Decodes from `r`.
    pub fn decode(r: &mut ByteReader) -> Result<Self, PayloadError> {
        Ok(Self(ResumeOkPayload::decode(r)?.streams))
    }
}

// ── Top-level convenience functions ─────────────────────────────────

/// Parses a payload based on the frame's `msg_type`.
///
/// Returns `Ok(None)` for types that do not have a structured payload in this
/// module:
/// - All data-plane types except `Error`, `Heartbeat`, and the pub/sub
///   variants (TextToken, AudioFrame, VideoFrame, ImageFrame, ToolCall,
///   ToolResult, StateUpdate)
/// - `Fragment` (handled by [`FragmentReassembler`](crate::reassembly::FragmentReassembler))
///
/// # Errors
///
/// Returns [`PayloadError`] if the payload is truncated or contains invalid data.
pub fn parse_payload<'a>(
    msg_type: MessageType,
    payload: &'a [u8],
) -> Result<Option<ParsedPayload<'a>>, PayloadError> {
    let mut r = ByteReader::new(payload);
    let result = match msg_type {
        MessageType::StreamOpen => Some(ParsedPayload::StreamOpen(StreamOpen::decode(&mut r)?)),
        MessageType::StreamClose => Some(ParsedPayload::StreamClose(StreamClose::decode(&mut r)?)),
        MessageType::StreamReset => Some(ParsedPayload::StreamReset(StreamReset::decode(&mut r)?)),
        MessageType::Handshake => Some(ParsedPayload::Handshake(Handshake::decode(&mut r)?)),
        MessageType::Ack => Some(ParsedPayload::Ack(Ack::decode(&mut r)?)),
        MessageType::Error => Some(ParsedPayload::Error(ErrorPayload::decode(&mut r)?)),
        MessageType::Heartbeat => {
            // Heartbeat payload is empty — anything non-empty is ignored.
            Some(ParsedPayload::Heartbeat)
        }
        MessageType::Subscribe => Some(ParsedPayload::Subscribe(SubscribePayload::decode(&mut r)?)),
        MessageType::Unsubscribe => Some(ParsedPayload::Unsubscribe(UnsubscribePayload::decode(
            &mut r,
        )?)),
        MessageType::Publish => Some(ParsedPayload::Publish(PublishPayload::decode(&mut r)?)),
        MessageType::TopicCreate => Some(ParsedPayload::TopicCreate(TopicCreatePayload::decode(
            &mut r,
        )?)),
        MessageType::TopicDelete => Some(ParsedPayload::TopicDelete(TopicDeletePayload::decode(
            &mut r,
        )?)),
        MessageType::Fetch => Some(ParsedPayload::Fetch(FetchPayload::decode(&mut r)?)),
        MessageType::FetchReply => Some(ParsedPayload::FetchReply(FetchReplyPayload::decode(
            &mut r,
        )?)),
        MessageType::Resume => Some(ParsedPayload::Resume(ResumePayload::decode(&mut r)?)),
        MessageType::ResumeOk => Some(ParsedPayload::ResumeOk(ResumeOkPayload::decode(&mut r)?)),
        MessageType::ResumeUnavailable => Some(ParsedPayload::ResumeUnavailable(
            ResumeUnavailablePayload::decode(&mut r)?,
        )),
        // Data-plane types and Fragment have no structured payload here.
        _ => None,
    };
    Ok(result)
}

/// Computes the exact wire size for a [`ParsedPayload`].
#[must_use]
pub fn payload_wire_size(payload: &ParsedPayload<'_>) -> usize {
    match payload {
        ParsedPayload::StreamOpen(_) => StreamOpen::WIRE_SIZE,
        ParsedPayload::StreamClose(_) => StreamClose::WIRE_SIZE,
        ParsedPayload::Handshake(_) => Handshake::WIRE_SIZE,
        ParsedPayload::Ack(_) => Ack::WIRE_SIZE,
        ParsedPayload::StreamReset(sr) => sr.wire_size(),
        ParsedPayload::Error(e) => e.wire_size(),
        ParsedPayload::Heartbeat => 0,
        ParsedPayload::Subscribe(p) => p.wire_size(),
        ParsedPayload::Unsubscribe(p) => p.wire_size(),
        ParsedPayload::Publish(p) => p.wire_size(),
        ParsedPayload::TopicCreate(p) => p.wire_size(),
        ParsedPayload::TopicDelete(p) => p.wire_size(),
        ParsedPayload::Fetch(p) => p.wire_size(),
        ParsedPayload::FetchReply(p) => p.wire_size(),
        ParsedPayload::Resume(p) => p.wire_size(),
        ParsedPayload::ResumeOk(p) => p.wire_size(),
        ParsedPayload::ResumeUnavailable(p) => p.wire_size(),
    }
}

/// Encodes a [`ParsedPayload`] into a freshly allocated `Vec<u8>`.
#[must_use]
pub fn encode_payload(payload: &ParsedPayload<'_>) -> Vec<u8> {
    let size = payload_wire_size(payload);
    let mut buf = vec![0u8; size];
    let mut w = ByteWriter::new(&mut buf);
    match payload {
        ParsedPayload::StreamOpen(p) => p.encode(&mut w),
        ParsedPayload::StreamClose(p) => p.encode(&mut w),
        ParsedPayload::StreamReset(p) => p.encode(&mut w),
        ParsedPayload::Handshake(p) => p.encode(&mut w),
        ParsedPayload::Ack(p) => p.encode(&mut w),
        ParsedPayload::Error(p) => p.encode(&mut w),
        ParsedPayload::Heartbeat => {}
        ParsedPayload::Subscribe(p) => p.encode(&mut w),
        ParsedPayload::Unsubscribe(p) => p.encode(&mut w),
        ParsedPayload::Publish(p) => p.encode(&mut w),
        ParsedPayload::TopicCreate(p) => p.encode(&mut w),
        ParsedPayload::TopicDelete(p) => p.encode(&mut w),
        ParsedPayload::Fetch(p) => p.encode(&mut w),
        ParsedPayload::FetchReply(p) => p.encode(&mut w),
        ParsedPayload::Resume(p) => p.encode(&mut w),
        ParsedPayload::ResumeOk(p) => p.encode(&mut w),
        ParsedPayload::ResumeUnavailable(p) => p.encode(&mut w),
    }
    buf
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    // ── StreamDirection ─────────────────────────────────────────────

    #[test]
    fn stream_direction_round_trip() {
        assert_eq!(
            StreamDirection::from_byte(0).unwrap(),
            StreamDirection::Bidirectional
        );
        assert_eq!(
            StreamDirection::from_byte(1).unwrap(),
            StreamDirection::Unidirectional
        );
        assert_eq!(StreamDirection::Bidirectional.as_byte(), 0);
        assert_eq!(StreamDirection::Unidirectional.as_byte(), 1);
    }

    #[test]
    fn stream_direction_rejects_unknown_byte() {
        assert!(matches!(
            StreamDirection::from_byte(2),
            Err(PayloadError::InvalidDirection { value: 2 })
        ));
        assert!(matches!(
            StreamDirection::from_byte(0xFF),
            Err(PayloadError::InvalidDirection { value: 0xFF })
        ));
    }

    // ── StreamOpen ──────────────────────────────────────────────────

    #[test]
    fn stream_open_round_trip() {
        let open = StreamOpen {
            direction: StreamDirection::Bidirectional,
        };
        let mut buf = [0u8; StreamOpen::WIRE_SIZE];
        let mut w = ByteWriter::new(&mut buf);
        open.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = StreamOpen::decode(&mut r).unwrap();
        assert_eq!(decoded, open);
    }

    #[test]
    fn stream_open_truncated() {
        let r = ByteReader::new(&[]);
        let err = StreamOpen::decode(&mut ByteReader::new(&[])).unwrap_err();
        let _ = r; // suppress unused
        assert!(matches!(
            err,
            PayloadError::Truncated { needed: 1, have: 0 }
        ));
    }

    // ── StreamClose ─────────────────────────────────────────────────

    #[test]
    fn stream_close_round_trip() {
        let close = StreamClose { reason: 42 };
        let mut buf = [0u8; StreamClose::WIRE_SIZE];
        let mut w = ByteWriter::new(&mut buf);
        close.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = StreamClose::decode(&mut r).unwrap();
        assert_eq!(decoded, close);
    }

    #[test]
    fn stream_close_normal_constant() {
        assert_eq!(StreamClose::NORMAL, 0);
    }

    #[test]
    fn stream_close_truncated() {
        let err = StreamClose::decode(&mut ByteReader::new(&[0])).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated { needed: 2, have: 1 }
        ));
    }

    // ── StreamReset ─────────────────────────────────────────────────

    #[test]
    fn stream_reset_round_trip_with_reason() {
        let reset = StreamReset {
            error_code: 0x1234_5678,
            reason: "internal error",
        };
        let mut buf = vec![0u8; reset.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        reset.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = StreamReset::decode(&mut r).unwrap();
        assert_eq!(decoded.error_code, reset.error_code);
        assert_eq!(decoded.reason, reset.reason);
    }

    #[test]
    fn stream_reset_empty_reason() {
        let reset = StreamReset {
            error_code: 1,
            reason: "",
        };
        let mut buf = vec![0u8; reset.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        reset.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = StreamReset::decode(&mut r).unwrap();
        assert_eq!(decoded.reason, "");
        assert_eq!(decoded.error_code, 1);
    }

    #[test]
    fn stream_reset_truncated_header() {
        // Only 3 bytes but header needs 6.
        let err = StreamReset::decode(&mut ByteReader::new(&[0; 3])).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated { needed: 6, have: 3 }
        ));
    }

    #[test]
    fn stream_reset_truncated_reason() {
        // Header says 5 bytes of reason but only 2 are present.
        let mut buf = [0u8; 8]; // error_code(4) + reason_len(2) + 2 reason bytes
        let mut w = ByteWriter::new(&mut buf);
        w.write_u32(0);
        w.write_u16(5); // claims 5 bytes
        w.write_bytes(b"ab"); // only 2
        let err = StreamReset::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated {
                needed: 11, // 6 + 5
                have: 8     // 6 + 2
            }
        ));
    }

    #[test]
    fn stream_reset_invalid_utf8() {
        let mut buf = vec![0u8; 8];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u32(0);
        w.write_u16(2);
        w.write_bytes(&[0xFF, 0xFE]); // invalid UTF-8
        let err = StreamReset::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(err, PayloadError::InvalidUtf8 { .. }));
    }

    // ── Handshake ───────────────────────────────────────────────────

    #[test]
    fn handshake_round_trip() {
        let hs = Handshake {
            version: Version::V1,
            capabilities: 0xCAFE_F00D,
            principal_id: 42,
            credential_token: 0xDEAD_BEEF,
        };
        let mut buf = [0u8; Handshake::WIRE_SIZE];
        let mut w = ByteWriter::new(&mut buf);
        hs.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = Handshake::decode(&mut r).unwrap();
        assert_eq!(decoded, hs);
    }

    #[test]
    fn handshake_truncated() {
        let err = Handshake::decode(&mut ByteReader::new(&[0; 4])).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated {
                needed: 21,
                have: 4
            }
        ));
    }

    // ── Ack ─────────────────────────────────────────────────────────

    #[test]
    fn ack_round_trip() {
        let ack = Ack {
            ack_seq: 0xDEAD_BEEF_CAFE,
        };
        let mut buf = [0u8; Ack::WIRE_SIZE];
        let mut w = ByteWriter::new(&mut buf);
        ack.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = Ack::decode(&mut r).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn ack_truncated() {
        let err = Ack::decode(&mut ByteReader::new(&[0; 4])).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated { needed: 8, have: 4 }
        ));
    }

    // ── ErrorPayload ────────────────────────────────────────────────

    #[test]
    fn error_payload_round_trip_with_message() {
        let err = ErrorPayload {
            code: 500,
            message: "internal server error",
        };
        let mut buf = vec![0u8; err.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        err.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = ErrorPayload::decode(&mut r).unwrap();
        assert_eq!(decoded.code, err.code);
        assert_eq!(decoded.message, err.message);
    }

    #[test]
    fn error_payload_empty_message() {
        let err = ErrorPayload {
            code: 404,
            message: "",
        };
        let mut buf = vec![0u8; err.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        err.encode(&mut w);

        let mut r = ByteReader::new(&buf);
        let decoded = ErrorPayload::decode(&mut r).unwrap();
        assert_eq!(decoded.message, "");
        assert_eq!(decoded.code, 404);
    }

    #[test]
    fn error_payload_truncated_header() {
        let err = ErrorPayload::decode(&mut ByteReader::new(&[0; 5])).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated { needed: 6, have: 5 }
        ));
    }

    #[test]
    fn error_payload_invalid_utf8() {
        let mut buf = vec![0u8; 8];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u32(0);
        w.write_u16(2);
        w.write_bytes(&[0xFF, 0xFE]); // invalid UTF-8
        let err = ErrorPayload::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(err, PayloadError::InvalidUtf8 { .. }));
    }

    // ── parse_payload dispatcher ────────────────────────────────────

    #[test]
    fn parse_payload_dispatches_all_control_types() {
        // StreamOpen
        let mut buf = vec![0u8; StreamOpen::WIRE_SIZE];
        StreamOpen {
            direction: StreamDirection::Bidirectional,
        }
        .encode(&mut ByteWriter::new(&mut buf));
        assert!(matches!(
            parse_payload(MessageType::StreamOpen, &buf).unwrap(),
            Some(ParsedPayload::StreamOpen(_))
        ));

        // StreamClose
        let mut buf = vec![0u8; StreamClose::WIRE_SIZE];
        StreamClose { reason: 7 }.encode(&mut ByteWriter::new(&mut buf));
        assert!(matches!(
            parse_payload(MessageType::StreamClose, &buf).unwrap(),
            Some(ParsedPayload::StreamClose(StreamClose { reason: 7 }))
        ));

        // Handshake
        let mut buf = vec![0u8; Handshake::WIRE_SIZE];
        Handshake {
            version: Version::V1,
            capabilities: 1,
            principal_id: 0,
            credential_token: 0,
        }
        .encode(&mut ByteWriter::new(&mut buf));
        assert!(matches!(
            parse_payload(MessageType::Handshake, &buf).unwrap(),
            Some(ParsedPayload::Handshake(_))
        ));

        // Ack
        let mut buf = vec![0u8; Ack::WIRE_SIZE];
        Ack { ack_seq: 99 }.encode(&mut ByteWriter::new(&mut buf));
        assert!(matches!(
            parse_payload(MessageType::Ack, &buf).unwrap(),
            Some(ParsedPayload::Ack(Ack { ack_seq: 99 }))
        ));

        // Heartbeat — empty payload
        assert!(matches!(
            parse_payload(MessageType::Heartbeat, &[]).unwrap(),
            Some(ParsedPayload::Heartbeat)
        ));
    }

    #[test]
    fn parse_payload_returns_none_for_data_types() {
        // Data-plane types that are NOT Error/Heartbeat/pubsub return None.
        assert!(
            parse_payload(MessageType::TextToken, b"hello")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_payload(MessageType::AudioFrame, &[0; 128])
                .unwrap()
                .is_none()
        );
        assert!(
            parse_payload(MessageType::VideoFrame, &[0; 4])
                .unwrap()
                .is_none()
        );
        assert!(
            parse_payload(MessageType::ImageFrame, b"img")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_payload(MessageType::ToolCall, b"{}")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_payload(MessageType::ToolResult, b"{}")
                .unwrap()
                .is_none()
        );
        assert!(
            parse_payload(MessageType::StateUpdate, b"{}")
                .unwrap()
                .is_none()
        );
        // Fragment has no structured payload here either.
        assert!(
            parse_payload(MessageType::Fragment, b"raw")
                .unwrap()
                .is_none()
        );
    }

    // ── pub/sub payloads ──────────────────────────────────────────

    #[test]
    fn subscribe_round_trip() {
        let s = SubscribePayload {
            pattern: "sensor.*",
            qos: 1,
        };
        let mut buf = vec![0u8; s.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        s.encode(&mut w);

        let decoded = SubscribePayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.pattern, "sensor.*");
        assert_eq!(decoded.qos, 1);
        assert!(matches!(
            parse_payload(MessageType::Subscribe, &buf).unwrap(),
            Some(ParsedPayload::Subscribe(_))
        ));
    }

    #[test]
    fn subscribe_truncated_qos() {
        // pattern_len(2) + pattern(3) but no qos byte.
        let mut buf = vec![0u8; 5];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u16(3);
        w.write_bytes(b"abc");
        let err = SubscribePayload::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated { needed: 1, have: 0 }
        ));
    }

    #[test]
    fn unsubscribe_round_trip() {
        let u = UnsubscribePayload {
            pattern: "alerts.high.*",
        };
        let mut buf = vec![0u8; u.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        u.encode(&mut w);

        let decoded = UnsubscribePayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.pattern, "alerts.high.*");
    }

    #[test]
    fn publish_round_trip_with_payload() {
        let p = PublishPayload {
            topic: "chat.room.42",
            payload: b"hello, world",
        };
        let mut buf = vec![0u8; p.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        p.encode(&mut w);

        let decoded = PublishPayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.topic, "chat.room.42");
        assert_eq!(decoded.payload, b"hello, world");
    }

    #[test]
    fn publish_empty_payload() {
        let p = PublishPayload {
            topic: "t",
            payload: &[],
        };
        let mut buf = vec![0u8; p.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        p.encode(&mut w);

        let decoded = PublishPayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.topic, "t");
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn topic_create_round_trip() {
        let tc = TopicCreatePayload {
            topic: "metrics",
            partitions: 8,
            retention_ms: 86_400_000,
        };
        let mut buf = vec![0u8; tc.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        tc.encode(&mut w);

        let decoded = TopicCreatePayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.topic, "metrics");
        assert_eq!(decoded.partitions, 8);
        assert_eq!(decoded.retention_ms, 86_400_000);
    }

    #[test]
    fn topic_create_truncated_tail() {
        // topic_len(2) + topic(3) + only 4 of 12 tail bytes.
        let mut buf = vec![0u8; 9];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u16(3);
        w.write_bytes(b"abc");
        w.write_u32(7);
        let err = TopicCreatePayload::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated {
                needed: 12,
                have: 4
            }
        ));
    }

    #[test]
    fn topic_delete_round_trip() {
        let td = TopicDeletePayload { topic: "stale" };
        let mut buf = vec![0u8; td.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        td.encode(&mut w);

        let decoded = TopicDeletePayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.topic, "stale");
    }

    #[test]
    fn fetch_round_trip() {
        let f = FetchPayload {
            topic: "sensor.temp",
            from_offset: 42,
            max_count: 16,
        };
        let mut buf = vec![0u8; f.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        f.encode(&mut w);

        let decoded = FetchPayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.topic, "sensor.temp");
        assert_eq!(decoded.from_offset, 42);
        assert_eq!(decoded.max_count, 16);
    }

    #[test]
    fn fetch_truncated_tail() {
        // topic_len(2) + topic(3) + only 4 of 12 tail bytes.
        let mut buf = vec![0u8; 9];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u16(3);
        w.write_bytes(b"abc");
        w.write_u32(7);
        let err = FetchPayload::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated {
                needed: 12,
                have: 4
            }
        ));
    }

    #[test]
    fn fetch_reply_round_trip() {
        let fr = FetchReplyPayload {
            topic: "sensor.temp",
            offset: 99,
            timestamp_ns: 1_700_000_000_000_000,
            payload: &[0xDE, 0xAD, 0xBE, 0xEF],
        };
        let mut buf = vec![0u8; fr.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        fr.encode(&mut w);

        let decoded = FetchReplyPayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded.topic, "sensor.temp");
        assert_eq!(decoded.offset, 99);
        assert_eq!(decoded.timestamp_ns, 1_700_000_000_000_000);
        assert_eq!(decoded.payload, &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn fetch_reply_truncated_payload() {
        // topic_len(2) + topic(1) + tail(20) declares payload_len=10 but
        // the buffer ends right after the tail.
        let mut buf = vec![0u8; 2 + 1 + 20];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u16(1);
        w.write_bytes(b"x");
        w.write_u64(0); // offset
        w.write_u64(0); // timestamp_ns
        w.write_u32(10); // payload_len — but no payload bytes follow
        let err = FetchReplyPayload::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated {
                needed: 30,
                have: 20
            }
        ));
    }

    #[test]
    fn parse_payload_dispatches_all_pubsub_types() {
        // Subscribe
        let original = ParsedPayload::Subscribe(SubscribePayload {
            pattern: "p",
            qos: 2,
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::Subscribe, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);

        // Publish with opaque bytes
        let original = ParsedPayload::Publish(PublishPayload {
            topic: "t",
            payload: &[0xDE, 0xAD, 0xBE, 0xEF],
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::Publish, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);

        // TopicCreate
        let original = ParsedPayload::TopicCreate(TopicCreatePayload {
            topic: "x",
            partitions: 4,
            retention_ms: 1000,
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::TopicCreate, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);

        // Fetch
        let original = ParsedPayload::Fetch(FetchPayload {
            topic: "replay.topic",
            from_offset: 7,
            max_count: 32,
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::Fetch, &bytes).unwrap().unwrap();
        assert_eq!(parsed, original);

        // FetchReply
        let original = ParsedPayload::FetchReply(FetchReplyPayload {
            topic: "replay.topic",
            offset: 7,
            timestamp_ns: 1234,
            payload: &[1, 2, 3, 4],
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::FetchReply, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn pubsub_payload_with_invalid_utf8_rejected() {
        // pattern_len(2) + invalid-utf8 bytes.
        let mut buf = vec![0u8; 4];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u16(2);
        w.write_bytes(&[0xFF, 0xFE]);
        let err = SubscribePayload::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(err, PayloadError::InvalidUtf8 { .. }));
    }

    #[test]
    fn parse_payload_round_trip_via_encode_payload() {
        // Encode then parse every variant.
        let original = ParsedPayload::StreamOpen(StreamOpen {
            direction: StreamDirection::Unidirectional,
        });
        let bytes = encode_payload(&original);
        assert_eq!(bytes.len(), payload_wire_size(&original));
        let parsed = parse_payload(MessageType::StreamOpen, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);

        let original = ParsedPayload::Error(ErrorPayload {
            code: 418,
            message: "I'm a teapot",
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::Error, &bytes).unwrap().unwrap();
        assert_eq!(parsed, original);

        let original = ParsedPayload::Heartbeat;
        let bytes = encode_payload(&original);
        assert!(bytes.is_empty());
        let parsed = parse_payload(MessageType::Heartbeat, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);
    }

    // ── Resume / reliability payloads ──────────────────────────────

    #[test]
    fn resume_slot_round_trip() {
        let slot = ResumeSlot {
            stream_id: 42,
            last_acked: 99,
        };
        let mut buf = [0u8; ResumeSlot::WIRE_SIZE];
        let mut w = ByteWriter::new(&mut buf);
        slot.encode(&mut w);
        let decoded = ResumeSlot::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded, slot);
    }

    #[test]
    fn resume_payload_round_trip_with_slots() {
        let rp = ResumePayload {
            logical_session_id: 0xDEAD_BEEF,
            streams: vec![
                ResumeSlot {
                    stream_id: 1,
                    last_acked: 100,
                },
                ResumeSlot {
                    stream_id: 2,
                    last_acked: 200,
                },
            ],
        };
        let mut buf = vec![0u8; rp.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        rp.encode(&mut w);
        let decoded = ResumePayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded, rp);
        // count = 2 → head(9) + 2×16 = 41
        assert_eq!(rp.wire_size(), 9 + 32);
    }

    #[test]
    fn resume_payload_empty_slots_is_session_hello() {
        let rp = ResumePayload {
            logical_session_id: 7,
            streams: vec![],
        };
        let mut buf = vec![0u8; rp.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        rp.encode(&mut w);
        assert_eq!(buf.len(), 9); // head only, no slots
        let decoded = ResumePayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded, rp);
    }

    #[test]
    fn resume_payload_truncated_head() {
        let err = ResumePayload::decode(&mut ByteReader::new(&[0; 4])).unwrap_err();
        assert!(matches!(err, PayloadError::Truncated { needed: 9, have: 4 }));
    }

    #[test]
    fn resume_payload_truncated_slots() {
        // head(9) + count=1 but no slot bytes.
        let mut buf = vec![0u8; 9];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u64(7);
        w.write_u8(1); // claims 1 slot
        let err = ResumePayload::decode(&mut ByteReader::new(&buf)).unwrap_err();
        assert!(matches!(
            err,
            PayloadError::Truncated { needed, have } if needed == 9 + 16 && have == 9
        ));
    }

    #[test]
    fn resume_ok_payload_round_trip() {
        let ok = ResumeOkPayload {
            streams: vec![
                ResumeOkSlot {
                    stream_id: 1,
                    oldest_available: 101,
                },
                ResumeOkSlot {
                    stream_id: 2,
                    oldest_available: 0,
                },
            ],
        };
        let mut buf = vec![0u8; ok.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        ok.encode(&mut w);
        let decoded = ResumeOkPayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded, ok);
        assert_eq!(ok.wire_size(), 1 + 32);
    }

    #[test]
    fn resume_unavailable_payload_round_trip() {
        let un = ResumeUnavailablePayload(vec![ResumeOkSlot {
            stream_id: 9,
            oldest_available: 500,
        }]);
        let mut buf = vec![0u8; un.wire_size()];
        let mut w = ByteWriter::new(&mut buf);
        un.encode(&mut w);
        let decoded = ResumeUnavailablePayload::decode(&mut ByteReader::new(&buf)).unwrap();
        assert_eq!(decoded, un);
    }

    #[test]
    fn parse_payload_dispatches_resume_types() {
        let original = ParsedPayload::Resume(ResumePayload {
            logical_session_id: 42,
            streams: vec![ResumeSlot {
                stream_id: 7,
                last_acked: 3,
            }],
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::Resume, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);

        let original = ParsedPayload::ResumeOk(ResumeOkPayload {
            streams: vec![ResumeOkSlot {
                stream_id: 7,
                oldest_available: 4,
            }],
        });
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::ResumeOk, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);

        let original =
            ParsedPayload::ResumeUnavailable(ResumeUnavailablePayload(vec![ResumeOkSlot {
                stream_id: 7,
                oldest_available: 500,
            }]));
        let bytes = encode_payload(&original);
        let parsed = parse_payload(MessageType::ResumeUnavailable, &bytes)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, original);
    }

    // ── ByteReader/ByteWriter u16 round-trip ────────────────────────

    #[test]
    fn u16_round_trip() {
        let values = [0u16, 1, 0xFF, 0x0100, 0xCAFE, 0xFFFF];
        for &v in &values {
            let mut buf = [0u8; 2];
            let mut w = ByteWriter::new(&mut buf);
            w.write_u16(v);
            let mut r = ByteReader::new(&buf);
            assert_eq!(r.read_u16(), v);
        }
    }

    #[test]
    fn u16_writes_big_endian() {
        let mut buf = [0u8; 2];
        let mut w = ByteWriter::new(&mut buf);
        w.write_u16(0xCAFE);
        assert_eq!(buf, [0xCA, 0xFE]);
    }
}
