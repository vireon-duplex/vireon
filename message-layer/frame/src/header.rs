//! Wire-format constants and header-field types for the frame layer.
//!
//! Every frame on the wire is laid out as a 40-byte fixed header followed by a
//! variable-length payload. All multi-byte integers are **big-endian**
//! (network byte order).
//!
//! ```text
//!   offset  0  1  version     (u8,  see Version)
//!   offset  1  1  msg_type    (u8,  see MessageType)
//!   offset  2  1  flags       (u8,  see FrameFlags)
//!   offset  3  1  priority    (u8,  see Priority)
//!   offset  4  8  session_id  (u64, see SessionId)
//!   offset 12  8  stream_id   (u64, see StreamId)
//!   offset 20  8  seq         (u64, see Seq)
//!   offset 28  8  timestamp   (u64, see Timestamp)
//!   offset 36  4  payload_len (u32)
//!   offset 40     payload     (payload_len bytes)
//! ```
//!
//! The leading four 1-byte fields form a control quadlet that receivers can
//! inspect to dispatch (type), gate processing (priority / flags) and validate
//! protocol compatibility (version) before touching the larger 64-bit
//! identifiers. `seq` gives each frame a per-stream monotonically-increasing
//! sequence number so the receiver can detect gaps and reorder; `timestamp`
//! carries a sender-side clock sample for jitter / RTT estimation.
//!
//! See [`FrameHeader`] for the typed bundle, and [`encode_frame`] / [`decode_frame`]
//! for whole-frame (header + payload) helpers.
use crate::byte_reader::ByteReader;
use crate::byte_writer::ByteWriter;
use crate::error::FrameError;
use std::time::{SystemTime, UNIX_EPOCH};

/// Fixed header size in bytes: `4 × u8 + 4 × u64 + u32 = 40`.
pub const HEADER_SIZE: usize = 40;

/// Maximum payload length expressible by the `u32` wire field.
pub const MAX_PAYLOAD_LEN: usize = u32::MAX as usize; // ~4 GiB − 1

// ---------------------------------------------------------------------------
// Big-endian read helpers (avoid try_into + unwrap to satisfy clippy denies)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[inline]
fn read_be_u64(src: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&src[at..at + 8]);
    u64::from_be_bytes(b)
}

#[cfg(test)]
#[inline]
fn read_be_u32(src: &[u8], at: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&src[at..at + 4]);
    u32::from_be_bytes(b)
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

/// Wire protocol version carried in byte 0 of every frame header.
///
/// Mismatches between the sender's and receiver's [`Version`] are fatal for the
/// connection: a receiver that does not recognise the version must not attempt
/// to interpret the rest of the header. [`Version::CURRENT`] tracks the version
/// this build emits.
///
/// Note: `Version` intentionally does **not** implement `Default` — version `0`
/// is reserved, so there is no zero value that is also a valid default. Always
/// construct a `Version` explicitly or via [`Version::CURRENT`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Version(pub u8);

impl Version {
    /// Version 1 — the only value this build currently understands.
    pub const V1: Self = Self(1);

    /// The version emitted by [`encode_frame`] / [`FrameHeader::encode`].
    pub const CURRENT: Self = Self::V1;

    /// Constructs a `Version` from a raw byte.
    #[inline]
    pub const fn new(v: u8) -> Self {
        Self(v)
    }

    /// Returns the raw byte.
    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// `true` when this version equals [`CURRENT`](Self::CURRENT).
    #[inline]
    pub const fn is_current(self) -> bool {
        self.0 == Self::CURRENT.0
    }
}

impl From<u8> for Version {
    #[inline]
    fn from(v: u8) -> Self {
        Self(v)
    }
}

impl From<Version> for u8 {
    #[inline]
    fn from(v: Version) -> u8 {
        v.0
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Priority
// ---------------------------------------------------------------------------

/// Per-frame scheduling priority carried in byte 3 of the header.
///
/// Higher numerical value means higher priority — frames with a larger
/// `Priority` should be processed / sent ahead of lower-priority frames on the
/// same stream. The value is deliberately a full byte so callers can map any
/// external scheme (CoS, DSCP, application levels) onto it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Priority(pub u8);

impl Priority {
    /// Lowest possible priority.
    pub const MIN: Self = Self(0);
    /// Background / bulk traffic (e.g. large file transfer).
    pub const LOW: Self = Self(64);
    /// Default priority for ordinary data frames.
    pub const NORMAL: Self = Self(128);
    /// Latency-sensitive frames (e.g. control / acknowledgements).
    pub const HIGH: Self = Self(192);
    /// Highest possible priority — process before everything else.
    pub const MAX: Self = Self(255);

    /// Constructs a `Priority` from a raw byte.
    #[inline]
    pub const fn new(p: u8) -> Self {
        Self(p)
    }

    /// Returns the raw byte.
    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl From<u8> for Priority {
    #[inline]
    fn from(p: u8) -> Self {
        Self(p)
    }
}

impl From<Priority> for u8 {
    #[inline]
    fn from(p: Priority) -> u8 {
        p.0
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "prio:{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// SessionId
// ---------------------------------------------------------------------------

/// A session identifier carried in bytes 4..12 of every frame header.
///
/// A *session* groups one or more [`StreamId`]s that share a common security
/// context and lifetime — typically one QUIC connection, one authenticated
/// user, or one multiplexed tenant. Splitting session from stream lets a
/// demultiplexer route by session before consulting any stream-scoped state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SessionId(pub u64);

impl SessionId {
    /// The zero session identifier.
    pub const ZERO: Self = Self(0);

    /// Constructs a `SessionId` from a raw `u64`.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw `u64`.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for SessionId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<SessionId> for u64 {
    #[inline]
    fn from(v: SessionId) -> u64 {
        v.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "session:{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// StreamId
// ---------------------------------------------------------------------------

/// A logical stream identifier carried in bytes 12..20 of every frame header.
///
/// `StreamId` is an *application-layer* concept — it multiplexes independent
/// logical channels inside a single [`SessionId`]. It is decoupled from QUIC
/// transport stream IDs so the application can also encode channel or
/// sub-stream information in the same 64 bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct StreamId(pub u64);

impl StreamId {
    /// The zero stream identifier.
    pub const ZERO: Self = Self(0);

    /// Constructs a `StreamId` from a raw `u64`.
    #[inline]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw `u64`.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for StreamId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<StreamId> for u64 {
    #[inline]
    fn from(v: StreamId) -> u64 {
        v.0
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream:{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Seq — per-stream sequence number
// ---------------------------------------------------------------------------

/// A per-stream, monotonically-increasing sequence number (bytes 20..28).
///
/// Senders **must** increment `seq` for every frame sent on a given
/// [`StreamId`]. Receivers use it to:
///
/// * detect gaps (missing frames → request retransmit or skip),
/// * reorder frames when the transport does not guarantee in-order delivery
///   (QUIC datagrams, or when multiple logical streams share one transport
///   stream),
/// * reassemble [`MessageType::Fragment`] chains in the right order.
///
/// `u64` gives ~584 years at 1 M frames/s per stream before wrap — effectively
/// never. [`Seq::next`] uses `wrapping_add` so an overflow is defined behaviour
/// rather than a panic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Seq(pub u64);

impl Seq {
    /// The zero sequence number (typically the first frame on a stream).
    pub const ZERO: Self = Self(0);

    /// Constructs a `Seq` from a raw `u64`.
    #[inline]
    pub const fn new(n: u64) -> Self {
        Self(n)
    }

    /// Returns the raw `u64`.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the sequence number that follows `self` (wrapping on overflow).
    #[inline]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    /// Distance from `earlier` to `self` (`self − earlier`), saturating at 0
    /// when `earlier > self`. Useful for jitter-buffer sizing.
    #[inline]
    pub const fn gap_from(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl From<u64> for Seq {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<Seq> for u64 {
    #[inline]
    fn from(v: Seq) -> u64 {
        v.0
    }
}

impl std::fmt::Display for Seq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq:{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Timestamp — sender-side clock sample (bytes 28..36)
// ---------------------------------------------------------------------------

/// A sender-side clock sample stored as nanoseconds since the Unix epoch.
///
/// `Timestamp` is written verbatim by the sender and **not** normalised by the
/// receiver: two endpoints need not share a synchronised clock, so the value
/// is most useful as a relative measure (frame-to-frame delta, or RTT
/// estimation against a locally captured send time). Use [`Timestamp::now`]
/// for the common case of stamping a frame at enqueue time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// The zero timestamp (e.g. for frames that carry no clock sample).
    pub const ZERO: Self = Self(0);

    /// Constructs a `Timestamp` from a raw `u64` (nanoseconds since epoch).
    #[inline]
    pub const fn new(ns: u64) -> Self {
        Self(ns)
    }

    /// Constructs a `Timestamp` from a millisecond count since epoch.
    #[inline]
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms.saturating_mul(1_000_000))
    }

    /// Constructs a `Timestamp` from a microsecond count since epoch.
    #[inline]
    pub const fn from_micros(us: u64) -> Self {
        Self(us.saturating_mul(1_000))
    }

    /// Constructs a `Timestamp` from a nanosecond count since epoch.
    #[inline]
    pub const fn from_nanos(ns: u64) -> Self {
        Self(ns)
    }

    /// Returns the raw `u64` (nanoseconds since epoch).
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Captures the current wall-clock time as nanoseconds since the Unix
    /// epoch. Returns [`Timestamp::ZERO`] if the system clock is set before the
    /// epoch (defined behaviour rather than a panic).
    #[inline]
    pub fn now() -> Self {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => Self(d.as_nanos() as u64),
            Err(_) => Self::ZERO,
        }
    }
}

impl From<u64> for Timestamp {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<Timestamp> for u64 {
    #[inline]
    fn from(v: Timestamp) -> u64 {
        v.0
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "t:{}ns", self.0)
    }
}

// ---------------------------------------------------------------------------
// FrameFlags — per-frame option bits (byte 2)
// ---------------------------------------------------------------------------

/// Per-frame option bits carried in byte 2 of the header.
///
/// Wrapped in a transparent newtype rather than pulling in the `bitflags` crate.
/// Supports `|` and `|=` for ergonomic composition.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct FrameFlags(pub u8);

impl FrameFlags {
    /// No flags set.
    pub const NONE: Self = Self(0);

    /// Final frame of a logical stream (graceful close signal — pairs with
    /// [`MessageType::StreamClose`]).
    pub const FIN: Self = Self(1 << 0);

    /// Payload is compressed (e.g. LZ4, zstd). The application is responsible
    /// for decompression before interpreting the payload.
    pub const COMPRESSED: Self = Self(1 << 1);

    /// This frame is a fragment of a larger message; use [`Seq`] to reassemble.
    pub const FRAGMENT: Self = Self(1 << 2);

    /// High-priority frame — process before queued data of the same stream.
    pub const URGENT: Self = Self(1 << 3);

    /// Sender requests an app-level acknowledgment ([`MessageType::Ack`]).
    pub const ACK_REQ: Self = Self(1 << 4);

    /// Constructs from raw bits.
    #[inline]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns the raw bit pattern.
    #[inline]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// `true` when `self` contains **all** bits in `other`.
    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// `true` when no bits are set.
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Sets the bits in `other` (in-place `|=`).
    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    /// Clears the bits in `other`.
    #[inline]
    pub fn remove(&mut self, other: Self) {
        self.0 &= !other.0;
    }
}

impl From<u8> for FrameFlags {
    #[inline]
    fn from(bits: u8) -> Self {
        Self(bits)
    }
}

impl From<FrameFlags> for u8 {
    #[inline]
    fn from(f: FrameFlags) -> u8 {
        f.0
    }
}

impl std::ops::BitOr for FrameFlags {
    type Output = Self;

    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for FrameFlags {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl std::ops::BitAnd for FrameFlags {
    type Output = Self;

    #[inline]
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}

// ---------------------------------------------------------------------------
// MessageType (byte 1)
// ---------------------------------------------------------------------------

/// The message discriminator carried in byte 1 of every frame header.
///
/// Values `0x01`–`0x09` are **data-plane** types — they carry an opaque payload
/// whose semantics the application defines. Values `0x0A`–`0x0F` are
/// **control-plane** types that the frame layer itself understands: they drive
/// multi-stream lifecycle, fragmentation, handshake, and app-level reliability.
/// Values `0x10`–`0x14` are **pub/sub** data-plane types — they carry a
/// structured payload (see `frame::payload`) that the application dispatches
/// to the pub/sub engine. Values `0x15`–`0x16` extend the pub/sub plane with
/// replay-by-offset semantics: `Fetch` requests missed entries from a topic's
/// WAL; `FetchReply` returns one entry per frame.
///
/// The `u8` representation leaves `0x00` and `0x1B`–`0xFF` reserved for future
/// expansion; unknown bytes surface as [`FrameError::UnknownMessageType`] rather
/// than silent corruption.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MessageType {
    // ---- data plane ----
    /// A piece of text — chat token, log line, JSON fragment.
    TextToken = 0x01,
    /// An encoded audio chunk (e.g. opus / PCM frame).
    AudioFrame = 0x02,
    /// An encoded video chunk (e.g. H.264 / VP9 NAL unit).
    VideoFrame = 0x03,
    /// A still image or thumbnail.
    ImageFrame = 0x04,
    /// A request to invoke a tool (function call).
    ToolCall = 0x05,
    /// The result of a preceding [`ToolCall`].
    ToolResult = 0x06,
    /// A state mutation (presence, config, cursor position, …).
    StateUpdate = 0x07,
    /// An error reported by the peer.
    Error = 0x08,
    /// Liveness keep-alive; payload is typically empty.
    Heartbeat = 0x09,

    // ---- control plane (multi-stream) ----
    /// Opens a new logical stream; payload carries stream metadata.
    StreamOpen = 0x0A,
    /// Gracefully closes a logical stream; payload is typically empty.
    StreamClose = 0x0B,
    /// Abruptly resets a logical stream; payload carries a reason code.
    StreamReset = 0x0C,
    /// Protocol handshake / version negotiation.
    Handshake = 0x0D,
    /// Continuation fragment of a message that spans multiple frames.
    Fragment = 0x0E,
    /// Acknowledges receipt of a previous frame (app-level reliability).
    Ack = 0x0F,

    // ---- pub/sub data plane (application-dispatched, structured payload) ----
    /// Subscribe to a topic pattern; payload is `Subscribe`.
    Subscribe = 0x10,
    /// Remove a previously-registered subscription.
    Unsubscribe = 0x11,
    /// Publish an opaque payload to a topic.
    Publish = 0x12,
    /// Create a topic with retention / partition config.
    TopicCreate = 0x13,
    /// Delete a topic.
    TopicDelete = 0x14,
    /// Subscriber → server: replay WAL entries for a topic starting at
    /// `from_offset`. Payload is [`crate::payload::FetchPayload`].
    Fetch = 0x15,
    /// Server → subscriber: one replayed WAL entry. Payload is
    /// [`crate::payload::FetchReplyPayload`]. A single `Fetch` request
    /// yields zero or more `FetchReply` frames (bounded by the
    /// request's `max_count`).
    FetchReply = 0x16,

    // ---- consumer-group control plane ----
    /// Consumer → server: join a consumer group for a topic. Payload is
    /// `topic_len(u16) | topic | group_len(u16) | group_id | consumer_len(u16)
    /// | consumer_id | partitions(u32)`. The server assigns partitions
    /// and returns the generation in an ACK.
    ConsumerGroupJoin = 0x17,
    /// Consumer → server: leave a consumer group. Payload is
    /// `topic_len(u16) | topic | group_len(u16) | group_id | consumer_len(u16)
    /// | consumer_id`.
    ConsumerGroupLeave = 0x18,
    /// Consumer → server: heartbeat for a consumer group membership.
    /// Payload is `topic_len(u16) | topic | group_len(u16) | group_id
    /// | consumer_len(u16) | consumer_id | generation(u32)`.
    ConsumerGroupHeartbeat = 0x19,

    // ---- adaptive delivery (high-fanout optimization) ----
    /// Server → subscriber: a new message is available at WAL `offset`
    /// for `topic`. Lightweight alternative to forwarding the full
    /// payload when fan-out is high — the subscriber auto-fetches via
    /// [`MessageType::Fetch`]. Payload is `topic_len(u16) | topic |
    /// offset(u64)`.
    NotifyOffset = 0x1A,

    // ---- application-level reliability (resume / replay) ----
    /// Client → server: resume a logical session from a per-stream
    /// `last_acked` sequence. Carries a client-stable `logical_session_id`
    /// (distinct from the per-connection [`SessionId`]) plus a list of
    /// `(stream_id, last_acked)` slots. An empty slot list is a "session
    /// hello" that registers the logical id on first connect so the server
    /// can retain replay state for it. Payload is
    /// [`crate::payload::ResumePayload`].
    Resume = 0x1B,
    /// Server → client: resume accepted. Reports the oldest retained
    /// sequence per requested stream so the client can detect whether the
    /// replay starts exactly at `last_acked + 1` or was clamped up to the
    /// window floor. Payload is [`crate::payload::ResumeOkPayload`].
    /// The server follows this frame with the actual replayed `Publish`
    /// frames.
    ResumeOk = 0x1C,
    /// Server → client: resume rejected because at least one requested
    /// stream's `last_acked` is older than the server's oldest retained
    /// sequence (replay window exceeded). The client falls back to a
    /// fresh subscription. Payload is
    /// [`crate::payload::ResumeUnavailablePayload`] (same shape as
    /// [`crate::payload::ResumeOkPayload`]).
    ResumeUnavailable = 0x1D,
}

/// Every defined [`MessageType`], ascending by wire byte.
///
/// Useful for exhaustive round-trip tests and for dispatchers that want to
/// validate they handle every variant.
pub const ALL_MESSAGE_TYPES: [MessageType; 29] = [
    MessageType::TextToken,
    MessageType::AudioFrame,
    MessageType::VideoFrame,
    MessageType::ImageFrame,
    MessageType::ToolCall,
    MessageType::ToolResult,
    MessageType::StateUpdate,
    MessageType::Error,
    MessageType::Heartbeat,
    MessageType::StreamOpen,
    MessageType::StreamClose,
    MessageType::StreamReset,
    MessageType::Handshake,
    MessageType::Fragment,
    MessageType::Ack,
    MessageType::Subscribe,
    MessageType::Unsubscribe,
    MessageType::Publish,
    MessageType::TopicCreate,
    MessageType::TopicDelete,
    MessageType::Fetch,
    MessageType::FetchReply,
    MessageType::ConsumerGroupJoin,
    MessageType::ConsumerGroupLeave,
    MessageType::ConsumerGroupHeartbeat,
    MessageType::NotifyOffset,
    MessageType::Resume,
    MessageType::ResumeOk,
    MessageType::ResumeUnavailable,
];

impl MessageType {
    /// Returns the wire byte for this variant.
    #[inline]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Decodes a wire byte. Returns `None` for reserved / unknown values.
    #[inline]
    pub const fn from_byte(b: u8) -> Option<Self> {
        Some(match b {
            0x01 => Self::TextToken,
            0x02 => Self::AudioFrame,
            0x03 => Self::VideoFrame,
            0x04 => Self::ImageFrame,
            0x05 => Self::ToolCall,
            0x06 => Self::ToolResult,
            0x07 => Self::StateUpdate,
            0x08 => Self::Error,
            0x09 => Self::Heartbeat,
            0x0A => Self::StreamOpen,
            0x0B => Self::StreamClose,
            0x0C => Self::StreamReset,
            0x0D => Self::Handshake,
            0x0E => Self::Fragment,
            0x0F => Self::Ack,
            0x10 => Self::Subscribe,
            0x11 => Self::Unsubscribe,
            0x12 => Self::Publish,
            0x13 => Self::TopicCreate,
            0x14 => Self::TopicDelete,
            0x15 => Self::Fetch,
            0x16 => Self::FetchReply,
            0x17 => Self::ConsumerGroupJoin,
            0x18 => Self::ConsumerGroupLeave,
            0x19 => Self::ConsumerGroupHeartbeat,
            0x1A => Self::NotifyOffset,
            0x1B => Self::Resume,
            0x1C => Self::ResumeOk,
            0x1D => Self::ResumeUnavailable,
            _ => return None,
        })
    }

    /// `true` when this variant carries application data (the data plane).
    ///
    /// Includes the pub/sub data-plane types (`Subscribe`..`TopicDelete`) —
    /// they are dispatched to the application layer just like other data
    /// frames, even though their payload is structured (parsed by the frame
    /// layer rather than the application).
    #[inline]
    pub const fn is_data(self) -> bool {
        matches!(
            self,
            Self::TextToken
                | Self::AudioFrame
                | Self::VideoFrame
                | Self::ImageFrame
                | Self::ToolCall
                | Self::ToolResult
                | Self::StateUpdate
                | Self::Error
                | Self::Heartbeat
                | Self::Subscribe
                | Self::Unsubscribe
                | Self::Publish
                | Self::TopicCreate
                | Self::TopicDelete
                | Self::Fetch
                | Self::FetchReply
                | Self::ConsumerGroupJoin
                | Self::ConsumerGroupLeave
                | Self::ConsumerGroupHeartbeat
                | Self::NotifyOffset
        )
    }

    /// `true` when this variant is one of the pub/sub data-plane types
    /// (`Subscribe`..`FetchReply`). These carry structured payloads parsed
    /// by the frame layer and are routed to the pub/sub engine by the
    /// application dispatcher.
    #[inline]
    pub const fn is_pubsub(self) -> bool {
        matches!(
            self,
            Self::Subscribe
                | Self::Unsubscribe
                | Self::Publish
                | Self::TopicCreate
                | Self::TopicDelete
                | Self::Fetch
                | Self::FetchReply
                | Self::ConsumerGroupJoin
                | Self::ConsumerGroupLeave
                | Self::ConsumerGroupHeartbeat
                | Self::NotifyOffset
        )
    }

    /// `true` when this variant belongs to the control plane.
    #[inline]
    pub const fn is_control(self) -> bool {
        !self.is_data()
    }
}

impl TryFrom<u8> for MessageType {
    type Error = FrameError;

    #[inline]
    fn try_from(b: u8) -> Result<Self, FrameError> {
        Self::from_byte(b).ok_or(FrameError::UnknownMessageType { value: b })
    }
}

// ---------------------------------------------------------------------------
// FrameHeader — bundle of all header fields (no payload)
// ---------------------------------------------------------------------------

/// All fixed-header fields in one `Copy` value.
///
/// On the wire this is exactly [`HEADER_SIZE`] bytes. The `payload_len` field
/// describes how many payload bytes follow the header; it is kept in the struct
/// (rather than derived) so that a header decoded in isolation still knows the
/// size of the frame body it describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FrameHeader {
    /// Protocol version — receivers reject unknown versions.
    pub version: Version,
    /// Discriminator telling the application how to interpret the payload.
    pub msg_type: MessageType,
    /// Per-frame option bits.
    pub flags: FrameFlags,
    /// Scheduling priority (higher = more important).
    pub priority: Priority,
    /// Session this frame belongs to.
    pub session_id: SessionId,
    /// Logical stream within the session.
    pub stream_id: StreamId,
    /// Per-stream sequence number for ordering / gap detection.
    pub seq: Seq,
    /// Sender-side clock sample (nanoseconds since epoch).
    pub timestamp: Timestamp,
    /// Number of payload bytes that immediately follow this header.
    pub payload_len: u32,
}

impl FrameHeader {
    /// Convenience constructor with sensible defaults:
    /// `version = CURRENT`, `flags = NONE`, `priority = NORMAL`,
    /// `session_id = ZERO`, `seq = ZERO`, `timestamp = ZERO`,
    /// `payload_len = 0`.
    #[inline]
    pub const fn new(session_id: SessionId, stream_id: StreamId, msg_type: MessageType) -> Self {
        Self {
            version: Version::CURRENT,
            msg_type,
            flags: FrameFlags::NONE,
            priority: Priority::NORMAL,
            session_id,
            stream_id,
            seq: Seq::ZERO,
            timestamp: Timestamp::ZERO,
            payload_len: 0,
        }
    }

    /// Builder: set the protocol version.
    #[inline]
    pub const fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Builder: set the flags.
    #[inline]
    pub const fn with_flags(mut self, flags: FrameFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Builder: set the priority.
    #[inline]
    pub const fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Builder: set the sequence number.
    #[inline]
    pub const fn with_seq(mut self, seq: Seq) -> Self {
        self.seq = seq;
        self
    }

    /// Builder: set the timestamp.
    #[inline]
    pub const fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Builder: set the declared payload length.
    #[inline]
    pub const fn with_payload_len(mut self, payload_len: u32) -> Self {
        self.payload_len = payload_len;
        self
    }

    /// Writes the 40-byte fixed header into `dst` (big-endian).
    ///
    /// Returns [`FrameError::IncompleteHeader`] when `dst` is shorter than
    /// [`HEADER_SIZE`].
    #[inline]
    pub fn encode(&self, dst: &mut [u8]) -> Result<(), FrameError> {
        if dst.len() < HEADER_SIZE {
            return Err(FrameError::IncompleteHeader {
                needed: HEADER_SIZE,
                have: dst.len(),
            });
        }

        let mut w = ByteWriter::new(dst);

        w.write_u8(self.version.0);
        w.write_u8(self.msg_type.as_byte());
        w.write_u8(self.flags.bits());
        w.write_u8(self.priority.0);

        w.write_u64(self.session_id.0);
        w.write_u64(self.stream_id.0);
        w.write_u64(self.seq.0);
        w.write_u64(self.timestamp.0);

        w.write_u32(self.payload_len);
        Ok(())
    }

    #[inline(always)]
    pub fn encode_into(&self, w: &mut ByteWriter) {
        w.write_u8(self.version.0);
        w.write_u8(self.msg_type.as_byte());
        w.write_u8(self.flags.bits());
        w.write_u8(self.priority.0);
        w.write_u64(self.session_id.0);
        w.write_u64(self.stream_id.0);
        w.write_u64(self.seq.0);
        w.write_u64(self.timestamp.0);
        w.write_u32(self.payload_len);
    }

    /// Reads the 40-byte fixed header from `src` (big-endian).
    ///
    /// Returns [`FrameError::IncompleteHeader`] when `src` is shorter than
    /// [`HEADER_SIZE`], or [`FrameError::UnknownMessageType`] when byte 1 does
    /// not map to a known [`MessageType`].
    #[inline]
    pub fn decode(src: &[u8]) -> Result<Self, FrameError> {
        if src.len() < HEADER_SIZE {
            return Err(FrameError::IncompleteHeader {
                needed: HEADER_SIZE,
                have: src.len(),
            });
        }

        let mut r = ByteReader::new(src);

        Ok(Self {
            version: Version::new(r.read_u8()),
            msg_type: MessageType::try_from(r.read_u8())?,
            flags: FrameFlags::from_bits(r.read_u8()),
            priority: Priority::new(r.read_u8()),
            session_id: SessionId(r.read_u64()),
            stream_id: StreamId(r.read_u64()),
            seq: Seq(r.read_u64()),
            timestamp: Timestamp(r.read_u64()),
            payload_len: r.read_u32(),
        })
    }

    #[inline(always)]
    pub fn decode_from(r: &mut ByteReader) -> Result<Self, FrameError> {
        Ok(Self {
            version: Version::new(r.read_u8()),
            msg_type: MessageType::try_from(r.read_u8())?,
            flags: FrameFlags::from_bits(r.read_u8()),
            priority: Priority::new(r.read_u8()),
            session_id: SessionId(r.read_u64()),
            stream_id: StreamId(r.read_u64()),
            seq: Seq(r.read_u64()),
            timestamp: Timestamp(r.read_u64()),
            payload_len: r.read_u32(),
        })
    }
}

// ---------------------------------------------------------------------------
// Whole-frame helpers (header + payload)
// ---------------------------------------------------------------------------

/// Encodes a complete frame (header + payload) by appending to `out`.
///
/// `header.payload_len` is overwritten with `payload.len()` so the caller
/// cannot accidentally declare a length that disagrees with the bytes that
/// follow. Payloads larger than [`MAX_PAYLOAD_LEN`] return
/// [`FrameError::PayloadTooLarge`].
/// +------------+--------------+
/// | Header 40B | Payload (N)  |
/// +------------+--------------+
#[inline(always)]
pub fn encode_frame(
    header: &FrameHeader,
    payload: &[u8],
    writer: &mut ByteWriter,
) -> Result<(), FrameError> {
    // Check payload length before writing the header, so we don't write a
    // header that claims a length we can't satisfy.
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLarge {
            len: payload.len(),
            max: MAX_PAYLOAD_LEN,
        });
    }

    // Copy the header so we can mutate the payload_len field without affecting
    // the caller's value.
    let mut header = *header;

    // Write the header and payload in one shot.
    header.payload_len = payload.len() as u32;

    // Write the header first, then the payload.
    header.encode_into(writer);

    // Write the payload bytes after the header.
    writer.write_slice(payload);
    Ok(())
}

/// Decodes a complete frame from `src`, returning the parsed header and a
/// borrow over the payload bytes.
///
/// Returns [`FrameError::IncompleteHeader`] when fewer than [`HEADER_SIZE`]
/// bytes are available, or [`FrameError::IncompletePayload`] when the declared
/// `payload_len` exceeds the bytes that follow the header.
#[inline(always)]
pub fn decode_frame<'a>(
    reader: &mut ByteReader<'a>,
) -> Result<(FrameHeader, &'a [u8]), FrameError> {
    let header = FrameHeader::decode_from(reader)?;
    let payload_len = header.payload_len as usize;
    let have = reader.remaining();
    if have < payload_len {
        return Err(FrameError::IncompletePayload {
            declared: payload_len,
            have,
        });
    }
    let payload = reader.read_slice(payload_len);
    Ok((header, payload))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- read helpers ----

    #[test]
    fn read_be_helpers_match_to_be_bytes() {
        let mut buf = [0u8; 20];
        buf[0..8].copy_from_slice(&0x0123_4567_89AB_CDEFu64.to_be_bytes());
        buf[8..12].copy_from_slice(&0xCAFEBABEu32.to_be_bytes());
        assert_eq!(read_be_u64(&buf, 0), 0x0123_4567_89AB_CDEF);
        assert_eq!(read_be_u32(&buf, 8), 0xCAFEBABE);
    }

    // ---- Version ----

    #[test]
    fn version_current_is_v1_and_display() {
        assert_eq!(Version::CURRENT, Version::V1);
        assert_eq!(Version::CURRENT.get(), 1);
        assert!(Version::CURRENT.is_current());
        assert!(!Version::new(2).is_current());
        assert_eq!(u8::from(Version::new(7)), 7);
        assert_eq!(format!("{}", Version::new(3)), "v3");
    }

    // ---- Priority ----

    #[test]
    fn priority_ordering_and_conversions() {
        assert!(Priority::MIN < Priority::LOW);
        assert!(Priority::LOW < Priority::NORMAL);
        assert!(Priority::NORMAL < Priority::HIGH);
        assert!(Priority::HIGH < Priority::MAX);
        assert_eq!(Priority::NORMAL.get(), 128);
        assert_eq!(u8::from(Priority::new(200)), 200);
        assert_eq!(format!("{}", Priority::new(10)), "prio:10");
    }

    // ---- SessionId / StreamId / Seq ----

    #[test]
    fn session_stream_seq_conversions() {
        assert_eq!(SessionId::new(7).get(), 7);
        assert_eq!(u64::from(SessionId::from(42)), 42);
        assert_eq!(format!("{}", SessionId::new(5)), "session:5");
        assert_eq!(SessionId::ZERO.get(), 0);

        assert_eq!(StreamId::new(7).get(), 7);
        assert_eq!(u64::from(StreamId::from(42)), 42);
        assert_eq!(format!("{}", StreamId::new(5)), "stream:5");
        assert!(StreamId::new(1) < StreamId::new(2));
    }

    #[test]
    fn seq_arithmetic() {
        assert_eq!(Seq::ZERO.get(), 0);
        assert_eq!(Seq::new(5).next(), Seq::new(6));
        assert_eq!(Seq::new(u64::MAX).next(), Seq::ZERO); // wraps
        assert_eq!(Seq::new(10).gap_from(Seq::new(3)), 7);
        assert_eq!(Seq::new(3).gap_from(Seq::new(10)), 0); // saturates
        assert!(Seq::new(1) < Seq::new(2));
    }

    // ---- Timestamp ----

    #[test]
    fn timestamp_constructors_and_now() {
        assert_eq!(Timestamp::from_millis(3).get(), 3_000_000);
        assert_eq!(Timestamp::from_micros(3).get(), 3_000);
        assert_eq!(Timestamp::from_nanos(3).get(), 3);
        assert_eq!(Timestamp::new(123).get(), 123);
        assert_eq!(u64::from(Timestamp::from(9)), 9);

        // now() is non-deterministic; just assert ordering against zero.
        let t = Timestamp::now();
        assert!(t >= Timestamp::ZERO);
        // A real clock in 2026 is comfortably above 1e18 ns.
        assert!(t.get() > 1_000_000_000_000_000_000 || t == Timestamp::ZERO);
    }

    // ---- FrameFlags ----

    #[test]
    fn flags_bit_operations() {
        let f = FrameFlags::FIN | FrameFlags::URGENT;
        assert!(f.contains(FrameFlags::FIN));
        assert!(f.contains(FrameFlags::URGENT));
        assert!(!f.contains(FrameFlags::COMPRESSED));

        let mut g = FrameFlags::NONE;
        g |= FrameFlags::FRAGMENT;
        g |= FrameFlags::ACK_REQ;
        assert!(g.contains(FrameFlags::FRAGMENT));
        assert!(g.contains(FrameFlags::ACK_REQ));
        assert!(!g.contains(FrameFlags::FIN));

        let h = FrameFlags::from_bits(0xFF);
        assert!(h.contains(FrameFlags::FIN));
        assert!(h.contains(FrameFlags::COMPRESSED));
        assert!(h.contains(FrameFlags::FRAGMENT));
        assert!(h.contains(FrameFlags::URGENT));
        assert!(h.contains(FrameFlags::ACK_REQ));
    }

    // ---- MessageType ----

    #[test]
    fn all_message_types_round_trip_through_byte() {
        for mt in ALL_MESSAGE_TYPES {
            let b = mt.as_byte();
            assert_eq!(MessageType::from_byte(b), Some(mt));
            assert_eq!(MessageType::try_from(b), Ok(mt));
        }
    }

    #[test]
    fn reserved_and_unknown_bytes_map_to_none() {
        assert_eq!(MessageType::from_byte(0x00), None);
        // 0x1B–0x1D are now Resume / ResumeOk / ResumeUnavailable.
        assert_eq!(MessageType::from_byte(0x1E), None);
        assert_eq!(MessageType::from_byte(0xFF), None);
        assert!(matches!(
            MessageType::try_from(0x42),
            Err(FrameError::UnknownMessageType { value: 0x42 })
        ));
    }

    #[test]
    fn data_vs_control_partition_covers_all_variants() {
        for mt in ALL_MESSAGE_TYPES {
            assert!(mt.is_data() ^ mt.is_control());
        }
        assert!(MessageType::TextToken.is_data());
        assert!(MessageType::StreamOpen.is_control());
    }

    #[test]
    fn wire_bytes_are_dense_and_unique() {
        let bytes: Vec<u8> = ALL_MESSAGE_TYPES.iter().map(|mt| mt.as_byte()).collect();
        assert_eq!(bytes, (0x01..=0x1Du8).collect::<Vec<_>>());
        let unique: std::collections::HashSet<_> = bytes.iter().copied().collect();
        assert_eq!(unique.len(), bytes.len());
    }

    #[test]
    fn pubsub_variants_classified_correctly() {
        for mt in ALL_MESSAGE_TYPES {
            if mt.is_pubsub() {
                assert!(mt.is_data(), "{mt:?}: pub/sub must be data-plane");
                assert!(!mt.is_control(), "{mt:?}: pub/sub must not be control");
            }
        }
        assert!(MessageType::Subscribe.is_pubsub());
        assert!(MessageType::Publish.is_pubsub());
        assert!(MessageType::ConsumerGroupJoin.is_pubsub());
        assert!(MessageType::ConsumerGroupLeave.is_pubsub());
        assert!(MessageType::ConsumerGroupHeartbeat.is_pubsub());
        assert!(!MessageType::StateUpdate.is_pubsub());
        assert!(!MessageType::StreamOpen.is_pubsub());
    }

    // ---- FrameHeader builder ----

    #[test]
    fn frame_header_builder_defaults_and_chain() {
        let h = FrameHeader::new(SessionId::new(1), StreamId::new(2), MessageType::TextToken)
            .with_seq(Seq::new(42))
            .with_flags(FrameFlags::FIN | FrameFlags::ACK_REQ)
            .with_priority(Priority::HIGH)
            .with_timestamp(Timestamp::new(12345))
            .with_payload_len(99);

        assert_eq!(h.version, Version::CURRENT);
        assert_eq!(h.session_id, SessionId::new(1));
        assert_eq!(h.stream_id, StreamId::new(2));
        assert_eq!(h.msg_type, MessageType::TextToken);
        assert_eq!(h.seq, Seq::new(42));
        assert_eq!(h.priority, Priority::HIGH);
        assert_eq!(h.timestamp, Timestamp::new(12345));
        assert_eq!(h.payload_len, 99);
        assert!(h.flags.contains(FrameFlags::FIN));
        assert!(h.flags.contains(FrameFlags::ACK_REQ));

        let minimal = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::Heartbeat);
        assert_eq!(minimal.seq, Seq::ZERO);
        assert_eq!(minimal.priority, Priority::NORMAL);
        assert!(minimal.flags.is_empty());
        assert_eq!(minimal.payload_len, 0);
    }

    // ---- FrameHeader encode / decode ----

    #[test]
    fn header_round_trip_preserves_all_fields() -> Result<(), FrameError> {
        let original = FrameHeader::new(
            SessionId::new(0xA1B2_C3D4_E5F6_0718u64),
            StreamId::new(0x1122_3344_5566_7788u64),
            MessageType::AudioFrame,
        )
        .with_version(Version::V1)
        .with_flags(FrameFlags::COMPRESSED | FrameFlags::FRAGMENT)
        .with_priority(Priority::new(200))
        .with_seq(Seq::new(0xDEAD_BEEF_CAFE_BABEu64))
        .with_timestamp(Timestamp::new(0x1234_5678_9ABC_DEF0u64))
        .with_payload_len(0x00FF_FFFF);

        let mut buf = [0u8; HEADER_SIZE];
        original.encode(&mut buf)?;
        let decoded = FrameHeader::decode(&buf)?;
        assert_eq!(original, decoded);
        Ok(())
    }

    #[test]
    fn header_encode_uses_exact_wire_offsets() -> Result<(), FrameError> {
        let h = FrameHeader::new(SessionId::new(1), StreamId::new(2), MessageType::TextToken)
            .with_priority(Priority::new(0x77))
            .with_seq(Seq::new(0x0A0B_0C0D_0E0F_1011u64));

        let mut buf = [0u8; HEADER_SIZE];
        h.encode(&mut buf)?;

        // version at 0
        assert_eq!(buf[0], Version::CURRENT.get());
        // msg_type at 1
        assert_eq!(buf[1], MessageType::TextToken.as_byte());
        // flags at 2
        assert_eq!(buf[2], FrameFlags::NONE.bits());
        // priority at 3
        assert_eq!(buf[3], 0x77);
        // session_id big-endian at 4..12
        assert_eq!(&buf[4..12], &1u64.to_be_bytes());
        // stream_id big-endian at 12..20
        assert_eq!(&buf[12..20], &2u64.to_be_bytes());
        // seq big-endian at 20..28
        assert_eq!(&buf[20..28], &0x0A0B_0C0D_0E0F_1011u64.to_be_bytes());
        // timestamp big-endian at 28..36
        assert_eq!(&buf[28..36], &0u64.to_be_bytes());
        // payload_len big-endian at 36..40
        assert_eq!(&buf[36..40], &0u32.to_be_bytes());
        Ok(())
    }

    #[test]
    fn header_encode_rejects_short_buffer() {
        let h = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::Heartbeat);
        let mut buf = [0u8; HEADER_SIZE - 1];
        let err = h.encode(&mut buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::IncompleteHeader {
                needed,
                have
            } if needed == HEADER_SIZE && have == HEADER_SIZE - 1
        ));
    }

    #[test]
    fn header_decode_rejects_short_buffer() {
        let buf = [0u8; 10];
        let err = FrameHeader::decode(&buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::IncompleteHeader {
                needed,
                have: 10
            } if needed == HEADER_SIZE
        ));
    }

    #[test]
    fn header_decode_rejects_unknown_message_type() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0] = Version::CURRENT.get();
        buf[1] = 0x42; // reserved / unknown
        let err = FrameHeader::decode(&buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::UnknownMessageType { value: 0x42 }
        ));
    }

    #[test]
    fn header_round_trip_for_every_message_type() -> Result<(), FrameError> {
        let mut buf = [0u8; HEADER_SIZE];
        for mt in ALL_MESSAGE_TYPES {
            let h = FrameHeader::new(SessionId::new(7), StreamId::new(3), mt);
            h.encode(&mut buf)?;
            let decoded = FrameHeader::decode(&buf)?;
            assert_eq!(decoded.msg_type, mt);
            assert_eq!(decoded.version, Version::CURRENT);
        }
        Ok(())
    }

    // ---- whole-frame encode / decode ----

    #[test]
    fn frame_round_trip_with_payload() -> Result<(), FrameError> {
        let header = FrameHeader::new(
            SessionId::new(42),
            StreamId::new(7),
            MessageType::AudioFrame,
        )
        .with_flags(FrameFlags::COMPRESSED)
        .with_seq(Seq::new(100));
        let payload = b"hello frame world";

        let wire_size = HEADER_SIZE + payload.len();
        let mut out = vec![0u8; wire_size];
        let mut writer = ByteWriter::new(&mut out);
        encode_frame(&header, payload, &mut writer)?;

        assert_eq!(out.len(), HEADER_SIZE + payload.len());

        // payload_len must be reconciled by encode_frame.
        let prelude_header = FrameHeader::decode(&out)?;
        assert_eq!(prelude_header.payload_len as usize, payload.len());

        let mut reader = ByteReader::new(&out);
        let (decoded_header, decoded_payload) = decode_frame(&mut reader)?;
        assert_eq!(decoded_header.msg_type, MessageType::AudioFrame);
        assert_eq!(decoded_header.flags, FrameFlags::COMPRESSED);
        assert_eq!(decoded_header.seq, Seq::new(100));
        assert_eq!(decoded_payload, payload);
        Ok(())
    }

    #[test]
    fn frame_round_trip_with_empty_payload() -> Result<(), FrameError> {
        let header = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::Heartbeat);
        let mut out = vec![0u8; HEADER_SIZE];
        let mut writer = ByteWriter::new(&mut out);
        encode_frame(&header, &[], &mut writer)?;
        assert_eq!(out.len(), HEADER_SIZE);
        let mut reader = ByteReader::new(&out);
        let (h, p) = decode_frame(&mut reader)?;
        assert_eq!(h.payload_len, 0);
        assert!(p.is_empty());
        Ok(())
    }

    #[test]
    fn decode_frame_rejects_truncated_payload() -> Result<(), FrameError> {
        let header = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::TextToken)
            .with_payload_len(100);
        let mut buf = [0u8; HEADER_SIZE];
        header.encode(&mut buf)?;
        // Only 10 of the 100 declared payload bytes are present.
        let truncated = [&buf[..], b"0123456789".as_ref()].concat();

        let mut reader = ByteReader::new(&truncated);
        let err = decode_frame(&mut reader).unwrap_err();
        assert!(matches!(
            err,
            FrameError::IncompletePayload {
                declared: 100,
                have: 10
            }
        ));
        Ok(())
    }

    #[test]
    fn encode_frame_rejects_oversized_payload() {
        let header = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::TextToken);
        let oversized = vec![0u8; MAX_PAYLOAD_LEN + 1];
        // encode_frame returns PayloadTooLarge before touching the writer.
        let mut dummy: [u8; 0] = [];
        let mut writer = ByteWriter::new(&mut dummy);
        let err = encode_frame(&header, &oversized, &mut writer).unwrap_err();
        assert!(matches!(
            err,
            FrameError::PayloadTooLarge {
                len,
                max
            } if len == MAX_PAYLOAD_LEN + 1 && max == MAX_PAYLOAD_LEN
        ));
    }

    #[test]
    fn encode_frame_overwrites_declared_payload_len() -> Result<(), FrameError> {
        // Caller declares a wrong length; encode_frame must fix it.
        let header = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::TextToken)
            .with_payload_len(999);
        let payload = b"abc";
        let mut out = vec![0u8; HEADER_SIZE + payload.len()];
        let mut writer = ByteWriter::new(&mut out);
        encode_frame(&header, payload, &mut writer)?;
        let mut reader = ByteReader::new(&out);
        let (h, p) = decode_frame(&mut reader)?;
        assert_eq!(h.payload_len as usize, payload.len());
        assert_eq!(p, payload);
        Ok(())
    }
}
