//! Frame value types and the whole-frame codec.
//!
//! [`FrameRef`] is what the zero-copy [`decode_from`] / [`decode`] functions
//! return — the payload is a `&[u8]` borrowed from the caller's input buffer,
//! so decoding a frame touches no allocator. When the data must outlive that
//! buffer (cross a task boundary, land in a queue, …) call
//! [`FrameRef::into_owned`] to obtain a [`Frame`] whose payload is a
//! reference-counted [`bytes::Bytes`]; cloning that `Frame` is a single
//! refcount bump.
//!
//! Both value types embed a [`FrameHeader`] (the 40-byte fixed header defined
//! in [`crate::header`]); the header's individual fields live there. This
//! module composes header + payload and provides the whole-frame codec:
//!
//! * [`encode_into`] / [`Frame::encode_into`] / [`FrameRef::encode_into`] —
//!   the **primary** serialisation API; writes into a caller-supplied
//!   [`ByteWriter`] and never allocates.
//! * [`encode`] / [`Frame::encode`] / [`FrameRef::encode`] — convenience
//!   wrappers that append to a `Vec<u8>`; call the primary API internally.
//! * [`decode_from`] — the **primary** deserialisation API; reads from a
//!   caller-supplied [`ByteReader`].
//! * [`decode`] — convenience wrapper that wraps a `&[u8]` in a
//!   [`ByteReader`] and calls [`decode_from`].
//!
//! For header-only (40-byte) encoding use [`FrameHeader::encode`] /
//! [`FrameHeader::decode`] directly.

use bytes::Bytes;

use crate::byte_reader::ByteReader;
use crate::byte_writer::ByteWriter;
use crate::error::FrameError;
use crate::header::{
    FrameFlags, FrameHeader, HEADER_SIZE, MAX_PAYLOAD_LEN, MessageType, Priority, Seq, SessionId,
    StreamId, Timestamp, Version,
};

// ---------------------------------------------------------------------------
// FrameRef — borrowed payload (zero-copy)
// ---------------------------------------------------------------------------

/// A decoded frame that borrows its payload from the input buffer.
///
/// Produced by [`decode`]. No allocation, no copy: `payload` references the
/// caller's buffer and is valid for as long as that buffer is. Use
/// [`into_owned`](Self::into_owned) when the data must outlive the source.
#[derive(Clone, Copy, Debug)]
pub struct FrameRef<'a> {
    /// The fixed header fields (version, type, flags, priority, ids, …).
    pub header: FrameHeader,
    /// Raw payload bytes (zero-copy borrow of the input).
    pub payload: &'a [u8],
}

impl<'a> FrameRef<'a> {
    /// Total wire size this frame occupies: header + payload.
    #[inline]
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }

    /// Converts to an owned [`Frame`] by copying the payload once.
    ///
    /// The resulting `Frame` can be cloned cheaply and moved freely; use this
    /// when the data needs to outlive the source buffer.
    #[inline]
    pub fn into_owned(self) -> Frame {
        let payload = Bytes::copy_from_slice(self.payload);
        let mut header = self.header;
        header.payload_len = payload.len() as u32;
        Frame { header, payload }
    }

    /// Serialises this frame (header + payload) into `writer`.
    ///
    /// This is the primary codec method; it does not allocate.
    #[inline]
    pub fn encode_into(&self, writer: &mut ByteWriter) -> Result<(), FrameError> {
        encode_into(&self.header, self.payload, writer)
    }

    /// Appends this frame (header + payload) to `out`.
    ///
    /// Convenience wrapper over [`Self::encode_into`]. Use [`encode_into`] when
    /// writing into a pre-allocated buffer or a transport layer that supplies
    /// its own memory.
    ///
    /// # TODO(buffer-pool)
    ///
    /// When a `Connection`-scoped encoder buffer is available, callers should
    /// pass a pool-backed `ByteWriter` to [`encode_into`] directly instead of
    /// relying on this `Vec` growth path.
    #[inline]
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), FrameError> {
        let wire = self.wire_size();
        let start = out.len();
        out.resize(start + wire, 0);
        let mut writer = ByteWriter::new(&mut out[start..]);
        self.encode_into(&mut writer)
    }
}

// ---------------------------------------------------------------------------

/// An owned frame with a reference-counted payload.
///
/// Cloning a [`Frame`] bumps a refcount — no bytes are copied. Pass `Frame`
/// across task / thread boundaries or store it in queues; pass [`FrameRef`]
/// when you are decoding and consuming in the same stack frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// The fixed header fields (version, type, flags, priority, ids, …).
    pub header: FrameHeader,
    /// Raw payload bytes (ref-counted, cheap to clone).
    pub payload: Bytes,
}

impl Frame {
    /// Creates a new frame with sensible header defaults
    /// (`version = CURRENT`, `flags = NONE`, `priority = NORMAL`,
    /// `seq = ZERO`, `timestamp = ZERO`).
    #[inline]
    pub fn new(
        session_id: SessionId,
        stream_id: StreamId,
        msg_type: MessageType,
        payload: impl Into<Bytes>,
    ) -> Self {
        let payload = payload.into();
        let mut header = FrameHeader::new(session_id, stream_id, msg_type);
        // Invariant: a Frame's header.payload_len always matches its payload.
        header.payload_len = payload.len() as u32;
        Self { header, payload }
    }

    /// Creates a frame from an explicit header + payload.
    #[inline]
    pub fn from_header(mut header: FrameHeader, payload: impl Into<Bytes>) -> Self {
        let payload = payload.into();
        // Invariant: a Frame's header.payload_len always matches its payload.
        header.payload_len = payload.len() as u32;
        Self { header, payload }
    }

    /// Builder: set the protocol version.
    #[inline]
    pub fn with_version(mut self, version: Version) -> Self {
        self.header.version = version;
        self
    }

    /// Builder: set the flags.
    #[inline]
    pub fn with_flags(mut self, flags: FrameFlags) -> Self {
        self.header.flags = flags;
        self
    }

    /// Builder: set the priority.
    #[inline]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.header.priority = priority;
        self
    }

    /// Builder: set the sequence number.
    #[inline]
    pub fn with_seq(mut self, seq: Seq) -> Self {
        self.header.seq = seq;
        self
    }

    /// Builder: set the timestamp.
    #[inline]
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.header.timestamp = timestamp;
        self
    }

    /// Total wire size this frame will occupy: header + payload.
    #[inline]
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }

    /// Borrows this owned frame as a zero-copy [`FrameRef`].
    #[inline]
    pub fn as_ref(&self) -> FrameRef<'_> {
        FrameRef {
            header: self.header,
            payload: &self.payload,
        }
    }

    /// Serialises this frame (header + payload) into `writer`.
    ///
    /// This is the primary codec method; it does not allocate.
    #[inline]
    pub fn encode_into(&self, writer: &mut ByteWriter) -> Result<(), FrameError> {
        encode_into(&self.header, &self.payload, writer)
    }

    /// Appends this frame (header + payload) to `out`.
    ///
    /// Convenience wrapper over [`Self::encode_into`]. Use [`encode_into`] when
    /// writing into a pre-allocated buffer or a transport layer that supplies
    /// its own memory.
    ///
    /// # TODO(buffer-pool)
    ///
    /// When a `Connection`-scoped encoder buffer is available, callers should
    /// pass a pool-backed `ByteWriter` to [`encode_into`] directly instead of
    /// relying on this `Vec` growth path.
    #[inline]
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), FrameError> {
        let wire = self.wire_size();
        let start = out.len();
        out.resize(start + wire, 0);
        let mut writer = ByteWriter::new(&mut out[start..]);
        self.encode_into(&mut writer)
    }
}

// ---------------------------------------------------------------------------
// Whole-frame codec
// ---------------------------------------------------------------------------

/// Serialises a whole frame (header + payload) into `writer`.
///
/// This is the primary encode entry point; it does not allocate.
/// `header.payload_len` is overwritten with `payload.len()` so the caller
/// cannot accidentally declare a length that disagrees with the bytes that
/// follow. Payloads larger than [`MAX_PAYLOAD_LEN`] return
/// [`FrameError::PayloadTooLarge`].
#[inline]
pub fn encode_into(
    header: &FrameHeader,
    payload: &[u8],
    writer: &mut ByteWriter,
) -> Result<(), FrameError> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLarge {
            len: payload.len(),
            max: MAX_PAYLOAD_LEN,
        });
    }
    let mut hdr = *header;
    hdr.payload_len = payload.len() as u32;
    hdr.encode_into(writer);
    writer.write_slice(payload);
    Ok(())
}

/// Encodes a whole frame (header + payload) by appending to `out`.
///
/// Convenience wrapper over [`encode_into`]. `header.payload_len` is
/// overwritten with `payload.len()`. Payloads larger than [`MAX_PAYLOAD_LEN`]
/// return [`FrameError::PayloadTooLarge`] without writing anything.
///
/// # TODO(buffer-pool)
///
/// When a `Connection`-scoped encoder buffer is available, callers should
/// pass a pool-backed `ByteWriter` to [`encode_into`] directly. This `Vec`
/// resize path exists only for convenience in tests and one-shot use.
#[inline]
pub fn encode(header: FrameHeader, payload: &[u8], out: &mut Vec<u8>) -> Result<(), FrameError> {
    // Validate before touching `out` so a failed call leaves it unchanged.
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLarge {
            len: payload.len(),
            max: MAX_PAYLOAD_LEN,
        });
    }
    let wire = HEADER_SIZE + payload.len();
    let start = out.len();
    out.resize(start + wire, 0);
    let mut writer = ByteWriter::new(&mut out[start..]);
    encode_into(&header, payload, &mut writer)
}

/// Decodes a whole frame from `reader`, returning a [`FrameRef`] whose payload
/// borrows the reader's backing buffer.
///
/// This is the primary decode entry point: only the 40-byte header is parsed;
/// the payload is borrowed in place via [`ByteReader::read_slice`]. Returns
/// [`FrameError::IncompleteHeader`] when fewer than [`HEADER_SIZE`] bytes are
/// available, or [`FrameError::IncompletePayload`] when the declared
/// `payload_len` exceeds the bytes that follow the header.
#[inline]
pub fn decode_from<'a>(reader: &mut ByteReader<'a>) -> Result<FrameRef<'a>, FrameError> {
    if reader.remaining() < HEADER_SIZE {
        return Err(FrameError::IncompleteHeader {
            needed: HEADER_SIZE,
            have: reader.remaining(),
        });
    }
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
    Ok(FrameRef { header, payload })
}

/// Decodes a whole frame from `src`, returning a [`FrameRef`] whose payload
/// borrows `src`.
///
/// Convenience wrapper over [`decode_from`]; constructs a temporary
/// [`ByteReader`] around `src` and delegates to it.
#[inline]
pub fn decode<'a>(src: &'a [u8]) -> Result<FrameRef<'a>, FrameError> {
    let mut reader = ByteReader::new(src);
    decode_from(&mut reader)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_ref_into_owned_copies_payload() {
        let backing = b"hello world".to_vec();
        let view = FrameRef {
            header: FrameHeader::new(SessionId::new(1), StreamId::new(2), MessageType::TextToken),
            payload: &backing,
        };
        let owned = view.into_owned();
        assert_eq!(owned.payload.as_ref(), &backing[..]);
        assert_eq!(owned.header.session_id, SessionId::new(1));
        assert_eq!(owned.header.stream_id, StreamId::new(2));
        assert_eq!(owned.header.msg_type, MessageType::TextToken);
    }

    #[test]
    fn frame_builder_sets_all_header_fields() {
        let f = Frame::new(
            SessionId::new(10),
            StreamId::new(20),
            MessageType::AudioFrame,
            Bytes::from_static(b"audio"),
        )
        .with_version(Version::V1)
        .with_flags(FrameFlags::URGENT | FrameFlags::COMPRESSED)
        .with_priority(Priority::HIGH)
        .with_seq(Seq::new(7))
        .with_timestamp(Timestamp::new(123));

        assert_eq!(f.header.version, Version::V1);
        assert_eq!(f.header.session_id, SessionId::new(10));
        assert_eq!(f.header.stream_id, StreamId::new(20));
        assert_eq!(f.header.msg_type, MessageType::AudioFrame);
        assert_eq!(f.header.priority, Priority::HIGH);
        assert_eq!(f.header.seq, Seq::new(7));
        assert_eq!(f.header.timestamp, Timestamp::new(123));
        assert!(f.header.flags.contains(FrameFlags::URGENT));
        assert!(f.header.flags.contains(FrameFlags::COMPRESSED));
        assert_eq!(f.payload.as_ref(), b"audio");
    }

    #[test]
    fn frame_clone_preserves_fields() {
        let f = Frame::new(
            SessionId::new(2),
            StreamId::new(3),
            MessageType::AudioFrame,
            Bytes::from_static(b"audio"),
        )
        .with_seq(Seq::new(7))
        .with_flags(FrameFlags::URGENT);
        let f2 = f.clone();
        assert_eq!(f, f2);
        assert_eq!(f.payload, f2.payload);
    }

    #[test]
    fn wire_size_accounts_for_header_and_payload() {
        let view = FrameRef {
            header: FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::Heartbeat),
            payload: &[],
        };
        assert_eq!(view.wire_size(), HEADER_SIZE);

        let owned = Frame::new(
            SessionId::ZERO,
            StreamId::ZERO,
            MessageType::TextToken,
            Bytes::from_static(b"abc"),
        );
        assert_eq!(owned.wire_size(), HEADER_SIZE + 3);
    }

    #[test]
    fn as_ref_round_trips_header_and_payload() {
        let f = Frame::new(
            SessionId::new(5),
            StreamId::new(6),
            MessageType::Fragment,
            Bytes::from_static(b"x"),
        )
        .with_seq(Seq::new(99))
        .with_flags(FrameFlags::FRAGMENT | FrameFlags::FIN);
        let r = f.as_ref();
        assert_eq!(r.header, f.header);
        assert_eq!(r.payload, &b"x"[..]);
    }

    #[test]
    fn encode_decode_round_trip() -> Result<(), FrameError> {
        let original = Frame::new(
            SessionId::new(42),
            StreamId::new(7),
            MessageType::AudioFrame,
            Bytes::from_static(b"hello frame world"),
        )
        .with_flags(FrameFlags::COMPRESSED)
        .with_priority(Priority::HIGH)
        .with_seq(Seq::new(100))
        .with_timestamp(Timestamp::new(5_000_000));

        let mut out = Vec::new();
        original.encode(&mut out)?;
        assert_eq!(out.len(), HEADER_SIZE + original.payload.len());

        let decoded = decode(&out)?;
        assert_eq!(decoded.header, original.header);
        assert_eq!(decoded.payload, original.payload.as_ref());
        assert_eq!(decoded.wire_size(), out.len());
        Ok(())
    }

    #[test]
    fn decode_rejects_truncated_payload() -> Result<(), FrameError> {
        let header = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::TextToken)
            .with_payload_len(100);
        let mut buf = [0u8; HEADER_SIZE];
        header.encode(&mut buf)?;
        // Only 10 of the 100 declared payload bytes are present.
        let truncated = [&buf[..], b"0123456789".as_ref()].concat();

        let err = decode(&truncated).unwrap_err();
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
    fn encode_rejects_oversized_payload() {
        let header = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::TextToken);
        let oversized = vec![0u8; MAX_PAYLOAD_LEN + 1];
        let mut out = Vec::new();
        let err = encode(header, &oversized, &mut out).unwrap_err();
        assert!(matches!(
            err,
            FrameError::PayloadTooLarge {
                len,
                max
            } if len == MAX_PAYLOAD_LEN + 1 && max == MAX_PAYLOAD_LEN
        ));
        // Nothing should have been written on failure.
        assert!(out.is_empty());
    }

    #[test]
    fn encode_reconciles_declared_payload_len() -> Result<(), FrameError> {
        // Caller declares a wrong length; encode must fix it from the payload.
        let header = FrameHeader::new(SessionId::ZERO, StreamId::ZERO, MessageType::TextToken)
            .with_payload_len(999);
        let payload = b"abc";
        let mut out = Vec::new();
        encode(header, payload, &mut out)?;
        let decoded = decode(&out)?;
        assert_eq!(decoded.payload.len(), payload.len());
        assert_eq!(decoded.payload, payload);
        Ok(())
    }

    #[test]
    fn into_owned_then_decode_still_matches() -> Result<(), FrameError> {
        let backing: Vec<u8> = (0..32u8).collect();
        let view = FrameRef {
            header: FrameHeader::new(
                SessionId::new(1),
                StreamId::new(9),
                MessageType::StateUpdate,
            )
            .with_seq(Seq::new(4)),
            payload: &backing,
        };
        let owned = view.into_owned();
        let mut wire = Vec::new();
        owned.encode(&mut wire)?;
        let decoded = decode(&wire)?;
        assert_eq!(decoded.header, owned.header);
        assert_eq!(decoded.payload, owned.payload.as_ref());
        Ok(())
    }
}
