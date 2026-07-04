//! Codec-layer frame encode / decode — 22-byte header, optimised for throughput.
//!
//! Wire layout:
//! ```text
//! offset  0   8  stream_id   (u64, big-endian)
//! offset  8   8  seq         (u64, big-endian)
//! offset 16   1  msg_type    (u8,  MessageType)
//! offset 17   1  flags       (u8,  FrameFlags)
//! offset 18   4  payload_len (u32, big-endian)
//! offset 22      payload     (payload_len bytes)
//! ```
//!
//! For the full 40-byte header used by [`crate::frame`] / [`crate::header`]
//! see those modules.
//!
//! ## One-shot helpers
//!
//! * [`encode_into`] — appends one frame to a caller-managed [`BytesMut`].
//!   Pre-allocate the buffer and reuse across sends to avoid per-frame
//!   allocation. The hot path avoids `reserve()` entirely when capacity is
//!   already sufficient; all header fields are written via raw pointer stores.
//! * [`decode`] / [`decode_with_len`] — parse one frame from a `&[u8]`,
//!   returning a zero-copy [`FrameRef`] that borrows the input. All header
//!   reads after the bounds check use unchecked pointer arithmetic.
//!
//! ## Streaming
//!
//! [`FrameDecoder`] accumulates bytes from QUIC `stream_recv` and yields one
//! complete [`Frame`] per call. The full header is parsed in a single pass
//! before `split_to` — no double-read.
//!
//! ## Buffer pool
//!
//! [`FrameEncoder::encode_buffer`] allocates a fresh [`BytesMut`] per frame.
//! For hot send paths, enable the `buffer-pool` cargo feature and use
//! [`FrameEncoder::encode_buffer_pooled`], which acquires a slot from a
//! shared [`BufferPool`](buffer_pool::BufferPool), writes into it via
//! [`encode_into_slice`] (a raw `&mut [u8]` encoder), and returns an RAII
//! [`Buffer`](buffer_pool::Buffer) that recycles on drop.
//!
//! The [`encode_into`] / [`decode`] / [`FrameDecoder`] paths are untouched:
//! they remain the lowest-level entry points and continue to work with
//! caller-managed [`BytesMut`] / `&[u8]` storage. Inline `Future optimisation`
//! comments below mark remaining architectural decisions (ring-buffer
//! decoder, `FrameRef`-returning decode) that require larger API changes.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::FrameError;
use crate::header::{FrameFlags, MessageType, Seq, StreamId};

/// Wire size of the codec-layer header in bytes.
///
/// Layout: `stream_id(8) + seq(8) + msg_type(1) + flags(1) + payload_len(4) = 22`.
pub const HEADER_SIZE: usize = 22;

// ---------------------------------------------------------------------------
// Codec-local value types
// ---------------------------------------------------------------------------

/// Simplified frame header for the 22-byte codec wire format.
///
/// Distinct from [`crate::header::FrameHeader`] (40 bytes); this type carries
/// only the fields the codec layer needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// Stream identifier.
    pub stream_id: StreamId,
    /// Per-stream sequence number.
    pub seq: Seq,
    /// Message type.
    pub msg_type: MessageType,
    /// Frame flags.
    pub flags: FrameFlags,
}

impl FrameHeader {
    /// Creates a header with `seq = Seq::ZERO` and `flags = FrameFlags::NONE`.
    #[inline(always)]
    pub fn new(stream_id: StreamId, msg_type: MessageType) -> Self {
        Self {
            stream_id,
            seq: Seq::ZERO,
            msg_type,
            flags: FrameFlags::NONE,
        }
    }

    /// Builder: set the sequence number.
    #[inline(always)]
    pub fn with_seq(mut self, seq: Seq) -> Self {
        self.seq = seq;
        self
    }

    /// Builder: set the frame flags.
    #[inline(always)]
    pub fn with_flags(mut self, flags: FrameFlags) -> Self {
        self.flags = flags;
        self
    }
}

/// A decoded frame that borrows its payload from the input buffer — zero copy.
///
/// Produced by [`decode`] / [`decode_with_len`]. Valid for the lifetime of the
/// source buffer. Call [`to_owned`](Self::to_owned) when the data must outlive
/// the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameRef<'a> {
    /// Stream identifier.
    pub stream_id: StreamId,
    /// Per-stream sequence number.
    pub seq: Seq,
    /// Message type.
    pub msg_type: MessageType,
    /// Frame flags.
    pub flags: FrameFlags,
    /// Payload bytes — zero-copy borrow of the input buffer.
    pub payload: &'a [u8],
}

impl<'a> FrameRef<'a> {
    /// Total wire bytes this frame occupies: header + payload.
    #[inline(always)]
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }

    /// Copies the payload once to produce an owned [`Frame`].
    #[inline]
    pub fn to_owned(self) -> Frame {
        Frame {
            stream_id: self.stream_id,
            seq: self.seq,
            msg_type: self.msg_type,
            flags: self.flags,
            payload: Bytes::copy_from_slice(self.payload),
        }
    }
}

/// An owned frame with a reference-counted payload — cheap to clone.
///
/// Cloning bumps a refcount; no bytes are copied. Use [`FrameRef`] for
/// decode-and-consume in a single stack frame to avoid the `Arc` cost entirely.
///
/// # Future optimisation
///
/// For the highest-throughput streaming-decoder path, [`FrameDecoder::decode_frame`]
/// could grow a `decode_frame_ref` variant that returns `FrameRef<'_>` borrowing
/// the internal buffer, eliminating the `freeze()` Arc increment on every decoded
/// frame. This is an independent API-change follow-up — it does not require the
/// `buffer-pool` feature, but would compose naturally with a pool-backed slab.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// Stream identifier.
    pub stream_id: StreamId,
    /// Per-stream sequence number.
    pub seq: Seq,
    /// Message type.
    pub msg_type: MessageType,
    /// Frame flags.
    pub flags: FrameFlags,
    /// Payload bytes — ref-counted, cheap to clone.
    pub payload: Bytes,
}

impl Frame {
    /// Total wire bytes this frame occupies: header + payload.
    #[inline(always)]
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }
}

// ---------------------------------------------------------------------------
// One-shot encode
// ---------------------------------------------------------------------------

/// Appends a single frame to `dst`.
///
/// ## Performance
///
/// Pre-allocate `dst` once (e.g. `BytesMut::with_capacity(64 * 1024)`) and
/// reuse across sends: when spare capacity is sufficient, no allocator is
/// touched. All header fields are written via raw unaligned pointer stores —
/// no per-field `put_*` call overhead.
///
/// ## Errors
///
/// Returns [`FrameError::PayloadTooLarge`] when `payload.len() > u32::MAX`.
///
/// ## Pool-backed alternative
///
/// The `reserve()` below only runs when `dst` lacks spare capacity. For
/// connection-scoped encoders the recommended pattern is a single
/// pre-allocated `BytesMut::with_capacity(64 * 1024)` reused with `clear()`
/// between frames. When the in-tree [`buffer_pool::BufferPool`] is available
/// (feature `buffer-pool`), prefer [`FrameEncoder::encode_buffer_pooled`] —
/// it acquires a sized slot up front, writes via [`encode_into_slice`], and
/// recycles the slot on drop.
#[inline(always)]
pub fn encode_into(
    dst: &mut BytesMut,
    header: FrameHeader,
    payload: &[u8],
) -> Result<(), FrameError> {
    let len = u32::try_from(payload.len()).map_err(|_| FrameError::PayloadTooLarge {
        len: payload.len(),
        max: u32::MAX as usize,
    })?;
    let needed = HEADER_SIZE + payload.len();
    // Skip reserve() when capacity is already sufficient — avoids the branch
    // cost when the caller pre-allocates the buffer.
    let spare = dst.capacity() - dst.len();
    if spare < needed {
        // Pool-backed callers route through `encode_buffer_pooled` and never
        // hit this branch — the slot is sized up front via `pool.get(needed)`.
        // This `reserve()` is the BytesMut growth path for non-pool callers.
        dst.reserve(needed - spare);
    }
    // SAFETY: `dst` now has at least `needed` spare bytes starting at
    // `as_mut_ptr() + len()`. Writing there and committing via `advance_mut`
    // is the documented BytesMut unsafe-write pattern.
    unsafe {
        let p = dst.as_mut_ptr().add(dst.len());
        core::ptr::write_unaligned(p as *mut u64, header.stream_id.get().to_be());
        core::ptr::write_unaligned(p.add(8) as *mut u64, header.seq.get().to_be());
        *p.add(16) = header.msg_type.as_byte();
        *p.add(17) = header.flags.bits();
        core::ptr::write_unaligned(p.add(18) as *mut u32, len.to_be());
        if !payload.is_empty() {
            core::ptr::copy_nonoverlapping(payload.as_ptr(), p.add(HEADER_SIZE), payload.len());
        }
        dst.advance_mut(needed);
    }
    Ok(())
}

/// Writes a single frame into a raw `&mut [u8]` slice — no allocator touched.
///
/// This is the lowest-level encoder: every other encode entry point either
/// writes into a [`BytesMut`] ([`encode_into`]) or a pool-backed slot
/// ([`FrameEncoder::encode_buffer_pooled`]); both ultimately funnel through
/// this function so the wire-format logic lives in exactly one place.
///
/// `dst` must be at least `HEADER_SIZE + payload.len()` bytes long. On
/// success, returns the number of bytes written (which equals
/// `HEADER_SIZE + payload.len()`).
///
/// ## Errors
///
/// * [`FrameError::PayloadTooLarge`] — `payload.len() > u32::MAX`.
/// * [`FrameError::BufferTooSmall`] — `dst.len() < HEADER_SIZE + payload.len()`.
#[inline(always)]
pub fn encode_into_slice(
    dst: &mut [u8],
    header: FrameHeader,
    payload: &[u8],
) -> Result<usize, FrameError> {
    let len = u32::try_from(payload.len()).map_err(|_| FrameError::PayloadTooLarge {
        len: payload.len(),
        max: u32::MAX as usize,
    })?;
    let needed = HEADER_SIZE + payload.len();
    if dst.len() < needed {
        return Err(FrameError::BufferTooSmall {
            needed,
            have: dst.len(),
        });
    }
    // SAFETY: `dst.len() >= needed` (checked above). Every offset below is
    // strictly less than `needed` and therefore in-bounds. Unaligned stores
    // are sound because `u8` slices have no alignment guarantee — we never
    // read these back as typed values, only as raw bytes on the wire.
    unsafe {
        let p = dst.as_mut_ptr();
        core::ptr::write_unaligned(p as *mut u64, header.stream_id.get().to_be());
        core::ptr::write_unaligned(p.add(8) as *mut u64, header.seq.get().to_be());
        *p.add(16) = header.msg_type.as_byte();
        *p.add(17) = header.flags.bits();
        core::ptr::write_unaligned(p.add(18) as *mut u32, len.to_be());
        if !payload.is_empty() {
            core::ptr::copy_nonoverlapping(payload.as_ptr(), p.add(HEADER_SIZE), payload.len());
        }
    }
    Ok(needed)
}

// ---------------------------------------------------------------------------
// One-shot decode
// ---------------------------------------------------------------------------

/// Decodes exactly one frame from the front of `src`.
///
/// Returns a [`FrameRef`] that borrows `src` — zero allocation, zero copy.
/// Use [`decode_with_len`] when you also need the consumed byte count.
#[inline(always)]
pub fn decode(src: &[u8]) -> Result<FrameRef<'_>, FrameError> {
    decode_with_len(src).map(|(f, _)| f)
}

/// Decodes one frame and also returns the total bytes consumed
/// (`HEADER_SIZE + payload_len`).
///
/// ## Performance
///
/// All header reads after the length pre-check use unchecked pointer
/// arithmetic — the `src.len() >= HEADER_SIZE` guard proves every byte
/// offset below is valid, so no additional bounds checks are generated by
/// the compiler.
#[inline(always)]
pub fn decode_with_len(src: &[u8]) -> Result<(FrameRef<'_>, usize), FrameError> {
    if src.len() < HEADER_SIZE {
        return Err(FrameError::IncompleteHeader {
            needed: HEADER_SIZE,
            have: src.len(),
        });
    }
    // SAFETY: src.len() >= HEADER_SIZE = 22; every offset below is < 22.
    let (stream_id, seq, msg_type_byte, flags, payload_len) = unsafe {
        let p = src.as_ptr();
        let stream_id = StreamId(u64::from_be(core::ptr::read_unaligned(p as *const u64)));
        let seq = Seq(u64::from_be(core::ptr::read_unaligned(
            p.add(8) as *const u64
        )));
        let msg_type_byte = *p.add(16);
        let flags = FrameFlags::from_bits(*p.add(17));
        let payload_len = u32::from_be(core::ptr::read_unaligned(p.add(18) as *const u32)) as usize;
        (stream_id, seq, msg_type_byte, flags, payload_len)
    };
    let msg_type = MessageType::try_from(msg_type_byte)?;
    let frame_end = HEADER_SIZE
        .checked_add(payload_len)
        .ok_or(FrameError::PayloadTooLarge {
            len: payload_len,
            max: usize::MAX.saturating_sub(HEADER_SIZE),
        })?;
    if src.len() < frame_end {
        return Err(FrameError::IncompletePayload {
            declared: payload_len,
            have: src.len() - HEADER_SIZE,
        });
    }
    Ok((
        FrameRef {
            stream_id,
            seq,
            msg_type,
            flags,
            // SAFETY: frame_end = HEADER_SIZE + payload_len <= src.len()
            payload: unsafe { src.get_unchecked(HEADER_SIZE..frame_end) },
        },
        frame_end,
    ))
}

// ---------------------------------------------------------------------------
// Streaming decoder
// ---------------------------------------------------------------------------

/// Incremental frame decoder for a byte stream that may deliver partial frames
/// or multiple frames per read.
///
/// ## Typical usage
///
/// ```rust,ignore
/// let mut dec = FrameDecoder::new();
/// dec.push(&raw[..n]);                          // feed QUIC bytes
/// while let Some(frame) = dec.decode_frame()? { // drain complete frames
///     handle(frame);
/// }
/// ```
///
/// ## Zero-copy receive path
///
/// To avoid the `memcpy` inside [`push`](Self::push), write QUIC recv bytes
/// directly into the internal buffer:
///
/// ```rust,ignore
/// let spare = dec.spare_capacity_mut(4096);
/// let n = stream.recv(spare)?;
/// // SAFETY: `stream.recv` initialised exactly `n` bytes.
/// unsafe { dec.commit(n); }
/// ```
///
/// ## Error model
///
/// `Ok(None)` means "need more bytes". `Err(_)` leaves the decoder in an
/// unrecoverable state; call [`clear`](Self::clear) to reset.
pub struct FrameDecoder {
    // Future optimisation: replace `BytesMut` with a fixed-size ring buffer
    // (head/tail pointers into a pool-backed slab) to eliminate the cost of
    // `split_to()` advancing the internal offset on every decoded frame.
    // NATS, Redis, and DPDK all use ring buffers for receive paths at this
    // performance tier. Requires API surface changes (`spare_capacity_mut`
    // returning a pair of slices, `commit` accepting wrap-around) and is
    // tracked separately from the pool-backed encoder path.
    buf: BytesMut,
    max_payload_len: usize,
}

/// Conservative default cap: large enough for a full HD video NAL unit or a
/// batch of audio frames; small enough that a misbehaving peer cannot exhaust
/// memory before the connection is shut down.
pub const DEFAULT_MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024; // 16 MiB

impl FrameDecoder {
    /// Creates a decoder with an empty buffer and the default payload cap.
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
            max_payload_len: DEFAULT_MAX_PAYLOAD_LEN,
        }
    }

    /// Creates a decoder pre-allocated for `capacity` bytes, with the default
    /// payload cap. Use when the expected frame size is known.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
            max_payload_len: DEFAULT_MAX_PAYLOAD_LEN,
        }
    }

    /// Sets the maximum accepted payload length.
    ///
    /// Frames declaring a larger payload surface as
    /// [`FrameError::PayloadTooLarge`] from [`decode_frame`](Self::decode_frame).
    #[inline(always)]
    pub fn max_payload_len(mut self, max: usize) -> Self {
        self.max_payload_len = max;
        self
    }

    /// Appends raw bytes to the internal accumulation buffer.
    ///
    /// Prefer the zero-copy path ([`spare_capacity_mut`](Self::spare_capacity_mut)
    /// + [`commit`](Self::commit)) when reading directly from a QUIC stream to
    ///   avoid the `extend_from_slice` memcpy.
    #[inline]
    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Returns the spare capacity of the receive buffer for a zero-copy receive.
    ///
    /// Write up to `spare_capacity_mut(hint).len()` bytes, then call
    /// `unsafe { self.commit(n) }` to make them visible to
    /// [`decode_frame`](Self::decode_frame). Grows the buffer if needed.
    #[inline]
    pub fn spare_capacity_mut(&mut self, hint: usize) -> &mut [u8] {
        let spare = self.buf.capacity() - self.buf.len();
        if spare < hint {
            // Future optimisation: route this growth through a pool-backed
            // slab once `FrameDecoder` switches to a ring buffer (see the
            // note on `buf` above). The current contiguous `BytesMut` model
            // cannot hand out a pool slot without an API change.
            self.buf.reserve(hint - spare);
        }
        // SAFETY: pointer stays within the allocated buffer;
        // the returned bytes are uninitialized — caller must fill them.
        unsafe {
            let p = self.buf.as_mut_ptr().add(self.buf.len());
            core::slice::from_raw_parts_mut(p, self.buf.capacity() - self.buf.len())
        }
    }

    /// Commits `n` bytes written into the slice returned by
    /// [`spare_capacity_mut`](Self::spare_capacity_mut).
    ///
    /// # Safety
    ///
    /// The first `n` bytes of the last `spare_capacity_mut` slice must have
    /// been fully initialised by the caller.
    #[inline]
    pub unsafe fn commit(&mut self, n: usize) {
        // SAFETY: caller guarantees the first `n` bytes of the last
        // `spare_capacity_mut` slice have been initialised.
        unsafe { self.buf.advance_mut(n) };
    }

    /// Attempts to decode the next complete frame.
    ///
    /// * `Ok(Some(frame))` — a whole frame was parsed and consumed.
    /// * `Ok(None)` — not enough bytes yet; call [`push`](Self::push) again.
    /// * `Err(_)` — the buffer contains an invalid frame; decoder is in an
    ///   error state. Call [`clear`](Self::clear) to recover.
    pub fn decode_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.buf.len() < HEADER_SIZE {
            return Ok(None);
        }
        // Read the complete header in a single pass before split_to —
        // no double-parse (previously msg_type/payload_len were read before
        // split, then stream_id/seq/flags were re-read from frame_bytes).
        // SAFETY: buf.len() >= HEADER_SIZE = 22; all offsets are in-bounds.
        let (stream_id, seq, msg_type_byte, flags, payload_len) = unsafe {
            let p = self.buf.as_ptr();
            let stream_id = StreamId(u64::from_be(core::ptr::read_unaligned(p as *const u64)));
            let seq = Seq(u64::from_be(core::ptr::read_unaligned(
                p.add(8) as *const u64
            )));
            let msg_type_byte = *p.add(16);
            let flags = FrameFlags::from_bits(*p.add(17));
            let payload_len =
                u32::from_be(core::ptr::read_unaligned(p.add(18) as *const u32)) as usize;
            (stream_id, seq, msg_type_byte, flags, payload_len)
        };
        // Validate type *before* split_to so an error leaves offending bytes
        // in the buffer for inspection / recovery.
        let msg_type = MessageType::try_from(msg_type_byte)?;
        if payload_len > self.max_payload_len {
            return Err(FrameError::PayloadTooLarge {
                len: payload_len,
                max: self.max_payload_len,
            });
        }
        let frame_end =
            HEADER_SIZE
                .checked_add(payload_len)
                .ok_or(FrameError::PayloadTooLarge {
                    len: payload_len,
                    max: self.max_payload_len,
                })?;
        if self.buf.len() < frame_end {
            return Ok(None);
        }
        // All validation passed. Detach the frame bytes — zero-copy split.
        // `freeze()` bumps a refcount; `slice()` gives a sub-view with no copy.
        // Future optimisation: a `decode_frame_ref` variant returning
        // `FrameRef<'_>` borrowing `self.buf` would eliminate the `freeze()`
        // Arc increment entirely. Tracked alongside the `Frame` doc note and
        // the ring-buffer follow-up above — independent of the pool-backed
        // encoder path that is already implemented.
        let frame_bytes = self.buf.split_to(frame_end).freeze();
        let payload = frame_bytes.slice(HEADER_SIZE..);
        Ok(Some(Frame {
            stream_id,
            seq,
            msg_type,
            flags,
            payload,
        }))
    }

    /// Returns the undecoded bytes currently buffered.
    #[inline(always)]
    pub fn remaining(&self) -> &[u8] {
        &self.buf
    }

    /// Drops every buffered byte and resets the decoder to a clean state.
    ///
    /// Use after an unrecoverable [`decode_frame`](Self::decode_frame) error.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.buf.clear();
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Pool-aware encoder
// ---------------------------------------------------------------------------

/// Frame encoder that provides one-shot encoding with an output buffer.
///
/// For the highest-throughput path prefer the free function [`encode_into`]
/// writing into a connection-scoped reused [`BytesMut`] (pre-allocate once,
/// `clear()` between frames). [`FrameEncoder::encode_buffer`] is useful when
/// the caller cannot own the buffer across frames.
///
/// ## Pool-backed path
///
/// With the `buffer-pool` cargo feature enabled,
/// [`encode_buffer_pooled`](Self::encode_buffer_pooled) acquires a slot from
/// a shared [`BufferPool`](buffer_pool::BufferPool), encodes into it via
/// [`encode_into_slice`], and returns a [`Buffer`](buffer_pool::Buffer) guard
/// that recycles the slot on `Drop` — the same API shape as `encode_buffer`
/// but without touching the global allocator on the steady-state path.
pub struct FrameEncoder {
    max_payload_len: usize,
}

/// Error returned by pool-backed encode methods
/// ([`FrameEncoder::encode_buffer_pooled`], [`Frame::encode_pooled`]).
///
/// Combines the codec-layer [`FrameError`] (payload-too-large / buffer-too-small
/// / unknown-message-type) with the pool-layer
/// [`BufferPoolError`](buffer_pool::BufferPoolError) (pool closed, backpressure
/// timeout, allocation failure). The two underlying causes have nothing in
/// common, so they are kept distinct rather than collapsed into a single
/// string.
#[cfg(feature = "buffer-pool")]
#[derive(Debug, thiserror::Error)]
pub enum EncodePooledError {
    /// The codec rejected the inputs (oversized payload, undersized slice, …).
    #[error(transparent)]
    Frame(#[from] FrameError),

    /// The pool could not satisfy the lease (closed, backpressure, OOM).
    #[error(transparent)]
    Pool(#[from] buffer_pool::BufferPoolError),
}

/// Default payload cap mirroring [`DEFAULT_MAX_PAYLOAD_LEN`].
pub const ENCODER_DEFAULT_MAX_PAYLOAD_LEN: usize = DEFAULT_MAX_PAYLOAD_LEN;

impl FrameEncoder {
    /// Creates an encoder with the default payload cap.
    pub fn new() -> Self {
        Self {
            max_payload_len: ENCODER_DEFAULT_MAX_PAYLOAD_LEN,
        }
    }

    /// Sets the maximum accepted payload length.
    #[inline(always)]
    pub fn max_payload_len(mut self, max: usize) -> Self {
        self.max_payload_len = max;
        self
    }

    /// Encodes a single frame into a freshly allocated [`BytesMut`].
    ///
    /// Each call hits the global allocator (`BytesMut::with_capacity`). For
    /// hot send paths, prefer [`encode_buffer_pooled`](Self::encode_buffer_pooled)
    /// (requires the `buffer-pool` feature), which recycles a slot from a
    /// [`BufferPool`](buffer_pool::BufferPool) and avoids the allocator entirely
    /// once the pool is warm.
    pub fn encode_buffer(
        &self,
        header: FrameHeader,
        payload: &[u8],
    ) -> Result<BytesMut, FrameError> {
        self.check_len(payload.len())?;
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
        encode_into(&mut buf, header, payload)?;
        Ok(buf)
    }

    /// Encodes a single frame into a slot leased from `pool`.
    ///
    /// Requires cargo feature `buffer-pool`. Acquires a buffer of exactly
    /// `HEADER_SIZE + payload.len()` bytes, writes the frame via
    /// [`encode_into_slice`], and returns the [`Buffer`](buffer_pool::Buffer)
    /// guard. The slot is recycled back to `pool` on `Drop`.
    ///
    /// The returned guard [`Deref`](core::ops::Deref)s to `&[u8]` covering
    /// exactly the encoded frame bytes — safe to feed straight into
    /// `quinn::End::write_all` or equivalent. Use
    /// [`Buffer::into_bytes`](buffer_pool::Buffer::into_bytes) if a refcounted
    /// [`Bytes`] is required downstream (this copies once).
    ///
    /// ## Errors
    ///
    /// Same conditions as [`encode_buffer`](Self::encode_buffer), plus
    /// [`BufferPoolError`](buffer_pool::BufferPoolError) propagated from the
    /// pool itself (pool closed, backpressure timeout, allocation failure).
    #[cfg(feature = "buffer-pool")]
    #[inline]
    pub fn encode_buffer_pooled(
        &self,
        pool: &buffer_pool::BufferPool,
        header: FrameHeader,
        payload: &[u8],
    ) -> Result<buffer_pool::Buffer, EncodePooledError> {
        self.check_len(payload.len())?;
        let needed = HEADER_SIZE + payload.len();
        let mut buf = pool.get(needed).map_err(EncodePooledError::Pool)?;
        // SAFETY: `encode_into_slice` only writes within `needed` bytes and
        // `buf` was just leased for at least `needed` bytes.
        let written = encode_into_slice(&mut buf, header, payload)?;
        debug_assert_eq!(written, needed);
        Ok(buf)
    }

    #[inline(always)]
    fn check_len(&self, len: usize) -> Result<(), FrameError> {
        if len > self.max_payload_len {
            return Err(FrameError::PayloadTooLarge {
                len,
                max: self.max_payload_len,
            });
        }
        if len > u32::MAX as usize {
            return Err(FrameError::PayloadTooLarge {
                len,
                max: u32::MAX as usize,
            });
        }
        Ok(())
    }
}

impl Default for FrameEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    // ---------- encode_into + decode round-trip ----------

    #[test]
    fn encode_then_decode_preserves_all_fields() {
        let mut buf = BytesMut::new();
        let header = FrameHeader::new(StreamId::new(0x0123_4567_89AB_CDEF), MessageType::TextToken)
            .with_seq(Seq::new(999))
            .with_flags(FrameFlags::FIN | FrameFlags::ACK_REQ);
        let payload = b"the quick brown fox";
        encode_into(&mut buf, header, payload).unwrap();

        let (frame, consumed) = decode_with_len(&buf).unwrap();
        assert_eq!(frame.stream_id, StreamId::new(0x0123_4567_89AB_CDEF));
        assert_eq!(frame.seq, Seq::new(999));
        assert_eq!(frame.msg_type, MessageType::TextToken);
        assert!(frame.flags.contains(FrameFlags::FIN));
        assert!(frame.flags.contains(FrameFlags::ACK_REQ));
        assert_eq!(frame.payload, payload);
        assert_eq!(consumed, HEADER_SIZE + payload.len());
        assert_eq!(frame.wire_size(), consumed);
    }

    #[test]
    fn encode_into_writes_big_endian_header() {
        let mut buf = BytesMut::new();
        let header = FrameHeader::new(StreamId::new(1), MessageType::Heartbeat)
            .with_seq(Seq::new(2))
            .with_flags(FrameFlags::FIN);
        encode_into(&mut buf, header, b"").unwrap();
        // stream_id u64 BE = 1
        assert_eq!(&buf[..8], &[0u8, 0, 0, 0, 0, 0, 0, 1]);
        // seq u64 BE = 2
        assert_eq!(&buf[8..16], &[0u8, 0, 0, 0, 0, 0, 0, 2]);
        // msg_type
        assert_eq!(buf[16], MessageType::Heartbeat.as_byte());
        // flags = FIN
        assert_eq!(buf[17], FrameFlags::FIN.bits());
        // payload_len u32 BE = 0
        assert_eq!(&buf[18..22], &[0u8, 0, 0, 0]);
        assert_eq!(buf.len(), HEADER_SIZE);
    }

    #[test]
    fn encode_into_reuses_preallocated_capacity() {
        // Pre-allocate exact capacity; encode_into must not grow the buffer.
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + 4);
        let header = FrameHeader::new(StreamId::new(1), MessageType::TextToken);
        encode_into(&mut buf, header, b"abcd").unwrap();
        assert_eq!(buf.len(), HEADER_SIZE + 4);
    }

    #[test]
    fn decode_rejects_truncated_header() {
        let short = [0u8; 10];
        let err = decode(&short).unwrap_err();
        assert!(matches!(
            err,
            FrameError::IncompleteHeader {
                needed: 22,
                have: 10
            }
        ));
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let mut buf = BytesMut::new();
        let header = FrameHeader::new(StreamId::new(1), MessageType::TextToken);
        encode_into(&mut buf, header, b"abc").unwrap();
        // Truncate the payload by one byte.
        let truncated = &buf[..buf.len() - 1];
        let err = decode(truncated).unwrap_err();
        assert!(matches!(
            err,
            FrameError::IncompletePayload {
                declared: 3,
                have: 2
            }
        ));
    }

    #[test]
    fn decode_rejects_unknown_message_type() {
        // Hand-craft a buffer with an invalid type byte (0x00) at offset 16.
        let mut buf = BytesMut::new();
        buf.put_u64(1); // stream_id
        buf.put_u64(0); // seq
        buf.put_u8(0x00); // invalid type
        buf.put_u8(0); // flags
        buf.put_u32(0); // length
        let err = decode(&buf).unwrap_err();
        assert!(matches!(
            err,
            FrameError::UnknownMessageType { value: 0x00 }
        ));
    }

    // ---------- exhaustive round-trip for every MessageType ----------

    #[test]
    fn roundtrip_all_message_types() {
        for mt in ALL_MESSAGE_TYPES_TEST {
            let payload: &[u8] = match mt {
                MessageType::Heartbeat => b"",
                MessageType::TextToken => b"hello",
                MessageType::AudioFrame => &[0xAA; 128],
                MessageType::VideoFrame => &[0xBB; 1024],
                _ => b"payload",
            };
            let header = FrameHeader::new(StreamId::new(7), mt)
                .with_seq(Seq::new(42))
                .with_flags(if mt.is_control() {
                    FrameFlags::URGENT
                } else {
                    FrameFlags::NONE
                });
            let mut buf = BytesMut::new();
            encode_into(&mut buf, header, payload).unwrap();
            let frame = decode(&buf).unwrap();
            assert_eq!(frame.msg_type, mt, "type {:?} did not round-trip", mt);
            assert_eq!(frame.stream_id, StreamId::new(7));
            assert_eq!(frame.seq, Seq::new(42));
            assert_eq!(frame.payload, payload);
        }
    }

    const ALL_MESSAGE_TYPES_TEST: [MessageType; 15] = crate::header::ALL_MESSAGE_TYPES;

    // ---------- seq ordering across frames ----------

    #[test]
    fn decoder_preserves_seq_ordering_within_stream() {
        let mut wire = BytesMut::new();
        for i in 0..5 {
            let header =
                FrameHeader::new(StreamId::new(1), MessageType::TextToken).with_seq(Seq::new(i));
            encode_into(&mut wire, header, &[i as u8; 4]).unwrap();
        }
        let mut dec = FrameDecoder::new();
        dec.push(&wire);
        let mut seqs = Vec::new();
        while let Some(frame) = dec.decode_frame().unwrap() {
            seqs.push(frame.seq);
        }
        assert_eq!(
            seqs,
            vec![
                Seq::new(0),
                Seq::new(1),
                Seq::new(2),
                Seq::new(3),
                Seq::new(4)
            ]
        );
    }

    #[test]
    fn seq_gap_detection() {
        // Two frames with seq 0 and seq 5 — gap of 4.
        let mut wire = BytesMut::new();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(1), MessageType::TextToken).with_seq(Seq::new(0)),
            b"a",
        )
        .unwrap();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(1), MessageType::TextToken).with_seq(Seq::new(5)),
            b"b",
        )
        .unwrap();
        let mut dec = FrameDecoder::new();
        dec.push(&wire);
        let f0 = dec.decode_frame().unwrap().unwrap();
        let f5 = dec.decode_frame().unwrap().unwrap();
        assert_eq!(f0.seq.gap_from(f5.seq), 0); // backward gap saturates at 0
        assert_eq!(f5.seq.gap_from(f0.seq), 5); // forward gap
    }

    // ---------- flags round-trip ----------

    #[test]
    fn all_flag_combinations_round_trip() {
        let combos = [
            FrameFlags::NONE,
            FrameFlags::FIN,
            FrameFlags::COMPRESSED,
            FrameFlags::FRAGMENT,
            FrameFlags::URGENT,
            FrameFlags::ACK_REQ,
            FrameFlags::FIN | FrameFlags::COMPRESSED,
            FrameFlags::FIN | FrameFlags::FRAGMENT | FrameFlags::URGENT,
            FrameFlags::from_bits(0xFF), // all bits (incl. reserved)
        ];
        for flags in combos {
            let mut buf = BytesMut::new();
            let header = FrameHeader::new(StreamId::new(1), MessageType::TextToken)
                .with_flags(flags)
                .with_seq(Seq::new(1));
            encode_into(&mut buf, header, b"x").unwrap();
            let frame = decode(&buf).unwrap();
            assert_eq!(frame.flags, flags, "flags {:?} did not round-trip", flags);
        }
    }

    // ---------- FrameDecoder streaming ----------

    #[test]
    fn decoder_yields_whole_frame_in_one_push() {
        let mut wire = BytesMut::new();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(1), MessageType::TextToken).with_seq(Seq::new(1)),
            b"hi",
        )
        .unwrap();
        let mut dec = FrameDecoder::new();
        dec.push(&wire);
        let frame = dec.decode_frame().unwrap().unwrap();
        assert_eq!(frame.stream_id, StreamId::new(1));
        assert_eq!(frame.seq, Seq::new(1));
        assert_eq!(frame.payload.as_ref(), b"hi");
        assert_eq!(dec.decode_frame().unwrap(), None);
    }

    #[test]
    fn decoder_yields_multiple_frames_in_one_push() {
        let mut wire = BytesMut::new();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(1), MessageType::TextToken).with_seq(Seq::new(0)),
            b"one",
        )
        .unwrap();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(2), MessageType::AudioFrame).with_seq(Seq::new(0)),
            b"two",
        )
        .unwrap();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(3), MessageType::Heartbeat).with_seq(Seq::new(0)),
            b"",
        )
        .unwrap();

        let mut dec = FrameDecoder::new();
        dec.push(&wire);

        let f1 = dec.decode_frame().unwrap().unwrap();
        assert_eq!(f1.stream_id, StreamId::new(1));
        assert_eq!(f1.payload.as_ref(), b"one");

        let f2 = dec.decode_frame().unwrap().unwrap();
        assert_eq!(f2.stream_id, StreamId::new(2));
        assert_eq!(f2.msg_type, MessageType::AudioFrame);

        let f3 = dec.decode_frame().unwrap().unwrap();
        assert_eq!(f3.msg_type, MessageType::Heartbeat);
        assert!(f3.payload.is_empty());

        assert_eq!(dec.decode_frame().unwrap(), None);
    }

    #[test]
    fn decoder_assembles_frame_across_pushes() {
        let mut wire = BytesMut::new();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(9), MessageType::VideoFrame).with_seq(Seq::new(3)),
            b"payload",
        )
        .unwrap();

        let mut dec = FrameDecoder::new();
        // Feed the first half — not enough for a complete frame.
        let split = HEADER_SIZE + 3;
        dec.push(&wire[..split]);
        assert_eq!(dec.decode_frame().unwrap(), None);

        // Feed the remainder.
        dec.push(&wire[split..]);
        let frame = dec.decode_frame().unwrap().unwrap();
        assert_eq!(frame.stream_id, StreamId::new(9));
        assert_eq!(frame.seq, Seq::new(3));
        assert_eq!(frame.payload.as_ref(), b"payload");
        assert_eq!(dec.decode_frame().unwrap(), None);
    }

    #[test]
    fn decoder_spare_capacity_zero_copy_path() {
        // Write a frame into the decoder via spare_capacity_mut + commit
        // instead of push — simulates the zero-copy QUIC recv path.
        let mut wire = BytesMut::new();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(5), MessageType::TextToken).with_seq(Seq::new(7)),
            b"zc",
        )
        .unwrap();

        let mut dec = FrameDecoder::new();
        let n = wire.len();
        let spare = dec.spare_capacity_mut(n);
        spare[..n].copy_from_slice(&wire);
        // SAFETY: we just initialised exactly n bytes above.
        unsafe { dec.commit(n) };

        let frame = dec.decode_frame().unwrap().unwrap();
        assert_eq!(frame.stream_id, StreamId::new(5));
        assert_eq!(frame.seq, Seq::new(7));
        assert_eq!(frame.payload.as_ref(), b"zc");
    }

    #[test]
    fn decoder_rejects_payload_over_cap() {
        let mut wire = BytesMut::new();
        // Declare a 1 MiB payload but supply nothing — the length alone trips the cap.
        wire.put_u64(1); // stream_id
        wire.put_u64(0); // seq
        wire.put_u8(MessageType::TextToken.as_byte());
        wire.put_u8(0); // flags
        wire.put_u32(1024 * 1024);

        let mut dec = FrameDecoder::new().max_payload_len(4096);
        dec.push(&wire);
        let err = dec.decode_frame().unwrap_err();
        assert!(matches!(
            err,
            FrameError::PayloadTooLarge {
                len: 1_048_576,
                max: 4096
            }
        ));
    }

    #[test]
    fn decoder_clear_resets_state() {
        let mut wire = BytesMut::new();
        // Bad frame: unknown type byte.
        wire.put_u64(1); // stream_id
        wire.put_u64(0); // seq
        wire.put_u8(0xEE); // not in the MessageType table
        wire.put_u8(0); // flags
        wire.put_u32(0);

        let mut dec = FrameDecoder::new();
        dec.push(&wire);
        assert!(dec.decode_frame().is_err());
        assert!(!dec.remaining().is_empty());

        dec.clear();
        assert!(dec.remaining().is_empty());
        assert_eq!(dec.decode_frame().unwrap(), None);
    }

    // ---------- FrameEncoder ----------

    #[test]
    fn encoder_encodes_correctly() {
        let enc = FrameEncoder::new();
        let header =
            FrameHeader::new(StreamId::new(1), MessageType::TextToken).with_seq(Seq::new(5));
        let out = enc.encode_buffer(header, b"abc").unwrap();
        assert_eq!(out.len(), HEADER_SIZE + 3);
        let frame = decode(&out).unwrap();
        assert_eq!(frame.stream_id, StreamId::new(1));
        assert_eq!(frame.seq, Seq::new(5));
        assert_eq!(frame.payload, b"abc");
    }

    #[test]
    fn encoder_rejects_oversized_payload() {
        let enc = FrameEncoder::new().max_payload_len(8);
        let big = vec![0u8; 9];
        let err = enc
            .encode_buffer(
                FrameHeader::new(StreamId::new(0), MessageType::Fragment),
                &big,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            FrameError::PayloadTooLarge { len: 9, max: 8 }
        ));
    }

    // ---------- encode_into_slice tests ----------

    #[test]
    fn encode_into_slice_writes_correctly() {
        let header =
            FrameHeader::new(StreamId::new(1), MessageType::TextToken).with_seq(Seq::new(5));
        let payload = b"hello slice";
        let mut buf = vec![0u8; HEADER_SIZE + payload.len()];
        let written = encode_into_slice(&mut buf, header, payload).unwrap();
        assert_eq!(written, HEADER_SIZE + payload.len());

        let frame = decode(&buf).unwrap();
        assert_eq!(frame.stream_id, StreamId::new(1));
        assert_eq!(frame.seq, Seq::new(5));
        assert_eq!(frame.payload, payload);
    }

    #[test]
    fn encode_into_slice_rejects_small_buffer() {
        let header = FrameHeader::new(StreamId::new(1), MessageType::TextToken);
        let payload = b"hello slice";
        let mut buf = vec![0u8; HEADER_SIZE + payload.len() - 1];
        let err = encode_into_slice(&mut buf, header, payload).unwrap_err();
        assert!(matches!(
            err,
            FrameError::BufferTooSmall { needed, have }
            if needed == HEADER_SIZE + payload.len() && have == HEADER_SIZE + payload.len() - 1
        ));
    }

    // ---------- encode_buffer_pooled tests ----------

    #[cfg(feature = "buffer-pool")]
    #[test]
    fn encode_buffer_pooled_round_trips() {
        let pool = buffer_pool::BufferPool::default();
        let enc = FrameEncoder::new();
        let header =
            FrameHeader::new(StreamId::new(2), MessageType::AudioFrame).with_seq(Seq::new(10));
        let payload = b"hello pooled codec";
        let buf = enc.encode_buffer_pooled(&pool, header, payload).unwrap();
        assert_eq!(buf.len(), HEADER_SIZE + payload.len());

        let frame = decode(&buf).unwrap();
        assert_eq!(frame.stream_id, StreamId::new(2));
        assert_eq!(frame.seq, Seq::new(10));
        assert_eq!(frame.payload, payload);
    }

    // ---------- zero-copy verification ----------

    #[test]
    fn decode_returns_borrow_into_input_buffer() {
        let mut wire = BytesMut::new();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(1), MessageType::TextToken),
            b"payload",
        )
        .unwrap();
        let frame = decode(&wire).unwrap();
        // The payload pointer must fall inside `wire`'s allocation.
        let wire_ptr = wire.as_ptr();
        let payload_ptr = frame.payload.as_ptr();
        let wire_end = unsafe { wire_ptr.add(wire.len()) };
        assert!(payload_ptr >= wire_ptr && payload_ptr < wire_end);
    }

    #[test]
    fn decoder_next_payload_shares_allocation() {
        let mut wire = BytesMut::new();
        encode_into(
            &mut wire,
            FrameHeader::new(StreamId::new(1), MessageType::TextToken),
            b"hello",
        )
        .unwrap();

        let mut dec = FrameDecoder::new();
        dec.push(&wire);
        let frame = dec.decode_frame().unwrap().unwrap();
        // frame_bytes was split off (header + payload in one allocation);
        // the payload slice is a ref-counted view, not a copy.
        assert_eq!(frame.payload.as_ref(), b"hello");
        // A cheap clone should share the allocation (same pointer).
        let cloned = frame.payload.clone();
        assert_eq!(cloned.as_ptr(), frame.payload.as_ptr());
    }
}
