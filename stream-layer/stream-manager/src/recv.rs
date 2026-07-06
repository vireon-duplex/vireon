//! [`RecvBuffer`] — per-stream inbound byte reassembly buffer.
//!
//! See `DESIGN.md` Part 6 (receive side).
//!
//! # Responsibilities (Phase 2)
//!
//! QUIC delivers STREAM frames in any order; the recv buffer's job
//! is to:
//!
//! 1. **Accept** out-of-order byte ranges keyed by offset.
//! 2. **Track gaps** — bytes the peer has sent but not yet arrived
//!    (or that were lost and need retransmit).
//! 3. **Reassemble** — once a contiguous prefix from `recv_offset`
//!    onward is filled, surface it to the application.
//! 4. **Enforce flow control** — reject frames that exceed
//!    `max_stream_data` (RFC 9000 §4.1).
//! 5. **Detect final size** — FIN or RESET_STREAM carries the final
//!    size; further data after that is a protocol violation.
//!
//! # Data structure sketch
//!
//! Phase 2 will use a slab of `Bytes` slices indexed by offset, with
//! a sorted gap list (smallvec of `(start, end)` ranges). Phase 0
//! ships only the field shape.
//!
//! # Phase 0 status
//!
//! Field shape + accessor stubs. Functional `ingest` / `read` land
//! in Phase 2.

use crate::error::StreamManagerError;

/// Maximum bytes a single stream may buffer (recv side). Bounded by
/// the peer's `max_stream_data` window.
pub const RECV_BUFFER_MAX_BYTES: u64 = 1024 * 1024;

/// Per-stream receive reassembly buffer. Phase 0 placeholder.
// TODO(phase-2): back this with `Bytes` slices + gap list.
#[derive(Clone, Debug, Default)]
pub struct RecvBuffer {
    /// Next byte offset to deliver to the application.
    read_offset: u64,
    /// Highest contiguous offset we can deliver.
    contiguous_offset: u64,
    /// Highest byte offset the peer has sent (including gaps).
    max_seen_offset: u64,
    /// Final size once FIN / RESET_STREAM received.
    final_size: Option<u64>,
}

impl RecvBuffer {
    /// Construct an empty buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read_offset: 0,
            contiguous_offset: 0,
            max_seen_offset: 0,
            final_size: None,
        }
    }

    /// Bytes ready to deliver to the application (contiguous).
    #[must_use]
    pub const fn readable(&self) -> u64 {
        self.contiguous_offset.saturating_sub(self.read_offset)
    }

    /// Bytes the peer has sent but we cannot yet deliver (in gaps).
    #[must_use]
    pub const fn holes(&self) -> u64 {
        self.max_seen_offset.saturating_sub(self.contiguous_offset)
    }

    /// Whether the final size is known.
    #[must_use]
    pub const fn final_size_known(&self) -> bool {
        self.final_size.is_some()
    }

    /// Phase-2 `ingest` placeholder.
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn ingest(
        &mut self,
        _offset: u64,
        _bytes: &[u8],
        _fin: bool,
    ) -> Result<(), StreamManagerError> {
        // TODO(phase-2): insert into gap list, coalesce, bump offsets.
        Err(StreamManagerError::ResourceExhausted(
            "recv buffer phase-2 stub",
        ))
    }

    /// Phase-2 `read` placeholder.
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn read(&mut self, _out: &mut [u8]) -> Result<u64, StreamManagerError> {
        // TODO(phase-2): copy contiguous bytes, bump read_offset.
        Err(StreamManagerError::ResourceExhausted(
            "recv buffer phase-2 stub",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_empty() {
        let b = RecvBuffer::default();
        assert_eq!(b.readable(), 0);
        assert_eq!(b.holes(), 0);
        assert!(!b.final_size_known());
    }

    #[test]
    fn new_matches_default() {
        assert_eq!(RecvBuffer::new().readable(), 0);
    }
}
