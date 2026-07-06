//! [`StreamIdAllocator`] — RFC 9000 §3.2 stream-id allocation.
//!
//! See `DESIGN.md` Part 4 (ID allocation).
//!
//! # Allocation rules
//!
//! For each of the four stream types
//! ([`StreamType`](crate::id::StreamType)), the allocator tracks the
//! next id to hand out. Ids of the same type are spaced by 4:
//!
//! ```text
//! client-bidi: 0, 4, 8, 12, …
//! server-bidi: 1, 5, 9, 13, …
//! client-uni:  2, 6, 10, 14, …
//! server-uni:  3, 7, 11, 15, …
//! ```
//!
//! The allocator only hands out locally-initiated ids — it never
//! allocates a server-bidi id on the client. Peer-initiated streams
//! are accepted (validated against `MAX_STREAMS`) rather than
//! allocated.
//!
//! # Phase 0 status
//!
//! Phase 0 ships the counter shape; the functional
//! `allocate_bidi` / `allocate_uni` (which consult the connection's
//! `MAX_STREAMS` budget and bump the counter atomically) land in
//! Phase 1.

use crate::error::StreamManagerError;
use crate::id::{STREAM_ID_MAX, StreamId, StreamType};

/// Per-allocator state. One allocator per connection (per peer).
#[derive(Clone, Debug)]
pub struct StreamIdAllocator {
    /// Whether the local peer is the QUIC client.
    is_client: bool,
    /// Next client-initiated bidi id to hand out.
    next_bidi_client: u64,
    /// Next server-initiated bidi id to hand out.
    next_bidi_server: u64,
    /// Next client-initiated uni id to hand out.
    next_uni_client: u64,
    /// Next server-initiated uni id to hand out.
    next_uni_server: u64,
}

impl StreamIdAllocator {
    /// Construct an allocator. `is_client` selects which half of the
    /// id space the local peer allocates from.
    #[must_use]
    pub const fn new(is_client: bool) -> Self {
        Self {
            is_client,
            next_bidi_client: StreamType::BidiClient as u8 as u64,
            next_bidi_server: StreamType::BidiServer as u8 as u64,
            next_uni_client: StreamType::UniClient as u8 as u64,
            next_uni_server: StreamType::UniServer as u8 as u64,
        }
    }

    /// Whether the local peer is the QUIC client.
    #[must_use]
    pub const fn is_client(&self) -> bool {
        self.is_client
    }

    /// Peek at the next id the allocator would hand out for the
    /// given type without advancing the counter.
    #[must_use]
    pub const fn peek(&self, ty: StreamType) -> StreamId {
        let raw = match ty {
            StreamType::BidiClient => self.next_bidi_client,
            StreamType::BidiServer => self.next_bidi_server,
            StreamType::UniClient => self.next_uni_client,
            StreamType::UniServer => self.next_uni_server,
        };
        StreamId::new_unchecked(raw)
    }

    /// Phase-0 placeholder. Phase 1 will validate the budget against
    /// `MAX_STREAMS` and advance the appropriate counter.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::StreamExhausted`] on id-space
    /// overflow (exceeds `STREAM_ID_MAX`).
    pub fn allocate_bidi(&mut self) -> Result<StreamId, StreamManagerError> {
        let ty = if self.is_client {
            StreamType::BidiClient
        } else {
            StreamType::BidiServer
        };
        self.advance(ty)
    }

    /// Phase-0 placeholder. Phase 1 will validate the budget against
    /// `MAX_STREAMS` and advance the appropriate counter.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::StreamExhausted`] on id-space
    /// overflow (exceeds `STREAM_ID_MAX`).
    pub fn allocate_uni(&mut self) -> Result<StreamId, StreamManagerError> {
        let ty = if self.is_client {
            StreamType::UniClient
        } else {
            StreamType::UniServer
        };
        self.advance(ty)
    }

    fn advance(&mut self, ty: StreamType) -> Result<StreamId, StreamManagerError> {
        let slot = match ty {
            StreamType::BidiClient => &mut self.next_bidi_client,
            StreamType::BidiServer => &mut self.next_bidi_server,
            StreamType::UniClient => &mut self.next_uni_client,
            StreamType::UniServer => &mut self.next_uni_server,
        };
        let id = *slot;
        if id > STREAM_ID_MAX {
            return Err(StreamManagerError::StreamExhausted(ty));
        }
        *slot = id
            .checked_add(4)
            .ok_or(StreamManagerError::StreamExhausted(ty))?;
        Ok(StreamId::new_unchecked(id))
    }
}

impl Default for StreamIdAllocator {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_starts_at_zero() {
        let a = StreamIdAllocator::new(true);
        assert_eq!(a.peek(StreamType::BidiClient).raw(), 0);
        assert_eq!(a.peek(StreamType::UniClient).raw(), 2);
    }

    #[test]
    fn server_starts_at_one() {
        let a = StreamIdAllocator::new(false);
        assert_eq!(a.peek(StreamType::BidiServer).raw(), 1);
        assert_eq!(a.peek(StreamType::UniServer).raw(), 3);
    }

    #[test]
    fn allocate_bidi_advances_by_four() {
        let mut a = StreamIdAllocator::new(true);
        let id0 = a.allocate_bidi().expect("first");
        let id1 = a.allocate_bidi().expect("second");
        assert_eq!(id0.raw(), 0);
        assert_eq!(id1.raw(), 4);
    }

    #[test]
    fn allocate_uni_uses_correct_type() {
        let mut a = StreamIdAllocator::new(true);
        let id = a.allocate_uni().expect("uni");
        assert_eq!(id.raw(), 2);
        assert_eq!(id.stream_type(), StreamType::UniClient);
    }
}
