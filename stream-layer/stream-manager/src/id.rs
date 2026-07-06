//! [`StreamId`] + [`StreamType`] — RFC 9000 §3.2 stream-id
//! encoding.
//!
//! A QUIC stream id is a 62-bit integer (the top two bits of a
//! `u64` are reserved). The low two bits encode type:
//!
//! ```text
//! bit 0:  0 = client-initiated,  1 = server-initiated
//! bit 1:  0 = bidirectional,     1 = unidirectional
//! ```
//!
//! giving four canonical types:
//!
//! | id     | type                          | first id |
//! |--------|-------------------------------|----------|
//! | `0x00` | client-initiated bidi         | 0, 4, 8, … |
//! | `0x01` | server-initiated bidi         | 1, 5, 9, … |
//! | `0x02` | client-initiated uni          | 2, 6, 10, … |
//! | `0x03` | server-initiated uni          | 3, 7, 11, … |
//!
//! All stream ids of the same type are spaced by 4 (RFC 9000
//! §3.2). The maximum stream count per type is therefore
//! `2^60 / 4 = 2^58` (vastly more than any deployment will use).

/// Top two bits of a `u64` stream id must be zero (62-bit id space).
pub const STREAM_ID_MAX: u64 = (1u64 << 62) - 1;

/// Stream-type classification derived from the low two bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StreamType {
    /// Client-initiated, bidirectional. Low bits = `0b00`.
    BidiClient = 0b00,
    /// Server-initiated, bidirectional. Low bits = `0b01`.
    BidiServer = 0b01,
    /// Client-initiated, unidirectional. Low bits = `0b10`.
    UniClient = 0b10,
    /// Server-initiated, unidirectional. Low bits = `0b11`.
    UniServer = 0b11,
}

impl StreamType {
    /// Decode from the low two bits of a stream id.
    #[must_use]
    pub const fn from_id(id: u64) -> Self {
        match (id & 0b11) as u8 {
            0b00 => Self::BidiClient,
            0b01 => Self::BidiServer,
            0b10 => Self::UniClient,
            _ => Self::UniServer,
        }
    }

    /// Whether this type is bidirectional (has both send + recv halves).
    #[must_use]
    pub const fn is_bidi(self) -> bool {
        matches!(self, Self::BidiClient | Self::BidiServer)
    }

    /// Whether this type is unidirectional (send-only at the initiator).
    #[must_use]
    pub const fn is_uni(self) -> bool {
        !self.is_bidi()
    }

    /// Whether the local peer is the initiator.
    #[must_use]
    pub const fn is_locally_initiated(self, is_client: bool) -> bool {
        match self {
            Self::BidiClient | Self::UniClient => is_client,
            Self::BidiServer | Self::UniServer => !is_client,
        }
    }
}

/// A QUIC stream identifier (RFC 9000 §3.2).
///
/// Wraps a `u64` so the type system can distinguish QUIC stream
/// ids from other u64 fields. The top two bits are reserved (must
/// be zero); the low two bits encode [`StreamType`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamId(pub u64);

impl StreamId {
    /// Construct from a raw `u64`. Returns `None` if the top two
    /// bits are set (out-of-band id).
    #[must_use]
    pub const fn new(id: u64) -> Option<Self> {
        if id <= STREAM_ID_MAX {
            Some(Self(id))
        } else {
            None
        }
    }

    /// Construct without validation. The caller must ensure `id`
    /// is within `STREAM_ID_MAX`.
    ///
    /// # Safety
    /// No memory-undefined behaviour — this is a logical safety
    /// contract: passing an id with top-two bits set violates
    /// RFC 9000 §3.2.
    #[must_use]
    pub const fn new_unchecked(id: u64) -> Self {
        Self(id)
    }

    /// The raw `u64` value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Whether bit 0 is clear (client-initiated).
    #[must_use]
    pub const fn is_client_initiated(self) -> bool {
        self.0 & 0b01 == 0
    }

    /// Whether bit 0 is set (server-initiated).
    #[must_use]
    pub const fn is_server_initiated(self) -> bool {
        !self.is_client_initiated()
    }

    /// Whether bit 1 is clear (bidirectional).
    #[must_use]
    pub const fn is_bidi(self) -> bool {
        self.0 & 0b10 == 0
    }

    /// Whether bit 1 is set (unidirectional).
    #[must_use]
    pub const fn is_uni(self) -> bool {
        !self.is_bidi()
    }

    /// Classified stream type.
    #[must_use]
    pub const fn stream_type(self) -> StreamType {
        StreamType::from_id(self.0)
    }

    /// The first stream id of this type (e.g. for the allocator).
    #[must_use]
    pub const fn first_of(ty: StreamType) -> Self {
        Self(ty as u8 as u64)
    }

    /// Compute the next id of the same type (offset by 4 per
    /// RFC 9000 §3.2). Returns `None` on overflow.
    #[must_use]
    pub const fn next_of_same_type(self) -> Option<Self> {
        match self.0.checked_add(4) {
            Some(n) if n <= STREAM_ID_MAX => Some(Self(n)),
            _ => None,
        }
    }

    /// Whether the local peer is the initiator of this stream.
    #[must_use]
    pub const fn is_locally_initiated(self, is_client: bool) -> bool {
        self.stream_type().is_locally_initiated(is_client)
    }
}

impl From<StreamId> for u64 {
    fn from(id: StreamId) -> Self {
        id.0
    }
}

impl core::fmt::Display for StreamId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "stream:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_decode_from_low_bits() {
        assert_eq!(StreamType::from_id(0), StreamType::BidiClient);
        assert_eq!(StreamType::from_id(1), StreamType::BidiServer);
        assert_eq!(StreamType::from_id(2), StreamType::UniClient);
        assert_eq!(StreamType::from_id(3), StreamType::UniServer);
        assert_eq!(StreamType::from_id(4), StreamType::BidiClient);
        assert_eq!(StreamType::from_id(7), StreamType::UniServer);
    }

    #[test]
    fn bidi_uni_classification() {
        assert!(StreamType::BidiClient.is_bidi());
        assert!(StreamType::BidiServer.is_bidi());
        assert!(!StreamType::UniClient.is_bidi());
        assert!(!StreamType::UniServer.is_bidi());
    }

    #[test]
    fn locally_initiated_by_client() {
        assert!(StreamType::BidiClient.is_locally_initiated(true));
        assert!(!StreamType::BidiServer.is_locally_initiated(true));
        assert!(StreamType::UniClient.is_locally_initiated(true));
        assert!(!StreamType::UniServer.is_locally_initiated(true));
    }

    #[test]
    fn stream_id_bit_predicates() {
        let c_bidi = StreamId(0);
        assert!(c_bidi.is_client_initiated());
        assert!(c_bidi.is_bidi());

        let s_uni = StreamId(3);
        assert!(s_uni.is_server_initiated());
        assert!(s_uni.is_uni());
    }

    #[test]
    fn first_of_each_type() {
        assert_eq!(StreamId::first_of(StreamType::BidiClient).raw(), 0);
        assert_eq!(StreamId::first_of(StreamType::BidiServer).raw(), 1);
        assert_eq!(StreamId::first_of(StreamType::UniClient).raw(), 2);
        assert_eq!(StreamId::first_of(StreamType::UniServer).raw(), 3);
    }

    #[test]
    fn next_of_same_type_advances_by_four() {
        let id = StreamId(4);
        assert_eq!(id.next_of_same_type(), Some(StreamId(8)));
        assert_eq!(StreamId(7).next_of_same_type(), Some(StreamId(11)));
    }

    #[test]
    fn next_of_same_type_overflow() {
        let id = StreamId(STREAM_ID_MAX);
        assert_eq!(id.next_of_same_type(), None);
    }

    #[test]
    fn new_rejects_top_bits() {
        assert!(StreamId::new(STREAM_ID_MAX).is_some());
        assert!(StreamId::new(STREAM_ID_MAX + 1).is_none());
        assert!(StreamId::new(u64::MAX).is_none());
    }

    #[test]
    fn display_uses_raw_value() {
        let s = format!("{}", StreamId(42));
        assert_eq!(s, "stream:42");
    }

    #[test]
    fn ordering_by_raw_value() {
        let mut ids = [StreamId(8), StreamId(0), StreamId(4)];
        ids.sort();
        assert_eq!(ids.map(|i| i.raw()), [0, 4, 8]);
    }
}
