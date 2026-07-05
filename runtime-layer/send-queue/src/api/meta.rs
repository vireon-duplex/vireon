//! Application-facing metadata types (Layer 12).
//!
//! [`PacketMeta`] is supplied by the application at submit time; [`PacketId`]
//! is returned for correlation and (future) cancellation.

/// Application-supplied metadata for a submitted packet.
#[derive(Clone, Copy, Debug, Default)]
pub struct PacketMeta {
    /// Priority (lower = higher priority). Used by PriorityPolicy.
    pub priority: u8,
    /// Absolute deadline in monotonic nanos. `0` means "no deadline"
    /// (best-effort). Used by DeadlinePolicy.
    pub deadline_ns: u64,
    /// Application stream / frame id. Used by WFQ and retry correlation.
    pub stream_id: u64,
}

impl PacketMeta {
    /// Creates a metadata struct with defaults (priority 0, no deadline).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the priority (lower = higher priority).
    #[must_use]
    pub fn priority(mut self, p: u8) -> Self {
        self.priority = p;
        self
    }

    /// Sets the absolute deadline (monotonic nanos).
    #[must_use]
    pub fn deadline(mut self, ns: u64) -> Self {
        self.deadline_ns = ns;
        self
    }

    /// Sets the stream / frame id.
    #[must_use]
    pub fn stream_id(mut self, id: u64) -> Self {
        self.stream_id = id;
        self
    }
}

/// Opaque identifier for a submitted packet.
///
/// Encodes `(tracking_slot_index, generation)` — see
/// [`encode_id`](crate::lifecycle::encode_id). The raw `u64` is passed
/// to the IO Layer as the `id` parameter; the IO Layer prepends
/// `OpKind` to produce the final `user_data`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PacketId(u64);

impl PacketId {
    /// Creates a packet id from its raw encoding.
    #[inline]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw `u64` representation.
    #[inline]
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_builder_methods() {
        let m = PacketMeta::new()
            .priority(5)
            .deadline(1_000_000)
            .stream_id(42);
        assert_eq!(m.priority, 5);
        assert_eq!(m.deadline_ns, 1_000_000);
        assert_eq!(m.stream_id, 42);
    }

    #[test]
    fn packet_id_round_trip() {
        let id = PacketId::from_raw(0x1234_5678);
        assert_eq!(id.raw(), 0x1234_5678);
    }

    #[test]
    fn packet_id_equality() {
        let a = PacketId::from_raw(42);
        let b = PacketId::from_raw(42);
        assert_eq!(a, b);
    }
}
