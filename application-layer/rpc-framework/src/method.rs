//! [`RpcMethodId`], [`RpcMethodType`] — RPC method identifiers and
//! streaming modes.
//!
//! See `DESIGN.md` Part 6 (method registry).

// ── RpcMethodId ───────────────────────────────────────────────────

/// Unique identifier for an RPC method.
///
/// Methods are registered with the server during startup and clients
/// invoke them by ID. Zero (`NIL`) is reserved and never assigned to
/// a real method.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RpcMethodId(u32);

impl RpcMethodId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new method ID.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Construct the nil method ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil method ID.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl Default for RpcMethodId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for RpcMethodId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "method#{}", self.0)
    }
}

// ── RpcMethodType ────────────────────────────────────────────────

/// The streaming mode of an RPC method.
///
/// Each RPC method can be one of four streaming modes, following the
/// gRPC pattern.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RpcMethodType {
    /// Unary: one request, one response.
    #[default]
    Unary = 0,
    /// Server streaming: one request, multiple responses.
    ServerStreaming = 1,
    /// Client streaming: multiple requests, one response.
    ClientStreaming = 2,
    /// Bidirectional streaming: multiple requests, multiple responses.
    BidiStreaming = 3,
}

impl RpcMethodType {
    /// Whether this method type involves any streaming.
    ///
    /// Returns `true` for all non-unary modes.
    #[must_use]
    pub const fn is_streaming(&self) -> bool {
        !matches!(self, Self::Unary)
    }

    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unary => "unary",
            Self::ServerStreaming => "server_streaming",
            Self::ClientStreaming => "client_streaming",
            Self::BidiStreaming => "bidi_streaming",
        }
    }

    /// Whether the server streams responses to the client.
    #[must_use]
    pub const fn server_streams(&self) -> bool {
        matches!(self, Self::ServerStreaming | Self::BidiStreaming)
    }

    /// Whether the client streams requests to the server.
    #[must_use]
    pub const fn client_streams(&self) -> bool {
        matches!(self, Self::ClientStreaming | Self::BidiStreaming)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_id_new() {
        let id = RpcMethodId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn method_id_nil() {
        let id = RpcMethodId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn method_id_nil_constant() {
        assert_eq!(RpcMethodId::NIL, RpcMethodId::nil());
        assert!(RpcMethodId::NIL.is_nil());
    }

    #[test]
    fn method_id_default_is_nil() {
        let id = RpcMethodId::default();
        assert_eq!(id, RpcMethodId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn method_id_equality() {
        let id1 = RpcMethodId::new(42);
        let id2 = RpcMethodId::new(42);
        let id3 = RpcMethodId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn method_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RpcMethodId::new(1));
        set.insert(RpcMethodId::new(2));
        set.insert(RpcMethodId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn method_type_default_is_unary() {
        let mt = RpcMethodType::default();
        assert_eq!(mt, RpcMethodType::Unary);
    }

    #[test]
    fn method_type_is_streaming() {
        assert!(!RpcMethodType::Unary.is_streaming());
        assert!(RpcMethodType::ServerStreaming.is_streaming());
        assert!(RpcMethodType::ClientStreaming.is_streaming());
        assert!(RpcMethodType::BidiStreaming.is_streaming());
    }

    #[test]
    fn method_type_as_str() {
        assert_eq!(RpcMethodType::Unary.as_str(), "unary");
        assert_eq!(RpcMethodType::ServerStreaming.as_str(), "server_streaming");
        assert_eq!(RpcMethodType::ClientStreaming.as_str(), "client_streaming");
        assert_eq!(RpcMethodType::BidiStreaming.as_str(), "bidi_streaming");
    }

    #[test]
    fn method_type_server_streams() {
        assert!(!RpcMethodType::Unary.server_streams());
        assert!(RpcMethodType::ServerStreaming.server_streams());
        assert!(!RpcMethodType::ClientStreaming.server_streams());
        assert!(RpcMethodType::BidiStreaming.server_streams());
    }

    #[test]
    fn method_type_client_streams() {
        assert!(!RpcMethodType::Unary.client_streams());
        assert!(!RpcMethodType::ServerStreaming.client_streams());
        assert!(RpcMethodType::ClientStreaming.client_streams());
        assert!(RpcMethodType::BidiStreaming.client_streams());
    }

    #[test]
    fn method_type_equality() {
        assert_eq!(RpcMethodType::Unary, RpcMethodType::Unary);
        assert_ne!(RpcMethodType::Unary, RpcMethodType::ServerStreaming);
    }

    #[test]
    fn method_type_all_variants_distinct() {
        let variants = [
            RpcMethodType::Unary,
            RpcMethodType::ServerStreaming,
            RpcMethodType::ClientStreaming,
            RpcMethodType::BidiStreaming,
        ];
        let mut seen = std::collections::HashSet::new();
        for v in variants {
            assert!(seen.insert(v), "duplicate variant detected");
        }
    }

    #[test]
    fn method_type_repr_u8() {
        // Ensure the repr is as expected for FFI/serialization.
        assert_eq!(RpcMethodType::Unary as u8, 0);
        assert_eq!(RpcMethodType::ServerStreaming as u8, 1);
        assert_eq!(RpcMethodType::ClientStreaming as u8, 2);
        assert_eq!(RpcMethodType::BidiStreaming as u8, 3);
    }

    #[test]
    fn method_id_raw_roundtrip() {
        let id = RpcMethodId::new(123);
        assert_eq!(RpcMethodId::new(id.raw()), id);
    }

    #[test]
    fn method_type_bidi_streams_both_directions() {
        let mt = RpcMethodType::BidiStreaming;
        assert!(mt.server_streams());
        assert!(mt.client_streams());
    }

    #[test]
    fn method_type_unary_streams_neither_direction() {
        let mt = RpcMethodType::Unary;
        assert!(!mt.server_streams());
        assert!(!mt.client_streams());
    }
}
