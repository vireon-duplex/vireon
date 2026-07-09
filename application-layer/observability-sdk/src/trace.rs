//! [`TraceId`], [`SpanId`], [`SpanKind`] — distributed tracing identifiers.
//!
//! See `DESIGN.md` Part 6 (distributed tracing).
//!
//! # Design
//!
//! - `TraceId`: 128-bit identifier for an entire trace (distributed transaction).
//! - `SpanId`: 64-bit identifier for a single span within a trace.
//! - `SpanKind`: Classification of span type (server/client/internal/etc.).

// ── TraceId ─────────────────────────────────────────────────────

/// A distributed trace identifier (128-bit).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TraceId(u128);

impl TraceId {
    /// The nil/unset trace ID (all zeros).
    pub const NIL: Self = Self(0);

    /// Construct a new trace ID from a u128.
    #[must_use]
    pub const fn new(id: u128) -> Self {
        Self(id)
    }

    /// Construct the nil trace ID.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this trace ID is nil.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw u128 value.
    #[must_use]
    pub const fn raw(&self) -> u128 {
        self.0
    }

    /// Whether this trace ID is valid (non-nil and within valid range).
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        !self.is_nil()
    }
}

// ── SpanId ─────────────────────────────────────────────────────

/// A span identifier within a trace (64-bit).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SpanId(u64);

impl SpanId {
    /// The nil/unset span ID (all zeros).
    pub const NIL: Self = Self(0);

    /// Construct a new span ID from a u64.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil span ID.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this span ID is nil.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw u64 value.
    #[must_use]
    pub const fn raw(&self) -> u64 {
        self.0
    }

    /// Whether this span ID is valid (non-nil).
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        !self.is_nil()
    }
}

// ── SpanKind ────────────────────────────────────────────────────

/// The kind/span type classification.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SpanKind {
    /// Server span: received request.
    Server = 0,
    /// Client span: sending request.
    Client = 1,
    /// Internal span: local computation.
    Internal = 2,
    /// Producer span: publishing to a queue/topic.
    Producer = 3,
    /// Consumer span: consuming from a queue/topic.
    Consumer = 4,
}

impl SpanKind {
    /// Human-readable name for the span kind.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Client => "client",
            Self::Internal => "internal",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }

    /// Whether this span kind represents a remote operation.
    #[must_use]
    pub const fn is_remote(&self) -> bool {
        matches!(
            self,
            Self::Server | Self::Client | Self::Producer | Self::Consumer
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_trace_id() {
        let id = TraceId::nil();
        assert!(id.is_nil());
        assert!(!id.is_valid());
        assert_eq!(id.raw(), 0);
    }

    #[test]
    fn new_trace_id() {
        let id = TraceId::new(12345);
        assert!(!id.is_nil());
        assert!(id.is_valid());
        assert_eq!(id.raw(), 12345);
    }

    #[test]
    fn nil_span_id() {
        let id = SpanId::nil();
        assert!(id.is_nil());
        assert!(!id.is_valid());
        assert_eq!(id.raw(), 0);
    }

    #[test]
    fn new_span_id() {
        let id = SpanId::new(999);
        assert!(!id.is_nil());
        assert!(id.is_valid());
        assert_eq!(id.raw(), 999);
    }

    #[test]
    fn trace_id_copy() {
        let a = TraceId::new(42);
        let b = a;
        assert_eq!(a, b);
        assert_eq!(b.raw(), 42);
    }

    #[test]
    fn span_id_copy() {
        let a = SpanId::new(777);
        let b = a;
        assert_eq!(a, b);
        assert_eq!(b.raw(), 777);
    }

    #[test]
    fn span_kind_names() {
        assert_eq!(SpanKind::Server.as_str(), "server");
        assert_eq!(SpanKind::Client.as_str(), "client");
        assert_eq!(SpanKind::Internal.as_str(), "internal");
        assert_eq!(SpanKind::Producer.as_str(), "producer");
        assert_eq!(SpanKind::Consumer.as_str(), "consumer");
    }

    #[test]
    fn remote_span_kinds() {
        assert!(SpanKind::Server.is_remote());
        assert!(SpanKind::Client.is_remote());
        assert!(SpanKind::Producer.is_remote());
        assert!(SpanKind::Consumer.is_remote());
        assert!(!SpanKind::Internal.is_remote());
    }

    #[test]
    fn const_comparison_works() {
        // Test that we can compare in const contexts using raw().
        const A: TraceId = TraceId::new(1);
        const B: TraceId = TraceId::new(2);
        assert!(A.raw() < B.raw());

        const X: SpanId = SpanId::new(10);
        const Y: SpanId = SpanId::new(20);
        assert!(X.raw() < Y.raw());
    }
}
