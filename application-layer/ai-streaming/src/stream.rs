//! [`AiStreamId`], [`StreamMode`] — stream identifiers and modes for
//! AI token streaming.
//!
//! See `DESIGN.md` Part 4 (stream model).

// ── AiStreamId ───────────────────────────────────────────────────

/// Unique identifier for an AI stream.
///
/// Streams are created when a client initiates a token generation
/// session. Each stream has a unique ID and persists until the
/// generation completes or the stream is closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AiStreamId(u64);

impl AiStreamId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new stream ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil stream ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil stream ID.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for AiStreamId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for AiStreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ai-stream#{}", self.0)
    }
}

// ── StreamMode ───────────────────────────────────────────────────

/// The mode of an AI stream, determining what types of tokens it
/// carries.
///
/// Following the LLM streaming pattern, streams can carry pure text
/// tokens, structured tool-call tokens, or a mix of both.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum StreamMode {
    /// Text-only streaming: the stream carries text tokens only.
    #[default]
    Text = 0,
    /// Tool-call streaming: the stream carries tool-call tokens only.
    ToolCalls = 1,
    /// Mixed streaming: the stream carries both text and tool-call tokens.
    Mixed = 2,
}

impl StreamMode {
    /// Whether this mode involves any streaming.
    ///
    /// All variants involve streaming, so this always returns `true`.
    #[must_use]
    pub const fn is_streaming(&self) -> bool {
        true
    }

    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::ToolCalls => "tool_calls",
            Self::Mixed => "mixed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_new() {
        let id = AiStreamId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn stream_id_nil() {
        let id = AiStreamId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn stream_id_nil_constant() {
        assert_eq!(AiStreamId::NIL, AiStreamId::nil());
        assert!(AiStreamId::NIL.is_nil());
    }

    #[test]
    fn stream_id_default_is_nil() {
        let id = AiStreamId::default();
        assert_eq!(id, AiStreamId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn stream_id_equality() {
        let id1 = AiStreamId::new(42);
        let id2 = AiStreamId::new(42);
        let id3 = AiStreamId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn stream_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(AiStreamId::new(1));
        set.insert(AiStreamId::new(2));
        set.insert(AiStreamId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn stream_id_ordering() {
        let a = AiStreamId::new(10);
        let b = AiStreamId::new(20);
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn stream_id_display() {
        let id = AiStreamId::new(42);
        let s = format!("{id}");
        assert!(s.contains("42"));
        assert!(s.contains("ai-stream"));
    }

    #[test]
    fn stream_id_raw_roundtrip() {
        let id = AiStreamId::new(12345);
        assert_eq!(AiStreamId::new(id.raw()), id);
    }

    #[test]
    fn stream_id_const_evaluation() {
        const ID: AiStreamId = AiStreamId::new(99);
        const NIL: AiStreamId = AiStreamId::nil();
        assert!(!ID.is_nil());
        assert!(NIL.is_nil());
        assert_eq!(ID.raw(), 99);
        assert_eq!(NIL.raw(), 0);
    }

    #[test]
    fn stream_mode_default_is_text() {
        let mode = StreamMode::default();
        assert_eq!(mode, StreamMode::Text);
    }

    #[test]
    fn stream_mode_is_streaming() {
        assert!(StreamMode::Text.is_streaming());
        assert!(StreamMode::ToolCalls.is_streaming());
        assert!(StreamMode::Mixed.is_streaming());
    }

    #[test]
    fn stream_mode_as_str() {
        assert_eq!(StreamMode::Text.as_str(), "text");
        assert_eq!(StreamMode::ToolCalls.as_str(), "tool_calls");
        assert_eq!(StreamMode::Mixed.as_str(), "mixed");
    }

    #[test]
    fn stream_mode_repr_u8() {
        assert_eq!(StreamMode::Text as u8, 0);
        assert_eq!(StreamMode::ToolCalls as u8, 1);
        assert_eq!(StreamMode::Mixed as u8, 2);
    }

    #[test]
    fn stream_mode_equality() {
        assert_eq!(StreamMode::Text, StreamMode::Text);
        assert_ne!(StreamMode::Text, StreamMode::ToolCalls);
        assert_ne!(StreamMode::ToolCalls, StreamMode::Mixed);
    }

    #[test]
    fn stream_mode_copy_trait() {
        let m1 = StreamMode::Mixed;
        let m2 = m1; // Copy, not Move
        assert_eq!(m1, m2);
    }
}
