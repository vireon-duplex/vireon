//! [`TokenChunk`], [`TokenType`] — token chunks for AI streaming.
//!
//! See `DESIGN.md` Part 5 (token model).

use std::borrow::Cow;

// ── TokenType ────────────────────────────────────────────────────

/// The type of a token in an AI stream.
///
/// Tokens can be text content, tool-call invocations, end-of-stream
/// markers, or error indicators.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum TokenType {
    /// A text token carrying generated text content.
    #[default]
    Text = 0,
    /// A tool-call token requesting tool execution.
    ToolCall = 1,
    /// End-of-stream marker: no more tokens will follow.
    EndOfStream = 2,
    /// Error token: the stream encountered an error.
    Error = 3,
}

impl TokenType {
    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::ToolCall => "tool_call",
            Self::EndOfStream => "end_of_stream",
            Self::Error => "error",
        }
    }
}

// ── TokenChunk ───────────────────────────────────────────────────

/// A chunk of tokens in an AI stream.
///
/// Token chunks are the atomic unit of streaming. Each chunk carries
/// a portion of generated text (or tool-call arguments) and a flag
/// indicating whether it is the final chunk in the stream.
///
/// The `text` field uses [`Cow<'a, str>`] so that callers who receive
/// frames from a byte buffer can pass `Cow::Borrowed` — zero allocation.
/// Callers who need to own the text (e.g. aggregators that store chunks)
/// can call [`TokenChunk::into_owned`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenChunk<'a> {
    /// Unique identifier for this token within the stream.
    pub token_id: u64,
    /// The text content of this token chunk.
    pub text: Cow<'a, str>,
    /// Whether this is the final token in the stream.
    pub is_final: bool,
}

impl<'a> TokenChunk<'a> {
    /// Create a new token chunk with the given ID and text.
    ///
    /// Accepts `impl Into<Cow<'a, str>>` so both `&str` (zero-alloc
    /// `Cow::Borrowed`) and `String` (`Cow::Owned`) work.
    #[must_use]
    pub fn new(token_id: u64, text: impl Into<Cow<'a, str>>) -> Self {
        Self {
            token_id,
            text: text.into(),
            is_final: false,
        }
    }

    /// Create a new final token chunk.
    #[must_use]
    pub fn new_final(token_id: u64, text: impl Into<Cow<'a, str>>) -> Self {
        Self {
            token_id,
            text: text.into(),
            is_final: true,
        }
    }

    /// Builder-style: set the final flag.
    #[must_use]
    pub fn with_final(mut self, is_final: bool) -> Self {
        self.is_final = is_final;
        self
    }

    /// Converts the chunk to `'static` lifetime, copying the text if
    /// it is currently borrowed.
    ///
    /// Use this when you need to store the chunk beyond the lifetime
    /// of the source buffer (e.g. in an aggregator).
    #[must_use]
    pub fn into_owned(self) -> TokenChunk<'static> {
        TokenChunk {
            token_id: self.token_id,
            text: Cow::Owned(self.text.into_owned()),
            is_final: self.is_final,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_type_default_is_text() {
        let tt = TokenType::default();
        assert_eq!(tt, TokenType::Text);
    }

    #[test]
    fn token_type_as_str() {
        assert_eq!(TokenType::Text.as_str(), "text");
        assert_eq!(TokenType::ToolCall.as_str(), "tool_call");
        assert_eq!(TokenType::EndOfStream.as_str(), "end_of_stream");
        assert_eq!(TokenType::Error.as_str(), "error");
    }

    #[test]
    fn token_type_repr_u8() {
        assert_eq!(TokenType::Text as u8, 0);
        assert_eq!(TokenType::ToolCall as u8, 1);
        assert_eq!(TokenType::EndOfStream as u8, 2);
        assert_eq!(TokenType::Error as u8, 3);
    }

    #[test]
    fn token_type_equality() {
        assert_eq!(TokenType::Text, TokenType::Text);
        assert_ne!(TokenType::Text, TokenType::ToolCall);
        assert_ne!(TokenType::ToolCall, TokenType::EndOfStream);
        assert_ne!(TokenType::EndOfStream, TokenType::Error);
    }

    #[test]
    fn token_type_all_variants_distinct() {
        let variants = [
            TokenType::Text,
            TokenType::ToolCall,
            TokenType::EndOfStream,
            TokenType::Error,
        ];
        let mut seen = std::collections::HashSet::new();
        for v in variants {
            assert!(seen.insert(v), "duplicate variant detected");
        }
    }

    #[test]
    fn token_type_copy_trait() {
        let t1 = TokenType::Error;
        let t2 = t1; // Copy, not Move
        assert_eq!(t1, t2);
    }

    #[test]
    fn token_chunk_new() {
        let chunk = TokenChunk::new(1, String::from("hello"));
        assert_eq!(chunk.token_id, 1);
        assert_eq!(chunk.text, "hello");
        assert!(!chunk.is_final);
    }

    #[test]
    fn token_chunk_new_final() {
        let chunk = TokenChunk::new_final(5, String::from("world"));
        assert_eq!(chunk.token_id, 5);
        assert_eq!(chunk.text, "world");
        assert!(chunk.is_final);
    }

    #[test]
    fn token_chunk_with_final() {
        let chunk = TokenChunk::new(1, String::from("hi")).with_final(true);
        assert!(chunk.is_final);
    }

    #[test]
    fn token_chunk_equality() {
        let c1 = TokenChunk::new(1, String::from("text"));
        let c2 = TokenChunk::new(1, String::from("text"));
        assert_eq!(c1, c2);
    }

    #[test]
    fn token_chunk_inequality() {
        let c1 = TokenChunk::new(1, String::from("text"));
        let c2 = TokenChunk::new(2, String::from("text"));
        let c3 = TokenChunk::new(1, String::from("other"));
        let c4 = TokenChunk::new_final(1, String::from("text"));
        assert_ne!(c1, c2);
        assert_ne!(c1, c3);
        assert_ne!(c1, c4);
    }

    #[test]
    fn token_chunk_empty_text() {
        let chunk = TokenChunk::new(0, String::new());
        assert_eq!(chunk.text, "");
        assert!(!chunk.is_final);
    }
}
