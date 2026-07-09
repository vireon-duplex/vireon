//! [`ToolCallId`], [`ToolCall`], [`ToolStatus`] — tool call types for
//! AI streaming.
//!
//! See `DESIGN.md` Part 6 (tool call model).

// ── ToolCallId ───────────────────────────────────────────────────

/// Unique identifier for a tool call within an AI stream.
///
/// Tool calls are issued by the AI model during streaming and are
/// tracked individually. Zero (`NIL`) is reserved and never assigned
/// to a real tool call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ToolCallId(u64);

impl ToolCallId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new tool call ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil tool call ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil tool call ID.
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

impl Default for ToolCallId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for ToolCallId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tool-call#{}", self.0)
    }
}

// ── ToolStatus ───────────────────────────────────────────────────

/// The status of a tool call in its execution lifecycle.
///
/// Tool calls progress through Pending -> Executing -> Completed/Failed.
/// Once a tool call reaches Completed or Failed, it is terminal and
/// will not transition further.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ToolStatus {
    /// The tool call has been issued but not yet started.
    #[default]
    Pending = 0,
    /// The tool call is currently executing.
    Executing = 1,
    /// The tool call completed successfully.
    Completed = 2,
    /// The tool call failed.
    Failed = 3,
}

impl ToolStatus {
    /// Whether this status is terminal (no further transitions).
    ///
    /// Returns `true` for `Completed` and `Failed`.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }

    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Executing => "executing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

// ── ToolCall ─────────────────────────────────────────────────────

/// A tool call issued by the AI model during streaming.
///
/// Tool calls carry the tool name and serialized arguments. The
/// execution status is tracked separately via [`ToolStatus`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolCall {
    /// Unique identifier for this tool call.
    pub id: ToolCallId,
    /// The name of the tool to invoke.
    pub name: String,
    /// Serialized arguments for the tool call (typically JSON).
    pub arguments: String,
}

impl ToolCall {
    /// Create a new tool call with the given ID, name, and arguments.
    #[must_use]
    pub fn new(id: ToolCallId, name: String, arguments: String) -> Self {
        Self {
            id,
            name,
            arguments,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_call_id_new() {
        let id = ToolCallId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn tool_call_id_nil() {
        let id = ToolCallId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn tool_call_id_nil_constant() {
        assert_eq!(ToolCallId::NIL, ToolCallId::nil());
        assert!(ToolCallId::NIL.is_nil());
    }

    #[test]
    fn tool_call_id_default_is_nil() {
        let id = ToolCallId::default();
        assert_eq!(id, ToolCallId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn tool_call_id_equality() {
        let id1 = ToolCallId::new(42);
        let id2 = ToolCallId::new(42);
        let id3 = ToolCallId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn tool_call_id_ordering() {
        let a = ToolCallId::new(10);
        let b = ToolCallId::new(20);
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn tool_call_id_display() {
        let id = ToolCallId::new(42);
        let s = format!("{id}");
        assert!(s.contains("42"));
        assert!(s.contains("tool-call"));
    }

    #[test]
    fn tool_call_id_raw_roundtrip() {
        let id = ToolCallId::new(999);
        assert_eq!(ToolCallId::new(id.raw()), id);
    }

    #[test]
    fn tool_status_default_is_pending() {
        let status = ToolStatus::default();
        assert_eq!(status, ToolStatus::Pending);
    }

    #[test]
    fn tool_status_is_terminal() {
        assert!(!ToolStatus::Pending.is_terminal());
        assert!(!ToolStatus::Executing.is_terminal());
        assert!(ToolStatus::Completed.is_terminal());
        assert!(ToolStatus::Failed.is_terminal());
    }

    #[test]
    fn tool_status_as_str() {
        assert_eq!(ToolStatus::Pending.as_str(), "pending");
        assert_eq!(ToolStatus::Executing.as_str(), "executing");
        assert_eq!(ToolStatus::Completed.as_str(), "completed");
        assert_eq!(ToolStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn tool_status_repr_u8() {
        assert_eq!(ToolStatus::Pending as u8, 0);
        assert_eq!(ToolStatus::Executing as u8, 1);
        assert_eq!(ToolStatus::Completed as u8, 2);
        assert_eq!(ToolStatus::Failed as u8, 3);
    }

    #[test]
    fn tool_status_equality() {
        assert_eq!(ToolStatus::Pending, ToolStatus::Pending);
        assert_ne!(ToolStatus::Pending, ToolStatus::Executing);
        assert_ne!(ToolStatus::Completed, ToolStatus::Failed);
    }

    #[test]
    fn tool_status_copy_trait() {
        let s1 = ToolStatus::Completed;
        let s2 = s1; // Copy, not Move
        assert_eq!(s1, s2);
    }

    #[test]
    fn tool_status_all_variants_distinct() {
        let variants = [
            ToolStatus::Pending,
            ToolStatus::Executing,
            ToolStatus::Completed,
            ToolStatus::Failed,
        ];
        let mut seen = std::collections::HashSet::new();
        for v in variants {
            assert!(seen.insert(v), "duplicate variant detected");
        }
    }

    #[test]
    fn tool_call_new() {
        let call = ToolCall::new(
            ToolCallId::new(1),
            String::from("search"),
            String::from(r#"{"query":"rust"}"#),
        );
        assert_eq!(call.id, ToolCallId::new(1));
        assert_eq!(call.name, "search");
        assert_eq!(call.arguments, r#"{"query":"rust"}"#);
    }

    #[test]
    fn tool_call_equality() {
        let c1 = ToolCall::new(
            ToolCallId::new(1),
            String::from("search"),
            String::from("{}"),
        );
        let c2 = ToolCall::new(
            ToolCallId::new(1),
            String::from("search"),
            String::from("{}"),
        );
        assert_eq!(c1, c2);
    }

    #[test]
    fn tool_call_inequality() {
        let c1 = ToolCall::new(
            ToolCallId::new(1),
            String::from("search"),
            String::from("{}"),
        );
        let c2 = ToolCall::new(
            ToolCallId::new(2),
            String::from("search"),
            String::from("{}"),
        );
        let c3 = ToolCall::new(ToolCallId::new(1), String::from("calc"), String::from("{}"));
        assert_ne!(c1, c2);
        assert_ne!(c1, c3);
    }
}
