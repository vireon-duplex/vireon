//! [`ToolId`], [`ToolDescriptor`], [`ToolResult`] — tool types for
//! the agent runtime.
//!
//! See `DESIGN.md` Part 6 (tool model).

// ── ToolId ───────────────────────────────────────────────────────

/// Unique identifier for a tool registered with the agent runtime.
///
/// Tools are registered when an agent is configured. Each tool has
/// a unique ID, a name, and a timeout. Zero (`NIL`) is reserved and
/// never assigned to a real tool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ToolId(u64);

impl ToolId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new tool ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil tool ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil tool ID.
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

impl Default for ToolId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for ToolId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tool#{}", self.0)
    }
}

// ── ToolDescriptor ───────────────────────────────────────────────

/// Static descriptor for a tool available to agents.
///
/// Tool descriptors are registered when an agent is created and
/// describe the tool's identity, name, and execution timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ToolDescriptor {
    /// Unique identifier for this tool.
    pub id: ToolId,
    /// The name of the tool (a static string identifier).
    pub name: &'static str,
    /// Timeout for tool execution in milliseconds.
    pub timeout_ms: u32,
}

impl ToolDescriptor {
    /// Default tool timeout (30 seconds).
    pub const DEFAULT_TIMEOUT_MS: u32 = 30_000;

    /// Create a new tool descriptor with the given ID and name.
    ///
    /// Uses the default timeout of 30 seconds.
    #[must_use]
    pub fn new(id: ToolId, name: &'static str) -> Self {
        Self {
            id,
            name,
            timeout_ms: Self::DEFAULT_TIMEOUT_MS,
        }
    }

    /// Builder-style: set the timeout in milliseconds.
    #[must_use]
    pub const fn with_timeout_ms(mut self, ms: u32) -> Self {
        self.timeout_ms = ms;
        self
    }
}

// ── ToolResult ───────────────────────────────────────────────────

/// The result of a tool execution.
///
/// Carries the tool ID, success/failure status, and the output
/// string (typically serialized JSON or plain text).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolResult {
    /// The ID of the tool that was executed.
    pub tool_id: ToolId,
    /// Whether the tool execution succeeded.
    pub success: bool,
    /// The output of the tool execution (serialized result or error message).
    pub output: String,
}

impl ToolResult {
    /// Create a successful tool result.
    #[must_use]
    pub fn success(tool_id: ToolId, output: String) -> Self {
        Self {
            tool_id,
            success: true,
            output,
        }
    }

    /// Create a failed tool result.
    #[must_use]
    pub fn failure(tool_id: ToolId, output: String) -> Self {
        Self {
            tool_id,
            success: false,
            output,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_id_new() {
        let id = ToolId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn tool_id_nil() {
        let id = ToolId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn tool_id_nil_constant() {
        assert_eq!(ToolId::NIL, ToolId::nil());
        assert!(ToolId::NIL.is_nil());
    }

    #[test]
    fn tool_id_default_is_nil() {
        let id = ToolId::default();
        assert_eq!(id, ToolId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn tool_id_equality() {
        let id1 = ToolId::new(42);
        let id2 = ToolId::new(42);
        let id3 = ToolId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn tool_id_ordering() {
        let a = ToolId::new(10);
        let b = ToolId::new(20);
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn tool_id_display() {
        let id = ToolId::new(42);
        let s = format!("{id}");
        assert!(s.contains("42"));
        assert!(s.contains("tool"));
    }

    #[test]
    fn tool_id_raw_roundtrip() {
        let id = ToolId::new(999);
        assert_eq!(ToolId::new(id.raw()), id);
    }

    #[test]
    fn tool_descriptor_new() {
        let desc = ToolDescriptor::new(ToolId::new(1), "search");
        assert_eq!(desc.id, ToolId::new(1));
        assert_eq!(desc.name, "search");
        assert_eq!(desc.timeout_ms, ToolDescriptor::DEFAULT_TIMEOUT_MS);
    }

    #[test]
    fn tool_descriptor_with_timeout() {
        let desc = ToolDescriptor::new(ToolId::new(2), "calc").with_timeout_ms(5000);
        assert_eq!(desc.timeout_ms, 5000);
    }

    #[test]
    fn tool_descriptor_equality() {
        let d1 = ToolDescriptor::new(ToolId::new(1), "search");
        let d2 = ToolDescriptor::new(ToolId::new(1), "search");
        assert_eq!(d1, d2);
    }

    #[test]
    fn tool_descriptor_inequality() {
        let d1 = ToolDescriptor::new(ToolId::new(1), "search");
        let d2 = ToolDescriptor::new(ToolId::new(2), "search");
        let d3 = ToolDescriptor::new(ToolId::new(1), "calc");
        assert_ne!(d1, d2);
        assert_ne!(d1, d3);
    }

    #[test]
    fn tool_result_success() {
        let result = ToolResult::success(ToolId::new(1), String::from("ok"));
        assert_eq!(result.tool_id, ToolId::new(1));
        assert!(result.success);
        assert_eq!(result.output, "ok");
    }

    #[test]
    fn tool_result_failure() {
        let result = ToolResult::failure(ToolId::new(2), String::from("err"));
        assert_eq!(result.tool_id, ToolId::new(2));
        assert!(!result.success);
        assert_eq!(result.output, "err");
    }

    #[test]
    fn tool_result_equality() {
        let r1 = ToolResult::success(ToolId::new(1), String::from("ok"));
        let r2 = ToolResult::success(ToolId::new(1), String::from("ok"));
        assert_eq!(r1, r2);
    }

    #[test]
    fn tool_result_inequality() {
        let r1 = ToolResult::success(ToolId::new(1), String::from("ok"));
        let r2 = ToolResult::failure(ToolId::new(1), String::from("ok"));
        let r3 = ToolResult::success(ToolId::new(2), String::from("ok"));
        assert_ne!(r1, r2);
        assert_ne!(r1, r3);
    }
}
