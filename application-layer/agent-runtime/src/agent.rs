//! [`AgentId`], [`AgentState`] — agent identity and lifecycle state.
//!
//! See `DESIGN.md` Part 3 (agent model).

// ── AgentId ──────────────────────────────────────────────────────

/// Unique identifier for an agent in the runtime.
///
/// Agents are created when a client registers an agent with the
/// runtime. Each agent has a unique ID and persists until it is
/// stopped. Zero (`NIL`) is reserved and never assigned to a real
/// agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AgentId(u64);

impl AgentId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new agent ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil agent ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil agent ID.
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

impl Default for AgentId {
    fn default() -> Self {
        Self::NIL
    }
}

impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "agent#{}", self.0)
    }
}

// ── AgentState ───────────────────────────────────────────────────

/// The lifecycle state of an agent.
///
/// Agents progress through Idle -> Running -> Waiting -> Stopped/Failed.
/// Once an agent reaches Stopped or Failed, it is terminal and will
/// not transition further.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum AgentState {
    /// The agent has been created but is not yet running.
    #[default]
    Idle = 0,
    /// The agent is actively processing.
    Running = 1,
    /// The agent is waiting for a tool call or external event.
    Waiting = 2,
    /// The agent has been stopped normally.
    Stopped = 3,
    /// The agent failed and is no longer running.
    Failed = 4,
}

impl AgentState {
    /// Whether this state is active (the agent is doing work or
    /// waiting for work).
    ///
    /// Returns `true` for `Running` and `Waiting`.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Running | Self::Waiting)
    }

    /// Whether this state is terminal (no further transitions).
    ///
    /// Returns `true` for `Stopped` and `Failed`.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }

    /// Human-readable name for logging and metrics.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_id_new() {
        let id = AgentId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn agent_id_nil() {
        let id = AgentId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn agent_id_nil_constant() {
        assert_eq!(AgentId::NIL, AgentId::nil());
        assert!(AgentId::NIL.is_nil());
    }

    #[test]
    fn agent_id_default_is_nil() {
        let id = AgentId::default();
        assert_eq!(id, AgentId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn agent_id_equality() {
        let id1 = AgentId::new(42);
        let id2 = AgentId::new(42);
        let id3 = AgentId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn agent_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(AgentId::new(1));
        set.insert(AgentId::new(2));
        set.insert(AgentId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn agent_id_ordering() {
        let a = AgentId::new(10);
        let b = AgentId::new(20);
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn agent_id_display() {
        let id = AgentId::new(42);
        let s = format!("{id}");
        assert!(s.contains("42"));
        assert!(s.contains("agent"));
    }

    #[test]
    fn agent_id_raw_roundtrip() {
        let id = AgentId::new(12345);
        assert_eq!(AgentId::new(id.raw()), id);
    }

    #[test]
    fn agent_id_const_evaluation() {
        const ID: AgentId = AgentId::new(99);
        const NIL: AgentId = AgentId::nil();
        assert!(!ID.is_nil());
        assert!(NIL.is_nil());
        assert_eq!(ID.raw(), 99);
        assert_eq!(NIL.raw(), 0);
    }

    #[test]
    fn agent_state_default_is_idle() {
        let state = AgentState::default();
        assert_eq!(state, AgentState::Idle);
    }

    #[test]
    fn agent_state_is_active() {
        assert!(!AgentState::Idle.is_active());
        assert!(AgentState::Running.is_active());
        assert!(AgentState::Waiting.is_active());
        assert!(!AgentState::Stopped.is_active());
        assert!(!AgentState::Failed.is_active());
    }

    #[test]
    fn agent_state_is_terminal() {
        assert!(!AgentState::Idle.is_terminal());
        assert!(!AgentState::Running.is_terminal());
        assert!(!AgentState::Waiting.is_terminal());
        assert!(AgentState::Stopped.is_terminal());
        assert!(AgentState::Failed.is_terminal());
    }

    #[test]
    fn agent_state_as_str() {
        assert_eq!(AgentState::Idle.as_str(), "idle");
        assert_eq!(AgentState::Running.as_str(), "running");
        assert_eq!(AgentState::Waiting.as_str(), "waiting");
        assert_eq!(AgentState::Stopped.as_str(), "stopped");
        assert_eq!(AgentState::Failed.as_str(), "failed");
    }

    #[test]
    fn agent_state_repr_u8() {
        assert_eq!(AgentState::Idle as u8, 0);
        assert_eq!(AgentState::Running as u8, 1);
        assert_eq!(AgentState::Waiting as u8, 2);
        assert_eq!(AgentState::Stopped as u8, 3);
        assert_eq!(AgentState::Failed as u8, 4);
    }

    #[test]
    fn agent_state_copy_trait() {
        let s1 = AgentState::Running;
        let s2 = s1; // Copy, not Move
        assert_eq!(s1, s2);
    }

    #[test]
    fn agent_state_all_variants_distinct() {
        let variants = [
            AgentState::Idle,
            AgentState::Running,
            AgentState::Waiting,
            AgentState::Stopped,
            AgentState::Failed,
        ];
        let mut seen = std::collections::HashSet::new();
        for v in variants {
            assert!(seen.insert(v), "duplicate variant detected");
        }
    }
}
