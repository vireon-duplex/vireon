//! [`AgentContext`] — per-agent execution context with token budget
//! tracking.
//!
//! See `DESIGN.md` Part 7 (context model).

use crate::agent::AgentId;
use session_manager::SessionId;

// ── AgentContext ─────────────────────────────────────────────────

/// Per-agent execution context.
///
/// Tracks the agent's identity, associated session, and token usage.
/// The token budget is enforced via [`AgentContext::use_tokens`],
/// which saturates at the limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentContext {
    /// The agent this context belongs to.
    pub agent_id: AgentId,
    /// The session associated with this agent.
    pub session_id: SessionId,
    /// Tokens consumed so far.
    pub tokens_used: u64,
    /// Maximum tokens allowed for this context.
    pub tokens_limit: u64,
}

impl AgentContext {
    /// Default token limit (8192 tokens).
    pub const DEFAULT_TOKEN_LIMIT: u64 = 8_192;

    /// Create a new agent context with the default token limit.
    #[must_use]
    pub fn new(agent_id: AgentId, session_id: SessionId) -> Self {
        Self {
            agent_id,
            session_id,
            tokens_used: 0,
            tokens_limit: Self::DEFAULT_TOKEN_LIMIT,
        }
    }

    /// Builder-style: set the token limit.
    #[must_use]
    pub const fn with_token_limit(mut self, limit: u64) -> Self {
        self.tokens_limit = limit;
        self
    }

    /// Remaining tokens in the budget.
    #[must_use]
    pub const fn remaining_tokens(&self) -> u64 {
        self.tokens_limit.saturating_sub(self.tokens_used)
    }

    /// Consume tokens from the budget.
    ///
    /// Saturates at the limit — if `n` would exceed the remaining
    /// budget, `tokens_used` is set to `tokens_limit`.
    pub fn use_tokens(&mut self, n: u64) {
        self.tokens_used = self.tokens_used.saturating_add(n);
        if self.tokens_used > self.tokens_limit {
            self.tokens_used = self.tokens_limit;
        }
    }

    /// Whether the token budget is exhausted.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.tokens_used >= self.tokens_limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session_id() -> SessionId {
        SessionId::from_pair(1, 1)
    }

    #[test]
    fn new_context_has_defaults() {
        let ctx = AgentContext::new(AgentId::new(1), test_session_id());
        assert_eq!(ctx.agent_id, AgentId::new(1));
        assert_eq!(ctx.tokens_used, 0);
        assert_eq!(ctx.tokens_limit, AgentContext::DEFAULT_TOKEN_LIMIT);
    }

    #[test]
    fn with_token_limit() {
        let ctx = AgentContext::new(AgentId::new(1), test_session_id()).with_token_limit(10_000);
        assert_eq!(ctx.tokens_limit, 10_000);
    }

    #[test]
    fn remaining_tokens_initial() {
        let ctx = AgentContext::new(AgentId::new(1), test_session_id());
        assert_eq!(ctx.remaining_tokens(), AgentContext::DEFAULT_TOKEN_LIMIT);
    }

    #[test]
    fn use_tokens_decrements_remaining() {
        let mut ctx = AgentContext::new(AgentId::new(1), test_session_id());
        ctx.use_tokens(100);
        assert_eq!(ctx.tokens_used, 100);
        assert_eq!(
            ctx.remaining_tokens(),
            AgentContext::DEFAULT_TOKEN_LIMIT - 100
        );
    }

    #[test]
    fn use_tokens_saturates_at_limit() {
        let mut ctx = AgentContext::new(AgentId::new(1), test_session_id()).with_token_limit(1000);
        ctx.use_tokens(600);
        assert_eq!(ctx.tokens_used, 600);
        ctx.use_tokens(600);
        assert_eq!(ctx.tokens_used, 1000);
        assert_eq!(ctx.remaining_tokens(), 0);
    }

    #[test]
    fn is_exhausted_false_initially() {
        let ctx = AgentContext::new(AgentId::new(1), test_session_id());
        assert!(!ctx.is_exhausted());
    }

    #[test]
    fn is_exhausted_true_at_limit() {
        let mut ctx = AgentContext::new(AgentId::new(1), test_session_id()).with_token_limit(100);
        ctx.use_tokens(100);
        assert!(ctx.is_exhausted());
    }

    #[test]
    fn is_exhausted_true_over_limit() {
        let mut ctx = AgentContext::new(AgentId::new(1), test_session_id()).with_token_limit(100);
        ctx.use_tokens(200);
        assert!(ctx.is_exhausted());
        assert_eq!(ctx.tokens_used, 100);
    }

    #[test]
    fn remaining_tokens_never_underflows() {
        let mut ctx = AgentContext::new(AgentId::new(1), test_session_id()).with_token_limit(50);
        ctx.use_tokens(100);
        assert_eq!(ctx.remaining_tokens(), 0);
    }

    #[test]
    fn use_tokens_accumulates() {
        let mut ctx = AgentContext::new(AgentId::new(1), test_session_id()).with_token_limit(1000);
        ctx.use_tokens(100);
        ctx.use_tokens(200);
        ctx.use_tokens(50);
        assert_eq!(ctx.tokens_used, 350);
    }

    #[test]
    fn context_equality() {
        let c1 = AgentContext::new(AgentId::new(1), test_session_id());
        let c2 = AgentContext::new(AgentId::new(1), test_session_id());
        assert_eq!(c1, c2);
    }

    #[test]
    fn context_inequality() {
        let c1 = AgentContext::new(AgentId::new(1), test_session_id());
        let c2 = AgentContext::new(AgentId::new(2), test_session_id());
        assert_ne!(c1, c2);
    }
}
