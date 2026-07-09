//! [`AgentRuntimeConfig`], [`Preset`] — declarative configuration for
//! the agent runtime.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes:
//!
//! | Preset | Max Agents | Max Tools | Tool Timeout | Context Window |
//! |--------|------------|-----------|--------------|----------------|
//! | `Minimal` | 32 | 8 | 5_000 ms | 4_096 |
//! | `Standard` | 128 | 32 | 30_000 ms | 16_384 |
//! | `Full` | 512 | 128 | 60_000 ms | 65_536 |

use crate::error::AgentRuntimeError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of agents.
pub const MIN_MAX_AGENTS: u32 = 1;

/// Maximum number of agents.
pub const MAX_MAX_AGENTS: u32 = 10_000;

/// Minimum number of tools per agent.
pub const MIN_MAX_TOOLS_PER_AGENT: u32 = 1;

/// Maximum number of tools per agent.
pub const MAX_MAX_TOOLS_PER_AGENT: u32 = 1_000;

/// Minimum tool timeout in milliseconds.
pub const MIN_TOOL_TIMEOUT_MS: u32 = 100;

/// Maximum tool timeout in milliseconds (10 minutes).
pub const MAX_TOOL_TIMEOUT_MS: u32 = 600_000;

/// Minimum context window in tokens.
pub const MIN_CONTEXT_WINDOW_TOKENS: u64 = 128;

/// Maximum context window in tokens.
pub const MAX_CONTEXT_WINDOW_TOKENS: u64 = 1_000_000;

// ── AgentRuntimeConfig ───────────────────────────────────────────

/// Declarative configuration for the agent runtime.
///
/// Constructed via [`AgentRuntimeConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentRuntimeConfig {
    /// Maximum number of concurrent agents.
    pub max_agents: u32,
    /// Maximum number of tools per agent.
    pub max_tools_per_agent: u32,
    /// Timeout for tool execution in milliseconds.
    pub tool_timeout_ms: u32,
    /// Context window size in tokens.
    pub context_window_tokens: u64,
}

impl AgentRuntimeConfig {
    /// Construct a config with sensible defaults (matches `Standard` preset).
    ///
    /// Defaults: 128 agents, 32 tools, 30_000 ms timeout, 16_384 tokens.
    pub fn new() -> Self {
        Self {
            max_agents: 128,
            max_tools_per_agent: 32,
            tool_timeout_ms: 30_000,
            context_window_tokens: 16_384,
        }
    }

    /// Builder-style: set the max agents.
    #[must_use]
    pub const fn with_max_agents(mut self, agents: u32) -> Self {
        self.max_agents = agents;
        self
    }

    /// Builder-style: set the max tools per agent.
    #[must_use]
    pub const fn with_max_tools_per_agent(mut self, tools: u32) -> Self {
        self.max_tools_per_agent = tools;
        self
    }

    /// Builder-style: set the tool timeout (ms).
    #[must_use]
    pub const fn with_tool_timeout_ms(mut self, ms: u32) -> Self {
        self.tool_timeout_ms = ms;
        self
    }

    /// Builder-style: set the context window tokens.
    #[must_use]
    pub const fn with_context_window_tokens(mut self, tokens: u64) -> Self {
        self.context_window_tokens = tokens;
        self
    }

    /// Validate the config.
    ///
    /// # Errors
    /// Returns [`AgentRuntimeError::ToolFailed`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), AgentRuntimeError> {
        if self.max_agents < MIN_MAX_AGENTS || self.max_agents > MAX_MAX_AGENTS {
            return Err(AgentRuntimeError::ToolFailed("max_agents out of range"));
        }
        if self.max_tools_per_agent < MIN_MAX_TOOLS_PER_AGENT
            || self.max_tools_per_agent > MAX_MAX_TOOLS_PER_AGENT
        {
            return Err(AgentRuntimeError::ToolFailed(
                "max_tools_per_agent out of range",
            ));
        }
        if self.tool_timeout_ms < MIN_TOOL_TIMEOUT_MS || self.tool_timeout_ms > MAX_TOOL_TIMEOUT_MS
        {
            return Err(AgentRuntimeError::ToolFailed(
                "tool_timeout_ms out of range",
            ));
        }
        if self.context_window_tokens < MIN_CONTEXT_WINDOW_TOKENS
            || self.context_window_tokens > MAX_CONTEXT_WINDOW_TOKENS
        {
            return Err(AgentRuntimeError::ToolFailed(
                "context_window_tokens out of range",
            ));
        }
        Ok(())
    }
}

impl Default for AgentRuntimeConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// agent workload profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Minimal: few agents, limited tools, short timeouts.
    Minimal,
    /// Standard: moderate concurrency, generous toolset.
    Standard,
    /// Full: high concurrency, large toolset, generous timeouts.
    Full,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub fn to_config(self) -> AgentRuntimeConfig {
        match self {
            Self::Minimal => AgentRuntimeConfig::new()
                .with_max_agents(32)
                .with_max_tools_per_agent(8)
                .with_tool_timeout_ms(5_000)
                .with_context_window_tokens(4_096),
            Self::Standard => AgentRuntimeConfig::new(),
            Self::Full => AgentRuntimeConfig::new()
                .with_max_agents(512)
                .with_max_tools_per_agent(128)
                .with_tool_timeout_ms(60_000)
                .with_context_window_tokens(65_536),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = AgentRuntimeConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_agents, 128);
        assert_eq!(cfg.max_tools_per_agent, 32);
        assert_eq!(cfg.tool_timeout_ms, 30_000);
        assert_eq!(cfg.context_window_tokens, 16_384);
    }

    #[test]
    fn new_matches_default() {
        let cfg = AgentRuntimeConfig::new();
        let default = AgentRuntimeConfig::default();
        assert_eq!(cfg, default);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = AgentRuntimeConfig::new()
            .with_max_agents(256)
            .with_max_tools_per_agent(64)
            .with_tool_timeout_ms(10_000)
            .with_context_window_tokens(32_768);
        assert_eq!(cfg.max_agents, 256);
        assert_eq!(cfg.max_tools_per_agent, 64);
        assert_eq!(cfg.tool_timeout_ms, 10_000);
        assert_eq!(cfg.context_window_tokens, 32_768);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_max_agents() {
        let mut cfg = AgentRuntimeConfig::default();
        cfg.max_agents = 0;
        assert!(cfg.validate().is_err());
        cfg.max_agents = MAX_MAX_AGENTS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_tools() {
        let mut cfg = AgentRuntimeConfig::default();
        cfg.max_tools_per_agent = 0;
        assert!(cfg.validate().is_err());
        cfg.max_tools_per_agent = MAX_MAX_TOOLS_PER_AGENT + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_tool_timeout() {
        let mut cfg = AgentRuntimeConfig::default();
        cfg.tool_timeout_ms = MIN_TOOL_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.tool_timeout_ms = MAX_TOOL_TIMEOUT_MS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_context_window() {
        let mut cfg = AgentRuntimeConfig::default();
        cfg.context_window_tokens = MIN_CONTEXT_WINDOW_TOKENS / 2;
        assert!(cfg.validate().is_err());
        cfg.context_window_tokens = MAX_CONTEXT_WINDOW_TOKENS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn standard_preset_matches_default() {
        let preset_cfg = Preset::Standard.to_config();
        let default_cfg = AgentRuntimeConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn minimal_preset_config() {
        let cfg = Preset::Minimal.to_config();
        assert_eq!(cfg.max_agents, 32);
        assert_eq!(cfg.max_tools_per_agent, 8);
        assert_eq!(cfg.tool_timeout_ms, 5_000);
        assert_eq!(cfg.context_window_tokens, 4_096);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn full_preset_config() {
        let cfg = Preset::Full.to_config();
        assert_eq!(cfg.max_agents, 512);
        assert_eq!(cfg.max_tools_per_agent, 128);
        assert_eq!(cfg.tool_timeout_ms, 60_000);
        assert_eq!(cfg.context_window_tokens, 65_536);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Minimal, Preset::Standard, Preset::Full] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }

    #[test]
    fn builder_overrides_preset() {
        let cfg = Preset::Minimal
            .to_config()
            .with_max_agents(100)
            .with_tool_timeout_ms(30_000);
        assert_eq!(cfg.max_agents, 100);
        assert_eq!(cfg.tool_timeout_ms, 30_000);
        assert_eq!(cfg.max_tools_per_agent, 8); // from preset
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let cfg = AgentRuntimeConfig::new()
            .with_max_agents(MIN_MAX_AGENTS)
            .with_max_tools_per_agent(MIN_MAX_TOOLS_PER_AGENT)
            .with_tool_timeout_ms(MIN_TOOL_TIMEOUT_MS)
            .with_context_window_tokens(MIN_CONTEXT_WINDOW_TOKENS);
        assert!(cfg.validate().is_ok());

        let cfg = AgentRuntimeConfig::new()
            .with_max_agents(MAX_MAX_AGENTS)
            .with_max_tools_per_agent(MAX_MAX_TOOLS_PER_AGENT)
            .with_tool_timeout_ms(MAX_TOOL_TIMEOUT_MS)
            .with_context_window_tokens(MAX_CONTEXT_WINDOW_TOKENS);
        assert!(cfg.validate().is_ok());
    }
}
