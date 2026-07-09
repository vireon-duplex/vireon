//! [`AgentRuntimeBuilder`] — fluent builder for [`AgentRuntimeConfig`]
//! and the agent runtime.
//!
//! See `DESIGN.md` Part 13 (configuration).

use crate::config::{AgentRuntimeConfig, Preset};
use crate::error::AgentRuntimeError;

/// Fluent builder for [`AgentRuntimeConfig`].
#[derive(Debug, Clone)]
pub struct AgentRuntimeBuilder {
    cfg: AgentRuntimeConfig,
}

impl AgentRuntimeBuilder {
    /// Start a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cfg: AgentRuntimeConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max agents.
    #[must_use]
    pub fn max_agents(mut self, agents: u32) -> Self {
        self.cfg = self.cfg.with_max_agents(agents);
        self
    }

    /// Override the max tools per agent.
    #[must_use]
    pub fn max_tools_per_agent(mut self, tools: u32) -> Self {
        self.cfg = self.cfg.with_max_tools_per_agent(tools);
        self
    }

    /// Override the tool timeout (ms).
    #[must_use]
    pub fn tool_timeout_ms(mut self, ms: u32) -> Self {
        self.cfg = self.cfg.with_tool_timeout_ms(ms);
        self
    }

    /// Override the context window tokens.
    #[must_use]
    pub fn context_window_tokens(mut self, tokens: u64) -> Self {
        self.cfg = self.cfg.with_context_window_tokens(tokens);
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`AgentRuntimeError::ToolFailed`] if any field is out
    /// of range.
    pub fn build(self) -> Result<AgentRuntimeConfig, AgentRuntimeError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for AgentRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_CONTEXT_WINDOW_TOKENS, MAX_MAX_AGENTS, MAX_MAX_TOOLS_PER_AGENT, MAX_TOOL_TIMEOUT_MS,
        MIN_CONTEXT_WINDOW_TOKENS, MIN_MAX_AGENTS, MIN_MAX_TOOLS_PER_AGENT, MIN_TOOL_TIMEOUT_MS,
    };

    #[test]
    fn builder_new_has_defaults() {
        let builder = AgentRuntimeBuilder::new();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_agents, 128);
        assert_eq!(cfg.max_tools_per_agent, 32);
        assert_eq!(cfg.tool_timeout_ms, 30_000);
        assert_eq!(cfg.context_window_tokens, 16_384);
    }

    #[test]
    fn builder_from_preset() {
        let builder = AgentRuntimeBuilder::from_preset(Preset::Full);
        let cfg = builder.build().unwrap();
        assert_eq!(cfg.max_agents, 512);
        assert_eq!(cfg.context_window_tokens, 65_536);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = AgentRuntimeBuilder::new()
            .max_agents(256)
            .max_tools_per_agent(64)
            .tool_timeout_ms(10_000)
            .context_window_tokens(32_768)
            .build()
            .unwrap();
        assert_eq!(cfg.max_agents, 256);
        assert_eq!(cfg.max_tools_per_agent, 64);
        assert_eq!(cfg.tool_timeout_ms, 10_000);
        assert_eq!(cfg.context_window_tokens, 32_768);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = AgentRuntimeBuilder::new()
            .max_agents(0) // invalid
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = AgentRuntimeBuilder::new()
            .max_agents(MIN_MAX_AGENTS)
            .max_tools_per_agent(MIN_MAX_TOOLS_PER_AGENT)
            .tool_timeout_ms(MIN_TOOL_TIMEOUT_MS)
            .context_window_tokens(MIN_CONTEXT_WINDOW_TOKENS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_agents, MIN_MAX_AGENTS);
        assert_eq!(cfg.max_tools_per_agent, MIN_MAX_TOOLS_PER_AGENT);
        assert_eq!(cfg.tool_timeout_ms, MIN_TOOL_TIMEOUT_MS);
        assert_eq!(cfg.context_window_tokens, MIN_CONTEXT_WINDOW_TOKENS);

        let cfg = AgentRuntimeBuilder::new()
            .max_agents(MAX_MAX_AGENTS)
            .max_tools_per_agent(MAX_MAX_TOOLS_PER_AGENT)
            .tool_timeout_ms(MAX_TOOL_TIMEOUT_MS)
            .context_window_tokens(MAX_CONTEXT_WINDOW_TOKENS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_agents, MAX_MAX_AGENTS);
        assert_eq!(cfg.max_tools_per_agent, MAX_MAX_TOOLS_PER_AGENT);
        assert_eq!(cfg.tool_timeout_ms, MAX_TOOL_TIMEOUT_MS);
        assert_eq!(cfg.context_window_tokens, MAX_CONTEXT_WINDOW_TOKENS);
    }

    #[test]
    fn builder_from_preset_preserves_overrides() {
        let cfg = AgentRuntimeBuilder::from_preset(Preset::Minimal)
            .max_agents(100) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_agents, 100);
        assert_eq!(cfg.max_tools_per_agent, 8); // from preset
    }

    #[test]
    fn builder_default_is_default_trait() {
        let builder = AgentRuntimeBuilder::default();
        let cfg = builder.build().unwrap();
        assert_eq!(cfg, AgentRuntimeConfig::default());
    }

    #[test]
    fn builder_clone_produces_independent() {
        let b1 = AgentRuntimeBuilder::new().max_agents(100);
        let b2 = b1.clone();
        let cfg1 = b1.build().unwrap();
        let cfg2 = b2.max_agents(200).build().unwrap();
        assert_eq!(cfg1.max_agents, 100);
        assert_eq!(cfg2.max_agents, 200);
    }
}
