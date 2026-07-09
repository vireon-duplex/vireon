//! [`ConfigSource`] — configuration source enum with priority.
//!
//! See `DESIGN.md` Part 6 (sources & priority).

// ── ConfigSource ──────────────────────────────────────────────────

/// Configuration source with inherent priority.
///
/// Sources are layered by priority; higher priority sources override
/// lower priority sources.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfigSource {
    /// File-based configuration (e.g. TOML, JSON).
    File,
    /// Environment variables.
    Env,
    /// Command-line flags.
    CliFlag,
    /// Runtime overrides (API, hot reload).
    Runtime,
    /// Default/fallback values.
    Default,
}

impl ConfigSource {
    /// Minimum priority value.
    pub const MIN_PRIORITY: u8 = 10;

    /// Maximum priority value.
    pub const MAX_PRIORITY: u8 = 100;

    /// Get the priority of this source.
    ///
    /// Higher values win. Priority ordering:
    /// - Runtime: 100
    /// - CliFlag: 90
    /// - Env: 80
    /// - File: 70
    /// - Default: 10
    #[must_use]
    pub const fn priority(&self) -> u8 {
        match self {
            Self::Runtime => 100,
            Self::CliFlag => 90,
            Self::Env => 80,
            Self::File => 70,
            Self::Default => 10,
        }
    }

    /// Get the source as a string identifier.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Env => "env",
            Self::CliFlag => "cli",
            Self::Runtime => "runtime",
            Self::Default => "default",
        }
    }

    /// Whether this source can be overridden at runtime.
    #[must_use]
    pub const fn is_runtime_overridable(&self) -> bool {
        matches!(self, Self::File | Self::Env | Self::Default)
    }
}

impl Default for ConfigSource {
    fn default() -> Self {
        Self::Default
    }
}

impl PartialOrd for ConfigSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConfigSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by priority (higher priority = greater).
        self.priority().cmp(&other.priority())
    }
}

impl std::fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_values_are_correct() {
        assert_eq!(ConfigSource::Runtime.priority(), 100);
        assert_eq!(ConfigSource::CliFlag.priority(), 90);
        assert_eq!(ConfigSource::Env.priority(), 80);
        assert_eq!(ConfigSource::File.priority(), 70);
        assert_eq!(ConfigSource::Default.priority(), 10);
    }

    #[test]
    fn as_str_returns_identifier() {
        assert_eq!(ConfigSource::File.as_str(), "file");
        assert_eq!(ConfigSource::Env.as_str(), "env");
        assert_eq!(ConfigSource::CliFlag.as_str(), "cli");
        assert_eq!(ConfigSource::Runtime.as_str(), "runtime");
        assert_eq!(ConfigSource::Default.as_str(), "default");
    }

    #[test]
    fn runtime_overridable_sources() {
        assert!(ConfigSource::File.is_runtime_overridable());
        assert!(ConfigSource::Env.is_runtime_overridable());
        assert!(ConfigSource::Default.is_runtime_overridable());
        assert!(!ConfigSource::CliFlag.is_runtime_overridable());
        assert!(!ConfigSource::Runtime.is_runtime_overridable());
    }

    #[test]
    fn sources_order_by_priority() {
        assert!(ConfigSource::Runtime > ConfigSource::CliFlag);
        assert!(ConfigSource::CliFlag > ConfigSource::Env);
        assert!(ConfigSource::Env > ConfigSource::File);
        assert!(ConfigSource::File > ConfigSource::Default);
    }

    #[test]
    fn display_shows_source_name() {
        assert_eq!(format!("{}", ConfigSource::File), "file");
        assert_eq!(format!("{}", ConfigSource::Env), "env");
        assert_eq!(format!("{}", ConfigSource::Runtime), "runtime");
    }

    #[test]
    fn default_source_is_default() {
        let source = ConfigSource::default();
        assert_eq!(source, ConfigSource::Default);
    }

    #[test]
    fn priority_constants_are_correct() {
        assert_eq!(ConfigSource::MIN_PRIORITY, 10);
        assert_eq!(ConfigSource::MAX_PRIORITY, 100);
    }
}
