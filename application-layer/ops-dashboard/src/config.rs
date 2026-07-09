//! [`OpsDashboardConfig`], [`Preset`] — declarative configuration for
//! the ops-dashboard crate.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for the dashboard:
//!
//! | Preset | Widgets | Refresh | Timeout | Use case |
//! |--------|---------|---------|---------|----------|
//! | `Minimal` | 8 | 5s | 2s | Lightweight monitoring |
//! | `Standard` | 32 | 2s | 5s | Production dashboard |
//! | `Full` | 128 | 1s | 10s | Full NOC dashboard |

use crate::error::OpsDashboardError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum number of widgets the dashboard will render.
pub const MIN_MAX_WIDGETS: u32 = 1;
/// Maximum number of widgets the dashboard will render (hard cap).
pub const MAX_MAX_WIDGETS: u32 = 1_000;

/// Minimum refresh interval in milliseconds (100ms).
pub const MIN_REFRESH_INTERVAL_MS: u64 = 100;
/// Maximum refresh interval in milliseconds (60s).
pub const MAX_REFRESH_INTERVAL_MS: u64 = 60_000;

/// Minimum query timeout in milliseconds (100ms).
pub const MIN_QUERY_TIMEOUT_MS: u64 = 100;
/// Maximum query timeout in milliseconds (30s).
pub const MAX_QUERY_TIMEOUT_MS: u64 = 30_000;

/// Minimum maximum query range in milliseconds (1s).
pub const MIN_MAX_QUERY_RANGE_MS: u64 = 1_000;
/// Maximum maximum query range in milliseconds (24h).
pub const MAX_MAX_QUERY_RANGE_MS: u64 = 86_400_000;

// ── OpsDashboardConfig ──────────────────────────────────────────

/// Declarative configuration for the ops-dashboard.
///
/// Constructed via [`OpsDashboardConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpsDashboardConfig {
    /// Maximum number of widgets the dashboard may render.
    pub max_widgets: u32,
    /// Widget refresh interval in milliseconds.
    pub refresh_interval_ms: u64,
    /// Query timeout in milliseconds.
    pub query_timeout_ms: u64,
    /// Maximum allowed query time range in milliseconds.
    pub max_query_range_ms: u64,
}

impl OpsDashboardConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 32 widgets, 2s refresh, 5s timeout, 1h max range.
    pub const fn new() -> Self {
        Self {
            max_widgets: 32,
            refresh_interval_ms: 2_000,
            query_timeout_ms: 5_000,
            max_query_range_ms: 3_600_000,
        }
    }

    /// Builder-style: set the max widgets.
    #[must_use]
    pub const fn with_max_widgets(mut self, max: u32) -> Self {
        self.max_widgets = max;
        self
    }

    /// Builder-style: set the refresh interval (ms).
    #[must_use]
    pub const fn with_refresh_interval_ms(mut self, interval_ms: u64) -> Self {
        self.refresh_interval_ms = interval_ms;
        self
    }

    /// Builder-style: set the query timeout (ms).
    #[must_use]
    pub const fn with_query_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.query_timeout_ms = timeout_ms;
        self
    }

    /// Builder-style: set the max query range (ms).
    #[must_use]
    pub const fn with_max_query_range_ms(mut self, range_ms: u64) -> Self {
        self.max_query_range_ms = range_ms;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`OpsDashboardError::QueryFailed`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), OpsDashboardError> {
        if self.max_widgets < MIN_MAX_WIDGETS || self.max_widgets > MAX_MAX_WIDGETS {
            return Err(OpsDashboardError::QueryFailed("max_widgets out of range"));
        }
        if self.refresh_interval_ms < MIN_REFRESH_INTERVAL_MS
            || self.refresh_interval_ms > MAX_REFRESH_INTERVAL_MS
        {
            return Err(OpsDashboardError::QueryFailed(
                "refresh_interval_ms out of range",
            ));
        }
        if self.query_timeout_ms < MIN_QUERY_TIMEOUT_MS
            || self.query_timeout_ms > MAX_QUERY_TIMEOUT_MS
        {
            return Err(OpsDashboardError::QueryFailed(
                "query_timeout_ms out of range",
            ));
        }
        if self.max_query_range_ms < MIN_MAX_QUERY_RANGE_MS
            || self.max_query_range_ms > MAX_MAX_QUERY_RANGE_MS
        {
            return Err(OpsDashboardError::QueryFailed(
                "max_query_range_ms out of range",
            ));
        }
        Ok(())
    }
}

impl Default for OpsDashboardConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// dashboard profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Minimal: 8 widgets, 5s refresh, 2s timeout — lightweight monitoring.
    Minimal,
    /// Standard: 32 widgets, 2s refresh, 5s timeout — production dashboard.
    Standard,
    /// Full: 128 widgets, 1s refresh, 10s timeout — full NOC dashboard.
    Full,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> OpsDashboardConfig {
        match self {
            Self::Minimal => OpsDashboardConfig {
                max_widgets: 8,
                refresh_interval_ms: 5_000,
                query_timeout_ms: 2_000,
                max_query_range_ms: 3_600_000,
            },
            Self::Standard => OpsDashboardConfig {
                max_widgets: 32,
                refresh_interval_ms: 2_000,
                query_timeout_ms: 5_000,
                max_query_range_ms: 3_600_000,
            },
            Self::Full => OpsDashboardConfig {
                max_widgets: 128,
                refresh_interval_ms: 1_000,
                query_timeout_ms: 10_000,
                max_query_range_ms: 86_400_000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = OpsDashboardConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_widgets, 32);
        assert_eq!(cfg.refresh_interval_ms, 2_000);
        assert_eq!(cfg.query_timeout_ms, 5_000);
        assert_eq!(cfg.max_query_range_ms, 3_600_000);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = OpsDashboardConfig::new()
            .with_max_widgets(64)
            .with_refresh_interval_ms(1_000)
            .with_query_timeout_ms(3_000)
            .with_max_query_range_ms(7_200_000);
        assert_eq!(cfg.max_widgets, 64);
        assert_eq!(cfg.refresh_interval_ms, 1_000);
        assert_eq!(cfg.query_timeout_ms, 3_000);
        assert_eq!(cfg.max_query_range_ms, 7_200_000);
    }

    #[test]
    fn validate_rejects_bad_max_widgets() {
        let mut cfg = OpsDashboardConfig::default();
        cfg.max_widgets = 0;
        assert!(cfg.validate().is_err());
        cfg.max_widgets = MAX_MAX_WIDGETS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_refresh_interval() {
        let mut cfg = OpsDashboardConfig::default();
        cfg.refresh_interval_ms = MIN_REFRESH_INTERVAL_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.refresh_interval_ms = MAX_REFRESH_INTERVAL_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_query_timeout() {
        let mut cfg = OpsDashboardConfig::default();
        cfg.query_timeout_ms = MIN_QUERY_TIMEOUT_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.query_timeout_ms = MAX_QUERY_TIMEOUT_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_query_range() {
        let mut cfg = OpsDashboardConfig::default();
        cfg.max_query_range_ms = MIN_MAX_QUERY_RANGE_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.max_query_range_ms = MAX_MAX_QUERY_RANGE_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn minimal_preset_has_low_widget_count() {
        let cfg = Preset::Minimal.to_config();
        assert_eq!(cfg.max_widgets, 8);
        assert_eq!(cfg.refresh_interval_ms, 5_000);
        assert_eq!(cfg.query_timeout_ms, 2_000);
    }

    #[test]
    fn standard_preset_has_default_values() {
        let cfg = Preset::Standard.to_config();
        assert_eq!(cfg.max_widgets, 32);
        assert_eq!(cfg.refresh_interval_ms, 2_000);
        assert_eq!(cfg.query_timeout_ms, 5_000);
    }

    #[test]
    fn full_preset_has_high_widget_count() {
        let cfg = Preset::Full.to_config();
        assert_eq!(cfg.max_widgets, 128);
        assert_eq!(cfg.refresh_interval_ms, 1_000);
        assert_eq!(cfg.query_timeout_ms, 10_000);
        assert_eq!(cfg.max_query_range_ms, 86_400_000);
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
}
