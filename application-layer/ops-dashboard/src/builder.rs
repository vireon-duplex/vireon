//! [`OpsDashboardBuilder`] — fluent builder for
//! [`OpsDashboardConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `OpsDashboardConfig` has several fields and most callers only need
//! to override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching every
//! field.

use crate::config::{OpsDashboardConfig, Preset};
use crate::error::OpsDashboardError;

/// Fluent builder for [`OpsDashboardConfig`].
#[derive(Debug, Clone)]
pub struct OpsDashboardBuilder {
    cfg: OpsDashboardConfig,
}

impl OpsDashboardBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: OpsDashboardConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max widgets.
    #[must_use]
    pub const fn max_widgets(mut self, max: u32) -> Self {
        self.cfg.max_widgets = max;
        self
    }

    /// Override the refresh interval (ms).
    #[must_use]
    pub const fn refresh_interval_ms(mut self, interval_ms: u64) -> Self {
        self.cfg.refresh_interval_ms = interval_ms;
        self
    }

    /// Override the query timeout (ms).
    #[must_use]
    pub const fn query_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.cfg.query_timeout_ms = timeout_ms;
        self
    }

    /// Override the max query range (ms).
    #[must_use]
    pub const fn max_query_range_ms(mut self, range_ms: u64) -> Self {
        self.cfg.max_query_range_ms = range_ms;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`OpsDashboardError::QueryFailed`] if any field is out
    /// of range.
    pub fn build(self) -> Result<OpsDashboardConfig, OpsDashboardError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for OpsDashboardBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_MAX_QUERY_RANGE_MS, MAX_MAX_WIDGETS, MAX_QUERY_TIMEOUT_MS, MAX_REFRESH_INTERVAL_MS,
        MIN_MAX_QUERY_RANGE_MS, MIN_MAX_WIDGETS, MIN_QUERY_TIMEOUT_MS, MIN_REFRESH_INTERVAL_MS,
    };

    #[test]
    fn new_builder_has_defaults() {
        let cfg = OpsDashboardBuilder::new().build().unwrap();
        assert_eq!(cfg.max_widgets, 32);
        assert_eq!(cfg.refresh_interval_ms, 2_000);
        assert_eq!(cfg.query_timeout_ms, 5_000);
        assert_eq!(cfg.max_query_range_ms, 3_600_000);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = OpsDashboardBuilder::new()
            .max_widgets(64)
            .refresh_interval_ms(1_000)
            .query_timeout_ms(3_000)
            .max_query_range_ms(7_200_000)
            .build()
            .unwrap();
        assert_eq!(cfg.max_widgets, 64);
        assert_eq!(cfg.refresh_interval_ms, 1_000);
        assert_eq!(cfg.query_timeout_ms, 3_000);
        assert_eq!(cfg.max_query_range_ms, 7_200_000);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = OpsDashboardBuilder::new()
            .max_widgets(0) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = OpsDashboardBuilder::new()
            .max_widgets(MIN_MAX_WIDGETS)
            .refresh_interval_ms(MIN_REFRESH_INTERVAL_MS)
            .query_timeout_ms(MIN_QUERY_TIMEOUT_MS)
            .max_query_range_ms(MIN_MAX_QUERY_RANGE_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_widgets, MIN_MAX_WIDGETS);
        assert_eq!(cfg.refresh_interval_ms, MIN_REFRESH_INTERVAL_MS);

        let cfg = OpsDashboardBuilder::new()
            .max_widgets(MAX_MAX_WIDGETS)
            .refresh_interval_ms(MAX_REFRESH_INTERVAL_MS)
            .query_timeout_ms(MAX_QUERY_TIMEOUT_MS)
            .max_query_range_ms(MAX_MAX_QUERY_RANGE_MS)
            .build()
            .unwrap();
        assert_eq!(cfg.max_widgets, MAX_MAX_WIDGETS);
        assert_eq!(cfg.refresh_interval_ms, MAX_REFRESH_INTERVAL_MS);
        assert_eq!(cfg.query_timeout_ms, MAX_QUERY_TIMEOUT_MS);
        assert_eq!(cfg.max_query_range_ms, MAX_MAX_QUERY_RANGE_MS);
    }

    #[test]
    fn builder_from_preset() {
        let cfg = OpsDashboardBuilder::from_preset(Preset::Standard)
            .max_widgets(64) // override
            .build()
            .unwrap();
        assert_eq!(cfg.max_widgets, 64);
        assert_eq!(cfg.refresh_interval_ms, 2_000); // from preset
    }

    #[test]
    fn builder_from_preset_minimal() {
        let cfg = OpsDashboardBuilder::from_preset(Preset::Minimal)
            .build()
            .unwrap();
        assert_eq!(cfg.max_widgets, 8);
        assert_eq!(cfg.refresh_interval_ms, 5_000);
    }

    #[test]
    fn builder_from_preset_full() {
        let cfg = OpsDashboardBuilder::from_preset(Preset::Full)
            .build()
            .unwrap();
        assert_eq!(cfg.max_widgets, 128);
        assert_eq!(cfg.refresh_interval_ms, 1_000);
        assert_eq!(cfg.query_timeout_ms, 10_000);
        assert_eq!(cfg.max_query_range_ms, 86_400_000);
    }

    #[test]
    fn builder_default_is_same_as_new() {
        let a = OpsDashboardBuilder::new().build().unwrap();
        let b = OpsDashboardBuilder::default().build().unwrap();
        assert_eq!(a, b);
    }
}
