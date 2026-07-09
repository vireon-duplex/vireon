//! [`ObservabilityBuilder`] — fluent builder for
//! [`ObservabilityConfig`].
//!
//! See `DESIGN.md` Part 13 (configuration).
//!
//! # Why a builder?
//!
//! `ObservabilityConfig` has many fields and most callers only need to
//! override a handful. The builder lets them start from a preset
//! or sensible defaults and chain overrides without touching
//! every field.

use crate::config::{ObservabilityConfig, Preset};
use crate::error::ObservabilityError;

/// Fluent builder for [`ObservabilityConfig`].
#[derive(Debug, Clone)]
pub struct ObservabilityBuilder {
    cfg: ObservabilityConfig,
}

impl ObservabilityBuilder {
    /// Start a new builder with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cfg: ObservabilityConfig::new(),
        }
    }

    /// Start from a preset.
    #[must_use]
    pub const fn from_preset(preset: Preset) -> Self {
        Self {
            cfg: preset.to_config(),
        }
    }

    /// Override the max metrics.
    #[must_use]
    pub const fn max_metrics(mut self, max: u32) -> Self {
        self.cfg.max_metrics = max;
        self
    }

    /// Override the max spans.
    #[must_use]
    pub const fn max_spans(mut self, max: u32) -> Self {
        self.cfg.max_spans = max;
        self
    }

    /// Override the export interval (ms).
    #[must_use]
    pub const fn export_interval_ms(mut self, interval_ms: u64) -> Self {
        self.cfg.export_interval_ms = interval_ms;
        self
    }

    /// Enable or disable tracing.
    #[must_use]
    pub const fn tracing(mut self, enable: bool) -> Self {
        self.cfg.enable_tracing = enable;
        self
    }

    /// Enable or disable metrics.
    #[must_use]
    pub const fn metrics(mut self, enable: bool) -> Self {
        self.cfg.enable_metrics = enable;
        self
    }

    /// Override the sampling rate (bps).
    #[must_use]
    pub const fn sampling_rate_bps(mut self, rate_bps: u16) -> Self {
        self.cfg.sampling_rate_bps = rate_bps;
        self
    }

    /// Validate and produce the final config.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::InvalidLabel`] if any field is out
    /// of range.
    pub fn build(self) -> Result<ObservabilityConfig, ObservabilityError> {
        self.cfg.validate()?;
        Ok(self.cfg)
    }
}

impl Default for ObservabilityBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        MAX_EXPORT_INTERVAL_MS, MAX_METRICS, MAX_SAMPLING_BPS, MAX_SPANS, MIN_EXPORT_INTERVAL_MS,
        MIN_METRICS, MIN_SPANS,
    };

    #[test]
    fn new_builder_has_defaults() {
        let cfg = ObservabilityBuilder::new().build().unwrap();
        assert_eq!(cfg.max_metrics, 10_000);
        assert_eq!(cfg.max_spans, 100_000);
        assert_eq!(cfg.export_interval_ms, 5_000);
    }

    #[test]
    fn builder_chains_overrides() {
        let cfg = ObservabilityBuilder::new()
            .max_metrics(50_000)
            .max_spans(200_000)
            .export_interval_ms(10_000)
            .tracing(true)
            .sampling_rate_bps(500)
            .build()
            .unwrap();
        assert_eq!(cfg.max_metrics, 50_000);
        assert_eq!(cfg.max_spans, 200_000);
        assert_eq!(cfg.export_interval_ms, 10_000);
        assert!(cfg.enable_tracing);
        assert_eq!(cfg.sampling_rate_bps, 500);
    }

    #[test]
    fn builder_validates_on_build() {
        let res = ObservabilityBuilder::new()
            .max_metrics(0) // too small
            .build();
        assert!(res.is_err());
    }

    #[test]
    fn builder_accepts_boundary_values() {
        let cfg = ObservabilityBuilder::new()
            .export_interval_ms(MIN_EXPORT_INTERVAL_MS)
            .max_metrics(MIN_METRICS)
            .max_spans(MIN_SPANS)
            .build()
            .unwrap();
        assert_eq!(cfg.export_interval_ms, MIN_EXPORT_INTERVAL_MS);

        let cfg = ObservabilityBuilder::new()
            .export_interval_ms(MAX_EXPORT_INTERVAL_MS)
            .max_metrics(MAX_METRICS)
            .max_spans(MAX_SPANS)
            .sampling_rate_bps(MAX_SAMPLING_BPS)
            .build()
            .unwrap();
        assert_eq!(cfg.export_interval_ms, MAX_EXPORT_INTERVAL_MS);
    }

    #[test]
    fn builder_from_preset() {
        let cfg = ObservabilityBuilder::from_preset(Preset::Standard)
            .sampling_rate_bps(200) // override
            .build()
            .unwrap();
        assert_eq!(cfg.sampling_rate_bps, 200);
        assert!(cfg.enable_tracing); // from preset
    }

    #[test]
    fn builder_disables_features() {
        let cfg = ObservabilityBuilder::new()
            .metrics(false)
            .tracing(false)
            .build()
            .unwrap();
        assert!(!cfg.enable_metrics);
        assert!(!cfg.enable_tracing);
    }

    #[test]
    fn builder_default_is_same_as_new() {
        let a = ObservabilityBuilder::new().build().unwrap();
        let b = ObservabilityBuilder::default().build().unwrap();
        assert_eq!(a, b);
    }
}
