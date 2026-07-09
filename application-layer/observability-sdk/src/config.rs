//! [`ObservabilityConfig`], [`Preset`] — declarative configuration for
//! the observability SDK.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The three presets encode deployment archetypes for observability:
//!
//! | Preset | Metrics | Tracing | Sampling | Use case |
//! |--------|---------|---------|----------|----------|
//! | `Minimal` | Enabled | Disabled | 0 bps | Production, minimal overhead |
//! | `Standard` | Enabled | Enabled | 100 bps (1%) | Production, balanced |
//! | `Verbose` | Enabled | Enabled | 1000 bps (10%) | Development, debugging |

use crate::error::ObservabilityError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum sampling rate in basis points (0%).
pub const MIN_SAMPLING_BPS: u16 = 0;
/// Maximum sampling rate in basis points (100%).
pub const MAX_SAMPLING_BPS: u16 = 10_000;

/// Minimum export interval in milliseconds (100ms).
pub const MIN_EXPORT_INTERVAL_MS: u64 = 100;
/// Maximum export interval in milliseconds (60s).
pub const MAX_EXPORT_INTERVAL_MS: u64 = 60_000;

/// Minimum number of metrics the registry will accept.
pub const MIN_METRICS: u32 = 1;
/// Maximum number of metrics the registry will accept (hard cap).
pub const MAX_METRICS: u32 = 100_000;

/// Minimum number of spans the tracer will accept.
pub const MIN_SPANS: u32 = 1;
/// Maximum number of spans the tracer will accept (hard cap).
pub const MAX_SPANS: u32 = 1_000_000;

// ── ObservabilityConfig ──────────────────────────────────────────────

/// Declarative configuration for the observability SDK.
///
/// Constructed via [`ObservabilityConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservabilityConfig {
    /// Maximum metrics the registry may track.
    pub max_metrics: u32,
    /// Maximum spans the tracer may track.
    pub max_spans: u32,
    /// Export interval in milliseconds.
    pub export_interval_ms: u64,
    /// Whether distributed tracing is enabled.
    pub enable_tracing: bool,
    /// Whether metrics collection is enabled.
    pub enable_metrics: bool,
    /// Sampling rate in basis points (0..=10_000).
    pub sampling_rate_bps: u16,
}

impl ObservabilityConfig {
    /// Construct a minimal config with sensible defaults.
    ///
    /// Defaults: 10K metrics, 100K spans, 5s export interval,
    /// metrics enabled, tracing disabled, 0% sampling.
    pub const fn new() -> Self {
        Self {
            max_metrics: 10_000,
            max_spans: 100_000,
            export_interval_ms: 5_000,
            enable_tracing: false,
            enable_metrics: true,
            sampling_rate_bps: 0,
        }
    }

    /// Builder-style: set the max metrics.
    #[must_use]
    pub const fn with_max_metrics(mut self, max: u32) -> Self {
        self.max_metrics = max;
        self
    }

    /// Builder-style: set the max spans.
    #[must_use]
    pub const fn with_max_spans(mut self, max: u32) -> Self {
        self.max_spans = max;
        self
    }

    /// Builder-style: set the export interval (ms).
    #[must_use]
    pub const fn with_export_interval_ms(mut self, interval_ms: u64) -> Self {
        self.export_interval_ms = interval_ms;
        self
    }

    /// Builder-style: enable or disable tracing.
    #[must_use]
    pub const fn with_tracing(mut self, enable: bool) -> Self {
        self.enable_tracing = enable;
        self
    }

    /// Builder-style: enable or disable metrics.
    #[must_use]
    pub const fn with_metrics(mut self, enable: bool) -> Self {
        self.enable_metrics = enable;
        self
    }

    /// Builder-style: set the sampling rate (bps).
    #[must_use]
    pub const fn with_sampling_rate_bps(mut self, rate_bps: u16) -> Self {
        self.sampling_rate_bps = rate_bps;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::InvalidConfig`] if any field is out
    /// of range.
    pub fn validate(&self) -> Result<(), ObservabilityError> {
        if self.max_metrics < MIN_METRICS || self.max_metrics > MAX_METRICS {
            return Err(ObservabilityError::InvalidLabel("max_metrics out of range"));
        }
        if self.max_spans < MIN_SPANS || self.max_spans > MAX_SPANS {
            return Err(ObservabilityError::InvalidLabel("max_spans out of range"));
        }
        if self.export_interval_ms < MIN_EXPORT_INTERVAL_MS
            || self.export_interval_ms > MAX_EXPORT_INTERVAL_MS
        {
            return Err(ObservabilityError::InvalidLabel(
                "export_interval_ms out of range",
            ));
        }
        if self.sampling_rate_bps > MAX_SAMPLING_BPS {
            return Err(ObservabilityError::InvalidLabel(
                "sampling_rate_bps out of range",
            ));
        }
        Ok(())
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Deployment archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// observability profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Minimal: metrics only, no tracing, zero sampling.
    Minimal,
    /// Standard: metrics + tracing with 1% sampling.
    Standard,
    /// Verbose: all features, 10% sampling, shorter export interval.
    Verbose,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    #[must_use]
    pub const fn to_config(self) -> ObservabilityConfig {
        match self {
            Self::Minimal => ObservabilityConfig {
                max_metrics: 10_000,
                max_spans: 100_000,
                export_interval_ms: 5_000,
                enable_tracing: false,
                enable_metrics: true,
                sampling_rate_bps: 0,
            },
            Self::Standard => ObservabilityConfig {
                max_metrics: 10_000,
                max_spans: 100_000,
                export_interval_ms: 5_000,
                enable_tracing: true,
                enable_metrics: true,
                sampling_rate_bps: 100, // 1%
            },
            Self::Verbose => ObservabilityConfig {
                max_metrics: 50_000,
                max_spans: 500_000,
                export_interval_ms: 1_000,
                enable_tracing: true,
                enable_metrics: true,
                sampling_rate_bps: 1_000, // 10%
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = ObservabilityConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_metrics, 10_000);
        assert_eq!(cfg.max_spans, 100_000);
        assert_eq!(cfg.export_interval_ms, 5_000);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = ObservabilityConfig::new()
            .with_max_metrics(50_000)
            .with_max_spans(200_000)
            .with_export_interval_ms(10_000)
            .with_tracing(true)
            .with_sampling_rate_bps(500);
        assert_eq!(cfg.max_metrics, 50_000);
        assert_eq!(cfg.max_spans, 200_000);
        assert_eq!(cfg.export_interval_ms, 10_000);
        assert!(cfg.enable_tracing);
        assert_eq!(cfg.sampling_rate_bps, 500);
    }

    #[test]
    fn validate_rejects_bad_max_metrics() {
        let mut cfg = ObservabilityConfig::default();
        cfg.max_metrics = 0;
        assert!(cfg.validate().is_err());
        cfg.max_metrics = MAX_METRICS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_max_spans() {
        let mut cfg = ObservabilityConfig::default();
        cfg.max_spans = 0;
        assert!(cfg.validate().is_err());
        cfg.max_spans = MAX_SPANS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_export_interval() {
        let mut cfg = ObservabilityConfig::default();
        cfg.export_interval_ms = MIN_EXPORT_INTERVAL_MS / 2;
        assert!(cfg.validate().is_err());
        cfg.export_interval_ms = MAX_EXPORT_INTERVAL_MS * 2;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_sampling_rate() {
        let mut cfg = ObservabilityConfig::default();
        cfg.sampling_rate_bps = MAX_SAMPLING_BPS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn minimal_preset_disables_tracing() {
        let cfg = Preset::Minimal.to_config();
        assert!(!cfg.enable_tracing);
        assert!(cfg.enable_metrics);
        assert_eq!(cfg.sampling_rate_bps, 0);
    }

    #[test]
    fn standard_preset_enables_tracing_with_low_sampling() {
        let cfg = Preset::Standard.to_config();
        assert!(cfg.enable_tracing);
        assert!(cfg.enable_metrics);
        assert_eq!(cfg.sampling_rate_bps, 100);
    }

    #[test]
    fn verbose_preset_has_high_sampling() {
        let cfg = Preset::Verbose.to_config();
        assert!(cfg.enable_tracing);
        assert!(cfg.enable_metrics);
        assert_eq!(cfg.sampling_rate_bps, 1_000);
        assert_eq!(cfg.export_interval_ms, 1_000);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [Preset::Minimal, Preset::Standard, Preset::Verbose] {
            let cfg = preset.to_config();
            assert!(
                cfg.validate().is_ok(),
                "preset {preset:?} produced invalid config"
            );
        }
    }
}
