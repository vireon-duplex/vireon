//! [`MigrationConfig`], [`Preset`] — declarative configuration for
//! the session migration manager.
//!
//! See `DESIGN.md` Part 13 (configuration & presets).
//!
//! # Presets
//!
//! The four presets encode migration archetypes tuned for different
//! operational profiles:
//!
//! | Preset | Timeout | Max Concurrent | Retries | Zero-Downtime |
//! |--------|---------|----------------|---------|---------------|
//! | `Conservative` | 60s | 16 | 5 | false |
//! | `Balanced` | 30s | 64 | 3 | false |
//! | `Aggressive` | 10s | 256 | 2 | true |
//! | `Streaming` | 120s | 32 | 4 | true |

use crate::error::MigrationError;

// ── Constants ───────────────────────────────────────────────────

/// Minimum migration timeout (1 second).
pub const MIN_TIMEOUT_MS: u64 = 1_000;

/// Maximum migration timeout (5 minutes).
pub const MAX_TIMEOUT_MS: u64 = 300_000;

/// Minimum concurrent migrations.
pub const MIN_CONCURRENT: u32 = 1;

/// Maximum concurrent migrations.
pub const MAX_CONCURRENT: u32 = 10_000;

/// Minimum checkpoint interval (100 ms).
pub const MIN_CHECKPOINT_INTERVAL_MS: u64 = 100;

/// Maximum checkpoint interval (60 seconds).
pub const MAX_CHECKPOINT_INTERVAL_MS: u64 = 60_000;

/// Maximum retry attempts.
pub const MAX_RETRIES: u8 = 10;

// ── MigrationConfig ─────────────────────────────────────────────

/// Declarative configuration for the session migration manager.
///
/// Constructed via [`MigrationConfig::new`] or obtained from a
/// [`Preset`]. Always validated before use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrationConfig {
    /// Default per-migration timeout in milliseconds.
    pub default_timeout_ms: u64,
    /// Maximum simultaneous migrations across all sessions.
    pub max_concurrent_migrations: u32,
    /// Maximum retry attempts per phase.
    pub max_retries: u8,
    /// Whether to compress snapshots before transfer.
    pub snapshot_compression: bool,
    /// Whether to preserve stream ordering during migration.
    pub preserve_ordering: bool,
    /// Interval between checkpoints for crash recovery (ms).
    pub checkpoint_interval_ms: u64,
    /// Whether zero-downtime (dual-ownership) migration is enabled.
    pub enable_zero_downtime: bool,
}

impl MigrationConfig {
    /// Construct a config with sensible defaults.
    ///
    /// Defaults: 30 s timeout, 64 concurrent, 3 retries, compression
    /// on, ordering preserved, 5 s checkpoint, zero-downtime off.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            default_timeout_ms: 30_000,
            max_concurrent_migrations: 64,
            max_retries: 3,
            snapshot_compression: true,
            preserve_ordering: true,
            checkpoint_interval_ms: 5_000,
            enable_zero_downtime: false,
        }
    }

    /// Builder-style: set the default timeout (ms).
    #[must_use]
    pub const fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.default_timeout_ms = ms;
        self
    }

    /// Builder-style: set the max concurrent migrations.
    #[must_use]
    pub const fn with_max_concurrent(mut self, n: u32) -> Self {
        self.max_concurrent_migrations = n;
        self
    }

    /// Builder-style: set the max retries.
    #[must_use]
    pub const fn with_max_retries(mut self, n: u8) -> Self {
        self.max_retries = n;
        self
    }

    /// Builder-style: enable/disable snapshot compression.
    #[must_use]
    pub const fn with_compression(mut self, on: bool) -> Self {
        self.snapshot_compression = on;
        self
    }

    /// Builder-style: enable/disable ordering preservation.
    #[must_use]
    pub const fn with_ordering(mut self, on: bool) -> Self {
        self.preserve_ordering = on;
        self
    }

    /// Builder-style: set the checkpoint interval (ms).
    #[must_use]
    pub const fn with_checkpoint_interval(mut self, ms: u64) -> Self {
        self.checkpoint_interval_ms = ms;
        self
    }

    /// Builder-style: enable/disable zero-downtime migration.
    #[must_use]
    pub const fn with_zero_downtime(mut self, on: bool) -> Self {
        self.enable_zero_downtime = on;
        self
    }

    /// Validate the config in-place.
    ///
    /// # Errors
    /// Returns [`MigrationError::InvalidConfig`] if any field is out
    /// of range or contradictory.
    pub fn validate(&self) -> Result<(), MigrationError> {
        if self.default_timeout_ms < MIN_TIMEOUT_MS || self.default_timeout_ms > MAX_TIMEOUT_MS {
            return Err(MigrationError::InvalidConfig(format!(
                "default_timeout_ms {} out of range [{}, {}]",
                self.default_timeout_ms, MIN_TIMEOUT_MS, MAX_TIMEOUT_MS,
            )));
        }
        if self.max_concurrent_migrations < MIN_CONCURRENT
            || self.max_concurrent_migrations > MAX_CONCURRENT
        {
            return Err(MigrationError::InvalidConfig(format!(
                "max_concurrent_migrations {} out of range [{}, {}]",
                self.max_concurrent_migrations, MIN_CONCURRENT, MAX_CONCURRENT,
            )));
        }
        if self.max_retries > MAX_RETRIES {
            return Err(MigrationError::InvalidConfig(format!(
                "max_retries {} exceeds maximum {}",
                self.max_retries, MAX_RETRIES,
            )));
        }
        if self.checkpoint_interval_ms < MIN_CHECKPOINT_INTERVAL_MS
            || self.checkpoint_interval_ms > MAX_CHECKPOINT_INTERVAL_MS
        {
            return Err(MigrationError::InvalidConfig(format!(
                "checkpoint_interval_ms {} out of range [{}, {}]",
                self.checkpoint_interval_ms, MIN_CHECKPOINT_INTERVAL_MS, MAX_CHECKPOINT_INTERVAL_MS,
            )));
        }
        Ok(())
    }
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ── Preset ──────────────────────────────────────────────────────

/// Migration archetypes pre-encoded as configurations.
///
/// Each preset is a self-consistent starting point for a specific
/// operational profile. Tune via builder methods afterwards.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    /// Safe defaults for production deployments where correctness
    /// and stability are prioritized over speed.
    Conservative,
    /// Good defaults for most deployments. Balances speed and safety.
    Balanced,
    /// Fast migrations for latency-sensitive workloads. Shorter
    /// timeouts, higher concurrency, zero-downtime enabled.
    Aggressive,
    /// Large-state migrations using streaming transfer. Longer
    /// timeouts, zero-downtime enabled.
    Streaming,
}

impl Preset {
    /// Materialize the preset into a concrete config.
    ///
    /// # Errors
    /// Returns [`MigrationError::InvalidConfig`] if validation fails
    /// (should not happen for built-in presets).
    pub fn to_config(self) -> Result<MigrationConfig, MigrationError> {
        let cfg = match self {
            Self::Conservative => MigrationConfig::new()
                .with_timeout_ms(60_000)
                .with_max_concurrent(16)
                .with_max_retries(5)
                .with_compression(true)
                .with_ordering(true)
                .with_checkpoint_interval(2_000)
                .with_zero_downtime(false),
            Self::Balanced => MigrationConfig::new()
                .with_timeout_ms(30_000)
                .with_max_concurrent(64)
                .with_max_retries(3)
                .with_compression(true)
                .with_ordering(true)
                .with_checkpoint_interval(5_000)
                .with_zero_downtime(false),
            Self::Aggressive => MigrationConfig::new()
                .with_timeout_ms(10_000)
                .with_max_concurrent(256)
                .with_max_retries(2)
                .with_compression(false)
                .with_ordering(true)
                .with_checkpoint_interval(1_000)
                .with_zero_downtime(true),
            Self::Streaming => MigrationConfig::new()
                .with_timeout_ms(120_000)
                .with_max_concurrent(32)
                .with_max_retries(4)
                .with_compression(false)
                .with_ordering(true)
                .with_checkpoint_interval(10_000)
                .with_zero_downtime(true),
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = MigrationConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.default_timeout_ms, 30_000);
        assert_eq!(cfg.max_concurrent_migrations, 64);
        assert_eq!(cfg.max_retries, 3);
        assert!(cfg.snapshot_compression);
        assert!(cfg.preserve_ordering);
        assert!(!cfg.enable_zero_downtime);
    }

    #[test]
    fn builder_methods_chain() {
        let cfg = MigrationConfig::new()
            .with_timeout_ms(45_000)
            .with_max_concurrent(128)
            .with_max_retries(4)
            .with_compression(false)
            .with_ordering(false)
            .with_checkpoint_interval(3_000)
            .with_zero_downtime(true);
        assert_eq!(cfg.default_timeout_ms, 45_000);
        assert_eq!(cfg.max_concurrent_migrations, 128);
        assert_eq!(cfg.max_retries, 4);
        assert!(!cfg.snapshot_compression);
        assert!(!cfg.preserve_ordering);
        assert!(cfg.enable_zero_downtime);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_bad_timeout() {
        let mut cfg = MigrationConfig::default();
        cfg.default_timeout_ms = 100; // < MIN
        assert!(cfg.validate().is_err());
        cfg.default_timeout_ms = 301_000; // > MAX
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_concurrent() {
        let mut cfg = MigrationConfig::default();
        cfg.max_concurrent_migrations = 0;
        assert!(cfg.validate().is_err());
        cfg.max_concurrent_migrations = 10_001;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_retries() {
        let mut cfg = MigrationConfig::default();
        cfg.max_retries = 11; // > MAX_RETRIES
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_checkpoint_interval() {
        let mut cfg = MigrationConfig::default();
        cfg.checkpoint_interval_ms = 50; // < MIN
        assert!(cfg.validate().is_err());
        cfg.checkpoint_interval_ms = 61_000; // > MAX
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn balanced_preset_matches_defaults() {
        let preset_cfg = Preset::Balanced.to_config().unwrap();
        let default_cfg = MigrationConfig::default();
        assert_eq!(preset_cfg, default_cfg);
    }

    #[test]
    fn all_presets_produce_valid_configs() {
        for preset in [
            Preset::Conservative,
            Preset::Balanced,
            Preset::Aggressive,
            Preset::Streaming,
        ] {
            let cfg = preset.to_config();
            assert!(cfg.is_ok(), "preset {preset:?} produced invalid config");
            assert!(cfg.unwrap().validate().is_ok());
        }
    }

    #[test]
    fn preset_characteristics() {
        let conservative = Preset::Conservative.to_config().unwrap();
        assert!(!conservative.enable_zero_downtime);
        assert_eq!(conservative.default_timeout_ms, 60_000);

        let aggressive = Preset::Aggressive.to_config().unwrap();
        assert!(aggressive.enable_zero_downtime);
        assert_eq!(aggressive.default_timeout_ms, 10_000);

        let streaming = Preset::Streaming.to_config().unwrap();
        assert!(streaming.enable_zero_downtime);
        assert_eq!(streaming.default_timeout_ms, 120_000);
    }
}
