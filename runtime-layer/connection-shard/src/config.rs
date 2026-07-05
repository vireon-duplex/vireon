//! [`ShardConfig`] + [`Preset`] (Layer 2).
//!
//! Configuration for a [`ConnectionShard`](crate::shard::ConnectionShard).
//! Follows the workspace pattern: a plain struct with a `validate()`
//! method, sensible defaults, and named presets for common deployments.

use crate::error::ShardError;
use crate::id::SLOT_BITS;

/// Default slab capacity (number of connection slots pre-allocated).
pub const DEFAULT_CAPACITY: usize = 65_536;

/// Minimum allowed capacity.
pub const MIN_CAPACITY: usize = 4;

/// Maximum allowed capacity (limited by the 20-bit slot index field).
pub const MAX_CAPACITY: usize = 1 << SLOT_BITS; // 1 048 576

/// Default soft-backpressure threshold (percentage of capacity).
pub const DEFAULT_SOFT_BP_PCT: u8 = 80;

/// Default hard-backpressure threshold (percentage of capacity).
pub const DEFAULT_HARD_BP_PCT: u8 = 95;

/// Configuration for a [`ConnectionShard`](crate::shard::ConnectionShard).
#[derive(Clone, Debug)]
pub struct ShardConfig {
    /// Maximum number of connections the shard can hold.
    pub capacity: usize,

    /// Percentage of `capacity` at which soft backpressure engages.
    ///
    /// Must be in `[1, 99]` and strictly less than `hard_bp_pct`.
    pub soft_bp_pct: u8,

    /// Percentage of `capacity` at which hard backpressure engages.
    ///
    /// Must be in `[1, 100]` and strictly greater than `soft_bp_pct`.
    pub hard_bp_pct: u8,

    /// The reactor / core id that owns this shard.
    pub reactor_id: u32,

    /// NUMA node affinity hint (for future allocator integration).
    pub numa_node: u32,

    /// Whether the shard is in a pre-shutdown state.
    pub shutting_down: bool,
}

impl ShardConfig {
    /// Validates the configuration and returns `Err` on violation.
    pub fn validate(&self) -> Result<(), ShardError> {
        if self.capacity < MIN_CAPACITY {
            return Err(ShardError::InvalidConfig(format!(
                "capacity {capacity} < MIN_CAPACITY {MIN_CAPACITY}",
                capacity = self.capacity
            )));
        }
        if self.capacity > MAX_CAPACITY {
            return Err(ShardError::InvalidConfig(format!(
                "capacity {capacity} > MAX_CAPACITY {MAX_CAPACITY}",
                capacity = self.capacity
            )));
        }
        if self.soft_bp_pct == 0 || self.soft_bp_pct >= 100 {
            return Err(ShardError::InvalidConfig(format!(
                "soft_bp_pct must be in [1, 99] (is {})",
                self.soft_bp_pct
            )));
        }
        if self.hard_bp_pct == 0 || self.hard_bp_pct > 100 {
            return Err(ShardError::InvalidConfig(format!(
                "hard_bp_pct must be in [1, 100] (is {})",
                self.hard_bp_pct
            )));
        }
        if self.soft_bp_pct >= self.hard_bp_pct {
            return Err(ShardError::InvalidConfig(format!(
                "soft_bp_pct ({}) must be < hard_bp_pct ({})",
                self.soft_bp_pct, self.hard_bp_pct
            )));
        }
        Ok(())
    }

    /// Computes the absolute soft-backpressure threshold (connection count).
    #[must_use]
    pub fn soft_threshold(&self) -> usize {
        (self.capacity as u64 * u64::from(self.soft_bp_pct) / 100) as usize
    }

    /// Computes the absolute hard-backpressure threshold (connection count).
    #[must_use]
    pub fn hard_threshold(&self) -> usize {
        (self.capacity as u64 * u64::from(self.hard_bp_pct) / 100) as usize
    }
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_CAPACITY,
            soft_bp_pct: DEFAULT_SOFT_BP_PCT,
            hard_bp_pct: DEFAULT_HARD_BP_PCT,
            reactor_id: 0,
            numa_node: 0,
            shutting_down: false,
        }
    }
}

/// Named configuration presets for common deployment scenarios.
///
/// Each preset populates a [`ShardConfig`] with values tuned for the
/// target environment.
#[derive(Clone, Copy, Debug)]
pub enum Preset {
    /// Cloud VMs — smaller capacity, aggressive backpressure.
    Cloud,
    /// Bare-metal servers — balanced defaults.
    BareMetal,
    /// 100 GbE NICs — large capacity for high throughput.
    HundredGbe,
    /// Latency-sensitive workloads — lower capacity, earlier BP.
    LowLatency,
    /// Maximum throughput — very large capacity, relaxed BP.
    MaxThroughput,
}

impl Preset {
    /// Materializes the preset into a validated [`ShardConfig`].
    ///
    /// # Panics
    /// Never — all presets are hard-coded to valid values. If validation
    /// somehow fails the config is replaced with [`ShardConfig::default`].
    #[must_use]
    pub fn to_config(self) -> ShardConfig {
        let config = match self {
            Self::Cloud => ShardConfig {
                capacity: 16_384,
                soft_bp_pct: 70,
                hard_bp_pct: 90,
                ..ShardConfig::default()
            },
            Self::BareMetal => ShardConfig::default(),
            Self::HundredGbe => ShardConfig {
                capacity: 131_072,
                soft_bp_pct: 82,
                hard_bp_pct: 96,
                ..ShardConfig::default()
            },
            Self::LowLatency => ShardConfig {
                capacity: 32_768,
                soft_bp_pct: 60,
                hard_bp_pct: 85,
                ..ShardConfig::default()
            },
            Self::MaxThroughput => ShardConfig {
                capacity: 262_144,
                soft_bp_pct: 85,
                hard_bp_pct: 98,
                ..ShardConfig::default()
            },
        };
        if config.validate().is_err() {
            return ShardConfig::default();
        }
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        assert!(ShardConfig::default().validate().is_ok());
    }

    #[test]
    fn capacity_below_min_rejected() {
        let c = ShardConfig {
            capacity: MIN_CAPACITY - 1,
            ..ShardConfig::default()
        };
        assert!(matches!(c.validate(), Err(ShardError::InvalidConfig(_))));
    }

    #[test]
    fn capacity_above_max_rejected() {
        let c = ShardConfig {
            capacity: MAX_CAPACITY + 1,
            ..ShardConfig::default()
        };
        assert!(matches!(c.validate(), Err(ShardError::InvalidConfig(_))));
    }

    #[test]
    fn soft_bp_zero_rejected() {
        let c = ShardConfig {
            soft_bp_pct: 0,
            ..ShardConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn hard_bp_above_100_rejected() {
        let c = ShardConfig {
            hard_bp_pct: 101,
            ..ShardConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn soft_ge_hard_rejected() {
        let c = ShardConfig {
            soft_bp_pct: 90,
            hard_bp_pct: 90,
            ..ShardConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn thresholds_are_consistent() {
        let c = ShardConfig {
            capacity: 10_000,
            soft_bp_pct: 80,
            hard_bp_pct: 95,
            ..ShardConfig::default()
        };
        assert_eq!(c.soft_threshold(), 8_000);
        assert_eq!(c.hard_threshold(), 9_500);
        assert!(c.soft_threshold() < c.hard_threshold());
    }

    #[test]
    fn all_presets_validate() {
        for preset in [
            Preset::Cloud,
            Preset::BareMetal,
            Preset::HundredGbe,
            Preset::LowLatency,
            Preset::MaxThroughput,
        ] {
            let config = preset.to_config();
            let result = config.validate();
            assert!(result.is_ok(), "preset {preset:?} failed: {result:?}");
        }
    }

    #[test]
    fn cloud_preset_values() {
        let c = Preset::Cloud.to_config();
        assert_eq!(c.capacity, 16_384);
        assert_eq!(c.soft_bp_pct, 70);
        assert_eq!(c.hard_bp_pct, 90);
    }

    #[test]
    fn baremetal_matches_default() {
        assert_eq!(
            Preset::BareMetal.to_config().capacity,
            ShardConfig::default().capacity
        );
    }
}
