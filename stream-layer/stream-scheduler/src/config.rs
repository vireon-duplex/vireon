//! [`SchedulerConfig`] + [`Preset`] — construction-time
//! configuration for the stream scheduler.
//!
//! See `DESIGN.md` Part 1 (responsibilities) and Part 14
//! (scalability).

use crate::error::SchedulerError;
use crate::policy::SchedulingPolicy;

// ── Defaults ────────────────────────────────────────────────────

/// Default maximum streams considered per tick.
pub const DEFAULT_MAX_STREAMS_PER_TICK: u32 = 64;
/// Default maximum batch size (streams per packet flush).
pub const DEFAULT_BATCH_SIZE_MAX: u32 = 16;
/// Default aging granularity (microseconds).
pub const DEFAULT_AGING_GRANULARITY_US: u64 = 1_000;
/// Default starvation threshold (microseconds).
pub const DEFAULT_STARVATION_THRESHOLD_US: u64 = 10_000;
/// Default incremental share percentage (of each tick's budget).
pub const DEFAULT_INCREMENTAL_SHARE_PCT: u8 = 25;
/// Default fairness-check interval (ticks between checks).
pub const DEFAULT_FAIRNESS_CHECK_INTERVAL_TICKS: u32 = 100;

/// Minimum streams per tick.
pub const MIN_MAX_STREAMS_PER_TICK: u32 = 1;
/// Maximum streams per tick (bounded by batch + packet budget).
pub const MAX_MAX_STREAMS_PER_TICK: u32 = 4_096;
/// Minimum batch size.
pub const MIN_BATCH_SIZE: u32 = 1;
/// Maximum batch size (bounded by packet MTU / GSO).
pub const MAX_BATCH_SIZE: u32 = 256;

/// Construction-time configuration. Built directly or via
/// [`Preset`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchedulerConfig {
    /// Active scheduling policy.
    pub policy: SchedulingPolicy,
    /// Maximum streams considered per tick.
    pub max_streams_per_tick: u32,
    /// Maximum streams per packet flush (GSO batch).
    pub batch_size_max: u32,
    /// Aging granularity — how often to re-evaluate wait times.
    pub aging_granularity_us: u64,
    /// Starvation threshold — promote stream after this wait.
    pub starvation_threshold_us: u64,
    /// Share of each tick's budget allocated to incremental streams.
    pub incremental_share_pct: u8,
    /// Ticks between fairness checks.
    pub fairness_check_interval_ticks: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            policy: SchedulingPolicy::default(),
            max_streams_per_tick: DEFAULT_MAX_STREAMS_PER_TICK,
            batch_size_max: DEFAULT_BATCH_SIZE_MAX,
            aging_granularity_us: DEFAULT_AGING_GRANULARITY_US,
            starvation_threshold_us: DEFAULT_STARVATION_THRESHOLD_US,
            incremental_share_pct: DEFAULT_INCREMENTAL_SHARE_PCT,
            fairness_check_interval_ticks: DEFAULT_FAIRNESS_CHECK_INTERVAL_TICKS,
        }
    }
}

impl SchedulerConfig {
    /// Validate every field.
    ///
    /// # Errors
    /// Returns [`SchedulerError::InvalidConfig`] on any
    /// out-of-range field.
    pub fn validate(&self) -> Result<(), SchedulerError> {
        if !(MIN_MAX_STREAMS_PER_TICK..=MAX_MAX_STREAMS_PER_TICK)
            .contains(&self.max_streams_per_tick)
        {
            return Err(SchedulerError::InvalidConfig(format!(
                "max_streams_per_tick {} out of range",
                self.max_streams_per_tick
            )));
        }
        if !(MIN_BATCH_SIZE..=MAX_BATCH_SIZE).contains(&self.batch_size_max) {
            return Err(SchedulerError::InvalidConfig(format!(
                "batch_size_max {} out of range",
                self.batch_size_max
            )));
        }
        if self.aging_granularity_us == 0 {
            return Err(SchedulerError::InvalidConfig(
                "aging_granularity_us must be > 0".into(),
            ));
        }
        if self.starvation_threshold_us < self.aging_granularity_us {
            return Err(SchedulerError::InvalidConfig(
                "starvation_threshold_us must be >= aging_granularity_us".into(),
            ));
        }
        if self.incremental_share_pct > 100 {
            return Err(SchedulerError::InvalidConfig(format!(
                "incremental_share_pct {} exceeds 100",
                self.incremental_share_pct
            )));
        }
        if self.fairness_check_interval_ticks == 0 {
            return Err(SchedulerError::InvalidConfig(
                "fairness_check_interval_ticks must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// Tuned configuration presets.
///
/// See `DESIGN.md` Part 14 (scalability) for the workload
/// assumptions behind each preset.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    /// Balanced fairness + priority. Default for general workloads.
    Fair,
    /// Strict-priority dominant; fairness is best-effort.
    PriorityHeavy,
    /// Latency-first: small batches, aggressive aging.
    LatencyFirst,
    /// Throughput-first: large batches, GSO-friendly.
    ThroughputFirst,
}

impl Preset {
    /// Materialise the preset into a [`SchedulerConfig`].
    #[must_use]
    pub fn to_config(self) -> SchedulerConfig {
        match self {
            Self::Fair => SchedulerConfig::default(),
            Self::PriorityHeavy => SchedulerConfig {
                policy: SchedulingPolicy::StrictPriority,
                max_streams_per_tick: 128,
                batch_size_max: 32,
                aging_granularity_us: 5_000,
                starvation_threshold_us: 50_000,
                incremental_share_pct: 10,
                fairness_check_interval_ticks: 1_000,
            },
            Self::LatencyFirst => SchedulerConfig {
                policy: SchedulingPolicy::Hybrid,
                max_streams_per_tick: 32,
                batch_size_max: 4,
                aging_granularity_us: 500,
                starvation_threshold_us: 2_000,
                incremental_share_pct: 50,
                fairness_check_interval_ticks: 50,
            },
            Self::ThroughputFirst => SchedulerConfig {
                policy: SchedulingPolicy::WeightedFair,
                max_streams_per_tick: 256,
                batch_size_max: 64,
                aging_granularity_us: 2_000,
                starvation_threshold_us: 20_000,
                incremental_share_pct: 20,
                fairness_check_interval_ticks: 500,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        SchedulerConfig::default().validate().expect("default");
    }

    #[test]
    fn validate_rejects_zero_max_streams() {
        let cfg = SchedulerConfig {
            max_streams_per_tick: 0,
            ..SchedulerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_huge_batch() {
        let cfg = SchedulerConfig {
            batch_size_max: MAX_BATCH_SIZE + 1,
            ..SchedulerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_starvation_below_granularity() {
        let cfg = SchedulerConfig {
            aging_granularity_us: 5_000,
            starvation_threshold_us: 1_000,
            ..SchedulerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_share_over_100() {
        let cfg = SchedulerConfig {
            incremental_share_pct: 101,
            ..SchedulerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn all_presets_validate() {
        for p in [
            Preset::Fair,
            Preset::PriorityHeavy,
            Preset::LatencyFirst,
            Preset::ThroughputFirst,
        ] {
            p.to_config().validate().expect("preset validates");
        }
    }

    #[test]
    fn latency_first_uses_small_batches() {
        let cfg = Preset::LatencyFirst.to_config();
        assert!(cfg.batch_size_max <= 8);
    }

    #[test]
    fn throughput_first_uses_large_batches() {
        let cfg = Preset::ThroughputFirst.to_config();
        assert!(cfg.batch_size_max >= 32);
    }
}
