//! Configuration types (Layer 2).
//!
//! [`SendQueueConfig`] is the plain-data description of a queue; the fluent
//! [`SendQueueBuilder`](crate::SendQueueBuilder) lives next to the
//! [`SendQueue`](crate::SendQueue) it constructs.

use crate::error::SendQueueError;

// ── Defaults ──────────────────────────────────────────────────────────

/// Default ring capacity (must be a power of two).
pub const DEFAULT_CAPACITY: usize = 4096;
/// Default maximum packets per batch flush.
pub const DEFAULT_BATCH_MAX_PACKETS: usize = 64;
/// Default maximum bytes per batch flush.
pub const DEFAULT_BATCH_MAX_BYTES: usize = 65_536;
/// Default maximum nanoseconds a packet waits in a batch before flush.
pub const DEFAULT_BATCH_MAX_AGE_NS: u64 = 50_000;
/// Default low watermark percentage of capacity.
pub const DEFAULT_LOW_WATERMARK_PCT: u8 = 75;
/// Default high watermark percentage of capacity.
pub const DEFAULT_HIGH_WATERMARK_PCT: u8 = 90;
/// Default hysteresis band as a percentage of capacity.
pub const DEFAULT_HYSTERESIS_PCT: u8 = 10;

/// Default initial RTO (retransmit timeout) in nanoseconds.
pub const DEFAULT_INITIAL_RTO_NS: u64 = 1_000_000;
/// Default minimum RTO floor.
pub const DEFAULT_MIN_RTO_NS: u64 = 200_000;
/// Default maximum RTO ceiling.
pub const DEFAULT_MAX_RTO_NS: u64 = 60_000_000;
/// Default maximum retry attempts before dropping.
pub const DEFAULT_MAX_RETRIES: u8 = 3;

// ── Bounds ────────────────────────────────────────────────────────────

/// Absolute minimum ring capacity.
pub const MIN_CAPACITY: usize = 16;
/// Absolute maximum ring capacity (`2^20` slots).
pub const MAX_CAPACITY: usize = 1 << 20;
/// Absolute minimum batch size.
pub const MIN_BATCH_PACKETS: usize = 1;

// ── Schedule policy kind ─────────────────────────────────────────────

/// Selectable scheduling policies.
///
/// The actual policy implementations live in the [`scheduler`](crate::scheduler)
/// module; this enum tags which one to construct at build time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SchedulePolicyKind {
    /// First-in-first-out — submission order is transmission order.
    #[default]
    Fifo,
    /// Lower `priority` value transmitted first.
    Priority,
    /// Earliest `deadline_ns` transmitted first.
    Deadline,
    /// Per-stream weighted fair queue.
    WeightedFair,
}

// ── Retry config ──────────────────────────────────────────────────────

/// Retry / retransmission parameters.
#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    /// Initial RTO before any RTT sample (nanoseconds).
    pub initial_rto_ns: u64,
    /// Minimum RTO floor (clamps adaptive estimates).
    pub min_rto_ns: u64,
    /// Maximum RTO ceiling.
    pub max_rto_ns: u64,
    /// Maximum retry attempts before giving up.
    pub max_retries: u8,
    /// Whether to enable adaptive RTT estimation (future: SRTT + RTTVAR).
    pub adaptive_rtt: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_rto_ns: DEFAULT_INITIAL_RTO_NS,
            min_rto_ns: DEFAULT_MIN_RTO_NS,
            max_rto_ns: DEFAULT_MAX_RTO_NS,
            max_retries: DEFAULT_MAX_RETRIES,
            adaptive_rtt: false,
        }
    }
}

impl RetryConfig {
    /// Validates the retry parameters.
    pub fn validate(&self) -> Result<(), SendQueueError> {
        if self.min_rto_ns > self.initial_rto_ns {
            return Err(SendQueueError::InvalidConfig(format!(
                "min_rto_ns ({}) must be <= initial_rto_ns ({})",
                self.min_rto_ns, self.initial_rto_ns
            )));
        }
        if self.initial_rto_ns > self.max_rto_ns {
            return Err(SendQueueError::InvalidConfig(format!(
                "initial_rto_ns ({}) must be <= max_rto_ns ({})",
                self.initial_rto_ns, self.max_rto_ns
            )));
        }
        if self.max_retries > 16 {
            return Err(SendQueueError::InvalidConfig(format!(
                "max_retries must be <= 16 (is {})",
                self.max_retries
            )));
        }
        Ok(())
    }
}

// ── Main config ───────────────────────────────────────────────────────

/// Plain-data configuration for a [`SendQueue`](crate::SendQueue).
#[derive(Clone, Debug)]
pub struct SendQueueConfig {
    /// Ingress ring capacity (rounded up to a power of two).
    pub capacity: usize,

    /// Scheduling policy.
    pub policy: SchedulePolicyKind,

    /// Maximum packets per batch flush.
    pub batch_max_packets: usize,
    /// Maximum bytes per batch flush.
    pub batch_max_bytes: usize,
    /// Maximum nanoseconds a packet waits in a batch before flush.
    pub batch_max_age_ns: u64,
    /// Whether to coalesce same-destination packets into one buffer.
    pub coalesce_enabled: bool,

    /// Low watermark (depth at which `Elevated` pressure begins).
    pub low_watermark: usize,
    /// High watermark (depth at which `Saturated` pressure begins).
    pub high_watermark: usize,
    /// Hysteresis band for pressure transitions.
    pub hysteresis: usize,

    /// Retry / retransmission configuration.
    pub retry: RetryConfig,
}

impl SendQueueConfig {
    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), SendQueueError> {
        if self.capacity < MIN_CAPACITY {
            return Err(SendQueueError::InvalidConfig(format!(
                "capacity must be >= {MIN_CAPACITY} (is {})",
                self.capacity
            )));
        }
        if self.capacity > MAX_CAPACITY {
            return Err(SendQueueError::InvalidConfig(format!(
                "capacity must be <= {MAX_CAPACITY} (is {})",
                self.capacity
            )));
        }
        if !self.capacity.is_power_of_two() {
            return Err(SendQueueError::InvalidConfig(format!(
                "capacity must be a power of two (is {})",
                self.capacity
            )));
        }

        if self.batch_max_packets < MIN_BATCH_PACKETS {
            return Err(SendQueueError::InvalidConfig(format!(
                "batch_max_packets must be >= {MIN_BATCH_PACKETS} (is {})",
                self.batch_max_packets
            )));
        }
        if self.batch_max_packets > self.capacity {
            return Err(SendQueueError::InvalidConfig(format!(
                "batch_max_packets ({}) must be <= capacity ({})",
                self.batch_max_packets, self.capacity
            )));
        }
        if self.batch_max_bytes == 0 {
            return Err(SendQueueError::InvalidConfig(
                "batch_max_bytes must be > 0".into(),
            ));
        }
        if self.batch_max_age_ns == 0 {
            return Err(SendQueueError::InvalidConfig(
                "batch_max_age_ns must be > 0".into(),
            ));
        }

        if self.low_watermark >= self.high_watermark {
            return Err(SendQueueError::InvalidConfig(format!(
                "low_watermark ({}) must be < high_watermark ({})",
                self.low_watermark, self.high_watermark
            )));
        }
        if self.high_watermark > self.capacity {
            return Err(SendQueueError::InvalidConfig(format!(
                "high_watermark ({}) must be <= capacity ({})",
                self.high_watermark, self.capacity
            )));
        }
        if self.hysteresis >= self.high_watermark.saturating_sub(self.low_watermark) {
            return Err(SendQueueError::InvalidConfig(format!(
                "hysteresis ({}) must be < (high - low) = {}",
                self.hysteresis,
                self.high_watermark.saturating_sub(self.low_watermark)
            )));
        }

        self.retry.validate()?;
        Ok(())
    }

    /// Rounds `capacity` up to the next power of two (in place).
    pub fn round_capacity_up(&mut self) {
        self.capacity = self.capacity.next_power_of_two();
    }
}

impl Default for SendQueueConfig {
    fn default() -> Self {
        let capacity = DEFAULT_CAPACITY;
        let low = capacity * DEFAULT_LOW_WATERMARK_PCT as usize / 100;
        let high = capacity * DEFAULT_HIGH_WATERMARK_PCT as usize / 100;
        let hysteresis = capacity * DEFAULT_HYSTERESIS_PCT as usize / 100;
        Self {
            capacity,
            policy: SchedulePolicyKind::default(),
            batch_max_packets: DEFAULT_BATCH_MAX_PACKETS,
            batch_max_bytes: DEFAULT_BATCH_MAX_BYTES,
            batch_max_age_ns: DEFAULT_BATCH_MAX_AGE_NS,
            coalesce_enabled: false,
            low_watermark: low,
            high_watermark: high,
            hysteresis,
            retry: RetryConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_validates() {
        SendQueueConfig::default()
            .validate()
            .expect("default validates");
    }

    #[test]
    fn rejects_non_power_of_two() {
        let cfg = SendQueueConfig {
            capacity: 1000,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_too_small() {
        let cfg = SendQueueConfig {
            capacity: 4,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_watermark_inversion() {
        let cfg = SendQueueConfig {
            low_watermark: 3000,
            high_watermark: 2000,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_hysteresis_too_large() {
        // low=3072, high=3686, diff=614
        let cfg = SendQueueConfig {
            hysteresis: 700,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_batch_bytes() {
        let cfg = SendQueueConfig {
            batch_max_bytes: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_batch_larger_than_capacity() {
        let cfg = SendQueueConfig {
            batch_max_packets: DEFAULT_CAPACITY + 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_rto_inversion() {
        let cfg = SendQueueConfig {
            retry: RetryConfig {
                min_rto_ns: 2_000_000,
                initial_rto_ns: 1_000_000,
                ..RetryConfig::default()
            },
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_excessive_retries() {
        let cfg = SendQueueConfig {
            retry: RetryConfig {
                max_retries: 20,
                ..RetryConfig::default()
            },
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn round_capacity_up_works() {
        let mut cfg = SendQueueConfig {
            capacity: 1000,
            ..Default::default()
        };
        cfg.round_capacity_up();
        assert_eq!(cfg.capacity, 1024);
    }
}
