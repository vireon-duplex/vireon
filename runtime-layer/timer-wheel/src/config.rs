//! Configuration types (Layer 2).
//!
//! [`TimerWheelConfig`] is the plain-data description of a wheel; the fluent
//! [`TimerWheelBuilder`](crate::TimerWheelBuilder) lives next to the
//! [`TimerWheel`](crate::TimerWheel) it constructs.

use crate::error::TimerError;

/// Bits per wheel level. Default: 6 (64 slots/level).
pub const DEFAULT_BITS_PER_LEVEL: u32 = 6;
/// Number of hierarchical levels. Default: 4.
pub const DEFAULT_LEVELS: u32 = 4;
/// Nanoseconds per tick. Default: 1 ms.
pub const DEFAULT_TICK_NS: u64 = 1_000_000;
/// Slab slots per shard. Default: 65 536 outstanding timers.
pub const DEFAULT_CAPACITY_PER_SHARD: usize = 1 << 16;
/// Capacity of each shard's command MPSC. Rounded up to a power of two.
pub const DEFAULT_CMD_QUEUE_CAPACITY: usize = 1024;
/// Park duration when no timer is outstanding. Default: `DEFAULT_TICK_NS`.
pub const DEFAULT_IDLE_INTERVAL_NS: u64 = DEFAULT_TICK_NS;

/// Upper bound on `bits_per_level`.
pub const MAX_BITS_PER_LEVEL: u32 = 16;
/// Upper bound on `levels`.
pub const MAX_LEVELS: u32 = 10;

/// Plain-data configuration for a [`TimerWheel`](crate::TimerWheel).
#[derive(Clone, Copy, Debug)]
pub struct TimerWheelConfig {
    /// Bits per level (each level has `2^bits_per_level` slots).
    pub bits_per_level: u32,
    /// Number of hierarchical levels.
    pub levels: u32,
    /// Nanoseconds per tick.
    pub tick_ns: u64,
    /// Slab slots (max outstanding timers) per shard.
    pub capacity_per_shard: usize,
    /// Number of worker shards. `0` means "use `available_parallelism()`".
    pub shards: u32,
    /// Command-queue capacity per shard (rounded up to a power of two).
    pub cmd_queue_capacity: usize,
    /// Park duration in nanoseconds when no timer is outstanding.
    pub idle_interval_ns: u64,
}

impl TimerWheelConfig {
    /// Number of slots per level (`2^bits_per_level`).
    #[inline]
    #[must_use]
    pub fn slots_per_level(&self) -> usize {
        1usize << self.bits_per_level
    }

    /// Bit mask for a level's slot index.
    #[inline]
    #[must_use]
    pub fn slot_mask(&self) -> usize {
        self.slots_per_level() - 1
    }

    /// Maximum representable horizon in ticks (`2^(bits*levels) - 1`).
    ///
    /// Timers scheduled beyond this horizon return [`TimerError::InvalidDuration`].
    #[inline]
    #[must_use]
    pub fn max_horizon_ticks(&self) -> u64 {
        // bits*levels is validated to be < 64, so the shift is in range.
        (1u64 << (self.bits_per_level * self.levels)).saturating_sub(1)
    }

    /// Validates the configuration and resolves derived defaults.
    ///
    /// Returns a [`TimerError::InvalidConfig`] describing the first violation.
    pub fn validate(&self) -> Result<(), TimerError> {
        let bits = self.bits_per_level;
        let levels = self.levels;
        if bits == 0 {
            return Err(TimerError::InvalidConfig(
                "bits_per_level must be > 0".into(),
            ));
        }
        if bits > MAX_BITS_PER_LEVEL {
            return Err(TimerError::InvalidConfig(format!(
                "bits_per_level must be <= {MAX_BITS_PER_LEVEL}"
            )));
        }
        if levels == 0 {
            return Err(TimerError::InvalidConfig("levels must be > 0".into()));
        }
        if levels > MAX_LEVELS {
            return Err(TimerError::InvalidConfig(format!(
                "levels must be <= {MAX_LEVELS}"
            )));
        }
        // Keep one bit of slack at the top of the deadline so tick math cannot overflow u64.
        if u64::from(bits * levels) >= 63 {
            return Err(TimerError::InvalidConfig(format!(
                "bits_per_level * levels must be <= 62 (is {}); the top deadline bit is reserved for cancellation",
                bits * levels
            )));
        }
        if self.tick_ns == 0 {
            return Err(TimerError::InvalidConfig("tick_ns must be > 0".into()));
        }
        if self.capacity_per_shard == 0 {
            return Err(TimerError::InvalidConfig(
                "capacity_per_shard must be > 0".into(),
            ));
        }
        // capacity must leave room for the slot-0 null sentinel.
        if self.capacity_per_shard >= u32::MAX as usize {
            return Err(TimerError::InvalidConfig(
                "capacity_per_shard must be < u32::MAX".into(),
            ));
        }
        if self.shards == 0 {
            return Err(TimerError::InvalidConfig(
                "shards must be > 0 (or use builder default)".into(),
            ));
        }
        if self.cmd_queue_capacity == 0 {
            return Err(TimerError::InvalidConfig(
                "cmd_queue_capacity must be > 0".into(),
            ));
        }
        if self.idle_interval_ns == 0 {
            return Err(TimerError::InvalidConfig(
                "idle_interval_ns must be > 0".into(),
            ));
        }
        Ok(())
    }

    /// Rounds `cmd_queue_capacity` up to the next power of two (in place).
    pub fn round_cmd_queue_up(&mut self) {
        self.cmd_queue_capacity = self.cmd_queue_capacity.next_power_of_two();
    }
}

impl Default for TimerWheelConfig {
    fn default() -> Self {
        let shards = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        Self {
            bits_per_level: DEFAULT_BITS_PER_LEVEL,
            levels: DEFAULT_LEVELS,
            tick_ns: DEFAULT_TICK_NS,
            capacity_per_shard: DEFAULT_CAPACITY_PER_SHARD,
            shards,
            cmd_queue_capacity: DEFAULT_CMD_QUEUE_CAPACITY,
            idle_interval_ns: DEFAULT_IDLE_INTERVAL_NS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> TimerWheelConfig {
        TimerWheelConfig {
            bits_per_level: 6,
            levels: 4,
            tick_ns: 1_000_000,
            capacity_per_shard: 1024,
            shards: 2,
            cmd_queue_capacity: 256,
            idle_interval_ns: 1_000_000,
        }
    }

    #[test]
    fn default_uses_available_parallelism() {
        let cfg = TimerWheelConfig::default();
        assert!(cfg.shards >= 1);
        assert_eq!(cfg.bits_per_level, DEFAULT_BITS_PER_LEVEL);
        assert_eq!(cfg.levels, DEFAULT_LEVELS);
        cfg.validate().expect("default validates");
    }

    #[test]
    fn slots_and_mask_are_power_of_two() {
        let cfg = valid();
        assert_eq!(cfg.slots_per_level(), 64);
        assert_eq!(cfg.slot_mask(), 63);
        assert!(cfg.slots_per_level().is_power_of_two());
    }

    #[test]
    fn horizon_matches_bit_product() {
        let cfg = valid();
        assert_eq!(cfg.max_horizon_ticks(), (1u64 << 24) - 1);
    }

    #[test]
    fn rejects_zero_bits() {
        let mut cfg = valid();
        cfg.bits_per_level = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_bit_product_at_or_above_63() {
        let mut cfg = valid();
        // 8 bits * 8 levels = 64 > 62.
        cfg.bits_per_level = 8;
        cfg.levels = 8;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_max_product_62() {
        let mut cfg = valid();
        cfg.bits_per_level = 6;
        cfg.levels = 10; // 6 * 10 = 60 <= 62
        assert_eq!(cfg.max_horizon_ticks(), (1u64 << 60) - 1);
        cfg.validate().expect("60-bit product ok");
    }

    #[test]
    fn rejects_zero_tick() {
        let mut cfg = valid();
        cfg.tick_ns = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_capacity() {
        let mut cfg = valid();
        cfg.capacity_per_shard = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_shards() {
        let mut cfg = valid();
        cfg.shards = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn round_cmd_queue_moves_to_next_power_of_two() {
        let mut cfg = valid();
        cfg.cmd_queue_capacity = 1000;
        cfg.round_cmd_queue_up();
        assert_eq!(cfg.cmd_queue_capacity, 1024);
    }
}
