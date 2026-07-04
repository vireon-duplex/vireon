//! Typed errors for the `timer-wheel` crate.
//!
//! Every fallible public operation returns [`TimerError`]; no site in
//! non-test code panics (per `AGENTS.md` rule 1).

/// Identifier of a shard within a [`TimerWheel`](crate::TimerWheel).
pub type ShardId = u32;

/// Errors returned by [`TimerWheel`](crate::TimerWheel) operations.
#[derive(Debug, thiserror::Error)]
pub enum TimerError {
    /// The selected shard's command queue is full (backpressure).
    ///
    /// The caller should shed load, retry, or raise `cmd_queue_capacity`.
    #[error("shard {shard} command queue is full ({capacity})")]
    ShardFull {
        /// Shard that rejected the command.
        shard: ShardId,
        /// Configured queue capacity.
        capacity: usize,
    },

    /// The per-shard slab is exhausted — too many outstanding timers.
    #[error("shard {shard} slab exhausted ({capacity} outstanding timers)")]
    WheelFull {
        /// Shard that ran out of slots.
        shard: ShardId,
        /// Configured slab capacity.
        capacity: usize,
    },

    /// The requested duration cannot be represented by the wheel.
    ///
    /// Either it is zero/negative-equivalent, or it exceeds the wheel's
    /// maximum horizon (`2 ^ (bits_per_level * levels)` ticks).
    #[error("duration {nanos} ns is outside the wheel's representable range")]
    InvalidDuration {
        /// Requested duration in nanoseconds.
        nanos: u128,
    },

    /// The supplied configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// The wheel has been (or is being) shut down and refuses new commands.
    #[error("timer wheel is shutting down")]
    ShuttingDown,

    /// A cancel handle did not resolve to a live timer (already fired or
    /// unknown).
    #[error("timer handle {{ shard: {shard}, slot: {slot} }} is unknown or already fired")]
    UnknownHandle {
        /// Shard the handle pointed at.
        shard: ShardId,
        /// Slab slot the handle pointed at.
        slot: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_full_formats_capacity() {
        let err = TimerError::ShardFull {
            shard: 2,
            capacity: 1024,
        };
        assert_eq!(err.to_string(), "shard 2 command queue is full (1024)");
    }

    #[test]
    fn wheel_full_formats_capacity() {
        let err = TimerError::WheelFull {
            shard: 0,
            capacity: 65_536,
        };
        assert_eq!(
            err.to_string(),
            "shard 0 slab exhausted (65536 outstanding timers)"
        );
    }

    #[test]
    fn invalid_duration_carries_nanos() {
        let err = TimerError::InvalidDuration { nanos: 5_000_000 };
        assert_eq!(
            err.to_string(),
            "duration 5000000 ns is outside the wheel's representable range"
        );
    }

    #[test]
    fn invalid_config_carries_context() {
        let err = TimerError::InvalidConfig("bits_per_level must be > 0".into());
        assert_eq!(
            err.to_string(),
            "invalid configuration: bits_per_level must be > 0"
        );
    }

    #[test]
    fn shutting_down_is_static() {
        assert_eq!(
            TimerError::ShuttingDown.to_string(),
            "timer wheel is shutting down"
        );
    }

    #[test]
    fn unknown_handle_formats_locator() {
        let err = TimerError::UnknownHandle { shard: 1, slot: 7 };
        let msg = err.to_string();
        assert!(msg.contains("shard: 1"));
        assert!(msg.contains("slot: 7"));
    }
}
