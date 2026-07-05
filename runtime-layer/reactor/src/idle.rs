//! [`IdleStrategy`] — what the reactor does when the ring is
//! empty and no timers are firing.
//!
//! See `DESIGN.md` §3 (event loop) for where this is consulted.
//!
//! # Behaviour per variant
//!
//! | Variant      | Ring empty                       | Latency | CPU |
//! |--------------|----------------------------------|---------|-----|
//! | `Block`      | `io_uring_enter(IORING_ENTER_GETEVENTS)` | wake-on-CQE | 0 % |
//! | `Spin`       | busy-loop reaping                | min     | 100 % |
//! | `Hybrid`     | spin for `spin_us`, then block   | mid     | mid |

/// Default spin time for `Hybrid` (50 μs).
pub const DEFAULT_HYBRID_SPIN_US: u32 = 50;
/// Maximum allowed spin time for `Hybrid` (10 ms).
pub const MAX_HYBRID_SPIN_US: u32 = 10_000;

/// What to do when the reactor has nothing to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdleStrategy {
    /// Block in `io_uring_enter` with `IORING_ENTER_GETEVENTS`.
    /// Zero CPU when idle, wake latency ≈ kernel scheduler.
    Block,
    /// Busy-spin reaping the CQ. Lowest wake latency, burns a
    /// full core. Use on dedicated pin cores.
    Spin,
    /// Spin for `spin_us` microseconds; if still idle, fall back
    /// to `Block`. Matches the Glommio / tokio-uring hybrid.
    Hybrid {
        /// Microseconds to spin before blocking.
        spin_us: u32,
    },
}

impl Default for IdleStrategy {
    fn default() -> Self {
        Self::Hybrid {
            spin_us: DEFAULT_HYBRID_SPIN_US,
        }
    }
}

impl IdleStrategy {
    /// Construct a `Hybrid` with the given spin budget.
    #[must_use]
    pub const fn hybrid(spin_us: u32) -> Self {
        Self::Hybrid { spin_us }
    }

    /// Whether this strategy ever busy-spins.
    #[must_use]
    pub const fn spins(&self) -> bool {
        matches!(self, Self::Spin | Self::Hybrid { .. })
    }

    /// Whether this strategy ever blocks in the kernel.
    #[must_use]
    pub const fn blocks(&self) -> bool {
        matches!(self, Self::Block | Self::Hybrid { .. })
    }

    /// Validate any spin budget. Returns `Err` if `spin_us` is
    /// out of range for the `Hybrid` variant.
    ///
    /// # Errors
    /// Returns [`ReactorError::InvalidConfig`](crate::error::ReactorError::InvalidConfig)
    /// on out-of-range spin time.
    pub fn validate(&self) -> Result<(), crate::error::ReactorError> {
        if let Self::Hybrid { spin_us } = self {
            if *spin_us > MAX_HYBRID_SPIN_US {
                return Err(crate::error::ReactorError::InvalidConfig(format!(
                    "hybrid spin_us {spin_us} exceeds max {MAX_HYBRID_SPIN_US}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_hybrid_50us() {
        assert_eq!(
            IdleStrategy::default(),
            IdleStrategy::Hybrid { spin_us: 50 }
        );
    }

    #[test]
    fn block_does_not_spin() {
        assert!(!IdleStrategy::Block.spins());
        assert!(IdleStrategy::Block.blocks());
    }

    #[test]
    fn spin_does_spin() {
        assert!(IdleStrategy::Spin.spins());
        assert!(!IdleStrategy::Spin.blocks());
    }

    #[test]
    fn hybrid_does_both() {
        let h = IdleStrategy::hybrid(100);
        assert!(h.spins());
        assert!(h.blocks());
    }

    #[test]
    fn validate_rejects_huge_spin() {
        let h = IdleStrategy::hybrid(MAX_HYBRID_SPIN_US + 1);
        assert!(h.validate().is_err());
    }

    #[test]
    fn validate_accepts_block() -> Result<(), crate::error::ReactorError> {
        IdleStrategy::Block.validate()?;
        IdleStrategy::hybrid(100).validate()?;
        Ok(())
    }
}
