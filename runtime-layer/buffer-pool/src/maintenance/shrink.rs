//! Shrinker (Layer 18).
//!
//! [`maybe_shrink`] trims caches and purges the arena when the configured
//! [`ShrinkPolicy`] and current [`PressureLevel`] warrant it.

use crate::buffer::policy::{PressureLevel, ShrinkPolicy};
use crate::buffer::pool::BufferPool;
use crate::maintenance::pressure::should_shrink;

/// Runs the shrinker if `policy` and `level` warrant it.
///
/// Returns `true` when a shrink actually ran.
pub fn maybe_shrink(pool: &BufferPool, level: PressureLevel, policy: ShrinkPolicy) -> bool {
    if should_shrink(level, policy) {
        pool.shrink();
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maybe_shrink_skips_when_never_policy() {
        let pool = BufferPool::default();
        assert!(!maybe_shrink(
            &pool,
            PressureLevel::Critical,
            ShrinkPolicy::Never
        ));
    }

    #[test]
    fn maybe_shrink_skips_when_low_pressure_on_pressure_policy() {
        let pool = BufferPool::default();
        assert!(!maybe_shrink(
            &pool,
            PressureLevel::Low,
            ShrinkPolicy::OnPressure
        ));
    }

    #[test]
    fn maybe_shrink_runs_when_moderate_and_on_pressure() {
        let pool = BufferPool::default();
        assert!(maybe_shrink(
            &pool,
            PressureLevel::Moderate,
            ShrinkPolicy::OnPressure
        ));
    }

    #[test]
    fn maybe_shrink_always_runs_for_aggressive() {
        let pool = BufferPool::default();
        assert!(maybe_shrink(
            &pool,
            PressureLevel::Low,
            ShrinkPolicy::Aggressive
        ));
    }
}
