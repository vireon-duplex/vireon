//! Lifecycle policies (Layer 24): zeroing, shrinking, pressure response.
//!
//! These are plain enums so a [`BufferPoolConfig`](super::config::BufferPoolConfig)
//! can carry them by value without pulling in heavier types.

/// How a buffer's memory is cleared before reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroingPolicy {
    /// No clearing — fastest, but the next owner observes stale bytes.
    None,
    /// Zero on release via `memset`; the optimiser may elide dead stores.
    Clear,
    /// Volatile zeroing the optimiser cannot remove — for secret-bearing data.
    Secure,
    /// Skip zeroing on release; zero lazily on the next acquire instead.
    Lazy,
}

impl Default for ZeroingPolicy {
    /// Defaults to [`ZeroingPolicy::Clear`] — safe-by-default without paying
    /// the volatile-write cost of [`ZeroingPolicy::Secure`] on every release.
    #[inline]
    fn default() -> Self {
        ZeroingPolicy::Clear
    }
}

/// Aggressiveness of the background shrinker (Layer 18).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShrinkPolicy {
    /// Never shrink automatically; only [`BufferPool::shrink`] does.
    ///
    /// [`BufferPool::shrink`]: crate::buffer::BufferPool::shrink
    Never,
    /// Drop idle buffers only when memory pressure rises.
    OnPressure,
    /// Continuously return idle buffers to the operating system.
    Aggressive,
}

impl Default for ShrinkPolicy {
    #[inline]
    fn default() -> Self {
        ShrinkPolicy::OnPressure
    }
}

/// Categorical memory-pressure level (Layer 16).
///
/// Reported by [`maintenance::pressure`](crate::maintenance) and consumed by
/// the shrinker and backpressure gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureLevel {
    /// RSS is well below the configured budget.
    Low,
    /// RSS is approaching the budget; start trimming caches.
    Moderate,
    /// RSS is near the budget; shed aggressively.
    High,
    /// RSS has exceeded the budget; reject new allocations.
    Critical,
}

impl Default for PressureLevel {
    #[inline]
    fn default() -> Self {
        PressureLevel::Low
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_safe() {
        assert_eq!(ZeroingPolicy::default(), ZeroingPolicy::Clear);
        assert_eq!(ShrinkPolicy::default(), ShrinkPolicy::OnPressure);
        assert_eq!(PressureLevel::default(), PressureLevel::Low);
    }

    #[test]
    fn pressure_levels_are_ordered() {
        assert!(PressureLevel::Low < PressureLevel::Moderate);
        assert!(PressureLevel::Moderate < PressureLevel::High);
        assert!(PressureLevel::High < PressureLevel::Critical);
    }

    #[test]
    fn policies_are_copy() {
        let z = ZeroingPolicy::Secure;
        let z2 = z;
        assert_eq!(z, z2);
    }
}
