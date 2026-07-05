//! [`Reactor`] — the facade that owns one io_uring ring and every
//! runtime-layer primitive.
//!
//! See `DESIGN.md` §1 (responsibilities) and §2 (architecture).
//!
//! # Ownership (Phase 1+)
//!
//! [`Reactor`] will own:
//! - `RingOwner` (Phase 2) — the io_uring ring
//! - `EventfdWake` (Phase 3) — cross-thread wake
//! - `ProvidedBufferRing` (Phase 4)
//! - `buffer_pool::BufferPool`
//! - `cqe_processor::CqeProcessor`
//! - `timer_wheel::TimerWheel`
//! - `connection_shard::ConnectionShard`
//! - `send_queue::SendQueue`
//! - `task_queue::TaskQueue`
//! - `ShutdownCoordinator` (Phase 5)
//! - A `ReactorDriver` impl (passed in at start)

use crate::config::ReactorConfig;
use crate::stats::ReactorStats;

/// The reactor. One per CPU core, pinned via
/// [`CpuPinning`](crate::pin::CpuPinning).
// TODO(phase-1): populate with real ownership + lifecycle.
#[derive(Debug)]
pub struct Reactor {
    /// Configuration snapshot — immutable after build.
    config: ReactorConfig,
    /// Atomic counters — safe to share across threads for stats
    /// scraping even while the reactor thread is ticking.
    stats: ReactorStats,
}

impl Reactor {
    /// Phase-0 placeholder constructor. Phase 1 will replace this
    /// with a real constructor invoked from
    /// [`ReactorBuilder::build`](crate::builder::ReactorBuilder::build).
    #[must_use]
    pub const fn placeholder(config: ReactorConfig) -> Self {
        Self {
            config,
            stats: ReactorStats::new(),
        }
    }

    /// Read-only access to the configuration.
    #[must_use]
    pub const fn config(&self) -> &ReactorConfig {
        &self.config
    }

    /// Atomic stats. Always safe to call from any thread.
    #[must_use]
    pub const fn stats(&self) -> &ReactorStats {
        &self.stats
    }

    // TODO(phase-1):
    //   pub fn tick<D: ReactorDriver>(&mut self, driver: &mut D)
    //       -> Result<TickResult, ReactorError>
    //   pub fn shutdown(&mut self) -> Result<(), ReactorError>
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_constructs() {
        let r = Reactor::placeholder(ReactorConfig::default());
        assert_eq!(r.config().ring_entries, crate::config::DEFAULT_RING_ENTRIES);
        // Stats start at zero.
        let snap = r.stats().snapshot();
        assert_eq!(snap.ticks_total, 0);
        // TODO(phase-1): real ownership + tick.
    }
}
