//! [`EventLoop`] — the per-tick drive function.
//!
//! See `DESIGN.md` §3 for the canonical loop ordering. The
//! Phase-1 implementation will follow this exact sequence:
//!
//! ```text
//! 1. park_on_ring(deadline)
//! 2. cqe_proc.drain_cq(&mut ring, now)
//! 3. apply outcomes (recycles / send-queue / timers / tasks)
//! 4. timer_wheel.drain_expired(now)
//! 5. send_queue.flush(&mut sink, now)
//! 6. task_queue.tick(&mut ctx, budget)
//! 7. shard.drain_closed()
//! 8. driver.on_tick_complete(&tick)
//! 9. check_shutdown()
//! ```

use crate::error::ReactorError;
use crate::reactor::Reactor;
use crate::tick::TickResult;
use profiler::profile;

/// Stateless event-loop helper. The actual state lives on
/// [`Reactor`]; this type only namespaces the drive method.
// TODO(phase-1): implement run_one_tick with the canonical loop.
pub struct EventLoop;

impl EventLoop {
    /// Drive the reactor through one tick.
    ///
    /// Phase 0 returns an idle [`TickResult`]; Phase 1 implements
    /// the full loop (see module docs).
    #[profile]
    pub fn run_one_tick(_reactor: &mut Reactor) -> Result<TickResult, ReactorError> {
        // TODO(phase-1): park → drain_cq → drain_expired → flush
        //   → task_queue.tick → shard.drain_closed → on_tick_complete
        //   → check_shutdown.
        Ok(TickResult::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReactorConfig;

    #[test]
    fn placeholder() {
        let mut r = Reactor::placeholder(ReactorConfig::default());
        let tick = EventLoop::run_one_tick(&mut r).expect("phase-0 stub");
        assert!(tick.was_idle());
        // TODO(phase-1): real event loop with CQEs, timers, tasks.
    }
}
