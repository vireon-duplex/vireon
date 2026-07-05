//! ShutdownManager — phase machine (Layer 20).
//!
//! Phase 1 implementation tracks only the phase flag; the full drain
//! loop with grace period lives in [`TaskQueue::shutdown`](crate::queue::TaskQueue::shutdown).

use std::sync::atomic::{AtomicU8, Ordering};

const RUNNING: u8 = 0;
const DRAINING: u8 = 1;
const STOPPED: u8 = 2;

/// Lifecycle phase of the Task Queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownPhase {
    /// Normal operation; spawns accepted.
    Running,
    /// Drain in progress; new spawns rejected.
    Draining,
    /// Fully stopped; all queues drained and slots freed.
    Stopped,
}

impl ShutdownPhase {
    #[inline]
    fn from_u8(v: u8) -> Self {
        match v {
            RUNNING => Self::Running,
            DRAINING => Self::Draining,
            STOPPED => Self::Stopped,
            _ => Self::Stopped,
        }
    }
}

/// Atomic-flag shutdown coordinator.
pub struct ShutdownManager {
    phase: AtomicU8,
}

impl ShutdownManager {
    /// Creates a manager in the `Running` phase.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phase: AtomicU8::new(RUNNING),
        }
    }

    /// Returns the current phase.
    #[must_use]
    pub fn phase(&self) -> ShutdownPhase {
        ShutdownPhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    /// Returns `true` once `begin_drain` has been called.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.phase() != ShutdownPhase::Running
    }

    /// Returns `true` once `mark_stopped` has been called.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.phase() == ShutdownPhase::Stopped
    }

    /// Transitions `Running → Draining`. No-op if already past this point.
    pub fn begin_drain(&self) {
        let _ = self
            .phase
            .compare_exchange(RUNNING, DRAINING, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Transitions `Draining → Stopped`.
    pub fn mark_stopped(&self) {
        self.phase.store(STOPPED, Ordering::Release);
    }

    /// Resets to `Running` (testing convenience).
    #[cfg(test)]
    pub fn reset(&self) {
        self.phase.store(RUNNING, Ordering::Release);
    }
}

impl Default for ShutdownManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ShutdownManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShutdownManager")
            .field("phase", &self.phase())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_running_to_stopped() {
        let m = ShutdownManager::new();
        assert_eq!(m.phase(), ShutdownPhase::Running);
        assert!(!m.is_shutting_down());

        m.begin_drain();
        assert_eq!(m.phase(), ShutdownPhase::Draining);
        assert!(m.is_shutting_down());

        m.mark_stopped();
        assert_eq!(m.phase(), ShutdownPhase::Stopped);
        assert!(m.is_stopped());
    }

    #[test]
    fn begin_drain_is_idempotent() {
        let m = ShutdownManager::new();
        m.begin_drain();
        m.begin_drain(); // no-op
        assert_eq!(m.phase(), ShutdownPhase::Draining);
    }
}
