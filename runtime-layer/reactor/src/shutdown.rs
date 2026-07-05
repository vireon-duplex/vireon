//! [`ShutdownCoordinator`] — phase-coordinated graceful drain.
//!
//! See `DESIGN.md` §14 (failure handling) and the state-machine
//! table in §18.
//!
//! # Phases
//!
//! ```text
//! Running ──begin_drain()──► Draining ──drain_complete()──► Stopped
//!     │                                                        ▲
//!     └──────────────────force()──────────────────────────────┘
//! ```
//!
//! - **Running** — the reactor is ticking normally.
//! - **Draining** — no new work is accepted; in-flight sends and
//!   tasks run to completion; timers fire their final callbacks.
//! - **Stopped** — the reactor thread is about to exit; all
//!   resources are released.

use std::sync::atomic::{AtomicU8, Ordering};

/// Shutdown state machine. Encoded as a `u8` so it can live in an
/// atomic for cross-thread visibility (other threads observe the
/// phase change via [`ShutdownCoordinator::phase`]).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum ShutdownPhase {
    /// The reactor is ticking normally.
    #[default]
    Running = 0,
    /// The reactor is draining in-flight work.
    Draining = 1,
    /// The reactor has fully stopped.
    Stopped = 2,
}

impl ShutdownPhase {
    /// Encode to the wire representation.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Decode from the wire representation.
    ///
    /// # Panics
    /// Panics if `v` is not a valid phase index. The caller must
    /// ensure `v` came from [`Self::as_u8`].
    #[must_use]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Running,
            1 => Self::Draining,
            _ => Self::Stopped,
        }
    }
}

/// Coordinates reactor shutdown. Lock-free: phase transitions are
/// atomic `u8` CAS operations.
// TODO(phase-5): wire to real reactor drain logic.
pub struct ShutdownCoordinator {
    phase: AtomicU8,
}

impl ShutdownCoordinator {
    /// Construct a coordinator in the `Running` phase.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phase: AtomicU8::new(ShutdownPhase::Running.as_u8()),
        }
    }

    /// Current phase. Observed atomically by any thread.
    #[must_use]
    pub fn phase(&self) -> ShutdownPhase {
        ShutdownPhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    /// `true` once shutdown has begun (`Draining` or `Stopped`).
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.phase() != ShutdownPhase::Running
    }

    /// `true` only once the reactor has fully stopped.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.phase() == ShutdownPhase::Stopped
    }

    // TODO(phase-5):
    //   pub fn begin_drain(&self) -> bool
    //   pub fn drain_complete(&self) -> bool
    //   pub fn force(&self) -> ShutdownPhase
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let c = ShutdownCoordinator::new();
        assert_eq!(c.phase(), ShutdownPhase::Running);
        assert!(!c.is_shutdown());
        assert!(!c.is_stopped());
        // TODO(phase-5): real drain logic + force path.
    }

    #[test]
    fn phase_roundtrip() {
        for p in [
            ShutdownPhase::Running,
            ShutdownPhase::Draining,
            ShutdownPhase::Stopped,
        ] {
            assert_eq!(ShutdownPhase::from_u8(p.as_u8()), p);
        }
    }
}
