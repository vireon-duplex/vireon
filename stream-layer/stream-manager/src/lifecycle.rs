//! [`StreamLifecycle`] — create → activate → idle → drain → destroy.
//!
//! See `DESIGN.md` Part 9 (lifecycle) and Part 5 (FSM transitions).
//!
//! # Lifecycle states (logical)
//!
//! ```text
//!                            ┌──── peer FIN/RESET ack'd ────┐
//!   create ─► activate ─► idle ◄──► active                  │
//!                │              │                           ▼
//!                │              └── idle timeout ──► drain ──► destroy
//!                └── local close ─────────────────► drain
//! ```
//!
//! The lifecycle wraps the per-half FSMs ([`crate::state`]) and
//! orchestrates side-effects (stats bumps, slot release,
//! `STREAM_DATA_BLOCKED` flushes). The FSMs themselves are the
//! source of truth for send/recv state; this module sequences them.
//!
//! # Phase 0 status
//!
//! Phase 0 ships only the high-level state enum and a placeholder
//! controller. Functional transitions land in Phase 3.

use crate::error::StreamManagerError;
use crate::id::StreamId;

/// High-level lifecycle state of a stream (orthogonal to the
/// per-half [`StreamState`](crate::state::StreamState)).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum LifecycleState {
    /// Slot reserved but the application has not yet opened the
    /// stream.
    #[default]
    Created,
    /// Stream is open and the application can read/write.
    Active,
    /// No activity for some time; pending idle-timeout check.
    Idle,
    /// One or both halves are closing; outstanding data is being
    /// flushed / drained.
    Draining,
    /// Slot can be reclaimed by the slab.
    Destroyed,
}

impl LifecycleState {
    /// Whether the lifecycle is past the point of no return.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Destroyed)
    }

    /// Whether the stream can still accept new writes.
    #[must_use]
    pub const fn can_write(self) -> bool {
        matches!(self, Self::Created | Self::Active | Self::Idle)
    }
}

/// Lifecycle controller. Phase 0 placeholder.
///
/// Phase 3 will own the FSM transition rules (RFC 9000 §3.3), the
/// idle-timeout wheel, the drain queue, and the slot reclamation.
// TODO(phase-3): full transition machinery.
#[derive(Clone, Debug, Default)]
pub struct StreamLifecycle;

impl StreamLifecycle {
    /// Construct a new lifecycle controller.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Phase-3 `create` placeholder.
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn create(&mut self, _id: StreamId) -> Result<LifecycleState, StreamManagerError> {
        // TODO(phase-3): reserve slot, insert into table, bump stats.
        Err(StreamManagerError::ResourceExhausted(
            "lifecycle phase-3 stub",
        ))
    }

    /// Phase-3 `close` placeholder (initiate drain).
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn close(&mut self, _id: StreamId) -> Result<LifecycleState, StreamManagerError> {
        // TODO(phase-3): transition to Draining, flush, await ack.
        Err(StreamManagerError::ResourceExhausted(
            "lifecycle phase-3 stub",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_can_write() {
        assert!(LifecycleState::Created.can_write());
        assert!(LifecycleState::Active.can_write());
        assert!(LifecycleState::Idle.can_write());
        assert!(!LifecycleState::Draining.can_write());
        assert!(!LifecycleState::Destroyed.can_write());
    }

    #[test]
    fn terminal_state() {
        assert!(LifecycleState::Destroyed.is_terminal());
        assert!(!LifecycleState::Active.is_terminal());
    }

    #[test]
    fn default_is_created() {
        assert_eq!(LifecycleState::default(), LifecycleState::Created);
    }
}
