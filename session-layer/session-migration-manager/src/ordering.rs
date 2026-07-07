//! [`OrderingPreserver`] — maintains stream ordering across migration.
//!
//! See `DESIGN.md` Part 6 (ordering preservation).
//!
//! # Role
//!
//! During migration, messages may be in-flight in both directions.
//! `OrderingPreserver` ensures that:
//!
//! 1. Messages sent before the snapshot are not replayed.
//! 2. Messages sent after the snapshot arrive in order.
//! 3. A **fence token** prevents the old owner from accepting new
//!    writes after cutover.
//!
//! # Phase 0
//!
//! Skeleton only.

use crate::error::MigrationError;
use session_manager::SessionId;

// ── OrderingPreserver (Phase 1 skeleton) ────────────────────────

/// Maintains stream ordering during migration.
///
/// Phase 0 ships only a typed skeleton.
// TODO(phase-1): real ordering with sequence numbers + replay buffer.
#[derive(Debug, Default)]
pub struct OrderingPreserver {
    /// Capacity of the replay buffer (number of messages).
    #[allow(dead_code)]
    replay_buffer_capacity: usize,
}

impl OrderingPreserver {
    /// Construct a new ordering preserver.
    #[must_use]
    pub const fn new(replay_buffer_capacity: usize) -> Self {
        Self {
            replay_buffer_capacity,
        }
    }

    /// Take a checkpoint of the current sequence state.
    ///
    /// # Errors
    /// Always returns [`MigrationError::OrderingViolation`] in Phase 0.
    pub fn checkpoint(&self, _id: SessionId) -> Result<u64, MigrationError> {
        // TODO(phase-1): record last-applied sequence per stream.
        Err(MigrationError::OrderingViolation("phase-1 stub"))
    }

    /// Replay buffered messages from the given sequence number.
    ///
    /// # Errors
    /// Always returns [`MigrationError::OrderingViolation`] in Phase 0.
    pub fn replay(&self, _id: SessionId, _from_seq: u64) -> Result<u64, MigrationError> {
        // TODO(phase-1): drain replay buffer from checkpoint.
        Err(MigrationError::OrderingViolation("phase-1 stub"))
    }

    /// Verify that the ordering is consistent after migration.
    ///
    /// # Errors
    /// Always returns [`MigrationError::OrderingViolation`] in Phase 0.
    pub fn verify_order(&self, _id: SessionId) -> Result<(), MigrationError> {
        // TODO(phase-1): check no gaps in sequence.
        Err(MigrationError::OrderingViolation("phase-1 stub"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_ordering_preserver() {
        let op = OrderingPreserver::new(1_024);
        let _ = format!("{op:?}");
    }

    #[test]
    fn checkpoint_returns_stub_error() {
        let op = OrderingPreserver::new(1_024);
        assert!(matches!(
            op.checkpoint(SessionId::nil()),
            Err(MigrationError::OrderingViolation(_))
        ));
    }

    #[test]
    fn replay_returns_stub_error() {
        let op = OrderingPreserver::new(1_024);
        assert!(matches!(
            op.replay(SessionId::nil(), 0),
            Err(MigrationError::OrderingViolation(_))
        ));
    }

    #[test]
    fn verify_order_returns_stub_error() {
        let op = OrderingPreserver::new(1_024);
        assert!(matches!(
            op.verify_order(SessionId::nil()),
            Err(MigrationError::OrderingViolation(_))
        ));
    }
}
