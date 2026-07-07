//! [`MigrationPhase`] — the lifecycle state machine for a migration.
//!
//! See `DESIGN.md` Part 2 (migration lifecycle).
//!
//! # State Machine
//!
//! ```text
//!     ┌────────────┐
//!     │ Initiated  │
//!     └─────┬──────┘
//!           │ capture snapshot
//!           ▼
//!     ┌────────────┐
//!     │ Snapshot   │
//!     │ Captured   │
//!     └─────┬──────┘
//!           │ transfer state
//!           ▼
//!     ┌────────────┐
//!     │ Transferred│
//!     └─────┬──────┘
//!           │ restore on target
//!           ▼
//!     ┌────────────┐
//!     │  Restored  │
//!     └─────┬──────┘
//!           │ finalize
//!           ▼
//!     ┌────────────┐
//!     │ Finalized  │ ◄── success terminal
//!     └────────────┘
//!
//!  At any pre-terminal phase:
//!           │
//!           ├──► Aborted  (operator or policy)
//!           └──► Failed   (error)
//! ```

// ── MigrationPhase ──────────────────────────────────────────────

/// The lifecycle phase of a session migration.
///
/// Each migration progresses through a strict state machine:
/// `Initiated → SnapshotCaptured → Transferred → Restored → Finalized`.
/// At any point before terminal success, the migration may transition
/// to `Aborted` (intentional cancel) or `Failed` (error).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MigrationPhase {
    /// Migration has been requested but no snapshot taken yet.
    #[default]
    Initiated,
    /// Session snapshot captured on the source node.
    SnapshotCaptured,
    /// State transferred from source to target.
    Transferred,
    /// State restored on the target node; verification pending.
    Restored,
    /// Migration completed successfully; ownership transferred.
    Finalized,
    /// Migration was cancelled by an operator or policy.
    Aborted,
    /// Migration failed and rollback was attempted.
    Failed,
}

impl MigrationPhase {
    /// Whether the phase is in-progress (not terminal).
    #[must_use]
    pub const fn is_in_progress(self) -> bool {
        matches!(
            self,
            Self::Initiated | Self::SnapshotCaptured | Self::Transferred | Self::Restored
        )
    }

    /// Whether the phase is terminal (no further transitions).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finalized | Self::Aborted | Self::Failed)
    }

    /// Whether the phase indicates a successful migration.
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Finalized)
    }

    /// Whether a transition from `self` to `target` is legal.
    ///
    /// Valid forward transitions:
    /// - `Initiated` → `SnapshotCaptured`, `Aborted`, `Failed`
    /// - `SnapshotCaptured` → `Transferred`, `Aborted`, `Failed`
    /// - `Transferred` → `Restored`, `Aborted`, `Failed`
    /// - `Restored` → `Finalized`, `Aborted`, `Failed`
    /// - Terminal states have no legal transitions.
    #[must_use]
    #[allow(clippy::match_like_matches_macro)]
    pub const fn can_transition_to(self, target: Self) -> bool {
        use MigrationPhase as P;
        match (self, target) {
            (P::Initiated, P::SnapshotCaptured | P::Aborted | P::Failed) => true,
            (P::SnapshotCaptured, P::Transferred | P::Aborted | P::Failed) => true,
            (P::Transferred, P::Restored | P::Aborted | P::Failed) => true,
            (P::Restored, P::Finalized | P::Aborted | P::Failed) => true,
            _ => false,
        }
    }

    /// Human-readable name for metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Initiated => "initiated",
            Self::SnapshotCaptured => "snapshot_captured",
            Self::Transferred => "transferred",
            Self::Restored => "restored",
            Self::Finalized => "finalized",
            Self::Aborted => "aborted",
            Self::Failed => "failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_initiated() {
        assert_eq!(MigrationPhase::default(), MigrationPhase::Initiated);
    }

    #[test]
    fn in_progress_phases() {
        assert!(MigrationPhase::Initiated.is_in_progress());
        assert!(MigrationPhase::SnapshotCaptured.is_in_progress());
        assert!(MigrationPhase::Transferred.is_in_progress());
        assert!(MigrationPhase::Restored.is_in_progress());
        assert!(!MigrationPhase::Finalized.is_in_progress());
        assert!(!MigrationPhase::Aborted.is_in_progress());
        assert!(!MigrationPhase::Failed.is_in_progress());
    }

    #[test]
    fn terminal_phases() {
        assert!(MigrationPhase::Finalized.is_terminal());
        assert!(MigrationPhase::Aborted.is_terminal());
        assert!(MigrationPhase::Failed.is_terminal());
        assert!(!MigrationPhase::Initiated.is_terminal());
        assert!(!MigrationPhase::Restored.is_terminal());
    }

    #[test]
    fn success_phase() {
        assert!(MigrationPhase::Finalized.is_success());
        assert!(!MigrationPhase::Aborted.is_success());
        assert!(!MigrationPhase::Failed.is_success());
        assert!(!MigrationPhase::Initiated.is_success());
    }

    #[test]
    fn forward_transitions_are_legal() {
        use MigrationPhase as P;
        assert!(P::Initiated.can_transition_to(P::SnapshotCaptured));
        assert!(P::SnapshotCaptured.can_transition_to(P::Transferred));
        assert!(P::Transferred.can_transition_to(P::Restored));
        assert!(P::Restored.can_transition_to(P::Finalized));
    }

    #[test]
    fn abort_from_any_in_progress_phase() {
        use MigrationPhase as P;
        assert!(P::Initiated.can_transition_to(P::Aborted));
        assert!(P::SnapshotCaptured.can_transition_to(P::Aborted));
        assert!(P::Transferred.can_transition_to(P::Aborted));
        assert!(P::Restored.can_transition_to(P::Aborted));
    }

    #[test]
    fn fail_from_any_in_progress_phase() {
        use MigrationPhase as P;
        assert!(P::Initiated.can_transition_to(P::Failed));
        assert!(P::SnapshotCaptured.can_transition_to(P::Failed));
        assert!(P::Transferred.can_transition_to(P::Failed));
        assert!(P::Restored.can_transition_to(P::Failed));
    }

    #[test]
    fn backward_transitions_are_illegal() {
        use MigrationPhase as P;
        assert!(!P::SnapshotCaptured.can_transition_to(P::Initiated));
        assert!(!P::Transferred.can_transition_to(P::SnapshotCaptured));
        assert!(!P::Restored.can_transition_to(P::Transferred));
        assert!(!P::Finalized.can_transition_to(P::Restored));
    }

    #[test]
    fn terminal_phases_have_no_transitions() {
        use MigrationPhase as P;
        assert!(!P::Finalized.can_transition_to(P::Aborted));
        assert!(!P::Aborted.can_transition_to(P::Failed));
        assert!(!P::Failed.can_transition_to(P::Aborted));
    }

    #[test]
    fn as_str_labels() {
        assert_eq!(MigrationPhase::Initiated.as_str(), "initiated");
        assert_eq!(
            MigrationPhase::SnapshotCaptured.as_str(),
            "snapshot_captured"
        );
        assert_eq!(MigrationPhase::Transferred.as_str(), "transferred");
        assert_eq!(MigrationPhase::Restored.as_str(), "restored");
        assert_eq!(MigrationPhase::Finalized.as_str(), "finalized");
        assert_eq!(MigrationPhase::Aborted.as_str(), "aborted");
        assert_eq!(MigrationPhase::Failed.as_str(), "failed");
    }
}
