//! [`MigrationRecord`], [`MigrationCoordinator`] — the per-session
//! migration record and the coordinating facade.
//!
//! See `DESIGN.md` Part 1 (architecture) and Part 2 (lifecycle).
//!
//! # Role
//!
//! The `MigrationCoordinator` is the top-level entry point for
//! initiating and driving session migrations. It tracks one
//! [`MigrationRecord`] per active migration, enforces concurrency
//! limits, and transitions phases in the correct order.
//!
//! # Phase 0
//!
//! `MigrationRecord` is a real implementation. `MigrationCoordinator`
//! is a Phase 1 skeleton — every method returns a stub error so
//! callers can code against the API contract today.

use crate::error::MigrationError;
use crate::phase::MigrationPhase;
use crate::plan::MigrationPlan;
use crate::snapshot::MigrationSnapshotRef;
use session_manager::SessionId;

// ── MigrationRecord ─────────────────────────────────────────────

/// Mutable per-session migration tracking record.
///
/// One of these exists for each active or recently-completed
/// migration. The coordinator updates `phase` and `snapshot` as the
/// migration progresses.
#[derive(Clone, Debug)]
pub struct MigrationRecord {
    /// The immutable plan for this migration.
    pub plan: MigrationPlan,
    /// Current lifecycle phase.
    pub phase: MigrationPhase,
    /// Snapshot reference, once captured.
    pub snapshot: Option<MigrationSnapshotRef>,
    /// Number of retry attempts so far.
    pub retries: u8,
    /// Nanoseconds since cluster epoch of the last phase change.
    pub last_phase_change_ns: u64,
}

impl MigrationRecord {
    /// Construct a new record in the `Initiated` phase.
    #[must_use]
    pub const fn new(plan: MigrationPlan, now_ns: u64) -> Self {
        Self {
            plan,
            phase: MigrationPhase::Initiated,
            snapshot: None,
            retries: 0,
            last_phase_change_ns: now_ns,
        }
    }

    /// Whether the migration is still in-progress.
    #[must_use]
    pub const fn is_in_progress(&self) -> bool {
        self.phase.is_in_progress()
    }

    /// Whether the migration has reached a terminal state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.phase.is_terminal()
    }
}

impl PartialEq for MigrationRecord {
    fn eq(&self, other: &Self) -> bool {
        self.plan == other.plan
            && self.phase == other.phase
            && self.snapshot == other.snapshot
            && self.retries == other.retries
            && self.last_phase_change_ns == other.last_phase_change_ns
    }
}

impl Eq for MigrationRecord {}

// ── MigrationCoordinator (Phase 1 skeleton) ─────────────────────

/// The top-level coordinator for session migrations.
///
/// Phase 0 ships a typed skeleton. Every method returns a stub error
/// so callers can code against the API contract today. Phase 1
/// populates real logic with a concurrent migration table.
// TODO(phase-1): real coordinator with DashMap + concurrency limits.
#[derive(Debug, Default)]
pub struct MigrationCoordinator {
    /// Number of migrations tracked (Phase 0 state).
    #[allow(dead_code)]
    count: usize,
}

impl MigrationCoordinator {
    /// Construct a new coordinator.
    #[must_use]
    pub const fn new() -> Self {
        Self { count: 0 }
    }

    /// Initiate a new migration.
    ///
    /// # Errors
    /// Always returns [`MigrationError::ShuttingDown`] in Phase 0.
    pub fn initiate(&mut self, _plan: MigrationPlan) -> Result<MigrationRecord, MigrationError> {
        // TODO(phase-1): insert plan into migration table.
        Err(MigrationError::ShuttingDown)
    }

    /// Capture a snapshot of the session on the source node.
    ///
    /// # Errors
    /// Always returns [`MigrationError::MigrationNotFound`] in Phase 0.
    pub fn capture_snapshot(
        &mut self,
        id: SessionId,
    ) -> Result<MigrationSnapshotRef, MigrationError> {
        // TODO(phase-1): invoke source snapshot API.
        Err(MigrationError::MigrationNotFound(id))
    }

    /// Transfer the snapshot from source to target.
    ///
    /// # Errors
    /// Always returns [`MigrationError::MigrationNotFound`] in Phase 0.
    pub fn transfer(&mut self, id: SessionId) -> Result<(), MigrationError> {
        // TODO(phase-1): invoke state transfer protocol.
        Err(MigrationError::MigrationNotFound(id))
    }

    /// Restore the snapshot on the target node.
    ///
    /// # Errors
    /// Always returns [`MigrationError::MigrationNotFound`] in Phase 0.
    pub fn restore(&mut self, id: SessionId) -> Result<(), MigrationError> {
        // TODO(phase-1): invoke target restore API.
        Err(MigrationError::MigrationNotFound(id))
    }

    /// Finalize the migration: bump generation, grant new lease.
    ///
    /// # Errors
    /// Always returns [`MigrationError::MigrationNotFound`] in Phase 0.
    pub fn finalize(&mut self, id: SessionId) -> Result<(), MigrationError> {
        // TODO(phase-1): ownership transfer + lease grant.
        Err(MigrationError::MigrationNotFound(id))
    }

    /// Abort an in-progress migration.
    ///
    /// # Errors
    /// Always returns [`MigrationError::MigrationNotFound`] in Phase 0.
    pub fn abort(&mut self, id: SessionId) -> Result<(), MigrationError> {
        // TODO(phase-1): rollback + cleanup.
        Err(MigrationError::MigrationNotFound(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::MigrationFlags;
    use session_registry::{Generation, NodeId};

    fn sample_plan() -> MigrationPlan {
        MigrationPlan::new(
            SessionId::from_pair(1, 1),
            NodeId::new(1),
            NodeId::new(2),
            10_000_000_000,
            0,
            MigrationFlags::default(),
            Generation::INITIAL,
        )
    }

    #[test]
    fn record_starts_in_initiated() {
        let plan = sample_plan();
        let rec = MigrationRecord::new(plan, 0);
        assert_eq!(rec.phase, MigrationPhase::Initiated);
        assert!(rec.is_in_progress());
        assert!(!rec.is_terminal());
        assert!(rec.snapshot.is_none());
        assert_eq!(rec.retries, 0);
    }

    #[test]
    fn record_terminal_when_phase_terminal() {
        let mut rec = MigrationRecord::new(sample_plan(), 0);
        rec.phase = MigrationPhase::Finalized;
        assert!(rec.is_terminal());
        assert!(!rec.is_in_progress());

        rec.phase = MigrationPhase::Failed;
        assert!(rec.is_terminal());

        rec.phase = MigrationPhase::Aborted;
        assert!(rec.is_terminal());
    }

    #[test]
    fn coordinator_default_is_new() {
        let c = MigrationCoordinator::default();
        let _ = format!("{c:?}");
    }

    #[test]
    fn initiate_phase0_stub() {
        let mut c = MigrationCoordinator::new();
        assert!(c.initiate(sample_plan()).is_err());
    }

    #[test]
    fn capture_snapshot_phase0_stub() {
        let mut c = MigrationCoordinator::new();
        assert!(c.capture_snapshot(SessionId::nil()).is_err());
    }

    #[test]
    fn all_phase_methods_return_migration_not_found() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);
        assert!(matches!(
            c.transfer(id),
            Err(MigrationError::MigrationNotFound(_))
        ));
        assert!(matches!(
            c.restore(id),
            Err(MigrationError::MigrationNotFound(_))
        ));
        assert!(matches!(
            c.finalize(id),
            Err(MigrationError::MigrationNotFound(_))
        ));
        assert!(matches!(
            c.abort(id),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }
}
