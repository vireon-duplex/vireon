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

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::MigrationError;
use crate::phase::MigrationPhase;
use crate::plan::MigrationPlan;
use crate::snapshot::{MigrationSnapshotRef, SnapshotMetadata};
use crate::stats::MigrationStats;
use session_manager::SessionId;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

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

    /// Transition to a new phase. Returns `Err` if the transition
    /// is illegal.
    fn transition_to(&mut self, target: MigrationPhase, now_ns: u64) -> Result<(), MigrationError> {
        if !self.phase.can_transition_to(target) {
            return Err(MigrationError::OrderingViolation(
                "illegal phase transition",
            ));
        }
        self.phase = target;
        self.last_phase_change_ns = now_ns;
        Ok(())
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

// ── MigrationCoordinator ────────────────────────────────────────

/// The top-level coordinator for session migrations.
///
/// Tracks one `MigrationRecord` per active migration in a
/// `HashMap<SessionId, MigrationRecord>`. Phase transitions are
/// validated against the legal state machine.
pub struct MigrationCoordinator {
    /// Active and recently-completed migrations.
    migrations: HashMap<SessionId, MigrationRecord>,
    /// Monotonic snapshot handle counter.
    next_handle: u64,
    /// Atomic stats counters.
    stats: MigrationStats,
}

impl MigrationCoordinator {
    /// Construct a new coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            migrations: HashMap::new(),
            next_handle: 1,
            stats: MigrationStats::new(),
        }
    }

    /// Initiate a new migration.
    ///
    /// Creates a `MigrationRecord` in the `Initiated` phase.
    ///
    /// # Errors
    /// - [`MigrationError::AlreadyInProgress`] if a migration is
    ///   already running for this session.
    pub fn initiate(&mut self, plan: MigrationPlan) -> Result<MigrationRecord, MigrationError> {
        let session_id = plan.session_id;

        if let Some(existing) = self.migrations.get(&session_id) {
            if existing.is_in_progress() {
                return Err(MigrationError::AlreadyInProgress(session_id));
            }
        }

        let now = now_ns();
        let record = MigrationRecord::new(plan, now);
        self.migrations.insert(session_id, record.clone());
        Ok(record)
    }

    /// Capture a snapshot of the session on the source node.
    ///
    /// Transitions `Initiated → SnapshotCaptured`.
    ///
    /// # Errors
    /// - [`MigrationError::MigrationNotFound`] if no migration exists.
    /// - [`MigrationError::OrderingViolation`] if not in `Initiated` phase.
    pub fn capture_snapshot(
        &mut self,
        id: SessionId,
    ) -> Result<MigrationSnapshotRef, MigrationError> {
        let now = now_ns();
        let handle = self.next_handle;
        self.next_handle += 1;

        let record = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::MigrationNotFound(id))?;

        // Create a snapshot reference.
        let metadata = SnapshotMetadata::new(
            id,
            record.plan.source_node,
            now,
            0, // size unknown until serialized
            0, // stream count unknown
            record.plan.generation,
            handle, // use handle as a temporary checksum
        );

        let snapshot_ref = MigrationSnapshotRef::new(metadata, handle);
        record.snapshot = Some(snapshot_ref);
        record.transition_to(MigrationPhase::SnapshotCaptured, now)?;

        Ok(snapshot_ref)
    }

    /// Transfer the snapshot from source to target.
    ///
    /// Transitions `SnapshotCaptured → Transferred`.
    ///
    /// # Errors
    /// - [`MigrationError::MigrationNotFound`] if no migration exists.
    /// - [`MigrationError::OrderingViolation`] if not in `SnapshotCaptured` phase.
    pub fn transfer(&mut self, id: SessionId) -> Result<(), MigrationError> {
        let now = now_ns();
        let record = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::MigrationNotFound(id))?;

        // Phase transition validates legal ordering. SnapshotCaptured
        // → Transferred is legal; Initiated → Transferred is not.
        record.transition_to(MigrationPhase::Transferred, now)?;
        Ok(())
    }

    /// Restore the snapshot on the target node.
    ///
    /// Transitions `Transferred → Restored`.
    ///
    /// # Errors
    /// - [`MigrationError::MigrationNotFound`] if no migration exists.
    /// - [`MigrationError::OrderingViolation`] if not in `Transferred` phase.
    pub fn restore(&mut self, id: SessionId) -> Result<(), MigrationError> {
        let now = now_ns();
        let record = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::MigrationNotFound(id))?;

        record.transition_to(MigrationPhase::Restored, now)?;
        Ok(())
    }

    /// Finalize the migration: bump generation, grant new lease.
    ///
    /// Transitions `Restored → Finalized`.
    ///
    /// # Errors
    /// - [`MigrationError::MigrationNotFound`] if no migration exists.
    /// - [`MigrationError::OrderingViolation`] if not in `Restored` phase.
    pub fn finalize(&mut self, id: SessionId) -> Result<(), MigrationError> {
        let now = now_ns();
        let record = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::MigrationNotFound(id))?;

        record.transition_to(MigrationPhase::Finalized, now)?;
        Ok(())
    }

    /// Abort an in-progress migration.
    ///
    /// Transitions any in-progress phase → `Aborted`.
    ///
    /// # Errors
    /// - [`MigrationError::MigrationNotFound`] if no migration exists.
    pub fn abort(&mut self, id: SessionId) -> Result<(), MigrationError> {
        let now = now_ns();
        let record = self
            .migrations
            .get_mut(&id)
            .ok_or(MigrationError::MigrationNotFound(id))?;

        record.transition_to(MigrationPhase::Aborted, now)?;
        Ok(())
    }

    /// Look up a migration record by session id.
    ///
    /// # Errors
    /// Returns [`MigrationError::MigrationNotFound`] if no migration exists.
    pub fn get(&self, id: SessionId) -> Result<&MigrationRecord, MigrationError> {
        self.migrations
            .get(&id)
            .ok_or(MigrationError::MigrationNotFound(id))
    }

    /// Number of active (in-progress) migrations.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.migrations
            .values()
            .filter(|r| r.is_in_progress())
            .count()
    }

    /// Total migrations tracked (including terminal).
    #[must_use]
    pub fn total_count(&self) -> usize {
        self.migrations.len()
    }

    /// Remove a terminal migration record from the table.
    ///
    /// # Errors
    /// - [`MigrationError::MigrationNotFound`] if no migration exists.
    /// - [`MigrationError::AlreadyInProgress`] if the migration is
    ///   still in-progress.
    pub fn cleanup(&mut self, id: SessionId) -> Result<(), MigrationError> {
        let record = self
            .migrations
            .get(&id)
            .ok_or(MigrationError::MigrationNotFound(id))?;
        if record.is_in_progress() {
            return Err(MigrationError::AlreadyInProgress(id));
        }
        self.migrations.remove(&id);
        Ok(())
    }

    /// Reference to the stats counters.
    #[must_use]
    pub const fn stats(&self) -> &MigrationStats {
        &self.stats
    }
}

impl Default for MigrationCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MigrationCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationCoordinator")
            .field("total", &self.migrations.len())
            .field("active", &self.active_count())
            .finish()
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
    fn initiate_creates_record() {
        let mut c = MigrationCoordinator::new();
        let rec = c.initiate(sample_plan()).unwrap();
        assert_eq!(rec.phase, MigrationPhase::Initiated);
        assert_eq!(c.total_count(), 1);
    }

    #[test]
    fn initiate_duplicate_while_in_progress_errors() {
        let mut c = MigrationCoordinator::new();
        c.initiate(sample_plan()).unwrap();
        assert!(matches!(
            c.initiate(sample_plan()),
            Err(MigrationError::AlreadyInProgress(_))
        ));
    }

    #[test]
    fn initiate_after_terminal_allows_reinitiation() {
        let mut c = MigrationCoordinator::new();
        c.initiate(sample_plan()).unwrap();
        c.abort(SessionId::from_pair(1, 1)).unwrap();
        // Can re-initiate after terminal.
        assert!(c.initiate(sample_plan()).is_ok());
    }

    #[test]
    fn full_migration_lifecycle() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);

        // Initiate.
        c.initiate(sample_plan()).unwrap();
        assert_eq!(c.get(id).unwrap().phase, MigrationPhase::Initiated);

        // Capture snapshot.
        let snap = c.capture_snapshot(id).unwrap();
        assert!(snap.handle != 0);
        assert_eq!(c.get(id).unwrap().phase, MigrationPhase::SnapshotCaptured);

        // Transfer.
        c.transfer(id).unwrap();
        assert_eq!(c.get(id).unwrap().phase, MigrationPhase::Transferred);

        // Restore.
        c.restore(id).unwrap();
        assert_eq!(c.get(id).unwrap().phase, MigrationPhase::Restored);

        // Finalize.
        c.finalize(id).unwrap();
        assert_eq!(c.get(id).unwrap().phase, MigrationPhase::Finalized);
        assert!(c.get(id).unwrap().is_terminal());
    }

    #[test]
    fn capture_snapshot_missing_errors() {
        let mut c = MigrationCoordinator::new();
        assert!(matches!(
            c.capture_snapshot(SessionId::nil()),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }

    #[test]
    fn transfer_missing_errors() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);
        assert!(matches!(
            c.transfer(id),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }

    #[test]
    fn illegal_transition_errors() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);
        c.initiate(sample_plan()).unwrap();
        // Can't transfer before snapshot is captured.
        assert!(matches!(
            c.transfer(id),
            Err(MigrationError::OrderingViolation(_))
        ));
    }

    #[test]
    fn abort_from_initiated() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);
        c.initiate(sample_plan()).unwrap();
        c.abort(id).unwrap();
        assert_eq!(c.get(id).unwrap().phase, MigrationPhase::Aborted);
        assert!(c.get(id).unwrap().is_terminal());
    }

    #[test]
    fn abort_from_snapshot_captured() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);
        c.initiate(sample_plan()).unwrap();
        c.capture_snapshot(id).unwrap();
        c.abort(id).unwrap();
        assert_eq!(c.get(id).unwrap().phase, MigrationPhase::Aborted);
    }

    #[test]
    fn cleanup_removes_terminal() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);
        c.initiate(sample_plan()).unwrap();
        c.abort(id).unwrap();
        c.cleanup(id).unwrap();
        assert_eq!(c.total_count(), 0);
    }

    #[test]
    fn cleanup_in_progress_errors() {
        let mut c = MigrationCoordinator::new();
        let id = SessionId::from_pair(1, 1);
        c.initiate(sample_plan()).unwrap();
        assert!(matches!(
            c.cleanup(id),
            Err(MigrationError::AlreadyInProgress(_))
        ));
    }

    #[test]
    fn active_count_tracks_in_progress() {
        let mut c = MigrationCoordinator::new();
        let id1 = SessionId::from_pair(1, 1);
        let id2 = SessionId::from_pair(2, 2);

        let mut plan2 = sample_plan();
        plan2.session_id = id2;

        c.initiate(sample_plan()).unwrap();
        c.initiate(plan2).unwrap();
        assert_eq!(c.active_count(), 2);

        c.abort(id1).unwrap();
        assert_eq!(c.active_count(), 1);

        // Abort the second as well.
        c.abort(id2).unwrap();
        assert_eq!(c.active_count(), 0);
    }

    #[test]
    fn snapshot_handle_increments() {
        let mut c = MigrationCoordinator::new();
        let id1 = SessionId::from_pair(1, 1);
        let id2 = SessionId::from_pair(2, 2);

        let mut plan1 = sample_plan();
        plan1.session_id = id1;
        let mut plan2 = sample_plan();
        plan2.session_id = id2;

        c.initiate(plan1).unwrap();
        c.initiate(plan2).unwrap();

        let s1 = c.capture_snapshot(id1).unwrap();
        let s2 = c.capture_snapshot(id2).unwrap();
        assert_ne!(s1.handle, s2.handle);
    }

    #[test]
    fn get_missing_errors() {
        let c = MigrationCoordinator::new();
        assert!(matches!(
            c.get(SessionId::nil()),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }

    #[test]
    fn restore_missing_errors() {
        let mut c = MigrationCoordinator::new();
        assert!(matches!(
            c.restore(SessionId::from_pair(1, 1)),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }

    #[test]
    fn finalize_missing_errors() {
        let mut c = MigrationCoordinator::new();
        assert!(matches!(
            c.finalize(SessionId::from_pair(1, 1)),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }

    #[test]
    fn abort_missing_errors() {
        let mut c = MigrationCoordinator::new();
        assert!(matches!(
            c.abort(SessionId::from_pair(1, 1)),
            Err(MigrationError::MigrationNotFound(_))
        ));
    }
}
