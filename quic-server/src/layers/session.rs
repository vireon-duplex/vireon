//! Session layer — session lifecycle management.
//!
//! Owns the `StreamLayer`, the `SessionTable`, and the distributed
//! session infrastructure (registry, router, migration, replication).
//! Session creation and destruction delegate to the stream layer for
//! per-connection stream initialization and cleanup, and to the
//! registry for ownership tracking.

use frame::codec::FrameRef;
use session_manager::table::SessionTable;
use session_manager::{SessionId, SessionManagerConfig};
use session_migration_manager::coordinator::MigrationCoordinator;
use session_registry::NodeId;
use session_registry::ownership::OwnershipTable;
use session_replication_service::config::ReplicationConfig;
use session_router::decision::RoutingDecision;

use super::message::FrameAction;
use super::stream::StreamLayer;
use crate::error::ServerError;

/// The local node ID for single-node / multi-worker mode.
/// In distributed mode this would be assigned by the cluster manager.
const LOCAL_NODE_ID: u32 = 1;

/// Session layer — manages session slots, distributed registry,
/// routing, migration, and replication. Delegates frame processing
/// to the stream layer.
pub struct SessionLayer {
    /// Stream layer (owned — sits below session in the stack).
    stream: StreamLayer,
    /// Session slab — allocates/tracks session slots.
    table: SessionTable,
    /// Distributed session ownership table (session-registry).
    #[allow(dead_code)]
    ownership: OwnershipTable,
    /// Local node ID for routing decisions.
    local_node: NodeId,
    /// Session migration coordinator.
    #[allow(dead_code)]
    migration: MigrationCoordinator,
    /// Replication configuration (strategy + consistency level).
    #[allow(dead_code)]
    replication_config: ReplicationConfig,
}

impl SessionLayer {
    /// Creates a new session layer with default configuration and
    /// distributed session infrastructure.
    pub fn new() -> Result<Self, ServerError> {
        let replication_config =
            session_replication_service::builder::ReplicationServiceBuilder::new()
                .build()
                .map_err(|e| ServerError::EngineInit(format!("replication: {e:?}")))?;

        Ok(Self {
            stream: StreamLayer::new()?,
            table: SessionTable::new(SessionManagerConfig::default()),
            ownership: OwnershipTable::new(),
            local_node: NodeId::new(LOCAL_NODE_ID),
            migration: MigrationCoordinator::new(),
            replication_config,
        })
    }

    /// Creates a new session: reserves a slot, registers ownership,
    /// and initializes stream state for the connection.
    ///
    /// Returns the application-level `SessionId`. If the session table
    /// is full, returns `None`.
    pub fn on_connect(&mut self, conn_idx: usize) -> Option<SessionId> {
        match self.table.try_reserve_slot() {
            Ok(generation) => {
                tracing::debug!(conn_idx, session_gen = generation, "session slot reserved");
            }
            Err(e) => {
                tracing::warn!(conn_idx, error = ?e, "session table full");
                return None;
            }
        }

        // Delegate to stream layer for per-connection stream init.
        self.stream.on_connect(conn_idx);

        let session_id = SessionId::new();

        // [session-registry] Register ownership (Phase 0 stub — logs only).
        // In Phase 1+ this will write to the OwnershipTable.
        tracing::debug!(
            conn_idx,
            session_id = ?session_id,
            node_id = self.local_node.raw(),
            "[session-registry] ownership registered"
        );

        Some(session_id)
    }

    /// Delegates frame processing to the stream layer and resolves
    /// routing for the session.
    ///
    /// Returns the `FrameAction` from the stream layer so the
    /// application layer can decide whether to dispatch.
    pub fn on_frame(&mut self, conn_idx: usize, frame: &FrameRef<'_>) -> FrameAction {
        // [session-router] Resolve routing decision.
        // In single-node mode all sessions are local.
        let decision = RoutingDecision::Local;
        if !decision.is_local() {
            tracing::debug!(
                conn_idx,
                decision = ?decision,
                "[session-router] non-local routing (forward required)"
            );
        }

        self.stream.on_frame(conn_idx, frame)
    }

    /// Destroys a session: releases streams, releases the session slot,
    /// and unregisters ownership.
    ///
    /// Returns the number of streams released.
    pub fn on_close(&mut self, conn_idx: usize) -> usize {
        let released = self.stream.on_close(conn_idx);
        self.table.release_slot();

        // [session-registry] Unregister ownership (Phase 0 stub).
        // In Phase 1+ this will remove from the OwnershipTable.
        tracing::debug!(
            conn_idx,
            ownership_entries = self.ownership.len(),
            "[session-registry] ownership released"
        );

        released
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the session table occupancy.
    #[must_use]
    pub fn occupancy(&self) -> usize {
        self.table.occupancy() as usize
    }

    /// Returns a reference to the owned stream layer (for introspection).
    #[must_use]
    #[allow(dead_code)]
    pub const fn stream(&self) -> &StreamLayer {
        &self.stream
    }
}
