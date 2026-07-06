//! [`StreamManager`] — facade that owns the stream table, ID
//! allocator, slab, stats, and health for a single connection.
//!
//! See `DESIGN.md` Part 1 (responsibilities) and Part 9 (lifecycle).
//!
//! # Role
//!
//! One `StreamManager` exists per connection (per peer). It is the
//! single application-facing handle for "everything streams". The
//! session layer holds the manager; the manager internally
//! coordinates:
//!
//! - [`StreamTable`](crate::table::StreamTable) — slab + index of
//!   live [`Stream`](crate::stream::Stream)s.
//! - [`StreamIdAllocator`](crate::allocator::StreamIdAllocator) —
//!   hands out locally-initiated ids.
//! - [`StreamStats`](crate::stats::StreamStats) — atomic counters.
//! - [`StreamFlowControl`](crate::flow_control::StreamFlowControl)
//!   — per-stream credit windows (Phase 2).
//! - [`StreamLifecycle`](crate::lifecycle::StreamLifecycle) — FSM
//!   transition controller (Phase 3).
//!
//! Wire-level frames (STREAM, RESET_STREAM, MAX_STREAM_DATA,
//! STOP_SENDING) arrive from the transport layer; the manager
//! translates them into high-level [`StreamEvent`]s for the
//! application.
//!
//! # Phase 0 status
//!
//! Phase 0 ships only the facade shape. Functional methods land in
//! Phase 1.

use crate::allocator::StreamIdAllocator;
use crate::config::StreamManagerConfig;
use crate::error::StreamManagerError;
use crate::stats::StreamStats;
use crate::table::StreamTable;

/// Per-connection stream manager.
///
/// Phase 0 holds the component parts so other modules can reference
/// them; Phase 1 adds the functional surface (`create_stream`,
/// `recv_frame`, `poll_event`, …).
// TODO(phase-1): functional methods.
#[allow(dead_code)]
pub struct StreamManager {
    cfg: StreamManagerConfig,
    table: StreamTable,
    allocator: StreamIdAllocator,
    stats: StreamStats,
}

impl StreamManager {
    /// Construct a manager from a validated config.
    ///
    /// Phase 0 assembles the parts; Phase 1 will also validate the
    /// config and start the lifecycle controller.
    ///
    /// # Errors
    /// Returns [`StreamManagerError::InvalidConfig`] if validation
    /// fails.
    pub fn new(cfg: StreamManagerConfig) -> Result<Self, StreamManagerError> {
        cfg.validate()?;
        Ok(Self {
            table: StreamTable::new(cfg),
            allocator: StreamIdAllocator::new(true),
            stats: StreamStats::new(),
            cfg,
        })
    }

    /// Borrow the config.
    #[must_use]
    pub const fn config(&self) -> &StreamManagerConfig {
        &self.cfg
    }

    /// Borrow the live stats counters.
    #[must_use]
    pub const fn stats(&self) -> &StreamStats {
        &self.stats
    }

    /// Current occupancy of the stream table.
    #[must_use]
    pub const fn active_streams(&self) -> u32 {
        self.table.occupancy()
    }

    /// Phase-1 `create_stream` placeholder.
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn create_stream(
        &mut self,
        _bidi: bool,
    ) -> Result<crate::id::StreamId, StreamManagerError> {
        // TODO(phase-1): allocate id, reserve slot, install Stream,
        // bump stats, emit Created event.
        Err(StreamManagerError::ResourceExhausted(
            "StreamManager::create_stream phase-1 stub",
        ))
    }

    /// Phase-1 `close_stream` placeholder.
    ///
    /// # Errors
    /// Always returns [`StreamManagerError::ResourceExhausted`] in
    /// Phase 0.
    pub fn close_stream(&mut self, _id: crate::id::StreamId) -> Result<(), StreamManagerError> {
        // TODO(phase-1): transition to Draining, emit Closed event.
        Err(StreamManagerError::ResourceExhausted(
            "StreamManager::close_stream phase-1 stub",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_validates_config() {
        let m = StreamManager::new(StreamManagerConfig::default()).expect("default ok");
        assert_eq!(m.active_streams(), 0);
        assert!(m.stats().snapshot().streams_active == 0);
    }

    #[test]
    fn new_rejects_invalid_config() {
        let bad = StreamManagerConfig {
            max_streams_bidi: 0,
            ..StreamManagerConfig::default()
        };
        assert!(StreamManager::new(bad).is_err());
    }

    #[test]
    fn create_stream_phase0_stub() {
        let mut m = StreamManager::new(StreamManagerConfig::default()).expect("ok");
        assert!(m.create_stream(true).is_err());
    }
}
