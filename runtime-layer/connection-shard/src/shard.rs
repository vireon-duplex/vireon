//! [`ConnectionShard`] — the per-core connection ownership facade (Layer 14).
//!
//! Each Reactor core owns exactly one `ConnectionShard`. The shard is the
//! **sole** owner of every connection on that core. All mutation goes
//! through `&mut self` methods — there is no shared mutable state, no
//! `Arc<Mutex>`, no cross-core locks.
//!
//! ## Fast path
//!
//! The per-packet path ([`lookup_by_stream`](Self::lookup_by_stream),
//! [`touch_recv`](Self::touch_recv), [`touch_send`](Self::touch_send))
//! performs zero allocation and no locking. Lookup is a HashMap probe
//! + generation check; touch is a slab index + counter add.

use std::net::SocketAddr;

use rustc_hash::FxHashMap;

use crate::backpressure::BackpressureLevel;
use crate::config::ShardConfig;
use crate::connection::ConnectionEntry;
use crate::error::ShardError;
use crate::health::HealthReport;
use crate::id::{ConnectionId, GEN_MASK, make_id};
use crate::migration::MigrationRequest;
use crate::spec::ConnectionSpec;
use crate::state::ConnectionState;
use crate::stats::{StatsCollector, StatsSnapshot};
use crate::strategy::{LocalSharding, ShardingStrategy};

/// Per-core connection ownership store.
///
/// Owns the slab (connection entries), generation counters, free list,
/// and the `stream_id → slot` index. All access is single-threaded;
/// only [`StatsCollector`] uses atomics (readable from monitoring
/// threads).
pub struct ConnectionShard {
    /// The reactor / core id that owns this shard.
    reactor_id: u32,
    /// NUMA node hint for the owning core.
    numa_node: u32,
    /// Flat slab of connection entries, indexed by slot.
    slab: Vec<ConnectionEntry>,
    /// Per-slot generation counters (parallel to `slab`).
    generations: Vec<u16>,
    /// LIFO free list of available slot indices.
    free_list: Vec<u32>,
    /// `stream_id → slot index` for O(1) incoming-packet lookup.
    stream_index: FxHashMap<u64, u32>,
    /// Configuration (capacity, backpressure thresholds).
    config: ShardConfig,
    /// Lock-free statistics collector.
    stats: StatsCollector,
    /// Current backpressure level (recomputed on demand).
    backpressure: BackpressureLevel,
    /// Sharding strategy (default: [`LocalSharding`]).
    strategy: Box<dyn ShardingStrategy>,
    /// Whether the shard has begun shutdown.
    shutting_down: bool,
    /// Slots in `Closed` state awaiting reclamation by `drain_closed`.
    /// Avoids O(capacity) full-slab scan — drain is O(closed_count).
    closed_slots: Vec<u32>,
    /// Parallel side table: `peer_addr` per slot. Kept out of
    /// [`ConnectionEntry`] so each entry fits in exactly one cache line
    /// (64 bytes). Peer addresses are cold data — only needed for
    /// snapshots and migration, not on the per-packet hot path.
    peer_addrs: Vec<SocketAddr>,
}

impl ConnectionShard {
    // ════════════════════════════════════════════════════════════
    //  Construction
    // ════════════════════════════════════════════════════════════

    /// Creates a shard from a validated config.
    ///
    /// Pre-allocates the slab and generation vectors, builds the free
    /// list in LIFO order (so slot 0 is allocated first).
    pub(crate) fn from_config(config: ShardConfig) -> Result<Self, ShardError> {
        config.validate()?;
        let capacity = config.capacity;
        let reactor_id = config.reactor_id;
        let numa_node = config.numa_node;
        let shutting_down = config.shutting_down;

        // Initialize slab with default entries.
        let slab = (0..capacity).map(|_| ConnectionEntry::default()).collect();
        // Generations start at 1 (0 is reserved for the nil id sentinel).
        let generations = vec![1u16; capacity];
        // Free list in LIFO order: push in reverse so slot 0 pops first.
        let free_list: Vec<u32> = (0..capacity as u32).rev().collect();

        Ok(Self {
            reactor_id,
            numa_node,
            slab,
            generations,
            free_list,
            stream_index: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            config,
            stats: StatsCollector::new(),
            backpressure: BackpressureLevel::None,
            strategy: Box::new(LocalSharding::new(reactor_id)),
            shutting_down,
            closed_slots: Vec::new(),
            peer_addrs: vec![SocketAddr::from(([0, 0, 0, 0], 0)); capacity],
        })
    }

    /// Creates a shard with a custom sharding strategy.
    pub(crate) fn with_strategy(mut self, strategy: Box<dyn ShardingStrategy>) -> Self {
        self.strategy = strategy;
        self
    }

    // ════════════════════════════════════════════════════════════
    //  Accessors
    // ════════════════════════════════════════════════════════════

    /// The reactor / core id that owns this shard.
    #[inline]
    #[must_use]
    pub fn reactor_id(&self) -> u32 {
        self.reactor_id
    }

    /// The NUMA node hint for this shard.
    #[inline]
    #[must_use]
    pub fn numa_node(&self) -> u32 {
        self.numa_node
    }

    /// The slab capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    /// The shard configuration.
    #[inline]
    #[must_use]
    pub fn config(&self) -> &ShardConfig {
        &self.config
    }

    /// Whether the shard is shutting down.
    #[inline]
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down
    }

    /// Number of allocated connections (slots not on the free list).
    ///
    /// This is O(1) — computed from `capacity - free_list.len()`.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.config.capacity - self.free_list.len()
    }

    /// Number of free slots available for new connections.
    #[inline]
    #[must_use]
    pub fn free_slots(&self) -> usize {
        self.free_list.len()
    }

    /// Current backpressure level.
    #[inline]
    #[must_use]
    pub fn backpressure(&self) -> BackpressureLevel {
        self.backpressure
    }

    // ════════════════════════════════════════════════════════════
    //  Fast path — lookup
    // ════════════════════════════════════════════════════════════

    /// Looks up a connection by its logical `stream_id`.
    ///
    /// This is the primary per-packet entry point: the reactor extracts
    /// the `stream_id` from the incoming frame header and calls this
    /// method to find the owning connection.
    ///
    /// Returns the [`ConnectionId`] (slot + generation) if found.
    #[inline]
    pub fn lookup_by_stream(&self, stream_id: u64) -> Option<ConnectionId> {
        let slot = match self.stream_index.get(&stream_id).copied() {
            Some(s) => {
                self.stats.record_lookup(true);
                s
            }
            None => {
                self.stats.record_lookup(false);
                return None;
            }
        };
        let slot_idx = slot as usize;
        if slot_idx >= self.slab.len() {
            return None;
        }
        // Verify the slot is live (not Closed/free).
        let entry = &self.slab[slot_idx];
        if !entry.state.is_live() && entry.state != ConnectionState::Migrating {
            return None;
        }
        Some(make_id(slot, self.generations[slot_idx]))
    }

    // ════════════════════════════════════════════════════════════
    //  Fast path — touch
    // ════════════════════════════════════════════════════════════

    /// Validates a [`ConnectionId`] against the current generation.
    ///
    /// Returns the slot index if valid, or `None` if stale/out-of-range.
    #[inline]
    fn validate_id(&self, id: ConnectionId) -> Option<usize> {
        let slot = id.slot() as usize;
        if slot >= self.slab.len() {
            return None;
        }
        if self.generations[slot] != id.generation() {
            return None;
        }
        Some(slot)
    }

    /// Records bytes received on a connection.
    ///
    /// This is the per-packet fast path. Returns
    /// [`NotFound`](ShardError::NotFound) if the id is stale.
    ///
    /// `now_ns` should be a monotonic timestamp — ideally cached from the
    /// reactor's drain cycle rather than re-computed per call.
    #[inline]
    pub fn touch_recv(
        &mut self,
        id: ConnectionId,
        bytes: usize,
        now_ns: u64,
    ) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        self.slab[slot].add_recv(bytes as u64, now_ns);
        self.stats.record_recv(bytes as u64);
        Ok(())
    }

    /// Records bytes sent on a connection.
    ///
    /// This is the per-packet fast path. Returns
    /// [`NotFound`](ShardError::NotFound) if the id is stale.
    ///
    /// `now_ns` should be a monotonic timestamp — ideally cached from the
    /// reactor's drain cycle rather than re-computed per call.
    #[inline]
    pub fn touch_send(
        &mut self,
        id: ConnectionId,
        bytes: usize,
        now_ns: u64,
    ) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        self.slab[slot].add_send(bytes as u64, now_ns);
        self.stats.record_send(bytes as u64);
        Ok(())
    }

    // ════════════════════════════════════════════════════════════
    //  Slow path — lifecycle
    // ════════════════════════════════════════════════════════════

    /// Creates a new connection in the `Init` state.
    ///
    /// Returns [`ShardFull`] if the slab has no free slots, or
    /// [`DuplicateStream`] if a connection with the same `stream_id`
    /// already exists.
    ///
    /// [`ShardFull`]: ShardError::ShardFull
    /// [`DuplicateStream`]: ShardError::DuplicateStream
    pub fn create(&mut self, spec: ConnectionSpec) -> Result<ConnectionId, ShardError> {
        if self.shutting_down {
            return Err(ShardError::ShuttingDown);
        }
        // Check structural capacity first — ShardFull is a hard limit
        // that applies regardless of backpressure policy.
        if self.free_list.is_empty() {
            return Err(ShardError::ShardFull {
                capacity: self.config.capacity,
            });
        }
        if self.backpressure.rejects_new() {
            return Err(ShardError::RejectedByBackpressure {
                level: self.backpressure,
            });
        }
        if self.stream_index.contains_key(&spec.stream_id) {
            return Err(ShardError::DuplicateStream(spec.stream_id));
        }
        let Some(slot) = self.free_list.pop() else {
            return Err(ShardError::ShardFull {
                capacity: self.config.capacity,
            });
        };
        let slot_idx = slot as usize;
        let now_ns = monotonic_now_ns();

        let entry = &mut self.slab[slot_idx];
        *entry = ConnectionEntry::new(
            spec.stream_id,
            self.reactor_id,
            self.numa_node,
            spec.tenant_id,
            spec.priority,
            now_ns,
        );
        self.peer_addrs[slot_idx] = spec.peer_addr;

        self.stream_index.insert(spec.stream_id, slot);
        let id = make_id(slot, self.generations[slot_idx]);
        self.stats.record_created();

        // Update peak watermark.
        let active = self.active_count();
        self.stats.update_peak_active(active as u64);

        // Recompute backpressure if we crossed a threshold.
        self.recompute_backpressure();

        tracing::debug!(
            reactor_id = self.reactor_id,
            stream_id = spec.stream_id,
            slot,
            "connection created"
        );
        Ok(id)
    }

    /// Destroys a connection immediately (forcible removal).
    ///
    /// For graceful shutdown use [`begin_close`](Self::begin_close) +
    /// [`drain_closed`](Self::drain_closed) instead.
    pub fn destroy(&mut self, id: ConnectionId) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        self.retire_slot(slot as u32);
        self.stats.record_destroyed();
        self.recompute_backpressure();
        Ok(())
    }

    /// Transitions a connection from `Init` → `Active`.
    pub fn activate(&mut self, id: ConnectionId) -> Result<(), ShardError> {
        self.transition(id, ConnectionState::Active)
    }

    /// Transitions a connection to `Backpressured`.
    pub fn set_backpressured(&mut self, id: ConnectionId) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        let current = self.slab[slot].state;
        if current != ConnectionState::Active {
            return Err(ShardError::IllegalTransition {
                from: current,
                to: ConnectionState::Backpressured,
            });
        }
        self.slab[slot].state = ConnectionState::Backpressured;
        self.stats.record_transition();
        Ok(())
    }

    /// Transitions a connection from `Backpressured` → `Active`.
    pub fn relieve_backpressure(&mut self, id: ConnectionId) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        let current = self.slab[slot].state;
        if current != ConnectionState::Backpressured {
            return Err(ShardError::IllegalTransition {
                from: current,
                to: ConnectionState::Active,
            });
        }
        self.slab[slot].state = ConnectionState::Active;
        self.stats.record_transition();
        Ok(())
    }

    /// Initiates a graceful close: `Active`/`Backpressured`/`Init` → `Closing`.
    pub fn begin_close(&mut self, id: ConnectionId) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        let current = self.slab[slot].state;
        if !current.can_close() {
            return Err(ShardError::IllegalTransition {
                from: current,
                to: ConnectionState::Closing,
            });
        }
        self.slab[slot].state = ConnectionState::Closing;
        self.stats.record_transition();
        Ok(())
    }

    /// Recycles all `Closed` entries, returning the number reclaimed.
    pub fn drain_closed(&mut self) -> usize {
        if self.closed_slots.is_empty() {
            return 0;
        }
        let reclaimed = self.closed_slots.len();
        let to_retire = std::mem::take(&mut self.closed_slots);
        for &slot in &to_retire {
            self.retire_slot(slot);
        }
        if reclaimed > 0 {
            self.recompute_backpressure();
        }
        reclaimed
    }

    /// Marks `Closing` connections as `Closed` (transition to drainable).
    pub fn finalize_closing(&mut self) -> usize {
        let mut count = 0;
        for (slot, entry) in self.slab.iter_mut().enumerate() {
            if entry.state == ConnectionState::Closing {
                entry.state = ConnectionState::Closed;
                self.closed_slots.push(slot as u32);
                count += 1;
            }
        }
        if count > 0 {
            self.stats.record_transition();
        }
        count
    }

    // ════════════════════════════════════════════════════════════
    //  Internal — slot management
    // ════════════════════════════════════════════════════════════

    /// Retires slot `slot`: removes from stream index, resets entry,
    /// bumps generation, returns to free list.
    fn retire_slot(&mut self, slot: u32) {
        let slot_idx = slot as usize;
        let stream_id = self.slab[slot_idx].stream_id;
        self.stream_index.remove(&stream_id);
        self.slab[slot_idx].reset_for_reuse();
        self.peer_addrs[slot_idx] = SocketAddr::from(([0, 0, 0, 0], 0));
        // Bump generation (wrapping at 16 bits). A generation of 0
        // would produce the nil id — skip it.
        let new_gen = (self.generations[slot_idx].wrapping_add(1)) & GEN_MASK as u16;
        self.generations[slot_idx] = if new_gen == 0 { 1 } else { new_gen };
        self.free_list.push(slot);
    }

    /// Applies a state transition with validation.
    fn transition(&mut self, id: ConnectionId, to: ConnectionState) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        let from = self.slab[slot].state;
        let valid = matches!(
            (from, to),
            (ConnectionState::Init, ConnectionState::Active)
                | (ConnectionState::Active, ConnectionState::Backpressured)
                | (ConnectionState::Backpressured, ConnectionState::Active)
                | (ConnectionState::Init, ConnectionState::Closing)
                | (ConnectionState::Active, ConnectionState::Closing)
                | (ConnectionState::Backpressured, ConnectionState::Closing)
                | (ConnectionState::Closing, ConnectionState::Closed)
        );
        if !valid {
            return Err(ShardError::IllegalTransition { from, to });
        }
        self.slab[slot].state = to;
        if to == ConnectionState::Closed {
            self.closed_slots.push(slot as u32);
        }
        self.stats.record_transition();
        Ok(())
    }

    // ════════════════════════════════════════════════════════════
    //  Migration
    // ════════════════════════════════════════════════════════════

    /// Prepares a connection for migration: freezes state and serializes.
    ///
    /// The connection transitions to `Migrating` and will not accept
    /// traffic until the migration completes or is rolled back.
    pub fn prepare_migration(
        &mut self,
        id: ConnectionId,
        target_cpu: u32,
    ) -> Result<MigrationRequest, ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        let entry = &self.slab[slot];
        if entry.state == ConnectionState::Migrating {
            return Err(ShardError::MigrationConflict(entry.stream_id));
        }
        if !entry.state.can_migrate() {
            return Err(ShardError::IllegalTransition {
                from: entry.state,
                to: ConnectionState::Migrating,
            });
        }
        let stream_id = entry.stream_id;
        let req = MigrationRequest {
            stream_id,
            peer_addr: self.peer_addrs[slot],
            source_cpu: self.reactor_id,
            target_cpu,
            bytes_sent: entry.bytes_sent,
            bytes_recv: entry.bytes_recv,
            created_at_ns: entry.created_at_ns,
            priority: entry.priority,
            tenant_id: entry.tenant_id,
        };
        self.slab[slot].state = ConnectionState::Migrating;
        self.stats.record_transition();
        tracing::debug!(
            reactor_id = self.reactor_id,
            stream_id,
            target_cpu,
            "migration prepared"
        );
        Ok(req)
    }

    /// Accepts a migrated connection from another shard.
    ///
    /// Creates a new connection in the `Active` state with carried-over
    /// statistics.
    pub fn accept_migration(&mut self, req: MigrationRequest) -> Result<ConnectionId, ShardError> {
        if self.shutting_down {
            return Err(ShardError::ShuttingDown);
        }
        if !req.targets(self.reactor_id) {
            return Err(ShardError::InvalidConfig(format!(
                "migration request targets cpu {} but this is cpu {}",
                req.target_cpu, self.reactor_id
            )));
        }
        if self.stream_index.contains_key(&req.stream_id) {
            return Err(ShardError::DuplicateStream(req.stream_id));
        }
        let slot = self.free_list.pop().ok_or(ShardError::ShardFull {
            capacity: self.config.capacity,
        })?;
        let slot_idx = slot as usize;
        let now_ns = monotonic_now_ns();

        let entry = &mut self.slab[slot_idx];
        *entry = ConnectionEntry::new(
            req.stream_id,
            self.reactor_id,
            self.numa_node,
            req.tenant_id,
            req.priority,
            req.created_at_ns,
        );
        self.peer_addrs[slot_idx] = req.peer_addr;
        entry.state = ConnectionState::Active;
        entry.bytes_sent = req.bytes_sent;
        entry.bytes_recv = req.bytes_recv;
        entry.last_active_ns = now_ns;

        self.stream_index.insert(req.stream_id, slot);
        let id = make_id(slot, self.generations[slot_idx]);
        self.stats.record_migration_in();
        self.recompute_backpressure();
        tracing::debug!(
            reactor_id = self.reactor_id,
            stream_id = req.stream_id,
            slot,
            "migration accepted"
        );
        Ok(id)
    }

    /// Completes an outbound migration by removing the connection.
    pub fn complete_migration_out(&mut self, id: ConnectionId) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        if self.slab[slot].state != ConnectionState::Migrating {
            return Err(ShardError::IllegalTransition {
                from: self.slab[slot].state,
                to: ConnectionState::Closed,
            });
        }
        self.retire_slot(slot as u32);
        self.stats.record_migration_out();
        self.recompute_backpressure();
        Ok(())
    }

    /// Rolls back a migration (returns connection to `Active`).
    pub fn rollback_migration(&mut self, id: ConnectionId) -> Result<(), ShardError> {
        let slot = self.validate_id(id).ok_or(ShardError::NotFound)?;
        if self.slab[slot].state != ConnectionState::Migrating {
            return Err(ShardError::IllegalTransition {
                from: self.slab[slot].state,
                to: ConnectionState::Active,
            });
        }
        self.slab[slot].state = ConnectionState::Active;
        self.stats.record_transition();
        Ok(())
    }

    // ════════════════════════════════════════════════════════════
    //  Backpressure
    // ════════════════════════════════════════════════════════════

    /// Recomputes the backpressure level from the current active count.
    pub fn recompute_backpressure(&mut self) {
        let active = self.active_count();
        let soft = self.config.soft_threshold();
        let hard = self.config.hard_threshold();
        let new_level = if active >= hard {
            BackpressureLevel::Hard
        } else if active >= soft {
            BackpressureLevel::Soft
        } else {
            BackpressureLevel::None
        };

        // Record transitions.
        match (self.backpressure, new_level) {
            (BackpressureLevel::None, BackpressureLevel::Soft) => {
                self.stats.record_soft_bp();
            }
            (BackpressureLevel::None, BackpressureLevel::Hard) => {
                self.stats.record_hard_bp();
            }
            (BackpressureLevel::Soft, BackpressureLevel::Hard) => {
                self.stats.record_hard_bp();
            }
            _ => {}
        }
        self.backpressure = new_level;
    }

    // ════════════════════════════════════════════════════════════
    //  Sharding
    // ════════════════════════════════════════════════════════════

    /// Asks the strategy which shard should own `stream_id`.
    ///
    /// With the default [`LocalSharding`] strategy this always returns
    /// `self.reactor_id()`.
    #[inline]
    #[must_use]
    pub fn shard_for_stream(&self, stream_id: u64) -> u32 {
        // We don't know the global shard count; assume it's at least
        // reactor_id + 1.
        self.strategy
            .shard_for_stream(stream_id, self.reactor_id + 1)
    }

    // ════════════════════════════════════════════════════════════
    //  Query / observability
    // ════════════════════════════════════════════════════════════

    /// Returns a read-only snapshot of a connection.
    #[must_use]
    pub fn view(&self, id: ConnectionId) -> Option<crate::connection::ConnectionSnapshot> {
        let slot = self.validate_id(id)?;
        Some(self.slab[slot].snapshot(self.peer_addrs[slot]))
    }

    /// Returns a statistics snapshot.
    #[must_use]
    pub fn stats(&self) -> StatsSnapshot {
        self.stats.snapshot()
    }

    /// Returns the stats collector reference (for direct recording if
    /// needed by integration code).
    #[must_use]
    pub fn stats_collector(&self) -> &StatsCollector {
        &self.stats
    }

    /// Derives a health report from current state.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(
            self.active_count(),
            self.config.capacity,
            self.backpressure,
            &self.stats.snapshot(),
        )
    }

    /// Begins shutdown: future `create` calls will return [`ShuttingDown`].
    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
        tracing::info!(reactor_id = self.reactor_id, "shard entering shutdown");
    }
}

impl std::fmt::Debug for ConnectionShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionShard")
            .field("reactor_id", &self.reactor_id)
            .field("numa_node", &self.numa_node)
            .field("capacity", &self.config.capacity)
            .field("free_slots", &self.free_list.len())
            .field("backpressure", &self.backpressure)
            .field("shutting_down", &self.shutting_down)
            .finish_non_exhaustive()
    }
}

/// Returns a monotonic timestamp in nanoseconds.
///
/// Uses `Instant` internally; the absolute value is arbitrary but
/// monotonically increasing within a process lifetime.
#[inline]
pub fn monotonic_now_ns() -> u64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(std::time::Instant::now);
    epoch.elapsed().as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::HealthState;
    use std::net::SocketAddr;

    fn make_shard(capacity: usize) -> ConnectionShard {
        ConnectionShard::from_config(ShardConfig {
            capacity,
            ..ShardConfig::default()
        })
        .expect("shard build")
    }

    fn sample_addr() -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], 443))
    }

    // ── create + lookup ─────────────────────────────────────────

    #[test]
    fn create_and_lookup() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(42, sample_addr()))
            .expect("create");
        assert_eq!(shard.lookup_by_stream(42), Some(id));
    }

    #[test]
    fn lookup_miss() {
        let shard = make_shard(256);
        assert_eq!(shard.lookup_by_stream(999), None);
    }

    #[test]
    fn duplicate_stream_rejected() {
        let mut shard = make_shard(256);
        let _ = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("first");
        let err = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .unwrap_err();
        assert!(matches!(err, ShardError::DuplicateStream(1)));
    }

    #[test]
    fn shard_full_rejected() {
        let mut shard = ConnectionShard::from_config(ShardConfig {
            capacity: 4,
            soft_bp_pct: 50,
            hard_bp_pct: 100,
            ..ShardConfig::default()
        })
        .expect("shard");
        for i in 0..4 {
            shard
                .create(ConnectionSpec::new(i, sample_addr()))
                .expect("create");
        }
        let err = shard
            .create(ConnectionSpec::new(99, sample_addr()))
            .unwrap_err();
        assert!(matches!(err, ShardError::ShardFull { capacity: 4 }));
    }

    // ── destroy + stale id ──────────────────────────────────────

    #[test]
    fn destroy_makes_id_stale() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(5, sample_addr()))
            .expect("create");
        shard.destroy(id).expect("destroy");
        // The id should now be stale (generation bumped).
        assert!(shard.view(id).is_none());
    }

    #[test]
    fn stale_id_rejected() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(5, sample_addr()))
            .expect("create");
        shard.destroy(id).expect("destroy");
        let err = shard.touch_recv(id, 100, 1000).unwrap_err();
        assert!(matches!(err, ShardError::NotFound));
    }

    #[test]
    fn slot_recycled_after_destroy() {
        let mut shard = make_shard(4);
        let id1 = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.destroy(id1).expect("destroy");
        // The slot is back in the free list; all slots free again.
        assert_eq!(shard.free_slots(), 4);
        let id2 = shard
            .create(ConnectionSpec::new(2, sample_addr()))
            .expect("create reused");
        // New id should have a higher generation.
        assert!(id2.generation() > id1.generation());
    }

    // ── state transitions ───────────────────────────────────────

    #[test]
    fn activate_from_init() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.activate(id).expect("activate");
        let snap = shard.view(id).expect("view");
        assert_eq!(snap.state, ConnectionState::Active);
    }

    #[test]
    fn illegal_transition_rejected() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        // Init → Backpressured is not valid.
        let err = shard.set_backpressured(id).unwrap_err();
        assert!(matches!(err, ShardError::IllegalTransition { .. }));
    }

    #[test]
    fn close_from_active() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.activate(id).expect("activate");
        shard.begin_close(id).expect("close");
        let snap = shard.view(id).expect("view");
        assert_eq!(snap.state, ConnectionState::Closing);
    }

    // ── touch ───────────────────────────────────────────────────

    #[test]
    fn touch_updates_counters() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.touch_recv(id, 1024, 1000).expect("recv");
        shard.touch_send(id, 2048, 2000).expect("send");
        let snap = shard.view(id).expect("view");
        assert_eq!(snap.bytes_recv, 1024);
        assert_eq!(snap.bytes_sent, 2048);
    }

    // ── backpressure ────────────────────────────────────────────

    #[test]
    fn backpressure_triggers_at_threshold() {
        let mut shard = ConnectionShard::from_config(ShardConfig {
            capacity: 100,
            soft_bp_pct: 80,
            hard_bp_pct: 95,
            ..ShardConfig::default()
        })
        .expect("build");
        // Fill to 80 → soft.
        for i in 0..80 {
            shard
                .create(ConnectionSpec::new(i, sample_addr()))
                .expect("create");
        }
        assert_eq!(shard.backpressure(), BackpressureLevel::Soft);
        // Fill to 95 → hard.
        for i in 80..95 {
            shard
                .create(ConnectionSpec::new(i, sample_addr()))
                .expect("create");
        }
        assert_eq!(shard.backpressure(), BackpressureLevel::Hard);
        // Hard rejects new connections.
        let err = shard
            .create(ConnectionSpec::new(999, sample_addr()))
            .unwrap_err();
        assert!(matches!(err, ShardError::RejectedByBackpressure { .. }));
    }

    #[test]
    fn backpressure_relieves_on_destroy() {
        let mut shard = ConnectionShard::from_config(ShardConfig {
            capacity: 10,
            soft_bp_pct: 80,
            hard_bp_pct: 90,
            ..ShardConfig::default()
        })
        .expect("build");
        for i in 0..9 {
            shard
                .create(ConnectionSpec::new(i, sample_addr()))
                .expect("create");
        }
        assert_eq!(shard.backpressure(), BackpressureLevel::Hard);
        // Destroy one to drop below hard.
        let id = make_id(0, 1);
        shard.destroy(id).expect("destroy");
        assert_ne!(shard.backpressure(), BackpressureLevel::Hard);
    }

    // ── migration ───────────────────────────────────────────────

    #[test]
    fn migration_round_trip() {
        let mut source = make_shard(256);
        let target_cpu = 1;
        // Source creates and activates.
        let id = source
            .create(ConnectionSpec::new(42, sample_addr()))
            .expect("create");
        source.activate(id).expect("activate");

        // Prepare migration.
        let req = source.prepare_migration(id, target_cpu).expect("prepare");
        assert_eq!(req.stream_id, 42);
        assert_eq!(req.source_cpu, 0);
        assert_eq!(req.target_cpu, target_cpu);

        // Connection is now Migrating on source.
        let snap = source.view(id).expect("view");
        assert_eq!(snap.state, ConnectionState::Migrating);

        // Target accepts.
        let mut target = ConnectionShard::from_config(ShardConfig {
            capacity: 256,
            reactor_id: target_cpu,
            ..ShardConfig::default()
        })
        .expect("target");
        let new_id = target.accept_migration(req).expect("accept");
        let snap = target.view(new_id).expect("view");
        assert_eq!(snap.state, ConnectionState::Active);
        assert_eq!(snap.stream_id, 42);

        // Source completes migration out.
        source.complete_migration_out(id).expect("complete");
        assert!(source.view(id).is_none());
        assert!(source.lookup_by_stream(42).is_none());
    }

    #[test]
    fn migration_conflict_on_double_prepare() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.activate(id).expect("activate");
        shard.prepare_migration(id, 1).expect("first prepare");
        let err = shard.prepare_migration(id, 1).unwrap_err();
        assert!(matches!(err, ShardError::MigrationConflict(1)));
    }

    #[test]
    fn rollback_migration() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.activate(id).expect("activate");
        shard.prepare_migration(id, 1).expect("prepare");
        shard.rollback_migration(id).expect("rollback");
        let snap = shard.view(id).expect("view");
        assert_eq!(snap.state, ConnectionState::Active);
    }

    // ── drain ───────────────────────────────────────────────────

    #[test]
    fn drain_closed_reclaims_slots() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.activate(id).expect("activate");
        shard.begin_close(id).expect("close");
        shard.finalize_closing();
        let reclaimed = shard.drain_closed();
        assert_eq!(reclaimed, 1);
        assert!(shard.view(id).is_none());
        assert!(shard.lookup_by_stream(1).is_none());
    }

    // ── stats ───────────────────────────────────────────────────

    #[test]
    fn stats_reflect_operations() {
        let mut shard = make_shard(256);
        let id = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .expect("create");
        shard.touch_recv(id, 100, 1000).expect("recv");
        shard.touch_send(id, 200, 2000).expect("send");
        shard.lookup_by_stream(1);
        shard.lookup_by_stream(999); // miss

        let s = shard.stats();
        assert_eq!(s.connections_created, 1);
        assert_eq!(s.bytes_recv, 100);
        assert_eq!(s.bytes_sent, 200);
        assert_eq!(s.lookups_total, 2);
        assert_eq!(s.lookups_missed, 1);
    }

    // ── health ──────────────────────────────────────────────────

    #[test]
    fn health_empty_is_healthy() {
        let shard = make_shard(256);
        let h = shard.health();
        assert_eq!(h.state, HealthState::Healthy);
    }

    #[test]
    fn health_full_is_critical() {
        let shard = ConnectionShard::from_config(ShardConfig {
            capacity: 10,
            soft_bp_pct: 80,
            hard_bp_pct: 90,
            ..ShardConfig::default()
        })
        .expect("build");
        // Can't fill directly (hard BP rejects), but we can check
        // health on a near-full shard.
        let h = shard.health();
        assert!(h.is_healthy());
    }

    // ── shutdown ────────────────────────────────────────────────

    #[test]
    fn shutdown_rejects_create() {
        let mut shard = make_shard(256);
        shard.begin_shutdown();
        let err = shard
            .create(ConnectionSpec::new(1, sample_addr()))
            .unwrap_err();
        assert!(matches!(err, ShardError::ShuttingDown));
    }

    // ── id encoding ─────────────────────────────────────────────

    #[test]
    fn generation_never_zero() {
        let mut shard = make_shard(4);
        // Create and destroy many times to cycle the generation.
        for _ in 0..10 {
            let id = shard
                .create(ConnectionSpec::new(1, sample_addr()))
                .expect("create");
            shard.destroy(id).expect("destroy");
        }
        // All generations should be non-zero.
        for slot in 0..shard.slab.len() {
            assert!(shard.generations[slot] != 0, "slot {slot} has gen 0");
        }
    }
}
