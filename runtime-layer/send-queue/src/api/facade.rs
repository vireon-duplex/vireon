//! [`SendQueue`] facade (Layer 12).
//!
//! The facade hides the ring, scheduler, batch composer, in-flight table,
//! and backpressure gate behind a small set of synchronous operations.

use std::collections::HashSet;
use std::net::SocketAddr;

use crate::api::meta::{PacketId, PacketMeta};
use crate::backpressure::{BackpressureGate, PressureLevel, PressureObserver};
use crate::batch::BatchComposer;
use crate::config::SendQueueConfig;
use crate::error::SendQueueError;
use crate::lifecycle::{InFlightTable, decode_slot, encode_id};
use crate::queue::{IngressRing, SlotState};
use crate::retry::RetryTracker;
use crate::scheduler::{SchedulePolicy, build_policy};
use crate::stats::StatsCollector;
use crate::submit::SubmissionSink;

/// Tracking-slot allocator state.
struct TrackingSlotAlloc {
    /// Current generation per tracking slot. Incremented each time a slot
    /// is freed, so stale CQEs from a previous packet are rejected.
    gens: Vec<u16>,
    /// LIFO free-list of available tracking-slot indices.
    free_slots: Vec<u16>,
}

impl TrackingSlotAlloc {
    fn new(capacity: usize) -> Self {
        Self {
            gens: vec![0u16; capacity],
            free_slots: (0..capacity as u16).rev().collect(),
        }
    }

    /// Pops a free slot and returns the encoded `(slot_index, generation)` id.
    fn alloc(&mut self) -> Option<u64> {
        let slot_idx = self.free_slots.pop()?;
        let generation = self.gens[slot_idx as usize];
        Some(encode_id(slot_idx, generation))
    }

    /// Returns a slot to the free-list and bumps its generation.
    fn free(&mut self, id: u64) {
        let idx = decode_slot(id) as usize;
        if idx < self.gens.len() {
            self.gens[idx] = self.gens[idx].wrapping_add(1);
            self.free_slots.push(idx as u16);
        }
    }

    /// Number of free tracking slots.
    fn free_count(&self) -> usize {
        self.free_slots.len()
    }
}

/// Thread-local send queue. Owns the ingress ring, scheduler, batch
/// composer, retry tracker, and in-flight table.
///
/// Constructed via [`SendQueueBuilder`](crate::SendQueueBuilder). All
/// mutation methods take `&mut self`; read-only accessors take `&self`.
pub struct SendQueue {
    ring: IngressRing,
    scheduler: Box<dyn SchedulePolicy>,
    batch: BatchComposer,
    inflight: InFlightTable,
    gate: BackpressureGate,
    retry: RetryTracker,
    stats: StatsCollector,
    config: SendQueueConfig,
    tracking: TrackingSlotAlloc,
    cancelled: HashSet<u64>,
    observers: Vec<Box<dyn PressureObserver>>,
}

impl SendQueue {
    /// Creates a queue from a validated config. Prefer
    /// [`SendQueueBuilder`](crate::SendQueueBuilder) for public construction.
    pub(crate) fn from_config(config: SendQueueConfig) -> Result<Self, SendQueueError> {
        let mut cfg = config;
        cfg.round_capacity_up();

        // Adjust derived defaults if they're inconsistent with the
        // rounded capacity (e.g. when the builder changed capacity after
        // defaults were populated).
        if cfg.high_watermark > cfg.capacity {
            cfg.low_watermark = cfg.capacity * 3 / 4;
            cfg.high_watermark = cfg.capacity * 9 / 10;
            cfg.hysteresis = cfg.capacity / 10;
        }
        if cfg.batch_max_packets > cfg.capacity {
            cfg.batch_max_packets = cfg.capacity;
        }

        cfg.validate()?;

        let capacity = cfg.capacity;
        let ring = IngressRing::new(capacity)?;
        let scheduler = build_policy(cfg.policy);
        let batch = BatchComposer::new(
            cfg.batch_max_packets,
            cfg.batch_max_bytes,
            cfg.batch_max_age_ns,
        );
        let inflight = InFlightTable::new(capacity);
        let gate = BackpressureGate::new(
            cfg.low_watermark,
            cfg.high_watermark,
            cfg.hysteresis,
            capacity,
        );
        let retry = RetryTracker::new(cfg.retry);
        let stats = StatsCollector::new();
        let tracking = TrackingSlotAlloc::new(capacity);

        Ok(Self {
            ring,
            scheduler,
            batch,
            inflight,
            gate,
            retry,
            stats,
            config: cfg,
            tracking,
            cancelled: HashSet::new(),
            observers: Vec::new(),
        })
    }

    // ── Submission ──────────────────────────────────────────────────

    /// Submits a packet for transmission.
    ///
    /// `buf_index` is the io_uring registered-buffer index; `len` is the
    /// payload length within that buffer. The caller must keep the buffer
    /// alive until the matching `SendBufferReclaim` completion (signalled
    /// via [`on_reclaim_cqe`](Self::on_reclaim_cqe)).
    ///
    /// Returns [`Err(QueueFull)`](SendQueueError::QueueFull) when the
    /// queue or the tracking-slot pool is saturated.
    pub fn submit(
        &mut self,
        buf_index: u16,
        len: u32,
        dest: SocketAddr,
        meta: PacketMeta,
    ) -> Result<PacketId, SendQueueError> {
        self.stats.record_submit();

        // Backpressure admission check.
        let depth = self.ring.len();
        if let Err(e) = self.gate.admit(depth) {
            self.stats.record_submit_rejected();
            return Err(e);
        }

        // Allocate a tracking slot.
        let tracking_id = self.tracking.alloc().ok_or(SendQueueError::QueueFull {
            capacity: self.config.capacity,
        })?;

        // Fill and push to ring.
        let mut slot = crate::queue::slot::PacketSlot::empty();
        slot.fill(
            tracking_id,
            buf_index,
            len,
            dest,
            meta.priority,
            meta.deadline_ns,
            meta.stream_id,
        );
        if let Err(e) = self.ring.push(slot) {
            // Roll back the tracking-slot allocation.
            self.tracking.free(tracking_id);
            self.stats.record_submit_rejected();
            return Err(e);
        }

        // Update pressure gauge and notify observers on transition.
        let new_depth = depth + 1;
        let level = self.gate.post_change(new_depth);
        self.notify_observers(level, new_depth);

        self.stats.record_submit_success();
        Ok(PacketId::from_raw(tracking_id))
    }

    /// Best-effort cancellation. Marks the packet for lazy removal: if it
    /// has not yet been flushed, it will be skipped and its tracking slot
    /// freed on the next [`flush`](Self::flush) call.
    ///
    /// If the packet has already been submitted to the IO Layer, this is
    /// a no-op (the CQE lifecycle runs to completion).
    pub fn cancel(&mut self, id: PacketId) {
        self.stats.record_cancel();
        if self.inflight.is_tracked(id.raw()) {
            return;
        }
        self.cancelled.insert(id.raw());
    }

    // ── Flush ───────────────────────────────────────────────────────

    /// Drains queued packets, batches them, and submits via `sink`.
    ///
    /// Returns the number of packets successfully submitted to the sink.
    pub fn flush(
        &mut self,
        sink: &mut dyn SubmissionSink,
        now_ns: u64,
    ) -> Result<usize, SendQueueError> {
        self.drain_and_submit(sink, now_ns, self.config.batch_max_packets, false)
    }

    /// Forces an immediate flush regardless of batch triggers, draining at
    /// most `max_packets` from the ring first.
    pub fn force_flush(
        &mut self,
        sink: &mut dyn SubmissionSink,
        now_ns: u64,
        max_packets: usize,
    ) -> Result<usize, SendQueueError> {
        self.drain_and_submit(sink, now_ns, max_packets, true)
    }

    fn drain_and_submit(
        &mut self,
        sink: &mut dyn SubmissionSink,
        now_ns: u64,
        max_drain: usize,
        _force: bool,
    ) -> Result<usize, SendQueueError> {
        // 1. Drain from scheduler into batch.
        let max = max_drain.min(self.config.batch_max_packets);
        let slots = self.scheduler.drain(&self.ring, max);
        for slot in &slots {
            if self.cancelled.remove(&slot.tracking_id) {
                self.tracking.free(slot.tracking_id);
                continue;
            }
            self.batch.push(slot, now_ns);
        }

        if self.batch.is_empty() {
            return Ok(0);
        }

        // 2. Submit the batch.
        let entries = self.batch.flush();
        debug_assert!(!entries.is_empty());
        let submitted = sink.submit_batch(&entries)?;

        // 3. Track submitted entries in the in-flight table.
        for entry in entries[..submitted].iter() {
            self.inflight.track(entry.user_data, now_ns);
        }
        if submitted > 0 {
            self.stats.record_flush(submitted as u64);
        }

        // 4. Push back un-submitted entries (SQ was full).
        for entry in entries[submitted..].iter() {
            self.batch.push_entry(*entry, now_ns);
        }

        // 5. Update pressure gauge.
        let depth = self.ring.len();
        let level = self.gate.post_change(depth);
        self.notify_observers(level, depth);

        Ok(submitted)
    }

    // ── CQE callbacks ───────────────────────────────────────────────

    /// Callback for the first CQE of a `SEND_ZC` pair (`Send` completion).
    ///
    /// Transitions the packet from `InFlight` to `Accepted`.
    pub fn on_send_cqe(&mut self, id: u64) {
        self.stats.record_send_cqe();
        self.inflight.transition(id, SlotState::Accepted);
    }

    /// Callback for the second CQE of a `SEND_ZC` pair
    /// (`SendBufferReclaim` completion).
    ///
    /// Retires the packet and frees its tracking slot so the buffer may
    /// be recycled.
    pub fn on_reclaim_cqe(&mut self, id: u64) {
        self.stats.record_reclaim_cqe();
        if self.inflight.retire(id) {
            self.tracking.free(id);
        }
    }

    /// Handles a retransmission timeout for packet `id`.
    ///
    /// Returns `Ok(rto_ns)` if the packet should be retried, or
    /// `Err(DropReason)` if the packet was dropped.
    pub fn on_timeout(&mut self, id: u64) -> Result<u64, crate::retry::DropReason> {
        let result = self.retry.on_timeout(&mut self.inflight, id);
        match &result {
            Ok(_) => self.stats.record_retry(),
            Err(_) => {
                self.stats.record_drop();
                self.tracking.free(id);
            }
        }
        result
    }

    // ── Observability ───────────────────────────────────────────────

    /// Current queue depth (relaxed atomic load).
    #[must_use]
    pub fn depth(&self) -> usize {
        self.ring.len()
    }

    /// Current pressure level.
    #[must_use]
    pub fn pressure(&self) -> PressureLevel {
        self.gate.level()
    }

    /// Number of packets currently tracked as in-flight.
    #[must_use]
    pub fn inflight_count(&self) -> usize {
        self.inflight.tracked_count()
    }

    /// Number of entries pending in the batch.
    #[must_use]
    pub fn batch_pending(&self) -> usize {
        self.batch.len()
    }

    /// Number of free tracking slots.
    #[must_use]
    pub fn free_tracking_slots(&self) -> usize {
        self.tracking.free_count()
    }

    /// Snapshot of the queue's metrics.
    #[must_use]
    pub fn stats(&self) -> crate::stats::StatsSnapshot {
        self.stats.snapshot(
            self.depth(),
            self.inflight_count(),
            self.batch_pending(),
            self.pressure().as_u8(),
        )
    }

    /// Returns the configured capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    // ── Observers ───────────────────────────────────────────────────

    /// Adds a pressure observer.
    pub fn add_observer(&mut self, obs: Box<dyn PressureObserver>) {
        self.observers.push(obs);
    }

    fn notify_observers(&self, level: PressureLevel, depth: usize) {
        for obs in &self.observers {
            obs.on_pressure(level, depth);
        }
    }
}

impl std::fmt::Debug for SendQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendQueue")
            .field("capacity", &self.config.capacity)
            .field("depth", &self.depth())
            .field("inflight", &self.inflight_count())
            .field("batch_pending", &self.batch_pending())
            .field("pressure", &self.pressure())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::submit::test_sink::RecordingSink;

    fn make_queue(cap: usize) -> SendQueue {
        let cfg = SendQueueConfig {
            capacity: cap,
            low_watermark: cap * 3 / 4,
            high_watermark: cap * 9 / 10,
            hysteresis: cap / 10,
            ..Default::default()
        };
        SendQueue::from_config(cfg).expect("queue")
    }

    #[test]
    fn submit_returns_packet_id() {
        let mut sq = make_queue(64);
        let id = sq
            .submit(0, 64, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
            .expect("submit");
        // Slot 0 with generation 0 encodes to 0 — that's valid.
        // Verify the id can be used for tracking by doing a round-trip.
        assert_eq!(crate::lifecycle::decode_slot(id.raw()), 0);
    }

    #[test]
    fn flush_submits_to_sink() {
        let mut sq = make_queue(64);
        sq.submit(0, 64, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
            .expect("submit");
        sq.submit(1, 128, "127.0.0.1:2".parse().unwrap(), PacketMeta::new())
            .expect("submit");

        let mut sink = RecordingSink::new();
        let n = sq.flush(&mut sink, 0).expect("flush");
        assert_eq!(n, 2);
        assert_eq!(sink.count(), 2);
        assert_eq!(sq.inflight_count(), 2);
    }

    #[test]
    fn reclaim_frees_tracking_slot() {
        let mut sq = make_queue(64);
        let id = sq
            .submit(0, 64, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
            .expect("submit");

        let mut sink = RecordingSink::new();
        sq.flush(&mut sink, 0).expect("flush");

        let free_before = sq.free_tracking_slots();
        sq.on_reclaim_cqe(id.raw());
        assert_eq!(sq.inflight_count(), 0);
        assert_eq!(sq.free_tracking_slots(), free_before + 1);
    }

    #[test]
    fn send_cqe_transitions_to_accepted() {
        let mut sq = make_queue(64);
        let id = sq
            .submit(0, 64, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
            .expect("submit");

        let mut sink = RecordingSink::new();
        sq.flush(&mut sink, 0).expect("flush");
        assert_eq!(sq.inflight.state(id.raw()), Some(SlotState::InFlight));

        sq.on_send_cqe(id.raw());
        assert_eq!(sq.inflight.state(id.raw()), Some(SlotState::Accepted));
    }

    #[test]
    fn cancel_before_flush_skips_packet() {
        let mut sq = make_queue(64);
        let id = sq
            .submit(0, 64, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
            .expect("submit");
        sq.cancel(id);

        let mut sink = RecordingSink::new();
        let n = sq.flush(&mut sink, 0).expect("flush");
        assert_eq!(n, 0);
        assert_eq!(sink.count(), 0);
    }

    #[test]
    fn queue_full_when_saturated() {
        let mut sq = make_queue(16);
        // Fill to high watermark (16 * 0.9 = 14.4 → high = 14)
        for _ in 0..14 {
            sq.submit(0, 4, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
                .expect("submit");
        }
        // Next submit at depth 14 should be rejected.
        let err = sq
            .submit(0, 4, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
            .unwrap_err();
        assert!(matches!(err, SendQueueError::QueueFull { .. }));
    }

    #[test]
    fn stats_snapshot_reflects_activity() {
        let mut sq = make_queue(64);
        sq.submit(0, 64, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
            .expect("submit");
        let mut sink = RecordingSink::new();
        sq.flush(&mut sink, 0).expect("flush");

        let snap = sq.stats();
        assert_eq!(snap.submit_total, 1);
        assert_eq!(snap.submit_success_total, 1);
        assert_eq!(snap.flush_total, 1);
        assert_eq!(snap.packets_submitted_total, 1);
        assert_eq!(snap.inflight_count, 1);
        assert_eq!(snap.queue_depth, 0);
    }

    #[test]
    fn end_to_end_submit_send_reclaim() {
        let mut sq = make_queue(64);
        let id = sq
            .submit(3, 256, "10.0.0.1:80".parse().unwrap(), PacketMeta::new())
            .expect("submit");

        let mut sink = RecordingSink::new();
        let n = sq.flush(&mut sink, 0).expect("flush");
        assert_eq!(n, 1);
        assert_eq!(sink.submitted()[0].buf_index, 3);
        assert_eq!(sink.submitted()[0].len, 256);

        sq.on_send_cqe(id.raw());
        sq.on_reclaim_cqe(id.raw());

        assert_eq!(sq.inflight_count(), 0);
        let snap = sq.stats();
        assert_eq!(snap.send_cqe_total, 1);
        assert_eq!(snap.reclaim_cqe_total, 1);
    }

    #[test]
    fn force_flush_drains_all() {
        let mut sq = make_queue(64);
        for i in 0..5 {
            sq.submit(i, 64, "127.0.0.1:1".parse().unwrap(), PacketMeta::new())
                .expect("submit");
        }
        let mut sink = RecordingSink::new();
        let n = sq.force_flush(&mut sink, 0, 10).expect("flush");
        assert_eq!(n, 5);
    }
}
