//! [`SendPolicyTable`] — the hot-path API consumed by `stream-scheduler`.
//!
//! Keyed by `(conn_idx, StreamId)` — the **logical** stream id from the
//! frame header (see [`frame::header::StreamId`]), not the QUIC transport
//! stream id. Mirrors the receive-side [`delivery_policy::DeliveryPolicyTable`]
//! so scheduler code can treat both halves of a stream's policy state
//! symmetrically.

use std::mem;

use frame::header::StreamId;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::decision::{DequeueOutcome, EnqueueDecision, FramePayload};
use crate::error::SendPolicyError;
use crate::metrics::SendPolicyMetrics;
use crate::state::PerStreamSendState;
use delivery_policy::DeliveryPolicy;

/// Maximum number of `(conn_idx, stream_id)` pairs the table will track.
///
/// Bounds worst-case memory: 65 536 streams × ~256 B state ≈ 16 MiB.
/// Past this, [`SendPolicyTable::bind`] returns
/// [`SendPolicyError::TableFull`] — the caller logs and rejects further
/// `StreamOpen` attempts until existing streams close.
///
/// Mirrors `delivery_policy::table::MAX_TRACKED_STREAMS` so a stream's
/// send-side and receive-side tables cap at the same point.
pub const MAX_TRACKED_STREAMS: usize = 65_536;

/// The per-stream send-policy registry. One instance per `QuicEngine`
/// (i.e. per reactor core).
#[derive(Debug)]
pub struct SendPolicyTable {
    /// `(conn_idx, logical_stream_id)` → state.
    entries: FxHashMap<(usize, StreamId), PerStreamSendState>,
    /// Streams that have been lazy-defaulted to `ReliableOrdered`
    /// because `enqueue` was called before its `StreamOpen`. Each entry
    /// is warned exactly once; removed when `bind` upgrades the stream.
    warned_default: FxHashSet<(usize, StreamId)>,
    /// Streams whose state currently has at least one frame ready for
    /// `dequeue`. Tracked so the scheduler's per-tick selection scan
    /// only visits streams with actual work.
    ///
    /// Membership mirrors [`PerStreamSendState::is_ready`]; updated on
    /// every `enqueue` and cleared by `dequeue` when the queue empties.
    ready: FxHashSet<(usize, StreamId)>,
    /// Atomic observability counters.
    metrics: SendPolicyMetrics,
}

impl SendPolicyTable {
    /// Construct an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: FxHashMap::default(),
            warned_default: FxHashSet::default(),
            ready: FxHashSet::default(),
            metrics: SendPolicyMetrics::new(),
        }
    }

    /// Number of streams currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` when no streams are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Register or upgrade the policy for a stream. Called when a
    /// `MessageType::StreamOpen` frame is about to be sent (or, on the
    /// receive side, has been decoded — both sides bind the same way).
    ///
    /// If the stream was already bound (either by a previous `StreamOpen`
    /// or by lazy-default insertion from [`Self::enqueue`]), all currently
    /// queued frames are flushed out in send order in the returned `Vec`
    /// and the state is replaced. The caller is expected to emit those
    /// before the next `enqueue` so no in-flight data is lost.
    ///
    /// Returns [`SendPolicyError::TableFull`] if inserting a new stream
    /// would exceed [`MAX_TRACKED_STREAMS`].
    pub fn bind(
        &mut self,
        conn_idx: usize,
        stream_id: StreamId,
        policy: DeliveryPolicy,
    ) -> Result<Vec<FramePayload>, SendPolicyError> {
        let key = (conn_idx, stream_id);

        // Pre-capacity check for new streams (existing ones are a swap).
        let is_new = !self.entries.contains_key(&key);
        if is_new && self.entries.len() >= MAX_TRACKED_STREAMS {
            return Err(SendPolicyError::TableFull {
                cap: MAX_TRACKED_STREAMS,
            });
        }

        // Stop tracking as "default" — this stream now has an explicit policy.
        self.warned_default.remove(&key);
        // The flushed frames are emitted via the return value; the new
        // state starts clean, so any prior ready marker is stale.
        self.ready.remove(&key);

        let drained = match self.entries.get_mut(&key) {
            Some(existing) => {
                // Upgrade path: flush queued frames from the old state.
                if existing.policy() != policy {
                    tracing::info!(
                        conn_idx,
                        stream_id = stream_id.get(),
                        old = ?existing.policy(),
                        new = ?policy,
                        "upgrading send policy for stream"
                    );
                }
                let mut acc: Vec<FramePayload> = Vec::new();
                existing.flush_for_upgrade(&mut acc);
                // Replace with a fresh state for the new policy.
                let _ = mem::replace(existing, PerStreamSendState::new(policy));
                self.metrics
                    .bind_upgrades
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                acc
            }
            None => {
                self.entries.insert(key, PerStreamSendState::new(policy));
                Vec::new()
            }
        };

        Ok(drained)
    }

    /// Apply the policy for a stream to one outgoing frame. Consumes
    /// `payload`.
    ///
    /// If no `StreamOpen` has been seen yet for `(conn_idx, stream_id)`,
    /// the stream is lazy-inserted as `ReliableOrdered` (the only policy
    /// that never drops) with a one-shot `WARN` per stream. This mirrors
    /// the receive-side contract — if a caller starts enqueueing before
    /// declaring a policy, the safe fallback is FIFO with no drops.
    ///
    /// If the table is at [`MAX_TRACKED_STREAMS`] and this is a brand-new
    /// stream, the frame is dropped silently with a per-table `WARN`.
    /// This bounds both `entries` and `warned_default` and closes the
    /// lazy-default DoS vector.
    #[inline]
    pub fn enqueue(
        &mut self,
        conn_idx: usize,
        stream_id: StreamId,
        payload: FramePayload,
    ) -> EnqueueDecision {
        let seq = payload.seq;
        let key = (conn_idx, stream_id);

        // Lazy-default path: check & warn OUTSIDE the entry() borrow to
        // avoid overlapping mutable borrows of `self`.
        let needs_default = !self.entries.contains_key(&key);
        if needs_default {
            if self.entries.len() >= MAX_TRACKED_STREAMS {
                tracing::warn!(
                    conn_idx,
                    stream_id = stream_id.get(),
                    seq = seq.get(),
                    tracked = self.entries.len(),
                    cap = MAX_TRACKED_STREAMS,
                    "send-policy table full — dropping frame for unbound stream"
                );
                self.metrics
                    .frames_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return EnqueueDecision::Drop;
            }
            if self.warned_default.insert(key) {
                tracing::warn!(
                    conn_idx,
                    stream_id = stream_id.get(),
                    seq = seq.get(),
                    "enqueue before StreamOpen — defaulting to ReliableOrdered"
                );
                self.metrics
                    .lazy_default_inserts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        let state = self
            .entries
            .entry(key)
            .or_insert_with(|| PerStreamSendState::new(DeliveryPolicy::ReliableOrdered));
        let decision = state.enqueue(payload);

        // Maintain the ready index. `enqueue` can only transition a stream
        // false → true (it never empties the queue), so we skip the
        // symmetric remove here — stale markers are cleaned by `dequeue`,
        // `bind` (which flushes), and `remove_stream` / `remove_connection`.
        if state.is_ready() {
            self.ready.insert(key);
        }

        use std::sync::atomic::Ordering;
        match &decision {
            EnqueueDecision::Queue => {
                self.metrics.frames_queued.fetch_add(1, Ordering::Relaxed);
            }
            EnqueueDecision::Replace(_) => {
                self.metrics.frames_replaced.fetch_add(1, Ordering::Relaxed);
            }
            EnqueueDecision::Drop => {
                self.metrics.frames_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        decision
    }

    /// Take the next ready frame for a stream. Returns
    /// [`DequeueOutcome::Unknown`] for streams that are not tracked, so
    /// the scheduler can prune its ready set without an extra lookup.
    #[inline]
    pub fn dequeue(&mut self, conn_idx: usize, stream_id: StreamId) -> DequeueOutcome {
        let key = (conn_idx, stream_id);
        let Some(state) = self.entries.get_mut(&key) else {
            return DequeueOutcome::Unknown;
        };
        let outcome = state.dequeue();
        if matches!(outcome, DequeueOutcome::Some(_)) {
            self.metrics
                .frames_dequeued
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if !state.is_ready() {
            self.ready.remove(&key);
        }
        outcome
    }

    /// Snapshot the streams on a given connection that currently have at
    /// least one frame ready for `dequeue`. Returned in unspecified order
    /// — the scheduler is expected to apply its own fairness / priority
    /// policy across the result.
    ///
    /// This is the primary entry point the scheduler calls per tick: it
    /// avoids scanning every tracked stream (which on a hot engine can
    /// be tens of thousands) and instead visits only the ready subset.
    /// The steady state typically has a few ready streams per tick.
    pub fn ready_streams_for_connection(&self, conn_idx: usize, out: &mut Vec<StreamId>) {
        out.extend(
            self.ready
                .iter()
                .copied()
                .filter(|(c, _)| *c == conn_idx)
                .map(|(_, s)| s),
        );
    }

    /// Snapshot every stream that currently has at least one frame ready
    /// for `dequeue`, across all connections. Returned in unspecified
    /// order.
    ///
    /// Use this when the caller drains per-batch (not per-connection),
    /// e.g. a stack-level outbox drain that collects ready frames from
    /// every connection into one outgoing batch. For per-connection
    /// drain, prefer [`Self::ready_streams_for_connection`] which avoids
    /// scanning the full ready set.
    pub fn ready_streams_all(&self, out: &mut Vec<(usize, StreamId)>) {
        out.extend(self.ready.iter().copied());
    }

    /// Number of streams on the given connection that have at least one
    /// ready frame. Cheap O(1)-ish check the scheduler can use to short-
    /// circuit a tick when there is nothing to send.
    #[must_use]
    pub fn ready_count_for_connection(&self, conn_idx: usize) -> usize {
        self.ready.iter().filter(|(c, _)| *c == conn_idx).count()
    }

    /// `true` when `(conn_idx, stream_id)` has an explicit policy bound
    /// (or has been lazy-defaulted by a prior `enqueue`). The stack uses
    /// this to decide whether to route an `OutgoingFrame` through the
    /// policy (returns `true`) or send it directly on the QUIC stream
    /// (returns `false` — control frames, unbound subscriber streams).
    #[inline]
    #[must_use]
    pub fn contains(&self, conn_idx: usize, stream_id: StreamId) -> bool {
        self.entries.contains_key(&(conn_idx, stream_id))
    }

    /// Point-in-time snapshot of the table's observability counters.
    /// Counters are monotonic; the engine typically diffs snapshots
    /// across a window (e.g. per-second) to derive rates.
    #[must_use]
    pub fn metrics(&self) -> crate::metrics::MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Remove a single stream's policy state. Called on `StreamReset`
    /// or `StreamClose`. Idempotent — calling for an unknown stream is
    /// a no-op. Any queued frames are discarded (the receiver is
    /// closing the stream, so further sends would be ignored anyway).
    pub fn remove_stream(&mut self, conn_idx: usize, stream_id: StreamId) {
        let key = (conn_idx, stream_id);
        self.entries.remove(&key);
        self.warned_default.remove(&key);
        self.ready.remove(&key);
    }

    /// Remove all streams for a connection. Called from `cleanup_closed`
    /// / `close_connection` in `quic-engine` when a QUIC connection ends.
    pub fn remove_connection(&mut self, conn_idx: usize) {
        self.entries.retain(|(c, _), _| *c != conn_idx);
        self.warned_default.retain(|(c, _)| *c != conn_idx);
        self.ready.retain(|(c, _)| *c != conn_idx);
    }
}

impl Default for SendPolicyTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;
    use frame::header::{FrameFlags, MessageType, Seq};

    fn payload(seq: u64, byte: u8) -> FramePayload {
        FramePayload::new(
            MessageType::TextToken,
            FrameFlags::NONE,
            Seq::new(seq),
            Bytes::from(vec![byte]),
        )
    }

    #[test]
    fn new_table_is_empty() {
        let t = SendPolicyTable::new();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn bind_then_enqueue_uses_policy() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::RealtimeDropOld)
            .unwrap();

        // First frame queues.
        let d = t.enqueue(0, StreamId::new(1), payload(5, 0xAA));
        assert!(matches!(d, EnqueueDecision::Queue));

        // Older seq drops.
        let d = t.enqueue(0, StreamId::new(1), payload(3, 0xBB));
        assert!(matches!(d, EnqueueDecision::Drop));

        // Newer seq queues.
        let d = t.enqueue(0, StreamId::new(1), payload(9, 0xCC));
        assert!(matches!(d, EnqueueDecision::Queue));
    }

    #[test]
    fn enqueue_without_bind_uses_default_with_warning() {
        let mut t = SendPolicyTable::new();
        // No bind — should lazy-insert ReliableOrdered (FIFO).
        let d = t.enqueue(0, StreamId::new(2), payload(0, 0x11));
        assert!(matches!(d, EnqueueDecision::Queue));
        // Stream should now be tracked.
        assert_eq!(t.len(), 1);
        // Warned-default marker prevents re-warning on next enqueue.
        let d = t.enqueue(0, StreamId::new(2), payload(1, 0x22));
        assert!(matches!(d, EnqueueDecision::Queue));
    }

    #[test]
    fn dequeue_returns_frames_in_fifo_order() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        let _ = t.enqueue(0, StreamId::new(1), payload(0, 1));
        let _ = t.enqueue(0, StreamId::new(1), payload(1, 2));
        let _ = t.enqueue(0, StreamId::new(1), payload(2, 3));

        match t.dequeue(0, StreamId::new(1)) {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[1]),
            _ => panic!("expected seq=0 first"),
        }
        match t.dequeue(0, StreamId::new(1)) {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[2]),
            _ => panic!("expected seq=1 second"),
        }
        match t.dequeue(0, StreamId::new(1)) {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[3]),
            _ => panic!("expected seq=2 third"),
        }
        assert!(matches!(
            t.dequeue(0, StreamId::new(1)),
            DequeueOutcome::Empty
        ));
    }

    #[test]
    fn dequeue_unknown_stream_returns_unknown() {
        let mut t = SendPolicyTable::new();
        assert!(matches!(
            t.dequeue(0, StreamId::new(99)),
            DequeueOutcome::Unknown
        ));
    }

    #[test]
    fn bind_upgrades_state_and_drains_queued() {
        let mut t = SendPolicyTable::new();
        // Enqueue first → triggers default ReliableOrdered (FIFO).
        let _ = t.enqueue(0, StreamId::new(3), payload(0, 1));
        let _ = t.enqueue(0, StreamId::new(3), payload(1, 2));

        // Now StreamOpen arrives upgrading to LatestOnly.
        let drained = t
            .bind(0, StreamId::new(3), DeliveryPolicy::LatestOnly)
            .unwrap();
        // Two frames were queued — they should be drained.
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].bytes.as_ref(), &[1]);
        assert_eq!(drained[1].bytes.as_ref(), &[2]);
    }

    #[test]
    fn remove_stream_is_idempotent() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        t.remove_stream(0, StreamId::new(1));
        t.remove_stream(0, StreamId::new(1)); // no-op
        t.remove_stream(0, StreamId::new(999)); // unknown — no-op
        assert!(t.is_empty());
    }

    #[test]
    fn remove_connection_clears_all_streams_for_conn() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        t.bind(0, StreamId::new(2), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        t.bind(1, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();

        t.remove_connection(0);
        // Conn 0 streams gone; conn 1 stream remains.
        assert_eq!(t.len(), 1);
        let _ = t.enqueue(1, StreamId::new(1), payload(0, 0x00));
    }

    #[test]
    fn ready_streams_skips_idle_streams() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        t.bind(0, StreamId::new(2), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        // Only stream 1 gets a frame.
        let _ = t.enqueue(0, StreamId::new(1), payload(0, 1));

        let mut ready = Vec::new();
        t.ready_streams_for_connection(0, &mut ready);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0], StreamId::new(1));

        assert_eq!(t.ready_count_for_connection(0), 1);
    }

    #[test]
    fn ready_marker_cleared_after_drain() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        let _ = t.enqueue(0, StreamId::new(1), payload(0, 1));

        assert_eq!(t.ready_count_for_connection(0), 1);

        let _ = t.dequeue(0, StreamId::new(1));
        assert_eq!(
            t.ready_count_for_connection(0),
            0,
            "drain should clear ready"
        );
    }

    #[test]
    fn enqueue_caps_warned_default_at_max_tracked_streams() {
        // Fill the table to capacity via explicit binds (one stream each).
        let mut t = SendPolicyTable::new();
        for i in 0..MAX_TRACKED_STREAMS as u64 {
            t.bind(0, StreamId::new(i), DeliveryPolicy::ReliableOrdered)
                .unwrap();
        }
        assert_eq!(t.len(), MAX_TRACKED_STREAMS);

        // A new unbound stream arrives via enqueue → must Drop, must NOT
        // grow `entries` or `warned_default`.
        let before_len = t.len();
        let d = t.enqueue(
            0,
            StreamId::new(MAX_TRACKED_STREAMS as u64 + 1),
            payload(0, 0xEE),
        );
        assert!(matches!(d, EnqueueDecision::Drop));
        assert_eq!(t.len(), before_len, "entries must not grow past cap");

        // An already-tracked stream is unaffected.
        let d = t.enqueue(0, StreamId::new(0), payload(0, 0xFF));
        assert!(matches!(d, EnqueueDecision::Queue));
    }

    #[test]
    fn latest_only_replace_returns_displaced_payload() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::LatestOnly)
            .unwrap();
        let _ = t.enqueue(0, StreamId::new(1), payload(0, 1));
        // Newer replaces older.
        match t.enqueue(0, StreamId::new(1), payload(1, 2)) {
            EnqueueDecision::Replace(p) => assert_eq!(p.bytes.as_ref(), &[1]),
            _ => panic!("expected Replace"),
        }
        // Dequeue the winner.
        match t.dequeue(0, StreamId::new(1)) {
            DequeueOutcome::Some(p) => assert_eq!(p.bytes.as_ref(), &[2]),
            _ => panic!("expected newest"),
        }
    }

    #[test]
    fn metrics_track_enqueue_decisions_and_dequeue() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        let _ = t.enqueue(0, StreamId::new(1), payload(0, 0));
        let _ = t.enqueue(0, StreamId::new(1), payload(1, 1));
        let s = t.metrics();
        assert_eq!(s.frames_queued, 2);
        assert_eq!(s.frames_dequeued, 0);

        let _ = t.dequeue(0, StreamId::new(1));
        let s = t.metrics();
        assert_eq!(s.frames_dequeued, 1);
    }

    #[test]
    fn metrics_track_bind_upgrades_and_lazy_defaults() {
        let mut t = SendPolicyTable::new();
        // Lazy-default via enqueue.
        let _ = t.enqueue(0, StreamId::new(2), payload(0, 0x11));
        let s = t.metrics();
        assert_eq!(s.lazy_default_inserts, 1);
        assert_eq!(s.frames_queued, 1);

        // Upgrade via bind.
        let _ = t
            .bind(0, StreamId::new(2), DeliveryPolicy::LatestOnly)
            .unwrap();
        let s = t.metrics();
        assert_eq!(s.bind_upgrades, 1);
    }

    #[test]
    fn metrics_track_realtime_drops() {
        let mut t = SendPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::RealtimeDropOld)
            .unwrap();
        let _ = t.enqueue(0, StreamId::new(1), payload(0, 0));
        let _ = t.enqueue(0, StreamId::new(1), payload(5, 1));
        // Older than 5 → drop.
        let _ = t.enqueue(0, StreamId::new(1), payload(2, 2));

        let s = t.metrics();
        assert_eq!(s.frames_queued, 2);
        assert_eq!(s.frames_dropped, 1);
    }
}
