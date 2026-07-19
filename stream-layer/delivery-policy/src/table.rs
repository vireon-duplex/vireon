//! [`DeliveryPolicyTable`] — the hot-path API consumed by `quic-engine`.
//!
//! Keyed by `(conn_idx, StreamId)` — the **logical** stream id from the
//! frame header (see [`frame::header::StreamId`]), not the QUIC transport
//! stream id. Multiple logical streams may share one QUIC stream in the
//! future, so the disambiguation must be at the logical layer.

use std::mem;

use frame::header::{Seq, StreamId};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::decision::{Decision, FramePayload};
use crate::error::DeliveryPolicyError;
use crate::policy::DeliveryPolicy;
use crate::state::PerStreamState;

/// Maximum frames buffered in `ReliableOrdered` reorder window.
///
/// Bounds peak memory: 1024 frames × ~16 KiB payload ≈ 16 MiB per stream.
/// When exceeded, the oldest (smallest-`Seq`) buffered frame is evicted
/// with a `WARN` log — this represents accepted data loss for streams
/// with persistent gaps.
pub const MAX_REORDER_WINDOW: usize = 1024;

/// Maximum entries in `ReliableUnordered` dedup set before eviction.
///
/// The dedup window is a sliding set of recently-delivered `Seq`s. When
/// full, the smallest entry is evicted, which may allow a very late
/// duplicate to slip through. This is acceptable: unbounded growth would
/// be a DoS vector.
pub const MAX_DEDUP_SET: usize = 4096;

/// Maximum number of `(conn_idx, stream_id)` pairs the table will track.
///
/// Bounds worst-case memory: 65 536 streams × ~256 B state ≈ 16 MiB.
/// Past this, [`DeliveryPolicyTable::bind`] returns
/// [`DeliveryPolicyError::TableFull`] — the engine logs and drops further
/// `StreamOpen` attempts until existing streams close.
pub const MAX_TRACKED_STREAMS: usize = 65_536;

/// The per-stream policy registry. One instance per `QuicEngine`
/// (i.e. per reactor core).
#[derive(Debug)]
pub struct DeliveryPolicyTable {
    /// `(conn_idx, logical_stream_id)` → state.
    entries: FxHashMap<(usize, StreamId), PerStreamState>,
    /// Streams that have been lazy-defaulted to `ReliableOrdered`
    /// because a data frame arrived before its `StreamOpen`. Each entry
    /// is warned exactly once; removed when `bind` upgrades the stream.
    warned_default: FxHashSet<(usize, StreamId)>,
}

impl DeliveryPolicyTable {
    /// Construct an empty table.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: FxHashMap::default(),
            warned_default: FxHashSet::default(),
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
    /// `MessageType::StreamOpen` frame is decoded.
    ///
    /// If the stream was already bound (either by a previous `StreamOpen`
    /// or by lazy-default insertion from [`Self::apply`]), all currently
    /// buffered frames are flushed out in `Seq` order in the returned
    /// `Vec` and the state is replaced. The engine is expected to emit
    /// those as `FrameReceived` events so no in-flight data is lost.
    ///
    /// Returns [`DeliveryPolicyError::TableFull`] if inserting a new
    /// stream would exceed [`MAX_TRACKED_STREAMS`].
    pub fn bind(
        &mut self,
        conn_idx: usize,
        stream_id: StreamId,
        policy: DeliveryPolicy,
    ) -> Result<Vec<(Seq, FramePayload)>, DeliveryPolicyError> {
        let key = (conn_idx, stream_id);

        // Pre-capacity check for new streams (existing ones are a swap).
        let is_new = !self.entries.contains_key(&key);
        if is_new && self.entries.len() >= MAX_TRACKED_STREAMS {
            return Err(DeliveryPolicyError::TableFull {
                cap: MAX_TRACKED_STREAMS,
            });
        }

        // Stop tracking as "default" — this stream now has an explicit policy.
        self.warned_default.remove(&key);

        let drained = match self.entries.get_mut(&key) {
            Some(existing) => {
                // Upgrade path: flush buffered frames from the old state.
                if existing.policy() != policy {
                    tracing::info!(
                        conn_idx,
                        stream_id = stream_id.get(),
                        old = ?existing.policy(),
                        new = ?policy,
                        "upgrading delivery policy for stream"
                    );
                }
                let mut acc: Vec<(Seq, FramePayload)> = Vec::new();
                existing.flush_for_upgrade(&mut acc);
                // Replace with a fresh state for the new policy.
                let _ = mem::replace(existing, PerStreamState::new(policy));
                acc
            }
            None => {
                self.entries.insert(key, PerStreamState::new(policy));
                Vec::new()
            }
        };

        Ok(drained)
    }

    /// Apply the policy for a stream to one decoded frame. Consumes
    /// `payload`.
    ///
    /// If no `StreamOpen` has been seen yet for `(conn_idx, stream_id)`,
    /// the stream is lazy-inserted as `ReliableOrdered` (the only policy
    /// that never drops) with a one-shot `WARN` per stream.
    ///
    /// If the table is at [`MAX_TRACKED_STREAMS`] and this is a brand-new
    /// stream, the frame is dropped silently with a per-table `WARN`. This
    /// bounds both `entries` and `warned_default` and closes the lazy-default
    /// DoS vector (an attacker flooding data frames with arbitrary stream
    /// ids cannot exhaust memory).
    #[inline]
    pub fn apply(
        &mut self,
        conn_idx: usize,
        stream_id: StreamId,
        seq: Seq,
        payload: FramePayload,
    ) -> Decision {
        let key = (conn_idx, stream_id);

        // Lazy-default path: check & warn OUTSIDE the entry() borrow to
        // avoid overlapping mutable borrows of `self`.
        let needs_default = !self.entries.contains_key(&key);
        if needs_default {
            // Bound both `entries` and `warned_default` — the lazy-default
            // path is reachable without a `StreamOpen`, so an attacker can
            // drive it directly. The cap matches `bind`'s contract.
            if self.entries.len() >= MAX_TRACKED_STREAMS {
                tracing::warn!(
                    conn_idx,
                    stream_id = stream_id.get(),
                    seq = seq.get(),
                    tracked = self.entries.len(),
                    cap = MAX_TRACKED_STREAMS,
                    "delivery-policy table full — dropping frame for unbound stream"
                );
                return Decision::Drop;
            }
            if self.warned_default.insert(key) {
                tracing::warn!(
                    conn_idx,
                    stream_id = stream_id.get(),
                    seq = seq.get(),
                    "data frame arrived before StreamOpen — defaulting to ReliableOrdered"
                );
            }
        }

        let state = self
            .entries
            .entry(key)
            .or_insert_with(|| PerStreamState::new(DeliveryPolicy::ReliableOrdered));
        state.apply(seq, payload)
    }

    /// Drain frames that became deliverable for **any** stream on the
    /// given connection. Appends `(stream_id, seq, payload)` triples in
    /// delivery order to `out`.
    ///
    /// Intended to be called once at the end of a decode batch by
    /// `process_single_stream` (or `process_readable`), so that gap-filled
    /// `ReliableOrdered` frames and pending `LatestOnly` frames are
    /// released to the engine without waiting for the next data arrival.
    pub fn drain_ready_for_connection(
        &mut self,
        conn_idx: usize,
        out: &mut Vec<(StreamId, Seq, FramePayload)>,
    ) {
        // Iterate by raw keys to avoid borrow issues while mutating
        // values. FxHashMap iteration order is unspecified but does not
        // affect correctness here — each stream's drain is independent.
        let keys: Vec<(usize, StreamId)> = self
            .entries
            .keys()
            .copied()
            .filter(|(c, _)| *c == conn_idx)
            .collect();

        let mut scratch: Vec<(Seq, FramePayload)> = Vec::new();
        for key in keys {
            let Some(state) = self.entries.get_mut(&key) else {
                continue;
            };
            scratch.clear();
            state.drain_ready(&mut scratch);
            for (seq, payload) in scratch.drain(..) {
                out.push((key.1, seq, payload));
            }
        }
    }

    /// Remove a single stream's policy state. Called on `StreamReset`
    /// or `StreamClose`. Idempotent — calling for an unknown stream is
    /// a no-op.
    pub fn remove_stream(&mut self, conn_idx: usize, stream_id: StreamId) {
        let key = (conn_idx, stream_id);
        self.entries.remove(&key);
        self.warned_default.remove(&key);
    }

    /// Remove all streams for a connection. Called from `cleanup_closed`
    /// / `close_connection` in `quic-engine` when a QUIC connection ends.
    pub fn remove_connection(&mut self, conn_idx: usize) {
        self.entries.retain(|(c, _), _| *c != conn_idx);
        self.warned_default.retain(|(c, _)| *c != conn_idx);
    }
}

impl Default for DeliveryPolicyTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;
    use frame::header::{FrameFlags, MessageType};

    fn payload(n: u8) -> FramePayload {
        FramePayload::new(
            MessageType::TextToken,
            FrameFlags::NONE,
            Bytes::from(vec![n]),
        )
    }

    #[test]
    fn new_table_is_empty() {
        let t = DeliveryPolicyTable::new();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn bind_then_apply_uses_policy() {
        let mut t = DeliveryPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::RealtimeDropOld)
            .unwrap();

        // First frame delivers.
        let d = t.apply(0, StreamId::new(1), Seq::new(5), payload(0xAA));
        assert!(matches!(d, Decision::Deliver(_)));

        // Older seq drops.
        let d = t.apply(0, StreamId::new(1), Seq::new(3), payload(0xBB));
        assert!(matches!(d, Decision::Drop));

        // Newer seq delivers.
        let d = t.apply(0, StreamId::new(1), Seq::new(9), payload(0xCC));
        assert!(matches!(d, Decision::Deliver(_)));
    }

    #[test]
    fn apply_without_bind_uses_default_with_warning() {
        let mut t = DeliveryPolicyTable::new();
        // No bind — should lazy-insert ReliableOrdered.
        let d = t.apply(0, StreamId::new(2), Seq::new(0), payload(0x11));
        assert!(matches!(d, Decision::Deliver(_)));
        // Stream should now be tracked.
        assert_eq!(t.len(), 1);
        // Warned-default marker prevents re-warning on next apply.
        let d = t.apply(0, StreamId::new(2), Seq::new(1), payload(0x22));
        assert!(matches!(d, Decision::Deliver(_)));
    }

    #[test]
    fn bind_upgrades_state_and_drains_buffered() {
        let mut t = DeliveryPolicyTable::new();
        // Send data frames first → triggers default ReliableOrdered.
        t.apply(0, StreamId::new(3), Seq::new(1), payload(0x01));
        t.apply(0, StreamId::new(3), Seq::new(3), payload(0x03)); // buffered
        t.apply(0, StreamId::new(3), Seq::new(5), payload(0x05)); // buffered

        // Now StreamOpen arrives upgrading to LatestOnly.
        let drained = t
            .bind(0, StreamId::new(3), DeliveryPolicy::LatestOnly)
            .unwrap();
        // Two frames were buffered (seq 3 and 5) — they should be drained.
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].0, Seq::new(3));
        assert_eq!(drained[1].0, Seq::new(5));
    }

    #[test]
    fn remove_stream_is_idempotent() {
        let mut t = DeliveryPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        t.remove_stream(0, StreamId::new(1));
        t.remove_stream(0, StreamId::new(1)); // no-op
        t.remove_stream(0, StreamId::new(999)); // unknown stream — no-op
        assert!(t.is_empty());
    }

    #[test]
    fn remove_connection_clears_all_streams_for_conn() {
        let mut t = DeliveryPolicyTable::new();
        t.bind(0, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        t.bind(0, StreamId::new(2), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        t.bind(1, StreamId::new(1), DeliveryPolicy::ReliableOrdered)
            .unwrap();

        t.remove_connection(0);
        // Conn 0 streams gone; conn 1 stream remains.
        assert_eq!(t.len(), 1);
        t.apply(1, StreamId::new(1), Seq::new(0), payload(0x00));
    }

    #[test]
    fn drain_ready_for_connection_emits_buffered_frames() {
        let mut t = DeliveryPolicyTable::new();
        t.bind(0, StreamId::new(7), DeliveryPolicy::ReliableOrdered)
            .unwrap();
        // Apply seq 1 then 3 (3 is buffered because 2 is missing).
        let d1 = t.apply(0, StreamId::new(7), Seq::new(1), payload(0x01));
        let d3 = t.apply(0, StreamId::new(7), Seq::new(3), payload(0x03));
        // seq=2 fills the gap — it delivers immediately via apply; the
        // previously-buffered seq=3 is released via drain_ready.
        let d2 = t.apply(0, StreamId::new(7), Seq::new(2), payload(0x02));

        assert!(matches!(d1, Decision::Deliver(_)));
        assert!(matches!(d3, Decision::Buffer));
        assert!(matches!(d2, Decision::Deliver(_)));

        let mut out: Vec<(StreamId, Seq, FramePayload)> = Vec::new();
        t.drain_ready_for_connection(0, &mut out);
        // Only seq=3 was held in the reorder window — seq=1 and seq=2
        // were emitted synchronously by apply().
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, Seq::new(3));

        // Second call produces nothing.
        let mut out2: Vec<(StreamId, Seq, FramePayload)> = Vec::new();
        t.drain_ready_for_connection(0, &mut out2);
        assert!(out2.is_empty());
    }

    #[test]
    fn apply_caps_warned_default_at_max_tracked_streams() {
        // Fill the table to capacity via explicit binds (one stream each).
        let mut t = DeliveryPolicyTable::new();
        for i in 0..MAX_TRACKED_STREAMS as u64 {
            t.bind(0, StreamId::new(i), DeliveryPolicy::RealtimeDropOld)
                .unwrap();
        }
        assert_eq!(t.len(), MAX_TRACKED_STREAMS);

        // A new unbound stream arrives via apply → must Drop, must NOT
        // grow `entries` or `warned_default`.
        let before_len = t.len();
        let d = t.apply(
            0,
            StreamId::new(MAX_TRACKED_STREAMS as u64 + 1),
            Seq::new(0),
            payload(0xEE),
        );
        assert!(matches!(d, Decision::Drop));
        assert_eq!(t.len(), before_len, "entries must not grow past cap");

        // An already-tracked stream is unaffected.
        let d = t.apply(0, StreamId::new(0), Seq::new(0), payload(0xFF));
        assert!(matches!(d, Decision::Deliver(_)));
    }
}
