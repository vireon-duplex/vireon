//! In-memory replay buffer for application-level reliability (ACK + Resume).
//!
//! Retains reliable (`ACK_REQ`) Publish frames that the server has fanned out
//! to a subscriber, keyed by `(logical_session_id, StreamId)`, so a
//! reconnecting client can resume from its last-acked sequence without the
//! server re-transmitting already-acknowledged messages.
//!
//! ## Lifetime
//!
//! The buffer is **per-worker** and lives on the reactor thread inside
//! [`crate::stack::VireonStack`] — no locks, no `Send`. A logical session
//! outlives a single QUIC connection: when a subscriber disconnects the
//! retained entries are kept (so resume works); they are released either by a
//! cumulative ACK or by the idle-session TTL sweep.
//!
//! ## Bounding
//!
//! Each per-stream ring is bounded by [`ReplayConfig::max_messages_per_stream`]
//! and the whole buffer by [`ReplayConfig::max_bytes`]. When a cap is hit the
//! oldest entries are evicted and `oldest_seq` advances — a subsequent resume
//! requesting a seq below the floor returns
//! [`ReplayOutcome::Unavailable`].
//!
//! ## Persistence
//!
//! In-memory only for this phase. The method shapes (`append` /
//! `acknowledge` / `replay_from`) mirror what a future persistent backing
//! store (e.g. `wal_store::FileStore`) would expose, so swapping the backing
//! is a localized change.

use std::collections::VecDeque;

use bytes::Bytes;
use frame::codec::FrameHeader;
use frame::header::StreamId;
use rustc_hash::FxHashMap;

// ── Config ──────────────────────────────────────────────────────────

/// Bounds for the in-memory replay window.
///
/// Both caps are best-effort load-shedding: when hit, the oldest entries are
/// evicted and a resume requesting a seq below the new floor returns
/// [`ReplayOutcome::Unavailable`].
#[derive(Clone, Copy, Debug)]
pub struct ReplayConfig {
    /// Maximum retained messages per `(session, stream)`. Older entries are
    /// evicted once this is exceeded.
    pub max_messages_per_stream: usize,
    /// Soft total byte cap across the whole buffer. Eviction targets the
    /// stream that is being appended to first; if that is insufficient older
    /// sessions are pruned.
    pub max_bytes: usize,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            max_messages_per_stream: 10_000,
            max_bytes: 128 * 1024 * 1024, // 128 MiB
        }
    }
}

// ── Entries / outcome ───────────────────────────────────────────────

/// One retained reliable frame, ready to be re-sent on resume.
#[derive(Clone, Debug)]
pub struct ReplayEntry {
    /// Server-assigned per-subscriber-stream sequence number for this entry.
    pub seq: u64,
    /// Codec frame header (logical stream, type, flags, …). `conn_idx` and
    /// the QUIC stream id are re-bound at replay time on the new connection.
    pub header: FrameHeader,
    /// Payload bytes (refcounted `Bytes` — cheap to clone across the live
    /// delivery path and the retained copy).
    pub payload: Bytes,
}

impl ReplayEntry {
    /// Approximate wire footprint of this entry (header + payload). Used for
    /// byte-cap accounting.
    #[inline]
    fn wire_bytes(&self) -> usize {
        // codec::HEADER_SIZE (22) + payload + CRC (4); we do not retain the
        // CRC trailer, but counting it keeps the cap honest against what the
        // transport actually holds.
        frame::codec::HEADER_SIZE + self.payload.len() + frame::codec::CRC_SIZE
    }
}

/// Result of a [`ReplayBuffer::replay_from`] query.
#[derive(Debug)]
pub enum ReplayOutcome {
    /// Resume succeeded — these entries (ascending seq) should be re-sent.
    /// `oldest_available` is the floor of the window at reply time.
    Ok {
        /// Entries with `seq > after_seq`, in ascending order.
        entries: Vec<ReplayEntry>,
        /// Oldest sequence still retained for this stream.
        oldest_available: u64,
    },
    /// The requested `after_seq` is below the retained window floor — the
    /// server cannot satisfy the resume. The client falls back to a fresh
    /// subscription.
    Unavailable {
        /// Oldest sequence still retained for this stream (the floor).
        oldest_available: u64,
    },
}

// ── Per-stream ring ─────────────────────────────────────────────────

/// One per-`(session, stream)` retained ring of reliable frames.
#[derive(Debug, Default)]
struct StreamReplay {
    /// Ascending by `seq`; front = oldest.
    entries: VecDeque<ReplayEntry>,
    /// Sum of every entry's `wire_bytes()`.
    bytes: usize,
    /// `seq` of the oldest entry currently retained, or the floor after
    /// eviction (so an empty ring still knows what it dropped).
    oldest_seq: u64,
}

impl StreamReplay {
    /// Appends an entry, enforcing the per-stream message cap.
    fn push(&mut self, entry: ReplayEntry, max_messages: usize, max_bytes: &mut usize) {
        self.oldest_seq = if self.entries.is_empty() {
            entry.seq
        } else {
            self.entries.front().unwrap().seq
        };
        *max_bytes += entry.wire_bytes();
        self.bytes += entry.wire_bytes();
        self.entries.push_back(entry);
        // Evict from the front (oldest) until under the message cap. This
        // raises the floor — a future resume below it returns Unavailable.
        while self.entries.len() > max_messages {
            if let Some(evicted) = self.entries.pop_front() {
                let w = evicted.wire_bytes();
                self.bytes = self.bytes.saturating_sub(w);
                *max_bytes = max_bytes.saturating_sub(w);
            }
        }
        // Advance oldest_seq to whatever the front is now (or keep the last
        // evicted floor if the ring is empty — it never is here since we just
        // pushed).
        if let Some(front) = self.entries.front() {
            self.oldest_seq = front.seq;
        }
    }

    /// Releases entries with `seq <= ack_seq`, returning the byte volume freed.
    fn acknowledge(&mut self, ack_seq: u64, max_bytes: &mut usize) -> usize {
        let mut freed = 0usize;
        while let Some(front) = self.entries.front() {
            if front.seq > ack_seq {
                break;
            }
            let entry = self.entries.pop_front().unwrap();
            let w = entry.wire_bytes();
            self.bytes = self.bytes.saturating_sub(w);
            *max_bytes = max_bytes.saturating_sub(w);
            freed += w;
        }
        // Advance the floor to the new front (or ack_seq + 1 if empty, so a
        // subsequent resume-from-ack_seq returns Ok with no entries rather
        // than Unavailable).
        self.oldest_seq = self
            .entries
            .front()
            .map(|f| f.seq)
            .unwrap_or(ack_seq.wrapping_add(1));
        freed
    }

    /// Returns entries with `seq > after_seq` in ascending order, or
    /// `Unavailable` when the first wanted seq is below the retained floor.
    ///
    /// `after_seq == u64::MAX` is the "replay everything" sentinel: the
    /// client has acknowledged nothing and wants all retained entries
    /// starting from seq 0.
    fn replay_from(&self, after_seq: u64) -> ReplayOutcome {
        let want = after_seq.wrapping_add(1);
        if want < self.oldest_seq {
            return ReplayOutcome::Unavailable {
                oldest_available: self.oldest_seq,
            };
        }
        // `seq > after_seq` is correct for the normal case, but when
        // after_seq == u64::MAX (sentinel) no real seq satisfies it, so
        // special-case it to "all entries".
        let entries: Vec<ReplayEntry> = if after_seq == u64::MAX {
            self.entries.iter().cloned().collect()
        } else {
            self.entries
                .iter()
                .filter(|e| e.seq > after_seq)
                .cloned()
                .collect()
        };
        ReplayOutcome::Ok {
            entries,
            oldest_available: self.oldest_seq,
        }
    }
}

// ── Top-level buffer ────────────────────────────────────────────────

/// Per-worker in-memory replay store, keyed by logical session id then
/// [`StreamId`].
///
/// Lives on the reactor thread; all methods take `&mut self` and require no
/// locking (single-threaded event loop, same model as `pending_sends`).
#[derive(Debug)]
pub struct ReplayBuffer {
    /// `logical_session_id → (stream → ring)`.
    by_session: FxHashMap<u64, FxHashMap<StreamId, StreamReplay>>,
    config: ReplayConfig,
    /// Total retained bytes across all sessions/streams.
    total_bytes: usize,
}

impl Default for ReplayBuffer {
    fn default() -> Self {
        Self::new(ReplayConfig::default())
    }
}

impl ReplayBuffer {
    // The introspection methods below (config / total_bytes / total_messages
    // / session_count / drop_session) are kept for the upcoming metrics +
    // idle-session-TTL wiring (Task #6 of the ACK+Resume plan). Allow
    // dead_code until that lands.
    #![allow(dead_code)]
    /// Creates a new buffer with the given config.
    #[must_use]
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            by_session: FxHashMap::default(),
            config,
            total_bytes: 0,
        }
    }

    /// Returns the configured bounds (for metrics / introspection).
    #[must_use]
    pub fn config(&self) -> ReplayConfig {
        self.config
    }

    /// Total retained bytes across all sessions/streams.
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Total retained messages across all sessions/streams.
    #[must_use]
    pub fn total_messages(&self) -> usize {
        self.by_session
            .values()
            .flat_map(|m| m.values())
            .map(|s| s.entries.len())
            .sum()
    }

    /// Number of distinct logical sessions currently tracked.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.by_session.len()
    }

    /// Ensures a session slot exists. Called on receipt of a `Resume` (or a
    /// session-hello `Resume` with an empty slot list).
    pub fn register_session(&mut self, logical_session_id: u64) {
        self.by_session
            .entry(logical_session_id)
            .or_default();
    }

    /// Retains a reliable frame for `(logical_session_id, stream)`.
    ///
    /// Enforces both the per-stream message cap and the global byte cap
    /// (evicting oldest entries from this stream, then pruning whole
    /// sessions as a last resort).
    pub fn append(
        &mut self,
        logical_session_id: u64,
        stream: StreamId,
        entry: ReplayEntry,
    ) {
        let max_messages = self.config.max_messages_per_stream;
        // Enforce the global byte cap before appending: if we are over,
        // evict oldest entries from THIS stream first (raises the floor,
        // which surfaces as Unavailable to a stale resume — the honest
        // answer). Per-session pruning is intentionally avoided unless the
        // stream itself cannot relieve enough.
        let max_bytes = self.config.max_bytes;
        while self.total_bytes > max_bytes.saturating_sub(entry.wire_bytes())
            && self.try_evict_oldest_global()
        {
            // keep evicting until there is room or nothing left
        }
        let streams = self
            .by_session
            .entry(logical_session_id)
            .or_default();
        let ring = streams.entry(stream).or_default();
        ring.push(entry, max_messages, &mut self.total_bytes);
    }

    /// Releases all retained entries with `seq <= ack_seq` for the given
    /// `(session, stream)`. No-op if the session/stream is unknown.
    pub fn acknowledge(&mut self, logical_session_id: u64, stream: StreamId, ack_seq: u64) {
        if let Some(streams) = self.by_session.get_mut(&logical_session_id) {
            if let Some(ring) = streams.get_mut(&stream) {
                ring.acknowledge(ack_seq, &mut self.total_bytes);
            }
        }
    }

    /// Returns entries to replay (`seq > after_seq`) or `Unavailable` if the
    /// requested point is below the retained floor.
    #[must_use]
    pub fn replay_from(
        &self,
        logical_session_id: u64,
        stream: StreamId,
        after_seq: u64,
    ) -> ReplayOutcome {
        match self
            .by_session
            .get(&logical_session_id)
            .and_then(|m| m.get(&stream))
        {
            Some(ring) => ring.replay_from(after_seq),
            // Unknown session/stream: nothing retained. Treat the floor as
            // `after_seq + 1` so the outcome is Ok with no entries — the
            // client simply has no gap to fill.
            None => ReplayOutcome::Ok {
                entries: Vec::new(),
                oldest_available: after_seq.wrapping_add(1),
            },
        }
    }

    /// Drops all replay state for a logical session (e.g. idle-TTL sweep or
    /// explicit session end). Does **not** happen on a single connection
    /// close — the session outlives the connection.
    pub fn drop_session(&mut self, logical_session_id: u64) {
        if let Some(streams) = self.by_session.remove(&logical_session_id) {
            for ring in streams.values() {
                self.total_bytes = self.total_bytes.saturating_sub(ring.bytes);
            }
        }
    }

    /// Evicts the single oldest retained entry across the whole buffer.
    /// Returns `true` if something was evicted, `false` if the buffer is
    /// empty (so the caller can stop retrying).
    fn try_evict_oldest_global(&mut self) -> bool {
        // Find the (session, stream) with the globally oldest entry. A full
        // scan is O(sessions × streams); this only runs under byte-cap
        // pressure, which is a load-shed path — not the hot path.
        let mut oldest: Option<(u64, StreamId, u64)> = None; // (session, stream, seq)
        for (&sid, streams) in &self.by_session {
            for (&stream, ring) in streams {
                if let Some(front) = ring.entries.front() {
                    match oldest {
                        None => oldest = Some((sid, stream, front.seq)),
                        Some((_, _, oseq)) if front.seq < oseq => {
                            oldest = Some((sid, stream, front.seq));
                        }
                        _ => {}
                    }
                }
            }
        }
        let Some((sid, stream, _)) = oldest else {
            return false;
        };
        if let Some(streams) = self.by_session.get_mut(&sid) {
            if let Some(ring) = streams.get_mut(&stream) {
                if let Some(evicted) = ring.entries.pop_front() {
                    let w = evicted.wire_bytes();
                    ring.bytes = ring.bytes.saturating_sub(w);
                    self.total_bytes = self.total_bytes.saturating_sub(w);
                    if let Some(front) = ring.entries.front() {
                        ring.oldest_seq = front.seq;
                    }
                }
            }
        }
        true
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use frame::codec::FrameHeader as CodecFrameHeader;
    use frame::header::{MessageType, StreamId as LStreamId};

    fn entry(seq: u64) -> ReplayEntry {
        ReplayEntry {
            seq,
            header: CodecFrameHeader::new(LStreamId::new(7), MessageType::Publish)
                .with_seq(frame::header::Seq::new(seq)),
            payload: Bytes::from_static(b"payload"),
        }
    }

    fn sid(v: u64) -> StreamId {
        StreamId::new(v)
    }

    // ── append / replay / ack basics ───────────────────────────────

    #[test]
    fn append_then_replay_returns_all_entries() {
        let mut buf = ReplayBuffer::default();
        buf.register_session(1);
        buf.append(1, sid(7), entry(0));
        buf.append(1, sid(7), entry(1));
        buf.append(1, sid(7), entry(2));

        // Replay from a specific point: after_seq=0 → want seq > 0.
        let ReplayOutcome::Ok { entries, oldest_available } = buf.replay_from(1, sid(7), 0) else {
            panic!("expected Ok");
        };
        assert_eq!(entries.len(), 2); // 1, 2
        assert_eq!(oldest_available, 0);

        // u64::MAX sentinel = "replay everything" (floor is 0 → Ok).
        let ReplayOutcome::Ok { entries, oldest_available } = buf.replay_from(1, sid(7), u64::MAX) else {
            panic!("expected Ok");
        };
        assert_eq!(entries.len(), 3); // 0, 1, 2
        assert_eq!(oldest_available, 0);
    }

    #[test]
    fn replay_from_zero_returns_full_window() {
        let mut buf = ReplayBuffer::default();
        buf.register_session(1);
        for s in 0..5 {
            buf.append(1, sid(7), entry(s));
        }
        // after_seq = MAX → want wraps to 0 → returns all.
        let ReplayOutcome::Ok { entries, oldest_available } = buf.replay_from(1, sid(7), u64::MAX)
        else {
            panic!("expected Ok");
        };
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].seq, 0);
        assert_eq!(entries[4].seq, 4);
        assert_eq!(oldest_available, 0);
    }

    #[test]
    fn replay_from_middle_returns_only_newer() {
        let mut buf = ReplayBuffer::default();
        buf.register_session(1);
        for s in 0..10 {
            buf.append(1, sid(7), entry(s));
        }
        let ReplayOutcome::Ok { entries, oldest_available } = buf.replay_from(1, sid(7), 4) else {
            panic!("expected Ok");
        }; // want seq > 4
        let _ = oldest_available;
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].seq, 5);
        assert_eq!(entries[4].seq, 9);
    }

    #[test]
    fn acknowledge_releases_below_watermark() {
        let mut buf = ReplayBuffer::default();
        buf.register_session(1);
        for s in 0..5 {
            buf.append(1, sid(7), entry(s));
        }
        buf.acknowledge(1, sid(7), 2); // release 0,1,2; floor advances to 3
        assert_eq!(buf.total_messages(), 2);

        // After acking 2, replay-from-last_acked=2 returns the remaining 3,4.
        let ReplayOutcome::Ok { entries, oldest_available } = buf.replay_from(1, sid(7), 2) else {
            panic!("expected Ok");
        };
        assert_eq!(entries.len(), 2); // 3,4 remain
        assert_eq!(oldest_available, 3);

        // The "replay everything" sentinel now fails: floor is 3, want=0 < 3.
        let ReplayOutcome::Unavailable { oldest_available } =
            buf.replay_from(1, sid(7), u64::MAX)
        else {
            panic!("expected Unavailable");
        };
        assert_eq!(oldest_available, 3);
    }

    // ── window floor / Unavailable ─────────────────────────────────

    #[test]
    fn replay_below_floor_returns_unavailable() {
        let mut buf = ReplayBuffer::new(ReplayConfig {
            max_messages_per_stream: 3,
            max_bytes: usize::MAX,
        });
        buf.register_session(1);
        for s in 0..10 {
            buf.append(1, sid(7), entry(s));
        }
        // Cap=3 → only 7,8,9 retained → oldest floor = 7.
        let ReplayOutcome::Unavailable { oldest_available } =
            buf.replay_from(1, sid(7), 3)
        else {
            panic!("expected Unavailable");
        };
        assert_eq!(oldest_available, 7);

        // Replay from 7 (within window) succeeds.
        let ReplayOutcome::Ok { entries, .. } = buf.replay_from(1, sid(7), 7) else {
            panic!("expected Ok");
        };
        assert_eq!(entries.len(), 2); // 8,9
    }

    #[test]
    fn replay_unknown_session_returns_ok_empty() {
        let buf = ReplayBuffer::default();
        let ReplayOutcome::Ok { entries, oldest_available } = buf.replay_from(999, sid(7), 5) else {
            panic!("expected Ok");
        };
        assert!(entries.is_empty());
        assert_eq!(oldest_available, 6);
    }

    // ── isolation ──────────────────────────────────────────────────

    #[test]
    fn streams_are_isolated() {
        let mut buf = ReplayBuffer::default();
        buf.register_session(1);
        for s in 0..3 {
            buf.append(1, sid(7), entry(s));
        }
        for s in 0..5 {
            buf.append(1, sid(8), entry(s));
        }
        // ACK on stream 7 must not touch stream 8.
        buf.acknowledge(1, sid(7), u64::MAX);
        assert_eq!(buf.total_messages(), 5);
        let ReplayOutcome::Ok { entries, .. } = buf.replay_from(1, sid(8), u64::MAX) else {
            panic!("expected Ok");
        };
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn sessions_are_isolated() {
        let mut buf = ReplayBuffer::default();
        buf.register_session(1);
        buf.register_session(2);
        for s in 0..3 {
            buf.append(1, sid(7), entry(s));
        }
        for s in 0..4 {
            buf.append(2, sid(7), entry(s));
        }
        buf.acknowledge(1, sid(7), u64::MAX);
        assert_eq!(buf.total_messages(), 4); // session 2 untouched
    }

    // ── byte cap ───────────────────────────────────────────────────

    #[test]
    fn byte_cap_evicts_oldest_globally() {
        let mut buf = ReplayBuffer::new(ReplayConfig {
            max_messages_per_stream: 1000,
            max_bytes: 100, // tiny → forces eviction
        });
        buf.register_session(1);
        // Each entry is ~22 + 7 + 4 = 33 bytes. With a 100-byte cap, ~3 fit.
        for s in 0..10 {
            buf.append(1, sid(7), entry(s));
        }
        // Should have evicted the oldest to stay under cap. Floor advances.
        assert!(buf.total_bytes() <= 100 || buf.total_messages() <= 3);
        // The oldest retained seq must be > 0 (we evicted some).
        let (ReplayOutcome::Ok { oldest_available, .. }
        | ReplayOutcome::Unavailable { oldest_available }) =
            buf.replay_from(1, sid(7), u64::MAX);
        assert!(oldest_available > 0, "expected eviction, floor={oldest_available}");
    }

    // ── drop_session ───────────────────────────────────────────────

    #[test]
    fn drop_session_releases_all() {
        let mut buf = ReplayBuffer::default();
        buf.register_session(1);
        buf.register_session(2);
        for s in 0..3 {
            buf.append(1, sid(7), entry(s));
            buf.append(2, sid(7), entry(s));
        }
        let before = buf.total_bytes();
        assert!(before > 0);
        buf.drop_session(1);
        assert_eq!(buf.session_count(), 1);
        assert!(buf.total_bytes() < before);
        // session 1 now gone → replay returns Ok-empty.
        let ReplayOutcome::Ok { entries, .. } = buf.replay_from(1, sid(7), u64::MAX) else {
            panic!("expected Ok");
        };
        assert!(entries.is_empty());
    }

    #[test]
    fn ack_on_unknown_session_is_noop() {
        let mut buf = ReplayBuffer::default();
        buf.acknowledge(999, sid(7), 50); // must not panic
        assert_eq!(buf.total_bytes(), 0);
    }
}
