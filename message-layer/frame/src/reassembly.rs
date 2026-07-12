//! Fragment reassembly — reconstructs messages split across multiple
//! [`MessageType::Fragment`] frames.
//!
//! When a logical message is too large for a single frame (or when a sender
//! streams data in chunks), it is split into `Fragment` frames. Each fragment
//! carries a piece of the original payload and shares the same
//! `(session_id, stream_id)`. The [`Seq`] field provides ordering, and
//! [`FrameFlags::FIN`] on the last fragment signals end-of-chain.
//!
//! [`FragmentReassembler`] collects fragments, orders them by `seq`, and
//! returns the concatenated payload when the chain completes.
//!
//! # Example
//!
//! ```
//! use frame::reassembly::FragmentReassembler;
//! use frame::codec::FrameRef;
//! use frame::header::{FrameFlags, MessageType, Seq, StreamId};
//!
//! let mut reassembler = FragmentReassembler::new();
//!
//! // Fragment 1 (seq=0, no FIN)
//! let f1 = FrameRef {
//!     stream_id: StreamId::new(2),
//!     seq: Seq::new(0),
//!     msg_type: MessageType::Fragment,
//!     flags: FrameFlags::FRAGMENT,
//!     payload: b"Hello ",
//! };
//! assert!(reassembler.on_fragment(&f1).unwrap().is_none());
//!
//! // Fragment 2 (seq=1, FIN set → chain complete)
//! let f2 = FrameRef {
//!     stream_id: StreamId::new(2),
//!     seq: Seq::new(1),
//!     msg_type: MessageType::Fragment,
//!     flags: FrameFlags::FRAGMENT | FrameFlags::FIN,
//!     payload: b"World!",
//! };
//! let result = reassembler.on_fragment(&f2).unwrap().unwrap();
//! assert_eq!(result.as_ref(), b"Hello World!");
//! ```

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::codec::FrameRef;
use crate::header::{FrameFlags, MessageType, StreamId};

// ── Constants ───────────────────────────────────────────────────────

/// Default maximum bytes buffered per stream before rejecting fragments.
/// 16 MiB — large enough for realistic multi-fragment messages.
const DEFAULT_MAX_BYTES_PER_STREAM: usize = 16 * 1024 * 1024;

/// Default maximum number of concurrent pending reassemblies.
const DEFAULT_MAX_PENDING_ENTRIES: usize = 4096;

// ── Error type ──────────────────────────────────────────────────────

/// Errors returned by [`FragmentReassembler::on_fragment`].
#[derive(Debug, thiserror::Error)]
pub enum ReassemblyError {
    /// A fragment's payload would push the per-stream buffer past the
    /// configured limit. The fragment is rejected; the pending entry is
    /// **not** removed (the caller may decide to evict or keep it).
    #[error("fragment payload exceeds per-stream limit ({max}): {actual} bytes")]
    StreamLimitExceeded {
        /// The configured per-stream byte limit.
        max: usize,
        /// The total bytes that would be buffered (existing + new fragment).
        actual: usize,
    },

    /// The number of concurrent pending reassemblies exceeds the configured
    /// cap. The fragment is rejected.
    #[error("too many concurrent pending reassemblies ({max})")]
    TooManyPending {
        /// The configured maximum pending entry count.
        max: usize,
    },

    /// A fragment with the same `seq` was already received for this stream.
    /// The duplicate is rejected; the existing entry is unchanged.
    #[error("duplicate fragment seq {seq} on stream {stream_id}")]
    DuplicateFragment {
        /// The duplicate sequence number.
        seq: u64,
        /// Stream ID of the stream.
        stream_id: u64,
    },
}

// ── Pending reassembly ──────────────────────────────────────────────

/// In-progress reassembly state for one `(session_id, stream_id)`.
struct PendingReassembly {
    /// Fragment payloads keyed by `seq` — `BTreeMap` keeps them sorted.
    /// Uses `Vec<u8>` instead of `Bytes` to avoid per-fragment `Arc`
    /// allocation. `Bytes` (with its reference-counted `Arc<[u8]>`)
    /// is only created once at reassembly time.
    fragments: BTreeMap<u64, Vec<u8>>,
    /// Total bytes accumulated across all fragments.
    total_bytes: usize,
    /// When the first fragment arrived — for timeout eviction.
    started_at: Instant,
}

impl PendingReassembly {
    fn new(now: Instant) -> Self {
        Self {
            fragments: BTreeMap::new(),
            total_bytes: 0,
            started_at: now,
        }
    }

    /// Adds a fragment's payload. Returns `Err` on duplicate seq.
    fn insert(&mut self, seq: u64, payload: Vec<u8>) -> Result<(), ReassemblyError> {
        if self.fragments.contains_key(&seq) {
            return Err(ReassemblyError::DuplicateFragment { seq, stream_id: 0 });
        }
        self.total_bytes += payload.len();
        self.fragments.insert(seq, payload);
        Ok(())
    }

    /// Concatenates all fragment payloads in seq order, consuming `self`.
    fn reassemble(self) -> Bytes {
        if self.fragments.len() == 1 {
            // Single fragment — wrap in Bytes (one Arc alloc instead of N).
            return Bytes::from(self.fragments.into_values().next().unwrap_or_default());
        }
        let mut buf = Vec::with_capacity(self.total_bytes);
        for payload in self.fragments.values() {
            buf.extend_from_slice(payload);
        }
        Bytes::from(buf)
    }
}

// ── FragmentReassembler ─────────────────────────────────────────────

/// Collects [`MessageType::Fragment`] frames and reassembles them into
/// complete payloads.
///
/// Fragments are correlated by [`StreamId`] and ordered by [`Seq`]. The
/// chain is considered complete when a fragment with [`FrameFlags::FIN`]
/// is received.
///
/// # DoS protection
///
/// Two limits prevent unbounded memory growth:
/// - `max_bytes_per_stream`: total bytes buffered per stream (default 16 MiB)
/// - `max_pending_entries`: number of concurrent incomplete chains (default 4096)
///
/// Stale incomplete chains can be evicted via [`evict_timeouts`](Self::evict_timeouts).
pub struct FragmentReassembler {
    /// Pending reassemblies keyed by `stream_id`.
    pending: HashMap<StreamId, PendingReassembly>,
    /// Maximum bytes buffered per individual stream.
    max_bytes_per_stream: usize,
    /// Maximum number of concurrent pending entries.
    max_pending_entries: usize,
    /// Total bytes across all pending entries.
    total_pending_bytes: usize,
}

impl Default for FragmentReassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl FragmentReassembler {
    /// Creates a reassembler with default limits
    /// (16 MiB/stream, 4096 pending entries).
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_BYTES_PER_STREAM, DEFAULT_MAX_PENDING_ENTRIES)
    }

    /// Creates a reassembler with custom DoS limits.
    ///
    /// # Panics
    ///
    /// Debug-mode assertion if either limit is zero.
    #[must_use]
    pub fn with_limits(max_bytes_per_stream: usize, max_pending_entries: usize) -> Self {
        debug_assert!(max_bytes_per_stream > 0);
        debug_assert!(max_pending_entries > 0);
        Self {
            pending: HashMap::new(),
            max_bytes_per_stream,
            max_pending_entries,
            total_pending_bytes: 0,
        }
    }

    /// Feeds a frame to the reassembler.
    ///
    /// - If `frame.msg_type != Fragment`, returns `Ok(None)` (ignored).
    /// - If the fragment is accepted but the chain is not yet complete,
    ///   returns `Ok(None)`.
    /// - If the fragment completes the chain (`FIN` flag seen), returns
    ///   `Ok(Some(Bytes))` containing the concatenated payload.
    ///
    /// # Errors
    ///
    /// Returns [`ReassemblyError`] if the fragment exceeds the per-stream
    /// byte limit, the pending-entry cap, or is a duplicate seq.
    pub fn on_fragment(&mut self, frame: &FrameRef<'_>) -> Result<Option<Bytes>, ReassemblyError> {
        // Ignore non-Fragment frames.
        if frame.msg_type != MessageType::Fragment {
            return Ok(None);
        }

        let key = frame.stream_id;
        let seq = frame.seq.get();
        let is_fin = frame.flags.contains(FrameFlags::FIN);
        let payload = frame.payload.to_vec();

        // Check per-stream byte limit BEFORE inserting.
        let existing_bytes = self.pending.get(&key).map_or(0, |p| p.total_bytes);
        let new_total = existing_bytes + payload.len();
        if new_total > self.max_bytes_per_stream {
            return Err(ReassemblyError::StreamLimitExceeded {
                max: self.max_bytes_per_stream,
                actual: new_total,
            });
        }

        // Check pending-entry cap (only for new entries).
        let is_new_entry = !self.pending.contains_key(&key);
        if is_new_entry && self.pending.len() >= self.max_pending_entries {
            return Err(ReassemblyError::TooManyPending {
                max: self.max_pending_entries,
            });
        }

        // Insert the fragment.
        let pending = self
            .pending
            .entry(key)
            .or_insert_with(|| PendingReassembly::new(Instant::now()));

        if let Err(mut e) = pending.insert(seq, payload) {
            if let ReassemblyError::DuplicateFragment { stream_id, .. } = &mut e {
                *stream_id = key.get();
            }
            return Err(e);
        }
        self.total_pending_bytes += pending.fragments.get(&seq).map_or(0, |b| b.len());

        // Check completion.
        if is_fin {
            if let Some(p) = self.pending.remove(&key) {
                self.total_pending_bytes -= p.total_bytes;
                let reassembled = p.reassemble();
                return Ok(Some(reassembled));
            }
        }

        Ok(None)
    }

    /// Evicts pending reassemblies that have been inactive longer than
    /// `timeout`. Returns the number of entries evicted.
    ///
    /// Call this periodically (e.g. once per second from a timer tick) to
    /// reclaim memory from abandoned fragment chains.
    pub fn evict_timeouts(&mut self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut evicted = 0;
        self.pending.retain(|_, p| {
            let stale = now.duration_since(p.started_at) > timeout;
            if stale {
                self.total_pending_bytes -= p.total_bytes;
                evicted += 1;
                false
            } else {
                true
            }
        });
        evicted
    }

    /// Number of pending (incomplete) reassemblies.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Total bytes currently buffered across all pending reassemblies.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.total_pending_bytes
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use crate::codec::FrameRef;
    use crate::header::{FrameFlags, MessageType, Seq, StreamId};

    /// Helper: build a Fragment FrameRef with the given fields.
    fn frag(stream: u64, seq: u64, payload: &[u8], fin: bool) -> FrameRef<'_> {
        let mut flags = FrameFlags::FRAGMENT;
        if fin {
            flags |= FrameFlags::FIN;
        }
        FrameRef {
            stream_id: StreamId::new(stream),
            seq: Seq::new(seq),
            msg_type: MessageType::Fragment,
            flags,
            payload,
        }
    }

    #[test]
    fn basic_two_fragment_chain() {
        let mut r = FragmentReassembler::new();

        let f1 = frag(2, 0, b"Hello ", false);
        assert!(r.on_fragment(&f1).unwrap().is_none());

        let f2 = frag(2, 1, b"World!", true);
        let result = r.on_fragment(&f2).unwrap().unwrap();

        assert_eq!(result.as_ref(), b"Hello World!");
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn three_fragment_chain() {
        let mut r = FragmentReassembler::new();

        let f1 = frag(1, 0, b"AAA", false);
        let f2 = frag(1, 1, b"BBB", false);
        let f3 = frag(1, 2, b"CCC", true);

        assert!(r.on_fragment(&f1).unwrap().is_none());
        assert!(r.on_fragment(&f2).unwrap().is_none());
        let result = r.on_fragment(&f3).unwrap().unwrap();

        assert_eq!(result.as_ref(), b"AAABBBCCC");
    }

    #[test]
    fn fin_completes_even_with_gaps() {
        let mut r = FragmentReassembler::new();

        // FIN arrives first (seq=2), but seq 0 and 1 haven't arrived yet.
        // FIN triggers immediate completion — the reassembler returns
        // whatever fragments it has, ordered by seq.
        let f3 = frag(1, 2, b"CCC", true);
        let result = r.on_fragment(&f3).unwrap().unwrap();
        assert_eq!(result.as_ref(), b"CCC");
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn out_of_order_all_arrive_before_fin() {
        let mut r = FragmentReassembler::new();

        // Fragments arrive out of order, FIN arrives last
        let f2 = frag(1, 1, b"BBB", false);
        let f1 = frag(1, 0, b"AAA", false);
        let f3 = frag(1, 2, b"CCC", true);

        assert!(r.on_fragment(&f2).unwrap().is_none());
        assert!(r.on_fragment(&f1).unwrap().is_none());
        let result = r.on_fragment(&f3).unwrap().unwrap();

        // BTreeMap orders by seq → correct concatenation
        assert_eq!(result.as_ref(), b"AAABBBCCC");
    }

    #[test]
    fn duplicate_seq_rejected() {
        let mut r = FragmentReassembler::new();

        let f1 = frag(1, 0, b"AAA", false);
        assert!(r.on_fragment(&f1).unwrap().is_none());

        let dup = frag(1, 0, b"XYZ", false);
        let err = r.on_fragment(&dup).unwrap_err();
        assert!(matches!(
            err,
            ReassemblyError::DuplicateFragment {
                seq: 0,
                stream_id: 1
            }
        ));

        // Original entry unchanged
        assert_eq!(r.pending_count(), 1);
    }

    #[test]
    fn multiple_concurrent_streams() {
        let mut r = FragmentReassembler::new();

        let s1_f1 = frag(10, 0, b"A", false);
        let s2_f1 = frag(20, 0, b"B", false);
        let s1_f2 = frag(10, 1, b"C", true);
        let s2_f2 = frag(20, 1, b"D", true);

        r.on_fragment(&s1_f1).unwrap();
        r.on_fragment(&s2_f1).unwrap();
        assert_eq!(r.pending_count(), 2);

        let r1 = r.on_fragment(&s1_f2).unwrap().unwrap();
        assert_eq!(r1.as_ref(), b"AC");
        assert_eq!(r.pending_count(), 1);

        let r2 = r.on_fragment(&s2_f2).unwrap().unwrap();
        assert_eq!(r2.as_ref(), b"BD");
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn per_stream_byte_limit_enforced() {
        let mut r = FragmentReassembler::with_limits(10, 100);

        let f1 = frag(1, 0, &[0u8; 6], false); // 6 bytes
        assert!(r.on_fragment(&f1).unwrap().is_none());

        let f2 = frag(1, 1, &[0u8; 5], false); // 6 + 5 = 11 > 10
        let err = r.on_fragment(&f2).unwrap_err();
        assert!(matches!(
            err,
            ReassemblyError::StreamLimitExceeded {
                max: 10,
                actual: 11
            }
        ));
    }

    #[test]
    fn pending_entry_cap_enforced() {
        let mut r = FragmentReassembler::with_limits(1024, 2);

        // Two streams — both fit
        r.on_fragment(&frag(10, 0, b"A", false)).unwrap();
        r.on_fragment(&frag(20, 0, b"B", false)).unwrap();
        assert_eq!(r.pending_count(), 2);

        // Third stream — exceeds cap
        let err = r.on_fragment(&frag(30, 0, b"C", false)).unwrap_err();
        assert!(matches!(err, ReassemblyError::TooManyPending { max: 2 }));
    }

    #[test]
    fn single_fragment_with_fin() {
        let mut r = FragmentReassembler::new();

        let f = frag(1, 0, b"single", true);
        let result = r.on_fragment(&f).unwrap().unwrap();
        assert_eq!(result.as_ref(), b"single");
    }

    #[test]
    fn non_fragment_frames_ignored() {
        let mut r = FragmentReassembler::new();

        let frame = FrameRef {
            stream_id: StreamId::new(2),
            seq: Seq::new(0),
            msg_type: MessageType::TextToken,
            flags: FrameFlags::NONE,
            payload: b"hello",
        };
        assert!(r.on_fragment(&frame).unwrap().is_none());
        assert_eq!(r.pending_count(), 0);
    }

    #[test]
    fn timeout_eviction() {
        let mut r = FragmentReassembler::new();

        // Insert a fragment with no FIN — stays pending
        r.on_fragment(&frag(1, 0, b"stale", false)).unwrap();
        assert_eq!(r.pending_count(), 1);
        assert_eq!(r.pending_bytes(), 5);

        // Immediate eviction (0-duration timeout)
        let evicted = r.evict_timeouts(Duration::from_millis(0));
        assert_eq!(evicted, 1);
        assert_eq!(r.pending_count(), 0);
        assert_eq!(r.pending_bytes(), 0);
    }

    #[test]
    fn timeout_keeps_recent_entries() {
        let mut r = FragmentReassembler::new();

        r.on_fragment(&frag(1, 0, b"fresh", false)).unwrap();

        // 1-hour timeout — should NOT evict our just-inserted entry
        let evicted = r.evict_timeouts(Duration::from_secs(3600));
        assert_eq!(evicted, 0);
        assert_eq!(r.pending_count(), 1);
    }

    #[test]
    fn pending_bytes_tracks_correctly() {
        let mut r = FragmentReassembler::new();

        r.on_fragment(&frag(1, 0, b"AAAA", false)).unwrap();
        assert_eq!(r.pending_bytes(), 4);

        r.on_fragment(&frag(2, 0, b"BB", false)).unwrap();
        assert_eq!(r.pending_bytes(), 6);

        // Complete stream 1
        let result = r.on_fragment(&frag(1, 1, b"CCCC", true)).unwrap().unwrap();
        assert_eq!(result.as_ref(), b"AAAACCCC");
        assert_eq!(r.pending_bytes(), 2); // only stream 2 remains
    }
}
