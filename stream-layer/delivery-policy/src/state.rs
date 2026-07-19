//! Per-stream policy state — one enum variant per [`DeliveryPolicy`].
//!
//! Each variant owns the minimal mutable state its policy needs to make a
//! decision. All variants expose the same surface:
//!
//! * [`PerStreamState::apply`] — decide the fate of one incoming frame.
//! * [`PerStreamState::drain_ready`] — emit frames that became deliverable
//!   due to gap-fill (`ReliableOrdered`) or that have been held in a
//!   pending slot (`LatestOnly`).
//! * [`PerStreamState::flush_for_upgrade`] — emit **all** buffered frames
//!   in `Seq` order; used when a default-inserted `ReliableOrdered` state
//!   is replaced by the policy declared in a late `StreamOpen`.
//!
//! The variants are intentionally kept narrow: no flow-control credits,
//! no FSM transitions — those live in `stream-manager`. This crate only
//! answers the question "given a `Seq` and a payload, what should the
//! receiver do with it?".

use frame::header::Seq;
use rustc_hash::FxHashMap;

use crate::decision::{Decision, FramePayload};
use crate::policy::DeliveryPolicy;
use crate::table::{MAX_DEDUP_SET, MAX_REORDER_WINDOW};

/// The per-stream policy state.
///
/// Constructed by [`DeliveryPolicyTable::bind`](crate::DeliveryPolicyTable::bind)
/// and never re-typed after creation (upgrades happen by `mem::replace` at
/// the table level — see [`Self::flush_for_upgrade`]).
#[derive(Debug)]
pub enum PerStreamState {
    /// In-order delivery with a bounded reorder window. Never drops.
    ReliableOrdered(ReliableOrderedState),
    /// Deliver all immediately; dedup by `Seq`. Never drops except dups.
    ReliableUnordered(ReliableUnorderedState),
    /// Drop any frame with `Seq ≤ last_delivered`.
    RealtimeDropOld(RealtimeDropOldState),
    /// Keep only the newest pending frame; older is discarded on arrival.
    LatestOnly(LatestOnlyState),
}

impl PerStreamState {
    /// Construct an empty state for the given policy.
    #[inline]
    #[must_use]
    pub fn new(policy: DeliveryPolicy) -> Self {
        match policy {
            DeliveryPolicy::ReliableOrdered => Self::ReliableOrdered(ReliableOrderedState::new()),
            DeliveryPolicy::ReliableUnordered => {
                Self::ReliableUnordered(ReliableUnorderedState::new())
            }
            DeliveryPolicy::RealtimeDropOld => Self::RealtimeDropOld(RealtimeDropOldState::new()),
            DeliveryPolicy::LatestOnly => Self::LatestOnly(LatestOnlyState::new()),
        }
    }

    /// Which policy this state is configured for.
    #[inline]
    #[must_use]
    pub fn policy(&self) -> DeliveryPolicy {
        match self {
            Self::ReliableOrdered(_) => DeliveryPolicy::ReliableOrdered,
            Self::ReliableUnordered(_) => DeliveryPolicy::ReliableUnordered,
            Self::RealtimeDropOld(_) => DeliveryPolicy::RealtimeDropOld,
            Self::LatestOnly(_) => DeliveryPolicy::LatestOnly,
        }
    }

    /// Apply the policy to one decoded frame. Consumes `payload`.
    ///
    /// Returns [`Decision::Deliver`] when the frame should be emitted
    /// immediately, [`Decision::Buffer`] when the policy has retained the
    /// bytes (caller must NOT emit), and [`Decision::Drop`] when the frame
    /// is stale, duplicate, or otherwise discarded.
    #[inline]
    pub fn apply(&mut self, seq: Seq, payload: FramePayload) -> Decision {
        match self {
            Self::ReliableOrdered(s) => s.apply(seq, payload),
            Self::ReliableUnordered(s) => s.apply(seq, payload),
            Self::RealtimeDropOld(s) => s.apply(seq, payload),
            Self::LatestOnly(s) => s.apply(seq, payload),
        }
    }

    /// Drain frames that have become deliverable due to gap-fill
    /// (in-order policies) or that are waiting in a pending slot
    /// (`LatestOnly`).
    ///
    /// Appends `(Seq, payload)` pairs in delivery order. The state is
    /// left empty of deliverable frames — calling this twice in a row
    /// with no intervening [`Self::apply`] produces nothing on the
    /// second call.
    pub fn drain_ready(&mut self, out: &mut Vec<(Seq, FramePayload)>) {
        match self {
            Self::ReliableOrdered(s) => s.drain_ready(out),
            Self::ReliableUnordered(_) | Self::RealtimeDropOld(_) => {
                // These policies never hold frames — they emit immediately
                // on `apply`. Nothing to drain.
            }
            Self::LatestOnly(s) => s.drain_ready(out),
        }
    }

    /// Drain **every** buffered frame regardless of whether it is
    /// deliverable yet, in ascending `Seq` order.
    ///
    /// Used by [`DeliveryPolicyTable::bind`](crate::DeliveryPolicyTable::bind)
    /// when a default-inserted state is being upgraded to the policy
    /// declared in a late `StreamOpen`. After this returns, the state is
    /// empty but still typed — the table will `mem::replace` it with a
    /// fresh state for the new policy.
    pub fn flush_for_upgrade(&mut self, out: &mut Vec<(Seq, FramePayload)>) {
        match self {
            Self::ReliableOrdered(s) => s.flush_all(out),
            Self::LatestOnly(s) => s.drain_ready(out),
            // ReliableUnordered and RealtimeDropOld never buffer — no-op.
            Self::ReliableUnordered(_) | Self::RealtimeDropOld(_) => {}
        }
    }

    /// `true` if [`drain_ready`](Self::drain_ready) would emit at least
    /// one frame right now. Used by the table to skip idle streams during
    /// the per-tick drain scan.
    ///
    /// Semantics:
    /// * `ReliableOrdered` — dirty while the reorder window is non-empty
    ///   (gap-fill pending, or a just-arrived anchor unblocked a run).
    /// * `LatestOnly` — dirty while the pending slot is occupied.
    /// * `ReliableUnordered` / `RealtimeDropOld` — never dirty (they
    ///   always emit synchronously on `apply`).
    #[inline]
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        match self {
            Self::ReliableOrdered(s) => !s.reorder.is_empty(),
            Self::LatestOnly(s) => s.pending.is_some(),
            Self::ReliableUnordered(_) | Self::RealtimeDropOld(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// ReliableOrdered
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::ReliableOrdered`].
///
/// Gap-fill semantics: an in-order frame is delivered immediately via
/// [`Decision::Deliver`]. The engine then calls `drain_ready` (typically
/// at the end of the decode batch) to flush any frames that became
/// deliverable as a result of the cursor advancing.
///
/// `reorder` is an `FxHashMap` rather than a `BTreeMap`: insert / lookup
/// are O(1) average instead of O(log n), and drain walks forward by
/// keyed remove starting at `last_delivered.next()` — that loop is also
/// O(1) per delivered frame. The cost is finding the smallest seq on
/// overflow eviction; that's mitigated by caching `min_buffered` so the
/// O(window) scan only runs when the cache is invalidated (after an
/// eviction or a drain that consumed the current min).
#[derive(Debug)]
pub struct ReliableOrderedState {
    /// The highest contiguous `Seq` delivered so far. `None` until the
    /// first frame arrives — at which point it is treated as the anchor
    /// and the next expected `Seq` is `last_delivered.next()`.
    last_delivered: Option<Seq>,
    /// Out-of-order frames held until their predecessors arrive, keyed
    /// by `Seq`. Bounded by [`MAX_REORDER_WINDOW`].
    reorder: FxHashMap<Seq, FramePayload>,
    /// Cached smallest `Seq` currently in `reorder`, or `None` when the
    /// map is empty *or* the cache has been invalidated. The invariant
    /// is: if `Some(s)`, then `s` is in `reorder` and no smaller key is.
    /// `None` is treated as "unknown — recompute on next eviction".
    min_buffered: Option<Seq>,
}

impl ReliableOrderedState {
    #[must_use]
    fn new() -> Self {
        Self {
            last_delivered: None,
            reorder: FxHashMap::default(),
            min_buffered: None,
        }
    }

    fn apply(&mut self, seq: Seq, payload: FramePayload) -> Decision {
        match self.last_delivered {
            None => {
                // First frame on this stream — accept unconditionally as
                // the anchor. The next expected Seq is `seq.next()`.
                self.last_delivered = Some(seq);
                Decision::Deliver(payload)
            }
            Some(last) => {
                if seq <= last {
                    // Stale or duplicate.
                    Decision::Drop
                } else if seq == last.next() {
                    // Exactly the next expected frame — deliver immediately.
                    // Gap-fill (drain_ready) happens at the end of the
                    // batch in the engine, which keeps this hot path tight.
                    self.last_delivered = Some(seq);
                    Decision::Deliver(payload)
                } else {
                    // Future frame — buffer in the reorder window.
                    self.insert_bounded(seq, payload)
                }
            }
        }
    }

    /// Insert into the reorder window, evicting the oldest entry (data
    /// loss) if the window is full. Handles duplicate seq by dropping
    /// the newer payload.
    fn insert_bounded(&mut self, seq: Seq, payload: FramePayload) -> Decision {
        if self.reorder.len() >= MAX_REORDER_WINDOW {
            // Cache may be stale (None) after a prior eviction or drain.
            // Recompute on demand — the steady state skips this entirely.
            if self.min_buffered.is_none() && !self.reorder.is_empty() {
                self.min_buffered = self.reorder.keys().min().copied();
            }
            // Evict the smallest entry to make room — but only if the
            // incoming seq is greater than the smallest (otherwise we'd
            // evict to make room for an even-older frame, which is
            // senseless).
            if let Some(s) = self.min_buffered {
                if s < seq {
                    self.reorder.remove(&s);
                    // Invalidate the cache; the next overflow will recompute.
                    self.min_buffered = None;
                    tracing::warn!(
                        evicted_seq = s.get(),
                        new_seq = seq.get(),
                        window = self.reorder.len(),
                        max = MAX_REORDER_WINDOW,
                        "reorder window overflow — evicting oldest (data loss)"
                    );
                }
            }
        }
        match self.reorder.insert(seq, payload) {
            None => {
                // Update min cache on insert — single comparison, cheaper
                // than recomputing later.
                if self.min_buffered.map_or(true, |m| seq < m) {
                    self.min_buffered = Some(seq);
                }
                Decision::Buffer
            }
            // Already present — duplicate. Preserve the existing entry,
            // drop the new payload.
            Some(prev) => {
                tracing::trace!(
                    seq = seq.get(),
                    "duplicate seq in reorder window — dropping new"
                );
                self.reorder.insert(seq, prev);
                Decision::Drop
            }
        }
    }

    fn drain_ready(&mut self, out: &mut Vec<(Seq, FramePayload)>) {
        let Some(mut cursor) = self.last_delivered else {
            return;
        };
        // Walk forward emitting contiguous seqs. Each iteration is an
        // O(1) keyed remove — total cost is O(k) where k is the number
        // of frames that became deliverable.
        let mut drained_min = false;
        loop {
            let want = cursor.next();
            match self.reorder.remove_entry(&want) {
                Some((s, p)) => {
                    if self.min_buffered == Some(s) {
                        drained_min = true;
                    }
                    out.push((s, p));
                    cursor = s;
                }
                None => break,
            }
        }
        self.last_delivered = Some(cursor);
        if drained_min {
            // The cached min was consumed; invalidate so the next
            // overflow-triggered eviction recomputes.
            self.min_buffered = self.reorder.keys().min().copied();
        }
    }

    fn flush_all(&mut self, out: &mut Vec<(Seq, FramePayload)>) {
        // Emit every buffered frame in ascending Seq order, regardless
        // of gap state. Used during policy upgrade so no in-flight data
        // is silently lost when the table swaps state.
        //
        // FxHashMap doesn't iterate in key order, so we collect + sort.
        // This is O(k log k) for k buffered frames — rare (only on
        // upgrade) so the sort cost is acceptable.
        let taken = std::mem::take(&mut self.reorder);
        self.min_buffered = None;
        if taken.is_empty() {
            return;
        }
        let mut sorted: Vec<(Seq, FramePayload)> = taken.into_iter().collect();
        sorted.sort_by_key(|(s, _)| s.get());
        out.extend(sorted);
    }
}

// ---------------------------------------------------------------------------
// ReliableUnordered
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::ReliableUnordered`].
///
/// Holds a sliding-window bitmap of recently-delivered `Seq`s for dedup.
/// The window covers exactly [`MAX_DEDUP_SET`] seqs starting from `base`;
/// each seq occupies one bit. Test/set/clear are O(1) — a single word
/// fetch + bit op.
///
/// When a seq arrives beyond the current window, the bitmap slides
/// forward (in word-sized chunks) so the new seq maps to the last bit.
/// Bits that fall off the bottom of the window are zeroed. As with the
/// old `BTreeSet` design, a very late duplicate may slip through once
/// its bit has been cleared — accepted behaviour, since unbounded
/// growth would be a DoS vector.
///
/// Storage cost: 4096 bits = 512 bytes per ReliableUnordered stream
/// (was ~32 bytes per entry × up to 4096 entries = up to 128 KiB for
/// the `BTreeSet`). The per-stream cost is therefore higher for sparse
/// windows but bounded and allocation-free.
#[derive(Debug)]
pub struct ReliableUnorderedState {
    /// Fixed-size bit array. Bit `i` represents seq `base + i`.
    words: [u64; MAX_DEDUP_SET / 64],
    /// The seq at bit 0. Meaningful only after the first frame.
    base: Seq,
    initialized: bool,
}

impl ReliableUnorderedState {
    #[must_use]
    fn new() -> Self {
        Self {
            words: [0; MAX_DEDUP_SET / 64],
            base: Seq::ZERO,
            initialized: false,
        }
    }

    fn apply(&mut self, seq: Seq, payload: FramePayload) -> Decision {
        if !self.initialized {
            self.base = seq;
            self.initialized = true;
            self.set_bit(0);
            return Decision::Deliver(payload);
        }

        // Seq below the window: the BTreeSet version would have evicted
        // it by now, so treat as not-seen and deliver. (We do NOT try to
        // extend the window backward — that would require clearing the
        // whole bitmap.)
        let offset = match seq.0.checked_sub(self.base.0) {
            None => return Decision::Deliver(payload),
            Some(o) => o as usize,
        };

        // Seq beyond the window: slide forward so it fits at the last bit.
        if offset >= MAX_DEDUP_SET {
            self.slide_window_to_fit(seq);
        }

        let offset = (seq.0 - self.base.0) as usize;
        if self.test_bit(offset) {
            Decision::Drop
        } else {
            self.set_bit(offset);
            Decision::Deliver(payload)
        }
    }

    /// Advance `base` so that `seq` maps to the last bit of the window,
    /// shifting the bitmap left and zeroing bits that fell off.
    fn slide_window_to_fit(&mut self, seq: Seq) {
        let new_base = Seq(seq.0 - (MAX_DEDUP_SET as u64 - 1));
        let shift = (new_base.0 - self.base.0) as usize;

        if shift >= MAX_DEDUP_SET {
            // Whole window replaced — clear everything.
            self.words.fill(0);
        } else {
            // Shift bits left by `shift`: bit[i + shift] in the old
            // layout becomes bit[i] in the new layout. Done in two
            // passes (whole-word then sub-word) so each pass is a
            // simple forward scan reading only unmodified cells.
            let word_shift = shift / 64;
            let bit_shift = shift % 64;

            if word_shift > 0 {
                for i in 0..MAX_DEDUP_SET / 64 {
                    self.words[i] = if i + word_shift < MAX_DEDUP_SET / 64 {
                        self.words[i + word_shift]
                    } else {
                        0
                    };
                }
            }

            if bit_shift > 0 {
                let words_len = MAX_DEDUP_SET / 64;
                for i in 0..words_len {
                    let high = if i + 1 < words_len {
                        self.words[i + 1]
                    } else {
                        0
                    };
                    self.words[i] = (self.words[i] >> bit_shift) | (high << (64 - bit_shift));
                }
            }
        }

        self.base = new_base;
    }

    #[inline]
    fn test_bit(&self, offset: usize) -> bool {
        let word = offset / 64;
        let bit = offset % 64;
        if word >= MAX_DEDUP_SET / 64 {
            return false;
        }
        self.words[word] & (1u64 << bit) != 0
    }

    #[inline]
    fn set_bit(&mut self, offset: usize) {
        let word = offset / 64;
        let bit = offset % 64;
        if word < MAX_DEDUP_SET / 64 {
            self.words[word] |= 1u64 << bit;
        }
    }
}

// ---------------------------------------------------------------------------
// RealtimeDropOld
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::RealtimeDropOld`].
#[derive(Debug)]
pub struct RealtimeDropOldState {
    /// The highest `Seq` delivered so far. `None` until the first frame.
    last_delivered: Option<Seq>,
}

impl RealtimeDropOldState {
    #[must_use]
    fn new() -> Self {
        Self {
            last_delivered: None,
        }
    }

    fn apply(&mut self, seq: Seq, payload: FramePayload) -> Decision {
        match self.last_delivered {
            None => {
                self.last_delivered = Some(seq);
                Decision::Deliver(payload)
            }
            Some(last) if seq > last => {
                self.last_delivered = Some(seq);
                Decision::Deliver(payload)
            }
            Some(_) => Decision::Drop,
        }
    }
}

// ---------------------------------------------------------------------------
// LatestOnly
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::LatestOnly`].
///
/// `apply` always returns [`Decision::Buffer`] — even on the first frame.
/// The engine must call `drain_ready` (via
/// [`DeliveryPolicyTable::drain_ready_for_connection`](crate::DeliveryPolicyTable::drain_ready_for_connection))
/// at the end of each decode batch to actually emit the pending frame.
#[derive(Debug)]
pub struct LatestOnlyState {
    /// The single pending frame, paired with its `Seq` so arrivals can be
    /// compared. Replaced by any newer `Seq`; older arrivals are dropped.
    pending: Option<(Seq, FramePayload)>,
}

impl LatestOnlyState {
    #[must_use]
    fn new() -> Self {
        Self { pending: None }
    }

    fn apply(&mut self, seq: Seq, payload: FramePayload) -> Decision {
        match self.pending {
            None => {
                self.pending = Some((seq, payload));
                Decision::Buffer
            }
            Some((cur_seq, _)) if seq > cur_seq => {
                // Newer frame replaces the pending one; the old is lost.
                self.pending = Some((seq, payload));
                Decision::Buffer
            }
            Some(_) => {
                // Older or equal — keep the existing pending, drop new.
                Decision::Drop
            }
        }
    }

    fn drain_ready(&mut self, out: &mut Vec<(Seq, FramePayload)>) {
        if let Some((s, p)) = self.pending.take() {
            out.push((s, p));
        }
    }
}
