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

use std::collections::{BTreeMap, BTreeSet};

use frame::header::Seq;

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
#[derive(Debug)]
pub struct ReliableOrderedState {
    /// The highest contiguous `Seq` delivered so far. `None` until the
    /// first frame arrives — at which point it is treated as the anchor
    /// and the next expected `Seq` is `last_delivered.next()`.
    last_delivered: Option<Seq>,
    /// Out-of-order frames held until their predecessors arrive, keyed
    /// and sorted by `Seq`. Bounded by [`MAX_REORDER_WINDOW`].
    reorder: BTreeMap<Seq, FramePayload>,
}

impl ReliableOrderedState {
    #[must_use]
    fn new() -> Self {
        Self {
            last_delivered: None,
            reorder: BTreeMap::new(),
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
            // Evict the smallest entry to make room — but only if the
            // incoming seq is greater than the smallest (otherwise we'd
            // evict to make room for an even-older frame, which is
            // senseless).
            if let Some(&smallest) = self.reorder.keys().next() {
                if smallest < seq {
                    self.reorder.remove(&smallest);
                    tracing::warn!(
                        evicted_seq = smallest.get(),
                        new_seq = seq.get(),
                        window = self.reorder.len(),
                        max = MAX_REORDER_WINDOW,
                        "reorder window overflow — evicting oldest (data loss)"
                    );
                }
            }
        }
        match self.reorder.insert(seq, payload) {
            None => Decision::Buffer,
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
        // Walk forward through the reorder map emitting contiguous seqs.
        // BTreeMap iteration is ascending, so this is O(k) where k is the
        // number of frames that became deliverable.
        loop {
            let want = cursor.next();
            match self.reorder.remove_entry(&want) {
                Some((s, p)) => {
                    out.push((s, p));
                    cursor = s;
                }
                None => break,
            }
        }
        self.last_delivered = Some(cursor);
    }

    fn flush_all(&mut self, out: &mut Vec<(Seq, FramePayload)>) {
        // Emit every buffered frame in ascending Seq order, regardless
        // of gap state. Used during policy upgrade so no in-flight data
        // is silently lost when the table swaps state.
        let taken = std::mem::take(&mut self.reorder);
        for (s, p) in taken {
            out.push((s, p));
        }
    }
}

// ---------------------------------------------------------------------------
// ReliableUnordered
// ---------------------------------------------------------------------------

/// State for [`DeliveryPolicy::ReliableUnordered`].
///
/// Holds a sliding window of recently-delivered `Seq`s for dedup. The
/// window is bounded by [`MAX_DEDUP_SET`]; when full, the smallest entry
/// is evicted, which may allow a very late duplicate to slip through.
/// This is accepted behaviour: unbounded growth would be a DoS vector.
#[derive(Debug)]
pub struct ReliableUnorderedState {
    seen: BTreeSet<Seq>,
}

impl ReliableUnorderedState {
    #[must_use]
    fn new() -> Self {
        Self {
            seen: BTreeSet::new(),
        }
    }

    fn apply(&mut self, seq: Seq, payload: FramePayload) -> Decision {
        if self.seen.contains(&seq) {
            Decision::Drop
        } else {
            // Insert; if at cap, evict the smallest first.
            if self.seen.len() >= MAX_DEDUP_SET {
                if let Some(&smallest) = self.seen.first() {
                    if smallest < seq {
                        self.seen.remove(&smallest);
                    }
                }
            }
            self.seen.insert(seq);
            Decision::Deliver(payload)
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
