//! [`ConsumerOffsets`] — per-consumer ack tracking.
//!
//! Lives separately from [`crate::WalStore`] backends so the store
//! remains purely a byte log; consumer positions are an application-
//! level concern that varies by delivery semantic (at-least-once vs.
//! at-most-once vs. exactly-once-on-the-receiver).
//!
//! The map is keyed by `(topic, consumer_id)` and stores the highest
//! offset the consumer has acknowledged. Publishers do not consult
//! this map; it is queried by the consumer-restart path to decide
//! where to resume replay.

use rustc_hash::FxHashMap;

use crate::offset::Offset;

/// In-memory `(topic, consumer_id) → Offset` map.
///
/// Single-threaded by design — each reactor owns its own consumer
/// offsets. Cross-reactor fan-out (for HA) arrives in Phase 6
/// (multi-node pub/sub).
#[derive(Debug, Default)]
pub struct ConsumerOffsets {
    map: FxHashMap<(String, String), Offset>,
}

impl ConsumerOffsets {
    /// Construct an empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `consumer_id` acknowledged `offset` on `topic`.
    ///
    /// Acknowledgements are monotonic — if the stored offset is already
    /// greater than `offset`, the call is a no-op. Returns the offset
    /// that is now stored for `(topic, consumer_id)` (post-update).
    pub fn ack(&mut self, topic: &str, consumer_id: &str, offset: Offset) -> Offset {
        if offset.is_none() {
            // Ack of NONE is meaningless — leave the stored value alone.
            return self.get(topic, consumer_id);
        }
        let key = (topic.to_owned(), consumer_id.to_owned());
        // Read current value (if any) BEFORE the entry() call so we
        // can compare against the NONE sentinel correctly — NONE is
        // stored as u64::MAX and would otherwise win every ">"
        // comparison against a real offset.
        let current = self.map.get(&key).copied().unwrap_or(Offset::NONE);
        if current.is_none() || offset.get() > current.get() {
            self.map.insert(key, offset);
            offset
        } else {
            current
        }
    }

    /// Highest acknowledged offset for `(topic, consumer_id)`, or
    /// [`Offset::NONE`] if the consumer has never acked.
    #[must_use]
    pub fn get(&self, topic: &str, consumer_id: &str) -> Offset {
        self.map
            .get(&(topic.to_owned(), consumer_id.to_owned()))
            .copied()
            .unwrap_or(Offset::NONE)
    }

    /// Where `consumer_id` should resume on `topic`. Equal to
    /// `get(topic, consumer_id).next()` if the consumer has acked at
    /// least once, otherwise [`Offset::OLDEST`] (start from the
    /// beginning of the retained log).
    #[must_use]
    pub fn resume_from(&self, topic: &str, consumer_id: &str) -> Offset {
        match self.get(topic, consumer_id) {
            Offset::NONE => Offset::OLDEST,
            other => other.next(),
        }
    }

    /// Forget a consumer's position on a topic. Used when a consumer
    /// unsubscribes or its session expires.
    pub fn forget(&mut self, topic: &str, consumer_id: &str) {
        self.map.remove(&(topic.to_owned(), consumer_id.to_owned()));
    }

    /// Forget every consumer position on `topic`. Used by topic-delete.
    pub fn forget_topic(&mut self, topic: &str) {
        self.map.retain(|(t, _), _| t != topic);
    }

    /// Number of (topic, consumer) pairs tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// `true` when no pairs are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn new_is_empty() {
        let c = ConsumerOffsets::new();
        assert!(c.is_empty());
        assert_eq!(c.len(), 0);
        assert_eq!(c.get("t", "u"), Offset::NONE);
    }

    #[test]
    fn ack_then_get() {
        let mut c = ConsumerOffsets::new();
        let now = c.ack("t", "u", Offset::new(5));
        assert_eq!(now, Offset::new(5));
        assert_eq!(c.get("t", "u"), Offset::new(5));
    }

    #[test]
    fn ack_is_monotonic() {
        let mut c = ConsumerOffsets::new();
        c.ack("t", "u", Offset::new(5));
        let now = c.ack("t", "u", Offset::new(2));
        // Stored value should not regress.
        assert_eq!(now, Offset::new(5));
        assert_eq!(c.get("t", "u"), Offset::new(5));
    }

    #[test]
    fn ack_none_is_noop() {
        let mut c = ConsumerOffsets::new();
        c.ack("t", "u", Offset::new(3));
        c.ack("t", "u", Offset::NONE);
        assert_eq!(c.get("t", "u"), Offset::new(3));
    }

    #[test]
    fn resume_from_oldest_for_new_consumer() {
        let c = ConsumerOffsets::new();
        assert_eq!(c.resume_from("t", "u"), Offset::OLDEST);
    }

    #[test]
    fn resume_from_next_after_ack() {
        let mut c = ConsumerOffsets::new();
        c.ack("t", "u", Offset::new(5));
        assert_eq!(c.resume_from("t", "u"), Offset::new(6));
    }

    #[test]
    fn forget_removes_one() {
        let mut c = ConsumerOffsets::new();
        c.ack("t", "u1", Offset::new(1));
        c.ack("t", "u2", Offset::new(2));
        c.forget("t", "u1");
        assert_eq!(c.get("t", "u1"), Offset::NONE);
        assert_eq!(c.get("t", "u2"), Offset::new(2));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn forget_topic_removes_all_on_topic() {
        let mut c = ConsumerOffsets::new();
        c.ack("t1", "u1", Offset::new(1));
        c.ack("t1", "u2", Offset::new(2));
        c.ack("t2", "u3", Offset::new(3));
        c.forget_topic("t1");
        assert_eq!(c.len(), 1);
        assert_eq!(c.get("t2", "u3"), Offset::new(3));
    }
}
