//! [`RemoteSubscriberRegistry`] — tracks subscribers that live on
//! **other** nodes.
//!
//! When a non-owner node wants to receive publishes for a pattern, it
//! sends an `InterNodeMessage::Subscribe` to the topic owner. The
//! owner records the remote interest here. On every publish, the
//! owner looks up matching remote subscribers and fans out a
//! `Fanout` to each.
//!
//! ## Wildcard semantics
//!
//! Matches the single-segment `*` rule from
//! `pubsub_engine::TopicPattern`:
//! * Pattern segments are split on `.`.
//! * `*` matches exactly one segment.
//! * Topic must have the same segment count as the pattern.
//!
//! Re-implemented locally to avoid a hard cross-crate dependency on
//! `pubsub-engine`. If pubsub-engine later supports `#`
//! (multi-segment wildcards), this module must mirror.

use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use session_registry::NodeId;

/// Location of a subscriber on a remote node. The two `remote_*`
/// fields are opaque to us — they are back-routed through
/// `InterNodeMessage::Fanout` so the receiving node knows where to
/// deliver locally.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RemoteSubscriber {
    /// Node hosting the subscriber.
    pub node_id: NodeId,
    /// Slab index on the remote node's connection table.
    pub remote_conn_idx: u32,
    /// QUIC stream id on the remote connection.
    pub remote_quic_stream_id: u64,
}

/// Pattern → subscribers index, with a by-node reverse index for
/// `O(patterns-per-node)` cleanup when a node goes down.
#[derive(Default, Debug)]
pub struct RemoteSubscriberRegistry {
    /// Pattern → set of remote subscribers under that pattern.
    by_pattern: FxHashMap<String, FxHashSet<RemoteSubscriber>>,
    /// Node → patterns it has subscribed under. Drives node-down cleanup.
    by_node: FxHashMap<NodeId, FxHashSet<String>>,
}

impl RemoteSubscriberRegistry {
    /// Construct empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `sub` under `pattern`. Idempotent — re-adding the same
    /// `(pattern, sub)` pair is a no-op.
    pub fn add(&mut self, pattern: &str, sub: RemoteSubscriber) {
        let inserted = self
            .by_pattern
            .entry(pattern.to_owned())
            .or_default()
            .insert(sub);
        if inserted {
            self.by_node
                .entry(sub.node_id)
                .or_default()
                .insert(pattern.to_owned());
        }
    }

    /// Remove `sub` from `pattern`. Returns `true` if it was present.
    pub fn remove(&mut self, pattern: &str, sub: &RemoteSubscriber) -> bool {
        let removed = self
            .by_pattern
            .get_mut(pattern)
            .is_some_and(|set| set.remove(sub));
        if removed {
            // Drop the pattern entry if now empty.
            if self.by_pattern.get(pattern).is_some_and(|s| s.is_empty()) {
                self.by_pattern.remove(pattern);
            }
            // Drop the pattern from the node's reverse index.
            if let Some(pats) = self.by_node.get_mut(&sub.node_id) {
                pats.remove(pattern);
                if pats.is_empty() {
                    self.by_node.remove(&sub.node_id);
                }
            }
        }
        removed
    }

    /// Every remote subscriber whose pattern matches `topic`.
    ///
    /// Iterates all patterns (typically a small N — tens to low
    /// hundreds cluster-wide) and applies the single-segment `*`
    /// match.
    #[must_use]
    pub fn lookup(&self, topic: &str) -> Vec<RemoteSubscriber> {
        let mut out: Vec<RemoteSubscriber> = Vec::new();
        for (pattern, subs) in &self.by_pattern {
            if pattern_matches(pattern, topic) {
                out.extend(subs.iter().copied());
            }
        }
        out
    }

    /// Drop every subscription held by `node`. Returns the count
    /// removed. Used by [`crate::ClusterCoordinator::mark_node_down`].
    pub fn remove_node(&mut self, node: NodeId) -> usize {
        let patterns = match self.by_node.remove(&node) {
            Some(p) => p,
            None => return 0,
        };
        let mut dropped = 0usize;
        for pattern in patterns {
            if let Some(set) = self.by_pattern.get_mut(&pattern) {
                let before = set.len();
                set.retain(|s| s.node_id != node);
                dropped += before - set.len();
                if set.is_empty() {
                    self.by_pattern.remove(&pattern);
                }
            }
        }
        dropped
    }

    /// Distinct patterns tracked.
    #[must_use]
    pub fn pattern_count(&self) -> usize {
        self.by_pattern.len()
    }

    /// Total subscriber entries (sum across patterns).
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.by_pattern.values().map(|s| s.len()).sum()
    }
}

// ── Pattern matching ────────────────────────────────────────────

/// Single-segment wildcard matcher. `pattern` and `topic` are split
/// on `.`; segments must be equal in count and each must match
/// exactly, except `*` matches any single segment.
fn pattern_matches(pattern: &str, topic: &str) -> bool {
    let pat = pattern.split('.').count();
    let top = topic.split('.').count();
    if pat != top {
        return false;
    }
    pattern
        .split('.')
        .zip(topic.split('.'))
        .all(|(p, t)| p == "*" || p == t)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn sub(node: u32, conn: u32, stream: u64) -> RemoteSubscriber {
        RemoteSubscriber {
            node_id: NodeId::new(node),
            remote_conn_idx: conn,
            remote_quic_stream_id: stream,
        }
    }

    fn pat_matches(p: &str, t: &str) -> bool {
        pattern_matches(p, t)
    }

    // ── pattern_matches ──────────────────────────────────────────

    #[test]
    fn exact_pattern_matches() {
        assert!(pat_matches("sensor.temp", "sensor.temp"));
        assert!(!pat_matches("sensor.temp", "sensor.humidity"));
    }

    #[test]
    fn single_segment_wildcard_matches() {
        assert!(pat_matches("sensor.*", "sensor.temp"));
        assert!(pat_matches("sensor.*", "sensor.humidity"));
    }

    #[test]
    fn wildcard_does_not_match_zero_or_multi_segments() {
        assert!(!pat_matches("sensor.*", "sensor"));
        assert!(!pat_matches("sensor.*", "sensor.temp.room1"));
    }

    #[test]
    fn multi_wildcard_pattern() {
        assert!(pat_matches("*.*", "a.b"));
        assert!(!pat_matches("*.*", "a.b.c"));
        assert!(pat_matches("*.*.*", "a.b.c"));
    }

    #[test]
    fn empty_pattern_matches_only_empty_topic() {
        assert!(pat_matches("", ""));
        assert!(!pat_matches("", "x"));
    }

    // ── registry ────────────────────────────────────────────────

    #[test]
    fn add_then_lookup() {
        let mut r = RemoteSubscriberRegistry::new();
        r.add("sensor.*", sub(2, 10, 100));
        let hits = r.lookup("sensor.temp");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], sub(2, 10, 100));
    }

    #[test]
    fn lookup_no_match_returns_empty() {
        let mut r = RemoteSubscriberRegistry::new();
        r.add("alert.*", sub(2, 10, 100));
        assert!(r.lookup("sensor.temp").is_empty());
    }

    #[test]
    fn add_same_pair_is_idempotent() {
        let mut r = RemoteSubscriberRegistry::new();
        r.add("t", sub(2, 10, 100));
        r.add("t", sub(2, 10, 100));
        assert_eq!(r.subscriber_count(), 1);
    }

    #[test]
    fn add_distinct_subscribers_under_same_pattern() {
        let mut r = RemoteSubscriberRegistry::new();
        r.add("t", sub(2, 10, 100));
        r.add("t", sub(3, 20, 200));
        assert_eq!(r.subscriber_count(), 2);
        let hits = r.lookup("t");
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn add_same_subscriber_under_distinct_patterns() {
        let mut r = RemoteSubscriberRegistry::new();
        let s = sub(2, 10, 100);
        r.add("sensor.*", s);
        r.add("alert.*", s);
        assert_eq!(r.pattern_count(), 2);
        assert_eq!(r.subscriber_count(), 2);
        // The same subscriber appears once per pattern (not deduped).
        let hits = r.lookup("sensor.temp");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], s);
    }

    #[test]
    fn remove_returns_true_when_present() {
        let mut r = RemoteSubscriberRegistry::new();
        let s = sub(2, 10, 100);
        r.add("t", s);
        assert!(r.remove("t", &s));
        assert_eq!(r.subscriber_count(), 0);
        // Pattern entry is dropped when set empties.
        assert_eq!(r.pattern_count(), 0);
    }

    #[test]
    fn remove_returns_false_when_absent() {
        let mut r = RemoteSubscriberRegistry::new();
        let s = sub(2, 10, 100);
        assert!(!r.remove("t", &s));
        // And removing from a pattern that exists but doesn't contain the sub.
        r.add("t", sub(3, 1, 1));
        assert!(!r.remove("t", &s));
    }

    #[test]
    fn remove_node_clears_all_its_subscriptions() {
        let mut r = RemoteSubscriberRegistry::new();
        r.add("sensor.*", sub(2, 10, 100));
        r.add("alert.*", sub(2, 11, 200));
        r.add("logs.*", sub(3, 5, 50));

        let dropped = r.remove_node(NodeId::new(2));
        assert_eq!(dropped, 2);
        assert_eq!(r.subscriber_count(), 1);
        // Node 3 unaffected.
        assert_eq!(r.lookup("logs.error").len(), 1);
        // Node 2 fully cleared.
        assert!(r.lookup("sensor.temp").is_empty());
        assert!(r.lookup("alert.fire").is_empty());
    }

    #[test]
    fn remove_node_unknown_returns_zero() {
        let mut r = RemoteSubscriberRegistry::new();
        r.add("t", sub(2, 1, 1));
        assert_eq!(r.remove_node(NodeId::new(99)), 0);
    }

    #[test]
    fn counts_reflect_state() {
        let mut r = RemoteSubscriberRegistry::new();
        assert_eq!(r.pattern_count(), 0);
        assert_eq!(r.subscriber_count(), 0);
        r.add("a.*", sub(2, 1, 1));
        r.add("a.*", sub(3, 1, 1));
        r.add("b.*", sub(2, 2, 2));
        assert_eq!(r.pattern_count(), 2);
        assert_eq!(r.subscriber_count(), 3);
    }
}
