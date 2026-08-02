//! [`TopicTrie`] — trie-based topic pattern matching.
//!
//! Provides O(k) pattern insertion and lookup where k is the number
//! of dot-separated segments in the topic. Supports `*` wildcard
//! segments that match exactly one topic segment.
//!
//! ```text
//! Pattern "sensor.*" inserted:
//!
//!   root
//!     └─ "sensor"
//!          └─ "*"
//!               └─ [sub_id=1]
//!
//! Lookup "sensor.temp":
//!   root → "sensor" → "temp" (exact) + "*" (wildcard) → [sub_id=1]
//! ```

use crate::config::PubSubConfig;
use crate::error::PubSubError;
use crate::stats::PubSubStats;
use crate::subscription::SubscriptionId;
use rustc_hash::FxHashMap;
use std::fmt;

// ── Trie node ─────────────────────────────────────────────────────

/// Internal trie node.
struct TrieNode {
    /// Subscription IDs whose pattern terminates at this node.
    subs: Vec<SubscriptionId>,
    /// Children keyed by segment literal (including `"*"` for wildcards).
    children: FxHashMap<String, TrieNode>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            subs: Vec::new(),
            children: FxHashMap::default(),
        }
    }
}

// ── TopicTrie ─────────────────────────────────────────────────────

/// Trie-based topic pattern matcher.
///
/// Supports `*` wildcard that matches a single dot-delimited segment.
/// Insertion and lookup are both O(k) where k = segment count.
pub struct TopicTrie {
    #[allow(dead_code)]
    cfg: PubSubConfig,
    stats: PubSubStats,
    root: TrieNode,
}

impl fmt::Debug for TopicTrie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicTrie")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl TopicTrie {
    /// Creates a new topic trie with the given config.
    #[must_use]
    pub fn new(cfg: PubSubConfig) -> Self {
        Self {
            cfg,
            stats: PubSubStats::new(),
            root: TrieNode::new(),
        }
    }

    /// Inserts a topic pattern into the trie, associated with a subscription ID.
    ///
    /// The pattern is split by `.` into segments. Each segment becomes a
    /// child node. The subscription ID is stored at the terminal node.
    /// Duplicate `(pattern, sub_id)` pairs are silently ignored.
    ///
    /// # Errors
    /// Returns [`PubSubError::PublishFailed`] if the pattern is empty.
    pub fn insert(&mut self, pattern: &str, sub_id: SubscriptionId) -> Result<(), PubSubError> {
        let segments: Vec<&str> = pattern.split('.').collect();
        if segments.is_empty() || (segments.len() == 1 && segments[0].is_empty()) {
            return Err(PubSubError::PublishFailed("empty pattern"));
        }

        let node = segments.iter().fold(&mut self.root, |node, seg| {
            node.children
                .entry((*seg).to_string())
                .or_insert_with(TrieNode::new)
        });

        if !node.subs.contains(&sub_id) {
            node.subs.push(sub_id);
        }
        Ok(())
    }

    /// Removes a subscription ID from the node identified by `pattern`.
    ///
    /// Returns `Ok(())` even if the pattern or sub_id was not present
    /// (idempotent). Empty child nodes are pruned to reclaim memory.
    ///
    /// # Errors
    /// Returns [`PubSubError::SubscriptionNotFound`] if `sub_id` is nil.
    pub fn remove(&mut self, pattern: &str, sub_id: SubscriptionId) -> Result<(), PubSubError> {
        if sub_id.is_nil() {
            return Err(PubSubError::SubscriptionNotFound(sub_id));
        }

        let segments: Vec<&str> = pattern.split('.').collect();
        Self::remove_recursive(&mut self.root, &segments, 0, sub_id);
        Ok(())
    }

    fn remove_recursive(
        node: &mut TrieNode,
        segments: &[&str],
        depth: usize,
        sub_id: SubscriptionId,
    ) -> bool {
        if depth == segments.len() {
            node.subs.retain(|&s| s != sub_id);
            // Return true if this node can be pruned (no subs, no children).
            return node.subs.is_empty() && node.children.is_empty();
        }

        let seg = segments[depth];
        if let Some(child) = node.children.get_mut(seg) {
            let prune = Self::remove_recursive(child, segments, depth + 1, sub_id);
            if prune {
                node.children.remove(seg);
            }
        }

        // Return whether *this* node can also be pruned.
        node.subs.is_empty() && node.children.is_empty()
    }

    /// Finds all subscription IDs matching the given concrete topic.
    ///
    /// At each level, the traversal checks both the exact segment and
    /// the `"*"` wildcard child. This correctly handles patterns with
    /// wildcards at any position.
    ///
    /// Returns a deduplicated `Vec<SubscriptionId>`.
    ///
    /// # Errors
    /// Currently never errors; returns `Ok` for consistency with the
    /// trait contract.
    pub fn lookup(&self, topic: &str) -> Result<Vec<SubscriptionId>, PubSubError> {
        let mut results = Vec::new();
        Self::lookup_recursive(&self.root, topic, &mut results);

        // Deduplicate (a topic may match multiple wildcard patterns).
        results.sort_unstable_by_key(|s| s.raw());
        results.dedup_by_key(|s| s.raw());
        Ok(results)
    }

    /// Recursive lookup using lazy `&str` slicing — zero allocation
    /// for segment parsing (the previous version allocated a
    /// `Vec<&str>` on every publish).
    fn lookup_recursive(node: &TrieNode, topic_rest: &str, out: &mut Vec<SubscriptionId>) {
        if topic_rest.is_empty() {
            // All segments consumed — collect subs at this node.
            out.extend_from_slice(&node.subs);
            return;
        }

        // Find current segment (up to next '.') and remaining topic.
        let (seg, rest) = match topic_rest.find('.') {
            Some(pos) => (&topic_rest[..pos], &topic_rest[pos + 1..]),
            None => (topic_rest, ""), // last segment
        };

        // Exact match child.
        if let Some(child) = node.children.get(seg) {
            Self::lookup_recursive(child, rest, out);
        }

        // Wildcard `*` child.
        if let Some(child) = node.children.get("*") {
            Self::lookup_recursive(child, rest, out);
        }
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> crate::health::HealthReport {
        crate::health::HealthReport::derive(&self.stats.snapshot())
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn t(s: &str) -> &str {
        s
    }

    #[test]
    fn trie_creation() {
        let trie = TopicTrie::new(PubSubConfig::new());
        let _ = trie.health();
    }

    #[test]
    fn insert_then_lookup_exact() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("sensor.temp", SubscriptionId::new(1)).unwrap();
        let result = trie.lookup(&t("sensor.temp")).unwrap();
        assert_eq!(result, vec![SubscriptionId::new(1)]);
    }

    #[test]
    fn lookup_misses_non_matching() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("sensor.temp", SubscriptionId::new(1)).unwrap();
        let result = trie.lookup(&t("sensor.humidity")).unwrap();
        assert!(result.is_empty(), "expected no matches, got {result:?}");
    }

    #[test]
    fn wildcard_single_segment() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("sensor.*", SubscriptionId::new(1)).unwrap();

        assert!(
            trie.lookup(&t("sensor.temp"))
                .unwrap()
                .contains(&SubscriptionId::new(1))
        );
        assert!(
            trie.lookup(&t("sensor.humidity"))
                .unwrap()
                .contains(&SubscriptionId::new(1))
        );
        assert!(trie.lookup(&t("sensor")).unwrap().is_empty()); // too few segments
        assert!(trie.lookup(&t("sensor.temp.extra")).unwrap().is_empty()); // too many
    }

    #[test]
    fn wildcard_at_different_positions() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("*.temp", SubscriptionId::new(1)).unwrap();
        trie.insert("sensor.*", SubscriptionId::new(2)).unwrap();

        let r = trie.lookup(&t("sensor.temp")).unwrap();
        assert!(r.contains(&SubscriptionId::new(1)));
        assert!(r.contains(&SubscriptionId::new(2)));

        let r = trie.lookup(&t("room.temp")).unwrap();
        assert!(r.contains(&SubscriptionId::new(1)));
        assert!(!r.contains(&SubscriptionId::new(2)));

        let r = trie.lookup(&t("sensor.light")).unwrap();
        assert!(!r.contains(&SubscriptionId::new(1)));
        assert!(r.contains(&SubscriptionId::new(2)));
    }

    #[test]
    fn multiple_subscriptions_same_pattern() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("sensor.*", SubscriptionId::new(1)).unwrap();
        trie.insert("sensor.*", SubscriptionId::new(2)).unwrap();

        let r = trie.lookup(&t("sensor.temp")).unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn duplicate_insert_is_idempotent() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("sensor.*", SubscriptionId::new(1)).unwrap();
        trie.insert("sensor.*", SubscriptionId::new(1)).unwrap();

        let r = trie.lookup(&t("sensor.temp")).unwrap();
        assert_eq!(r.len(), 1); // not 2
    }

    #[test]
    fn remove_clears_subscription() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("sensor.*", SubscriptionId::new(1)).unwrap();
        trie.insert("sensor.*", SubscriptionId::new(2)).unwrap();

        trie.remove("sensor.*", SubscriptionId::new(1)).unwrap();

        let r = trie.lookup(&t("sensor.temp")).unwrap();
        assert_eq!(r, vec![SubscriptionId::new(2)]);
    }

    #[test]
    fn remove_prunes_empty_nodes() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("a.b.c", SubscriptionId::new(1)).unwrap();
        trie.remove("a.b.c", SubscriptionId::new(1)).unwrap();

        let r = trie.lookup(&t("a.b.c")).unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn deep_nested_patterns() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("a.b.c.d.e", SubscriptionId::new(1)).unwrap();
        trie.insert("a.b.*.d.e", SubscriptionId::new(2)).unwrap();
        trie.insert("a.*.c.*.e", SubscriptionId::new(3)).unwrap();

        let r = trie.lookup(&t("a.b.c.d.e")).unwrap();
        assert!(r.contains(&SubscriptionId::new(1)));
        assert!(r.contains(&SubscriptionId::new(2)));
        assert!(r.contains(&SubscriptionId::new(3)));
        assert_eq!(r.len(), 3);

        let r = trie.lookup(&t("a.x.c.y.e")).unwrap();
        assert_eq!(r, vec![SubscriptionId::new(3)]);
    }

    #[test]
    fn single_segment_patterns() {
        let mut trie = TopicTrie::new(PubSubConfig::new());
        trie.insert("events", SubscriptionId::new(1)).unwrap();
        trie.insert("*", SubscriptionId::new(2)).unwrap();

        let r = trie.lookup(&t("events")).unwrap();
        assert_eq!(r.len(), 2);

        let r = trie.lookup(&t("alerts")).unwrap();
        assert_eq!(r, vec![SubscriptionId::new(2)]);
    }

    #[test]
    fn health_report_initially_healthy() {
        let trie = TopicTrie::new(PubSubConfig::new());
        let health = trie.health();
        assert!(health.is_healthy());
    }
}
