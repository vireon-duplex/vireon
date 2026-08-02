//! [`TopicRegistry`] — production subscription tracker for the receive path.
//!
//! `PubSubEngine` (`engine.rs`) stores subscriptions by id with a flat
//! `HashMap<SubscriptionId, TopicPattern>` and matches a published topic
//! by iterating every entry — O(subscriptions) per publish. The dispatcher
//! hot path (the application layer's `handle_publish`) needs to find *where*
//! to send each delivered frame, so it kept its own
//! `Vec<(TopicPattern, conn_idx, quic_stream_id)>` and did the same linear
//! scan.
//!
//! `TopicRegistry` replaces both: a `TopicTrie` provides O(k) pattern
//! lookup (k = segment count), a `HashMap<SubscriptionId, Subscriber>`
//! resolves each matched id back to its QUIC location, and a per-connection
//! index makes connection-close cleanup O(1) in the number of
//! subscriptions being removed rather than the total table size.
//!
//! ## Concurrency
//!
//! Single-threaded — the dispatcher is driven by the reactor. Concurrency
//! is across connections on the same shard, not across threads.

use crate::config::PubSubConfig;
use crate::error::PubSubError;
use crate::subscription::SubscriptionId;
use crate::trie::TopicTrie;
use rustc_hash::FxHashMap;
use std::fmt;

// ── Subscriber ─────────────────────────────────────────────────────

/// Location of a subscriber — enough for the dispatcher to enqueue an
/// outgoing frame on the right QUIC stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Subscriber {
    /// Slab index of the subscriber's QUIC connection.
    pub conn_idx: usize,
    /// QUIC stream id on which the subscriber issued `Subscribe`.
    pub quic_stream_id: u64,
}

// ── TopicRegistry ──────────────────────────────────────────────────

/// Trie-backed subscription registry.
///
/// Tracks active subscriptions and resolves a published topic into a
/// list of subscriber locations. Designed to live on the dispatcher
/// thread; not `Sync`.
pub struct TopicRegistry {
    /// Trie mapping topic-pattern → subscription-ids under that pattern.
    trie: TopicTrie,
    /// Per-subscription metadata (location + pattern).
    by_id: FxHashMap<SubscriptionId, SubscriberRecord>,
    /// Per-connection subscription index — drives O(1) cleanup on close.
    by_conn: FxHashMap<usize, Vec<SubscriptionId>>,
    /// Monotonic subscription-id allocator (starts at 1; 0 is reserved nil).
    next_id: u64,
}

/// Stored alongside the subscriber location so we can prune the trie by
/// pattern during unsubscribe-by-pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SubscriberRecord {
    subscriber: Subscriber,
    pattern: String,
}

impl fmt::Debug for TopicRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicRegistry")
            .field("active_subscriptions", &self.by_id.len())
            .field("connections_tracked", &self.by_conn.len())
            .finish()
    }
}

impl TopicRegistry {
    /// Creates a new, empty registry.
    #[must_use]
    pub fn new(cfg: PubSubConfig) -> Self {
        Self {
            trie: TopicTrie::new(cfg),
            by_id: FxHashMap::default(),
            by_conn: FxHashMap::default(),
            next_id: 1,
        }
    }

    /// Registers a subscription and returns its id.
    ///
    /// `pattern` is validated by [`TopicTrie::insert`]; an empty pattern
    /// surfaces as [`PubSubError::PublishFailed`].
    pub fn subscribe(
        &mut self,
        subscriber: Subscriber,
        pattern: &str,
    ) -> Result<SubscriptionId, PubSubError> {
        let id = SubscriptionId::new(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);

        self.trie.insert(pattern, id)?;
        self.by_id.insert(
            id,
            SubscriberRecord {
                subscriber,
                pattern: pattern.to_owned(),
            },
        );
        self.by_conn
            .entry(subscriber.conn_idx)
            .or_default()
            .push(id);
        Ok(id)
    }

    /// Removes a single subscription by id. Idempotent — returns Ok if
    /// the id was unknown.
    pub fn unsubscribe(&mut self, id: SubscriptionId) -> Result<(), PubSubError> {
        if let Some(record) = self.by_id.remove(&id) {
            let _ = self.trie.remove(&record.pattern, id);
            if let Some(ids) = self.by_conn.get_mut(&record.subscriber.conn_idx) {
                ids.retain(|&x| x != id);
                if ids.is_empty() {
                    self.by_conn.remove(&record.subscriber.conn_idx);
                }
            }
        }
        Ok(())
    }

    /// Removes every subscription held by `(conn_idx, pattern)` — i.e. the
    /// pattern the same connection registered earlier. Returns the count
    /// removed. Used by the `Unsubscribe` control frame.
    pub fn unsubscribe_pattern(&mut self, conn_idx: usize, pattern: &str) -> usize {
        let mut removed = 0;
        let ids: Vec<SubscriptionId> = self.by_conn.get(&conn_idx).map_or_else(Vec::new, |ids| {
            ids.iter()
                .copied()
                .filter(|id| {
                    self.by_id
                        .get(id)
                        .is_some_and(|r| r.subscriber.conn_idx == conn_idx && r.pattern == pattern)
                })
                .collect()
        });
        for id in ids {
            if self.by_id.remove(&id).is_some() {
                removed += 1;
                let _ = self.trie.remove(pattern, id);
            }
        }
        if removed > 0 {
            if let Some(ids) = self.by_conn.get_mut(&conn_idx) {
                ids.retain(|x| self.by_id.contains_key(x));
                if ids.is_empty() {
                    self.by_conn.remove(&conn_idx);
                }
            }
        }
        removed
    }

    /// Resolves `topic` into the list of subscriber locations that should
    /// receive a delivery. The caller is responsible for any fan-out
    /// filtering (e.g. suppressing echo to the origin connection).
    #[must_use]
    pub fn lookup(&self, topic: &str) -> Vec<Subscriber> {
        match self.trie.lookup(topic) {
            Ok(ids) => ids
                .iter()
                .filter_map(|id| self.by_id.get(id).map(|r| r.subscriber))
                .collect(),
            // lookup only errors on internal invariant violations; treat
            // as "no matches" rather than propagating to keep the publish
            // hot path infallible.
            Err(_) => Vec::new(),
        }
    }

    /// Removes every subscription belonging to `conn_idx`. Returns the
    /// count removed. Called when a QUIC connection closes.
    pub fn remove_connection(&mut self, conn_idx: usize) -> usize {
        let ids = match self.by_conn.remove(&conn_idx) {
            Some(ids) => ids,
            None => return 0,
        };
        let removed = ids.len();
        for id in ids {
            if let Some(record) = self.by_id.remove(&id) {
                let _ = self.trie.remove(&record.pattern, id);
            }
        }
        removed
    }

    /// Like [`remove_connection`](Self::remove_connection) but also
    /// returns the distinct patterns that were removed (one entry per
    /// removed subscription, so duplicates appear when multiple subs
    /// of the same pattern were registered). Callers tracking
    /// per-pattern counts (e.g. quota enforcers) use this to keep
    /// their mirror in sync without scanning the registry.
    pub fn remove_connection_with_patterns(&mut self, conn_idx: usize) -> Vec<String> {
        let ids = match self.by_conn.remove(&conn_idx) {
            Some(ids) => ids,
            None => return Vec::new(),
        };
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(record) = self.by_id.remove(&id) {
                let _ = self.trie.remove(&record.pattern, id);
                out.push(record.pattern);
            }
        }
        out
    }

    /// Number of active subscriptions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// `true` when no subscriptions are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

impl Default for TopicRegistry {
    fn default() -> Self {
        Self::new(PubSubConfig::new())
    }
}

// ── TopicCatalog ───────────────────────────────────────────────────

/// Topic-level configuration recorded via `MessageType::TopicCreate`.
///
/// Phase 2 stores the catalog in-memory so that `TopicDelete` can
/// validate against it and operators can introspect it. Enforcement
/// (retention eviction, partition routing) lands in later phases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicConfig {
    /// Number of partitions for the topic (≥ 1).
    pub partitions: u32,
    /// Retention window in milliseconds (0 = unbounded).
    pub retention_ms: u64,
}

impl TopicConfig {
    /// Default config: single partition, no retention limit.
    pub const DEFAULT: Self = Self {
        partitions: 1,
        retention_ms: 0,
    };
}

/// In-memory catalog of created topics.
///
/// Durable persistence (WAL) and partition placement arrive in Phase 4;
/// until then the catalog is the source of truth for "does this topic
/// exist and what are its declared parameters".
#[derive(Default, Debug)]
pub struct TopicCatalog {
    topics: FxHashMap<String, TopicConfig>,
}

impl TopicCatalog {
    /// Creates an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a created topic. Overwrites any prior config for the same
    /// name (idempotent re-create is allowed).
    pub fn create(&mut self, topic: &str, config: TopicConfig) {
        self.topics.insert(topic.to_owned(), config);
    }

    /// Removes a topic. Returns `true` if it existed.
    pub fn delete(&mut self, topic: &str) -> bool {
        self.topics.remove(topic).is_some()
    }

    /// Returns the recorded config for `topic`, or `None` if no
    /// `TopicCreate` has been seen.
    #[must_use]
    pub fn get(&self, topic: &str) -> Option<TopicConfig> {
        self.topics.get(topic).copied()
    }

    /// Number of topics currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.topics.len()
    }

    /// `true` when no topics are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn sub(conn: usize, stream: u64) -> Subscriber {
        Subscriber {
            conn_idx: conn,
            quic_stream_id: stream,
        }
    }

    fn t(s: &str) -> &str {
        s
    }

    #[test]
    fn empty_registry_has_zero_len() {
        let r = TopicRegistry::new(PubSubConfig::new());
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
        assert!(r.lookup(&t("any.topic")).is_empty());
    }

    #[test]
    fn subscribe_returns_unique_ids() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        let a = r.subscribe(sub(0, 1), "sensor.*").unwrap();
        let b = r.subscribe(sub(0, 2), "alert.*").unwrap();
        assert_ne!(a, b);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn lookup_finds_matching_subscriber() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(7, 100), "sensor.temp").unwrap();
        r.subscribe(sub(8, 200), "sensor.humidity").unwrap();

        let hits = r.lookup(&t("sensor.temp"));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0], sub(7, 100));
    }

    #[test]
    fn lookup_wildcard_matches_multiple() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(1, 10), "sensor.*").unwrap();
        r.subscribe(sub(2, 20), "sensor.*").unwrap();
        r.subscribe(sub(3, 30), "alert.*").unwrap();

        let hits = r.lookup(&t("sensor.temp"));
        assert_eq!(hits.len(), 2);
        let conns: Vec<_> = hits.iter().map(|h| h.conn_idx).collect();
        assert!(conns.contains(&1));
        assert!(conns.contains(&2));
    }

    #[test]
    fn unsubscribe_removes_from_lookup() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        let id = r.subscribe(sub(0, 5), "sensor.*").unwrap();
        assert_eq!(r.lookup(&t("sensor.x")).len(), 1);

        r.unsubscribe(id).unwrap();
        assert!(r.lookup(&t("sensor.x")).is_empty());
        assert!(r.is_empty());
    }

    #[test]
    fn unsubscribe_unknown_id_is_noop() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(0, 1), "t").unwrap();
        // Unknown id should not panic.
        r.unsubscribe(SubscriptionId::new(999)).unwrap();
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn unsubscribe_pattern_removes_only_match() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(0, 1), "sensor.*").unwrap();
        r.subscribe(sub(0, 1), "alert.*").unwrap();

        // Remove only "sensor.*" for conn 0.
        let removed = r.unsubscribe_pattern(0, "sensor.*");
        assert_eq!(removed, 1);
        assert!(r.lookup(&t("sensor.temp")).is_empty());
        assert_eq!(r.lookup(&t("alert.fire")).len(), 1);
    }

    #[test]
    fn unsubscribe_pattern_only_affects_caller_connection() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(0, 1), "sensor.*").unwrap();
        r.subscribe(sub(1, 1), "sensor.*").unwrap();

        // Conn 0 unsubscribes; conn 1 should still receive matches.
        let removed = r.unsubscribe_pattern(0, "sensor.*");
        assert_eq!(removed, 1);
        let hits = r.lookup(&t("sensor.temp"));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].conn_idx, 1);
    }

    #[test]
    fn remove_connection_clears_all_its_subscriptions() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(0, 1), "a.*").unwrap();
        r.subscribe(sub(0, 2), "b.*").unwrap();
        r.subscribe(sub(1, 1), "a.*").unwrap();

        let removed = r.remove_connection(0);
        assert_eq!(removed, 2);
        assert_eq!(r.len(), 1);
        // Conn 1's subscription survives.
        assert_eq!(r.lookup(&t("a.x")).len(), 1);
        assert_eq!(r.lookup(&t("a.x"))[0].conn_idx, 1);
    }

    #[test]
    fn remove_connection_with_no_subscriptions_is_zero() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(1, 1), "t").unwrap();
        assert_eq!(r.remove_connection(99), 0);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn duplicate_subscribes_for_same_conn_pattern_produce_unique_ids() {
        // A client that subscribes twice to the same pattern should get
        // two deliveries (one per registration) — matching NATS / MQTT
        // semantics where duplicate subscriptions are independent.
        let mut r = TopicRegistry::new(PubSubConfig::new());
        let a = r.subscribe(sub(0, 1), "t.*").unwrap();
        let b = r.subscribe(sub(0, 1), "t.*").unwrap();
        assert_ne!(a, b);

        let hits = r.lookup(&t("t.x"));
        assert_eq!(hits.len(), 2, "both registrations should match");
    }

    #[test]
    fn empty_pattern_rejected() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        assert!(r.subscribe(sub(0, 1), "").is_err());
        assert!(r.is_empty());
    }

    #[test]
    fn debug_format_does_not_panic() {
        let mut r = TopicRegistry::new(PubSubConfig::new());
        r.subscribe(sub(0, 1), "t").unwrap();
        let _ = format!("{r:?}");
    }

    // ── TopicCatalog ──────────────────────────────────────────────

    #[test]
    fn catalog_create_then_get() {
        let mut c = TopicCatalog::new();
        assert!(c.is_empty());
        c.create(
            "metrics",
            TopicConfig {
                partitions: 4,
                retention_ms: 1000,
            },
        );
        assert_eq!(c.len(), 1);
        let cfg = c.get("metrics").unwrap();
        assert_eq!(cfg.partitions, 4);
        assert_eq!(cfg.retention_ms, 1000);
    }

    #[test]
    fn catalog_delete_returns_prior_existence() {
        let mut c = TopicCatalog::new();
        c.create("a", TopicConfig::DEFAULT);
        assert!(c.delete("a"));
        assert!(!c.delete("a"));
        assert!(c.is_empty());
    }

    #[test]
    fn catalog_create_overwrites_prior_config() {
        let mut c = TopicCatalog::new();
        c.create(
            "t",
            TopicConfig {
                partitions: 1,
                retention_ms: 0,
            },
        );
        c.create(
            "t",
            TopicConfig {
                partitions: 8,
                retention_ms: 5000,
            },
        );
        let cfg = c.get("t").unwrap();
        assert_eq!(cfg.partitions, 8);
        assert_eq!(cfg.retention_ms, 5000);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn catalog_get_unknown_returns_none() {
        let c = TopicCatalog::new();
        assert!(c.get("nope").is_none());
    }
}
