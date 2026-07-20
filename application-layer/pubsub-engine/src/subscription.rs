//! [`SubscriptionId`], [`TopicPattern`] — subscription identifiers
//! and topic pattern matching.
//!
//! See `DESIGN.md` Part 4 (subscriptions) and Part 5 (topic patterns).

use crate::error::PubSubError;
use crate::topic::Topic;
use std::fmt;

// ── SubscriptionId ───────────────────────────────────────────────

/// Unique identifier for a subscription.
///
/// Subscriptions are the core abstraction in the pub/sub engine.
/// Each subscription is associated with a topic pattern and receives
/// messages matching that pattern.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    /// The reserved nil value (0).
    pub const NIL: Self = Self(0);

    /// Construct a new subscription ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Construct the nil subscription ID (0).
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil subscription ID.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw underlying value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::NIL
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sub#{}", self.0)
    }
}

// ── TopicPattern ────────────────────────────────────────────────

/// A topic pattern for subscription matching.
///
/// Patterns support simple wildcard matching with `*`:
/// - `*` matches any single segment
/// - `sensor.*` matches `sensor.temp`, `sensor.humidity`, etc.
/// - `*.*` matches any two-segment topic
/// - Literal segments match exactly
///
/// The wildcard `*` matches a single dot-delimited segment. There
/// is no recursive `#` wildcard in Phase 0.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicPattern {
    /// The raw pattern string.
    pattern: String,
}

impl TopicPattern {
    /// Create a new topic pattern from a string slice.
    ///
    /// # Errors
    /// Returns [`PubSubError::PublishFailed`] if the pattern is empty
    /// or exceeds 256 bytes.
    pub fn new(pattern: &str) -> Result<Self, PubSubError> {
        if pattern.is_empty() {
            return Err(PubSubError::PublishFailed("topic pattern cannot be empty"));
        }
        if pattern.len() > 256 {
            return Err(PubSubError::PublishFailed(
                "topic pattern exceeds 256 bytes",
            ));
        }
        Ok(Self {
            pattern: pattern.to_owned(),
        })
    }

    /// Get the raw pattern string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.pattern
    }

    /// Check whether a concrete topic matches this pattern.
    ///
    /// Matching rules:
    /// - Split both pattern and topic by `.`.
    /// - Each `*` segment in the pattern matches any single segment
    ///   in the topic.
    /// - Non-wildcard segments must match exactly.
    /// - The number of segments must be equal.
    #[must_use]
    pub fn matches(&self, topic: &Topic) -> bool {
        let pat_segments: Vec<&str> = self.pattern.split('.').collect();
        let topic_segments: Vec<&str> = topic.as_str().split('.').collect();

        if pat_segments.len() != topic_segments.len() {
            return false;
        }

        for (pat, topic) in pat_segments.iter().zip(topic_segments.iter()) {
            if *pat != "*" && *pat != *topic {
                return false;
            }
        }
        true
    }
}

impl fmt::Display for TopicPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pattern)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn t(s: &str) -> Topic {
        Topic::new(s).unwrap()
    }

    // ── SubscriptionId tests ─────────────────────────────────────

    #[test]
    fn subscription_id_new() {
        let id = SubscriptionId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn subscription_id_nil() {
        let id = SubscriptionId::nil();
        assert_eq!(id.raw(), 0);
        assert!(id.is_nil());
    }

    #[test]
    fn subscription_id_nil_constant() {
        assert_eq!(SubscriptionId::NIL, SubscriptionId::nil());
        assert!(SubscriptionId::NIL.is_nil());
    }

    #[test]
    fn subscription_id_default_is_nil() {
        let id = SubscriptionId::default();
        assert_eq!(id, SubscriptionId::NIL);
        assert!(id.is_nil());
    }

    #[test]
    fn subscription_id_equality() {
        let id1 = SubscriptionId::new(42);
        let id2 = SubscriptionId::new(42);
        let id3 = SubscriptionId::new(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn subscription_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(SubscriptionId::new(1));
        set.insert(SubscriptionId::new(2));
        set.insert(SubscriptionId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn subscription_id_display() {
        let id = SubscriptionId::new(42);
        assert_eq!(format!("{id}"), "sub#42");
    }

    #[test]
    fn subscription_id_display_nil() {
        let id = SubscriptionId::nil();
        assert_eq!(format!("{id}"), "sub#0");
    }

    // ── TopicPattern tests ───────────────────────────────────────

    #[test]
    fn topic_pattern_new_valid() {
        let pat = TopicPattern::new("sensor.temp").unwrap();
        assert_eq!(pat.as_str(), "sensor.temp");
    }

    #[test]
    fn topic_pattern_new_empty_rejected() {
        assert!(TopicPattern::new("").is_err());
    }

    #[test]
    fn topic_pattern_new_too_long_rejected() {
        let long = "a".repeat(257);
        assert!(TopicPattern::new(&long).is_err());
    }

    #[test]
    fn topic_pattern_new_max_length_ok() {
        let max = "a".repeat(256);
        assert!(TopicPattern::new(&max).is_ok());
    }

    #[test]
    fn topic_pattern_matches_exact() {
        let pat = TopicPattern::new("sensor.temp").unwrap();
        assert!(pat.matches(&t("sensor.temp")));
        assert!(!pat.matches(&t("sensor.humidity")));
        assert!(!pat.matches(&t("sensor.temp.high")));
    }

    #[test]
    fn topic_pattern_matches_single_wildcard() {
        let pat = TopicPattern::new("sensor.*").unwrap();
        assert!(pat.matches(&t("sensor.temp")));
        assert!(pat.matches(&t("sensor.humidity")));
        assert!(!pat.matches(&t("sensor.temp.high")));
        assert!(!pat.matches(&t("sensor")));
    }

    #[test]
    fn topic_pattern_matches_all_wildcards() {
        let pat = TopicPattern::new("*.*").unwrap();
        assert!(pat.matches(&t("sensor.temp")));
        assert!(pat.matches(&t("alert.fire")));
        assert!(!pat.matches(&t("sensor.temp.high")));
        assert!(!pat.matches(&t("sensor")));
    }

    #[test]
    fn topic_pattern_matches_single_segment() {
        let pat = TopicPattern::new("*").unwrap();
        assert!(pat.matches(&t("sensor")));
        assert!(pat.matches(&t("anything")));
        assert!(!pat.matches(&t("sensor.temp")));
    }

    #[test]
    fn topic_pattern_matches_literal_single_segment() {
        let pat = TopicPattern::new("alerts").unwrap();
        assert!(pat.matches(&t("alerts")));
        assert!(!pat.matches(&t("sensor")));
        assert!(!pat.matches(&t("alerts.fire")));
    }

    #[test]
    fn topic_pattern_matches_mixed_wildcards() {
        let pat = TopicPattern::new("sensor.*.high").unwrap();
        assert!(pat.matches(&t("sensor.temp.high")));
        assert!(pat.matches(&t("sensor.humidity.high")));
        assert!(!pat.matches(&t("sensor.temp.low")));
        assert!(!pat.matches(&t("sensor.temp.high.extra")));
    }

    #[test]
    fn topic_pattern_matches_multi_segment_wildcards() {
        let pat = TopicPattern::new("*.*.*").unwrap();
        assert!(pat.matches(&t("a.b.c")));
        assert!(pat.matches(&t("x.y.z")));
        assert!(!pat.matches(&t("a.b")));
        assert!(!pat.matches(&t("a.b.c.d")));
    }

    #[test]
    fn topic_pattern_no_match_different_segment_count() {
        let pat = TopicPattern::new("a.b.c").unwrap();
        assert!(!pat.matches(&t("a.b")));
        assert!(!pat.matches(&t("a.b.c.d")));
    }

    #[test]
    fn topic_pattern_display() {
        let pat = TopicPattern::new("sensor.*").unwrap();
        assert_eq!(format!("{pat}"), "sensor.*");
    }

    #[test]
    fn topic_pattern_equality() {
        let p1 = TopicPattern::new("sensor.*").unwrap();
        let p2 = TopicPattern::new("sensor.*").unwrap();
        let p3 = TopicPattern::new("sensor.temp").unwrap();
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }
}
