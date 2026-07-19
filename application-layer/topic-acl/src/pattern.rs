//! Topic-pattern matching — single-segment `*` wildcard.
//!
//! Identical semantics to
//! `cluster_coordinator::subscriber::pattern_matches` and
//! `pubsub_engine::TopicPattern`. Re-implemented locally so this
//! crate stays standalone (no cross-crate dep).

/// Whether `pattern` matches `topic`.
///
/// # Semantics
/// * Split both on `.`.
/// * Equal segment count required.
/// * Each segment matches exactly, OR the pattern segment is `*`.
///
/// # Examples
/// ```
/// use topic_acl::pattern_matches;
/// assert!(pattern_matches("sensor.*", "sensor.temp"));
/// assert!(!pattern_matches("sensor.*", "sensor.temp.room1"));
/// assert!(pattern_matches("a.b.c", "a.b.c"));
/// ```
#[must_use]
pub fn pattern_matches(pattern: &str, topic: &str) -> bool {
    let pat_count = pattern.split('.').count();
    let top_count = topic.split('.').count();
    if pat_count != top_count {
        return false;
    }
    pattern
        .split('.')
        .zip(topic.split('.'))
        .all(|(p, t)| p == "*" || p == t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(pattern_matches("sensor.temp", "sensor.temp"));
        assert!(!pattern_matches("sensor.temp", "sensor.humidity"));
    }

    #[test]
    fn single_segment_wildcard() {
        assert!(pattern_matches("sensor.*", "sensor.temp"));
        assert!(pattern_matches("sensor.*", "sensor.humidity"));
        assert!(pattern_matches("*", "anything"));
    }

    #[test]
    fn wildcard_does_not_span_segments() {
        assert!(!pattern_matches("sensor.*", "sensor"));
        assert!(!pattern_matches("sensor.*", "sensor.temp.room1"));
    }

    #[test]
    fn multi_wildcard_patterns() {
        assert!(pattern_matches("*.*", "a.b"));
        assert!(!pattern_matches("*.*", "a.b.c"));
        assert!(pattern_matches("*.*.*", "a.b.c"));
        assert!(pattern_matches("a.*.c", "a.b.c"));
        assert!(!pattern_matches("a.*.c", "a.b.d"));
    }

    #[test]
    fn empty_pattern_matches_only_empty_topic() {
        assert!(pattern_matches("", ""));
        assert!(!pattern_matches("", "x"));
        assert!(!pattern_matches("x", ""));
    }

    #[test]
    fn pattern_with_no_wildcard_is_exact() {
        assert!(pattern_matches("a.b.c", "a.b.c"));
        assert!(!pattern_matches("a.b.c", "a.b.d"));
        assert!(!pattern_matches("a.b.c", "a.b"));
    }

    #[test]
    fn trailing_dot_creates_empty_segment() {
        // `"a.".split('.')` → ["a", ""] (two segments).
        assert!(pattern_matches("a.", "a."));
        assert!(!pattern_matches("a.", "a"));
    }
}
