//! [`PubSubMessage`] — message type for pub/sub communication.
//!
//! See `DESIGN.md` Part 7 (message model).

use std::collections::BTreeMap;

use crate::topic::Topic;

// ── PubSubMessage ───────────────────────────────────────────────

/// A message published to a topic in the pub/sub engine.
///
/// Messages carry a validated [`Topic`], a binary payload, a timestamp,
/// and optional headers. The topic is used by the engine to route
/// the message to matching subscriptions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PubSubMessage {
    /// The topic this message is published to.
    pub topic: Topic,
    /// The binary payload.
    pub payload: Vec<u8>,
    /// Timestamp in milliseconds since epoch.
    pub timestamp_ms: u64,
    /// Optional headers (key-value pairs).
    pub headers: BTreeMap<String, String>,
}

impl PubSubMessage {
    /// Create a new pub/sub message with the given topic and payload.
    ///
    /// The timestamp is set to 0; callers should set it externally.
    /// Headers are initialized as empty.
    #[must_use]
    pub fn new(topic: Topic, payload: Vec<u8>) -> Self {
        Self {
            topic,
            payload,
            timestamp_ms: 0,
            headers: BTreeMap::new(),
        }
    }

    /// Create a new pub/sub message with a timestamp.
    #[must_use]
    pub fn with_timestamp(topic: Topic, payload: Vec<u8>, timestamp_ms: u64) -> Self {
        Self {
            topic,
            payload,
            timestamp_ms,
            headers: BTreeMap::new(),
        }
    }

    /// Add a header to the message.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Get the payload size in bytes.
    #[must_use]
    pub fn payload_size(&self) -> usize {
        self.payload.len()
    }

    /// Get a header value by key.
    #[must_use]
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(|s| s.as_str())
    }

    /// Get the topic as a typed reference.
    #[must_use]
    pub fn topic(&self) -> &Topic {
        &self.topic
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn t(s: &str) -> Topic {
        Topic::new(s).unwrap()
    }

    #[test]
    fn new_message() {
        let msg = PubSubMessage::new(t("sensor.temp"), vec![1, 2, 3]);
        assert_eq!(msg.topic.as_str(), "sensor.temp");
        assert_eq!(msg.payload, vec![1, 2, 3]);
        assert_eq!(msg.timestamp_ms, 0);
        assert!(msg.headers.is_empty());
    }

    #[test]
    fn new_with_timestamp() {
        let msg = PubSubMessage::with_timestamp(t("alerts"), vec![42], 1_700_000_000_000);
        assert_eq!(msg.topic.as_str(), "alerts");
        assert_eq!(msg.payload, vec![42]);
        assert_eq!(msg.timestamp_ms, 1_700_000_000_000);
    }

    #[test]
    fn with_header_chains() {
        let msg = PubSubMessage::new(t("topic"), vec![])
            .with_header("source", "sensor-1")
            .with_header("priority", "high");
        assert_eq!(msg.header("source"), Some("sensor-1"));
        assert_eq!(msg.header("priority"), Some("high"));
        assert_eq!(msg.header("missing"), None);
    }

    #[test]
    fn payload_size() {
        let msg = PubSubMessage::new(t("topic"), vec![0; 1024]);
        assert_eq!(msg.payload_size(), 1024);
    }

    #[test]
    fn empty_payload() {
        let msg = PubSubMessage::new(t("topic"), vec![]);
        assert_eq!(msg.payload_size(), 0);
    }

    #[test]
    fn topic_accessor() {
        let msg = PubSubMessage::new(t("sensor.temp"), vec![]);
        assert_eq!(msg.topic().as_str(), "sensor.temp");
    }

    #[test]
    fn message_equality() {
        let m1 = PubSubMessage::with_timestamp(t("a"), vec![1], 100);
        let m2 = PubSubMessage::with_timestamp(t("a"), vec![1], 100);
        let m3 = PubSubMessage::with_timestamp(t("a"), vec![1], 200);
        assert_eq!(m1, m2);
        assert_ne!(m1, m3);
    }

    #[test]
    fn message_clone() {
        let m1 = PubSubMessage::new(t("topic"), vec![1, 2, 3]);
        let m2 = m1.clone();
        assert_eq!(m1, m2);
    }

    #[test]
    fn headers_ordered() {
        let msg = PubSubMessage::new(t("topic"), vec![])
            .with_header("zebra", "z")
            .with_header("apple", "a")
            .with_header("mango", "m");
        // BTreeMap maintains sorted order.
        let keys: Vec<&str> = msg.headers.keys().map(|s| s.as_str()).collect();
        assert_eq!(keys, vec!["apple", "mango", "zebra"]);
    }
}
