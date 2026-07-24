//! [`MemoryStore`] — in-process backend for tests, dev, and ephemeral
//! workloads.
//!
//! Stores each topic as a `Vec<WalEntry>` plus a `retained_bytes` sum.
//! Truncation moves a `head` cursor forward without compacting the
//! vector — the storage is reclaimed only when the topic is dropped
//! (or via explicit `clear`).
//!
//! This matches the file backend's semantics: truncate does not
//! renumber offsets.

use rustc_hash::FxHashMap;

use crate::entry::{WalEntry, WalRecord};
use crate::error::WalError;
use crate::offset::Offset;
use crate::store::{WalStore, validate_topic};

/// In-memory [`WalStore`] implementation.
#[derive(Debug, Default)]
pub struct MemoryStore {
    topics: FxHashMap<String, TopicLog>,
}

#[derive(Debug, Default)]
struct TopicLog {
    entries: Vec<WalEntry>,
    /// Index in `entries` of the oldest *retained* entry. Truncate
    /// advances this without compacting `entries`.
    oldest_idx: usize,
    /// Next offset to assign on append. Monotonic across truncates —
    /// truncation does not renumber.
    next_offset: u64,
    /// Sum of `WalEntry::wire_len` for entries `[oldest_idx..entries.len())`.
    retained_bytes: u64,
}

impl TopicLog {
    fn offset_to_idx(&self, offset: Offset) -> Option<usize> {
        if offset.is_none() {
            return None;
        }
        // Offset of entries[i] is `next_offset - (entries.len() - i)`.
        let head_offset = self.head_offset();
        if offset.get() > head_offset.get() {
            return None;
        }
        if offset.get() < self.oldest_offset().get() {
            return None;
        }
        // idx = i such that entries[i].offset.get() == offset.get().
        // Since the vector is dense and offsets are contiguous, we can
        // compute idx directly:
        //   entries[i].offset = next_offset - (entries.len() - i)
        //   i = entries.len() - (next_offset - offset)
        let idx = self.entries.len() as u64 - (self.next_offset - offset.get());
        Some(idx as usize)
    }

    fn head_offset(&self) -> Offset {
        if self.next_offset == 0 {
            Offset::NONE
        } else {
            Offset::new(self.next_offset - 1)
        }
    }

    fn oldest_offset(&self) -> Offset {
        if self.oldest_idx >= self.entries.len() {
            Offset::NONE
        } else {
            self.entries[self.oldest_idx].offset
        }
    }
}

impl MemoryStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_create(&mut self, topic: &str) -> &mut TopicLog {
        // Entry API: `or_default_with_key` would let us avoid cloning
        // `topic` on the existing-key path, but FxHashMap does not
        // expose that. We pay the small clone in the rare create path.
        self.topics.entry(topic.to_owned()).or_default()
    }
}

impl WalStore for MemoryStore {
    fn append(&mut self, topic: &str, record: WalRecord) -> Result<Offset, WalError> {
        validate_topic(topic)?;
        let log = self.get_or_create(topic);
        let offset = Offset::new(log.next_offset);
        log.next_offset += 1;
        let entry = record.into_entry(offset);
        log.retained_bytes += entry.wire_len() as u64;
        log.entries.push(entry);
        Ok(offset)
    }

    fn read(&self, topic: &str, offset: Offset) -> Result<WalEntry, WalError> {
        validate_topic(topic)?;
        let Some(log) = self.topics.get(topic) else {
            return Err(WalError::TopicEmpty {
                topic: topic.to_owned(),
            });
        };
        if log.entries.is_empty() {
            return Err(WalError::TopicEmpty {
                topic: topic.to_owned(),
            });
        }
        let Some(idx) = log.offset_to_idx(offset) else {
            return Err(WalError::OffsetOutOfRange {
                requested: offset.get(),
                oldest: log.oldest_offset().get(),
                head: log.head_offset().get(),
            });
        };
        Ok(log.entries[idx].clone())
    }

    fn read_range(
        &self,
        topic: &str,
        from: Offset,
        until: Offset,
        out: &mut Vec<WalEntry>,
    ) -> Result<usize, WalError> {
        validate_topic(topic)?;
        let Some(log) = self.topics.get(topic) else {
            return Ok(0);
        };
        if log.entries.is_empty() {
            return Ok(0);
        }
        let oldest_o = log.oldest_offset();
        let head_o = log.head_offset();
        // Treat `from` = NONE as "oldest retained".
        let from = if from.is_none() { oldest_o } else { from };
        if from.get() > head_o.get() {
            return Ok(0);
        }
        if from.get() < oldest_o.get() {
            return Err(WalError::OffsetOutOfRange {
                requested: from.get(),
                oldest: oldest_o.get(),
                head: head_o.get(),
            });
        }
        // `until` inclusive when set; when NONE, walk to head.
        let until_inclusive = if until.is_none() { head_o } else { until };
        // `offset_to_idx(from)` is guaranteed Some because we validated
        // `from` is in [oldest..=head] above. Use `map_or` to avoid the
        // unwrap_used / expect_used workspace lints.
        let start = log.offset_to_idx(from).map_or(0, |i| i);
        let end = log
            .offset_to_idx(until_inclusive)
            .map_or(log.entries.len(), |i| i + 1);
        let added = end.saturating_sub(start);
        out.extend(log.entries[start..end].iter().cloned());
        Ok(added)
    }

    fn oldest(&self, topic: &str) -> Offset {
        self.topics
            .get(topic)
            .map_or(Offset::NONE, TopicLog::oldest_offset)
    }

    fn head(&self, topic: &str) -> Offset {
        self.topics
            .get(topic)
            .map_or(Offset::NONE, TopicLog::head_offset)
    }

    fn len(&self, topic: &str) -> usize {
        let Some(log) = self.topics.get(topic) else {
            return 0;
        };
        log.entries.len().saturating_sub(log.oldest_idx)
    }

    fn truncate(&mut self, topic: &str, before_offset: Offset) -> Result<usize, WalError> {
        validate_topic(topic)?;
        let Some(log) = self.topics.get_mut(topic) else {
            return Ok(0);
        };
        if log.entries.is_empty() {
            return Ok(0);
        }
        let head_o = log.head_offset();
        if before_offset.get() > head_o.get() {
            // Truncate everything.
            let removed = log.entries.len() - log.oldest_idx;
            log.retained_bytes = 0;
            log.entries.clear();
            log.oldest_idx = 0;
            return Ok(removed);
        }
        let Some(target_idx) = log.offset_to_idx(before_offset) else {
            return Ok(0);
        };
        let removed = target_idx - log.oldest_idx;
        for entry in &log.entries[log.oldest_idx..target_idx] {
            log.retained_bytes = log.retained_bytes.saturating_sub(entry.wire_len() as u64);
        }
        log.oldest_idx = target_idx;
        Ok(removed)
    }

    fn retained_bytes(&self) -> u64 {
        self.topics.values().map(|t| t.retained_bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;

    fn rec(payload: &[u8]) -> WalRecord {
        WalRecord::new(0, Bytes::copy_from_slice(payload))
    }

    #[test]
    fn append_assigns_monotonic_offsets() {
        let mut s = MemoryStore::new();
        let o0 = s.append("t", rec(b"a")).unwrap();
        let o1 = s.append("t", rec(b"b")).unwrap();
        let o2 = s.append("t", rec(b"c")).unwrap();
        assert_eq!(o0, Offset::new(0));
        assert_eq!(o1, Offset::new(1));
        assert_eq!(o2, Offset::new(2));
        assert_eq!(s.head("t"), Offset::new(2));
        assert_eq!(s.oldest("t"), Offset::new(0));
        assert_eq!(s.len("t"), 3);
    }

    #[test]
    fn read_returns_correct_payload() {
        let mut s = MemoryStore::new();
        let _ = s.append("t", rec(b"hello")).unwrap();
        let e = s.read("t", Offset::new(0)).unwrap();
        assert_eq!(e.payload.as_ref(), b"hello");
    }

    #[test]
    fn read_unknown_topic_errors() {
        let s = MemoryStore::new();
        assert!(matches!(
            s.read("nope", Offset::new(0)),
            Err(WalError::TopicEmpty { .. })
        ));
    }

    #[test]
    fn read_out_of_range_errors() {
        let mut s = MemoryStore::new();
        let _ = s.append("t", rec(b"x")).unwrap();
        assert!(matches!(
            s.read("t", Offset::new(5)),
            Err(WalError::OffsetOutOfRange { .. })
        ));
    }

    #[test]
    fn read_range_walks_in_order() {
        let mut s = MemoryStore::new();
        for i in 0..5u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        let mut out = Vec::new();
        let n = s
            .read_range("t", Offset::new(1), Offset::new(3), &mut out)
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(out[0].payload.as_ref(), &[1]);
        assert_eq!(out[1].payload.as_ref(), &[2]);
        assert_eq!(out[2].payload.as_ref(), &[3]);
    }

    #[test]
    fn read_range_none_until_means_to_head() {
        let mut s = MemoryStore::new();
        for i in 0..3u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        let mut out = Vec::new();
        let n = s
            .read_range("t", Offset::new(0), Offset::NONE, &mut out)
            .unwrap();
        assert_eq!(n, 3);
    }

    #[test]
    fn truncate_below_oldest_noop() {
        let mut s = MemoryStore::new();
        let _ = s.append("t", rec(b"x")).unwrap();
        // Truncate at offset 5 — beyond head; everything goes.
        let removed = s.truncate("t", Offset::new(5)).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(s.len("t"), 0);
    }

    #[test]
    fn truncate_advances_oldest_without_renumbering() {
        let mut s = MemoryStore::new();
        for i in 0..5u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        // Remove offsets [0..3) — i.e. before offset 3.
        let removed = s.truncate("t", Offset::new(3)).unwrap();
        assert_eq!(removed, 3);
        assert_eq!(s.oldest("t"), Offset::new(3));
        assert_eq!(s.len("t"), 2);
        // Head offset is unchanged.
        assert_eq!(s.head("t"), Offset::new(4));
        // Read at offset 3 returns the right entry.
        let e = s.read("t", Offset::new(3)).unwrap();
        assert_eq!(e.payload.as_ref(), &[3]);
        // Read at offset 2 — truncated — should error.
        assert!(matches!(
            s.read("t", Offset::new(2)),
            Err(WalError::OffsetOutOfRange { .. })
        ));
    }

    #[test]
    fn retained_bytes_reflects_truncation() {
        let mut s = MemoryStore::new();
        for i in 0..5u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        let before = s.retained_bytes();
        let _ = s.truncate("t", Offset::new(3)).unwrap();
        let after = s.retained_bytes();
        assert!(after < before, "truncate must reduce retained bytes");
        assert!(after > 0, "two entries should still be retained");
    }

    #[test]
    fn truncate_then_next_append_keeps_offsets_monotonic() {
        let mut s = MemoryStore::new();
        for i in 0..3u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        let _ = s.truncate("t", Offset::new(3)).unwrap();
        // Now append — offset should be 3 (next_offset was already 3).
        let o = s.append("t", rec(b"d")).unwrap();
        assert_eq!(o, Offset::new(3));
        assert_eq!(s.head("t"), Offset::new(3));
        assert_eq!(s.len("t"), 1);
    }

    #[test]
    fn invalid_topic_rejected() {
        let mut s = MemoryStore::new();
        assert!(matches!(
            s.append("", rec(b"x")),
            Err(WalError::InvalidTopic { .. })
        ));
        assert!(matches!(
            s.append("bad/name", rec(b"x")),
            Err(WalError::InvalidTopic { .. })
        ));
    }
}
