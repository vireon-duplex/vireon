//! [`RetentionPolicy`] — declarative rules that drive the sweeper.
//!
//! Two independent policies are supported; both can be active at once:
//!
//! * **By bytes** — once a topic's retained size exceeds `max_bytes`,
//!   the sweeper truncates the oldest entries until size drops below
//!   the cap. Bounded total memory use.
//! * **By age** — entries older than `max_age_ns` are truncated.
//!   Bounded temporal retention.
//!
//! The sweeper is invoked manually by the runtime's tick loop — there
//! is no background thread. The runtime decides the cadence (typically
//! once per second per topic) so the policy itself stays synchronous
//! and cheap.

use crate::entry::WalEntry;
use crate::offset::Offset;
use crate::store::WalStore;

/// Declarative retention rules. Both fields are optional; an absent
/// field means "no limit on that dimension".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Maximum total retained bytes per topic. `None` = unlimited.
    pub max_bytes: Option<u64>,
    /// Maximum entry age in nanoseconds. `None` = unlimited. Entries
    /// whose `timestamp_ns` is older than `now - max_age_ns` are
    /// candidates for truncation.
    pub max_age_ns: Option<u64>,
}

impl RetentionPolicy {
    /// No retention — keep everything forever.
    pub const fn keep_forever() -> Self {
        Self {
            max_bytes: None,
            max_age_ns: None,
        }
    }

    /// Cap by bytes only.
    pub const fn by_bytes(max_bytes: u64) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            max_age_ns: None,
        }
    }

    /// Cap by age only.
    pub const fn by_age(max_age_ns: u64) -> Self {
        Self {
            max_bytes: None,
            max_age_ns: Some(max_age_ns),
        }
    }

    /// Apply this policy to `topic` on `store`. Returns the number of
    /// entries removed and the new oldest offset (or `Offset::NONE`
    /// when the topic is empty after the sweep).
    ///
    /// `now_ns` is the current time in nanoseconds since UNIX epoch.
    /// The caller is responsible for sourcing it consistently —
    /// typically `time::now()` or a reactor-managed clock.
    pub fn apply<S: WalStore + ?Sized>(
        &self,
        store: &mut S,
        topic: &str,
        now_ns: u64,
    ) -> Result<RetentionStats, crate::error::WalError> {
        if self.max_bytes.is_none() && self.max_age_ns.is_none() {
            // Nothing to do — bail before any read.
            return Ok(RetentionStats {
                removed: 0,
                new_oldest: store.oldest(topic),
            });
        }

        let head = store.head(topic);
        if head.is_none() {
            return Ok(RetentionStats {
                removed: 0,
                new_oldest: Offset::NONE,
            });
        }

        // Phase 1: age-based truncation. Walk forward from oldest,
        // finding the first entry within the age window. Everything
        // before it is a candidate.
        let age_cutoff = self.max_age_ns.map(|a| now_ns.saturating_sub(a));
        let mut scan = Vec::new();
        let _ = store.read_range(topic, store.oldest(topic), head, &mut scan)?;
        let age_remove_until: Option<Offset> = age_cutoff.map(|cutoff| {
            // Walk in order; once we find an entry within the window,
            // everything older can go.
            for e in &scan {
                if e.timestamp_ns >= cutoff {
                    return e.offset;
                }
            }
            // Everything is older than the cutoff — truncate past head.
            head.next()
        });

        // Apply age truncation first.
        let mut removed = 0;
        if let Some(until) = age_remove_until {
            removed += store.truncate(topic, until)?;
        }

        // Phase 2: byte-based truncation. After age truncation, size
        // may still exceed `max_bytes`. Walk forward accumulating the
        // cutoff offset, then issue a **single** `truncate` call —
        // with segment-based stores this drops whole segment files in
        // O(1) rather than rewriting per entry.
        if let Some(cap) = self.max_bytes {
            // After age truncation, the read range may be stale.
            scan.clear();
            let _ = store.read_range(topic, store.oldest(topic), store.head(topic), &mut scan)?;
            let mut total: u64 = scan.iter().map(WalEntry::wire_len).map(|n| n as u64).sum();
            if total > cap && !scan.is_empty() {
                let mut cutoff = Offset::NONE;
                for e in &scan {
                    if total <= cap {
                        break;
                    }
                    total = total.saturating_sub(e.wire_len() as u64);
                    cutoff = e.offset.next();
                }
                if cutoff != Offset::NONE {
                    removed += store.truncate(topic, cutoff)?;
                }
            }
        }

        Ok(RetentionStats {
            removed,
            new_oldest: store.oldest(topic),
        })
    }
}

/// Outcome of a retention sweep.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionStats {
    /// Total entries removed by the sweep.
    pub removed: usize,
    /// Oldest retained offset after the sweep. `Offset::NONE` when the
    /// topic is now empty.
    pub new_oldest: Offset,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::entry::WalRecord;
    use crate::memory::MemoryStore;
    use bytes::Bytes;

    fn rec(ts: u64, payload: &[u8]) -> WalRecord {
        WalRecord::new(ts, Bytes::copy_from_slice(payload))
    }

    #[test]
    fn keep_forever_drops_nothing() {
        let mut s = MemoryStore::new();
        for i in 0..10u8 {
            let _ = s.append("t", rec(0, &[i])).unwrap();
        }
        let stats = RetentionPolicy::keep_forever()
            .apply(&mut s, "t", 1_000_000_000)
            .unwrap();
        assert_eq!(stats.removed, 0);
        assert_eq!(stats.new_oldest, Offset::new(0));
        assert_eq!(s.len("t"), 10);
    }

    #[test]
    fn by_age_removes_old_entries() {
        let mut s = MemoryStore::new();
        // Five entries at ts=0, five at ts=100.
        for i in 0..5u8 {
            let _ = s.append("t", rec(0, &[i])).unwrap();
        }
        for i in 5..10u8 {
            let _ = s.append("t", rec(100, &[i])).unwrap();
        }
        // Now = 110; max_age = 50 → cutoff = 60.
        // The five ts=0 entries are below cutoff; ts=100 are above.
        let stats = RetentionPolicy::by_age(50).apply(&mut s, "t", 110).unwrap();
        assert_eq!(stats.removed, 5);
        assert_eq!(stats.new_oldest, Offset::new(5));
        assert_eq!(s.len("t"), 5);
        assert_eq!(s.read("t", Offset::new(5)).unwrap().payload.as_ref(), &[5]);
    }

    #[test]
    fn by_bytes_removes_until_under_cap() {
        let mut s = MemoryStore::new();
        // Each entry is ~38 bytes wire (header 18 + payload 1 + key 0 + crc 4).
        // Cap at 200 bytes → roughly 5 entries survive.
        for i in 0..20u8 {
            let _ = s.append("t", rec(0, &[i])).unwrap();
        }
        let stats = RetentionPolicy::by_bytes(200)
            .apply(&mut s, "t", 0)
            .unwrap();
        assert!(stats.removed > 0, "must remove something");
        // After sweep, retained bytes should be ≤ cap (or empty).
        let retained = s.retained_bytes();
        assert!(
            retained <= 200 || s.len("t") == 0,
            "retained={retained} should be under cap"
        );
    }

    #[test]
    fn by_age_then_bytes_combines_policies() {
        let mut s = MemoryStore::new();
        // 10 old entries at ts=0, 5 new at ts=100.
        for i in 0..10u8 {
            let _ = s.append("t", rec(0, &[i])).unwrap();
        }
        for i in 10..15u8 {
            let _ = s.append("t", rec(100, &[i])).unwrap();
        }
        let policy = RetentionPolicy {
            max_bytes: Some(150),
            max_age_ns: Some(50),
        };
        let stats = policy.apply(&mut s, "t", 110).unwrap();
        assert!(stats.removed >= 10, "at least the 10 old entries must go");
        let retained = s.retained_bytes();
        assert!(retained <= 150 || s.len("t") == 0);
    }

    #[test]
    fn apply_on_empty_topic_is_noop() {
        let mut s = MemoryStore::new();
        let stats = RetentionPolicy::by_bytes(100)
            .apply(&mut s, "t", 0)
            .unwrap();
        assert_eq!(stats.removed, 0);
        assert_eq!(stats.new_oldest, Offset::NONE);
    }
}
