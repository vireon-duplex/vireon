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
//!
//! ## Persistence
//!
//! [`ConsumerOffsets::save_to`] writes a binary snapshot to disk;
//! [`ConsumerOffsets::load_from`] reads it back. The server calls
//! `save_to` during the periodic WAL retention sweep so consumer
//! positions survive restarts. The on-disk format is a length-prefixed
//! list — see `MAGIC` for the file-tag.

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

use rustc_hash::FxHashMap;

use crate::offset::Offset;

/// File-tag at the start of every consumer-offsets snapshot. Lets a
/// future format bump detect the old layout and skip cleanly instead
/// of misparsing it.
const MAGIC: &[u8; 4] = b"VOFF";
/// Snapshot format version. Bump on any breaking layout change.
const FORMAT_VERSION: u32 = 1;

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

    /// Write a binary snapshot to `path`. Atomically renames a `.tmp`
    /// file over the destination so a crash mid-write never leaves a
    /// truncated snapshot — the previous snapshot survives intact.
    ///
    /// Format (all integers big-endian):
    /// ```text
    /// MAGIC("VOFF") | version(u32) | count(u32)
    ///   | repeat:
    ///       topic_len(u16) | topic(utf8)
    ///       consumer_len(u16) | consumer(utf8)
    ///       offset(u64)
    /// ```
    ///
    /// # Errors
    /// Surface any I/O error from the temp-file write or the rename.
    pub fn save_to(&self, path: &Path) -> std::io::Result<()> {
        let tmp = path.with_extension("dat.tmp");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(MAGIC)?;
        f.write_all(&FORMAT_VERSION.to_be_bytes())?;
        let count = u32::try_from(self.map.len()).unwrap_or(u32::MAX);
        f.write_all(&count.to_be_bytes())?;
        for ((topic, consumer), offset) in &self.map {
            let tlen = u16::try_from(topic.len()).unwrap_or(u16::MAX);
            f.write_all(&tlen.to_be_bytes())?;
            f.write_all(topic.as_bytes())?;
            let clen = u16::try_from(consumer.len()).unwrap_or(u16::MAX);
            f.write_all(&clen.to_be_bytes())?;
            f.write_all(consumer.as_bytes())?;
            f.write_all(&offset.get().to_be_bytes())?;
        }
        f.flush()?;
        f.sync_data()?;
        std::fs::rename(&tmp, path)
    }

    /// Load a snapshot previously written by [`Self::save_to`].
    ///
    /// Returns an empty map (not an error) if the file does not exist
    /// — the common case on a fresh worker dir.
    ///
    /// # Errors
    /// [`wal_store::WalError`]::`Io` if the file exists but is corrupt
    /// (wrong magic, unsupported version, truncated entry).
    pub fn load_from(path: &Path) -> std::io::Result<Self> {
        let mut f = match OpenOptions::new().read(true).open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(e),
        };
        let mut magic = [0u8; 4];
        f.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "consumer-offsets snapshot: bad magic",
            ));
        }
        let mut buf4 = [0u8; 4];
        f.read_exact(&mut buf4)?;
        let version = u32::from_be_bytes(buf4);
        if version != FORMAT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("consumer-offsets snapshot: unsupported version {version}"),
            ));
        }
        f.read_exact(&mut buf4)?;
        let count = u32::from_be_bytes(buf4) as usize;
        let mut map = FxHashMap::with_capacity_and_hasher(count, Default::default());
        let mut buf2 = [0u8; 2];
        let mut buf8 = [0u8; 8];
        for _ in 0..count {
            f.read_exact(&mut buf2)?;
            let tlen = u16::from_be_bytes(buf2) as usize;
            let mut topic = vec![0u8; tlen];
            f.read_exact(&mut topic)?;
            f.read_exact(&mut buf2)?;
            let clen = u16::from_be_bytes(buf2) as usize;
            let mut consumer = vec![0u8; clen];
            f.read_exact(&mut consumer)?;
            f.read_exact(&mut buf8)?;
            let offset = u64::from_be_bytes(buf8);
            map.insert(
                (
                    String::from_utf8(topic).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "consumer-offsets snapshot: non-utf8 topic",
                        )
                    })?,
                    String::from_utf8(consumer).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "consumer-offsets snapshot: non-utf8 consumer",
                        )
                    })?,
                ),
                Offset::new(offset),
            );
        }
        Ok(Self { map })
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

    #[test]
    fn save_load_round_trip_preserves_all_entries() {
        let dir = std::env::temp_dir().join(format!(
            "vireon-wal-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("offsets.voff");

        let mut c = ConsumerOffsets::new();
        c.ack("orders", "worker-1", Offset::new(42));
        c.ack("orders", "worker-2", Offset::new(17));
        c.ack("analytics.events", "consumer-a", Offset::new(1000));
        c.save_to(&path).unwrap();

        let loaded = ConsumerOffsets::load_from(&path).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.get("orders", "worker-1"), Offset::new(42));
        assert_eq!(loaded.get("orders", "worker-2"), Offset::new(17));
        assert_eq!(
            loaded.get("analytics.events", "consumer-a"),
            Offset::new(1000)
        );
        // resume_from should also work after load.
        assert_eq!(loaded.resume_from("orders", "worker-1"), Offset::new(43));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn load_from_missing_file_returns_empty() {
        let path = std::env::temp_dir().join(format!(
            "vireon-no-such-{}-{}.voff",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // File intentionally does not exist.
        let loaded = ConsumerOffsets::load_from(&path).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn load_from_bad_magic_is_error() {
        let dir = std::env::temp_dir().join(format!(
            "vireon-bad-magic-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("bad.voff");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, b"XXXX").unwrap();
        let err = ConsumerOffsets::load_from(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn save_empty_map_round_trips() {
        let dir = std::env::temp_dir().join(format!(
            "vireon-empty-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = dir.join("empty.voff");
        std::fs::create_dir_all(&dir).unwrap();

        let c = ConsumerOffsets::new();
        c.save_to(&path).unwrap();
        let loaded = ConsumerOffsets::load_from(&path).unwrap();
        assert!(loaded.is_empty());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
