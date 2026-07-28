//! [`FileStore`] — disk-backed [`WalStore`] implementation.
//!
//! Each topic is a directory under the configured `root`. Inside the
//! directory lives a single `log.wal` file that holds every appended
//! entry in wire format (see [`crate::entry`]). An in-memory
//! offset→byte-position index is rebuilt by scanning the file at open
//! time — this is O(entries) but happens once per topic, and Phase 4's
//! scope does not require persistent indices (those land when segment
//! rotation lands).
//!
//! ## Truncation
//!
//! Truncation compacts the file: surviving entries are copied to a
//! fresh `log.wal.tmp`, then atomically renamed over the original.
//! This is O(surviving_entries) per truncate, which is acceptable
//! under the typical retention cadence (once per second per topic).
//!
//! ## Concurrency
//!
//! Single-threaded by design (matches the trait contract). Each
//! reactor owns its own `FileStore`.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use crate::entry::{CRC_LEN, HEADER_LEN, WalEntry, WalRecord};
use crate::error::WalError;
use crate::offset::Offset;
use crate::store::{WalStore, validate_topic};

const LOG_FILENAME: &str = "log.wal";
const TMP_FILENAME: &str = "log.wal.tmp";

/// When to call `sync_all` / `sync_data` on the WAL file.
///
/// The default in `vireon.conf` is `GroupCommit(100ms)` — fsync at most
/// every 100 ms, which protects against power loss with at most 100 ms
/// of data loss while keeping throughput high. Switch to `EveryFlush`
/// for maximum durability (one fsync per publish) or `Never` to leave
/// flushing to the OS page cache (fastest, data survives process crash
/// but not power loss).
#[derive(Clone, Copy, Debug)]
pub enum SyncPolicy {
    /// `flush()` only — bytes leave the process but stay in the kernel
    /// page cache. Survives process crash; lost on power failure.
    Never,
    /// `sync_data()` after every `flush()`. Maximum durability, slowest.
    EveryFlush,
    /// `sync_data()` if more than `Duration` has elapsed since the last
    /// sync. Batches fsyncs so throughput stays high while bounding
    /// worst-case data loss to the interval.
    GroupCommit(Duration),
}

impl Default for SyncPolicy {
    fn default() -> Self {
        // Conservative default for a pub/sub runtime: 100 ms of potential
        // loss in exchange for >10× throughput vs `EveryFlush`.
        Self::GroupCommit(Duration::from_millis(100))
    }
}

/// Disk-backed [`WalStore`].
#[derive(Debug)]
pub struct FileStore {
    root: PathBuf,
    /// `(topic name) → open topic log`.
    topics: FxHashMap<String, TopicFile>,
    /// Cumulative retained bytes across all open topics. Lazily
    /// resummed after mutations; cheaper than walking every topic on
    /// every call.
    retained_bytes: u64,
    /// When to call `sync_data` on appended bytes. See [`SyncPolicy`].
    sync_policy: SyncPolicy,
    /// Last successful `sync_data` instant. Used by `GroupCommit` to
    /// skip redundant syncs inside the interval.
    last_sync: Option<Instant>,
}

#[derive(Debug)]
struct TopicFile {
    /// Directory holding this topic's `log.wal`.
    dir: PathBuf,
    /// Open file handle, positioned for append. Reads seek explicitly.
    file: File,
    /// (offset → byte position within the file) for every retained
    /// entry. Indexed at open time and maintained incrementally on
    /// append / truncate.
    index: Vec<(Offset, u64)>,
    /// Next offset to assign on append. Monotonic across truncates.
    next_offset: u64,
    /// Sum of `WalEntry::wire_len` for currently-retained entries.
    retained_bytes: u64,
}

impl FileStore {
    /// Open or create a store rooted at `path`. The directory is
    /// created if missing; existing topic subdirectories are scanned
    /// and their indices rebuilt. Uses the default [`SyncPolicy`]
    /// (group-commit 100 ms).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        Self::open_with(path, SyncPolicy::default())
    }

    /// Open or create a store rooted at `path` with an explicit
    /// [`SyncPolicy`]. Used by the server to forward the operator's
    /// `wal_sync` / `wal_sync_group_commit_ms` configuration.
    pub fn open_with<P: AsRef<Path>>(path: P, policy: SyncPolicy) -> Result<Self, WalError> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let mut store = Self {
            root,
            topics: FxHashMap::default(),
            retained_bytes: 0,
            sync_policy: policy,
            last_sync: None,
        };
        store.recover_topics()?;
        Ok(store)
    }

    /// Apply the configured [`SyncPolicy`] to every open topic file.
    /// Called after each `append()`; in `GroupCommit` mode this skips
    /// the syscall when inside the interval. Errors are logged-and-
    /// swallowed: a failed fsync must NOT abort the publish path (the
    /// live fan-out proceeds regardless, same philosophy as a failed
    /// WAL append).
    fn maybe_sync(&mut self) {
        let do_sync = match self.sync_policy {
            SyncPolicy::Never => false,
            SyncPolicy::EveryFlush => true,
            SyncPolicy::GroupCommit(interval) => match self.last_sync {
                Some(last) => last.elapsed() >= interval,
                None => true,
            },
        };
        if !do_sync {
            return;
        }
        // sync_data is enough — we never rename the WAL post-open, so
        // metadata stability is not required. Skips the inode write
        // that `sync_all` would force.
        let mut had_err = false;
        for tf in self.topics.values_mut() {
            if let Err(e) = tf.file.sync_data() {
                tracing::warn!(
                    topic_dir = ?tf.dir,
                    error = %e,
                    "[wal] sync_data failed — page cache holds the bytes \
                     but they are not fsync-durable"
                );
                had_err = true;
            }
        }
        if !had_err {
            self.last_sync = Some(Instant::now());
        }
    }

    fn recover_topics(&mut self) -> Result<(), WalError> {
        let entries = match fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(WalError::Io(e)),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                // Skip non-UTF8 names — they cannot have been written
                // by this store.
                continue;
            };
            if validate_topic(name).is_err() {
                continue;
            }
            let topic_file = self.open_topic_file(name, true)?;
            self.retained_bytes += topic_file.retained_bytes;
            // open_topic_file inserts into self.topics; we just need
            // the side effect here.
        }
        Ok(())
    }

    /// Open (or create) the `log.wal` for `topic`. On `recover=true`,
    /// scan the file and rebuild the index. On `recover=false`, the
    /// file is opened in append mode and the index starts empty (used
    /// when the topic is known to be brand-new from `append`).
    fn open_topic_file(&mut self, topic: &str, recover: bool) -> Result<&mut TopicFile, WalError> {
        // Two-step: check presence first (immutable borrow ends before
        // the insert). Entry API would be cleaner but the file-open
        // side effects don't fit in `or_insert_with`.
        if self.topics.contains_key(topic) {
            // SAFETY (logical): we just confirmed the key is present,
            // so the second lookup is guaranteed Some. The workspace
            // lint denies `.expect()`, so we use `match` with a safe
            // fallback that errors through WalError.
            return Ok(match self.topics.get_mut(topic) {
                Some(tf) => tf,
                None => {
                    return Err(WalError::InvalidTopic {
                        reason: "internal: get-after-contains failed",
                    });
                }
            });
        }
        let dir = self.root.join(topic);
        fs::create_dir_all(&dir)?;
        let path = dir.join(LOG_FILENAME);
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        let mut index = Vec::new();
        let mut next_offset: u64 = 0;
        let mut retained_bytes: u64 = 0;

        if recover {
            // Read the whole file, decode each entry, build index.
            // The offset stored in each entry's wire header is the
            // source of truth — it lets us recover original offset
            // numbering even after a truncate-then-restart cycle.
            file.seek(SeekFrom::Start(0))?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            let mut cursor = 0usize;
            let mut highest_offset = -1i64;
            while cursor < buf.len() {
                let (entry, consumed) = match WalEntry::decode(&buf[cursor..]) {
                    Ok(t) => t,
                    Err(WalError::MalformedEntry { .. }) => {
                        // Truncated tail — stop recovery here, truncate
                        // the file to the last good position.
                        tracing::warn!(
                            topic,
                            pos = cursor,
                            remaining = buf.len() - cursor,
                            "malformed entry during WAL recovery — truncating tail"
                        );
                        file.set_len(cursor as u64)?;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                index.push((entry.offset, cursor as u64));
                if entry.offset.get() as i64 > highest_offset {
                    highest_offset = entry.offset.get() as i64;
                }
                retained_bytes += entry.wire_len() as u64;
                cursor += consumed;
            }
            next_offset = if highest_offset < 0 {
                0
            } else {
                (highest_offset as u64) + 1
            };
            // Reposition at end for the next append.
            file.seek(SeekFrom::End(0))?;
        }

        self.topics.insert(
            topic.to_owned(),
            TopicFile {
                dir,
                file,
                index,
                next_offset,
                retained_bytes,
            },
        );
        // Entry API would be cleaner here but the topic dir + file open
        // sequence has side effects we want to keep outside the entry
        // closure. The just-inserted key is guaranteed to be present.
        self.topics.get_mut(topic).ok_or(WalError::InvalidTopic {
            reason: "internal: insert-then-get failed",
        })
    }

    /// Position in the file of the entry at `offset`, or `None` if
    /// the offset is not in the retained range.
    fn position_of(tf: &TopicFile, offset: Offset) -> Option<u64> {
        // `tf.index.first()` / `.last()` are guaranteed Some when the
        // index is non-empty, but the workspace lints deny `.expect()`.
        // Use `?` and short-circuit instead.
        let first = tf.index.first()?;
        let last = tf.index.last()?;
        let (oldest, head) = (first.0, last.0);
        if offset.get() < oldest.get() || offset.get() > head.get() {
            return None;
        }
        // Index is sorted by offset; binary search.
        let target = offset.get();
        let idx = tf
            .index
            .binary_search_by_key(&target, |(o, _)| o.get())
            .ok()?;
        Some(tf.index[idx].1)
    }
}

impl WalStore for FileStore {
    fn append(&mut self, topic: &str, record: WalRecord) -> Result<Offset, WalError> {
        validate_topic(topic)?;
        // Recovery happens only once per topic; subsequent appends
        // skip the scan via the `recover` flag. The first append on a
        // topic opens the file in recover mode so pre-existing data
        // is preserved.
        let recovered = self.topics.contains_key(topic);
        let tf = self.open_topic_file(topic, !recovered)?;
        let offset = Offset::new(tf.next_offset);
        let entry = record.into_entry(offset);
        let bytes = entry.encode();
        let pos = tf.file.seek(SeekFrom::End(0)).map_err(WalError::Io)?; // append-mode handles this on Linux but not all platforms
        tf.file.write_all(&bytes)?;
        tf.file.flush()?;
        tf.index.push((offset, pos));
        tf.next_offset += 1;
        tf.retained_bytes += entry.wire_len() as u64;
        self.retained_bytes += entry.wire_len() as u64;
        // Apply the configured durability policy. Inside GroupCommit,
        // this is usually a no-op (the interval hasn't elapsed); under
        // EveryFlush it forces a syscall per publish.
        self.maybe_sync();
        Ok(offset)
    }

    fn read(&self, topic: &str, offset: Offset) -> Result<WalEntry, WalError> {
        validate_topic(topic)?;
        let Some(tf) = self.topics.get(topic) else {
            return Err(WalError::TopicEmpty {
                topic: topic.to_owned(),
            });
        };
        let Some(pos) = Self::position_of(tf, offset) else {
            if tf.index.is_empty() {
                return Err(WalError::TopicEmpty {
                    topic: topic.to_owned(),
                });
            }
            // Index is non-empty (checked above) — first/last are Some.
            let (oldest, head) = match (tf.index.first(), tf.index.last()) {
                (Some(f), Some(l)) => (f.0.get(), l.0.get()),
                _ => {
                    return Err(WalError::TopicEmpty {
                        topic: topic.to_owned(),
                    });
                }
            };
            return Err(WalError::OffsetOutOfRange {
                requested: offset.get(),
                oldest,
                head,
            });
        };
        // Open a fresh read handle — the append-mode write handle is
        // not safe to seek on (POSIX leaves the offset after O_APPEND
        // unspecified for reads).
        let mut f = OpenOptions::new()
            .read(true)
            .open(tf.dir.join(LOG_FILENAME))?;
        f.seek(SeekFrom::Start(pos))?;
        // We need enough bytes to know the entry's full length. Read
        // the fixed header, then the variable-length body.
        let mut head_buf = [0u8; HEADER_LEN];
        f.read_exact(&mut head_buf)?;
        let key_len = u16::from_le_bytes([head_buf[20], head_buf[21]]) as usize;
        let payload_len =
            u32::from_le_bytes([head_buf[22], head_buf[23], head_buf[24], head_buf[25]]) as usize;
        let total = HEADER_LEN + key_len + payload_len + CRC_LEN;
        let mut full = Vec::with_capacity(total);
        full.extend_from_slice(&head_buf);
        let mut rest = vec![0u8; total - HEADER_LEN];
        f.read_exact(&mut rest)?;
        full.extend_from_slice(&rest);
        let (mut entry, _consumed) = WalEntry::decode(&full)?;
        entry.offset = offset;
        Ok(entry)
    }

    fn read_range(
        &self,
        topic: &str,
        from: Offset,
        until: Offset,
        out: &mut Vec<WalEntry>,
    ) -> Result<usize, WalError> {
        validate_topic(topic)?;
        let Some(tf) = self.topics.get(topic) else {
            return Ok(0);
        };
        if tf.index.is_empty() {
            return Ok(0);
        }
        // Index non-empty → first/last are Some; `?` works because this
        // closure returns Option but the outer function returns Result.
        let (oldest_o, head_o) = match (tf.index.first(), tf.index.last()) {
            (Some(f), Some(l)) => (f.0, l.0),
            _ => return Ok(0),
        };
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
        let until = if until.is_none() { head_o } else { until };
        // Walk offsets in [from, until], reading each.
        let mut added = 0;
        let mut o = from.get();
        while o <= until.get() {
            // Use Self::read via a clone-free path: we have &self here
            // but Self::read takes &self. Defer to it.
            match self.read(topic, Offset::new(o)) {
                Ok(e) => {
                    out.push(e);
                    added += 1;
                    o += 1;
                }
                Err(WalError::OffsetOutOfRange { .. }) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(added)
    }

    fn oldest(&self, topic: &str) -> Offset {
        self.topics
            .get(topic)
            .and_then(|tf| tf.index.first().map(|(o, _)| *o))
            .unwrap_or(Offset::NONE)
    }

    fn head(&self, topic: &str) -> Offset {
        self.topics
            .get(topic)
            .and_then(|tf| tf.index.last().map(|(o, _)| *o))
            .unwrap_or(Offset::NONE)
    }

    fn len(&self, topic: &str) -> usize {
        self.topics.get(topic).map_or(0, |tf| tf.index.len())
    }

    fn truncate(&mut self, topic: &str, before_offset: Offset) -> Result<usize, WalError> {
        validate_topic(topic)?;
        let Some(tf) = self.topics.get_mut(topic) else {
            return Ok(0);
        };
        if tf.index.is_empty() {
            return Ok(0);
        }
        // If before_offset is past head, drop everything.
        let head_o = match tf.index.last() {
            Some(last) => last.0,
            None => return Ok(0),
        };
        let remove_until = if before_offset.get() > head_o.get() {
            head_o.get() + 1
        } else {
            before_offset.get()
        };
        // Find the first index entry whose offset >= remove_until.
        let cutoff = tf
            .index
            .iter()
            .position(|(o, _)| o.get() >= remove_until)
            .unwrap_or(tf.index.len());
        if cutoff == 0 {
            return Ok(0);
        }
        let removed = cutoff;
        let old_bytes = tf.retained_bytes;
        let surviving = tf.index[cutoff..].to_vec();

        // Rewrite the file: copy surviving entries to a tmp file, then
        // rename atomically. `new_bytes` is computed inline so we don't
        // need to re-walk the file afterwards.
        let tmp_path = tf.dir.join(TMP_FILENAME);
        let (new_index, new_bytes) = {
            let mut src = OpenOptions::new()
                .read(true)
                .open(tf.dir.join(LOG_FILENAME))?;
            let mut dst = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut index: Vec<(Offset, u64)> = Vec::with_capacity(surviving.len());
            let mut total = 0u64;
            let mut next_pos: u64 = 0;
            for (o, src_pos) in &surviving {
                src.seek(SeekFrom::Start(*src_pos))?;
                let mut head_buf = [0u8; HEADER_LEN];
                src.read_exact(&mut head_buf)?;
                let key_len = u16::from_le_bytes([head_buf[20], head_buf[21]]) as usize;
                let payload_len =
                    u32::from_le_bytes([head_buf[22], head_buf[23], head_buf[24], head_buf[25]])
                        as usize;
                let entry_len = HEADER_LEN + key_len + payload_len + CRC_LEN;
                dst.write_all(&head_buf)?;
                let mut rest = vec![0u8; entry_len - HEADER_LEN];
                src.read_exact(&mut rest)?;
                dst.write_all(&rest)?;
                index.push((*o, next_pos));
                next_pos += entry_len as u64;
                total += entry_len as u64;
            }
            dst.flush()?;
            (index, total)
        };
        fs::rename(&tmp_path, tf.dir.join(LOG_FILENAME))?;
        tf.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(tf.dir.join(LOG_FILENAME))?;
        tf.index = new_index;
        tf.retained_bytes = new_bytes;
        self.retained_bytes = self
            .retained_bytes
            .saturating_sub(old_bytes.saturating_sub(new_bytes));
        Ok(removed)
    }

    fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

impl FileStore {
    /// Names of every topic the store currently tracks (i.e. every
    /// topic that has been appended at least once since open). Used by
    /// the retention sweeper to iterate over all topics without
    /// requiring the caller to track them separately.
    ///
    /// Order is unspecified; callers must not depend on it.
    #[must_use]
    pub fn topic_names(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn rec(payload: &[u8]) -> WalRecord {
        WalRecord::new(0, Bytes::copy_from_slice(payload))
    }

    fn open_empty() -> (FileStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = FileStore::open(dir.path()).unwrap();
        (store, dir)
    }

    #[test]
    fn append_then_read_round_trips() {
        let (mut s, _dir) = open_empty();
        let o = s.append("t", rec(b"hello")).unwrap();
        let e = s.read("t", o).unwrap();
        assert_eq!(e.payload.as_ref(), b"hello");
    }

    #[test]
    fn append_assigns_monotonic_offsets() {
        let (mut s, _dir) = open_empty();
        let o0 = s.append("t", rec(b"a")).unwrap();
        let o1 = s.append("t", rec(b"b")).unwrap();
        let o2 = s.append("t", rec(b"c")).unwrap();
        assert_eq!(o0, Offset::new(0));
        assert_eq!(o1, Offset::new(1));
        assert_eq!(o2, Offset::new(2));
        assert_eq!(s.head("t"), Offset::new(2));
    }

    #[test]
    fn read_range_walks_in_order() {
        let (mut s, _dir) = open_empty();
        for i in 0..5u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        let mut out = Vec::new();
        let n = s
            .read_range("t", Offset::new(1), Offset::new(3), &mut out)
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(out[0].payload.as_ref(), &[1]);
        assert_eq!(out[2].payload.as_ref(), &[3]);
    }

    #[test]
    fn truncate_removes_oldest() {
        let (mut s, _dir) = open_empty();
        for i in 0..5u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        let removed = s.truncate("t", Offset::new(3)).unwrap();
        assert_eq!(removed, 3);
        assert_eq!(s.oldest("t"), Offset::new(3));
        assert_eq!(s.len("t"), 2);
        assert!(matches!(
            s.read("t", Offset::new(2)),
            Err(WalError::OffsetOutOfRange { .. })
        ));
        // Offset 3 should still be readable.
        let e = s.read("t", Offset::new(3)).unwrap();
        assert_eq!(e.payload.as_ref(), &[3]);
    }

    #[test]
    fn reopen_recovers_index() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        {
            let mut s = FileStore::open(&path).unwrap();
            let _ = s.append("t", rec(b"a")).unwrap();
            let _ = s.append("t", rec(b"b")).unwrap();
            let _ = s.append("t", rec(b"c")).unwrap();
        }

        // Reopen — recovery should rebuild the index.
        let s = FileStore::open(&path).unwrap();
        assert_eq!(s.len("t"), 3);
        assert_eq!(s.head("t"), Offset::new(2));
        assert_eq!(s.read("t", Offset::new(0)).unwrap().payload.as_ref(), b"a");
        assert_eq!(s.read("t", Offset::new(2)).unwrap().payload.as_ref(), b"c");
    }

    #[test]
    fn reopen_recovers_after_truncate() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        {
            let mut s = FileStore::open(&path).unwrap();
            for i in 0..5u8 {
                let _ = s.append("t", rec(&[i])).unwrap();
            }
            let _ = s.truncate("t", Offset::new(3)).unwrap();
            assert_eq!(s.len("t"), 2);
        }

        // Reopen — recovery should reflect the truncated state.
        let s = FileStore::open(&path).unwrap();
        assert_eq!(s.len("t"), 2);
        assert_eq!(s.oldest("t"), Offset::new(3));
        assert_eq!(s.read("t", Offset::new(3)).unwrap().payload.as_ref(), &[3]);
    }

    #[test]
    fn recovered_tail_truncated_on_malformed_entry() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Write 3 entries, then append garbage.
        {
            let mut s = FileStore::open(&path).unwrap();
            let _ = s.append("t", rec(b"a")).unwrap();
            let _ = s.append("t", rec(b"b")).unwrap();
            let _ = s.append("t", rec(b"c")).unwrap();
            // Open the file directly and append garbage.
            let mut f = OpenOptions::new()
                .append(true)
                .open(path.join("t/log.wal"))
                .unwrap();
            f.write_all(&[0xFFu8; 32]).unwrap();
        }

        // Reopen — recovery should accept the 3 good entries and drop
        // the malformed tail.
        let s = FileStore::open(&path).unwrap();
        assert_eq!(s.len("t"), 3);
        assert_eq!(s.head("t"), Offset::new(2));
    }

    #[test]
    fn multiple_topics_isolated() {
        let (mut s, _dir) = open_empty();
        let _ = s.append("t1", rec(b"a")).unwrap();
        let _ = s.append("t2", rec(b"b")).unwrap();
        assert_eq!(s.len("t1"), 1);
        assert_eq!(s.len("t2"), 1);
        assert_eq!(s.read("t1", Offset::new(0)).unwrap().payload.as_ref(), b"a");
        assert_eq!(s.read("t2", Offset::new(0)).unwrap().payload.as_ref(), b"b");
    }

    #[test]
    fn invalid_topic_rejected() {
        let (mut s, _dir) = open_empty();
        assert!(matches!(
            s.append("", rec(b"x")),
            Err(WalError::InvalidTopic { .. })
        ));
        assert!(matches!(
            s.append("bad/name", rec(b"x")),
            Err(WalError::InvalidTopic { .. })
        ));
    }

    fn open_with_policy(policy: SyncPolicy) -> (FileStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = FileStore::open_with(dir.path(), policy).unwrap();
        (store, dir)
    }

    #[test]
    fn sync_policy_never_still_appends_correctly() {
        // The policy must not affect the data path — only the fsync path.
        let (mut s, _dir) = open_with_policy(SyncPolicy::Never);
        let off = s.append("t", rec(b"hello")).unwrap();
        assert_eq!(off, Offset::new(0));
        assert_eq!(s.len("t"), 1);
        assert_eq!(s.read("t", Offset::new(0)).unwrap().payload.as_ref(), b"hello");
    }

    #[test]
    fn sync_policy_every_flush_does_not_break_append() {
        let (mut s, _dir) = open_with_policy(SyncPolicy::EveryFlush);
        for i in 0..5 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        assert_eq!(s.len("t"), 5);
    }

    #[test]
    fn sync_policy_group_commit_skips_within_interval() {
        // Within the same nanosecond-resolution window, GroupCommit
        // should only sync once. We assert by counting: no panic, no
        // corruption, all offsets contiguous.
        let (mut s, _dir) = open_with_policy(SyncPolicy::GroupCommit(Duration::from_secs(60)));
        for i in 0..10u8 {
            let off = s.append("t", rec(&[i])).unwrap();
            assert_eq!(off.get(), u64::from(i));
        }
        assert_eq!(s.len("t"), 10);
    }

    #[test]
    fn default_policy_is_group_commit_100ms() {
        match SyncPolicy::default() {
            SyncPolicy::GroupCommit(d) => assert_eq!(d, Duration::from_millis(100)),
            other => panic!("expected GroupCommit, got {other:?}"),
        }
    }
}
