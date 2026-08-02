//! [`FileStore`] — disk-backed [`WalStore`] implementation with segment
//! rotation.
//!
//! Each topic is a directory under the configured `root`. Inside the
//! directory live one or more segment files named
//! `seg-{base_offset:020}.wal`. The **active segment** (highest
//! base_offset) is the only one that receives appends; it is rolled to
//! a fresh segment once it exceeds `segment_max_bytes`. Older segments
//! are read-only and can be deleted wholesale by the retention sweeper.
//!
//! ## Why segments?
//!
//! The previous single-file design rewrote the *entire* surviving log
//! on every retention truncate — O(n) per sweep, which stalls the
//! reactor on GiB-scale topics. Segments make dropping old data an O(1)
//! file `unlink` per segment; only the rare boundary case (an offset
//! cut lands mid-segment) rewrites a *single* bounded segment.
//!
//! ## Backward compatibility
//!
//! On recovery, if a topic directory still contains a legacy `log.wal`
//! (single-file format) and no segments, it is renamed to
//! `seg-00000000000000000000.wal` and treated as a segment going
//! forward.
//!
//! ## Concurrency
//!
//! Single-threaded by design (matches the trait contract). Each reactor
//! owns its own `FileStore`.

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use crate::entry::{WalEntry, WalRecord};
use crate::error::WalError;
use crate::offset::Offset;
use crate::segment::{Segment, parse_segment_filename, segment_filename};
use crate::store::{WalStore, validate_topic};

/// Legacy single-file WAL name (pre-segment format). Kept for migration.
const LEGACY_LOG_FILENAME: &str = "log.wal";
/// Tmp suffix used when rewriting the boundary segment on truncate.
const TMP_SUFFIX: &str = ".tmp";

/// Default per-segment size cap. 1 GiB balances truncation granularity
/// against file-descriptor / inode overhead. Configurable via
/// `FileStore::open_with_segment_limit`.
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// When to call `sync_all` / `sync_data` on the WAL segments.
///
/// The default in `vireon.conf` is `GroupCommit(100ms)` — fsync at most
/// every 100 ms, which protects against power loss with at most 100 ms
/// of data loss while keeping throughput high.
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

/// Disk-backed [`WalStore`] with segment rotation.
#[derive(Debug)]
pub struct FileStore {
    root: PathBuf,
    /// `(topic name) → open topic log`.
    topics: FxHashMap<String, TopicFile>,
    /// Cumulative retained bytes across all open topics.
    retained_bytes: u64,
    /// When to call `sync_data` on appended bytes.
    sync_policy: SyncPolicy,
    /// Last successful `sync_data` instant (GroupCommit interval gate).
    last_sync: Option<Instant>,
    /// Per-segment size cap. A segment is rolled once it exceeds this.
    segment_max_bytes: u64,
}

/// Per-topic collection of segments. The **active segment** (back of
/// the deque) receives appends; front segments are candidates for
/// retention deletion.
#[derive(Debug)]
struct TopicFile {
    /// Directory holding this topic's segment files.
    dir: PathBuf,
    /// Segments ordered oldest → newest. Back = active (append target).
    segments: VecDeque<Segment>,
    /// Next offset to assign on append. Monotonic across truncates.
    next_offset: u64,
    /// Sum of `WalEntry::wire_len` for currently-retained entries across
    /// all segments.
    retained_bytes: u64,
}

impl TopicFile {
    /// Mutably borrow the active segment. Caller guarantees non-empty.
    fn active_mut(&mut self) -> Option<&mut Segment> {
        self.segments.back_mut()
    }

    /// Ensure an active segment exists and isn't full; roll a new one
    /// at `base = next_offset` if needed.
    fn ensure_active(&mut self, segment_max_bytes: u64) -> Result<(), WalError> {
        let needs_roll = match self.segments.back() {
            None => true,
            Some(seg) => seg.is_full(segment_max_bytes),
        };
        if needs_roll {
            let base = Offset::new(self.next_offset);
            let seg = Segment::create(&self.dir, base)?;
            self.segments.push_back(seg);
        }
        Ok(())
    }
}

impl FileStore {
    /// Open or create a store rooted at `path` with the default
    /// [`SyncPolicy`] (group-commit 100 ms) and default segment cap.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        Self::open_with(path, SyncPolicy::default())
    }

    /// Open with an explicit [`SyncPolicy`] and the default segment cap.
    pub fn open_with<P: AsRef<Path>>(path: P, policy: SyncPolicy) -> Result<Self, WalError> {
        Self::open_full(path, policy, DEFAULT_SEGMENT_MAX_BYTES)
    }

    /// Open with an explicit [`SyncPolicy`] **and** a custom per-segment
    /// size cap. Used by the server to forward the operator's
    /// `wal_sync` / `wal_segment_max_bytes` configuration.
    pub fn open_full<P: AsRef<Path>>(
        path: P,
        policy: SyncPolicy,
        segment_max_bytes: u64,
    ) -> Result<Self, WalError> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let cap = if segment_max_bytes == 0 {
            DEFAULT_SEGMENT_MAX_BYTES
        } else {
            segment_max_bytes
        };
        let mut store = Self {
            root,
            topics: FxHashMap::default(),
            retained_bytes: 0,
            sync_policy: policy,
            last_sync: None,
            segment_max_bytes: cap,
        };
        store.recover_topics()?;
        Ok(store)
    }

    /// Apply the configured [`SyncPolicy`] to every open segment. Inside
    /// `GroupCommit` this skips the syscall when inside the interval.
    /// Errors are logged-and-swallowed: a failed fsync must NOT abort
    /// the publish path.
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
        let mut had_err = false;
        for tf in self.topics.values_mut() {
            for seg in tf.segments.iter_mut() {
                if let Err(e) = seg.sync_data() {
                    tracing::warn!(
                        topic_dir = ?tf.dir,
                        segment = ?seg.base_offset,
                        error = %e,
                        "[wal] sync_data failed — page cache holds the bytes \
                         but they are not fsync-durable"
                    );
                    had_err = true;
                }
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
                continue;
            };
            if validate_topic(name).is_err() {
                continue;
            }
            let topic_file = self.open_topic_file(name, true)?;
            self.retained_bytes += topic_file.retained_bytes;
        }
        Ok(())
    }

    /// Open (or create) the segment set for `topic`. On `recover=true`,
    /// scan the directory for existing segment files and rebuild each
    /// segment's index. On `recover=false`, the topic is known to be
    /// brand-new (first append) — segments will be created lazily.
    fn open_topic_file(&mut self, topic: &str, recover: bool) -> Result<&mut TopicFile, WalError> {
        if self.topics.contains_key(topic) {
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

        let mut segments: VecDeque<Segment> = VecDeque::new();
        let mut next_offset: u64 = 0;
        let mut retained_bytes: u64 = 0;
        let mut highest_offset: u64 = 0;

        if recover {
            // Migrate legacy single-file log.wal if present and no segments.
            let legacy = dir.join(LEGACY_LOG_FILENAME);
            let has_segments = match fs::read_dir(&dir) {
                Ok(it) => it.filter_map(|e| e.ok()).any(|e| {
                    e.file_name()
                        .to_str()
                        .is_some_and(|n| n.starts_with("seg-"))
                }),
                Err(_) => false,
            };
            if legacy.exists() && !has_segments {
                let migrated = dir.join(segment_filename(Offset::new(0)));
                fs::rename(&legacy, &migrated)?;
            } else if legacy.exists() {
                // Segments coexist with stale log.wal — drop the legacy file.
                let _ = fs::remove_file(&legacy);
            }

            // Collect and sort segment files by base_offset.
            let mut seg_files: Vec<(Offset, PathBuf)> = Vec::new();
            for dirent in fs::read_dir(&dir)? {
                let dirent = dirent?;
                let fname = dirent.file_name();
                let fname = match fname.to_str() {
                    Some(s) => s.to_owned(),
                    None => continue,
                };
                if let Some(base) = parse_segment_filename(&fname) {
                    seg_files.push((base, dirent.path()));
                }
            }
            seg_files.sort_by_key(|(base, _)| *base);

            for (base, path) in seg_files {
                let seg = Segment::open_recover(path, base)?;
                if let Some(last) = seg.index.last() {
                    if last.0.get() >= highest_offset {
                        highest_offset = last.0.get();
                    }
                }
                retained_bytes += seg.retained_bytes;
                segments.push_back(seg);
            }
            next_offset = if segments.is_empty() {
                0
            } else {
                highest_offset + 1
            };
        }

        self.topics.insert(
            topic.to_owned(),
            TopicFile {
                dir,
                segments,
                next_offset,
                retained_bytes,
            },
        );
        self.topics.get_mut(topic).ok_or(WalError::InvalidTopic {
            reason: "internal: insert-then-get failed",
        })
    }

    /// Find the segment containing `offset`, returning `(&Segment, idx)`
    /// so the caller can read or inspect it.
    fn segment_for(tf: &TopicFile, offset: Offset) -> Option<(&Segment, usize)> {
        // Segments are sorted by base_offset. Find the segment whose
        // index range contains `offset`.
        for (i, seg) in tf.segments.iter().enumerate() {
            let first = seg.oldest_offset();
            let last = seg.head_offset();
            if first.is_none() {
                continue;
            }
            if offset.get() >= first.get() && offset.get() <= last.get() {
                return Some((seg, i));
            }
        }
        None
    }
}

impl WalStore for FileStore {
    fn append(&mut self, topic: &str, record: WalRecord) -> Result<Offset, WalError> {
        validate_topic(topic)?;
        let segment_max_bytes = self.segment_max_bytes;
        let recovered = self.topics.contains_key(topic);
        let tf = self.open_topic_file(topic, !recovered)?;
        let offset = Offset::new(tf.next_offset);
        let entry = record.into_entry(offset);
        let encoded = entry.encode();
        let wire_len = encoded.len() as u64;

        // Roll a new segment if the active one is full (or absent).
        tf.ensure_active(segment_max_bytes)?;
        let seg = tf.active_mut().ok_or(WalError::InvalidTopic {
            reason: "internal: no active segment after ensure_active",
        })?;
        seg.append_raw(&encoded, offset)?;

        tf.next_offset += 1;
        tf.retained_bytes += wire_len;
        self.retained_bytes += wire_len;

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
        if tf.segments.is_empty() {
            return Err(WalError::TopicEmpty {
                topic: topic.to_owned(),
            });
        }
        let Some((seg, _)) = Self::segment_for(tf, offset) else {
            // Offset not in any segment's range.
            let oldest = tf
                .segments
                .front()
                .map(|s| s.oldest_offset())
                .unwrap_or(Offset::NONE);
            let head = tf
                .segments
                .back()
                .map(|s| s.head_offset())
                .unwrap_or(Offset::NONE);
            return Err(WalError::OffsetOutOfRange {
                requested: offset.get(),
                oldest: oldest.get(),
                head: head.get(),
            });
        };
        // Binary-search within the segment's index for the exact position.
        let target = offset.get();
        let idx = seg
            .index
            .binary_search_by_key(&target, |(o, _)| o.get())
            .map_err(|_| WalError::OffsetOutOfRange {
                requested: offset.get(),
                oldest: seg.oldest_offset().get(),
                head: seg.head_offset().get(),
            })?;
        let pos = seg.index[idx].1;
        let mut entry = seg.read_at(pos)?;
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
        if tf.segments.is_empty() {
            return Ok(0);
        }
        let oldest_o = tf
            .segments
            .front()
            .map(|s| s.oldest_offset())
            .unwrap_or(Offset::NONE);
        let head_o = tf
            .segments
            .back()
            .map(|s| s.head_offset())
            .unwrap_or(Offset::NONE);
        if oldest_o.is_none() {
            return Ok(0);
        }
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
        let mut added = 0;
        let mut o = from.get();
        while o <= until.get() {
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
            .and_then(|tf| tf.segments.front())
            .and_then(|s| {
                let o = s.oldest_offset();
                if o.is_none() { None } else { Some(o) }
            })
            .unwrap_or(Offset::NONE)
    }

    fn head(&self, topic: &str) -> Offset {
        self.topics
            .get(topic)
            .and_then(|tf| tf.segments.back())
            .and_then(|s| {
                let o = s.head_offset();
                if o.is_none() { None } else { Some(o) }
            })
            .unwrap_or(Offset::NONE)
    }

    fn len(&self, topic: &str) -> usize {
        self.topics
            .get(topic)
            .map(|tf| tf.segments.iter().map(Segment::len).sum())
            .unwrap_or(0)
    }

    fn truncate(&mut self, topic: &str, before_offset: Offset) -> Result<usize, WalError> {
        validate_topic(topic)?;
        let Some(tf) = self.topics.get_mut(topic) else {
            return Ok(0);
        };
        if tf.segments.is_empty() {
            return Ok(0);
        }

        let head_o = tf
            .segments
            .back()
            .and_then(|s| {
                let o = s.head_offset();
                if o.is_none() { None } else { Some(o) }
            })
            .unwrap_or(Offset::NONE);
        let remove_until = if before_offset.get() > head_o.get() {
            head_o.next().get()
        } else {
            before_offset.get()
        };

        let mut removed: usize = 0;

        // Phase 1: Drop whole segments whose head < remove_until.
        while let Some(front) = tf.segments.front() {
            let front_head = front.head_offset();
            if front_head.is_none() {
                // Empty segment — shouldn't normally exist, but drop it.
                let Some(seg) = tf.segments.pop_front() else {
                    break;
                };
                removed += seg.len();
                tf.retained_bytes = tf.retained_bytes.saturating_sub(seg.retained_bytes);
                self.retained_bytes = self.retained_bytes.saturating_sub(seg.retained_bytes);
                let _ = fs::remove_file(seg.path());
                continue;
            }
            if front_head.get() < remove_until {
                let Some(seg) = tf.segments.pop_front() else {
                    break;
                };
                removed += seg.len();
                tf.retained_bytes = tf.retained_bytes.saturating_sub(seg.retained_bytes);
                self.retained_bytes = self.retained_bytes.saturating_sub(seg.retained_bytes);
                let _ = fs::remove_file(seg.path());
            } else {
                break;
            }
        }

        // Phase 2: Boundary segment — trim entries with offset < remove_until.
        if let Some(boundary) = tf.segments.front_mut() {
            let cutoff = boundary
                .index
                .iter()
                .position(|(o, _)| o.get() >= remove_until);
            if let Some(cutoff) = cutoff {
                if cutoff > 0 {
                    let (trim_removed, byte_delta) = Self::trim_segment_head(boundary, cutoff)?;
                    removed += trim_removed;
                    tf.retained_bytes = tf.retained_bytes.saturating_sub(byte_delta);
                    self.retained_bytes = self.retained_bytes.saturating_sub(byte_delta);
                }
            } else {
                // Entire boundary segment is below remove_until — drop it.
                if let Some(seg) = tf.segments.pop_front() {
                    removed += seg.len();
                    tf.retained_bytes = tf.retained_bytes.saturating_sub(seg.retained_bytes);
                    self.retained_bytes = self.retained_bytes.saturating_sub(seg.retained_bytes);
                    let _ = fs::remove_file(seg.path());
                }
            }
        }

        // Ensure an active segment exists for future appends.
        if tf.segments.is_empty() {
            let base = Offset::new(tf.next_offset);
            let seg = Segment::create(&tf.dir, base)?;
            tf.segments.push_back(seg);
        }

        Ok(removed)
    }

    fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

impl FileStore {
    /// Trim the first `cutoff` entries from `seg` by rewriting it: copy
    /// the surviving entries to a fresh file named after the new base
    /// offset, then replace the old file. Bounded by the segment's size.
    ///
    /// Returns `(entries_removed, byte_delta)` where `byte_delta` is the
    /// number of bytes freed (old retained − new retained). The caller
    /// must subtract `byte_delta` from both the topic-level and
    /// store-level `retained_bytes`.
    fn trim_segment_head(seg: &mut Segment, cutoff: usize) -> Result<(usize, u64), WalError> {
        use std::io::{Read, Seek, SeekFrom, Write};
        let dir = seg.path().parent().ok_or(WalError::InvalidTopic {
            reason: "segment path has no parent dir",
        })?;
        let surviving = seg.index[cutoff..].to_vec();
        let removed = cutoff;
        if surviving.is_empty() {
            // Edge case: nothing survives — caller handles full-segment
            // drop, but defensively return zero byte delta here.
            return Ok((removed, 0));
        }
        let new_base = surviving[0].0;
        let new_name = segment_filename(new_base);
        let new_path = dir.join(&new_name);
        let tmp_path = dir.join(format!("{new_name}{TMP_SUFFIX}"));

        // Copy surviving entries into the tmp file.
        let mut src = OpenOptions::new().read(true).open(seg.path())?;
        let mut dst = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut new_retained: u64 = 0;
        for (_, src_pos) in &surviving {
            src.seek(SeekFrom::Start(*src_pos))?;
            let mut head_buf = [0u8; crate::entry::HEADER_LEN];
            src.read_exact(&mut head_buf)?;
            let key_len = u16::from_le_bytes([head_buf[20], head_buf[21]]) as usize;
            let payload_len =
                u32::from_le_bytes([head_buf[22], head_buf[23], head_buf[24], head_buf[25]])
                    as usize;
            let entry_len =
                crate::entry::HEADER_LEN + key_len + payload_len + crate::entry::CRC_LEN;
            dst.write_all(&head_buf)?;
            let mut rest = vec![0u8; entry_len - crate::entry::HEADER_LEN];
            src.read_exact(&mut rest)?;
            dst.write_all(&rest)?;
            new_retained += entry_len as u64;
        }
        dst.flush()?;

        // Remove old segment file, rename tmp into place.
        let old_retained = seg.retained_bytes;
        let byte_delta = old_retained.saturating_sub(new_retained);
        let _ = fs::remove_file(seg.path());
        fs::rename(&tmp_path, &new_path)?;

        // Reopen the trimmed segment as the new boundary.
        let new_seg = Segment::open_recover(new_path, new_base)?;
        debug_assert_eq!(new_seg.retained_bytes, new_retained);
        *seg = new_seg;
        Ok((removed, byte_delta))
    }

    /// Names of every topic the store currently tracks.
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
    use std::io::Write;
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
            // Find the segment file and append garbage.
            let seg_path = path.join("t").join(segment_filename(Offset::new(0)));
            let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
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
        let (mut s, _dir) = open_with_policy(SyncPolicy::Never);
        let off = s.append("t", rec(b"hello")).unwrap();
        assert_eq!(off, Offset::new(0));
        assert_eq!(s.len("t"), 1);
        assert_eq!(
            s.read("t", Offset::new(0)).unwrap().payload.as_ref(),
            b"hello"
        );
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

    // ── Segment-specific tests ──────────────────────────────────────

    #[test]
    fn segment_rolls_at_size_cap() {
        let dir = TempDir::new().unwrap();
        // Tiny cap so we roll after a few entries.
        let mut s = FileStore::open_full(dir.path(), SyncPolicy::Never, 100).unwrap();
        for i in 0..20u8 {
            let _ = s.append("t", rec(&[i; 10])).unwrap();
        }
        // Should have multiple segments now.
        let tf = s.topics.get("t").unwrap();
        assert!(
            tf.segments.len() > 1,
            "expected segment rotation, got {} segments",
            tf.segments.len()
        );
        // All data readable across segments.
        for i in 0..20u8 {
            let e = s.read("t", Offset::new(u64::from(i))).unwrap();
            assert_eq!(e.payload.as_ref(), &[i; 10]);
        }
    }

    #[test]
    fn truncate_drops_whole_segments() {
        let dir = TempDir::new().unwrap();
        let mut s = FileStore::open_full(dir.path(), SyncPolicy::Never, 100).unwrap();
        for i in 0..20u8 {
            let _ = s.append("t", rec(&[i; 10])).unwrap();
        }
        let tf = s.topics.get("t").unwrap();
        let segs_before = tf.segments.len();
        assert!(segs_before > 1);
        // Truncate a large chunk — should drop front segments.
        let removed = s.truncate("t", Offset::new(15)).unwrap();
        assert_eq!(removed, 15);
        assert_eq!(s.oldest("t"), Offset::new(15));
        assert_eq!(s.len("t"), 5);
        // Verify directory has fewer segment files.
        let seg_count = fs::read_dir(dir.path().join("t"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map_or(false, |n| n.starts_with("seg-"))
            })
            .count();
        assert!(
            seg_count <= segs_before,
            "segment files should not grow after truncate"
        );
    }

    #[test]
    fn reopen_recovers_multiple_segments() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        {
            let mut s = FileStore::open_full(&path, SyncPolicy::Never, 100).unwrap();
            for i in 0..20u8 {
                let _ = s.append("t", rec(&[i; 10])).unwrap();
            }
        }
        // Reopen — all 20 entries must be readable across recovered segments.
        let s = FileStore::open(&path).unwrap();
        assert_eq!(s.len("t"), 20);
        assert_eq!(s.oldest("t"), Offset::new(0));
        assert_eq!(s.head("t"), Offset::new(19));
        for i in 0..20u8 {
            let e = s.read("t", Offset::new(u64::from(i))).unwrap();
            assert_eq!(e.payload.as_ref(), &[i; 10], "offset {i}");
        }
    }

    #[test]
    fn append_after_full_truncate_creates_fresh_segment() {
        let (mut s, _dir) = open_empty();
        for i in 0..5u8 {
            let _ = s.append("t", rec(&[i])).unwrap();
        }
        // Truncate everything.
        let removed = s.truncate("t", Offset::new(100)).unwrap();
        assert_eq!(removed, 5);
        assert_eq!(s.len("t"), 0);
        // Append must work — creates a fresh active segment.
        let o = s.append("t", rec(b"z")).unwrap();
        assert_eq!(o, Offset::new(5));
        assert_eq!(s.read("t", o).unwrap().payload.as_ref(), b"z");
    }

    #[test]
    fn legacy_log_wal_migrated_on_open() {
        let dir = TempDir::new().unwrap();
        let topic_dir = dir.path().join("t");
        fs::create_dir_all(&topic_dir).unwrap();
        // Write a legacy-format log.wal with 3 entries.
        {
            let legacy = topic_dir.join(LEGACY_LOG_FILENAME);
            let mut buf = Vec::new();
            for i in 0..3u8 {
                let entry = rec(&[i]).into_entry(Offset::new(u64::from(i)));
                buf.extend_from_slice(&entry.encode());
            }
            fs::write(&legacy, &buf).unwrap();
        }
        // Open — should migrate and recover.
        let s = FileStore::open(dir.path()).unwrap();
        assert_eq!(s.len("t"), 3);
        assert_eq!(s.read("t", Offset::new(0)).unwrap().payload.as_ref(), &[0]);
        // Legacy file should be gone.
        assert!(!topic_dir.join(LEGACY_LOG_FILENAME).exists());
    }
}
