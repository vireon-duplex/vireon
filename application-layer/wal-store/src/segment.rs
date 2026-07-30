//! [`Segment`] — a single size-bounded WAL file within a topic directory.
//!
//! A topic's WAL is composed of one or more segments. Each segment is a
//! standalone file named `seg-{base_offset:020}.wal` containing a
//! contiguous run of entries. The **active segment** (the last one) is
//! the only one that receives appends; older segments are read-only and
//! can be deleted wholesale by retention.
//!
//! ## Why segments?
//!
//! The previous single-file design rewrote the *entire* surviving log on
//! every retention truncate — O(n) per sweep. With segments, dropping
//! old data is an O(1) file `unlink` per segment; only the rare boundary
//! case (an offset cut lands mid-segment) needs an entry-level rewrite
//! of a *single* bounded segment.
//!
//! ## Indexing
//!
//! Each segment keeps an in-memory `index: Vec<(Offset, pos)>` rebuilt
//! by scanning the file at open time. Positions are relative to the
//! start of *this* segment file, so reads need no cross-segment
//! arithmetic.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::entry::{CRC_LEN, HEADER_LEN, WalEntry};
use crate::error::WalError;
use crate::offset::Offset;

/// Zero-padded width of the base-offset field in segment filenames.
/// 20 decimal digits covers the full `u64` range.
const OFFSET_WIDTH: usize = 20;

/// Filename prefix for segment files.
pub const SEGMENT_PREFIX: &str = "seg-";
/// Filename extension for segment files.
pub const SEGMENT_EXT: &str = ".wal";

/// Build the filename for a segment whose first entry has `base_offset`.
#[must_use]
pub fn segment_filename(base_offset: Offset) -> String {
    format!("{SEGMENT_PREFIX}{base_offset:0OFFSET_WIDTH$}{SEGMENT_EXT}", base_offset = base_offset.get())
}

/// Parse a base offset out of a `seg-NNNNNNNNNN.wal` filename.
/// Returns `None` for names that don't match the scheme.
pub fn parse_segment_filename(name: &str) -> Option<Offset> {
    let stem = name.strip_prefix(SEGMENT_PREFIX)?;
    let stem = stem.strip_suffix(SEGMENT_EXT)?;
    let n: u64 = stem.parse().ok()?;
    Some(Offset::new(n))
}

/// One size-bounded WAL segment file.
#[derive(Debug)]
pub struct Segment {
    /// Offset of the first entry in this segment. Invariant: every
    /// entry in `index` has offset >= `base_offset`.
    pub base_offset: Offset,
    /// Full path to the segment file.
    path: PathBuf,
    /// Append handle (`O_APPEND`). Reads use a fresh handle because
    /// POSIX leaves the file offset after `O_APPEND` writes unspecified
    /// for reads.
    file: File,
    /// `(offset → byte position within this file)` for every retained
    /// entry. Sorted ascending by offset.
    pub index: Vec<(Offset, u64)>,
    /// Sum of wire lengths of currently-retained entries.
    pub retained_bytes: u64,
}

impl Segment {
    /// Create a brand-new segment file at `dir/seg-{base}.wal`. The
    /// file is opened read+write+create+truncate. Writes are positioned
    /// via `seek(End)` in [`append_raw`] — not `O_APPEND`, which has
    /// portability quirks on some kernels.
    pub fn create(dir: &Path, base_offset: Offset) -> Result<Self, WalError> {
        let name = segment_filename(base_offset);
        let path = dir.join(&name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            base_offset,
            path,
            file,
            index: Vec::new(),
            retained_bytes: 0,
        })
    }

    /// Open an existing segment file at `path` and rebuild its index by
    /// scanning. `base_offset` is taken from the filename (trusted) but
    /// cross-checked against decoded entries where possible.
    ///
    /// On a malformed tail entry, the file is truncated to the last good
    /// position — mirroring the single-file recovery contract.
    pub fn open_recover(path: PathBuf, base_offset: Offset) -> Result<Self, WalError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let mut index: Vec<(Offset, u64)> = Vec::new();
        let mut retained_bytes: u64 = 0;
        // Scan the whole file to rebuild the index.
        file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut cursor = 0usize;
        while cursor < buf.len() {
            let (entry, consumed) = match WalEntry::decode(&buf[cursor..]) {
                Ok(t) => t,
                Err(WalError::MalformedEntry { .. }) => {
                    // Truncated tail — stop recovery, trim the file.
                    tracing::warn!(
                        path = %path.display(),
                        pos = cursor,
                        remaining = buf.len() - cursor,
                        "malformed entry during segment recovery — truncating tail"
                    );
                    file.set_len(cursor as u64)?;
                    break;
                }
                Err(e) => return Err(e),
            };
            index.push((entry.offset, cursor as u64));
            retained_bytes += entry.wire_len() as u64;
            cursor += consumed;
        }
        Ok(Self {
            base_offset,
            path,
            file,
            index,
            retained_bytes,
        })
    }

    /// Append an already-encoded entry blob. The caller has assigned
    /// `offset` and encoded it into the blob. Returns the byte position
    /// the entry was written at (for index bookkeeping).
    ///
    /// The file is opened read+write (no `O_APPEND`), so we seek to End
    /// before each write to position correctly.
    pub fn append_raw(&mut self, encoded: &[u8], offset: Offset) -> Result<u64, WalError> {
        let pos = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(encoded)?;
        self.file.flush()?;
        self.index.push((offset, pos));
        self.retained_bytes += encoded.len() as u64;
        Ok(pos)
    }

    /// Read the entry at `pos` within this segment's file. Opens a
    /// fresh read handle (the append handle is not seek-safe for reads).
    pub fn read_at(&self, pos: u64) -> Result<WalEntry, WalError> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        f.seek(SeekFrom::Start(pos))?;
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
        let (entry, _consumed) = WalEntry::decode(&full)?;
        Ok(entry)
    }

    /// `true` when this segment has accumulated at least `max_bytes` of
    /// retained data and should be rolled.
    pub fn is_full(&self, max_bytes: u64) -> bool {
        self.retained_bytes >= max_bytes
    }

    /// Highest offset retained in this segment, or `Offset::NONE` if empty.
    pub fn head_offset(&self) -> Offset {
        self.index.last().map(|(o, _)| *o).unwrap_or(Offset::NONE)
    }

    /// Lowest offset retained in this segment, or `Offset::NONE` if empty.
    pub fn oldest_offset(&self) -> Offset {
        self.index.first().map(|(o, _)| *o).unwrap_or(Offset::NONE)
    }

    /// Number of entries in this segment.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Flush + fsync the segment's data.
    pub fn sync_data(&mut self) -> Result<(), WalError> {
        self.file.sync_data().map_err(WalError::Io)
    }

    /// Full path of the segment file (for deletion on truncate).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn rec(payload: &[u8]) -> crate::entry::WalRecord {
        crate::entry::WalRecord::new(0, Bytes::copy_from_slice(payload))
    }

    #[test]
    fn filename_round_trip() {
        for o in [0u64, 1, 42, 9_999_999, u64::MAX] {
            let name = segment_filename(Offset::new(o));
            assert_eq!(parse_segment_filename(&name), Some(Offset::new(o)));
        }
    }

    #[test]
    fn parse_rejects_non_segment() {
        assert_eq!(parse_segment_filename("log.wal"), None);
        assert_eq!(parse_segment_filename("seg-abc.wal"), None);
        assert_eq!(parse_segment_filename("seg-.wal"), None);
        assert_eq!(parse_segment_filename("notasegment"), None);
    }

    #[test]
    fn create_append_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), Offset::new(0)).unwrap();
        // Encode and append three entries.
        for i in 0u64..3 {
            let entry = rec(&[i as u8]).into_entry(Offset::new(i));
            let encoded = entry.encode();
            seg.append_raw(&encoded, Offset::new(i)).unwrap();
        }
        assert_eq!(seg.len(), 3);
        assert_eq!(seg.oldest_offset(), Offset::new(0));
        assert_eq!(seg.head_offset(), Offset::new(2));

        // Read the second entry.
        let pos1 = seg.index[1].1;
        let back = seg.read_at(pos1).unwrap();
        assert_eq!(back.offset, Offset::new(1));
        assert_eq!(back.payload.as_ref(), &[1]);
    }

    #[test]
    fn open_recover_rebuilds_index() {
        let dir = TempDir::new().unwrap();
        let path;
        {
            let mut seg = Segment::create(dir.path(), Offset::new(100)).unwrap();
            for i in 0u64..5 {
                let off = Offset::new(100 + i);
                let entry = rec(&[i as u8]).into_entry(off);
                seg.append_raw(&entry.encode(), off).unwrap();
            }
            assert_eq!(seg.len(), 5);
            path = seg.path().to_path_buf();
        }
        // Reopen.
        let seg = Segment::open_recover(path, Offset::new(100)).unwrap();
        assert_eq!(seg.base_offset, Offset::new(100));
        assert_eq!(seg.len(), 5);
        assert_eq!(seg.oldest_offset(), Offset::new(100));
        assert_eq!(seg.head_offset(), Offset::new(104));
    }

    #[test]
    fn open_recover_truncates_malformed_tail() {
        let dir = TempDir::new().unwrap();
        let path;
        {
            let mut seg = Segment::create(dir.path(), Offset::new(0)).unwrap();
            for i in 0u64..3 {
                let entry = rec(&[i as u8]).into_entry(Offset::new(i));
                seg.append_raw(&entry.encode(), Offset::new(i)).unwrap();
            }
            path = seg.path().to_path_buf();
        }
        // Append garbage to the segment file.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xFFu8; 16]).unwrap();
        }
        // Reopen — the 3 good entries should survive, garbage dropped.
        let seg = Segment::open_recover(path, Offset::new(0)).unwrap();
        assert_eq!(seg.len(), 3);
    }

    #[test]
    fn is_full_threshold() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), Offset::new(0)).unwrap();
        // Each entry is HEADER_LEN(26) + 1 byte payload + CRC(4) = 31 bytes.
        let entry = rec(b"x").into_entry(Offset::new(0));
        seg.append_raw(&entry.encode(), Offset::new(0)).unwrap();
        assert_eq!(seg.retained_bytes, 31);
        assert!(seg.is_full(30));
        assert!(!seg.is_full(32));
    }
}
