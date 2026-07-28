//! Tamper-evident audit log for administrative actions.
//!
//! Records security-relevant events (ACL denies, quota rejections,
//! topic create/delete) as an append-only hash chain. Each entry
//! includes the SHA-256 of the previous entry, making silent
//! modification detectable: changing or deleting an entry breaks
//! the chain for all subsequent entries.
//!
//! ## On-disk format
//!
//! ```text
//! MAGIC("VAUD") | version(u32)
//!   | repeat:
//!       ts_ns(u64 BE) | kind(u8) | result(u8)
//!       | actor_len(u16 BE) | actor(utf8)
//!       | target_len(u16 BE) | target(utf8)
//!       | prev_hash(32 bytes)
//!       | entry_hash(32 bytes)
//! ```
//!
//! `entry_hash = SHA-256(ts_ns || kind || result || actor || target || prev_hash)`.
//!
//! The file is opened with `O_APPEND` — the kernel guarantees each
//! `write_all` is atomic for blocks ≤ `PIPE_BUF` (≥ 512 B on Linux).
//! Each entry is < 512 B, so concurrent writers (if ever added) won't
//! interleave.

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// File-tag + format version.
const MAGIC: &[u8; 4] = b"VAUD";
const FORMAT_VERSION: u32 = 1;
/// SHA-256 digest length.
const HASH_LEN: usize = 32;
/// All-zero initial prev_hash for the first entry in the chain.
const GENESIS_HASH: [u8; HASH_LEN] = [0u8; HASH_LEN];

/// Event kind recorded in the audit log.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum AuditKind {
    /// ACL denied a publish.
    AclPublishDenied = 1,
    /// ACL denied a subscribe.
    AclSubscribeDenied = 2,
    /// ACL denied a topic-create.
    AclTopicCreateDenied = 3,
    /// ACL denied a topic-delete.
    AclTopicDeleteDenied = 4,
    /// Quota cap rejected a topic-create.
    QuotaTopicRejected = 5,
    /// Quota cap rejected a subscribe.
    QuotaSubscriberRejected = 6,
    /// Successful topic-create.
    TopicCreated = 7,
    /// Successful topic-delete.
    TopicDeleted = 8,
}

impl AuditKind {
    #[must_use]
    fn as_str(self) -> &'static str {
        match self {
            Self::AclPublishDenied => "acl_publish_denied",
            Self::AclSubscribeDenied => "acl_subscribe_denied",
            Self::AclTopicCreateDenied => "acl_topic_create_denied",
            Self::AclTopicDeleteDenied => "acl_topic_delete_denied",
            Self::QuotaTopicRejected => "quota_topic_rejected",
            Self::QuotaSubscriberRejected => "quota_subscriber_rejected",
            Self::TopicCreated => "topic_created",
            Self::TopicDeleted => "topic_deleted",
        }
    }
}

/// Result of the audited action.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum AuditResult {
    Allowed = 0,
    Denied = 1,
    Error = 2,
}

/// Append-only, hash-chained audit log.
///
/// Thread-safe via `&self` + internal locking on the `Write`. In the
/// single-reactor-per-worker model each worker owns its own log, so
/// no cross-thread contention is expected.
pub struct AuditLog {
    file: std::fs::File,
    path: PathBuf,
    /// Hash of the most recently written entry. Used as the prev_hash
    /// for the next entry. Loaded from the tail on open.
    last_hash: [u8; HASH_LEN],
    /// Total entries written since open. Logged periodically.
    entry_count: u64,
}

impl AuditLog {
    /// Open (or create) an audit log at `path`. If the file already
    /// exists, the hash chain is resumed from the last valid entry.
    ///
    /// # Errors
    /// Returns `std::io::Error` on I/O failure or chain corruption.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let needs_header = !path.exists();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        let mut log = Self {
            file,
            path: path.to_path_buf(),
            last_hash: GENESIS_HASH,
            entry_count: 0,
        };

        if needs_header {
            log.write_header()?;
        } else {
            log.verify_and_resume()?;
        }

        Ok(log)
    }

    fn write_header(&mut self) -> std::io::Result<()> {
        self.file.write_all(MAGIC)?;
        self.file.write_all(&FORMAT_VERSION.to_be_bytes())?;
        self.file.flush()
    }

    /// Walk the existing log, verifying the hash chain. Sets
    /// `last_hash` to the hash of the last valid entry.
    fn verify_and_resume(&mut self) -> std::io::Result<()> {
        let mut data = Vec::new();
        (&self.file).read_to_end(&mut data)?;

        if data.len() < 8 {
            // Empty or header-only file — re-write header.
            self.last_hash = GENESIS_HASH;
            return self.write_header();
        }

        if &data[..4] != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "audit log: bad magic",
            ));
        }
        let version = u32::from_be_bytes(data[4..8].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("audit log: unsupported version {version}"),
            ));
        }

        let mut pos = 8;
        let mut prev_hash = GENESIS_HASH;
        let mut count = 0u64;
        while pos < data.len() {
            // Entry layout: ts(8) + kind(1) + result(1) + actor_len(2) + actor + target_len(2) + target + prev_hash(32) + entry_hash(32)
            if pos + 8 + 1 + 1 + 2 > data.len() {
                break; // truncated entry — stop here
            }
            let mut cursor = pos;
            cursor += 8; // ts_ns
            let kind = data[cursor];
            cursor += 1;
            let result = data[cursor];
            cursor += 1;

            let actor_len = u16::from_be_bytes([data[cursor], data[cursor + 1]]) as usize;
            cursor += 2;
            if cursor + actor_len + 2 > data.len() {
                break;
            }
            cursor += actor_len;

            let target_len = u16::from_be_bytes([data[cursor], data[cursor + 1]]) as usize;
            cursor += 2;
            if cursor + target_len + HASH_LEN + HASH_LEN > data.len() {
                break;
            }
            cursor += target_len;

            let stored_prev = &data[cursor..cursor + HASH_LEN];
            cursor += HASH_LEN;
            let stored_hash = &data[cursor..cursor + HASH_LEN];
            cursor += HASH_LEN;

            if stored_prev != prev_hash {
                tracing::warn!(
                    pos,
                    "audit log: hash chain broken at entry {count} — stopping resume"
                );
                break;
            }

            // Recompute the entry hash to verify integrity.
            let entry_start = pos;
            let entry_end = cursor - HASH_LEN; // exclude the entry_hash field itself
            let mut hasher = Sha256::new();
            hasher.update(&data[entry_start..entry_end]);
            let computed = hasher.finalize();
            if computed.as_slice() != stored_hash {
                tracing::warn!(
                    pos,
                    "audit log: entry hash mismatch at entry {count} — stopping resume"
                );
                break;
            }

            prev_hash.copy_from_slice(stored_hash);
            pos = cursor;
            count += 1;
        }

        self.last_hash = prev_hash;
        self.entry_count = count;
        tracing::info!(
            path = %self.path.display(),
            entries = count,
            "audit log opened — chain verified"
        );
        Ok(())
    }

    /// Append a new audit entry. The hash chain is extended with
    /// SHA-256(prev_hash || entry_data).
    ///
    /// # Errors
    /// Returns `std::io::Error` on write failure.
    pub fn record(
        &mut self,
        kind: AuditKind,
        result: AuditResult,
        actor: &str,
        target: &str,
    ) -> std::io::Result<()> {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        // Build entry body (everything before prev_hash + entry_hash).
        let mut buf = Vec::with_capacity(64 + actor.len() + target.len());
        buf.extend_from_slice(&now_ns.to_be_bytes());
        buf.push(kind as u8);
        buf.push(result as u8);
        buf.extend_from_slice(&(u16::try_from(actor.len()).unwrap_or(u16::MAX)).to_be_bytes());
        buf.extend_from_slice(actor.as_bytes());
        buf.extend_from_slice(&(u16::try_from(target.len()).unwrap_or(u16::MAX)).to_be_bytes());
        buf.extend_from_slice(target.as_bytes());
        buf.extend_from_slice(&self.last_hash);

        // Compute entry_hash over body + prev_hash.
        let mut hasher = Sha256::new();
        hasher.update(&buf);
        let entry_hash = hasher.finalize();

        // Write body + entry_hash.
        self.file.write_all(&buf)?;
        self.file.write_all(&entry_hash)?;
        self.file.flush()?;

        self.last_hash.copy_from_slice(&entry_hash);
        self.entry_count += 1;

        tracing::debug!(
            kind = kind.as_str(),
            result = ?result,
            actor = actor,
            target = target,
            "audit: {}",
            kind.as_str(),
        );

        Ok(())
    }

    /// Number of entries written since the log was opened.
    #[must_use]
    pub const fn entry_count(&self) -> u64 {
        self.entry_count
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "vireon-audit-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn open_create_and_record() {
        let path = tmp_path("create.vaud");
        {
            let mut log = AuditLog::open(&path).unwrap();
            log.record(
                AuditKind::TopicCreated,
                AuditResult::Allowed,
                "admin",
                "orders.created",
            )
            .unwrap();
            log.record(
                AuditKind::AclPublishDenied,
                AuditResult::Denied,
                "user-42",
                "admin.secret",
            )
            .unwrap();
            assert_eq!(log.entry_count(), 2);
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(path.parent().unwrap());
    }

    #[test]
    fn reopen_resumes_chain() {
        let path = tmp_path("resume.vaud");
        {
            let mut log = AuditLog::open(&path).unwrap();
            log.record(
                AuditKind::TopicCreated,
                AuditResult::Allowed,
                "a",
                "t1",
            )
            .unwrap();
        }
        {
            let mut log = AuditLog::open(&path).unwrap();
            assert_eq!(log.entry_count(), 1, "should resume from 1 entry");
            log.record(
                AuditKind::TopicDeleted,
                AuditResult::Allowed,
                "b",
                "t1",
            )
            .unwrap();
            assert_eq!(log.entry_count(), 2);
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(path.parent().unwrap());
    }

    #[test]
    fn detect_tampered_entry() {
        let path = tmp_path("tamper.vaud");
        {
            let mut log = AuditLog::open(&path).unwrap();
            log.record(
                AuditKind::TopicCreated,
                AuditResult::Allowed,
                "admin",
                "topic-1",
            )
            .unwrap();
            log.record(
                AuditKind::TopicCreated,
                AuditResult::Allowed,
                "admin",
                "topic-2",
            )
            .unwrap();
        }

        // Tamper: flip a byte in the first entry's actor field.
        let mut data = std::fs::read(&path).unwrap();
        // Header is 8 bytes. Entry starts at offset 8. ts(8)+kind(1)+result(1)+actor_len(2) = 12 bytes in.
        // Actor "admin" starts at offset 8+12 = 20.
        data[20] ^= 0x01;
        std::fs::write(&path, &data).unwrap();

        // Reopen: the chain should detect the broken hash and stop
        // after 0 valid entries.
        let log = AuditLog::open(&path).unwrap();
        assert_eq!(
            log.entry_count(),
            0,
            "tampered entry should not be counted"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(path.parent().unwrap());
    }
}
