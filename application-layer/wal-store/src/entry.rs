//! [`WalEntry`] and [`WalRecord`] — wire format and ownership split.
//!
//! Each entry on disk is laid out as:
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │ magic        u32   0x57_4F_4C_31 ("WOL1")                  │
//! │ offset       u64   store-assigned monotonic offset          │
//! │ timestamp_ns u64   nanoseconds since UNIX epoch             │
//! │ key_len      u16   length of `key` in bytes                 │
//! │ payload_len  u32   length of `payload` in bytes             │
//! │ key          [u8; key_len]                                  │
//! │ payload      [u8; payload_len]                              │
//! │ crc32        u32   over everything above (magic..payload)   │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! Fixed header is 4 + 8 + 8 + 2 + 4 = 26 bytes; CRC trailer is 4 bytes.
//! `key` is optional (zero-length is fine) — it carries a routing key
//! or partitioning hint that consumers can use for filtering without
//! deserializing the payload.
//!
//! The `offset` field is the only header value the store assigns
//! (everything else is set by the publisher). Persisting it lets the
//! file backend recover the original offset numbering after a restart
//! and after a truncate-then-restart cycle.

use crate::error::WalError;
use crate::offset::Offset;
use bytes::Bytes;

/// Fixed-size wire prefix shared by every entry.
pub const MAGIC: u32 = 0x574F_4C31; // "WOL1"
pub const HEADER_LEN: usize = 4 + 8 + 8 + 2 + 4;
pub const CRC_LEN: usize = 4;

/// An entry as it travels through the WAL API.
///
/// `offset` is filled in by [`crate::WalStore::append`] — the caller
/// constructing the entry for the first time uses
/// [`WalRecord::into_entry`] with `Offset::NONE` as a placeholder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalEntry {
    /// The offset the store assigned to this entry. `Offset::NONE` for
    /// entries that have not yet been appended.
    pub offset: Offset,
    /// Nanoseconds since UNIX epoch. The store does not interpret this
    /// value; retention by time compares it against `now`.
    pub timestamp_ns: u64,
    /// Routing / partitioning key. May be empty.
    pub key: Bytes,
    /// Opaque payload bytes. The store does not interpret this value.
    pub payload: Bytes,
}

/// Same as [`WalEntry`] but without an assigned offset — the form a
/// publisher builds before calling `append`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecord {
    pub timestamp_ns: u64,
    pub key: Bytes,
    pub payload: Bytes,
}

impl WalRecord {
    /// Construct a record.
    #[inline]
    #[must_use]
    pub fn new(timestamp_ns: u64, payload: Bytes) -> Self {
        Self {
            timestamp_ns,
            key: Bytes::new(),
            payload,
        }
    }

    /// Attach a routing key.
    #[inline]
    #[must_use]
    pub fn with_key(mut self, key: Bytes) -> Self {
        self.key = key;
        self
    }

    /// Promote to a [`WalEntry`] with the given offset assigned.
    #[inline]
    #[must_use]
    pub fn into_entry(self, offset: Offset) -> WalEntry {
        WalEntry {
            offset,
            timestamp_ns: self.timestamp_ns,
            key: self.key,
            payload: self.payload,
        }
    }
}

impl WalEntry {
    /// Wire-format length of this entry (header + key + payload + crc).
    #[inline]
    #[must_use]
    pub fn wire_len(&self) -> usize {
        HEADER_LEN + self.key.len() + self.payload.len() + CRC_LEN
    }

    /// Encode into a freshly-allocated `Vec<u8>`. The caller is
    /// responsible for writing the result to disk.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let total = self.wire_len();
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&self.offset.get().to_le_bytes());
        out.extend_from_slice(&self.timestamp_ns.to_le_bytes());
        out.extend_from_slice(&(self.key.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.key);
        out.extend_from_slice(&self.payload);
        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(out.len(), total);
        out
    }

    /// Decode the first entry from `buf`. Returns the entry and the
    /// number of bytes consumed.
    ///
    /// Returns [`WalError::MalformedEntry`] if the buffer is too short,
    /// the magic is wrong, or the CRC check fails.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), WalError> {
        if buf.len() < HEADER_LEN + CRC_LEN {
            return Err(WalError::MalformedEntry {
                reason: "buffer shorter than header + crc",
            });
        }
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != MAGIC {
            return Err(WalError::MalformedEntry {
                reason: "bad magic",
            });
        }
        let offset = Offset::new(u64::from_le_bytes([
            buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
        ]));
        let timestamp_ns = u64::from_le_bytes([
            buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
        ]);
        let key_len = u16::from_le_bytes([buf[20], buf[21]]) as usize;
        let payload_len = u32::from_le_bytes([buf[22], buf[23], buf[24], buf[25]]) as usize;
        let total = HEADER_LEN + key_len + payload_len + CRC_LEN;
        if buf.len() < total {
            return Err(WalError::MalformedEntry {
                reason: "buffer shorter than declared length",
            });
        }
        // CRC covers bytes [0 .. total - 4).
        let body = &buf[..total - CRC_LEN];
        let want_crc = u32::from_le_bytes([
            buf[total - 4],
            buf[total - 3],
            buf[total - 2],
            buf[total - 1],
        ]);
        let got_crc = crc32(body);
        if got_crc != want_crc {
            return Err(WalError::MalformedEntry {
                reason: "crc mismatch",
            });
        }
        let key = Bytes::copy_from_slice(&buf[HEADER_LEN..HEADER_LEN + key_len]);
        let payload =
            Bytes::copy_from_slice(&buf[HEADER_LEN + key_len..HEADER_LEN + key_len + payload_len]);
        Ok((
            Self {
                offset,
                timestamp_ns,
                key,
                payload,
            },
            total,
        ))
    }
}

// ── CRC-32 (IEEE polynomial 0xEDB88320) ────────────────────────────

use std::sync::OnceLock;

/// Precomputed table — built at first use and reused for the lifetime
/// of the process. Stored as `Box<[u32; 256]>` so the lock can hold it
/// behind a stable pointer.
static CRC_TABLE: OnceLock<Box<[u32; 256]>> = OnceLock::new();

fn crc32(data: &[u8]) -> u32 {
    let table = CRC_TABLE.get_or_init(|| {
        let mut t = Box::new([0u32; 256]);
        for (i, entry) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                if c & 1 != 0 {
                    c = 0xEDB8_8320 ^ (c >> 1);
                } else {
                    c >>= 1;
                }
            }
            *entry = c;
        }
        t
    });
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ table[idx];
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn record_into_entry_assigns_offset() {
        let r = WalRecord::new(123, Bytes::from_static(b"hello"));
        let e = r.into_entry(Offset::new(7));
        assert_eq!(e.offset, Offset::new(7));
        assert_eq!(e.timestamp_ns, 123);
        assert!(e.key.is_empty());
        assert_eq!(e.payload.as_ref(), b"hello");
    }

    #[test]
    fn encode_then_decode_round_trips() {
        let e = WalRecord::new(99, Bytes::from_static(b"payload bytes"))
            .with_key(Bytes::from_static(b"route-key"))
            .into_entry(Offset::new(42));
        let bytes = e.encode();
        assert_eq!(bytes.len(), e.wire_len());

        let (back, consumed) = WalEntry::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(back.offset, Offset::new(42));
        assert_eq!(back.timestamp_ns, 99);
        assert_eq!(back.key.as_ref(), b"route-key");
        assert_eq!(back.payload.as_ref(), b"payload bytes");
    }

    #[test]
    fn decode_empty_payload() {
        let e = WalRecord::new(0, Bytes::new()).into_entry(Offset::new(0));
        let bytes = e.encode();
        let (back, _) = WalEntry::decode(&bytes).unwrap();
        assert!(back.payload.is_empty());
        assert!(back.key.is_empty());
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bytes = WalRecord::new(0, Bytes::new())
            .into_entry(Offset::new(0))
            .encode();
        bytes[0] = 0xFF; // corrupt magic
        assert!(matches!(
            WalEntry::decode(&bytes),
            Err(WalError::MalformedEntry { .. })
        ));
    }

    #[test]
    fn decode_rejects_corrupt_crc() {
        let mut bytes = WalRecord::new(0, Bytes::from_static(b"data"))
            .into_entry(Offset::new(0))
            .encode();
        // Flip a payload byte — CRC should fail.
        let flip_at = bytes.len() - 5; // somewhere in the body
        bytes[flip_at] ^= 0xFF;
        assert!(matches!(
            WalEntry::decode(&bytes),
            Err(WalError::MalformedEntry {
                reason: "crc mismatch"
            })
        ));
    }

    #[test]
    fn decode_rejects_short_buffer() {
        let short = [0u8; 5];
        assert!(matches!(
            WalEntry::decode(&short),
            Err(WalError::MalformedEntry { .. })
        ));
    }

    #[test]
    fn crc32_known_vectors() {
        // Standard CRC-32 test vectors.
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"abc"), 0x3524_41C2);
        assert_eq!(crc32(b"hello world"), 0x0D4A_1185);
    }
}
