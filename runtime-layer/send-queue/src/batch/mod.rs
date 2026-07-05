//! Batch composer (Layer 5).
//!
//! [`BatchComposer`] accumulates packets drained from the scheduler and
//! decides when to flush them to the IO Layer. Flushing is triggered by
//! packet count, total bytes, or maximum packet age (whichever fires
//! first).

use std::net::SocketAddr;

use crate::queue::PacketSlot;

/// A single entry in a submission batch — the data the IO Layer needs to
/// push a `SEND_ZC` SQE.
#[derive(Clone, Copy, Debug)]
pub struct BatchEntry {
    /// Registered fixed-buffer index (for `SEND_ZC` `buf_index`).
    pub buf_index: u16,
    /// Payload length in bytes.
    pub len: u32,
    /// Destination address.
    pub dest: SocketAddr,
    /// Encoded `(tracking_slot, generation)` packed into a `u64`. The IO
    /// Layer prepends `OpKind` to produce the final `user_data`.
    pub user_data: u64,
}

impl BatchEntry {
    /// Builds a [`BatchEntry`] from a drained [`PacketSlot`].
    ///
    /// The slot's `tracking_id` is used as `user_data` directly — it
    /// already encodes `(tracking_slot, generation)`.
    #[must_use]
    pub fn from_slot(slot: &PacketSlot) -> Self {
        Self {
            buf_index: slot.buf_index,
            len: slot.len,
            dest: slot.dest,
            user_data: slot.tracking_id,
        }
    }
}

/// Accumulates packets and decides when to flush based on configurable
/// triggers.
#[derive(Debug)]
pub struct BatchComposer {
    /// Pending entries waiting to be flushed.
    entries: Vec<BatchEntry>,
    /// Total bytes accumulated in the current batch.
    total_bytes: u64,
    /// Timestamp (monotonic nanos) when the first entry was added.
    /// `0` means the batch is empty.
    first_entry_ns: u64,
    /// Maximum packets per batch.
    max_packets: usize,
    /// Maximum bytes per batch.
    max_bytes: u64,
    /// Maximum age in nanoseconds before a flush is forced.
    max_age_ns: u64,
}

impl BatchComposer {
    /// Creates a new composer with the given limits.
    #[must_use]
    pub fn new(max_packets: usize, max_bytes: usize, max_age_ns: u64) -> Self {
        Self {
            entries: Vec::with_capacity(max_packets),
            total_bytes: 0,
            first_entry_ns: 0,
            max_packets,
            max_bytes: max_bytes as u64,
            max_age_ns,
        }
    }

    /// Whether the batch currently holds any entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Current number of pending entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Adds a slot to the pending batch.
    pub fn push(&mut self, slot: &PacketSlot, now_ns: u64) {
        if self.entries.is_empty() {
            self.first_entry_ns = now_ns;
        }
        self.total_bytes += u64::from(slot.len);
        self.entries.push(BatchEntry::from_slot(slot));
    }

    /// Pushes a raw entry back into the batch (used when the sink accepted
    /// only a partial batch — the remainder is preserved for the next
    /// flush attempt).
    pub fn push_entry(&mut self, entry: BatchEntry, now_ns: u64) {
        if self.entries.is_empty() {
            self.first_entry_ns = now_ns;
        }
        self.total_bytes += u64::from(entry.len);
        self.entries.push(entry);
    }

    /// Whether any flush trigger has fired.
    #[must_use]
    pub fn should_flush(&self, now_ns: u64) -> bool {
        if self.entries.is_empty() {
            return false;
        }
        // Trigger 1: packet count.
        if self.entries.len() >= self.max_packets {
            return true;
        }
        // Trigger 2: total bytes.
        if self.total_bytes >= self.max_bytes {
            return true;
        }
        // Trigger 3: oldest entry age.
        if now_ns.saturating_sub(self.first_entry_ns) >= self.max_age_ns {
            return true;
        }
        false
    }

    /// Drains the pending batch, returning the entries.
    ///
    /// Resets the composer to an empty state.
    pub fn flush(&mut self) -> Vec<BatchEntry> {
        self.total_bytes = 0;
        self.first_entry_ns = 0;
        std::mem::take(&mut self.entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_slot(tracking_id: u64, len: u32) -> PacketSlot {
        let mut s = PacketSlot::empty();
        s.fill(
            tracking_id,
            0,
            len,
            "10.0.0.1:80".parse::<SocketAddr>().unwrap(),
            0,
            0,
            tracking_id,
        );
        s
    }

    #[test]
    fn empty_composer_is_empty() {
        let bc = BatchComposer::new(64, 65_536, 50_000);
        assert!(bc.is_empty());
        assert_eq!(bc.len(), 0);
        assert!(!bc.should_flush(0));
    }

    #[test]
    fn count_trigger_fires() {
        let mut bc = BatchComposer::new(4, 65_536, 50_000);
        for i in 0..3 {
            bc.push(&make_slot(i, 64), 0);
        }
        assert!(!bc.should_flush(0));
        bc.push(&make_slot(3, 64), 0);
        assert!(bc.should_flush(0));
    }

    #[test]
    fn bytes_trigger_fires() {
        let mut bc = BatchComposer::new(64, 256, 50_000);
        bc.push(&make_slot(0, 128), 0);
        assert!(!bc.should_flush(0));
        bc.push(&make_slot(1, 128), 0);
        assert!(bc.should_flush(0)); // 256 bytes >= 256
    }

    #[test]
    fn age_trigger_fires() {
        let mut bc = BatchComposer::new(64, 65_536, 50_000);
        bc.push(&make_slot(0, 64), 10_000);
        assert!(!bc.should_flush(10_000));
        assert!(!bc.should_flush(50_000));
        assert!(bc.should_flush(60_001)); // 60_001 - 10_000 = 50_001 >= 50_000
    }

    #[test]
    fn flush_returns_and_resets() {
        let mut bc = BatchComposer::new(64, 65_536, 50_000);
        bc.push(&make_slot(0, 64), 0);
        bc.push(&make_slot(1, 128), 0);
        let entries = bc.flush();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].user_data, 0);
        assert_eq!(entries[1].user_data, 1);
        assert!(bc.is_empty());
        assert_eq!(bc.len(), 0);
    }

    #[test]
    fn batch_entry_from_slot_uses_tracking_id() {
        let mut slot = PacketSlot::empty();
        slot.fill(42, 5, 1024, "10.0.0.1:80".parse().unwrap(), 0, 0, 99);
        let entry = BatchEntry::from_slot(&slot);
        assert_eq!(entry.buf_index, 5);
        assert_eq!(entry.len, 1024);
        assert_eq!(entry.user_data, 42); // tracking_id
    }
}
