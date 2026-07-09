//! [`MediaStreamingStats`], [`StatsSnapshot`] — atomic counters and a
//! point-in-time snapshot for observability.
//!
//! See `DESIGN.md` Part 12 (observability).

use std::sync::atomic::{AtomicU64, Ordering};

// ── MediaStreamingStats ──────────────────────────────────────────────

/// Atomic counters for the media streaming system.
///
/// All fields are public so any subsystem can record events
/// directly. Use [`MediaStreamingStats::snapshot`] to get a consistent
/// point-in-time view.
#[derive(Debug)]
pub struct MediaStreamingStats {
    // ── Track lifecycle ────────────────────────────────────────────
    /// Tracks currently active (gauge).
    pub tracks_active: AtomicU64,
    /// Tracks created since startup (counter).
    pub tracks_created: AtomicU64,
    /// Tracks removed since startup (counter).
    pub tracks_removed: AtomicU64,

    // ── Packet counters ─────────────────────────────────────────────
    /// RTP packets sent (counter).
    pub packets_sent: AtomicU64,
    /// RTP packets received (counter).
    pub packets_received: AtomicU64,
    /// Bytes sent (counter).
    pub bytes_sent: AtomicU64,
    /// Bytes received (counter).
    pub bytes_received: AtomicU64,

    // ── ABR events ──────────────────────────────────────────────────
    /// Bitrate adjustments performed (counter).
    pub bitrate_adjustments: AtomicU64,
    /// Layer switches performed (counter).
    pub layer_switches: AtomicU64,

    // ── Quality metrics ────────────────────────────────────────────
    /// Frames dropped due to encoding/decoding issues (counter).
    pub frame_drops: AtomicU64,
    /// RTT samples in nanoseconds (counter; sum of all samples).
    pub rtt_ns: AtomicU64,
    /// RTT sample count (counter).
    pub rtt_samples: AtomicU64,
}

impl Default for MediaStreamingStats {
    fn default() -> Self {
        Self::new()
    }
}

impl MediaStreamingStats {
    /// Construct a zeroed stats instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tracks_active: AtomicU64::new(0),
            tracks_created: AtomicU64::new(0),
            tracks_removed: AtomicU64::new(0),
            packets_sent: AtomicU64::new(0),
            packets_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bitrate_adjustments: AtomicU64::new(0),
            layer_switches: AtomicU64::new(0),
            frame_drops: AtomicU64::new(0),
            rtt_ns: AtomicU64::new(0),
            rtt_samples: AtomicU64::new(0),
        }
    }

    /// Record a track creation.
    pub fn record_track_created(&self) {
        self.tracks_created.fetch_add(1, Ordering::Relaxed);
        self.tracks_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a track removal.
    pub fn record_track_removed(&self) {
        self.tracks_removed.fetch_add(1, Ordering::Relaxed);
        self.tracks_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a sent packet.
    pub fn record_packet_sent(&self, bytes: u64) {
        self.packets_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a received packet.
    pub fn record_packet_received(&self, bytes: u64) {
        self.packets_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a bitrate adjustment.
    pub fn record_bitrate_adjustment(&self) {
        self.bitrate_adjustments.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a layer switch.
    pub fn record_layer_switch(&self) {
        self.layer_switches.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a frame drop.
    pub fn record_frame_drop(&self) {
        self.frame_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an RTT sample.
    pub fn record_rtt(&self, rtt_ns: u64) {
        self.rtt_ns.fetch_add(rtt_ns, Ordering::Relaxed);
        self.rtt_samples.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent point-in-time snapshot.
    ///
    /// Reads happen with `Relaxed` ordering; counts may be
    /// momentarily inconsistent across fields. This is acceptable
    /// for advisory stats.
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            tracks_active: self.tracks_active.load(Ordering::Relaxed),
            tracks_created: self.tracks_created.load(Ordering::Relaxed),
            tracks_removed: self.tracks_removed.load(Ordering::Relaxed),
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            bitrate_adjustments: self.bitrate_adjustments.load(Ordering::Relaxed),
            layer_switches: self.layer_switches.load(Ordering::Relaxed),
            frame_drops: self.frame_drops.load(Ordering::Relaxed),
            rtt_ns: self.rtt_ns.load(Ordering::Relaxed),
            rtt_samples: self.rtt_samples.load(Ordering::Relaxed),
        }
    }
}

// ── StatsSnapshot ──────────────────────────────────────────────────

/// Point-in-time snapshot of media streaming counters.
///
/// All rates are precomputed so consumers don't need to do division
/// at export time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Tracks currently active.
    pub tracks_active: u64,
    /// Tracks created since startup.
    pub tracks_created: u64,
    /// Tracks removed since startup.
    pub tracks_removed: u64,
    /// Packets sent.
    pub packets_sent: u64,
    /// Packets received.
    pub packets_received: u64,
    /// Bytes sent.
    pub bytes_sent: u64,
    /// Bytes received.
    pub bytes_received: u64,
    /// Bitrate adjustments.
    pub bitrate_adjustments: u64,
    /// Layer switches.
    pub layer_switches: u64,
    /// Frame drops.
    pub frame_drops: u64,
    /// Sum of RTT samples in nanoseconds.
    pub rtt_ns: u64,
    /// RTT sample count.
    pub rtt_samples: u64,
}

impl StatsSnapshot {
    /// Packet loss rate in basis points (0..=10_000).
    ///
    /// Returns 0 when no packets have been received.
    #[must_use]
    pub const fn packet_loss_rate_bps(&self) -> u16 {
        if self.packets_sent == 0 {
            return 0;
        }

        // Estimate loss as sent - received (simplification; in reality
        // we'd track explicit loss reports).
        let lost = self.packets_sent.saturating_sub(self.packets_received);
        let rate = lost.saturating_mul(10_000) / self.packets_sent;
        if rate > u16::MAX as u64 {
            u16::MAX
        } else {
            rate as u16
        }
    }

    /// Average RTT in nanoseconds.
    ///
    /// Returns 0 if no samples.
    #[must_use]
    pub const fn avg_rtt_ns(&self) -> u64 {
        if self.rtt_samples == 0 {
            return 0;
        }
        self.rtt_ns / self.rtt_samples
    }

    /// Average RTT in milliseconds.
    ///
    /// Returns 0 if no samples.
    #[must_use]
    pub const fn avg_rtt_ms(&self) -> u64 {
        self.avg_rtt_ns() / 1_000_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stats_are_zero() {
        let s = MediaStreamingStats::new();
        let snap = s.snapshot();
        assert_eq!(snap, StatsSnapshot::default());
    }

    #[test]
    fn record_track_created_increments_counters() {
        let s = MediaStreamingStats::new();
        s.record_track_created();
        s.record_track_created();
        let snap = s.snapshot();
        assert_eq!(snap.tracks_created, 2);
        assert_eq!(snap.tracks_active, 2);
    }

    #[test]
    fn record_track_removed_decrements_active() {
        let s = MediaStreamingStats::new();
        s.record_track_created();
        s.record_track_created();
        s.record_track_removed();
        let snap = s.snapshot();
        assert_eq!(snap.tracks_removed, 1);
        assert_eq!(snap.tracks_active, 1);
    }

    #[test]
    fn record_packet_sent_increments_counters() {
        let s = MediaStreamingStats::new();
        s.record_packet_sent(1200);
        s.record_packet_sent(1400);
        let snap = s.snapshot();
        assert_eq!(snap.packets_sent, 2);
        assert_eq!(snap.bytes_sent, 2600);
    }

    #[test]
    fn record_packet_received_increments_counters() {
        let s = MediaStreamingStats::new();
        s.record_packet_received(800);
        s.record_packet_received(1000);
        let snap = s.snapshot();
        assert_eq!(snap.packets_received, 2);
        assert_eq!(snap.bytes_received, 1800);
    }

    #[test]
    fn record_bitrate_adjustment_counter() {
        let s = MediaStreamingStats::new();
        s.record_bitrate_adjustment();
        s.record_bitrate_adjustment();
        assert_eq!(s.snapshot().bitrate_adjustments, 2);
    }

    #[test]
    fn record_layer_switch_counter() {
        let s = MediaStreamingStats::new();
        s.record_layer_switch();
        assert_eq!(s.snapshot().layer_switches, 1);
    }

    #[test]
    fn record_frame_drop_counter() {
        let s = MediaStreamingStats::new();
        s.record_frame_drop();
        s.record_frame_drop();
        s.record_frame_drop();
        assert_eq!(s.snapshot().frame_drops, 3);
    }

    #[test]
    fn record_rtt_samples() {
        let s = MediaStreamingStats::new();
        s.record_rtt(50_000_000); // 50ms
        s.record_rtt(60_000_000); // 60ms
        let snap = s.snapshot();
        assert_eq!(snap.rtt_samples, 2);
        assert_eq!(snap.rtt_ns, 110_000_000);
        assert_eq!(snap.avg_rtt_ns(), 55_000_000);
        assert_eq!(snap.avg_rtt_ms(), 55);
    }

    #[test]
    fn packet_loss_rate_with_no_packets() {
        let s = MediaStreamingStats::new();
        assert_eq!(s.snapshot().packet_loss_rate_bps(), 0);
    }

    #[test]
    fn packet_loss_rate_with_traffic() {
        let s = MediaStreamingStats::new();
        // Send 100, receive 95 → 5% loss = 500 bps
        for _ in 0..100 {
            s.record_packet_sent(1200);
        }
        for _ in 0..95 {
            s.record_packet_received(1200);
        }
        let snap = s.snapshot();
        // (100 - 95) * 10000 / 100 = 500
        assert_eq!(snap.packet_loss_rate_bps(), 500);
    }

    #[test]
    fn avg_rtt_with_no_samples() {
        let s = MediaStreamingStats::new();
        assert_eq!(s.snapshot().avg_rtt_ns(), 0);
        assert_eq!(s.snapshot().avg_rtt_ms(), 0);
    }

    #[test]
    fn avg_rtt_calculation() {
        let s = MediaStreamingStats::new();
        s.record_rtt(100_000_000); // 100ms
        s.record_rtt(200_000_000); // 200ms
        s.record_rtt(300_000_000); // 300ms
        // Average = 200ms = 200,000,000ns
        assert_eq!(s.snapshot().avg_rtt_ns(), 200_000_000);
        assert_eq!(s.snapshot().avg_rtt_ms(), 200);
    }

    #[test]
    fn snapshot_is_consistent_under_repeated_reads() {
        let s = MediaStreamingStats::new();
        s.record_packet_sent(1200);
        let a = s.snapshot();
        let b = s.snapshot();
        assert_eq!(a, b);
    }

    #[test]
    fn multiple_track_lifecycle() {
        let s = MediaStreamingStats::new();
        s.record_track_created();
        s.record_track_created();
        s.record_track_created();
        s.record_track_removed();
        let snap = s.snapshot();
        assert_eq!(snap.tracks_created, 3);
        assert_eq!(snap.tracks_removed, 1);
        assert_eq!(snap.tracks_active, 2);
    }
}
