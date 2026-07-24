//! Realistic per-`MessageType` workload generator.
//!
//! Builds one workload containing all 20 frame types — each with a payload
//! whose size and shape match what a real Vireon client would send. Unlike
//! the previous design, payloads are stored **raw** (not pre-encoded); the
//! layer stack encodes them at send time via `ClientStack::encode_frame_coalesced`.
//!
//! ## Target batch size: ~1 MiB

use frame::header::{ALL_MESSAGE_TYPES, FrameFlags, MessageType, Version};
use frame::payload::{Handshake, ParsedPayload, encode_payload};

use crate::conn_state::STREAM_ID_COUNT;

/// Per-type metadata, surfaced in the summary table so the operator can verify
/// the realistic workload at a glance.
#[derive(Clone)]
pub struct WorkloadEntry {
    /// The message type this entry exercises.
    pub msg_type: MessageType,
    /// Human-readable label for the summary table.
    pub name: &'static str,
    /// Payload byte count (excludes the 22-byte header).
    pub payload_len: usize,
    /// Per-type flags matching how a real client would tag each frame.
    pub flags: FrameFlags,
    /// Raw payload bytes (encoded at send time by the layer stack).
    pub payload: Vec<u8>,
    /// Logical stream index (0..19) — mapped to QUIC stream `i * 4`.
    pub logical_stream: u64,
}

/// How many consecutive copies of a frame we encode + send in one
/// `stream_send` call.
///
/// Sending 32 copies in a single `stream_send` lets quiche fill several
/// full-size QUIC packets from one call, reducing per-frame quiche overhead and
/// lowering the number of `conn.send()` calls by roughly this factor.
pub const COALESCE: usize = 32;

/// The complete workload.
pub struct Workload {
    /// Per-type entries with raw payloads.
    pub entries: Box<[WorkloadEntry]>,
    /// Sum of every payload length (no headers) — for the summary.
    pub total_payload_bytes: usize,
    /// Total wire bytes for one un-coalesced batch (all 20 frames × 1 copy).
    pub batch_wire_bytes: usize,
    /// How many consecutive copies of each frame to coalesce per stream_send.
    /// The full workload uses [`COALESCE`] (32); latency sweeps use 1 to avoid
    /// exceeding QUIC flow-control windows with large payloads.
    pub coalesce: usize,
}

/// Builds a single-frame workload with exactly `payload_bytes` of TextToken
/// data on logical stream 0.
///
/// Used for the payload-size sweep — isolates payload size as the sole
/// variable while keeping the encode/decode path identical to the full
/// workload.
pub fn build_fixed_size(payload_bytes: usize) -> anyhow::Result<Workload> {
    let payload = vec![0u8; payload_bytes];
    let batch_wire_bytes = frame::codec::HEADER_SIZE + payload_bytes + frame::codec::CRC_SIZE;
    Ok(Workload {
        entries: vec![WorkloadEntry {
            msg_type: MessageType::TextToken,
            name: "TextToken",
            payload_len: payload_bytes,
            flags: FrameFlags::NONE,
            payload,
            logical_stream: 0,
        }]
        .into_boxed_slice(),
        total_payload_bytes: payload_bytes,
        batch_wire_bytes,
        coalesce: 1,
    })
}

/// Topic + group used by the `--mode group` consumer-group scenario.
/// Kept as a `pub const` so `conn_state` can build matching Publish +
/// ConsumerGroupJoin payloads without duplicating the strings.
pub const GROUP_TOPIC: &str = "bench.group";
/// Consumer-group name all `--mode group` members join.
pub const GROUP_NAME: &str = "bench";
/// Partitions declared at join time (the value is advisory for the
/// current group coordinator — delivery is round-robin regardless).
pub const GROUP_PARTITIONS: u32 = 4;

/// Topic used by the `--mode broadcast` scenario. Subscribers send a
/// Subscribe frame matching this exact topic; the publisher streams
/// Publish frames to it. The server fans each publish out to every
/// active subscriber (unlike consumer-group round-robin).
pub const BROADCAST_TOPIC: &str = "bench.broadcast";

/// Number of bytes reserved at the start of a Publish data payload
/// (after `topic_len + topic`) to carry an embedded `bench_seq: u64 BE`.
/// This lets the member/subscriber verify per-message delivery order
/// without relying on the frame-header seq (which the server overwrites).
pub const BENCH_SEQ_BYTES: usize = 8;

/// Builds the 1-entry workload used for display/logging when running
/// `--mode group`. The actual group-mode frames (Publish for the
/// publisher, ConsumerGroupJoin for members) are built inside
/// `ConnState`, not from this `entries` slice — but `run_benchmark`
/// and the summary table still reference a `Workload`, so this keeps
/// the displayed breakdown honest (one Publish entry to `bench.group`)
/// rather than showing the irrelevant 25-type blend.
///
/// Coalesce is 1 so each Publish frame carries a unique embedded
/// `bench_seq`. Use `--coalesce N` for raw-throughput runs without
/// verification.
pub fn build_group() -> anyhow::Result<Workload> {
    Ok(build_publish_workload(GROUP_TOPIC, 1))
}

/// Builds the 1-entry workload used for `--mode broadcast`. Same
/// structure as `build_group` but targets `BROADCAST_TOPIC`. Coalesce
/// is 1 for per-message seq verification.
pub fn build_broadcast() -> anyhow::Result<Workload> {
    Ok(build_publish_workload(BROADCAST_TOPIC, 1))
}

/// Shared builder for group/broadcast: one Publish entry to `topic`
/// with `coalesce` copies per stream_send.
fn build_publish_workload(topic: &str, coalesce: usize) -> Workload {
    let topic_b = topic.as_bytes();
    // data = bench_seq(8) + padding to fill ~4 KiB
    let data_payload = vec![0xE8u8; 4096];
    let mut payload = Vec::with_capacity(2 + topic_b.len() + data_payload.len());
    payload.extend_from_slice(&(topic_b.len() as u16).to_be_bytes());
    payload.extend_from_slice(topic_b);
    payload.extend_from_slice(&data_payload);
    let payload_len = payload.len();
    Workload {
        entries: vec![WorkloadEntry {
            msg_type: MessageType::Publish,
            name: "Publish",
            payload_len,
            flags: FrameFlags::NONE,
            payload,
            logical_stream: 0,
        }]
        .into_boxed_slice(),
        total_payload_bytes: payload_len,
        batch_wire_bytes: frame::codec::HEADER_SIZE + payload_len + frame::codec::CRC_SIZE,
        coalesce,
    }
}

/// Builds a Subscribe frame payload for `topic`: `pattern_len:u16 BE |
/// pattern | qos:u8`. Used by broadcast subscribers to register interest
/// with the server's `pubsub_registry`.
pub fn build_subscribe_payload(topic: &str) -> Vec<u8> {
    let pattern = topic.as_bytes();
    let mut v = Vec::with_capacity(2 + pattern.len() + 1);
    v.extend_from_slice(&(pattern.len() as u16).to_be_bytes());
    v.extend_from_slice(pattern);
    v.push(0); // qos = 0 (at-most-once)
    v
}

/// Builds the full 20-frame workload.
///
/// Each `MessageType` is exercised, but they are mapped onto the 15
/// QUIC stream ids declared in `conn_state::STREAM_IDS` (the bench
/// architecture's "1 user = 15 streams" model). With 20 message types
/// and 15 streams, types 15..=19 share a stream id with types 0..=4 —
/// this is realistic: a single connection multiplexes many frame kinds
/// across a bounded stream pool.
pub fn build() -> anyhow::Result<Workload> {
    let mut entries: Vec<WorkloadEntry> = Vec::with_capacity(ALL_MESSAGE_TYPES.len());
    let mut total_payload_bytes = 0usize;

    for (i, &mt) in ALL_MESSAGE_TYPES.iter().enumerate() {
        let payload = realistic_payload(mt);
        let payload_len = payload.len();
        total_payload_bytes += payload_len;

        entries.push(WorkloadEntry {
            msg_type: mt,
            name: type_name(mt),
            payload_len,
            flags: realistic_flags(mt),
            payload,
            // STREAM_IDS has 15 slots; cycle so types ≥15 share a stream
            // with an earlier type instead of indexing out of bounds.
            logical_stream: (i % STREAM_ID_COUNT) as u64,
        });
    }

    let batch_wire_bytes = entries.len() * frame::codec::HEADER_SIZE + total_payload_bytes;

    Ok(Workload {
        entries: entries.into_boxed_slice(),
        total_payload_bytes,
        batch_wire_bytes,
        coalesce: COALESCE,
    })
}

// ---- per-type payloads -----------------------------------------------------

/// Returns a realistic payload for `mt`.
///
/// Target: 20 frames total ≈ 1 MiB. Heavy media frames carry the bulk;
/// text/control types are larger-than-typical but still realistic.
fn realistic_payload(mt: MessageType) -> Vec<u8> {
    match mt {
        // 100 KiB pseudo-ASCII — simulates an LLM streaming a large document chunk.
        MessageType::TextToken => {
            let mut v = vec![0u8; 100_000];
            fill_pseudo_text(&mut v, 0xA0);
            v
        }

        // Opus ~10 s @ 64 kbps ≈ 80 KiB. toc byte 0xC0 = CELT-only, 20 ms.
        MessageType::AudioFrame => {
            let mut v = vec![0u8; 80_000];
            v[0] = 0xC0;
            fill_pseudo_media(&mut v[1..], 0xA1);
            v
        }

        // H.264 I-frame at ~2.5 Mbit/s, 1 s ≈ 320 KiB.
        // Annex-B start code + IDR NAL header (0x65).
        MessageType::VideoFrame => {
            let mut v = vec![0u8; 320_000];
            v[..5].copy_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x65]);
            v[5] = 0x88;
            fill_pseudo_media(&mut v[6..], 0xB2);
            v
        }

        // JPEG full-resolution image ≈ 256 KiB. SOI + APP0 + EOI markers.
        MessageType::ImageFrame => {
            let mut v = vec![0u8; 262_144];
            v[..4].copy_from_slice(&[0xFF, 0xD8, 0xFF, 0xE0]);
            v[262_142..].copy_from_slice(&[0xFF, 0xD9]);
            fill_pseudo_media(&mut v[4..262_142], 0xC3);
            v
        }

        // 4 KiB JSON — tool name + large pseudo-embedding arguments.
        MessageType::ToolCall => {
            let mut v = Vec::with_capacity(4096);
            let prefix = br#"{"id":"call_a3f0","name":"vector_search","arguments":{"query":"rust async","embedding":"#;
            v.extend_from_slice(prefix);
            let need = 4096usize.saturating_sub(prefix.len() + 3);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD0);
            v.extend_from_slice(&tmp);
            v.extend_from_slice(b"\"}}");
            v
        }

        // 148 KiB JSON — RAG results with full document snippets.
        MessageType::ToolResult => {
            let mut v = Vec::with_capacity(151_552);
            let prefix = br#"{"id":"call_a3f0","results":[{"score":0.987,"text":"#;
            v.extend_from_slice(prefix);
            let need = 151_552usize.saturating_sub(prefix.len() + 4);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD1);
            v.extend_from_slice(&tmp);
            v.extend_from_slice(b"\"}]}");
            v
        }

        // 4 KiB document-delta patch.
        MessageType::StateUpdate => {
            let mut v = Vec::with_capacity(4096);
            let prefix = br#"{"type":"doc_delta","ops":[{"op":"insert","text":"#;
            v.extend_from_slice(prefix);
            let need = 4096usize.saturating_sub(prefix.len() + 4);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD2);
            v.extend_from_slice(&tmp);
            v.extend_from_slice(b"\"}]}");
            v
        }

        // 2 KiB error with stack trace.
        MessageType::Error => {
            let mut v = Vec::with_capacity(2048);
            let prefix = br#"{"code":"INTERNAL_ERROR","trace":"#;
            v.extend_from_slice(prefix);
            let need = 2048usize.saturating_sub(prefix.len() + 2);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD3);
            v.extend_from_slice(&tmp);
            v.extend_from_slice(b"\"}");
            v
        }

        // 64 bytes — keep heartbeat tiny so latency measurement is accurate.
        MessageType::Heartbeat => br#"{"ts":1719900000000,"seq":1042,"ok":true}"#.to_vec(),

        // 1 KiB stream-open metadata + QoS policy.
        MessageType::StreamOpen => {
            let mut v = Vec::with_capacity(1024);
            let prefix = br#"{"stream_id":"chat_42","type":"bidi","meta":"#;
            v.extend_from_slice(prefix);
            let need = 1024usize.saturating_sub(prefix.len() + 2);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD4);
            v.extend_from_slice(&tmp);
            v.push(b'}');
            v
        }

        // 64 bytes — stream-close is a control signal.
        MessageType::StreamClose => br#"{"code":0,"reason":"done","seq":9999}"#.to_vec(),

        // 256 bytes reset with last-seq and reason.
        MessageType::StreamReset => {
            let mut v = Vec::with_capacity(256);
            let prefix = br#"{"code":7,"reason":"timeout","detail":"#;
            v.extend_from_slice(prefix);
            let need = 256usize.saturating_sub(prefix.len() + 2);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD5);
            v.extend_from_slice(&tmp);
            v.push(b'}');
            v
        }

        // 21-byte handshake — the real wire format the server decodes:
        // version(1) + capabilities(4) + principal_id(8) + credential_token(8).
        // The bench claims principal 42 with a fixed credential token; the
        // server's `AuthenticationPipeline` will reject it (operator hasn't
        // registered the principal) but the connection still completes with
        // the default principal, which is enough to exercise the decode path
        // and keep the bench compatible with pre-auth deployments.
        MessageType::Handshake => encode_payload(&ParsedPayload::Handshake(Handshake {
            version: Version::new(1),
            capabilities: 0xFFFF_FFFF,
            principal_id: 42,
            credential_token: 0xDEAD_BEEF_CAFE_BABE,
        })),

        // 120 KiB video NAL continuation (CABAC residuals).
        MessageType::Fragment => {
            let mut v = vec![0u8; 122_880];
            fill_pseudo_media(&mut v, 0xE4);
            v
        }

        // 64 bytes — ACK is a control frame.
        MessageType::Ack => br#"{"ack_seq":1042,"highest_seq":1042,"delay_ms":3}"#.to_vec(),

        // 32-byte subscribe control: 2-byte len + pattern + 1-byte qos.
        MessageType::Subscribe => {
            let pattern = b"sensor.temperature.zone.*";
            let mut v = Vec::with_capacity(2 + pattern.len() + 1);
            v.extend_from_slice(&(pattern.len() as u16).to_be_bytes());
            v.extend_from_slice(pattern);
            v.push(1); // qos
            v
        }

        // 28-byte unsubscribe control: 2-byte len + pattern.
        MessageType::Unsubscribe => {
            let pattern = b"sensor.temperature.zone.*";
            let mut v = Vec::with_capacity(2 + pattern.len());
            v.extend_from_slice(&(pattern.len() as u16).to_be_bytes());
            v.extend_from_slice(pattern);
            v
        }

        // 4 KiB publish — pseudo media chunk as opaque bytes.
        MessageType::Publish => {
            let topic = b"sensor.temperature.zone1";
            let mut v = Vec::with_capacity(2 + topic.len() + 4096);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            let mut payload = vec![0u8; 4096];
            fill_pseudo_media(&mut payload, 0xE8);
            v.extend_from_slice(&payload);
            v
        }

        // TopicCreate — small control: topic + partitions(4) + retention_ms(8).
        MessageType::TopicCreate => {
            let topic = b"metrics.cpu.host.*";
            let mut v = Vec::with_capacity(2 + topic.len() + 12);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&8u32.to_be_bytes());
            v.extend_from_slice(&(3_600_000u64).to_be_bytes());
            v
        }

        // TopicDelete — minimal control: 2-byte len + topic.
        MessageType::TopicDelete => {
            let topic = b"metrics.cpu.host.stale";
            let mut v = Vec::with_capacity(2 + topic.len());
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v
        }

        // Fetch — request replay from offset 0, up to 32 entries. The
        // topic matches the `Publish` entry so a server with `--wal-root`
        // set actually has WAL state to replay; otherwise the Fetch arm
        // returns empty (or `wal_disabled`).
        MessageType::Fetch => {
            let topic = b"sensor.temperature.zone1";
            let mut v = Vec::with_capacity(2 + topic.len() + 12);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&0u64.to_be_bytes()); // from_offset
            v.extend_from_slice(&32u32.to_be_bytes()); // max_count
            v
        }

        // FetchReply — one replayed WAL entry: small payload is fine.
        MessageType::FetchReply => {
            let topic = b"sensor.temperature.zone1";
            let payload = b"replayed-entry-payload";
            let mut v = Vec::with_capacity(2 + topic.len() + 20 + payload.len());
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&0u64.to_be_bytes()); // offset
            v.extend_from_slice(&0u64.to_be_bytes()); // timestamp_ns
            v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            v.extend_from_slice(payload);
            v
        }

        // ConsumerGroupJoin — topic + group + consumer + partitions.
        MessageType::ConsumerGroupJoin => {
            let topic = b"sensor.temperature.zone1";
            let group = b"zone1-alerts";
            let consumer = b"consumer-1";
            let mut v =
                Vec::with_capacity(2 + topic.len() + 2 + group.len() + 2 + consumer.len() + 4);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&(group.len() as u16).to_be_bytes());
            v.extend_from_slice(group);
            v.extend_from_slice(&(consumer.len() as u16).to_be_bytes());
            v.extend_from_slice(consumer);
            v.extend_from_slice(&4u32.to_be_bytes()); // partitions
            v
        }

        // ConsumerGroupLeave — topic + group + consumer.
        MessageType::ConsumerGroupLeave => {
            let topic = b"sensor.temperature.zone1";
            let group = b"zone1-alerts";
            let consumer = b"consumer-1";
            let mut v = Vec::with_capacity(2 + topic.len() + 2 + group.len() + 2 + consumer.len());
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&(group.len() as u16).to_be_bytes());
            v.extend_from_slice(group);
            v.extend_from_slice(&(consumer.len() as u16).to_be_bytes());
            v.extend_from_slice(consumer);
            v
        }

        // ConsumerGroupHeartbeat — topic + group + consumer + generation.
        MessageType::ConsumerGroupHeartbeat => {
            let topic = b"sensor.temperature.zone1";
            let group = b"zone1-alerts";
            let consumer = b"consumer-1";
            let mut v =
                Vec::with_capacity(2 + topic.len() + 2 + group.len() + 2 + consumer.len() + 4);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&(group.len() as u16).to_be_bytes());
            v.extend_from_slice(group);
            v.extend_from_slice(&(consumer.len() as u16).to_be_bytes());
            v.extend_from_slice(consumer);
            v.extend_from_slice(&1u32.to_be_bytes()); // generation
            v
        }
    }
}

/// Per-type flags matching how a real client would tag each frame.
fn realistic_flags(mt: MessageType) -> FrameFlags {
    match mt {
        MessageType::Heartbeat => FrameFlags::URGENT,
        MessageType::Fragment => FrameFlags::FRAGMENT | FrameFlags::FIN,
        MessageType::StreamClose => FrameFlags::FIN,
        MessageType::ToolCall | MessageType::Ack => FrameFlags::ACK_REQ,
        _ => FrameFlags::NONE,
    }
}

/// Human-readable name for the summary table.
fn type_name(mt: MessageType) -> &'static str {
    match mt {
        MessageType::TextToken => "TextToken",
        MessageType::AudioFrame => "AudioFrame",
        MessageType::VideoFrame => "VideoFrame",
        MessageType::ImageFrame => "ImageFrame",
        MessageType::ToolCall => "ToolCall",
        MessageType::ToolResult => "ToolResult",
        MessageType::StateUpdate => "StateUpdate",
        MessageType::Error => "Error",
        MessageType::Heartbeat => "Heartbeat",
        MessageType::StreamOpen => "StreamOpen",
        MessageType::StreamClose => "StreamClose",
        MessageType::StreamReset => "StreamReset",
        MessageType::Handshake => "Handshake",
        MessageType::Fragment => "Fragment",
        MessageType::Ack => "Ack",
        MessageType::Subscribe => "Subscribe",
        MessageType::Unsubscribe => "Unsubscribe",
        MessageType::Publish => "Publish",
        MessageType::TopicCreate => "TopicCreate",
        MessageType::TopicDelete => "TopicDelete",
        MessageType::Fetch => "Fetch",
        MessageType::FetchReply => "FetchReply",
        MessageType::ConsumerGroupJoin => "ConsumerGroupJoin",
        MessageType::ConsumerGroupLeave => "ConsumerGroupLeave",
        MessageType::ConsumerGroupHeartbeat => "ConsumerGroupHeartbeat",
    }
}

/// Deterministic xorshift64 fill — produces non-trivial, weakly compressible
/// bytes that resemble real compressed media.
fn fill_pseudo_media(buf: &mut [u8], seed: u64) {
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    for b in buf.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *b = (state & 0xFF) as u8;
    }
}

/// Fills `buf` with printable ASCII (0x20–0x7E) using xorshift64 PRNG.
fn fill_pseudo_text(buf: &mut [u8], seed: u64) {
    let mut state = seed.wrapping_add(0x6C62_272E_07BB_0142);
    for b in buf.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *b = 0x20 + (state % 95) as u8;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_has_twenty_five_entries() {
        // 25 = 22 base message types + ConsumerGroupJoin/Leave/Heartbeat (P5).
        let w = build().unwrap();
        assert_eq!(w.entries.len(), 25);
    }

    #[test]
    fn batch_is_at_least_one_mib() {
        let w = build().unwrap();
        assert!(
            w.batch_wire_bytes >= 1_024 * 1_024,
            "batch is only {} bytes, expected >= 1 MiB",
            w.batch_wire_bytes
        );
    }

    #[test]
    fn logical_streams_cycle_within_stream_id_count() {
        // The 20 workload entries cycle through the 15 QUIC stream IDs
        // (types 15..=19 share a stream with 0..=4) — see `build()` for
        // why this mod-cycle is required.
        let w = build().unwrap();
        for (i, e) in w.entries.iter().enumerate() {
            assert_eq!(e.logical_stream, (i % STREAM_ID_COUNT) as u64);
        }
    }

    #[test]
    fn fixed_size_workload_has_correct_wire_bytes() {
        let w = build_fixed_size(4096).unwrap();
        assert_eq!(w.entries.len(), 1);
        assert_eq!(w.entries[0].payload_len, 4096);
        assert_eq!(w.total_payload_bytes, 4096);
        assert_eq!(
            w.batch_wire_bytes,
            frame::codec::HEADER_SIZE + 4096 + frame::codec::CRC_SIZE
        );
    }
}
