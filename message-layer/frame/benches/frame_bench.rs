// Benchmarks are test-adjacent: a malformed fixture SHOULD panic loudly so a
// bad workload is caught immediately, rather than silently measuring the wrong
// path. The workspace's `expect_used` / `unwrap_used` denies target production
// code where panicking must be explicit and bounded.
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

//! Criterion benchmarks for the `frame` crate.
//!
//! ## What is measured
//!
//! All benchmarks use realistic, production-sized payloads (see the workload
//! section below). Workload construction happens once in `setup`; only the
//! codec hot path is measured.
//!
//! ## Anti-optimisation strategy
//!
//! Reusing a single immutable buffer across iterations measures *cache speed*
//! more than codec speed: the wire bytes sit permanently in L1/L2, every
//! branch is perfectly predicted, and pages stay warm in the TLB. Production
//! traffic arrives cold from the NIC. To model that, the aggregate benches
//! draw from a **frame pool** (≥ L3 size) and cycle through it, so each
//! iteration touches a different frame with different type / size / seq.
//!
//! Inputs and outputs are fenced with `criterion::black_box` so LLVM cannot
//! hoist reads, fold constants, or elide writes:
//!
//! * encode inputs (header, payload) and the written length are black-boxed;
//! * decode outputs are sunk across *several* fields (`stream_id`, `seq`,
//!   `msg_type`, `flags`, `payload` pointer) — not just `payload.len()`,
//!   which is a compile-time constant for a given frame;
//! * the `*_touch` benches go further and actually **read the payload bytes**
//!   (one byte per cache line) — `decode()` alone never loads the payload, so
//!   a no-touch bench measures an unrealistically cheap path. The `*_touch`
//!   benches charge the payload-fetch cost a real receiver always pays;
//! * a post-loop length assert guards against dead-store elimination on the
//!   encode buffer.
//!
//! ### Bench groups
//!
//! | Group | What it measures |
//! |---|---|
//! | `encode/per_type` | `encode_into` per `MessageType` (comparative) |
//! | `encode/batch_all` | Encode all 15 frames back-to-back |
//! | `encode/random_pool` | Encode frames drawn from a 128-frame pool (cache pressure) |
//! | `decode/per_type` | `decode` per frame type (header-parse only — upper bound) |
//! | `decode/per_type_touch` | `decode` **+ payload consumption** per type (realistic) |
//! | `decode/batch_all` | Walk the 15-frame batch with `decode_with_len` |
//! | `decode/random_pool` | Decode frames drawn from a 128-frame pool |
//! | `decode/cache_cold` | Decode from a pool sized > L3 (worst-case cache) |
//! | `decode/cache_cold_touch` | Same, **+ payload consumption** (worst-case realistic) |
//! | `decode/mixed_sizes` | Realistic size distribution (many small, few large) |
//! | `streaming/frame_decoder` | `FrameDecoder::decode_frame` draining a batch |
//! | `streaming/zero_copy_recv` | `spare_capacity_mut` + `commit` receive path |
//! | `streaming/frame_encoder` | `FrameEncoder::encode_buffer` per-type |
//! | `throughput/encode` · `throughput/decode` | GB/s over the full batch |
//! | `roundtrip` | encode then immediately decode |
//! | `frame_layer/*` | 40-byte `Frame` / `FrameRef` codec (incl. `decode_touch`) |

use std::sync::OnceLock;

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use frame::codec::{
    FrameDecoder, FrameEncoder, FrameHeader, HEADER_SIZE, decode, decode_with_len, encode_into,
};
use frame::header::{ALL_MESSAGE_TYPES, FrameFlags, MessageType, Seq, StreamId};

// ============================================================================
// Realistic workload generator (adapted from the FluxIO bench workload)
// ============================================================================

/// Deterministic xorshift64 fill — weakly compressible, distinct per type.
fn fill_pseudo_media(buf: &mut [u8], seed: u64) {
    let mut state = seed.wrapping_add(0x9E3779B9_7F4A7C15);
    for b in buf.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *b = (state & 0xFF) as u8;
    }
}

/// Printable ASCII fill — simulates LLM text / JSON.
fn fill_pseudo_text(buf: &mut [u8], seed: u64) {
    let mut state = seed.wrapping_add(0x6C62272E_07BB0142);
    for b in buf.iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *b = 0x20 + (state % 95) as u8;
    }
}

/// Builds a realistic payload for `mt` whose size mirrors real FluxIO traffic.
///
/// | Type | Payload |
/// |---|---|
/// | `TextToken` | 100 KiB pseudo-ASCII (LLM chunk) |
/// | `AudioFrame` | 80 KiB Opus (10 s @ 64 kbps) |
/// | `VideoFrame` | 320 KiB H.264 I-frame |
/// | `ImageFrame` | 256 KiB JPEG |
/// | `ToolCall` | 4 KiB JSON |
/// | `ToolResult` | 148 KiB JSON |
/// | `StateUpdate` | 4 KiB document delta |
/// | `Error` | 2 KiB stack trace |
/// | `Heartbeat` | 64 B |
/// | `StreamOpen` | 1 KiB |
/// | `StreamClose` | 64 B |
/// | `StreamReset` | 256 B |
/// | `Handshake` | 2 KiB |
/// | `Fragment` | 120 KiB NAL continuation |
/// | `Ack` | 64 B |
fn realistic_payload(mt: MessageType) -> Vec<u8> {
    match mt {
        MessageType::TextToken => {
            let mut v = vec![0u8; 100_000];
            fill_pseudo_text(&mut v, 0xA0);
            v
        }
        MessageType::AudioFrame => {
            let mut v = vec![0u8; 80_000];
            v[0] = 0xC0;
            fill_pseudo_media(&mut v[1..], 0xA1);
            v
        }
        MessageType::VideoFrame => {
            let mut v = vec![0u8; 320_000];
            v[..5].copy_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x65]);
            v[5] = 0x88;
            fill_pseudo_media(&mut v[6..], 0xB2);
            v
        }
        MessageType::ImageFrame => {
            let mut v = vec![0u8; 262_144];
            v[..4].copy_from_slice(&[0xFF, 0xD8, 0xFF, 0xE0]);
            v[262_142..].copy_from_slice(&[0xFF, 0xD9]);
            fill_pseudo_media(&mut v[4..262_142], 0xC3);
            v
        }
        MessageType::ToolCall => {
            let mut v = Vec::with_capacity(4096);
            let prefix =
                br#"{"id":"call_a3f0","name":"vector_search","arguments":{"query":"rust async","embedding":"#;
            v.extend_from_slice(prefix);
            let need = 4096usize.saturating_sub(prefix.len() + 3);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD0);
            v.extend_from_slice(&tmp);
            v.extend_from_slice(b"\"}}");
            v
        }
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
        MessageType::Heartbeat => br#"{"ts":1719900000000,"seq":1042,"ok":true}"#.to_vec(),
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
        MessageType::StreamClose => br#"{"code":0,"reason":"done","seq":9999}"#.to_vec(),
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
        MessageType::Handshake => {
            let mut v = Vec::with_capacity(2048);
            let prefix = br#"{"proto":"fluxio","version":"1.0.0","capabilities":["audio","video","tools"],"ext":"#;
            v.extend_from_slice(prefix);
            let need = 2048usize.saturating_sub(prefix.len() + 2);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD6);
            v.extend_from_slice(&tmp);
            v.push(b'}');
            v
        }
        MessageType::Fragment => {
            let mut v = vec![0u8; 122_880];
            fill_pseudo_media(&mut v, 0xE4);
            v
        }
        MessageType::Ack => br#"{"ack_seq":1042,"highest_seq":1042,"delay_ms":3}"#.to_vec(),

        // Pub/sub Phase 1 wire format — binary payloads (no JSON).
        MessageType::Subscribe => {
            let pattern = b"sensor.temperature.zone.*";
            let mut v = Vec::with_capacity(2 + pattern.len() + 1);
            v.extend_from_slice(&(pattern.len() as u16).to_be_bytes());
            v.extend_from_slice(pattern);
            v.push(1);
            v
        }
        MessageType::Unsubscribe => {
            let pattern = b"sensor.temperature.zone.*";
            let mut v = Vec::with_capacity(2 + pattern.len());
            v.extend_from_slice(&(pattern.len() as u16).to_be_bytes());
            v.extend_from_slice(pattern);
            v
        }
        MessageType::Publish => {
            let topic = b"sensor.temperature.zone1";
            let mut v = Vec::with_capacity(2 + topic.len() + 4096);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            let mut payload = vec![0u8; 4096];
            payload
                .iter_mut()
                .enumerate()
                .for_each(|(i, b)| *b = (i & 0xFF) as u8);
            v.extend_from_slice(&payload);
            v
        }
        MessageType::TopicCreate => {
            let topic = b"metrics.cpu.host.*";
            let mut v = Vec::with_capacity(2 + topic.len() + 12);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&8u32.to_be_bytes());
            v.extend_from_slice(&(3_600_000u64).to_be_bytes());
            v
        }
        MessageType::TopicDelete => {
            let topic = b"metrics.cpu.host.stale";
            let mut v = Vec::with_capacity(2 + topic.len());
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v
        }
        MessageType::Fetch => {
            let topic = b"metrics.cpu.host.0";
            let mut v = Vec::with_capacity(2 + topic.len() + 12);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&0u64.to_be_bytes());
            v.extend_from_slice(&32u32.to_be_bytes());
            v
        }
        MessageType::FetchReply => {
            let topic = b"metrics.cpu.host.0";
            let payload = b"replayed-entry-payload";
            let mut v = Vec::with_capacity(2 + topic.len() + 20 + payload.len());
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&0u64.to_be_bytes());
            v.extend_from_slice(&0u64.to_be_bytes());
            v.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            v.extend_from_slice(payload);
            v
        }
        MessageType::ConsumerGroupJoin => {
            let topic = b"metrics.cpu.host.0";
            let group = b"zone1-alerts";
            let consumer = b"consumer-1";
            let mut v = Vec::with_capacity(2 + topic.len() + 2 + group.len() + 2 + consumer.len() + 4);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&(group.len() as u16).to_be_bytes());
            v.extend_from_slice(group);
            v.extend_from_slice(&(consumer.len() as u16).to_be_bytes());
            v.extend_from_slice(consumer);
            v.extend_from_slice(&4u32.to_be_bytes());
            v
        }
        MessageType::ConsumerGroupLeave => {
            let topic = b"metrics.cpu.host.0";
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
        MessageType::ConsumerGroupHeartbeat => {
            let topic = b"metrics.cpu.host.0";
            let group = b"zone1-alerts";
            let consumer = b"consumer-1";
            let mut v = Vec::with_capacity(2 + topic.len() + 2 + group.len() + 2 + consumer.len() + 4);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&(group.len() as u16).to_be_bytes());
            v.extend_from_slice(group);
            v.extend_from_slice(&(consumer.len() as u16).to_be_bytes());
            v.extend_from_slice(consumer);
            v.extend_from_slice(&1u32.to_be_bytes());
            v
        }
        MessageType::NotifyOffset => {
            let topic = b"metrics.cpu.host.0";
            let mut v = Vec::with_capacity(2 + topic.len() + 8);
            v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            v.extend_from_slice(topic);
            v.extend_from_slice(&42u64.to_be_bytes());
            v
        }
    }
}

fn realistic_flags(mt: MessageType) -> FrameFlags {
    match mt {
        MessageType::Heartbeat => FrameFlags::URGENT,
        MessageType::Fragment => FrameFlags::FRAGMENT,
        MessageType::StreamClose => FrameFlags::FIN,
        MessageType::ToolCall | MessageType::Ack => FrameFlags::ACK_REQ,
        _ => FrameFlags::NONE,
    }
}

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
        MessageType::NotifyOffset => "NotifyOffset",
    }
}

/// Pre-built, immutable per-type wire frames + full batch.
struct Workload {
    /// All 15 frames encoded back-to-back.
    batch: Vec<u8>,
    /// Per-type encoded wire frame (header + payload).
    frames: Vec<(MessageType, Vec<u8>, Vec<u8>)>, // (type, payload, wire)
    /// Total wire bytes across all 15 frames.
    total_wire_bytes: usize,
}

fn build_workload() -> Workload {
    let mut frames = Vec::with_capacity(ALL_MESSAGE_TYPES.len());
    let mut batch_buf = BytesMut::with_capacity(2 * 1024 * 1024);

    for (i, &mt) in ALL_MESSAGE_TYPES.iter().enumerate() {
        let payload = realistic_payload(mt);
        let header = FrameHeader::new(StreamId::new(i as u64), mt)
            .with_seq(Seq::ZERO)
            .with_flags(realistic_flags(mt));

        let frame_start = batch_buf.len();
        encode_into(&mut batch_buf, header, &payload).unwrap_or_default();
        let wire = batch_buf[frame_start..].to_vec();
        frames.push((mt, payload, wire));
    }

    let total_wire_bytes = batch_buf.len();
    Workload {
        batch: batch_buf.to_vec(),
        frames,
        total_wire_bytes,
    }
}

// ============================================================================
// Frame pools — defeat L1/L2/L3 residency and branch-predictor training.
//
// Each pool is built once (via OnceLock) and shared across bench functions.
// The 128-frame pool (~9 MB) exceeds typical L2 (≤ 1 MB) and most L3
// (8–16 MB); the 1024-frame pool (~70 MB) reliably exceeds all on-die cache.
// A fixed-seed shuffle breaks the regular type cycle so the msg_type match
// branch is not predictable from iteration to iteration.
// ============================================================================

const POOL_SMALL: usize = 128; // ~9 MB  — exceeds L2, partial L3 pressure
const POOL_LARGE: usize = 1024; // ~70 MB — exceeds L3 (cache-cold model)
const POOL_SMALL_MASK: usize = POOL_SMALL - 1;
const POOL_LARGE_MASK: usize = POOL_LARGE - 1;

const _: () = assert!(POOL_SMALL.is_power_of_two());
const _: () = assert!(POOL_LARGE.is_power_of_two());

/// Deterministic in-place Fisher–Yates with a fixed xorshift seed.
fn shuffle<T>(items: &mut [T]) {
    let mut state = 0xDEAD_BEEF_CAFE_BABEu64;
    let mut i = items.len();
    while i > 1 {
        i -= 1;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        items.swap(i, j);
    }
}

/// Encodes `n` frames cycling through all message types with varying
/// `stream_id` / `seq`, then shuffles them to defeat branch prediction.
fn build_pool(n: usize) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(n);
    for i in 0..n {
        let mt = ALL_MESSAGE_TYPES[i % ALL_MESSAGE_TYPES.len()];
        let payload = realistic_payload(mt);
        let header = FrameHeader::new(StreamId::new(i as u64), mt)
            .with_seq(Seq::new((i as u64).wrapping_mul(7)))
            .with_flags(realistic_flags(mt));
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
        encode_into(&mut buf, header, &payload).unwrap_or_default();
        out.push(buf.to_vec());
    }
    shuffle(&mut out);
    out
}

fn decode_pool_small() -> &'static Vec<Vec<u8>> {
    static POOL: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    POOL.get_or_init(|| build_pool(POOL_SMALL))
}

fn decode_pool_large() -> &'static Vec<Vec<u8>> {
    static POOL: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    POOL.get_or_init(|| build_pool(POOL_LARGE))
}

/// Encode-side pool: (header, payload) pairs, shuffled.
fn encode_pool_small() -> &'static Vec<(FrameHeader, Vec<u8>)> {
    static POOL: OnceLock<Vec<(FrameHeader, Vec<u8>)>> = OnceLock::new();
    POOL.get_or_init(|| {
        let mut out: Vec<(FrameHeader, Vec<u8>)> = Vec::with_capacity(POOL_SMALL);
        for i in 0..POOL_SMALL {
            let mt = ALL_MESSAGE_TYPES[i % ALL_MESSAGE_TYPES.len()];
            let payload = realistic_payload(mt);
            let header = FrameHeader::new(StreamId::new(i as u64), mt)
                .with_seq(Seq::new((i as u64).wrapping_mul(7)))
                .with_flags(realistic_flags(mt));
            out.push((header, payload));
        }
        shuffle(&mut out);
        out
    })
}

/// Sink every observable field of a decoded `FrameRef` so the header parse
/// cannot be elided and the payload pointer must be materialised. NOTE: this
/// does **not** read the payload bytes — see [`touch_payload_checksum`] for
/// the realistic variant.
#[inline(always)]
fn sink_frame_ref(f: frame::codec::FrameRef<'_>) {
    criterion::black_box(f.stream_id);
    criterion::black_box(f.seq);
    criterion::black_box(f.msg_type);
    criterion::black_box(f.flags);
    criterion::black_box(f.payload.as_ptr());
}

/// Forces the CPU to actually fetch every 64-byte cache line of `payload`,
/// modelling a realistic consumer that reads / parses / hashes the payload
/// after decoding. Returns a checksum to sink.
///
/// `decode()` alone only parses the header — the payload pointer is derived
/// but the bytes are never loaded. A benchmark that stops there measures an
/// unrealistically cheap path: in production every frame's payload is
/// consumed. This helper charges that cost.
///
/// A stride of 64 (one byte per cache line) isolates the *memory-fetch* cost
/// — the dominant hidden expense — without dominating the measurement with
/// checksum ALU work. A full byte-wise checksum (CRC32 / xxhash) would
/// measure the hash, not the decode; use that as a separate workload if
/// needed.
#[inline(always)]
fn touch_payload_checksum(payload: &[u8]) -> u64 {
    let mut sum = 0u64;
    for &b in payload.iter().step_by(64) {
        sum += b as u64;
    }
    sum
}

/// Forces an encode to have *actually written* its output bytes — not just
/// advanced the buffer's length.
///
/// `criterion::black_box(buf.len())` is **insufficient** as an encode sink:
/// `buf.len()` equals `HEADER_SIZE + payload.len()`, a compile-time constant,
/// so LLVM can elide the entire payload memcpy and report the length from a
/// register. The result is a "1.6 ns encode" that measured nothing. Reading
/// bytes out of the output buffer makes the copy observably necessary, so the
/// measured time reflects the real memcpy-bound encode cost.
#[inline(always)]
fn sink_encoded(buf: &[u8]) {
    // Touch the whole output (header + payload) one cache line at a time.
    // Overhead is ~1 read/cacheline — negligible next to the copy it forces.
    let sum = touch_payload_checksum(buf);
    criterion::black_box(sum);
}

// ============================================================================
// encode/per_type — encode_into for each MessageType (comparative)
// ============================================================================

fn bench_encode_per_type(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("encode/per_type");

    for (mt, payload, _wire) in &workload.frames {
        let name = type_name(*mt);
        let payload_len = payload.len();
        group.throughput(Throughput::Bytes((HEADER_SIZE + payload_len) as u64));

        let header = FrameHeader::new(StreamId::new(0), *mt)
            .with_seq(Seq::ZERO)
            .with_flags(realistic_flags(*mt));

        group.bench_with_input(
            BenchmarkId::new(name, payload_len),
            payload,
            |b, payload| {
                // Pre-allocate once; reuse across iterations — mirrors a real
                // connection encoder that calls `clear()` between frames.
                let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
                b.iter(|| {
                    buf.clear();
                    encode_into(
                        &mut buf,
                        criterion::black_box(header),
                        criterion::black_box(payload.as_slice()),
                    )
                    .unwrap_or_default();
                    // Read the output bytes — `black_box(buf.len())` alone is
                    // a compile-time constant and lets LLVM elide the copy.
                    sink_encoded(&buf);
                });
                // Post-loop sanity: the encode path did real work.
                buf.clear();
                encode_into(&mut buf, header, payload).unwrap_or_default();
                assert_eq!(buf.len(), HEADER_SIZE + payload.len());
            },
        );
    }
    group.finish();
}

// ============================================================================
// encode/batch_all — encode all 15 frames back-to-back
// ============================================================================

fn bench_encode_batch_all(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("encode/batch_all");
    group.throughput(Throughput::Bytes(workload.total_wire_bytes as u64));

    let inputs: Vec<(FrameHeader, &[u8])> = workload
        .frames
        .iter()
        .enumerate()
        .map(|(i, (mt, payload, _))| {
            let h = FrameHeader::new(StreamId::new(i as u64), *mt)
                .with_seq(Seq::ZERO)
                .with_flags(realistic_flags(*mt));
            (h, payload.as_slice())
        })
        .collect();

    let buf_cap = workload.total_wire_bytes + 64;
    group.bench_function("all_15_types", |b| {
        let mut buf = BytesMut::with_capacity(buf_cap);
        b.iter(|| {
            buf.clear();
            for &(header, payload) in &inputs {
                encode_into(
                    &mut buf,
                    criterion::black_box(header),
                    criterion::black_box(payload),
                )
                .unwrap_or_default();
            }
            sink_encoded(&buf);
        });
        assert_eq!(buf.len(), workload.total_wire_bytes);
    });
    group.finish();
}

// ============================================================================
// encode/random_pool — encode frames drawn from a 128-frame pool
//
// Unlike batch_all (fixed 15 frames, hot in cache), this cycles through a
// shuffled 128-frame pool so payload source memory and msg_type branches
// vary iteration to iteration.
// ============================================================================

fn bench_encode_random_pool(c: &mut Criterion) {
    let pool = encode_pool_small();
    // Throughput is bytes processed *per iteration* (one frame), so report
    // the average frame size — not the whole-pool sum.
    let avg_frame: u64 = (pool
        .iter()
        .map(|(_, p)| HEADER_SIZE + p.len())
        .sum::<usize>() as u64)
        / pool.len() as u64;
    let max_frame = pool
        .iter()
        .map(|(_, p)| HEADER_SIZE + p.len())
        .max()
        .unwrap_or(0);

    let mut group = c.benchmark_group("encode/random_pool");
    group.throughput(Throughput::Bytes(avg_frame));

    group.bench_function("cycle_128", |b| {
        let mut buf = BytesMut::with_capacity(max_frame);
        let mut i = 0usize;
        b.iter(|| {
            let (header, payload) = &pool[i & POOL_SMALL_MASK];
            i = i.wrapping_add(1);
            buf.clear();
            encode_into(
                &mut buf,
                criterion::black_box(*header),
                criterion::black_box(payload.as_slice()),
            )
            .unwrap_or_default();
            sink_encoded(&buf);
        });
    });
    group.finish();
}

// ============================================================================
// decode/per_type — one-shot zero-copy decode for each MessageType
// (comparative across types; per-type cache warmth affects all equally,
// so the relative comparison stays fair.)
// ============================================================================

fn bench_decode_per_type(c: &mut Criterion) {
    let workload = build_workload();
    // No `Throughput`: this bench only parses the 22-byte header — the payload
    // is never loaded — so frame-bytes / time would be fictional (tens of
    // thousands of GiB/s). The honest metric is time-per-header in ns. For
    // realistic byte throughput see `decode/per_type_touch`.
    let mut group = c.benchmark_group("decode/per_type");

    for (mt, _payload, wire) in &workload.frames {
        let name = type_name(*mt);
        let wire_len = wire.len();

        group.bench_with_input(BenchmarkId::new(name, wire_len), wire, |b, wire| {
            b.iter(|| {
                let frame_ref = decode(criterion::black_box(wire.as_slice())).expect("valid frame");
                sink_frame_ref(frame_ref);
            });
        });
    }
    group.finish();
}

// ============================================================================
// decode/batch_all — walk the full 15-frame batch with decode_with_len
// ============================================================================

fn bench_decode_batch_all(c: &mut Criterion) {
    let workload = build_workload();
    // Header-parse only → no payload bytes processed → no honest GiB/s. The
    // reported metric is total wall time to walk all 15 headers.
    let mut group = c.benchmark_group("decode/batch_all");

    let batch = workload.batch.clone();
    group.bench_function("all_15_types", |b| {
        b.iter(|| {
            let mut src = criterion::black_box(batch.as_slice());
            while !src.is_empty() {
                let (frame, consumed) = decode_with_len(src).expect("valid frame");
                // Sink a varying field so the parse is not folded.
                criterion::black_box(frame.seq);
                criterion::black_box(frame.payload.as_ptr());
                src = &src[consumed..];
            }
        });
    });
    group.finish();
}

// ============================================================================
// decode/random_pool — decode frames drawn from a 128-frame pool
// ============================================================================

fn bench_decode_random_pool(c: &mut Criterion) {
    let pool = decode_pool_small();
    // Header-parse only → no byte throughput to report. Compare against
    // `decode/cache_cold_touch` for the realistic, payload-touching number.
    let mut group = c.benchmark_group("decode/random_pool");

    group.bench_function("cycle_128", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let wire = &pool[i & POOL_SMALL_MASK];
            i = i.wrapping_add(1);
            let frame = decode(criterion::black_box(wire.as_slice())).expect("valid frame");
            sink_frame_ref(frame);
        });
    });
    group.finish();
}

// ============================================================================
// decode/cache_cold — pool sized to exceed L3 (~70 MB).
//
// Worst-case cache model: each iteration's frame is unlikely to be resident
// in any on-die cache, mirroring a freshly-NIC-delivered packet.
// ============================================================================

fn bench_decode_cache_cold(c: &mut Criterion) {
    let pool = decode_pool_large();
    // Header-parse only → time-per-decode (ns) is the metric, not GiB/s.
    let mut group = c.benchmark_group("decode/cache_cold");

    group.bench_function("cycle_1024", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let wire = &pool[i & POOL_LARGE_MASK];
            i = i.wrapping_add(1);
            let frame = decode(criterion::black_box(wire.as_slice())).expect("valid frame");
            sink_frame_ref(frame);
        });
    });
    group.finish();
}

// ============================================================================
// decode/per_type_touch — decode + consume payload, per MessageType
//
// The realistic counterpart to `decode/per_type`: after parsing the header we
// stride through the payload so every cache line is fetched. The difference
// vs the no-touch bench exposes how much of "decode" cost is actually payload
// memory fetch — the part a production receiver always pays.
// ============================================================================

fn bench_decode_per_type_touch(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("decode/per_type_touch");

    for (mt, _payload, wire) in &workload.frames {
        let name = type_name(*mt);
        let wire_len = wire.len();
        group.throughput(Throughput::Bytes(wire_len as u64));

        group.bench_with_input(BenchmarkId::new(name, wire_len), wire, |b, wire| {
            b.iter(|| {
                let frame_ref = decode(criterion::black_box(wire.as_slice())).expect("valid frame");
                sink_frame_ref(frame_ref);
                let sum = touch_payload_checksum(frame_ref.payload);
                criterion::black_box(sum);
            });
        });
    }
    group.finish();
}

// ============================================================================
// decode/cache_cold_touch — pooled (> L3) decode + payload consumption.
//
// Worst-case realistic decode: frames drawn from a ~70 MB pool so they are
// not resident in on-die cache, AND the payload bytes are actually read.
// This is the number a production NIC→decode pipeline approaches.
// ============================================================================

fn bench_decode_cache_cold_touch(c: &mut Criterion) {
    let pool = decode_pool_large();
    let avg_frame: u64 = (pool.iter().map(|w| w.len()).sum::<usize>() as u64) / pool.len() as u64;

    let mut group = c.benchmark_group("decode/cache_cold_touch");
    group.throughput(Throughput::Bytes(avg_frame));

    group.bench_function("cycle_1024", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let wire = &pool[i & POOL_LARGE_MASK];
            i = i.wrapping_add(1);
            let frame = decode(criterion::black_box(wire.as_slice())).expect("valid frame");
            let sum = touch_payload_checksum(frame.payload);
            criterion::black_box(sum);
        });
    });
    group.finish();
}

// ============================================================================
// decode/mixed_sizes — realistic size distribution
//
// Models the long-tail of real traffic: many small control frames, a handful
// of medium data frames, a few large media frames. Draws from a weighted
// size table to measure the codec over a production-like mix rather than a
// single fixed payload.
// ============================================================================

fn bench_decode_mixed_sizes(c: &mut Criterion) {
    // (size_bytes, frequency) — frequencies chosen so the 256-entry pool has
    // the right shape: ~50% tiny, ~30% small, ~15% medium, ~5% large.
    const SIZES: &[(usize, usize)] = &[
        (64, 64),      // heartbeats / acks
        (256, 32),     // control
        (1_024, 48),   // small json
        (4_096, 40),   // tool calls / state
        (16_384, 32),  // medium
        (65_536, 24),  // audio-ish
        (262_144, 16), // large media
    ];
    const N: usize = 256;
    const MASK: usize = N - 1;
    const _: () = assert!(N.is_power_of_two());

    static POOL: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    let pool = POOL.get_or_init(|| {
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(N);
        let mut idx = 0usize;
        for &(size, freq) in SIZES {
            for _ in 0..freq {
                let mt = ALL_MESSAGE_TYPES[idx % ALL_MESSAGE_TYPES.len()];
                let mut payload = vec![0u8; size];
                fill_pseudo_media(&mut payload, idx as u64);
                let header = FrameHeader::new(StreamId::new(idx as u64), mt)
                    .with_seq(Seq::new(idx as u64))
                    .with_flags(realistic_flags(mt));
                let mut buf = BytesMut::with_capacity(HEADER_SIZE + size);
                encode_into(&mut buf, header, &payload).unwrap_or_default();
                out.push(buf.to_vec());
                idx += 1;
            }
        }
        // Pad / truncate to exactly N (the table above sums to 256).
        debug_assert_eq!(out.len(), N);
        shuffle(&mut out);
        out
    });

    // Header-parse only → no byte throughput reported.
    let mut group = c.benchmark_group("decode/mixed_sizes");

    group.bench_function("cycle_256", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let wire = &pool[i & MASK];
            i = i.wrapping_add(1);
            let frame = decode(criterion::black_box(wire.as_slice())).expect("valid frame");
            sink_frame_ref(frame);
        });
    });
    group.finish();
}

// ============================================================================
// streaming/frame_decoder — FrameDecoder::decode_frame draining a full batch
// ============================================================================

fn bench_streaming_frame_decoder(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("streaming/frame_decoder");
    group.throughput(Throughput::Bytes(workload.total_wire_bytes as u64));

    let batch = workload.batch.clone();
    group.bench_function("drain_batch", |b| {
        b.iter(|| {
            let mut dec = FrameDecoder::new();
            dec.push(criterion::black_box(&batch));
            while let Some(frame) = dec.decode_frame().expect("valid frame") {
                criterion::black_box(frame.seq);
                criterion::black_box(frame.payload.as_ptr());
            }
        });
    });
    group.finish();
}

// ============================================================================
// streaming/frame_decoder_reuse — reuse FrameDecoder across iterations
//
// Measures the hot decode loop without re-creating the decoder struct or
// re-pushing data that was already buffered, more accurately modelling a
// long-lived connection receive loop.
// ============================================================================

fn bench_streaming_frame_decoder_reuse(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("streaming/frame_decoder_reuse");
    group.throughput(Throughput::Bytes(workload.total_wire_bytes as u64));

    let batch = workload.batch.clone();
    // Create the decoder once outside the iter closure; push + decode + clear.
    let mut dec = FrameDecoder::new();
    group.bench_function("reuse_decoder", |b| {
        b.iter(|| {
            dec.push(criterion::black_box(&batch));
            while let Some(frame) = dec.decode_frame().expect("valid frame") {
                criterion::black_box(frame.seq);
                criterion::black_box(frame.payload.as_ptr());
            }
            // `clear` is idempotent here — no bytes remain — but it models
            // an explicit reset point at stream end.
            dec.clear();
        });
    });
    group.finish();
}

// ============================================================================
// streaming/frame_encoder — FrameEncoder::encode_buffer per-type
// ============================================================================

fn bench_streaming_frame_encoder(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("streaming/frame_encoder");

    for (mt, payload, _wire) in &workload.frames {
        let name = type_name(*mt);
        let wire_len = HEADER_SIZE + payload.len();
        group.throughput(Throughput::Bytes(wire_len as u64));

        let header = FrameHeader::new(StreamId::new(0), *mt)
            .with_seq(Seq::ZERO)
            .with_flags(realistic_flags(*mt));

        group.bench_with_input(BenchmarkId::new(name, wire_len), payload, |b, payload| {
            let enc = FrameEncoder::new();
            b.iter(|| {
                let buf = enc
                    .encode_buffer(
                        criterion::black_box(header),
                        criterion::black_box(payload.as_slice()),
                    )
                    .expect("encode ok");
                sink_encoded(&buf);
            });
        });
    }
    group.finish();
}

// ============================================================================
// throughput/encode — GB/s throughput for encode_into
// ============================================================================

fn bench_throughput_encode(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("throughput/encode");
    group.throughput(Throughput::Bytes(workload.total_wire_bytes as u64));

    let inputs: Vec<(FrameHeader, Vec<u8>)> = workload
        .frames
        .iter()
        .enumerate()
        .map(|(i, (mt, payload, _))| {
            let h = FrameHeader::new(StreamId::new(i as u64), *mt)
                .with_seq(Seq::ZERO)
                .with_flags(realistic_flags(*mt));
            (h, payload.clone())
        })
        .collect();

    let buf_cap = workload.total_wire_bytes + 64;
    group.bench_function("bytes_per_sec", |b| {
        let mut buf = BytesMut::with_capacity(buf_cap);
        b.iter(|| {
            buf.clear();
            for (header, payload) in &inputs {
                encode_into(
                    &mut buf,
                    criterion::black_box(*header),
                    criterion::black_box(payload.as_slice()),
                )
                .unwrap_or_default();
            }
            sink_encoded(&buf);
        });
    });
    group.finish();
}

// ============================================================================
// throughput/decode — GB/s throughput for decode_with_len + payload consume.
//
// This is the realistic whole-batch decode throughput. The payload bytes are
// actually read (one cache line at a time) so the reported GiB/s reflects
// what a production receive loop achieves, not a header-parse-only upper
// bound. Without the payload touch the throughput would be fictional: the
// decode only reads the header, so dividing full-frame bytes by header-parse
// time yields absurd numbers (tens of thousands of GiB/s).
// ============================================================================

fn bench_throughput_decode(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("throughput/decode");
    group.throughput(Throughput::Bytes(workload.total_wire_bytes as u64));

    let batch = workload.batch.clone();
    group.bench_function("bytes_per_sec", |b| {
        b.iter(|| {
            let mut src = criterion::black_box(batch.as_slice());
            while !src.is_empty() {
                let (frame, consumed) = decode_with_len(src).expect("valid frame");
                let sum = touch_payload_checksum(frame.payload);
                criterion::black_box(sum);
                src = &src[consumed..];
            }
        });
    });
    group.finish();
}

// ============================================================================
// roundtrip — encode then immediately decode, exercising both paths together
// ============================================================================

fn bench_roundtrip(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("roundtrip");
    group.throughput(Throughput::Bytes(workload.total_wire_bytes as u64));

    let inputs: Vec<(FrameHeader, Vec<u8>)> = workload
        .frames
        .iter()
        .enumerate()
        .map(|(i, (mt, payload, _))| {
            let h = FrameHeader::new(StreamId::new(i as u64), *mt)
                .with_seq(Seq::ZERO)
                .with_flags(realistic_flags(*mt));
            (h, payload.clone())
        })
        .collect();

    let buf_cap = workload.total_wire_bytes + 64;
    group.bench_function("encode_then_decode_all", |b| {
        let mut buf = BytesMut::with_capacity(buf_cap);
        b.iter(|| {
            buf.clear();
            for (header, payload) in &inputs {
                encode_into(
                    &mut buf,
                    criterion::black_box(*header),
                    criterion::black_box(payload.as_slice()),
                )
                .unwrap_or_default();
            }
            // Decode pass — touch each frame's payload so both the encode
            // copy and the decode fetch are observably necessary.
            let mut src: &[u8] = criterion::black_box(&buf);
            while !src.is_empty() {
                let (frame, consumed) = decode_with_len(src).expect("valid frame");
                let sum = touch_payload_checksum(frame.payload);
                criterion::black_box(sum);
                src = &src[consumed..];
            }
        });
    });
    group.finish();
}

// ============================================================================
// frame_decoder/spare_capacity_mut — zero-copy receive path
//
// Simulates `stream.recv()` writing directly into the decoder's internal
// buffer via `spare_capacity_mut` + `commit`, bypassing the `push()` memcpy.
// ============================================================================

fn bench_streaming_zero_copy_recv(c: &mut Criterion) {
    let workload = build_workload();
    let mut group = c.benchmark_group("streaming/zero_copy_recv");
    group.throughput(Throughput::Bytes(workload.total_wire_bytes as u64));

    let batch = workload.batch.clone();
    let n = batch.len();
    let mut dec = FrameDecoder::with_capacity(n + 64);

    group.bench_function("spare_capacity_commit", |b| {
        b.iter(|| {
            // Write directly into the decoder's spare capacity — no memcpy.
            let spare = dec.spare_capacity_mut(n);
            spare[..n].copy_from_slice(criterion::black_box(&batch));
            // SAFETY: we initialised exactly `n` bytes above.
            unsafe { dec.commit(n) };

            while let Some(frame) = dec.decode_frame().expect("valid frame") {
                criterion::black_box(frame.seq);
                criterion::black_box(frame.payload.as_ptr());
            }
            dec.clear();
        });
    });
    group.finish();
}

// ============================================================================
// Frame (40-byte header) encode/decode — full frame.rs layer
// ============================================================================

fn bench_frame_layer_encode(c: &mut Criterion) {
    use frame::frame::Frame;
    use frame::header::{MessageType as MT, SessionId, StreamId as SI};

    let mut group = c.benchmark_group("frame_layer/encode_into");

    // Use a mid-size payload representative of the mixed workload average.
    let payload_bytes: Vec<u8> = {
        let mut v = vec![0u8; 4096];
        fill_pseudo_text(&mut v, 0xBB);
        v
    };

    group.throughput(Throughput::Bytes(
        (frame::header::HEADER_SIZE + payload_bytes.len()) as u64,
    ));

    let owned_frame = Frame::new(
        SessionId::new(1),
        SI::new(2),
        MT::TextToken,
        payload_bytes.clone(),
    );

    group.bench_function("Frame_encode_into", |b| {
        let mut out = Vec::with_capacity(frame::header::HEADER_SIZE + payload_bytes.len());
        b.iter(|| {
            out.clear();
            owned_frame.encode(&mut out).unwrap_or_default();
            sink_encoded(&out);
        });
        assert_eq!(out.len(), frame::header::HEADER_SIZE + payload_bytes.len());
    });

    #[cfg(feature = "buffer-pool")]
    group.bench_function("Frame_encode_pooled", |b| {
        let pool = buffer_pool::BufferPool::default();
        b.iter(|| {
            let buf = owned_frame.encode_pooled(&pool).unwrap();
            sink_encoded(&buf);
        });
    });

    group.bench_function("FrameRef_encode_into", |b| {
        let frame_ref = owned_frame.as_ref();
        let mut out = Vec::with_capacity(frame::header::HEADER_SIZE + payload_bytes.len());
        b.iter(|| {
            out.clear();
            frame_ref.encode(&mut out).unwrap_or_default();
            sink_encoded(&out);
        });
        assert_eq!(out.len(), frame::header::HEADER_SIZE + payload_bytes.len());
    });

    #[cfg(feature = "buffer-pool")]
    group.bench_function("FrameRef_encode_pooled", |b| {
        let frame_ref = owned_frame.as_ref();
        let pool = buffer_pool::BufferPool::default();
        b.iter(|| {
            let buf = frame_ref.encode_pooled(&pool).unwrap();
            sink_encoded(&buf);
        });
    });

    group.finish();
}

// Shared pool of 40-byte frames with realistic, varied payload sizes (64 B –
// 320 KiB), encoded via the `frame::frame::Frame` API and shuffled. Used by
// both `frame_layer/decode` (header-parse only) and `frame_layer/decode_touch`
// (header parse + payload fetch) so the two can be compared apples-to-apples.
const FRAME_POOL: usize = 128;
const FRAME_POOL_MASK: usize = FRAME_POOL - 1;
const _: () = assert!(FRAME_POOL.is_power_of_two());

fn frame_layer_pool() -> &'static Vec<Vec<u8>> {
    use frame::frame::Frame;
    use frame::header::{SessionId, StreamId as SI};
    static POOL: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    POOL.get_or_init(|| {
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(FRAME_POOL);
        for i in 0..FRAME_POOL {
            let mt = ALL_MESSAGE_TYPES[i % ALL_MESSAGE_TYPES.len()];
            // Realistic per-type sizes — the touch benchmark needs varied
            // payload lengths for the cache-line fetch cost to show through.
            let payload = realistic_payload(mt);
            let wire_len = frame::header::HEADER_SIZE + payload.len();
            let frame = Frame::new(SessionId::new(i as u64), SI::new(i as u64), mt, payload)
                .with_seq(Seq::new(i as u64));
            let mut wire = Vec::with_capacity(wire_len);
            frame.encode(&mut wire).expect("encode ok");
            out.push(wire);
        }
        shuffle(&mut out);
        out
    })
}

/// Sink every header field of a 40-byte `FrameRef`.
#[inline(always)]
fn sink_frame_header(h: &frame::header::FrameHeader) {
    criterion::black_box(h.version);
    criterion::black_box(h.msg_type);
    criterion::black_box(h.flags);
    criterion::black_box(h.priority);
    criterion::black_box(h.session_id);
    criterion::black_box(h.stream_id);
    criterion::black_box(h.seq);
    criterion::black_box(h.timestamp);
}

fn bench_frame_layer_decode(c: &mut Criterion) {
    let pool = frame_layer_pool();

    // Header-parse only — no byte throughput reported (the payload is never
    // loaded, so GiB/s would be fictional). The metric is time-per-header.
    // Compare against `frame_layer/decode_touch` for realistic throughput.
    let mut group = c.benchmark_group("frame_layer/decode");

    group.bench_function("header_only", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let wire = &pool[i & FRAME_POOL_MASK];
            i = i.wrapping_add(1);
            let frame_ref = frame::frame::decode(criterion::black_box(wire)).expect("valid frame");
            sink_frame_header(&frame_ref.header);
            criterion::black_box(frame_ref.payload.as_ptr());
        });
    });

    group.finish();
}

// ============================================================================
// frame_layer/decode_touch — decode + consume the payload bytes.
//
// The counterpart to `frame_layer/decode`: same pool, but after decoding we
// stride through the payload forcing every cache line to be fetched. The
// delta between this and `header_only` is the real cost of "consuming" a
// frame — the number a production receive loop actually pays.
// ============================================================================

fn bench_frame_layer_decode_touch(c: &mut Criterion) {
    let pool = frame_layer_pool();
    let avg_frame: u64 = (pool.iter().map(|w| w.len()).sum::<usize>() as u64) / pool.len() as u64;

    let mut group = c.benchmark_group("frame_layer/decode_touch");
    group.throughput(Throughput::Bytes(avg_frame));

    group.bench_function("touch_payload", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let wire = &pool[i & FRAME_POOL_MASK];
            i = i.wrapping_add(1);
            let frame_ref = frame::frame::decode(criterion::black_box(wire)).expect("valid frame");
            sink_frame_header(&frame_ref.header);
            let sum = touch_payload_checksum(frame_ref.payload);
            criterion::black_box(sum);
        });
    });

    group.finish();
}

// ============================================================================
// Criterion entry points
// ============================================================================
//
// Multi-threaded encode/decode and end-to-end QUIC recv → decode → dispatch
// benches are deliberately out of scope here: they measure allocator /
// scheduler / transport contention rather than the codec, and require a QUIC
// stack that does not belong in the `frame` crate. Measure them at the
// integration layer instead.
// ============================================================================

criterion_group!(
    benches,
    bench_encode_per_type,
    bench_encode_batch_all,
    bench_encode_random_pool,
    bench_decode_per_type,
    bench_decode_per_type_touch,
    bench_decode_batch_all,
    bench_decode_random_pool,
    bench_decode_cache_cold,
    bench_decode_cache_cold_touch,
    bench_decode_mixed_sizes,
    bench_streaming_frame_decoder,
    bench_streaming_frame_decoder_reuse,
    bench_streaming_frame_encoder,
    bench_throughput_encode,
    bench_throughput_decode,
    bench_roundtrip,
    bench_streaming_zero_copy_recv,
    bench_frame_layer_encode,
    bench_frame_layer_decode,
    bench_frame_layer_decode_touch,
);
criterion_main!(benches);
