//! Realistic per-`MessageType` workload generator.
//!
//! Builds one wire batch containing all 15 frame types — each with a payload
//! whose size and shape match what a real Vireon client would send. The batch
//! is encoded once at startup and reused on every iteration so the measurement
//! exercises the transport + server decode path, not the encoder.
//!
//! ## Target batch size: ~1 MiB

use anyhow::Result;
use frame::codec::{FrameHeader, HEADER_SIZE, encode_into_slice};
use frame::header::{ALL_MESSAGE_TYPES, FrameFlags, MessageType, Seq, StreamId};

/// Per-type metadata, surfaced in the summary table so the operator can verify
/// the realistic workload at a glance.
#[derive(Clone, Copy)]
pub struct WorkloadEntry {
    /// The message type this entry exercises.
    pub msg_type: MessageType,
    /// Human-readable label for the summary table.
    pub name: &'static str,
    /// Payload byte count (excludes the 22-byte header).
    pub payload_len: usize,
}

/// How many consecutive copies of a frame we pre-encode into one
/// [`Workload::thick_frames`] entry.
///
/// Sending 32 copies in a single `stream_send` call lets quiche fill several
/// full-size QUIC packets from one call, reducing per-frame quiche overhead and
/// lowering the number of `conn.send()` calls by roughly this factor.
pub const COALESCE: usize = 32;

/// The complete pre-encoded workload.
pub struct Workload {
    /// All 15 frames encoded back-to-back (used for summary byte counts).
    pub batch: Vec<u8>,
    /// Each frame encoded individually; index `i` is sent on QUIC stream `i*4`.
    pub frames: Vec<Vec<u8>>,
    /// Each entry is [`COALESCE`] consecutive copies of `frames[i]`.
    pub thick_frames: Vec<Vec<u8>>,
    /// [`COALESCE`] copies of [`Self::batch`] concatenated — for `measure_rr`.
    pub thick_batch: Vec<u8>,
    /// Per-type metadata, in the same order the frames were encoded.
    pub entries: Box<[WorkloadEntry]>,
    /// Sum of every payload length (no headers) — for the summary.
    pub total_payload_bytes: usize,
}

/// Builds the full 15-frame workload.
///
/// Each `MessageType` gets its own logical `StreamId` (0..14), matching the
/// "1 user = 15 streams" model: N users produce 15×N logical streams.
pub fn build() -> Result<Workload> {
    let mut batch: Vec<u8> = Vec::with_capacity(32 * 1024);
    let mut frames: Vec<Vec<u8>> = Vec::with_capacity(ALL_MESSAGE_TYPES.len());
    let mut entries: Vec<WorkloadEntry> = Vec::with_capacity(ALL_MESSAGE_TYPES.len());
    let mut total_payload_bytes = 0usize;

    for (i, &mt) in ALL_MESSAGE_TYPES.iter().enumerate() {
        let payload = realistic_payload(mt);
        let header = FrameHeader::new(StreamId::new(i as u64), mt)
            .with_seq(Seq::ZERO)
            .with_flags(realistic_flags(mt));
        let payload_len = payload.len();

        // Encode into a standalone buffer, then append to batch.
        let frame_buf = encode_frame(header, &payload);
        total_payload_bytes += payload_len;
        batch.extend_from_slice(&frame_buf);
        frames.push(frame_buf);

        entries.push(WorkloadEntry {
            msg_type: mt,
            name: type_name(mt),
            payload_len,
        });
    }

    let thick_frames: Vec<Vec<u8>> = frames.iter().map(|f| f.repeat(COALESCE)).collect();
    let thick_batch: Vec<u8> = batch.repeat(COALESCE);

    Ok(Workload {
        batch,
        frames,
        thick_frames,
        thick_batch,
        entries: entries.into_boxed_slice(),
        total_payload_bytes,
    })
}

/// Encodes a single frame into a fresh `Vec<u8>`.
fn encode_frame(header: FrameHeader, payload: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; HEADER_SIZE + payload.len()];
    // encode_into_slice is infallible when the buffer is sized correctly.
    let _ = encode_into_slice(&mut buf, header, payload);
    buf
}

// ---- per-type payloads -----------------------------------------------------

/// Returns a realistic payload for `mt`.
///
/// Target: 15 frames total ≈ 1 MiB. Heavy media frames carry the bulk;
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

        // 2 KiB handshake with full capabilities list.
        MessageType::Handshake => {
            let mut v = Vec::with_capacity(2048);
            let prefix = br#"{"proto":"vireon","version":"1.0.0","capabilities":["audio","video","tools"],"ext":"#;
            v.extend_from_slice(prefix);
            let need = 2048usize.saturating_sub(prefix.len() + 2);
            let mut tmp = vec![0u8; need];
            fill_pseudo_text(&mut tmp, 0xD6);
            v.extend_from_slice(&tmp);
            v.push(b'}');
            v
        }

        // 120 KiB video NAL continuation (CABAC residuals).
        MessageType::Fragment => {
            let mut v = vec![0u8; 122_880];
            fill_pseudo_media(&mut v, 0xE4);
            v
        }

        // 64 bytes — ACK is a control frame.
        MessageType::Ack => br#"{"ack_seq":1042,"highest_seq":1042,"delay_ms":3}"#.to_vec(),
    }
}

/// Per-type flags matching how a real client would tag each frame.
fn realistic_flags(mt: MessageType) -> FrameFlags {
    match mt {
        MessageType::Heartbeat => FrameFlags::URGENT,
        MessageType::Fragment => FrameFlags::FRAGMENT,
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
    fn batch_has_fifteen_frames() {
        let w = build().unwrap();
        assert_eq!(w.entries.len(), 15);
        let expected_wire = 15 * HEADER_SIZE + w.total_payload_bytes;
        assert_eq!(w.batch.len(), expected_wire);
    }

    #[test]
    fn batch_is_at_least_one_mib() {
        let w = build().unwrap();
        assert!(
            w.batch.len() >= 1_024 * 1_024,
            "batch is only {} bytes, expected >= 1 MiB",
            w.batch.len()
        );
    }
}
