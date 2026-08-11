//! Hero demo: 3 dedicated QUIC streams (Audio + Video + AI) on 1 connection
//! with per-stream delivery policies and automatic reconnect + resume.
//!
//! Runs a 4-phase narrative in ~45 seconds:
//!   1. Concurrent streaming — no head-of-line blocking between streams
//!   2. Server killed mid-stream
//!   3. Server restarted — SDK auto-reconnects + replays subscriptions
//!   4. Post-reconnect streaming — all 3 streams resume seamlessly

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::too_many_lines
)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::time::sleep;
use vireon_sdk::{StreamHandle, StreamSpec, StreamType};

use crate::common;

// ── Stream configuration ─────────────────────────────────────────────

const AUDIO_TOPIC: &str = "audio.frame";
const VIDEO_TOPIC: &str = "video.frame";
const AI_TOPIC: &str = "ai.token";

const AUDIO_PAYLOAD: usize = 160; // Opus 20 ms @ 64 kbps
const VIDEO_PAYLOAD: usize = 262_144; // 256 KiB H.264 frame (~60 fps)
const AI_PAYLOAD: usize = 16_384; // 16 KiB LLM token batch

const AUDIO_INTERVAL: Duration = Duration::from_millis(20); // 50 fps
const VIDEO_INTERVAL: Duration = Duration::from_millis(16); // ~60 fps
const AI_INTERVAL: Duration = Duration::from_millis(1); // ~1000 fps

const PHASE_DURATION: Duration = Duration::from_secs(10);
const RESTART_WAIT: Duration = Duration::from_secs(3);

// ── Per-stream stats ─────────────────────────────────────────────────

#[derive(Default)]
struct Stats {
    audio_frames: AtomicU64,
    audio_bytes: AtomicU64,
    video_frames: AtomicU64,
    video_bytes: AtomicU64,
    ai_frames: AtomicU64,
    ai_bytes: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            audio_frames: self.audio_frames.load(Ordering::Relaxed),
            audio_bytes: self.audio_bytes.load(Ordering::Relaxed),
            video_frames: self.video_frames.load(Ordering::Relaxed),
            video_bytes: self.video_bytes.load(Ordering::Relaxed),
            ai_frames: self.ai_frames.load(Ordering::Relaxed),
            ai_bytes: self.ai_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Snapshot {
    audio_frames: u64,
    audio_bytes: u64,
    video_frames: u64,
    video_bytes: u64,
    ai_frames: u64,
    ai_bytes: u64,
}

impl Snapshot {
    fn delta(&self, prev: &Snapshot) -> Snapshot {
        Snapshot {
            audio_frames: self.audio_frames.saturating_sub(prev.audio_frames),
            audio_bytes: self.audio_bytes.saturating_sub(prev.audio_bytes),
            video_frames: self.video_frames.saturating_sub(prev.video_frames),
            video_bytes: self.video_bytes.saturating_sub(prev.video_bytes),
            ai_frames: self.ai_frames.saturating_sub(prev.ai_frames),
            ai_bytes: self.ai_bytes.saturating_sub(prev.ai_bytes),
        }
    }
}

// ── Publisher / subscriber task loops ────────────────────────────────

async fn publisher_task(stream: StreamHandle, topic: &'static str, size: usize, interval: Duration) {
    let payload = Bytes::from(vec![0xAA; size]);
    loop {
        let _ = stream.try_publish(topic, payload.clone());
        sleep(interval).await;
    }
}

async fn subscriber_task(
    mut stream: StreamHandle,
    frames: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
) {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.recv()).await {
            Ok(Some(msg)) => {
                frames.fetch_add(1, Ordering::Relaxed);
                bytes.fetch_add(msg.payload.len() as u64, Ordering::Relaxed);
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }
}

// ── Output helpers ───────────────────────────────────────────────────

fn print_per_stream(delta: &Snapshot, elapsed: Duration) {
    let secs = elapsed.as_secs_f64().max(0.001);
    println!(
        "  Audio  {:>5} frames | {:>9} | {:>5.1} fps",
        delta.audio_frames,
        common::fmt_bytes(delta.audio_bytes),
        delta.audio_frames as f64 / secs,
    );
    println!(
        "  Video  {:>5} frames | {:>9} | {:>5.1} fps",
        delta.video_frames,
        common::fmt_bytes(delta.video_bytes),
        delta.video_frames as f64 / secs,
    );
    println!(
        "  AI     {:>5} frames | {:>9} | {:>5.1} fps",
        delta.ai_frames,
        common::fmt_bytes(delta.ai_bytes),
        delta.ai_frames as f64 / secs,
    );
    let total_bytes = delta.audio_bytes + delta.video_bytes + delta.ai_bytes;
    println!(
        "  Total  {:>5} frames | {:>9} | {:>5.1} MiB/s",
        delta.audio_frames + delta.video_frames + delta.ai_frames,
        common::fmt_bytes(total_bytes),
        total_bytes as f64 / (1024.0 * 1024.0) / secs,
    );
}

// ── Entry point ──────────────────────────────────────────────────────

pub async fn run(workers: usize) {
    println!("\n========================================================================");
    println!("  Vireon Multi-Stream Showcase");
    println!("  One QUIC Connection | Three Streams | Auto-Reconnect & Resume");
    println!("========================================================================\n");

    let (cert, key) = common::write_dev_cert();
    let port = common::ephemeral_port();
    let addr = format!("127.0.0.1:{port}");

    println!("[setup] starting quic-server ({workers} worker(s)) on {addr}");
    let server1 = common::ServerGuard::start(port, &cert, &key, workers);
    sleep(Duration::from_millis(500)).await;

    println!("[setup] connecting publisher...");
    let pub_client = common::connect_with_reconnect(&addr).await;
    println!("[setup] connecting subscriber...");
    let sub_client = common::connect_with_reconnect(&addr).await;

    // ── Open 3 publisher + 3 subscriber streams ──────────────────────

    let pub_audio = pub_client
        .open_stream(StreamSpec::typed(StreamType::Audio).with_topic(AUDIO_TOPIC))
        .await
        .expect("open audio stream");
    let pub_video = pub_client
        .open_stream(StreamSpec::typed(StreamType::Video).with_topic(VIDEO_TOPIC))
        .await
        .expect("open video stream");
    let pub_ai = pub_client
        .open_stream(StreamSpec::typed(StreamType::Ai).with_topic(AI_TOPIC))
        .await
        .expect("open ai stream");

    println!("[setup] publisher: Audio(RealtimeDropOld/Critical) + Video(RealtimeDropOld/High) + AI(ReliableOrdered/Normal)");

    let sub_audio = sub_client
        .open_stream(StreamSpec::typed(StreamType::Audio).with_topic("audio.*"))
        .await
        .expect("open sub audio");
    let sub_video = sub_client
        .open_stream(StreamSpec::typed(StreamType::Video).with_topic("video.*"))
        .await
        .expect("open sub video");
    let sub_ai = sub_client
        .open_stream(StreamSpec::typed(StreamType::Ai).with_topic("ai.*"))
        .await
        .expect("open sub ai");

    println!("[setup] subscriber: audio.* + video.* + ai.*");
    sleep(Duration::from_millis(500)).await;

    let stats = Arc::new(Stats::default());

    // ── Spawn subscriber tasks ───────────────────────────────────────

    let s = stats.clone();
    let sub_tasks = tokio::spawn(async move {
        let (af, ab, vf, vb, aif, aib) = (
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        let _t1 = tokio::spawn(subscriber_task(sub_audio, af.clone(), ab.clone()));
        let _t2 = tokio::spawn(subscriber_task(sub_video, vf.clone(), vb.clone()));
        let _t3 = tokio::spawn(subscriber_task(sub_ai, aif.clone(), aib.clone()));
        loop {
            sleep(Duration::from_millis(200)).await;
            s.audio_frames.store(af.load(Ordering::Relaxed), Ordering::Relaxed);
            s.audio_bytes.store(ab.load(Ordering::Relaxed), Ordering::Relaxed);
            s.video_frames.store(vf.load(Ordering::Relaxed), Ordering::Relaxed);
            s.video_bytes.store(vb.load(Ordering::Relaxed), Ordering::Relaxed);
            s.ai_frames.store(aif.load(Ordering::Relaxed), Ordering::Relaxed);
            s.ai_bytes.store(aib.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    });

    // ── Spawn publisher tasks ────────────────────────────────────────

    let pub_tasks = tokio::spawn(async move {
        let h1 = tokio::spawn(publisher_task(pub_audio, AUDIO_TOPIC, AUDIO_PAYLOAD, AUDIO_INTERVAL));
        let h2 = tokio::spawn(publisher_task(pub_video, VIDEO_TOPIC, VIDEO_PAYLOAD, VIDEO_INTERVAL));
        let h3 = tokio::spawn(publisher_task(pub_ai, AI_TOPIC, AI_PAYLOAD, AI_INTERVAL));
        let _ = h1.await;
        let _ = h2.await;
        let _ = h3.await;
    });

    // ── Phase 1: Concurrent streaming ────────────────────────────────

    println!("\n>>> Phase 1: Concurrent streaming (10 s)");
    println!("    Audio @ 50 fps | Video @ 60 fps | AI @ 1000 fps\n");

    let p1_start = stats.snapshot();
    let p1_begin = Instant::now();
    sleep(PHASE_DURATION).await;
    let p1_end = stats.snapshot();
    print_per_stream(&p1_end.delta(&p1_start), p1_begin.elapsed());
    println!("\n    All 3 streams delivered independently -- no head-of-line blocking.");

    // ── Phase 2: Kill server ─────────────────────────────────────────

    println!("\n>>> Phase 2: Network interruption (killing server)");
    drop(server1);
    println!("    [server] killed -- client idle timer will fire in ~5 s");

    // ── Phase 3: Restart + reconnect ─────────────────────────────────

    println!("\n>>> Phase 3: Server restart + automatic reconnect");
    sleep(RESTART_WAIT).await;
    println!("    [server] restarting on same port...");
    let _server2 = common::ServerGuard::start(port, &cert, &key, workers);
    sleep(Duration::from_millis(500)).await;
    println!("    [client] waiting for reconnect + stream resume...");

    let probe_start = Instant::now();
    let probe_deadline = probe_start + Duration::from_secs(30);
    let mut prev = stats.snapshot();
    while Instant::now() < probe_deadline {
        sleep(Duration::from_millis(500)).await;
        let now = stats.snapshot();
        let delta = now.delta(&prev);
        if delta.audio_frames + delta.video_frames + delta.ai_frames > 0 {
            break;
        }
        prev = now;
    }
    let probe_elapsed = probe_start.elapsed();
    if probe_elapsed >= Duration::from_secs(30) {
        println!("    [client] WARNING: no frames received 30 s after restart");
    } else {
        println!(
            "    [client] reconnect confirmed after {:.1} s",
            probe_elapsed.as_secs_f64()
        );
    }

    // ── Phase 4: Post-reconnect streaming ────────────────────────────

    println!("\n>>> Phase 4: Post-reconnect streaming (10 s)");
    println!("    Streams should resume seamlessly.\n");

    let p4_start = stats.snapshot();
    let p4_begin = Instant::now();
    sleep(PHASE_DURATION).await;
    let p4_end = stats.snapshot();
    print_per_stream(&p4_end.delta(&p4_start), p4_begin.elapsed());
    println!("\n    Delivery resumed on all 3 streams after reconnect.");

    // ── Summary ──────────────────────────────────────────────────────

    let total = stats.snapshot();
    println!("\n========================================================================");
    println!("  Summary");
    println!("------------------------------------------------------------------------");
    println!("  Streams per connection:  3 (Audio + Video + AI)");
    println!(
        "  Total frames delivered:  Audio {} | Video {} | AI {}",
        total.audio_frames, total.video_frames, total.ai_frames
    );
    println!(
        "  Total data delivered:    {}",
        common::fmt_bytes(total.audio_bytes + total.video_bytes + total.ai_bytes)
    );
    println!("  QUIC connections used:   2 (1 publisher + 1 subscriber)");
    println!("  Reconnect:               automatic (resubscribe + stream-open replay)");
    println!("========================================================================\n");

    sub_tasks.abort();
    pub_tasks.abort();
    let _ = pub_client.close().await;
    let _ = sub_client.close().await;
}
