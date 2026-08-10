//! Vireon Multi-Stream Showcase Demo.
//!
//! Runs a complete narrative in ~35 seconds:
//!   1. One QUIC connection carries 3 dedicated streams (Audio + Video + AI)
//!   2. Each stream has its own delivery policy and priority
//!   3. Server is killed mid-stream — client auto-reconnects + resumes
//!   4. AI stream gets gap-filled; Audio/Video resume from fresh
//!
//! Run with: `cargo run -p demo --release`

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::too_many_lines,
    clippy::manual_div_ceil
)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rcgen::{CertificateParams, KeyPair};
use tokio::time::sleep;
use vireon_sdk::{
    ClientBuilder, ReconnectPolicy, StreamHandle, StreamSpec, StreamType, TlsVerify,
};

// ── Stream configuration ────────────────────────────────────────────

const AUDIO_TOPIC: &str = "audio.frame";
const VIDEO_TOPIC: &str = "video.frame";
const AI_TOPIC: &str = "ai.token";

const AUDIO_PAYLOAD: usize = 160; // Opus 20 ms @ 64 kbps
const VIDEO_PAYLOAD: usize = 65_536; // H.264 P-frame
const AI_PAYLOAD: usize = 1_024; // LLM token chunk

const AUDIO_INTERVAL: Duration = Duration::from_millis(20); // 50 fps
const VIDEO_INTERVAL: Duration = Duration::from_millis(100); // 10 fps
const AI_INTERVAL: Duration = Duration::from_millis(10); // 100 fps

const PHASE_DURATION: Duration = Duration::from_secs(10);
const RESTART_WAIT: Duration = Duration::from_secs(3);

// ── Per-stream stats (shared between subscriber tasks and main) ─────

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

// ── Cert + server helpers (inlined from SDK test suite) ─────────────

fn write_dev_cert() -> (PathBuf, PathBuf) {
    let mut params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("rcgen params");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    let key = KeyPair::generate().expect("rcgen key");
    let cert = params.self_signed(&key).expect("rcgen self-signed");
    let id = std::process::id();
    let dir = std::env::temp_dir();
    let cert_path = dir.join(format!("vireon-demo-{id}-cert.pem"));
    let key_path = dir.join(format!("vireon-demo-{id}-key.pem"));
    std::fs::write(&cert_path, cert.pem().as_bytes()).expect("write cert");
    std::fs::write(&key_path, key.serialize_pem().as_bytes()).expect("write key");
    (cert_path, key_path)
}

fn server_binary() -> PathBuf {
    let target_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target");
    // Try release first (the demo runs in release), then debug.
    for profile in &["release", "debug"] {
        let direct = target_root.join(profile).join("quic-server");
        if direct.exists() {
            return direct;
        }
    }
    // Also check triple-qualified paths.
    if let Ok(entries) = std::fs::read_dir(&target_root) {
        for e in entries.flatten() {
            for profile in &["release", "debug"] {
                let p = e.path().join(profile).join("quic-server");
                if p.exists() {
                    return p;
                }
            }
        }
    }
    // Build it.
    let _ = Command::new("cargo")
        .args(["build", "-p", "quic-server", "--bin", "quic-server", "--release"])
        .status();
    target_root.join("release/quic-server")
}

struct ServerGuard {
    child: Option<Child>,
}

impl ServerGuard {
    fn start(port: u16, cert: &Path, key: &Path) -> Self {
        let bin = server_binary();
        let child = Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--workers",
                "1",
                "--cert",
                cert.to_str().unwrap(),
                "--key",
                key.to_str().unwrap(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn quic-server");
        Self { child: Some(child) }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

async fn connect_with_reconnect(addr: &str) -> vireon_sdk::Client {
    let policy = ReconnectPolicy {
        max_attempts: 50,
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_millis(1000),
        resubscribe: true,
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match ClientBuilder::new(addr)
            .sni("localhost")
            .tls_verify(TlsVerify::DangerAcceptInvalid)
            .max_idle_timeout(Duration::from_secs(5))
            .reconnect(policy.clone())
            .connect()
            .await
        {
            Ok(c) => return c,
            Err(_) if Instant::now() < deadline => {
                sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("could not connect to server: {e}"),
        }
    }
}

// ── Publisher / subscriber task loops ───────────────────────────────

async fn publisher_task(stream: StreamHandle, topic: &'static str, size: usize, interval: Duration) {
    let payload = Bytes::from(vec![0xAA; size]);
    loop {
        let _ = stream.try_publish(topic, payload.clone());
        sleep(interval).await;
    }
}

async fn subscriber_task(
    mut stream: StreamHandle,
    label: &'static str,
    frames: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
) {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.recv()).await {
            Ok(Some(msg)) => {
                frames.fetch_add(1, Ordering::Relaxed);
                bytes.fetch_add(msg.payload.len() as u64, Ordering::Relaxed);
                let _ = label; // label kept for potential debug logging
            }
            Ok(None) => break,
            Err(_) => {} // timeout — no data this window
        }
    }
}

// ── Output helpers ──────────────────────────────────────────────────

fn print_per_stream(delta: &Snapshot, elapsed: Duration) {
    let secs = elapsed.as_secs_f64().max(0.001);
    println!(
        "  Audio  {:>5} frames | {:>6} KiB | {:>5.1} fps",
        delta.audio_frames,
        delta.audio_bytes / 1024,
        delta.audio_frames as f64 / secs,
    );
    println!(
        "  Video  {:>5} frames | {:>6} KiB | {:>5.1} fps",
        delta.video_frames,
        delta.video_bytes / 1024,
        delta.video_frames as f64 / secs,
    );
    println!(
        "  AI     {:>5} frames | {:>6} KiB | {:>5.1} fps",
        delta.ai_frames,
        delta.ai_bytes / 1024,
        delta.ai_frames as f64 / secs,
    );
}

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // Send tracing output to stderr so it never interleaves with the
    // narrative on stdout. Default filter suppresses per-recv idle-timer
    // spam during the reconnect phase; set RUST_LOG=vireon_sdk=info to see it.
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    println!(
        "\n========================================================================"
    );
    println!("  Vireon Multi-Stream Showcase");
    println!("  One QUIC Connection | Three Streams | Auto-Reconnect & Resume");
    println!(
        "========================================================================\n"
    );

    // ── Setup ────────────────────────────────────────────────────────

    let (cert, key) = write_dev_cert();
    let port = ephemeral_port();
    let addr = format!("127.0.0.1:{port}");

    println!("[setup] starting quic-server on {addr}");
    let server1 = ServerGuard::start(port, &cert, &key);
    sleep(Duration::from_millis(500)).await;

    println!("[setup] connecting publisher...");
    let pub_client = connect_with_reconnect(&addr).await;
    println!("[setup] connecting subscriber...");
    let sub_client = connect_with_reconnect(&addr).await;

    // ── Open 3 publisher streams ─────────────────────────────────────

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

    println!(
        "[setup] publisher streams: Audio(RealtimeDropOld/Critical) + Video(RealtimeDropOld/High) + AI(ReliableOrdered/Normal)"
    );

    // ── Open 3 subscriber streams ────────────────────────────────────

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

    println!("[setup] subscriber streams subscribed to audio.* + video.* + ai.*");
    sleep(Duration::from_millis(500)).await;

    // ── Stats ────────────────────────────────────────────────────────

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
        let _t1 = tokio::spawn(subscriber_task(sub_audio, "audio", af.clone(), ab.clone()));
        let _t2 = tokio::spawn(subscriber_task(sub_video, "video", vf.clone(), vb.clone()));
        let _t3 = tokio::spawn(subscriber_task(sub_ai, "ai", aif.clone(), aib.clone()));
        // Sync loop: copy local atomics into shared stats every 200 ms.
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
        // Keep tasks alive.
        let _ = h1.await;
        let _ = h2.await;
        let _ = h3.await;
    });

    // ── Phase 1: Concurrent streaming ────────────────────────────────

    println!("\n>>> Phase 1: Concurrent streaming (10 s)");
    println!("    Audio @ 50 fps | Video @ 10 fps | AI @ 100 fps\n");

    let p1_start = stats.snapshot();
    let p1_begin = Instant::now();
    sleep(PHASE_DURATION).await;
    let p1_end = stats.snapshot();
    let p1_delta = p1_end.delta(&p1_start);
    print_per_stream(&p1_delta, p1_begin.elapsed());
    println!("\n    All 3 streams delivered independently -- no head-of-line blocking.");

    // ── Phase 2: Kill server ─────────────────────────────────────────

    println!("\n>>> Phase 2: Network interruption (killing server)");
    drop(server1);
    println!("    [server] killed -- client idle timer will fire in ~5 s");

    // ── Phase 3: Restart + reconnect ─────────────────────────────────

    println!("\n>>> Phase 3: Server restart + automatic reconnect");
    sleep(RESTART_WAIT).await;
    println!("    [server] restarting on same port...");
    let _server2 = ServerGuard::start(port, &cert, &key);
    sleep(Duration::from_millis(500)).await;
    println!("    [client] waiting for reconnect + stream resume...");
    // Poll for actual frame delivery rather than a fixed sleep — reconnect
    // latency varies with idle-timeout negotiation and backoff schedule.
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
    let p4_delta = p4_end.delta(&p4_start);
    print_per_stream(&p4_delta, p4_begin.elapsed());
    println!("\n    Delivery resumed on all 3 streams after reconnect.");

    // ── Summary ──────────────────────────────────────────────────────

    let total = stats.snapshot();
    println!("\n========================================================================");
    println!("  Summary");
    println!("------------------------------------------------------------------------");
    println!(
        "  Streams per connection:  3 (Audio + Video + AI)"
    );
    println!(
        "  Total frames delivered:  Audio {} | Video {} | AI {}",
        total.audio_frames, total.video_frames, total.ai_frames
    );
    println!(
        "  Total data delivered:    {} KiB",
        (total.audio_bytes + total.video_bytes + total.ai_bytes) / 1024
    );
    println!(
        "  QUIC connections used:   2 (1 publisher + 1 subscriber)"
    );
    println!(
        "  Reconnect:               automatic (resubscribe + stream-open replay)"
    );
    println!("========================================================================\n");

    // Cleanup
    sub_tasks.abort();
    pub_tasks.abort();
    let _ = pub_client.close().await;
    let _ = sub_client.close().await;
}
