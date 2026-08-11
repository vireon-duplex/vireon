//! Shared infrastructure for all demo scenarios.
//!
//! Provides cert generation, server lifecycle (RAII guard with graceful
//! shutdown), client connect helpers, and output formatting. Every scenario
//! (`showcase`, `fanout`, `group`, `cluster`) builds on these primitives.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rcgen::{CertificateParams, KeyPair};
use tokio::time::sleep;
use vireon_sdk::{ClientBuilder, ReconnectPolicy, TlsVerify};

// ── Cert generation ──────────────────────────────────────────────────

/// Write a self-signed TLS cert + key to temp files. Returns `(cert, key)`.
pub fn write_dev_cert() -> (PathBuf, PathBuf) {
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

// ── Server binary location ───────────────────────────────────────────

/// Locate the `quic-server` binary under `target/`, preferring release.
/// Falls back to building it if not found.
pub fn server_binary() -> PathBuf {
    let target_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target");
    for profile in &["release", "debug"] {
        let direct = target_root.join(profile).join("quic-server");
        if direct.exists() {
            return direct;
        }
    }
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
    let _ = Command::new("cargo")
        .args(["build", "-p", "quic-server", "--bin", "quic-server", "--release"])
        .status();
    target_root.join("release/quic-server")
}

// ── Server lifecycle ─────────────────────────────────────────────────

/// RAII guard: spawns a `quic-server` child process and kills it on drop.
/// Drop sends SIGINT first (graceful drain), then SIGKILL as fallback.
pub struct ServerGuard {
    child: Option<Child>,
}

impl ServerGuard {
    /// Check if the child is still running (for ghost-socket detection).
    pub fn is_alive(&mut self) -> bool {
        if let Some(child) = &mut self.child {
            matches!(child.try_wait(), Ok(None))
        } else {
            false
        }
    }

    /// Spawn a server with `--workers N` and standard flags.
    pub fn start(port: u16, cert: &Path, key: &Path, workers: usize) -> Self {
        let workers_str = workers.to_string().leak();
        Self::start_with(port, cert, key, &["--workers", workers_str])
    }

    /// Spawn with extra CLI args (e.g. cluster flags). Does NOT add `--workers`
    /// automatically — callers pass it themselves.
    pub fn start_with(port: u16, cert: &Path, key: &Path, extra_args: &[&str]) -> Self {
        let bin = server_binary();
        let mut cmd = Command::new(&bin);
        cmd.args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--cert",
            cert.to_str().expect("cert path utf8"),
            "--key",
            key.to_str().expect("key path utf8"),
        ])
        .args(extra_args.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
        let child = cmd.spawn().expect("failed to spawn quic-server");
        Self { child: Some(child) }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            // SIGINT first so the server runs its graceful shutdown path
            // (drain → Reactor::drop → cancel io_uring → fds close).
            let pid = c.id() as i32;
            unsafe {
                libc::kill(pid, libc::SIGINT);
            }
            // Poll for graceful exit (up to 3s).
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                match c.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    _ => break,
                }
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

// ── Port allocation ──────────────────────────────────────────────────

/// Allocate an ephemeral port by briefly binding a TCP socket.
pub fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

// ── Client connect helpers ───────────────────────────────────────────

/// Connect with a ReconnectPolicy (for scenarios that need auto-reconnect).
/// Retries until the server is ready (up to 15s deadline).
pub async fn connect_with_reconnect(addr: &str) -> vireon_sdk::Client {
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

/// Simple retry-connect without a ReconnectPolicy (for fire-and-forget scenarios).
/// Retries until the server is ready (up to 30s deadline).
pub async fn connect_ready(addr: &str) -> vireon_sdk::Client {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match tokio::time::timeout(
            Duration::from_secs(2),
            ClientBuilder::new(addr)
                .sni("localhost")
                .tls_verify(TlsVerify::DangerAcceptInvalid)
                .connect(),
        )
        .await
        {
            Ok(Ok(c)) => return c,
            Ok(Err(_)) | Err(_) if Instant::now() < deadline => {
                sleep(Duration::from_millis(150)).await;
            }
            Ok(Err(e)) => panic!("could not connect to server: {e}"),
            Err(_) => panic!("connect_ready deadline exceeded"),
        }
    }
}

// ── Output helpers ───────────────────────────────────────────────────

/// Format byte counts: MiB for >= 1 MiB, KiB otherwise.
pub fn fmt_bytes(bytes: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else {
        format!("{} KiB", bytes / 1024)
    }
}

/// Initialise tracing output on stderr (keeps stdout clean for narrative).
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();
}

// ── Latency measurement ──────────────────────────────────────────────

/// Current time in nanoseconds since UNIX epoch. Used as a publish-side
/// timestamp embedded in the payload so receivers can compute end-to-end
/// latency without clock-sync concerns (same machine, same clock).
pub fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos() as u64
}

/// Encode a sequence ID + publish timestamp into the first 16 bytes of
/// a payload buffer. Bytes 0..8 = seq (BE), bytes 8..16 = timestamp (BE).
pub fn encode_payload(buf: &mut [u8], seq: u64, pub_nanos: u64) {
    if buf.len() >= 16 {
        buf[..8].copy_from_slice(&seq.to_be_bytes());
        buf[8..16].copy_from_slice(&pub_nanos.to_be_bytes());
    }
}

/// Decode (seq, publish_nanos) from a received payload. Returns `None`
/// if the payload is too short for the encoded header.
pub fn decode_payload(payload: &[u8]) -> Option<(u64, u64)> {
    if payload.len() >= 16 {
        let mut s = [0u8; 8];
        s.copy_from_slice(&payload[..8]);
        let seq = u64::from_be_bytes(s);
        let mut t = [0u8; 8];
        t.copy_from_slice(&payload[8..16]);
        let ts = u64::from_be_bytes(t);
        Some((seq, ts))
    } else {
        None
    }
}

/// Latency histogram. Collects raw nanosecond samples and computes
/// percentiles at the end.
#[derive(Default)]
pub struct LatencyStats {
    samples: Vec<u64>,
}

impl LatencyStats {
    /// Record an end-to-end latency sample (nanoseconds).
    pub fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    /// Number of recorded samples.
    pub fn count(&self) -> usize {
        self.samples.len()
    }

    /// Compute the requested percentile (0..=100). Returns `None` when empty.
    fn percentile(&self, pct: f64) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        Some(sorted[idx.min(sorted.len() - 1)])
    }

    /// p50 (median) latency in nanoseconds.
    pub fn p50(&self) -> Option<u64> {
        self.percentile(50.0)
    }

    /// p99 latency in nanoseconds.
    pub fn p99(&self) -> Option<u64> {
        self.percentile(99.0)
    }

    /// Maximum latency in nanoseconds.
    pub fn max(&self) -> Option<u64> {
        self.samples.iter().copied().max()
    }
}

/// Format a nanosecond duration in a human-friendly unit (μs or ms).
pub fn fmt_latency(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} μs", ns as f64 / 1_000.0)
    } else {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    }
}
