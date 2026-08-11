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
