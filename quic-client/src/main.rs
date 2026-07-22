//! `quic-client` — interactive QUIC client for Vireon `quic-server`.
//!
//! Connects via QUIC/TLS 1.3 and exercises different application-layer
//! patterns: chat, pub/sub, RPC, event bus.
//!
//! All communication uses QUIC bidirectional stream 0 and the Vireon
//! frame codec (22-byte header + payload + 4-byte CRC32C).

use std::io::{BufRead, Write};
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};

use bytes::BytesMut;
use frame::codec::{self, CRC_SIZE, Frame, FrameDecoder, FrameHeader, HEADER_SIZE};
use frame::header::{FrameFlags, MessageType, StreamId};

// ── Constants ─────────────────────────────────────────────────────

/// ALPN — must match the server's advertised protocols.
const ALPN: &[u8] = b"h3-29";

/// Client uses QUIC bidirectional stream 0.
const QUIC_STREAM_ID: u64 = 0;

/// Logical frame stream id used for outgoing frames.
const LOGICAL_STREAM: u64 = 1;

/// UDP datagram scratch buffer size.
const DGRAM_BUF: usize = 65_535;

/// Handshake timeout.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// Read timeout for the UDP socket (non-blocking loop).
const READ_TIMEOUT: Duration = Duration::from_millis(50);

// ── CLI ───────────────────────────────────────────────────────────

/// Vireon QUIC client — connect to a Vireon quic-server.
#[derive(Parser, Debug)]
#[command(name = "quic-client", version, about)]
struct Cli {
    /// Server host.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Server UDP port.
    #[arg(long, default_value_t = 4433)]
    port: u16,

    /// SNI hostname (defaults to --host).
    #[arg(long)]
    sni: Option<String>,

    /// Connection mode.
    #[arg(short = 'm', long = "mode", value_enum, default_value_t = Mode::Chat)]
    mode: Mode,

    // ── Pub/Sub mode args ──────────────────────────────────────────
    /// Pub/Sub action: subscribe or publish.
    #[arg(long, default_value = "publish")]
    action: String,

    /// Topic (for publish) or pattern (for subscribe).
    #[arg(long)]
    topic: Option<String>,

    /// Text payload to send.
    #[arg(long, short = 't')]
    text: Option<String>,

    /// How long to listen for incoming messages (seconds).
    #[arg(long, short = 'l')]
    listen: Option<u64>,

    /// Use native `MessageType::Subscribe`/`Publish`/... frames instead of
    /// the legacy JSON-over-StateUpdate encoding (Phase 1 wire format).
    #[arg(long, default_value_t = false)]
    native: bool,

    // ── RPC mode args ──────────────────────────────────────────────
    /// RPC method name.
    #[arg(long)]
    method: Option<String>,

    /// RPC arguments (JSON string).
    #[arg(long)]
    args: Option<String>,
}

#[derive(Clone, Debug, ValueEnum)]
enum Mode {
    /// Send TextToken frames, display received text.
    Chat,
    /// Pub/Sub — subscribe to topics, publish messages.
    Pubsub,
    /// RPC — send ToolCall, display ToolResult.
    Rpc,
    /// Event bus — send StateUpdate to event bus.
    Eventbus,
}

// ── QUIC connection wrapper ───────────────────────────────────────

/// Holds the QUIC connection + UDP socket, providing send/recv helpers.
struct ClientConn {
    conn: quiche::Connection,
    socket: UdpSocket,
    peer: SocketAddr,
    local: SocketAddr,
    send_buf: Vec<u8>,
    recv_buf: Vec<u8>,
}

impl ClientConn {
    /// Creates a UDP socket, initiates QUIC handshake, and waits for
    /// the connection to be established.
    fn connect(host: &str, port: u16, sni: &str) -> Result<Self> {
        let peer: SocketAddr = format!("{host}:{port}").parse()?;
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(READ_TIMEOUT))?;
        let local = socket.local_addr()?;

        let mut config = build_client_config()?;
        let scid = quiche::ConnectionId::from_vec(gen_scid());
        let conn = quiche::connect(Some(sni), &scid, local, peer, &mut config)?;

        let mut c = Self {
            conn,
            socket,
            peer,
            local,
            send_buf: vec![0u8; DGRAM_BUF],
            recv_buf: vec![0u8; DGRAM_BUF],
        };

        // Handshake loop
        let hs_start = Instant::now();
        c.flush()?;
        loop {
            if c.conn.is_established() {
                break;
            }
            if c.conn.is_closed() {
                bail!("connection closed during handshake");
            }
            if hs_start.elapsed() > HANDSHAKE_DEADLINE {
                bail!("handshake timed out");
            }
            c.recv()?;
            c.flush()?;
        }

        eprintln!(
            "[client] connected to {peer} (handshake {}ms)",
            hs_start.elapsed().as_millis()
        );
        Ok(c)
    }

    /// Reads incoming UDP datagrams and feeds them to quiche.
    fn recv(&mut self) -> Result<()> {
        loop {
            match self.socket.recv(&mut self.recv_buf) {
                Ok(n) => {
                    let info = quiche::RecvInfo {
                        from: self.peer,
                        to: self.local,
                    };
                    match self.conn.recv(&mut self.recv_buf[..n], info) {
                        Ok(_) | Err(quiche::Error::Done) => {}
                        Err(e) => bail!("quiche recv: {e}"),
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => bail!("socket recv: {e}"),
            }
        }
    }

    /// Flushes pending quiche output as UDP datagrams.
    fn flush(&mut self) -> Result<()> {
        loop {
            match self.conn.send(&mut self.send_buf) {
                Ok((n, info)) => {
                    self.socket.send_to(&self.send_buf[..n], info.to)?;
                }
                Err(quiche::Error::Done) => return Ok(()),
                Err(e) => bail!("quiche send: {e}"),
            }
        }
    }

    /// Sends raw bytes on QUIC stream 0.
    fn stream_send(&mut self, stream_id: u64, data: &[u8]) -> Result<()> {
        loop {
            match self.conn.stream_send(stream_id, data, false) {
                Ok(_) => return Ok(()),
                Err(quiche::Error::Done) => {
                    // Flow-control blocked — process packets and retry.
                    self.recv()?;
                    self.flush()?;
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => bail!("stream_send: {e}"),
            }
        }
    }

    /// Encodes and sends a Vireon frame on QUIC stream 0.
    fn send_frame(&mut self, msg_type: MessageType, payload: &[u8]) -> Result<()> {
        let header =
            FrameHeader::new(StreamId::new(LOGICAL_STREAM), msg_type).with_flags(FrameFlags::NONE);
        let wire_size = HEADER_SIZE + payload.len() + CRC_SIZE;
        let mut buf = BytesMut::with_capacity(wire_size);
        codec::encode_into(&mut buf, header, payload)?;
        self.stream_send(QUIC_STREAM_ID, &buf)?;
        self.flush()?;
        Ok(())
    }

    /// Reads from QUIC stream 0, returns decoded owned frames.
    fn drain_frames(&mut self, decoder: &mut FrameDecoder) -> Result<Vec<Frame>> {
        let mut frames_out = Vec::new();

        loop {
            let mut buf = vec![0u8; DGRAM_BUF];
            match self.conn.stream_recv(QUIC_STREAM_ID, &mut buf) {
                Ok((n, _)) => {
                    decoder.push(&buf[..n]);
                }
                Err(quiche::Error::Done) => break,
                Err(e) => {
                    eprintln!("[client] stream_recv error: {e}");
                    break;
                }
            }
        }

        // Decode all complete frames
        loop {
            match decoder.decode_frame() {
                Ok(Some(frame)) => {
                    frames_out.push(frame);
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("[client] decode error: {e:?}");
                    decoder.clear();
                    break;
                }
            }
        }

        Ok(frames_out)
    }

    /// Processes events (recv + flush) without blocking.
    fn tick(&mut self) -> Result<()> {
        self.recv()?;
        self.flush()?;
        Ok(())
    }

    /// Returns true if the connection is closed.
    fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    /// Closes the connection gracefully.
    fn close(&mut self) {
        let _ = self.conn.close(true, 0x00, b"bye");
        let _ = self.flush();
    }
}

/// Builds quiche client config with ALPN matching the server.
fn build_client_config() -> Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.set_application_protos(&[ALPN])?;
    config.set_max_idle_timeout(60_000);
    config.set_max_recv_udp_payload_size(65507);
    config.set_max_send_udp_payload_size(65507);
    config.set_initial_max_data(100 * 1024 * 1024);
    config.set_initial_max_stream_data_bidi_local(10 * 1024 * 1024);
    config.set_initial_max_stream_data_bidi_remote(10 * 1024 * 1024);
    config.set_initial_max_stream_data_uni(10 * 1024 * 1024);
    config.set_initial_max_streams_bidi(1024);
    config.set_initial_max_streams_uni(1024);
    config.set_disable_active_migration(true);
    config.discover_pmtu(false);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    Ok(config)
}

/// Generates a unique 8-byte source connection ID.
fn gen_scid() -> Vec<u8> {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let mut bytes = vec![0u8; 8];
    bytes.copy_from_slice(&n.to_be_bytes());
    bytes
}

// ── Mode handlers ─────────────────────────────────────────────────

/// Chat mode — sends TextToken, displays received text.
fn chat_mode(conn: &mut ClientConn, text: &str) -> Result<()> {
    eprintln!("[chat] sending: {text}");
    conn.send_frame(MessageType::TextToken, text.as_bytes())?;

    // Wait for echo
    let mut decoder = FrameDecoder::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        conn.tick()?;
        let frames = conn.drain_frames(&mut decoder)?;
        for f in frames {
            let text = String::from_utf8_lossy(&f.payload);
            println!("[chat] << {text}");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    eprintln!("[chat] no response (timeout)");
    Ok(())
}

/// RPC mode — sends ToolCall, displays ToolResult.
fn rpc_mode(conn: &mut ClientConn, method: &str, args: &str) -> Result<()> {
    let payload = format!(r#"{{"method":"{method}","args":{args}}}"#);
    eprintln!("[rpc] calling {method}({args})");
    conn.send_frame(MessageType::ToolCall, payload.as_bytes())?;

    let mut decoder = FrameDecoder::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        conn.tick()?;
        let frames = conn.drain_frames(&mut decoder)?;
        for f in frames {
            let text = String::from_utf8_lossy(&f.payload);
            println!("[rpc] result: {text}");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    eprintln!("[rpc] no response (timeout)");
    Ok(())
}

/// Event bus mode — sends StateUpdate to the event bus.
fn eventbus_mode(conn: &mut ClientConn, text: &str) -> Result<()> {
    let payload = format!(r#"{{"topic_id":"events","data":"{text}"}}"#);
    eprintln!("[eventbus] publishing: {text}");
    conn.send_frame(MessageType::StateUpdate, payload.as_bytes())?;
    conn.tick()?;
    eprintln!("[eventbus] sent");
    Ok(())
}

// ── Pub/Sub mode ──────────────────────────────────────────────────

/// Commands from the stdin reader thread.
enum StdinCmd {
    Publish(String),
    Quit,
}

/// Interactive pub/sub mode.
///
/// - **subscribe**: sends a subscribe frame, then listens for messages.
/// - **publish**: sends a single publish frame.
fn pubsub_mode(conn: &mut ClientConn, cli: &Cli) -> Result<()> {
    let running = std::sync::Arc::new(AtomicBool::new(true));

    if cli.native {
        return pubsub_mode_native(conn, cli, running);
    }

    match cli.action.as_str() {
        "subscribe" => {
            let pattern = cli.topic.as_deref().unwrap_or("*");
            let payload = format!(r#"{{"action":"subscribe","pattern":"{pattern}"}}"#);
            eprintln!("[pubsub] subscribing to '{pattern}'");
            conn.send_frame(MessageType::StateUpdate, payload.as_bytes())?;

            // Spawn stdin reader for publish commands
            let (tx, rx) = mpsc::channel::<StdinCmd>();
            let running_clone = running.clone();
            std::thread::spawn(move || {
                let stdin = std::io::stdin();
                for line in stdin.lock().lines() {
                    if !running_clone.load(Ordering::Relaxed) {
                        break;
                    }
                    match line {
                        Ok(ref l) if l == "/q" || l == "/quit" => {
                            let _ = tx.send(StdinCmd::Quit);
                            break;
                        }
                        Ok(ref l) if !l.trim().is_empty() => {
                            let _ = tx.send(StdinCmd::Publish(l.trim().to_string()));
                        }
                        _ => {}
                    }
                }
            });

            eprintln!("[pubsub] listening for messages... (type to publish, /q to quit)");

            let listen_duration = cli.listen.map(Duration::from_secs);
            let start = Instant::now();
            let mut decoder = FrameDecoder::new();

            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                if conn.is_closed() {
                    eprintln!("[pubsub] connection closed");
                    break;
                }
                if let Some(dur) = listen_duration {
                    if start.elapsed() > dur {
                        break;
                    }
                }

                conn.tick()?;
                let frames = conn.drain_frames(&mut decoder)?;
                for f in frames {
                    let text = String::from_utf8_lossy(&f.payload);
                    println!("[pubsub] << {text}");
                }

                // Check for stdin commands (non-blocking; keep listening
                // even if stdin is unavailable, e.g. background process).
                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(StdinCmd::Publish(text)) => {
                        let topic = cli.topic.as_deref().unwrap_or("default");
                        let payload = format!(
                            r#"{{"action":"publish","topic":"{topic}","payload":"{text}"}}"#
                        );
                        eprintln!("[pubsub] >> publishing to '{topic}': {text}");
                        conn.send_frame(MessageType::StateUpdate, payload.as_bytes())?;
                    }
                    Ok(StdinCmd::Quit) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // stdin closed (e.g. background process) — keep listening
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            }

            running.store(false, Ordering::Relaxed);
        }
        "publish" => {
            let topic = cli.topic.as_deref().unwrap_or("default");
            let text = cli.text.as_deref().unwrap_or("");
            let payload = format!(r#"{{"action":"publish","topic":"{topic}","payload":"{text}"}}"#);
            eprintln!("[pubsub] >> publishing to '{topic}': {text}");
            conn.send_frame(MessageType::StateUpdate, payload.as_bytes())?;

            // Interactive: keep alive and listen for a bit
            let listen_duration = cli
                .listen
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(2));
            let deadline = Instant::now() + listen_duration;
            let mut decoder = FrameDecoder::new();
            while Instant::now() < deadline {
                conn.tick()?;
                let frames = conn.drain_frames(&mut decoder)?;
                for f in frames {
                    let text = String::from_utf8_lossy(&f.payload);
                    println!("[pubsub] << {text}");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        other => {
            eprintln!("[pubsub] unknown action '{other}' (use: publish, subscribe)");
        }
    }

    Ok(())
}

/// Native pub/sub mode — emits Phase 1 wire-format frames
/// (`MessageType::Subscribe` / `Publish` / `Unsubscribe`) instead of
/// JSON-in-StateUpdate.
fn pubsub_mode_native(conn: &mut ClientConn, cli: &Cli, running: Arc<AtomicBool>) -> Result<()> {
    match cli.action.as_str() {
        "subscribe" => {
            let pattern = cli.topic.as_deref().unwrap_or("*");
            eprintln!("[pubsub:native] subscribing to '{pattern}'");
            let payload = encode_subscribe_payload(pattern, 0);
            conn.send_frame(MessageType::Subscribe, &payload)?;

            let (tx, rx) = mpsc::channel::<StdinCmd>();
            let running_clone = running.clone();
            std::thread::spawn(move || {
                let stdin = std::io::stdin();
                for line in stdin.lock().lines() {
                    if !running_clone.load(Ordering::Relaxed) {
                        break;
                    }
                    match line {
                        Ok(ref l) if l == "/q" || l == "/quit" => {
                            let _ = tx.send(StdinCmd::Quit);
                            break;
                        }
                        Ok(ref l) if !l.trim().is_empty() => {
                            let _ = tx.send(StdinCmd::Publish(l.trim().to_string()));
                        }
                        _ => {}
                    }
                }
            });

            eprintln!("[pubsub:native] listening... (type to publish, /q to quit)");

            let listen_duration = cli.listen.map(Duration::from_secs);
            let start = Instant::now();
            let mut decoder = FrameDecoder::new();

            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                if conn.is_closed() {
                    eprintln!("[pubsub:native] connection closed");
                    break;
                }
                if let Some(dur) = listen_duration {
                    if start.elapsed() > dur {
                        break;
                    }
                }

                conn.tick()?;
                let frames = conn.drain_frames(&mut decoder)?;
                for f in frames {
                    let text = String::from_utf8_lossy(&f.payload);
                    println!("[pubsub:native] << (type={:?}) {text}", f.msg_type);
                }

                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(StdinCmd::Publish(text)) => {
                        let topic = cli.topic.as_deref().unwrap_or("default");
                        eprintln!("[pubsub:native] >> publishing to '{topic}': {text}");
                        let payload = encode_publish_payload(topic, text.as_bytes());
                        conn.send_frame(MessageType::Publish, &payload)?;
                    }
                    Ok(StdinCmd::Quit) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            }

            running.store(false, Ordering::Relaxed);
        }
        "publish" => {
            let topic = cli.topic.as_deref().unwrap_or("default");
            let text = cli.text.as_deref().unwrap_or("");
            eprintln!("[pubsub:native] >> publishing to '{topic}': {text}");
            let payload = encode_publish_payload(topic, text.as_bytes());
            conn.send_frame(MessageType::Publish, &payload)?;

            let listen_duration = cli
                .listen
                .map(Duration::from_secs)
                .unwrap_or(Duration::from_secs(2));
            let deadline = Instant::now() + listen_duration;
            let mut decoder = FrameDecoder::new();
            while Instant::now() < deadline {
                conn.tick()?;
                let frames = conn.drain_frames(&mut decoder)?;
                for f in frames {
                    let text = String::from_utf8_lossy(&f.payload);
                    println!("[pubsub:native] << (type={:?}) {text}", f.msg_type);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        "unsubscribe" => {
            let pattern = cli.topic.as_deref().unwrap_or("*");
            eprintln!("[pubsub:native] unsubscribing from '{pattern}'");
            let payload = encode_unsubscribe_payload(pattern);
            conn.send_frame(MessageType::Unsubscribe, &payload)?;
        }
        other => {
            eprintln!(
                "[pubsub:native] unknown action '{other}' (use: publish, subscribe, unsubscribe)"
            );
        }
    }

    Ok(())
}

/// Wire-encodes a `Subscribe` payload: `pattern_len(u16) + pattern + qos(u8)`.
fn encode_subscribe_payload(pattern: &str, qos: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + pattern.len() + 1);
    v.extend_from_slice(&(pattern.len() as u16).to_be_bytes());
    v.extend_from_slice(pattern.as_bytes());
    v.push(qos);
    v
}

/// Wire-encodes an `Unsubscribe` payload: `pattern_len(u16) + pattern`.
fn encode_unsubscribe_payload(pattern: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + pattern.len());
    v.extend_from_slice(&(pattern.len() as u16).to_be_bytes());
    v.extend_from_slice(pattern.as_bytes());
    v
}

/// Wire-encodes a `Publish` payload: `topic_len(u16) + topic + payload_bytes`.
fn encode_publish_payload(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + topic.len() + payload.len());
    v.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    v.extend_from_slice(topic.as_bytes());
    v.extend_from_slice(payload);
    v
}

// ── Main ──────────────────────────────────────────────────────────

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let sni = cli.sni.clone().unwrap_or_else(|| cli.host.clone());

    eprintln!(
        "[client] connecting to {}:{} (mode={:?})",
        cli.host, cli.port, cli.mode
    );

    let mut conn = ClientConn::connect(&cli.host, cli.port, &sni)?;

    // Set Ctrl-C handler
    let running = std::sync::Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let _ = ctrlc::set_handler(move || {
        r.store(false, Ordering::Relaxed);
        eprintln!("\n[client] interrupt received, closing...");
    });

    let result = match cli.mode {
        Mode::Chat => {
            let text = cli.text.as_deref().unwrap_or("hello from quic-client");
            chat_mode(&mut conn, text)
        }
        Mode::Rpc => {
            let method = cli.method.as_deref().unwrap_or("echo");
            let args = cli.args.as_deref().unwrap_or(r#""hello""#);
            rpc_mode(&mut conn, method, args)
        }
        Mode::Eventbus => {
            let text = cli.text.as_deref().unwrap_or("event");
            eventbus_mode(&mut conn, text)
        }
        Mode::Pubsub => pubsub_mode(&mut conn, &cli),
    };

    conn.close();

    let _ = std::io::stderr().flush();
    result
}
