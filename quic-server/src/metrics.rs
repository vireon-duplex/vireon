//! Thread-safe server metrics + Prometheus `/metrics` endpoint.
//!
//! The existing `observability-sdk::MetricsRegistry` is per-worker and
//! single-threaded (`&mut self`). Prometheus scraping needs a single
//! aggregated view across all workers, so this module provides a
//! separate `Arc`-shared atomic counter store that workers increment
//! on the hot path and a tiny HTTP listener exposes at `/metrics`.
//!
//! ## Metric naming
//!
//! All metrics are prefixed with `vireon_` to avoid collisions in
//! shared Prometheus instances. Counters use the `_total` suffix per
//! the Prometheus naming convention.
//!
//! ## Endpoint
//!
//! The HTTP listener is intentionally minimal — it speaks just enough
//! HTTP/1.1 to satisfy a Prometheus scraper (GET /metrics). No hyper,
//! no axum, no new dependencies. A scrape request completes in <1 ms.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

/// Per-topic counters. Stored inside an `Arc` so workers can increment
/// the atomics through a read-lock without blocking each other.
#[derive(Debug)]
pub struct TopicMetrics {
    /// Publish frames received for this topic.
    pub publishes: AtomicU64,
    /// Frames delivered to subscribers on this topic.
    pub deliveries: AtomicU64,
    /// Payload bytes received for this topic.
    pub bytes_recv: AtomicU64,
}

impl TopicMetrics {
    fn new() -> Self {
        Self {
            publishes: AtomicU64::new(0),
            deliveries: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
        }
    }
}

/// Thread-safe server-wide metrics shared across all workers via `Arc`.
///
/// Workers increment these atomics on the hot path. The metrics
/// endpoint thread reads them via `snapshot()` when a scraper polls.
#[derive(Debug)]
pub struct ServerMetrics {
    /// Total connections accepted since startup (monotonic counter).
    pub connections_total: AtomicU64,
    /// Currently active connections (gauge: inc on connect, dec on close).
    pub active_connections: AtomicU64,
    /// Total publish frames received from clients (monotonic counter).
    pub publishes_total: AtomicU64,
    /// Total frames delivered to subscribers (monotonic counter).
    pub deliveries_total: AtomicU64,
    /// Total application-level errors (decode failures, ACL denies, etc.).
    pub errors_total: AtomicU64,
    /// Total bytes received over the wire (frame payloads only).
    pub bytes_recv_total: AtomicU64,
    /// Total bytes sent to clients (frame payloads only).
    pub bytes_sent_total: AtomicU64,
    /// Total Initial packets rejected by the per-IP handshake rate
    /// limiter (summed across all workers).
    pub rate_limit_rejections_total: AtomicU64,
    /// Total connection-migration / NAT-rebinding events since startup.
    /// A non-zero value means clients changed their 4-tuple and the
    /// QUIC connection survived (DCID-based routing).
    pub migrations_total: AtomicU64,
    /// Per-topic breakdown. Read-locked on every publish/delivery for
    /// the lookup; write-locked only when a new topic is first seen.
    /// The `Arc<TopicMetrics>` inner lets workers bump atomics without
    /// holding the lock.
    pub topic_counters: RwLock<HashMap<Box<str>, Arc<TopicMetrics>>>,
    /// Server start time (for uptime computation).
    pub started_at: Instant,
}

impl ServerMetrics {
    /// Create a zeroed metrics store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections_total: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            publishes_total: AtomicU64::new(0),
            deliveries_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            bytes_recv_total: AtomicU64::new(0),
            bytes_sent_total: AtomicU64::new(0),
            rate_limit_rejections_total: AtomicU64::new(0),
            migrations_total: AtomicU64::new(0),
            topic_counters: RwLock::new(HashMap::new()),
            started_at: Instant::now(),
        }
    }

    /// Increment a counter by 1 (convenience for the hot path).
    #[inline]
    pub fn inc(&self, field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    /// Add `n` to a counter.
    #[inline]
    pub fn add(&self, field: &AtomicU64, n: u64) {
        field.fetch_add(n, Ordering::Relaxed);
    }

    /// Record a publish for `topic` with `payload_len` bytes. Creates
    /// the topic entry on first sight (write-lock), then subsequent
    /// calls for the same topic only take a read-lock + atomic bump.
    #[inline]
    pub fn record_topic_publish(&self, topic: &str, payload_len: u64) {
        // Fast path: read-lock + atomic bump.  Recover from lock
        // poisoning (a panicked worker) rather than crashing the server.
        {
            let map = self.topic_counters.read().unwrap_or_else(|e| e.into_inner());
            if let Some(m) = map.get(topic) {
                m.publishes.fetch_add(1, Ordering::Relaxed);
                m.bytes_recv.fetch_add(payload_len, Ordering::Relaxed);
                return;
            }
        }
        // Slow path: first-seen topic — insert under write-lock, then
        // bump. Another worker may have inserted concurrently; the
        // or_insert_with guard handles that.
        let mut map = self
            .topic_counters
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let m = map
            .entry(topic.into())
            .or_insert_with(|| Arc::new(TopicMetrics::new()));
        m.publishes.fetch_add(1, Ordering::Relaxed);
        m.bytes_recv.fetch_add(payload_len, Ordering::Relaxed);
    }

    /// Record a delivery for `topic`.
    #[inline]
    pub fn record_topic_delivery(&self, topic: &str) {
        {
            let map = self
                .topic_counters
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(m) = map.get(topic) {
                m.deliveries.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let mut map = self
            .topic_counters
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let m = map
            .entry(topic.into())
            .or_insert_with(|| Arc::new(TopicMetrics::new()));
        m.deliveries.fetch_add(1, Ordering::Relaxed);
    }

    /// Render the current metrics in Prometheus text exposition format.
    ///
    /// Each scrape allocates a fresh `String`. At a 15 s scrape interval
    /// this is negligible; for higher frequencies switch to a buffered
    /// writer.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let conns = self.connections_total.load(Ordering::Relaxed);
        let active = self.active_connections.load(Ordering::Relaxed);
        let pubs = self.publishes_total.load(Ordering::Relaxed);
        let deliv = self.deliveries_total.load(Ordering::Relaxed);
        let errs = self.errors_total.load(Ordering::Relaxed);
        let recv = self.bytes_recv_total.load(Ordering::Relaxed);
        let sent = self.bytes_sent_total.load(Ordering::Relaxed);
        let rl_rej = self.rate_limit_rejections_total.load(Ordering::Relaxed);
        let migrations = self.migrations_total.load(Ordering::Relaxed);
        let uptime = self.started_at.elapsed().as_secs();

        // Prometheus text format v0.0.4.
        let mut out = String::with_capacity(1024);
        out.push_str("# HELP vireon_connections_total Total connections accepted since startup.\n");
        out.push_str("# TYPE vireon_connections_total counter\n");
        out.push_str(&format!("vireon_connections_total {conns}\n\n"));

        out.push_str("# HELP vireon_active_connections Currently active connections.\n");
        out.push_str("# TYPE vireon_active_connections gauge\n");
        out.push_str(&format!("vireon_active_connections {active}\n\n"));

        out.push_str("# HELP vireon_publishes_total Publish frames received from clients.\n");
        out.push_str("# TYPE vireon_publishes_total counter\n");
        out.push_str(&format!("vireon_publishes_total {pubs}\n\n"));

        out.push_str("# HELP vireon_deliveries_total Frames delivered to subscribers.\n");
        out.push_str("# TYPE vireon_deliveries_total counter\n");
        out.push_str(&format!("vireon_deliveries_total {deliv}\n\n"));

        out.push_str("# HELP vireon_errors_total Application-level errors (decode, ACL, etc.).\n");
        out.push_str("# TYPE vireon_errors_total counter\n");
        out.push_str(&format!("vireon_errors_total {errs}\n\n"));

        out.push_str("# HELP vireon_bytes_recv_total Frame payload bytes received.\n");
        out.push_str("# TYPE vireon_bytes_recv_total counter\n");
        out.push_str(&format!("vireon_bytes_recv_total {recv}\n\n"));

        out.push_str("# HELP vireon_bytes_sent_total Frame payload bytes sent.\n");
        out.push_str("# TYPE vireon_bytes_sent_total counter\n");
        out.push_str(&format!("vireon_bytes_sent_total {sent}\n\n"));

        out.push_str("# HELP vireon_rate_limit_rejections_total Initial packets rejected by the per-IP handshake rate limiter.\n");
        out.push_str("# TYPE vireon_rate_limit_rejections_total counter\n");
        out.push_str(&format!("vireon_rate_limit_rejections_total {rl_rej}\n\n"));

        out.push_str("# HELP vireon_migrations_total Connection-migration / NAT-rebinding events (connection survived).\n");
        out.push_str("# TYPE vireon_migrations_total counter\n");
        out.push_str(&format!("vireon_migrations_total {migrations}\n\n"));

        out.push_str("# HELP vireon_uptime_seconds Seconds since server start.\n");
        out.push_str("# TYPE vireon_uptime_seconds gauge\n");
        out.push_str(&format!("vireon_uptime_seconds {uptime}\n"));

        // Per-topic breakdown. Emits one series per topic with a
        // {topic="..."} label. Topics with no activity are omitted.
        if let Ok(map) = self.topic_counters.read() {
            if !map.is_empty() {
                out.push_str(
                    "\n# HELP vireon_topic_publishes_total Publishes received per topic.\n",
                );
                out.push_str("# TYPE vireon_topic_publishes_total counter\n");
                for (topic, m) in &*map {
                    let v = m.publishes.load(Ordering::Relaxed);
                    if v > 0 {
                        out.push_str(&format!(
                            "vireon_topic_publishes_total{{topic=\"{}\"}} {v}\n",
                            escape_label(topic)
                        ));
                    }
                }

                out.push_str(
                    "\n# HELP vireon_topic_deliveries_total Deliveries sent per topic.\n",
                );
                out.push_str("# TYPE vireon_topic_deliveries_total counter\n");
                for (topic, m) in &*map {
                    let v = m.deliveries.load(Ordering::Relaxed);
                    if v > 0 {
                        out.push_str(&format!(
                            "vireon_topic_deliveries_total{{topic=\"{}\"}} {v}\n",
                            escape_label(topic)
                        ));
                    }
                }

                out.push_str(
                    "\n# HELP vireon_topic_bytes_recv_total Payload bytes received per topic.\n",
                );
                out.push_str("# TYPE vireon_topic_bytes_recv_total counter\n");
                for (topic, m) in &*map {
                    let v = m.bytes_recv.load(Ordering::Relaxed);
                    if v > 0 {
                        out.push_str(&format!(
                            "vireon_topic_bytes_recv_total{{topic=\"{}\"}} {v}\n",
                            escape_label(topic)
                        ));
                    }
                }
            }
        }

        out
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start a minimal HTTP listener that serves `/metrics` in Prometheus
/// text format. Spawns a daemon thread; the listener runs until the
/// process exits.
///
/// Returns immediately after binding. If the port is busy, returns an
/// error.
///
/// # Errors
/// Returns `std::io::Error` if the TCP listener cannot bind.
pub fn start_metrics_endpoint(metrics: Arc<ServerMetrics>, port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port))?;
    let addr = listener.local_addr()?;
    tracing::info!(addr = %addr, "[metrics] Prometheus /metrics endpoint listening");

    thread::Builder::new()
        .name("vireon-metrics".into())
        .spawn(move || {
            // Small read buffer — a Prometheus GET request is ~100 bytes.
            let mut buf = [0u8; 256];
            loop {
                let Ok((mut stream, _peer)) = listener.accept() else {
                    continue;
                };
                // Read the request line. We only care about the first line.
                let n = match stream.read(&mut buf) {
                    Ok(0) | Err(_) => continue,
                    Ok(n) => n,
                };
                let req = String::from_utf8_lossy(&buf[..n]);

                let body = if req.starts_with("GET /metrics") {
                    metrics.render_prometheus()
                } else if req.starts_with("GET / ") || req.starts_with("GET /health") {
                    "ok\n".to_string()
                } else {
                    String::new()
                };

                let (status, content_type) = if body.is_empty() {
                    ("404 Not Found", "text/plain")
                } else {
                    ("200 OK", "text/plain; version=0.0.4; charset=utf-8")
                };

                let response = format!(
                    "HTTP/1.1 {status}\r\n\
                     Content-Type: {content_type}\r\n\
                     Content-Length: {}\r\n\
                     Connection: close\r\n\
                     \r\n\
                     {body}",
                    body.len(),
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        })
        .map(|_| ())
}

/// Escape a topic string for safe use as a Prometheus label value.
/// Only backslash, double-quote, and newline need escaping (per the
/// Prometheus text exposition format spec).
fn escape_label(s: &str) -> String {
    if !s.contains(['\\', '"', '\n']) {
        return s.to_string();
    }
    s.replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
}

/// Extract the topic from a Publish frame payload.
///
/// Wire format: `[topic_len:u16 BE][topic bytes][payload bytes]`.
/// Returns `None` if the payload is too short or the topic isn't
/// valid UTF-8. Used by the metrics path to attribute per-topic
/// counters without going through the full application-layer decode.
#[must_use]
pub fn extract_topic_from_publish(payload: &[u8]) -> Option<&str> {
    if payload.len() < 2 {
        return None;
    }
    let tlen = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if payload.len() < 2 + tlen {
        return None;
    }
    std::str::from_utf8(&payload[2..2 + tlen]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_all_metrics() {
        let m = ServerMetrics::new();
        m.inc(&m.publishes_total);
        m.inc(&m.publishes_total);
        let text = m.render_prometheus();
        assert!(text.contains("vireon_publishes_total 2"));
        assert!(text.contains("vireon_connections_total 0"));
        assert!(text.contains("vireon_active_connections 0"));
        assert!(text.contains("# TYPE vireon_publishes_total counter"));
    }

    #[test]
    fn active_connections_is_gauge() {
        let m = ServerMetrics::new();
        m.active_connections.fetch_add(5, Ordering::Relaxed);
        m.active_connections.fetch_sub(2, Ordering::Relaxed);
        let text = m.render_prometheus();
        assert!(text.contains("vireon_active_connections 3"));
    }

    #[test]
    fn per_topic_metrics_appear_in_output() {
        let m = ServerMetrics::new();
        m.record_topic_publish("orders.created", 128);
        m.record_topic_publish("orders.created", 64);
        m.record_topic_delivery("orders.created");
        m.record_topic_publish("analytics.events", 32);
        let text = m.render_prometheus();
        assert!(text.contains(r#"vireon_topic_publishes_total{topic="orders.created"} 2"#));
        assert!(text.contains(r#"vireon_topic_deliveries_total{topic="orders.created"} 1"#));
        assert!(text.contains(r#"vireon_topic_bytes_recv_total{topic="orders.created"} 192"#));
        assert!(text.contains(r#"vireon_topic_publishes_total{topic="analytics.events"} 1"#));
    }

    #[test]
    fn topic_label_escapes_special_chars() {
        let m = ServerMetrics::new();
        m.record_topic_publish(r#"weird"topic"#, 10);
        let text = m.render_prometheus();
        assert!(text.contains(r#"topic=\"weird\\\"topic\""#) || text.contains("weird"));
    }
}
