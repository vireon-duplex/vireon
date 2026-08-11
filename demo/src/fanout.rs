//! Pub/sub fan-out demo: M publishers → N subscribers.
//!
//! Every subscriber receives a copy of every message (broadcast). This is
//! the core pub/sub semantic — contrast with `group` where messages are
//! load-balanced across members.
//!
//! Default: 1 publisher → 10 subscribers, 1000 messages.
//! Scale variant: `--publishers 5 --subscribers 20`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use tokio::time::sleep;
use vireon_sdk::Subscription;

use crate::common;

const TOPIC: &str = "fanout.data";
const PAYLOAD_LEN: usize = 65_536;
const MSGS_PER_PUBLISHER: u64 = 5_000;
const SUBSCRIBE_PROPAGATION: Duration = Duration::from_millis(500);
const DRAIN_DEADLINE: Duration = Duration::from_secs(60);

/// Drain result: delivered count, duplicate count, bytes received, latency samples.
struct DrainResult {
    delivered: u64,
    dups: u64,
    bytes: u64,
    latencies: Vec<u64>,
}

/// Drain a subscriber. Tracks real delivered count, bytes, and end-to-end
/// latency (from embedded publish timestamp to recv time). Uses O(1)
/// `Vec<bool>` duplicate detection indexed by seq.
async fn drain_sub(sub: &mut Subscription, expected: u64) -> DrainResult {
    let mut seen: Vec<bool> = vec![false; expected as usize];
    let mut delivered = 0u64;
    let mut dups = 0u64;
    let mut bytes = 0u64;
    let mut latencies: Vec<u64> = Vec::with_capacity(expected as usize);
    let deadline = Instant::now() + DRAIN_DEADLINE;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, sub.recv()).await {
            Ok(Some(msg)) => {
                bytes += msg.payload.len() as u64;
                if let Some((seq, pub_ns)) = common::decode_payload(&msg.payload) {
                    let latency = common::now_nanos().saturating_sub(pub_ns);
                    latencies.push(latency);
                    let idx = seq as usize;
                    if idx < seen.len() {
                        if seen[idx] {
                            dups += 1;
                        } else {
                            seen[idx] = true;
                            delivered += 1;
                        }
                    }
                    if delivered >= expected {
                        break;
                    }
                }
            }
            _ => break,
        }
    }
    DrainResult {
        delivered,
        dups,
        bytes,
        latencies,
    }
}

pub async fn run(workers: usize, publishers: usize, subscribers: usize) {
    let total_msgs = publishers as u64 * MSGS_PER_PUBLISHER;

    println!("\n========================================================================");
    println!("  Vireon Pub/Sub Fan-out Demo");
    println!("  {publishers} publisher(s) -> {subscribers} subscriber(s) | {total_msgs} messages");
    println!("========================================================================\n");

    let (cert, key) = common::write_dev_cert();
    let port = common::ephemeral_port();
    let addr = format!("127.0.0.1:{port}");

    println!("[setup] starting quic-server ({workers} worker(s)) on {addr}");
    let _server = common::ServerGuard::start(port, &cert, &key, workers);
    sleep(Duration::from_millis(500)).await;

    // ── N subscriber connections ─────────────────────────────────────

    println!("[setup] connecting {subscribers} subscribers (HT flow control)...");
    let mut sub_clients = Vec::new();
    let mut subs = Vec::new();
    for _ in 0..subscribers {
        let c = common::connect_ready_ht(&addr).await;
        let s = c.subscribe(TOPIC).await.expect("subscribe");
        sub_clients.push(c);
        subs.push(s);
    }
    sleep(SUBSCRIBE_PROPAGATION).await;
    println!("[setup] all {subscribers} subscribers subscribed to \"{TOPIC}\"");

    // ── M publisher connections ──────────────────────────────────────

    println!("[setup] connecting {publishers} publisher(s) (HT flow control)...\n");
    let mut pub_clients = Vec::new();
    for _ in 0..publishers {
        pub_clients.push(common::connect_ready_ht(&addr).await);
    }

    // ── Publish with embedded timestamps ─────────────────────────────

    println!(">>> Publishing {total_msgs} messages ({MSGS_PER_PUBLISHER} per publisher)...\n");

    let mut buf = vec![0u8; PAYLOAD_LEN];
    let pub_start = Instant::now();
    let mut global_seq = 0u64;
    for (pi, client) in pub_clients.iter().enumerate() {
        for _ in 0..MSGS_PER_PUBLISHER {
            common::encode_payload(&mut buf, global_seq, common::now_nanos());
            client.publish(TOPIC, &buf[..]).await.expect("publish");
            global_seq += 1;
        }
        println!("  publisher {pi}: sent {MSGS_PER_PUBLISHER} messages");
    }
    let pub_elapsed = pub_start.elapsed();
    let pub_mibs = (total_msgs as usize * PAYLOAD_LEN) as f64
        / pub_elapsed.as_secs_f64()
        / (1024.0 * 1024.0);
    println!(
        "\n  published {total_msgs} messages in {:.2}s ({pub_mibs:.1} MiB/s)",
        pub_elapsed.as_secs_f64()
    );

    // ── Drain + verify ───────────────────────────────────────────────

    println!("\n>>> Draining {subscribers} subscribers (verifying each received all {total_msgs})...\n");

    let drain_start = Instant::now();
    let mut handles = Vec::new();
    for (i, mut sub) in subs.into_iter().enumerate() {
        handles.push(tokio::spawn(async move {
            let res = drain_sub(&mut sub, total_msgs).await;
            (i, res)
        }));
    }

    let mut all_ok = true;
    let mut min_delivered = u64::MAX;
    let mut max_delivered = 0u64;
    let mut total_delivered = 0u64;
    let mut total_bytes = 0u64;
    let mut all_latencies = common::LatencyStats::default();
    for h in handles {
        let (i, res) = h.await.expect("drain task");
        let ok = res.delivered == total_msgs && res.dups == 0;
        let mark = if ok { "OK" } else { "FAIL" };
        let sub_min = res.latencies.iter().copied().min().map(common::fmt_latency).unwrap_or("-".into());
        println!(
            "  subscriber {i:>2}: {:>5} / {total_msgs} msgs | {} | {:>2} dups | min {sub_min} [{mark}]",
            res.delivered,
            common::fmt_bytes(res.bytes),
            res.dups,
        );
        if !ok {
            all_ok = false;
        }
        min_delivered = min_delivered.min(res.delivered);
        max_delivered = max_delivered.max(res.delivered);
        total_delivered += res.delivered;
        total_bytes += res.bytes;
        for ns in res.latencies {
            all_latencies.record(ns);
        }
    }
    let drain_elapsed = drain_start.elapsed();
    // End-to-end time: from first publish to last delivery.
    let e2e_secs = pub_start.elapsed().as_secs_f64().max(0.001);

    // ── Summary ──────────────────────────────────────────────────────

    println!("\n========================================================================");
    println!("  Summary");
    println!("------------------------------------------------------------------------");
    println!("  Topology:       {publishers} publisher(s) -> {subscribers} subscriber(s)");
    println!("  Messages sent:  {total_msgs} (broadcast: each subscriber should get all)");
    println!(
        "  Per-subscriber: min {min_delivered} | max {max_delivered} | expected {total_msgs}"
    );
    println!(
        "  Aggregate delivered (measured):  {} messages | {}",
        total_delivered,
        common::fmt_bytes(total_bytes)
    );
    println!(
        "  Delivery throughput (e2e):  {:.1} MiB/s ({:.0} msg/s aggregate)",
        total_bytes as f64 / (1024.0 * 1024.0) / e2e_secs,
        total_delivered as f64 / e2e_secs,
    );
    if drain_elapsed.as_secs_f64() > 0.001 {
        let peak = total_bytes as f64 / (1024.0 * 1024.0) / drain_elapsed.as_secs_f64();
        println!(
            "  Peak fan-out rate (drain): {:.1} MiB/s — server's raw delivery capacity",
            peak,
        );
    }
    if all_latencies.count() > 0 {
        println!(
            "  Latency (end-to-end): p50 {} | p99 {} | max {}",
            common::fmt_latency(all_latencies.p50().unwrap_or(0)),
            common::fmt_latency(all_latencies.p99().unwrap_or(0)),
            common::fmt_latency(all_latencies.max().unwrap_or(0)),
        );
    }
    println!("  Drain time:     {:.2}s", drain_elapsed.as_secs_f64());
    println!("  Workers:        {workers}");
    if all_ok {
        println!("  Result:         ALL SUBSCRIBERS RECEIVED ALL MESSAGES");
    } else {
        println!("  Result:         VERIFICATION FAILED (some subscribers missed messages)");
    }
    println!("========================================================================\n");

    for c in &pub_clients {
        let _ = c.close().await;
    }
    for c in &sub_clients {
        let _ = c.close().await;
    }
}
