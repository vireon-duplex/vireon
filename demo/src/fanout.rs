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
const PAYLOAD_LEN: usize = 1024;
const MSGS_PER_PUBLISHER: u64 = 1000;
const SUBSCRIBE_PROPAGATION: Duration = Duration::from_millis(500);
const DRAIN_DEADLINE: Duration = Duration::from_secs(15);

/// Drain a subscriber, collecting received sequence IDs from the first
/// 8 bytes of each payload. Returns (delivered, duplicates).
async fn drain_sub(sub: &mut Subscription, expected: u64) -> (u64, u64) {
    let mut ids: Vec<u64> = Vec::with_capacity(expected as usize);
    let mut dups = 0u64;
    let deadline = Instant::now() + DRAIN_DEADLINE;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, sub.recv()).await {
            Ok(Some(msg)) if msg.payload.len() >= 8 => {
                let mut b = [0u8; 8];
                b.copy_from_slice(&msg.payload[..8]);
                let id = u64::from_be_bytes(b);
                if ids.contains(&id) {
                    dups += 1;
                } else {
                    ids.push(id);
                }
                if ids.len() as u64 >= expected {
                    break;
                }
            }
            _ => break,
        }
    }
    (ids.len() as u64, dups)
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

    println!("[setup] connecting {subscribers} subscribers...");
    let mut sub_clients = Vec::new();
    let mut subs = Vec::new();
    for _ in 0..subscribers {
        let c = common::connect_ready(&addr).await;
        let s = c.subscribe(TOPIC).await.expect("subscribe");
        sub_clients.push(c);
        subs.push(s);
    }
    // Let subscriptions propagate before publishing.
    sleep(SUBSCRIBE_PROPAGATION).await;
    println!("[setup] all {subscribers} subscribers subscribed to \"{TOPIC}\"");

    // ── M publisher connections ──────────────────────────────────────

    println!("[setup] connecting {publishers} publisher(s)...\n");
    let mut pub_clients = Vec::new();
    for _ in 0..publishers {
        pub_clients.push(common::connect_ready(&addr).await);
    }

    // ── Publish ──────────────────────────────────────────────────────

    println!(">>> Publishing {total_msgs} messages ({MSGS_PER_PUBLISHER} per publisher)...\n");

    let mut buf = vec![0u8; PAYLOAD_LEN];
    let pub_start = Instant::now();
    let mut global_seq = 0u64;
    for (pi, client) in pub_clients.iter().enumerate() {
        for _ in 0..MSGS_PER_PUBLISHER {
            buf[..8].copy_from_slice(&global_seq.to_be_bytes());
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
            let (delivered, dups) = drain_sub(&mut sub, total_msgs).await;
            (i, delivered, dups)
        }));
    }

    let mut all_ok = true;
    let mut min_delivered = u64::MAX;
    let mut max_delivered = 0u64;
    for h in handles {
        let (i, delivered, dups) = h.await.expect("drain task");
        let ok = delivered == total_msgs && dups == 0;
        let mark = if ok { "OK" } else { "FAIL" };
        println!(
            "  subscriber {i:>2}: {delivered:>5} / {total_msgs} messages ({dups} dups) [{mark}]"
        );
        if !ok {
            all_ok = false;
        }
        min_delivered = min_delivered.min(delivered);
        max_delivered = max_delivered.max(delivered);
    }
    let drain_elapsed = drain_start.elapsed();

    // ── Summary ──────────────────────────────────────────────────────

    println!("\n========================================================================");
    println!("  Summary");
    println!("------------------------------------------------------------------------");
    println!("  Topology:       {publishers} publisher(s) -> {subscribers} subscriber(s)");
    println!("  Messages sent:  {total_msgs} (broadcast: each subscriber should get all)");
    println!(
        "  Per-subscriber: min {min_delivered} | max {max_delivered} | expected {total_msgs}"
    );
    let aggregate = max_delivered * subscribers as u64;
    println!(
        "  Aggregate delivery: {} ({})",
        aggregate,
        common::fmt_bytes(aggregate * PAYLOAD_LEN as u64)
    );
    println!(
        "  Drain time:     {:.2}s",
        drain_elapsed.as_secs_f64()
    );
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
