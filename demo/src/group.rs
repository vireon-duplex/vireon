//! Consumer group demo: 1 publisher → N group members (load-balanced).
//!
//! Unlike `fanout` (broadcast), each message goes to exactly ONE group
//! member — Kafka-style consumer group semantics. The server distributes
//! messages round-robin across active members.
//!
//! Default: 5 members, 1000 jobs. Verifies sum == total and fair distribution.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use tokio::time::sleep;
use vireon_sdk::GroupSubscription;

use crate::common;

const TOPIC: &str = "jobs.tasks";
const GROUP: &str = "workers";
const PAYLOAD_LEN: usize = 16_384;
const TOTAL_JOBS: u64 = 20_000;
const MEMBER_READY: Duration = Duration::from_millis(500);
const DRAIN_IDLE: Duration = Duration::from_millis(500);
const DRAIN_DEADLINE: Duration = Duration::from_secs(60);

/// Drain result for one group member.
struct MemberResult {
    count: u64,
    bytes: u64,
    latencies: Vec<u64>,
}

/// Drain a group subscription. Exits early when no messages arrive for
/// DRAIN_IDLE. Tracks real job count, bytes, and end-to-end latency.
async fn drain_member(sub: &mut GroupSubscription) -> MemberResult {
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut latencies: Vec<u64> = Vec::new();
    let deadline = Instant::now() + DRAIN_DEADLINE;
    loop {
        match tokio::time::timeout(DRAIN_IDLE, sub.recv()).await {
            Ok(Some(msg)) => {
                bytes += msg.payload.len() as u64;
                if let Some((_seq, pub_ns)) = common::decode_payload(&msg.payload) {
                    let latency = common::now_nanos().saturating_sub(pub_ns);
                    latencies.push(latency);
                }
                count += 1;
            }
            Err(_) => break,
            Ok(None) => break,
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    MemberResult {
        count,
        bytes,
        latencies,
    }
}

pub async fn run(workers: usize, members: usize) {
    println!("\n========================================================================");
    println!("  Vireon Consumer Group Demo");
    println!("  1 publisher -> {members} group members | {TOTAL_JOBS} jobs (load-balanced)");
    println!("========================================================================\n");

    let (cert, key) = common::write_dev_cert();
    let port = common::ephemeral_port();
    let addr = format!("127.0.0.1:{port}");

    println!("[setup] starting quic-server ({workers} worker(s)) on {addr}");
    let _server = common::ServerGuard::start(port, &cert, &key, workers);
    sleep(Duration::from_millis(500)).await;

    // ── M group member connections ───────────────────────────────────

    println!("[setup] connecting {members} group members to group \"{GROUP}\" (HT flow control)...");
    let mut member_clients = Vec::new();
    let mut group_subs = Vec::new();
    for i in 0..members {
        let c = common::connect_ready_ht(&addr).await;
        let consumer_id = format!("c{i}");
        let gs = c
            .subscribe_group(TOPIC, GROUP, &consumer_id)
            .await
            .expect("subscribe_group");
        member_clients.push(c);
        group_subs.push(gs);
    }
    sleep(MEMBER_READY).await;
    println!("[setup] {members} members joined group \"{GROUP}\" on topic \"{TOPIC}\"");

    // ── Publisher ────────────────────────────────────────────────────

    println!("[setup] connecting publisher (HT flow control)...\n");
    let pub_client = common::connect_ready_ht(&addr).await;

    // ── Publish jobs with embedded timestamps ────────────────────────

    println!(">>> Publishing {TOTAL_JOBS} jobs (each goes to exactly ONE member)...\n");

    let mut buf = vec![0u8; PAYLOAD_LEN];
    let pub_start = Instant::now();
    for n in 0..TOTAL_JOBS {
        common::encode_payload(&mut buf, n, common::now_nanos());
        pub_client
            .publish(TOPIC, &buf[..])
            .await
            .expect("publish");
    }
    let pub_elapsed = pub_start.elapsed();
    let pub_mibs = (TOTAL_JOBS as usize * PAYLOAD_LEN) as f64
        / pub_elapsed.as_secs_f64()
        / (1024.0 * 1024.0);
    println!(
        "  published {TOTAL_JOBS} jobs in {:.2}s ({pub_mibs:.1} MiB/s)",
        pub_elapsed.as_secs_f64()
    );

    // ── Drain + verify ───────────────────────────────────────────────

    println!("\n>>> Draining {members} members (verifying load-balanced distribution)...\n");

    let drain_start = Instant::now();
    let mut handles = Vec::new();
    for (i, mut gs) in group_subs.into_iter().enumerate() {
        handles.push(tokio::spawn(async move {
            let res = drain_member(&mut gs).await;
            (i, res)
        }));
    }

    let mut counts = Vec::new();
    let mut total_delivered = 0u64;
    let mut total_bytes = 0u64;
    let mut all_latencies = common::LatencyStats::default();
    for h in handles {
        let (i, res) = h.await.expect("drain task");
        counts.push(res.count);
        total_delivered += res.count;
        total_bytes += res.bytes;
        let member_min = res.latencies.iter().copied().min().map(common::fmt_latency).unwrap_or("-".into());
        println!(
            "  member c{i}: {:>5} jobs | {} | min latency {member_min}",
            res.count,
            common::fmt_bytes(res.bytes),
        );
        for ns in res.latencies {
            all_latencies.record(ns);
        }
    }
    let drain_elapsed = drain_start.elapsed();
    let e2e_secs = pub_start.elapsed().as_secs_f64().max(0.001);

    let min_count = counts.iter().copied().min().unwrap_or(0);
    let max_count = counts.iter().copied().max().unwrap_or(0);
    let all_got_work = min_count > 0;
    let no_loss = total_delivered == TOTAL_JOBS;
    let ok = all_got_work && no_loss;

    // ── Summary ──────────────────────────────────────────────────────

    println!("\n========================================================================");
    println!("  Summary");
    println!("------------------------------------------------------------------------");
    println!("  Topology:       1 publisher -> {members} group members");
    println!("  Jobs published: {TOTAL_JOBS}");
    println!("  Jobs delivered: {total_delivered} (sum across all members)");
    println!("  Distribution:   min {min_count} | max {max_count} | ideal ~{}", TOTAL_JOBS / members as u64);
    println!(
        "  Throughput:     {:.1} MiB/s ({:.0} jobs/s, end-to-end)",
        total_bytes as f64 / (1024.0 * 1024.0) / e2e_secs,
        total_delivered as f64 / e2e_secs,
    );
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
    if ok {
        println!("  Result:         LOAD-BALANCED: sum={total_delivered}, all members got work");
    } else if !no_loss {
        println!("  Result:         FAIL: lost {} jobs", TOTAL_JOBS.saturating_sub(total_delivered));
    } else if !all_got_work {
        println!("  Result:         FAIL: at least one member got 0 jobs (unfair)");
    }
    println!("========================================================================\n");

    let _ = pub_client.close().await;
    for c in &member_clients {
        let _ = c.close().await;
    }
}
