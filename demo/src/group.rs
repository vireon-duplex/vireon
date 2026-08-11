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
const PAYLOAD_LEN: usize = 256;
const TOTAL_JOBS: u64 = 1000;
const MEMBER_READY: Duration = Duration::from_millis(500);
const DRAIN_DEADLINE: Duration = Duration::from_secs(15);

/// Drain a group subscription. Exits early when no messages arrive for
/// IDLE_TIMEOUT (so the demo doesn't wait the full DRAIN_DEADLINE once
/// delivery is done).
async fn drain_member(sub: &mut GroupSubscription) -> u64 {
    let mut count = 0u64;
    let deadline = Instant::now() + DRAIN_DEADLINE;
    loop {
        match tokio::time::timeout(Duration::from_millis(500), sub.recv()).await {
            Ok(Some(_)) => count += 1,
            Err(_) => break, // 500ms idle — assume delivery is done
            Ok(None) => break,
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    count
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

    println!("[setup] connecting {members} group members to group \"{GROUP}\"...");
    let mut member_clients = Vec::new();
    let mut group_subs = Vec::new();
    for i in 0..members {
        let c = common::connect_ready(&addr).await;
        let consumer_id = format!("c{i}");
        let gs = c
            .subscribe_group(TOPIC, GROUP, &consumer_id)
            .await
            .expect("subscribe_group");
        member_clients.push(c);
        group_subs.push(gs);
    }
    // Let heartbeats register all members before publishing.
    sleep(MEMBER_READY).await;
    println!("[setup] {members} members joined group \"{GROUP}\" on topic \"{TOPIC}\"");

    // ── Publisher ────────────────────────────────────────────────────

    println!("[setup] connecting publisher...\n");
    let pub_client = common::connect_ready(&addr).await;

    // ── Publish jobs ─────────────────────────────────────────────────

    println!(">>> Publishing {TOTAL_JOBS} jobs (each goes to exactly ONE member)...\n");

    let payload = vec![0u8; PAYLOAD_LEN];
    let pub_start = Instant::now();
    for _ in 0..TOTAL_JOBS {
        pub_client
            .publish(TOPIC, &payload[..])
            .await
            .expect("publish");
    }
    let pub_elapsed = pub_start.elapsed();
    println!(
        "  published {TOTAL_JOBS} jobs in {:.2}s",
        pub_elapsed.as_secs_f64()
    );

    // ── Drain + verify ───────────────────────────────────────────────

    println!("\n>>> Draining {members} members (verifying load-balanced distribution)...\n");

    let drain_start = Instant::now();
    let mut handles = Vec::new();
    for (i, mut gs) in group_subs.into_iter().enumerate() {
        handles.push(tokio::spawn(async move {
            let count = drain_member(&mut gs).await;
            (i, count)
        }));
    }

    let mut counts = Vec::new();
    let mut total_delivered = 0u64;
    for h in handles {
        let (i, count) = h.await.expect("drain task");
        counts.push(count);
        total_delivered += count;
        println!("  member c{i}: {count:>5} jobs");
    }
    let drain_elapsed = drain_start.elapsed();

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
        "  Drain time:     {:.2}s",
        drain_elapsed.as_secs_f64()
    );
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
