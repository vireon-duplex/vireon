//! Cluster cross-node fan-out demo: N server nodes, subscribers on every
//! node, publisher on node 1.
//!
//! Proves that when a topic has subscribers spread across multiple cluster
//! nodes, the owner node routes deliveries to every subscriber regardless
//! of which node they're on. Every node receives every message.
//!
//! Default: 3-node cluster, replication=2, 2000 messages.
//!
//! Mirrors the pattern from `client-sdk/examples/s18_cross_node_fanout.rs`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use tokio::time::sleep;
use vireon_sdk::Subscription;

use crate::common;

const TOPIC: &str = "cluster.fanout";
const PAYLOAD_LEN: usize = 8_192;
const TOTAL_MSGS: u64 = 2000;
const REPLICATION: u8 = 2;
const SUB_PROPAGATION: Duration = Duration::from_millis(2000);
const DRAIN_DEADLINE: Duration = Duration::from_secs(15);
const MAX_START_RETRIES: u32 = 4;

/// Drain result for one node.
struct DrainResult {
    delivered: u64,
    gaps: u64,
    dups: u64,
    bytes: u64,
    latencies: Vec<u64>,
}

/// Drain a subscriber, tracking real delivered count, bytes, latency.
async fn drain_verify(sub: &mut Subscription, expected: u64) -> DrainResult {
    let mut ids: Vec<u64> = Vec::with_capacity(expected as usize);
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
                    if ids.contains(&seq) {
                        dups += 1;
                    } else {
                        ids.push(seq);
                    }
                    if ids.len() as u64 >= expected {
                        break;
                    }
                }
            }
            _ => break,
        }
    }
    ids.sort_unstable();
    ids.dedup();
    let gaps = expected.saturating_sub(ids.len() as u64);
    DrainResult {
        delivered: ids.len() as u64,
        gaps,
        dups,
        bytes,
        latencies,
    }
}

pub async fn run(workers: usize, nodes: usize) {
    println!("\n========================================================================");
    println!("  Vireon Cluster Cross-Node Fan-out Demo");
    println!("  {nodes} server nodes | 1 publisher -> subscribers on every node");
    println!("  replication factor = {REPLICATION}");
    println!("========================================================================\n");

    let (cert, key) = common::write_dev_cert();

    // ── Spawn N-node cluster with ghost-socket retry ─────────────────
    //
    // Ghost UDP sockets from prior runs can cause a node to fail binding.
    // We detect this by checking process liveness after startup and retry
    // with fresh ports.

    let _guards: Vec<common::ServerGuard>;
    let mut quic_ports: Vec<u16> = Vec::new();
    let mut started = false;

    'outer: for attempt in 0..MAX_START_RETRIES {
        // Pick fresh ports each attempt.
        quic_ports = (0..nodes).map(|_| common::ephemeral_port()).collect();
        let cluster_ports: Vec<u16> = (0..nodes).map(|_| common::ephemeral_port()).collect();

        let peers = (1..=nodes)
            .map(|i| {
                format!("{i}=127.0.0.1:{}", cluster_ports[i - 1])
            })
            .collect::<Vec<_>>()
            .join(",");
        let peers_leaked = peers.leak();
        let workers_str = workers.to_string().leak();
        let repl_str = REPLICATION.to_string().leak();

        let mut new_guards = Vec::new();
        for n in 1..=nodes {
            let node_id_str = n.to_string().leak();
            let args: &[&str] = &[
                "--cluster-node-id",
                node_id_str,
                "--cluster-peers",
                peers_leaked,
                "--cluster-replication-factor",
                repl_str,
                "--workers",
                workers_str,
            ];
            let g = common::ServerGuard::start_with(quic_ports[n - 1], &cert, &key, args);
            new_guards.push(g);
        }

        // Wait for servers to bind (or detect early exit).
        sleep(Duration::from_millis(800)).await;

        let all_alive = new_guards.iter_mut().all(|g| g.is_alive());
        if all_alive {
            // Extra warmup for cluster mesh formation.
            sleep(Duration::from_millis(1000)).await;
            print!("[cluster] {nodes} nodes started (attempt {}):", attempt + 1);
            for (i, p) in quic_ports.iter().enumerate() {
                print!(" node{}=127.0.0.1:{}", i + 1, p);
            }
            println!();
            _guards = new_guards;
            started = true;
            break 'outer;
        }

        eprintln!(
            "[cluster] attempt {}: a node exited early (likely ghost-socket bind), retrying...",
            attempt + 1
        );
        drop(new_guards);

        if attempt + 1 < MAX_START_RETRIES {
            sleep(Duration::from_secs(2)).await;
        }
    }

    if !started {
        panic!("cluster failed to start after {MAX_START_RETRIES} attempts");
    }

    // ── Subscribers on all N nodes ────────────────────────────────────

    println!("\n[setup] connecting subscribers to all {nodes} nodes...");
    let mut sub_clients = Vec::new();
    let mut subs: Vec<Subscription> = Vec::new();
    for port in quic_ports.iter() {
        let addr = format!("127.0.0.1:{port}");
        let c = common::connect_ready(&addr).await;
        let s = c.subscribe(TOPIC).await.expect("subscribe");
        sub_clients.push(c);
        subs.push(s);
    }
    println!("[setup] {nodes} subscribers connected (one per node)");

    // Wait for Subscribe frames to propagate via inter-node UDP mesh.
    sleep(SUB_PROPAGATION).await;

    // ── Publisher on node 1 ──────────────────────────────────────────

    let pub_addr = format!("127.0.0.1:{}", quic_ports[0]);
    let pub_client = common::connect_ready(&pub_addr).await;
    println!("[setup] publisher connected to node 1 ({pub_addr})");

    // ── Publish ──────────────────────────────────────────────────────

    println!("\n>>> Publishing {TOTAL_MSGS} messages ({PAYLOAD_LEN} B each) to node 1...\n");

    let mut buf = vec![0u8; PAYLOAD_LEN];
    let pub_start = Instant::now();
    for n in 0..TOTAL_MSGS {
        common::encode_payload(&mut buf, n, common::now_nanos());
        pub_client
            .publish(TOPIC, &buf[..])
            .await
            .expect("publish");
    }
    let pub_elapsed = pub_start.elapsed();
    let pub_mibs = (TOTAL_MSGS as usize * PAYLOAD_LEN) as f64
        / pub_elapsed.as_secs_f64()
        / (1024.0 * 1024.0);
    println!(
        "  published {TOTAL_MSGS} frames in {:.2}s ({pub_mibs:.1} MiB/s)",
        pub_elapsed.as_secs_f64()
    );

    // ── Drain + verify each node concurrently ────────────────────────

    println!("\n>>> Draining {nodes} subscribers (verifying each node received all {TOTAL_MSGS})...\n");

    let drain_start = Instant::now();
    let mut handles = Vec::new();
    for (i, mut sub) in subs.into_iter().enumerate() {
        handles.push(tokio::spawn(async move {
            let res = drain_verify(&mut sub, TOTAL_MSGS).await;
            (i, res)
        }));
    }

    let mut all_ok = true;
    let mut total_delivered = 0u64;
    let mut total_bytes = 0u64;
    let mut all_latencies = common::LatencyStats::default();
    for h in handles {
        let (i, res) = h.await.expect("drain task");
        let ok = res.delivered == TOTAL_MSGS && res.gaps == 0 && res.dups == 0;
        let mark = if ok { "OK" } else { "FAIL" };
        println!(
            "  node {}: {:>5} / {} msgs | {} | {} gaps, {} dups [{mark}]",
            i + 1,
            res.delivered,
            TOTAL_MSGS,
            common::fmt_bytes(res.bytes),
            res.gaps,
            res.dups,
        );
        if !ok {
            all_ok = false;
        }
        total_delivered += res.delivered;
        total_bytes += res.bytes;
        for ns in res.latencies {
            all_latencies.record(ns);
        }
    }
    let drain_elapsed = drain_start.elapsed();
    let e2e_secs = pub_start.elapsed().as_secs_f64().max(0.001);

    // ── Summary ──────────────────────────────────────────────────────

    println!("\n========================================================================");
    println!("  Summary");
    println!("------------------------------------------------------------------------");
    println!("  Cluster nodes:    {nodes} (replication={REPLICATION})");
    println!("  Messages sent:    {TOTAL_MSGS} (cross-node fan-out: every node gets all)");
    println!(
        "  Aggregate delivered (measured): {} messages | {}",
        total_delivered,
        common::fmt_bytes(total_bytes)
    );
    println!(
        "  Delivery throughput:  {:.1} MiB/s ({:.0} msg/s aggregate across {nodes} nodes, end-to-end)",
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
    println!("  Drain time:       {:.2}s", drain_elapsed.as_secs_f64());
    println!("  Workers/node:     {workers}");
    if all_ok {
        println!("  Result:           ALL {nodes} NODES RECEIVED ALL {TOTAL_MSGS} MESSAGES");
    } else {
        println!("  Result:           VERIFICATION FAILED");
    }
    println!("========================================================================\n");

    let _ = pub_client.close().await;
    for c in &sub_clients {
        let _ = c.close().await;
    }
}
