//! Thread-per-core mio reactor — the event loop that drives all
//! connections assigned to this reactor thread.
//!
//! Each reactor owns a [`mio::Poll`] and a slab of [`ConnState`] entries.
//! The reactor sleeps on `epoll_wait` until sockets are readable, processes
//! inbound data, then ticks every connection's send logic. This replaces
//! the previous thread-per-connection + 1 ms-timeout polling model that
//! produced inflated syscall counts and suffered from thread
//! oversubscription at high connection counts.

use std::net::SocketAddr;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::conn_state::{BenchMode, ConnState, EgressPolicy, Phase};
use crate::stats::WorkerResult;
use crate::workload::WorkloadEntry;

/// Per-worker stagger delay before sending the Initial packet.
const STAGGER_PER_WORKER: Duration = Duration::from_micros(200);
/// Handshake deadline.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(30);
/// Poll timeout — bounds the time between shutdown checks.
const POLL_TIMEOUT: Duration = Duration::from_millis(100);
/// Warmup delay for group/broadcast modes — gives subscribers time to
/// register before the publisher starts streaming. Prevents the race
/// where InterWorkerPublish fan-out arrives before the subscription.
const WARMUP_DELAY: Duration = Duration::from_millis(500);
/// mio event buffer capacity.
const EVENTS_CAPACITY: usize = 1024;

/// Runs one reactor thread's lifecycle: setup → handshake → measurement → close.
///
/// Returns one [`WorkerResult`] per connection assigned to this reactor.
#[allow(clippy::too_many_arguments)]
pub fn run(
    target: SocketAddr,
    sni: &str,
    entries: &[WorkloadEntry],
    conn_start: usize,
    conn_count: usize,
    mode: BenchMode,
    pipeline_depth: usize,
    egress_policy: EgressPolicy,
    coalesce: usize,
    duration: Duration,
    shutdown: &AtomicBool,
    barrier: &Barrier,
) -> Vec<WorkerResult> {
    match run_inner(
        target,
        sni,
        entries,
        conn_start,
        conn_count,
        mode,
        pipeline_depth,
        egress_policy,
        coalesce,
        duration,
        shutdown,
        barrier,
    ) {
        Ok(results) => results,
        Err(e) => {
            tracing::error!("reactor fatal error: {e:#}");
            (0..conn_count)
                .map(|_| WorkerResult::failed(format!("reactor: {e:#}"), Duration::ZERO))
                .collect()
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_inner(
    target: SocketAddr,
    sni: &str,
    entries: &[WorkloadEntry],
    conn_start: usize,
    conn_count: usize,
    mode: BenchMode,
    pipeline_depth: usize,
    egress_policy: EgressPolicy,
    coalesce: usize,
    duration: Duration,
    shutdown: &AtomicBool,
    barrier: &Barrier,
) -> Result<Vec<WorkerResult>> {
    if conn_count == 0 {
        barrier.wait();
        return Ok(Vec::new());
    }

    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(EVENTS_CAPACITY);

    // ── Phase 1: Setup — create connections with stagger ───────────
    let mut conns: Vec<Option<ConnState>> = Vec::with_capacity(conn_count);

    for local_idx in 0..conn_count {
        if local_idx > 0 {
            std::thread::sleep(STAGGER_PER_WORKER);
        }
        let global_idx = conn_start + local_idx;
        let token = mio::Token(local_idx);

        let mut config = crate::config::build_client_config()?;
        match ConnState::new(
            target,
            sni,
            &mut config,
            mode,
            global_idx,
            pipeline_depth,
            egress_policy,
            entries,
            coalesce,
            &poll,
            token,
        ) {
            Ok(mut conn) => {
                // Send Initial packet
                conn.flush_outbound();
                conns.push(Some(conn));
            }
            Err(e) => {
                tracing::warn!(global_idx, "connection setup failed: {e}");
                conns.push(None);
            }
        }
    }

    // ── Phase 2: Handshake ─────────────────────────────────────────
    let hs_deadline = Instant::now() + HANDSHAKE_DEADLINE;
    loop {
        poll.poll(&mut events, Some(POLL_TIMEOUT))?;

        for event in events.iter() {
            let idx = event.token().0;
            if let Some(conn) = conns[idx].as_mut() {
                if matches!(conn.phase, Phase::Handshake) {
                    conn.recv_inbound();
                    conn.flush_outbound();
                    if conn.is_established() {
                        conn.handshake_done();
                        conn.phase = Phase::Ready;
                    } else if conn.is_closed() || Instant::now() > hs_deadline {
                        conn.handshake_timeout();
                        conn.phase = Phase::Done;
                    }
                }
            }
        }

        // Check if all connections have left the Handshake phase
        let still_handshaking = conns.iter().any(|c| {
            c.as_ref()
                .is_some_and(|c| matches!(c.phase, Phase::Handshake))
        });
        if !still_handshaking {
            break;
        }
    }

    // Release the barrier — all reactors + main thread synchronize here.
    barrier.wait();

    // Warmup: let subscribers' Subscribe frames reach the server and be
    // registered before the publisher starts streaming. Without this,
    // multi-worker mode has a race where the publisher's InterWorkerPublish
    // fan-out arrives at subscriber workers before their local registry
    // has the subscription, causing 0-delivery runs.
    if matches!(mode, BenchMode::Broadcast | BenchMode::Group) {
        let warmup_deadline = Instant::now() + WARMUP_DELAY;
        while Instant::now() < warmup_deadline {
            poll.poll(&mut events, Some(POLL_TIMEOUT))?;
            for event in events.iter() {
                let idx = event.token().0;
                if let Some(conn) = conns[idx].as_mut() {
                    if matches!(conn.phase, Phase::Ready) {
                        conn.recv_inbound();
                        conn.drain_and_process(entries.len());
                        // tick with a zero-future deadline: subscribers
                        // send their Subscribe frame (no deadline check),
                        // publishers immediately return (deadline passed).
                        conn.tick(entries, coalesce, Instant::now());
                        conn.flush_outbound();
                    }
                }
            }
        }
    }

    // ── Phase 3: Measurement ───────────────────────────────────────
    let deadline = Instant::now() + duration;
    // Transition all ready connections to Measuring.
    for conn in conns.iter_mut().flatten() {
        if matches!(conn.phase, Phase::Ready) {
            conn.phase = Phase::Measuring { deadline };
        }
    }

    while !shutdown.load(Ordering::Relaxed) && Instant::now() < deadline {
        poll.poll(&mut events, Some(POLL_TIMEOUT))?;

        // Process readable events — recv frees flow-control credit,
        // clearing send_blocked so the connection is ticked again.
        for event in events.iter() {
            let idx = event.token().0;
            if let Some(conn) = conns[idx].as_mut() {
                if matches!(conn.phase, Phase::Measuring { .. }) {
                    conn.recv_inbound();
                    conn.drain_and_process(entries.len());
                    conn.flush_outbound();
                }
            }
        }

        // Fairness sweep — tick all measuring connections
        for conn in conns.iter_mut().flatten() {
            if let Phase::Measuring { deadline: dl } = conn.phase {
                if Instant::now() < dl {
                    conn.tick(entries, coalesce, dl);
                    conn.flush_outbound();
                } else {
                    conn.phase = Phase::Done;
                }
            }
        }
    }

    // ── Phase 4: Close + collect ───────────────────────────────────
    let mut results = Vec::with_capacity(conn_count);
    for conn_opt in conns {
        match conn_opt {
            Some(mut conn) => {
                conn.close();
                conn.deregister(&poll);
                results.push(conn.into_result());
            }
            None => {
                results.push(WorkerResult::failed(
                    "connection setup failed".into(),
                    Duration::ZERO,
                ));
            }
        }
    }

    Ok(results)
}
