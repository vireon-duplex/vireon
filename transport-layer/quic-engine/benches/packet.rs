//! Benchmark: packet routing, parse, and coalescing.
//!
//! Phase 2 will populate this. Targets:
//! - Header-form decode (single byte)
//! - Long-header dispatch (Initial / Handshake / 0-RTT / VN)
//! - Short-header CID lookup
//! - Coalesced-packet iteration

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_packet_route(c: &mut Criterion) {
    // TODO(phase-2): real packet samples via fixtures, measure
    // routing + coalescing + quiche::Connection::recv path.
    let _g = c.benchmark_group("packet_route");
}

criterion_group!(benches, bench_packet_route);
criterion_main!(benches);
