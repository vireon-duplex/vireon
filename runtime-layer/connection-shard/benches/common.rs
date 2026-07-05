//! Shared bench helpers for connection-shard benchmarks.

#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use connection_shard::{ConnectionShardBuilder, ConnectionSpec};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Creates a fresh shard with the given capacity.
pub fn fresh_shard(capacity: usize) -> connection_shard::ConnectionShard {
    ConnectionShardBuilder::new()
        .capacity(capacity)
        .build()
        .expect("shard build")
}

/// Creates a fresh shard with backpressure effectively disabled.
///
/// `soft_bp_pct = 99` and `hard_bp_pct = 100` push Soft to ~99% fill
/// (advisory only — Soft never rejects) and Hard to 100% (unreachable:
/// `ShardFull` is checked first). Used by benchmarks that need to fill
/// the slab to a known level without `RejectedByBackpressure`
/// interfering — e.g. slab alloc / recycle churn, where the metric
/// under test is allocation cost, not admission policy.
pub fn fresh_shard_no_bp(capacity: usize) -> connection_shard::ConnectionShard {
    ConnectionShardBuilder::new()
        .capacity(capacity)
        .soft_bp_pct(99)
        .hard_bp_pct(100)
        .build()
        .expect("shard build")
}

/// Creates a unique spec for stream `n`.
pub fn make_spec(n: u64) -> ConnectionSpec {
    let addr = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(
            10,
            0,
            ((n >> 8) & 0xFF) as u8,
            (n & 0xFF) as u8,
        )),
        443,
    );
    ConnectionSpec::new(n, addr)
}

/// Fills a shard to `fill_pct` of its capacity and returns all created ids.
///
/// **Backpressure caveat**: the caller is responsible for constructing the
/// shard with `soft_bp_pct = 100` and `hard_bp_pct = 100` (see
/// [`fresh_shard_no_bp`]) if `fill_pct` would otherwise trip a default
/// 80% / 95% threshold.
pub fn fill_shard(
    shard: &mut connection_shard::ConnectionShard,
    fill_pct: f64,
) -> Vec<connection_shard::ConnectionId> {
    let cap = shard.capacity();
    let target = ((cap as f64) * fill_pct) as usize;
    let mut ids = Vec::with_capacity(target);
    for i in 0..target {
        let id = shard.create(make_spec(i as u64)).expect("create");
        ids.push(id);
    }
    ids
}
