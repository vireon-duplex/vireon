//! Benchmark: CpuTopology::discover latency.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use criterion::{Criterion, criterion_group, criterion_main};

use kernel_nic::CpuTopology;

fn bench_topology_discover(c: &mut Criterion) {
    let mut g = c.benchmark_group("topology");
    g.bench_function("discover", |b| {
        b.iter(|| {
            let topo = CpuTopology::discover().expect("discover");
            let _ = topo.thread_count();
        });
    });
    g.finish();
}

fn bench_topology_lookup(c: &mut Criterion) {
    let topo = CpuTopology::discover().expect("discover");
    let cpus: Vec<u32> = (0..topo.thread_count() as u32).collect();
    let mut g = c.benchmark_group("topology_lookup");
    g.bench_function("cpu_lookup", |b| {
        b.iter(|| {
            for &cpu in &cpus {
                let _ = topo.cpu(cpu);
                let _ = topo.numa_node_of(cpu);
                let _ = topo.smt_sibling_of(cpu);
            }
        });
    });
    g.finish();
}

criterion_group!(benches, bench_topology_discover, bench_topology_lookup);
criterion_main!(benches);
