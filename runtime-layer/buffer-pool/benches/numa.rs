//! NUMA awareness benchmarks (Category 11).
//!
//! **Requires the `numa` feature** — compiles only with `--features numa`.
//!
//! Measures the cost of NUMA-related operations:
//!
//! - **current_node**: `getcpu` syscall latency.
//! - **allocate_node**: `alloc` + `mbind` round-trip for preferred-node memory.
//! - **bind**: `mbind` on an existing page-aligned region.
//! - **migrate**: rebinding pages to a different node.
//!
//! On single-node machines these still run — `current_node` returns `node0`
//! and `bind`/`migrate` are no-ops or best-effort. On multi-socket systems the
//! cross-node access penalty becomes visible.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::print_stdout)]

use std::alloc::{Layout, alloc, dealloc};
use std::hint::black_box;
use std::ptr::NonNull;

use criterion::BatchSize;
use criterion::Criterion;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main};

use buffer_pool::numa::{
    NodeId, allocate_node, bind, current_node, deallocate, is_numa_available, migrate,
};

const PAGE_SIZE: usize = 4096;

/// Allocates `size` bytes aligned to `PAGE_SIZE`.
fn alloc_page_aligned(size: usize) -> (NonNull<u8>, Layout) {
    let layout = Layout::from_size_align(size, PAGE_SIZE).expect("layout");
    let ptr = unsafe { alloc(layout) };
    let nn = NonNull::new(ptr).expect("alloc");
    (nn, layout)
}

// ============================================================================
// current_node — getcpu syscall (Category 11)
// ============================================================================

fn bench_current_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("numa/current_node");
    group.throughput(Throughput::Elements(1));

    group.bench_function("detect", |b| {
        let numa = is_numa_available();
        b.iter(|| {
            let node = current_node();
            black_box(node);
        });
        println!("NUMA available: {numa}, current node: {}", current_node());
    });

    group.finish();
}

// ============================================================================
// allocate_node — alloc + bind (Category 11)
// ============================================================================

fn bench_allocate_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("numa/allocate_node");

    for &size in &[PAGE_SIZE, 64 * PAGE_SIZE, 256 * PAGE_SIZE] {
        group.throughput(Throughput::Bytes(size as u64));
        let label = format!("{}KiB", size / 1024);

        group.bench_function(&label, |b| {
            b.iter_batched(
                || size,
                |size| {
                    let ptr = allocate_node(size, NodeId::SINGLE).expect("alloc");
                    black_box(ptr.as_ptr());
                    // SAFETY: ptr was returned by allocate_node with this size.
                    unsafe {
                        deallocate(ptr, size);
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// bind — mbind on existing memory (Category 11)
// ============================================================================

fn bench_bind(c: &mut Criterion) {
    let mut group = c.benchmark_group("numa/bind");

    for &size in &[PAGE_SIZE, 64 * PAGE_SIZE] {
        group.throughput(Throughput::Bytes(size as u64));
        let label = format!("{}KiB", size / 1024);

        group.bench_function(&label, |b| {
            b.iter_batched(
                || alloc_page_aligned(size),
                |(ptr, layout)| {
                    // SAFETY: ptr is valid, page-aligned, `size` bytes.
                    let _ = unsafe { bind(ptr, size, NodeId::SINGLE) };
                    black_box(ptr.as_ptr());
                    // SAFETY: ptr was allocated with this layout.
                    unsafe {
                        dealloc(ptr.as_ptr(), layout);
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// migrate — rebinding pages (Category 11)
// ============================================================================

fn bench_migrate(c: &mut Criterion) {
    let mut group = c.benchmark_group("numa/migrate");

    for &size in &[PAGE_SIZE, 64 * PAGE_SIZE] {
        group.throughput(Throughput::Bytes(size as u64));
        let label = format!("{}KiB", size / 1024);

        group.bench_function(&label, |b| {
            b.iter_batched(
                || alloc_page_aligned(size),
                |(ptr, layout)| {
                    // SAFETY: page-aligned valid region.
                    let _ = unsafe { migrate(ptr, size, NodeId::SINGLE) };
                    black_box(ptr.as_ptr());
                    unsafe {
                        dealloc(ptr.as_ptr(), layout);
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Cross-node buffer access (Category 11 — locality)
// ============================================================================

/// Allocates memory on node 0, then reads it from the current thread (which
/// may be on a different node). On single-node systems this measures plain
/// memory read bandwidth; on multi-node systems it reveals the remote-access
/// penalty.
fn bench_cross_node_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("numa/cross_node_read");

    let size = 256 * PAGE_SIZE; // 1 MiB
    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function("1MiB", |b| {
        let ptr = allocate_node(size, NodeId::SINGLE).expect("alloc");

        // Fill with a pattern.
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr(), 0x42, size);
        }

        b.iter(|| {
            let base = ptr.as_ptr();
            let mut sum: u64 = 0;
            let mut off = 0;
            while off < size {
                // SAFETY: `base` is valid for `size` bytes, filled above.
                sum = sum.wrapping_add(unsafe { base.add(off).read_volatile() } as u64);
                off += 64; // cache-line stride
            }
            black_box(sum);
        });

        // SAFETY: ptr was returned by allocate_node with this size.
        unsafe {
            deallocate(ptr, size);
        }
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_current_node,
    bench_allocate_node,
    bench_bind,
    bench_migrate,
    bench_cross_node_read,
);
criterion_main!(benches);
