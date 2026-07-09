//! Benchmark for flag key lookups.
//!
//! Measures the performance of creating, comparing, and hashing
//! flag keys.

#![allow(missing_docs)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use feature_flags::FlagKey;

fn bench_flag_key_new(c: &mut Criterion) {
    let long_name = "a".repeat(63);
    let names = ["a", "flag", "my_feature_flag", long_name.as_str()];

    let mut group = c.benchmark_group("flag_key_new");
    for name in names {
        group.bench_with_input(format!("len_{}", name.len()), name, |b, name| {
            b.iter(|| {
                let _ = black_box(FlagKey::new(black_box(name)));
            });
        });
    }
    group.finish();
}

fn bench_flag_key_as_str(c: &mut Criterion) {
    let key = FlagKey::new("test_flag_key").unwrap();

    c.bench_function("flag_key_as_str", |b| {
        b.iter(|| {
            let _ = black_box(key.as_str());
        });
    });
}

fn bench_flag_key_is_nil(c: &mut Criterion) {
    let key = FlagKey::new("test").unwrap();
    let nil_key = FlagKey::nil();

    c.bench_function("flag_key_is_nil_non_nil", |b| {
        b.iter(|| {
            let _ = black_box(key.is_nil());
        });
    });

    c.bench_function("flag_key_is_nil_nil", |b| {
        b.iter(|| {
            let _ = black_box(nil_key.is_nil());
        });
    });
}

fn bench_flag_key_clone(c: &mut Criterion) {
    let key = FlagKey::new("clone_test").unwrap();

    c.bench_function("flag_key_clone", |b| {
        b.iter(|| {
            let _ = black_box(key);
        });
    });
}

fn bench_flag_key_equality(c: &mut Criterion) {
    let key1 = FlagKey::new("equality_test").unwrap();
    let key2 = FlagKey::new("equality_test").unwrap();
    let key3 = FlagKey::new("other").unwrap();

    c.bench_function("flag_key_eq_equal", |b| {
        b.iter(|| {
            let _ = black_box(key1.raw() == key2.raw());
        });
    });

    c.bench_function("flag_key_eq_not_equal", |b| {
        b.iter(|| {
            let _ = black_box(key1.raw() == key3.raw());
        });
    });
}

criterion_group!(
    benches,
    bench_flag_key_new,
    bench_flag_key_as_str,
    bench_flag_key_is_nil,
    bench_flag_key_clone,
    bench_flag_key_equality,
);
criterion_main!(benches);
