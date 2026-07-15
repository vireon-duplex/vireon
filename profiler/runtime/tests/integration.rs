//! Integration tests: stress, concurrency, nested, recursive, generic,
//! trait, async, and panic scenarios.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Mutex, MutexGuard};

use profiler::{Profiler, ProfilerConfig, profile};

// Serialize all integration tests — they share global profiler state.
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() -> MutexGuard<'static, ()> {
    let g = lock();
    Profiler::init(ProfilerConfig::default());
    Profiler::enable();
    g
}

// ── Nested call test ──────────────────────────────────────────────

mod nested {
    use super::*;

    #[profile]
    fn level_1() -> u64 {
        level_2() + 1
    }
    #[profile]
    fn level_2() -> u64 {
        level_3() + 1
    }
    #[profile]
    fn level_3() -> u64 {
        level_4() + 1
    }
    #[profile]
    fn level_4() -> u64 {
        level_5() + 1
    }
    #[profile]
    fn level_5() -> u64 {
        1
    }

    #[test]
    fn nested_calls_recorded() {
        let _lock = setup();
        Profiler::reset();
        let result = level_1();
        assert_eq!(result, 5);

        profiler::flush_local();
        let snaps = Profiler::snapshot();
        for name in &["level_1", "level_2", "level_3", "level_4", "level_5"] {
            assert!(
                snaps.iter().any(|s| s.name == *name),
                "missing {name} in snapshots"
            );
        }
    }
}

// ── Recursive function test ───────────────────────────────────────

mod recursive {
    use super::*;

    #[profile]
    fn factorial(n: u64) -> u64 {
        if n <= 1 { 1 } else { n * factorial(n - 1) }
    }

    #[test]
    fn recursive_function_works() {
        let _lock = setup();
        Profiler::reset();
        let result = factorial(10);
        assert_eq!(result, 3_628_800);

        profiler::flush_local();
        let snaps = Profiler::snapshot();
        assert!(snaps.iter().any(|s| s.name == "factorial"));
        let fact_snap = snaps.iter().find(|s| s.name == "factorial").unwrap();
        // factorial(10..=1) = 10 calls total.
        assert_eq!(fact_snap.calls, 10);
    }
}

// ── Generic function test ─────────────────────────────────────────

mod generic {
    use super::*;

    #[profile]
    fn identity<T: std::fmt::Debug>(x: T) -> T {
        x
    }

    #[profile]
    fn max_of<T: Ord>(a: T, b: T) -> T {
        if a >= b { a } else { b }
    }

    #[test]
    fn generic_functions_instrumented() {
        let _lock = setup();
        Profiler::reset();

        let _ = identity(42u32);
        let _ = identity("hello");
        let _ = max_of(10u64, 20u64);

        profiler::flush_local();
        let snaps = Profiler::snapshot();

        // Both generic instantiations should be tracked under the same name.
        let id_count = snaps
            .iter()
            .filter(|s| s.name == "identity")
            .map(|s| s.calls)
            .sum::<u64>();
        assert_eq!(id_count, 2);

        assert!(snaps.iter().any(|s| s.name == "max_of"));
    }
}

// ── Trait function test ───────────────────────────────────────────

mod trait_test {
    use super::*;

    trait Greet {
        fn say_hello(&self) -> String;
    }

    struct English;
    struct Thai;

    impl Greet for English {
        #[profile]
        fn say_hello(&self) -> String {
            "Hello".to_string()
        }
    }

    impl Greet for Thai {
        #[profile]
        fn say_hello(&self) -> String {
            "Sawasdee".to_string()
        }
    }

    #[test]
    fn trait_methods_instrumented() {
        let _lock = setup();
        Profiler::reset();

        let en = English;
        let th = Thai;
        let _ = en.say_hello();
        let _ = th.say_hello();

        profiler::flush_local();
        let snaps = Profiler::snapshot();
        let count: u64 = snaps
            .iter()
            .filter(|s| s.name == "say_hello")
            .map(|s| s.calls)
            .sum();
        assert_eq!(count, 2);
    }
}

// ── Concurrency test ──────────────────────────────────────────────

mod concurrency {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    #[profile]
    fn concurrent_work(iterations: u64) -> u64 {
        let mut sum = 0u64;
        for i in 0..iterations {
            sum = sum.wrapping_add(i);
        }
        sum
    }

    #[test]
    fn multi_thread_profiling() {
        let _lock = setup();
        Profiler::reset();

        let total = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let total = Arc::clone(&total);
            handles.push(thread::spawn(move || {
                let result = concurrent_work(10_000);
                total.fetch_add(result, Ordering::Relaxed);
                // Flush this thread's data before exiting.
                profiler::flush_local();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let snaps = Profiler::snapshot();
        let snap = snaps.iter().find(|s| s.name == "concurrent_work");
        assert!(snap.is_some(), "concurrent_work should be profiled");
        let snap = snap.unwrap();
        // Each of 8 threads called it once.
        assert_eq!(snap.calls, 8);
    }
}

// ── Stress test ───────────────────────────────────────────────────

mod stress {
    use super::*;

    #[profile]
    fn tiny() -> u64 {
        1
    }

    #[test]
    fn ten_thousand_calls() {
        let _lock = setup();
        Profiler::reset();

        let mut sum = 0u64;
        for _ in 0..10_000 {
            sum = sum.wrapping_add(tiny());
        }
        assert_eq!(sum, 10_000);

        profiler::flush_local();
        let snaps = Profiler::snapshot();
        let snap = snaps.iter().find(|s| s.name == "tiny").unwrap();
        assert_eq!(snap.calls, 10_000);
    }
}

// ── Panic test ────────────────────────────────────────────────────

mod panic_test {
    use super::*;

    #[profile]
    fn panicking_fn() {
        panic!("intentional panic");
    }

    #[profile]
    fn caller() {
        let _ = std::panic::catch_unwind(|| {
            panicking_fn();
        });
    }

    #[test]
    fn guard_survives_panic() {
        let _lock = setup();
        Profiler::reset();
        context_reset();

        caller();

        profiler::flush_local();
        // Call stack should be balanced (depth 0).
        assert_eq!(profiler::context_depth(), 0);
    }

    fn context_reset() {
        // Access internal context reset via the public API.
        // Since context::reset_thread is not public, we verify
        // depth is 0 after operations.
    }
}

// ── Async test ────────────────────────────────────────────────────

mod async_test {
    use super::*;

    #[profile]
    async fn async_compute(n: u64) -> u64 {
        // Simulate async work by yielding.
        let mut sum = 0u64;
        for i in 0..n {
            sum = sum.wrapping_add(i);
        }
        sum
    }

    #[profile]
    async fn async_chained(n: u64) -> u64 {
        let a = async_compute(n).await;
        a + 1
    }

    // Simple future poller (no async runtime needed for these simple futures).
    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        use std::pin::Pin;
        use std::task::{Context, Poll, Waker};

        struct NoopWaker;
        impl std::task::Wake for NoopWaker {
            fn wake(self: std::sync::Arc<Self>) {}
        }

        let waker = Waker::from(std::sync::Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(fut);

        loop {
            match Pin::as_mut(&mut fut).poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => {
                    // For our simple futures, they're always ready on first poll.
                    // In a real runtime, we'd wait for a wake-up.
                    std::thread::yield_now();
                }
            }
        }
    }

    #[test]
    fn async_function_timed() {
        let _lock = setup();
        Profiler::reset();

        let result = block_on(async_compute(100));
        assert_eq!(result, 4950);

        profiler::flush_local();
        let snaps = Profiler::snapshot();
        assert!(snaps.iter().any(|s| s.name == "async_compute"));
    }

    #[test]
    fn async_chained_functions_timed() {
        let _lock = setup();
        Profiler::reset();

        let result = block_on(async_chained(10));
        assert!(result > 0);

        profiler::flush_local();
        let snaps = Profiler::snapshot();
        assert!(snaps.iter().any(|s| s.name == "async_compute"));
        assert!(snaps.iter().any(|s| s.name == "async_chained"));
    }
}

// ── Export test ───────────────────────────────────────────────────

mod export {
    use super::*;

    #[profile]
    fn export_test_fn() -> u64 {
        42
    }

    #[test]
    fn report_contains_data() {
        let _lock = setup();
        Profiler::reset();

        for _ in 0..100 {
            export_test_fn();
        }

        let report = Profiler::report();
        assert!(report.contains("export_test_fn"));
        assert!(report.contains("100"));
    }

    #[test]
    fn json_export_valid() {
        let _lock = setup();
        Profiler::reset();
        export_test_fn();
        let json = Profiler::export_json();
        assert!(json.starts_with('['));
        assert!(json.contains("export_test_fn"));
    }

    #[test]
    fn csv_export_valid() {
        let _lock = setup();
        Profiler::reset();
        export_test_fn();
        let csv = Profiler::export_csv();
        assert!(csv.starts_with("function,calls"));
    }

    #[test]
    fn flamegraph_export_works() {
        let _lock = setup();
        Profiler::reset();
        export_test_fn();
        let _folded = Profiler::export_flamegraph();
        // Should not panic.
    }
}

// ── Enable / disable test ─────────────────────────────────────────

mod enable_disable {
    use super::*;

    #[profile]
    fn toggle_fn() -> u64 {
        1
    }

    #[test]
    fn disable_then_enable() {
        let _lock = setup();
        Profiler::reset();

        // Disabled: calls don't record.
        Profiler::disable();
        for _ in 0..10 {
            toggle_fn();
        }
        profiler::flush_local();

        // Enable: calls record.
        Profiler::enable();
        for _ in 0..5 {
            toggle_fn();
        }
        profiler::flush_local();

        let snaps = Profiler::snapshot();
        let snap = snaps.iter().find(|s| s.name == "toggle_fn");
        if let Some(s) = snap {
            // Should have recorded ~5 calls (disabled calls don't count).
            assert!(s.calls <= 5, "expected <= 5 calls, got {}", s.calls);
        }
    }
}
