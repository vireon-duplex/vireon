//! **Production-grade function profiler for Rust.**
//!
//! Instrument any function with the `#[profile]` attribute:
//!
//! ```ignore
//! use profiler::profile;
//!
//! #[profile]
//! fn process_packet(data: &[u8]) -> usize {
//!     data.len()
//! }
//!
//! #[profile]
//! async fn send_stream(id: u64) -> Result<(), Error> {
//!     // ...
//! }
//! ```
//!
//! The macro expands to:
//! ```ignore
//! fn process_packet(data: &[u8]) -> usize {
//!     let _guard = ::profiler::Guard::enter("process_packet");
//!     data.len()
//! }
//! ```
//!
//! # Architecture
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`guard`] | RAII timing guard (Drop records elapsed) |
//! | [`collector`] | Thread-local accumulator with periodic flush |
//! | [`registry`] | Global `RwLock<HashMap>` of per-function stats |
//! | [`stats`] | `FunctionStats` (atomics) + `LocalEntry` (thread-local) |
//! | [`histogram`] | 512-bucket lock-free logarithmic histogram |
//! | [`context`] | Thread-local call-tree stack |
//! | [`flamegraph`] | Edge tracking + folded-stack export |
//! | [`report`] | Formatted text report |
//! | [`exporter`] | JSON, CSV, Chrome Trace export |
//! | [`config`] | `ProfilerConfig` |
//!
//! # Feature flags
//!
//! - `enabled` — activates instrumentation. When off, `Guard` is a
//!   zero-cost no-op (empty struct, empty `Drop`).
//!
//! # Quick start
//!
//! ```no_run
//! use profiler::{Profiler, ProfilerConfig};
//!
//! // Initialize with defaults, enable profiling.
//! Profiler::init(ProfilerConfig::default());
//! Profiler::enable();
//! Profiler::start();
//!
//! // ... run your instrumented code ...
//!
//! // Print report.
//! println!("{}", Profiler::report());
//!
//! Profiler::stop();
//! ```

pub mod collector;
pub mod config;
pub mod context;
pub mod exporter;
pub mod flamegraph;
pub mod guard;
pub mod histogram;
pub mod registry;
pub mod report;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use collector::flush_local;
pub use config::{ProfilerConfig, ProfilerConfigBuilder};
pub use context::depth as context_depth;
pub use guard::Guard;
pub use stats::FunctionSnapshot;

// Re-export the proc-macro.
pub use profiler_macro::profile;

// ── Internal imports ──────────────────────────────────────────────

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

use collector::{bump_tick, set_enabled, set_flush_threshold, set_sampling};

// ── Background thread state ───────────────────────────────────────

static BG_STOP: AtomicBool = AtomicBool::new(false);
static BG_RUNNING: AtomicBool = AtomicBool::new(false);

static BG_HANDLE: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

// ── Profiler facade ───────────────────────────────────────────────

/// Top-level profiler API.
///
/// All methods are static — the profiler uses global state internally.
/// Call [`Profiler::init`] once at startup, then [`Profiler::enable`]
/// and [`Profiler::start`] to begin profiling.
pub struct Profiler;

impl Profiler {
    /// Initializes the profiler with the given configuration.
    ///
    /// Sets the global enable/disable flag, sampling rate, and flush
    /// threshold. Safe to call multiple times.
    pub fn init(config: ProfilerConfig) {
        set_enabled(config.enabled);
        set_sampling(config.sampling);
        set_flush_threshold(config.thread_local_capacity);
    }

    /// Enables profiling globally.
    pub fn enable() {
        set_enabled(true);
    }

    /// Disables profiling globally.
    ///
    /// Existing guards will skip recording. No data is lost —
    /// accumulated thread-local data is preserved until the next flush.
    pub fn disable() {
        set_enabled(false);
    }

    /// Starts the background collector thread.
    ///
    /// The thread increments the global tick generation every
    /// `flush_interval_ms`, causing all threads to flush their local
    /// data on the next instrumented call.
    ///
    /// # Panics
    /// Panics if the background mutex is poisoned.
    pub fn start() {
        Self::start_with_interval(ProfilerConfig::default().flush_interval_ms);
    }

    /// Starts the background collector with a custom interval (ms).
    pub fn start_with_interval(flush_interval_ms: u64) {
        if BG_RUNNING
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return; // Already running.
        }
        BG_STOP.store(false, Ordering::SeqCst);

        let handle = thread::Builder::new()
            .name("profiler-bg".into())
            .spawn(move || {
                while !BG_STOP.load(Ordering::Relaxed) {
                    thread::sleep(std::time::Duration::from_millis(
                        flush_interval_ms.max(1),
                    ));
                    if BG_STOP.load(Ordering::Relaxed) {
                        break;
                    }
                    bump_tick();
                }
            })
            .ok();

        let mut guard = BG_HANDLE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = handle;
    }

    /// Stops the background collector thread and flushes all local data.
    pub fn stop() {
        BG_STOP.store(true, Ordering::SeqCst);
        BG_RUNNING.store(false, Ordering::SeqCst);

        let handle = {
            let mut guard = BG_HANDLE
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(h) = handle {
            let _ = h.join();
        }

        // Flush the calling thread's local data.
        flush_local();
    }

    /// Generates and returns a formatted text report.
    #[must_use]
    pub fn report() -> String {
        flush_local();
        let snapshots = registry::snapshot_all();
        report::generate(&snapshots)
    }

    /// Resets all profiling data (global stats + edges).
    pub fn reset() {
        registry::clear_all();
        flamegraph::reset();
    }

    /// Takes a snapshot of all function stats, sorted by total time.
    #[must_use]
    pub fn snapshot() -> Vec<FunctionSnapshot> {
        flush_local();
        registry::snapshot_all()
    }

    /// Exports all stats as a JSON string.
    #[must_use]
    pub fn export_json() -> String {
        let snapshots = Self::snapshot();
        exporter::export_json(&snapshots)
    }

    /// Exports all stats as a CSV string.
    #[must_use]
    pub fn export_csv() -> String {
        let snapshots = Self::snapshot();
        exporter::export_csv(&snapshots)
    }

    /// Exports all stats in Chrome Trace format.
    #[must_use]
    pub fn export_chrome_trace() -> String {
        let snapshots = Self::snapshot();
        exporter::export_chrome_trace(&snapshots)
    }

    /// Exports call-graph data in folded-stack format (for flamegraph.pl).
    #[must_use]
    pub fn export_flamegraph() -> String {
        flush_local();
        flamegraph::export_folded()
    }

    /// Exports call-graph data in Graphviz DOT format.
    #[must_use]
    pub fn export_dot() -> String {
        flush_local();
        flamegraph::export_dot()
    }

    /// Number of registered functions.
    #[must_use]
    pub fn function_count() -> usize {
        registry::count()
    }
}

impl std::fmt::Debug for Profiler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Profiler")
            .field("enabled", &collector::is_enabled())
            .field("functions", &registry::count())
            .field("bg_running", &BG_RUNNING.load(Ordering::Relaxed))
            .finish()
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(all(test, feature = "enabled"))]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn reset_profiler() {
        Profiler::reset();
        Profiler::enable();
        collector::set_sampling(1);
        collector::set_flush_threshold(1024);
        context::reset_thread();
    }

    #[test]
    fn init_enable_disable() {
        Profiler::init(ProfilerConfig::default());
        Profiler::disable();
        assert!(!collector::is_enabled());
        Profiler::enable();
        assert!(collector::is_enabled());
    }

    #[test]
    fn report_works() {
        reset_profiler();
        // Record some data via guard.
        {
            let _g = Guard::enter("report_test_fn");
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
        flush_local();
        let report = Profiler::report();
        assert!(report.contains("PROFILER REPORT"));
        assert!(report.contains("report_test_fn"));
    }

    #[test]
    fn snapshot_returns_sorted() {
        reset_profiler();
        {
            let _g = Guard::enter("snapshot_a");
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
        {
            let _g = Guard::enter("snapshot_b");
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        flush_local();
        let snaps = Profiler::snapshot();
        assert!(snaps.len() >= 2);
        // Sorted by total_ns descending.
        assert!(snaps[0].total_ns >= snaps[1].total_ns);
    }

    #[test]
    fn export_json_valid() {
        reset_profiler();
        {
            let _g = Guard::enter("json_test");
        }
        flush_local();
        let json = Profiler::export_json();
        assert!(json.starts_with('['));
    }

    #[test]
    fn export_csv_has_header() {
        reset_profiler();
        let csv = Profiler::export_csv();
        assert!(csv.starts_with("function,calls"));
    }

    #[test]
    fn reset_clears_data() {
        reset_profiler();
        {
            let _g = Guard::enter("reset_test_fn");
        }
        flush_local();
        assert!(Profiler::function_count() > 0);
        Profiler::reset();
        assert_eq!(Profiler::function_count(), 0);
    }

    #[test]
    fn start_stop_background() {
        Profiler::start_with_interval(10);
        assert!(BG_RUNNING.load(Ordering::Relaxed));
        std::thread::sleep(std::time::Duration::from_millis(50));
        Profiler::stop();
        assert!(!BG_RUNNING.load(Ordering::Relaxed));
    }
}
