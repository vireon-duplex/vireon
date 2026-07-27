//! Vireon QUIC Server — production-grade QUIC server on io_uring + quiche.

mod acl_loader;
mod cli;
mod cluster;
mod config;
mod connection;
mod distributed;
mod error;
mod eviction;
mod layers;
mod log_throttle;
mod metrics;
mod server;
mod socket_registry;
mod stack;
mod worker;

#[cfg(feature = "profiling")]
use std::sync::OnceLock;

use clap::Parser;

use crate::cli::Cli;
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::server::QuicServer;

/// Global: holds the `--profile-output` path so the signal handler can
/// dump the profiler report before `process::exit(130)`.
#[cfg(feature = "profiling")]
static PROFILE_OUTPUT: OnceLock<Option<String>> = OnceLock::new();

fn main() -> Result<(), ServerError> {
    let cli = Cli::parse();
    init_tracing(&cli);

    // Install a panic hook that force-closes every registered socket fd.
    // With `panic = "unwind"` (default), Drop chains already run on panic
    // — but a panic on a worker thread doesn't trigger the main-thread
    // Drop chain for that thread's Reactor/Socket if the thread is still
    // mid-flight when the hook runs. Closing via the registry is a
    // belt-and-braces safety net that runs regardless of which thread
    // panicked.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("[panic] {info}");
        // Cancel io_uring multishot RECV ops, then shutdown+close fds.
        io::cancel_all_rings();
        crate::socket_registry::REGISTRY.close_all();
        eprintln!("note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace");
    }));

    tracing::info!("vireon quic-server starting");
    tracing::debug!(cli = ?cli, "parsed CLI");

    #[cfg(feature = "profiling")]
    {
        profiler::init();
        let _ = PROFILE_OUTPUT.set(cli.profile_output.clone());
    }

    let config = ServerConfig::from_cli(&cli).map_err(|e| ServerError::Config(format!("{e:#}")))?;

    print_banner(&config);

    let server = QuicServer::new(config);
    let result = server.run();

    #[cfg(feature = "profiling")]
    profiler::dump_report();

    result
}

fn init_tracing(cli: &Cli) {
    let filter = if cli.verbose {
        "quic_server=debug,quic_engine=debug,io=debug,quiche=trace,warn"
    } else {
        "quic_server=info,quic_engine=info,warn"
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn print_banner(config: &ServerConfig) {
    tracing::info!("============================================================");
    tracing::info!("  Vireon QUIC Server");
    tracing::info!("  Mode:     {:?}", config.mode);
    tracing::info!("  Preset:   {:?}", config.preset);
    tracing::info!("  Bind:     {}:{}", config.bind_addr, config.port);
    tracing::info!("  Workers:  {}", config.num_workers());
    tracing::info!("  Cert:     {}", config.cert_path);
    tracing::info!("  Max conns/shard: {}", config.quic_config.max_connections);
    tracing::info!("  CC:       {:?}", config.quic_config.congestion);
    if config.echo {
        tracing::info!("  Echo:     ENABLED (benchmark mode)");
    }
    if config.skip_crc {
        tracing::info!("  Skip CRC: ENABLED (trusted mode — no integrity check)");
    }
    if let Some(ca) = &config.quic_config.client_ca {
        tracing::info!("  mTLS:     ENABLED (client CA: {ca})");
    }
    if let Some(port) = config.metrics_port {
        tracing::info!("  Metrics:  http://0.0.0.0:{port}/metrics");
    }
    tracing::info!("============================================================");
}

// ── Profiler helpers ─────────────────────────────────────────────

#[cfg(feature = "profiling")]
mod profiler_helpers {
    use crate::PROFILE_OUTPUT;
    use std::path::Path;

    /// Initialise, enable, and start the profiler background thread.
    pub fn init() {
        let config = profiler::ProfilerConfig::builder()
            .enabled()
            .sampling(1)
            .flush_interval_ms(1000)
            .build();
        profiler::Profiler::init(config);
        profiler::Profiler::enable();
        profiler::Profiler::start();

        tracing::info!("profiler: enabled (build feature `profiling`)");
    }

    /// Stop the profiler and dump the report (called on graceful exit).
    pub fn dump_report() {
        profiler::Profiler::stop();
        do_dump();
    }

    /// Force-dump without stopping the background thread (called from
    /// signal handler before `process::exit`).
    pub fn force_dump() {
        profiler::Profiler::stop();
        do_dump();
    }

    fn do_dump() {
        let snapshots = profiler::Profiler::snapshot();

        let output_path = PROFILE_OUTPUT
            .get()
            .and_then(|opt| opt.as_ref())
            .map(String::as_str);

        match output_path {
            Some(path) => write_to_file(path),
            None => log_summary(&snapshots),
        }
    }

    fn write_to_file(path: &str) {
        let p = Path::new(path);
        let content = match p.extension().and_then(|e| e.to_str()) {
            Some("json") => profiler::Profiler::export_json(),
            Some("csv") => profiler::Profiler::export_csv(),
            Some("ctrace") => profiler::Profiler::export_chrome_trace(),
            Some("folded") => profiler::Profiler::export_flamegraph(),
            Some("dot") => profiler::Profiler::export_dot(),
            _ => profiler::Profiler::report(),
        };

        match std::fs::write(p, &content) {
            Ok(()) => {
                tracing::info!(path = %path, "profiler: report written");
            }
            Err(e) => {
                tracing::error!(error = %e, path = %path, "profiler: failed to write report");
            }
        }
    }

    fn log_summary(snapshots: &[profiler::FunctionSnapshot]) {
        if snapshots.is_empty() {
            tracing::warn!("profiler: no data recorded");
            return;
        }

        // Log top-10 by total time (snapshots are already sorted by total_ns desc).
        tracing::info!(
            "profiler: top functions by total time (of {}):",
            snapshots.len()
        );
        for s in snapshots.iter().take(10) {
            tracing::info!(
                "  {:<40} calls={:>8}  total={:>12} ns  avg={:>8} ns  p99={:>8} ns",
                s.name,
                s.calls,
                s.total_ns,
                s.avg_ns,
                s.p99,
            );
        }

        tracing::info!("profiler: pass --profile-output <file> for full report");
        tracing::info!("profiler:   .json .csv .ctrace .folded .dot  (extension selects format)");
        tracing::info!("profiler:   any other extension = text table");
    }
}

#[cfg(feature = "profiling")]
use profiler_helpers as profiler;

/// Called from the signal handler (`worker.rs`) before `process::exit(130)`.
#[cfg(feature = "profiling")]
pub(crate) fn profiler_force_dump() {
    profiler::force_dump();
}
