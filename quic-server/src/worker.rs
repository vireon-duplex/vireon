//! Worker — owns one `io::Reactor` + one `QuicEngine` and drives the event loop.
//!
//! In single-worker mode the worker runs on the main thread.
//! In multi-worker mode each worker runs on its own pinned thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;

use io::Reactor;
use quic_engine::QuicEngineBuilder;

use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::stack::VireonStack;

/// A QUIC worker — one Reactor + one VireonStack (engine + layers).
///
/// Not `Send` (io_uring ring + buffer pool hold raw pointers).
/// Each worker must stay on the thread that created it.
pub struct Worker {
    core_id: u32,
    reactor: Reactor,
    handler: VireonStack,
}

impl Worker {
    /// Creates a new worker pinned to `core_id`, bound to `config.port`.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if reactor or engine initialization fails.
    pub fn create(
        config: &ServerConfig,
        core_id: u32,
        shutdown_requested: Arc<AtomicBool>,
    ) -> Result<Self, ServerError> {
        tracing::info!(core_id, port = config.port, "initializing reactor");

        let reactor = Reactor::init(core_id, config.port, config.queue_depth)
            .map_err(|e| ServerError::ReactorInit(format!("{e:?}")))?;

        let local_addr = reactor
            .socket()
            .local_addr()
            .map_err(|e| ServerError::ReactorInit(format!("local_addr: {e:?}")))?;

        tracing::info!(core_id, %local_addr, "reactor bound");

        let engine = QuicEngineBuilder::from_config(config.quic_config.clone())
            .reactor_id(core_id)
            .local_addr(local_addr)
            .cert_path(&config.cert_path)
            .key_path(&config.key_path)
            .buffer_pool(buffer_pool::BufferPool::default())
            .build()
            .map_err(|e| ServerError::EngineInit(format!("{e:?}")))?;

        let handler = VireonStack::new(engine, core_id, shutdown_requested)?;

        Ok(Self {
            core_id,
            reactor,
            handler,
        })
    }

    /// Runs the event loop until the reactor's stop flag is set.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the io_uring loop fails.
    pub fn run(mut self) -> Result<(), ServerError> {
        tracing::info!(core_id = self.core_id, "worker started");
        // Wire the reactor's stop handle into the handler so it can
        // signal reactor exit after the graceful-drain period.
        self.handler.set_reactor_stop(self.reactor.stop_handle());
        self.reactor
            .run_with_handler(&mut self.handler)
            .map_err(|e| ServerError::EventLoop(format!("{e:?}")))?;
        tracing::info!(core_id = self.core_id, "worker stopped");
        Ok(())
    }

    /// Returns a stop handle so an external thread can signal shutdown.
    #[must_use]
    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        self.reactor.stop_handle()
    }

    /// Returns the core id this worker is pinned to.
    #[must_use]
    #[allow(dead_code)]
    pub const fn core_id(&self) -> u32 {
        self.core_id
    }

    /// Spawns a worker on a dedicated thread pinned to `core_id`.
    ///
    /// Returns the `JoinHandle` plus a receiver that yields the worker's
    /// `stop_handle` once the reactor is initialised inside the thread.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if the thread cannot be spawned.
    pub fn spawn(
        config: ServerConfig,
        core_id: u32,
        shutdown_requested: Arc<AtomicBool>,
    ) -> Result<
        (
            thread::JoinHandle<Result<(), ServerError>>,
            mpsc::Receiver<Arc<AtomicBool>>,
        ),
        ServerError,
    > {
        let (tx, rx) = mpsc::channel();
        let builder = thread::Builder::new().name(format!("vireon-reactor-{core_id}"));
        let handle = builder
            .spawn(move || {
                let worker = match Worker::create(&config, core_id, shutdown_requested) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!(core_id, error = %e, "failed to create worker");
                        return Err(e);
                    }
                };
                // Send stop handle back before entering run loop.
                let _ = tx.send(worker.stop_handle());
                worker.run()
            })
            .map_err(|e| ServerError::ThreadSpawn(format!("{e}")))?;
        Ok((handle, rx))
    }
}

/// Installs a Ctrl-C handler that sets `shutdown_requested` to true.
///
/// Unlike the old behavior (which set the reactor stop flag directly),
/// this triggers the graceful drain: connections receive CONNECTION_CLOSE
/// frames, and the reactor exits after [`DRAIN_TIMEOUT`](crate::stack::DRAIN_TIMEOUT).
///
/// # Errors
///
/// Returns an error if the signal handler cannot be installed.
pub fn install_signal_handler(shutdown_requested: Arc<AtomicBool>) -> Result<(), ServerError> {
    ctrlc::set_handler(move || {
        tracing::info!("received SIGINT/SIGTERM, initiating graceful shutdown...");
        shutdown_requested.store(true, Ordering::Release);
    })
    .map_err(|e| ServerError::SignalHandler(format!("{e}")))
}
