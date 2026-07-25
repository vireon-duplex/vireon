//! Server-level error type.

/// Errors emitted by the QUIC server.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("reactor initialization failed: {0}")]
    ReactorInit(String),

    #[error("QUIC engine initialization failed: {0}")]
    EngineInit(String),

    #[error("event loop error: {0}")]
    EventLoop(String),

    #[error("thread spawn failed: {0}")]
    ThreadSpawn(String),

    #[error("signal handler installation failed: {0}")]
    SignalHandler(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("worker thread panicked: {0}")]
    WorkerPanic(String),
}
