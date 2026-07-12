//! Application layer — the outermost layer.
//!
//! Owns the `SessionLayer` and a [`VireonDispatcher`] that routes
//! data-plane frames to the five application-layer modules:
//!
//! * `ai-streaming` — text tokens
//! * `rpc-framework` — tool calls / results
//! * `media-streaming` — audio / video / image frames
//! * `event-bus` — state updates (pub/sub partition bus)
//! * `pubsub-engine` — state updates (topic-pattern subscriptions)
//!
//! Responses (e.g. a `ToolResult` echoing a `ToolCall`) are queued in
//! an outbox that [`VireonStack`](crate::stack::VireonStack) drains and
//! sends via the QUIC engine.
//!
//! ```text
//! ApplicationLayer
//!   ├─ SessionLayer (owns StreamLayer → MessageLayer)
//!   └─ VireonDispatcher
//!        ├─ AiStreamPipeline
//!        ├─ RpcServer
//!        ├─ MediaSession
//!        ├─ EventBus
//!        ├─ PubSubEngine
//!        ├─ outbox: Vec<OutgoingFrame>
//!        └─ next_seq: per-QUIC-stream response seq
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;

use agent_runtime::AgentRuntime;
use ai_streaming::{AiStreamConfig, AiStreamId, AiStreamPipeline, StreamMode, TokenChunk};
use application_runtime::AppRuntime;
use application_runtime::stats::AppRuntimeStats;
use auth_framework::authenticator::AuthenticationPipeline;
use configuration_system::manager::ConfigurationManager;
use dependency_injection::container::Container;
use event_bus::{EventBus, EventBusConfig, PartitionId, TopicId};
use feature_flags::manager::FeatureFlagManager;
use frame::codec::{FrameHeader, FrameRef};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use graceful_startup_shutdown::StartupShutdownCoordinator;
use media_streaming::{
    MediaError, MediaSession, MediaStreamingConfig, MediaTrackId, TrackDirection, TrackKind,
};
use middleware_pipeline::pipeline::Pipeline;
use module_lifecycle::coordinator::LifecycleCoordinator;
use observability_sdk::registry::MetricsRegistry;
use ops_dashboard::OpsDashboardService;
use plugin_manager::manager::PluginManager;
use pubsub_engine::{PubSubConfig, PubSubEngine, PubSubMessage, TopicPattern};
use resource_quotas::manager::QuotaManager;
use rpc_framework::{RpcConfig, RpcServer};
use scheduler_hooks::coordinator::HookCoordinator;
use service_registry::registry::ServiceRegistry;
use session_manager::SessionId;
use version_negotiation::negotiator::VersionNegotiator;

use super::message::FrameAction;
use super::session::SessionLayer;
use crate::error::ServerError;

// ── FrameContext & OutgoingFrame ──────────────────────────────────

/// Connection + stream context passed into dispatcher callbacks.
///
/// Carries everything the dispatcher needs to route a frame and (if
/// needed) enqueue a response on the same QUIC stream the request
/// arrived on.
#[derive(Clone, Copy)]
pub struct FrameContext {
    /// Application-layer session id.
    pub session_id: SessionId,
    /// Slab index of the QUIC connection.
    pub conn_idx: usize,
    /// QUIC stream id the frame arrived on (used for responses).
    pub quic_stream_id: u64,
}

/// One frame queued for sending by the dispatcher.
///
/// [`VireonStack`](crate::stack::VireonStack) drains these, encodes
/// header + payload via `codec::encode_into_slice`, and calls
/// `QuicEngine::send_stream`.
pub struct OutgoingFrame {
    /// Slab index of the destination connection.
    pub conn_idx: usize,
    /// QUIC stream id to send on.
    pub quic_stream_id: u64,
    /// Codec frame header (logical stream, seq, type, flags).
    pub header: FrameHeader,
    /// Payload bytes.
    pub payload: Vec<u8>,
}

// ── AppDispatcher trait ───────────────────────────────────────────

/// Trait implemented by application-layer services to receive data-plane
/// events from the pipeline.
///
/// All callbacks are synchronous and run on the reactor thread.
/// Implementations must be non-blocking.
pub trait AppDispatcher {
    /// Called when a QUIC connection completes handshake and a new
    /// session is created.
    fn on_connection_established(&mut self, session_id: SessionId, peer: SocketAddr);

    /// Called for every data-plane frame received.
    fn on_data_frame(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>);

    /// Called when a session is destroyed (connection closed).
    fn on_connection_closed(&mut self, session_id: SessionId, reason: Option<&str>);

    /// Drains queued outgoing frames (responses produced by the modules).
    fn drain_outbox(&mut self) -> Vec<OutgoingFrame>;
}

// ── VireonDispatcher ──────────────────────────────────────────────

/// Production dispatcher that routes frames by [`MessageType`] to the
/// appropriate application-layer module(s).
///
/// Each module owns real in-memory state (Phase 1). The dispatcher
/// translates incoming frames into module calls and collects any
/// outgoing responses in an outbox.
pub struct VireonDispatcher {
    /// [application-layer] AI streaming pipeline.
    ai: AiStreamPipeline,
    /// [application-layer] RPC server.
    rpc: RpcServer,
    /// [application-layer] Media session.
    media: MediaSession,
    /// [application-layer] Event bus.
    event_bus: EventBus,
    /// [application-layer] Pub/Sub engine.
    pubsub: PubSubEngine,

    /// Maps logical frame stream → AI stream id (lazily opened).
    ai_streams: HashMap<u64, AiStreamId>,
    /// Default event-bus topic for `StateUpdate` routing.
    default_topic: TopicId,

    /// Queued outgoing frames, drained by the stack after each recv.
    outbox: Vec<OutgoingFrame>,
    /// Per-QUIC-stream response sequence counter.
    next_seq: HashMap<u64, Seq>,
}

impl VireonDispatcher {
    /// Creates a new dispatcher with default-configured modules.
    #[must_use]
    pub fn new() -> Self {
        let mut event_bus = EventBus::new(EventBusConfig::new());
        let default_topic = event_bus
            .create_topic(event_bus::TopicConfig::new(4))
            .unwrap_or(TopicId::NIL);

        let mut pubsub = PubSubEngine::new(PubSubConfig::new());
        if let Ok(pattern) = TopicPattern::new("state.*") {
            let _ = pubsub.subscribe(pattern);
        }

        Self {
            ai: AiStreamPipeline::new(AiStreamConfig::default()),
            rpc: RpcServer::new(RpcConfig::new()),
            media: MediaSession::new(MediaStreamingConfig::new()),
            event_bus,
            pubsub,
            ai_streams: HashMap::new(),
            default_topic,
            outbox: Vec::new(),
            next_seq: HashMap::new(),
        }
    }

    /// Builds a response header with an incrementing per-stream seq and
    /// enqueues it in the outbox.
    fn enqueue_response(
        &mut self,
        ctx: &FrameContext,
        logical_stream: StreamId,
        msg_type: MessageType,
        payload: Vec<u8>,
    ) {
        let seq = self.next_seq.entry(ctx.quic_stream_id).or_insert(Seq::ZERO);
        let header = FrameHeader::new(logical_stream, msg_type)
            .with_seq(*seq)
            .with_flags(FrameFlags::NONE);
        *seq = seq.next();
        self.outbox.push(OutgoingFrame {
            conn_idx: ctx.conn_idx,
            quic_stream_id: ctx.quic_stream_id,
            header,
            payload,
        });
    }

    /// Routes a `TextToken` frame through the AI streaming pipeline,
    /// lazily opening a per-logical-stream AI stream.
    fn route_ai_token(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let logical = frame.stream_id.get();
        let chunk = TokenChunk {
            token_id: frame.seq.get(),
            text: String::from_utf8_lossy(frame.payload).into_owned(),
            is_final: frame.flags.contains(FrameFlags::FIN),
        };
        if !self.ai_streams.contains_key(&logical) {
            match self.ai.open_stream(StreamMode::Text) {
                Ok(id) => {
                    self.ai_streams.insert(logical, id);
                }
                Err(e) => {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        error = ?e,
                        "[app:ai-streaming] open_stream failed"
                    );
                    return;
                }
            }
        }
        // SAFETY: we just ensured the key exists (or returned early).
        let &ai_id = self.ai_streams.get(&logical).unwrap_or(&AiStreamId::NIL);
        if let Err(e) = self.ai.send_token_chunk(ai_id, chunk) {
            tracing::warn!(
                conn_idx = ctx.conn_idx,
                error = ?e,
                "[app:ai-streaming] send_token_chunk failed"
            );
        }
    }

    /// Routes a media frame to the media session, lazily adding the track.
    fn route_media_frame(&mut self, _ctx: &FrameContext, frame: &FrameRef<'_>) {
        let track_id = MediaTrackId::new(frame.stream_id.get());
        let kind = match frame.msg_type {
            MessageType::AudioFrame => TrackKind::Audio,
            MessageType::VideoFrame => TrackKind::Video,
            _ => TrackKind::Data,
        };
        if let Err(MediaError::TrackNotFound(_)) =
            self.media.on_media_frame(track_id, frame.payload.len())
        {
            let _ = self
                .media
                .add_track(track_id, kind, TrackDirection::RecvOnly);
            let _ = self.media.on_media_frame(track_id, frame.payload.len());
        }
    }

    /// Routes a `StateUpdate` frame to the event bus and pub/sub engine.
    fn route_state_update(&mut self, _ctx: &FrameContext, frame: &FrameRef<'_>) {
        let msg_hash = frame.seq.get();
        let _ = self
            .event_bus
            .publish(self.default_topic, PartitionId::new(0), msg_hash);
        let msg = PubSubMessage::new("state.update", frame.payload.to_vec());
        let _ = self.pubsub.publish(msg);
    }
}

impl Default for VireonDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl AppDispatcher for VireonDispatcher {
    fn on_connection_established(&mut self, session_id: SessionId, peer: SocketAddr) {
        tracing::info!(session_id = ?session_id, %peer, "[app] connection established");
    }

    fn on_data_frame(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        match frame.msg_type {
            MessageType::TextToken => {
                self.route_ai_token(ctx, frame);
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    logical_stream = frame.stream_id.get(),
                    seq = frame.seq.get(),
                    payload_len = frame.payload.len(),
                    "[app:ai-streaming] TextToken"
                );
            }
            MessageType::ToolCall => {
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    payload_len = frame.payload.len(),
                    "[app:rpc] ToolCall → handle"
                );
                match self.rpc.handle(frame.payload) {
                    Ok(response) => {
                        self.enqueue_response(
                            ctx,
                            frame.stream_id,
                            MessageType::ToolResult,
                            response,
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            conn_idx = ctx.conn_idx,
                            error = ?e,
                            "[app:rpc] handle failed"
                        );
                    }
                }
            }
            MessageType::ToolResult => {
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    payload_len = frame.payload.len(),
                    "[app:rpc] ToolResult (client-side, log only)"
                );
            }
            MessageType::AudioFrame | MessageType::VideoFrame | MessageType::ImageFrame => {
                self.route_media_frame(ctx, frame);
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    msg_type = ?frame.msg_type,
                    logical_stream = frame.stream_id.get(),
                    payload_len = frame.payload.len(),
                    "[app:media] frame"
                );
            }
            MessageType::StateUpdate => {
                self.route_state_update(ctx, frame);
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    payload_len = frame.payload.len(),
                    "[app:event-bus+pubsub] StateUpdate"
                );
            }
            MessageType::Error => {
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    payload_len = frame.payload.len(),
                    "[app] Error frame"
                );
            }
            MessageType::Heartbeat => {
                tracing::debug!(
                    conn_idx = ctx.conn_idx,
                    session_id = ?ctx.session_id,
                    "[app] Heartbeat"
                );
            }
            _ => {
                tracing::debug!(
                    conn_idx = ctx.conn_idx,
                    msg_type = ?frame.msg_type,
                    "[app] unhandled data type"
                );
            }
        }
    }

    fn on_connection_closed(&mut self, session_id: SessionId, reason: Option<&str>) {
        tracing::info!(
            session_id = ?session_id,
            reason = reason.unwrap_or("none"),
            "[app] connection closed"
        );
    }

    fn drain_outbox(&mut self) -> Vec<OutgoingFrame> {
        std::mem::take(&mut self.outbox)
    }
}

// ── PlatformServices ──────────────────────────────────────────────

/// Container for the 16 infrastructure/platform services.
/// Each field holds the real facade object (not just a config),
/// constructed from its builder-produced config + default stats.
#[allow(dead_code)]
pub struct PlatformServices {
    /// [agent-runtime] AI agent execution runtime.
    agent: AgentRuntime,
    /// [application-runtime] Top-level app lifecycle composition root.
    app: AppRuntime,
    /// [auth-framework] Authentication + authorization pipeline.
    auth: AuthenticationPipeline,
    /// [configuration-system] Config key-value store.
    config_mgr: ConfigurationManager,
    /// [dependency-injection] DI container for module construction.
    di: Container,
    /// [feature-flags] Feature toggle management.
    feature_flags: FeatureFlagManager,
    /// [graceful-startup-shutdown] Startup/shutdown phase coordinator.
    graceful: StartupShutdownCoordinator,
    /// [middleware-pipeline] Frame middleware chain.
    middleware: Pipeline,
    /// [module-lifecycle] Module init/shutdown ordering.
    lifecycle: LifecycleCoordinator,
    /// [observability-sdk] Metrics registry + tracing.
    observability: MetricsRegistry,
    /// [ops-dashboard] Operational monitoring dashboard.
    ops: OpsDashboardService,
    /// [plugin-manager] Dynamic plugin loading.
    plugin: PluginManager,
    /// [resource-quotas] Per-tenant quota enforcement.
    quotas: QuotaManager,
    /// [scheduler-hooks] Scheduler event callbacks.
    scheduler_hooks: HookCoordinator,
    /// [service-registry] Service discovery for modules.
    registry: ServiceRegistry,
    /// [version-negotiation] Protocol version ALPN negotiation.
    version: VersionNegotiator,
}

impl PlatformServices {
    /// Constructs all 16 platform service facades from builder-produced
    /// configs and default stats.
    ///
    /// # Errors
    /// Returns [`ServerError`] if any facade fails to construct.
    pub fn new() -> Result<Self, ServerError> {
        // Build configs via each crate's builder (validates defaults).
        let agent_cfg = agent_runtime::builder::AgentRuntimeBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("agent-runtime: {e:?}")))?;

        let app_cfg = application_runtime::builder::AppRuntimeBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("application-runtime: {e:?}")))?;

        let registry_cfg = service_registry::builder::ServiceRegistryBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("service-registry: {e:?}")))?;

        let di_cfg = dependency_injection::builder::ContainerBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("dependency-injection: {e:?}")))?;

        let lifecycle_cfg = module_lifecycle::builder::LifecycleBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("module-lifecycle: {e:?}")))?;

        let middleware_cfg = middleware_pipeline::builder::MiddlewarePipelineBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("middleware-pipeline: {e:?}")))?;

        let config_cfg = configuration_system::builder::ConfigBuilder::new()
            .map_err(|e| ServerError::EngineInit(format!("configuration-system: {e:?}")))?
            .build()
            .map_err(|e| ServerError::EngineInit(format!("configuration-system: {e:?}")))?;

        let plugin_cfg = plugin_manager::builder::PluginManagerBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("plugin-manager: {e:?}")))?;

        let scheduler_cfg = scheduler_hooks::builder::SchedulerHooksBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("scheduler-hooks: {e:?}")))?;

        let auth_cfg = auth_framework::builder::AuthFrameworkBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("auth-framework: {e:?}")))?;

        let observability_cfg = observability_sdk::builder::ObservabilityBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("observability-sdk: {e:?}")))?;

        let quotas_cfg = resource_quotas::builder::ResourceQuotasBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("resource-quotas: {e:?}")))?;

        let feature_cfg = feature_flags::builder::FeatureFlagsBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("feature-flags: {e:?}")))?;

        let version_cfg = version_negotiation::builder::VersionNegotiationBuilder::new()
            .map_err(|e| ServerError::EngineInit(format!("version-negotiation: {e:?}")))?
            .build()
            .map_err(|e| ServerError::EngineInit(format!("version-negotiation: {e:?}")))?;

        let graceful_cfg = graceful_startup_shutdown::builder::GracefulBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("graceful-startup-shutdown: {e:?}")))?;

        let ops_cfg = ops_dashboard::builder::OpsDashboardBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("ops-dashboard: {e:?}")))?;

        // Construct facades from configs (+ default stats where needed).
        Ok(Self {
            agent: AgentRuntime::new(agent_cfg),
            app: AppRuntime::new(
                app_cfg,
                application_runtime::runtime_id::RuntimeId::new(1),
                AppRuntimeStats::default(),
            ),
            auth: AuthenticationPipeline::new(auth_cfg)
                .map_err(|e| ServerError::EngineInit(format!("auth-framework facade: {e:?}")))?,
            config_mgr: ConfigurationManager::new(config_cfg).map_err(|e| {
                ServerError::EngineInit(format!("configuration-system facade: {e:?}"))
            })?,
            di: Container::new(di_cfg),
            feature_flags: FeatureFlagManager::new(feature_cfg)
                .map_err(|e| ServerError::EngineInit(format!("feature-flags facade: {e:?}")))?,
            graceful: StartupShutdownCoordinator::new(graceful_cfg),
            middleware: Pipeline::new(middleware_cfg).map_err(|e| {
                ServerError::EngineInit(format!("middleware-pipeline facade: {e:?}"))
            })?,
            lifecycle: LifecycleCoordinator::new(lifecycle_cfg)
                .map_err(|e| ServerError::EngineInit(format!("module-lifecycle facade: {e:?}")))?,
            observability: MetricsRegistry::new(
                observability_cfg,
                observability_sdk::stats::ObservabilityStats::default(),
            ),
            ops: OpsDashboardService::new(
                ops_cfg,
                ops_dashboard::stats::OpsDashboardStats::default(),
            ),
            plugin: PluginManager::new(plugin_cfg)
                .map_err(|e| ServerError::EngineInit(format!("plugin-manager facade: {e:?}")))?,
            quotas: QuotaManager::new(
                quotas_cfg,
                resource_quotas::stats::ResourceQuotaStats::default(),
            ),
            scheduler_hooks: HookCoordinator::new(scheduler_cfg),
            registry: ServiceRegistry::new(registry_cfg)
                .map_err(|e| ServerError::EngineInit(format!("service-registry facade: {e:?}")))?,
            version: VersionNegotiator::new(version_cfg).map_err(|e| {
                ServerError::EngineInit(format!("version-negotiation facade: {e:?}"))
            })?,
        })
    }
}

// ── ApplicationLayer ──────────────────────────────────────────────

/// Application layer — owns the session layer, platform services,
/// and the application dispatcher.
pub struct ApplicationLayer {
    /// Session layer (owned — sits below application in the stack).
    session: SessionLayer,
    /// Platform infrastructure services.
    #[allow(dead_code)]
    platform: PlatformServices,
    /// Application dispatch boundary.
    dispatcher: VireonDispatcher,
}

impl ApplicationLayer {
    /// Creates a new application layer.
    pub fn new() -> Result<Self, ServerError> {
        Ok(Self {
            session: SessionLayer::new()?,
            platform: PlatformServices::new()?,
            dispatcher: VireonDispatcher::new(),
        })
    }

    /// Creates a session and notifies the application dispatcher.
    ///
    /// Returns `Some(SessionId)` on success, `None` if the session
    /// table or connection shard is full.
    pub fn on_connect(&mut self, conn_idx: usize, peer: SocketAddr) -> Option<SessionId> {
        let session_id = self.session.on_connect(conn_idx)?;

        // Notify application dispatcher.
        self.dispatcher.on_connection_established(session_id, peer);

        tracing::info!(
            conn_idx,
            session_id = ?session_id,
            session_occupancy = self.session.occupancy(),
            %peer,
            "session created"
        );

        Some(session_id)
    }

    /// Processes a frame through the session/stream layers, then
    /// dispatches to the application if it's a data frame.
    pub fn on_frame(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let action = self.session.on_frame(ctx.conn_idx, frame);

        if action == FrameAction::DispatchToApp {
            self.dispatcher.on_data_frame(ctx, frame);
        }
    }

    /// Drives the stream scheduler once per recv batch.
    /// Propagates through session → stream.
    pub fn on_batch_tick(&mut self) {
        self.session.on_batch_tick();
    }

    /// Drains queued outgoing frames produced by the dispatcher.
    ///
    /// Called by [`VireonStack`](crate::stack::VireonStack) after each
    /// `on_frame` batch; the returned frames are encoded and sent.
    pub fn drain_outbox(&mut self) -> Vec<OutgoingFrame> {
        self.dispatcher.drain_outbox()
    }

    /// Tears down the session and notifies the application dispatcher.
    pub fn on_close(&mut self, conn_idx: usize, session_id: SessionId, reason: Option<&str>) {
        let released = self.session.on_close(conn_idx);

        self.dispatcher.on_connection_closed(session_id, reason);

        tracing::info!(
            conn_idx,
            session_id = ?session_id,
            open_streams = released,
            reason = reason.unwrap_or("none"),
            "session destroyed"
        );
    }

    // ── Introspection ──────────────────────────────────────────────

    /// Returns the session table occupancy.
    #[must_use]
    pub fn session_occupancy(&self) -> usize {
        self.session.occupancy()
    }

    /// Returns a reference to the owned session layer.
    #[must_use]
    #[allow(dead_code)]
    pub const fn session(&self) -> &SessionLayer {
        &self.session
    }
}
