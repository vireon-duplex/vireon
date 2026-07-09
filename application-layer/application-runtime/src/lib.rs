//! # application-runtime
//!
//! **Top-level composition root** for the Vireon application layer.
//! Ties together all 20 application-layer crates into a single
//! runtime facade with unified configuration, health monitoring,
//! and lifecycle management.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   AppRuntimeBuilder ──► AppRuntime ──► CompositionRoot
//!                              │
//!                              ├─ 20 Application-Layer Crates
//!                              │   ├─ configuration-system
//!                              │   ├─ version-negotiation
//!                              │   ├─ feature-flags
//!                              │   ├─ scheduler-hooks
//!                              │   ├─ observability-sdk
//!                              │   ├─ resource-quotas
//!                              │   ├─ module-lifecycle
//!                              │   ├─ middleware-pipeline
//!                              │   ├─ plugin-manager
//!                              │   ├─ service-registry
//!                              │   ├─ auth-framework
//!                              │   ├─ dependency-injection
//!                              │   ├─ graceful-startup-shutdown
//!                              │   ├─ rpc-framework
//!                              │   ├─ event-bus
//!                              │   ├─ pubsub-engine
//!                              │   ├─ media-streaming
//!                              │   ├─ ai-streaming
//!                              │   ├─ agent-runtime
//!                              │   └─ ops-dashboard
//!                              │
//!                              └─ Lower-Layer Crates
//!                                  ├─ session-manager
//!                                  ├─ session-registry
//!                                  ├─ connection-shard
//!                                  ├─ stream-manager
//!                                  └─ reactor
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Layer map
//!
//! | #  | Module         | Phase | Responsibility |
//! |----|----------------|-------|----------------|
//! | 1  | [`error`]      | 0     | `AppRuntimeError` thiserror enum |
//! | 2  | [`config`]     | 0     | `AppRuntimeConfig`, `Preset` |
//! | 3  | [`runtime_id`] | 0     | `RuntimeId` newtype, `RuntimeRole` enum |
//! | 4  | [`stats`]      | 0     | `AppRuntimeStats` atomics, `StatsSnapshot` |
//! | 5  | [`health`]     | 0     | `HealthReport`, `HealthState` |
//! | 6  | [`builder`]    | 0     | `AppRuntimeBuilder` (fluent) |
//! | 7  | [`runtime`]    | 1     | `AppRuntime` facade |
//! | 8  | [`composition`]| 1     | `CompositionRoot` facade |
//!
//! # Phase 0 status
//!
//! Modules 1-6 are real implementations. Modules 7-8 ship as typed
//! skeletons. Functional runtime + composition land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod composition;
pub mod config;
pub mod error;
pub mod health;
pub mod runtime;
pub mod runtime_id;
pub mod stats;

// ── Re-exports (local) ──────────────────────────────────────────

pub use builder::AppRuntimeBuilder;
pub use composition::CompositionRoot;
pub use config::{AppRuntimeConfig, Preset};
pub use error::AppRuntimeError;
pub use health::{HealthReport, HealthState};
pub use runtime::AppRuntime;
pub use runtime_id::{RuntimeId, RuntimeRole};
pub use stats::{AppRuntimeStats, StatsSnapshot};

// ── Re-exports: Application-Layer Crates ────────────────────────

// 1. configuration-system
pub use configuration_system::{
    ConfigBuilder as ConfigurationSystemConfigBuilder, ConfigError, ConfigKey, ConfigLayer,
    ConfigSource, ConfigStats as ConfigurationSystemStats, ConfigValue, ConfigurationSystemConfig,
};

// 2. version-negotiation
pub use version_negotiation::{
    CompatEntry, CompatibilityMatrix, ProtocolVersion, VersionNegotiationBuilder,
    VersionNegotiationConfig, VersionNegotiationError, VersionNegotiationStats, VersionRange,
};

// 3. feature-flags
pub use feature_flags::{
    EvaluationContext, FeatureFlagManager, FeatureFlagsBuilder, FeatureFlagsConfig,
    FeatureFlagsStats, FlagError, FlagKey, FlagRule, FlagState, FlagValue, RuleCondition,
};

// 4. scheduler-hooks
pub use scheduler_hooks::{
    HookPriority, HookTrigger, SchedulerHookError, SchedulerHookId, SchedulerHookStats,
    SchedulerHooksBuilder, SchedulerHooksConfig,
};

// 5. observability-sdk
pub use observability_sdk::{
    LogLevel, LogRecord, MetricKey, MetricType, MetricValue, ObservabilityBuilder,
    ObservabilityConfig, ObservabilityError, ObservabilityStats, SpanId, SpanKind, TraceId,
};

// 6. resource-quotas
pub use resource_quotas::{
    QuotaError, QuotaLimit, QuotaUsage, ResourceQuotaStats, ResourceQuotasBuilder,
    ResourceQuotasConfig, TenantId, TenantTier, UsageSnapshot,
};

// 7. module-lifecycle
pub use module_lifecycle::{
    LifecycleBuilder, LifecycleConfig, ModuleDescriptor, ModuleId, ModuleLifecycleError,
    ModuleLifecycleStats, ModulePhase,
};

// 8. middleware-pipeline
pub use middleware_pipeline::{
    MiddlewareConfig, MiddlewareContext, MiddlewareError, MiddlewareId, MiddlewarePipelineBuilder,
    MiddlewareStage, MiddlewareStats,
};

// 9. plugin-manager
pub use plugin_manager::{
    ApiVersion, PluginDescriptor, PluginError, PluginId, PluginLoader, PluginManager,
    PluginManagerBuilder, PluginManagerConfig, PluginManagerStats, PluginVersion,
};

// 10. service-registry
pub use service_registry::{
    ServiceEndpoint, ServiceId, ServiceInfo, ServiceInstance, ServiceRegistryBuilder,
    ServiceRegistryConfig, ServiceRegistryError, ServiceRegistryStats, ServiceState, WatchHandle,
};

// 11. auth-framework
pub use auth_framework::{
    AuthConfig, AuthError, AuthFrameworkBuilder, AuthStats, AuthenticationPipeline,
    AuthorizationEngine, Credential, CredentialType, Permission, PermissionSet, PrincipalId,
    PrincipalType, Role, RoleAssignment, TokenManager,
};

// 12. dependency-injection
pub use dependency_injection::{
    BindingDescriptor, Container, ContainerBuilder, ContainerConfig, ContainerStats,
    DependencyError, DependencyResolver, ServiceLifetime, ServiceToken, TokenId,
};

// 13. graceful-startup-shutdown
pub use graceful_startup_shutdown::{
    ConnectionDrainer, GracefulBuilder, GracefulConfig, GracefulStats, PhaseBarrier, ShutdownError,
    ShutdownPhase, StartupPhase, StartupShutdownCoordinator,
};

// 14. rpc-framework
pub use rpc_framework::{
    MethodDispatcher, RpcClient, RpcConfig, RpcError, RpcFrameworkBuilder, RpcMethodId,
    RpcMethodType, RpcServer, RpcStats, RpcStatus, StreamHandle,
};

// 15. event-bus
pub use event_bus::{
    ConsumerGroupId, ConsumerId, EventBusBuilder, EventBusConfig, EventBusStats, EventError,
    Offset, OffsetStore, PartitionId, PartitionRebalancer, PartitionState, SubscriptionMode,
    TopicConfig, TopicId,
};

// 16. pubsub-engine
pub use pubsub_engine::{
    PubSubBuilder, PubSubConfig, PubSubEngine as PubSubEngineFacade, PubSubError, PubSubMessage,
    PubSubStats, SubscriptionId, TopicPattern, TopicTrie,
};

// 17. media-streaming
pub use media_streaming::{
    AdaptiveBitrateController, BitrateLadder, CodecConfig, CodecType, MediaError, MediaSession,
    MediaStreamingBuilder, MediaStreamingConfig, MediaStreamingStats, MediaTrackId, TrackDirection,
    TrackKind,
};

// 18. ai-streaming
pub use ai_streaming::{
    AiStreamBuilder, AiStreamConfig, AiStreamError, AiStreamId, AiStreamPipeline, AiStreamStats,
    StreamMode, TokenAggregator, TokenChunk, TokenType, ToolCall, ToolCallId, ToolStatus,
};

// 19. agent-runtime
pub use agent_runtime::{
    AgentContext, AgentExecutor, AgentId, AgentRuntime as AgentRuntimeFacade, AgentRuntimeBuilder,
    AgentRuntimeConfig, AgentRuntimeError, AgentRuntimeStats, AgentScheduler, AgentState,
    ToolDescriptor, ToolId, ToolResult,
};

// 20. ops-dashboard
pub use ops_dashboard::{
    DashboardWidgetId, MetricsCollector, OpsDashboardBuilder, OpsDashboardConfig,
    OpsDashboardError, OpsDashboardService, OpsDashboardStats, QueryAggregation, QueryRange,
    WidgetType,
};

// ── Re-exports: Lower-Layer Crates ──────────────────────────────

// session-manager
pub use session_manager::{
    QuotaSnapshot, ResourceQuota as SessionResourceQuota, SessionClass, SessionId,
    SessionManagerConfig, SessionManagerError, SessionState, SessionStats,
};

// session-registry
pub use session_registry::{
    Generation, LeaseInfo, LeaseState, NodeAddress, NodeCapacity, NodeId, NodeInfo, NodeState,
    OwnershipEntry, OwnershipTable, Region, RegistryConfig, RegistryError, RegistryStats,
    SessionLocator, Zone,
};

// connection-shard
pub use connection_shard::{
    BackpressureLevel, ConnectionId, ConnectionPriority, ConnectionShard, ConnectionShardBuilder,
    ConnectionSpec, ConnectionState, ShardConfig, ShardError,
};

// stream-manager
pub use stream_manager::{
    StreamId, StreamManagerConfig, StreamManagerError, StreamPriority,
    StreamState as StreamManagerStreamState, StreamType,
};

// reactor
pub use reactor::{
    CpuPinning, IdleStrategy, Reactor, ReactorBuilder, ReactorConfig, ReactorError, ReactorPreset,
    ReactorStats, TickBudget, TickResult,
};
