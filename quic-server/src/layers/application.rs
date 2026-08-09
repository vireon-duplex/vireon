//! Application layer — the outermost layer.
//!
//! Owns the `SessionLayer` and a [`VireonDispatcher`] that routes
//! data-plane frames to the pub/sub runtime:
//!
//! * `pubsub-engine` — topic-pattern subscriptions + fan-out
//! * `wal-store` — durable append + offset-based replay
//! * `auth-framework` — per-connection principal resolution
//! * `topic-acl` — per-publish / per-subscribe authorization
//!
//! Responses (acks, FetchReply frames, fan-out copies) are queued in an
//! outbox that [`VireonStack`](crate::stack::VireonStack) drains and
//! sends via the QUIC engine.
//!
//! ```text
//! ApplicationLayer
//!   ├─ SessionLayer (owns StreamLayer → MessageLayer)
//!   ├─ PlatformServices (observability + quotas)
//!   └─ VireonDispatcher
//!        ├─ TopicRegistry + TopicCatalog
//!        ├─ SendPolicyTable (per-stream egress policy)
//!        ├─ AuthenticationPipeline (per-connection principals)
//!        ├─ AclTable (topic-pattern authorization)
//!        ├─ Option<FileStore> (durable WAL)
//!        ├─ outbox: Vec<OutgoingFrame>
//!        └─ next_seq: per-QUIC-stream response seq
//! ```
//!
//! ## Adaptive Delivery Strategy
//!
//! The dispatcher selects a fan-out algorithm per-publish based on subscriber
//! count, configurable via `[delivery]` in the TOML config:
//!
//! ```text
//! Transport path (automatic, based on subscriber location):
//!
//!   ┌─────────────────┬──────────────────────────────────┐
//!   │ Subscriber loc  │ Transport                        │
//!   ├─────────────────┼──────────────────────────────────┤
//!   │ Same worker     │ Direct enqueue_frame             │
//!   │ Other worker    │ InterWorkerMsg via mpsc channel  │
//!   │ Other node      │ InterNodeMessage via UDP mesh    │
//!   └─────────────────┴──────────────────────────────────┘
//!
//! Fan-out strategy (adaptive, based on total subscriber count):
//!
//!   fan-out < logtail_threshold  →  BatchPush (encode once, clone per sub)
//!   fan-out ≥ logtail_threshold  →  LogTail (WAL append + NotifyOffset)
//! ```
//!
//! **BatchPush** encodes the payload once into `Bytes`, then each subscriber
//! gets a `.clone()` (Arc refcount bump — zero-copy). Optimal for small
//! fan-out where latency matters.
//!
//! **LogTail** appends to WAL, then sends a ~16-byte `NotifyOffset` frame per
//! subscriber. The subscriber auto-fetches the full payload via `Fetch`.
//! Reduces per-publish send-queue memory from `N × payload_size` to
//! `N × 16 bytes` — optimal for high fan-out (hundreds+ subscribers).

use arrayvec::ArrayVec;
use rustc_hash::FxHashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use auth_framework::authenticator::AuthenticationPipeline;
use cluster_coordinator::{
    ClusterConfig, ClusterCoordinator, InterNodeMessage, PublishDecision, RemoteSubscriber,
};
use consumer_groups::{ConsumerId, GroupCoordinator, GroupId};
use frame::byte_writer::ByteWriter;
use frame::codec::{FrameHeader, FrameRef};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use frame::payload::{
    Ack as AckPayload, FetchReplyPayload, ParsedPayload, ResumeOkPayload, ResumeOkSlot,
    ResumePayload, ResumeUnavailablePayload, SubscribePayload, TopicCreatePayload,
    TopicDeletePayload, UnsubscribePayload, parse_payload,
};
use observability_sdk::registry::MetricsRegistry;
use pubsub_engine::{
    PubSubConfig, Subscriber, TopicCatalog, TopicConfig, TopicPattern, TopicRegistry,
};
use resource_quotas::enforcer::QuotaEnforcer;
use send_policy::{
    DeliveryPolicy, DequeueOutcome, EnqueueDecision, FramePayload as SendFramePayload,
    SendPolicyTable, StreamOpenMeta,
};
use session_manager::SessionId;
use topic_acl::{AclDecision, AclTable, AclTableBuilder, Action, ActionSet, PrincipalId};
use wal_store::WalStore;

use auth_framework::{AuthConfig, Credential, CredentialType, PrincipalId as AuthPrincipalId};

use observability_sdk::{MetricKey, MetricType};
use resource_quotas::{QuotaLimit, TenantId, TenantTier};

use super::message::FrameAction;
use super::session::SessionLayer;
use crate::cluster::OutgoingMsg;
use crate::conf::LayerTuning;
use crate::error::ServerError;
use crate::replay::{ReplayConfig, ReplayEntry, ReplayOutcome};
use bytes::{Bytes, BytesMut};

// ── Inter-worker pub/sub forwarding ────────────────────────────────

/// Delivery algorithm selected per-publish based on fan-out count.
///
/// **BatchPush** — encode once, clone `Bytes` (Arc refcount bump) per
/// subscriber, enqueue directly. Optimal for small fan-out.
///
/// **LogTail** — append to WAL, send lightweight `NotifyOffset` (~16 bytes)
/// per subscriber; the subscriber auto-fetches the full payload via
/// `Fetch`. Reduces per-publish memory from `N × payload_size` to
/// `N × 16` bytes in send queues — optimal for large fan-out (hundreds+).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutStrategy {
    BatchPush,
    LogTail,
}

/// Resolved adaptive-delivery configuration. Controls when the server
/// switches from BatchPush to LogTail based on subscriber count.
#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    /// Subscriber count above which LogTail is used. Default 256.
    pub logtail_threshold: usize,
    /// Force a strategy regardless of subscriber count.
    pub force_strategy: crate::conf::DeliveryMode,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            logtail_threshold: 256,
            force_strategy: crate::conf::DeliveryMode::Auto,
        }
    }
}

impl DeliveryConfig {
    /// Selects the strategy for a publish given the total fan-out count
    /// and whether WAL is available.
    #[must_use]
    pub fn select(&self, total_fanout: usize, wal_available: bool) -> FanoutStrategy {
        match self.force_strategy {
            crate::conf::DeliveryMode::ForceBatch => FanoutStrategy::BatchPush,
            crate::conf::DeliveryMode::ForceLogTail => {
                if wal_available {
                    FanoutStrategy::LogTail
                } else {
                    tracing::warn!(
                        "[delivery] force_strategy=logtail but WAL is disabled — falling back to BatchPush"
                    );
                    FanoutStrategy::BatchPush
                }
            }
            crate::conf::DeliveryMode::Auto => {
                if wal_available && total_fanout >= self.logtail_threshold {
                    FanoutStrategy::LogTail
                } else {
                    FanoutStrategy::BatchPush
                }
            }
        }
    }
}

/// Messages exchanged between workers for cross-worker pub/sub.
///
/// **`Publish`** — a published frame forwarded to another worker for
/// fan-out to that worker's local subscribers. Receiving workers call
/// `deliver_framed_local` and do NOT re-broadcast (loop protection).
///
/// **`SubGained` / `SubLost`** — subscription-interest notifications.
/// When a worker's subscriber count for a pattern transitions 0↔non-zero,
/// it broadcasts the change so other workers can skip it during fan-out
/// if it has no matching subscribers. This avoids sending `Publish`
/// messages to workers that have nobody listening for the topic.
#[derive(Clone)]
pub enum InterWorkerMsg {
    /// Pre-encoded wire frame for delivery to local subscribers.
    /// `msg_type` distinguishes Publish (full payload) from NotifyOffset
    /// (LogTail — WAL offset pointer).
    Publish {
        framed: Bytes,
        msg_type: MessageType,
        /// Whether the original publish carried ACK_REQ — the receiving
        /// worker needs this to set `reliable_fanout` for replay retention.
        reliable: bool,
    },
    /// Sender `worker_idx` now has ≥1 subscriber for `pattern`.
    SubGained { worker_idx: u16, pattern: String },
    /// Sender `worker_idx` has 0 subscribers for `pattern`.
    SubLost { worker_idx: u16, pattern: String },
    /// Sender `worker_idx` gained a consumer-group member.
    /// Worker 0 uses this to maintain a global member registry.
    GroupGained {
        worker_idx: u16,
        topic: String,
        group: String,
        conn_idx: usize,
        stream_id: u64,
        consumer_id: String,
    },
    /// Sender `worker_idx` lost a consumer-group member.
    GroupLost {
        worker_idx: u16,
        topic: String,
        group: String,
        consumer_id: String,
    },
    /// Deliver a framed publish to one consumer-group member.
    /// `target = None`: worker 0 should decide and route.
    /// `target = Some(conn_idx, stream_id)`: deliver to this specific member.
    GroupDeliver {
        framed: Bytes,
        target: Option<(usize, u64)>,
        msg_type: MessageType,
    },
}

// ── Resume payload encoders ────────────────────────────────────────

/// Encodes a `ResumeOkPayload` into wire bytes for sending. Used by
/// [`VireonDispatcher::handle_resume`] to reply with the per-stream
/// `oldest_available` watermarks.
fn encode_resume_ok(slots: Vec<ResumeOkSlot>) -> Bytes {
    let p = ResumeOkPayload { streams: slots };
    let size = p.wire_size();
    let mut buf = BytesMut::with_capacity(size);
    // SAFETY: we resize to exactly `size` and `encode` writes precisely
    // that many bytes; the uninit between capacity and len never escapes.
    // BytesMut::resize zero-fills, which is a tiny waste but avoids any
    // unsafe here.
    buf.resize(size, 0);
    let mut w = ByteWriter::new(&mut buf);
    p.encode(&mut w);
    buf.freeze()
}

/// Encodes a `ResumeUnavailablePayload` into wire bytes for sending.
fn encode_resume_unavailable(slots: Vec<ResumeOkSlot>) -> Bytes {
    let p = ResumeUnavailablePayload(slots);
    let size = p.wire_size();
    let mut buf = BytesMut::with_capacity(size);
    buf.resize(size, 0);
    let mut w = ByteWriter::new(&mut buf);
    p.encode(&mut w);
    buf.freeze()
}

/// Topic-pattern match for dormant-session retention. Mirrors
/// `TopicPattern::matches` from `pubsub-engine` but operates on raw `&str`
/// (the dormant-session map stores `Box<str>` patterns, not constructed
/// `TopicPattern`s, to avoid validation overhead on the disconnect path).
/// Supports single-segment `*` wildcards; segment counts must match.
fn pattern_matches(pattern: &str, topic: &str) -> bool {
    let mut pat = pattern.split('.');
    let mut top = topic.split('.');
    loop {
        match (pat.next(), top.next()) {
            (Some(p), Some(t)) => {
                if p != "*" && p != t {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false, // different segment counts
        }
    }
}

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
    /// Payload bytes (refcounted for cheap cloning / zero-copy reuse).
    pub payload: Bytes,
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
    fn on_connection_closed(
        &mut self,
        conn_idx: usize,
        session_id: SessionId,
        reason: Option<&str>,
    );

    /// Drains queued outgoing frames (responses produced by the modules).
    fn drain_outbox(&mut self) -> Vec<OutgoingFrame>;
}

// ── Consumer-group local tracking ─────────────────────────────────

/// One local member of a consumer group (tracked per-connection).
struct GroupMember {
    conn_idx: usize,
    quic_stream_id: u64,
    consumer: ConsumerId,
}

/// Per-(topic, group) local state for round-robin delivery.
struct GroupLocal {
    members: Vec<GroupMember>,
    /// Round-robin index advanced on each publish.
    rr_idx: usize,
}

/// A consumer-group member tracked in worker 0's global registry.
/// Unlike `GroupMember`, this carries `worker_idx` so worker 0 knows
/// which worker owns the connection.
struct GlobalGroupMember {
    worker_idx: u16,
    conn_idx: usize,
    stream_id: u64,
    consumer_id: String,
}

// ── VireonDispatcher ──────────────────────────────────────────────

/// Per-connection metrics. Single-threaded (owned by one worker's
/// dispatcher), so plain `u64` — no atomics needed. Logged at close
/// time for observability; a future enhancement can aggregate them into
/// the shared [`ServerMetrics`] for the `/metrics` endpoint.
#[derive(Debug, Default, Clone)]
pub struct ConnMetrics {
    /// Publish frames received from this connection.
    pub publishes_in: u64,
    /// Delivery frames sent to this connection.
    pub deliveries_out: u64,
    /// Bytes received via publish payloads.
    pub bytes_in: u64,
    /// Bytes sent via delivery payloads.
    pub bytes_out: u64,
    /// Publishes rejected (ACL, quota, backpressure).
    pub publishes_rejected: u64,
}

impl ConnMetrics {
    /// Record an incoming publish of `payload_len` bytes.
    pub fn record_publish_in(&mut self, payload_len: usize) {
        self.publishes_in += 1;
        self.bytes_in += payload_len as u64;
    }

    /// Record an outgoing delivery of `payload_len` bytes.
    pub fn record_delivery_out(&mut self, payload_len: usize) {
        self.deliveries_out += 1;
        self.bytes_out += payload_len as u64;
    }

    /// Record a rejected publish.
    pub fn record_reject(&mut self) {
        self.publishes_rejected += 1;
    }
}

/// A subscription that may need forwarding to a remote topic owner.
/// Tracked for periodic re-broadcast so that a lost UDP datagram or a
/// late-forming cluster ring doesn't permanently break delivery.
#[derive(Clone)]
struct ForwardedSub {
    conn_idx: u32,
    stream_id: u64,
    pattern: String,
}

/// Production dispatcher that routes frames by [`MessageType`] to the
/// appropriate application-layer module(s).
///
/// Each module owns real in-memory state (Phase 1). The dispatcher
/// translates incoming frames into module calls and collects any
/// outgoing responses in an outbox.
pub struct VireonDispatcher {
    /// [application-layer] Trie-backed subscription registry (Phase 2 hot path).
    pubsub_registry: TopicRegistry,
    /// [application-layer] Created-topic catalog (Phase 2 metadata only).
    topic_catalog: TopicCatalog,

    /// Queued outgoing frames, drained by the stack after each recv.
    outbox: Vec<OutgoingFrame>,
    /// Reusable scratch for consumer-group fan-out targets.
    /// Avoids a `Vec::new()` allocation per publish in the group fan-out
    /// loop (handle_publish runs thousands of times/sec under load).
    group_targets_scratch: ArrayVec<(usize, u64), 8>,
    /// Reusable scratch for subscriber fan-out targets. Avoids a
    /// `Vec::new()` allocation per publish — the registry returns owned
    /// `Subscriber` values which we collect here, then iterate by ref.
    targets_scratch: Vec<pubsub_engine::Subscriber>,
    /// Reusable scratch for cross-worker inbox drain. Avoids
    /// `Vec::with_capacity(8192)` allocation per tick — the inbox is
    /// usually empty, and even when it's not the capacity grows once
    /// and is reused across calls.
    worker_inbox_scratch: Vec<InterWorkerMsg>,
    /// Reusable scratch for cluster inbox drain. Same rationale as
    /// `worker_inbox_scratch` — avoids `Vec::with_capacity(1024)` per
    /// tick when the cluster inbox is usually empty.
    cluster_inbox_scratch: Vec<InterNodeMessage>,
    /// Per-connection metrics (`conn_idx → ConnMetrics`). Single-threaded
    /// per worker. Removed on [`Self::on_close_conn`]. Logged at close.
    conn_metrics: FxHashMap<usize, ConnMetrics>,
    /// Per-(conn_idx, QUIC-stream) response sequence counter.
    next_seq: FxHashMap<(usize, u64), Seq>,

    /// [stream-layer] Per-stream send-side delivery policy. Bound when a
    /// subscriber's `StreamOpen` arrives — the policy byte declares how
    /// the subscriber wants the server to constrain egress on that stream
    /// (`LatestOnly`, `RealtimeDropOld`, etc.). Mirrors the receive-side
    /// `DeliveryPolicyTable` that lives in `quic-engine`.
    send_policy: SendPolicyTable,
    /// `(conn_idx, logical_stream_id)` → QUIC stream id, for rebuilding
    /// `OutgoingFrame`s after send-policy drains them. Populated alongside
    /// `send_policy` binds; cleared alongside removes. The logical id is
    /// the key (not the QUIC id) because `SendPolicyTable` is keyed by
    /// logical id and egress rebuild needs to recover the QUIC stream.
    egress_quic_streams: FxHashMap<(usize, StreamId), u64>,

    /// [application-layer] Topic-ACL table. Phase 7 ships with a single
    /// broad allow-all rule (`allow("*.*", None, ALL)`) so existing
    /// pub/sub traffic is unaffected; operators can swap in a stricter
    /// table at construction time. Real per-connection `PrincipalId`
    /// resolution lands via the `Handshake` arm in `on_data_frame` →
    /// `auth.authenticate` → stored in `principals`.
    acl: AclTable,
    /// Principal assumed for any connection that has not completed the
    /// auth handshake (no `Handshake` frame processed yet, or auth
    /// failed). The `acl` table's wildcard rules match any principal,
    /// so the specific value here only matters for non-wildcard rules.
    default_principal: PrincipalId,
    /// `[conn_idx]` → authenticated principal. Populated when a
    /// `Handshake` frame arrives and `AuthenticationPipeline::authenticate`
    /// succeeds; removed in `on_connection_closed`. Connections that
    /// never complete auth fall back to `default_principal` during
    /// ACL checks.
    principals: FxHashMap<usize, PrincipalId>,
    /// [application-layer] Authentication pipeline. Verifies the
    /// `(principal_id, credential_token)` carried by `Handshake`
    /// payloads. Pre-loaded with the configured default principal so
    /// single-tenant deployments that don't ship credentials still
    /// authenticate successfully.
    auth: AuthenticationPipeline,
    /// [application-layer] Optional WAL — when `Some`, every accepted
    /// publish is appended to a per-worker `FileStore` before fan-out,
    /// and `Fetch` requests can replay entries by offset. `None` means
    /// the operator did not pass `--wal-root`; publish/fetch work as
    /// before (no persistence, fetch returns an error ack). Each worker
    /// owns its own store rooted at `<root>/worker_<core_id>/` — this
    /// matches Vireon's thread-per-core design. Cross-worker sharing
    /// is a P5/P6 concern.
    wal: Option<wal_store::FileStore>,
    /// Where consumer-offsets snapshots are persisted. `None` when no
    /// WAL is configured (consumer positions are then ephemeral).
    /// Lives next to the WAL: `<wal_root>/worker_<id>/offsets.dat`.
    offsets_path: Option<std::path::PathBuf>,
    /// Per-(topic, consumer_id) ack tracking. Auto-updated when the
    /// server delivers to a group member so that on restart the
    /// consumer resumes from `last_acked.next()` rather than `OLDEST`.
    consumer_offsets: wal_store::ConsumerOffsets,

    /// [application-layer] Consumer-group coordinator — tracks group
    /// membership, generations, and partition assignment for load-
    /// balanced delivery. Publish fan-out consults `group_locals` to
    /// deliver each message to exactly one consumer per group (round-
    /// robin within the group's local members on this worker).
    groups: GroupCoordinator,
    /// Per-(topic, group_id) local member tracking. Keyed by the raw
    /// strings so publish fan-out can iterate groups for a topic without
    /// constructing typed wrappers on the hot path.
    group_locals: FxHashMap<(String, String), GroupLocal>,
    /// Global consumer-group member registry — only populated on worker 0.
    /// Maps (topic, group) → all members across ALL workers. Used by
    /// `do_group_delivery` for global round-robin so that each publish
    /// is delivered to exactly one member regardless of which worker
    /// the member's connection lives on.
    remote_group_members: FxHashMap<(String, String), Vec<GlobalGroupMember>>,
    /// Global round-robin counter per (topic, group). Only used by
    /// worker 0 in `do_group_delivery`.
    group_rr: FxHashMap<(String, String), usize>,
    /// Per-connection group memberships — used to leave all groups on
    /// disconnect. Each entry is `(topic, group_id, consumer_id)`.
    conn_group_keys: FxHashMap<usize, Vec<(String, String, ConsumerId)>>,

    // ── P6: multi-node cluster wiring ────────────────────────────
    /// The cluster coordinator — owns the topic router, remote
    /// subscriber registry, and cluster stats. `None` in single-node
    /// mode (all topics owned locally). When `Some`, this dispatcher
    /// is one of N (one per worker); all share the same `ClusterConfig`
    /// so routing decisions are deterministic across workers.
    coordinator: Option<ClusterCoordinator>,
    /// This node's id (cached for the fan-out path to stamp on every
    /// outgoing `InterNodeMessage`). `NodeId::NIL` when `coordinator`
    /// is `None`.
    node_id: session_registry::NodeId,
    /// Sender into the cluster transport's outgoing queue. Workers push
    /// [`OutgoingMsg`]s here to send inter-node UDP datagrams. `None`
    /// in single-node mode.
    cluster_outgoing_tx: Option<mpsc::Sender<OutgoingMsg>>,
    /// Per-worker incoming queue from the cluster transport. Each
    /// worker drains its own queue in `on_batch_tick` →
    /// [`Self::drain_cluster_inbox`]. `None` in single-node mode.
    cluster_incoming_rx: Option<mpsc::Receiver<InterNodeMessage>>,
    /// Outboxes to all workers for pub/sub fan-out. Index = worker
    /// reactor_id. The sender at `worker_self_idx` is skipped during
    /// broadcast. Empty in single-worker mode.
    worker_outboxes: Vec<mpsc::SyncSender<InterWorkerMsg>>,
    /// Inbox of forwarded publishes from other workers. Drained in
    /// [`Self::drain_worker_inbox`]. `None` in single-worker mode.
    worker_inbox: Option<mpsc::Receiver<InterWorkerMsg>>,
    /// This worker's reactor_id — used to skip self when broadcasting.
    worker_self_idx: usize,
    /// Last time a cluster heartbeat was broadcast. Worker 0 is the
    /// designated heart-beater; other workers skip the send to avoid
    /// N×redundant datagrams.
    last_heartbeat_send: Option<std::time::Instant>,
    /// Last time `tick_health` ran the failure detector. Throttled to
    /// `heartbeat_interval` to avoid scanning on every reactor tick.
    last_health_check: Option<std::time::Instant>,
    /// Backpressure signal from the Stack's `pending_sends` queue. Set by
    /// the Stack before `on_batch_tick` so `drain_worker_inbox` can stop
    /// accepting cross-worker publishes when subscriber QUIC streams are
    /// flow-controlled. Without this, multi-worker fan-out fills
    /// `pending_sends` to the 1 GiB byte cap, triggering eviction and
    /// silent frame drops (manifests as 0.3–0.5× fanout in broadcast benches).
    pending_sends_high: bool,
    /// Shared pressure flags — one per worker. This worker sets its own
    /// flag when `pending_sends_high` is true; reads others' flags to
    /// decide whether to forward cross-worker publishes or backpressure
    /// the publisher instead. Enables cross-worker backpressure without
    /// polling channel depth (which `SyncSender` doesn't expose).
    worker_pressure_flags: Vec<Arc<AtomicBool>>,
    /// Aggregate count of cross-worker inbox-full drops. Logged at most
    /// once every `WORKER_DROP_LOG_COOLDOWN` via [`worker_drop_cooldown`]
    /// — a sustained burst otherwise produces hundreds of identical WARN
    /// lines per second and drowns the rest of the log.
    worker_drop_count: u64,
    /// Time-based throttle for worker-inbox-full warnings.
    worker_drop_cooldown: crate::log_throttle::Cooldown,
    /// Which **remote** workers have subscribers for which patterns.
    /// Maps pattern → bitmap of interested worker indices (bit i = worker i).
    /// Used to skip workers without matching subscribers during cross-worker
    /// fan-out — avoids sending `Publish` to workers that have nobody
    /// listening for the topic.
    remote_pattern_interest: FxHashMap<pubsub_engine::TopicPattern, u32>,
    /// Cache: topic → worker bitmap for cross-worker fan-out. Avoids
    /// iterating `remote_pattern_interest` + calling `TopicPattern::matches`
    /// on every publish. Invalidated (cleared) on SubGained/SubLost.
    topic_worker_bitmap_cache: FxHashMap<String, u32>,
    /// Subscriptions forwarded to remote topic owners. Each entry records
    /// the (owner, conn_idx, stream_id, pattern) tuple needed to
    /// re-construct an `InterNodeMessage::Subscribe`. Re-broadcast on
    /// every heartbeat tick (1 s) to self-heal from lost UDP datagrams —
    /// inter-node Subscribe is fire-and-forget, so a single packet drop
    /// would otherwise cause a subscriber to receive 0 deliveries until
    /// manual re-subscribe.
    forwarded_subs: Vec<ForwardedSub>,
    /// ── per-frame error throttles ────────────────────────────────
    /// Persistently misbehaving clients (malformed payloads, denied
    /// topics, stuck WAL disk) can fire warn!/info! per inbound frame.
    /// Each counter yields one log line per N events.
    decode_drop: crate::log_throttle::RateCounter,
    acl_denied_drop: crate::log_throttle::RateCounter,
    wal_fail_drop: crate::log_throttle::RateCounter,
    malformed_drop: crate::log_throttle::RateCounter,
    quota_drop: crate::log_throttle::RateCounter,
    heartbeat_fail_drop: crate::log_throttle::RateCounter,
    /// Invalid subscribe pattern — usually a buggy client retry loop.
    invalid_pattern_drop: crate::log_throttle::RateCounter,
    /// Misbehaving client sends FetchReply on a non-server stream.
    fetch_reply_ignore_drop: crate::log_throttle::RateCounter,

    // ── WAL retention ────────────────────────────────────────────
    /// Active retention policy. Defaults to `keep_forever`; mutated
    /// via [`Self::set_wal_retention`]. The sweep is applied to every
    /// tracked topic when [`Self::wal_retention_last_run`] is older
    /// than `wal_sweep_interval`.
    wal_retention: wal_store::RetentionPolicy,
    /// Last time the retention sweep ran ( Instant::now()-based,
    /// throttled by `wal_sweep_interval` to amortise directory walks).
    wal_retention_last_run: Option<std::time::Instant>,
    /// Interval between WAL retention sweeps. Default 60 s.
    wal_sweep_interval: Duration,

    // ── Resource quotas ─────────────────────────────────────────
    /// Cap on total topics tracked by `topic_catalog`. `None` = unlimited.
    /// Enforced in `handle_topic_create` BEFORE catalog insertion.
    max_topics: Option<usize>,
    /// Cap on subscribers per distinct pattern string. `None` = unlimited.
    /// Tracked via `pattern_sub_counts`; enforced in `handle_subscribe`
    /// BEFORE registry insertion.
    max_subscribers_per_topic: Option<usize>,
    /// Per-pattern subscriber count (mirrors what's in `pubsub_registry`
    /// but keyed by pattern string for O(1) cap lookups). Incremented
    /// on successful subscribe, decremented on unsubscribe.
    pattern_sub_counts: FxHashMap<String, usize>,
    /// Total TopicCreate / Subscribe requests rejected by the caps.
    /// Logged via the periodic stats; surfaced in `[quota] reject` lines.
    quota_topic_rejects: u64,
    quota_subscriber_rejects: u64,

    // ── Reusable encode scratch ────────────────────────────────────
    // Eliminates per-publish Vec allocation in the fan-out path.
    // clear() + split().freeze() produces a Bytes without allocating.
    encode_scratch: BytesMut,
    /// Reusable scratch buffer for JSON ACK responses. Same clear +
    /// split().freeze() pattern as `encode_scratch` — zero allocation
    /// per ACK after warmup.
    ack_scratch: BytesMut,
    /// Reusable scratch for encoding LogTail `NotifyOffset` frames.
    /// Same clear + split().freeze() pattern — zero allocation after warmup.
    notify_scratch: BytesMut,

    // ── Adaptive delivery ──────────────────────────────────────────
    /// Resolved delivery-strategy configuration (thresholds + force mode).
    delivery_config: DeliveryConfig,
    /// Cumulative count of publishes routed via BatchPush.
    batch_push_count: u64,
    /// Cumulative count of publishes routed via LogTail.
    logtail_count: u64,

    /// Optional tamper-evident audit log. Records security-relevant
    /// events (ACL denies, quota rejections, topic lifecycle). `None`
    /// when `--audit-log` is not configured.
    audit_log: Option<crate::audit::AuditLog>,

    // ── Application-level reliability (ACK + Sequence + Resume) ──────
    /// Per-worker in-memory replay buffer. Retains reliable (`ACK_REQ`)
    /// Publish frames keyed by `(logical_session_id, StreamId)` so a
    /// reconnecting client can resume from its last-acked sequence.
    /// Wrapped in `Arc<Mutex<SharedReplayState>>` so multi-worker
    /// deployments can share a single buffer + dormant-session map —
    /// a reconnecting client may land on a different worker than the
    /// original connection, and the replay entries must be findable
    /// regardless. Lock contention is negligible (reliability
    /// operations are opt-in and rare compared to the ephemeral hot path).
    replay: Arc<std::sync::Mutex<crate::replay::SharedReplayState>>,
    /// `conn_idx → client-assigned logical session id`. Populated when a
    /// `Resume` frame arrives; cleared in `on_connection_closed`. The
    /// logical session outlives the QUIC connection: a reconnecting
    /// client re-sends `Resume` with the same id, and the new conn_idx
    /// is rebound to it.
    conn_logical_session: FxHashMap<usize, u64>,
    /// Transient: set to `true` only while `handle_publish` is in its
    /// fan-out section AND the incoming publish carried `ACK_REQ`. Read
    /// by [`Self::enqueue_frame`] to stamp `ACK_REQ` on outgoing
    /// deliveries and to retain a copy in the replay buffer (when the
    /// subscriber has a registered logical session). Always `false`
    /// outside `handle_publish`, so `send_ack` paths and cross-worker /
    /// cross-cluster drain paths (which run later from `on_batch_tick`)
    /// are NOT retained — that is the documented multi-worker resume
    /// limitation.
    reliable_fanout: bool,
    /// Per-`(logical_session_id, stream)` last-seen cumulative ack seq.
    /// Used so a subsequent `Resume` from the same session (without an
    /// intervening reconnect) can be answered from the buffered window
    /// without forcing the client to re-send its ack position.
    session_last_acked: FxHashMap<(u64, StreamId), u64>,
    /// Shared server-wide metrics counters (Prometheus `/metrics` endpoint).
    /// `None` in benches/tests; set by `VireonStack` at worker startup so
    /// the reliability-layer handlers can bump `resume_*` / `messages_*`
    /// outcome counters directly.
    metrics: Option<Arc<crate::metrics::ServerMetrics>>,
    /// `(conn_idx, quic_stream_id) → pending replay frames` for Resume
    /// replays targeting a QUIC stream the client hasn't opened yet.
    /// Flushed to the outbox when the matching StreamOpen arrives.
    pending_replays: FxHashMap<(usize, u64), Vec<OutgoingFrame>>,
}

/// Minimum gap between cross-worker drop warning logs.
const WORKER_DROP_LOG_COOLDOWN: Duration = Duration::from_secs(5);

impl VireonDispatcher {
    /// Creates a new dispatcher with default-configured modules.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pubsub_registry: TopicRegistry::new(PubSubConfig::new()),
            topic_catalog: TopicCatalog::new(),
            outbox: Vec::new(),
            group_targets_scratch: ArrayVec::new(),
            targets_scratch: Vec::new(),
            worker_inbox_scratch: Vec::new(),
            cluster_inbox_scratch: Vec::new(),
            conn_metrics: FxHashMap::default(),
            next_seq: FxHashMap::default(),
            send_policy: SendPolicyTable::new(),
            egress_quic_streams: FxHashMap::default(),
            // Permissive default: any principal can do anything on any
            // two-segment topic. Operators swap this out via
            // [`Self::with_acl`] when enforcing real authorization.
            // `build()` only fails on accumulated rule errors, but the
            // builder already logs-and-skips malformed rules, so the
            // worst case here is an empty table (deny-by-default) —
            // still a valid state, not worth panicking over.
            acl: AclTableBuilder::new()
                .allow("*.*", None, ActionSet::ALL)
                .build()
                .unwrap_or_default(),
            default_principal: PrincipalId::new(1),
            principals: FxHashMap::default(),
            // AuthConfig::new() is a `const fn` that always validates;
            // the only failure mode is misconfiguration (TTL out of
            // range), which can't happen with the default. Falling back
            // to an empty pipeline still works — every `authenticate`
            // returns `UnknownPrincipal`, so connections fall back to
            // `default_principal`.
            auth: AuthenticationPipeline::new(AuthConfig::new())
                .unwrap_or_else(|_| AuthenticationPipeline::default()),
            wal: None,
            groups: GroupCoordinator::new(),
            group_locals: FxHashMap::default(),
            remote_group_members: FxHashMap::default(),
            group_rr: FxHashMap::default(),
            conn_group_keys: FxHashMap::default(),
            coordinator: None,
            node_id: session_registry::NodeId::NIL,
            cluster_outgoing_tx: None,
            cluster_incoming_rx: None,
            worker_outboxes: Vec::new(),
            worker_inbox: None,
            worker_self_idx: 0,
            last_heartbeat_send: None,
            last_health_check: None,
            pending_sends_high: false,
            worker_pressure_flags: Vec::new(),
            worker_drop_count: 0,
            worker_drop_cooldown: crate::log_throttle::Cooldown::new(WORKER_DROP_LOG_COOLDOWN),
            remote_pattern_interest: FxHashMap::default(),
            topic_worker_bitmap_cache: FxHashMap::default(),
            forwarded_subs: Vec::new(),
            // Per-class throttles. `every` values chosen so that under
            // a sustained ~5k frames/s/conn burst we still see ~5
            // log lines/s per category — enough to diagnose without
            // flooding. First occurrence always logs (RateCounter
            // semantics).
            decode_drop: crate::log_throttle::RateCounter::new(256),
            acl_denied_drop: crate::log_throttle::RateCounter::new(256),
            wal_fail_drop: crate::log_throttle::RateCounter::new(128),
            malformed_drop: crate::log_throttle::RateCounter::new(256),
            quota_drop: crate::log_throttle::RateCounter::new(64),
            heartbeat_fail_drop: crate::log_throttle::RateCounter::new(64),
            invalid_pattern_drop: crate::log_throttle::RateCounter::new(64),
            fetch_reply_ignore_drop: crate::log_throttle::RateCounter::new(128),
            wal_retention: wal_store::RetentionPolicy::keep_forever(),
            wal_retention_last_run: None,
            wal_sweep_interval: Duration::from_secs(60),
            offsets_path: None,
            consumer_offsets: wal_store::ConsumerOffsets::new(),
            max_topics: None,
            max_subscribers_per_topic: None,
            pattern_sub_counts: FxHashMap::default(),
            quota_topic_rejects: 0,
            quota_subscriber_rejects: 0,
            encode_scratch: BytesMut::new(),
            ack_scratch: BytesMut::new(),
            notify_scratch: BytesMut::new(),
            delivery_config: DeliveryConfig::default(),
            batch_push_count: 0,
            logtail_count: 0,
            audit_log: None,
            replay: Arc::new(std::sync::Mutex::new(
                crate::replay::SharedReplayState::new(ReplayConfig::default()),
            )),
            conn_logical_session: FxHashMap::default(),
            reliable_fanout: false,
            session_last_acked: FxHashMap::default(),
            metrics: None,
            pending_replays: FxHashMap::default(),
        }
    }

    /// Sets the WAL retention sweep interval.
    #[must_use]
    pub fn with_wal_sweep_interval(mut self, interval: Duration) -> Self {
        self.wal_sweep_interval = interval;
        self
    }

    /// Sets the adaptive-delivery configuration (thresholds + force mode).
    #[must_use]
    pub fn with_delivery_config(mut self, cfg: DeliveryConfig) -> Self {
        self.delivery_config = cfg;
        self
    }

    /// Sets the reliability-layer replay buffer configuration (message /
    /// byte caps). Called once at worker startup from the `[replay]` TOML
    /// section; defaults are sane so most deployments omit this.
    #[must_use]
    pub fn with_replay_config(mut self, cfg: ReplayConfig) -> Self {
        self.replay = Arc::new(std::sync::Mutex::new(
            crate::replay::SharedReplayState::new(cfg),
        ));
        self
    }

    /// Attaches the shared server-wide metrics counters so the
    /// reliability-layer handlers can bump `resume_*` / `messages_*`
    /// outcome counters directly. Called once at worker startup; `None`
    /// in benches/tests (counters simply aren't recorded).
    #[must_use]
    #[allow(dead_code)]
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::ServerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Post-construction variant of [`Self::with_metrics`] for layers that
    /// receive the metrics `Arc` after the dispatcher has already been
    /// built (the production path: `VireonStack::new` → `worker.rs`).
    pub fn set_metrics(&mut self, metrics: Arc<crate::metrics::ServerMetrics>) {
        self.metrics = Some(metrics);
    }

    /// Replaces the per-worker replay buffer with a shared one, so all
    /// workers in a multi-worker deployment can find replay entries
    /// regardless of which worker the original subscriber landed on.
    /// Called once at worker startup when `num_workers > 1`.
    #[allow(dead_code)]
    pub fn set_shared_replay(
        &mut self,
        shared: Arc<std::sync::Mutex<crate::replay::SharedReplayState>>,
    ) {
        self.replay = shared;
    }

    /// Snapshots the replay-buffer occupancy into the `replay_window_bytes`
    /// and `replay_window_messages` Prometheus gauges. Called once per
    /// batch tick from [`ApplicationLayer::on_batch_tick`]; no-op when no
    /// metrics store is attached (benches / unit tests).
    pub fn publish_replay_gauges(&self) {
        if let Some(m) = &self.metrics {
            let rs = self.replay.lock().unwrap();
            m.replay_window_bytes
                .store(rs.buffer.total_bytes() as u64, Ordering::Relaxed);
            m.replay_window_messages
                .store(rs.buffer.total_messages() as u64, Ordering::Relaxed);
        }
    }

    /// Builds a response header with an incrementing per-stream seq and
    /// enqueues it in the outbox.
    fn enqueue_response(
        &mut self,
        ctx: &FrameContext,
        logical_stream: StreamId,
        msg_type: MessageType,
        payload: Bytes,
    ) {
        self.enqueue_frame(
            ctx.conn_idx,
            ctx.quic_stream_id,
            logical_stream,
            msg_type,
            payload,
        );
    }

    /// Assembles a JSON ACK from literal + dynamic parts into the
    /// reusable `ack_scratch` buffer and returns it as `Bytes`.
    ///
    /// The `split().freeze()` pattern reuses the buffer's capacity —
    /// zero allocation per ACK after warmup (unlike `format!` which
    /// allocates a new String every call).
    fn ack_parts(&mut self, parts: &[&[u8]]) -> Bytes {
        self.ack_scratch.clear();
        for p in parts {
            self.ack_scratch.extend_from_slice(p);
        }
        self.ack_scratch.split().freeze()
    }

    /// Convenience: enqueues a JSON ACK built from `parts` as the
    /// response payload for `msg_type`.
    fn send_ack(
        &mut self,
        ctx: &FrameContext,
        logical_stream: StreamId,
        msg_type: MessageType,
        parts: &[&[u8]],
    ) {
        let payload = self.ack_parts(parts);
        self.enqueue_response(ctx, logical_stream, msg_type, payload);
    }

    /// Enqueues a frame targeting an explicit connection + QUIC stream.
    /// Used for both same-connection responses and cross-connection delivery.
    ///
    /// When [`Self::reliable_fanout`] is `true` (set only inside the
    /// fan-out section of `handle_publish` while the incoming publish
    /// carried `ACK_REQ`) and `msg_type` is `Publish`:
    ///   * the outgoing header is stamped with `FrameFlags::ACK_REQ` so
    ///     the subscriber knows to send a cumulative `Ack`, and
    ///   * if the subscriber's conn has a registered logical session,
    ///     a [`ReplayEntry`] copy is retained in the replay buffer so a
    ///     future `Resume` from that session can re-send it.
    fn enqueue_frame(
        &mut self,
        conn_idx: usize,
        quic_stream_id: u64,
        logical_stream: StreamId,
        msg_type: MessageType,
        payload: Bytes,
    ) {
        // Record per-connection delivery metrics for publish deliveries.
        if msg_type == MessageType::Publish {
            let plen = payload.len();
            self.conn_metrics
                .entry(conn_idx)
                .or_default()
                .record_delivery_out(plen);
        }
        let seq = self
            .next_seq
            .entry((conn_idx, quic_stream_id))
            .or_insert(Seq::ZERO);
        let seq_val = *seq;
        // Reliable delivery: stamp ACK_REQ on outgoing Publish headers so
        // the subscriber knows to ack. Retain in the replay buffer only
        // when the subscriber has registered a logical session.
        let reliable = self.reliable_fanout && msg_type == MessageType::Publish;
        let logical = if reliable {
            self.conn_logical_session.get(&conn_idx).copied()
        } else {
            None
        };
        let header = if reliable {
            FrameHeader::new(logical_stream, msg_type)
                .with_seq(seq_val)
                .with_flags(FrameFlags::ACK_REQ)
        } else {
            FrameHeader::new(logical_stream, msg_type)
                .with_seq(seq_val)
                .with_flags(FrameFlags::NONE)
        };
        *seq = seq_val.next();
        if let Some(logical) = logical {
            // Key the replay ring by the subscriber's quic_stream_id (not
            // the publisher's logical stream). The client keys its ACK
            // and Resume state by the QUIC stream id it receives on, so
            // the replay lookup must use the same key to match.
            let mut rs = self.replay.lock().unwrap();
            rs.session_stream_seq
                .insert((logical, quic_stream_id), seq_val.next());
            rs.buffer.append(
                logical,
                StreamId::new(quic_stream_id),
                ReplayEntry {
                    seq: seq_val.get(),
                    header: header.clone(),
                    payload: payload.clone(),
                },
            );
        }
        self.outbox.push(OutgoingFrame {
            conn_idx,
            quic_stream_id,
            header,
            payload,
        });
    }

    // ── ACL ────────────────────────────────────────────────────────

    /// Replace the built-in permissive ACL with a custom table. Call
    /// before the dispatcher sees its first frame (e.g. at stack
    /// construction time); later calls are also safe but won't affect
    /// rules already evaluated.
    ///
    /// The default table ships with one rule: `allow("*.*", None, ALL)`.
    /// Swap in a stricter table when the deployment has known topic
    /// namespaces (e.g. `deny("system.*", None, ALL)` then
    /// `allow(<tenant-ns>.*, None, pub_sub)`).
    ///
    /// Used by integration benches in `benches/pubsub_dispatch.rs`. The
    /// production binary path goes through `ApplicationLayer::set_acl`
    /// at startup; the binary crate's view of this method therefore
    /// looks dead, hence the allow.
    #[allow(dead_code)]
    pub fn with_acl(mut self, table: AclTable) -> Self {
        self.acl = table;
        self
    }

    /// Attaches a WAL backend. When set, every accepted `Publish` is
    /// appended to the store before fan-out so `Fetch` requests can
    /// replay entries by offset.
    #[allow(dead_code)]
    pub fn with_wal(mut self, wal: wal_store::FileStore) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Sets the WAL backend in place (non-builder form).
    #[allow(dead_code)]
    pub fn set_wal(&mut self, wal: wal_store::FileStore) {
        self.wal = Some(wal);
    }

    /// Sets the loaded consumer-offsets map + the on-disk path the
    /// periodic sweeper should write snapshots to. Called once during
    /// startup after [`Self::set_wal`].
    pub fn set_consumer_offsets(
        &mut self,
        offsets: wal_store::ConsumerOffsets,
        path: std::path::PathBuf,
    ) {
        self.consumer_offsets = offsets;
        self.offsets_path = Some(path);
    }

    /// Persists consumer offsets to disk immediately. Called from the
    /// shutdown path (`VireonStack` entering `Draining`) so the last
    /// batch of acks isn't lost between periodic sweeps.
    pub fn save_offsets(&self) {
        if let Some(path) = self.offsets_path.as_ref() {
            if let Err(e) = self.consumer_offsets.save_to(path) {
                tracing::warn!(
                    path = ?path,
                    error = %e,
                    "[shutdown] consumer-offsets snapshot failed — previous snapshot retained"
                );
            }
        }
    }

    /// Attaches an audit log. Once set, security-relevant events are
    /// hash-chained to the log.
    pub fn set_audit_log(&mut self, log: crate::audit::AuditLog) {
        self.audit_log = Some(log);
    }

    /// Sets the retention policy applied to the WAL by the periodic
    /// sweep in [`ApplicationLayer::on_batch_tick`]. Replaces any
    /// prior policy. The sweep is a no-op when WAL is `None` or the
    /// policy is `keep_forever`.
    pub fn set_wal_retention(&mut self, policy: wal_store::RetentionPolicy) {
        self.wal_retention = policy;
    }

    /// Cap the total number of topics tracked in the catalog. TopicCreate
    /// requests that would exceed the cap are rejected with an error ack.
    /// `None` (default) = unlimited.
    pub fn set_max_topics(&mut self, max: Option<usize>) {
        self.max_topics = max;
    }

    /// Cap the number of subscribers per distinct subscription pattern.
    /// Subscribe requests whose pattern already has `max` subscribers
    /// are rejected with an error ack. `None` (default) = unlimited.
    pub fn set_max_subscribers_per_topic(&mut self, max: Option<usize>) {
        self.max_subscribers_per_topic = max;
    }

    /// Attaches a cluster coordinator + transport channels (P6).
    ///
    /// When set, the dispatcher consults the coordinator's
    /// `route_publish` for every publish and forwards non-owned topics
    /// to their owner via [`InterNodeMessage::Publish`]. It also drains
    /// its incoming cluster queue on each `on_batch_tick` to process
    /// publishes/fanouts/subscribes from other nodes.
    ///
    /// Each worker calls this with its own coordinator copy (same
    /// config → same routing decisions).
    #[allow(dead_code)]
    pub fn set_cluster(
        &mut self,
        cfg: &ClusterConfig,
        outgoing_tx: mpsc::Sender<OutgoingMsg>,
        incoming_rx: mpsc::Receiver<InterNodeMessage>,
    ) {
        match ClusterCoordinator::new(cfg.clone()) {
            Ok(coord) => {
                self.node_id = coord.self_id();
                self.coordinator = Some(coord);
                self.cluster_outgoing_tx = Some(outgoing_tx);
                self.cluster_incoming_rx = Some(incoming_rx);
            }
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    "[cluster] coordinator construction failed — running in single-node mode"
                );
            }
        }
    }

    /// Installs inter-worker pub/sub channels for cross-worker fan-out.
    ///
    /// When set, every local publish is broadcast to all other workers
    /// via `try_send`. Receiving workers drain their inbox in
    /// `on_batch_tick` → [`Self::drain_worker_inbox`] and deliver to
    /// their local subscribers.
    pub fn set_worker_channels(
        &mut self,
        self_idx: usize,
        inbox: mpsc::Receiver<InterWorkerMsg>,
        outboxes: Vec<mpsc::SyncSender<InterWorkerMsg>>,
    ) {
        self.worker_self_idx = self_idx;
        self.worker_inbox = Some(inbox);
        self.worker_outboxes = outboxes;
        // Initialize one shared pressure flag per worker. Each worker sets
        // its own flag when its pending_sends is high; other workers read
        // it to apply publisher-side backpressure before forwarding.
        let n = self.worker_outboxes.len();
        self.worker_pressure_flags = (0..n).map(|_| Arc::new(AtomicBool::new(false))).collect();
    }

    /// Evaluates `(conn_idx's principal, action, topic_or_pattern)` against
    /// the ACL table. If `conn_idx` does not have an authenticated
    /// principal (no `Handshake` processed yet, or auth failed) the
    /// dispatcher's `default_principal` is used.
    ///
    /// `topic_or_pattern` is the raw wire string for Subscribe (a
    /// pattern like `sensor.*`) and the concrete topic name for
    /// Publish / TopicCreate / TopicDelete. The ACL `pattern_matches`
    /// function interprets both uniformly.
    #[inline]
    pub fn check_acl(
        &self,
        conn_idx: usize,
        action: Action,
        topic_or_pattern: &str,
    ) -> AclDecision {
        let principal = self
            .principals
            .get(&conn_idx)
            .copied()
            .unwrap_or(self.default_principal);
        self.acl.check(principal, action, topic_or_pattern)
    }

    /// Returns the principal currently bound to `conn_idx`, or
    /// `default_principal` if the connection hasn't completed the
    /// auth handshake. Used by the connection registry + tracing.
    #[must_use]
    #[allow(dead_code)]
    pub fn principal_for(&self, conn_idx: usize) -> PrincipalId {
        self.principals
            .get(&conn_idx)
            .copied()
            .unwrap_or(self.default_principal)
    }

    /// Registers a credential with the auth pipeline. The dispatcher
    /// calls this for every principal that should be able to complete
    /// the handshake; in single-tenant deployments the caller registers
    /// just the default principal with a known credential.
    #[allow(dead_code)]
    pub fn register_principal(&mut self, principal: PrincipalId, credential: Credential) {
        // `auth_framework::PrincipalId` and `topic_acl::PrincipalId`
        // are the same type — topic_acl re-exports auth_framework's.
        // The import alias keeps the call site unambiguous.
        let _ = AuthPrincipalId::new(principal.raw());
        self.auth
            .register_principal(AuthPrincipalId::new(principal.raw()), credential);
    }

    // ── Send-policy lifecycle ──────────────────────────────────────

    /// Binds the send-side delivery policy for `(conn_idx, logical_stream)`.
    ///
    /// Called when a subscriber's `StreamOpen` arrives — the 1-byte
    /// payload declares the policy the subscriber wants the server to
    /// apply on egress (`LatestOnly`, `RealtimeDropOld`, etc.). After
    /// this call, every `OutgoingFrame` the dispatcher enqueues for
    /// this stream is routed through [`SendPolicyTable::enqueue`] and
    /// emerges later from [`Self::drain_ready_egress`] in policy order.
    ///
    /// `quic_stream_id` is recorded alongside the bind so the rebuild
    /// step after dequeue can reconstruct an `OutgoingFrame` with the
    /// correct transport target.
    ///
    /// Any frames that were lazy-defaulted into the table before this
    /// bind are flushed (returned from `SendPolicyTable::bind`) and
    /// re-enqueued into the outbox so they are not silently lost —
    /// they'll be routed through the (now-known) policy on the next
    /// `drain_outbox` pass.
    pub fn bind_send_policy(
        &mut self,
        conn_idx: usize,
        quic_stream_id: u64,
        logical_stream: StreamId,
        policy: DeliveryPolicy,
    ) {
        match self.send_policy.bind(conn_idx, logical_stream, policy) {
            Ok(flushed) => {
                // Lazy-default frames (or prior-policy state) were
                // displaced. Re-enqueue each as a direct OutgoingFrame
                // so it goes through the stack's encode+send. These
                // pre-date the bind so they bypass policy.
                for fp in flushed {
                    self.outbox.push(OutgoingFrame {
                        conn_idx,
                        quic_stream_id,
                        header: FrameHeader::new(logical_stream, fp.msg_type)
                            .with_seq(fp.seq)
                            .with_flags(fp.flags),
                        payload: fp.bytes,
                    });
                }
            }
            Err(e) => {
                tracing::warn!(
                    conn_idx,
                    stream_id = logical_stream.get(),
                    ?policy,
                    error = ?e,
                    "[send-policy] bind failed — stream will lazy-default"
                );
            }
        }
        // Always record the quic_stream_id even if the bind failed: a
        // later lazy-default `enqueue` will still need to recover it
        // when drain_ready_egress rebuilds the OutgoingFrame.
        self.egress_quic_streams
            .insert((conn_idx, logical_stream), quic_stream_id);
        // Flush any pending resume-replay frames for this stream. The
        // Resume (sent on the default stream) may have arrived before the
        // client opened this dedicated stream, so the replayed frames were
        // buffered until now.
        if let Some(pending) = self.pending_replays.remove(&(conn_idx, quic_stream_id)) {
            tracing::debug!(
                conn_idx,
                quic_stream_id,
                count = pending.len(),
                "[reliability] flushing pending replay frames on StreamOpen"
            );
            self.outbox.extend(pending);
        }
    }

    /// Removes send-policy state for a single stream. Called when the
    /// subscriber closes or resets the stream. Any frames still queued
    /// are discarded — the receiver is tearing down, so further sends
    /// would be ignored anyway.
    fn remove_send_policy(&mut self, conn_idx: usize, logical_stream: StreamId) {
        self.send_policy.remove_stream(conn_idx, logical_stream);
        self.egress_quic_streams.remove(&(conn_idx, logical_stream));
    }

    /// Removes send-policy state for every stream on a connection.
    /// Called when the QUIC connection closes.
    fn remove_connection_send_policy(&mut self, conn_idx: usize) {
        self.send_policy.remove_connection(conn_idx);
        self.egress_quic_streams.retain(|&(c, _), _| c != conn_idx);
    }

    // ── Send-policy egress ─────────────────────────────────────────

    /// Routes an outgoing frame through the send policy if the stream
    /// is bound; otherwise hands the frame back so the caller can send
    /// it directly.
    ///
    /// Returns `None` when the frame was consumed by the policy table
    /// (caller does nothing further this tick — the frame will emerge
    /// from `drain_ready_egress` in policy order on this or a later
    /// tick). Returns `Some(out)` when the stream is not bound to a
    /// policy — the caller then calls `encode_and_send` directly.
    /// Control frames and unbound subscriber streams take this fast path.
    ///
    /// The FramePayload the table stores preserves `msg_type`, `flags`,
    /// and `seq` so the rebuild step can reconstruct a wire-correct
    /// `OutgoingFrame`. The payload bytes are refcounted (`Bytes`), so
    /// `LatestOnly`'s replace-displace and `RealtimeDropOld`'s drop are
    /// cheap.
    pub fn enqueue_egress(&mut self, out: OutgoingFrame) -> Option<OutgoingFrame> {
        let key = (out.conn_idx, out.header.stream_id);
        if !self
            .send_policy
            .contains(out.conn_idx, out.header.stream_id)
        {
            return Some(out);
        }
        let payload = SendFramePayload::new(
            out.header.msg_type,
            out.header.flags,
            out.header.seq,
            out.payload,
        );
        // Record the quic_stream_id before enqueue so drain_ready_egress
        // can rebuild the OutgoingFrame even if this is the first frame
        // on a lazy-default stream.
        self.egress_quic_streams.insert(key, out.quic_stream_id);
        match self
            .send_policy
            .enqueue(out.conn_idx, out.header.stream_id, payload)
        {
            EnqueueDecision::Queue | EnqueueDecision::Replace(_) => None,
            EnqueueDecision::Drop => {
                // Policy dropped the frame (e.g. `RealtimeDropOld`
                // rejected a stale seq, or `LatestOnly` displaced an
                // older pending frame). The drop is already counted in
                // send-policy metrics; nothing else to do here.
                None
            }
        }
    }

    /// Drains every policy-cleared frame across all connections, in
    /// unspecified order. The caller (stack) sends each via the normal
    /// `encode_and_send` path.
    ///
    /// Called once per batch after `drain_outbox` has routed frames
    /// through the policy. Frames that the policy is holding back
    /// (e.g. an older `LatestOnly` slot waiting to be displaced by a
    /// newer arrival) stay in the table and may emerge on a later call.
    pub fn drain_ready_egress(&mut self, out: &mut Vec<OutgoingFrame>) {
        // Collect ready keys into a local vec first so we don't hold an
        // immutable borrow of `send_policy` across the mutable calls.
        let mut ready: Vec<(usize, StreamId)> = Vec::new();
        self.send_policy.ready_streams_all(&mut ready);
        if ready.is_empty() {
            return;
        }
        for (conn_idx, stream_id) in ready {
            while let DequeueOutcome::Some(fp) = self.send_policy.dequeue(conn_idx, stream_id) {
                // Look up the quic_stream_id recorded at bind or
                // enqueue time. If missing (e.g. the stream was
                // removed between enqueue and drain), drop the
                // frame — the receiver is gone.
                let Some(&quic_stream_id) = self.egress_quic_streams.get(&(conn_idx, stream_id))
                else {
                    continue;
                };
                out.push(OutgoingFrame {
                    conn_idx,
                    quic_stream_id,
                    header: FrameHeader::new(stream_id, fp.msg_type)
                        .with_seq(fp.seq)
                        .with_flags(fp.flags),
                    payload: fp.bytes,
                });
            }
        }
    }

    /// Routes a native pub/sub control frame (`Subscribe`/`Unsubscribe`/
    /// `Publish`/`TopicCreate`/`TopicDelete`).
    ///
    /// The payload is parsed by the frame layer into a structured type
    /// (see `frame::payload`); the dispatcher then invokes the same
    /// subscribe/publish helpers used by the legacy JSON path so both
    /// paths share routing semantics.
    fn route_native_pubsub(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        match parse_payload(frame.msg_type, frame.payload) {
            Ok(Some(ParsedPayload::Subscribe(SubscribePayload { pattern, qos: _ }))) => {
                self.handle_subscribe(ctx, frame.stream_id, pattern);
            }
            Ok(Some(ParsedPayload::Unsubscribe(UnsubscribePayload { pattern }))) => {
                self.handle_unsubscribe(ctx, frame.stream_id, pattern);
            }
            Ok(Some(ParsedPayload::Publish(frame::payload::PublishPayload { topic, payload }))) => {
                let topic = match self.validate_topic_or_log(ctx, topic) {
                    Some(t) => t,
                    None => return,
                };
                let reliable = frame.flags.contains(FrameFlags::ACK_REQ);
                self.handle_publish(ctx, frame.stream_id, topic, payload, reliable);
            }
            Ok(Some(ParsedPayload::TopicCreate(TopicCreatePayload {
                topic,
                partitions,
                retention_ms,
            }))) => {
                self.handle_topic_create(ctx, frame.stream_id, topic, partitions, retention_ms);
            }
            Ok(Some(ParsedPayload::TopicDelete(TopicDeletePayload { topic }))) => {
                let topic = match self.validate_topic_or_log(ctx, topic) {
                    Some(t) => t,
                    None => return,
                };
                self.handle_topic_delete(ctx, frame.stream_id, topic);
            }
            Ok(Some(other)) => {
                if self.decode_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        msg_type = ?frame.msg_type,
                        parsed = ?other,
                        total = self.decode_drop.count(),
                        "[pubsub] msg_type / payload mismatch (logging every 256th)"
                    );
                }
            }
            Ok(None) => {
                if self.decode_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        msg_type = ?frame.msg_type,
                        total = self.decode_drop.count(),
                        "[pubsub] no structured payload for type (logging every 256th)"
                    );
                }
            }
            Err(e) => {
                if self.decode_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        msg_type = ?frame.msg_type,
                        error = ?e,
                        total = self.decode_drop.count(),
                        "[pubsub] payload decode failed (logging every 256th)"
                    );
                }
            }
        }
    }

    /// Bench/test helper: binds `conn_idx → logical_session_id` so that
    /// `handle_publish`'s reliable fan-out branch retains entries in the
    /// replay buffer for that subscriber. Mirrors what `handle_resume`
    /// does on receipt of a `Resume` frame, without the wire round-trip.
    #[allow(dead_code)]
    pub fn register_logical_session_for_bench(&mut self, conn_idx: usize, logical_session_id: u64) {
        self.conn_logical_session
            .insert(conn_idx, logical_session_id);
        self.replay
            .lock()
            .unwrap()
            .buffer
            .register_session(logical_session_id);
    }

    /// Bench/test helper: acknowledges every retained entry for every
    /// registered `(logical_session, stream)` pair. Used by
    /// `pubsub_reliable` benches to measure the steady-state cost of
    /// reliable delivery with full ack release.
    #[allow(dead_code)]
    pub fn ack_all_logical_for_bench(&mut self) {
        // Snapshot the bindings to avoid borrowing self.conn_logical_session
        // while self.replay.acknowledge mutably borrows.
        let bindings: Vec<(usize, u64)> = self
            .conn_logical_session
            .iter()
            .map(|(&c, &s)| (c, s))
            .collect();
        // Ack at u64::MAX so every retained entry for each (session,
        // stream) pair is released regardless of its seq.
        for (_conn, logical) in bindings {
            // We don't track per-(session, stream) for this bench helper;
            // just walk the inner map by inserting a high watermark on
            // every known stream — approximated by replaying from MAX,
            // which the buffer's acknowledge path handles correctly.
            self.replay
                .lock()
                .unwrap()
                .buffer
                .acknowledge(logical, StreamId::new(0xFFFF_FFFF), u64::MAX);
        }
    }

    /// Registers a subscription for the calling connection.
    pub fn handle_subscribe(&mut self, ctx: &FrameContext, stream: StreamId, pattern_str: &str) {
        // Phase 7: check Subscribe against the ACL before any state
        // mutation. The pattern itself is the topic-or-pattern the
        // rule matches against, so `allow("sensor.*", None, Subscribe)`
        // permits subscriptions on `sensor.temp` / `sensor.humidity` /
        // etc. Deny produces an error ack so the client knows its
        // subscription was rejected.
        if !self
            .check_acl(ctx.conn_idx, Action::Subscribe, pattern_str)
            .is_allow()
        {
            if self.acl_denied_drop.tick() {
                tracing::info!(
                    conn_idx = ctx.conn_idx,
                    pattern = %pattern_str,
                    total = self.acl_denied_drop.count(),
                    "[acl] subscribe denied (logging every 256th)"
                );
            }
            self.send_ack(
                ctx,
                stream,
                MessageType::Subscribe,
                &[
                    b"{\"error\":\"denied\",\"action\":\"subscribe\",\"pattern\":\"",
                    pattern_str.as_bytes(),
                    b"\"}",
                ],
            );
            return;
        }
        match TopicPattern::new(pattern_str) {
            Ok(_) => {
                // Resource quota: max-subscribers-per-topic. Counts
                // subscribers per distinct pattern string — the practical
                // interpretation for a pattern-based routing model.
                // Skip the check for the SDK's heartbeat probe to avoid
                // penalising liveness traffic against the cap.
                if let Some(cap) = self.max_subscribers_per_topic {
                    let current = self
                        .pattern_sub_counts
                        .get(pattern_str)
                        .copied()
                        .unwrap_or(0);
                    if current >= cap {
                        self.quota_subscriber_rejects =
                            self.quota_subscriber_rejects.wrapping_add(1);
                        if pattern_str != "_heartbeat.probe" {
                            tracing::info!(
                                conn_idx = ctx.conn_idx,
                                pattern = %pattern_str,
                                cap,
                                current,
                                rejects = self.quota_subscriber_rejects,
                                "[quota] subscribe rejected — max_subscribers_per_topic reached"
                            );
                        }
                        let mut cap_buf = itoa::Buffer::new();
                        let cap_s = cap_buf.format(cap);
                        self.send_ack(
                            ctx,
                            stream,
                            MessageType::Subscribe,
                            &[
                                b"{\"error\":\"quota_exceeded\",\"limit\":\"max_subscribers_per_topic\",\"cap\":",
                                cap_s.as_bytes(),
                                b",\"pattern\":\"",
                                pattern_str.as_bytes(),
                                b"\"}",
                            ],
                        );
                        return;
                    }
                }
                let subscriber = Subscriber {
                    conn_idx: ctx.conn_idx,
                    quic_stream_id: ctx.quic_stream_id,
                };
                if let Err(e) = self.pubsub_registry.subscribe(subscriber, pattern_str) {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        pattern = %pattern_str,
                        error = ?e,
                        "[pubsub] registry rejected subscribe"
                    );
                    return;
                }
                // Bump per-pattern subscriber counter for quota accounting.
                // Only reached on successful registry insertion.
                let entry = self
                    .pattern_sub_counts
                    .entry(pattern_str.to_string())
                    .or_insert(0);
                let was_zero = *entry == 0;
                *entry += 1;
                // Notify other workers that this worker now has interest
                // in this pattern (0→1 transition). Skips heartbeat probes
                // to avoid churning the inter-worker channel.
                if was_zero
                    && !pattern_str.starts_with("_heartbeat.")
                    && !self.worker_outboxes.is_empty()
                {
                    self.broadcast_worker_msg(InterWorkerMsg::SubGained {
                        worker_idx: self.worker_self_idx as u16,
                        pattern: pattern_str.to_string(),
                    });
                }
                // The SDK's dead-peer probe re-subscribes to
                // `_heartbeat.probe` every ~1 s per connection. Logging
                // it at INFO drowns real subscribe events — drop to DEBUG.
                if pattern_str.starts_with("_heartbeat.") {
                    tracing::debug!(
                        conn_idx = ctx.conn_idx,
                        pattern = %pattern_str,
                        quic_stream = ctx.quic_stream_id,
                        active = self.pubsub_registry.len(),
                        "[pubsub] subscribe (heartbeat probe)"
                    );
                } else {
                    tracing::info!(
                        conn_idx = ctx.conn_idx,
                        pattern = %pattern_str,
                        quic_stream = ctx.quic_stream_id,
                        active = self.pubsub_registry.len(),
                        "[pubsub] subscribe"
                    );
                }

                // P6: if we are not the owner of this pattern, notify
                // the owner of our local interest so future publishes
                // fan out to us. The owner will record us in its
                // RemoteSubscriberRegistry.
                //
                // We track EVERY local subscription for periodic re-broadcast
                // (even if the cluster ring isn't formed yet) so that a
                // late-forming ring or a lost UDP datagram self-heals on the
                // next heartbeat tick. The re-broadcast resolves the owner
                // dynamically and skips self-owned patterns.
                if self.coordinator.is_some() {
                    // Try immediate forward if the owner is already known.
                    if let Some(coord) = &self.coordinator {
                        if let Some(owner) = coord.router().owner_of(pattern_str) {
                            if owner != self.node_id {
                                if let Some(tx) = &self.cluster_outgoing_tx {
                                    let msg = InterNodeMessage::subscribe(
                                        self.node_id,
                                        ctx.conn_idx as u32,
                                        ctx.quic_stream_id,
                                        pattern_str.to_string(),
                                    );
                                    let _ = tx.send(OutgoingMsg { msg, target: owner });
                                }
                                tracing::debug!(
                                    conn_idx = ctx.conn_idx,
                                    pattern = %pattern_str,
                                    owner = %owner.raw(),
                                    "[cluster] subscribe forwarded to owner"
                                );
                            }
                        }
                    }
                    // Always track for periodic re-broadcast (handles ring
                    // not-yet-formed AND datagram loss).
                    self.forwarded_subs.push(ForwardedSub {
                        conn_idx: ctx.conn_idx as u32,
                        stream_id: ctx.quic_stream_id,
                        pattern: pattern_str.to_string(),
                    });
                }

                self.send_ack(
                    ctx,
                    stream,
                    MessageType::Subscribe,
                    &[b"{\"subscribed\":\"", pattern_str.as_bytes(), b"\"}"],
                );
            }
            Err(e) => {
                if self.invalid_pattern_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        pattern = %pattern_str,
                        error = ?e,
                        total = self.invalid_pattern_drop.count(),
                        "[pubsub] invalid pattern (logging every 64th)"
                    );
                }
            }
        }
    }

    /// Removes any subscription for the calling connection whose pattern
    /// matches `pattern_str` exactly.
    fn handle_unsubscribe(&mut self, ctx: &FrameContext, stream: StreamId, pattern_str: &str) {
        let removed = self
            .pubsub_registry
            .unsubscribe_pattern(ctx.conn_idx, pattern_str);
        // Mirror the per-pattern counter decrement. Only decrement by
        // the number actually removed so the counter stays in sync with
        // the registry's view.
        if removed > 0 {
            if let Some(count) = self.pattern_sub_counts.get_mut(pattern_str) {
                *count = count.saturating_sub(removed);
                if *count == 0 {
                    // Drop empty entries so the map stays bounded under
                    // pattern churn.
                    self.pattern_sub_counts.remove(pattern_str);
                    // Notify other workers that this worker lost interest.
                    if !pattern_str.starts_with("_heartbeat.") && !self.worker_outboxes.is_empty() {
                        self.broadcast_worker_msg(InterWorkerMsg::SubLost {
                            worker_idx: self.worker_self_idx as u16,
                            pattern: pattern_str.to_string(),
                        });
                    }
                }
            }
        }
        tracing::info!(
            conn_idx = ctx.conn_idx,
            pattern = %pattern_str,
            removed,
            active = self.pubsub_registry.len(),
            "[pubsub] unsubscribe"
        );
        // Stop re-broadcasting the forwarded subscription (if any).
        if removed > 0 {
            self.forwarded_subs
                .retain(|fs| !(fs.conn_idx == ctx.conn_idx as u32 && fs.pattern == pattern_str));
        }
        let mut rbuf = itoa::Buffer::new();
        let r = rbuf.format(removed);
        self.send_ack(
            ctx,
            stream,
            MessageType::Unsubscribe,
            &[
                b"{\"unsubscribed\":\"",
                pattern_str.as_bytes(),
                b"\",\"removed\":",
                r.as_bytes(),
                b"}",
            ],
        );
    }

    /// Fans a published payload out to every subscriber whose pattern matches
    /// `topic`, skipping the origin connection (no echo).
    pub fn handle_publish(
        &mut self,
        ctx: &FrameContext,
        stream: StreamId,
        topic: &str,
        payload: &[u8],
        reliable: bool,
    ) {
        // Phase 7: check Publish against the ACL before fan-out. Skip
        // the registry lookup entirely on Deny — the publish is
        // invisible to subscribers. The check uses the concrete topic
        // string (not the publisher's pattern, which isn't known).
        if !self
            .check_acl(ctx.conn_idx, Action::Publish, topic)
            .is_allow()
        {
            self.conn_metrics
                .entry(ctx.conn_idx)
                .or_default()
                .record_reject();
            if self.acl_denied_drop.tick() {
                tracing::info!(
                    conn_idx = ctx.conn_idx,
                    topic = %topic,
                    total = self.acl_denied_drop.count(),
                    "[acl] publish denied (logging every 256th)"
                );
            }
            if let Some(log) = &mut self.audit_log {
                let actor = format!("conn:{}", ctx.conn_idx);
                let _ = log.record(
                    crate::audit::AuditKind::AclPublishDenied,
                    crate::audit::AuditResult::Denied,
                    &actor,
                    topic,
                );
            }
            self.send_ack(
                ctx,
                stream,
                MessageType::Publish,
                &[
                    b"{\"error\":\"denied\",\"action\":\"publish\",\"topic\":\"",
                    topic.as_bytes(),
                    b"\"}",
                ],
            );
            return;
        }

        // Record per-connection publish-in metric.
        self.conn_metrics
            .entry(ctx.conn_idx)
            .or_default()
            .record_publish_in(payload.len());

        // Copy payload once — the &[u8] points into the io_uring buffer
        // pool which gets recycled after this function returns. All
        // downstream paths (WAL, cluster, fan-out) clone this Bytes
        // instead of each doing their own copy_from_slice.
        let payload_bytes = Bytes::copy_from_slice(payload);

        // P6: cluster routing. If a coordinator is wired, consult it
        // to decide whether this publish belongs to us (deliver
        // locally + fan out to remote subscribers) or another node
        // (forward the publish via UDP).
        let mut remote_node_count: usize = 0;
        if let Some(coord) = &self.coordinator {
            let decision = coord.route_publish(topic);
            // Hoist ONE Arc<str> allocation for the topic — every
            // InterNodeMessage we construct below (publish/fanout) and
            // every msg.clone() in the fan-out loops reuses this via
            // refcount bump instead of deep-allocating Strings.
            let topic_arc: Arc<str> = Arc::from(topic);
            match decision {
                PublishDecision::Forward { owner } => {
                    // Send the publish to the owning node via UDP.
                    // The owner will fan out to its local subscribers
                    // + any other nodes with remote interest (but will
                    // skip us — we deliver locally below).
                    if let Some(tx) = &self.cluster_outgoing_tx {
                        let msg = InterNodeMessage::publish(
                            self.node_id,
                            ctx.conn_idx as u32,
                            ctx.quic_stream_id,
                            topic_arc.clone(),
                            payload_bytes.clone(),
                        );
                        let _ = tx.send(OutgoingMsg { msg, target: owner });
                    }
                    tracing::debug!(
                        conn_idx = ctx.conn_idx,
                        topic = %topic,
                        owner = %owner.raw(),
                        "[cluster] publish forwarded to owner (also delivering locally)"
                    );
                    // Fall through to local delivery — our subscribers
                    // get the publish immediately without waiting for
                    // the owner's round-trip fanout.
                }
                PublishDecision::Own {
                    remote_targets,
                    replicas,
                } => {
                    // Replication: forward the publish to replica
                    // nodes for durability. Fire-and-forget — the
                    // replicas append to their own WAL asynchronously.
                    if !replicas.is_empty() {
                        if let Some(tx) = &self.cluster_outgoing_tx {
                            let msg = InterNodeMessage::publish(
                                self.node_id,
                                ctx.conn_idx as u32,
                                ctx.quic_stream_id,
                                topic_arc.clone(),
                                payload_bytes.clone(),
                            );
                            for target in &replicas {
                                let _ = tx.send(OutgoingMsg {
                                    msg: msg.clone(),
                                    target: *target,
                                });
                            }
                            tracing::trace!(
                                topic = %topic,
                                replicas = replicas.len(),
                                "[cluster] publish replicated to backup nodes"
                            );
                        }
                    }
                    // Fan out to remote subscribers via UDP. Each
                    // remote target is a node whose local subscribers
                    // want this topic. Skip nodes that are also replicas —
                    // they receive Publish (which triggers cluster_deliver_local
                    // including subscriber delivery), so a Fanout would
                    // double-deliver.
                    remote_node_count = remote_targets
                        .iter()
                        .filter(|t| !replicas.contains(t))
                        .count();
                    if let Some(tx) = &self.cluster_outgoing_tx {
                        let msg = InterNodeMessage::fanout(
                            self.node_id,
                            ctx.conn_idx as u32,
                            ctx.quic_stream_id,
                            topic_arc.clone(),
                            payload_bytes.clone(),
                        );
                        for target in remote_targets {
                            if replicas.contains(&target) {
                                continue;
                            }
                            let _ = tx.send(OutgoingMsg {
                                msg: msg.clone(),
                                target,
                            });
                        }
                    }
                    // Fall through to local delivery.
                }
            }
        }

        self.targets_scratch.clear();
        self.targets_scratch.extend(
            self.pubsub_registry
                .lookup(topic)
                .into_iter()
                .filter(|s| s.conn_idx != ctx.conn_idx),
        );

        tracing::debug!(
            conn_idx = ctx.conn_idx,
            topic = %topic,
            targets = self.targets_scratch.len(),
            subs = self.pubsub_registry.len(),
            "[pubsub] publish"
        );

        // Re-encode as a Publish frame so downstream subscribers receive the
        // same wire shape the publisher used (topic_len + topic + payload).
        let topic_str = topic;
        self.encode_scratch.clear();
        self.encode_scratch
            .extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
        self.encode_scratch.extend_from_slice(topic_str.as_bytes());
        self.encode_scratch.extend_from_slice(&payload_bytes);
        let bytes = self.encode_scratch.split().freeze();

        // Phase 4: durable append before fan-out. A failure here is
        // operator-visible (warn log) but does not abort the publish —
        // the message still reaches live subscribers. Subscribers that
        // later `Fetch(offset)` for this entry will see a gap; that is
        // preferable to dropping the live fan-out, which has no recovery
        // path. The `topic` invariant (validated at publish
        // entry) guarantees the topic is also a valid WAL key.
        //
        // `wal_offset` is lifted out so the consumer-group fan-out
        // below can auto-ack the chosen member at the right offset
        // (without it, restart would replay from OLDEST).
        let mut wal_offset: Option<wal_store::Offset> = None;
        if let Some(wal) = &mut self.wal {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let record = wal_store::WalRecord::new(now_ns, payload_bytes.clone());
            match wal.append(topic_str, record) {
                Ok(offset) => {
                    tracing::trace!(
                        conn_idx = ctx.conn_idx,
                        topic = topic_str,
                        offset = offset.get(),
                        "[wal] appended"
                    );
                    wal_offset = Some(offset);
                }
                Err(e) => {
                    if self.wal_fail_drop.tick() {
                        tracing::warn!(
                            conn_idx = ctx.conn_idx,
                            topic = topic_str,
                            error = ?e,
                            total = self.wal_fail_drop.count(),
                            "[wal] append failed — live fan-out proceeds, replay will have a gap \
                             (logging every 128th)"
                        );
                    }
                }
            }
        }

        // ── Adaptive delivery strategy selection ──────────────────────
        // BatchPush (default): forward the full payload to each subscriber.
        // LogTail (high fan-out): send a lightweight NotifyOffset; subscribers
        // auto-fetch the payload from WAL. This reduces per-publish memory
        // from N × payload_size to N × ~16 bytes in subscriber send queues.
        //
        // LogTail requires a single WAL: the NotifyOffset carries a WAL offset
        // that the subscriber's Fetch reads back. In multi-worker mode each
        // worker has its own WAL instance, so a NotifyOffset generated by
        // worker W is meaningless to a subscriber whose Fetch lands on worker
        // V. We therefore treat WAL as unavailable for strategy selection when
        // more than one worker is active.
        let total_fanout = self.targets_scratch.len() + remote_node_count;
        let wal_usable = self.wal.is_some() && self.worker_outboxes.is_empty();
        let strategy = self.delivery_config.select(total_fanout, wal_usable);
        let (fanout_bytes, fanout_msg_type) = match strategy {
            FanoutStrategy::BatchPush => {
                self.batch_push_count = self.batch_push_count.wrapping_add(1);
                (bytes.clone(), MessageType::Publish)
            }
            FanoutStrategy::LogTail => {
                self.logtail_count = self.logtail_count.wrapping_add(1);
                // Encode NotifyOffset: topic_len(u16) | topic | offset(u64).
                let offset = wal_offset.unwrap_or_else(|| wal_store::Offset::new(0));
                self.notify_scratch.clear();
                self.notify_scratch
                    .extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
                self.notify_scratch.extend_from_slice(topic_str.as_bytes());
                self.notify_scratch
                    .extend_from_slice(&offset.get().to_be_bytes());
                (
                    self.notify_scratch.split().freeze(),
                    MessageType::NotifyOffset,
                )
            }
        };

        tracing::debug!(
            conn_idx = ctx.conn_idx,
            topic = %topic,
            strategy = ?strategy,
            local_subs = self.targets_scratch.len(),
            remote_nodes = remote_node_count,
            "[delivery] publish routed"
        );

        // ── Reliable fan-out section ───────────────────────────────────
        // While `reliable_fanout` is true, `enqueue_frame` stamps ACK_REQ
        // on outgoing Publish headers and retains a copy in the replay
        // buffer for subscribers with a registered logical session. Set
        // ONLY here (after all early-return paths) so `send_ack` calls
        // above cannot pollute the replay buffer with ack-responses.
        self.reliable_fanout = reliable;

        let target_count = self.targets_scratch.len();
        for i in 0..target_count {
            let (conn_idx, quic_stream_id) = {
                let t = &self.targets_scratch[i];
                (t.conn_idx, t.quic_stream_id)
            };
            self.enqueue_frame(
                conn_idx,
                quic_stream_id,
                stream,
                fanout_msg_type,
                fanout_bytes.clone(),
            );
        }

        // P5: consumer-group delivery.
        //
        // In single-worker mode: local round-robin via group_locals.
        //
        // In multi-worker mode: worker 0 does global round-robin via
        // remote_group_members; other workers forward to worker 0 via
        // GroupDeliver. This ensures exactly one delivery per publish
        // regardless of which worker the group members live on.
        if self.worker_outboxes.is_empty() {
            // Single-worker: inline local delivery (hot path).
            self.group_targets_scratch.clear();
            for ((t, _g), local) in &mut self.group_locals {
                if t.as_str() != topic_str || local.members.is_empty() {
                    continue;
                }
                let idx = local.rr_idx % local.members.len();
                local.rr_idx = local.rr_idx.wrapping_add(1);
                let m = &local.members[idx];
                if let Some(off) = wal_offset {
                    self.consumer_offsets
                        .ack(topic_str, m.consumer.as_str(), off);
                }
                let _ = self
                    .group_targets_scratch
                    .try_push((m.conn_idx, m.quic_stream_id));
            }
            let group_len = self.group_targets_scratch.len();
            for i in 0..group_len {
                let (conn_idx, quic_stream_id) = self.group_targets_scratch[i];
                self.enqueue_frame(
                    conn_idx,
                    quic_stream_id,
                    stream,
                    fanout_msg_type,
                    fanout_bytes.clone(),
                );
            }
        } else if self.worker_self_idx == 0 {
            self.do_group_delivery(fanout_bytes.clone(), fanout_msg_type);
        } else if let Some(outbox) = self.worker_outboxes.first() {
            let _ = outbox.try_send(InterWorkerMsg::GroupDeliver {
                framed: fanout_bytes.clone(),
                target: None,
                msg_type: fanout_msg_type,
            });
        } else {
            tracing::error!("worker_outboxes empty in group-deliver path — skipping");
        }

        // Cross-worker fan-out: send to other workers that have subscribers
        // matching this topic. We skip workers with no matching patterns
        // (tracked via SubGained/SubLost messages) to avoid wasting CPU on
        // try_send + drain + lookup for workers that have nobody listening.
        //
        // Forward the already-encoded `bytes` (a `Bytes` handle) — worker
        // B's clone is a refcount bump with zero memcpy.
        if !self.worker_outboxes.is_empty() {
            // Compute which workers have ≥1 pattern matching this topic.
            let target_bitmap = self.matching_worker_bitmap(topic);
            if target_bitmap != 0 {
                for (i, outbox) in self.worker_outboxes.iter().enumerate() {
                    if i == self.worker_self_idx {
                        continue;
                    }
                    if (target_bitmap & (1u32 << i)) == 0 {
                        continue;
                    }
                    if let Err(std::sync::mpsc::TrySendError::Full(_)) =
                        outbox.try_send(InterWorkerMsg::Publish {
                            framed: fanout_bytes.clone(),
                            msg_type: fanout_msg_type,
                            reliable,
                        })
                    {
                        self.worker_drop_count = self.worker_drop_count.wrapping_add(1);
                        if self.worker_drop_cooldown.tick() {
                            tracing::warn!(
                                worker = i,
                                total_drops = self.worker_drop_count,
                                "[pubsub] worker inbox full — dropping publishes \
                                 (raise cap in build_inter_worker_pubsub_channels)"
                            );
                        }
                    }
                }
            }
        }

        // ── Dormant session retention ──────────────────────────────────
        // Reliable publishes that match a dormant session's subscription
        // patterns are retained in the replay buffer even though the
        // subscriber's connection is gone. This ensures gap frames
        // published during a disconnect survive until Resume replays them.
        if reliable {
            let mut rs = self.replay.lock().unwrap();
            if !rs.dormant_sessions.is_empty() {
                let dormant: Vec<(u64, Vec<u64>)> = rs
                    .dormant_sessions
                    .iter()
                    .filter(|(_, (_, pats))| {
                        pats.iter()
                            .any(|p| pattern_matches(p.as_ref(), topic_str))
                    })
                    .map(|(&l, (qsid, _))| (l, qsid.clone()))
                    .collect();
                for (logical, quic_stream_ids) in dormant {
                    for qsid in &quic_stream_ids {
                        let seq_val = rs
                            .session_stream_seq
                            .get(&(logical, *qsid))
                            .copied()
                            .unwrap_or(Seq::ZERO);
                        let header = FrameHeader::new(stream, MessageType::Publish)
                            .with_seq(seq_val)
                            .with_flags(FrameFlags::ACK_REQ);
                        rs.buffer.append(
                            logical,
                            StreamId::new(*qsid),
                            ReplayEntry {
                                seq: seq_val.get(),
                                header: header.clone(),
                                payload: fanout_bytes.clone(),
                            },
                        );
                        rs.session_stream_seq
                            .insert((logical, *qsid), seq_val.next());
                    }
                }
            }
        }

        // End of reliable fan-out section — always reset so subsequent
        // `enqueue_frame` calls (e.g. from `send_ack` on the next frame)
        // are not treated as reliable.
        self.reliable_fanout = false;
    }

    /// Handles an inbound cumulative `Ack` frame from a subscriber.
    ///
    /// Looks up the subscriber's logical session by `conn_idx` and releases
    /// all retained entries with `seq <= ack_seq` for the given stream. If
    /// the connection has no registered logical session (no `Resume` seen),
    /// the ack is a no-op — the server was not retaining anything for it.
    fn handle_ack(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let ack_seq = match parse_payload(frame.msg_type, frame.payload) {
            Ok(Some(ParsedPayload::Ack(AckPayload { ack_seq }))) => ack_seq,
            Ok(_) => {
                if self.malformed_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        "[reliability] Ack frame payload did not parse as Ack"
                    );
                }
                return;
            }
            Err(e) => {
                if self.malformed_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        error = ?e,
                        "[reliability] Ack payload decode failed"
                    );
                }
                return;
            }
        };
        let stream = frame.stream_id;
        let Some(&logical) = self.conn_logical_session.get(&ctx.conn_idx) else {
            // Subscriber never sent Resume → nothing retained for it.
            tracing::trace!(
                conn_idx = ctx.conn_idx,
                stream = ?stream,
                ack_seq,
                "[reliability] Ack from conn with no logical session — ignoring"
            );
            return;
        };
        self.replay
            .lock()
            .unwrap()
            .buffer
            .acknowledge(logical, stream, ack_seq);
        self.session_last_acked.insert((logical, stream), ack_seq);
        if let Some(m) = &self.metrics {
            m.inc(&m.messages_acked_total);
        }
        tracing::trace!(
            conn_idx = ctx.conn_idx,
            logical_session = logical,
            stream = ?stream,
            ack_seq,
            "[reliability] cumulative ack applied"
        );
    }

    /// Handles an inbound `Resume` frame from a reconnecting client.
    ///
    /// Wire shape: `ResumePayload { logical_session_id, streams: Vec<ResumeSlot> }`.
    /// An empty `streams` list is a session-hello (first connect): the server
    /// registers the logical session and replies with an empty `ResumeOk` so
    /// the client knows the session is established. For each non-empty slot
    /// the server attempts `replay_from(logical, stream, last_acked)` and:
    ///   * On `Ok` — enqueues a `ResumeOk` slot, then re-sends each retained
    ///     `Publish` frame (with `ACK_REQ` set) to the new connection.
    ///   * On `Unavailable` — enqueues a `ResumeUnavailable` slot so the
    ///     client knows the requested seq is below the window floor and can
    ///     fall back to a fresh subscription.
    fn handle_resume(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let parsed = match parse_payload(frame.msg_type, frame.payload) {
            Ok(Some(ParsedPayload::Resume(rp))) => rp,
            Ok(_) => {
                if self.malformed_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        "[reliability] Resume frame payload did not parse as Resume"
                    );
                }
                return;
            }
            Err(e) => {
                if self.malformed_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        error = ?e,
                        "[reliability] Resume payload decode failed"
                    );
                }
                return;
            }
        };
        let ResumePayload {
            logical_session_id,
            streams,
        } = parsed;

        // Bind the new conn_idx to the logical session (rebinding on
        // reconnect — the old conn_idx's entry is removed in
        // `on_connection_closed`).
        self.conn_logical_session
            .insert(ctx.conn_idx, logical_session_id);
        {
            let mut rs = self.replay.lock().unwrap();
            rs.buffer.register_session(logical_session_id);
            // The session is live again — stop retaining gap frames for it
            // (live fan-out will reach the subscriber directly from here on).
            rs.dormant_sessions.remove(&logical_session_id);
        }
        if let Some(m) = &self.metrics {
            m.inc(&m.resume_attempts_total);
        }

        tracing::info!(
            conn_idx = ctx.conn_idx,
            logical_session = logical_session_id,
            slots = streams.len(),
            "[reliability] Resume received"
        );

        // Session-hello (empty slots): reply with an empty ResumeOk so the
        // client knows the session is bound. No replay needed.
        if streams.is_empty() {
            let payload = encode_resume_ok(Vec::new());
            self.enqueue_frame(
                ctx.conn_idx,
                ctx.quic_stream_id,
                frame.stream_id,
                MessageType::ResumeOk,
                payload,
            );
            return;
        }

        // Partition slots by outcome so we send exactly one summary frame
        // (ResumeOk or ResumeUnavailable) per Resume — clients key off the
        // stream_id in each slot.
        let mut ok_slots: Vec<ResumeOkSlot> = Vec::new();
        let mut unavail_slots: Vec<ResumeOkSlot> = Vec::new();
        // Collect (stream, entries) for the Ok paths so we can re-send the
        // retained Publish frames AFTER the ResumeOk summary.
        let mut to_replay: Vec<(StreamId, Vec<crate::replay::ReplayEntry>)> = Vec::new();

        for slot in &streams {
            let stream = StreamId::new(slot.stream_id);
            let outcome = self
                .replay
                .lock()
                .unwrap()
                .buffer
                .replay_from(logical_session_id, stream, slot.last_acked);
            match outcome {
                ReplayOutcome::Ok {
                    entries,
                    oldest_available,
                } => {
                    ok_slots.push(ResumeOkSlot {
                        stream_id: slot.stream_id,
                        oldest_available,
                    });
                    if !entries.is_empty() {
                        to_replay.push((stream, entries));
                    }
                }
                ReplayOutcome::Unavailable { oldest_available } => {
                    unavail_slots.push(ResumeOkSlot {
                        stream_id: slot.stream_id,
                        oldest_available,
                    });
                }
            }
        }

        // Send the ResumeOk summary (one frame, all Ok slots).
        if !ok_slots.is_empty() {
            if let Some(m) = &self.metrics {
                m.add(&m.resume_success_total, ok_slots.len() as u64);
            }
            let payload = encode_resume_ok(ok_slots);
            self.enqueue_frame(
                ctx.conn_idx,
                ctx.quic_stream_id,
                frame.stream_id,
                MessageType::ResumeOk,
                payload,
            );
        }
        // Send the ResumeUnavailable summary (one frame, all unavailable slots).
        if !unavail_slots.is_empty() {
            if let Some(m) = &self.metrics {
                m.add(&m.resume_unavailable_total, unavail_slots.len() as u64);
            }
            let payload = encode_resume_unavailable(unavail_slots);
            self.enqueue_frame(
                ctx.conn_idx,
                ctx.quic_stream_id,
                frame.stream_id,
                MessageType::ResumeUnavailable,
                payload,
            );
        }

        // Re-send each retained Publish frame with ACK_REQ set. The
        // OutgoingFrame's conn_idx is re-bound to the new connection, and
        // the quic_stream_id is set to the SUBSCRIBER's original stream
        // (stored in the replay key) so the frames land on the same
        // dedicated stream the client's StreamHandle is listening on.
        // The retained header's seq is preserved — the client dedups by
        // header.seq across reconnects, so a fresh seq would either be
        // dropped as a duplicate (if < highest_accepted) or break
        // monotonicity.
        let new_conn = ctx.conn_idx;
        let mut replayed_total: usize = 0;
        for (replay_stream, entries) in to_replay {
            // The replay key is the subscriber's quic_stream_id.
            let target_quic_stream = replay_stream.get();
            let buf = self
                .pending_replays
                .entry((new_conn, target_quic_stream))
                .or_default();
            for entry in entries {
                // The retained header already carries the original logical
                // stream, msg_type=Publish, flags=ACK_REQ, and seq. Re-stamp
                // only the ACK_REQ flag in case it was dropped (shouldn't
                // happen, but the invariant is cheap to enforce).
                let header = entry
                    .header
                    .with_flags(FrameFlags::ACK_REQ);
                self.conn_metrics
                    .entry(new_conn)
                    .or_default()
                    .record_delivery_out(entry.payload.len());
                buf.push(OutgoingFrame {
                    conn_idx: new_conn,
                    quic_stream_id: target_quic_stream,
                    header,
                    payload: entry.payload,
                });
                replayed_total += 1;
            }
        }
        if replayed_total > 0 {
            if let Some(m) = &self.metrics {
                m.add(&m.messages_replayed_total, replayed_total as u64);
            }
            tracing::info!(
                conn_idx = ctx.conn_idx,
                logical_session = logical_session_id,
                replayed = replayed_total,
                "[reliability] Resume replayed retained frames"
            );
        }
    }

    /// Compute the bitmap of remote workers whose subscribed patterns
    /// match `topic`. Returns 0 when no remote worker has a matching
    /// subscriber — caller skips the fan-out entirely.
    ///
    /// Iterates over `remote_pattern_interest` (typically a small map,
    /// one entry per distinct pattern across all workers) and checks
    /// each pattern against `topic` via `TopicPattern::matches`.
    fn matching_worker_bitmap(&mut self, topic: &str) -> u32 {
        // Fast path: cache hit (common case — publishes go to a small
        // number of distinct topics, so the hit rate is very high).
        if let Some(&bm) = self.topic_worker_bitmap_cache.get(topic) {
            return bm;
        }
        // Cache miss: compute by iterating remote_pattern_interest.
        let mut bitmap: u32 = 0;
        for (pattern, &mask) in self.remote_pattern_interest.iter() {
            if pattern.matches(topic) {
                bitmap |= mask;
            }
        }
        self.topic_worker_bitmap_cache
            .insert(topic.to_string(), bitmap);
        bitmap
    }

    /// Broadcast a non-publish inter-worker message to all other workers.
    /// Used for SubGained/SubLost notifications. `try_send` is non-blocking.
    fn broadcast_worker_msg(&self, msg: InterWorkerMsg) {
        for (i, outbox) in self.worker_outboxes.iter().enumerate() {
            if i == self.worker_self_idx {
                continue;
            }
            let _ = outbox.try_send(msg.clone());
        }
    }

    /// Validates a wire-decoded topic string. Logs + returns `None` on
    /// invalid names so dispatch callers can early-return without
    /// enqueuing an error ack (the rejection is silent at the protocol
    /// level — only the operator sees the warning).
    ///
    /// Returns the validated `&str` (no allocation) instead of an owned
    /// `Topic` — the publish hot path calls this thousands of times/sec
    /// and the old `Topic::new` allocated a `String` per publish.
    fn validate_topic_or_log<'a>(&self, ctx: &FrameContext, raw: &'a str) -> Option<&'a str> {
        match pubsub_engine::Topic::validate(raw) {
            Ok(()) => Some(raw),
            Err(e) => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    topic = raw,
                    error = ?e,
                    "[pubsub] invalid topic name"
                );
                None
            }
        }
    }

    /// Records a `TopicCreate`. Retention / partition enforcement lands in
    /// Phase 4 — for now the catalog simply remembers the declared config.
    pub fn handle_topic_create(
        &mut self,
        ctx: &FrameContext,
        stream: StreamId,
        topic: &str,
        partitions: u32,
        retention_ms: u64,
    ) {
        // Phase 7: TopicCreate mutates the topic catalog, so it needs
        // an explicit ACL action distinct from Publish/Subscribe. Deny
        // returns an error ack and skips the catalog mutation.
        if !self
            .check_acl(ctx.conn_idx, Action::TopicCreate, topic)
            .is_allow()
        {
            tracing::info!(
                conn_idx = ctx.conn_idx,
                topic = %topic,
                "[acl] topic_create denied"
            );
            self.send_ack(
                ctx,
                stream,
                MessageType::TopicCreate,
                &[
                    b"{\"error\":\"denied\",\"action\":\"topic_create\",\"topic\":\"",
                    topic.as_bytes(),
                    b"\"}",
                ],
            );
            return;
        }
        // Resource quota: max-topics. Skip the check when the topic is
        // already tracked (re-create is idempotent and shouldn't bump
        // the counter). Reject BEFORE catalog insertion so the cap is
        // exact even under bursts.
        if let Some(cap) = self.max_topics {
            if !self.topic_catalog.get(topic).is_some() && self.topic_catalog.len() >= cap {
                self.quota_topic_rejects = self.quota_topic_rejects.wrapping_add(1);
                tracing::info!(
                    conn_idx = ctx.conn_idx,
                    topic = %topic,
                    cap,
                    total = self.topic_catalog.len(),
                    rejects = self.quota_topic_rejects,
                    "[quota] topic_create rejected — max_topics reached"
                );
                let mut cap_buf = itoa::Buffer::new();
                let cap_s = cap_buf.format(cap);
                self.send_ack(
                    ctx,
                    stream,
                    MessageType::TopicCreate,
                    &[
                        b"{\"error\":\"quota_exceeded\",\"limit\":\"max_topics\",\"cap\":",
                        cap_s.as_bytes(),
                        b",\"topic\":\"",
                        topic.as_bytes(),
                        b"\"}",
                    ],
                );
                return;
            }
        }
        let config = TopicConfig {
            partitions: partitions.max(1),
            retention_ms,
        };
        self.topic_catalog.create(topic, config);
        tracing::info!(
            conn_idx = ctx.conn_idx,
            topic = %topic,
            partitions = config.partitions,
            retention_ms,
            total_topics = self.topic_catalog.len(),
            "[pubsub] TopicCreate"
        );
        if let Some(log) = &mut self.audit_log {
            let actor = format!("conn:{}", ctx.conn_idx);
            let _ = log.record(
                crate::audit::AuditKind::TopicCreated,
                crate::audit::AuditResult::Allowed,
                &actor,
                topic,
            );
        }
        self.send_ack(
            ctx,
            stream,
            MessageType::TopicCreate,
            &[b"{\"topic_created\":\"", topic.as_bytes(), b"\"}"],
        );
    }

    /// Removes a topic from the catalog.
    pub fn handle_topic_delete(&mut self, ctx: &FrameContext, stream: StreamId, topic: &str) {
        // Phase 7: TopicDelete is the most dangerous operation (it can
        // wipe subscriber state), so its ACL action is separate from
        // TopicCreate. Deny returns an error ack and skips the catalog
        // mutation.
        if !self
            .check_acl(ctx.conn_idx, Action::TopicDelete, topic)
            .is_allow()
        {
            tracing::info!(
                conn_idx = ctx.conn_idx,
                topic = %topic,
                "[acl] topic_delete denied"
            );
            self.send_ack(
                ctx,
                stream,
                MessageType::TopicDelete,
                &[
                    b"{\"error\":\"denied\",\"action\":\"topic_delete\",\"topic\":\"",
                    topic.as_bytes(),
                    b"\"}",
                ],
            );
            return;
        }
        let existed = self.topic_catalog.delete(topic);
        tracing::info!(
            conn_idx = ctx.conn_idx,
            topic = %topic,
            existed,
            total_topics = self.topic_catalog.len(),
            "[pubsub] TopicDelete"
        );
        if let Some(log) = &mut self.audit_log {
            let actor = format!("conn:{}", ctx.conn_idx);
            let _ = log.record(
                crate::audit::AuditKind::TopicDeleted,
                crate::audit::AuditResult::Allowed,
                &actor,
                topic,
            );
        }
        let existed_str = if existed { b"true" as &[u8] } else { b"false" };
        self.send_ack(
            ctx,
            stream,
            MessageType::TopicDelete,
            &[
                b"{\"topic_deleted\":\"",
                topic.as_bytes(),
                b"\",\"existed\":",
                existed_str,
                b"}",
            ],
        );
    }

    /// Processes a `Handshake` frame: extract the claimed
    /// `(principal_id, credential_token)`, verify via
    /// `AuthenticationPipeline::authenticate`, and bind the resulting
    /// `PrincipalId` to `conn_idx` for use in subsequent ACL checks.
    ///
    /// On success the dispatcher records `(conn_idx, principal)` in
    /// `principals`. On failure (unknown principal, bad credential,
    /// rate-limited) the connection retains whatever principal it had
    /// before (typically `default_principal`) and a warn-level trace
    /// is emitted. Either way an ack is enqueued back to the client
    /// so it knows whether to re-auth.
    ///
    /// The `Handshake` payload is 21 bytes: version, capabilities,
    /// principal_id and credential_token. Pre-1.0 5-byte payloads fail
    /// `decode` with `PayloadError::Truncated` — this is the intended
    /// hard cutover.
    pub fn handle_handshake(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let parsed = match parse_payload(frame.msg_type, frame.payload) {
            Ok(Some(p)) => p,
            Ok(None) => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    "[auth] handshake payload not recognised"
                );
                let ack = br#"{"ok":false,"reason":"bad_handshake_payload"}"#;
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::Handshake,
                    Bytes::from(&ack[..]),
                );
                return;
            }
            Err(e) => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    error = ?e,
                    "[auth] handshake payload decode failed — connection retains default principal"
                );
                let ack = br#"{"ok":false,"reason":"bad_handshake_payload"}"#;
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::Handshake,
                    Bytes::from(&ack[..]),
                );
                return;
            }
        };
        let ParsedPayload::Handshake(hs) = parsed else {
            tracing::warn!(
                conn_idx = ctx.conn_idx,
                "[auth] handshake produced wrong parsed payload type"
            );
            return;
        };
        let principal = PrincipalId::new(hs.principal_id);
        let credential = Credential::new(
            CredentialType::Token,
            hs.credential_token,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        );
        let auth_principal = AuthPrincipalId::new(principal.raw());
        match self.auth.authenticate(auth_principal, credential) {
            Ok(raw) => {
                let resolved = PrincipalId::new(raw);
                self.principals.insert(ctx.conn_idx, resolved);
                tracing::info!(
                    conn_idx = ctx.conn_idx,
                    principal = %resolved,
                    version = ?hs.version,
                    "[auth] handshake ok — principal bound to connection"
                );
                let mut pbuf = itoa::Buffer::new();
                let p = pbuf.format(resolved.raw());
                self.send_ack(
                    ctx,
                    frame.stream_id,
                    MessageType::Handshake,
                    &[b"{\"ok\":true,\"principal\":", p.as_bytes(), b"}"],
                );
            }
            Err(e) => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    principal = %principal,
                    error = ?e,
                    "[auth] handshake failed — connection keeps default principal"
                );
                let ack = br#"{"ok":false,"reason":"auth_failed"}"#;
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::Handshake,
                    Bytes::from(&ack[..]),
                );
            }
        }
    }

    /// Handles a `Fetch` request: replay WAL entries for `topic` starting
    /// at `from_offset`, bounded by `max_count`.
    ///
    /// ACL gate: requires `Action::Subscribe` on the topic. Without it,
    /// the request is rejected with an error ack and no entries are
    /// surfaced.
    ///
    /// When no WAL is configured (`--wal-root` not supplied) the request
    /// fails fast with a `wal_disabled` error ack — clients can detect
    /// this and fall back to live subscription only.
    ///
    /// On success the dispatcher enqueues one `FetchReply` frame per
    /// entry (capped by `FETCH_MAX_COUNT`) into the outbox. Each frame
    /// carries its WAL-assigned offset so the subscriber can reconcile
    /// gaps.
    pub fn handle_fetch(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let parsed = match parse_payload(frame.msg_type, frame.payload) {
            Ok(Some(p)) => p,
            Ok(None) => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    "[wal] fetch payload not recognised"
                );
                let ack = br#"{"error":"bad_fetch_payload"}"#;
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::FetchReply,
                    Bytes::from(&ack[..]),
                );
                return;
            }
            Err(e) => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    error = ?e,
                    "[wal] fetch payload decode failed"
                );
                let ack = br#"{"error":"bad_fetch_payload"}"#;
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::FetchReply,
                    Bytes::from(&ack[..]),
                );
                return;
            }
        };
        let fetch = match parsed {
            ParsedPayload::Fetch(f) => f,
            other => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    parsed = ?other,
                    "[wal] fetch arm got non-Fetch payload"
                );
                return;
            }
        };

        // Validate the topic name — WAL rejects the same invalid names
        // Topic::new does, but we want to return a structured error ack
        // rather than let the WAL's InvalidTopic error surface raw.
        let topic_str = fetch.topic;
        if let Err(e) = pubsub_engine::Topic::new(topic_str) {
            tracing::warn!(
                conn_idx = ctx.conn_idx,
                topic = topic_str,
                error = ?e,
                "[wal] fetch on invalid topic name"
            );
            self.send_ack(
                ctx,
                frame.stream_id,
                MessageType::FetchReply,
                &[
                    b"{\"error\":\"invalid_topic\",\"topic\":\"",
                    topic_str.as_bytes(),
                    b"\"}",
                ],
            );
            return;
        }

        // ACL: Fetch is treated as Subscribe on the topic.
        if !self
            .check_acl(ctx.conn_idx, Action::Subscribe, topic_str)
            .is_allow()
        {
            tracing::info!(
                conn_idx = ctx.conn_idx,
                topic = topic_str,
                "[acl] fetch denied"
            );
            self.send_ack(
                ctx,
                frame.stream_id,
                MessageType::FetchReply,
                &[
                    b"{\"error\":\"denied\",\"action\":\"fetch\",\"topic\":\"",
                    topic_str.as_bytes(),
                    b"\"}",
                ],
            );
            return;
        }

        let Some(wal) = &mut self.wal else {
            tracing::debug!(
                conn_idx = ctx.conn_idx,
                topic = topic_str,
                "[wal] fetch requested but --wal-root not set"
            );
            let ack = br#"{"error":"wal_disabled"}"#;
            self.enqueue_response(
                ctx,
                frame.stream_id,
                MessageType::FetchReply,
                Bytes::from(&ack[..]),
            );
            return;
        };

        // Clamp the requested count to the hard cap so a wild request
        // can't saturate the subscriber's stream for seconds.
        let max_count = fetch.max_count.min(frame::payload::FETCH_MAX_COUNT);
        let from = wal_store::Offset::new(fetch.from_offset);
        let head = wal.head(topic_str);
        let mut entries: Vec<wal_store::WalEntry> = Vec::with_capacity(max_count as usize);
        let upper = if head == wal_store::Offset::NONE {
            from
        } else {
            // `read_range` treats `until` as INCLUSIVE. To read `max_count`
            // entries starting at `from`, the inclusive upper bound is
            // `from + max_count - 1`. Cap at `head` (the last live entry).
            let from_plus_cap = wal_store::Offset::new(
                from.get()
                    .saturating_add((max_count.saturating_sub(1)) as u64),
            );
            core::cmp::min(from_plus_cap, head)
        };
        match wal.read_range(topic_str, from, upper, &mut entries) {
            Ok(count) => {
                tracing::debug!(
                    conn_idx = ctx.conn_idx,
                    topic = topic_str,
                    requested_max = max_count,
                    delivered = count,
                    "[wal] fetch replay"
                );
            }
            Err(e) => {
                tracing::warn!(
                    conn_idx = ctx.conn_idx,
                    topic = topic_str,
                    error = ?e,
                    "[wal] read_range failed"
                );
                self.send_ack(
                    ctx,
                    frame.stream_id,
                    MessageType::FetchReply,
                    &[
                        b"{\"error\":\"read_failed\",\"topic\":\"",
                        topic_str.as_bytes(),
                        b"\"}",
                    ],
                );
                return;
            }
        }

        for entry in entries {
            // Build one FetchReply per WAL entry.
            let fr = FetchReplyPayload {
                topic: topic_str,
                offset: entry.offset.get(),
                timestamp_ns: entry.timestamp_ns,
                payload: entry.payload.as_ref(),
            };
            let size = fr.wire_size();
            // `vec![0u8; size]` (not `with_capacity`) — ByteWriter
            // writes through a `&mut [u8]` view, so the slice must
            // already be `len == size`, not just `capacity == size`.
            let mut buf = vec![0u8; size];
            let mut w = ByteWriter::new(&mut buf);
            fr.encode(&mut w);
            self.enqueue_response(
                ctx,
                frame.stream_id,
                MessageType::FetchReply,
                Bytes::from(buf),
            );
        }
    }

    // ── Consumer-group handlers (P5) ───────────────────────────────

    /// Parses the common wire prefix shared by all three consumer-group
    /// messages: `topic_len(u16) | topic | group_len(u16) | group_id
    /// | consumer_len(u16) | consumer_id`.
    ///
    /// Returns `(topic, group_id, consumer_id, remaining_bytes)` on
    /// success. The `remaining` slice carries any fields that follow
    /// the common prefix (e.g. `partitions(u32)` for Join, `generation
    /// (u32)` for Heartbeat).
    fn parse_group_prefix(payload: &[u8]) -> Option<(&str, &str, &str, &[u8])> {
        let mut pos = 0;
        // topic
        if payload.len() < pos + 2 {
            return None;
        }
        let tlen = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;
        if payload.len() < pos + tlen {
            return None;
        }
        let topic = std::str::from_utf8(&payload[pos..pos + tlen]).ok()?;
        pos += tlen;
        // group
        if payload.len() < pos + 2 {
            return None;
        }
        let glen = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;
        if payload.len() < pos + glen {
            return None;
        }
        let group = std::str::from_utf8(&payload[pos..pos + glen]).ok()?;
        pos += glen;
        // consumer
        if payload.len() < pos + 2 {
            return None;
        }
        let clen = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;
        if payload.len() < pos + clen {
            return None;
        }
        let consumer = std::str::from_utf8(&payload[pos..pos + clen]).ok()?;
        pos += clen;
        Some((topic, group, consumer, &payload[pos..]))
    }

    /// Records (or updates) a consumer-group member in worker 0's global
    /// registry. Deduplicates by `(worker_idx, consumer_id)`: if the
    /// member already exists, updates its `conn_idx`/`stream_id`
    /// (handles reconnection to a new QUIC connection on the same worker).
    fn record_global_member(
        &mut self,
        worker_idx: u16,
        topic: &str,
        group: &str,
        conn_idx: usize,
        stream_id: u64,
        consumer_id: &str,
    ) {
        let entry = self
            .remote_group_members
            .entry((topic.to_string(), group.to_string()))
            .or_default();
        if let Some(m) = entry
            .iter_mut()
            .find(|m| m.consumer_id == consumer_id && m.worker_idx == worker_idx)
        {
            m.conn_idx = conn_idx;
            m.stream_id = stream_id;
        } else {
            entry.push(GlobalGroupMember {
                worker_idx,
                conn_idx,
                stream_id,
                consumer_id: consumer_id.to_string(),
            });
        }
    }

    /// Handles `ConsumerGroupJoin`. Parses the payload, calls the group
    /// coordinator, records the local membership, and ACKs with the
    /// generation.
    pub fn handle_group_join(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let (topic, group, consumer, rest) = match Self::parse_group_prefix(frame.payload) {
            Some(v) => v,
            None => {
                tracing::warn!(conn_idx = ctx.conn_idx, "[groups] join: malformed payload");
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupJoin,
                    Bytes::from_static(br#"{"error":"malformed"}"#),
                );
                return;
            }
        };
        if rest.len() < 4 {
            tracing::warn!(conn_idx = ctx.conn_idx, "[groups] join: missing partitions");
            self.enqueue_response(
                ctx,
                frame.stream_id,
                MessageType::ConsumerGroupJoin,
                Bytes::from_static(br#"{"error":"missing_partitions"}"#),
            );
            return;
        }
        let partitions = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let group_id = match GroupId::new(group) {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(conn_idx = ctx.conn_idx, error = ?e, "[groups] invalid group id");
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupJoin,
                    Bytes::from_static(br#"{"error":"invalid_group"}"#),
                );
                return;
            }
        };
        let consumer_id = match ConsumerId::new(consumer) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(conn_idx = ctx.conn_idx, error = ?e, "[groups] invalid consumer id");
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupJoin,
                    Bytes::from_static(br#"{"error":"invalid_consumer"}"#),
                );
                return;
            }
        };
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let generation = match self
            .groups
            .join(topic, &group_id, &consumer_id, partitions, now_ns)
        {
            Ok(generation) => generation,
            Err(e) => {
                tracing::warn!(conn_idx = ctx.conn_idx, error = ?e, "[groups] join failed");
                let ack = r#"{"error":"join_failed"}"#.to_string();
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupJoin,
                    Bytes::from(ack),
                );
                return;
            }
        };
        // Record local membership.
        // Dedup by consumer_id: heartbeat re-joins (the bench harness
        // re-sends ConsumerGroupJoin every 1 s) must NOT create duplicate
        // entries — otherwise the round-robin counter cycles through the
        // same physical member multiple times, causing duplicate deliveries.
        let local = self
            .group_locals
            .entry((topic.to_string(), group.to_string()))
            .or_insert_with(|| GroupLocal {
                members: Vec::new(),
                rr_idx: 0,
            });
        let should_broadcast;
        if let Some(existing) = local.members.iter_mut().find(|m| m.consumer == consumer_id) {
            // Re-join from an existing consumer: update conn_idx/stream
            // (handles reconnection to a new QUIC connection). Only
            // broadcast if the connection actually changed.
            should_broadcast =
                existing.conn_idx != ctx.conn_idx || existing.quic_stream_id != ctx.quic_stream_id;
            existing.conn_idx = ctx.conn_idx;
            existing.quic_stream_id = ctx.quic_stream_id;
        } else {
            should_broadcast = true;
            local.members.push(GroupMember {
                conn_idx: ctx.conn_idx,
                quic_stream_id: ctx.quic_stream_id,
                consumer: consumer_id.clone(),
            });
        }
        // Notify other workers (and update worker 0's local global registry).
        // Skipped on heartbeat re-joins (same conn) to avoid flooding the
        // inter-worker channels.
        if should_broadcast && !self.worker_outboxes.is_empty() {
            if self.worker_self_idx == 0 {
                self.record_global_member(
                    0,
                    topic,
                    group,
                    ctx.conn_idx,
                    ctx.quic_stream_id,
                    consumer_id.as_str(),
                );
            }
            self.broadcast_worker_msg(InterWorkerMsg::GroupGained {
                worker_idx: self.worker_self_idx as u16,
                topic: topic.to_string(),
                group: group.to_string(),
                conn_idx: ctx.conn_idx,
                stream_id: ctx.quic_stream_id,
                consumer_id: consumer_id.to_string(),
            });
        }
        let group_key = (topic.to_string(), group.to_string(), consumer_id);
        let keys = self.conn_group_keys.entry(ctx.conn_idx).or_default();
        if !keys.contains(&group_key) {
            keys.push(group_key);
        }
        tracing::info!(
            conn_idx = ctx.conn_idx,
            topic = topic,
            group = group,
            generation = generation,
            "[groups] join"
        );
        let mut gbuf = itoa::Buffer::new();
        let g = gbuf.format(generation);
        self.send_ack(
            ctx,
            frame.stream_id,
            MessageType::ConsumerGroupJoin,
            &[
                b"{\"joined\":\"",
                topic.as_bytes(),
                b"\",\"group\":\"",
                group.as_bytes(),
                b"\",\"generation\":",
                g.as_bytes(),
                b"}",
            ],
        );
    }

    /// Handles `ConsumerGroupLeave`.
    pub fn handle_group_leave(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let (topic, group, consumer, _rest) = match Self::parse_group_prefix(frame.payload) {
            Some(v) => v,
            None => {
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupLeave,
                    Bytes::from_static(br#"{"error":"malformed"}"#),
                );
                return;
            }
        };
        let group_id = match GroupId::new(group) {
            Ok(g) => g,
            Err(_) => {
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupLeave,
                    Bytes::from_static(br#"{"error":"invalid_group"}"#),
                );
                return;
            }
        };
        let consumer_id = match ConsumerId::new(consumer) {
            Ok(c) => c,
            Err(_) => {
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupLeave,
                    Bytes::from_static(br#"{"error":"invalid_consumer"}"#),
                );
                return;
            }
        };
        let _ = self.groups.leave(topic, &group_id, &consumer_id);
        self.remove_group_member(topic, group, &consumer_id);
        // Remove from conn_group_keys.
        if let Some(keys) = self.conn_group_keys.get_mut(&ctx.conn_idx) {
            keys.retain(|(t, g, c)| !(t == topic && g == group && c.as_str() == consumer));
        }
        tracing::info!(
            conn_idx = ctx.conn_idx,
            topic = topic,
            group = group,
            "[groups] leave"
        );
        self.send_ack(
            ctx,
            frame.stream_id,
            MessageType::ConsumerGroupLeave,
            &[
                b"{\"left\":\"",
                topic.as_bytes(),
                b"\",\"group\":\"",
                group.as_bytes(),
                b"\"}",
            ],
        );
    }

    /// Handles `ConsumerGroupHeartbeat`.
    pub fn handle_group_heartbeat(&mut self, ctx: &FrameContext, frame: &FrameRef<'_>) {
        let (topic, group, consumer, rest) = match Self::parse_group_prefix(frame.payload) {
            Some(v) => v,
            None => {
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupHeartbeat,
                    Bytes::from_static(br#"{"error":"malformed"}"#),
                );
                return;
            }
        };
        if rest.len() < 4 {
            self.enqueue_response(
                ctx,
                frame.stream_id,
                MessageType::ConsumerGroupHeartbeat,
                Bytes::from_static(br#"{"error":"missing_generation"}"#),
            );
            return;
        }
        let generation = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let group_id = match GroupId::new(group) {
            Ok(g) => g,
            Err(_) => {
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupHeartbeat,
                    Bytes::from_static(br#"{"error":"invalid_group"}"#),
                );
                return;
            }
        };
        let consumer_id = match ConsumerId::new(consumer) {
            Ok(c) => c,
            Err(_) => {
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupHeartbeat,
                    Bytes::from_static(br#"{"error":"invalid_consumer"}"#),
                );
                return;
            }
        };
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        match self
            .groups
            .heartbeat(topic, &group_id, &consumer_id, generation, now_ns)
        {
            Ok(cur_gen) => {
                let mut gbuf = itoa::Buffer::new();
                let g = gbuf.format(cur_gen);
                self.send_ack(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupHeartbeat,
                    &[
                        b"{\"heartbeat\":\"",
                        topic.as_bytes(),
                        b"\",\"group\":\"",
                        group.as_bytes(),
                        b"\",\"generation\":",
                        g.as_bytes(),
                        b"}",
                    ],
                );
            }
            Err(e) => {
                if self.heartbeat_fail_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        error = ?e,
                        total = self.heartbeat_fail_drop.count(),
                        "[groups] heartbeat failed (logging every 64th)"
                    );
                }
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupHeartbeat,
                    Bytes::from_static(br#"{"error":"heartbeat_failed"}"#),
                );
            }
        }
    }

    /// Removes a consumer from `group_locals` for a given (topic, group).
    /// If the group has no remaining members, the entry is dropped
    /// entirely so publish fan-out skips it.
    fn remove_group_member(&mut self, topic: &str, group: &str, consumer: &ConsumerId) {
        let key = (topic.to_string(), group.to_string());
        let mut was_present = false;
        if let Some(local) = self.group_locals.get_mut(&key) {
            let before = local.members.len();
            local
                .members
                .retain(|m| m.consumer.as_str() != consumer.as_str());
            was_present = local.members.len() < before;
            if local.members.is_empty() {
                self.group_locals.remove(&key);
            } else if local.rr_idx >= local.members.len() {
                local.rr_idx = 0;
            }
        }
        if was_present {
            // Update worker 0's global registry
            if self.worker_self_idx == 0 {
                if let Some(members) = self.remote_group_members.get_mut(&key) {
                    members.retain(|m| {
                        !(m.consumer_id == consumer.as_str()
                            && m.worker_idx == self.worker_self_idx as u16)
                    });
                    if members.is_empty() {
                        self.remote_group_members.remove(&key);
                        self.group_rr.remove(&key);
                    }
                }
            }
            // Notify other workers
            if !self.worker_outboxes.is_empty() {
                self.broadcast_worker_msg(InterWorkerMsg::GroupLost {
                    worker_idx: self.worker_self_idx as u16,
                    topic: topic.to_string(),
                    group: group.to_string(),
                    consumer_id: consumer.as_str().to_string(),
                });
            }
        }
    }

    /// Drives the consumer-group heartbeat timeout. Evicts stale members
    /// and removes their local tracking entries. Called once per batch
    /// tick from [`ApplicationLayer::on_batch_tick`].
    pub fn tick_groups(&mut self) {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let evicted = self.groups.tick(now_ns);
        if evicted > 0 {
            tracing::debug!(evicted, "[groups] tick evicted stale members");
            // Clean up group_locals entries that have no live members.
            // The coordinator already removed stale consumers internally;
            // we mirror that in our local index by removing members whose
            // consumer_id is no longer tracked by the coordinator.
            let mut stale_keys: Vec<(String, String, ConsumerId)> = Vec::new();
            for ((topic, group), local) in &self.group_locals {
                let group_id = match GroupId::new(group) {
                    Ok(g) => g,
                    Err(_) => continue,
                };
                for m in &local.members {
                    if self
                        .groups
                        .owned_partitions(topic, &group_id, &m.consumer)
                        .is_none()
                    {
                        stale_keys.push((topic.clone(), group.clone(), m.consumer.clone()));
                    }
                }
            }
            for (topic, group, consumer) in stale_keys {
                self.remove_group_member(&topic, &group, &consumer);
                // Also clean conn_group_keys.
                self.conn_group_keys.values_mut().for_each(|keys| {
                    keys.retain(|(t, g, c)| !(t == &topic && g == &group && c == &consumer));
                });
            }
        }
    }

    // ── Cluster inbox draining (P6) ─────────────────────────────────

    /// Drains the per-worker cluster inbox, processing incoming
    /// [`InterNodeMessage`]s from other nodes.
    ///
    /// Called once per batch tick from [`ApplicationLayer::on_batch_tick`].
    /// Handles four message kinds:
    /// * `Publish` — another node forwarded a publish to us (the
    ///   topic owner). Fan out to local subscribers as if a local
    ///   client had published it.
    /// * `Fanout` — the topic owner is delivering a publish to us
    ///   because we previously registered remote interest. Deliver
    ///   to local subscribers via the inner payload's topic.
    /// * `Subscribe` — a non-owner node is registering remote
    ///   interest in a pattern. Record the remote subscriber in our
    ///   coordinator's `RemoteSubscriberRegistry`.
    /// * `Unsubscribe` — a non-owner is dropping remote interest.
    pub fn drain_cluster_inbox(&mut self) {
        // Drain into a reusable scratch Vec — releases the borrow on
        // `self.cluster_incoming_rx` before we call back into
        // `self.cluster_deliver_local` / `self.coordinator`, and avoids
        // `Vec::with_capacity(1024)` per tick (cluster inbox is usually
        // empty outside of high-throughput forwarded-publish scenarios).
        const CLUSTER_DRAIN_CAP: usize = 1024;
        self.cluster_inbox_scratch.clear();
        if let Some(rx) = &mut self.cluster_incoming_rx {
            while self.cluster_inbox_scratch.len() < CLUSTER_DRAIN_CAP {
                match rx.try_recv() {
                    Ok(msg) => self.cluster_inbox_scratch.push(msg),
                    Err(_) => break,
                }
            }
        }
        if self.cluster_inbox_scratch.is_empty() {
            return;
        }
        let processed = self.cluster_inbox_scratch.len();
        for i in 0..processed {
            let msg = std::mem::replace(
                &mut self.cluster_inbox_scratch[i],
                InterNodeMessage::publish(
                    session_registry::NodeId::nil(),
                    0,
                    0,
                    Arc::<str>::from("_"),
                    Bytes::new(),
                ),
            );
            match msg.kind {
                cluster_coordinator::InterNodeKind::Publish => {
                    // The owner received a forwarded publish. Fan out
                    // to remote subscribers (nodes whose local clients
                    // subscribed to this topic) before local delivery.
                    // Without this, a publish forwarded from a
                    // non-owner node never reaches subscribers on
                    // other nodes — the original Forward only sends
                    // to the owner, and cluster_deliver_local only
                    // checks local subscribers.
                    //
                    // Only the OWNER re-fans. Replicas receiving a
                    // replicated Publish must NOT re-fan (the owner
                    // already sent Fanout to all remote targets).
                    if let Some(coord) = &self.coordinator {
                        if coord.owns_topic(&msg.topic_or_pattern) {
                            let remote_targets =
                                coord.collect_remote_targets(&msg.topic_or_pattern);
                            // Skip the origin node — it already delivered
                            // locally when it forwarded the publish to us.
                            let fanout_targets: Vec<_> = remote_targets
                                .into_iter()
                                .filter(|&t| t != msg.origin_node)
                                .collect();
                            if !fanout_targets.is_empty() {
                                if let Some(tx) = &self.cluster_outgoing_tx {
                                    let fanout = cluster_coordinator::InterNodeMessage::fanout(
                                        msg.origin_node,
                                        msg.origin_conn_idx,
                                        msg.origin_stream_id,
                                        msg.topic_or_pattern.clone(),
                                        msg.payload.clone(),
                                    );
                                    for target in fanout_targets {
                                        let _ = tx.send(OutgoingMsg {
                                            msg: fanout.clone(),
                                            target,
                                        });
                                    }
                                    tracing::trace!(
                                        topic = %msg.topic_or_pattern,
                                        "[cluster] forwarded publish re-fanned to remote subscribers"
                                    );
                                }
                            }
                        }
                    }
                    if let Some(bytes) = self.cluster_deliver_local(
                        &msg.topic_or_pattern,
                        &msg.payload,
                        msg.origin_conn_idx,
                        msg.origin_stream_id,
                    ) {
                        // Consumer-group delivery: worker 0 only (or any
                        // worker in single-worker mode).
                        if self.worker_outboxes.is_empty() || self.worker_self_idx == 0 {
                            self.do_group_delivery(bytes, MessageType::Publish);
                        }
                    }
                }
                cluster_coordinator::InterNodeKind::Fanout => {
                    // Fanout messages are already targeted at nodes with
                    // subscribers — deliver locally only (no re-fanout,
                    // which would create forwarding loops). Group delivery
                    // was already done by the owner's Publish handler.
                    let _ = self.cluster_deliver_local(
                        &msg.topic_or_pattern,
                        &msg.payload,
                        msg.origin_conn_idx,
                        msg.origin_stream_id,
                    );
                }
                cluster_coordinator::InterNodeKind::Subscribe => {
                    if let Some(coord) = &mut self.coordinator {
                        coord.add_remote_sub(
                            &msg.topic_or_pattern,
                            RemoteSubscriber {
                                node_id: msg.origin_node,
                                remote_conn_idx: msg.origin_conn_idx,
                                remote_quic_stream_id: msg.origin_stream_id,
                            },
                        );
                        tracing::debug!(
                            node = %msg.origin_node.raw(),
                            pattern = %msg.topic_or_pattern,
                            "[cluster] remote subscribe registered"
                        );
                    }
                }
                cluster_coordinator::InterNodeKind::Unsubscribe => {
                    if let Some(coord) = &mut self.coordinator {
                        coord.remove_remote_sub(
                            &msg.topic_or_pattern,
                            &RemoteSubscriber {
                                node_id: msg.origin_node,
                                remote_conn_idx: msg.origin_conn_idx,
                                remote_quic_stream_id: msg.origin_stream_id,
                            },
                        );
                        tracing::debug!(
                            node = %msg.origin_node.raw(),
                            pattern = %msg.topic_or_pattern,
                            "[cluster] remote unsubscribe"
                        );
                    }
                }
                cluster_coordinator::InterNodeKind::Heartbeat => {
                    if let Some(coord) = &mut self.coordinator {
                        coord.record_heartbeat(msg.origin_node);
                    }
                }
                cluster_coordinator::InterNodeKind::NodeDown => {
                    // Gossip: mark the dead node (origin_node = dead node)
                    // down so ownership shifts to the next node on the
                    // ring. Convergence: once enough nodes agree, the
                    // dead node is fully removed from routing tables.
                    if let Some(coord) = &mut self.coordinator {
                        let dropped = coord.mark_node_down(msg.origin_node);
                        tracing::info!(
                            dead_node = msg.origin_node.raw(),
                            dropped_subs = dropped,
                            "[cluster] node-down gossip received, marked dead"
                        );
                    }
                }
            }
        }
        tracing::trace!(processed, "[cluster] inbox drained");
    }

    /// Drives the cluster heartbeat + failure-detection cycle.
    ///
    /// Called from [`ApplicationLayer::on_batch_tick`] (which fires on
    /// every reactor tick). To avoid redundant work:
    ///
    /// * **Heartbeat send** runs at most once per `heartbeat_interval`.
    ///   Only **worker 0** sends (or the sole worker in single-worker
    ///   mode) so N workers don't emit N× redundant datagrams.
    /// * **Health check** (`tick_health`) also runs at most once per
    ///   `heartbeat_interval` — scans the failure detector and marks
    ///   down any peer that hasn't heartbeated within `heartbeat_timeout`.
    ///   For each newly-dead node, a `NodeDown` gossip is broadcast so
    ///   the rest of the cluster converges on removal.
    pub fn tick_cluster_health(&mut self) {
        let now = std::time::Instant::now();
        let Some(coord) = &mut self.coordinator else {
            return; // single-node mode
        };
        let interval = coord.heartbeat_interval();
        let due = self
            .last_health_check
            .map_or(true, |t| now.duration_since(t) >= interval);
        if !due {
            return;
        }
        self.last_health_check = Some(now);

        // Worker 0 (or sole worker) broadcasts heartbeats to all peers.
        let is_heartbeater = self.worker_outboxes.is_empty() || self.worker_self_idx == 0;
        if is_heartbeater {
            if let Some(tx) = &self.cluster_outgoing_tx {
                let hb = cluster_coordinator::InterNodeMessage::heartbeat(self.node_id);
                let peers = coord.membership().alive_nodes();
                for peer in peers {
                    if peer != self.node_id {
                        let _ = tx.send(OutgoingMsg {
                            msg: hb.clone(),
                            target: peer,
                        });
                    }
                }
            }
            self.last_heartbeat_send = Some(now);
        }

        // Re-broadcast forwarded subscriptions to their topic owners.
        // Inter-node Subscribe is fire-and-forget UDP — a single lost
        // datagram (or a ring not yet formed at subscribe time) means the
        // owner never learns about our subscriber, causing 0 deliveries
        // until manual re-subscribe. Re-sending on every heartbeat tick
        // (1 s) self-heals from both. The owner's `add_remote_sub` is
        // idempotent, so duplicates are harmless.
        if is_heartbeater {
            if let Some(tx) = &self.cluster_outgoing_tx {
                for fs in &self.forwarded_subs {
                    // Resolve owner dynamically — the ring may have grown
                    // since the subscribe arrived.
                    let Some(owner) = coord.router().owner_of(&fs.pattern) else {
                        continue; // ring not yet formed for this pattern
                    };
                    if owner == self.node_id {
                        continue; // we own this pattern — no forwarding needed
                    }
                    let msg = InterNodeMessage::subscribe(
                        self.node_id,
                        fs.conn_idx,
                        fs.stream_id,
                        fs.pattern.clone(),
                    );
                    let _ = tx.send(OutgoingMsg { msg, target: owner });
                }
            }
        }

        // Check for expired nodes.
        let expired = coord.tick_health();
        if !expired.is_empty() {
            // Broadcast NodeDown gossip for each expired node.
            if let Some(tx) = &self.cluster_outgoing_tx {
                for &dead in &expired {
                    let gossip =
                        cluster_coordinator::InterNodeMessage::node_down(self.node_id, dead);
                    let peers = coord.membership().alive_nodes();
                    for peer in peers {
                        if peer != self.node_id {
                            let _ = tx.send(OutgoingMsg {
                                msg: gossip.clone(),
                                target: peer,
                            });
                        }
                    }
                }
            }
        }
    }

    /// Delivers an inter-node Publish/Fanout payload to local
    /// subscribers. Mirrors the wire encoding used by `handle_publish`:
    /// `topic_len(u16) | topic | payload`.
    ///
    /// Returns the framed `Bytes` on success so the caller can pass it
    /// to `do_group_delivery` for consumer-group delivery (worker 0 only).
    ///
    /// Inter-node delivered messages do NOT re-trigger cluster
    /// routing (that would loop): this method consults only the local
    /// `pubsub_registry`.
    fn cluster_deliver_local(
        &mut self,
        topic: &str,
        payload: &[u8],
        _origin_conn_idx: u32,
        _origin_stream_id: u64,
    ) -> Option<Bytes> {
        let topic_ref = match pubsub_engine::Topic::new(topic) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    topic = topic,
                    error = ?e,
                    "[cluster] incoming publish has invalid topic — dropping"
                );
                return None;
            }
        };
        let targets: Vec<Subscriber> = self
            .pubsub_registry
            .lookup(topic_ref.as_str())
            .into_iter()
            .collect();

        // Re-encode as a Publish frame so downstream subscribers receive the
        // same wire shape the publisher used (topic_len + topic + payload).
        let topic_str = topic_ref.as_str();
        self.encode_scratch.clear();
        self.encode_scratch
            .extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
        self.encode_scratch.extend_from_slice(topic_str.as_bytes());
        self.encode_scratch.extend_from_slice(payload);
        let bytes = self.encode_scratch.split().freeze();

        // WAL append: when a publish arrives via the cluster (either
        // forwarded to the owner or replicated to a backup), persist
        // it locally. This gives the cluster durable storage without
        // requiring a separate replication transport.
        if let Some(wal) = &mut self.wal {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let record = wal_store::WalRecord::new(now_ns, bytes.clone());
            if let Err(e) = wal.append(topic_str, record) {
                tracing::warn!(
                    topic = topic_str,
                    error = ?e,
                    "[cluster] WAL append failed for incoming publish"
                );
            }
        }

        // Use a synthetic logical stream for inter-node-delivered
        // publishes — they aren't bound to a particular subscriber's
        // declared stream. The wire shape still needs a StreamId for
        // the frame header.
        let stream = StreamId::new(0xFFFF_FFFF);
        for target in targets {
            self.enqueue_frame(
                target.conn_idx,
                target.quic_stream_id,
                stream,
                MessageType::Publish,
                bytes.clone(),
            );
        }

        // Consumer-group delivery is handled by the caller via
        // do_group_delivery (worker 0 only).
        Some(bytes)
    }

    /// Drains forwarded publishes from other workers and delivers them
    /// to local subscribers. Called from `on_batch_tick` each reactor
    /// cycle. Bounded at 256 messages per drain to avoid monopolising
    /// the reactor thread.
    ///
    /// Delivered publishes are NOT re-broadcast to other workers,
    /// preventing forwarding loops.
    pub fn drain_worker_inbox(&mut self) {
        let Some(rx) = &mut self.worker_inbox else {
            return;
        };
        // Backpressure gate: when subscriber QUIC streams are flow-controlled
        // and pending_sends is above the high-watermark, stop draining the
        // cross-worker inbox. This lets `drain_pending_sends` catch up
        // instead of flooding pending_sends to the 1 GiB eviction cap.
        // The sender-side `try_send` will see a full channel and drop,
        // providing backpressure to the publishing worker.
        if self.pending_sends_high {
            return;
        }
        const CAP: usize = 32_768;
        self.worker_inbox_scratch.clear();
        while self.worker_inbox_scratch.len() < CAP {
            match rx.try_recv() {
                Ok(msg) => self.worker_inbox_scratch.push(msg),
                Err(_) => break,
            }
        }
        if self.worker_inbox_scratch.is_empty() {
            return;
        }
        let count = self.worker_inbox_scratch.len();
        tracing::debug!(
            worker = self.worker_self_idx,
            count,
            "[pubsub] worker inbox drained"
        );
        // Process by index — `self` is borrowed mutably inside the
        // match arms, so we can't use `for msg in batch.drain(..)` with
        // a self-reference in the same closure.
        for i in 0..count {
            let msg = std::mem::replace(
                &mut self.worker_inbox_scratch[i],
                InterWorkerMsg::Publish {
                    framed: Bytes::new(),
                    msg_type: MessageType::Publish,
                    reliable: false,
                },
            );
            match msg {
                InterWorkerMsg::Publish {
                    framed,
                    msg_type,
                    reliable,
                } => {
                    self.deliver_framed_local(framed, msg_type, reliable);
                }
                InterWorkerMsg::SubGained {
                    worker_idx,
                    pattern,
                } => {
                    if let Ok(pat) = pubsub_engine::TopicPattern::new(&pattern) {
                        let bit = 1u32 << worker_idx;
                        *self.remote_pattern_interest.entry(pat).or_insert(0) |= bit;
                    }
                    self.topic_worker_bitmap_cache.clear();
                }
                InterWorkerMsg::SubLost {
                    worker_idx,
                    pattern,
                } => {
                    if let Ok(pat) = pubsub_engine::TopicPattern::new(&pattern) {
                        if let Some(bitmap) = self.remote_pattern_interest.get_mut(&pat) {
                            *bitmap &= !(1u32 << worker_idx);
                            if *bitmap == 0 {
                                self.remote_pattern_interest.remove(&pat);
                            }
                        }
                    }
                    self.topic_worker_bitmap_cache.clear();
                }
                InterWorkerMsg::GroupGained {
                    worker_idx,
                    topic,
                    group,
                    conn_idx,
                    stream_id,
                    consumer_id,
                } => {
                    // Only worker 0 maintains the global registry.
                    if self.worker_self_idx == 0 {
                        self.record_global_member(
                            worker_idx,
                            &topic,
                            &group,
                            conn_idx,
                            stream_id,
                            &consumer_id,
                        );
                    }
                }
                InterWorkerMsg::GroupLost {
                    worker_idx,
                    topic,
                    group,
                    consumer_id,
                } => {
                    if self.worker_self_idx == 0 {
                        let key = (topic, group);
                        if let Some(members) = self.remote_group_members.get_mut(&key) {
                            members.retain(|m| {
                                !(m.consumer_id == consumer_id && m.worker_idx == worker_idx)
                            });
                            if members.is_empty() {
                                self.remote_group_members.remove(&key);
                                self.group_rr.remove(&key);
                            }
                        }
                    }
                }
                InterWorkerMsg::GroupDeliver {
                    framed,
                    target,
                    msg_type,
                } => match target {
                    Some((conn_idx, stream_id)) => {
                        // Worker 0 routed a delivery to a member on this worker.
                        let stream = StreamId::new(0xFFFF_FFFF);
                        self.enqueue_frame(conn_idx, stream_id, stream, msg_type, framed);
                    }
                    None => {
                        // Another worker asks us (worker 0) to do global
                        // group delivery.
                        self.do_group_delivery(framed, msg_type);
                    }
                },
            }
        }
    }

    /// Global consumer-group delivery. Parses the topic from `framed`
    /// bytes (`[topic_len:u16 BE][topic][payload]`), finds all groups
    /// matching the topic, picks one member per group via round-robin,
    /// and delivers either locally (`enqueue_frame`) or remotely
    /// (`GroupDeliver` with a targeted `Some(conn_idx, stream_id)`).
    ///
    /// Two code paths:
    /// - **Multi-worker** (`remote_group_members` populated): worker 0
    ///   does global round-robin across all workers' members.
    /// - **Single-worker fallback** (no `remote_group_members`): local
    ///   round-robin via `group_locals` — same as the inline code in
    ///   `handle_publish`'s single-worker branch.
    fn do_group_delivery(&mut self, framed: Bytes, msg_type: MessageType) {
        // Parse topic from framed bytes.
        if framed.len() < 2 {
            return;
        }
        let topic_len = u16::from_be_bytes([framed[0], framed[1]]) as usize;
        if framed.len() < 2 + topic_len {
            return;
        }
        let topic_str = match std::str::from_utf8(&framed[2..2 + topic_len]) {
            Ok(s) => s,
            Err(_) => return,
        };

        // Check if we have global members for this topic (multi-worker).
        let has_global = self
            .remote_group_members
            .iter()
            .any(|((t, _), m)| t.as_str() == topic_str && !m.is_empty());

        if has_global {
            // Multi-worker: global round-robin via remote_group_members.
            // Phase 1: read-only scan.
            enum Target {
                Local(usize, u64),
                Remote(u16, usize, u64),
            }
            let mut decisions: Vec<Target> = Vec::new();
            let mut rr_updates: Vec<((String, String), usize)> = Vec::new();

            for (key, members) in &self.remote_group_members {
                if key.0.as_str() != topic_str || members.is_empty() {
                    continue;
                }
                let rr = self.group_rr.get(key).copied().unwrap_or(0);
                let idx = rr % members.len();
                rr_updates.push((key.clone(), rr.wrapping_add(1)));
                let m = &members[idx];
                if m.worker_idx as usize == self.worker_self_idx {
                    decisions.push(Target::Local(m.conn_idx, m.stream_id));
                } else {
                    decisions.push(Target::Remote(m.worker_idx, m.conn_idx, m.stream_id));
                }
            }

            // Phase 2: apply rr updates.
            for (key, new_rr) in rr_updates {
                self.group_rr.insert(key, new_rr);
            }

            // Phase 3: execute.
            let stream = StreamId::new(0xFFFF_FFFF);
            for decision in decisions {
                match decision {
                    Target::Local(conn_idx, stream_id) => {
                        self.enqueue_frame(conn_idx, stream_id, stream, msg_type, framed.clone());
                    }
                    Target::Remote(worker_idx, conn_idx, stream_id) => {
                        if let Some(outbox) = self.worker_outboxes.get(worker_idx as usize) {
                            let _ = outbox.try_send(InterWorkerMsg::GroupDeliver {
                                framed: framed.clone(),
                                target: Some((conn_idx, stream_id)),
                                msg_type,
                            });
                        }
                    }
                }
            }
        } else {
            // Single-worker fallback: local round-robin via group_locals.
            self.group_targets_scratch.clear();
            for ((t, _g), local) in &mut self.group_locals {
                if t.as_str() != topic_str || local.members.is_empty() {
                    continue;
                }
                let idx = local.rr_idx % local.members.len();
                local.rr_idx = local.rr_idx.wrapping_add(1);
                let m = &local.members[idx];
                let _ = self
                    .group_targets_scratch
                    .try_push((m.conn_idx, m.quic_stream_id));
            }
            let stream = StreamId::new(0xFFFF_FFFF);
            let group_len = self.group_targets_scratch.len();
            for i in 0..group_len {
                let (conn_idx, stream_id) = self.group_targets_scratch[i];
                self.enqueue_frame(conn_idx, stream_id, stream, msg_type, framed.clone());
            }
        }
    }

    /// Fast path for cross-worker publishes: deliver an already-encoded
    /// `[topic_len:u16 BE][topic][payload]` frame to local subscribers
    /// without re-encoding.
    ///
    /// This is the receive-side counterpart to the producer change in
    /// `handle_publish` — worker A encodes once and forwards the `Bytes`
    /// handle; worker B clones it (refcount bump) per subscriber. The
    /// topic is re-parsed from the frame header via zero-copy `Bytes`
    /// slicing for the registry lookup, avoiding the `String` allocation
    /// the previous `cluster_deliver_local` path incurred.
    ///
    /// Same semantics as `cluster_deliver_local` for regular fan-out.
    /// Consumer-group delivery is NOT handled here — it goes through
    /// the `GroupDeliver` inter-worker path to worker 0.
    fn deliver_framed_local(&mut self, framed: Bytes, msg_type: MessageType, reliable: bool) {
        // Zero-copy topic parse: first 2 bytes = topic_len BE, then topic.
        if framed.len() < 2 {
            return;
        }
        let topic_len = u16::from_be_bytes([framed[0], framed[1]]) as usize;
        if framed.len() < 2 + topic_len {
            return;
        }
        let topic_str: &str = match std::str::from_utf8(&framed[2..2 + topic_len]) {
            Ok(s) => s,
            Err(_) => return,
        };
        let topic_ref = match pubsub_engine::Topic::new(topic_str) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    topic = topic_str,
                    error = ?e,
                    "[pubsub] forwarded publish has invalid topic — dropping"
                );
                return;
            }
        };

        let targets: Vec<Subscriber> = self
            .pubsub_registry
            .lookup(topic_ref.as_str())
            .into_iter()
            .collect();

        // Set reliable_fanout so enqueue_frame stamps ACK_REQ and retains
        // copies in the shared replay buffer for subscribers with logical
        // sessions. Reset after the fan-out loop.
        self.reliable_fanout = reliable;

        // Synthetic stream id for forwarded publishes (same convention
        // as `cluster_deliver_local`).
        let stream = StreamId::new(0xFFFF_FFFF);
        for target in targets {
            self.enqueue_frame(
                target.conn_idx,
                target.quic_stream_id,
                stream,
                msg_type,
                framed.clone(),
            );
        }

        // Dormant session retention for cross-worker publishes — same logic
        // as handle_publish, but using the shared replay state.
        if reliable {
            let mut rs = self.replay.lock().unwrap();
            if !rs.dormant_sessions.is_empty() {
                let dormant: Vec<(u64, Vec<u64>)> = rs
                    .dormant_sessions
                    .iter()
                    .filter(|(_, (_, pats))| {
                        pats.iter()
                            .any(|p| pattern_matches(p.as_ref(), topic_str))
                    })
                    .map(|(&l, (qsid, _))| (l, qsid.clone()))
                    .collect();
                for (logical, quic_stream_ids) in dormant {
                    for qsid in &quic_stream_ids {
                        let seq_val = rs
                            .session_stream_seq
                            .get(&(logical, *qsid))
                            .copied()
                            .unwrap_or(Seq::ZERO);
                        let header = FrameHeader::new(stream, MessageType::Publish)
                            .with_seq(seq_val)
                            .with_flags(FrameFlags::ACK_REQ);
                        rs.buffer.append(
                            logical,
                            StreamId::new(*qsid),
                            ReplayEntry {
                                seq: seq_val.get(),
                                header: header.clone(),
                                payload: framed.clone(),
                            },
                        );
                        rs.session_stream_seq
                            .insert((logical, *qsid), seq_val.next());
                    }
                }
            }
        }

        self.reliable_fanout = false;

        // Consumer-group delivery is handled exclusively via the
        // GroupDeliver path (see handle_publish → do_group_delivery).
    }

    /// Apply the configured WAL retention policy to every tracked
    /// topic if the sweep interval has elapsed. Cheap no-op when WAL
    /// is unconfigured or the policy is `keep_forever`.
    ///
    /// Throttled to one sweep per `wal_sweep_interval`
    /// (60 s). The sweep walks every topic known to the store, calls
    /// [`wal_store::RetentionPolicy::apply`], and aggregates the
    /// removed-entry count for a single summary log line.
    fn maybe_run_wal_retention(&mut self) {
        // Fast path: no WAL — nothing to sweep, nothing to persist.
        if self.wal.is_none() && self.offsets_path.is_none() {
            return;
        }
        // Throttle interval applies to BOTH retention and offset-snapshot
        // so the disk walk happens at most once per `wal_sweep_interval`.
        let now = Instant::now();
        let due = self
            .wal_retention_last_run
            .map_or(true, |t| now.duration_since(t) >= self.wal_sweep_interval);
        if !due {
            return;
        }
        self.wal_retention_last_run = Some(now);

        // Run the retention walk only if a policy is configured. The
        // common case (keep-forever) skips the directory walk entirely.
        let has_policy =
            self.wal_retention.max_bytes.is_some() || self.wal_retention.max_age_ns.is_some();
        if has_policy {
            if let Some(wal) = self.wal.as_mut() {
                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                let policy = self.wal_retention;
                let topics = wal.topic_names();
                if !topics.is_empty() {
                    let mut total_removed: usize = 0;
                    let mut errored: u64 = 0;
                    for topic in &topics {
                        match policy.apply::<wal_store::FileStore>(wal, topic, now_ns) {
                            Ok(stats) => {
                                total_removed = total_removed.saturating_add(stats.removed)
                            }
                            Err(_) => errored = errored.saturating_add(1),
                        }
                    }
                    if total_removed > 0 || errored > 0 {
                        tracing::info!(
                            topics = topics.len(),
                            removed = total_removed,
                            errored,
                            retained_bytes = wal.retained_bytes(),
                            "[wal] retention sweep"
                        );
                    } else {
                        tracing::debug!(
                            topics = topics.len(),
                            retained_bytes = wal.retained_bytes(),
                            "[wal] retention sweep (no removals)"
                        );
                    }
                }
            }
        }

        // Persist consumer offsets alongside the retention sweep. A
        // failure here only risks at-least-once (replay restarts from
        // the previous snapshot), so we warn-and-continue rather than
        // surfacing an error that would abort the tick.
        if let Some(path) = self.offsets_path.as_ref() {
            if let Err(e) = self.consumer_offsets.save_to(path) {
                tracing::warn!(
                    path = ?path,
                    error = %e,
                    "[wal] consumer-offsets snapshot failed — previous snapshot retained"
                );
            }
        }
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
            MessageType::Subscribe
            | MessageType::Unsubscribe
            | MessageType::Publish
            | MessageType::TopicCreate
            | MessageType::TopicDelete => {
                self.route_native_pubsub(ctx, frame);
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    msg_type = ?frame.msg_type,
                    payload_len = frame.payload.len(),
                    "[app:pubsub] native control"
                );
            }
            MessageType::Fetch => {
                self.handle_fetch(ctx, frame);
            }
            MessageType::FetchReply => {
                // Server doesn't accept FetchReply from clients — it's
                // a server→subscriber data-plane frame. Log + drop.
                if self.fetch_reply_ignore_drop.tick() {
                    tracing::warn!(
                        conn_idx = ctx.conn_idx,
                        total = self.fetch_reply_ignore_drop.count(),
                        "[app:pubsub] client-sent FetchReply ignored (logging every 128th)"
                    );
                }
            }
            MessageType::Error => {
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    payload_len = frame.payload.len(),
                    "[app] Error frame"
                );
            }
            MessageType::Heartbeat => {
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    session_id = ?ctx.session_id,
                    "[app] Heartbeat"
                );
            }
            MessageType::Handshake => {
                self.handle_handshake(ctx, frame);
            }
            MessageType::ConsumerGroupJoin => {
                self.handle_group_join(ctx, frame);
            }
            MessageType::ConsumerGroupLeave => {
                self.handle_group_leave(ctx, frame);
            }
            MessageType::ConsumerGroupHeartbeat => {
                self.handle_group_heartbeat(ctx, frame);
            }
            MessageType::Ack => {
                self.handle_ack(ctx, frame);
            }
            MessageType::Resume => {
                self.handle_resume(ctx, frame);
            }
            MessageType::ResumeOk | MessageType::ResumeUnavailable => {
                // Server→client only — a client sending these is misbehaving.
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    msg_type = ?frame.msg_type,
                    "[app] client-sent ResumeOk/ResumeUnavailable ignored"
                );
            }
            _ => {
                tracing::trace!(
                    conn_idx = ctx.conn_idx,
                    msg_type = ?frame.msg_type,
                    "[app] unhandled data type"
                );
            }
        }
    }

    fn on_connection_closed(
        &mut self,
        conn_idx: usize,
        session_id: SessionId,
        reason: Option<&str>,
    ) {
        let removed_patterns = self
            .pubsub_registry
            .remove_connection_with_patterns(conn_idx);
        let removed = removed_patterns.len();
        // Stop re-broadcasting any forwarded subscriptions for this conn.
        self.forwarded_subs
            .retain(|fs| fs.conn_idx != conn_idx as u32);
        // Decrement per-pattern quota counters to match.
        if !removed_patterns.is_empty() {
            for pat in &removed_patterns {
                if let Some(count) = self.pattern_sub_counts.get_mut(pat) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.pattern_sub_counts.remove(pat);
                        // Notify other workers that this worker lost interest.
                        if !pat.starts_with("_heartbeat.") && !self.worker_outboxes.is_empty() {
                            self.broadcast_worker_msg(InterWorkerMsg::SubLost {
                                worker_idx: self.worker_self_idx as u16,
                                pattern: pat.clone(),
                            });
                        }
                    }
                }
            }
        }
        let principal = self.principals.remove(&conn_idx);
        // Save subscription patterns + quic_stream_ids for dormant retention
        // when the conn has a registered logical session. Stored in the
        // shared replay state so any worker can retain matching gap frames.
        if let Some(&logical) = self.conn_logical_session.get(&conn_idx) {
            if !removed_patterns.is_empty() {
                let mut rs = self.replay.lock().unwrap();
                let quic_stream_ids: Vec<u64> = rs
                    .session_stream_seq
                    .keys()
                    .filter(|&&(l, _)| l == logical)
                    .map(|&(_, qsid)| qsid)
                    .collect();
                rs.dormant_sessions.insert(
                    logical,
                    (
                        quic_stream_ids,
                        removed_patterns
                            .iter()
                            .map(|p| p.as_str().into())
                            .collect(),
                    ),
                );
            }
        }
        // Unbind the logical session — the session itself outlives the
        // connection (retained entries stay in the replay buffer for a
        // future Resume), only the conn_idx mapping goes away. A
        // reconnecting client re-sends Resume and re-establishes the
        // mapping on the new conn_idx.
        self.conn_logical_session.remove(&conn_idx);
        // Drop any pending replay frames for this connection (the streams
        // are gone, they'll be re-buffered on the next Resume).
        self.pending_replays.retain(|&(c, _), _| c != conn_idx);

        // Log + drop per-connection metrics on close.
        if let Some(m) = self.conn_metrics.remove(&conn_idx) {
            if m.publishes_in > 0 || m.deliveries_out > 0 || m.publishes_rejected > 0 {
                tracing::info!(
                    conn_idx,
                    session_id = ?session_id,
                    publishes_in = m.publishes_in,
                    deliveries_out = m.deliveries_out,
                    bytes_in = m.bytes_in,
                    bytes_out = m.bytes_out,
                    rejected = m.publishes_rejected,
                    "[metrics] connection lifetime counters"
                );
            }
        }

        // P5: leave all consumer groups the disconnected connection
        // was a member of.
        if let Some(keys) = self.conn_group_keys.remove(&conn_idx) {
            for (topic, group, consumer) in keys {
                if let Ok(gid) = GroupId::new(&group) {
                    let _ = self.groups.leave(&topic, &gid, &consumer);
                }
                self.remove_group_member(&topic, &group, &consumer);
            }
        }
        tracing::info!(
            conn_idx,
            session_id = ?session_id,
            subs_removed = removed,
            principal = ?principal,
            reason = reason.unwrap_or("none"),
            "[app] connection closed"
        );
    }

    fn drain_outbox(&mut self) -> Vec<OutgoingFrame> {
        std::mem::take(&mut self.outbox)
    }
}

// ── PlatformServices ──────────────────────────────────────────────

/// Container for the infrastructure services actually wired into the
/// hot path. Vireon's scope is pub/sub transport — we only need:
///
/// * `observability` — per-frame counters + connection gauges.
/// * `quotas` — per-tenant byte/session accounting.
///
/// Other application-layer crates (auth-framework, topic-acl, wal-store,
/// etc.) are depended on directly where needed; the dispatcher holds its
/// own `AuthenticationPipeline`, so there's no need for a second facade
/// here.
pub struct PlatformServices {
    /// [observability-sdk] Metrics registry + tracing.
    pub(crate) observability: MetricsRegistry,
    /// [resource-quotas] Per-tenant quota enforcement.
    pub(crate) quotas: QuotaEnforcer,
}

impl PlatformServices {
    /// Constructs the platform service facades wired into the server.
    ///
    /// # Errors
    /// Returns [`ServerError`] if any facade fails to construct.
    pub fn new() -> Result<Self, ServerError> {
        let observability_cfg = observability_sdk::builder::ObservabilityBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("observability-sdk: {e:?}")))?;

        let quotas_cfg = resource_quotas::builder::ResourceQuotasBuilder::new()
            .build()
            .map_err(|e| ServerError::EngineInit(format!("resource-quotas: {e:?}")))?;

        Ok(Self {
            observability: MetricsRegistry::new(
                observability_cfg,
                observability_sdk::stats::ObservabilityStats::default(),
            ),
            quotas: QuotaEnforcer::new(
                quotas_cfg,
                resource_quotas::stats::ResourceQuotaStats::default(),
            ),
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
    pub(crate) platform: PlatformServices,
    /// Application dispatch boundary.
    dispatcher: VireonDispatcher,

    // ── Pre-registered hot-path keys ────────────────────────────────
    // Constructed once at startup to avoid per-frame MetricKey allocation.
    // These mirror the keys registered in PlatformServices.
    metric_frames_rx: MetricKey,
    metric_connections: MetricKey,
    default_tenant: TenantId,
    default_limit: QuotaLimit,
}

impl ApplicationLayer {
    /// Creates a new application layer.
    ///
    /// Pre-registers hot-path metric keys and the default tenant into
    /// the platform services so that the data path can use them without
    /// per-frame allocation.
    pub fn new(t: &LayerTuning) -> Result<Self, ServerError> {
        let mut platform = PlatformServices::new()?;

        // Pre-register hot-path metrics (Counters).
        let metric_frames_rx = MetricKey::new("frames_rx_total")
            .map_err(|e| ServerError::EngineInit(format!("metric key: {e:?}")))?;
        let _ = platform
            .observability
            .register(metric_frames_rx, MetricType::Counter);

        let metric_connections = MetricKey::new("connections_total")
            .map_err(|e| ServerError::EngineInit(format!("metric key: {e:?}")))?;
        let _ = platform
            .observability
            .register(metric_connections, MetricType::Counter);

        // Pre-register default tenant (Phase 1: single tenant, no resolution).
        let default_tenant = TenantId::new(1);
        let default_limit = QuotaLimit::for_tier(TenantTier::Internal);
        let _ = platform
            .quotas
            .register_tenant(default_tenant, TenantTier::Internal);

        Ok(Self {
            session: SessionLayer::new(t)?,
            platform,
            dispatcher: VireonDispatcher::new()
                .with_wal_sweep_interval(t.wal_sweep_interval)
                .with_delivery_config(DeliveryConfig {
                    logtail_threshold: t.delivery_logtail_threshold,
                    force_strategy: t.delivery_force_strategy,
                })
                .with_replay_config(ReplayConfig {
                    max_messages_per_stream: t.replay_max_messages_per_stream,
                    max_bytes: t.replay_max_bytes,
                }),
            metric_frames_rx,
            metric_connections,
            default_tenant,
            default_limit,
        })
    }

    /// Creates a session and notifies the application dispatcher.
    ///
    /// Returns `Some(SessionId)` on success, `None` if the session
    /// table or connection shard is full.
    pub fn on_connect(&mut self, conn_idx: usize, peer: SocketAddr) -> Option<SessionId> {
        let session_id = self.session.on_connect(conn_idx)?;

        // Platform: record connection counter (observability).
        let _ = self
            .platform
            .observability
            .record(self.metric_connections, 1);

        // Platform: reserve a session slot for the default tenant (quotas).
        if let Err(e) = self
            .platform
            .quotas
            .reserve(self.default_tenant, &self.default_limit)
        {
            if self.dispatcher.quota_drop.tick() {
                tracing::warn!(
                    error = ?e,
                    total = self.dispatcher.quota_drop.count(),
                    "[app:quotas] reserve failed for default tenant (logging every 64th)"
                );
            }
        }

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

        // Bind / unbind send-policy for control frames regardless of
        // FrameAction — these carry the per-stream delivery contract
        // that constrains the server's egress on that stream. Doing it
        // here (rather than in stream.rs) keeps the policy adjacent to
        // the outbox that it eventually filters, and gives us
        // `quic_stream_id` via `ctx` without rippling signature changes
        // through session → stream.
        match frame.msg_type {
            MessageType::StreamOpen => match StreamOpenMeta::decode(frame.payload) {
                Ok(meta) => self.dispatcher.bind_send_policy(
                    ctx.conn_idx,
                    ctx.quic_stream_id,
                    frame.stream_id,
                    meta.policy,
                ),
                Err(e) => {
                    if self.dispatcher.malformed_drop.tick() {
                        tracing::warn!(
                            conn_idx = ctx.conn_idx,
                            stream_id = frame.stream_id.get(),
                            error = ?e,
                            total = self.dispatcher.malformed_drop.count(),
                            "[send-policy] malformed StreamOpen payload — \
                             stream will lazy-default to ReliableOrdered (logging every 256th)"
                        );
                    }
                }
            },
            MessageType::StreamClose | MessageType::StreamReset => {
                self.dispatcher
                    .remove_send_policy(ctx.conn_idx, frame.stream_id);
            }
            _ => {}
        }

        if action == FrameAction::DispatchToApp {
            // Platform: per-frame counter (observability).
            let _ = self.platform.observability.record(self.metric_frames_rx, 1);

            self.dispatcher.on_data_frame(ctx, frame);

            // Platform: enforce per-tenant bandwidth quota (tracks volume).
            let _ = self
                .platform
                .quotas
                .enforce_bandwidth(self.default_tenant, frame.payload.len() as u64);
        }
    }

    /// Set the pending-sends backpressure signal from the Stack. Called
    /// before `on_batch_tick` so cross-worker inbox draining can be gated
    /// when subscriber streams are flow-controlled. Also publishes this
    /// worker's pressure state to the shared flag so other workers can
    /// backpressure their publishers before forwarding.
    pub fn set_pending_pressure(&mut self, high: bool) {
        self.dispatcher.pending_sends_high = high;
        if let Some(flag) = self
            .dispatcher
            .worker_pressure_flags
            .get(self.dispatcher.worker_self_idx)
        {
            flag.store(high, Ordering::Relaxed);
        }
    }

    /// Returns true if any *other* worker has reported pending-sends
    /// pressure. Checked by the Stack before accepting a Publish so the
    /// publisher gets a backpressure ack instead of silently dropping at
    /// the cross-worker `try_send` boundary.
    pub fn any_remote_worker_pressured(&self) -> bool {
        let self_idx = self.dispatcher.worker_self_idx;
        self.dispatcher
            .worker_pressure_flags
            .iter()
            .enumerate()
            .any(|(i, f)| i != self_idx && f.load(Ordering::Relaxed))
    }

    /// Drives the stream scheduler once per recv batch.
    /// Propagates through session → stream, ticks the consumer-group
    /// heartbeat timeout, and drains the per-worker cluster inbox (P6).
    pub fn on_batch_tick(&mut self) {
        self.session.on_batch_tick();
        self.dispatcher.tick_groups();
        self.dispatcher.drain_cluster_inbox();
        self.dispatcher.drain_worker_inbox();
        self.dispatcher.maybe_run_wal_retention();
        self.dispatcher.tick_cluster_health();
        // Snapshot replay-window gauges into the shared metrics store so
        // the Prometheus endpoint reflects current buffer occupancy.
        self.dispatcher.publish_replay_gauges();
    }

    /// Drains queued outgoing frames produced by the dispatcher.
    ///
    /// Called by [`VireonStack`](crate::stack::VireonStack) after each
    /// `on_frame` batch; the returned frames are encoded and sent.
    pub fn drain_outbox(&mut self) -> Vec<OutgoingFrame> {
        self.dispatcher.drain_outbox()
    }

    /// Routes an outgoing frame through the send policy if the stream
    /// is bound. Returns `None` when consumed (caller does nothing
    /// further), `Some(out)` when the caller should send directly. See
    /// [`VireonDispatcher::enqueue_egress`].
    pub fn enqueue_egress(&mut self, out: OutgoingFrame) -> Option<OutgoingFrame> {
        self.dispatcher.enqueue_egress(out)
    }

    /// Drains policy-cleared frames across all connections. The caller
    /// sends each via the normal encode+send path. See
    /// [`VireonDispatcher::drain_ready_egress`].
    pub fn drain_ready_egress(&mut self, out: &mut Vec<OutgoingFrame>) {
        self.dispatcher.drain_ready_egress(out);
    }

    /// Tears down the session and notifies the application dispatcher.
    pub fn on_close(&mut self, conn_idx: usize, session_id: SessionId, reason: Option<&str>) {
        // Platform: release the session slot for the default tenant.
        let _ = self.platform.quotas.release(self.default_tenant);

        let released = self.session.on_close(conn_idx);

        // Drop all send-policy state for this connection before the
        // dispatcher's own teardown — queued frames are discarded
        // since the receiver is gone.
        self.dispatcher.remove_connection_send_policy(conn_idx);

        self.dispatcher
            .on_connection_closed(conn_idx, session_id, reason);

        tracing::info!(
            conn_idx,
            session_id = ?session_id,
            open_streams = released,
            reason = reason.unwrap_or("none"),
            "session destroyed"
        );
    }

    /// Rejects a publish because the server's subscriber-side send
    /// backlog (`pending_sends`) has exceeded the backpressure
    /// high-watermark. Enqueues a `backpressure` error ack on the
    /// publisher's own stream and records the reject in per-connection
    /// metrics. Non-fatal — the publisher may retry or drop per its
    /// own policy.
    ///
    /// Called by the stack's frame-dispatch path before the publish
    /// reaches fan-out, so no subscribers are touched.
    pub fn reject_publish_backpressure(
        &mut self,
        conn_idx: usize,
        quic_stream_id: u64,
        logical_stream: StreamId,
        topic: &str,
    ) {
        self.dispatcher
            .conn_metrics
            .entry(conn_idx)
            .or_default()
            .record_reject();
        let ctx = FrameContext {
            // Session id is unused by enqueue_frame; a fresh id is fine.
            session_id: SessionId::new(),
            conn_idx,
            quic_stream_id,
        };
        self.dispatcher.send_ack(
            &ctx,
            logical_stream,
            MessageType::Publish,
            &[
                b"{\"error\":\"backpressure\",\"action\":\"publish\",\"topic\":\"",
                topic.as_bytes(),
                b"\"}",
            ],
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

    /// Replaces the dispatcher's ACL table. Intended for startup-time
    /// configuration when the deployment has known topic namespaces
    /// (e.g. `deny("system.*", None, ALL)`). Calling after the first
    /// frame is processed is also safe — already-evaluated rules are
    /// unaffected; future calls use the new table.
    #[allow(dead_code)]
    pub fn set_acl(&mut self, table: AclTable) {
        self.dispatcher.acl = table;
    }

    /// Attaches the shared server-wide metrics counters to the inner
    /// dispatcher so reliability-layer handlers can bump outcome counters.
    /// Called from `worker.rs` after `VireonStack::new` when the metrics
    /// `Arc` becomes available.
    pub fn set_metrics(&mut self, metrics: Arc<crate::metrics::ServerMetrics>) {
        self.dispatcher.set_metrics(metrics);
    }

    /// Replaces the dispatcher's replay buffer with a shared, cross-worker
    /// one. Called from `worker.rs` when `num_workers > 1` so all workers
    /// can find replay entries regardless of which worker handled the
    /// original subscriber connection.
    pub fn set_shared_replay(
        &mut self,
        shared: Arc<std::sync::Mutex<crate::replay::SharedReplayState>>,
    ) {
        self.dispatcher.replay = shared;
    }

    /// Installs a WAL backend. Should be called before the first
    /// `Publish` arrives so every accepted message is durably
    /// appended before fan-out. Calling after the dispatcher has
    /// already seen traffic is also safe — only subsequent publishes
    /// are persisted; in-flight ones are unaffected.
    #[allow(dead_code)]
    pub fn set_wal(&mut self, wal: wal_store::FileStore) {
        self.dispatcher.wal = Some(wal);
    }

    /// Delegates to [`VireonDispatcher::set_wal_retention`]. The
    /// retention sweep runs from `on_batch_tick` once per ~60 s.
    pub fn set_wal_retention(&mut self, policy: wal_store::RetentionPolicy) {
        self.dispatcher.set_wal_retention(policy);
    }

    /// Delegates to [`VireonDispatcher::set_consumer_offsets`]. Loads
    /// the persisted consumer-offsets map + records the path the
    /// periodic sweeper should write snapshots to.
    pub fn set_consumer_offsets(
        &mut self,
        offsets: wal_store::ConsumerOffsets,
        path: std::path::PathBuf,
    ) {
        self.dispatcher.set_consumer_offsets(offsets, path);
    }

    /// Delegates to [`VireonDispatcher::save_offsets`]. Called from
    /// the shutdown path to flush the latest acks before drain.
    pub fn save_offsets(&self) {
        self.dispatcher.save_offsets();
    }

    /// Delegates to [`VireonDispatcher::set_audit_log`].
    pub fn set_audit_log(&mut self, log: crate::audit::AuditLog) {
        self.dispatcher.set_audit_log(log);
    }

    /// Delegates to [`VireonDispatcher::set_max_topics`].
    pub fn set_max_topics(&mut self, max: Option<usize>) {
        self.dispatcher.set_max_topics(max);
    }

    /// Delegates to [`VireonDispatcher::set_max_subscribers_per_topic`].
    pub fn set_max_subscribers_per_topic(&mut self, max: Option<usize>) {
        self.dispatcher.set_max_subscribers_per_topic(max);
    }

    /// Installs a cluster coordinator + transport channels (P6).
    /// Should be called before the first frame is processed so every
    /// publish is routed via the coordinator from the start.
    #[allow(dead_code)]
    pub fn set_cluster(
        &mut self,
        cfg: &ClusterConfig,
        outgoing_tx: mpsc::Sender<OutgoingMsg>,
        incoming_rx: mpsc::Receiver<InterNodeMessage>,
    ) {
        self.dispatcher.set_cluster(cfg, outgoing_tx, incoming_rx);
    }

    /// Installs inter-worker pub/sub channels for cross-worker fan-out.
    /// Should be called before the first frame is processed so every
    /// publish is broadcast to other workers from the start.
    #[allow(dead_code)]
    pub fn set_worker_channels(
        &mut self,
        self_idx: usize,
        inbox: mpsc::Receiver<InterWorkerMsg>,
        outboxes: Vec<mpsc::SyncSender<InterWorkerMsg>>,
    ) {
        self.dispatcher
            .set_worker_channels(self_idx, inbox, outboxes);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::Bytes;

    /// Build an OutgoingFrame for testing — uses `Publish` as a typical
    /// data-plane frame the fan-out path would enqueue.
    fn outgoing(
        conn_idx: usize,
        quic_stream_id: u64,
        stream: u64,
        seq: u64,
        payload: &[u8],
    ) -> OutgoingFrame {
        OutgoingFrame {
            conn_idx,
            quic_stream_id,
            header: FrameHeader::new(StreamId::new(stream), MessageType::Publish)
                .with_seq(Seq::new(seq))
                .with_flags(FrameFlags::NONE),
            payload: Bytes::copy_from_slice(payload),
        }
    }

    // ── bind_send_policy lifecycle ─────────────────────────────────

    #[test]
    fn bind_send_policy_records_quic_stream_id_for_egress_rebuild() {
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::LatestOnly);
        // After bind, the egress map should know the quic_stream_id to
        // use when rebuilding OutgoingFrame from a dequeued FramePayload.
        assert_eq!(d.egress_quic_streams.get(&(0, StreamId::new(7))), Some(&42),);
        // The send_policy table should also have the stream.
        assert!(d.send_policy.contains(0, StreamId::new(7)));
    }

    #[test]
    fn remove_send_policy_drops_stream_state_and_quic_map() {
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::LatestOnly);
        d.remove_send_policy(0, StreamId::new(7));
        assert!(!d.send_policy.contains(0, StreamId::new(7)));
        assert_eq!(d.egress_quic_streams.get(&(0, StreamId::new(7))), None);
    }

    #[test]
    fn remove_connection_send_policy_clears_all_streams_for_conn() {
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 100, StreamId::new(1), DeliveryPolicy::ReliableOrdered);
        d.bind_send_policy(0, 101, StreamId::new(2), DeliveryPolicy::LatestOnly);
        d.bind_send_policy(1, 200, StreamId::new(1), DeliveryPolicy::ReliableOrdered);
        d.remove_connection_send_policy(0);
        // Conn 0 streams gone.
        assert!(!d.send_policy.contains(0, StreamId::new(1)));
        assert!(!d.send_policy.contains(0, StreamId::new(2)));
        assert!(!d.egress_quic_streams.contains_key(&(0, StreamId::new(1))));
        assert!(!d.egress_quic_streams.contains_key(&(0, StreamId::new(2))));
        // Conn 1 unaffected.
        assert!(d.send_policy.contains(1, StreamId::new(1)));
    }

    // ── enqueue_egress routing ─────────────────────────────────────

    #[test]
    fn enqueue_egress_returns_some_for_unbound_stream() {
        // Stream was never bound via StreamOpen → caller sends directly.
        let mut d = VireonDispatcher::new();
        let out = outgoing(0, 42, 7, 0, b"hi");
        let result = d.enqueue_egress(out);
        assert!(result.is_some(), "unbound stream should bypass policy");
    }

    #[test]
    fn enqueue_egress_consumes_for_bound_stream() {
        // Stream is bound → frame goes into the policy table.
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::ReliableOrdered);
        let out = outgoing(0, 42, 7, 0, b"hi");
        let result = d.enqueue_egress(out);
        assert!(result.is_none(), "bound stream should be consumed");
    }

    #[test]
    fn enqueue_egress_drop_decision_consumes_silently() {
        // RealtimeDropOld with a stale seq → Drop. The frame is consumed
        // (None returned) but never emerges from drain_ready_egress.
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::RealtimeDropOld);
        // First enqueue seeds newest_seq = 9.
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 9, b"newer"));
        // Stale seq=5 → Drop.
        let result = d.enqueue_egress(outgoing(0, 42, 7, 5, b"older"));
        assert!(result.is_none(), "drop decision should still consume");
        // Only the newer frame should emerge from drain.
        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].header.seq, Seq::new(9));
    }

    // ── drain_ready_egress rebuild ─────────────────────────────────

    #[test]
    fn drain_ready_egress_rebuilds_outgoing_with_bound_quic_stream_id() {
        // The whole point of egress_quic_streams: when the policy
        // releases a FramePayload, the rebuild must recover the
        // quic_stream_id declared at bind time (not the publisher's).
        let mut d = VireonDispatcher::new();
        // Bind on quic_stream_id=42 (subscriber's stream).
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::ReliableOrdered);
        // Enqueue arrives with quic_stream_id=42 from the fan-out path.
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 0, b"x"));
        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].quic_stream_id, 42);
        assert_eq!(ready[0].header.stream_id, StreamId::new(7));
        assert_eq!(ready[0].payload.as_ref(), b"x");
    }

    #[test]
    fn drain_ready_egress_latest_only_keeps_newest_only() {
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::LatestOnly);
        // Three rapid enqueues before drain — the middle one displaces
        // the oldest, the newest wins.
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 0, b"old"));
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 1, b"mid"));
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 2, b"new"));
        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].payload.as_ref(), b"new");
        // Second drain should be empty — slot was cleared on take.
        let mut more = Vec::new();
        d.drain_ready_egress(&mut more);
        assert!(more.is_empty());
    }

    #[test]
    fn drain_ready_egress_fifo_preserves_order() {
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::ReliableOrdered);
        for i in 0..5u8 {
            let _ = d.enqueue_egress(outgoing(0, 42, 7, u64::from(i), &[i]));
        }
        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);
        assert_eq!(ready.len(), 5);
        for (i, frame) in ready.iter().enumerate() {
            assert_eq!(frame.payload.as_ref(), &[i as u8]);
        }
    }

    #[test]
    fn drain_ready_egress_skips_streams_whose_receiver_vanished() {
        // Edge case: bind → enqueue → remove_stream (receiver gone) →
        // drain. The frame is still in the table but egress_quic_streams
        // no longer has the quic_stream_id, so drain must skip it
        // instead of emitting with quic_stream_id=0.
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::ReliableOrdered);
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 0, b"x"));
        // Receiver vanished between enqueue and drain.
        d.remove_send_policy(0, StreamId::new(7));
        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);
        assert!(ready.is_empty(), "removed stream should yield no frames");
    }

    // ── bind upgrade flushes queued frames ─────────────────────────

    #[test]
    fn bind_send_policy_upgrade_flushes_prior_state_to_outbox() {
        // When a stream that's already bound gets re-bound with a
        // different policy (e.g. protocol re-handshake), the prior
        // state's queued frames are flushed to the outbox so they're
        // not silently lost. Mirrors `SendPolicyTable::bind` semantics.
        let mut d = VireonDispatcher::new();
        // First bind — ReliableOrdered, queue two frames.
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::ReliableOrdered);
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 0, b"a"));
        let _ = d.enqueue_egress(outgoing(0, 42, 7, 1, b"b"));
        // Re-bind to LatestOnly — flush should displace the FIFO frames.
        d.bind_send_policy(0, 42, StreamId::new(7), DeliveryPolicy::LatestOnly);
        let outbox = d.drain_outbox();
        assert_eq!(outbox.len(), 2);
        assert_eq!(outbox[0].payload.as_ref(), b"a");
        assert_eq!(outbox[1].payload.as_ref(), b"b");
        // The new state is empty — future enqueues start fresh.
        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);
        assert!(ready.is_empty());
    }

    #[test]
    fn enqueue_before_bind_routes_directly_and_does_not_create_state() {
        // Contract: unbound stream → frame bypasses policy. The frame
        // is returned to the caller (stack sends it via encode_and_send)
        // and send_policy.entries stays empty.
        let mut d = VireonDispatcher::new();
        let out = outgoing(0, 42, 7, 0, b"hi");
        let returned = d.enqueue_egress(out).expect("unbound returns Some");
        assert_eq!(returned.payload.as_ref(), b"hi");
        // No send_policy state was created — a later bind starts fresh.
        assert!(!d.send_policy.contains(0, StreamId::new(7)));
    }

    // ── end-to-end: policy-differentiated fan-out ──────────────────
    //
    // Mirrors the production fan-out path: one logical stream is bound
    // to three different subscriber streams, each with a different
    // policy. The dispatcher's `enqueue_egress` routes each subscriber's
    // copy through its own policy; `drain_ready_egress` then produces
    // per-policy-differentiated outputs from a single publish batch.

    #[test]
    fn fanout_three_subscribers_different_policies_yields_per_policy_counts() {
        let mut d = VireonDispatcher::new();

        // Three subscribers on the same topic, each on their own QUIC
        // stream, each having declared a different send policy.
        // (conn_idx, quic_stream_id, logical_stream_id, policy)
        let subs: &[(usize, u64, u64, DeliveryPolicy)] = &[
            (1, 100, 10, DeliveryPolicy::ReliableOrdered),
            (2, 200, 20, DeliveryPolicy::RealtimeDropOld),
            (3, 300, 30, DeliveryPolicy::LatestOnly),
        ];
        for &(conn_idx, quic_stream_id, logical_stream, policy) in subs {
            d.bind_send_policy(
                conn_idx,
                quic_stream_id,
                StreamId::new(logical_stream),
                policy,
            );
        }

        // Publisher sends 5 messages on the topic. Each is fanned out
        // to all three subscribers (one OutgoingFrame per subscriber).
        // We simulate the fan-out loop directly — `handle_publish` does
        // the same `enqueue_frame` → OutgoingFrame, then routes through
        // `enqueue_egress` at drain time.
        for seq in 0..5u64 {
            for &(conn_idx, quic_stream_id, logical_stream, _policy) in subs {
                let out = outgoing(conn_idx, quic_stream_id, logical_stream, seq, &[seq as u8]);
                // All subscribers are bound → all consumed (None).
                assert!(d.enqueue_egress(out).is_none());
            }
        }

        // Drain the policy-cleared frames.
        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);

        // Bucket per subscriber by quic_stream_id.
        let mut reliable: Vec<&OutgoingFrame> = Vec::new();
        let mut realtime: Vec<&OutgoingFrame> = Vec::new();
        let mut latest: Vec<&OutgoingFrame> = Vec::new();
        for f in &ready {
            match f.quic_stream_id {
                100 => reliable.push(f),
                200 => realtime.push(f),
                300 => latest.push(f),
                other => panic!("unexpected quic_stream_id {other}"),
            }
        }

        // ReliableOrdered → all 5 frames in order.
        assert_eq!(reliable.len(), 5, "ReliableOrdered must deliver all");
        assert_eq!(reliable[0].header.seq, Seq::new(0));
        assert_eq!(reliable[4].header.seq, Seq::new(4));

        // RealtimeDropOld → all 5 (each arrival was strictly newer than
        // the previous, so none were dropped; FIFO order preserved).
        assert_eq!(
            realtime.len(),
            5,
            "RealtimeDropOld keeps all in-order frames"
        );

        // LatestOnly → just 1 (the newest, seq=4). Older pending frames
        // were displaced as each newer one arrived.
        assert_eq!(latest.len(), 1, "LatestOnly must collapse to newest");
        assert_eq!(latest[0].header.seq, Seq::new(4));
        assert_eq!(latest[0].payload.as_ref(), &[4]);
    }

    #[test]
    fn fanout_under_burst_publish_late_subscriber_only_sees_latest() {
        // Stronger version of the LatestOnly case: a subscriber whose
        // drain happens *after* a burst of N publishes should only see
        // the single newest frame, never the intermediate ones.
        let mut d = VireonDispatcher::new();
        d.bind_send_policy(9, 999, StreamId::new(99), DeliveryPolicy::LatestOnly);

        // Burst of 10 publishes before any drain.
        for seq in 0..10u64 {
            let out = outgoing(9, 999, 99, seq, &[seq as u8]);
            assert!(d.enqueue_egress(out).is_none());
        }

        let mut ready = Vec::new();
        d.drain_ready_egress(&mut ready);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].header.seq, Seq::new(9));

        // Subsequent drain is empty — slot was cleared.
        let mut more = Vec::new();
        d.drain_ready_egress(&mut more);
        assert!(more.is_empty());
    }

    // ── ACL: permissive default + override via with_acl ────────────

    /// Test helper: frame context with default session_id.
    fn ctx(conn_idx: usize, quic_stream_id: u64) -> FrameContext {
        FrameContext {
            session_id: SessionId::new(),
            conn_idx,
            quic_stream_id,
        }
    }

    #[test]
    fn default_acl_allows_all_actions_on_two_segment_topics() {
        // The built-in default rule is `allow("*.*", None, ALL)`.
        let d = VireonDispatcher::new();
        for action in Action::ALL {
            assert!(
                d.check_acl(0, *action, "any.topic").is_allow(),
                "default ACL should allow {:?} on any.topic",
                action
            );
            assert!(
                d.check_acl(0, *action, "sensor.temp").is_allow(),
                "default ACL should allow {:?} on sensor.temp",
                action
            );
        }
    }

    #[test]
    fn default_acl_denies_patterns_that_do_not_match_wildcard() {
        // The default rule's pattern is `*.*` (exactly two segments).
        // Single-segment topics and 3+-segment topics don't match.
        let d = VireonDispatcher::new();
        for action in Action::ALL {
            assert!(
                !d.check_acl(0, *action, "single").is_allow(),
                "default ACL should NOT allow {:?} on single-segment topic",
                action
            );
            assert!(
                !d.check_acl(0, *action, "a.b.c").is_allow(),
                "default ACL should NOT allow {:?} on three-segment topic",
                action
            );
        }
    }

    #[test]
    fn with_acl_replaces_default_table() {
        // Operator swaps in a stricter table at construction time.
        let strict = AclTableBuilder::new()
            .allow("sensor.*", None, Action::Subscribe)
            .build()
            .unwrap_or_default();
        let d = VireonDispatcher::new().with_acl(strict);
        // sensor.* subscribe allowed.
        assert!(d.check_acl(0, Action::Subscribe, "sensor.temp").is_allow());
        // sensor.* publish denied (not in allow set).
        assert!(matches!(
            d.check_acl(0, Action::Publish, "sensor.temp"),
            AclDecision::Deny { .. }
        ));
        // Other topics denied (no rule matches).
        assert!(matches!(
            d.check_acl(0, Action::Subscribe, "alerts.fire"),
            AclDecision::Deny { .. }
        ));
    }

    // ── ACL gates the four action handlers ─────────────────────────

    /// Drains dispatcher outbox into owned bytes for assertion.
    fn drain_outbox_bytes(d: &mut VireonDispatcher) -> Vec<(MessageType, Vec<u8>)> {
        d.drain_outbox()
            .into_iter()
            .map(|o| (o.header.msg_type, o.payload.to_vec()))
            .collect()
    }

    #[test]
    fn acl_deny_subscribe_returns_error_ack_and_skips_registry() {
        // Subscribe to "system.*" is denied.
        let mut d = VireonDispatcher::new().with_acl(
            AclTableBuilder::new()
                .deny("system.*", None, Action::Subscribe)
                .allow("*.*", None, ActionSet::ALL)
                .build()
                .unwrap_or_default(),
        );
        d.handle_subscribe(&ctx(0, 100), StreamId::new(1), "system.logs");

        // Registry should NOT have recorded the subscription.
        assert_eq!(d.pubsub_registry.len(), 0);
        // Outbox should contain exactly one error ack on the Subscribe
        // stream — no Subscribe success ack.
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, MessageType::Subscribe);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(
            body.contains("\"error\":\"denied\"") && body.contains("system.logs"),
            "expected deny ack mentioning system.logs, got: {body}"
        );
    }

    #[test]
    fn acl_allow_subscribe_still_registers_and_acks() {
        let mut d = VireonDispatcher::new();
        d.handle_subscribe(&ctx(0, 100), StreamId::new(1), "sensor.temp");
        // Registry recorded the subscription.
        assert_eq!(d.pubsub_registry.len(), 1);
        // Outbox has a success ack.
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(out.len(), 1);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(body.contains("\"subscribed\":\"sensor.temp\""));
    }

    #[test]
    fn acl_deny_publish_skips_fanout_and_returns_error_ack() {
        // Set up: conn 0 is subscribed to "alerts.*" so a publish to
        // "alerts.fire" would normally fan out to it. Then deny publish
        // on alerts.* and verify fan-out is skipped.
        let mut d = VireonDispatcher::new().with_acl(
            AclTableBuilder::new()
                .deny("alerts.*", None, Action::Publish)
                .allow("*.*", None, ActionSet::ALL)
                .build()
                .unwrap_or_default(),
        );
        // Subscribe first (allowed by the broad allow).
        d.handle_subscribe(&ctx(0, 100), StreamId::new(1), "alerts.*");
        let _ = d.drain_outbox(); // discard the subscribe ack

        // Publish to alerts.fire from a different conn (so the
        // self-skip filter doesn't apply).
        let topic = pubsub_engine::Topic::new("alerts.fire").unwrap();
        d.handle_publish(&ctx(1, 200), StreamId::new(2), topic.as_str(), b"payload", false);

        // Outbox: only the error ack, NO fan-out to subscriber.
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(out.len(), 1, "expected only error ack, got {out:?}");
        assert_eq!(out[0].0, MessageType::Publish);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(
            body.contains("\"error\":\"denied\"") && body.contains("alerts.fire"),
            "expected deny ack, got: {body}"
        );
    }

    #[test]
    fn acl_allow_publish_fans_out_to_matching_subscribers() {
        // Mirror image of the deny test — confirms the happy path
        // still works end-to-end through the ACL check.
        let mut d = VireonDispatcher::new();
        d.handle_subscribe(&ctx(0, 100), StreamId::new(1), "alerts.*");
        let _ = d.drain_outbox();

        let topic = pubsub_engine::Topic::new("alerts.fire").unwrap();
        d.handle_publish(&ctx(1, 200), StreamId::new(2), topic.as_str(), b"hi", false);

        // Outbox: one Publish fan-out frame to conn 0 (the subscriber).
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, MessageType::Publish);
    }

    #[test]
    fn acl_deny_topic_create_skips_catalog_mutation() {
        let mut d = VireonDispatcher::new().with_acl(
            AclTableBuilder::new()
                .deny("system.*", None, Action::TopicCreate)
                .allow("*.*", None, ActionSet::ALL)
                .build()
                .unwrap_or_default(),
        );
        d.handle_topic_create(&ctx(0, 100), StreamId::new(1), "system.logs", 3, 60_000);

        // Catalog should be empty.
        assert_eq!(d.topic_catalog.len(), 0);
        // Outbox: error ack.
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(out.len(), 1);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(body.contains("\"error\":\"denied\"") && body.contains("topic_create"));
    }

    #[test]
    fn acl_deny_topic_delete_skips_catalog_mutation() {
        // Pre-seed the catalog with a topic, then deny its deletion.
        let mut d = VireonDispatcher::new();
        d.handle_topic_create(&ctx(0, 100), StreamId::new(1), "admin.config", 1, 0);
        let _ = d.drain_outbox();
        assert_eq!(d.topic_catalog.len(), 1);

        // Swap in a stricter table.
        let stricter = AclTableBuilder::new()
            .deny("admin.*", None, Action::TopicDelete)
            .allow("*.*", None, ActionSet::ALL)
            .build()
            .unwrap_or_default();
        d.acl = stricter;

        let topic = pubsub_engine::Topic::new("admin.config").unwrap();
        d.handle_topic_delete(&ctx(0, 100), StreamId::new(1), topic.as_str());

        // Catalog still has the topic.
        assert_eq!(d.topic_catalog.len(), 1);
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(out.len(), 1);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(body.contains("\"error\":\"denied\"") && body.contains("topic_delete"));
    }

    #[test]
    fn acl_topic_create_allowed_on_default_table() {
        // Sanity: TopicCreate works on the permissive default.
        let mut d = VireonDispatcher::new();
        d.handle_topic_create(&ctx(0, 100), StreamId::new(1), "my.topic", 4, 30_000);
        assert_eq!(d.topic_catalog.len(), 1);
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(out.len(), 1);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(body.contains("\"topic_created\":\"my.topic\""));
    }

    // ── Per-connection PrincipalId via Handshake ────────────────────

    use frame::header::Version;
    use frame::payload::{Handshake, ParsedPayload, encode_payload};

    /// Builds a zero-copy `FrameRef` borrowing from `payload_buf`.
    /// The buffer must contain exactly the handshake wire bytes.
    fn handshake_fref<'a>(payload_buf: &'a [u8]) -> FrameRef<'a> {
        FrameRef {
            stream_id: StreamId::new(0),
            seq: Seq::ZERO,
            msg_type: MessageType::Handshake,
            flags: FrameFlags::NONE,
            payload: payload_buf,
        }
    }

    /// Encodes a `Handshake` payload into a fresh `Vec<u8>`.
    fn encode_handshake(hs: Handshake) -> Vec<u8> {
        encode_payload(&ParsedPayload::Handshake(hs))
    }

    #[test]
    fn handshake_with_registered_credential_binds_principal_to_connection() {
        let mut d = VireonDispatcher::new();
        d.register_principal(
            PrincipalId::new(42),
            Credential::new(CredentialType::Token, 0xABCD, 0),
        );

        let payload = encode_handshake(Handshake {
            version: Version::new(1),
            capabilities: 0,
            principal_id: 42,
            credential_token: 0xABCD,
        });
        let fref = handshake_fref(&payload);
        d.handle_handshake(&ctx(7, 100), &fref);

        // principal_for(7) should now return 42 (not the default).
        assert_eq!(d.principal_for(7), PrincipalId::new(42));
        assert_ne!(d.principal_for(7), d.principal_for(0));
    }

    #[test]
    fn handshake_failure_keeps_default_principal_for_connection() {
        let mut d = VireonDispatcher::new();
        // No registered principal — auth will return UnknownPrincipal.

        let payload = encode_handshake(Handshake {
            version: Version::new(1),
            capabilities: 0,
            principal_id: 99,
            credential_token: 0xDEAD,
        });
        let fref = handshake_fref(&payload);
        d.handle_handshake(&ctx(3, 100), &fref);

        // Connection 3 should fall back to default_principal.
        assert_eq!(d.principal_for(3), d.principal_for(0));
        assert_eq!(d.principal_for(3), PrincipalId::new(1));
    }

    #[test]
    fn connection_close_removes_principal_binding() {
        let mut d = VireonDispatcher::new();
        d.register_principal(
            PrincipalId::new(7),
            Credential::new(CredentialType::Token, 0x1234, 0),
        );
        let payload = encode_handshake(Handshake {
            version: Version::new(1),
            capabilities: 0,
            principal_id: 7,
            credential_token: 0x1234,
        });
        let fref = handshake_fref(&payload);
        d.handle_handshake(&ctx(5, 100), &fref);
        assert_eq!(d.principal_for(5), PrincipalId::new(7));

        // Close the connection — principal binding should be gone.
        d.on_connection_closed(5, SessionId::new(), Some("test"));
        assert_eq!(d.principal_for(5), d.principal_for(0));
    }

    #[test]
    fn acl_distinguishes_principals_after_handshake() {
        // Strict ACL: principal 42 owns private.*, everyone else denied.
        let strict = AclTableBuilder::new()
            .allow("private.*", Some(PrincipalId::new(42)), ActionSet::ALL)
            .build()
            .unwrap_or_default();
        let mut d = VireonDispatcher::new().with_acl(strict);
        d.register_principal(
            PrincipalId::new(42),
            Credential::new(CredentialType::Token, 0xC0FFEE, 0),
        );

        // Connection 0 (no handshake) → default principal 1 → denied.
        assert!(matches!(
            d.check_acl(0, Action::Publish, "private.secret"),
            AclDecision::Deny { .. }
        ));

        // Connection 1 completes handshake as principal 42 → allowed.
        let payload = encode_handshake(Handshake {
            version: Version::new(1),
            capabilities: 0,
            principal_id: 42,
            credential_token: 0xC0FFEE,
        });
        let fref = handshake_fref(&payload);
        d.handle_handshake(&ctx(1, 100), &fref);
        assert!(d.check_acl(1, Action::Publish, "private.secret").is_allow());
    }

    // ── Phase 4: WAL persistence + Fetch(offset) replay ──────────────

    use frame::payload::{FetchPayload, FetchReplyPayload};
    use tempfile::TempDir;

    /// Encodes a FetchPayload + wraps it in a FrameRef the way the
    /// dispatcher's `handle_fetch` expects to read it.
    fn encode_fetch(fetch: FetchPayload<'_>) -> Vec<u8> {
        encode_payload(&ParsedPayload::Fetch(fetch))
    }

    fn fetch_fref(payload_buf: &[u8]) -> FrameRef<'_> {
        FrameRef {
            stream_id: StreamId::new(0),
            seq: Seq::ZERO,
            msg_type: MessageType::Fetch,
            flags: FrameFlags::NONE,
            payload: payload_buf,
        }
    }

    /// Opens a fresh in-memory-DFS FileStore rooted at a tempdir, and
    /// attaches it to a fresh dispatcher.
    fn dispatcher_with_wal() -> (VireonDispatcher, TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = wal_store::FileStore::open(dir.path()).expect("FileStore::open");
        let d = VireonDispatcher::new().with_wal(store);
        (d, dir)
    }

    #[test]
    fn publish_persists_to_wal() {
        let (mut d, _keepalive) = dispatcher_with_wal();

        let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
        for i in 0..5u8 {
            d.handle_publish(&ctx(1, 200), StreamId::new(2), topic.as_str(), &[i], false);
        }
        // Drain the fan-out outbox so later asserts aren't muddled.
        let _ = d.drain_outbox();

        // WAL head should be 4 (last assigned offset, 0-indexed).
        let wal = d.wal.as_ref().expect("wal attached");
        assert_eq!(wal.head("sensor.temp"), wal_store::Offset::new(4));
        assert_eq!(wal.len("sensor.temp"), 5);
    }

    #[test]
    fn fetch_replays_by_offset() {
        let (mut d, _keepalive) = dispatcher_with_wal();

        // Publish 5 entries on sensor.temp.
        let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
        for i in 0..5u8 {
            d.handle_publish(&ctx(1, 200), StreamId::new(2), topic.as_str(), &[i], false);
        }
        let _ = d.drain_outbox();

        // Subscriber on conn 2 sends Fetch(from=0, max=10).
        let payload = encode_fetch(FetchPayload {
            topic: "sensor.temp",
            from_offset: 0,
            max_count: 10,
        });
        let fref = fetch_fref(&payload);
        d.handle_fetch(&ctx(2, 300), &fref);

        // Outbox: 5 FetchReply frames in offset order.
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(
            out.len(),
            5,
            "expected 5 FetchReply frames, got {}",
            out.len()
        );
        for (i, (mt, body)) in out.iter().enumerate() {
            assert_eq!(*mt, MessageType::FetchReply);
            // Decode the body back to a FetchReplyPayload and check offset+payload.
            let mut r = frame::byte_reader::ByteReader::new(body);
            let decoded = FetchReplyPayload::decode(&mut r).expect("FetchReply decode");
            assert_eq!(decoded.offset, i as u64, "offset {i}");
            assert_eq!(decoded.payload, &[i as u8], "payload byte {i}");
            assert_eq!(decoded.topic, "sensor.temp");
        }
    }

    #[test]
    fn wal_disabled_when_not_configured() {
        // Default dispatcher has no WAL attached — publish should
        // proceed (live fan-out) and fetch should return a wal_disabled
        // ack rather than replaying anything.
        let mut d = VireonDispatcher::new();

        let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
        d.handle_publish(&ctx(1, 200), StreamId::new(2), topic.as_str(), b"payload", false);
        let pub_out = drain_outbox_bytes(&mut d);
        // No subscribers → no fan-out; zero frames queued by publish.
        assert!(
            pub_out.is_empty(),
            "publish shouldn't queue frames without subscribers"
        );

        // No WAL → fetch enqueues exactly one wal_disabled ack.
        let payload = encode_fetch(FetchPayload {
            topic: "sensor.temp",
            from_offset: 0,
            max_count: 10,
        });
        let fref = fetch_fref(&payload);
        d.handle_fetch(&ctx(2, 300), &fref);
        let fetch_out = drain_outbox_bytes(&mut d);
        assert_eq!(fetch_out.len(), 1);
        assert_eq!(fetch_out[0].0, MessageType::FetchReply);
        let body = String::from_utf8_lossy(&fetch_out[0].1);
        assert!(
            body.contains("\"error\":\"wal_disabled\""),
            "expected wal_disabled ack, got: {body}"
        );
    }

    #[test]
    fn wal_retention_bytes_truncates_after_threshold() {
        let (mut d, _keepalive) = dispatcher_with_wal();
        // Cap at 200 bytes (≈ 5 entries survive at ~38 B/entry).
        d.set_wal_retention(wal_store::RetentionPolicy::by_bytes(200));

        let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
        for i in 0..20u8 {
            d.handle_publish(&ctx(1, 200), StreamId::new(2), topic.as_str(), &[i], false);
        }
        let _ = d.drain_outbox();

        // Pre-sweep: 20 entries retained.
        assert_eq!(d.wal.as_ref().unwrap().len("sensor.temp"), 20);

        // Drive the sweep directly (we don't wait 60 s in unit tests).
        // The dispatcher exposes a private helper; replicate the call.
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let policy = d.wal_retention;
        let wal = d.wal.as_mut().unwrap();
        let stats = policy
            .apply::<wal_store::FileStore>(wal, "sensor.temp", now_ns)
            .expect("retention apply");
        assert!(stats.removed > 0, "sweep should remove entries");
        assert!(
            wal.retained_bytes() <= 200,
            "retained bytes ({}) should be under cap (200)",
            wal.retained_bytes()
        );
    }

    #[test]
    fn wal_retention_keep_forever_is_noop() {
        let (mut d, _keepalive) = dispatcher_with_wal();
        // Default policy = keep forever.
        let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
        for i in 0..10u8 {
            d.handle_publish(&ctx(1, 200), StreamId::new(2), topic.as_str(), &[i], false);
        }
        let _ = d.drain_outbox();

        // maybe_run_wal_retention should be a no-op with keep_forever,
        // regardless of how much time has elapsed.
        d.wal_retention_last_run = None; // force "due"
        d.maybe_run_wal_retention();

        assert_eq!(d.wal.as_ref().unwrap().len("sensor.temp"), 10);
    }

    #[test]
    fn quota_max_topics_rejects_beyond_cap() {
        let mut d = VireonDispatcher::new();
        d.set_max_topics(Some(2));

        d.handle_topic_create(&ctx(0, 100), StreamId::new(1), "topic.a", 1, 0);
        d.handle_topic_create(&ctx(0, 100), StreamId::new(1), "topic.b", 1, 0);
        let _ = d.drain_outbox();
        assert_eq!(d.topic_catalog.len(), 2);
        assert_eq!(d.quota_topic_rejects, 0);

        // Third topic should be rejected.
        d.handle_topic_create(&ctx(0, 100), StreamId::new(1), "topic.c", 1, 0);
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(d.topic_catalog.len(), 2, "catalog must not grow past cap");
        assert_eq!(d.quota_topic_rejects, 1);
        assert_eq!(out.len(), 1);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(
            body.contains("quota_exceeded") && body.contains("max_topics"),
            "expected quota_exceeded ack, got: {body}"
        );

        // Re-creating an existing topic should NOT be rejected (idempotent).
        d.handle_topic_create(&ctx(0, 100), StreamId::new(1), "topic.a", 1, 0);
        let _ = d.drain_outbox();
        assert_eq!(d.topic_catalog.len(), 2);
        assert_eq!(
            d.quota_topic_rejects, 1,
            "idempotent re-create shouldn't bump rejects"
        );
    }

    #[test]
    fn quota_max_topics_disabled_when_none() {
        let mut d = VireonDispatcher::new();
        // Default: None = unlimited.
        for i in 0..100 {
            let topic = format!("topic.{i}");
            d.handle_topic_create(&ctx(0, 100), StreamId::new(1), topic.as_str(), 1, 0);
        }
        let _ = d.drain_outbox();
        assert_eq!(d.topic_catalog.len(), 100);
        assert_eq!(d.quota_topic_rejects, 0);
    }

    #[test]
    fn quota_max_subscribers_per_topic_rejects_beyond_cap() {
        let mut d = VireonDispatcher::new();
        d.set_max_subscribers_per_topic(Some(2));

        // Two subscribers on the same pattern from different conns.
        d.handle_subscribe(&ctx(1, 200), StreamId::new(0), "sensor.*");
        d.handle_subscribe(&ctx(2, 200), StreamId::new(0), "sensor.*");
        let _ = d.drain_outbox();
        assert_eq!(
            d.pattern_sub_counts.get("sensor.*").copied(),
            Some(2),
            "counter should track 2 subscribers"
        );

        // Third subscriber on the same pattern should be rejected.
        d.handle_subscribe(&ctx(3, 200), StreamId::new(0), "sensor.*");
        let out = drain_outbox_bytes(&mut d);
        assert_eq!(
            d.pattern_sub_counts.get("sensor.*").copied(),
            Some(2),
            "counter must not grow past cap"
        );
        assert_eq!(d.quota_subscriber_rejects, 1);
        let body = String::from_utf8_lossy(&out[0].1);
        assert!(
            body.contains("quota_exceeded") && body.contains("max_subscribers_per_topic"),
            "expected quota_exceeded ack, got: {body}"
        );

        // A different pattern should be unaffected.
        d.handle_subscribe(&ctx(4, 200), StreamId::new(0), "alert.*");
        let _ = d.drain_outbox();
        assert_eq!(d.pattern_sub_counts.get("alert.*").copied(), Some(1));
    }

    #[test]
    fn quota_subscribers_decrement_on_unsubscribe_and_close() {
        let mut d = VireonDispatcher::new();
        d.set_max_subscribers_per_topic(Some(2));

        d.handle_subscribe(&ctx(1, 200), StreamId::new(0), "sensor.*");
        d.handle_subscribe(&ctx(2, 200), StreamId::new(0), "sensor.*");
        let _ = d.drain_outbox();
        assert_eq!(d.pattern_sub_counts.get("sensor.*").copied(), Some(2));

        // Third is rejected.
        d.handle_subscribe(&ctx(3, 200), StreamId::new(0), "sensor.*");
        let _ = d.drain_outbox();
        assert_eq!(d.quota_subscriber_rejects, 1);

        // Conn 1 unsubscribes — counter drops to 1.
        d.handle_unsubscribe(&ctx(1, 200), StreamId::new(0), "sensor.*");
        let _ = d.drain_outbox();
        assert_eq!(d.pattern_sub_counts.get("sensor.*").copied(), Some(1));

        // Now the previously-rejected conn 3 should succeed.
        d.handle_subscribe(&ctx(3, 200), StreamId::new(0), "sensor.*");
        let _ = d.drain_outbox();
        assert_eq!(d.pattern_sub_counts.get("sensor.*").copied(), Some(2));
        assert_eq!(
            d.quota_subscriber_rejects, 1,
            "no new rejection after decrement"
        );

        // Connection-close also decrements.
        let removed = d.pubsub_registry.remove_connection_with_patterns(2);
        assert_eq!(removed.len(), 1);
        // Reconcile mirror.
        for pat in &removed {
            if let Some(c) = d.pattern_sub_counts.get_mut(pat) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    d.pattern_sub_counts.remove(pat);
                }
            }
        }
        assert_eq!(d.pattern_sub_counts.get("sensor.*").copied(), Some(1));
    }

    // ── DeliveryConfig::select ──────────────────────────────────────

    #[test]
    fn delivery_select_auto_uses_batchpush_below_threshold() {
        let cfg = DeliveryConfig::default(); // threshold=256, Auto
        assert_eq!(
            cfg.select(0, true),
            FanoutStrategy::BatchPush,
            "zero fanout → BatchPush"
        );
        assert_eq!(
            cfg.select(255, true),
            FanoutStrategy::BatchPush,
            "below threshold → BatchPush"
        );
    }

    #[test]
    fn delivery_select_auto_uses_logtail_at_threshold_with_wal() {
        let cfg = DeliveryConfig::default();
        assert_eq!(
            cfg.select(256, true),
            FanoutStrategy::LogTail,
            "at threshold with WAL → LogTail"
        );
        assert_eq!(
            cfg.select(10_000, true),
            FanoutStrategy::LogTail,
            "well above threshold with WAL → LogTail"
        );
    }

    #[test]
    fn delivery_select_auto_falls_back_to_batchpush_without_wal() {
        let cfg = DeliveryConfig::default();
        assert_eq!(
            cfg.select(10_000, false),
            FanoutStrategy::BatchPush,
            "no WAL → BatchPush even at high fan-out"
        );
    }

    #[test]
    fn delivery_select_force_batch_overrides_everything() {
        let cfg = DeliveryConfig {
            logtail_threshold: 1,
            force_strategy: crate::conf::DeliveryMode::ForceBatch,
        };
        assert_eq!(cfg.select(10_000, true), FanoutStrategy::BatchPush);
    }

    #[test]
    fn delivery_select_force_logtail_requires_wal() {
        let cfg = DeliveryConfig {
            logtail_threshold: 1,
            force_strategy: crate::conf::DeliveryMode::ForceLogTail,
        };
        assert_eq!(
            cfg.select(1, true),
            FanoutStrategy::LogTail,
            "force_logtail + WAL → LogTail"
        );
        assert_eq!(
            cfg.select(1, false),
            FanoutStrategy::BatchPush,
            "force_logtail without WAL → fallback BatchPush"
        );
    }

    // ── Cross-worker backpressure flags ────────────────────────────

    #[test]
    fn pressure_flag_set_and_cleared() {
        let mut d = VireonDispatcher::new();
        // Simulate multi-worker setup: need channels + pressure flags.
        let (_tx, rx) = mpsc::sync_channel::<InterWorkerMsg>(16);
        let outboxes: Vec<_> = (0..3)
            .map(|_| mpsc::sync_channel::<InterWorkerMsg>(16).0)
            .collect();
        d.set_worker_channels(0, rx, outboxes);
        // Flags should be initialized to false.
        assert_eq!(d.worker_pressure_flags.len(), 3);
        assert!(!d.pending_sends_high);

        // Simulate the Stack setting pressure via the application layer.
        // (We test the dispatcher field directly since ApplicationLayer
        // is just a thin wrapper.)
        d.pending_sends_high = true;
        if let Some(f) = d.worker_pressure_flags.get(d.worker_self_idx) {
            f.store(true, Ordering::Relaxed);
        }
        assert!(d.worker_pressure_flags[0].load(Ordering::Relaxed));
    }

    #[test]
    fn any_remote_worker_pressured_detects_other_workers() {
        let mut d = VireonDispatcher::new();
        let (_tx, rx) = mpsc::sync_channel::<InterWorkerMsg>(16);
        let outboxes: Vec<_> = (0..3)
            .map(|_| mpsc::sync_channel::<InterWorkerMsg>(16).0)
            .collect();
        d.set_worker_channels(0, rx, outboxes);

        // No worker has pressure → false.
        assert!(!d.pending_sends_high);

        // Worker 1 signals pressure.
        d.worker_pressure_flags[1].store(true, Ordering::Relaxed);

        // Check from worker 0's perspective: any_remote should detect it.
        let self_idx = d.worker_self_idx;
        let any_remote = d
            .worker_pressure_flags
            .iter()
            .enumerate()
            .any(|(i, f)| i != self_idx && f.load(Ordering::Relaxed));
        assert!(any_remote, "should detect remote worker 1 pressure");

        // Worker 0's own flag is still false.
        assert!(!d.worker_pressure_flags[self_idx].load(Ordering::Relaxed));
    }

    #[test]
    fn metrics_counters_bump_on_reliability_events() {
        use crate::metrics::ServerMetrics;
        use frame::payload::{Ack, ParsedPayload, encode_payload};
        use std::sync::atomic::Ordering;

        let metrics = Arc::new(ServerMetrics::new());
        let mut d = VireonDispatcher::new().with_metrics(metrics.clone());

        // Register a subscriber conn with a logical session so reliable
        // fan-out retains entries.
        const LOGICAL: u64 = 0xCAFE;
        const SUB_CONN: usize = 1;
        const STREAM: u64 = 42;
        d.register_logical_session_for_bench(SUB_CONN, LOGICAL);
        d.bind_send_policy(
            SUB_CONN,
            100,
            StreamId::new(STREAM),
            DeliveryPolicy::ReliableUnordered,
        );
        d.handle_subscribe(
            &FrameContext {
                session_id: SessionId::new(),
                conn_idx: SUB_CONN,
                quic_stream_id: 100,
            },
            StreamId::new(STREAM),
            "sensor.*",
        );
        let _ = d.drain_outbox();

        // Reliable publish → retained in replay buffer.
        let topic = pubsub_engine::Topic::new("sensor.temp").unwrap();
        let payload = [0xABu8; 16];
        d.handle_publish(
            &FrameContext {
                session_id: SessionId::new(),
                conn_idx: 0,
                quic_stream_id: 999,
            },
            StreamId::new(999),
            topic.as_str(),
            &payload,
            true,
        );
        let _ = d.drain_outbox();

        // Gauges should reflect 1 retained message after a batch tick.
        d.publish_replay_gauges();
        assert_eq!(
            metrics.replay_window_messages.load(Ordering::Relaxed),
            1,
            "replay_window_messages gauge should be 1 after one reliable publish"
        );

        // Send an Ack frame for the subscriber's stream at seq=1.
        let ack_payload = encode_payload(&ParsedPayload::Ack(Ack { ack_seq: 1 }));
        let ack_frame = FrameRef {
            stream_id: StreamId::new(STREAM),
            seq: Seq::new(1),
            msg_type: MessageType::Ack,
            flags: FrameFlags::NONE,
            payload: &ack_payload[..],
        };
        d.handle_ack(
            &FrameContext {
                session_id: SessionId::new(),
                conn_idx: SUB_CONN,
                quic_stream_id: 100,
            },
            &ack_frame,
        );
        assert_eq!(
            metrics.messages_acked_total.load(Ordering::Relaxed),
            1,
            "messages_acked_total should be 1 after one Ack"
        );
    }
}
