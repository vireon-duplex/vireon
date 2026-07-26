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

use rustc_hash::FxHashMap;
use std::net::SocketAddr;
use std::sync::mpsc;

use auth_framework::authenticator::AuthenticationPipeline;
use cluster_coordinator::{
    ClusterConfig, ClusterCoordinator, InterNodeMessage, PublishDecision, RemoteSubscriber,
};
use consumer_groups::{ConsumerId, GroupCoordinator, GroupId};
use frame::byte_writer::ByteWriter;
use frame::codec::{FrameHeader, FrameRef};
use frame::header::{FrameFlags, MessageType, Seq, StreamId};
use frame::payload::{
    FetchReplyPayload, ParsedPayload, SubscribePayload, TopicCreatePayload, TopicDeletePayload,
    UnsubscribePayload, parse_payload,
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
use crate::error::ServerError;
use bytes::Bytes;

// ── Inter-worker pub/sub forwarding ────────────────────────────────

/// A published message forwarded from one worker to another for
/// cross-worker fan-out. Each worker broadcasts publishes to all other
/// workers so their local subscribers receive the message.
///
/// Receiving workers drain their inbox in `drain_worker_inbox` and call
/// `cluster_deliver_local` (the same code path as inter-node delivery).
/// Forwarded publishes are NOT re-broadcast, preventing loops.
pub struct InterWorkerPublish {
    /// Topic string (same as the wire-level topic in the Publish frame).
    pub topic: String,
    /// Application payload bytes.
    pub payload: Bytes,
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

// ── VireonDispatcher ──────────────────────────────────────────────

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
    worker_outboxes: Vec<mpsc::SyncSender<InterWorkerPublish>>,
    /// Inbox of forwarded publishes from other workers. Drained in
    /// [`Self::drain_worker_inbox`]. `None` in single-worker mode.
    worker_inbox: Option<mpsc::Receiver<InterWorkerPublish>>,
    /// This worker's reactor_id — used to skip self when broadcasting.
    worker_self_idx: usize,
    /// Aggregate count of cross-worker inbox-full drops. Logged every
    /// `WORKER_DROP_LOG_INTERVAL`-th drop instead of per-message — a
    /// sustained burst otherwise produces hundreds of identical WARN
    /// lines per second and drowns the rest of the log.
    worker_drop_count: u64,
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
}

/// Log cross-worker drop summaries every Nth drop.
const WORKER_DROP_LOG_INTERVAL: u64 = 1024;

impl VireonDispatcher {
    /// Creates a new dispatcher with default-configured modules.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pubsub_registry: TopicRegistry::new(PubSubConfig::new()),
            topic_catalog: TopicCatalog::new(),
            outbox: Vec::new(),
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
            conn_group_keys: FxHashMap::default(),
            coordinator: None,
            node_id: session_registry::NodeId::NIL,
            cluster_outgoing_tx: None,
            cluster_incoming_rx: None,
            worker_outboxes: Vec::new(),
            worker_inbox: None,
            worker_self_idx: 0,
            worker_drop_count: 0,
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

    /// Enqueues a frame targeting an explicit connection + QUIC stream.
    /// Used for both same-connection responses and cross-connection delivery.
    fn enqueue_frame(
        &mut self,
        conn_idx: usize,
        quic_stream_id: u64,
        logical_stream: StreamId,
        msg_type: MessageType,
        payload: Bytes,
    ) {
        let seq = self
            .next_seq
            .entry((conn_idx, quic_stream_id))
            .or_insert(Seq::ZERO);
        let header = FrameHeader::new(logical_stream, msg_type)
            .with_seq(*seq)
            .with_flags(FrameFlags::NONE);
        *seq = seq.next();
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
        inbox: mpsc::Receiver<InterWorkerPublish>,
        outboxes: Vec<mpsc::SyncSender<InterWorkerPublish>>,
    ) {
        self.worker_self_idx = self_idx;
        self.worker_inbox = Some(inbox);
        self.worker_outboxes = outboxes;
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
                self.handle_publish(ctx, frame.stream_id, &topic, payload);
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
                self.handle_topic_delete(ctx, frame.stream_id, &topic);
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
            let ack = format!(
                "{{\"error\":\"denied\",\"action\":\"subscribe\",\"pattern\":\"{pattern_str}\"}}"
            );
            self.enqueue_response(ctx, stream, MessageType::Subscribe, Bytes::from(ack));
            return;
        }
        match TopicPattern::new(pattern_str) {
            Ok(_) => {
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

                let ack = format!("{{\"subscribed\":\"{pattern_str}\"}}");
                self.enqueue_response(ctx, stream, MessageType::Subscribe, Bytes::from(ack));
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
        tracing::info!(
            conn_idx = ctx.conn_idx,
            pattern = %pattern_str,
            removed,
            active = self.pubsub_registry.len(),
            "[pubsub] unsubscribe"
        );
        let ack = format!("{{\"unsubscribed\":\"{pattern_str}\",\"removed\":{removed}}}");
        self.enqueue_response(ctx, stream, MessageType::Unsubscribe, Bytes::from(ack));
    }

    /// Fans a published payload out to every subscriber whose pattern matches
    /// `topic`, skipping the origin connection (no echo).
    pub fn handle_publish(
        &mut self,
        ctx: &FrameContext,
        stream: StreamId,
        topic: &pubsub_engine::Topic,
        payload: &[u8],
    ) {
        // Phase 7: check Publish against the ACL before fan-out. Skip
        // the registry lookup entirely on Deny — the publish is
        // invisible to subscribers. The check uses the concrete topic
        // string (not the publisher's pattern, which isn't known).
        if !self
            .check_acl(ctx.conn_idx, Action::Publish, topic.as_str())
            .is_allow()
        {
            if self.acl_denied_drop.tick() {
                tracing::info!(
                    conn_idx = ctx.conn_idx,
                    topic = %topic,
                    total = self.acl_denied_drop.count(),
                    "[acl] publish denied (logging every 256th)"
                );
            }
            let ack =
                format!("{{\"error\":\"denied\",\"action\":\"publish\",\"topic\":\"{topic}\"}}");
            self.enqueue_response(ctx, stream, MessageType::Publish, Bytes::from(ack));
            return;
        }

        // P6: cluster routing. If a coordinator is wired, consult it
        // to decide whether this publish belongs to us (deliver
        // locally + fan out to remote subscribers) or another node
        // (forward the publish via UDP).
        if let Some(coord) = &self.coordinator {
            let decision = coord.route_publish(topic.as_str());
            match decision {
                PublishDecision::Forward { owner } => {
                    // Send the publish to the owning node via UDP.
                    // The owner will fan out to its local subscribers
                    // + any other nodes with remote interest.
                    if let Some(tx) = &self.cluster_outgoing_tx {
                        let msg = InterNodeMessage::publish(
                            self.node_id,
                            ctx.conn_idx as u32,
                            ctx.quic_stream_id,
                            topic.as_str().to_string(),
                            Bytes::copy_from_slice(payload),
                        );
                        let _ = tx.send(OutgoingMsg { msg, target: owner });
                    }
                    tracing::debug!(
                        conn_idx = ctx.conn_idx,
                        topic = %topic,
                        owner = %owner.raw(),
                        "[cluster] publish forwarded to owner"
                    );
                    // Do not deliver locally — the owner handles it.
                    return;
                }
                PublishDecision::Own { remote_targets } => {
                    // Fan out to remote subscribers via UDP. Each
                    // remote target is a node whose local subscribers
                    // want this topic.
                    if let Some(tx) = &self.cluster_outgoing_tx {
                        // Origin = this node (we own the topic and are
                        // delivering to local subscribers). The remote
                        // nodes will use the origin back-route for
                        // any acks.
                        let msg = InterNodeMessage::fanout(
                            self.node_id,
                            ctx.conn_idx as u32,
                            ctx.quic_stream_id,
                            topic.as_str().to_string(),
                            Bytes::copy_from_slice(payload),
                        );
                        for target in remote_targets {
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

        let targets: Vec<Subscriber> = self
            .pubsub_registry
            .lookup(topic)
            .into_iter()
            .filter(|s| s.conn_idx != ctx.conn_idx)
            .collect();

        tracing::debug!(
            conn_idx = ctx.conn_idx,
            topic = %topic,
            targets = targets.len(),
            subs = self.pubsub_registry.len(),
            "[pubsub] publish"
        );

        // Re-encode as a Publish frame so downstream subscribers receive the
        // same wire shape the publisher used (topic_len + topic + payload).
        let topic_str = topic.as_str();
        let mut framed = Vec::with_capacity(2 + topic_str.len() + payload.len());
        framed.extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
        framed.extend_from_slice(topic_str.as_bytes());
        framed.extend_from_slice(payload);
        let bytes = Bytes::from(framed);

        // Phase 4: durable append before fan-out. A failure here is
        // operator-visible (warn log) but does not abort the publish —
        // the message still reaches live subscribers. Subscribers that
        // later `Fetch(offset)` for this entry will see a gap; that is
        // preferable to dropping the live fan-out, which has no recovery
        // path. The `topic.as_str()` invariant (validated at publish
        // entry) guarantees the topic is also a valid WAL key.
        if let Some(wal) = &mut self.wal {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let record = wal_store::WalRecord::new(now_ns, Bytes::copy_from_slice(payload));
            match wal.append(topic_str, record) {
                Ok(offset) => {
                    tracing::trace!(
                        conn_idx = ctx.conn_idx,
                        topic = topic_str,
                        offset = offset.get(),
                        "[wal] appended"
                    );
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

        for target in targets {
            self.enqueue_frame(
                target.conn_idx,
                target.quic_stream_id,
                stream,
                MessageType::Publish,
                bytes.clone(),
            );
        }

        // P5: consumer-group fan-out — for each group on this topic,
        // deliver to exactly one local member (round-robin) so the group
        // load-balances across its members. The selected consumer may
        // be on the same connection as the publisher; we still deliver
        // in that case (the consumer is a different logical subscriber).
        let topic_str_owned = topic_str.to_string();
        let mut group_targets: Vec<(usize, u64)> = Vec::new();
        for ((t, _g), local) in &mut self.group_locals {
            if t != &topic_str_owned || local.members.is_empty() {
                continue;
            }
            let idx = local.rr_idx % local.members.len();
            local.rr_idx = local.rr_idx.wrapping_add(1);
            let m = &local.members[idx];
            group_targets.push((m.conn_idx, m.quic_stream_id));
        }
        for (conn_idx, quic_stream_id) in group_targets {
            self.enqueue_frame(
                conn_idx,
                quic_stream_id,
                stream,
                MessageType::Publish,
                bytes.clone(),
            );
        }

        // Cross-worker fan-out: broadcast to all other workers so their
        // local subscribers receive this publish. `try_send` is
        // non-blocking — a full inbox drops the publish rather than
        // stalling the reactor thread.
        if !self.worker_outboxes.is_empty() {
            let fwd_topic = topic_str.to_string();
            let fwd_payload = Bytes::copy_from_slice(payload);
            for (i, outbox) in self.worker_outboxes.iter().enumerate() {
                if i == self.worker_self_idx {
                    continue;
                }
                if let Err(std::sync::mpsc::TrySendError::Full(_)) = outbox.try_send(InterWorkerPublish {
                    topic: fwd_topic.clone(),
                    payload: fwd_payload.clone(),
                }) {
                    self.worker_drop_count = self.worker_drop_count.wrapping_add(1);
                    if self.worker_drop_count % WORKER_DROP_LOG_INTERVAL == 0 {
                        tracing::warn!(
                            worker = i,
                            total_drops = self.worker_drop_count,
                            "[pubsub] worker inbox full — dropped {WORKER_DROP_LOG_INTERVAL} publishes \
                             (raise cap in build_inter_worker_pubsub_channels)"
                        );
                    }
                }
            }
        }
    }

    /// Validates a wire-decoded topic string. Logs + returns `None` on
    /// invalid names so dispatch callers can early-return without
    /// enqueuing an error ack (the rejection is silent at the protocol
    /// level — only the operator sees the warning).
    fn validate_topic_or_log(&self, ctx: &FrameContext, raw: &str) -> Option<pubsub_engine::Topic> {
        match pubsub_engine::Topic::new(raw) {
            Ok(t) => Some(t),
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
            let ack = format!(
                "{{\"error\":\"denied\",\"action\":\"topic_create\",\"topic\":\"{topic}\"}}"
            );
            self.enqueue_response(ctx, stream, MessageType::TopicCreate, Bytes::from(ack));
            return;
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
        let ack = format!("{{\"topic_created\":\"{topic}\"}}");
        self.enqueue_response(ctx, stream, MessageType::TopicCreate, Bytes::from(ack));
    }

    /// Removes a topic from the catalog.
    pub fn handle_topic_delete(
        &mut self,
        ctx: &FrameContext,
        stream: StreamId,
        topic: &pubsub_engine::Topic,
    ) {
        // Phase 7: TopicDelete is the most dangerous operation (it can
        // wipe subscriber state), so its ACL action is separate from
        // TopicCreate. Deny returns an error ack and skips the catalog
        // mutation.
        if !self
            .check_acl(ctx.conn_idx, Action::TopicDelete, topic.as_str())
            .is_allow()
        {
            tracing::info!(
                conn_idx = ctx.conn_idx,
                topic = %topic,
                "[acl] topic_delete denied"
            );
            let ack = format!(
                "{{\"error\":\"denied\",\"action\":\"topic_delete\",\"topic\":\"{topic}\"}}"
            );
            self.enqueue_response(ctx, stream, MessageType::TopicDelete, Bytes::from(ack));
            return;
        }
        let existed = self.topic_catalog.delete(topic.as_str());
        tracing::info!(
            conn_idx = ctx.conn_idx,
            topic = %topic,
            existed,
            total_topics = self.topic_catalog.len(),
            "[pubsub] TopicDelete"
        );
        let ack = format!("{{\"topic_deleted\":\"{topic}\",\"existed\":{existed}}}");
        self.enqueue_response(ctx, stream, MessageType::TopicDelete, Bytes::from(ack));
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
                let ack = format!("{{\"ok\":true,\"principal\":{}}}", resolved.raw());
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::Handshake,
                    Bytes::from(ack),
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
            let ack = format!("{{\"error\":\"invalid_topic\",\"topic\":\"{topic_str}\"}}");
            self.enqueue_response(
                ctx,
                frame.stream_id,
                MessageType::FetchReply,
                Bytes::from(ack),
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
            let ack =
                format!("{{\"error\":\"denied\",\"action\":\"fetch\",\"topic\":\"{topic_str}\"}}");
            self.enqueue_response(
                ctx,
                frame.stream_id,
                MessageType::FetchReply,
                Bytes::from(ack),
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
            // Read up to (from + max_count) or head+1, whichever is smaller.
            let from_plus_cap = wal_store::Offset::new(from.get().saturating_add(max_count as u64));
            let head_next = wal_store::Offset::new(head.get() + 1);
            core::cmp::min(from_plus_cap, head_next)
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
                let ack = format!("{{\"error\":\"read_failed\",\"topic\":\"{topic_str}\"}}");
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::FetchReply,
                    Bytes::from(ack),
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
        if let Some(existing) = local.members.iter_mut().find(|m| m.consumer == consumer_id) {
            // Re-join from an existing consumer: update conn_idx/stream
            // (handles reconnection to a new QUIC connection).
            existing.conn_idx = ctx.conn_idx;
            existing.quic_stream_id = ctx.quic_stream_id;
        } else {
            local.members.push(GroupMember {
                conn_idx: ctx.conn_idx,
                quic_stream_id: ctx.quic_stream_id,
                consumer: consumer_id.clone(),
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
        let ack = format!(r#"{{"joined":"{topic}","group":"{group}","generation":{generation}}}"#);
        self.enqueue_response(
            ctx,
            frame.stream_id,
            MessageType::ConsumerGroupJoin,
            Bytes::from(ack),
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
        let ack = format!(r#"{{"left":"{topic}","group":"{group}"}}"#);
        self.enqueue_response(
            ctx,
            frame.stream_id,
            MessageType::ConsumerGroupLeave,
            Bytes::from(ack),
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
                let ack = format!(
                    r#"{{"heartbeat":"{topic}","group":"{group}","generation":{cur_gen}}}"#
                );
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupHeartbeat,
                    Bytes::from(ack),
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
                let ack = r#"{"error":"heartbeat_failed"}"#.to_string();
                self.enqueue_response(
                    ctx,
                    frame.stream_id,
                    MessageType::ConsumerGroupHeartbeat,
                    Bytes::from(ack),
                );
            }
        }
    }

    /// Removes a consumer from `group_locals` for a given (topic, group).
    /// If the group has no remaining members, the entry is dropped
    /// entirely so publish fan-out skips it.
    fn remove_group_member(&mut self, topic: &str, group: &str, consumer: &ConsumerId) {
        let key = (topic.to_string(), group.to_string());
        if let Some(local) = self.group_locals.get_mut(&key) {
            local
                .members
                .retain(|m| m.consumer.as_str() != consumer.as_str());
            if local.members.is_empty() {
                self.group_locals.remove(&key);
            } else if local.rr_idx >= local.members.len() {
                local.rr_idx = 0;
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
        // Drain into a local Vec first to release the borrow on
        // `self.cluster_incoming_rx` before we call back into
        // `self.cluster_deliver_local` / `self.coordinator`.
        const CLUSTER_DRAIN_CAP: usize = 256;
        let mut batch: Vec<InterNodeMessage> = Vec::with_capacity(CLUSTER_DRAIN_CAP);
        if let Some(rx) = &mut self.cluster_incoming_rx {
            while batch.len() < CLUSTER_DRAIN_CAP {
                match rx.try_recv() {
                    Ok(msg) => batch.push(msg),
                    Err(_) => break,
                }
            }
        }
        if batch.is_empty() {
            return;
        }
        let processed = batch.len();
        for msg in batch {
            match msg.kind {
                cluster_coordinator::InterNodeKind::Publish
                | cluster_coordinator::InterNodeKind::Fanout => {
                    self.cluster_deliver_local(
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
            }
        }
        tracing::trace!(processed, "[cluster] inbox drained");
    }

    /// Delivers an inter-node Publish/Fanout payload to local
    /// subscribers. Mirrors the wire encoding used by `handle_publish`:
    /// `topic_len(u16) | topic | payload`.
    ///
    /// Inter-node delivered messages do NOT re-trigger cluster
    /// routing (that would loop): this method consults only the local
    /// `pubsub_registry` + `group_locals`.
    fn cluster_deliver_local(
        &mut self,
        topic: &str,
        payload: &[u8],
        _origin_conn_idx: u32,
        _origin_stream_id: u64,
    ) {
        let topic_ref = match pubsub_engine::Topic::new(topic) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    topic = topic,
                    error = ?e,
                    "[cluster] incoming publish has invalid topic — dropping"
                );
                return;
            }
        };
        let targets: Vec<Subscriber> = self
            .pubsub_registry
            .lookup(&topic_ref)
            .into_iter()
            .collect();

        // Re-encode as a Publish frame so downstream subscribers receive the
        // same wire shape the publisher used (topic_len + topic + payload).
        let topic_str = topic_ref.as_str();
        let mut framed = Vec::with_capacity(2 + topic_str.len() + payload.len());
        framed.extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
        framed.extend_from_slice(topic_str.as_bytes());
        framed.extend_from_slice(payload);
        let bytes = Bytes::from(framed);

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

        // Consumer-group fan-out (P5) — mirror handle_publish.
        let topic_str_owned = topic_str.to_string();
        let mut group_targets: Vec<(usize, u64)> = Vec::new();
        for ((t, _g), local) in &mut self.group_locals {
            if t != &topic_str_owned || local.members.is_empty() {
                continue;
            }
            let idx = local.rr_idx % local.members.len();
            local.rr_idx = local.rr_idx.wrapping_add(1);
            let m = &local.members[idx];
            group_targets.push((m.conn_idx, m.quic_stream_id));
        }
        for (conn_idx, quic_stream_id) in group_targets {
            self.enqueue_frame(
                conn_idx,
                quic_stream_id,
                stream,
                MessageType::Publish,
                bytes.clone(),
            );
        }
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
        const CAP: usize = 256;
        let mut batch: Vec<InterWorkerPublish> = Vec::with_capacity(CAP);
        while batch.len() < CAP {
            match rx.try_recv() {
                Ok(msg) => batch.push(msg),
                Err(_) => break,
            }
        }
        if batch.is_empty() {
            return;
        }
        tracing::debug!(
            worker = self.worker_self_idx,
            count = batch.len(),
            "[pubsub] worker inbox drained"
        );
        for msg in batch {
            self.cluster_deliver_local(&msg.topic, &msg.payload, 0, 0);
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
        let removed = self.pubsub_registry.remove_connection(conn_idx);
        let principal = self.principals.remove(&conn_idx);

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
    pub fn new() -> Result<Self, ServerError> {
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
            session: SessionLayer::new()?,
            platform,
            dispatcher: VireonDispatcher::new(),
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

    /// Drives the stream scheduler once per recv batch.
    /// Propagates through session → stream, ticks the consumer-group
    /// heartbeat timeout, and drains the per-worker cluster inbox (P6).
    pub fn on_batch_tick(&mut self) {
        self.session.on_batch_tick();
        self.dispatcher.tick_groups();
        self.dispatcher.drain_cluster_inbox();
        self.dispatcher.drain_worker_inbox();
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

    /// Installs a WAL backend. Should be called before the first
    /// `Publish` arrives so every accepted message is durably
    /// appended before fan-out. Calling after the dispatcher has
    /// already seen traffic is also safe — only subsequent publishes
    /// are persisted; in-flight ones are unaffected.
    #[allow(dead_code)]
    pub fn set_wal(&mut self, wal: wal_store::FileStore) {
        self.dispatcher.wal = Some(wal);
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
        inbox: mpsc::Receiver<InterWorkerPublish>,
        outboxes: Vec<mpsc::SyncSender<InterWorkerPublish>>,
    ) {
        self.dispatcher.set_worker_channels(self_idx, inbox, outboxes);
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
        d.handle_publish(&ctx(1, 200), StreamId::new(2), &topic, b"payload");

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
        d.handle_publish(&ctx(1, 200), StreamId::new(2), &topic, b"hi");

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
        d.handle_topic_delete(&ctx(0, 100), StreamId::new(1), &topic);

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
            d.handle_publish(&ctx(1, 200), StreamId::new(2), &topic, &[i]);
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
            d.handle_publish(&ctx(1, 200), StreamId::new(2), &topic, &[i]);
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
        d.handle_publish(&ctx(1, 200), StreamId::new(2), &topic, b"payload");
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
}
