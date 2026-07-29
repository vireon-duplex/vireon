//! # cluster-coordinator
//!
//! Multi-node pub/sub topology, routing, and inter-node message
//! format for the Vireon runtime.
//!
//! Phase 6 ships the **static-membership, single-coordinator** model:
//!
//! * Each topic has exactly one owning node, chosen by
//!   [`TopicRouter`] via consistent hashing.
//! * A non-owner node forwards publishes to the owner via
//!   [`InterNodeMessage::Publish`].
//! * The owner fans out to local subscribers **plus** every node
//!   that has registered remote interest in a matching pattern
//!   (via [`InterNodeMessage::Subscribe`]).
//! * Cluster membership is supplied at startup and does not change
//!   at runtime. SWIM/gossip-style dynamic membership is deferred.
//!
//! ## Modules
//!
//! | Module           | Responsibility |
//! |------------------|----------------|
//! | [`membership`]   | `ClusterMembership`, `ClusterConfig` |
//! | [`router`]       | `TopicRouter` (topic → owner) |
//! | [`subscriber`]   | `RemoteSubscriber`, `RemoteSubscriberRegistry` |
//! | [`message`]      | `InterNodeMessage` wire format |
//! | [`coordinator`]  | `ClusterCoordinator` facade |
//! | [`error`]        | `ClusterError` |
//!
//! ## Example
//!
//! ```no_run
//! use cluster_coordinator::{
//!     ClusterConfig, ClusterCoordinator, InterNodeMessage, RemoteSubscriber,
//! };
//! use session_registry::node::{NodeAddress, NodeInfo, NodeState, Region, Zone};
//! use session_registry::NodeId;
//! # fn make_node(id: u32) -> NodeInfo {
//! #     let mut n = NodeInfo::new(NodeId::new(id), Region::GLOBAL, Zone::DEFAULT,
//! #                               NodeAddress::loopback(4000 + id as u16));
//! #     n.state = NodeState::Alive; n
//! # }
//!
//! // Static two-node cluster, self is node 1.
//! let cfg = ClusterConfig::new(NodeId::new(1), vec![make_node(1), make_node(2)]);
//! let mut coord = ClusterCoordinator::new(cfg).unwrap();
//!
//! match coord.route_publish("sensor.temp") {
//!     cluster_coordinator::PublishDecision::Own { remote_targets, .. } => {
//!         // Deliver to local subscribers, then fan out.
//!     }
//!     cluster_coordinator::PublishDecision::Forward { owner } => {
//!         // Send InterNodeMessage::Publish to `owner`.
//!     }
//! }
//! ```

pub mod coordinator;
pub mod error;
pub mod membership;
pub mod message;
pub mod router;
pub mod subscriber;

// ── Re-exports ────────────────────────────────────────────────────

pub use coordinator::{ClusterCoordinator, ClusterStats, ClusterStatsSnapshot, PublishDecision};
pub use error::ClusterError;
pub use membership::{ClusterConfig, ClusterMembership, DEFAULT_VNODES};
pub use message::{InterNodeKind, InterNodeMessage, MAX_TOPIC_LEN};
pub use router::{MAX_VNODES, MIN_VNODES, TopicRouter};
pub use subscriber::{RemoteSubscriber, RemoteSubscriberRegistry};
