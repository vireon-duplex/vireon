# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Adaptive delivery strategy** — server selects BatchPush (small fan-out) or
  LogTail (WAL + NotifyOffset for 256+ subscribers) per-publish.
  Configurable via `[delivery]` TOML section.
- **Cluster subscription re-broadcast** — forwarded subscriptions are
  re-sent on every heartbeat tick to self-heal from lost UDP datagrams
  and late-forming cluster hash rings.
- **Cross-worker backpressure** — shared `AtomicBool` pressure flags
  prevent multi-worker fan-out from overwhelming subscriber streams.
- **QUIC connection migration** — WiFi/NAT switch no longer kills
  sessions; dedicated `migration` flag separate from multipath.
- **Subscriber channel default** increased from 8192 to 65536 to absorb
  publish bursts without dropping.
- **Consumer groups** — worker-0-centric global delivery eliminates N×dup
  in multi-worker deployments.
- **WAL segment rotation** — O(1) truncate on retention boundary.
- **Heartbeat failure detection** — `NodeDown` gossip for automatic
  cluster recovery.
- **Per-connection metrics** — gap markers on disconnect, publisher
  backpressure ack.
- **SDK diagnostic counters** — `notify_offset_count()` and
  `fetch_reply_count()` for LogTail introspection.
- **15 SDK examples** — quickstart, HOL congestion, ordering, full-limit,
  aggregate throughput, pool multiplex, consumer groups, cross-node
  fan-out, connection migration, LogTail delivery.
- **Bench-harness** — standalone throughput/load generator with broadcast,
  group, pipeline, and request-response modes.

### Changed
- `subscriber_buffer` default: 8192 → 65536.
- `encode_frame_coalesced_into` documented as destructive (`dst.clear()`)
  — callers must not invoke it in a loop.
- `WalStore::read_range` `until` parameter documented as INCLUSIVE.

### Fixed
- **Subscriber channel burst loss** — 60% message loss at 50K+ frames
  caused by subscriber channel (8192 depth) filling during publish burst.
  Fixed: default 65536 + concurrent drain in s14.
- **Cluster sub propagation** — intermittent s18 failures (node receives
  0 deliveries) caused by fire-and-forget UDP Subscribe. Fixed: periodic
  re-broadcast in `tick_cluster_health`.
- **Cross-worker fan-out instability** — multi-worker broadcast fanout
  was 0.3–3.0× unstable. Fixed via shared AtomicBool pressure flags.
- **SDK partial-write data loss** — `Transport::stream_send` discarded
  quiche partial-write count, silently dropping frame tails.
- **Server partial-write byte-order corruption** — retry queue let new
  frames bypass buffered tails.
- **Ghost UDP sockets at shutdown** — multishot RECV in io_uring held
  kernel socket ref ~60s after exit.
- **`encode_fetches_into` batch bug** — calling
  `encode_frame_coalesced_into` in a loop dropped all but the last Fetch.
- **`read_range` off-by-one** — Fetch handler computed exclusive upper
  bound but `read_range` treats `until` as INCLUSIVE, causing 2× delivery.

## [0.1.0] — Initial Workspace

### Added
- QUIC-native pub/sub runtime with per-stream delivery semantics.
- Clean architecture: transport-layer, stream-layer, application-layer,
  cluster-coordinator, message-layer.
- TLS 1.3 + mTLS support.
- ACL system with wildcard pattern matching.
- Per-IP rate limiting and topic quotas.
- WAL with configurable sync policy and retention.
- io_uring integration for zero-copy UDP I/O.
- Client SDK with connection pool, dedicated streams, consumer groups.
- Docker support with multi-arch builds.
- Prometheus `/metrics` endpoint.
- Comprehensive CI/CD (ci, security, benchmarks, release, docker).
