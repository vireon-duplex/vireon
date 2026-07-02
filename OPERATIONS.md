# Operations — Vireon

Observability, alerting, on-call, and incident response for production.
> If it is not monitored, it does not exist in production.

## 1. Observability pillars

**Logging** — structured JSON via `tracing`; every line carries `trace_id` +
`span_id`. Never log secrets/PII. Ship to a central store (Loki/ELK).

**Metrics (Prometheus):**

| Metric | Type | Description |
|---|---|---|
| `broker_messages_in_total` | Counter | Messages published (by stream) |
| `broker_messages_out_total` | Counter | Messages delivered (by stream/subscription) |
| `broker_message_bytes_total` | Counter | Bytes in/out |
| `broker_consumer_lag` | Gauge | Undelivered messages per consumer group |
| `broker_active_connections` | Gauge | Connected producers/consumers |
| `broker_stream_depth` | Gauge | Messages retained per stream |
| `broker_publish_duration_seconds` | Histogram | Append latency |
| `broker_delivery_duration_seconds` | Histogram | End-to-end delivery latency |
| `http_requests_total` / `_duration_seconds` | Counter/Histogram | Control-plane API |

**Tracing (OpenTelemetry):** OTLP gRPC → Jaeger/Tempo; W3C trace context;
head-based sampling (100% for errors) + tail-based.

## 2. Dashboards (Grafana, placeholders)

| Dashboard | Highlights |
|---|---|
| Broker throughput | msgs/sec in/out, bytes, publish/delivery latency |
| Consumer health | consumer lag, active groups, ack rate |
| Connections | active producers/consumers, disconnects |
| Infrastructure | CPU, memory, pod count, HPA, disk/retention |

## 3. Alerting

| Alert | Sev | Condition |
|---|---|---|
| Broker down / unhealthy | P1 | health check failing 1 min |
| Publish error rate high | P1 | append errors > 1% for 2 min |
| Consumer lag runaway | P2 | lag > threshold for 5 min (sustained growth) |
| Delivery latency P95 breach | P2 | P95 > target for 5 min |
| Connection exhaust / churn | P2 | reconnect storm or conn limit hit |
| Disk / retention pressure | P3 | usage > 85% |
| Cert expiring | P3 | TLS cert < 7 days |

## 4. SLOs / SLIs

| SLI | SLO | Window |
|---|---|---|
| Availability | > 99.9% | 30 days |
| Publish P95 latency | < 10ms | 30 days |
| Delivery P95 latency | < 50ms | 30 days |
| Durability (no acknowledged message lost) | 100% | 30 days |

Error budget at 99.9% ≈ 43 min/month; freeze non-critical deploys when exhausted.

## 5. On-call

- Primary (7-day rotation) + secondary (escalates after 5 min).
- When paged: ack < 5 min → triage → coordinate in incident channel → mitigate
  → resolve via dashboards → postmortem for P1/P2.

### Severity

| Sev | Definition | Ack |
|---|---|---|
| P1 | Outage / major impact | < 5 min |
| P2 | Significant degradation | < 15 min |
| P3 | Minor / no immediate impact | < 1 hr |
| P4 | Informational | next business day |

## 6. Incident response

`Detect → Acknowledge → Mitigate → Resolve → Postmortem`.
Postmortems are **blameless**, focus on systemic causes, with time-bound action
items, shared within 48h for P1/P2.

## 7. Capacity & scaling

- HPA on CPU/memory (and optionally consumer lag / connection count).
- Scale storage/retention before disk pressure; rebalance in the background,
  throttled, never starving foreground traffic.

### Backpressure

- Bounded channels everywhere; a slow consumer must not crash the broker.
- Apply explicit drop/reject policy under extreme load (e.g. reject new
  subscriptions when saturated) and expose queue depth as a metric.
