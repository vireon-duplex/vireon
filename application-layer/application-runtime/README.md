# application-runtime

**Top-level composition root** for the Vireon application layer. Ties together
all 20 application-layer crates into a single runtime facade with unified
configuration, health monitoring, and lifecycle management.

## Architecture (one-line)

```text
  AppRuntimeBuilder ──► AppRuntime ──► CompositionRoot
                              │
                              ├─ 20 Application-Layer Crates
                              │   ├─ configuration-system
                              │   ├─ version-negotiation
                              │   ├─ feature-flags
                              │   ├─ scheduler-hooks
                              │   ├─ observability-sdk
                              │   ├─ resource-quotas
                              │   ├─ module-lifecycle
                              │   ├─ middleware-pipeline
                              │   ├─ plugin-manager
                              │   ├─ service-registry
                              │   ├─ auth-framework
                              │   ├─ dependency-injection
                              │   ├─ graceful-startup-shutdown
                              │   ├─ rpc-framework
                              │   ├─ event-bus
                              │   ├─ pubsub-engine
                              │   ├─ media-streaming
                              │   ├─ ai-streaming
                              │   ├─ agent-runtime
                              │   └─ ops-dashboard
                              │
                              └─ Lower-Layer Crates
                                  ├─ session-manager
                                  ├─ session-registry
                                  ├─ connection-shard
                                  ├─ stream-manager
                                  └─ reactor
```

See [`DESIGN.md`](DESIGN.md) for the full 20-section specification.

## Layer map

| #  | Module          | Phase | Responsibility |
|----|-----------------|-------|----------------|
| 1  | `error`         | 0     | `AppRuntimeError` thiserror enum |
| 2  | `config`        | 0     | `AppRuntimeConfig`, `Preset` |
| 3  | `runtime_id`    | 0     | `RuntimeId` newtype, `RuntimeRole` enum |
| 4  | `stats`         | 0     | `AppRuntimeStats` atomics, `StatsSnapshot` |
| 5  | `health`        | 0     | `HealthReport`, `HealthState` |
| 6  | `builder`       | 0     | `AppRuntimeBuilder` (fluent) |
| 7  | `runtime`       | 1     | `AppRuntime` facade |
| 8  | `composition`   | 1     | `CompositionRoot` facade |

## Phase 0 status

Modules 1-6 are real implementations. Modules 7-8 ship as typed skeletons
returning `NotInitialized` / `ShuttingDown` errors. Functional runtime and
composition land in Phase 1+.

## Quick start

```rust
use application_runtime::{AppRuntimeBuilder, Preset};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start from a preset, tune as needed.
    let cfg = AppRuntimeBuilder::from_preset(Preset::Production)
        .max_modules(512)
        .startup_timeout_ms(45_000)
        .shutdown_timeout_ms(20_000)
        .health_check_interval_ms(3_000)
        .build()?;

    // Phase 0: config is validated. Runtime startup ships in Phase 1.
    let _ = cfg;
    Ok(())
}
```

## Presets

| Preset       | Modules | Startup | Shutdown | Health | Use case |
|--------------|---------|---------|----------|--------|----------|
| `Development`| 64      | 10s     | 5s       | 1s     | Local dev / testing |
| `Production` | 256     | 30s     | 15s      | 5s     | Production deployment |
| `Minimal`    | 8       | 5s      | 2s       | 10s    | Edge / embedded |

## Runtime roles

`RuntimeRole` classifies the runtime's deployment archetype:

| Role         | Clustered | Description |
|--------------|-----------|-------------|
| `Standalone` | no        | Single-node deployment |
| `Cluster`    | yes       | Member of a multi-node cluster |
| `Edge`       | no        | Edge node with limited resources |
| `Controller` | yes       | Cluster controller/coordinator |

## Re-exports

This crate re-exports the foundational types from every crate it composes, so
downstream consumers can depend on `application-runtime` alone:

- **Core framework**: `configuration-system`, `version-negotiation`,
  `feature-flags`, `scheduler-hooks`, `observability-sdk`, `resource-quotas`,
  `module-lifecycle`, `middleware-pipeline`, `plugin-manager`,
  `service-registry`, `auth-framework`, `dependency-injection`,
  `graceful-startup-shutdown`
- **Service modules**: `rpc-framework`, `event-bus`, `pubsub-engine`,
  `media-streaming`, `ai-streaming`, `agent-runtime`, `ops-dashboard`
- **Lower layer**: `session-manager`, `session-registry`, `connection-shard`,
  `stream-manager`, `reactor`

## Configuration bounds

| Field                   | Min       | Max       |
|-------------------------|-----------|-----------|
| `max_modules`           | 1         | 1,024     |
| `startup_timeout_ms`    | 100ms     | 300,000ms (5 min) |
| `shutdown_timeout_ms`   | 100ms     | 300,000ms (5 min) |
| `health_check_interval_ms` | 100ms  | 300,000ms (5 min) |

## Verification gates

Every crate passes:

```bash
cargo check
cargo clippy -- -D warnings
cargo fmt --check
cargo test
cargo bench --no-run
```

## License

See the workspace `Cargo.toml`.
