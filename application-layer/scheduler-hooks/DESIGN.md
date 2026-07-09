# scheduler-hooks — DESIGN

**Reactor scheduler hooks** for the Vireon runtime layer.

---

## 1. Overview & Goals

The `scheduler-hooks` crate provides a typed framework for injecting
custom logic into the reactor's event loop at well-defined points:
before/after each tick, before submission of I/O operations, after
completion processing, on idle, and on wakeup.

### Goals

1. **Type-safe hook IDs**: `SchedulerHookId` is a `u64` newtype with
   a nil sentinel — no magic integers.
2. **Explicit triggers**: `HookTrigger` enumerates the 6 injection
   points; no string matching.
3. **Priority ordering**: `HookPriority` (u8, 0=highest) lets
   critical hooks run first.
4. **Thread-safe stats**: all counters are `AtomicU64` with `Relaxed`
   ordering — no locks on the hot path.
5. **Observable health**: hook failure rates feed into a tri-state
   health report.

### Non-Goals

- Hook execution scheduling (that is the reactor's job).
- Hook code loading (Phase 1+ plugin manager).
- Inter-hook communication (hooks are independent).

---

## 2. Hook Triggers

Hooks fire at specific points in the reactor's tick cycle:

```text
  ┌─────────────────────────────────────────────────────┐
  │                    REACTOR TICK                      │
  │                                                      │
  │  1. PreTick ──────► (hooks run here)                │
  │  2. Process CQEs                                    │
  │  3. PreSubmit ────► (hooks run before io_uring_submit)│
  │  4. Submit SQEs                                     │
  │  5. PostComplete ─► (hooks run after completion)     │
  │  6. PostTick ─────► (hooks run here)                │
  │  7. If no work: OnIdle ─► (hooks run)               │
  │  8. If woken: OnWakeup ─► (hooks run)               │
  └─────────────────────────────────────────────────────┘
```

### Trigger Semantics

| Trigger | When | Typical Use |
|---------|------|-------------|
| `PreTick` | Start of each tick | Metrics sampling, timer checks |
| `PostTick` | End of each tick | Stats flush, health probe |
| `PreSubmit` | Before `io_uring_submit` | Batch coalescing, priority bump |
| `PostComplete` | After CQE processing | Response dispatch, state machine |
| `OnIdle` | No pending work | GC sweep, cache compaction |
| `OnWakeup` | Reactor woken externally | Urgent flush, deadline check |

---

## 3. Hook Priority

`HookPriority(u8)` controls ordering within a trigger. Lower value =
higher priority = runs first.

### Named Priorities

| Constant | Value | Use Case |
|----------|-------|----------|
| `HIGHEST` | 0 | Critical observability, panic handlers |
| `HIGH` | 32 | Security checks, rate limiting |
| `NORMAL` | 64 | Business logic |
| `LOW` | 128 | Logging, diagnostics |
| `LOWEST` | 255 | Cleanup, best-effort |

### Comparison

`is_more_important_than(other)` compares `.raw()` values (lower wins).
This comparison is `const fn` — no `PartialEq` dependency in const
context.

---

## 4. Hook Identity

`SchedulerHookId(u64)` uniquely identifies a registered hook.

- `NIL = 0` — sentinel for unassigned.
- `new(u64)` — constructs from raw value.
- `is_nil()` / `raw()` — predicates.

The registry (Phase 1) will assign monotonically increasing IDs.

---

## 5. Error Taxonomy

```rust
enum SchedulerHookError {
    HookPanic,                    // hook closure panicked
    DuplicateHook(SchedulerHookId), // ID already registered
    InvalidPhase(&'static str),   // trigger not in config
    HookFailed(&'static str),     // hook returned Err
    ShuttingDown,                 // lifecycle: stopping
}
```

### Classification

| Variant | `is_fatal` | `is_retryable` |
|---------|------------|----------------|
| `HookPanic` | true | false |
| `DuplicateHook` | false | false |
| `InvalidPhase` | true | false |
| `HookFailed` | false | true |
| `ShuttingDown` | true | false |

`HookPanic` is fatal because a panicking hook indicates a bug; the
reactor should abort the tick to avoid corruption.

---

## 6. Configuration

### `SchedulerHooksConfig`

| Field | Type | Default | Constraints |
|-------|------|---------|-------------|
| `max_hooks` | `u32` | 128 | 1..=1024 |
| `enable_pre_tick` | `bool` | true | — |
| `enable_post_tick` | `bool` | true | — |
| `enable_on_idle` | `bool` | true | — |
| `tick_budget_ns` | `u64` | 10_000 | 100..=1_000_000 |

### Presets

| Preset | PreTick | PostTick | OnIdle | Use Case |
|--------|---------|----------|--------|----------|
| `Minimal` | yes | yes | no | Low-overhead production |
| `Full` | yes | yes | yes | Development / debugging |

---

## 7. Observability

### Counters

| Counter | Type | Metric name |
|---------|------|-------------|
| `hooks_registered` | gauge | `sched_hooks_registered` |
| `hooks_invoked` | counter | `sched_hooks_invoked_total` |
| `hooks_failed` | counter | `sched_hooks_failed_total` |
| `hook_panics` | counter | `sched_hook_panics_total` |
| `total_hook_time_ns` | counter | `sched_hook_time_ns_total` |
| `tick_count` | counter | `sched_tick_count_total` |

### Derived Rates

- `failure_rate_bps()` — `failed / invoked * 10_000`
- `avg_hook_time_ns()` — `total_hook_time_ns / invoked`

### Health Thresholds

| Signal | Healthy | Degraded | Critical |
|--------|---------|----------|----------|
| Failure rate | ≤ 100 bps (1%) | 100–1000 | > 1000 (10%) |

A failure rate above 1% is already degraded because hooks run on
every tick — even small failure rates compound over millions of
ticks/second.

---

## 8. Hook Registry (Phase 1)

`HookRegistry` will store hooks in a priority-sorted structure per
trigger:

```text
Trigger::PreTick:
  [(priority=0, hook=A), (priority=32, hook=B), (priority=64, hook=C)]

Trigger::PostTick:
  [(priority=64, hook=D)]
```

### Operations (Phase 1)

| Method | Returns | Notes |
|--------|---------|-------|
| `register(id, trigger, priority)` | `Ok(())` or `DuplicateHook` | Adds hook |
| `unregister(id)` | `Ok(())` or `NotFound` | Removes hook |
| `invoke(trigger)` | `Ok(count)` or error | Runs all hooks for trigger |

### Invocation Contract

1. Hooks are invoked in priority order (lowest first).
2. If a hook fails, it is counted but execution continues.
3. If a hook panics, the tick aborts immediately (catch_unwind).
4. Hook execution time is added to `total_hook_time_ns`.

---

## 9. Hook Coordinator (Phase 1)

`HookCoordinator` wraps the registry and integrates with the reactor:

```rust
impl HookCoordinator {
    pub fn on_tick(&mut self) -> Result<u64, SchedulerHookError>;
    pub fn on_submit(&mut self) -> Result<(), SchedulerHookError>;
    pub fn on_complete(&mut self) -> Result<(), SchedulerHookError>;
    pub fn on_idle(&mut self) -> Result<(), SchedulerHookError>;
    pub fn on_wakeup(&mut self) -> Result<(), SchedulerHookError>;
}
```

Each method invokes the corresponding trigger's hooks and updates
stats.

---

## 10. Failure Modes

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Hook panic | `catch_unwind` → `HookPanic` | Abort tick, increment panic counter |
| Hook returns Err | Direct return → `HookFailed` | Continue, log, increment failure |
| Hook too slow | Exceeds `tick_budget_ns` | Warn, skip remaining hooks for tick |
| Duplicate registration | Registry check | Return `DuplicateHook` |
| Invalid trigger | Config check | Return `InvalidPhase` |

---

## 11. Performance Characteristics

| Operation | Complexity | Expected latency |
|-----------|------------|------------------|
| `HookTrigger::as_str` | O(1) | <1 ns |
| `HookPriority::is_more_important_than` | O(1) | <1 ns |
| `register` (Phase 1) | O(log n) | <100 ns |
| `invoke` (Phase 1) | O(n log n) | <1 µs per trigger |

The hot path is `invoke`, which runs on every tick. With ≤128 hooks
per trigger and simple callbacks, the overhead is dominated by the
callback itself, not the dispatch.

---

## 12. Integration Points

| Consumer | How it uses scheduler-hooks |
|----------|---------------------------|
| `reactor` (runtime-layer) | Calls `HookCoordinator::on_tick` etc. |
| `graceful-startup-shutdown` | Registers `PreTick` hook for drain check |
| `application-runtime` | Composes hooks into the full runtime |
| `observability-sdk` | Registers `PostTick` hook for metrics flush |

---

## 13. Testing Strategy

### Unit Tests (Phase 0)

- HookId nil/new/raw round-trip
- HookTrigger predicates and as_str
- HookPriority comparison
- Config validation (all presets produce valid configs)
- Stats recording and snapshot
- Health threshold classification

### Integration Tests (Phase 1+)

- Register → invoke → unregister cycle
- Priority ordering within a trigger
- Panic propagation (catch_unwind)
- Budget enforcement

---

## 14. API Reference

### Core Types
- `SchedulerHookId` — hook identifier newtype
- `HookTrigger` — trigger point enum
- `HookPriority` — priority newtype

### Config
- `SchedulerHooksConfig` — declarative configuration
- `Preset` — Minimal / Full

### Observability
- `SchedulerHookStats` — atomic counters
- `StatsSnapshot` — point-in-time snapshot
- `HealthReport` / `HealthState` — health summary

### Builder
- `SchedulerHooksBuilder` — fluent config builder

### Facades (Phase 1)
- `HookRegistry` — hook storage and dispatch
- `HookCoordinator` — reactor integration

---

## 15. Operational Runbook

### Symptom: High hook failure rate

1. Check `sched_hooks_failed_total` rate.
2. Identify which trigger has the most failures.
3. Disable the failing hook or fix its logic.

### Symptom: Reactor tick latency spike

1. Check `sched_hook_time_ns_total` rate — hooks may be slow.
2. If hooks dominate, reduce `tick_budget_ns` or disable non-critical
   hooks.
3. Use `Preset::Minimal` to disable `OnIdle` hooks.

---

## 16. Security Considerations

- Hooks run in the reactor thread — a malicious hook can stall the
  entire reactor.
- `tick_budget_ns` enforces a soft limit; Phase 1+ may add hard
  timeout with async cancellation.
- Hook registration should be restricted to trusted code (Phase 1+
  plugin manager will add capability checks).

---

## 17. Future Directions

- **Async hooks**: allow hooks to return futures for non-blocking
  work.
- **Hook profiling**: per-hook timing histograms for bottleneck
  identification.
- **Conditional hooks**: trigger only when a condition predicate
  matches (e.g., "run GC only if memory > 80%").
- **Hook chaining**: allow hooks to modify or cancel subsequent hooks.
- **NUMA-aware dispatch**: run hooks on the local NUMA node's core.
