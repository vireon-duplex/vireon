# scheduler-hooks

Reactor scheduler hooks for the Vireon runtime layer.

## Overview

Provides pre/post-tick, pre-submit, post-complete, on-idle, and
on-wakeup triggers with priority ordering for custom logic injection
into the reactor's event loop.

## Hook Triggers

```
PreTick → ProcessCQEs → PreSubmit → SubmitSQEs → PostComplete → PostTick
                                                                 ↓
                                                           OnIdle / OnWakeup
```

## Quick Start

```rust
use scheduler_hooks::{SchedulerHookId, HookTrigger, HookPriority};

let id = SchedulerHookId::new(42);
let trigger = HookTrigger::PreTick;
let priority = HookPriority::HIGH; // runs early
```

## Priority Levels

| Constant | Value | Use Case |
|----------|-------|----------|
| `HIGHEST` | 0 | Observability, panic handlers |
| `HIGH` | 32 | Security, rate limiting |
| `NORMAL` | 64 | Business logic |
| `LOW` | 128 | Logging, diagnostics |
| `LOWEST` | 255 | Cleanup, best-effort |

## Presets

| Preset | Triggers Enabled | Use Case |
|--------|-----------------|----------|
| `Minimal` | PreTick, PostTick | Low-overhead production |
| `Full` | All triggers | Development / debugging |

## Phase 0 Status

- **Real-impl**: SchedulerHookId, HookTrigger, HookPriority,
  Config/Stats/Health/Builder
- **Skeletons**: HookRegistry, HookCoordinator (return
  `ShuttingDown` errors)
- **Tests**: 60 unit tests
- **Benches**: 4 Criterion stubs

See `DESIGN.md` for the full specification.
