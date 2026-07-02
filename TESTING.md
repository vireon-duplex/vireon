# Testing Strategy — Vireon

Testing strategy, coverage targets, and layer rules. Supplements
[SKILLS.md §16](./SKILLS.md).

## 1. Pyramid

```text
       E2E          few, full stack
     Contract       wire/schema
   Integration      real storage/transport ports, API
  Unit              domain + application logic (the bulk)
```

- **Domain first** — pure, fast, deterministic; the most-tested layer.
- **Real adapters over mocks** — exercise storage/transport ports against real
  implementations (testcontainers); never mock a storage engine.
- **Incident-driven** — every prod bug gets a regression test before the fix.

## 2. Coverage targets

| Layer | Min | Preferred |
|---|---|---|
| Domain | 95% | 100% |
| Application | 85% | 95% |
| Infrastructure | 75% | 85% |
| API | 80% | 90% |
| **Overall** | **80%** | **90%+** |

## 3. Layer rules

| Layer | Style | Notes |
|---|---|---|
| Domain | pure unit | std + domain types only; no I/O |
| Application | unit + trait mocks | depends on domain port traits only |
| Infrastructure | integration | real storage/transport via testcontainers |
| API | integration + contract | full router; real auth, not stubbed |

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_rejects_empty_stream_name() {
        assert!(StreamName::new("").is_err());
    }
}
```

## 4. Broker-specific scenarios

| Category | Approach |
|---|---|
| **Ordering** | assert per-stream/per-partition offset order under concurrency |
| **Consumer groups** | competing consumers each message delivered exactly once per group |
| **Delivery semantics** | at-least-once redelivery on no-ack; exactly-once dedup |
| **Throughput / load** | sustained publish/deliver rate, backpressure saturation (k6 / custom harness) |
| **Retention** | messages expire after time/size; replay seeks by offset/time |
| **Failover** | kill a node/connection mid-stream; verify reconnect + offset resume |
| **Backpressure** | slow consumer must not crash broker or starve others |

## 5. Naming & structure

- Tests live in `#[cfg(test)] mod tests` within the same file.
- Name tests `it_<behavior>` or `should_<behavior>` (never `test_1`).
- Async tests use `#[tokio::test]`. Arrange-Act-Assert structure.
- One behavior per test.

## 6. Fixtures

- Builder/factory patterns for domain objects (`StreamBuilder::default()…build()`).
- `testcontainers` for storage/transport backends; isolate state per test.

## 7. CI integration

| Stage | Command | Gate |
|---|---|---|
| Unit + Integration | `cargo test --workspace` | blocks merge |
| Coverage | `cargo tarpaulin --workspace --fail-under 80` | blocks merge |
| Load | k6 against staging | required before prod release |

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

## 8. Do / Don't

| Do | Don't |
|---|---|
| Test against real storage/transport in integration | Mock the storage engine |
| Use `testcontainers` for backends | In-memory fakes that drift from reality |
| Name tests by behavior | `test_1`, `test_function` |
| Isolate state per test | Share mutable state between tests |
| Regression-test every prod bug | Fix and move on without coverage |
| `#[tokio::test]` for async | `block_on()` inside `#[test]` |
