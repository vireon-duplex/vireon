# version-negotiation — DESIGN

**Protocol version negotiation** for the Vireon application layer.

---

## 1. Overview & Goals

The `version-negotiation` crate provides a typed, compile-safe
framework for negotiating protocol versions between two endpoints.
It is used during connection setup (QUIC ALPN-style), API version
selection, and feature compatibility checks.

### Goals

1. **Type-safe versions**: `ProtocolVersion` is a newtype over `u32`
   with semver encoding — no magic integers in user code.
2. **Declarative ranges**: `VersionRange` expresses "we support
   versions 1.2.0 through 1.5.3" without manual comparisons.
3. **Explicit compatibility**: `CompatibilityMatrix` records which
   (local, remote) version pairs are known to interoperate.
4. **Observable**: every negotiation attempt is counted, classified,
   and exported for health monitoring.
5. **Zero-allocation hot path**: the negotiation types are `Copy` and
   comparison-based; no heap allocation on the negotiate path.

### Non-Goals

- Wire-format encoding (handled by `frame` layer).
- Transport-level negotiation (handled by `quic-engine` ALPN).
- Schema evolution (handled by `serde` in Phase 1+).

---

## 2. Version Encoding

A `ProtocolVersion` packs semver triple into a `u32`:

```text
  31              16 15       8  7        0
 ┌──────────────────┬──────────┬──────────┐
 │     Major (16)   │ Minor(8) │ Patch(8) │
 └──────────────────┴──────────┴──────────┘
```

| Field | Bits | Range | Notes |
|-------|------|-------|-------|
| Major | 16 | 0..=65535 | Breaking changes |
| Minor | 8 | 0..=255 | Backward-compatible features |
| Patch | 8 | 0..=254 | Bug fixes; 255 = deprecated sentinel |

### Version Classification

| Predicate | Condition | Example |
|-----------|-----------|---------|
| `is_stable()` | major > 0 && patch < 255 | `(1, 0, 0)` |
| `is_experimental()` | major == 0 | `(0, 9, 1)` |
| `is_deprecated()` | patch == 255 | `(1, 5, 255)` |

The `patch == 255` sentinel allows encoding deprecation without a
separate flag, keeping the type `Copy` and `u32`-backed.

---

## 3. Version Ranges

`VersionRange { min, max }` expresses a closed interval of supported
versions. All comparisons use `.raw()` to stay `const fn`-compatible
(derived `PartialEq` is not const in Rust 2024).

### Operations

| Method | Semantics | Complexity |
|--------|-----------|------------|
| `contains(v)` | `min.raw() <= v.raw() && v.raw() <= max.raw()` | O(1) |
| `overlaps(other)` | `self.min.raw() <= other.max.raw() && other.min.raw() <= self.max.raw()` | O(1) |

### Construction

`VersionRange::new(min, max)` returns `Err(InvalidVersion)` if
`min.raw() > max.raw()`.

---

## 4. Compatibility Matrix

`CompatibilityMatrix` stores up to 64 `CompatEntry` records in a
fixed-size array. Each entry records:

```rust
struct CompatEntry {
    local: ProtocolVersion,
    remote: ProtocolVersion,
    compatible: bool,
}
```

### Lookup

`check(local, remote)` scans the array linearly and returns:
- `Some(true)` — explicitly compatible
- `Some(false)` — explicitly incompatible
- `None` — unknown pair; caller falls back to range-based check

A linear scan over ≤64 entries is cache-friendly and avoids the
overhead of a `HashMap` in Phase 0. Phase 1+ may add a perfect hash
if the matrix grows.

### Why a fixed array?

1. No heap allocation in Phase 0.
2. Predictable memory layout (no pointer chasing).
3. The matrix is typically small (<20 entries) in practice.
4. Copy semantics for the containing struct if needed.

---

## 5. Negotiation Algorithm (Phase 1+)

The `VersionNegotiator` facade (Phase 1) implements the following:

```text
1. Collect local_supported: VersionRange
2. Collect remote_advertised: [ProtocolVersion]
3. For each remote version (highest first):
   a. If local_supported.contains(remote):
      i.  If matrix.check(local, remote) != Some(false):
          - return Ok(remote)   // success
4. If allow_experimental && any experimental remote in range:
   - return Ok(experimental_version) with warning
5. If allow_deprecated && any deprecated remote in range:
   - return Ok(deprecated_version) with warning
6. Return Err(NoOverlap)
```

### Fallback Strategy

`select_fallback()` returns the lowest local version in range — the
"most compatible" option — when negotiation fails. This is used for
diagnostics and optional retry.

---

## 6. Error Taxonomy

```rust
enum VersionNegotiationError {
    NoOverlap,              // no common version
    InvalidVersion(u32),    // raw value is malformed
    Deprecated(u32),        // version is in deprecated set
    IncompatibleVersions,   // matrix marks pair as incompatible
    ShuttingDown,           // lifecycle: service stopping
}
```

### Classification

| Variant | `is_fatal` | `is_retryable` | Caller action |
|---------|------------|----------------|---------------|
| `NoOverlap` | false | true | Retry with broader range |
| `InvalidVersion` | true | false | Fix caller code |
| `Deprecated` | false | true | Warn + retry |
| `IncompatibleVersions` | false | false | Hard reject |
| `ShuttingDown` | true | false | Abort |

---

## 7. Configuration & Presets

### `VersionNegotiationConfig`

| Field | Type | Default | Constraints |
|-------|------|---------|-------------|
| `max_supported` | `u32` | encoded `(2, 0, 0)` | must be > min |
| `min_supported` | `u32` | encoded `(1, 0, 0)` | must be < max |
| `allow_experimental` | `bool` | false | dev/staging only |
| `allow_deprecated` | `bool` | false | migration window |
| `negotiation_timeout_ms` | `u64` | 5_000 | 100..=60_000 |

### Presets

| Preset | allow_exp | allow_dep | timeout | Use case |
|--------|-----------|-----------|---------|----------|
| `Strict` | false | false | 5_000 | Production |
| `Lenient` | false | true | 10_000 | Rolling upgrade |
| `Experimental` | true | true | 15_000 | Dev / staging |

---

## 8. Observability

### Counters

| Counter | Type | Metric name |
|---------|------|-------------|
| `negotiations_total` | counter | `vn_negotiations_total` |
| `negotiations_succeeded` | counter | `vn_negotiations_succeeded_total` |
| `negotiations_failed` | counter | `vn_negotiations_failed_total` |
| `fallbacks` | counter | `vn_fallbacks_total` |
| `deprecated_warnings` | counter | `vn_deprecated_warnings_total` |
| `experimental_allowed` | counter | `vn_experimental_allowed_total` |

### Derived Rates

- `success_rate_bps()` — `succeeded / total * 10_000` (default 10_000)
- `fallback_rate_bps()` — `fallbacks / total * 10_000`

### Health Thresholds

| Signal | Healthy | Degraded | Critical |
|--------|---------|----------|----------|
| Success rate | ≥ 9900 bps | 9000–9899 | < 9000 |
| Fallback rate | ≤ 500 bps | 500–2000 | > 2000 |

Health uses a "worst signal wins" combiner.

---

## 9. Failure Modes

| Failure | Detection | Recovery |
|---------|-----------|----------|
| No common version | `NoOverlap` | Upgrade peer or broaden range |
| Malformed raw | `InvalidVersion` | Reject + log |
| Deprecated match | `Deprecated` variant | Warn + continue (if allowed) |
| Matrix mismatch | `IncompatibleVersions` | Hard reject |
| Timeout (Phase 1+) | `negotiation_timeout_ms` | Retry with backoff |

---

## 10. Performance Characteristics

| Operation | Complexity | Expected latency |
|-----------|------------|------------------|
| `ProtocolVersion::new` | O(1) | <1 ns |
| `VersionRange::contains` | O(1) | <1 ns |
| `CompatibilityMatrix::check` | O(n), n≤64 | <50 ns |
| `VersionNegotiator::negotiate` (Phase 1) | O(n*m) | <1 µs |

All types are `Copy`; no heap allocation on any Phase 0 path.

---

## 11. API Reference

### Core Types
- `ProtocolVersion` — semver-encoded version newtype
- `VersionRange` — closed interval of versions
- `CompatEntry` — (local, remote, compatible) triple
- `CompatibilityMatrix` — fixed-size array of entries

### Config
- `VersionNegotiationConfig` — declarative configuration
- `Preset` — deployment archetype enum

### Observability
- `VersionNegotiationStats` — atomic counters
- `StatsSnapshot` — point-in-time snapshot
- `HealthReport` / `HealthState` — health summary

### Builder
- `VersionNegotiationBuilder` — fluent config builder

### Facades (Phase 1)
- `VersionNegotiator` — negotiation engine
- `AdvertisedVersions` — version list manager

---

## 12. Testing Strategy

### Unit Tests (Phase 0)

- Version encoding / decoding round-trip
- Range containment and overlap
- Matrix add / check / clear
- Preset → config validation
- Stats recording and snapshot consistency
- Health derivation thresholds

### Integration Tests (Phase 1+)

- Full negotiation cycle with matrix
- Fallback behavior
- Timeout handling
- Concurrent negotiations

### Property Tests (Phase 2+)

- `VersionRange::contains` is monotonic
- `CompatibilityMatrix::check` is symmetric for compatible entries
- `overlaps` is commutative

---

## 13. Operational Runbook

### Symptom: High negotiation failure rate

1. Check `vn_negotiations_failed_total` trend.
2. Inspect `VersionNegotiationError` variant distribution.
3. If `NoOverlap` dominates: peer version is too old/new — upgrade
   or broaden `min_supported` / `max_supported`.
4. If `IncompatibleVersions` dominates: check matrix entries for
   stale incompatibility records.

### Symptom: High fallback rate

1. Fallback means negotiation succeeded but fell to lowest version.
2. Indicates peers are advertising only old versions.
3. Check if a rolling upgrade is in progress.

---

## 14. Integration Points

| Consumer | How it uses version-negotiation |
|----------|-------------------------------|
| `rpc-framework` | Selects RPC protocol version per connection |
| `media-streaming` | Negotiates codec / track version |
| `ai-streaming` | Negotiates streaming protocol version |
| `quic-engine` (transport-layer) | Provides ALPN strings derived from versions |

---

## 15. Future Directions

- **Perfect hash matrix**: for >64 entries, use a compile-time perfect
  hash to get O(1) lookup.
- **Version weighted selection**: prefer newer versions when multiple
  are available (load-aware).
- **Capability flags**: extend `ProtocolVersion` with a bitmask of
  optional features.
- **Rolling upgrade planner**: given current and target versions,
  compute a safe upgrade sequence across the cluster.
- **QUIC ALPN integration**: map `ProtocolVersion` to ALPN strings
  for transport-level negotiation.

---

## 16. Security Considerations

- Version negotiation is not a security mechanism; it is advisory.
- An attacker can advertise any version; authentication happens
  separately in `auth-framework`.
- `allow_experimental = true` should NEVER be set in production —
  experimental versions may have unstable semantics.

---

## 17. References

- [QUIC ALPN](https://datatracker.ietf.org/doc/html/rfc7301)
- [Semantic Versioning 2.0.0](https://semver.org/)
- gRPC version negotiation
- HTTP/2 SETTINGS frame versioning
