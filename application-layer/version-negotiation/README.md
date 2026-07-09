# version-negotiation

Protocol version negotiation for the Vireon application layer.

## Overview

Provides semver-encoded `ProtocolVersion`, `VersionRange` for
expressing supported version intervals, and `CompatibilityMatrix`
for recording known (in)compatible version pairs.

## Layer Map

```
ProtocolVersion ──► VersionRange ──► CompatibilityMatrix ──► VersionNegotiator
     (u32)            (min,max)          (entries[64])          (Phase 1)
```

## Quick Start

```rust
use version_negotiation::{ProtocolVersion, VersionRange, CompatibilityMatrix};

let local = VersionRange::new(
    ProtocolVersion::new(1, 0, 0),
    ProtocolVersion::new(2, 0, 0),
).unwrap();

let remote = ProtocolVersion::new(1, 5, 3);
assert!(local.contains(remote));
```

## Presets

| Preset | Experimental | Deprecated | Timeout | Use Case |
|--------|-------------|------------|---------|----------|
| `Strict` | No | No | 5s | Production |
| `Lenient` | No | Yes | 10s | Rolling upgrade |
| `Experimental` | Yes | Yes | 15s | Dev / staging |

## Phase 0 Status

- **Real-impl**: ProtocolVersion, VersionRange, CompatibilityMatrix,
  Config/Stats/Health/Builder
- **Skeletons**: VersionNegotiator, AdvertisedVersions (return
  `ShuttingDown` errors)
- **Tests**: 80 unit tests
- **Benches**: 4 Criterion stubs

## Version Encoding

```
Major (16 bits) | Minor (8 bits) | Patch (8 bits)
```

- `patch == 255` → deprecated sentinel
- `major == 0` → experimental

See `DESIGN.md` for the full specification.
