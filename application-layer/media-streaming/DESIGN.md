# Media Streaming Design Specification

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Track Model](#3-track-model)
4. [Codec Negotiation](#4-codec-negotiation)
5. [Adaptive Bitrate](#5-adaptive-bitrate)
6. [Simulcast](#6-simulcast)
7. [Session Management](#7-session-management)
8. [Error Handling](#8-error-handling)
9. [Observability](#9-observability)
10. [Configuration](#10-configuration)
11. [Health Monitoring](#11-health-monitoring)
12. [Performance](#12-performance)
13. [Security](#13-security)
14. [Testing](#14-testing)

---

## 1. Overview

The Media Streaming system provides comprehensive audio/video/screen track management with adaptive bitrate (ABR), codec negotiation, and simulcast layer management for the Vireon QUIC runtime.

### 1.1 Key Features

- **Multi-track support**: Audio, video, screen, and data tracks
- **Adaptive bitrate**: Dynamic quality adjustment based on network conditions
- **Simulcast**: Multiple spatial/temporal layers per track
- **Codec negotiation**: Opus, H.264, H.265, VP8, VP9, AV1, PCMU, PCMA
- **Real-time statistics**: Packet counters, bitrate tracking, RTT monitoring
- **Health monitoring**: Packet loss rate, frame drops, QoS metrics

### 1.2 Design Goals

1. **Low latency**: Minimize end-to-end latency for real-time communication
2. **High quality**: Maximize video/audio quality within bandwidth constraints
3. **Robustness**: Gracefully handle network congestion and packet loss
4. **Scalability**: Support hundreds of concurrent sessions per node
5. **Observability**: Comprehensive metrics for operational excellence

---

## 2. Architecture

### 2.1 Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    MediaStreaming                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐   │
│  │ MediaSession│─→│ MediaTrack   │─→│ CodecConfig     │   │
│  └─────────────┘  └──────────────┘  └─────────────────┘   │
│         │                  │                  │               │
│         v                  v                  v               │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐   │
│  │ ABRController│─→│ BitrateLadder│─→│ SimulcastLayer  │   │
│  └─────────────┘  └──────────────┘  └─────────────────┘   │
│         │                  │                                   │
│         v                  v                                   │
│  ┌─────────────────────────────────────────────────┐        │
│  │              MediaStreamingStats                 │        │
│  └─────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

1. **Track Creation**: Application requests track with kind/direction
2. **Codec Negotiation**: System negotiates compatible codecs
3. **Bitrate Selection**: ABR selects initial bitrate based on bandwidth
4. **Media Encoding**: Media encoded at selected bitrate
5. **RTP Packetization**: Encoded frames packetized into RTP
6. **Transmission**: RTP packets sent over QUIC streams
7. **Feedback**: RTCP feedback informs ABR decisions

### 2.3 Layer Map

| Module | Phase | Responsibility |
|--------|-------|----------------|
| error | 0 | `MediaError` thiserror enum |
| config | 0 | `MediaStreamingConfig`, `Preset` |
| track | 0 | `MediaTrackId`, `TrackKind`, `TrackDirection` |
| codec | 0 | `CodecType`, `CodecConfig` |
| bitrate | 0 | `BitrateLadder`, `AdaptiveBitrateConfig` |
| stats | 0 | `MediaStreamingStats`, `StatsSnapshot` |
| health | 0 | `HealthReport`, `HealthState` |
| builder | 0 | `MediaStreamingBuilder` (fluent) |
| session | 1 | `MediaSession` facade |
| abr | 1 | `AdaptiveBitrateController` |

---

## 3. Track Model

### 3.1 Track Identity

Each track has a unique `MediaTrackId` (u64) scoped to a session. NIL (0) is reserved.

```rust
let id = MediaTrackId::new(42);
assert!(!id.is_nil());
assert_eq!(id.raw(), 42);
```

### 3.2 Track Kinds

- **Audio**: Opus (48 kHz), PCMU/PCMA (8 kHz)
- **Video**: H.264, H.265, VP8, VP9, AV1
- **Screen**: High-bitrate video typically H.264 or VP9
- **Data**: Application-specific data channels

### 3.3 Track Direction

- **SendRecv**: Bidirectional (default)
- **SendOnly**: Publish only
- **RecvOnly**: Subscribe only
- **Inactive**: Neither send nor receive

### 3.4 Track Lifecycle

```
Created → Negotiating → Negotiated → Active → Closing → Closed
                        ↓                      ↓
                    Failed                Removed
```

---

## 4. Codec Negotiation

### 4.1 Supported Codecs

| Codec | Type | Clock Rate | Channels | RTCP FB |
|-------|------|------------|----------|---------|
| Opus  | Audio | 48 kHz | 1-2 | No |
| PCMU  | Audio | 8 kHz | 1 | No |
| PCMA  | Audio | 8 kHz | 1 | No |
| H.264 | Video | 90 kHz | N/A | Yes |
| H.265 | Video | 90 kHz | N/A | Yes |
| VP8   | Video | 90 kHz | N/A | Yes |
| VP9   | Video | 90 kHz | N/A | Yes |
| AV1   | Video | 90 kHz | N/A | Yes |

### 4.2 Codec Configuration

```rust
let audio = CodecConfig::for_audio(111, 48_000, 2); // Opus stereo
let video = CodecConfig::for_video(96, 90_000);    // H.264
```

### 4.3 Negotiation Process

1. Exchange codec capabilities via SDP
2. Find intersection of supported codecs
3. Select preferred codec per track kind
4. Assign payload types
5. Configure codec parameters

---

## 5. Adaptive Bitrate

### 5.1 Bitrate Ladder

The `BitrateLadder` stores simulcast layers sorted by bitrate:

```
Layer 0: 3000 Kbps (1080p60)
Layer 1: 1500 Kbps (720p30)
Layer 2:  750 Kbps (480p30)
Layer 3:  500 Kbps (360p15)
```

### 5.2 ABR Algorithm

The controller uses:

1. **RTT**: Round-trip time for congestion detection
2. **Loss rate**: Packet loss for quality assessment
3. **Bandwidth estimate**: From transport layer
4. **Buffer health**: Encoder buffer occupancy

### 5.3 Adjustment Logic

```rust
if available_bandwidth > current_bitrate + upgrade_threshold {
    upgrade_to_next_layer();
} else if available_bandwidth < current_bitrate - downgrade_threshold {
    downgrade_to_next_layer();
}
```

### 5.4 Hysteresis

To prevent oscillation:

- Upgrade threshold: +500 Kbps above current
- Downgrade threshold: -1000 Kbps below current

---

## 6. Simulcast

### 6.1 Spatial Layers

Multiple resolutions encoded simultaneously:

- Layer 0: 1080p (1920x1080)
- Layer 1: 720p (1280x720)
- Layer 2: 480p (854x480)
- Layer 3: 360p (640x360)

### 6.2 Temporal Layers

Multiple frame rates per resolution:

- T0: 60 fps
- T1: 30 fps
- T2: 15 fps

### 6.3 Layer Selection

Receivers select layers based on:

- Available bandwidth
- Display resolution
- CPU capabilities
- Battery level (mobile)

---

## 7. Session Management

### 7.1 MediaSession

The `MediaSession` manages:

- Track registry
- Codec state
- ABR state
- Statistics

### 7.2 Session Lifecycle

```
Created → Configured → Active → Draining → Shutdown
```

### 7.3 Track Limits

Per-session track limits:

- Voice only: 2 tracks (1 audio in, 1 audio out)
- Video call: 4 tracks (audio + video)
- Conference: 16 tracks (multi-party)
- Screen share: 4 tracks (high bitrate)
- Broadcasting: 8 tracks (simulcast)

---

## 8. Error Handling

### 8.1 Error Categories

| Category | Variants | Action |
|----------|----------|--------|
| Fatal | `InvalidSdp`, `ShuttingDown` | Abort |
| Retryable | `BufferUnderrun`, `BitrateUnavailable` | Retry |
| Capacity | `BandwidthExceeded`, `SessionClosed` | Shed |

### 8.2 Error Handling Strategy

```rust
match result {
    Err(e) if e.is_fatal() => {
        // Log and fail fast
        error!("Fatal error: {}", e);
        return Err(e);
    }
    Err(e) if e.is_retryable() => {
        // Back off and retry
        warn!("Retryable error: {}", e);
        retry_with_backoff();
    }
    Err(e) if e.is_capacity() => {
        // Shed load
        warn!("Capacity error: {}", e);
        shed_load();
    }
    Ok(_) => success(),
}
```

---

## 9. Observability

### 9.1 Metrics

Counters:
- `tracks_active`: Gauge of active tracks
- `packets_sent`: Counter of sent packets
- `packets_received`: Counter of received packets
- `bytes_sent`: Counter of sent bytes
- `bytes_received`: Counter of received bytes
- `bitrate_adjustments`: Counter of ABR changes
- `layer_switches`: Counter of layer switches
- `frame_drops`: Counter of dropped frames

Histograms:
- `rtt_ns`: RTT samples in nanoseconds

### 9.2 Health Metrics

- Packet loss rate (basis points)
- Frame drops per minute
- Active track count
- Average bitrate

---

## 10. Configuration

### 10.1 Presets

| Preset | Tracks | Bitrate (min/max) | ABR Interval | Simulcast |
|--------|--------|-------------------|---------------|-----------|
| VoiceOnly | 2 | 8k / 128k | 1000ms | No |
| VideoCall | 4 | 300k / 5M | 500ms | Yes |
| Conference | 16 | 500k / 8M | 300ms | Yes |
| ScreenShare | 4 | 1M / 20M | 1000ms | No |
| Broadcasting | 8 | 2M / 50M | 200ms | Yes |

### 10.2 Tunable Parameters

- `max_tracks_per_session`: 1-32 (u8)
- `min_bitrate_bps`: 8k - 50M (u32)
- `max_bitrate_bps`: 8k - 50M (u32)
- `default_bitrate_bps`: between min/max (u32)
- `abr_interval_ms`: 100-10000 (u16)
- `enable_simulcast`: bool

---

## 11. Health Monitoring

### 11.1 Health States

- **Healthy**: Packet loss < 5%, frame drops < 10/min
- **Degraded**: Packet loss 5-20% or frame drops 10-60/min
- **Critical**: Packet loss ≥ 20% or frame drops ≥ 60/min

### 11.2 Thresholds

```rust
const PACKET_LOSS_DEGRADED_BPS: u16 = 500;   // 5%
const PACKET_LOSS_CRITICAL_BPS: u16 = 2000;  // 20%
const FRAME_DROPS_DEGRADED_PER_MIN: u64 = 10;
const FRAME_DROPS_CRITICAL_PER_MIN: u64 = 60;
```

---

## 12. Performance

### 12.1 Target Metrics

- End-to-end latency: < 100ms (audio), < 150ms (video)
- Packet loss: < 1% (LAN), < 5% (WAN)
- Bitrate accuracy: ±10% of target
- ABR convergence: < 5 seconds
- Session capacity: > 1000 per node

### 12.2 Optimization Strategies

1. Lock-free atomic counters for stats
2. Relaxed memory ordering for advisory metrics
3. Bounded arrays for bitrate ladder (no heap alloc)
4. Stack-allocated snapshots for metrics export

---

## 13. Security

### 13.1 Threat Model

- Malicious SDP injection
- Resource exhaustion (too many tracks)
- Bitrate manipulation attacks
- Codec downgrade attacks

### 13.2 Mitigations

1. **Input validation**: All SDP must be validated
2. **Rate limiting**: Track creation rate-limited per tenant
3. **Capacity limits**: Hard caps on tracks and bitrate
4. **Codec whitelist**: Only supported codecs allowed

---

## 14. Testing

### 14.1 Unit Tests

- Track ID lifecycle (new, nil, is_nil, raw)
- Codec type predicates (is_audio, is_video)
- Bitrate ladder operations (add, best_for_bandwidth)
- Stats recording and snapshots
- Health report derivation

### 14.2 Integration Tests

- Session lifecycle (create, add tracks, shutdown)
- ABR adjustment under bandwidth changes
- Simulcast layer switching
- Multi-track scenarios

### 14.3 Benchmarks

- Bitrate adjustment: 100ns
- Track addition: 50ns
- RTP packetization: 20ns
- Stats snapshot: 100ns
- Layer switch: 50ns

---

## Appendix A: Configuration Examples

### Voice-Only Configuration

```rust
let cfg = Preset::VoiceOnly.to_config();
// max_tracks: 2
// bitrate: 8k - 128k
// simulcast: disabled
```

### Video Call Configuration

```rust
let cfg = Preset::VideoCall.to_config();
// max_tracks: 4
// bitrate: 300k - 5M
// simulcast: enabled
```

### Conference Configuration

```rust
let cfg = Preset::Conference.to_config();
// max_tracks: 16
// bitrate: 500k - 8M
// simulcast: enabled
```

---

## Appendix B: ABR Algorithms

### B.1 Gradient-Based ABR

Adjust bitrate proportional to bandwidth gradient:

```
rate(t+1) = rate(t) + α * (bandwidth - rate(t))
```

Where α = 0.1 is the gain factor.

### B.2 Buffer-Based ABR

Adjust based on encoder buffer occupancy:

```
if buffer_level < low_threshold:
    decrease_bitrate()
elif buffer_level > high_threshold:
    increase_bitrate()
```

---

## Appendix C: Codec Parameters

### C.1 Opus Parameters

- Frame sizes: 2.5ms, 5ms, 10ms, 20ms, 60ms
- Bitrates: 6-510 kbps
- Complexity: 0-10

### C.2 H.264 Parameters

- Profile: Baseline, Main, High
- Level: 1-5.2
- Packetization mode: 0, 1, 2

---

## Appendix D: Future Enhancements

### Phase 1

- Full session management
- Functional ABR controller
- RTCP feedback processing
- SDP offer/answer generation

### Phase 2

- SVC (Scalable Video Coding)
- Multi-codec simulcast
- Hardware encoder integration
- WebRTC compatibility layer

### Phase 3

- AI-enhanced ABR
- Content-aware encoding
- FEC (Forward Error Correction)
- Jitter buffer management
