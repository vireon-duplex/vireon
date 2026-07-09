# media-streaming

Media streaming: audio/video/screen tracks with adaptive bitrate, codec negotiation, and simulcast layer management.

## Overview

The **media-streaming** crate provides comprehensive audio/video/screen track management for the Vireon QUIC runtime. It supports:

- **Multi-track streaming**: Audio, video, screen sharing, and data tracks
- **Adaptive bitrate (ABR)**: Dynamic quality adjustment based on network conditions
- **Simulcast support**: Multiple spatial/temporal layers per track
- **Codec negotiation**: Opus, H.264, H.265, VP8, VP9, AV1, PCMU, PCMA
- **Real-time statistics**: Packet counters, bitrate tracking, RTT monitoring
- **Health monitoring**: Packet loss rate, frame drops, QoS metrics

## Architecture

```
MediaTrack ──► [MediaStreaming] ──► RTP Stream
                  │
                  ├─ MediaSession   (track container)
                  ├─ CodecNegotiator (codec compatibility)
                  ├─ BitrateLadder  (ABR layers)
                  ├─ AdaptiveBitrateController (dynamic adjustment)
                  └─ SimulcastManager (spatial/temporal layers)
```

## Key Components

### Track Management

- [`MediaTrackId`]: Unique track identifier (NIL sentinel)
- [`TrackKind`]: Audio, Video, Screen, Data
- [`TrackDirection`]: SendRecv, SendOnly, RecvOnly, Inactive

### Codec Support

- [`CodecType`]: Opus, H.264, H.265, VP8, VP9, AV1, PCMU, PCMA
- [`CodecConfig`]: Payload type, clock rate, channels

### Adaptive Bitrate

- [`BitrateLadder`]: Sorted layers (layer_id → bitrate_bps)
- [`AdaptiveBitrateConfig`]: ABR thresholds and parameters
- [`AdaptiveBitrateController`]: Dynamic quality adjustment

### Observability

- [`MediaStreamingStats`]: Atomic counters (packets, bytes, ABR events)
- [`StatsSnapshot`]: Point-in-time snapshot with precomputed rates
- [`HealthReport`]: Derived health state (Healthy/Degraded/Critical)

## Configuration

### Presets

The crate provides five presets for common scenarios:

| Preset | Tracks | Bitrate (min/max) | ABR Interval | Simulcast |
|--------|--------|-------------------|---------------|-----------|
| `VoiceOnly` | 2 | 8k / 128k | 1000ms | No |
| `VideoCall` | 4 | 300k / 5M | 500ms | Yes |
| `Conference` | 16 | 500k / 8M | 300ms | Yes |
| `ScreenShare` | 4 | 1M / 20M | 1000ms | No |
| `Broadcasting` | 8 | 2M / 50M | 200ms | Yes |

### Example: Using Presets

```rust
use media_streaming::{Preset, MediaStreamingConfig};

// Use video call preset
let cfg = Preset::VideoCall.to_config();

// Or use the builder
let cfg = MediaStreamingBuilder::from_preset(Preset::Conference)
    .max_tracks(8)
    .simulcast(true)
    .build()?;
```

## Usage Examples

### Creating a Media Session

```rust
use media_streaming::{MediaSession, Preset, TrackKind, TrackDirection};

let cfg = Preset::VideoCall.to_config();
let mut session = MediaSession::new(cfg);

// Add tracks (Phase 1 implementation)
session.add_track(track_id, TrackKind::Audio, TrackDirection::SendRecv)?;
session.add_track(track_id, TrackKind::Video, TrackDirection::SendRecv)?;

// Negotiate codecs
session.negotiate()?;

// Check health
let health = session.health();
if health.needs_attention() {
    warn!("Media session needs attention: {:?}", health.state);
}
```

### Using the Bitrate Ladder

```rust
use media_streaming::BitrateLadder;

let mut ladder = BitrateLadder::new();
ladder.add_layer(0, 3_000_000)?;  // 1080p
ladder.add_layer(1, 1_500_000)?;  // 720p
ladder.add_layer(2, 750_000)?;    // 480p

// Find best layer for bandwidth
if let Some(layer) = ladder.best_for_bandwidth(2_000_000) {
    println!("Best layer: {}", layer); // Layer 1 (720p)
}
```

### Recording Statistics

```rust
use media_streaming::MediaStreamingStats;

let stats = MediaStreamingStats::new();

// Record media events
stats.record_track_created();
stats.record_packet_sent(1200);
stats.record_packet_received(1200);
stats.record_rtt(50_000_000); // 50ms RTT

// Take snapshot
let snap = stats.snapshot();
println!("Packet loss: {} bps", snap.packet_loss_rate_bps());
println!("Avg RTT: {} ms", snap.avg_rtt_ms());
```

### Configuring ABR

```rust
use media_streaming::AdaptiveBitrateConfig;

// Use preset
let cfg = AdaptiveBitrateConfig::video_conference();

// Or configure manually
let cfg = AdaptiveBitrateConfig::new(
    300_000,   // min bitrate
    5_000_000, // max bitrate
    1_500_000, // starting bitrate
    500_000,   // upgrade threshold
    1_000_000, // downgrade threshold
)?;
```

## Error Handling

The crate uses typed errors via [`MediaError`]:

```rust
use media_streaming::MediaError;

match result {
    Err(e) if e.is_fatal() => {
        // Don't retry: InvalidSdp, ShuttingDown
        error!("Fatal error: {}", e);
    }
    Err(e) if e.is_retryable() => {
        // Back off and retry: BufferUnderrun, BitrateUnavailable
        warn!("Retryable error: {}", e);
        retry_with_backoff();
    }
    Err(e) if e.is_capacity() => {
        // Shed load: BandwidthExceeded, SessionClosed
        warn!("Capacity error: {}", e);
        shed_load();
    }
    Ok(_) => success(),
}
```

## Health Monitoring

Health is classified into three states:

| State | Meaning | Packet Loss | Frame Drops | Action |
|-------|---------|-------------|-------------|--------|
| Healthy | Nominal | < 5% | < 10/min | None |
| Degraded | Investigate | 5-20% | 10-60/min | Monitor |
| Critical | Page on-call | ≥ 20% | ≥ 60/min | Investigate immediately |

```rust
use media_streaming::HealthReport;

let report = HealthReport::derive(&stats_snapshot);
match report.state {
    HealthState::Healthy => info!("System healthy"),
    HealthState::Degraded => warn!("System degraded: investigate"),
    HealthState::Critical => error!("System critical: page on-call"),
}
```

## Performance

The crate is optimized for low-latency real-time streaming:

- **Lock-free**: All stats use atomic counters with relaxed ordering
- **Zero-allocation**: Bitrate ladder uses fixed-size arrays
- **Stack snapshots**: Stats snapshots are Copy types
- **No panics**: All operations return Result

Target performance:
- Bitrate adjustment: ~100ns
- Track addition: ~50ns
- RTP packetization: ~20ns
- Stats snapshot: ~100ns
- Layer switch: ~50ns

## Phase Status

**Phase 0** (Current): Core types and statistics
- ✅ Error types
- ✅ Configuration with presets
- ✅ Track identity and metadata
- ✅ Codec types and config
- ✅ Bitrate ladder and ABR config
- ✅ Statistics and snapshots
- ✅ Health monitoring
- ✅ Fluent builder
- 🔄 Session skeleton
- 🔄 ABR controller skeleton

**Phase 1** (Next): Functional streaming
- Session lifecycle management
- Codec negotiation
- ABR control logic
- Simulcast management

**Phase 2**: Advanced features
- SVC (Scalable Video Coding)
- Hardware encoder integration
- WebRTC compatibility

## Dependencies

- `configuration-system`: Hierarchical config
- `middleware-pipeline`: Request/response processing
- `resource-quotas`: Per-tenant limits
- `session-manager`: Session lifecycle
- `stream-manager`: Stream management
- `thiserror`: Error derive macros
- `tracing`: Structured logging

## License

MIT

## Contributing

See main Vireon contributing guidelines.

---

**Note**: This is Phase 0 of the media-streaming crate. Full streaming functionality will be implemented in Phase 1+.
