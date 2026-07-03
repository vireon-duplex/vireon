# `frame`

Application-layer wire framing for `FluxIO` QUIC transport. A fixed
**40-byte header** carries everything a demultiplexer needs — version, type,
flags, priority, session, stream, sequence, timestamp — before a
variable-length payload, with zero-copy encode / decode paths built on
[`ByteReader`](src/byte_reader.rs) / [`ByteWriter`](src/byte_writer.rs).

## Wire format

All multi-byte integers are **big-endian** (network byte order).

```text
 offset  size  field
      0     1  version      (u8,  see Version)
      1     1  msg_type     (u8,  see MessageType)
      2     1  flags        (u8,  see FrameFlags)
      3     1  priority     (u8,  see Priority)
      4     8  session_id   (u64, see SessionId)
     12     8  stream_id    (u64, see StreamId)
     20     8  seq          (u64, see Seq)
     28     8  timestamp    (u64, nanoseconds since epoch)
     36     4  payload_len  (u32)
     40   ...  payload      (payload_len bytes)
```

`HEADER_SIZE = 40`. The leading four single-byte fields form a control
quadlet a receiver can inspect to dispatch (type), gate processing
(priority / flags) and validate protocol compatibility (version) before
touching the larger 64-bit identifiers.

### Header fields

| Field | Size | Purpose |
|---|---|---|
| `version` | 1 B | Wire protocol version (currently `V1`); mismatches are fatal |
| `msg_type` | 1 B | Payload discriminator (see [Message types](#message-types)) |
| `flags` | 1 B | Per-frame bits (`FIN`, `COMPRESSED`, `FRAGMENT`, `URGENT`, `ACK_REQ`) |
| `priority` | 1 B | Scheduling priority — higher value means more important |
| `session_id` | 8 B | Security / lifetime group (one QUIC connection, one user, one tenant) |
| `stream_id` | 8 B | Logical stream within the session (app-layer, decoupled from QUIC stream) |
| `seq` | 8 B | Per-stream sequence number — gap detection, reordering, reassembly |
| `timestamp` | 8 B | Sender-side clock sample (ns since epoch) for jitter / RTT |
| `payload_len` | 4 B | Payload byte count (max ~4 GiB) |

### Why `seq`?

QUIC guarantees in-order delivery **within a single stream**. But when a
connection multiplexes logical streams over QUIC datagrams, or pools several
logical streams onto fewer transport streams, the receiver needs an
application-layer sequence number to:

1. **Detect gaps** — `seq` jumps signal lost frames.
2. **Reorder** — buffer out-of-order frames and emit them in `seq` order.
3. **Reassemble** — `Fragment` chains ordered by `seq`.

`Seq::next` uses `wrapping_add` (~584 years at 1 M frames/s per stream before
wrap); `Seq::gap_from` measures distance from an earlier seq, saturating at 0
when `earlier > self`.

### Flags

| Bit | Name | Meaning |
|---|---|---|
| 0 | `FIN` | Final frame of a logical stream |
| 1 | `COMPRESSED` | Payload is compressed (app decompresses) |
| 2 | `FRAGMENT` | Part of a multi-frame message |
| 3 | `URGENT` | High-priority, process before queued data |
| 4 | `ACK_REQ` | Sender requests an app-level `Ack` |
| 5–7 | reserved | Future use |

`FrameFlags` is a `u8` newtype supporting `|`, `|=`, `&`, `contains`, `insert`,
`remove` — no `bitflags` crate dependency.

## Message types

Values `0x01`–`0x09` are **data-plane** (opaque application payload);
`0x0A`–`0x0F` are **control-plane** that the frame layer itself understands.

| Byte | Variant | Plane |
|---|---|---|
| 0x01 | `TextToken` | data |
| 0x02 | `AudioFrame` | data |
| 0x03 | `VideoFrame` | data |
| 0x04 | `ImageFrame` | data |
| 0x05 | `ToolCall` | data |
| 0x06 | `ToolResult` | data |
| 0x07 | `StateUpdate` | data |
| 0x08 | `Error` | data |
| 0x09 | `Heartbeat` | data |
| 0x0A | `StreamOpen` | control |
| 0x0B | `StreamClose` | control |
| 0x0C | `StreamReset` | control |
| 0x0D | `Handshake` | control |
| 0x0E | `Fragment` | control |
| 0x0F | `Ack` | control |

Unknown bytes surface as `FrameError::UnknownMessageType` rather than silent
corruption; `0x00` and `0x10`–`0xFF` are reserved.

## API

The primary codec lives in the [`frame`](src/frame.rs) and
[`header`](src/header.rs) modules. All multi-byte I/O goes through
[`ByteReader`](src/byte_reader.rs) / [`ByteWriter`](src/byte_writer.rs) —
unaligned pointer reads/writes with `debug_assert` bounds checks, so release
builds carry no per-field bounds overhead.

### Build + encode + decode

```rust
use frame::frame::{Frame, decode};
use frame::header::{
    FrameFlags, MessageType, Priority, Seq, SessionId, StreamId, Timestamp,
};

let frame = Frame::new(
    SessionId::new(1),
    StreamId::new(7),
    MessageType::AudioFrame,
    b"hello frame world".as_ref(),
)
.with_flags(FrameFlags::COMPRESSED)
.with_priority(Priority::HIGH)
.with_seq(Seq::new(100))
.with_timestamp(Timestamp::new(5_000_000));

let mut wire = Vec::new();
frame.encode(&mut wire)?;            // header.payload_len reconciled automatically

let decoded = decode(&wire)?;        // FrameRef borrows `wire` — zero alloc
assert_eq!(decoded.payload, b"hello frame world");
let owned = decoded.into_owned();    // one copy; cheap to clone thereafter
```

### Zero-copy encode into a caller buffer

`encode_into` writes into a `ByteWriter` and never allocates — the hot path
for a connection-scoped send buffer:

```rust
use frame::byte_writer::ByteWriter;
use frame::frame::encode_into;
use frame::header::{FrameHeader, MessageType, SessionId, StreamId, HEADER_SIZE};

let payload = b"hello";
let mut buf = vec![0u8; HEADER_SIZE + payload.len()];
let mut w = ByteWriter::new(&mut buf);
encode_into(
    &FrameHeader::new(SessionId::new(1), StreamId::new(2), MessageType::TextToken),
    payload,
    &mut w,
)?;
```

### Header-only

When only the fixed header matters (dispatching, logging before payload is
touched), use `FrameHeader::encode` / `FrameHeader::decode` directly:

```rust
use frame::header::{FrameHeader, MessageType, Seq, SessionId, StreamId, HEADER_SIZE};

let h = FrameHeader::new(SessionId::new(1), StreamId::new(2), MessageType::TextToken)
    .with_seq(Seq::new(42));

let mut buf = [0u8; HEADER_SIZE];
h.encode(&mut buf)?;
let back = FrameHeader::decode(&buf)?;
assert_eq!(h, back);
```

For whole-frame (header + payload) helpers that return a `(FrameHeader, &[u8])`
tuple, see `encode_frame` / `decode_frame` in the [`header`](src/header.rs)
module.

## Zero-copy strategy

| Operation | Copy? |
|---|---|
| `decode` / `decode_from` | none — `FrameRef` borrows the input |
| `FrameRef::into_owned` | one — payload copied into ref-counted `Bytes` |
| `Frame::clone` | none — refcount bump |
| `Frame::encode_into` / `encode_into` | one — payload copied into the output buffer |
| `Frame::encode` | one — same, via a `Vec` growth path |

## Streaming decode (`codec` module)

The [`codec`](src/codec.rs) module provides an incremental `FrameDecoder` for a
byte stream that may deliver partial frames or multiple frames per read (e.g. a
QUIC `stream_recv` loop). It currently uses a compact **22-byte** header
(`stream_id + seq + msg_type + flags + payload_len`) and is the legacy layer;
the 40-byte format above is the target.

```rust
use frame::codec::{FrameDecoder, FrameHeader, FrameFlags};
use frame::header::{MessageType, Seq, StreamId};

let mut dec = FrameDecoder::new();
dec.push(&raw[..n]);                          // feed QUIC bytes
while let Some(frame) = dec.decode_frame()? { // drain complete frames
    // frame.seq gives per-stream ordering
}
```

`FrameDecoder` also exposes a zero-copy receive path: write QUIC bytes straight
into `spare_capacity_mut(hint)`, then `unsafe { commit(n) }`. A misbehaving peer
is bounded by `max_payload_len` (default 16 MiB).

## Public types

| Type | Module | Purpose |
|---|---|---|
| `Version` | `header` | Wire protocol version (`V1`, `CURRENT`) |
| `MessageType` | `header` | 15-variant discriminator (`u8`-backed, `is_data`/`is_control`) |
| `FrameFlags` | `header` | Bitflags newtype (`FIN \| URGENT`, `contains()`, …) |
| `Priority` | `header` | `MIN` / `LOW` / `NORMAL` / `HIGH` / `MAX` |
| `SessionId` | `header` | 64-bit session identifier |
| `StreamId` | `header` | 64-bit logical stream identifier |
| `Seq` | `header` | Per-stream sequence (`next()`, `gap_from()`) |
| `Timestamp` | `header` | Sender clock sample (`now()`, `from_millis`/`_micros`/`_nanos`) |
| `FrameHeader` | `header` | `Copy` bundle of all 40-byte header fields |
| `FrameRef<'a>` | `frame` | Borrowed decoded frame (zero-copy) |
| `Frame` | `frame` | Owned frame (`Bytes` payload, cheap clone, builder API) |
| `ByteReader` | `byte_reader` | Unchecked big-endian read cursor |
| `ByteWriter` | `byte_writer` | Unchecked big-endian write cursor |
| `FrameError` | `error` | Typed errors (`thiserror`) |
| `FrameDecoder` / `FrameEncoder` | `codec` | Legacy 22-byte streaming codec |

## Module layout

```text
src/
├── lib.rs         module declarations
├── header.rs      40-byte header: value types + FrameHeader + encode_frame / decode_frame
├── frame.rs       FrameRef (borrowed), Frame (owned) + whole-frame encode / decode
├── codec.rs       22-byte streaming codec: FrameDecoder, FrameEncoder (legacy)
├── byte_reader.rs unchecked big-endian read cursor
├── byte_writer.rs unchecked big-endian write cursor
└── error.rs       FrameError
```

## Status

A `buffer-pool` crate is planned but not yet wired up; sites that will consume
it are marked `// TODO(buffer-pool)` in the source. Until then, the `Vec`-backed
`encode` paths grow their own buffers.

## Quality gates

`cargo fmt`, `cargo clippy -p frame --lib --tests -- -D warnings`,
`cargo test -p frame`.
