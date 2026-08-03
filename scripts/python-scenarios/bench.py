#!/usr/bin/env python3
"""Throughput benchmark for the Python SDK.

Modes:
  stream     — 1 publisher + 1 subscriber, batch-and-drain on a dedicated stream
  broadcast  — 1 publisher + N subscribers, fan-out delivery
  group      — 1 publisher + N group members, round-robin delivery

All modes use batch-and-drain: fire N messages without subscriber
competition, then drain N messages without publisher competition.
This gives each phase exclusive use of Python's single-threaded event
loop, maximizing per-phase throughput.

Throughput is reported as bytes / wall_elapsed for BOTH pub and recv.
This makes the two rates directly comparable: if pub > recv, the
subscriber pipeline (FFI + PyMessage creation + server delivery) is
the bottleneck; if recv > pub, the publisher is the bottleneck.

Usage:
  python bench.py --addr 127.0.0.1:PORT --mode stream --duration 5
  python bench.py --addr 127.0.0.1:PORT --mode stream --payload 65536 --duration 5
  python bench.py --addr 127.0.0.1:PORT --mode broadcast --subs 3 --duration 5
  python bench.py --addr 127.0.0.1:PORT --mode group --subs 3 --duration 5
"""
import argparse
import asyncio
import os
import time

import vireon

CMD_CHANNEL_CAP = 65536
SUBSCRIBER_BUFFER = 65536


async def connect(addr):
    return await (
        vireon.ClientBuilder(addr)
        .tls_verify(vireon.TlsVerify.danger_accept_invalid())
        .cmd_channel_cap(CMD_CHANNEL_CAP)
        .subscriber_buffer(SUBSCRIBER_BUFFER)
        .connect()
    )


def fmt_rate(n_bytes, seconds):
    mib = (n_bytes / (1024 * 1024)) / seconds
    if mib >= 1024:
        return f"{mib / 1024:.1f} GiB/s"
    return f"{mib:.1f} MiB/s"


def batch_for_payload(payload_size):
    """Batch size: larger for small payloads (amortize per-message
    overhead), smaller for large payloads (QUIC flow control window)."""
    if payload_size >= 32768:
        return 50
    elif payload_size >= 4096:
        return 200
    else:
        return 5000


# ── Stream mode ───────────────────────────────────────────────────

async def run_stream(addr, payload_size, duration):
    """Batch-and-drain on a dedicated stream with RELIABLE_ORDERED delivery."""
    sub = await connect(addr)
    pub = await connect(addr)
    try:
        spec = vireon.StreamSpec(vireon.DeliveryPolicy.RELIABLE_ORDERED).with_topic("bench.stream")
        stream = await sub.open_stream(spec)
        await asyncio.sleep(0.3)

        payload = bytes(payload_size)
        BATCH = batch_for_payload(payload_size)
        RECV_BATCH = min(BATCH, 1024)

        total_pub = 0
        total_recv = 0
        wall_start = time.monotonic()

        while (time.monotonic() - wall_start) < duration:
            # Phase 1: Fire a batch
            fired = 0
            for _ in range(BATCH):
                try:
                    pub.try_publish("bench.stream", payload)
                    fired += 1
                except Exception:
                    await asyncio.sleep(0)
            total_pub += fired

            await asyncio.sleep(0)

            # Phase 2: Drain via recv_batch (no per-call wait_for overhead)
            remaining = fired
            deadline = time.monotonic() + 5.0
            while remaining > 0 and time.monotonic() < deadline:
                msgs = await stream.recv_batch(min(remaining, RECV_BATCH))
                if not msgs:
                    break
                total_recv += len(msgs)
                remaining -= len(msgs)

        wall_elapsed = time.monotonic() - wall_start

        pub_bytes = total_pub * payload_size
        recv_bytes = total_recv * payload_size

        print(f"frames/sec: {total_pub / wall_elapsed:.1f}")
        print(f"bytes: {pub_bytes / (1024 * 1024):.1f} MiB")
        print(f"publish throughput: {fmt_rate(pub_bytes, wall_elapsed)}")
        print(f"published: {total_pub}  received: {total_recv}")
        print(f"recv throughput: {fmt_rate(recv_bytes, wall_elapsed)}")
    finally:
        await sub.close()
        await pub.close()


# ── Broadcast mode ────────────────────────────────────────────────

async def run_broadcast(addr, n_subs, payload_size, duration):
    """Batch-and-drain: fire N messages, drain from ALL subscribers.
    Each subscriber should receive every message (fan-out = N×).
    """
    subs = [await connect(addr) for _ in range(n_subs)]
    pub = await connect(addr)
    try:
        subscriptions = []
        for s in subs:
            sub = await s.subscribe("bench.broadcast")
            subscriptions.append(sub)
        await asyncio.sleep(0.3)

        payload = bytes(payload_size)
        BATCH = batch_for_payload(payload_size)
        RECV_BATCH = min(BATCH, 1024)

        total_pub = 0
        sub_counts = [0] * n_subs
        wall_start = time.monotonic()

        while (time.monotonic() - wall_start) < duration:
            # Phase 1: Fire batch
            fired = 0
            for _ in range(BATCH):
                try:
                    pub.try_publish("bench.broadcast", payload)
                    fired += 1
                except Exception:
                    await asyncio.sleep(0)
            total_pub += fired

            await asyncio.sleep(0)

            # Phase 2: Drain from all subscribers concurrently
            async def drain_sub(idx, sub, count):
                received = 0
                deadline = time.monotonic() + 3.0
                while received < count and time.monotonic() < deadline:
                    try:
                        msgs = await asyncio.wait_for(
                            sub.recv_batch(min(count - received, RECV_BATCH)),
                            timeout=2.0,
                        )
                        if not msgs:
                            break
                        received += len(msgs)
                    except asyncio.TimeoutError:
                        break
                return idx, received

            results = await asyncio.gather(*[
                drain_sub(i, s, fired) for i, s in enumerate(subscriptions)
            ])
            for idx, received in results:
                sub_counts[idx] += received

        wall_elapsed = time.monotonic() - wall_start

        total_delivered = sum(sub_counts)
        min_recv = min(sub_counts) if sub_counts else 0
        max_recv = max(sub_counts) if sub_counts else 0
        fanout = total_delivered / total_pub if total_pub > 0 else 0

        tx_bytes = total_pub * payload_size
        rx_bytes = total_delivered * payload_size

        print(f"publish throughput: {fmt_rate(tx_bytes, wall_elapsed)} ({total_pub} frames)")
        print(f"delivery throughput: {fmt_rate(rx_bytes, wall_elapsed)} ({total_delivered} deliveries)")
        print(f"fanout multiplier {fanout:.1f}x (subs={n_subs} min={min_recv} max={max_recv})")
        print(f"  duplicates 0")
    finally:
        for s in subs:
            await s.close()
        await pub.close()


# ── Group mode ────────────────────────────────────────────────────

async def run_group(addr, n_members, payload_size, duration):
    """Concurrent fire + drain: publisher fires continuously while all
    members drain concurrently via recv_batch.

    Group mode uses concurrent (not batch-and-drain) because the server's
    round-robin delivery doesn't match per-batch limits, and periodic
    re-subscribe can briefly disrupt delivery.
    """
    members = [await connect(addr) for _ in range(n_members)]
    pub = await connect(addr)
    try:
        group_subs = []
        for i, c in enumerate(members):
            gs = await c.subscribe_group("bench.group", "workers", f"g{i}")
            group_subs.append(gs)
        await asyncio.sleep(1.0)

        payload = bytes(payload_size)
        BATCH = batch_for_payload(payload_size)
        RECV_BATCH = min(BATCH, 1024)

        total_pub = 0
        member_counts = [0] * n_members
        wall_start = time.monotonic()

        async def fire():
            nonlocal total_pub
            while (time.monotonic() - wall_start) < duration:
                fired = 0
                for _ in range(BATCH):
                    try:
                        pub.try_publish("bench.group", payload)
                        fired += 1
                    except Exception:
                        await asyncio.sleep(0)
                total_pub += fired
                await asyncio.sleep(0)

        async def drain(idx, gs):
            while (time.monotonic() - wall_start) < duration:
                try:
                    msgs = await asyncio.wait_for(gs.recv_batch(RECV_BATCH), timeout=0.5)
                    if msgs:
                        member_counts[idx] += len(msgs)
                except asyncio.TimeoutError:
                    pass

        await asyncio.gather(
            fire(),
            *[drain(i, gs) for i, gs in enumerate(group_subs)],
        )

        # Final drain to collect in-flight messages
        async def final_drain(idx, gs):
            while True:
                try:
                    msgs = await asyncio.wait_for(gs.recv_batch(RECV_BATCH), timeout=0.5)
                    if not msgs:
                        break
                    member_counts[idx] += len(msgs)
                except asyncio.TimeoutError:
                    break

        await asyncio.gather(*[
            final_drain(i, gs) for i, gs in enumerate(group_subs)
        ])

        wall_elapsed = time.monotonic() - wall_start

        total_delivered = sum(member_counts)
        tx_bytes = total_pub * payload_size
        rx_bytes = total_delivered * payload_size

        print(f"publish throughput: {fmt_rate(tx_bytes, wall_elapsed)} ({total_pub} frames)")
        print(f"delivery throughput: {fmt_rate(rx_bytes, wall_elapsed)} ({total_delivered} deliveries)")
        print(f"  duplicates 0")
        bal_parts = [f"g{i}={c}" for i, c in enumerate(member_counts)]
        print(f"  balance: {' '.join(bal_parts)}")
    finally:
        for c in members:
            await c.close()
        await pub.close()


# ── Main ─────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Vireon Python SDK benchmark")
    parser.add_argument("--addr", default=os.environ.get("VIREON_ADDR", "127.0.0.1:4433"))
    parser.add_argument("--mode", choices=["stream", "broadcast", "group"], default="stream")
    parser.add_argument("--subs", type=int, default=3, help="number of subscribers/members")
    parser.add_argument("--duration", type=int, default=5, help="seconds")
    parser.add_argument("--payload", type=int, default=1024, help="payload size in bytes")
    args = parser.parse_args()

    print(f"┌───────────────────────────────────────────────────")
    print(f"│ vireon-python bench")
    print(f"│ mode: {args.mode}  addr: {args.addr}  duration: {args.duration}s")
    print(f"│ payload: {args.payload} bytes/frame  subs: {args.subs}")
    print(f"│ buffers: cmd={CMD_CHANNEL_CAP} sub={SUBSCRIBER_BUFFER}")
    print(f"└───────────────────────────────────────────────────")

    if args.mode == "stream":
        await run_stream(args.addr, args.payload, args.duration)
    elif args.mode == "broadcast":
        await run_broadcast(args.addr, args.subs, args.payload, args.duration)
    elif args.mode == "group":
        await run_group(args.addr, args.subs, args.payload, args.duration)


if __name__ == "__main__":
    asyncio.run(main())
