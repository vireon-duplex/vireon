#!/usr/bin/env python3
"""Head-of-line congestion: 5 dedicated streams, burst publish, verify
per-stream delivery counts. Exercises HOL isolation — each stream
should receive its messages independently.

Mirrors Rust SDK ``s07_hol_congestion``.
"""
import asyncio
import os
import sys

import vireon

ADDR = os.environ.get("VIREON_ADDR", "127.0.0.1:4433")
N_STREAMS = 5
N_PER_STREAM = 100


async def connect(addr):
    return await (
        vireon.ClientBuilder(addr)
        .tls_verify(vireon.TlsVerify.danger_accept_invalid())
        .connect()
    )


async def main():
    print(f"┌───────────────────────────────────────────────────")
    print(f"│ vireon-python HOL congestion ({N_STREAMS} streams × {N_PER_STREAM} frames)")
    print(f"│ server: {ADDR}")
    print(f"└───────────────────────────────────────────────────")

    sub = await connect(ADDR)
    pub = await connect(ADDR)
    try:
        # Open 5 dedicated streams, each with its own topic
        streams = []
        for i in range(N_STREAMS):
            topic = f"hol.s{i}"
            spec = vireon.StreamSpec(vireon.DeliveryPolicy.RELIABLE_ORDERED).with_topic(topic)
            stream = await sub.open_stream(spec)
            streams.append((i, topic, stream))
        await asyncio.sleep(0.3)

        # Burst-publish to each stream
        print(f"[publish] sending {N_PER_STREAM} frames × {N_STREAMS} streams = {N_STREAMS * N_PER_STREAM} total")
        for i, topic, _ in streams:
            for j in range(N_PER_STREAM):
                await pub.publish(topic, f"s{i}-f{j}".encode())

        # Collect per-stream delivery counts
        results = {}
        for i, topic, stream in streams:
            count = 0
            try:
                while count < N_PER_STREAM:
                    msg = await asyncio.wait_for(stream.recv(), timeout=5.0)
                    if msg is None:
                        break
                    count += 1
            except asyncio.TimeoutError:
                pass
            results[i] = count

        # Report
        heaviest = max(results.values()) if results else 0
        lightest = min(results.values()) if results else 0
        total = sum(results.values())
        print(f"[results] heaviest stream deliver={heaviest} lightest={lightest} total={total}")
        for i in range(N_STREAMS):
            pct = results[i] / N_PER_STREAM * 100
            print(f"  stream {i}: {results[i]}/{N_PER_STREAM} ({pct:.0f}%)")

        all_delivered = all(c == N_PER_STREAM for c in results.values())
        if all_delivered:
            print(f"[hol] PASS — all {N_STREAMS} streams received {N_PER_STREAM}/{N_PER_STREAM}")
            sys.exit(0)
        else:
            print(f"[hol] FAIL — not all streams fully delivered")
            sys.exit(1)
    finally:
        await sub.close()
        await pub.close()


if __name__ == "__main__":
    asyncio.run(main())
