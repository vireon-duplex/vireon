#!/usr/bin/env python3
"""Quickstart: basic pubsub + 3 dedicated-stream delivery policies.

Mirrors the Rust SDK ``quickstart`` example. Uses TWO connections
(subscriber + publisher) because the server skips the origin on
fan-out (no echo).
"""
import asyncio
import os
import sys

import vireon

ADDR = os.environ.get("VIREON_ADDR", "127.0.0.1:4433")
CERT = os.environ.get("VIREON_CERT", "")


async def connect(addr):
    b = vireon.ClientBuilder(addr).tls_verify(vireon.TlsVerify.danger_accept_invalid())
    if CERT:
        b = b.tls_verify(vireon.TlsVerify.strict(CERT))
    return await b.connect()


async def main():
    print(f"┌───────────────────────────────────────────────────")
    print(f"│ vireon-python quickstart")
    print(f"│ server: {ADDR}")
    print(f"└───────────────────────────────────────────────────")
    print()

    # ── Default-channel pub/sub ────────────────────────────────────
    sub = await connect(ADDR)
    pub = await connect(ADDR)
    try:
        subscription = await sub.subscribe("chat.*")
        await asyncio.sleep(0.2)
        await pub.publish("chat.hello", b"hello from vireon-python")
        msg = await asyncio.wait_for(subscription.recv(), timeout=5.0)
        assert msg is not None, "no message received"
        assert msg.topic == "chat.hello"
        assert msg.payload == b"hello from vireon-python"
        print(f"[default] subscribed to chat.*")
        print(f"[publisher] publish chat.hello")
        print(f"[default] recv  topic={msg.topic} payload={msg.payload.decode()}")
        print()
    finally:
        await sub.close()
        await pub.close()

    # ── Dedicated streams: 3 delivery policies ─────────────────────
    sub = await connect(ADDR)
    pub = await connect(ADDR)
    try:
        policies = [
            ("ReliableOrdered", vireon.DeliveryPolicy.RELIABLE_ORDERED, "qs.data"),
            ("LatestOnly", vireon.DeliveryPolicy.LATEST_ONLY, "qs.cursor"),
            ("RealtimeDropOld", vireon.DeliveryPolicy.REALTIME_DROP_OLD, "qs.events"),
        ]
        streams = []
        for name, policy, topic in policies:
            spec = vireon.StreamSpec(policy).with_topic(topic)
            stream = await sub.open_stream(spec)
            streams.append((name, topic, stream))

        print(f"[streams] opened {len(streams)} dedicated QUIC streams "
              f"(ids {', '.join(str(s.stream_id) for _, _, s in streams)})")
        for name, topic, _ in streams:
            print(f"  {name:<20s} {topic}")
        print()

        await asyncio.sleep(0.3)

        n = 50
        for name, topic, _ in streams:
            for i in range(n):
                await pub.publish(topic, f"{topic}.{i}".encode())

        print(f"[publish] sending {n} frames per stream ...")
        total_recv = 0
        for name, topic, stream in streams:
            count = 0
            try:
                while count < n:
                    msg = await asyncio.wait_for(stream.recv(), timeout=3.0)
                    if msg is None:
                        break
                    count += 1
            except asyncio.TimeoutError:
                pass
            total_recv += count
            print(f"  {name:<20s} received {count}/{n}")
        print()
        print(f"[publish] total received across streams: {total_recv}")
    finally:
        await sub.close()
        await pub.close()

    print()
    print("[quickstart] all 3 delivery policies verified")


if __name__ == "__main__":
    asyncio.run(main())
