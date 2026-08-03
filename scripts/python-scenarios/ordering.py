#!/usr/bin/env python3
"""Ordering: publish 500 messages, verify zero gaps and zero duplicates.

Mirrors Rust SDK ``s11_ordering``. Uses a dedicated stream with
ReliableOrdered policy for guaranteed in-order delivery.
"""
import asyncio
import os
import sys

import vireon

ADDR = os.environ.get("VIREON_ADDR", "127.0.0.1:4433")
N = int(os.environ.get("VIREON_N", "500"))


async def connect(addr):
    return await (
        vireon.ClientBuilder(addr)
        .tls_verify(vireon.TlsVerify.danger_accept_invalid())
        .connect()
    )


async def main():
    print(f"┌───────────────────────────────────────────────────")
    print(f"│ vireon-python ordering ({N} frames)")
    print(f"│ server: {ADDR}")
    print(f"└───────────────────────────────────────────────────")

    sub = await connect(ADDR)
    pub = await connect(ADDR)
    try:
        spec = vireon.StreamSpec(vireon.DeliveryPolicy.RELIABLE_ORDERED).with_topic("seq.data")
        stream = await sub.open_stream(spec)
        await asyncio.sleep(0.3)

        print(f"[publish] sending {N} frames ...")
        for i in range(N):
            await pub.publish("seq.data", str(i).encode())

        print(f"[recv] collecting ...")
        seqs = []
        for _ in range(N):
            try:
                msg = await asyncio.wait_for(stream.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                break
            if msg is None:
                break
            seqs.append(int(msg.payload))

        received = len(seqs)
        # Check for gaps and duplicates
        seq_set = set(seqs)
        duplicates = received - len(seq_set)
        expected = set(range(N))
        missing = expected - seq_set
        gaps = len(missing)

        print(f"[recv] received {received}/{N}")
        print(f"[ordering] received {received} messages | gaps: {gaps} | duplicates: {duplicates}")

        if gaps == 0 and duplicates == 0 and received == N:
            print("[ordering] PASS — zero gaps, zero duplicates, all delivered")
            sys.exit(0)
        else:
            print(f"[ordering] FAIL — gaps={gaps} dup={duplicates} received={received}/{N}")
            sys.exit(1)
    finally:
        await sub.close()
        await pub.close()


if __name__ == "__main__":
    asyncio.run(main())
