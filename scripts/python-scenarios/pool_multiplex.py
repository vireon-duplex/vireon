#!/usr/bin/env python3
"""ClientPool multiplex: N-connection pool round-robins publishes.

Mirrors Rust SDK ``s14_pool_multiplex``. Opens a pool of N connections,
publishes via the pool, and verifies delivery to a single subscriber.
"""
import asyncio
import os
import sys

import vireon

ADDR = os.environ.get("VIREON_ADDR", "127.0.0.1:4433")
POOL_SIZE = 4
N_MESSAGES = 200


async def main():
    print(f"┌───────────────────────────────────────────────────")
    print(f"│ vireon-python pool multiplex (pool={POOL_SIZE}, msgs={N_MESSAGES})")
    print(f"│ server: {ADDR}")
    print(f"└───────────────────────────────────────────────────")

    # Subscriber
    sub = await (
        vireon.ClientBuilder(ADDR)
        .tls_verify(vireon.TlsVerify.danger_accept_invalid())
        .connect()
    )
    try:
        subscription = await sub.subscribe("pool.test")
        await asyncio.sleep(0.2)

        # Pool
        builder = (
            vireon.ClientBuilder(ADDR)
            .tls_verify(vireon.TlsVerify.danger_accept_invalid())
        )
        pool = await vireon.ClientPool.connect(builder, POOL_SIZE)
        assert pool.len == POOL_SIZE
        print(f"[pool] connected {pool.len} members")

        # Concurrent publish via pool
        print(f"[publish] sending {N_MESSAGES} messages via pool")
        start = asyncio.get_event_loop().time()

        async def publish_batch(offset, count):
            for i in range(offset, offset + count):
                await pool.publish("pool.test", f"msg-{i}".encode())

        per_worker = N_MESSAGES // POOL_SIZE
        tasks = [publish_batch(i * per_worker, per_worker) for i in range(POOL_SIZE)]
        await asyncio.gather(*tasks)

        publish_elapsed = asyncio.get_event_loop().time() - start
        print(f"[publish] done in {publish_elapsed:.2f}s")

        # Collect
        count = 0
        for _ in range(N_MESSAGES):
            try:
                msg = await asyncio.wait_for(subscription.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                break
            if msg is None:
                break
            count += 1

        elapsed = asyncio.get_event_loop().time() - start
        throughput = count / elapsed if elapsed > 0 else 0
        print(f"[recv] received {count}/{N_MESSAGES}")
        print(f"[pool] throughput: {throughput:.0f} msg/s | pool_size={POOL_SIZE}")

        if count == N_MESSAGES:
            print(f"[pool] PASS — all {N_MESSAGES} delivered via pool")
            sys.exit(0)
        else:
            print(f"[pool] FAIL — only {count}/{N_MESSAGES} delivered")
            sys.exit(1)
    finally:
        await sub.close()


if __name__ == "__main__":
    asyncio.run(main())
