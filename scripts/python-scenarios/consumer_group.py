#!/usr/bin/env python3
"""Consumer group: 3 members receive round-robin-balanced delivery.

Mirrors Rust SDK ``s15_consumer_group``. The publisher sends N messages
to a topic; the server assigns each message to exactly one group member
in round-robin order. Verifies zero duplicates and balanced delivery.
"""
import asyncio
import os
import sys

import vireon

ADDR = os.environ.get("VIREON_ADDR", "127.0.0.1:4433")
N_MEMBERS = 3
N_MESSAGES = 60


async def connect(addr):
    return await (
        vireon.ClientBuilder(addr)
        .tls_verify(vireon.TlsVerify.danger_accept_invalid())
        .connect()
    )


async def main():
    print(f"┌───────────────────────────────────────────────────")
    print(f"│ vireon-python consumer group ({N_MEMBERS} members × {N_MESSAGES} msgs)")
    print(f"│ server: {ADDR}")
    print(f"└───────────────────────────────────────────────────")

    members = [await connect(ADDR) for _ in range(N_MEMBERS)]
    pub = await connect(ADDR)
    try:
        # Join group
        group_subs = []
        for i, c in enumerate(members):
            gs = await c.subscribe_group("grp.work", "workers", f"c{i}")
            group_subs.append((f"c{i}", gs))
        await asyncio.sleep(1.0)  # let memberships register

        # Publish
        print(f"[publisher] sending {N_MESSAGES} messages to grp.work")
        for i in range(N_MESSAGES):
            await pub.publish("grp.work", f"job-{i}".encode())

        # Collect
        async def drain(name, gs):
            msgs = []
            for _ in range(N_MESSAGES):
                try:
                    msg = await asyncio.wait_for(gs.recv(), timeout=3.0)
                except asyncio.TimeoutError:
                    break
                if msg is None:
                    break
                msgs.append(msg)
            return name, msgs

        results = await asyncio.gather(*[drain(n, gs) for n, gs in group_subs])

        delivered = sum(len(msgs) for _, msgs in results)
        all_msgs = []
        for name, msgs in results:
            all_msgs.extend(m.payload for m in msgs)
        dup = delivered - len(set(all_msgs))

        print(f"[group] delivered: {delivered} | duplicates: {dup}")
        for name, msgs in results:
            print(f"  balance: member {name} received {len(msgs)}")

        # Verify: all delivered, no dups, each member got at least 1
        all_ok = delivered == N_MESSAGES and dup == 0
        for name, msgs in results:
            if len(msgs) == 0:
                all_ok = False

        if all_ok:
            print(f"[group] PASS — {delivered} delivered, {dup} duplicates")
            sys.exit(0)
        else:
            print(f"[group] FAIL — delivered={delivered}/{N_MESSAGES} dup={dup}")
            sys.exit(1)
    finally:
        for c in members:
            await c.close()
        await pub.close()


if __name__ == "__main__":
    asyncio.run(main())
