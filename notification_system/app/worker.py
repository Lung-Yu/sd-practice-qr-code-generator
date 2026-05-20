"""
Async delivery worker process.

Run with:
    python -m app.worker

Reads pending notification IDs from the Redis Stream via consumer group
'delivery-workers'. Multiple worker containers share the group — Redis ensures
each message is delivered to exactly one consumer, so delivery is never duplicated
even with N workers competing.

Tier 4 change: async main loop + asyncio.gather() for concurrent batch delivery.
Previously each batch of BATCH_SIZE messages was processed sequentially (total
time = sum of delivery times). Now they run concurrently (total time = max of
delivery times). channel.send() is still sync (simulates network I/O) so it
runs in the default thread executor — no event-loop blocking.

Tier 7+ batch fetch: abatch_get() pipelines N HGETALLs → 1 round-trip to primary
Redis (vs N individual round-trips). Batch XACK collapses N XACKs → 1 command to
delivery Redis per batch.

Tier 10A: PEL recovery — XAUTOCLAIM reclaims messages from dead consumers.
When a worker crashes mid-batch, its claimed messages stay in the Pending Entry
List indefinitely. Every PEL_CHECK_INTERVAL_S seconds each worker scans the PEL
for messages idle > PEL_CLAIM_TIMEOUT_MS and reprocesses them.

Consumer name = hostname (unique per container in docker-compose).
"""
import asyncio
import random
import signal
import socket
import sys
import time

import redis
import redis.asyncio as aioredis
from prometheus_client import start_http_server

from . import config
from .delivery import deliver
from .metrics import pel_recovered
from .queue import GROUP_NAME, STREAM_KEY, STREAM_KEY_CRITICAL
from .store import store

METRICS_PORT = 8001   # delivery-worker exposes /metrics on this port

BATCH_SIZE = config.WORKER_BATCH_SIZE   # set via WORKER_BATCH_SIZE env var
BLOCK_MS = 1000   # how long to block waiting for new messages

CONSUMER_NAME = socket.gethostname()


async def _wait_for_redis(r: aioredis.Redis, retries: int = 60, delay: float = 2.0) -> None:
    """Block until Redis is ready (handles BusyLoadingError on AOF replay)."""
    for attempt in range(1, retries + 1):
        try:
            await r.ping()
            return
        except (redis.exceptions.BusyLoadingError, redis.exceptions.ConnectionError) as e:
            print(f"[worker] Redis not ready ({e}), retry {attempt}/{retries}…", flush=True)
            await asyncio.sleep(delay)
    raise RuntimeError("Redis did not become ready in time")


async def _ensure_group(r: aioredis.Redis) -> None:
    for stream in (STREAM_KEY, STREAM_KEY_CRITICAL):
        try:
            await r.xgroup_create(stream, GROUP_NAME, id="0", mkstream=True)
        except redis.exceptions.ResponseError:
            pass  # group already exists


async def _process_batch(
    r: aioredis.Redis,
    msgs: list[tuple[str, dict]],
    loop: asyncio.AbstractEventLoop,
    stream_key: str = STREAM_KEY,
) -> None:
    """Fetch all notifications in one pipeline round-trip, deliver concurrently, ACK in batch."""
    msg_ids = [msg_id for msg_id, _ in msgs]
    nids = [data.get("notification_id") for _, data in msgs]

    # One pipeline round-trip to primary Redis (vs N individual HGETALLs).
    notifications = await store.abatch_get([nid for nid in nids if nid])

    # Map nid → notification for lookup (abatch_get preserves order of non-None nids).
    nid_list = [nid for nid in nids if nid]
    nid_to_notif = {nid: n for nid, n in zip(nid_list, notifications) if n is not None}

    # Concurrent delivery — one executor task per notification.
    deliver_tasks = []
    for nid in nids:
        notif = nid_to_notif.get(nid) if nid else None
        if notif is not None:
            deliver_tasks.append(loop.run_in_executor(None, deliver, notif))
        else:
            deliver_tasks.append(asyncio.sleep(0))

    results = await asyncio.gather(*deliver_tasks, return_exceptions=True)
    for exc in results:
        if isinstance(exc, Exception):
            print(f"[worker] delivery error: {exc}", flush=True)

    # Batch ACK: 1 XACK command for the whole batch (vs N individual XACKs).
    # ACK on the correct stream — critical messages live in STREAM_KEY_CRITICAL.
    if msg_ids:
        await r.xack(stream_key, GROUP_NAME, *msg_ids)


async def _recover_pending(
    r: aioredis.Redis,
    loop: asyncio.AbstractEventLoop,
) -> int:
    """XAUTOCLAIM messages idle > PEL_CLAIM_TIMEOUT_MS from any dead consumer.

    Iterates both streams until next_id returns to '0-0' (no more pending messages
    older than the timeout). Each page of claimed messages is fed through the normal
    _process_batch() path so delivery and ACK happen atomically.

    Returns total count of messages recovered across both streams.
    """
    total = 0
    for stream in (STREAM_KEY_CRITICAL, STREAM_KEY):
        start = "0-0"
        while True:
            next_id, claimed, _deleted = await r.xautoclaim(
                stream,
                GROUP_NAME,
                CONSUMER_NAME,
                min_idle_time=config.PEL_CLAIM_TIMEOUT_MS,
                start_id=start,
                count=BATCH_SIZE,
            )
            if claimed:
                print(
                    f"[worker] PEL recovery: reclaiming {len(claimed)} from {stream}",
                    flush=True,
                )
                await _process_batch(r, claimed, loop, stream_key=stream)
                pel_recovered.labels(stream=stream).inc(len(claimed))
                total += len(claimed)
            # next_id == "0-0" means no pending messages remain past this cursor
            if next_id == "0-0":
                break
            start = next_id
    if total:
        print(f"[worker] PEL recovery: {total} messages recovered", flush=True)
    return total


async def run() -> None:
    # Tier 7: XREADGROUP/XACK on delivery Redis; store reads/writes on primary Redis.
    r = aioredis.from_url(config.DELIVERY_REDIS_URL, decode_responses=True, max_connections=20)
    await _wait_for_redis(r)

    # Also wait for primary Redis — abatch_get() hits store._ar which connects
    # to REDIS_URL. Primary Redis may still be loading AOF when delivery Redis is
    # already available (they are independent processes with separate AOF files).
    # Without this wait, the worker enters a tight error loop at startup.
    primary_r = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    await _wait_for_redis(primary_r)
    await primary_r.aclose()

    await _ensure_group(r)

    start_http_server(METRICS_PORT)
    print(f"[worker] metrics server listening on :{METRICS_PORT}", flush=True)

    loop = asyncio.get_event_loop()
    running = True

    def _stop(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    # Stagger PEL checks across workers so they don't all sweep simultaneously.
    # Jitter: uniform(0, PEL_CHECK_INTERVAL_S) so workers spread out their first check.
    last_pel_check = time.monotonic() - random.uniform(0, config.PEL_CHECK_INTERVAL_S)

    print(
        f"[worker] {CONSUMER_NAME} ready — consuming {STREAM_KEY_CRITICAL} (priority) "
        f"then {STREAM_KEY} / group={GROUP_NAME} "
        f"(PEL recovery every {config.PEL_CHECK_INTERVAL_S}s, timeout={config.PEL_CLAIM_TIMEOUT_MS}ms)",
        flush=True,
    )

    while running:
        # Tier 10A: periodic PEL recovery — reclaim messages from dead consumers.
        now = time.monotonic()
        if now - last_pel_check >= config.PEL_CHECK_INTERVAL_S:
            try:
                await _recover_pending(r, loop)
            except Exception as exc:
                print(f"[worker] PEL recovery error: {exc}", flush=True)
            last_pel_check = time.monotonic()

        # Tier 9C priority: drain critical stream first (non-blocking),
        # then fall back to normal stream (blocking up to BLOCK_MS).
        critical_msgs = await r.xreadgroup(
            GROUP_NAME,
            CONSUMER_NAME,
            {STREAM_KEY_CRITICAL: ">"},
            count=BATCH_SIZE,
        )
        all_msgs: list[tuple[str, dict]] = []
        active_stream = STREAM_KEY_CRITICAL

        if critical_msgs:
            for _stream, msgs in critical_msgs:
                all_msgs.extend(msgs)
        else:
            # No critical messages — wait for normal ones
            normal_msgs = await r.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_KEY: ">"},
                count=BATCH_SIZE,
                block=BLOCK_MS,
            )
            active_stream = STREAM_KEY
            for _stream, msgs in (normal_msgs or []):
                all_msgs.extend(msgs)

        if all_msgs:
            try:
                await _process_batch(r, all_msgs, loop, stream_key=active_stream)
            except Exception as exc:
                print(f"[worker] unhandled batch error: {exc}", flush=True)

    print("[worker] shutting down gracefully", flush=True)


if __name__ == "__main__":
    if not config.REDIS_URL:
        print("REDIS_URL is not set — worker requires Redis", file=sys.stderr)
        sys.exit(1)
    asyncio.run(run())
