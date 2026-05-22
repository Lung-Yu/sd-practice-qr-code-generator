import os

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, make_asgi_app

from app.hash_ring import HashRing, HashStrategy
from app.models import (
    CacheSetRequest, CacheSetResponse, CacheGetResponse,
    CacheDeleteResponse, RingResponse, StatsResponse, ErrorResponse,
)

# ── configuration ──────────────────────────────────────────────────────
_raw_urls = os.environ.get("NODE_URLS", "http://dc-node1:8001,http://dc-node2:8002,http://dc-node3:8003")
NODE_URLS: dict[str, str] = {}

_strategy_env = os.environ.get("HASH_STRATEGY", "ring").lower()
_strategy = HashStrategy.RENDEZVOUS if _strategy_env == "rendezvous" else HashStrategy.RING
_virtual_nodes = int(os.environ.get("VIRTUAL_NODES", "150"))

for url in _raw_urls.split(","):
    url = url.strip()
    if not url:
        continue
    host = url.split("//")[-1].split(":")[0]
    node_id = host.replace("dc-", "")
    NODE_URLS[node_id] = url

# ── hash ring ──────────────────────────────────────────────────────────
ring = HashRing(virtual_nodes=_virtual_nodes, strategy=_strategy)
for node_id in NODE_URLS:
    ring.add_node(node_id)

# ── Prometheus metrics ─────────────────────────────────────────────────
_requests = Counter("http_requests_total", "HTTP requests", ["handler", "status"])
_route_duration = Histogram("cache_route_duration_seconds", "Time to route + proxy a request")

# ── FastAPI app ────────────────────────────────────────────────────────
# Sync handlers are intentional: FastAPI runs them in a bounded thread pool
# (~32 threads), which provides natural back-pressure against the nodes.
# Async httpx without a semaphore overwhelms node thread pools; the sync
# thread pool acts as an implicit concurrency limiter.
app = FastAPI(title="Distributed Cache Router")
app.mount("/metrics", make_asgi_app())

_client = httpx.Client(timeout=5.0)


def _node_url(key: str) -> tuple[str, str]:
    node_id = ring.node_for_key(key)
    return node_id, NODE_URLS[node_id]


@app.post("/cache/{key}", response_model=CacheSetResponse)
def set_key(key: str, body: CacheSetRequest) -> CacheSetResponse:
    node_id, base = _node_url(key)
    with _route_duration.time():
        try:
            r = _client.post(f"{base}/cache/{key}", json=body.model_dump())
            _requests.labels(handler="set", status=str(r.status_code)).inc()
            return CacheSetResponse(**r.json())
        except httpx.RequestError:
            _requests.labels(handler="set", status="503").inc()
            return JSONResponse(
                status_code=503,
                content=ErrorResponse(error="node_unreachable", key=key, node=node_id).model_dump(),
            )


@app.get("/cache/{key}")
def get_key(key: str):
    node_id, base = _node_url(key)
    with _route_duration.time():
        try:
            r = _client.get(f"{base}/cache/{key}")
            _requests.labels(handler="get", status=str(r.status_code)).inc()
            if r.status_code == 200:
                return CacheGetResponse(**r.json())
            return JSONResponse(status_code=r.status_code, content=r.json())
        except httpx.RequestError:
            _requests.labels(handler="get", status="503").inc()
            return JSONResponse(
                status_code=503,
                content=ErrorResponse(error="node_unreachable", key=key, node=node_id).model_dump(),
            )


@app.delete("/cache/{key}", response_model=CacheDeleteResponse)
def delete_key(key: str) -> CacheDeleteResponse:
    node_id, base = _node_url(key)
    with _route_duration.time():
        try:
            r = _client.delete(f"{base}/cache/{key}")
            _requests.labels(handler="delete", status=str(r.status_code)).inc()
            return CacheDeleteResponse(**r.json())
        except httpx.RequestError:
            _requests.labels(handler="delete", status="503").inc()
            return JSONResponse(
                status_code=503,
                content=ErrorResponse(error="node_unreachable", key=key, node=node_id).model_dump(),
            )


@app.get("/ring/{key}", response_model=RingResponse)
def ring_inspect(key: str) -> RingResponse:
    node_id = ring.node_for_key(key)
    return RingResponse(key=key, node=node_id, virtual_nodes=ring.virtual_count)


@app.get("/stats", response_model=StatsResponse)
def get_stats() -> StatsResponse:
    totals: dict[str, int] = {"hits": 0, "misses": 0, "evictions": 0, "size": 0, "capacity": 0}
    for node_id, base in NODE_URLS.items():
        try:
            r = _client.get(f"{base}/stats", timeout=2.0)
            if r.status_code == 200:
                data = r.json()
                for k in totals:
                    totals[k] += data.get(k, 0)
        except httpx.RequestError:
            pass
    return StatsResponse(**totals)
