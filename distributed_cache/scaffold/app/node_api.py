import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, make_asgi_app
from starlette.routing import Mount

from app.lru_cache import LRUCache, CacheMiss, CacheExpired
from app.models import (
    CacheSetRequest, CacheSetResponse, CacheGetResponse,
    CacheDeleteResponse, StatsResponse, ErrorResponse,
)

NODE_ID = os.environ.get("NODE_ID", "node1")
CAPACITY = int(os.environ.get("CAPACITY", "100"))

# Prometheus counters/gauges — all labeled by node
_hits_counter = Counter("cache_hits_total", "Cache hits", ["node"])
_misses_counter = Counter("cache_misses_total", "Cache misses", ["node"])
_evictions_counter = Counter("cache_evictions_total", "Cache evictions", ["node"])
_size_gauge = Gauge("cache_size", "Current number of keys", ["node"])
_capacity_gauge = Gauge("cache_capacity", "Max keys (capacity)", ["node"])

_capacity_gauge.labels(node=NODE_ID).set(CAPACITY)

cache = LRUCache(capacity=CAPACITY)
_prev_evictions = 0  # track delta for Prometheus

app = FastAPI(title=f"Cache Node {NODE_ID}")
app.mount("/metrics", make_asgi_app())


def _sync_metrics() -> None:
    """Update Prometheus gauges/counters from cache stats."""
    global _prev_evictions
    s = cache.stats()
    _size_gauge.labels(node=NODE_ID).set(s["size"])
    delta = s["evictions"] - _prev_evictions
    if delta > 0:
        _evictions_counter.labels(node=NODE_ID).inc(delta)
        _prev_evictions = s["evictions"]


@app.post("/cache/{key}", response_model=CacheSetResponse)
def set_key(key: str, body: CacheSetRequest) -> CacheSetResponse:
    cache.set(key, body.value, body.ttl)
    _sync_metrics()
    return CacheSetResponse(key=key, node=NODE_ID)


@app.get("/cache/{key}")
def get_key(key: str):
    try:
        value, ttl_remaining = cache.get(key)
        _hits_counter.labels(node=NODE_ID).inc()
        _sync_metrics()
        return CacheGetResponse(key=key, value=value, ttl_remaining=ttl_remaining)
    except CacheExpired:
        _misses_counter.labels(node=NODE_ID).inc()
        _sync_metrics()
        return JSONResponse(
            status_code=404,
            content=ErrorResponse(error="expired", key=key).model_dump(),
        )
    except CacheMiss:
        _misses_counter.labels(node=NODE_ID).inc()
        return JSONResponse(
            status_code=404,
            content=ErrorResponse(error="miss", key=key).model_dump(),
        )


@app.delete("/cache/{key}", response_model=CacheDeleteResponse)
def delete_key(key: str) -> CacheDeleteResponse:
    cache.delete(key)
    _sync_metrics()
    return CacheDeleteResponse(key=key)


@app.get("/stats", response_model=StatsResponse)
def get_stats() -> StatsResponse:
    return StatsResponse(**cache.stats())
