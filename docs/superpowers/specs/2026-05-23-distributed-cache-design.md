# Distributed Cache — Guided Scaffold Design

**Date:** 2026-05-23  
**Exercise:** `distributed_cache/`  
**Track:** Guided (TODO-marked functions for learner)

---

## Context

The `distributed_cache` exercise already has `PROMPT.md` (design questions answered) and `README.md`. This spec covers the scaffold that will live in `distributed_cache/scaffold/` — the pre-written structure a learner fills in on the Guided Track.

Goal: learner implements 5 focused algorithmic functions (LRU get/set, consistent hash ring add/get, rendezvous HRW), runs `verify.sh` to check correctness, then runs k6 to observe sharding behavior under load.

---

## Architecture

```
Client
  │
  ▼ :8000 (external)
┌──────────────┐
│  dc-router   │  FastAPI — hash_ring selects owning node, httpx forwards request
└──────┬───────┘
       │ internal Docker network
  ┌────┼────┬────────┐
  ▼    ▼    ▼        
node1 node2 node3   FastAPI :8001-8003 — each owns its LRU cache shard
                    each exposes /metrics for Prometheus

Shared monitoring: sd_monitoring Docker network (external)
Prometheus scrapes: dc-router:8000/metrics, dc-node{1,2,3}:800{1,2,3}/metrics
Grafana dashboard:  monitoring/grafana/dashboards/distributed-cache.json
```

---

## File Structure

```
distributed_cache/scaffold/
├── app/
│   ├── __init__.py
│   ├── hash_ring.py        # ConsistentHashRing + HashStrategy enum — TODOs here
│   ├── lru_cache.py        # LRUCache (dict + doubly-linked list + TTL) — TODOs here
│   ├── router_api.py       # FastAPI :8000, forwards via httpx + hash_ring
│   ├── node_api.py         # FastAPI :8001-8003, delegates to LRUCache
│   └── models.py           # Pydantic models + RFC 7807 error helpers
├── Dockerfile              # single image; command: overridden per service in docker-compose
├── docker-compose.yml      # router + cache-node-{1,2,3} + sd_monitoring network
├── docker-compose.env      # NODE_URLS, CAPACITY, VIRTUAL_NODES, HASH_STRATEGY
├── requirements.txt
└── scripts/
    ├── start.sh            # start / stop / rebuild
    └── verify.sh           # runs PROMPT.md curl tests, green/red per check
k6s/
└── k6.js                   # ramp to system limit, Prometheus remote write
```

Monitoring changes (at repo root):
- `monitoring/prometheus.yml` — add `distributed_cache_router` + `distributed_cache_nodes` scrape jobs
- `monitoring/grafana/dashboards/distributed-cache.json` — new dashboard

---

## The 5 TODO Functions

### 1. `LRUCache.get(key)` — `app/lru_cache.py`

```
Input:  key: str
Output: str | None  (None on miss or TTL expiry)
Side effects: move accessed node to head of LRU list; increment hits or misses counter
TTL: if node.expires_at is not None and time.time() > node.expires_at → delete + return None ("expired")
```

### 2. `LRUCache.set(key, value, ttl)` — `app/lru_cache.py`

```
Input:  key: str, value: str, ttl: int | None (seconds; None = no expiry)
Output: None
Side effects:
  - if key exists: update value/expires_at, move to head
  - if at capacity and key is new: evict tail node, increment evictions counter
  - insert new node at head
```

### 3. `HashRing.add_node(node_id)` — `app/hash_ring.py`

```
Input:  node_id: str  (e.g. "node1")
Effect: hash node_id + str(i) for i in range(VIRTUAL_NODES) via md5
        bisect.insort(_ring, hash_int) for each virtual node
        _ring_map[hash_int] = node_id
```

### 4. `HashRing.get_node(key)` — `app/hash_ring.py`

```
Input:  key: str
Output: str  (node_id that owns this key)
Algorithm: hash key → bisect_right(_ring, hash_int) → wrap mod len(_ring) → _ring_map[pos]
Raises: ValueError if ring is empty
```

### 5. `HashRing.rendezvous_node(key)` — `app/hash_ring.py`

```
Input:  key: str
Output: str  (node_id with highest HRW score)
Algorithm: for each physical node: score = hmac_hash(node_id + ":" + key)
           return node_id with max score
```

Toggled via `HASH_STRATEGY=ring` (default) or `HASH_STRATEGY=rendezvous` env var. Router reads this at startup.

---

## Data Structures (pre-written, not TODOs)

**`lru_cache.py`:**
```python
@dataclass
class _Node:
    key: str
    value: str
    expires_at: float | None
    prev: "_Node | None" = None
    next: "_Node | None" = None

class LRUCache:
    _cache: dict[str, _Node]   # O(1) lookup
    _head: _Node               # sentinel (MRU side)
    _tail: _Node               # sentinel (LRU side)
    _capacity: int
    _hits: int; _misses: int; _evictions: int
```

**`hash_ring.py`:**
```python
class HashRing:
    _ring: list[int]           # sorted virtual node positions
    _ring_map: dict[int, str]  # position → node_id
    _nodes: set[str]           # physical node ids
    virtual_nodes: int         # default 150
```

---

## API Endpoints

**Router** (`router_api.py`):

| Method | Path | Request | Response |
|--------|------|---------|----------|
| POST | `/cache/{key}` | `{value: str, ttl?: int}` | `{key, node, status: "ok"}` |
| GET | `/cache/{key}` | — | `{key, value, ttl_remaining: int\|null}` |
| DELETE | `/cache/{key}` | — | `{key, status: "deleted"}` |
| GET | `/ring/{key}` | — | `{key, node, virtual_nodes: int}` |
| GET | `/stats` | — | `{hits, misses, evictions, size, capacity}` (summed across nodes) |
| GET | `/metrics` | — | Prometheus text format |

**Nodes** (`node_api.py`) — same paths minus `/ring` and `/stats` (stats are local only):

| Method | Path | Notes |
|--------|------|-------|
| POST | `/cache/{key}` | store locally |
| GET | `/cache/{key}` | return value + ttl_remaining |
| DELETE | `/cache/{key}` | remove locally |
| GET | `/stats` | local stats only |
| GET | `/metrics` | Prometheus text |

**Error shapes (RFC 7807 simplified):**
```json
// 404 miss:    {"error": "miss",            "key": "foo"}
// 404 expired: {"error": "expired",         "key": "foo"}
// 503 node down: {"error": "node_unreachable", "node": "node1", "key": "foo"}
```

---

## Prometheus Metrics

**Per node** (labeled `node=node{1,2,3}`):
- `cache_hits_total` — counter
- `cache_misses_total` — counter
- `cache_evictions_total` — counter
- `cache_size` — gauge (current keys)
- `cache_capacity` — gauge (max keys)

**Router:**
- `http_requests_total{handler, status}` — counter
- `cache_route_duration_seconds` — histogram

**prometheus.yml additions:**
```yaml
- job_name: distributed_cache_router
  static_configs:
    - targets: ["dc-router:8000"]
  metrics_path: /metrics

- job_name: distributed_cache_nodes
  static_configs:
    - targets: ["dc-node1:8001", "dc-node2:8002", "dc-node3:8003"]
  metrics_path: /metrics
```

**Grafana dashboard panels:**
- Hit rate per node (timeseries)
- Overall hit ratio (stat)
- Evictions per node (timeseries)
- Cache size vs capacity per node (timeseries)
- Router request rate (timeseries)
- k6 row: RPS, p95 latency, error rate (from k6 Prometheus remote write)

---

## k6 Load Test + Improve Cycle

**`k6s/k6.js`** — ramp to system limit:
```
Stages: 0→100 RPS (30s) → 100→500 RPS (60s) → 500→1000 RPS (60s) → hold (120s) → ramp down
Checks: p95 < 100ms, error rate < 1%, hit rate > 80%
Output: Prometheus remote write (same pattern as notification_system)
```

Test mix: 70% GET (cache reads), 20% SET, 10% DELETE — realistic read-heavy workload.

**Improve cycle** (documented in README):
1. Run k6 → watch Grafana: hot node? LRU thrashing? router latency?
2. Tune `CAPACITY`, `VIRTUAL_NODES`, `HASH_STRATEGY` in `docker-compose.env`
3. Rebuild + re-run k6 → compare hit rate and latency curves

---

## Verification

**`scripts/verify.sh`** — runs each PROMPT.md curl test with assertion:
```
✓ SET foo=bar (ttl=30) → node assigned, status ok
✓ GET foo → value=bar, ttl_remaining > 0
✓ GET missing → 404, error=miss
✓ DELETE foo → status=deleted
✓ GET foo after delete → 404, error=miss
✓ TTL expiry → SET with ttl=2, sleep 3, GET → 404, error=expired
✓ Ring inspection → /ring/foo returns node + virtual_nodes
✓ Stats → hits/misses/evictions are integers
```

All 8 checks must pass before load testing.

---

## Docker Compose Topology

```yaml
services:
  dc-router:
    ports: ["8000:8000"]
    environment: [NODE_URLS=http://dc-node1:8001,http://dc-node2:8002,http://dc-node3:8003, HASH_STRATEGY=ring, VIRTUAL_NODES=150]

  dc-node1:
    ports: ["8001:8001"]
    environment: [NODE_ID=node1, CAPACITY=100, NODE_PORT=8001]

  dc-node2:
    ports: ["8002:8002"]
    environment: [NODE_ID=node2, CAPACITY=100, NODE_PORT=8002]

  dc-node3:
    ports: ["8003:8003"]
    environment: [NODE_ID=node3, CAPACITY=100, NODE_PORT=8003]

networks:
  sd_monitoring:
    external: true
```

---

## Out of Scope (future phases from PROMPT.md)

- Replication / fault tolerance
- Persistence (RDB snapshots)
- Gossip-based cluster membership
- Read-through / write-through caching
