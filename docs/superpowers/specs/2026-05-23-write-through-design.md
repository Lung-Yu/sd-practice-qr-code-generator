# Distributed Cache — Write-Through Design

**Date:** 2026-05-23
**Feature:** Write-through cache with configurable consistency modes
**Scope:** `distributed_cache/scaffold/go_router/` + new `go_store/`

---

## Goal

Add a durable backing store behind the cache so that every write is persisted beyond the in-memory nodes. Implement three write-through modes — `parallel`, `store_first`, `cache_first` — selectable by env var, so the trade-offs between consistency, availability, and durability can be observed experimentally.

GET cache misses remain 404 (no read-through). The store is the source of truth for writes; the cache is still the read path.

---

## Architecture

```
Client
  ↓ HTTP :8000
dc-router   STORE_URL=http://dc-store:8004
            WRITE_THROUGH_MODE=parallel|store_first|cache_first
  ↓ cache writes (RF=2, existing)     ↓ store writes (new)
dc-node1/2/3                      dc-store :8004
  (in-memory LRU, ephemeral)       (in-memory map, "durable DB")
```

Nodes and the store are completely unchanged in their internal logic. All write-through coordination lives in the router.

If `STORE_URL` is unset, the router behaves exactly as today (write-through disabled).

---

## New Service: `go_store`

**Location:** `distributed_cache/scaffold/go_store/main.go`

Single-file Go HTTP server. Plain KV store simulating a durable database — no TTL, no LRU, no eviction.

**Endpoints:**

| Method | Path | Success | Not found |
|--------|------|---------|-----------|
| `POST /store/{key}` | Store value | 200 | — |
| `GET /store/{key}` | Retrieve value | 200 | 404 |
| `DELETE /store/{key}` | Delete value | 200 | 200 (idempotent) |

**Implementation:** `sync.RWMutex`-protected `map[string][]byte`. Request/response body format matches the cache nodes (`{"value":"...","ttl":N}` on write; `{"value":"..."}` on read) so the router can forward the same bytes unchanged.

**Docker:** Added to `docker-compose.go-full.yml` as `dc-store`, port 8004, same image as router/nodes.

---

## Router Changes

### New env vars

| Var | Default | Values |
|-----|---------|--------|
| `STORE_URL` | `""` (disabled) | `http://dc-store:8004` |
| `WRITE_THROUGH_MODE` | `parallel` | `parallel`, `store_first`, `cache_first` |

### `callStore(ctx, method, key, body) nodeResult`

New helper analogous to `callNode`. Sends `method` to `STORE_URL/store/{key}` with `body`. Returns `nodeResult` with the same struct. No circuit breaker (store is a single instance; failures surface directly as `node_unreachable`).

### Write modes (SET and DELETE)

**`parallel`**
```
goroutine A: callNode(primary) + callNode(replica) in parallel
goroutine B: callStore(store)
WaitGroup — wait for A and B
any errMsg != "" or status >= 500 → 503 {"error":"write_through_failed","failed":"store"|"cache"}
all succeed → 200 (primary cache response)
```

**`store_first`**
```
1. callStore(store) → errMsg != "" or status >= 500 → 503 (abort, nothing written to cache)
2. callNode(primary) + callNode(replica) in parallel
   any failure → log warning, return 200 (store written; cache cold)
```

**`cache_first`**
```
1. callNode(primary) + callNode(replica) in parallel → any failure → 503 (abort, store not written)
2. callStore(store) → any failure → log warning, return 200 (cache written; store stale)
```

GET (`handleGet`) is unchanged — miss → 404, no read-through.

### `/health` endpoint update

Add `store` entry alongside nodes:
```json
{
  "status": "ok",
  "nodes": { ... },
  "store": { "alive": true, "write_through_mode": "parallel" }
}
```

---

## Failure Matrix

| Mode | Cache node down | Store down |
|------|----------------|------------|
| `parallel` | 503 `write_through_failed` | 503 `write_through_failed` |
| `store_first` | 200 (cache cold, data in store) | 503 (abort) |
| `cache_first` | 503 (abort) | 200 (store stale, data in cache) |

---

## Test Plan — `scripts/test_write_through.sh`

Router is restarted with each `WRITE_THROUGH_MODE` override between phases.

**Phase 1 — Happy path**
- Start with `parallel` mode, all services alive
- Seed 10 keys → all 200
- GET all 10 → all 200 (from cache)
- Verify store has them: `GET dc-store:8004/store/keyN` → 200

**Phase 2 — `parallel` mode: store down**
- `podman stop dc-store`
- SET a new key → must return **503**
- Restart store → SET succeeds

**Phase 3 — `store_first` mode: cache node down**
- Restart router with `WRITE_THROUGH_MODE=store_first`
- `podman stop dc-node2`; wait for CB to open
- SET a key that hashes to node2 → must return **200**
- `GET dc-store:8004/store/keyN` → **200** (store has it)
- `GET router:8000/cache/keyN` → **404** (cache cold)

**Phase 4 — `cache_first` mode: store down**
- Restart router with `WRITE_THROUGH_MODE=cache_first`
- `podman stop dc-store`
- SET a key → must return **200**
- `GET router:8000/cache/keyN` → **200** (in cache)
- `GET dc-store:8004/store/keyN` → **404** (store stale)

**Phase 5 — Recovery**
- Restart store; restart dc-node2; restart router in `parallel` mode
- Verify all services alive via `/health`
- SET + GET work normally

---

## What Does NOT Change

| File | Status |
|------|--------|
| `go_node/` | Unchanged |
| `circuit_breaker.go` | Unchanged — no CB on store (single instance, direct fail) |
| `ring.go` | Unchanged |
| `scripts/verify.sh` | Unchanged — write-through is additive |

---

## Trade-offs Accepted

| Trade-off | Decision |
|-----------|----------|
| Read path | No read-through; miss → 404 always |
| Store circuit breaker | Omitted; store failure surfaces as direct 503 or warning log |
| Store TTL | Omitted; store holds data forever (simulates durable DB) |
| Mode switching | Requires router restart (env var); no hot-reload |
| Store state on recovery | No backfill; store retains whatever was written before failure |
