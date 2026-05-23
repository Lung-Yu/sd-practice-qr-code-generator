# Distributed Cache — Sync Replication Design

**Date:** 2026-05-23
**Feature:** Router-layer synchronous replication (RF=2)
**Scope:** `distributed_cache/scaffold/go_router/`

---

## Goal

Eliminate the cache miss storm that occurs when a node fails. Currently, node failure causes ~1/3 of keys to return 404 (AP choice). With replication, the router falls back to the replica, keeping those keys as hits until the primary recovers.

---

## Consistency Model

**Writes:** Synchronous, RF=2. Router writes to primary AND replica in parallel; both must succeed before returning 200. If either is unreachable (CB open or TCP error), the write returns `503 replication_failed`. Only one node in ring → degrade to single-node write (no 503).

**Reads:** Try primary first. If primary's circuit breaker is open, try replica. If both CBs open → `503 no_nodes_available`.

**Deletes:** Same as writes — parallel delete from both; either failure → 503.

---

## Architecture

Nodes are completely unchanged. All replication logic lives in the router.

```
SET /cache/key4
  ring.nodesForKey("key4", 2) → [node2 (primary), node3 (replica)]
  goroutine → node2  POST /cache/key4 ─┐
  goroutine → node3  POST /cache/key4 ─┴─ both 200 → 200
                                          any fail  → 503 replication_failed

GET /cache/key4  (node2 CB open)
  ring.nodesForKey("key4", 2) → [node2, node3]
  node2 CB open → skip
  node3          → 200 hit  ← miss storm eliminated
```

Replica selection: the next distinct physical node clockwise on the ring from primary. With 3 nodes this is always deterministic and requires no configuration.

---

## Code Changes

### `ring.go` — new `nodesForKey`

```go
// Returns up to n distinct physical nodes for key. [0]=primary, [1]=replica, ...
func (r *hashRing) nodesForKey(key string, n int) []string
```

Internal helpers:
- `ringNodes(key, n)` — bisect to start position, walk clockwise skipping duplicate physical nodes
- `rendezvousNodes(key, n)` — rank all nodes by `hash(nodeID+":"+key)`, return top-n

Existing `nodeForKey()` is kept for the `/ring/{key}` handler (backward compat).

### `main.go` — three changes

**(1) `callNode()` helper**

New internal function that runs a single proxied request and returns a result struct instead of writing to `http.ResponseWriter`. Circuit breaker logic moves here.

```go
type nodeResult struct {
    status  int
    body    []byte
    headers http.Header
    errMsg  string  // "circuit_open" | "node_unreachable" | ""
}

func callNode(ctx context.Context, nodeID, method, targetURL string,
              body []byte, header http.Header) nodeResult
```

`proxy()` is refactored to call `callNode()` and write the result to `w` — no duplicated CB logic.

**(2) `handleSet` / `handleDelete` — parallel replication**

```
1. ring.nodesForKey(key, 2) → nodes
2. io.ReadAll(r.Body)       → reqBody  (must replay to both)
3. if len(nodes) == 1: callNode primary → write to w (degraded, no replica)
4. goroutine × 2: callNode primary, callNode replica
5. any errMsg != "" or status >= 500 → 503 replication_failed {node: failing_node}
6. both ok → write primary response to w
```

**(3) `handleGet` — replica fallback**

```
1. ring.nodesForKey(key, 2) → nodes
2. for each node in order:
     result = callNode(node, ...)
     if result.errMsg == "circuit_open": continue
     write result to w; return
3. all nodes exhausted → 503 no_nodes_available
```

**(4) `handleRing` — add `replica` field**

`node` field kept (= primary, backward compat). New `replica` field added.

```json
{"key":"key4","node":"node2","replica":"node3","virtual_nodes":150}
```

---

## Test Plan — `scripts/test_replication.sh`

**Phase 1 — Preflight + ring inspection**
- Ensure all 3 nodes alive
- Seed 30 keys
- Find a key on node2; confirm `/ring/keyN` shows `primary=node2, replica=node3`

**Phase 2 — Kill primary, read from replica**
- `podman stop dc-node2`
- Wait for CB to open (3 requests to node2 key)
- GET that key → must return **200** (from replica, not 404)
- GET all 30 keys → 0 errors (some from replica, rest from primary)

**Phase 3 — Write fails with primary down**
- SET a new key → must return **503 replication_failed** (cannot satisfy RF=2)

**Phase 4 — Recovery**
- `podman start dc-node2`, wait 6s
- SET + GET both work again
- `/ring` shows node2 back as primary

---

## What Does NOT Change

| File | Status |
|------|--------|
| `go_node/` | Unchanged — nodes have no replica awareness |
| `circuit_breaker.go` | Unchanged |
| `docker-compose.go-full.yml` | Unchanged — no new services |
| `scripts/verify.sh` | Unchanged — `/ring` response is backward compat |

---

## Tradeoffs Accepted

| Tradeoff | Decision |
|----------|----------|
| Write latency | +~0ms (parallel writes, latency ≈ max(p1,p2)) |
| Write availability | Reduced — primary OR replica down → 503 |
| Read availability | Improved — primary down → read from replica |
| Data durability | Stronger — survives single node failure without miss |
| Node complexity | Zero — nodes unchanged |
