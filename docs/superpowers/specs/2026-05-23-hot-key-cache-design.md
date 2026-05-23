# Distributed Cache — Hot Key Local Cache Design

**Date:** 2026-05-23
**Feature:** Router-layer L1 hot-key cache with three selectable promotion strategies
**Scope:** `distributed_cache/scaffold/go_router/`

---

## Goal

Eliminate the node hop for the most-read keys by maintaining a small in-memory L1 cache directly in the router. When a key becomes "hot" (frequently read), its value is cached at the router layer; subsequent GETs return immediately without forwarding to any cache node.

---

## Architecture

```
GET /cache/key
  ↓
Router L1 (local_cache.go)    LOCAL_CACHE_SIZE=100, LOCAL_CACHE_STRATEGY=counter|lfu|periodic
  hit → return immediately (zero node hop)
  miss ↓
Cache nodes L2 (existing RF=2 + replica fallback)
  hit → return + RecordHit (may promote to L1)
  miss → 404
```

All replication, circuit breaker, write-through, and health-check logic is unchanged. L1 is a pure read optimisation.

**New env vars:**

| Var | Default | Values |
|-----|---------|--------|
| `LOCAL_CACHE_SIZE` | `100` | any positive integer |
| `LOCAL_CACHE_STRATEGY` | `counter` | `counter`, `lfu`, `periodic` |
| `LOCAL_CACHE_REBUILD_INTERVAL` | `10` | seconds; only used by `periodic` strategy |

If `LOCAL_CACHE_SIZE=0`, L1 is disabled and the router behaves exactly as today.

---

## Interface

New file: `distributed_cache/scaffold/go_router/local_cache.go`

```go
type LocalCacheStats struct {
    Hits     int64
    Misses   int64
    Size     int
    Capacity int
    Strategy string
}

type LocalCache interface {
    Get(key string) ([]byte, bool)      // L1 hit → raw response bytes; miss → false
    RecordHit(key string, val []byte)   // called on every L2 hit (200); may promote
    Invalidate(key string)              // called on every SET and DELETE
    Stats() LocalCacheStats
}

func newLocalCache(strategy string, capacity int, rebuildInterval time.Duration) LocalCache
```

`main.go` constructs one `LocalCache` at startup and wires it into `handleGet`, `handleSet`, and `handleDelete`.

---

## Three Strategies

### `counter` — counter map + min-tracking

```
fields:
  counters  map[string]uint64          all keys ever seen → hit count
  cache     map[string]counterEntry    at most K live entries
  minKey    string                     weakest entry in cache
  minCount  uint64
  mu        sync.RWMutex

counterEntry { value []byte; count uint64; expiresAt time.Time }

RecordHit(key, val):
  mu.Lock()
  counters[key]++
  if key in cache: refresh value + expiresAt; no eviction
  elif len(cache) < K: insert; rescanMin()
  elif counters[key] > minCount: delete cache[minKey]; insert key; rescanMin()
  mu.Unlock()

rescanMin(): linear O(K) scan of cache to find new minKey/minCount
```

- **Reads:** O(1) map lookup (RLock)
- **Promotion:** O(K) on eviction (linear rescan)
- **Memory:** `counters` grows with unique key count (bounded by key space)

### `lfu` — exact LFU O(1)

```
fields:
  keyMap    map[string]*lfuEntry        key → entry (freq, value, expiresAt)
  freqMap   map[uint64]*freqBucket      freq → doubly-linked list of keys
  minFreq   uint64
  size      int
  capacity  int
  mu        sync.RWMutex

RecordHit(key, val):
  if key in keyMap:
    move entry from freqMap[freq] to freqMap[freq+1]
    if freqMap[freq] empty: delete freqMap[freq]; if freq == minFreq: minFreq++
  else:
    if size == capacity: evict tail of freqMap[minFreq]; size--
    insert key at freqMap[1]; minFreq = 1; size++
```

- **All operations:** O(1)
- **Memory:** O(K) — only tracks keys currently in cache
- **Pattern:** same doubly-linked-list structure as LRU nodes (direct comparison material)

### `periodic` — batch recompute

```
periodicEntry { value []byte; expiresAt time.Time }

fields:
  counters  atomic map (sync.Map, key → *atomic.Uint64)
  snapshot  map[string]periodicEntry    current L1; swapped atomically
  snapMu    sync.RWMutex
  interval  time.Duration

RecordHit(key, val):
  counters.LoadOrStore(key, new(atomic.Uint64)).Add(1)   // lock-free

Background goroutine (every interval):
  1. Collect all (key, count) pairs from counters
  2. Sort descending by count; take top K
  3. For each top-K key not already in snapshot:
       fetch fresh value from cache nodes (GET to L2)
  4. Build new snapshot; swap under snapMu.Lock()
```

- **Read path:** zero lock contention (RLock on snapMu only)
- **Promotion path:** fully async, no per-request latency
- **Consistency:** L1 can lag up to `rebuildInterval` (default 10s)
- **Extra complexity:** background goroutine makes L2 reads to fill snapshot

---

## TTL Handling

L1 entries carry `expiresAt time.Time`. On `RecordHit`, the router parses `ttl_remaining` from the node's JSON response:

```go
// extract ttl_remaining from node response JSON (best-effort; 0 = no TTL)
func parseTTL(body []byte) int {
    var r struct{ TTLRemaining int `json:"ttl_remaining"` }
    json.Unmarshal(body, &r)
    return r.TTLRemaining
}
```

If `ttl_remaining > 0`: `expiresAt = time.Now().Add(time.Duration(ttl_remaining) * time.Second)`
If `ttl_remaining == 0`: `expiresAt = time.Time{}` (zero = no expiry; entry lives until evicted or invalidated)

`Get()` checks `!expiresAt.IsZero() && time.Now().After(expiresAt)` → treat as miss + evict.

---

## Router Changes (`main.go`)

### `handleGet`

```
1. res, ok := localCache.Get(key)
   if ok → write res to w; increment L1 hit metric; return
2. [existing: iterate nodes, callNode, replica fallback]
3. on node 200: localCache.RecordHit(key, res.body)
4. write result to w
```

### `handleSet` and `handleDelete`

After writing to cache nodes (and store if write-through enabled):
```
localCache.Invalidate(key)
```

### `/health` update

Add `local_cache` field:
```json
{
  "status": "ok",
  "nodes": { ... },
  "store": { ... },
  "local_cache": {
    "strategy": "counter",
    "size": 42,
    "capacity": 100,
    "hits": 1500,
    "misses": 300
  }
}
```

### Prometheus metrics

```go
localCacheHitsTotal   = promauto.NewCounter(...)   // "cache_l1_hits_total"
localCacheMissesTotal = promauto.NewCounter(...)   // "cache_l1_misses_total"
localCacheSizeGauge   = promauto.NewGauge(...)     // "cache_l1_size"
```

---

## Test Plan — `scripts/test_hot_key.sh`

**Phase 1 — warm-up Top-K (counter mode default)**
- Restart router in `counter` mode; seed 20 keys
- Hit `key1` 50× → check `/health` shows `local_cache.size >= 1`
- Check `key1` count in health response or via Prometheus

**Phase 2 — L1 hit with nodes down**
- Stop all three cache nodes (`dc-node1/2/3`)
- GET `key1` → must return **200** (from L1, no node hop)
- GET `key20` (cold) → must return **503** (not in L1, nodes down)
- Restart nodes

**Phase 3 — invalidation on SET**
- SET `key1` new value → `localCache.Invalidate` fires
- GET `key1` → must return **new value** (L1 was cleared)

**Phase 4 — LFU strategy**
- Restart router with `LOCAL_CACHE_STRATEGY=lfu`
- Repeat Phase 1 + Phase 2; same pass criteria

**Phase 5 — periodic strategy**
- Restart router with `LOCAL_CACHE_STRATEGY=periodic`, `LOCAL_CACHE_REBUILD_INTERVAL=3`
- Hit `key1` 50× → wait 4s for rebuild goroutine
- Stop nodes → GET `key1` must return **200**

**Phase 6 — TTL expiry**
- SET `key1` with `ttl=2`; hit it 50× to promote
- Wait 3s → GET `key1` should return from L2 (L1 expired), then re-promote

---

## What Does NOT Change

| Component | Status |
|-----------|--------|
| Cache nodes (`go_node/`) | Unchanged |
| Ring, circuit breaker | Unchanged |
| Write-through (`callStore`, modes) | Unchanged — Invalidate is additive |
| `verify.sh` | Unchanged — L1 is transparent to GET semantics |

---

## Trade-offs

| Trade-off | Decision |
|-----------|----------|
| Multi-router invalidation | Not addressed — single router; multi-router needs pub/sub invalidation |
| Counter map unbounded growth | Acceptable for finite key spaces; add periodic cleanup if needed |
| `periodic` node reads during rebuild | Uses same `proxyClient`; counts as normal GET traffic |
| L1 disabled when `LOCAL_CACHE_SIZE=0` | Clean off-switch for benchmarking |
