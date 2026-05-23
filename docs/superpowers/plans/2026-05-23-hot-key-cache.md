# Hot Key Local Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an in-router L1 hot-key cache that serves the most-read keys directly from memory with no node hop, with three selectable promotion strategies (counter, lfu, periodic).

**Architecture:** A new `LocalCache` interface (in `local_cache.go`) wraps three independent implementations; the router's `handleGet` checks L1 first, calls `RecordHit` on every L2 hit, and `handleSet`/`handleDelete` always call `Invalidate`. Strategy and capacity are selected at startup via env vars; `LOCAL_CACHE_SIZE=0` disables L1 entirely.

**Tech Stack:** Go 1.22, `container/list` (stdlib) for LFU doubly-linked lists, `sync/atomic` + `sync.Map` for lock-free periodic counters, `github.com/prometheus/client_golang` (already a dependency).

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `go_router/local_cache.go` | `LocalCache` interface + all three strategy implementations |
| Create | `go_router/local_cache_test.go` | Unit tests for all four implementations (noop + 3 strategies) |
| Modify | `go_router/main.go` | Wire L1 into `handleGet`, `handleSet`, `handleDelete`, `handleHealth`, `main()` |
| Modify | `docker-compose.go-full.yml` | Add `LOCAL_CACHE_SIZE`, `LOCAL_CACHE_STRATEGY`, `LOCAL_CACHE_REBUILD_INTERVAL` |
| Create | `scripts/test_hot_key.sh` | 6-phase integration test |

---

## Task 1: Foundation — interface, noop, parseTTL

**Files:**
- Create: `distributed_cache/scaffold/go_router/local_cache.go`
- Create: `distributed_cache/scaffold/go_router/local_cache_test.go`

- [ ] **Step 1: Create `local_cache.go` with interface + noop + parseTTL**

```go
package main

import (
	"encoding/json"
	"time"
)

// ── types ─────────────────────────────────────────────────────────────────────

type LocalCacheStats struct {
	Hits     int64  `json:"hits"`
	Misses   int64  `json:"misses"`
	Size     int    `json:"size"`
	Capacity int    `json:"capacity"`
	Strategy string `json:"strategy"`
}

type LocalCache interface {
	Get(key string) ([]byte, bool)     // L1 hit → cached bytes; miss → false
	RecordHit(key string, val []byte)  // called on every L2 200; may promote to L1
	Invalidate(key string)             // called on every SET and DELETE
	Stats() LocalCacheStats
}

// parseTTL extracts ttl_remaining from a node GET response (best-effort; 0 = no TTL).
func parseTTL(body []byte) int {
	var r struct {
		TTLRemaining int `json:"ttl_remaining"`
	}
	json.Unmarshal(body, &r)
	return r.TTLRemaining
}

// newLocalCache constructs the appropriate LocalCache for the given strategy.
// capacity=0 returns a no-op cache (L1 disabled).
// fetchFn is only used by the "periodic" strategy to refresh values from L2.
func newLocalCache(strategy string, capacity int, rebuildInterval time.Duration, fetchFn func(string) ([]byte, bool)) LocalCache {
	if capacity == 0 {
		return &noopCache{}
	}
	switch strategy {
	case "lfu":
		return &noopCache{} // implemented in Task 3
	case "periodic":
		return &noopCache{} // implemented in Task 4
	default: // "counter"
		return &noopCache{} // implemented in Task 2
	}
}

// ── disabled (capacity=0) ─────────────────────────────────────────────────────

type noopCache struct{}

func (c *noopCache) Get(_ string) ([]byte, bool)  { return nil, false }
func (c *noopCache) RecordHit(_ string, _ []byte) {}
func (c *noopCache) Invalidate(_ string)          {}
func (c *noopCache) Stats() LocalCacheStats       { return LocalCacheStats{Strategy: "disabled"} }
```

- [ ] **Step 2: Create `local_cache_test.go` with makeVal helper + noop test**

```go
package main

import (
	"encoding/json"
	"testing"
	"time"
)

// makeVal returns a JSON body that mimics a node GET response.
func makeVal(value string, ttlSec int) []byte {
	m := map[string]any{"value": value}
	if ttlSec > 0 {
		m["ttl_remaining"] = ttlSec
	}
	b, _ := json.Marshal(m)
	return b
}

// ── noopCache ─────────────────────────────────────────────────────────────────

func TestNoopCacheAlwaysMisses(t *testing.T) {
	c := newLocalCache("counter", 0, 0, nil) // capacity=0 → noop
	c.RecordHit("k", makeVal("v", 0))
	if _, ok := c.Get("k"); ok {
		t.Fatal("noop cache should never hit")
	}
	if s := c.Stats(); s.Strategy != "disabled" {
		t.Fatalf("expected strategy=disabled, got %q", s.Strategy)
	}
}

// time is imported but not used yet; will be used in TTL tests added in Tasks 2–4.
var _ = time.Second
```

- [ ] **Step 3: Build and run the noop test**

```bash
cd distributed_cache/scaffold/go_router
go test ./... -v -run TestNoop
```

Expected output:
```
=== RUN   TestNoopCacheAlwaysMisses
--- PASS: TestNoopCacheAlwaysMisses (0.00s)
PASS
```

- [ ] **Step 4: Commit**

```bash
git add distributed_cache/scaffold/go_router/local_cache.go \
        distributed_cache/scaffold/go_router/local_cache_test.go
git commit -m "feat(l1-cache): add LocalCache interface, noop impl, parseTTL"
```

---

## Task 2: counter strategy

**Files:**
- Modify: `distributed_cache/scaffold/go_router/local_cache.go` (append counterCache; update newLocalCache)
- Modify: `distributed_cache/scaffold/go_router/local_cache_test.go` (append counter tests)

- [ ] **Step 1: Write failing counter tests**

Append to `local_cache_test.go` (after the noop section):

```go
// ── counterCache ─────────────────────────────────────────────────────────────

func TestCounterCacheMissOnEmpty(t *testing.T) {
	c := newCounterCache(5)
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss on empty cache")
	}
}

func TestCounterCachePromotesAfterHits(t *testing.T) {
	c := newCounterCache(3)
	val := makeVal("hello", 0)
	for i := 0; i < 5; i++ {
		c.RecordHit("key1", val)
	}
	got, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit after RecordHit calls")
	}
	if string(got) != string(val) {
		t.Fatalf("got %q, want %q", got, val)
	}
}

func TestCounterCacheEvictsMin(t *testing.T) {
	c := newCounterCache(2)
	v := makeVal("v", 0)
	c.RecordHit("a", v) // a: count=1
	c.RecordHit("b", v) // b: count=1
	for i := 0; i < 10; i++ {
		c.RecordHit("b", v) // b: count=11
	}
	// c hits 3 times → count=3 > minCount(a=1) → evict a, insert c
	for i := 0; i < 3; i++ {
		c.RecordHit("c", v)
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("a (count=1, min) should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b should still be in cache")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("c should be promoted into cache")
	}
}

func TestCounterCacheInvalidate(t *testing.T) {
	c := newCounterCache(5)
	val := makeVal("v", 0)
	for i := 0; i < 3; i++ {
		c.RecordHit("k", val)
	}
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit before invalidate")
	}
	c.Invalidate("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after invalidate")
	}
}

func TestCounterCacheTTLExpiry(t *testing.T) {
	c := newCounterCache(5)
	val := makeVal("v", 1) // ttl_remaining=1 second
	for i := 0; i < 3; i++ {
		c.RecordHit("k", val)
	}
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit immediately after RecordHit")
	}
	time.Sleep(1100 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after TTL expiry")
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
cd distributed_cache/scaffold/go_router
go test ./... -run TestCounter 2>&1 | head -5
```

Expected: `undefined: newCounterCache`

- [ ] **Step 3: Implement counterCache**

Add to `local_cache.go` — update the import block first (replace the existing `import` block):

```go
import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)
```

Then append the following after the `noopCache` section:

```go
// ── counter strategy — counter map + min-tracking ────────────────────────────

type counterEntry struct {
	value     []byte
	count     uint64
	expiresAt time.Time
}

type counterCache struct {
	mu       sync.RWMutex
	counters map[string]uint64
	cache    map[string]*counterEntry
	capacity int
	minKey   string
	minCount uint64
	hits     int64
	misses   int64
}

func newCounterCache(capacity int) *counterCache {
	return &counterCache{
		counters: make(map[string]uint64),
		cache:    make(map[string]*counterEntry),
		capacity: capacity,
		minCount: ^uint64(0),
	}
}

func (c *counterCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	e, ok := c.cache[key]
	if !ok {
		c.mu.RUnlock()
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	expired := !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
	var val []byte
	if !expired {
		val = e.value
	}
	c.mu.RUnlock()

	if expired {
		c.mu.Lock()
		if e2, ok2 := c.cache[key]; ok2 && !e2.expiresAt.IsZero() && time.Now().After(e2.expiresAt) {
			delete(c.cache, key)
			c.rescanMin()
		}
		c.mu.Unlock()
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	atomic.AddInt64(&c.hits, 1)
	return val, true
}

func (c *counterCache) RecordHit(key string, val []byte) {
	ttl := parseTTL(val)
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(time.Duration(ttl) * time.Second)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.counters[key]++
	count := c.counters[key]

	if e, ok := c.cache[key]; ok {
		e.value = val
		e.count = count
		e.expiresAt = expiresAt
		return
	}
	if len(c.cache) < c.capacity {
		c.cache[key] = &counterEntry{value: val, count: count, expiresAt: expiresAt}
		c.rescanMin()
		return
	}
	if count > c.minCount {
		delete(c.cache, c.minKey)
		c.cache[key] = &counterEntry{value: val, count: count, expiresAt: expiresAt}
		c.rescanMin()
	}
}

// rescanMin does a linear O(K) scan to find the new minimum entry.
func (c *counterCache) rescanMin() {
	c.minKey = ""
	c.minCount = ^uint64(0)
	for k, e := range c.cache {
		if e.count < c.minCount {
			c.minCount = e.count
			c.minKey = k
		}
	}
}

func (c *counterCache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.cache[key]; ok {
		delete(c.cache, key)
		c.rescanMin()
	}
}

func (c *counterCache) Stats() LocalCacheStats {
	c.mu.RLock()
	size := len(c.cache)
	c.mu.RUnlock()
	return LocalCacheStats{
		Hits:     atomic.LoadInt64(&c.hits),
		Misses:   atomic.LoadInt64(&c.misses),
		Size:     size,
		Capacity: c.capacity,
		Strategy: "counter",
	}
}
```

Also update `newLocalCache` — replace the `default` branch:

```go
	default: // "counter"
		return newCounterCache(capacity)
```

- [ ] **Step 4: Run counter tests**

```bash
cd distributed_cache/scaffold/go_router
go test ./... -v -run TestCounter -timeout 10s
```

Expected: all 5 counter tests PASS (TestCounterCacheTTLExpiry takes ~1.1 s).

- [ ] **Step 5: Confirm existing tests still pass**

```bash
go test ./... -v -timeout 15s
```

Expected: TestNoopCacheAlwaysMisses + all TestCounter* + TestNodesForKey* PASS.

- [ ] **Step 6: Commit**

```bash
git add distributed_cache/scaffold/go_router/local_cache.go \
        distributed_cache/scaffold/go_router/local_cache_test.go
git commit -m "feat(l1-cache): add counter strategy (counter map + min-tracking)"
```

---

## Task 3: LFU strategy

**Files:**
- Modify: `distributed_cache/scaffold/go_router/local_cache.go`
- Modify: `distributed_cache/scaffold/go_router/local_cache_test.go`

- [ ] **Step 1: Write failing LFU tests**

Append to `local_cache_test.go` (after the counter section):

```go
// ── lfuCache ─────────────────────────────────────────────────────────────────

func TestLFUCacheMissOnEmpty(t *testing.T) {
	c := newLFUCache(5)
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss on empty cache")
	}
}

func TestLFUCachePromotesFrequency(t *testing.T) {
	c := newLFUCache(3)
	val := makeVal("v", 0)
	c.RecordHit("k", val)
	c.RecordHit("k", val) // freq=2
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got) != string(val) {
		t.Fatalf("got %q, want %q", got, val)
	}
	if c.Stats().Size != 1 {
		t.Fatalf("expected size=1, got %d", c.Stats().Size)
	}
}

func TestLFUCacheEvictsLeastFrequent(t *testing.T) {
	c := newLFUCache(2)
	v := makeVal("v", 0)
	c.RecordHit("a", v) // a: freq=1
	c.RecordHit("b", v) // b: freq=1
	c.RecordHit("a", v) // a: freq=2
	// capacity=2 full; inserting c must evict b (freq=1, LFU)
	c.RecordHit("c", v)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b (freq=1, LFU) should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a (freq=2) should remain")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("c (new) should be in cache")
	}
}

func TestLFUCacheInvalidate(t *testing.T) {
	c := newLFUCache(5)
	v := makeVal("v", 0)
	c.RecordHit("k", v)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit before invalidate")
	}
	c.Invalidate("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after invalidate")
	}
	if c.Stats().Size != 0 {
		t.Fatalf("expected size=0, got %d", c.Stats().Size)
	}
}

func TestLFUCacheTTLExpiry(t *testing.T) {
	c := newLFUCache(5)
	val := makeVal("v", 1) // ttl_remaining=1 second
	c.RecordHit("k", val)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit immediately after RecordHit")
	}
	time.Sleep(1100 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after TTL expiry")
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
cd distributed_cache/scaffold/go_router
go test ./... -run TestLFU 2>&1 | head -5
```

Expected: `undefined: newLFUCache`

- [ ] **Step 3: Implement lfuCache**

Update the import block in `local_cache.go` to add `container/list`:

```go
import (
	"container/list"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)
```

Append to `local_cache.go` after the counter section:

```go
// ── LFU strategy — exact O(1) LFU with freq buckets ─────────────────────────

type lfuEntry struct {
	key       string
	value     []byte
	freq      uint64
	expiresAt time.Time
	listElem  *list.Element // position within freqMap[freq]
}

type lfuCache struct {
	mu       sync.RWMutex
	keyMap   map[string]*lfuEntry   // key → entry
	freqMap  map[uint64]*list.List  // freq → MRU-ordered list of *lfuEntry
	minFreq  uint64
	size     int
	capacity int
	hits     int64
	misses   int64
}

func newLFUCache(capacity int) *lfuCache {
	return &lfuCache{
		keyMap:   make(map[string]*lfuEntry),
		freqMap:  make(map[uint64]*list.List),
		capacity: capacity,
	}
}

func (c *lfuCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	e, ok := c.keyMap[key]
	if !ok {
		c.mu.RUnlock()
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	expired := !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
	var val []byte
	if !expired {
		val = e.value
	}
	c.mu.RUnlock()

	if expired {
		c.mu.Lock()
		if e2, ok2 := c.keyMap[key]; ok2 && !e2.expiresAt.IsZero() && time.Now().After(e2.expiresAt) {
			c.removeEntry(e2)
		}
		c.mu.Unlock()
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	atomic.AddInt64(&c.hits, 1)
	return val, true
}

// removeEntry removes e from both keyMap and freqMap. Must hold write lock.
func (c *lfuCache) removeEntry(e *lfuEntry) {
	bucket := c.freqMap[e.freq]
	bucket.Remove(e.listElem)
	if bucket.Len() == 0 {
		delete(c.freqMap, e.freq)
	}
	delete(c.keyMap, e.key)
	c.size--
}

// promoteEntry moves e from freqMap[freq] to freqMap[freq+1]. Must hold write lock.
func (c *lfuCache) promoteEntry(e *lfuEntry) {
	oldFreq := e.freq
	newFreq := oldFreq + 1

	bucket := c.freqMap[oldFreq]
	bucket.Remove(e.listElem)
	if bucket.Len() == 0 {
		delete(c.freqMap, oldFreq)
		if c.minFreq == oldFreq {
			c.minFreq = newFreq
		}
	}

	e.freq = newFreq
	if c.freqMap[newFreq] == nil {
		c.freqMap[newFreq] = list.New()
	}
	e.listElem = c.freqMap[newFreq].PushFront(e)
}

func (c *lfuCache) RecordHit(key string, val []byte) {
	ttl := parseTTL(val)
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(time.Duration(ttl) * time.Second)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.keyMap[key]; ok {
		c.promoteEntry(e)
		e.value = val
		e.expiresAt = expiresAt
		return
	}

	if c.size == c.capacity {
		if bucket := c.freqMap[c.minFreq]; bucket != nil && bucket.Len() > 0 {
			tail := bucket.Back().Value.(*lfuEntry)
			c.removeEntry(tail)
		}
	}

	e := &lfuEntry{key: key, value: val, freq: 1, expiresAt: expiresAt}
	if c.freqMap[1] == nil {
		c.freqMap[1] = list.New()
	}
	e.listElem = c.freqMap[1].PushFront(e)
	c.keyMap[key] = e
	c.minFreq = 1
	c.size++
}

func (c *lfuCache) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.keyMap[key]; ok {
		c.removeEntry(e)
	}
}

func (c *lfuCache) Stats() LocalCacheStats {
	c.mu.RLock()
	size := c.size
	c.mu.RUnlock()
	return LocalCacheStats{
		Hits:     atomic.LoadInt64(&c.hits),
		Misses:   atomic.LoadInt64(&c.misses),
		Size:     size,
		Capacity: c.capacity,
		Strategy: "lfu",
	}
}
```

Also update `newLocalCache` — replace the `"lfu"` branch:

```go
	case "lfu":
		return newLFUCache(capacity)
```

- [ ] **Step 4: Run LFU tests**

```bash
cd distributed_cache/scaffold/go_router
go test ./... -v -run TestLFU -timeout 10s
```

Expected: all 5 LFU tests PASS.

- [ ] **Step 5: Confirm all tests still pass**

```bash
go test ./... -timeout 20s
```

Expected: PASS (all counter, LFU, noop, ring tests).

- [ ] **Step 6: Commit**

```bash
git add distributed_cache/scaffold/go_router/local_cache.go \
        distributed_cache/scaffold/go_router/local_cache_test.go
git commit -m "feat(l1-cache): add LFU strategy (O(1) freq buckets + doubly-linked lists)"
```

---

## Task 4: periodic strategy

**Files:**
- Modify: `distributed_cache/scaffold/go_router/local_cache.go`
- Modify: `distributed_cache/scaffold/go_router/local_cache_test.go`

- [ ] **Step 1: Write failing periodic tests**

Append to `local_cache_test.go`:

```go
// ── periodicCache ─────────────────────────────────────────────────────────────

func TestPeriodicCacheMissOnEmpty(t *testing.T) {
	fetchFn := func(key string) ([]byte, bool) { return makeVal("v", 0), true }
	c := newPeriodicCache(5, 1*time.Hour, fetchFn)
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss on empty snapshot")
	}
}

func TestPeriodicCacheRebuildPromotesTopK(t *testing.T) {
	fetchFn := func(key string) ([]byte, bool) {
		return makeVal(key+"_val", 0), true
	}
	c := newPeriodicCache(2, 1*time.Hour, fetchFn) // long interval; call doRebuild manually
	v := makeVal("v", 0)
	for i := 0; i < 10; i++ { c.RecordHit("key1", v) }
	for i := 0; i < 5; i++ { c.RecordHit("key2", v) }
	c.RecordHit("key3", v) // count=1, rank 3

	c.doRebuild()

	if _, ok := c.Get("key1"); !ok {
		t.Fatal("key1 (rank 1) should be in L1 after rebuild")
	}
	if _, ok := c.Get("key2"); !ok {
		t.Fatal("key2 (rank 2) should be in L1 after rebuild")
	}
	if _, ok := c.Get("key3"); ok {
		t.Fatal("key3 (rank 3) must not be in L1 with capacity=2")
	}
}

func TestPeriodicCacheInvalidate(t *testing.T) {
	fetchFn := func(key string) ([]byte, bool) { return makeVal("v", 0), true }
	c := newPeriodicCache(5, 1*time.Hour, fetchFn)
	v := makeVal("v", 0)
	for i := 0; i < 3; i++ { c.RecordHit("k", v) }
	c.doRebuild()
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit after rebuild")
	}
	c.Invalidate("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after invalidate")
	}
}

func TestPeriodicCacheTTLExpiry(t *testing.T) {
	fetchFn := func(key string) ([]byte, bool) { return makeVal("v", 1), true } // ttl=1s
	c := newPeriodicCache(5, 1*time.Hour, fetchFn)
	v := makeVal("v", 1)
	for i := 0; i < 3; i++ { c.RecordHit("k", v) }
	c.doRebuild() // snapshot gets entry with expiresAt = now+1s
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit immediately after rebuild")
	}
	time.Sleep(1100 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after TTL expiry")
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
cd distributed_cache/scaffold/go_router
go test ./... -run TestPeriodic 2>&1 | head -5
```

Expected: `undefined: newPeriodicCache`

- [ ] **Step 3: Implement periodicCache**

Update the import block in `local_cache.go` to add `sort`:

```go
import (
	"container/list"
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
```

Append to `local_cache.go` after the LFU section:

```go
// ── periodic strategy — async batch recompute ─────────────────────────────────

type periodicEntry struct {
	value     []byte
	expiresAt time.Time
}

type periodicCache struct {
	counters sync.Map               // key → *atomic.Uint64 (lock-free hit counting)
	snapMu   sync.RWMutex
	snapshot map[string]periodicEntry
	capacity int
	interval time.Duration
	fetchFn  func(key string) ([]byte, bool)
	hits     int64
	misses   int64
	done     chan struct{}
}

func newPeriodicCache(capacity int, interval time.Duration, fetchFn func(string) ([]byte, bool)) *periodicCache {
	c := &periodicCache{
		snapshot: make(map[string]periodicEntry),
		capacity: capacity,
		interval: interval,
		fetchFn:  fetchFn,
		done:     make(chan struct{}),
	}
	go c.rebuildLoop()
	return c
}

func (c *periodicCache) rebuildLoop() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.doRebuild()
		case <-c.done:
			return
		}
	}
}

func (c *periodicCache) doRebuild() {
	type kc struct {
		key   string
		count uint64
	}
	var pairs []kc
	c.counters.Range(func(k, v any) bool {
		pairs = append(pairs, kc{key: k.(string), count: v.(*atomic.Uint64).Load()})
		return true
	})
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].count > pairs[j].count })
	if len(pairs) > c.capacity {
		pairs = pairs[:c.capacity]
	}

	c.snapMu.RLock()
	oldSnap := c.snapshot
	c.snapMu.RUnlock()

	newSnap := make(map[string]periodicEntry, len(pairs))
	for _, p := range pairs {
		if e, ok := oldSnap[p.key]; ok {
			newSnap[p.key] = e // keep existing cached value
		} else if val, ok2 := c.fetchFn(p.key); ok2 {
			ttl := parseTTL(val)
			var expiresAt time.Time
			if ttl > 0 {
				expiresAt = time.Now().Add(time.Duration(ttl) * time.Second)
			}
			newSnap[p.key] = periodicEntry{value: val, expiresAt: expiresAt}
		}
	}

	c.snapMu.Lock()
	c.snapshot = newSnap
	c.snapMu.Unlock()
}

func (c *periodicCache) Get(key string) ([]byte, bool) {
	c.snapMu.RLock()
	e, ok := c.snapshot[key]
	c.snapMu.RUnlock()

	if !ok {
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		c.snapMu.Lock()
		delete(c.snapshot, key)
		c.snapMu.Unlock()
		atomic.AddInt64(&c.misses, 1)
		return nil, false
	}
	atomic.AddInt64(&c.hits, 1)
	return e.value, true
}

func (c *periodicCache) RecordHit(key string, _ []byte) {
	v, _ := c.counters.LoadOrStore(key, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

func (c *periodicCache) Invalidate(key string) {
	c.snapMu.Lock()
	delete(c.snapshot, key)
	c.snapMu.Unlock()
	c.counters.Delete(key)
}

func (c *periodicCache) Stats() LocalCacheStats {
	c.snapMu.RLock()
	size := len(c.snapshot)
	c.snapMu.RUnlock()
	return LocalCacheStats{
		Hits:     atomic.LoadInt64(&c.hits),
		Misses:   atomic.LoadInt64(&c.misses),
		Size:     size,
		Capacity: c.capacity,
		Strategy: "periodic",
	}
}
```

Update `newLocalCache` — replace the `"periodic"` branch and change the `default` to use `newCounterCache`:

```go
func newLocalCache(strategy string, capacity int, rebuildInterval time.Duration, fetchFn func(string) ([]byte, bool)) LocalCache {
	if capacity == 0 {
		return &noopCache{}
	}
	switch strategy {
	case "lfu":
		return newLFUCache(capacity)
	case "periodic":
		return newPeriodicCache(capacity, rebuildInterval, fetchFn)
	default: // "counter"
		return newCounterCache(capacity)
	}
}
```

- [ ] **Step 4: Run periodic tests**

```bash
cd distributed_cache/scaffold/go_router
go test ./... -v -run TestPeriodic -timeout 15s
```

Expected: all 4 periodic tests PASS (TestPeriodicCacheTTLExpiry takes ~1.1 s).

- [ ] **Step 5: Run full test suite**

```bash
go test ./... -timeout 30s
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add distributed_cache/scaffold/go_router/local_cache.go \
        distributed_cache/scaffold/go_router/local_cache_test.go
git commit -m "feat(l1-cache): add periodic strategy (async batch recompute via goroutine)"
```

---

## Task 5: Wire `localCache` into `main.go`

**Files:**
- Modify: `distributed_cache/scaffold/go_router/main.go`

This task has no new tests (covered by the integration test in Task 6). Verify by building.

- [ ] **Step 1: Add `localCache` global and Prometheus metric vars**

In `main.go`, find the second `var (` block (lines 55–58, the one containing `storeURL` and `writeThroughMode`). Replace it with:

```go
var (
	storeURL         string // empty → write-through disabled
	writeThroughMode string // "parallel" | "store_first" | "cache_first"

	localCache           LocalCache
	localCacheHitsTotal  prometheus.Counter
	localCacheMissesTotal prometheus.Counter
)
```

- [ ] **Step 2: Update `handleGet` to check L1 first and call RecordHit**

Replace the entire `handleGet` function (currently lines 325–351) with:

```go
func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	// L1 hit — serve from local cache, zero node hop.
	if val, ok := localCache.Get(key); ok {
		localCacheHitsTotal.Inc()
		requestsTotal.WithLabelValues("get", "200").Inc()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(val) //nolint:errcheck
		return
	}
	localCacheMissesTotal.Inc()

	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		requestsTotal.WithLabelValues("get", "503").Inc()
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	for _, nodeID := range nodes {
		res := callNode(r.Context(), nodeID, "GET",
			nodeURLs[nodeID]+"/cache/"+key, nil, r.Header)
		if res.errMsg != "" {
			continue
		}
		if res.status == http.StatusOK {
			localCache.RecordHit(key, res.body)
		}
		requestsTotal.WithLabelValues("get", strconv.Itoa(res.status)).Inc()
		writeResult(w, res)
		return
	}
	requestsTotal.WithLabelValues("get", "503").Inc()
	writeJSON(w, http.StatusServiceUnavailable,
		map[string]string{"error": "no_nodes_available"})
}
```

- [ ] **Step 3: Add `defer localCache.Invalidate(key)` to `handleSet`**

In `handleSet`, after `key := r.PathValue("key")` (currently line 184), add one line:

```go
	defer localCache.Invalidate(key)
```

The function opening should look like:

```go
func handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	defer localCache.Invalidate(key)
	nodes := ring.nodesForKey(key, 2)
	// ... rest unchanged
```

- [ ] **Step 4: Add `defer localCache.Invalidate(key)` to `handleDelete`**

In `handleDelete`, after `key := r.PathValue("key")` (currently line 354), add one line:

```go
	defer localCache.Invalidate(key)
```

- [ ] **Step 5: Update `handleHealth` to include `local_cache` field**

In `handleHealth`, find the line `out := map[string]any{"status": status, "nodes": nodes}` and replace it with:

```go
	out := map[string]any{
		"status":      status,
		"nodes":       nodes,
		"local_cache": localCache.Stats(),
	}
```

- [ ] **Step 6: Wire `localCache` in `main()`**

In `main()`, find the line `storeURL = getEnv("STORE_URL", "")` (near the top of the function). After `writeThroughMode = getEnv("WRITE_THROUGH_MODE", "parallel")`, add:

```go
	localCacheSize, _ := strconv.Atoi(getEnv("LOCAL_CACHE_SIZE", "100"))
	localCacheStrategy := getEnv("LOCAL_CACHE_STRATEGY", "counter")
	localCacheRebuildInterval, _ := strconv.Atoi(getEnv("LOCAL_CACHE_REBUILD_INTERVAL", "10"))
```

Then find the block that starts `ring = newHashRing(...)` and ends with `circuitBreakers[nodeID] = NewCB(...)`. After that block, add:

```go
	fetchFromL2 := func(key string) ([]byte, bool) {
		nodes := ring.nodesForKey(key, 2)
		for _, nodeID := range nodes {
			res := callNode(context.Background(), nodeID, "GET",
				nodeURLs[nodeID]+"/cache/"+key, nil, http.Header{})
			if res.errMsg == "" && res.status == http.StatusOK {
				return res.body, true
			}
		}
		return nil, false
	}
	localCache = newLocalCache(
		localCacheStrategy,
		localCacheSize,
		time.Duration(localCacheRebuildInterval)*time.Second,
		fetchFromL2,
	)
```

- [ ] **Step 7: Register Prometheus metrics in `main()`**

After the `circuitOpenTotal = promauto.NewCounterVec(...)` block (and before `startHealthChecker()`), add:

```go
	localCacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_l1_hits_total",
		Help: "Requests served from the router's L1 local cache",
	})
	localCacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_l1_misses_total",
		Help: "GET requests that missed the L1 local cache",
	})
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cache_l1_size",
		Help: "Current number of entries in the L1 local cache",
	}, func() float64 {
		return float64(localCache.Stats().Size)
	})
```

- [ ] **Step 8: Update the startup log line**

Replace the existing `log.Printf("Go router starting...")` with:

```go
	log.Printf("Go router starting on :%s (nodes=%d, strategy=%s, vnodes=%d, store=%q, wt_mode=%s, l1_size=%d, l1_strategy=%s)",
		port, len(nodeURLs), stratEnv, virtualNodes, storeURL, writeThroughMode, localCacheSize, localCacheStrategy)
```

- [ ] **Step 9: Build**

```bash
cd distributed_cache/scaffold/go_router
go build ./...
```

Expected: no errors.

- [ ] **Step 10: Run all unit tests**

```bash
go test ./... -timeout 30s
```

Expected: all tests PASS.

- [ ] **Step 11: Commit**

```bash
git add distributed_cache/scaffold/go_router/main.go
git commit -m "feat(l1-cache): wire LocalCache into router (handleGet/Set/Delete/Health + metrics)"
```

---

## Task 6: docker-compose env vars + integration test script

**Files:**
- Modify: `distributed_cache/scaffold/docker-compose.go-full.yml`
- Create: `distributed_cache/scaffold/scripts/test_hot_key.sh`

- [ ] **Step 1: Add L1 env vars to `docker-compose.go-full.yml`**

In the `dc-router` service `environment` block, add three new lines (after `WRITE_THROUGH_MODE`):

```yaml
      LOCAL_CACHE_SIZE: "100"
      LOCAL_CACHE_STRATEGY: "${LOCAL_CACHE_STRATEGY:-counter}"
      LOCAL_CACHE_REBUILD_INTERVAL: "${LOCAL_CACHE_REBUILD_INTERVAL:-10}"
```

The full `dc-router` environment block should now look like:

```yaml
    environment:
      NODE_URLS: "http://dc-node1:8001,http://dc-node2:8002,http://dc-node3:8003"
      HASH_STRATEGY: "ring"
      VIRTUAL_NODES: "150"
      ROUTER_PORT: "8000"
      STORE_URL: "http://dc-store:8004"
      WRITE_THROUGH_MODE: "${WRITE_THROUGH_MODE:-parallel}"
      LOCAL_CACHE_SIZE: "100"
      LOCAL_CACHE_STRATEGY: "${LOCAL_CACHE_STRATEGY:-counter}"
      LOCAL_CACHE_REBUILD_INTERVAL: "${LOCAL_CACHE_REBUILD_INTERVAL:-10}"
```

- [ ] **Step 2: Rebuild the router image**

```bash
cd distributed_cache/scaffold
podman-compose -f docker-compose.yml -f docker-compose.go-full.yml \
    up -d --build --force-recreate dc-router
sleep 4
curl -s http://localhost:8000/health | python3 -m json.tool
```

Expected: `"local_cache"` field present in health response with `"strategy": "counter"`.

- [ ] **Step 3: Create `scripts/test_hot_key.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

ROUTER="http://localhost:8000"
NODE1="scaffold_dc-node1_1"
NODE2="scaffold_dc-node2_1"
NODE3="scaffold_dc-node3_1"
PASS=0; FAIL=0

ok()   { echo "  ✓ $1"; PASS=$((PASS+1)); }
fail() { echo "  ✗ $1"; FAIL=$((FAIL+1)); }
check() { [[ "$1" == "$2" ]] && ok "$3" || fail "$3: got '$1', want '$2'"; }

restart_router() {
    local strategy="${1:-counter}"
    local interval="${2:-10}"
    LOCAL_CACHE_STRATEGY="$strategy" LOCAL_CACHE_REBUILD_INTERVAL="$interval" \
        podman-compose -f docker-compose.yml -f docker-compose.go-full.yml \
        up -d --force-recreate --no-deps dc-router > /dev/null 2>&1
    sleep 4
}

stop_nodes() {
    podman stop "$NODE1" "$NODE2" "$NODE3" > /dev/null 2>&1
    sleep 2
}

start_nodes() {
    podman start "$NODE1" "$NODE2" "$NODE3" > /dev/null 2>&1
    sleep 4
}

seed_keys() {
    for i in $(seq 1 20); do
        curl -sf -X POST "$ROUTER/cache/key$i" \
            -H 'Content-Type: application/json' \
            -d "{\"value\":\"val$i\"}" > /dev/null
    done
}

hit_key() {
    local key="$1" n="${2:-50}"
    for i in $(seq 1 "$n"); do
        curl -sf "$ROUTER/cache/$key" > /dev/null
    done
}

# ─────────────────────────────────────────────────────────────────────────────

echo "=== Phase 1: warm-up Top-K (counter strategy) ==="
restart_router counter
seed_keys
hit_key key1 50

L1_SIZE=$(curl -sf "$ROUTER/health" | grep -o '"size":[0-9]*' | head -1 | cut -d: -f2)
[[ "${L1_SIZE:-0}" -ge 1 ]] \
    && ok "L1 size >= 1 after 50 hits on key1" \
    || fail "L1 size should be >= 1, got '${L1_SIZE:-0}'"

echo ""
echo "=== Phase 2: L1 hit with all cache nodes down ==="
stop_nodes

STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$ROUTER/cache/key1")
check "$STATUS" "200" "GET key1 returns 200 from L1 when nodes are down"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$ROUTER/cache/key20" || echo "503")
check "$STATUS" "503" "GET key20 (cold, not in L1) returns 503 with nodes down"

start_nodes

echo ""
echo "=== Phase 3: invalidation on SET ==="
curl -sf -X POST "$ROUTER/cache/key1" \
    -H 'Content-Type: application/json' \
    -d '{"value":"new_value"}' > /dev/null

VALUE=$(curl -sf "$ROUTER/cache/key1" | grep -o '"value":"[^"]*"' | cut -d'"' -f4)
check "$VALUE" "new_value" "GET key1 returns new_value after SET invalidates L1"

echo ""
echo "=== Phase 4: LFU strategy ==="
restart_router lfu
seed_keys
hit_key key1 50

STRATEGY=$(curl -sf "$ROUTER/health" | grep -o '"strategy":"[^"]*"' | tail -1 | cut -d'"' -f4)
check "$STRATEGY" "lfu" "Health reports strategy=lfu"

stop_nodes
STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$ROUTER/cache/key1")
check "$STATUS" "200" "GET key1 returns 200 from L1 (lfu) with nodes down"
start_nodes

echo ""
echo "=== Phase 5: periodic strategy (3s rebuild interval) ==="
restart_router periodic 3
seed_keys
hit_key key1 50
echo "  Waiting 4s for background rebuild goroutine..."
sleep 4

stop_nodes
STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$ROUTER/cache/key1")
check "$STATUS" "200" "GET key1 returns 200 from L1 (periodic) after rebuild"
start_nodes

echo ""
echo "=== Phase 6: TTL expiry ==="
restart_router counter
curl -sf -X POST "$ROUTER/cache/ttlkey" \
    -H 'Content-Type: application/json' \
    -d '{"value":"ttl_val","ttl":2}' > /dev/null
hit_key ttlkey 50

STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$ROUTER/cache/ttlkey")
check "$STATUS" "200" "GET ttlkey returns 200 immediately (L1 not yet expired)"

echo "  Waiting 3s for TTL to expire in both L1 and L2..."
sleep 3

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$ROUTER/cache/ttlkey" || echo "404")
check "$STATUS" "404" "GET ttlkey returns 404 after TTL expiry in L1 and L2"

echo ""
echo "==================================="
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
```

- [ ] **Step 4: Make script executable**

```bash
chmod +x distributed_cache/scaffold/scripts/test_hot_key.sh
```

- [ ] **Step 5: Run the integration test**

```bash
cd distributed_cache/scaffold
./scripts/test_hot_key.sh
```

Expected: all 9 checks pass, exit 0.

- [ ] **Step 6: Verify existing verify.sh still passes**

```bash
./scripts/verify.sh
```

Expected: 9/9 checks pass (L1 is transparent to GET semantics).

- [ ] **Step 7: Commit**

```bash
git add distributed_cache/scaffold/docker-compose.go-full.yml \
        distributed_cache/scaffold/scripts/test_hot_key.sh
git commit -m "feat(l1-cache): add docker-compose env vars and test_hot_key.sh integration test"
```

---

## Strategy Comparison Reference

| Property | `counter` | `lfu` | `periodic` |
|----------|-----------|-------|------------|
| Read path | O(1) RLock | O(1) RLock | O(1) snapMu RLock |
| Write (RecordHit) | O(K) lock (eviction rescan) | O(1) lock (freq list ops) | lock-free (atomic.Add) |
| Promotion latency | per-request (immediate) | per-request (immediate) | async (up to rebuildInterval) |
| Memory | counters map grows unbounded | O(K) — only live entries | counters map + snapshot |
| L1 consistency lag | zero | zero | up to rebuildInterval |
| Goroutine | none | none | one background goroutine |
| Best for | simple hot detection | accurate frequency ranking | high-write-rate keys |
