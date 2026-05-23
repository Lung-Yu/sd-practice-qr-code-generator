package main

import (
	"encoding/json"
	"sync"
	"sync/atomic"
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
	Get(key string) ([]byte, bool)    // L1 hit → cached bytes; miss → false
	RecordHit(key string, val []byte) // called on every L2 200; may promote to L1
	Invalidate(key string)            // called on every SET and DELETE
	Stats() LocalCacheStats
}

// parseTTL extracts ttl_remaining from a node GET response (best-effort; 0 = no TTL).
func parseTTL(body []byte) int {
	var r struct {
		TTLRemaining int `json:"ttl_remaining"`
	}
	_ = json.Unmarshal(body, &r) // best-effort; missing or unparseable = no TTL
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
		return newCounterCache(capacity)
	}
}

// ── disabled (capacity=0) ─────────────────────────────────────────────────────

type noopCache struct{}

func (c *noopCache) Get(_ string) ([]byte, bool)  { return nil, false }
func (c *noopCache) RecordHit(_ string, _ []byte) {}
func (c *noopCache) Invalidate(_ string)          {}
func (c *noopCache) Stats() LocalCacheStats       { return LocalCacheStats{Strategy: "disabled"} }

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

// rescanMin does a linear O(K) scan to find the entry with the lowest count.
// On an empty cache, minCount stays at maxUint64 so any future insertion is accepted.
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
