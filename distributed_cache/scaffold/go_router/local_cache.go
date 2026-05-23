package main

import (
	"container/list"
	"encoding/json"
	"sort"
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
		return newLFUCache(capacity)
	case "periodic":
		return newPeriodicCache(capacity, rebuildInterval, fetchFn)
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
	keyMap   map[string]*lfuEntry  // key → entry
	freqMap  map[uint64]*list.List // freq → MRU-ordered list of *lfuEntry
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

// removeEntry removes e from keyMap and freqMap. Must hold write lock.
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
