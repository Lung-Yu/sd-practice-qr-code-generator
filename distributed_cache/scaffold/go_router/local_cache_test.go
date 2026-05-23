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

// time is imported for TTL tests added in Tasks 2-4.
var _ = time.Second

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
