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
