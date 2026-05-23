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
