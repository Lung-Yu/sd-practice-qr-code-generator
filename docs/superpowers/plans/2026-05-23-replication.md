# Sync Replication (RF=2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add synchronous RF=2 replication to the Go router so that reads fall back to a replica when the primary is unreachable and writes require both primary + replica to succeed.

**Architecture:** `ring.nodesForKey(key, 2)` returns `[primary, replica]` (next distinct physical node clockwise). A new `callNode()` helper returns a `nodeResult` struct instead of writing to `http.ResponseWriter`, enabling parallel writes and sequential read fallback. All replication logic lives in the router — nodes are unchanged.

**Tech Stack:** Go 1.22 `net/http`, `sync.WaitGroup`, `bytes.NewReader`, existing `CircuitBreaker`, bash test script.

---

## File Map

| File | Change |
|------|--------|
| `go_router/ring.go` | Add `nodesForKey`, `ringNodes`, `rendezvousNodes` |
| `go_router/ring_test.go` | **New** — unit tests for `nodesForKey` |
| `go_router/main.go` | Add `nodeResult`, `callNode`, `writeResult`; refactor `proxy`; rewrite `handleSet/Get/Delete`; update `ringResp` + `handleRing` |
| `scripts/test_replication.sh` | **New** — 5-phase integration test |

---

## Task 1: `nodesForKey` in `ring.go`

**Files:**
- Create: `distributed_cache/scaffold/go_router/ring_test.go`
- Modify: `distributed_cache/scaffold/go_router/ring.go`

- [ ] **Step 1: Write failing tests in `ring_test.go`**

```go
package main

import "testing"

func TestNodesForKey_ReturnsTwoDistinctNodes(t *testing.T) {
	r := newHashRing(50, strategyRing)
	r.addNode("node1"); r.addNode("node2"); r.addNode("node3")
	nodes := r.nodesForKey("key4", 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Fatalf("expected distinct nodes, got %q and %q", nodes[0], nodes[1])
	}
}

func TestNodesForKey_PrimaryMatchesNodeForKey(t *testing.T) {
	r := newHashRing(50, strategyRing)
	r.addNode("node1"); r.addNode("node2"); r.addNode("node3")
	for _, key := range []string{"foo", "bar", "baz", "key1", "key10"} {
		single, _ := r.nodeForKey(key)
		multi := r.nodesForKey(key, 2)
		if multi[0] != single {
			t.Errorf("key %q: nodesForKey[0]=%q, nodeForKey=%q", key, multi[0], single)
		}
	}
}

func TestNodesForKey_OneNodeInRing(t *testing.T) {
	r := newHashRing(50, strategyRing)
	r.addNode("node1")
	nodes := r.nodesForKey("anykey", 2)
	if len(nodes) != 1 || nodes[0] != "node1" {
		t.Fatalf("expected [node1], got %v", nodes)
	}
}

func TestNodesForKey_EmptyRing(t *testing.T) {
	r := newHashRing(50, strategyRing)
	if nodes := r.nodesForKey("anykey", 2); nodes != nil {
		t.Fatalf("expected nil for empty ring, got %v", nodes)
	}
}

func TestNodesForKey_RendezvousPrimaryMatchesNodeForKey(t *testing.T) {
	r := newHashRing(50, strategyRendezvous)
	r.addNode("node1"); r.addNode("node2"); r.addNode("node3")
	for _, key := range []string{"foo", "bar", "baz"} {
		single, _ := r.nodeForKey(key)
		multi := r.nodesForKey(key, 2)
		if len(multi) != 2 {
			t.Errorf("key %q: expected 2 nodes, got %d", key, len(multi))
			continue
		}
		if multi[0] != single {
			t.Errorf("key %q: nodesForKey[0]=%q, nodeForKey=%q", key, multi[0], single)
		}
	}
}
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
cd distributed_cache/scaffold/go_router && go test ./... -run TestNodesForKey -v
```

Expected: `undefined: nodesForKey` or `FAIL` on all 5 tests.

- [ ] **Step 3: Add `nodesForKey`, `ringNodes`, `rendezvousNodes` to `ring.go`**

Append after `rendezvousNode` (after line 103, before `virtualCount`):

```go
// nodesForKey returns up to n distinct physical nodes for key.
// nodes[0] is the primary; nodes[1] is the replica (if n >= 2 and ring has >= 2 nodes).
// Callers must NOT hold r.mu.
func (r *hashRing) nodesForKey(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.nodes) == 0 {
		return nil
	}
	if r.strat == strategyRendezvous {
		return r.rendezvousNodes(key, n)
	}
	return r.ringNodes(key, n)
}

// ringNodes walks clockwise from the key's position, collecting up to n
// distinct physical nodes. Called with r.mu held for reading.
func (r *hashRing) ringNodes(key string, n int) []string {
	h := ringHash(key)
	start := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] > h }) % len(r.ring)
	seen := make(map[string]struct{})
	result := make([]string, 0, n)
	for i := 0; len(result) < n && len(seen) < len(r.nodes); i++ {
		pos := (start + i) % len(r.ring)
		nodeID := r.ringMap[r.ring[pos]]
		if _, ok := seen[nodeID]; !ok {
			seen[nodeID] = struct{}{}
			result = append(result, nodeID)
		}
	}
	return result
}

// rendezvousNodes ranks all nodes by hash score descending, returns top n.
// Called with r.mu held for reading.
func (r *hashRing) rendezvousNodes(key string, n int) []string {
	type scored struct {
		id    string
		score uint64
	}
	nodes := make([]scored, 0, len(r.nodes))
	for id := range r.nodes {
		nodes = append(nodes, scored{id, ringHash(fmt.Sprintf("%s:%s", id, key))})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].score > nodes[j].score })
	result := make([]string, 0, n)
	for i := 0; i < n && i < len(nodes); i++ {
		result = append(result, nodes[i].id)
	}
	return result
}
```

- [ ] **Step 4: Run tests — verify all 5 pass**

```bash
cd distributed_cache/scaffold/go_router && go test ./... -run TestNodesForKey -v
```

Expected: `PASS` on all 5. Full build check:

```bash
go build ./...
```

Expected: no output (clean build).

- [ ] **Step 5: Commit**

```bash
git add distributed_cache/scaffold/go_router/ring.go \
        distributed_cache/scaffold/go_router/ring_test.go
git commit -m "feat(distributed_cache): ring.nodesForKey — primary + replica lookup"
```

---

## Task 2: `nodeResult`, `callNode`, `writeResult` in `main.go`

**Files:**
- Modify: `distributed_cache/scaffold/go_router/main.go`

- [ ] **Step 1: Add `"bytes"` and `"context"` to the import block**

Change the import block from:
```go
import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
```
to:
```go
import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
```

- [ ] **Step 2: Add `nodeResult` type after the existing `statsResp` struct (around line 72)**

```go
// nodeResult holds the outcome of a single proxied call to a cache node.
// errMsg is non-empty when the request never reached the node (circuit_open
// or node_unreachable); status/body/headers are populated on a real HTTP response.
type nodeResult struct {
	status  int
	body    []byte
	headers http.Header
	errMsg  string // "circuit_open" | "node_unreachable" | ""
}
```

- [ ] **Step 3: Add `callNode` and `writeResult` after the `nodeFor` helper (around line 90)**

```go
// callNode executes one proxied HTTP request to a node and returns the result.
// It manages the circuit breaker: records failure on TCP error or 5xx, success otherwise.
// 404 (cache miss) is a success — it is normal cache behaviour.
func callNode(ctx context.Context, nodeID, method, targetURL string, body []byte, header http.Header) nodeResult {
	cb := circuitBreakers[nodeID]
	if !cb.Allow() {
		circuitOpenTotal.WithLabelValues(nodeID).Inc()
		return nodeResult{errMsg: "circuit_open"}
	}
	start := time.Now()
	req, _ := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(body))
	req.Header = header.Clone()
	req.ContentLength = int64(len(body))
	resp, err := proxyClient.Do(req)
	routeDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		cb.RecordFailure()
		return nodeResult{errMsg: "node_unreachable"}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 500 {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return nodeResult{status: resp.StatusCode, body: respBody, headers: resp.Header.Clone()}
}

// writeResult writes a nodeResult to w, copying status, headers, and body.
func writeResult(w http.ResponseWriter, res nodeResult) {
	for k, vs := range res.headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(res.status)
	w.Write(res.body) //nolint:errcheck
}
```

- [ ] **Step 4: Refactor `proxy()` to use `callNode()`**

Replace the entire `proxy` function with:

```go
// proxy forwards a single request to one node, writing the response to w.
// Used for handlers that do not need replication (e.g. /stats, /ring).
func proxy(w http.ResponseWriter, r *http.Request, nodeID, targetURL, handler string) {
	body, _ := io.ReadAll(r.Body)
	res := callNode(r.Context(), nodeID, r.Method, targetURL, body, r.Header)
	if res.errMsg == "circuit_open" {
		requestsTotal.WithLabelValues(handler, "503").Inc()
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "circuit_open",
			"node":  nodeID,
		})
		return
	}
	if res.errMsg == "node_unreachable" {
		requestsTotal.WithLabelValues(handler, "503").Inc()
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "node_unreachable", "node": nodeID})
		return
	}
	requestsTotal.WithLabelValues(handler, strconv.Itoa(res.status)).Inc()
	writeResult(w, res)
}
```

- [ ] **Step 5: Build — verify no compile errors**

```bash
cd distributed_cache/scaffold/go_router && go build ./...
```

Expected: clean (no output).

- [ ] **Step 6: Commit**

```bash
git add distributed_cache/scaffold/go_router/main.go
git commit -m "feat(distributed_cache): callNode + writeResult — buffered proxy primitive for replication"
```

---

## Task 3: Rewrite `handleSet` and `handleDelete` with parallel sync replication

**Files:**
- Modify: `distributed_cache/scaffold/go_router/main.go`

- [ ] **Step 1: Replace `handleSet` entirely**

```go
func handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	body, _ := io.ReadAll(r.Body)

	if len(nodes) == 1 {
		// Degraded ring (only one node alive) — single write, no replication
		res := callNode(r.Context(), nodes[0], "POST",
			nodeURLs[nodes[0]]+"/cache/"+key, body, r.Header)
		if res.errMsg != "" {
			requestsTotal.WithLabelValues("set", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": res.errMsg, "node": nodes[0]})
			return
		}
		requestsTotal.WithLabelValues("set", strconv.Itoa(res.status)).Inc()
		writeResult(w, res)
		return
	}

	// Sync replication: parallel write to primary (nodes[0]) + replica (nodes[1])
	results := make([]nodeResult, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i, nodeID := range nodes[:2] {
		go func(i int, nodeID string) {
			defer wg.Done()
			results[i] = callNode(r.Context(), nodeID, "POST",
				nodeURLs[nodeID]+"/cache/"+key, body, r.Header)
		}(i, nodeID)
	}
	wg.Wait()

	for i, nodeID := range nodes[:2] {
		res := results[i]
		if res.errMsg != "" || res.status >= 500 {
			requestsTotal.WithLabelValues("set", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "replication_failed",
				"node":  nodeID,
			})
			return
		}
	}
	requestsTotal.WithLabelValues("set", strconv.Itoa(results[0].status)).Inc()
	writeResult(w, results[0])
}
```

- [ ] **Step 2: Replace `handleDelete` entirely**

```go
func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	body, _ := io.ReadAll(r.Body)

	if len(nodes) == 1 {
		res := callNode(r.Context(), nodes[0], "DELETE",
			nodeURLs[nodes[0]]+"/cache/"+key, body, r.Header)
		if res.errMsg != "" {
			requestsTotal.WithLabelValues("delete", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": res.errMsg, "node": nodes[0]})
			return
		}
		requestsTotal.WithLabelValues("delete", strconv.Itoa(res.status)).Inc()
		writeResult(w, res)
		return
	}

	results := make([]nodeResult, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i, nodeID := range nodes[:2] {
		go func(i int, nodeID string) {
			defer wg.Done()
			results[i] = callNode(r.Context(), nodeID, "DELETE",
				nodeURLs[nodeID]+"/cache/"+key, body, r.Header)
		}(i, nodeID)
	}
	wg.Wait()

	for i, nodeID := range nodes[:2] {
		res := results[i]
		if res.errMsg != "" || res.status >= 500 {
			requestsTotal.WithLabelValues("delete", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "replication_failed",
				"node":  nodeID,
			})
			return
		}
	}
	requestsTotal.WithLabelValues("delete", strconv.Itoa(results[0].status)).Inc()
	writeResult(w, results[0])
}
```

- [ ] **Step 3: Build**

```bash
cd distributed_cache/scaffold/go_router && go build ./...
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add distributed_cache/scaffold/go_router/main.go
git commit -m "feat(distributed_cache): sync replication in handleSet + handleDelete (RF=2)"
```

---

## Task 4: Rewrite `handleGet` + update `handleRing`

**Files:**
- Modify: `distributed_cache/scaffold/go_router/main.go`

- [ ] **Step 1: Replace `handleGet` entirely**

Read fallback applies on ANY failure (`errMsg != ""`): both `circuit_open` and `node_unreachable` will attempt the replica. This eliminates the ~3-request window before the CB opens.

```go
func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
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
			continue // try next node (replica)
		}
		requestsTotal.WithLabelValues("get", strconv.Itoa(res.status)).Inc()
		writeResult(w, res)
		return
	}
	// All nodes in the replication set are unreachable
	requestsTotal.WithLabelValues("get", "503").Inc()
	writeJSON(w, http.StatusServiceUnavailable,
		map[string]string{"error": "no_nodes_available"})
}
```

- [ ] **Step 2: Update `ringResp` struct to include `Replica` field**

Change:
```go
type ringResp struct {
	Key          string `json:"key"`
	Node         string `json:"node"`
	VirtualNodes int    `json:"virtual_nodes"`
}
```

to:
```go
type ringResp struct {
	Key          string `json:"key"`
	Node         string `json:"node"`    // primary (kept for backward compat)
	Replica      string `json:"replica"` // replica node; empty if only one node in ring
	VirtualNodes int    `json:"virtual_nodes"`
}
```

- [ ] **Step 3: Replace `handleRing`**

```go
func handleRing(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodes := ring.nodesForKey(key, 2)
	if len(nodes) == 0 {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "no_nodes_available"})
		return
	}
	replica := ""
	if len(nodes) > 1 {
		replica = nodes[1]
	}
	writeJSON(w, http.StatusOK, ringResp{
		Key:          key,
		Node:         nodes[0],
		Replica:      replica,
		VirtualNodes: ring.virtualCount(),
	})
}
```

- [ ] **Step 4: Build**

```bash
cd distributed_cache/scaffold/go_router && go build ./...
```

Expected: clean.

- [ ] **Step 5: Rebuild Docker image, run `verify.sh`**

```bash
cd distributed_cache/scaffold
podman-compose -f docker-compose.yml -f docker-compose.go-full.yml down
podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build
sleep 3
bash scripts/verify.sh
```

Expected: `9 passed, 0 failed`. The `/ring` check in verify.sh uses `d['node']` which still exists.

- [ ] **Step 6: Spot-check replication is wired up**

```bash
curl -s -X POST http://localhost:8000/cache/testkey \
  -H 'Content-Type: application/json' -d '{"value":"hello","ttl":60}'
# Expected: {"key":"testkey","node":"nodeX","status":"ok"}

curl -s http://localhost:8000/ring/testkey
# Expected: {"key":"testkey","node":"nodeX","replica":"nodeY","virtual_nodes":450}
```

Confirm `replica` field appears and is a different node from `node`.

- [ ] **Step 7: Commit**

```bash
git add distributed_cache/scaffold/go_router/main.go
git commit -m "feat(distributed_cache): handleGet replica fallback + handleRing shows replica"
```

---

## Task 5: Write and run `test_replication.sh`

**Files:**
- Create: `distributed_cache/scaffold/scripts/test_replication.sh`

- [ ] **Step 1: Create `scripts/test_replication.sh`**

```bash
#!/usr/bin/env bash
# Sync replication (RF=2) test.
# Requires: podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d
#
# Timeline:
#   Phase 1: Seed 30 keys, verify /ring shows primary+replica
#   Phase 2: Kill node2, GET node2-primary key → 200 from replica (immediate)
#   Phase 3: SET node2-primary key → 503 replication_failed
#   Phase 4: Wait 6s, all 30 keys readable (replica serves node2's data)
#   Phase 5: Restart node2, SET+GET both recover

set -euo pipefail

BASE="http://localhost:8000"
NODE2="scaffold_dc-node2_1"
PASS=0; FAIL=0

ok()   { echo "  ✓ $1"; PASS=$((PASS+1)); }
fail() { echo "  ✗ $1"; FAIL=$((FAIL+1)); }

health() {
  curl -s "$BASE/health" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  status={d[\"status\"]}')
for nid, s in sorted(d['nodes'].items()):
    alive_icon = '🟢' if s['alive'] else '🔴'
    cb = s['circuit']
    cb_icon = {'closed':'✅','open':'🔴','half_open':'🟡'}.get(cb, '?')
    print(f'  {alive_icon} {nid}: alive={s[\"alive\"]}, circuit={cb} {cb_icon}')
"
}

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║   Sync Replication (RF=2) Test                            ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

# ── Preflight ──────────────────────────────────────────────────────────────
echo "▶ Preflight — ensure all 3 nodes alive"
podman start "$NODE2" > /dev/null 2>&1 || true
echo "  Waiting 8s for health checker..."
sleep 8
alive_count=$(curl -s "$BASE/health" | python3 -c "
import sys,json; d=json.load(sys.stdin)
print(sum(1 for s in d['nodes'].values() if s['alive']))
")
if [[ "$alive_count" == "3" ]]; then
  ok "All 3 nodes alive"
else
  echo "  ERROR: only $alive_count/3 nodes alive. Aborting."
  exit 1
fi
echo ""

# ── Phase 1: Seed + Ring inspection ───────────────────────────────────────
echo "▶ Phase 1 — Seed 30 keys, verify /ring shows replica field"
for i in $(seq 1 30); do
  curl -s -X POST "$BASE/cache/key$i" \
    -H 'Content-Type: application/json' \
    -d "{\"value\":\"val$i\",\"ttl\":300}" > /dev/null
done
echo "  Seeded key1…key30"

ring_resp=$(curl -s "$BASE/ring/key1")
primary=$(echo "$ring_resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])")
replica=$(echo "$ring_resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('replica',''))")
if [[ -n "$replica" && "$replica" != "$primary" ]]; then
  ok "/ring/key1 → primary=$primary replica=$replica"
else
  fail "/ring/key1 missing or invalid replica field (got: $ring_resp)"
fi
echo ""

# Find a key where node2 is primary
echo "▶ Phase 1b — Find key with node2 as primary"
NODE2_KEY=""
NODE2_REPLICA=""
for i in $(seq 1 30); do
  rr=$(curl -s "$BASE/ring/key$i")
  p=$(echo "$rr" | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])")
  rep=$(echo "$rr" | python3 -c "import sys,json; print(json.load(sys.stdin).get('replica',''))")
  if [[ "$p" == "node2" ]]; then
    NODE2_KEY="key$i"
    NODE2_REPLICA="$rep"
    echo "  $NODE2_KEY → primary=node2 replica=$NODE2_REPLICA ← using this"
    break
  fi
done
if [[ -z "$NODE2_KEY" ]]; then
  echo "  (no key with node2 as primary in key1-30 — using key10 as fallback)"
  NODE2_KEY="key10"
  NODE2_REPLICA="node3"
fi
echo ""

# ── Phase 2: Kill node2, GET from replica immediately ─────────────────────
echo "▶ Phase 2 — Kill node2, verify GET falls back to replica immediately"
podman stop "$NODE2" > /dev/null
echo "  node2 stopped."

status=$(curl -s -o /tmp/rep_body.txt -w "%{http_code}" "$BASE/cache/$NODE2_KEY")
body=$(cat /tmp/rep_body.txt)
if [[ "$status" == "200" ]]; then
  value=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin)['value'])")
  ok "GET $NODE2_KEY → 200 from replica immediately (value=$value)"
else
  error=$(python3 -c "import sys,json; print(json.load(sys.stdin).get('error','?'))" 2>/dev/null < /tmp/rep_body.txt || echo "?")
  fail "GET $NODE2_KEY → $status $error (expected 200 from replica)"
fi
echo ""

# ── Phase 3: SET must fail (RF=2 unsatisfied) ─────────────────────────────
echo "▶ Phase 3 — SET must fail when node2 is required for replication"
status=$(curl -s -o /tmp/rep_body.txt -w "%{http_code}" \
  -X POST "$BASE/cache/$NODE2_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"value":"new_value","ttl":60}')
body=$(cat /tmp/rep_body.txt)
error=$(python3 -c "import sys,json; print(json.load(sys.stdin).get('error','?'))" 2>/dev/null < /tmp/rep_body.txt || echo "?")
if [[ "$status" == "503" ]]; then
  ok "SET $NODE2_KEY → 503 $error (RF=2 not satisfied, write correctly rejected)"
else
  fail "SET $NODE2_KEY → $status $error (expected 503)"
fi
echo ""

# ── Phase 4: Wait for health checker, all 30 keys still readable ──────────
echo "▶ Phase 4 — Wait 6s for health checker, all 30 keys must be readable"
sleep 6
echo "  Health after health checker fires:"
health
echo ""

errors=0; hits=0; misses=0
for i in $(seq 1 30); do
  s=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/cache/key$i")
  case "$s" in
    200) hits=$((hits+1)) ;;
    404) misses=$((misses+1)) ;;
    *)   errors=$((errors+1)); echo "    key$i: unexpected $s" ;;
  esac
done
if [[ $errors -eq 0 ]]; then
  ok "All 30 keys: hits=$hits misses=$misses errors=0 (replica holds node2 data)"
else
  fail "$errors keys returned unexpected status (expected 0)"
fi
echo ""

# ── Phase 5: Restart node2, verify full recovery ──────────────────────────
echo "▶ Phase 5 — Restart node2, verify SET + GET recover"
podman start "$NODE2" > /dev/null
echo "  node2 started. Waiting 6s for health checker + circuit reset..."
sleep 6

echo "  Health after recovery:"
health
echo ""

status=$(curl -s -o /tmp/rep_body.txt -w "%{http_code}" \
  -X POST "$BASE/cache/recovered_key" \
  -H 'Content-Type: application/json' \
  -d '{"value":"recovered","ttl":60}')
if [[ "$status" == "200" ]]; then
  ok "SET recovered_key → 200 (replication works again)"
else
  body=$(cat /tmp/rep_body.txt)
  fail "SET recovered_key → $status $(python3 -c "import sys,json; print(json.load(sys.stdin).get('error','?'))" 2>/dev/null < /tmp/rep_body.txt || echo '?')"
fi

status=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/cache/recovered_key")
if [[ "$status" == "200" ]]; then
  ok "GET recovered_key → 200"
else
  fail "GET recovered_key → $status (expected 200)"
fi

rr=$(curl -s "$BASE/ring/$NODE2_KEY")
p=$(echo "$rr" | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])")
rep=$(echo "$rr" | python3 -c "import sys,json; print(json.load(sys.stdin).get('replica',''))")
if [[ "$p" == "node2" || "$rep" == "node2" ]]; then
  ok "node2 back in ring ($NODE2_KEY → primary=$p replica=$rep)"
else
  fail "node2 not in ring for $NODE2_KEY (primary=$p replica=$rep)"
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
```

- [ ] **Step 2: Make executable and run**

```bash
chmod +x distributed_cache/scaffold/scripts/test_replication.sh
bash distributed_cache/scaffold/scripts/test_replication.sh
```

Expected output (7 checks):
```
  ✓ All 3 nodes alive
  ✓ /ring/key1 → primary=nodeX replica=nodeY
  ✓ GET keyN → 200 from replica immediately
  ✓ SET keyN → 503 replication_failed
  ✓ All 30 keys: hits=30 misses=0 errors=0
  ✓ SET recovered_key → 200
  ✓ GET recovered_key → 200
  ✓ node2 back in ring

Results: 8 passed, 0 failed
```

- [ ] **Step 3: Run `verify.sh` one final time to confirm no regressions**

```bash
bash distributed_cache/scaffold/scripts/verify.sh
```

Expected: `9 passed, 0 failed`.

- [ ] **Step 4: Commit**

```bash
git add distributed_cache/scaffold/scripts/test_replication.sh
git commit -m "test(distributed_cache): test_replication.sh — 5-phase RF=2 integration test"
```

---

## Self-Review

**Spec coverage:**
- ✅ Sync write to primary + replica (Task 3)
- ✅ 503 if either unreachable (Task 3, replication_failed check)
- ✅ Read fallback to replica when primary unreachable (Task 4)
- ✅ Degrade to single-node write when only 1 node in ring (Task 3)
- ✅ `callNode` with CB logic (Task 2)
- ✅ `/ring` shows replica (Task 4)
- ✅ Test covers all phases (Task 5)

**Placeholder scan:** No TBD or TODO.

**Type consistency:**
- `nodeResult` defined in Task 2, used identically in Tasks 3, 4
- `callNode` signature: `(ctx, nodeID, method, url, body, header)` — consistent across all call sites
- `ring.nodesForKey(key, 2)` — consistent across all handlers
- `writeResult(w, res)` — consistent in Tasks 2, 3, 4
