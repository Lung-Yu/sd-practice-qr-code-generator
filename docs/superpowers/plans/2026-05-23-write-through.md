# Write-Through Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable backing store (`go_store`) behind the distributed cache and implement three write-through consistency modes (`parallel`, `store_first`, `cache_first`) selectable via env var.

**Architecture:** A new `go_store` Go service (plain in-memory KV, port 8004) acts as the simulated DB. The router gains `STORE_URL` and `WRITE_THROUGH_MODE` env vars; `handleSet` and `handleDelete` coordinate cache and store writes according to the mode. GET remains unchanged — cache miss → 404, no read-through.

**Tech Stack:** Go 1.22, stdlib only for `go_store`; existing `go_router` package.

---

## File Map

| Action | Path |
|--------|------|
| Create | `distributed_cache/scaffold/go_store/main.go` |
| Create | `distributed_cache/scaffold/go_store/go.mod` |
| Create | `distributed_cache/scaffold/go_store/Dockerfile` |
| Modify | `distributed_cache/scaffold/docker-compose.go-full.yml` |
| Modify | `distributed_cache/scaffold/go_router/main.go` |
| Create | `distributed_cache/scaffold/scripts/test_write_through.sh` |

---

## Task 1: `go_store` service

**Files:**
- Create: `distributed_cache/scaffold/go_store/main.go`
- Create: `distributed_cache/scaffold/go_store/go.mod`
- Create: `distributed_cache/scaffold/go_store/Dockerfile`

- [ ] **Step 1: Create `go_store/go.mod`**

```
module distributed-cache-store

go 1.22
```

File path: `distributed_cache/scaffold/go_store/go.mod`

- [ ] **Step 2: Create `go_store/main.go`**

```go
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
)

var (
	mu    sync.RWMutex
	store = map[string]json.RawMessage{}
)

func main() {
	port := os.Getenv("STORE_PORT")
	if port == "" {
		port = "8004"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "bad_request"})
			return
		}
		mu.Lock()
		store[key] = body
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"key": key, "status": "stored"})
	})

	mux.HandleFunc("GET /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		mu.RLock()
		v, ok := store[key]
		mu.RUnlock()
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "miss", "key": key})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(v)
	})

	mux.HandleFunc("DELETE /store/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		mu.Lock()
		delete(store, key)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"key": key, "status": "deleted"})
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	log.Printf("go_store listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
```

- [ ] **Step 3: Create `go_store/Dockerfile`**

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o cache-store .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/cache-store ./
EXPOSE 8004
CMD ["./cache-store"]
```

- [ ] **Step 4: Build and smoke-test locally**

```bash
cd distributed_cache/scaffold/go_store
go build -o cache-store .
./cache-store &
sleep 1

# POST
curl -s -X POST http://localhost:8004/store/foo \
  -H 'Content-Type: application/json' \
  -d '{"value":"bar","ttl":60}'
# Expected: {"key":"foo","status":"stored"}

# GET
curl -s http://localhost:8004/store/foo
# Expected: {"value":"bar","ttl":60}

# DELETE
curl -s -X DELETE http://localhost:8004/store/foo
# Expected: {"key":"foo","status":"deleted"}

# GET after DELETE → 404
curl -s -o /dev/null -w "%{http_code}" http://localhost:8004/store/foo
# Expected: 404

kill %1
rm cache-store
```

- [ ] **Step 5: Commit**

```bash
git add distributed_cache/scaffold/go_store/
git commit -m "feat(distributed_cache): go_store backing store service"
```

---

## Task 2: Add `dc-store` to docker-compose and wire env vars

**Files:**
- Modify: `distributed_cache/scaffold/docker-compose.go-full.yml`

- [ ] **Step 1: Add `dc-store` service and env vars to `docker-compose.go-full.yml`**

Replace the entire file with:

```yaml
version: "3.9"

# Full Go stack — Go router + Go nodes + Go backing store.
# Run with:
#   podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build

services:
  dc-router:
    build:
      context: ./go_router
      dockerfile: Dockerfile
    image: distributed-cache-go-router:latest
    command: ["./cache-router"]
    environment:
      NODE_URLS: "http://dc-node1:8001,http://dc-node2:8002,http://dc-node3:8003"
      HASH_STRATEGY: "ring"
      VIRTUAL_NODES: "150"
      ROUTER_PORT: "8000"
      STORE_URL: "http://dc-store:8004"
      WRITE_THROUGH_MODE: "${WRITE_THROUGH_MODE:-parallel}"

  dc-node1:
    build:
      context: ./go_node
      dockerfile: Dockerfile
    image: distributed-cache-go:latest
    command: ["./cache-node"]
    environment:
      NODE_ID: node1
      NODE_PORT: "8001"
      CAPACITY: "100"

  dc-node2:
    build:
      context: ./go_node
      dockerfile: Dockerfile
    image: distributed-cache-go:latest
    command: ["./cache-node"]
    environment:
      NODE_ID: node2
      NODE_PORT: "8002"
      CAPACITY: "100"

  dc-node3:
    build:
      context: ./go_node
      dockerfile: Dockerfile
    image: distributed-cache-go:latest
    command: ["./cache-node"]
    environment:
      NODE_ID: node3
      NODE_PORT: "8003"
      CAPACITY: "100"

  dc-store:
    build:
      context: ./go_store
      dockerfile: Dockerfile
    image: distributed-cache-go-store:latest
    command: ["./cache-store"]
    environment:
      STORE_PORT: "8004"
```

- [ ] **Step 2: Build and start the stack**

```bash
cd distributed_cache/scaffold
podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build
```

Expected: 5 containers running — `dc-router`, `dc-node1`, `dc-node2`, `dc-node3`, `dc-store`.

- [ ] **Step 3: Verify store is reachable**

```bash
# Direct smoke test on the store (via its exposed port — add ports: ["8004:8004"] to dc-store if needed)
# Or test via the fact that the router can reach it (will confirm in Task 3)
curl -s http://localhost:8000/health
```

Expected: JSON with `status: ok` (router started; store integration tested in Task 3).

- [ ] **Step 4: Commit**

```bash
git add distributed_cache/scaffold/docker-compose.go-full.yml
git commit -m "feat(distributed_cache): add dc-store service to go-full stack"
```

---

## Task 3: Router — `callStore`, env wiring, and `/health` update

**Files:**
- Modify: `distributed_cache/scaffold/go_router/main.go`

Context: `main.go` currently has globals at lines 33–53, helpers at 89–156, and `main()` starting at line 437. The changes below are additive — no existing code is removed.

- [ ] **Step 1: Add `storeURL` and `writeThroughMode` to globals**

In `main.go`, after the existing `var (...)` block (after line 53), add:

```go
var (
	storeURL         string // empty → write-through disabled
	writeThroughMode string // "parallel" | "store_first" | "cache_first"
)
```

- [ ] **Step 2: Add `callStore` helper after `callNode`**

After the `callNode` function (after line 122), add:

```go
// callStore sends one request to the backing store and returns the result.
// No circuit breaker — store failure surfaces directly as node_unreachable.
func callStore(ctx context.Context, method, key string, body []byte) nodeResult {
	req, _ := http.NewRequestWithContext(ctx, method, storeURL+"/store/"+key, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	resp, err := proxyClient.Do(req)
	if err != nil {
		return nodeResult{errMsg: "node_unreachable"}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return nodeResult{status: resp.StatusCode, body: respBody, headers: resp.Header.Clone()}
}
```

- [ ] **Step 3: Wire env vars in `main()`**

In `main()`, after the existing `getEnv` calls (after line 441), add:

```go
storeURL         = getEnv("STORE_URL", "")
writeThroughMode = getEnv("WRITE_THROUGH_MODE", "parallel")
```

Update the log line at the end of `main()` to include write-through info:

```go
log.Printf("Go router starting on :%s (nodes=%d, strategy=%s, vnodes=%d, store=%q, wt_mode=%s)",
    port, len(nodeURLs), stratEnv, virtualNodes, storeURL, writeThroughMode)
```

- [ ] **Step 4: Update `handleHealth` to include store status**

In `handleHealth`, replace the final `writeJSON` call (line 368) with:

```go
out := map[string]any{"status": status, "nodes": nodes}
if storeURL != "" {
    storeAlive := false
    resp, err := proxyClient.Get(storeURL + "/health")
    if err == nil && resp.StatusCode == http.StatusOK {
        storeAlive = true
    }
    if resp != nil {
        resp.Body.Close()
    }
    out["store"] = map[string]any{
        "alive":              storeAlive,
        "write_through_mode": writeThroughMode,
    }
}
writeJSON(w, http.StatusOK, out)
```

- [ ] **Step 5: Rebuild and verify**

```bash
cd distributed_cache/scaffold
podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build dc-router

curl -s http://localhost:8000/health | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('status:', d['status'])
print('store:', d.get('store'))
"
```

Expected output:
```
status: ok
store: {'alive': True, 'write_through_mode': 'parallel'}
```

- [ ] **Step 6: Commit**

```bash
git add distributed_cache/scaffold/go_router/main.go
git commit -m "feat(distributed_cache): callStore helper + STORE_URL/WRITE_THROUGH_MODE wiring"
```

---

## Task 4: Router — `handleSet` and `handleDelete` write-through modes

**Files:**
- Modify: `distributed_cache/scaffold/go_router/main.go`

- [ ] **Step 1: Replace `handleSet` with the write-through version**

Replace the entire `handleSet` function (lines 160–211 in current `main.go`) with:

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
		// Degraded ring (only one node alive) — single write, no replication or store
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

	// writeCacheNodes writes to primary + replica in parallel and returns their results.
	writeCacheNodes := func() []nodeResult {
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
		return results
	}

	if storeURL == "" {
		// No backing store — RF=2 replication only (original behaviour)
		results := writeCacheNodes()
		for i, nodeID := range nodes[:2] {
			if results[i].errMsg != "" || results[i].status >= 500 {
				requestsTotal.WithLabelValues("set", "503").Inc()
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "replication_failed", "node": nodeID,
				})
				return
			}
		}
		requestsTotal.WithLabelValues("set", strconv.Itoa(results[0].status)).Inc()
		writeResult(w, results[0])
		return
	}

	switch writeThroughMode {
	case "store_first":
		// Write store first; abort if store fails. Cache failure is best-effort (200).
		sr := callStore(r.Context(), "POST", key, body)
		if sr.errMsg != "" || sr.status >= 500 {
			requestsTotal.WithLabelValues("set", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": "write_through_failed", "failed": "store"})
			return
		}
		results := writeCacheNodes()
		for i, nodeID := range nodes[:2] {
			if results[i].errMsg != "" || results[i].status >= 500 {
				log.Printf("[write_through] store_first: cache write failed for %s (key=%s), store written", nodeID, key)
			}
		}
		// Return primary cache response if available; otherwise synthetic ok
		if results[0].errMsg == "" && results[0].status < 500 {
			requestsTotal.WithLabelValues("set", strconv.Itoa(results[0].status)).Inc()
			writeResult(w, results[0])
		} else {
			requestsTotal.WithLabelValues("set", "200").Inc()
			writeJSON(w, http.StatusOK, map[string]string{"key": key, "status": "stored", "warning": "cache_write_failed"})
		}

	case "cache_first":
		// Write cache first; abort if cache fails. Store failure is best-effort (200).
		results := writeCacheNodes()
		for i, nodeID := range nodes[:2] {
			if results[i].errMsg != "" || results[i].status >= 500 {
				requestsTotal.WithLabelValues("set", "503").Inc()
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "replication_failed", "node": nodeID,
				})
				return
			}
		}
		sr := callStore(r.Context(), "POST", key, body)
		if sr.errMsg != "" || sr.status >= 500 {
			log.Printf("[write_through] cache_first: store write failed (key=%s), cache written", key)
		}
		requestsTotal.WithLabelValues("set", strconv.Itoa(results[0].status)).Inc()
		writeResult(w, results[0])

	default: // "parallel"
		// Write cache nodes + store simultaneously; all three must succeed.
		cacheResults := make([]nodeResult, 2)
		var storeResult nodeResult
		var wg sync.WaitGroup
		wg.Add(3)
		for i, nodeID := range nodes[:2] {
			go func(i int, nodeID string) {
				defer wg.Done()
				cacheResults[i] = callNode(r.Context(), nodeID, "POST",
					nodeURLs[nodeID]+"/cache/"+key, body, r.Header)
			}(i, nodeID)
		}
		go func() {
			defer wg.Done()
			storeResult = callStore(r.Context(), "POST", key, body)
		}()
		wg.Wait()

		for i, nodeID := range nodes[:2] {
			if cacheResults[i].errMsg != "" || cacheResults[i].status >= 500 {
				requestsTotal.WithLabelValues("set", "503").Inc()
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "write_through_failed", "failed": nodeID,
				})
				return
			}
		}
		if storeResult.errMsg != "" || storeResult.status >= 500 {
			requestsTotal.WithLabelValues("set", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "write_through_failed", "failed": "store",
			})
			return
		}
		requestsTotal.WithLabelValues("set", strconv.Itoa(cacheResults[0].status)).Inc()
		writeResult(w, cacheResults[0])
	}
}
```

- [ ] **Step 2: Replace `handleDelete` with the write-through version**

Replace the entire `handleDelete` function (lines 241–290 in current `main.go`) with:

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

	deleteCacheNodes := func() []nodeResult {
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
		return results
	}

	if storeURL == "" {
		results := deleteCacheNodes()
		for i, nodeID := range nodes[:2] {
			if results[i].errMsg != "" || results[i].status >= 500 {
				requestsTotal.WithLabelValues("delete", "503").Inc()
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "replication_failed", "node": nodeID,
				})
				return
			}
		}
		requestsTotal.WithLabelValues("delete", strconv.Itoa(results[0].status)).Inc()
		writeResult(w, results[0])
		return
	}

	switch writeThroughMode {
	case "store_first":
		sr := callStore(r.Context(), "DELETE", key, body)
		if sr.errMsg != "" || sr.status >= 500 {
			requestsTotal.WithLabelValues("delete", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": "write_through_failed", "failed": "store"})
			return
		}
		results := deleteCacheNodes()
		for i, nodeID := range nodes[:2] {
			if results[i].errMsg != "" || results[i].status >= 500 {
				log.Printf("[write_through] store_first: cache delete failed for %s (key=%s)", nodeID, key)
			}
		}
		if results[0].errMsg == "" && results[0].status < 500 {
			requestsTotal.WithLabelValues("delete", strconv.Itoa(results[0].status)).Inc()
			writeResult(w, results[0])
		} else {
			requestsTotal.WithLabelValues("delete", "200").Inc()
			writeJSON(w, http.StatusOK, map[string]string{"key": key, "status": "deleted", "warning": "cache_delete_failed"})
		}

	case "cache_first":
		results := deleteCacheNodes()
		for i, nodeID := range nodes[:2] {
			if results[i].errMsg != "" || results[i].status >= 500 {
				requestsTotal.WithLabelValues("delete", "503").Inc()
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "replication_failed", "node": nodeID,
				})
				return
			}
		}
		sr := callStore(r.Context(), "DELETE", key, body)
		if sr.errMsg != "" || sr.status >= 500 {
			log.Printf("[write_through] cache_first: store delete failed (key=%s)", key)
		}
		requestsTotal.WithLabelValues("delete", strconv.Itoa(results[0].status)).Inc()
		writeResult(w, results[0])

	default: // "parallel"
		cacheResults := make([]nodeResult, 2)
		var storeResult nodeResult
		var wg sync.WaitGroup
		wg.Add(3)
		for i, nodeID := range nodes[:2] {
			go func(i int, nodeID string) {
				defer wg.Done()
				cacheResults[i] = callNode(r.Context(), nodeID, "DELETE",
					nodeURLs[nodeID]+"/cache/"+key, body, r.Header)
			}(i, nodeID)
		}
		go func() {
			defer wg.Done()
			storeResult = callStore(r.Context(), "DELETE", key, body)
		}()
		wg.Wait()

		for i, nodeID := range nodes[:2] {
			if cacheResults[i].errMsg != "" || cacheResults[i].status >= 500 {
				requestsTotal.WithLabelValues("delete", "503").Inc()
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"error": "write_through_failed", "failed": nodeID,
				})
				return
			}
		}
		if storeResult.errMsg != "" || storeResult.status >= 500 {
			requestsTotal.WithLabelValues("delete", "503").Inc()
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": "write_through_failed", "failed": "store",
			})
			return
		}
		requestsTotal.WithLabelValues("delete", strconv.Itoa(cacheResults[0].status)).Inc()
		writeResult(w, cacheResults[0])
	}
}
```

- [ ] **Step 3: Build and verify**

```bash
cd distributed_cache/scaffold
podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build dc-router

# Happy path: SET + GET + store verify
curl -s -X POST http://localhost:8000/cache/wt1 \
  -H 'Content-Type: application/json' \
  -d '{"value":"hello","ttl":300}'
# Expected: {"key":"wt1","node":"nodeX","status":"ok"}

curl -s http://localhost:8000/cache/wt1
# Expected: {"key":"wt1","value":"hello","ttl_remaining":...}

# Verify store has it (expose port 8004 in docker-compose.go-full.yml if not already)
# Or exec into dc-store container:
podman exec scaffold_dc-store_1 wget -qO- http://localhost:8004/store/wt1
# Expected: {"value":"hello","ttl":300}
```

- [ ] **Step 4: Rebuild and run `verify.sh` to confirm backward compat**

```bash
cd distributed_cache/scaffold
./scripts/verify.sh
```

Expected: `9 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add distributed_cache/scaffold/go_router/main.go
git commit -m "feat(distributed_cache): write-through handleSet + handleDelete (parallel/store_first/cache_first)"
```

---

## Task 5: Integration test `test_write_through.sh`

**Files:**
- Create: `distributed_cache/scaffold/scripts/test_write_through.sh`

The script switches modes by restarting dc-router with `WRITE_THROUGH_MODE=<mode> podman-compose up -d --force-recreate --no-deps dc-router`. This works because `docker-compose.go-full.yml` uses `${WRITE_THROUGH_MODE:-parallel}` substitution (set in Task 2).

- [ ] **Step 1: Add port 8004 to dc-store in `docker-compose.go-full.yml`**

Under `dc-store:`, add:
```yaml
    ports:
      - "8004:8004"
```

Rebuild: `podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build dc-store`

Verify: `curl -s http://localhost:8004/health` → `{"status":"ok"}`

- [ ] **Step 2: Create `scripts/test_write_through.sh`**

```bash
#!/usr/bin/env bash
# Write-through cache consistency modes test.
# Requires: podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build
#
# Switches WRITE_THROUGH_MODE by restarting dc-router with WRITE_THROUGH_MODE=<mode>
# podman-compose up --force-recreate --no-deps dc-router (compose file uses ${WRITE_THROUGH_MODE:-parallel}).
#
# Failure matrix tested:
#   parallel:    cache down → 503   store down → 503
#   store_first: cache down → 200 (store written)   store down → 503
#   cache_first: cache down → 503   store down → 200 (cache written, store stale)

set -euo pipefail

BASE="http://localhost:8000"
STORE="http://localhost:8004"
NODE2="scaffold_dc-node2_1"
STORE_CTR="scaffold_dc-store_1"
PASS=0; FAIL=0

ok()   { echo "  ✓ $1"; PASS=$((PASS+1)); }
fail() { echo "  ✗ $1"; FAIL=$((FAIL+1)); }

restart_router() {
  local mode="$1"
  echo "  → restarting router with WRITE_THROUGH_MODE=$mode"
  WRITE_THROUGH_MODE="$mode" podman-compose \
    -f docker-compose.yml -f docker-compose.go-full.yml \
    up -d --force-recreate --no-deps dc-router > /dev/null 2>&1
  sleep 4
}

ensure_all_up() {
  podman start "$NODE2"    > /dev/null 2>&1 || true
  podman start "$STORE_CTR" > /dev/null 2>&1 || true
  restart_router "parallel"
  sleep 6
  status=$(curl -s "$BASE/health" | python3 -c "
import sys,json; d=json.load(sys.stdin)
all_alive = all(s['alive'] for s in d['nodes'].values())
store_alive = d.get('store',{}).get('alive', False)
print('ok' if all_alive and store_alive else 'degraded')
")
  if [[ "$status" != "ok" ]]; then
    echo "  ERROR: not all services alive after preflight. Health:"
    curl -s "$BASE/health" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))"
    exit 1
  fi
}

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║   Write-Through Cache Consistency Modes Test              ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

# ── Preflight ───────────────────────────────────────────────────
echo "▶ Preflight — ensuring all services are up (parallel mode)"
ensure_all_up
echo "  All services alive."
echo ""

# ── Phase 1: parallel — happy path ──────────────────────────────
echo "▶ Phase 1 — parallel mode: happy path (SET writes both cache + store)"
for i in $(seq 1 5); do
  curl -s -X POST "$BASE/cache/wt$i" \
    -H 'Content-Type: application/json' \
    -d "{\"value\":\"val$i\",\"ttl\":300}" > /dev/null
done
store_hit=0
for i in $(seq 1 5); do
  sc=$(curl -s -o /dev/null -w "%{http_code}" "$STORE/store/wt$i")
  [[ "$sc" == "200" ]] && store_hit=$((store_hit+1))
done
[[ $store_hit -eq 5 ]] \
  && ok "parallel: all 5 keys written to store" \
  || fail "parallel: only $store_hit/5 keys in store"
echo ""

# ── Phase 2: parallel — store down → 503 ────────────────────────
echo "▶ Phase 2 — parallel mode: store down → SET returns 503"
podman stop "$STORE_CTR" > /dev/null
sleep 1
sc=$(curl -s -o /tmp/wt_body.txt -w "%{http_code}" \
  -X POST "$BASE/cache/wt_new" \
  -H 'Content-Type: application/json' -d '{"value":"x","ttl":60}')
err=$(python3 -c "import sys,json; print(json.load(sys.stdin).get('error','none'))" \
  < /tmp/wt_body.txt 2>/dev/null || echo "none")
[[ "$sc" == "503" && "$err" == "write_through_failed" ]] \
  && ok "parallel: store down → 503 write_through_failed" \
  || fail "parallel: store down → expected 503 write_through_failed, got HTTP $sc error=$err"
podman start "$STORE_CTR" > /dev/null
sleep 3
echo ""

# ── Phase 3: store_first — cache node down → 200 ────────────────
echo "▶ Phase 3 — store_first mode: cache node (node2) down → SET returns 200"
restart_router "store_first"

# Find a key that hashes to node2
NODE2_KEY=""
for i in $(seq 1 30); do
  node=$(curl -s "$BASE/ring/key$i" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])" 2>/dev/null || echo "")
  if [[ "$node" == "node2" ]]; then
    NODE2_KEY="key$i"; break
  fi
done
[[ -z "$NODE2_KEY" ]] && NODE2_KEY="key10"
echo "  Using $NODE2_KEY (hashes to node2)"

# Kill node2 and let CB open
podman stop "$NODE2" > /dev/null
sleep 1
for i in 1 2 3; do curl -s "$BASE/cache/$NODE2_KEY" > /dev/null; done
sleep 1

# SET a key that hashes to node2 → store written, cache cold → 200
sc=$(curl -s -o /tmp/wt_body.txt -w "%{http_code}" \
  -X POST "$BASE/cache/sf_test" \
  -H 'Content-Type: application/json' -d '{"value":"sf","ttl":60}')
[[ "$sc" == "200" ]] \
  && ok "store_first: cache node2 down → 200 (store written)" \
  || fail "store_first: cache node2 down → expected 200, got $sc"

# Verify store has the key
sc_store=$(curl -s -o /dev/null -w "%{http_code}" "$STORE/store/sf_test")
[[ "$sc_store" == "200" ]] \
  && ok "store_first: store has sf_test after cache failure" \
  || fail "store_first: store missing sf_test (got $sc_store)"

# GET from router → 404 or 503 (cache cold, no read-through)
sc_get=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/cache/sf_test")
[[ "$sc_get" == "404" || "$sc_get" == "503" ]] \
  && ok "store_first: router GET → $sc_get (cache cold, no read-through)" \
  || fail "store_first: router GET → expected 404/503, got $sc_get"

podman start "$NODE2" > /dev/null
sleep 6
echo ""

# ── Phase 4: cache_first — store down → 200 ─────────────────────
echo "▶ Phase 4 — cache_first mode: store down → SET returns 200"
restart_router "cache_first"

podman stop "$STORE_CTR" > /dev/null
sleep 1

sc=$(curl -s -o /tmp/wt_body.txt -w "%{http_code}" \
  -X POST "$BASE/cache/cf_test" \
  -H 'Content-Type: application/json' -d '{"value":"cf","ttl":60}')
[[ "$sc" == "200" ]] \
  && ok "cache_first: store down → 200 (cache written)" \
  || fail "cache_first: store down → expected 200, got $sc"

sc_get=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/cache/cf_test")
[[ "$sc_get" == "200" ]] \
  && ok "cache_first: router GET → 200 (data in cache)" \
  || fail "cache_first: router GET → expected 200, got $sc_get"

podman start "$STORE_CTR" > /dev/null
sleep 3

# Store must NOT have cf_test — it was down during the write
sc_store=$(curl -s -o /dev/null -w "%{http_code}" "$STORE/store/cf_test")
[[ "$sc_store" == "404" ]] \
  && ok "cache_first: store missing cf_test after recovery (store was stale)" \
  || fail "cache_first: store has cf_test unexpectedly (got $sc_store)"
echo ""

# ── Phase 5: Recovery ────────────────────────────────────────────
echo "▶ Phase 5 — Recovery: restore parallel mode, verify normal operation"
restart_router "parallel"
sleep 3

sc=$(curl -s -o /tmp/wt_recovery.txt -w "%{http_code}" \
  -X POST "$BASE/cache/recovery_test" \
  -H 'Content-Type: application/json' -d '{"value":"ok","ttl":60}')
node=$(python3 -c "import sys,json; print(json.load(sys.stdin).get('status','fail'))" \
  < /tmp/wt_recovery.txt 2>/dev/null || echo "fail")
[[ "$sc" == "200" && "$node" == "ok" ]] \
  && ok "recovery: parallel mode SET works after all restarts" \
  || fail "recovery: SET failed (HTTP $sc status=$node)"
echo ""

# ── Summary ──────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
```

- [ ] **Step 3: Make executable and run**

```bash
chmod +x distributed_cache/scaffold/scripts/test_write_through.sh
cd distributed_cache/scaffold
./scripts/test_write_through.sh
```

Expected: `7 passed, 0 failed`

- [ ] **Step 4: Run `verify.sh` to confirm nothing regressed**

```bash
./scripts/verify.sh
```

Expected: `9 passed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add distributed_cache/scaffold/scripts/test_write_through.sh
git add distributed_cache/scaffold/docker-compose.go-full.yml
git commit -m "test(distributed_cache): test_write_through.sh — 5-phase write-through modes test"
```

---

## Task 6: Document write-through trade-offs in `key_learnings.md`

**Files:**
- Modify: `distributed_cache/notes/key_learnings.md`

- [ ] **Step 1: Append §16 to `key_learnings.md`**

Add the following section at the end of the file (after §15):

```markdown
---

## 16. Write-Through Cache：三種一致性模式的代價

### 問題：Cache 與 DB 的數據不一致

In-memory cache 重啟就丟失所有數據。如果 client 的 SET 只寫進 cache，沒有同時寫入 DB，節點 crash 後就永久丟失。Write-through 解決「寫入持久化」的問題——每次 SET 都同時更新 cache 和 backing store（DB）。

### 三種寫入模式的比較

| 模式 | Cache 掛 | Store 掛 | 一致性保證 |
|------|---------|---------|-----------|
| `parallel` | 503 | 503 | 最強：兩邊都成功才 200 |
| `store_first` | 200（cache 冷） | 503 | DB 優先：至少 DB 有資料 |
| `cache_first` | 503 | 200（store 舊） | Cache 優先：至少 cache 有資料 |

### 觀察到的行為差異

**`parallel` 模式**：
```
store 掛掉 → POST /cache/key → 503 write_through_failed
cache node 掛掉 → POST /cache/key → 503 write_through_failed
```
寫入可用性最低，但一致性最強。

**`store_first` 模式**：
```
cache node 掛掉 → POST /cache/key → 200
  → store 有這個 key
  → GET /cache/key → 404（cache cold，不 read-through）
```
DB 是 source of truth。Cache 只是加速層；cache miss 是正常的降級狀態。

**`cache_first` 模式**：
```
store 掛掉 → POST /cache/key → 200
  → cache 有這個 key
  → GET /store/key → 404（store stale）
  → store 恢復後，這個 key 仍然不在 store 裡（沒有 backfill）
```
Cache 優先，但存在「store 永久遺失資料」的風險，除非有 backfill 機制。

### Write-Through 的根本取捨

| 取捨 | 說明 |
|------|------|
| 寫入延遲 | parallel: +~0ms（與複製一樣，latency = max(cache, store)） |
| 寫入可用性 | parallel < store_first ≈ cache_first |
| 資料持久性 | parallel = store_first > cache_first |
| 讀取行為 | GET miss 仍 404（no read-through）；這是獨立的設計選擇 |

### 帶走的原則

1. **Write-through vs Write-back**：Write-through 是同步寫入（latency 上升但不丟資料）；write-back 是非同步寫入（latency 低但 crash 時 dirty buffer 丟失）。選擇取決於資料的重要性。
2. **模式選擇 = 失敗時你信任哪邊**：`store_first` = 信任 DB；`cache_first` = 信任 cache（適合 DB 很慢、可以最終一致的場景）。
3. **No read-through 是一個有意識的選擇**：Read-through 讓 cache 透明（client 不感知 miss），但增加複雜度（router 需要知道 DB schema）。這個系統選擇 miss → 404，讓 client 自己決定要不要回 DB 拿。
4. **Backfill 問題**：`cache_first` 模式下 store 掛掉恢復後，cache 裡的 key 不會自動同步回 store。需要額外的 reconciliation 機制（定期掃描 cache、CDC 等）才能解決持久性問題。
```

- [ ] **Step 2: Commit**

```bash
git add distributed_cache/notes/key_learnings.md
git commit -m "docs(distributed_cache): key_learnings §16 — write-through consistency modes"
```
