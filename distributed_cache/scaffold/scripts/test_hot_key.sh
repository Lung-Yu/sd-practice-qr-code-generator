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
