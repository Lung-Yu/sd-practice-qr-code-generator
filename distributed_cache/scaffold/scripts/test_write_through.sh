#!/usr/bin/env bash
# Write-through cache consistency modes test.
# Requires: podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d --build
#
# Switches WRITE_THROUGH_MODE by restarting dc-router with:
#   WRITE_THROUGH_MODE=<mode> podman-compose ... up -d --force-recreate --no-deps dc-router
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

# SET a key → store written, cache cold (node2 down) → 200
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

# GET from router using a key on node2 (which is down) → 404 or 503 (cache cold, no read-through)
sc_get=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/cache/$NODE2_KEY")
[[ "$sc_get" == "404" || "$sc_get" == "503" ]] \
  && ok "store_first: router GET $NODE2_KEY → $sc_get (node2 down, cache cold, no read-through)" \
  || fail "store_first: router GET $NODE2_KEY → expected 404/503, got $sc_get"

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
