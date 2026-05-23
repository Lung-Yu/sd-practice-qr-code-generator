#!/usr/bin/env bash
# Node failure & recovery test.
# Requires the full Go stack to be running:
#   podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d
#
# What this tests:
#   1. Seed 30 keys across all three nodes
#   2. Kill dc-node2
#   3. Wait for health checker to detect the failure (≤ 6s)
#   4. Show that ALL keys still return 200 or 404 — no 503 (ring rerouted)
#   5. Confirm node2 keys are now cache misses (data was in node2's memory)
#   6. Restart dc-node2 and wait for recovery
#   7. Show ring restored and node2 keys reachable again after re-seeding

set -euo pipefail

BASE="http://localhost:8000"
NODE2="scaffold_dc-node2_1"
PASS=0; FAIL=0

ok()   { echo "  ✓ $1"; PASS=$((PASS+1)); }
fail() { echo "  ✗ $1"; FAIL=$((FAIL+1)); }

check_status() {
  local label="$1" expected="$2"
  local status
  status=$(curl -s -o /dev/null -w "%{http_code}" "${@:3}")
  if [[ "$status" == "$expected" ]]; then ok "$label (HTTP $status)"
  else fail "$label (expected $expected, got $status)"; fi
}

health() {
  curl -s "$BASE/health" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  status={d[\"status\"]}')
for nid, s in sorted(d['nodes'].items()):
    icon = '🟢' if s['alive'] else '🔴'
    print(f'  {icon}  {nid}: alive={s[\"alive\"]}, failures={s[\"failures\"]}')
"
}

echo "╔══════════════════════════════════════════════════╗"
echo "║       Node Failure + Health Check Test           ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── Phase 1: Seed keys ──────────────────────────────────────────────────────
echo "▶ Phase 1 — Seed 30 keys"
for i in $(seq 1 30); do
  curl -s -X POST "$BASE/cache/key$i" \
    -H 'Content-Type: application/json' \
    -d "{\"value\":\"val$i\",\"ttl\":300}" > /dev/null
done
echo "  Seeded key1…key30"
echo ""

# ── Phase 2: Show which keys live on each node ─────────────────────────────
echo "▶ Phase 2 — Ring inspection (sample)"
for k in key1 key5 key10 key15 key20 key25 key30; do
  node=$(curl -s "$BASE/ring/$k" | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])")
  echo "  $k → $node"
done
echo ""

# ── Phase 3: Health before failure ─────────────────────────────────────────
echo "▶ Phase 3 — Health BEFORE failure"
health
echo ""

# ── Phase 4: Kill node2 ────────────────────────────────────────────────────
echo "▶ Phase 4 — Stopping $NODE2 …"
podman stop "$NODE2" > /dev/null
echo "  Container stopped."
echo "  Waiting 6s for health checker (interval=2s, threshold=2 failures)…"
sleep 6
echo ""

# ── Phase 5: Health after failure detected ─────────────────────────────────
echo "▶ Phase 5 — Health AFTER failure detected"
health
echo ""

# ── Phase 6: All keys must return 200 or 404 — never 503 ──────────────────
echo "▶ Phase 6 — Access all 30 keys (must not get 503)"
errors=0; misses=0; hits=0
for i in $(seq 1 30); do
  status=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/cache/key$i")
  case "$status" in
    200) hits=$((hits+1)) ;;
    404) misses=$((misses+1)) ;;
    *)   errors=$((errors+1)); echo "  ✗ key$i returned $status" ;;
  esac
done
if [[ $errors -eq 0 ]]; then
  ok "All 30 keys returned 200 or 404 (hits=$hits, misses=$misses, errors=0)"
else
  fail "$errors keys returned unexpected status (503 = ring not updated yet?)"
fi
echo ""

# ── Phase 7: Confirm node2's keys are now misses ──────────────────────────
echo "▶ Phase 7 — Ring: node2 keys should now route to another node"
for k in key1 key5 key10 key15 key20 key25 key30; do
  node=$(curl -s "$BASE/ring/$k" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('node', 'ERROR: ' + str(d)))
" 2>/dev/null)
  echo "  $k → $node (was potentially node2, now rerouted)"
done
echo ""

# ── Phase 8: Restart node2 ────────────────────────────────────────────────
echo "▶ Phase 8 — Restarting $NODE2 …"
podman start "$NODE2" > /dev/null
echo "  Container started."
echo "  Waiting 6s for health checker to detect recovery…"
sleep 6
echo ""

# ── Phase 9: Health after recovery ────────────────────────────────────────
echo "▶ Phase 9 — Health AFTER recovery"
health
echo ""

# ── Phase 10: Re-seed and verify node2 is used again ─────────────────────
echo "▶ Phase 10 — Re-seed 10 keys and check ring routes to all 3 nodes"
for i in $(seq 1 10); do
  curl -s -X POST "$BASE/cache/reseed$i" \
    -H 'Content-Type: application/json' \
    -d "{\"value\":\"new$i\",\"ttl\":60}" > /dev/null
done
nodes_used=$(for i in $(seq 1 10); do
  curl -s "$BASE/ring/reseed$i" | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])"
done | sort -u | tr '\n' ' ')
echo "  Nodes used by reseed keys: $nodes_used"
if echo "$nodes_used" | grep -q "node2"; then
  ok "node2 is back in rotation"
else
  fail "node2 not appearing in ring assignments (recovery may have failed)"
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────
echo "══════════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
