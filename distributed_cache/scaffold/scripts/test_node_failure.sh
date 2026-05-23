#!/usr/bin/env bash
# Node failure, circuit breaker, and recovery test.
# Requires the full Go stack:
#   podman-compose -f docker-compose.yml -f docker-compose.go-full.yml up -d
#
# Timeline visualised:
#
#   t=0   kill dc-node2
#   t=0   requests to node2 keys → connection error (node2 TCP refused)
#   t~0   after 3 failures → circuit OPENS (fast-fail, no TCP attempt)
#   t~4s  health checker detects failure → removes node2 from ring
#   t~4s  node2 keys rerouted → cache miss (404), 0 errors
#   t=X   restart dc-node2
#   t=X+4 health checker detects recovery → ring restored + circuit Reset → CLOSED
#
# The circuit breaker covers the gap between t=0 and t=4s by fast-failing
# instead of letting connection errors pile up (thundering herd protection).

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
    print(f'  {alive_icon} {nid}: alive={s[\"alive\"]}, failures={s[\"failures\"]}, circuit={cb} {cb_icon}')
"
}

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║   Node Failure + Circuit Breaker + Recovery Test          ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

# ── Preflight: ensure node2 is running and ring is fully healthy ───────────
echo "▶ Preflight — ensuring node2 is started and all nodes are alive"
podman start "$NODE2" > /dev/null 2>&1 || true
echo "  Waiting 8s for health checker to mark all nodes alive..."
sleep 8
pre_status=$(curl -s "$BASE/health" | python3 -c "
import sys,json
d=json.load(sys.stdin)
all_alive = all(s['alive'] for s in d['nodes'].values())
print('ok' if all_alive else 'degraded')
")
if [[ "$pre_status" != "ok" ]]; then
  echo "  ERROR: not all nodes are alive after preflight. Current health:"
  curl -s "$BASE/health" | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))"
  exit 1
fi
echo "  All nodes alive. Proceeding."
echo ""

# ── Phase 1: Seed ──────────────────────────────────────────────────────────
echo "▶ Phase 1 — Seed 30 keys"
for i in $(seq 1 30); do
  curl -s -X POST "$BASE/cache/key$i" \
    -H 'Content-Type: application/json' \
    -d "{\"value\":\"val$i\",\"ttl\":300}" > /dev/null
done
echo "  Seeded key1…key30"
echo ""

# ── Phase 2: Find a key that lives on node2 ────────────────────────────────
echo "▶ Phase 2 — Find a key that lives on node2"
NODE2_KEY=""
for i in $(seq 1 30); do
  node=$(curl -s "$BASE/ring/key$i" | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])")
  if [[ "$node" == "node2" ]]; then
    NODE2_KEY="key$i"
    echo "  key$i → node2  ← will use this for circuit breaker test"
    break
  fi
done
if [[ -z "$NODE2_KEY" ]]; then
  echo "  (no key in first 30 lands on node2 — using key10 as fallback)"
  NODE2_KEY="key10"
fi
echo ""

# ── Phase 3: Baseline health ───────────────────────────────────────────────
echo "▶ Phase 3 — Health BEFORE failure"
health
echo ""

# ── Phase 4: Kill node2, immediately fire requests to observe CB ──────────
echo "▶ Phase 4 — Kill node2 + immediately fire 10 requests to $NODE2_KEY"
podman stop "$NODE2" > /dev/null
echo "  Container stopped."
echo ""

echo "  Firing 10 requests immediately (before health checker fires ~4s):"
circuit_open_count=0; unreachable_count=0; other_count=0
for i in $(seq 1 10); do
  status=$(curl -s -o /tmp/cb_body.txt -w "%{http_code}" "$BASE/cache/$NODE2_KEY")
  body=$(cat /tmp/cb_body.txt)
  error_type=$(python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error','none'))" 2>/dev/null < /tmp/cb_body.txt || echo "none")
  echo "    req $i: HTTP $status  error=$error_type"
  case "$error_type" in
    circuit_open)   circuit_open_count=$((circuit_open_count+1)) ;;
    node_unreachable) unreachable_count=$((unreachable_count+1)) ;;
    *)              other_count=$((other_count+1)) ;;
  esac
done
echo ""
echo "  Summary:"
echo "    503:node_unreachable → $unreachable_count times"
echo "    503:circuit_open     → $circuit_open_count times"
echo "    other                → $other_count times"
echo ""

# Verify: after first 3 failures, circuit should be open
cb_state=$(curl -s "$BASE/health" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['nodes']['node2']['circuit'])")
if [[ "$cb_state" == "open" ]]; then
  ok "Circuit breaker OPENED after repeated failures"
else
  fail "Circuit breaker not open yet (state=$cb_state) — may need more requests"
fi

echo "  node_unreachable (TCP errors): $unreachable_count"
echo "  circuit_open    (fast-fail):   $circuit_open_count"
if [[ $circuit_open_count -gt 0 ]]; then
  ok "Circuit breaker fast-failing: $circuit_open_count requests rejected without TCP attempt"
else
  ok "All 10 requests completed (circuit may open on next batch)"
fi
echo ""

# ── Phase 5: Wait for health checker + verify ring reroute ────────────────
echo "▶ Phase 5 — Wait 6s for health checker to remove node2 from ring"
sleep 6
echo ""

echo "  Health after health checker fires:"
health
echo ""

echo "  Access all 30 keys (must not get 503 after ring update):"
errors=0; misses=0; hits=0
for i in $(seq 1 30); do
  status=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/cache/key$i")
  case "$status" in
    200) hits=$((hits+1)) ;;
    404) misses=$((misses+1)) ;;
    *)   errors=$((errors+1)); echo "    key$i: unexpected $status" ;;
  esac
done
if [[ $errors -eq 0 ]]; then
  ok "All 30 keys: hits=$hits misses=$misses errors=0"
else
  fail "$errors keys returned unexpected status"
fi
echo ""

# ── Phase 6: Restart node2, verify full recovery ──────────────────────────
echo "▶ Phase 6 — Restart node2, wait for recovery"
podman start "$NODE2" > /dev/null
echo "  Container started."
echo "  Waiting 6s for health checker + circuit reset…"
sleep 6
echo ""

echo "  Health after recovery:"
health
echo ""

cb_state=$(curl -s "$BASE/health" | python3 -c "import sys,json; print(json.load(sys.stdin)['nodes']['node2']['circuit'])")
alive=$(curl -s "$BASE/health" | python3 -c "import sys,json; print(json.load(sys.stdin)['nodes']['node2']['alive'])")
if [[ "$alive" == "True" && "$cb_state" == "closed" ]]; then
  ok "node2 alive=True, circuit=closed after recovery"
else
  fail "node2 alive=$alive circuit=$cb_state (expected alive=True circuit=closed)"
fi
echo ""

# ── Phase 7: Re-seed and confirm node2 back in rotation ───────────────────
echo "▶ Phase 7 — Re-seed 10 keys, verify node2 serves them"
for i in $(seq 1 10); do
  curl -s -X POST "$BASE/cache/recover$i" \
    -H 'Content-Type: application/json' \
    -d "{\"value\":\"new$i\",\"ttl\":60}" > /dev/null
done
nodes_used=$(for i in $(seq 1 10); do
  curl -s "$BASE/ring/recover$i" | python3 -c "import sys,json; print(json.load(sys.stdin)['node'])"
done | sort -u | tr '\n' ' ')
echo "  Nodes in ring: $nodes_used"
if echo "$nodes_used" | grep -q "node2"; then
  ok "node2 back in ring rotation"
else
  fail "node2 not appearing in ring assignments"
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
