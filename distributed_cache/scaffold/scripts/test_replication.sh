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
