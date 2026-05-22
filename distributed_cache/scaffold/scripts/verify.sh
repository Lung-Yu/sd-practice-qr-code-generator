#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:8000"
PASS=0; FAIL=0

check() {
  local name="$1"; local expected_status="$2"; local actual_status="$3"; local body="$4"; local grep_pattern="${5:-}"
  if [[ "$actual_status" == "$expected_status" ]] && { [[ -z "$grep_pattern" ]] || echo "$body" | grep -q "$grep_pattern"; }; then
    echo "  ✓ $name"
    PASS=$((PASS + 1))
  else
    echo "  ✗ $name (status=$actual_status, body=$body)"
    FAIL=$((FAIL + 1))
  fi
}

echo "=== Distributed Cache Verification ==="
echo ""

# Test 1: SET foo=bar (ttl=60)
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/cache/foo" -H 'Content-Type: application/json' -d '{"value":"bar","ttl":60}')
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "SET foo=bar (ttl=60)" "200" "$STATUS" "$BODY" "ok"

# Test 2: GET foo
RESP=$(curl -s -w "\n%{http_code}" "$BASE/cache/foo")
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "GET foo" "200" "$STATUS" "$BODY" "bar"

# Test 3: Ring inspect foo
RESP=$(curl -s -w "\n%{http_code}" "$BASE/ring/foo")
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "Ring inspect foo" "200" "$STATUS" "$BODY" "node"

# Test 4: GET missing key
RESP=$(curl -s -w "\n%{http_code}" "$BASE/cache/__missing_key_xyz__")
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "GET missing key" "404" "$STATUS" "$BODY" "miss"

# Test 5: DELETE foo
RESP=$(curl -s -w "\n%{http_code}" -X DELETE "$BASE/cache/foo")
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "DELETE foo" "200" "$STATUS" "$BODY" "deleted"

# Test 6: GET foo after delete
RESP=$(curl -s -w "\n%{http_code}" "$BASE/cache/foo")
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "GET foo after delete" "404" "$STATUS" "$BODY"

# Test 7: SET baz with ttl=2
RESP=$(curl -s -w "\n%{http_code}" -X POST "$BASE/cache/baz" -H 'Content-Type: application/json' -d '{"value":"qux","ttl":2}')
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "SET baz with ttl=2" "200" "$STATUS" "$BODY" "ok"

# Test 8: TTL expiry (sleep 3s)
echo "  (sleeping 3s for TTL expiry test...)"
sleep 3
RESP=$(curl -s -w "\n%{http_code}" "$BASE/cache/baz")
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "TTL expiry (sleep 3s)" "404" "$STATUS" "$BODY" "expired"

# Test 9: Stats endpoint
RESP=$(curl -s -w "\n%{http_code}" "$BASE/stats")
BODY=$(echo "$RESP" | head -1); STATUS=$(echo "$RESP" | tail -1)
check "Stats endpoint" "200" "$STATUS" "$BODY" "hits"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
