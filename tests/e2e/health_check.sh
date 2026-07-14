#!/bin/bash
# health_check.sh -- Basic deployment verification for wallet-infra
#
# Verifies the Worker is alive and the unauthenticated JSON-RPC endpoint responds.
#
# Usage: ./tests/e2e/health_check.sh [base_url]

set -euo pipefail

BASE_URL="${1:-https://<your-worker-domain>}"
PASSED=0
FAILED=0

pass() { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }

echo "=== Health Check: ${BASE_URL} ==="
echo ""

# ---------------------------------------------------------------------------
# 1. /health endpoint returns 200
# ---------------------------------------------------------------------------
echo "--- Test 1: GET /health ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/")
if [ "$HTTP_CODE" = "200" ]; then
    pass "/health returned 200"
else
    fail "/health returned ${HTTP_CODE} (expected 200)"
fi

# ---------------------------------------------------------------------------
# 2. makeAvailable (no auth required) returns a JSON-RPC result
# ---------------------------------------------------------------------------
echo "--- Test 2: makeAvailable ---"
RESULT=$(curl -s -X POST "${BASE_URL}" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","method":"makeAvailable","params":[],"id":1}')

if echo "$RESULT" | jq -e '.result' > /dev/null 2>&1; then
    pass "makeAvailable returned valid result"
else
    fail "makeAvailable returned error or invalid JSON: ${RESULT}"
fi

# ---------------------------------------------------------------------------
# 3. POST to root with invalid JSON returns a parse error
# ---------------------------------------------------------------------------
echo "--- Test 3: Invalid JSON body ---"
RESULT=$(curl -s -X POST "${BASE_URL}" \
    -H "Content-Type: application/json" \
    -d 'not json at all')

if echo "$RESULT" | jq -e '.error' > /dev/null 2>&1; then
    pass "Invalid JSON body returned error response"
else
    fail "Invalid JSON body did not return error: ${RESULT}"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Health Check Summary: ${PASSED} passed, ${FAILED} failed ==="
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
