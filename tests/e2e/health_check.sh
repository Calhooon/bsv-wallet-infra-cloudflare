#!/bin/bash
# health_check.sh -- Basic deployment verification for wallet-infra
#
# Verifies the Worker is alive, answers CORS preflight, and that the JSON-RPC
# endpoint is behind BRC-31 auth.
#
# Why this changed (2026-10-09): since bsv-middleware-cloudflare 0.4.1 every
# unauthenticated POST is refused by the middleware's own 401 JSON
# (`{"status":"error","code":...}`, CORS set) before the JSON-RPC layer reads
# the body. The old checks ("makeAvailable returns a JSON-RPC result", "invalid
# JSON returns a JSON-RPC parse error") assumed unauthenticated RPC and failed
# identically on 0.4.1 and 0.5.0. They now assert that 401 contract instead.
# There is no /health route (it is 401 too): the health check is GET /.
#
# Read-only: no request here reaches storage.
#
# Usage: ./tests/e2e/health_check.sh [base_url]

set -euo pipefail

BASE_URL="${1:-https://wallet-infra.x402agency.com}"
BASE_URL="${BASE_URL%/}"
PASSED=0
FAILED=0

pass() { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }

REFUSAL_CODES='["UNAUTHORIZED","ERR_SESSION_NOT_FOUND","ERR_INVALID_AUTH","ERR_REPLAYED_REQUEST"]'
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# request <url> <curl args...> -- sets STATUS, CORS, BODY, CODE
request() {
    local url="$1"
    shift
    STATUS=$(curl -s -D "$TMP/h" -o "$TMP/b" -w '%{http_code}' "$@" "$url" || echo "000")
    CORS=$(grep -i '^access-control-allow-origin:' "$TMP/h" || true)
    BODY=$(cat "$TMP/b")
    CODE=$(echo "$BODY" | jq -r '.code // empty' 2>/dev/null || true)
}

# expect_refusal <name> <url> <curl args...> -- 401, a refusal code, CORS, no JSON-RPC envelope
expect_refusal() {
    local name="$1" url="$2"
    shift 2
    request "$url" "$@"
    if [ "$STATUS" = "401" ] && [ -n "$CORS" ] \
        && echo "$BODY" | jq -e --argjson codes "$REFUSAL_CODES" \
            '.status == "error" and (.code as $c | $codes | index($c)) and (has("jsonrpc") | not)' \
            > /dev/null 2>&1; then
        pass "${name}: 401 ${CODE} with CORS"
    else
        fail "${name}: POST ${url} expected 401 refusal with CORS, got status ${STATUS} code '${CODE}' cors '${CORS:+yes}' body ${BODY}"
    fi
}

echo "=== Health Check: ${BASE_URL} ==="
echo ""

# ---------------------------------------------------------------------------
# 1. GET / returns 200 with the service JSON
# ---------------------------------------------------------------------------
echo "--- Test 1: GET / (health) ---"
request "${BASE_URL}/"
if [ "$STATUS" = "200" ] && [ -n "$CORS" ] \
    && echo "$BODY" | jq -e '.status == "ok" and .service == "wallet-infra"' > /dev/null 2>&1; then
    pass "GET / returned 200 service JSON (broadcaster $(echo "$BODY" | jq -r '.broadcaster'))"
else
    fail "GET ${BASE_URL}/ expected 200 {status:ok,service:wallet-infra} with CORS, got status ${STATUS} code '${CODE}' body ${BODY}"
fi

# ---------------------------------------------------------------------------
# 2. CORS preflight
# ---------------------------------------------------------------------------
echo "--- Test 2: OPTIONS / (CORS preflight) ---"
request "${BASE_URL}/" -X OPTIONS
if [ "$STATUS" = "204" ] && [ -n "$CORS" ] && grep -qi '^access-control-allow-headers:.*x-bsv-auth-identity-key' "$TMP/h"; then
    pass "OPTIONS / returned 204 with BRC-31 CORS headers"
else
    fail "OPTIONS ${BASE_URL}/ expected 204 with x-bsv-auth CORS headers, got status ${STATUS} code '${CODE}' body ${BODY}"
fi

# ---------------------------------------------------------------------------
# 3. makeAvailable without auth is the middleware's 401
# ---------------------------------------------------------------------------
echo "--- Test 3: makeAvailable without auth ---"
expect_refusal "makeAvailable" "${BASE_URL}/" -X POST \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","method":"makeAvailable","params":[],"id":1}'

# ---------------------------------------------------------------------------
# 4. Invalid JSON body without auth is refused before parsing
# ---------------------------------------------------------------------------
echo "--- Test 4: Invalid JSON body without auth ---"
expect_refusal "Invalid JSON body" "${BASE_URL}/" -X POST \
    -H "Content-Type: application/json" \
    -d 'not json at all'

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Health Check Summary: ${PASSED} passed, ${FAILED} failed ==="
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
