#!/bin/bash
# auth_refusals.sh -- Authentication refusals are the middleware's own 401
#
# Since bsv-middleware-cloudflare 0.4.1 every refusal is answered by the
# middleware itself (401, its error JSON, CORS) and the worker passes it through
# unchanged (`AuthResult::Response` in src/lib.rs). Checks:
#   1. No auth headers            -> 401 UNAUTHORIZED ("message" field)
#   2. General message, no session -> 401 ERR_SESSION_NOT_FOUND
#   3. Unreadable handshake        -> 401 ERR_INVALID_AUTH
# ERR_REPLAYED_REQUEST needs a signed session and is not exercised here.
#
# Usage: ./tests/e2e/auth_refusals.sh [base_url]

set -euo pipefail

BASE_URL="${1:-https://wallet-infra.x402agency.com}"
BASE_URL="${BASE_URL%/}"
PASSED=0
FAILED=0

pass() { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }

RPC='{"jsonrpc":"2.0","method":"listOutputs","params":{},"id":1}'
# The secp256k1 generator point: a valid key no session belongs to.
IDENTITY="0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
NONCE="$(openssl rand -base64 32)"

# check <name> <expected code> <curl args...>
check() {
    local name="$1" code="$2"
    shift 2
    local out status cors body
    out=$(curl -s -i "$@")
    status=$(echo "$out" | head -1 | awk '{print $2}')
    cors=$(echo "$out" | grep -i '^access-control-allow-origin:' || true)
    body=$(echo "$out" | tail -1)
    if [ "$status" = "401" ] && [ -n "$cors" ] \
        && echo "$body" | jq -e --arg c "$code" '.status == "error" and .code == $c' > /dev/null 2>&1; then
        pass "${name}: 401 ${code} with CORS"
    else
        fail "${name}: expected 401 ${code} with CORS, got ${status} ${body}"
    fi
}

echo "--- Auth refusals (${BASE_URL}) ---"

check "no auth headers" "UNAUTHORIZED" \
    -X POST "${BASE_URL}/" -H 'Content-Type: application/json' -d "$RPC"

check "unknown session" "ERR_SESSION_NOT_FOUND" \
    -X POST "${BASE_URL}/" -H 'Content-Type: application/json' \
    -H 'x-bsv-auth-version: 0.1' \
    -H "x-bsv-auth-identity-key: ${IDENTITY}" \
    -H "x-bsv-auth-nonce: ${NONCE}" \
    -H "x-bsv-auth-your-nonce: ${NONCE}" \
    -H "x-bsv-auth-request-id: ${NONCE}" \
    -H 'x-bsv-auth-signature: 3006020101020101' \
    -d "$RPC"

check "unreadable handshake" "ERR_INVALID_AUTH" \
    -X POST "${BASE_URL}/.well-known/auth" -H 'Content-Type: application/json' \
    -d '{"version":"0.1","messageType":"initialRequest","identityKey":"zz"}'

echo ""
echo "Auth refusals: ${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ]
