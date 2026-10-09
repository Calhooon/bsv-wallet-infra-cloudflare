#!/bin/bash
# json_rpc_smoke.sh -- Smoke test all JSON-RPC methods
#
# Tests:
#   1. Every JSON-RPC method, called without auth, gets the middleware's 401
#   2. An unknown method, called without auth, gets the same 401
#   3. RPC-layer behaviour (method_not_found, protocol conformance) -- SKIPPED
#
# Why this changed (2026-10-09): since bsv-middleware-cloudflare 0.4.1 every
# unauthenticated call is answered by the middleware's own 401 JSON
# (`{"status":"error","code":"UNAUTHORIZED"|"ERR_SESSION_NOT_FOUND"|
# "ERR_INVALID_AUTH"|"ERR_REPLAYED_REQUEST","message":...}`, CORS set) before
# the JSON-RPC layer sees the body -- including makeAvailable, migrate,
# findOrInsertUser and the storage-transaction stubs, which used to answer
# unauthenticated. The old suite looked for JSON-RPC envelopes there and failed
# identically on 0.4.1 and 0.5.0. A JSON-RPC envelope on an unauthenticated
# call is now wrong; the 401 is the contract.
#
# Section 3 needs a BRC-104 session to reach the RPC layer. There is no bash
# handshake helper in the repo, and a session against production would write
# (KV session, resolve_auth auto-creates the user), so those cases are SKIPPED
# and named with the Rust unit tests that cover them.
#
# Read-only: every request here is refused before storage.
#
# Usage: ./tests/e2e/json_rpc_smoke.sh [base_url]

set -euo pipefail

BASE_URL="${1:-https://wallet-infra.x402agency.com}"
BASE_URL="${BASE_URL%/}"
PASSED=0
FAILED=0
SKIPPED=0
ID=0

pass() { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }
skip() { SKIPPED=$((SKIPPED + 1)); echo "  SKIPPED: $1"; }

REFUSAL_CODES='["UNAUTHORIZED","ERR_SESSION_NOT_FOUND","ERR_INVALID_AUTH","ERR_REPLAYED_REQUEST"]'
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# Send an unauthenticated JSON-RPC call; sets STATUS, CORS, BODY, CODE
rpc_call() {
    local method="$1"
    local params="$2"
    ID=$((ID + 1))
    STATUS=$(curl -s -D "$TMP/h" -o "$TMP/b" -w '%{http_code}' -X POST "${BASE_URL}/" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"${method}\",\"params\":${params},\"id\":${ID}}" \
        || echo "000")
    CORS=$(grep -i '^access-control-allow-origin:' "$TMP/h" || true)
    BODY=$(cat "$TMP/b")
    CODE=$(echo "$BODY" | jq -r '.code // empty' 2>/dev/null || true)
}

# 401, a refusal code, CORS, and no JSON-RPC envelope
assert_refused() {
    local method="$1"
    if [ "$STATUS" = "401" ] && [ -n "$CORS" ] \
        && echo "$BODY" | jq -e --argjson codes "$REFUSAL_CODES" \
            '.status == "error" and (.code as $c | $codes | index($c)) and (has("jsonrpc") | not)' \
            > /dev/null 2>&1; then
        pass "${method} refused without auth (401 ${CODE})"
    else
        fail "${method}: POST ${BASE_URL}/ expected 401 refusal with CORS, got status ${STATUS} code '${CODE}' cors '${CORS:+yes}' body ${BODY}"
    fi
}

echo "=== JSON-RPC Smoke Tests: ${BASE_URL} ==="
echo ""

# ============================================================================
# Section 1: Every method without auth -- the middleware's 401
# ============================================================================
echo "--- Section 1: All methods without auth (expect 401 refusal) ---"

# Positional params as BSV Toolbox's StorageClient sends them; never reach storage.
TEST_KEY="02e5bfa1f3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3"
rpc_call "makeAvailable" "[]";                       assert_refused "makeAvailable"
rpc_call "migrate" '["wallet-infra"]';               assert_refused "migrate"
rpc_call "findOrInsertUser" "[\"${TEST_KEY}\"]";     assert_refused "findOrInsertUser"
rpc_call "beginStorageTransaction" "[]";             assert_refused "beginStorageTransaction"
rpc_call "commitStorageTransaction" "[]";            assert_refused "commitStorageTransaction"
rpc_call "rollbackStorageTransaction" "[]";          assert_refused "rollbackStorageTransaction"

AUTH_METHODS=(
    "internalizeAction"
    "listOutputs"
    "listActions"
    "getBalance"
    "getAnalyticsSummary"
    "getBeefForTxid"
    "abortAction"
    "createAction"
    "processAction"
    "updateTransactionStatusAfterBroadcast"
    "relinquishOutput"
    "reserveOutputs"
    "unreserveOutputs"
    "reviewStatus"
)

for METHOD in "${AUTH_METHODS[@]}"; do
    rpc_call "$METHOD" "{}"
    assert_refused "$METHOD"
done

# ============================================================================
# Section 2: Unknown method without auth -- refused before dispatch
# ============================================================================
echo "--- Section 2: Unknown method without auth (expect 401 refusal) ---"

rpc_call "nonExistentMethod" "[]"
assert_refused "nonExistentMethod"

# ============================================================================
# Section 3: RPC layer -- needs a BRC-104 session (see header)
# ============================================================================
echo "--- Section 3: RPC layer (needs an authenticated session) ---"

skip "unknown method -> -32601: no bash BRC-104 session; covered by json_rpc::tests::method_not_found_error (the dispatch fallback arm, src/dispatch.rs, has no unit test)"
skip "response jsonrpc \"2.0\": no bash BRC-104 session; covered by json_rpc::tests::serialize_success_response, serialize_error_response"
skip "response id echoes request id: no bash BRC-104 session; covered by json_rpc::tests::serialize_success_response, serialize_success_response_with_string_id, method_not_found_error"

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=== JSON-RPC Smoke Test Summary: ${PASSED} passed, ${FAILED} failed, ${SKIPPED} skipped ==="
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
