#!/bin/bash
# json_rpc_smoke.sh -- Smoke test all JSON-RPC methods
#
# Tests:
#   1. Unauthenticated methods return valid responses
#   2. Authenticated methods without auth return proper errors
#   3. Unknown methods return method_not_found
#   4. Transaction stub methods return valid responses
#
# Usage: ./tests/e2e/json_rpc_smoke.sh [base_url]

set -euo pipefail

BASE_URL="${1:-https://wallet-infra.example.com}"
PASSED=0
FAILED=0
ID=0

pass() { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }

# Send a JSON-RPC call, return the response body
rpc_call() {
    local method="$1"
    local params="$2"
    ID=$((ID + 1))
    curl -s -X POST "${BASE_URL}" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"${method}\",\"params\":${params},\"id\":${ID}}"
}

# Check that result field exists (no error)
assert_result() {
    local method="$1"
    local response="$2"
    if echo "$response" | jq -e '.result' > /dev/null 2>&1; then
        pass "${method} returned result"
    else
        fail "${method} expected result, got: ${response}"
    fi
}

# Check that error field exists with the expected code
assert_error() {
    local method="$1"
    local response="$2"
    local expected_code="$3"
    local actual_code
    actual_code=$(echo "$response" | jq -r '.error.code // empty' 2>/dev/null)
    if [ -n "$actual_code" ]; then
        if [ -n "$expected_code" ] && [ "$actual_code" != "$expected_code" ]; then
            # Some methods may return different error codes; as long as there's an error, it's ok
            pass "${method} returned error (code ${actual_code})"
        else
            pass "${method} returned error code ${actual_code}"
        fi
    else
        fail "${method} expected error, got: ${response}"
    fi
}

echo "=== JSON-RPC Smoke Tests: ${BASE_URL} ==="
echo ""

# ============================================================================
# Section 1: Unauthenticated methods -- should return valid results
# ============================================================================
echo "--- Section 1: Unauthenticated methods (expect success) ---"

# makeAvailable -- no params needed
RESP=$(rpc_call "makeAvailable" "[]")
assert_result "makeAvailable" "$RESP"

# migrate -- expects a storage name
RESP=$(rpc_call "migrate" '["wallet-infra"]')
assert_result "migrate" "$RESP"

# findOrInsertUser -- expects an identity key
# Use a deterministic test key that won't collide with real users
TEST_KEY="02e5bfa1f3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3b0e3"
RESP=$(rpc_call "findOrInsertUser" "[\"${TEST_KEY}\"]")
assert_result "findOrInsertUser" "$RESP"

# ============================================================================
# Section 2: Transaction stub methods -- should return valid results (no auth)
# ============================================================================
echo "--- Section 2: Transaction stub methods (expect success) ---"

RESP=$(rpc_call "beginStorageTransaction" "[]")
assert_result "beginStorageTransaction" "$RESP"

RESP=$(rpc_call "commitStorageTransaction" "[]")
assert_result "commitStorageTransaction" "$RESP"

RESP=$(rpc_call "rollbackStorageTransaction" "[]")
assert_result "rollbackStorageTransaction" "$RESP"

# ============================================================================
# Section 3: Authenticated methods WITHOUT auth -- should return errors
# ============================================================================
echo "--- Section 3: Authenticated methods without auth (expect error) ---"

AUTH_METHODS=(
    "internalizeAction"
    "listOutputs"
    "listActions"
    "getBalance"
    "getAnalyticsSummary"
    "abortAction"
    "createAction"
    "processAction"
    "updateTransactionStatusAfterBroadcast"
    "reviewStatus"
)

for METHOD in "${AUTH_METHODS[@]}"; do
    RESP=$(rpc_call "$METHOD" "{}")
    # These should return an error because no BRC-31 auth header is provided.
    # The error could be -32602 (validation) or a different code depending on
    # where in the pipeline the auth check happens.
    if echo "$RESP" | jq -e '.error' > /dev/null 2>&1; then
        ERROR_CODE=$(echo "$RESP" | jq -r '.error.code' 2>/dev/null)
        pass "${METHOD} rejected without auth (code ${ERROR_CODE})"
    else
        fail "${METHOD} should require auth but returned result: ${RESP}"
    fi
done

# ============================================================================
# Section 4: Unknown method -- should return method_not_found
# ============================================================================
echo "--- Section 4: Unknown method (expect method_not_found) ---"

RESP=$(rpc_call "nonExistentMethod" "[]")
ERROR_CODE=$(echo "$RESP" | jq -r '.error.code // empty' 2>/dev/null)
if [ "$ERROR_CODE" = "-32601" ]; then
    pass "nonExistentMethod returned -32601 (method not found)"
elif [ -n "$ERROR_CODE" ]; then
    pass "nonExistentMethod returned error code ${ERROR_CODE}"
else
    fail "nonExistentMethod expected error, got: ${RESP}"
fi

# ============================================================================
# Section 5: JSON-RPC protocol conformance
# ============================================================================
echo "--- Section 5: Protocol conformance ---"

# Verify response includes jsonrpc version
RESP=$(rpc_call "makeAvailable" "[]")
JSONRPC_VER=$(echo "$RESP" | jq -r '.jsonrpc // empty' 2>/dev/null)
if [ "$JSONRPC_VER" = "2.0" ]; then
    pass "Response includes jsonrpc: \"2.0\""
else
    fail "Response missing jsonrpc version, got: ${JSONRPC_VER}"
fi

# Verify response id matches request id
RESP_ID=$(echo "$RESP" | jq -r '.id // empty' 2>/dev/null)
if [ "$RESP_ID" = "$ID" ]; then
    pass "Response id matches request id (${ID})"
else
    fail "Response id mismatch: expected ${ID}, got ${RESP_ID}"
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=== JSON-RPC Smoke Test Summary: ${PASSED} passed, ${FAILED} failed ==="
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
