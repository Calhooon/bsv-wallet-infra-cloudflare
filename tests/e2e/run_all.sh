#!/bin/bash
# run_all.sh -- Run all E2E tests for wallet-infra
#
# Usage:
#   ./tests/e2e/run_all.sh [base_url]
#
# Environment variables (required for monitor_health.sh):
#   CF_API_TOKEN   -- Cloudflare API token
#   CF_ACCOUNT_ID  -- Cloudflare account ID
#   D1_DATABASE_ID -- D1 database UUID
#
# Without the CF_* variables, monitor_health.sh is skipped.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_URL="${1:-https://<your-worker-domain>}"

TOTAL_PASSED=0
TOTAL_FAILED=0
TOTAL_SKIPPED=0
SUITES_RUN=()
SUITES_FAILED=()
SUITES_SKIPPED=()

run_suite() {
    local name="$1"
    local script="$2"
    shift 2

    echo ""
    echo "================================================================"
    echo "  Suite: ${name}"
    echo "================================================================"
    echo ""

    if [ ! -x "$script" ]; then
        echo "  ERROR: ${script} is not executable"
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        SUITES_FAILED+=("$name (not executable)")
        return
    fi

    if "$script" "$@"; then
        TOTAL_PASSED=$((TOTAL_PASSED + 1))
        SUITES_RUN+=("$name")
    else
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
        SUITES_FAILED+=("$name")
    fi
}

skip_suite() {
    local name="$1"
    local reason="$2"
    echo ""
    echo "================================================================"
    echo "  Suite: ${name} -- SKIPPED (${reason})"
    echo "================================================================"
    TOTAL_SKIPPED=$((TOTAL_SKIPPED + 1))
    SUITES_SKIPPED+=("$name")
}

echo "================================================================"
echo "  wallet-infra E2E Test Runner"
echo "  Target: ${BASE_URL}"
echo "  Time:   $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "================================================================"

# ---------------------------------------------------------------------------
# Suite 1: Health Check
# ---------------------------------------------------------------------------
run_suite "Health Check" "${SCRIPT_DIR}/health_check.sh" "${BASE_URL}"

# ---------------------------------------------------------------------------
# Suite 2: JSON-RPC Smoke Tests
# ---------------------------------------------------------------------------
run_suite "JSON-RPC Smoke" "${SCRIPT_DIR}/json_rpc_smoke.sh" "${BASE_URL}"

# ---------------------------------------------------------------------------
# Suite 3: Monitor Health (requires CF_* env vars)
# ---------------------------------------------------------------------------
if [ -n "${CF_API_TOKEN:-}" ] && [ -n "${CF_ACCOUNT_ID:-}" ] && [ -n "${D1_DATABASE_ID:-}" ]; then
    run_suite "Monitor Health" "${SCRIPT_DIR}/monitor_health.sh"
else
    skip_suite "Monitor Health" "CF_API_TOKEN, CF_ACCOUNT_ID, or D1_DATABASE_ID not set"
fi

# ---------------------------------------------------------------------------
# Final Summary
# ---------------------------------------------------------------------------
echo ""
echo "================================================================"
echo "  E2E Test Summary"
echo "================================================================"
echo ""
echo "  Suites passed:  ${TOTAL_PASSED}"
echo "  Suites failed:  ${TOTAL_FAILED}"
echo "  Suites skipped: ${TOTAL_SKIPPED}"
echo ""

if [ ${#SUITES_RUN[@]} -gt 0 ]; then
    echo "  Passed:"
    for s in "${SUITES_RUN[@]}"; do
        echo "    - ${s}"
    done
fi

if [ ${#SUITES_FAILED[@]} -gt 0 ]; then
    echo "  Failed:"
    for s in "${SUITES_FAILED[@]}"; do
        echo "    - ${s}"
    done
fi

if [ ${#SUITES_SKIPPED[@]} -gt 0 ]; then
    echo "  Skipped:"
    for s in "${SUITES_SKIPPED[@]}"; do
        echo "    - ${s}"
    done
fi

echo ""
if [ "$TOTAL_FAILED" -gt 0 ]; then
    echo "  RESULT: FAILED"
    exit 1
else
    echo "  RESULT: PASSED"
    exit 0
fi
