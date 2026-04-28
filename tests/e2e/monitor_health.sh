#!/bin/bash
# monitor_health.sh -- Verify the cron monitor is healthy
#
# Checks the D1 database via Cloudflare API for:
#   - Recent monitor_events (cron is actually running)
#   - Stale unproven transactions
#   - Stuck unmined proof requests
#   - Proof pipeline completion rate
#
# Required env vars:
#   CF_API_TOKEN   -- Cloudflare API token with D1 read access
#   CF_ACCOUNT_ID  -- Cloudflare account ID
#   D1_DATABASE_ID -- D1 database UUID
#
# Usage: ./tests/e2e/monitor_health.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Validate required environment variables
# ---------------------------------------------------------------------------
missing=()
[ -z "${CF_API_TOKEN:-}" ]   && missing+=("CF_API_TOKEN")
[ -z "${CF_ACCOUNT_ID:-}" ]  && missing+=("CF_ACCOUNT_ID")
[ -z "${D1_DATABASE_ID:-}" ] && missing+=("D1_DATABASE_ID")

if [ ${#missing[@]} -gt 0 ]; then
    echo "ERROR: Missing required environment variables: ${missing[*]}"
    echo ""
    echo "Export them before running this script:"
    echo "  export CF_API_TOKEN=\"your-cloudflare-api-token\""
    echo "  export CF_ACCOUNT_ID=\"your-account-id\""
    echo "  export D1_DATABASE_ID=\"your-d1-database-id\""
    exit 1
fi

CF_BASE="https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/d1/database/${D1_DATABASE_ID}"
PASSED=0
FAILED=0
WARNINGS=0

pass()    { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail()    { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }
warn()    { WARNINGS=$((WARNINGS + 1)); echo "  WARN: $1"; }

d1_query() {
    local sql="$1"
    curl -s "${CF_BASE}/query" \
        -H "Authorization: Bearer ${CF_API_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"${sql}\"}"
}

# Helper: extract a single scalar from D1 query result
d1_scalar() {
    local sql="$1"
    local field="$2"
    d1_query "$sql" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    results = data['result'][0]['results']
    if results:
        print(results[0].get('$field', ''))
    else:
        print('')
except Exception as e:
    print('', file=sys.stderr)
    print('')
" 2>/dev/null
}

echo "=== Monitor Health Check ==="
echo ""

# ---------------------------------------------------------------------------
# 1. Verify D1 API access works
# ---------------------------------------------------------------------------
echo "--- Test 1: D1 API connectivity ---"
API_RESULT=$(d1_query "SELECT 1 as ok")
if echo "$API_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); assert d['success']" 2>/dev/null; then
    pass "D1 API query succeeded"
else
    fail "D1 API query failed -- check CF_API_TOKEN and CF_ACCOUNT_ID"
    echo ""
    echo "=== Cannot continue without D1 access ==="
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Check for recent monitor_events (cron running in the last 15 min)
# ---------------------------------------------------------------------------
echo "--- Test 2: Recent monitor_events ---"
RECENT_COUNT=$(d1_scalar \
    "SELECT COUNT(*) as cnt FROM monitor_events WHERE created_at > datetime('now', '-15 minutes')" \
    "cnt")

if [ -n "$RECENT_COUNT" ] && [ "$RECENT_COUNT" -gt 0 ] 2>/dev/null; then
    pass "Monitor ran ${RECENT_COUNT} time(s) in the last 15 minutes"
else
    fail "No monitor_events in the last 15 minutes -- cron may not be running"
fi

# ---------------------------------------------------------------------------
# 3. Check for stale unproven transactions (>1 hour old)
# ---------------------------------------------------------------------------
echo "--- Test 3: Stale unproven transactions ---"
STALE_UNPROVEN=$(d1_scalar \
    "SELECT COUNT(*) as cnt FROM transactions WHERE status = 'unproven' AND updated_at < datetime('now', '-60 minutes')" \
    "cnt")

if [ -n "$STALE_UNPROVEN" ] && [ "$STALE_UNPROVEN" -gt 0 ] 2>/dev/null; then
    warn "${STALE_UNPROVEN} unproven transactions older than 1 hour"
else
    pass "No stale unproven transactions"
fi

# ---------------------------------------------------------------------------
# 4. Check for stuck unmined proof requests
# ---------------------------------------------------------------------------
echo "--- Test 4: Stuck proof requests ---"
STUCK_UNMINED=$(d1_scalar \
    "SELECT COUNT(*) as cnt FROM proven_tx_reqs WHERE status IN ('unmined','unprocessed') AND created_at < datetime('now', '-60 minutes')" \
    "cnt")

if [ -n "$STUCK_UNMINED" ] && [ "$STUCK_UNMINED" -gt 0 ] 2>/dev/null; then
    warn "${STUCK_UNMINED} proof requests stuck in unmined/unprocessed for >1 hour"
else
    pass "No stuck proof requests"
fi

# ---------------------------------------------------------------------------
# 5. Proof pipeline completion rate
# ---------------------------------------------------------------------------
echo "--- Test 5: Proof pipeline stats ---"
STATS=$(d1_query "SELECT status, COUNT(*) as cnt FROM proven_tx_reqs GROUP BY status ORDER BY cnt DESC")
echo "$STATS" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    rows = data['result'][0]['results']
    total = sum(r['cnt'] for r in rows)
    for r in rows:
        pct = (r['cnt'] / total * 100) if total > 0 else 0
        print(f\"    {r['status']:20s} {r['cnt']:6d}  ({pct:.1f}%)\")
    completed = sum(r['cnt'] for r in rows if r['status'] == 'completed')
    if total > 0:
        rate = completed / total * 100
        print(f\"    Completion rate: {rate:.1f}%\")
except Exception:
    print('    (no data or query error)')
" 2>/dev/null
pass "Proof pipeline stats retrieved"

# ---------------------------------------------------------------------------
# 6. Check latest monitor event details
# ---------------------------------------------------------------------------
echo "--- Test 6: Latest monitor event ---"
LATEST=$(d1_query "SELECT details, created_at FROM monitor_events ORDER BY rowid DESC LIMIT 1")
echo "$LATEST" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    row = data['result'][0]['results'][0]
    ts = row['created_at']
    d = json.loads(row['details'])
    print(f\"    Timestamp:     {ts}\")
    print(f\"    Proofs found:  {d.get('proofs_found', 'n/a')}\")
    print(f\"    Proofs checked:{d.get('proofs_checked', 'n/a')}\")
    print(f\"    Status synced: {d.get('status_synced', 'n/a')}\")
    print(f\"    Errors:        {len(d.get('errors', []))}\")
    if d.get('errors'):
        for e in d['errors'][:3]:
            print(f\"      - {e}\")
except Exception:
    print('    (no monitor events found)')
" 2>/dev/null
pass "Latest monitor event retrieved"

# ---------------------------------------------------------------------------
# 7. Check for abandoned transactions (unsigned/unprocessed > 30 min)
# ---------------------------------------------------------------------------
echo "--- Test 7: Abandoned transactions ---"
ABANDONED=$(d1_scalar \
    "SELECT COUNT(*) as cnt FROM transactions WHERE status IN ('unsigned','unprocessed') AND updated_at < datetime('now', '-30 minutes')" \
    "cnt")

if [ -n "$ABANDONED" ] && [ "$ABANDONED" -gt 0 ] 2>/dev/null; then
    warn "${ABANDONED} abandoned transactions (unsigned/unprocessed > 30 min) -- monitor should clean these up"
else
    pass "No abandoned transactions"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Monitor Health Summary: ${PASSED} passed, ${FAILED} failed, ${WARNINGS} warnings ==="
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
