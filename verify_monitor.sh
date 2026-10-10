#!/bin/bash
# verify_monitor.sh — Pre/post-deploy verification for monitor proof fixes
#
# Usage:
#   ./verify_monitor.sh pre    # Run before deploy to confirm the bug
#   ./verify_monitor.sh post   # Run after deploy to confirm the fix works
#   ./verify_monitor.sh watch  # Poll monitor_events until proofs_found > 0

set -euo pipefail

CF_ACCOUNT="<your-account-id>"
CF_DB="<your-d1-database-id>"
CF_TOKEN="${CLOUDFLARE_API_TOKEN:?set CLOUDFLARE_API_TOKEN in the environment}"
WOC_BASE="https://api.whatsonchain.com/v1/bsv/main"

d1_query() {
    local sql="$1"
    curl -s "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT}/d1/database/${CF_DB}/query" \
        -H "Authorization: Bearer ${CF_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"sql\": \"${sql}\"}" | python3 -m json.tool 2>/dev/null || echo "QUERY FAILED"
}

# ─────────────────────────────────────────────────────────────
# PRE-DEPLOY: Confirm the bug exists
# ─────────────────────────────────────────────────────────────
pre_deploy() {
    echo "=== PRE-DEPLOY VERIFICATION ==="
    echo ""

    # 1. Confirm WoC returns "null" for unconfirmed txids
    echo "--- Test 1: WoC TSC proof API returns 'null' for unconfirmed txids ---"
    UNMINED_TXID=$(curl -s "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT}/d1/database/${CF_DB}/query" \
        -H "Authorization: Bearer ${CF_TOKEN}" \
        -H "Content-Type: application/json" \
        -d '{"sql": "SELECT txid FROM proven_tx_reqs WHERE status = '\''unmined'\'' ORDER BY created_at DESC LIMIT 1"}' \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['result'][0]['results'][0]['txid'])" 2>/dev/null)

    if [ -z "$UNMINED_TXID" ]; then
        echo "  SKIP: No unmined txids found"
    else
        echo "  Txid: $UNMINED_TXID"
        PROOF_BODY=$(curl -s "${WOC_BASE}/tx/${UNMINED_TXID}/proof/tsc")
        echo "  WoC proof response: '$PROOF_BODY'"
        if [ "$PROOF_BODY" = "null" ]; then
            echo "  ✓ CONFIRMED: WoC returns 'null' (this is what breaks the parser)"
        elif [ "$PROOF_BODY" = "[]" ] || [ -z "$PROOF_BODY" ]; then
            echo "  INFO: WoC returns empty/[] — parser handles this correctly"
        else
            echo "  INFO: WoC returns actual proof data — this tx is mined"
        fi
    fi
    echo ""

    # 2. Show current stuck state
    echo "--- Test 2: Current stuck state ---"
    echo "  Unmined proven_tx_reqs:"
    d1_query "SELECT status, COUNT(*) as cnt, MIN(attempts) as min_att, MAX(attempts) as max_att FROM proven_tx_reqs WHERE status IN ('unmined','unprocessed') GROUP BY status"
    echo ""

    echo "  Unproven transactions (revenue invisible to dashboard):"
    d1_query "SELECT status, COUNT(*) as cnt, SUM(satoshis) as total_sats FROM transactions WHERE satoshis > 0 AND status = 'unproven' GROUP BY status"
    echo ""

    # 3. Show recent monitor runs — all proofs_found: 0
    echo "--- Test 3: Recent monitor runs (expect proofs_found: 0) ---"
    d1_query "SELECT details, created_at FROM monitor_events ORDER BY rowid DESC LIMIT 3"
    echo ""

    echo "=== PRE-DEPLOY DONE ==="
}

# ─────────────────────────────────────────────────────────────
# POST-DEPLOY: Verify the fix is working
# ─────────────────────────────────────────────────────────────
post_deploy() {
    echo "=== POST-DEPLOY VERIFICATION ==="
    echo ""

    # 1. Check most recent monitor events for proofs_found > 0
    echo "--- Test 1: Recent monitor runs (looking for proofs_found > 0) ---"
    d1_query "SELECT details, created_at FROM monitor_events ORDER BY rowid DESC LIMIT 5"
    echo ""

    # 2. Check proven_tx_reqs progress
    echo "--- Test 2: Proof request status summary ---"
    d1_query "SELECT status, COUNT(*) as cnt FROM proven_tx_reqs GROUP BY status ORDER BY cnt DESC"
    echo ""

    # 3. Check if any transactions transitioned to completed recently
    echo "--- Test 3: Recently completed transactions ---"
    d1_query "SELECT COUNT(*) as cnt FROM transactions WHERE status = 'completed' AND updated_at > datetime('now', '-30 minutes')"
    echo ""

    # 4. Check remaining unproven
    echo "--- Test 4: Remaining unproven transactions ---"
    d1_query "SELECT status, COUNT(*) as cnt, SUM(satoshis) as total_sats FROM transactions WHERE satoshis > 0 AND status = 'unproven' GROUP BY status"
    echo ""

    echo "=== POST-DEPLOY DONE ==="
}

# ─────────────────────────────────────────────────────────────
# WATCH: Poll until proofs start being found
# ─────────────────────────────────────────────────────────────
watch_monitor() {
    echo "=== WATCHING MONITOR (Ctrl+C to stop) ==="
    echo "  Polling every 60s for monitor_events with proofs_found > 0..."
    echo ""

    while true; do
        LATEST=$(curl -s "https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT}/d1/database/${CF_DB}/query" \
            -H "Authorization: Bearer ${CF_TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{"sql": "SELECT details, created_at FROM monitor_events ORDER BY rowid DESC LIMIT 1"}' \
            | python3 -c "
import sys, json
r = json.load(sys.stdin)['result'][0]['results'][0]
d = json.loads(r['details'])
ts = r['created_at']
print(f\"{ts} | proofs_found={d['proofs_found']} checked={d['proofs_checked']} synced={d['status_synced']} errors={len(d['errors'])}\")
if d['proofs_found'] > 0:
    print('  ✓ PROOFS BEING FOUND — fix is working!')
    sys.exit(42)
" 2>/dev/null)

        EXIT_CODE=$?
        echo "  $LATEST"

        if [ "$EXIT_CODE" -eq 42 ]; then
            echo ""
            echo "=== SUCCESS: Monitor is finding proofs again ==="
            post_deploy
            exit 0
        fi

        sleep 60
    done
}

# ─────────────────────────────────────────────────────────────
case "${1:-help}" in
    pre)  pre_deploy ;;
    post) post_deploy ;;
    watch) watch_monitor ;;
    *)
        echo "Usage: $0 {pre|post|watch}"
        echo "  pre   — Confirm the bug before deploying"
        echo "  post  — Check if the fix is working after deploy"
        echo "  watch — Poll until proofs_found > 0"
        ;;
esac
