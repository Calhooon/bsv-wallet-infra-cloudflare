# BEEF & UTXO Debugging Guide

Hard-won knowledge from a multi-day investigation (April 2026) that uncovered three interrelated bugs in the x402 payment/refund flow. This document is context for future debugging sessions.

## Architecture Overview

```
Client Wallet (MetaNet Client, port 3321)
    ↕ BRC-103/104 auth + BRC-29 payment
Agent Worker (e.g. openai-agent on Cloudflare)
    ↕ uses bsv-auth-cloudflare middleware for auth/payment/refund
    ↕ calls wallet-infra via STORAGE_URL for wallet operations
Wallet Infra Worker (wallet-infra.x402agency.com on Cloudflare)
    ↕ D1 (SQLite), R2 (blobs), KV (sessions)
    ↕ ARC + WhatsOnChain (broadcast, proofs, raw tx fetch)
```

All 11 agents share one wallet-infra instance. Fixing wallet-infra fixes all agents. Agent Workers do NOT need redeployment for wallet-infra changes.

## The Three Tables That Matter

```sql
-- Confirmed transactions with merkle proofs (TERMINAL — stops BEEF recursion)
proven_txs (txid, raw_tx BLOB, merkle_path BLOB, height, block_hash, merkle_root)

-- All transactions (incoming + outgoing). input_beef is NULLed after processAction.
transactions (txid, raw_tx BLOB, input_beef BLOB, status, is_outgoing, ...)

-- Broadcast queue. Preserves raw_tx + input_beef after transactions are NULLed.
proven_tx_reqs (txid, raw_tx BLOB, input_beef BLOB, status, ...)
```

**Data lifecycle:**
1. `createAction` → builds input_beef → stores in `transactions.input_beef`
2. `processAction` → NULLs `transactions.raw_tx` and `transactions.input_beef` → creates `proven_tx_reqs` with both
3. Monitor (every 5 min) → fetches merkle proofs → inserts into `proven_txs` → updates `proven_tx_reqs.status = 'completed'`

## UTXO Allocation

### How It Works
`allocate_change_input()` in `create_action.rs` selects the best-fit UTXO for spending.

### The Bug We Fixed
**Before:** Two-step SELECT→UPDATE. D1 has no transaction isolation, so concurrent requests could SELECT the same UTXO before either UPDATE committed. Result: 281 refund txs all spending the same UTXO.

**After:** Single atomic statement:
```sql
UPDATE outputs SET spendable = 0, spent_by = ?
WHERE output_id = (
    SELECT o.output_id FROM outputs o
    JOIN transactions t ON o.transaction_id = t.transaction_id
    WHERE o.user_id = ? AND o.basket_id = ?
      AND o.spent_by IS NULL AND o.spendable = 1
      AND t.status IN ('completed', 'unproven', 'nosend', 'sending')
    -- Note: matches TS reference (wallet-toolbox/StorageKnex.ts:1240-1248) —
    -- basket membership is the ownership gate; we do not filter on `change = 1`.
    ORDER BY CASE WHEN o.satoshis >= ? THEN 0 ELSE 1 END,
             ABS(o.satoshis - ?) ASC
    LIMIT 1
) AND spent_by IS NULL
RETURNING output_id, satoshis, txid, vout, ...
```

SQLite serializes writes within a single statement. No race window.

### Debugging UTXO Issues
```sql
-- Find frozen UTXOs (locked by failed transactions)
SELECT COUNT(*), SUM(satoshis) FROM outputs
WHERE spent_by IS NOT NULL AND spendable = 0
AND spent_by IN (SELECT transaction_id FROM transactions WHERE status = 'failed');

-- Release them
UPDATE outputs SET spendable = 1, spent_by = NULL, updated_at = datetime('now')
WHERE spent_by IS NOT NULL AND spendable = 0
AND spent_by IN (SELECT transaction_id FROM transactions WHERE status = 'failed');

-- Check wallet balance
SELECT SUM(satoshis) as balance, COUNT(*) as utxos
FROM outputs WHERE spendable = 1 AND spent_by IS NULL;

-- Check UTXO allocation status breakdown
SELECT t.status, COUNT(*) as cnt, SUM(o.satoshis) as total
FROM outputs o JOIN transactions t ON o.spent_by = t.transaction_id
WHERE o.spent_by IS NOT NULL AND o.spendable = 0
GROUP BY t.status;
```

## BEEF Building

### How It Works
`build_input_beef()` in `create_action.rs` recursively gathers ancestor proofs for all input UTXOs.

**BFS traversal with 4-tier lookup per txid:**
1. `proven_txs` — mined with merkle proof (terminal, stops recursion)
2. `transactions` — stored input_beef or raw_tx
3. `proven_tx_reqs` — broadcast queue with raw_tx + input_beef
4. **Network fallback** — WoC `GET /tx/{txid}/hex` for raw_tx + ARC/WoC for proof

### Reference Implementation Behavior (TS/Go/Rust toolboxes)
- BFS processes each txid individually
- Always adds raw_tx + proof together for each txid
- Network fallback when local DB doesn't have the data
- Caches fetched data in proven_tx_reqs
- `verify_valid(false)` on the output before returning
- `verify_valid(false)` on incoming BEEFs in internalizeAction
- **No `trim_known_proven` call** — this is bsv-rs only and creates orphans

### What Can Go Wrong

#### Orphaned Bump TXIDs
When `merge_beef` combines bumps from the same block (via `merge_bump` → `combine`), the combined bump references txids from BOTH sources. If one source didn't have raw_tx for some txids, those txids end up in bumps without BeefTx entries.

**Detection (TS SDK, used by MetaNet Client):**
```
sortTxs() → checks each tx's inputTxids against txidToTx map
→ orphaned bump txids show up as "missingInputs"
→ verifyValid returns { valid: false }
→ "The tx parameter must be valid AtomicBEEF"
```

**Detection (from Node.js for debugging):**
```javascript
const { Beef } = require('/path/to/ts-sdk/dist/cjs/mod.js');
const beef = Beef.fromBinary(beefBytes);

// Find orphaned bump txids
const bumpTxids = new Set();
for (const b of beef.bumps)
    if (b.path?.[0])
        for (const l of b.path[0])
            if (l.txid && l.hash) bumpTxids.add(l.hash);

const beefTxids = new Set();
for (const [, tx] of Object.entries(beef.txs))
    if (tx.tx) beefTxids.add(tx.tx.id('hex'));

const orphaned = [...bumpTxids].filter(t => !beefTxids.has(t));
console.log('orphaned:', orphaned.length);

// Full structural check
const r = beef.verifyValid(false);
console.log('valid:', r.valid);
const sr = beef.sortTxs();
console.log('missingInputs:', sr.missingInputs);
```

#### `trim_known_proven` (DO NOT USE in BEEF builder)
This is a bsv-rs-only function that removes proven ancestors not referenced as inputs by other txs in the BEEF. The TS and Go SDKs do not have it. It creates orphaned bump txids because:
1. Final pass adds missing txids referenced by bumps
2. `trim_known_proven` removes them (they're proven, no other tx references them)
3. But bumps still reference them → orphans

**Rule: never call `trim_known_proven()` in `build_input_beef`.**

#### Stored BEEF Corruption
Stored BEEFs in `proven_tx_reqs.input_beef` can be incomplete if they were built by a buggy builder or received from clients with bugs. When merged via `merge_beef`, incomplete stored BEEFs propagate their orphans.

**Detection:**
```sql
-- Count stored BEEFs by source
SELECT t.is_outgoing, COUNT(*), SUM(CASE WHEN p.input_beef IS NOT NULL THEN 1 ELSE 0 END)
FROM proven_tx_reqs p LEFT JOIN transactions t ON p.txid = t.txid
GROUP BY t.is_outgoing;
```

**Repair script:** `repair_beefs_v2.mjs` in the wallet-infra repo. Scans all stored BEEFs, finds orphaned bump txids, fetches raw_txs from proven_txs/WoC, and writes fixed BEEFs back to D1.

## BEEF Verification

### Current Behavior (matches reference toolboxes)
- `verify_beef()` in `beef_verification.rs` ALWAYS runs structural validation
- No skip mode — the `_mode` parameter is ignored
- `verify_valid(false)` catches: missing inputs, broken dependency chains, bad bump references
- Root verification (merkle roots vs block headers) runs when header provider is available
- Output validation: `build_input_beef` calls `verify_valid(false)` before returning

### What `verify_valid(false)` Checks
1. `sort_txs()` → categorizes txs as valid/invalid/missing
2. Rejects if `missing_inputs` is non-empty (input txids not in BEEF)
3. Rejects if `not_valid` is non-empty (unresolvable txs)
4. Rejects if `txid_only` is non-empty (when `allow_txid_only=false`)
5. Validates bump consistency (merkle roots)
6. Validates bump references (tx bump_index points to valid bump)
7. Validates dependency order (all inputs come before dependents)

### What `verify_valid` Does NOT Check
- Does NOT check that every txid in bump leaves has a BeefTx entry
- Orphaned bump txids are caught INDIRECTLY via `missing_inputs` only when another tx references them as inputs
- If no tx in the BEEF references the orphan as an input, it passes undetected

## BEEF Binary Format (V2)

All three SDKs (TS, Go, Rust) use identical format:

```
[4 bytes LE] Version: 0xEFBE0002 (BEEF V2) or 0x01010101 (AtomicBEEF wrapper)
[varint]     Bump count
[bumps...]   Each: blockHeight(varint) + treeHeight(1) + levels...
[varint]     TX count
[txs...]     Each: dataFormat(1) + data...

DataFormat values:
  0x00 = RawTx (no proof):           [raw tx bytes]
  0x01 = RawTxAndBumpIndex (proven):  [varint bump_index] [raw tx bytes]
  0x02 = TxidOnly:                    [32 bytes reversed txid]

AtomicBEEF wrapper:
  [4 bytes] 0x01010101
  [32 bytes] target txid (reversed)
  [rest]    standard BEEF V2
```

## Service Layer

### ProofService Trait
```rust
pub trait ProofService {
    fn get_proof(&self, txid: &str) -> Result<Option<ProofResult>>;
    fn get_raw_tx(&self, txid: &str) -> Result<Option<Vec<u8>>>;
}
```

### Providers
- **ARC**: `get_proof` via `GET /v1/tx/{txid}` (BRC-74 BUMP). No `get_raw_tx`.
- **WoC**: `get_proof` via `GET /tx/{txid}/proof/tsc` + header lookup. `get_raw_tx` via `GET /tx/{txid}/hex`.
- **MultiProvider**: ARC first for proofs (fallback WoC), WoC for raw_tx. Rate limit: 3 req/sec on WoC.

## D1 Constraints

- **No BEGIN/COMMIT** — use atomic single statements or batch operations
- **Batch limit**: 100 statements per batch (all-or-nothing)
- **BLOB size limit**: ~1MB per row (SQLITE_TOOBIG for larger)
- **SQL statement limit**: ~1MB (can't inline large hex in UPDATE...SET col = X'...')
- **Write serialization**: SQLite serializes writes — use this for atomic UPDATE...RETURNING

## Quick Diagnostic Commands

```bash
# D1 query via wrangler (need CLOUDFLARE_API_TOKEN)
CLOUDFLARE_API_TOKEN=<token> npx wrangler d1 execute wallet-infra --remote --command "<SQL>"

# Test BEEF validity with TS SDK
node -e "const{Beef}=require('/path/to/ts-sdk/dist/cjs/mod.js');const b=Beef.fromBinary(Array.from(require('fs').readFileSync('/tmp/beef.bin')));console.log(b.verifyValid(false))"

# Test D1 RETURNING support
UPDATE outputs SET updated_at = updated_at WHERE output_id = -999 RETURNING output_id, satoshis;

# Tail worker logs
CLOUDFLARE_API_TOKEN=<token> npx wrangler tail wallet-infra --format pretty

# Deploy wallet-infra
cd rust-wallet-infra && worker-build --release && CLOUDFLARE_API_TOKEN=<token> npx wrangler deploy

# Make test x402 payment with refund
cd ~/.claude/plugins/marketplaces/calgooon-x402/skills/x402
python3 ./scripts/brc31_helpers.py pay POST "https://openai-chat.x402agency.com/chat" \
  '{"model":"gpt-5-nano","messages":[{"role":"system","content":"You only say OK"},{"role":"user","content":"."}],"max_tokens":200}'
```

## Key Files

| File | Purpose |
|------|---------|
| `wallet-infra/src/storage/create_action.rs` | UTXO allocation + BEEF builder |
| `wallet-infra/src/storage/beef_verification.rs` | Incoming BEEF validation |
| `wallet-infra/src/services/woc.rs` | WoC provider (get_raw_tx, get_proof, broadcast) |
| `wallet-infra/src/services/multi.rs` | ARC+WoC failover provider |
| `wallet-infra/src/storage/process_action.rs` | Broadcast + data lifecycle |
| `wallet-infra/src/storage/internalize_action.rs` | Accept incoming payments |
| `wallet-infra/src/monitor.rs` | Proof fetching, abandoned tx cleanup |
| `bsv-auth-cloudflare/src/refund/mod.rs` | Refund construction (issue_refund) |
| `bsv-auth-cloudflare/src/refund/signer.rs` | Transaction signing |
| `bsv-rs/src/transaction/beef.rs` | BEEF struct, merge_beef, verify_valid, merge_bump |
| `bsv-rs/src/transaction/beef_tx.rs` | BeefTx struct, serialization format |
| `bsv-rs/src/transaction/merkle_path.rs` | MerklePath (BUMP), txids(), combine() |

## Lessons Learned

1. **D1 has no transaction isolation.** Any multi-step read-then-write MUST be a single atomic statement or a D1 batch. The SELECT→UPDATE pattern is ALWAYS a race condition on D1.

2. **`trim_known_proven` is bsv-rs only.** TS and Go SDKs don't have it. It removes txids that bumps reference. Never use it in the BEEF builder.

3. **`merge_bump` combines same-block bumps.** This can create txid references in bumps for txids that have no BeefTx entry. The final pass in build_input_beef handles this by detecting and resolving orphaned bump txids.

4. **`verify_valid` doesn't catch all orphans.** It only catches them indirectly when another tx references the orphan as an input. Direct orphaned bump txids (not referenced by any tx) pass undetected.

5. **Stored BEEFs can be corrupted from clients too.** The TS wallet-toolbox (MetaNet Client) also produces incomplete BEEFs. Always validate incoming BEEFs with `verify_valid(false)`.

6. **The refund tx can have many inputs.** Don't assume refund txs are simple 1-input/1-output. The wallet may consolidate UTXOs, creating multi-input refund txs whose ancestors span multiple blocks.

7. **WoC rate limit is 3 req/sec.** Network fallback fetches should respect this. Results are cached in proven_tx_reqs, so the cost is one-time per txid.

8. **All 11 agents share one wallet-infra.** Fixing wallet-infra fixes everything. Agent redeployment is only needed for middleware (bsv-auth-cloudflare) changes.
