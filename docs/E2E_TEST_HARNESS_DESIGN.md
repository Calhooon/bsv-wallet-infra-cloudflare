# E2E Test Harness Design — rust-wallet-infra

## Problem

We need to verify that wallet-infra correctly handles real-money payment flows across all 14 production agents. The harness must:
1. **Not lose sats** — every satoshi must be accounted for (fees + provider costs only)
2. **Test all refund paths** — inline, excess, deferred, and double-refund prevention
3. **Sweep after testing** — recover all remaining sats to a known address
4. **Balance reconciliation** — `balance_before - balance_after == fees + provider_costs`

## Existing Infrastructure We Can Use

| Tool | Location | What it does |
|------|----------|-------------|
| `manage` CLI | `~/bsv/agents/manage` | sweep, send, abort, balance, history for any agent wallet |
| `brc31_helpers.py` | x402 skill | BRC-31 auth + BRC-29 paid requests, auto-internalizes refunds |
| `test-agent` | `~/bsv/agents/test-agent` | 100-sat flat endpoint at `test.x402agency.com/donate` |
| `agents.toml` | `~/bsv/agents/manage/agents.toml` | All agent keys, identity keys, storage URLs |
| `rust-bsv-worm` | `~/bsv/rust-bsv-worm` | Client-side x402 payment SDK (WalletApi, AuthriteClient, refund processing) |
| `bsv-auth-cloudflare` | `~/bsv/rust-middleware` | Server-side payment middleware + refund issuance (`issue_refund()`) |
| `/monitor/audit` | wallet-infra | Both-level integrity check (0 issues = healthy) |

## Architecture

```
Test Harness (manage test / script)
  ├── brc31_helpers.py pay → agent endpoints (BRC-31 auth + BRC-29 payment)
  │     ├── Agent receives payment via bsv-auth-cloudflare middleware
  │     │     └── middleware calls wallet-infra internalizeAction
  │     ├── Agent processes request (calls upstream API)
  │     ├── On success: returns result (+ possible excess refund)
  │     └── On failure: calls issue_refund() → returns BEEF in response
  │           └── brc31_helpers.py auto-internalizes refund via MetaNet Client
  ├── manage sweep → wallet-infra createAction + processAction
  └── /monitor/audit → wallet-infra integrity verification
```

## Refund Paths to Test

### Path 1: Inline Service Failure Refund
All 12 custom agents support this. Upstream API (Anthropic, OpenAI, fal.ai, etc.) fails → agent calls `issue_refund()` → returns 500 with `refund` block.

**Shared code:** `~/bsv/rust-middleware/bsv-auth-cloudflare/src/refund/mod.rs`

**Response:**
```json
{
  "status": "error",
  "code": "ERR_SERVICE_FAILED_REFUND_ISSUED",
  "refund": {
    "transaction": "base64 AtomicBEEF",
    "derivationPrefix": "...",
    "derivationSuffix": "...",
    "senderIdentityKey": "03...",
    "satoshis": 5000,
    "txid": "abc123..."
  }
}
```

**Client side:** `brc31_helpers.py` detects `refund` or `excessRefund` in response body → calls MetaNet Client `POST /internalizeAction` with paymentRemittance.

### Path 2: Excess Refund (claude, openai only)
Agent quotes a price, actual token usage costs less. Agent refunds the excess.

**Trigger:** Send a short prompt like "Say hi" — actual token cost will be much less than quoted price.

**Response:** 200 OK with `excessRefund` block (same structure as above).

### Path 3: Deferred Refund (banana, banana2, veo, kling)
Async agents return `request_id` immediately. Job fails later. Refund appears when polling `/status/{request_id}`.

**Double-refund prevention:** KV stores `refunded: true` flag per prediction. Subsequent polls return `{already_refunded: true, txid: "..."}`.

### Path 4: No Refund on Client Error
claude/openai return 4xx for content policy violations — no refund issued. This is correct behavior. Test that balance decreases by the full amount.

### Path 5: No Refund by Design (polymirror)
Polymarket API is free upstream, so API failures don't trigger refunds. Only internal panics (code crash after payment) would.

## Deterministic Refund Test Inputs

| Agent | Input that triggers refund | Refund type | Cost |
|-------|---------------------------|------------|------|
| **whisper** | Empty/corrupt base64 audio → Workers AI fails | Inline | ~1.2k sats |
| **reader** | URL to non-existent domain → Jina timeout | Inline | ~6k sats |
| **claude** | "Say ok" → 2 tokens, massive excess | Excess | ~53k sats (most refunded) |
| **openai** | "Say ok" → 2 tokens, massive excess | Excess | ~4.1k sats (most refunded) |
| **seo** | Valid keyword → DataForSEO fails (if upstream down) | Inline | ~5k sats |

**Note:** Inline service failure refunds require the upstream API to actually fail. For deterministic testing, excess refunds (claude/openai with cheap prompts) are the most reliable.

## Agent Cost Table

| # | Agent | Cost (sats) | Endpoint | Minimal Input | Refund Support |
|---|-------|------------|----------|--------------|----------------|
| 1 | test-agent | 100 | POST /donate | `{}` | No |
| 2 | 1sat | 200 | POST /inscribe | `{data, contentType, publicKey}` | Yes (inline) |
| 3 | whisper | ~1.2k | POST /transcribe | `{audio: base64}` | Yes (inline) |
| 4 | openai | ~4.1k | POST /chat | `{messages: [{role,content}]}` | Yes (inline + excess) |
| 5 | seo | ~5k | POST /serp | `{keyword: "test"}` | Yes (inline) |
| 6 | reader | ~6k | POST /read | `{url: "..."}` | Yes (inline) |
| 7 | polymirror | ~10k | POST /leaderboard | `{}` | Internal only |
| 8 | claude | ~53k | POST /chat | `{messages: [{role,content}]}` | Yes (inline + excess) |
| 9 | x-research | ~125k | POST /search | `{query: "test"}` | Yes (inline) |
| 10 | banana | ~375k | POST /generate | `{prompt: "..."}` | Yes (inline + deferred) |
| 11 | banana2 | ~375k | POST /generate | `{prompt: "..."}` | Yes (inline + deferred) |
| 12 | kling | ~1.26M | POST /text-to-video | `{prompt: "..."}` | Yes (inline + deferred) |
| 13 | veo | ~1.5M | POST /text-to-video | `{prompt: "..."}` | Yes (inline + deferred) |
| 14 | messagebox | 0 | POST /sendMessage | `{message JSON}` | No |

## Test Tiers

### Tier 1: Smoke (~500 sats)
```
test-agent (100) + messagebox (0) + 1sat (200)
Purpose: Verify wallet-infra payment flow works at all
```

### Tier 2: Standard (~30k sats)
```
Tier 1 + whisper (1.2k) + openai (4.1k) + seo (5k) + reader (6k) + polymirror (10k)
Purpose: Test all cheap sync agents + verify balance reconciliation
```

### Tier 3: Refund Focus (~90k sats, most refunded)
```
Tier 2 + excess refund tests (openai "say ok" + claude "say ok")
Purpose: Full refund path validation. Most sats come back as excess refunds.
```

### Tier 4: Full (~300k sats)
```
Tier 3 + x-research (125k)
Purpose: All sync agents tested
```

### Tier 5: Complete (~5M sats, manual only)
```
Tier 4 + banana (375k) + banana2 (375k) + kling (1.26M) + veo (1.5M)
Purpose: Every agent including async video/image generation
```

## Test Phases

### Phase 0: Pre-flight
```
1. balance_before = getBalance(test-wallet)
2. utxo_count = listOutputs(test-wallet).totalOutputs
3. GET /monitor/audit?level=2 → assert 0 issues
4. Verify sufficient balance for selected tier
5. Split UTXOs if needed (manage split for concurrency)
```

### Phase 1: Agent Payment Tests
```
For each agent in selected tier:
  1. balance_pre = getBalance()
  2. response = brc31_helpers.py pay POST agent/endpoint {input}
  3. Parse response: check status, check for refund/excessRefund
  4. If refund auto-internalized: record refund_sats
  5. balance_post = getBalance()
  6. cost_actual = balance_pre - balance_post
  7. Assert: response succeeded OR refund was issued
  8. Log: {agent, cost_expected, cost_actual, refund_sats, success}
```

### Phase 2: Refund Path Tests
```
Test A — Excess Refund (most reliable):
  1. balance_pre = getBalance()
  2. response = pay POST openai/chat {messages: [{role: "user", content: "Say ok"}]}
  3. Assert: response.status == 200
  4. Assert: response has excessRefund block
  5. Assert: refund auto-internalized (refund.processed == true)
  6. balance_post = getBalance()
  7. refund_sats = excessRefund.satoshis
  8. Assert: balance_pre - balance_post < 500 (only fees, most refunded)

Test B — Service Failure Refund (if possible):
  1. balance_pre = getBalance()
  2. response = pay POST reader/read {url: "https://this-domain-does-not-exist-xyz123.test"}
  3. If response.code == "ERR_SERVICE_FAILED_REFUND_ISSUED":
     Assert: response has refund block
     Assert: refund auto-internalized
     balance_post = getBalance()
     Assert: balance_pre - balance_post <= 1000 (fee only)

Test C — No Refund Expected:
  1. balance_pre = getBalance()
  2. response = pay POST polymirror/leaderboard {}
  3. Assert: response succeeded (no refund)
  4. balance_post = getBalance()
  5. Assert: balance_pre - balance_post == polymirror_cost (within tolerance)
```

### Phase 3: Abort Test
```
1. balance_pre = getBalance()
2. manage send test-wallet <address> 1000 --no-send → returns reference
3. manage abort test-wallet <reference>
4. balance_post = getBalance()
5. Assert: balance_pre == balance_post (UTXOs released)
```

### Phase 4: Reconciliation
```
1. balance_after = getBalance()
2. total_agent_costs = sum(cost_actual for each agent test)
3. total_refunded = sum(refund_sats for each refund)
4. expected_balance = balance_before - total_agent_costs
5. Assert: |balance_after - expected_balance| < 2000 (fee tolerance)
6. GET /monitor/audit?level=1 → assert 0 issues
7. Print report:
   - Sats spent on agents: X
   - Sats refunded: Y
   - Net cost: X - Y
   - Miner fees: Z (= balance_before - balance_after - net_cost)
   - All agents: PASS/FAIL
```

### Phase 5: Sweep
```
1. manage sweep test-wallet <recovery-address>
2. Poll transaction status until completed
3. balance_final = getBalance()
4. Assert: balance_final == 0
5. Print: "Recovered N sats to <address>, lost M sats to fees"
```

## Implementation: Extend `manage` CLI

**Why manage:** It already has BRC-31 auth to wallet-infra, `sweep`, `send --no-send`, `abort`, `balance`, `history`. All agent configs in `agents.toml`.

**New subcommand:**
```
manage test [--tier 1|2|3|4|5] [--agent NAME] [--sweep-to ADDR] [--json]
```

**For x402 agent calls:** Shell out to `brc31_helpers.py pay` which handles the full BRC-31 + 402 + payment + refund flow with MetaNet Client. The manage CLI handles wallet-infra operations; brc31_helpers.py handles agent payment flows.

**Report output (JSON):**
```json
{
  "tier": 2,
  "balance_before": 500000,
  "balance_after": 468500,
  "agents_tested": 8,
  "agents_passed": 8,
  "total_spent": 31500,
  "total_refunded": 0,
  "miner_fees": 0,
  "tests": [
    {"agent": "test-agent", "cost": 100, "refund": 0, "status": "pass"},
    {"agent": "openai", "cost": 4100, "refund": 3800, "status": "pass", "note": "excess refund"},
    ...
  ],
  "audit_level2": "clean",
  "sweep_txid": "abc123...",
  "sweep_recovered": 468500
}
```

## x402 Refund Internalization Detail

When `brc31_helpers.py pay` gets a refund:

1. **Detect:** Check response body for `refund` or `excessRefund` key
2. **Parse:** Extract `transaction` (base64 BEEF), `derivationPrefix`, `derivationSuffix`, `senderIdentityKey`
3. **Internalize:** POST to MetaNet Client at `localhost:3321/internalizeAction`:
   ```json
   {
     "tx": [byte_array_from_base64],
     "outputs": [{
       "outputIndex": 0,
       "protocol": "wallet payment",
       "paymentRemittance": {
         "derivationPrefix": "...",
         "derivationSuffix": "...",
         "senderIdentityKey": "03..."
       }
     }],
     "description": "Refund: N sats (txid: ...)"
   }
   ```
4. **Result:** Refund output becomes spendable in wallet, balance restored

**Double-refund prevention (async agents):** KV stores `refunded: true` per request_id. Subsequent `/status` polls return `{already_refunded: true, txid: "..."}` — no duplicate refund issued.

## Server-Side Refund Flow (for reference)

All agents use shared `issue_refund()` from `bsv-auth-cloudflare/src/refund/mod.rs`:

```
1. Derive client's receiving key via BRC-29 (HMAC nonce prefix + random suffix)
2. Build P2PKH locking script from derived key
3. createAction on wallet-infra → get unsigned tx template
4. Sign locally with agent's private key (refund/signer.rs)
5. processAction on wallet-infra → broadcast signed tx
6. Build AtomicBEEF envelope
7. Return RefundInfo {transaction, derivationPrefix, derivationSuffix, senderIdentityKey, satoshis, txid}
```

## Key Risks

1. **Upstream API actually failing** — For service failure refund tests, we depend on the upstream API being down or rejecting input. This is non-deterministic. Excess refunds (claude/openai cheap prompts) are more reliable.
2. **Async agent timeouts** — banana/veo/kling jobs can take 3-5 minutes. Only test in manual tier.
3. **BSV/USD price volatility** — Agent costs are priced in USD, converted to sats dynamically. Costs may vary between runs.
4. **MetaNet Client must be running** — brc31_helpers.py needs localhost:3321 for both payments and refund internalization.

## Budget-Optimized Plan: All Code Paths for ~$0.15

The key insight: **test every code path type, not every agent**. Many agents share identical code from `bsv-auth-cloudflare`.

### Code Path → Minimum-Cost Representative

| # | Code Path | Agent | Input | Gross | Net |
|---|-----------|-------|-------|-------|-----|
| 1 | Sync → success, no refund | test-agent | POST /donate {} | 100 | 100 |
| 2 | Sync → excess refund | openai | POST /chat "Say ok" | ~4.1k | ~500 |
| 3 | Sync → no refund by design | polymirror | POST /leaderboard | ~10k | ~10k |
| 4 | Sync → content filter, correct no-refund | openai | Violating prompt | ~4.1k | ~4.1k |
| 5 | Async → poll → success | test-agent | POST /test-async {fail:false} | 100 | 100 |
| 6 | Async → poll → deferred refund | test-agent | POST /test-async {fail:true} | 100 | ~300 |
| 7 | Abort (UTXO release) | manage CLI | send --no-send + abort | 0 | 0 |
| 8 | Sweep | manage CLI | sweep to recovery addr | ~200 | ~200 |
| | **TOTAL** | | | | **~15.3k sats** |

### Required: Add `/test-async` to test-agent

The cheapest async agent (banana) costs 375k sats. We need an async test endpoint on test-agent (100 sats) that simulates:

```
POST /test-async {fail: false}  → returns {request_id: "abc123"}
GET /status/abc123              → first poll: {status: "processing"}
GET /status/abc123              → second poll: {status: "succeeded", result: "ok"}

POST /test-async {fail: true}   → returns {request_id: "def456"}
GET /status/def456              → {status: "failed", refund: {transaction, derivationPrefix, ...}}
```

This exercises the EXACT same wallet-infra code paths as banana/veo/kling (internalizeAction for payment, issue_refund for failure) but at 100 sats instead of 375k.

### When to Test Real Expensive Agents

Run the budget suite on every deploy. For expensive agents (banana, veo, kling, claude, x-research), test manually and only when:
- Changing payment middleware code
- Changing refund infrastructure
- After a major wallet-infra upgrade
- Periodic spot-check (monthly)

## Open Questions

1. **Which wallet identity?** Use test-agent's wallet or a dedicated test wallet?
2. **How to fund?** `manage send` from another agent, or externally fund?
3. **CI integration?** Budget suite on every deploy? Or manual `manage test` only?
