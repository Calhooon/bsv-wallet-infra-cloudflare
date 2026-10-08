# E2E Test Harness Implementation — Session Handoff

Copy everything below the line into a new Claude Code window.

---

## What you are building

An end-to-end test harness that exercises every payment code path in a BSV wallet infrastructure system using real satoshis, verifies all 5 refund paths work correctly, reconciles every satoshi spent, and sweeps remaining funds to a recovery address. Budget: ~15k sats ($0.15) per run.

There are 4 GitHub issues on Calgooon/rust-wallet-infra under the "E2E Test Harness" milestone. They have a dependency chain. Read each issue FULLY before starting — they contain exact specs, code references, JSON formats, and acceptance criteria.

## Issues (read these first)

```bash
gh issue view 17  # Add /test-async endpoint to test-agent (START HERE)
gh issue view 18  # Add `manage test` subcommand
gh issue view 19  # Verify all refund paths e2e with real sats
gh issue view 20  # Balance reconciliation and sweep verification
```

Also read the design doc:
```bash
cat ~/bsv/rust-wallet-infra/docs/E2E_TEST_HARNESS_DESIGN.md
```

## Dependency chain

```
#17 (test-agent /test-async)     ← no dependencies, start immediately
     ↓
#18 (manage test subcommand)     ← depends on #17 being deployed
     ↓
#19 (refund path verification)   ← depends on #17 + #18
#20 (balance reconciliation)     ← depends on #18 + #19
```

## Codebase locations

| Repo | Path | What |
|------|------|------|
| test-agent | ~/bsv/agents/test-agent | Cloudflare Worker, 100-sat test endpoint |
| manage CLI | ~/bsv/agents/manage | Rust CLI for wallet ops (sweep, send, abort, balance) |
| wallet-infra | ~/bsv/rust-wallet-infra | The storage server being tested (WASM on CF Workers) |
| auth middleware | ~/bsv/rust-middleware/bsv-auth-cloudflare | Shared BRC-31 auth + payment + refund |
| rust-bsv-worm | ~/bsv/rust-bsv-worm | Client-side x402 payment SDK |
| bsv-sdk | ~/bsv/rust-sdk | BSV primitives (wallet types, crypto) |
| x402 skill | ~/.claude/plugins/marketplaces/calgooon-x402/skills/x402 | brc31_helpers.py for auth+paid requests |
| agents config | ~/bsv/agents/manage/agents.toml | All agent keys, URLs, storage URLs |

## Execution plan

### Wave 1: Build #17 (test-agent /test-async endpoint)

**Work in:** ~/bsv/agents/test-agent

Add `/test-async` (POST, 100 sats, BRC-31 auth) and `/status/{id}` (GET, free, BRC-31 auth) to test-agent. These simulate the async agent pattern (banana/veo/kling) at 100 sats instead of 375k.

Follow the EXACT pattern from banana-agent (~/bsv/agents/banana-agent/src/lib.rs lines 895-967) for:
- KV storage of job context (identity_key, satoshis_paid, refunded flag)
- /status polling with processing → succeeded/failed states
- Deferred refund via issue_refund() from bsv-auth-cloudflare
- Double-refund prevention (refunded: true in KV, subsequent polls return already_refunded)

**Quality gate:**
```bash
cd ~/bsv/agents/test-agent
cargo fmt --check
cargo clippy -- -D warnings
cargo test
worker-build --release
```

**Deploy and verify:**
```bash
# Deploy
npx wrangler deploy

# Test sync endpoint still works
cd ~/.claude/plugins/marketplaces/calgooon-x402/skills/x402
python3 ./scripts/brc31_helpers.py pay POST https://test.x402agency.com/donate '{}'

# Test async success path
python3 ./scripts/brc31_helpers.py pay POST https://test.x402agency.com/test-async '{"fail":false,"delay_secs":5}'
# Extract request_id from response
# Wait 6 seconds
python3 ./scripts/brc31_helpers.py auth GET https://test.x402agency.com/status/{request_id}
# Assert: status == "succeeded"

# Test async failure + deferred refund path
python3 ./scripts/brc31_helpers.py pay POST https://test.x402agency.com/test-async '{"fail":true,"delay_secs":5}'
# Extract request_id
# Wait 6 seconds  
python3 ./scripts/brc31_helpers.py auth GET https://test.x402agency.com/status/{request_id}
# Assert: status == "failed", refund block present, refund.processed == true

# Test double-refund prevention
python3 ./scripts/brc31_helpers.py auth GET https://test.x402agency.com/status/{same_request_id}
# Assert: already_refunded == true, no new refund block
```

**DO NOT proceed to Wave 2 until ALL verification steps pass.** If something fails, investigate, fix, redeploy, and re-verify. This is the foundation everything else builds on.

### Wave 2: Build #18 (manage test subcommand)

**Work in:** ~/bsv/agents/manage

Add `manage test` subcommand. Read the full issue #18 for the spec. Key points:
- Orchestrates 5 phases: pre-flight → agent tests → refund tests → abort test → reconciliation
- Shells out to brc31_helpers.py for agent payment calls
- Uses StorageClient directly for wallet-infra operations
- Outputs structured JSON report
- Optional --sweep-to for final recovery

**Quality gate:**
```bash
cd ~/bsv/agents/manage
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo build --release
```

**Verify with dry run (tier 1 only, ~500 sats):**
```bash
cd ~/bsv/agents/manage
cargo run -- test --tier 1 --json
```

Assert:
- Pre-flight passes (audit clean, sufficient balance)
- test-agent /donate succeeds
- messagebox works (0 sats)
- 1sat inscribe works
- Balance reconciliation passes
- Report output is valid JSON

**Then run tier 2 (full code path coverage, ~15k sats):**
```bash
cargo run -- test --tier 2 --json
```

Assert all 8 code paths pass. If any fail, investigate and fix before proceeding.

### Wave 3: Verify #19 + #20 (refund paths + reconciliation)

These are verification issues, not implementation issues. Once manage test works with tier 2, the refund paths and reconciliation are being tested. But we need to explicitly verify each acceptance criterion.

**For #19 (refund paths), verify each checkbox:**
- [ ] Excess refund: openai response has excessRefund, auto-internalized, balance correct
- [ ] Deferred refund: test-agent /test-async fail=true, poll returns refund, auto-internalized
- [ ] No-refund-by-design: polymirror response has no refund block, full cost charged
- [ ] Double-refund prevention: second poll returns already_refunded, no balance change
- [ ] Audit clean after all refund tests

**For #20 (reconciliation), verify:**
- [ ] Balance equation: balance_before - balance_after == total_costs - total_refunds (within 2k tolerance)
- [ ] Sweep recovers all sats (balance = 0 after sweep)
- [ ] Audit level 1 + level 2 clean after sweep
- [ ] Every sat accounted for in report
- [ ] Report is valid structured JSON

### Wave 4: Final validation

Run the complete harness 3 times to verify consistency:
```bash
# Run 1
cargo run -- test --tier 2 --sweep-to <recovery-addr> --json > run1.json

# Run 2 (fund test wallet again first)
cargo run -- test --tier 2 --sweep-to <recovery-addr> --json > run2.json

# Run 3
cargo run -- test --tier 2 --sweep-to <recovery-addr> --json > run3.json
```

All 3 runs should:
- Pass all tests
- Have consistent cost per agent (within BSV/USD price movement)
- Reconcile to 0 unaccounted sats
- Sweep successfully
- Leave audit clean

## Critical rules

1. **Never skip quality gates.** If fmt/clippy/test/build fails, fix it. Don't --no-verify or ignore warnings.

2. **Verify against production after every deploy.** Don't trust local tests alone — the Phase 2 batch_id bug was only caught in production.

3. **Read the existing code before writing.** The banana-agent async pattern, the manage CLI sweep command, the brc31_helpers.py refund detection — these are battle-tested patterns. Study them, don't reinvent.

4. **If a test fails, investigate before retrying.** Understand WHY it failed. Is it a real bug, a timing issue, a balance problem? The Phase 1 audit taught us: 8,645 of 8,646 "issues" were false positives.

5. **Account for every satoshi.** The whole point of this harness is to prove no sats are lost. If reconciliation is off by more than 2000 sats, something is wrong — find it.

6. **MetaNet Client must be running** at localhost:3321 for brc31_helpers.py to work. Check with: `curl -s -H "Origin: http://localhost" http://localhost:3321/isAuthenticated`

7. **Don't deploy wallet-infra** without explicit user approval. You CAN deploy test-agent freely (it's a test endpoint). For wallet-infra: ask first.

8. **The Cloudflare API token** for deploying agents is in ~/bsv/teragunv2/secrets.md. Export as CLOUDFLARE_API_TOKEN.

## Budget

- Tier 1 (smoke): ~500 sats
- Tier 2 (all code paths): ~15k sats  
- Total for 3 validation runs + sweeps: ~50k sats (~$0.50)

If you're spending more than 100k sats total across all runs, something is wrong. Stop and investigate.
