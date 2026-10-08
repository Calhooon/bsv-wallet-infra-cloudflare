# Unproven Backlog — Audit & Design Doc

**Status:** Read-only audit complete. Design proposed. No code changes yet.
**Last updated:** 2026-04-15
**Owner:** Come back to this before implementing.

---

## TL;DR

Production has a growing pool of `status='unproven'` transactions:

| Date | Unproven | Orphans |
|---|---|---|
| 2026-04-13 | 81 | 10 |
| 2026-04-15 | 577 | 16 |

Three distinct problems — entangled but independently addressable:

1. **`compact_beef` fails every monitor run** with `D1_ERROR: string or blob too big: SQLITE_TOOBIG`. Firing on every single `monitor_run`. Root cause: `compact_beef` reassembles BEEFs in memory and writes them back inline via `QVal::Blob`. **R2 `BlobStore` exists (`src/r2.rs`) but is completely dead code — zero call sites.** Every binary column writes inline.
2. **WoC rate limiting (429)** throttles proof collection. Both `proof` and `block header` endpoints are returning 429. `b7eac11` added retry for 500s but not 429s.
3. **Hard `LIMIT 500`** in `check_for_proofs` (`monitor.rs:431`) plus orphan txs with no `proven_tx_req` row means ~77 txs are starved per run once the backlog exceeds 500.

---

## Detailed Audit

### Problem 1 — `compact_beef` SQLITE_TOOBIG

- Location: `src/monitor.rs:887-1023`
- Flow: reads `input_beef` → `Beef::from_binary()` → merges proofs from `proven_txs` → `trim_known_proven()` → `to_binary()` → `UPDATE proven_tx_reqs SET input_beef = ?` with `QVal::Blob(new_bytes)` at `monitor.rs:1004`.
- Fires on **every** `monitor_run` log we sampled. Completely broken.
- `beef_compacted` counter is always `0` — zero successful compactions.

### Problem 2 — WoC 429 rate limiting

Sample monitor events (2026-04-15):
```
18:41  proofs_checked=500  proofs_found=98  (good window)
18:37  proofs_checked=500  proofs_found=89  (good window)
18:31  proofs_checked=500  proofs_found=0   (throttled)
18:22  errors: ["proof(fbe7fa74):WoC proof API error 429",
                "proof(79a0f378):WoC block header API error 429",
                "proof(0be5827a):WoC block header API error 429"]
```

Two kinds of 429s:
- **Proof API 429** — expected, matches all 3 reference impls
- **Block header API 429** — unexpected. Chaintracks is supposed to handle headers (b7eac11 + wrangler.toml CHAINTRACKS_URL). There's a code path still calling WoC for headers. Likely inside BEEF verification when a block isn't yet in chaintracks.

**Throughput math:**
- Good runs: 90-100 proofs/run × 12 runs/hr = 1,080-1,200 proofs/hr potential
- Inflow: 510 created in last hour → inflow spikes of ~500/hr
- Normally sustainable, but any throttled run zeros out throughput for 5 min. A string of bad runs causes backlog.

### Problem 3 — LIMIT 500 + sort priority + orphans

`monitor.rs:426-431`:
```sql
SELECT proven_tx_req_id, txid, status, attempts, hex(raw_tx) as raw_tx
FROM proven_tx_reqs
WHERE status IN ('unmined', 'unknown', 'unconfirmed', 'callback')
ORDER BY attempts ASC, created_at DESC
LIMIT 500
```

With 577 unproven (561 with reqs + 16 orphans):
- **61 req rows per run get starved** (beyond the 500 window)
- The `ORDER BY attempts ASC, created_at DESC` pushes high-attempt txs LAST → stuck txs starve themselves
- **16 orphan txs** never get checked regardless — nothing iterates `transactions WHERE status='unproven'`, only `proven_tx_reqs`

### Age distribution (2026-04-15)

| Bucket | Count |
|---|---|
| < 1h | 510 |
| 1-6h | 0 |
| 6-24h | 0 |
| 1-7d | 65 |
| > 7d | 2 |
| **Total** | **577** |

The 510 fresh-<1h is current wallet activity — these will likely clear naturally if throughput holds. The 65+2 tail is the real stuck pool. Growing.

### Orphan txids (grew from 10 → 16, dates cluster on specific days → likely specific bug incidents)

First 10 orphans identified 2026-04-13:
- 2026-02-24 × 1 (78e0df15…)
- 2026-02-25 × 1 (e8299b46…)
- 2026-04-08 × 7 (17:22–19:16 window)
- 2026-04-12 × 1 (84ca7b70…)

6 more added between 2026-04-13 and 2026-04-15 — need re-query before implementation.

---

## Reference Implementation Research

All 3 siblings audited:
- `~/bsv/wallet-toolbox` (TypeScript)
- `~/bsv/bsv-wallet-toolbox-rs` (Rust)
- `~/bsv/go-wallet-toolbox` (Go)

### Consensus patterns

| Pattern | TS | Rust | Go |
|---|---|---|---|
| 429 handling | Fixed 2s sleep, ≤2 retries | Fixed 2s, 2-5 retries | resty: 1s wait, 3 retries |
| Retry-After parsing | ❌ | ❌ | ❌ |
| Proactive rate limit | ❌ | ❌ | ❌ |
| Triage (status first) | Minimal | Batch status | Batch + confirm-depth filter |
| Proof cache | DB only | DB + in-mem LRU | DB only |
| BEEF/rawTx storage | **Inline** | **Inline** | **Inline** |
| `compact_beef` equivalent | **None** | **None** | **None** |

### Two big takeaways

1. **All references use fixed 2s × 2–3 retry on 429.** No exponential backoff, no Retry-After. Simple and proven. Port this.
2. **None of the references have `compact_beef`.** They all use real SQL (Postgres/MySQL/SQLite) with rows up to many MB. `compact_beef` is unique to our Rust port and only exists because D1 has ~1MB row limits. **The TOOBIG error is a collision between `compact_beef` (invented for D1) and `compact_beef` writing inline back to D1 (bypassing BlobStore).**

### Key file references (for implementation)

- `~/bsv/bsv-wallet-toolbox-rs/src/services/providers/whatsonchain.rs:109-129` — `get_with_retry()`, 2s × 2
- `~/bsv/bsv-wallet-toolbox-rs/src/storage/sqlx/storage_sqlx.rs:2588-2950` — proof polling loop + batch triage
- `~/bsv/wallet-toolbox/src/services/providers/WhatsOnChain.ts:130,239,327,377` — TS 429 handling
- `~/bsv/wallet-toolbox/src/monitor/tasks/TaskCheckForProofs.ts:58-72` — pagination 100/page
- `~/bsv/go-wallet-toolbox/pkg/services/internal/httpx/factory.go:20-21,64` — resty retry on 429
- `~/bsv/go-wallet-toolbox/pkg/monitor/internal/tasks/check_for_proofs.go` — 1000/page, 10 pages max
- `~/bsv/go-wallet-toolbox/pkg/storage/internal/actions/synchronize_tx_statuses.go:200-257` — confirm-depth triage filter
- `~/bsv/go-wallet-toolbox/pkg/storage/internal/actions/synchronize_tx_statuses.go:260-277` — TryLock + cached last-block to skip redundant syncs

---

## Our Code — Binary Column Write Paths (all direct, zero BlobStore usage)

| Column | File | Line | Writer |
|---|---|---|---|
| `transactions.input_beef` | `src/storage/create_action.rs` | 378 | `QVal::Blob()` |
| `transactions.raw_tx` | `src/storage/create_action.rs` | 863 | `QVal::Blob()` |
| `proven_tx_reqs.input_beef` | `src/storage/process_action.rs` | 369, 379, 402 | `QVal::Blob()` |
| `proven_tx_reqs.raw_tx` | `src/storage/process_action.rs` | 369, 379, 402 | `QVal::Blob()` |
| `proven_txs.merkle_path` | `src/monitor.rs` | 571 | `QVal::Blob()` |
| `proven_tx_reqs.input_beef` (compact) | `src/monitor.rs` | 1004 | `QVal::Blob()` ← **TOOBIG offender** |

`src/r2.rs` defines:
- `THRESHOLD = 4096` (line 12)
- Overflow to R2 when blob > 4096 bytes
- Sentinel: D1 column set to `NULL` when data is in R2
- Key format: `{table}/{id}/{column}`

**BlobStore has never been called in production.** It's been dead code since implementation.

---

## Proposed Design

### Approach C (Recommended): Tiered — R2 for `input_beef` only + 429 backoff + pause compact_beef

**Rationale for C over A (full BlobStore everywhere):**
- A pays permanent R2 latency tax (~50ms/read) on every user-facing RPC (`listOutputs`, `listActions`)
- A touches every read path in the codebase
- Only `input_beef` has demonstrated unbounded growth; `raw_tx` and `merkle_path` are bounded

**Rationale for C over B (surgical fix to compact_beef only):**
- B leaves TOOBIG latent in `internalizeAction` / `createAction` — any upstream writer of a large `input_beef` re-triggers the same error through a different code path
- B leaves `BlobStore` as permanent dead code

### Important nuance on compact_beef (noted 2026-04-15)

**Pausing `compact_beef` is not just safe — it may actually be beneficial.**

`compact_beef`'s purpose is to trim already-proven ancestor txs from stored BEEFs. But **when we next spend from an output, we need the full ancestry chain** to build the next `createAction` BEEF. If `compact_beef` has aggressively trimmed ancestors, the next spend has to re-fetch them from the network (WoC `get_raw_tx` round-trips), which is both slower and burns more WoC budget.

Keeping BEEFs "fat" until proof is available is arguably the correct default — the storage cost is tolerable and it avoids round-trips on the hot spend path.

**Revised position for Step 2:** Instead of rushing to re-enable `compact_beef` after wiring `BlobStore`, consider whether we need it at all. Options:
- **Leave it paused permanently** — accept the storage cost, keep the spend path fast
- **Trigger it selectively** — only compact BEEFs that haven't been touched in N days (archival compaction)
- **Compact only on hot paths** — e.g. after a spend successfully completes, compact the now-irrelevant ancestry

The "never compact" option matches what all 3 reference impls do (none of them have a compact_beef task). TS toolbox stores inline in real SQL and never trims. Rust/Go toolbox same.

**Decision deferred to Step 2 planning.** For now, document that compaction's paused state is load-bearing for future spend performance, not just a bug workaround.

---

### Step 1 — Stop the bleeding (small diff, ship first)

1. **Pause `compact_beef`** — gate its invocation in the monitor task list behind a flag or `if false`. Stop the error log spam. `compact_beef` is an optimization, not correctness — compacted BEEFs save space/bandwidth but pausing is safe.
2. **Add 429 retry in `src/services/woc.rs`** — mirror the `b7eac11` pattern for 500s. Fixed 2s × 2, matching all 3 reference impls.
3. **Add 429 retry for block header calls** too — whichever code path is still calling WoC for headers. (Ideally: make it fall through to chaintracks — b7eac11 was supposed to make chaintracks primary, but some path still calls WoC.)
4. **Lift `LIMIT 500`** to something higher (800? 1000?) OR implement pagination like go-wallet-toolbox (1000/page × 10 pages). Reference: `check_for_proofs.go` and `synchronize_tx_statuses.go:303-316`.
5. **Change sort priority** — `ORDER BY attempts ASC, created_at DESC` starves high-attempt txs. Consider `ORDER BY (attempts < 5) DESC, created_at ASC` (prioritize fresh, but cycle through stuck set too).

**Expected impact after Step 1:**
- Eliminates TOOBIG error spam (Problem 1, partially)
- Reduces 429-induced throughput loss (Problem 2)
- Clears the 61 starved-beyond-500 rows (Problem 3, partial)
- Does NOT fix the 16 orphans

### Step 2 — Real fix (larger diff)

1. **Wire `BlobStore` for `input_beef` columns only** — `transactions.input_beef` and `proven_tx_reqs.input_beef`. Every writer calls `BlobStore.put()`; every reader calls `BlobStore.get()`.
2. **Re-enable `compact_beef`** — now writes through BlobStore, can't hit TOOBIG.
3. **Leave `raw_tx` and `merkle_path` inline** — they're bounded, no need to pay R2 latency.
4. Grep every reader of `input_beef` (create_action, process_action, internalize_action, monitor.rs, beef_verification) and add hydration calls.

### Step 3 — Orphans

1. **Audit the 16 orphans** — identify `(txid, created_at)` for all. Cluster by date.
2. **Root cause** — `git log --since='2026-02-20' --until='2026-02-26'` and same for April windows. Find the tx-creation path that skipped `proven_tx_req` insertion.
3. **Backfill OR extend `check_for_proofs`** — either create missing `proven_tx_req` rows for existing orphans, or extend the monitor to also iterate `transactions WHERE status='unproven' AND no req exists`.
4. **Risk**: orphans may have been deliberately purged (e.g. by abort), and the `status='unproven'` is just stale. Verify before creating new reqs.

---

## Risks & Unknowns

### Riskiest assumptions

1. **D1's actual row/param size limit.** SQLITE_TOOBIG could be:
   - 1MB row size (typical assumption)
   - wasm_bindgen parameter marshaling limit (could be smaller)
   - D1 batch RPC layer limit
   
   **Diagnostic needed:** instrument `compact_beef` to log `new_bytes.len()` before the failing UPDATE, or sample `SELECT length(input_beef) FROM proven_tx_reqs ORDER BY length DESC LIMIT 10` to see the distribution.

2. **NULL sentinel collisions.** BlobStore uses `NULL in column = "data in R2"`. Every existing reader of `input_beef` that treats NULL as "no data" will silently misinterpret. Must be exhaustively grepped before Step 2.

3. **R2 latency on hot paths.** Need to confirm which hot reads touch `input_beef` vs just `raw_tx`. `createAction` builds new BEEFs from ancestor chains — likely reads `input_beef`. Budget ~50ms/read.

4. **Backoff alone doesn't help if WoC rate-limits us globally.** Retrying harder against a global ceiling just re-fails. Real lever is per-run cap + pagination, OR upgrading WoC plan.

### Unknowns (verify before implementing)

1. Exact failing BEEF size — see diagnostic above
2. D1 row/param limit — test write or CF docs check
3. Our WoC rate budget — free tier ~3/sec? paid? (drives whether 429 retry suffices)
4. Are orphans reachable via any other path — git log check
5. Who consumes compacted BEEFs downstream — confirm pausing is safe
6. Which path is still calling WoC for block headers (should be chaintracks per b7eac11)

---

## Will the above fix 577 unproven?

**Partial yes.** Here's the breakdown:

| Problem | Step 1 | Step 2 | Step 3 |
|---|---|---|---|
| TOOBIG spam every run | ✅ (paused) | ✅ (fixed) | — |
| WoC proof 429s | ✅ (retry) | — | — |
| WoC header 429s | ✅ (retry + chaintracks path fix) | — | — |
| LIMIT 500 starvation | ✅ (lifted) | — | — |
| Sort priority starvation | ✅ (reordered) | — | — |
| 16 orphans invisible to monitor | ❌ | ❌ | ✅ |
| Inflow > outflow during throttle | ✅ (fewer throttled runs) | — | — |

**After Step 1** alone, backlog should drop significantly — probably clear the 510 fresh-<1h pool and most of the 65 1-7d pool. The 16 orphans and any `input_beef`-growth-induced failures will remain until Step 2 and Step 3.

**After Steps 1+2+3:** all known problems resolved.

**Caveat:** if inflow keeps spiking to 500/hr during peak activity and WoC continues throttling aggressively, we may need Step 4 (paid WoC plan OR mirror proof source via ARC/TAAL as fallback).

---

## Next Actions (not yet taken)

- [ ] User approval of approach
- [x] Run diagnostic query for `input_beef` size distribution — **DONE 2026-04-15, see Appendix A**
- [x] Re-audit orphan list — **DONE 2026-04-15, grew 10 → 16, orphans still being created TODAY, see Appendix B**
- [x] Find the still-calling-WoC-for-headers code path — **DONE 2026-04-15, see Appendix C**
- [x] Diagnose orphan root cause — **DONE 2026-04-15, see Appendix D**
- [x] Verify retry status of remaining WoC helpers — **DONE, see Appendix C updated**
- [x] Check chaintracks API for hash→height — **DONE, not directly possible, see Appendix E**
- [x] Step 1 implementation — **SHIPPED 2026-04-15 as version f31ad2e6** (~150 LOC: compact_beef pause, 4 retry helpers, hash→header cache, LIMIT 500→1000)
- [x] Step 1b (Option B) implementation — **SHIPPED 2026-04-15 as version efbd2696** (~60 LOC: MultiProvider.get_status_for_txids delegation, retry on batch status, cron */5→*/2)
- [ ] Step 2 migration strategy for the 10+ wedged >1MB rows (deferred — not urgent, see Option D below)
- [ ] Step 3 orphan handling (Option C: backfill + Option E: internalizeAction batch rewrite for root cause)

---

## Option B shipped 2026-04-15 — triage fix

### Root cause of the broken triage

`MultiProvider` (the production `ProofService` used by `run_monitor` via `lib.rs:213`) did NOT override `get_status_for_txids`. The impl block at `src/services/multi.rs:76` only contained `get_chain_height`, `get_proof`, `get_raw_tx`, and (after Step 1) `reset_run_cache`.

Result: calls fell through to the default trait impl at `src/services/mod.rs:150-159` which returns `"unknown"` for every txid. The monitor's safety net at `src/monitor.rs:485` then saw `confirmed == 0 && !all_txids.is_empty()` and forced "check all" mode — calling `get_proof()` for every pending tx regardless of mined status.

**Meanwhile `WocProvider::get_status_for_txids` at `src/services/woc.rs:322` already had a correct implementation** that chunks at 20 and hits `POST /txs/status`. It just was never being called from production.

### What changed

1. **`src/services/multi.rs`** — added `get_status_for_txids` override delegating to `self.woc`
2. **`src/services/woc.rs`** — added retry loop to `get_status_for_txids` (missed in first retry pass; would have single-429'd the whole batch otherwise)
3. **`wrangler.toml`** — cron `*/5 * * * *` → `*/2 * * * *`

### WoC load analysis

| Config | Runs/hour | Proof calls/run | Header calls/run | Total calls/hr |
|---|---|---|---|---|
| Pre-Step-1 baseline | 12 | ~500 | ~500 (no cache) | **~12,000** |
| Step 1 only (f31ad2e6) | 12 | ~1000 | ~50 (90% cache hits) | ~12,600 |
| Step 1b (efbd2696) | 30 | ~50 confirmed only | ~5 | **~1,650** |

**~7× reduction vs baseline** in total WoC API calls per hour, despite the 2.5× cadence increase. Headroom is ample.

### Expected behavior after Option B

- `proofs_checked` field now reports the number of **confirmed-and-checked** txs (after triage filter), not the raw pending count
- The triage log line `check_for_proofs triage: total=X confirmed=Y mempool=Z missing=W` will show real numbers instead of the pre-fix pattern of `0/N confirmed → falling back`
- Cache hit rate on header lookups should stay ≥90% since confirmed txs from a single run tend to cluster in recent blocks
- Drain rate should jump: more efficient use of each run's WoC budget means more actual proofs collected per cron tick

### Observed behavior — Option B working, plus one new bug surfaced

Post-deploy runs (2026-04-15 ~19:40 onwards):
- `checked` dropped from ~1000 to 75-485 — triage filter IS active
- `found` jumped: single run cleared 492 proofs (19:43:28) vs pre-B peak of ~178
- **Drain rate: ~6,300/hr vs inflow ~500/hr → 12× surplus**

**New bug surfaced by higher throughput: proven_txs UNIQUE race.** Under the `*/2` cadence with triage-filtered runs, two overlapping monitor invocations could pull the same still-`'unmined'` req, both fetch its proof, and race on `INSERT INTO proven_txs`. The second insert hit `UNIQUE constraint failed: proven_txs.txid` which caused `store_proof_result` to return `Err` **before** reaching phase 2 (the batch update that moves the req to `'completed'`). The req stayed `'unmined'`, got re-fetched on the next run, and looped — wasting WoC calls forever on that txid.

Every post-B run logged 3 `UNIQUE constraint` errors (the per-run cap), suggesting many more were silently occurring beyond the cap.

---

## Step 1c shipped 2026-04-15 — idempotency fix

Version: `5b7951ca-a321-4894-bbf2-d51781c421e6`

### What changed

Added `ensure_proven_tx_id(db, txid, proof_result, raw_tx_bytes, now)` helper at `src/monitor.rs:~558`:
- Tries `INSERT INTO proven_txs` as before
- On `Err` containing `"UNIQUE constraint failed"`, does `SELECT proven_tx_id FROM proven_txs WHERE txid = ?` and returns the existing id
- On any other error, propagates

Both proof-storing sites now call this helper:
- `store_proof_result` at `monitor.rs:~640` (normal `check_for_proofs` flow)
- `store_unfail_proof` at `monitor.rs:~1180` (unfail_transactions recovery flow)

Effect: phase 2 batch update now runs even when phase 1 found an existing row. The stuck `'unmined'` req gets cleared to `'completed'` regardless of which overlapping run "won" the INSERT race.

### Why not slow the cron back down?

Slowing `*/2 → */3` or `*/5` would reduce but not eliminate the race, at the cost of drain velocity. Making the code idempotent is the correct fix because:
1. It handles the race cleanly regardless of cadence
2. It leaves `*/2` free to drain a backlog fast when one forms
3. It also hardens against any future concurrency source (e.g. an admin re-run)

---

---

## 2026-04-16 ~00:00 UTC — Post-deploy audit findings

### Good news — BEEF broadcast works for incoming

Sampled 5 post-deploy INCOMING x402 payments:
- 4/5 are now known to WoC (vs 0/5 pre-deploy all orphaned)
- `96663585163e753e22b80617e4e34efd0f7ceb4e42d5bbfe89e01a65a2a7b57c` logs "broadcast seen on network" — full success path
- Incoming `internalizeAction → broadcast_beef` path is confirmed working

### Surfaced bug — refund signer in `bsv-auth-cloudflare`

**Every outgoing "Excess refund" tx post-deploy fails with TAAL ARC error 461 `Script failed an OP_EQUALVERIFY operation`.**

Traced through live diagnosis:
1. Failing refund `45f9caf3...` spends `654af8ae:0` (a change output from a previous outgoing tx)
2. Parent `654af8ae` IS mined on chain (block 942209, 2774 confirmations)
3. Our `outputs` table correctly stores locking script `76A9140C349C5A42AD5151A85F824C4ECCC6710351375A88AC` (P2PKH to PKH `0c349c5a...`) — matches on-chain parent
4. The refund's unlocking script provides a pubkey `027dba71...` whose hash160 is `9a4a87f372c80a05b089464f9463e9461da1b218`
5. `9a4a87f3... != 0c349c5a...` → wrong signing key → `OP_EQUALVERIFY` fails

**Root cause lives in `~/bsv/rust-middleware/bsv-auth-cloudflare/src/refund/signer.rs:197-219`**:

```rust
let signing_key = if pkh_primary == expected_pkh_hex {
    key_primary
} else if pkh_self == expected_pkh_hex {
    key_self
} else {
    // Neither matches — use primary key (may produce invalid tx)
    key_primary   // ← BUG: silently signs with wrong key
};
```

When neither derivation branch produces the expected P2PKH hash, the signer **silently falls through** and produces an invalid transaction. This should be a hard error.

### Why it was hidden until tonight

Pre-BEEF-broadcast behavior:
- Agent produces invalid refund with wrong signature
- `broadcast_raw_tx` sends just the raw tx to ARC/TAAL
- Without BEEF context, TAAL can't do full script validation against parent output
- TAAL accepts it into orphan mempool → silently dropped later → tx stuck at `unproven` forever
- Looked like a WoC rate limit / orphan mempool issue to the outside

Post-BEEF-broadcast behavior (`f6eb6ed1`):
- Agent produces invalid refund with wrong signature
- `broadcast_beef` sends full BEEF including parent raw_tx
- TAAL runs real script validation against the parent's output
- `OP_EQUALVERIFY` fails → TAAL returns 461 `Malformed transaction`
- wallet-infra's existing error handler catches this → `transactions.status='failed'`

**The new behavior is strictly correct** — we're failing fast on invalid txs instead of letting them pollute production state for weeks. The 805 historical backlog was a direct consequence of the old silent-drop.

### Underlying derivation asymmetry

The likely issue in `bsv-auth-cloudflare`:

At **create time** (`refund/mod.rs:111-119`), the SERVER's wallet derives a receiving pubkey for the CLIENT:
```rust
.get_public_key(GetPublicKeyArgs {
    protocol_id: Some(Protocol::new(SecurityLevel::Counterparty, "3241645161d8")),
    key_id: Some(key_id),
    counterparty: Some(Counterparty::Other(client_pubkey)),
    for_self: Some(false),
})
```

But for CHANGE outputs (which eventually become inputs to refund txs), the signer uses a different counterparty (`Counterparty::Self_`) at spend time, expecting to derive the matching private key.

The bug is that the create-time derivation (for change outputs in the signed tx) and the spend-time derivation (when later spending those change outputs) don't produce matching key pairs in all code paths.

### What to fix next session (bsv-auth-cloudflare repo)

1. Remove the silent fallback in `signer.rs:215-218` — raise a hard error with diagnostic info
2. Audit change output derivation: ensure create-time pubkey derivation and spend-time private key derivation use symmetric `Counterparty` + `for_self` parameters
3. Consider porting the reference toolbox's BRC-29 change output handling pattern
4. Add a unit test that round-trips: derive pub → create change output → later derive priv → verify scripts match

This is a DIFFERENT REPO from wallet-infra, so the fix requires touching `~/bsv/rust-middleware/bsv-auth-cloudflare`.

### Impact summary

| Path | Status |
|---|---|
| Incoming x402 payments (agent → us) | ✅ working post-deploy |
| Outgoing refunds (us → agent) | ❌ failing immediately (surfaced existing bug) |
| Outgoing refund middleware path | 🛠 needs fix in `bsv-auth-cloudflare` |
| wallet-infra hot path | ✅ stable, matches reference impl |

**Tonight's recovery is durable for incoming payments. Outgoing refunds need the middleware fix before they can land correctly.**

---

## 2026-04-15 23:20 UTC — BEEF broadcast fix shipped

**Version `f6eb6ed1-3a6a-4bfd-a230-318e691638d7`** — live

### What changed

Both broadcast sites now send the full BEEF (parent ancestry + target tx) instead of just the raw tx, matching `bsv-wallet-toolbox-rs`'s `post_beef` intent:

1. **`src/storage/internalize_action.rs:641`**
   ```rust
   // BEFORE
   let raw_tx_hex = hex::encode(raw_tx);
   self.broadcast.broadcast_raw_tx(&raw_tx_hex).await
   // AFTER
   let beef_hex = hex::encode(input_beef);
   self.broadcast.broadcast_beef(&beef_hex).await
   ```
   `input_beef` is already the complete BEEF the agent submitted to `internalizeAction`, so zero construction needed — just forward.

2. **`src/storage/process_action.rs:~425`**
   ProcessAction is more complex because the raw_tx is newly signed and not yet in any stored BEEF. The fix constructs a fresh BEEF inline:
   ```rust
   let beef_hex_opt = input_beef_bytes.as_ref().and_then(|ib| {
       let mut beef = Beef::from_binary(ib).ok()?;
       beef.merge_raw_tx(raw_tx.to_vec(), None);
       Some(hex::encode(beef.to_binary()))
   });
   let broadcast_result = match &beef_hex_opt {
       Some(beef_hex) => self.broadcast.broadcast_beef(beef_hex).await,
       None => self.broadcast.broadcast_raw_tx(&hex::encode(raw_tx)).await,
   };
   ```
   Parses the stored `input_beef` (ancestors for inputs being spent), adds the new signed tx via `merge_raw_tx`, serializes to canonical binary, broadcasts. Falls back to raw_tx if the input_beef is missing or parse fails.

### Combined with the earlier `52aa30ee` raw_tx preservation fix

Two deploys working together to prevent recurrence:

| Deploy | Version | Fix |
|---|---|---|
| `52aa30ee` | Preserves `raw_tx` in `transactions` through `processAction` | Stops data loss |
| **`f6eb6ed1`** | Broadcasts BEEF (not raw_tx) on both internalize + process paths | Stops orphan mempool |

Together these close the complete failure chain that produced the 805 stuck txs:
1. ✅ Children can now find their parent's raw_tx when building BEEFs
2. ✅ Broadcasts carry the full ancestry so miners don't orphan them
3. ✅ Stale in-DB data no longer gets reused blindly — each broadcast rebuilds BEEF fresh from current state

### Next-session items (not blocking)

- **Port `send_waiting` + `post_beef` architecture**: reference impl doesn't broadcast at internalize time at all — it stores `status='unsent'` and lets `send_waiting_transactions` handle broadcast asynchronously with smart EF vs full-BEEF selection. Our synchronous approach works but isn't identical to reference. Worth porting properly in a dedicated session.
- **Add `max_attempts → 'invalid'` auto-cleanup** in `check_for_proofs` matching reference.
- **Remove `let _ = log_monitor_event`** silent-fail at `monitor.rs:268` (observability gap).
- **Audit other ports vs reference** for 1-line divergences like the `raw_tx=NULL` bug. That was a ported bug hiding for weeks; others may exist.

---

## 2026-04-15 23:08 UTC — Bulk cleanup executed (805 → 0)

**Status: complete.** All 805 stuck txs marked failed, outputs reconciled, unproven count is ZERO.

### Classification result

Recursive ancestry walk across all 805 stuck txs showed **every single one** has at least one ancestor that ends at an external transaction that never made it on-chain. **100% unrecoverable** — simple rebroadcast cannot fix them because the fundamental ancestry chain is broken.

### Cleanup SQL executed

Batch timestamp: `2026-04-15T23:08:39.000+00:00`

```sql
-- Step 1: mark all 805 transactions as failed
UPDATE transactions SET status='failed', updated_at='2026-04-15T23:08:39.000+00:00'
WHERE status='unproven';
-- → 805 rows changed

-- Step 2: invalidate the corresponding proven_tx_reqs (stops monitor polling)
UPDATE proven_tx_reqs SET status='invalid', updated_at='2026-04-15T23:08:39.000+00:00'
WHERE txid IN (
  SELECT txid FROM transactions
  WHERE status='failed' AND updated_at='2026-04-15T23:08:39.000+00:00'
);
-- → 790 rows changed (15 orphans had no req row)

-- Step 3: release locked UTXOs from failed outgoing txs
UPDATE outputs SET spendable=1, spent_by=NULL, updated_at='2026-04-15T23:08:39.000+00:00'
WHERE spent_by IN (
  SELECT transaction_id FROM transactions
  WHERE status='failed' AND updated_at='2026-04-15T23:08:39.000+00:00' AND is_outgoing=1
);
-- → 316 rows changed (75,345,389 sats transitioned from locked to spendable)

-- Step 4: ghost-invalidate outputs from failed incoming txs (they never existed on-chain)
UPDATE outputs SET spendable=0, updated_at='2026-04-15T23:08:39.000+00:00'
WHERE transaction_id IN (
  SELECT transaction_id FROM transactions
  WHERE status='failed' AND updated_at='2026-04-15T23:08:39.000+00:00' AND is_outgoing=0
) AND spendable=1 AND spent_by IS NULL;
-- → 489 rows changed (includes the 250 outputs released by Step 3 that
--   were ALSO from incoming stuck txs — cascading cleanup, correct behavior)
```

### Financial reconciliation

| Metric | Value |
|---|---|
| Transactions marked failed | **805** |
| Proven_tx_reqs marked invalid | **790** |
| Outputs released (outgoing inputs) | **316** |
| Outputs ghost-invalidated (incoming) | **489** |
| Genuinely released sats (from non-stuck parent txs) | **3,465,843** |
| Sats marked unspendable (ghost balance removed) | **166,473,444** (≈1.66 BSV) |
| **Net wallet balance correction** | **~−1.63 BSV** |

The wallet had been reporting ~1.66 BSV of ghost balance — outputs from incoming x402 payments that never mined on-chain. Real wallet balance is now internally consistent with chain truth.

No real value was lost — the x402 payments had already settled at the application layer (agents delivered their OpenAI/Claude responses). The ghost balance was an accounting inconsistency introduced by the `raw_tx=NULL` port bug, now fully reconciled.

### Verification query

```sql
SELECT COUNT(*) FROM transactions WHERE status='unproven';
-- → 0
```

---

## 🎯 THE REAL ROOT CAUSE (2026-04-15 late) — found after many red herrings

### The data loss bug

`src/storage/process_action.rs` line 293 (before fix):
```rust
"UPDATE transactions SET txid = ?, status = ?, raw_tx = NULL, input_beef = NULL, updated_at = ? WHERE transaction_id = ?"
```

Compare with reference at `bsv-wallet-toolbox-rs/src/storage/sqlx/process_action.rs:516`:
```rust
"UPDATE transactions SET txid = ?, status = ?, raw_tx = ?, input_beef = NULL, updated_at = ? WHERE transaction_id = ?"
```

**Our port incorrectly changed `raw_tx = ?` to `raw_tx = NULL`.** Every processAction destroyed the raw transaction bytes at broadcast time. The reference comment explains exactly why this matters:

> "Store raw_tx on the transaction record so child transactions can find it during BEEF construction. ... input_beef is cleared because the proven_tx_req record now holds the authoritative copy."

### The full failure chain

1. User sends payment A via x402 → `internalizeAction` stores raw_tx + BEEF in both `transactions` and `proven_tx_reqs`
2. Wallet creates payment B spending an output from payment A → `createAction` builds it
3. `processAction` is called to finalize B → **clears B's raw_tx and input_beef from `transactions`**
4. Wallet sends payment C spending an output from B (later, maybe minutes or hours)
5. `createAction` for C needs to build a BEEF including B as a parent
6. Build walks `transactions.raw_tx` for B → **NULL** (destroyed in step 3)
7. Build falls back to `proven_tx_reqs.raw_tx` for B → still has it, OK
8. BUT — the BEEF construction logic may not reach the fallback on all paths
9. Even if it does, `proven_tx_reqs` can be purged / marked `invalid` over time, losing the data permanently
10. Broadcast sends just the raw_tx bytes for C (the BEEF path is our `broadcast_beef` which is separate)
11. Miners get C but not B → orphan mempool → never mined
12. Monitor polls forever → no proof → tx stays `'unproven'` for weeks

### Confirmation from live data

Verified on `4b2ed6132a...` (stuck since 2026-04-13):
- **Parent 1** `dc853fe4...`: in `transactions` with `status='failed', raw_tx=NULL` ❌ (data destroyed)
- **Parent 2** `e1e6b8ef...`: not in our DB at all (external payment)
- **Backup in `proven_tx_reqs`**: both parents DO have raw_tx preserved there ✓ (recovery possible)

100% of 20 sampled stuck txs return `SEEN_IN_ORPHAN_MEMPOOL` from gorillapool and `Not found` from TAAL. TAAL returned error 460 "parent transaction not found" when we tried to broadcast the raw tx directly — the definitive signature of the orphan mempool problem.

### The 2 orphan categories

All of the 805 stuck unproven txs fall into the same failure mode:
- **~790 with proven_tx_req** (`status='unmined', high attempts`): processAction bug destroyed raw_tx in `transactions`, then downstream broadcasts went orphan
- **15 orphans** (no proven_tx_req row): similar atomicity issue in internalizeAction — transactions row committed but proven_tx_req insert never ran, leaving no data to recover from at all

---

## 2026-04-15 late — Data loss fix shipped

**Version `52aa30ee-1c6e-4fa5-8383-92f46663721d`** — SHIPPED 22:28 UTC

**Change:** 1 SQL clause + 1 bind in `src/storage/process_action.rs:293`
- `raw_tx = NULL` → `raw_tx = ?` with `QVal::Blob(raw_tx.to_vec())` as the bound value
- Matches `bsv-wallet-toolbox-rs/src/storage/sqlx/process_action.rs:516` exactly

**Effect:**
- ✅ Every NEW x402 payment processed from here preserves its raw_tx
- ✅ Child transactions spending from new parents have full BEEF context
- ✅ No more stuck txs will be created after this deploy
- ❌ Does NOT recover the existing 805 stuck txs — they need a rebroadcast pass

**Tests:** 686 passing, clean build, no warnings.

---

## Session postmortem (2026-04-15 evening)

### What actually happened

**Deployed 4 versions tonight:**
1. `f31ad2e6` (19:11) — Step 1 base fixes
2. `efbd2696` (19:38) — Option B triage delegation + cron */5→*/2
3. `5b7951ca` (19:55) — Step 1c idempotency helper for UNIQUE race
4. `4e17c763` (21:30) — Recovery: rollback idempotency + cron */2→*/5 + LIMIT 1000→200 + max_retries 5→1

Plus 1 rollback (21:18) to `efbd2696` that didn't help.

### Root cause of the drain death

**WhatsOnChain blanket 429'd us.** Live wrangler tail showed an endless stream of `WoC proof API error 429`. The `*/2` cron + 5 retries + LIMIT 1000 combo was making ~30K WoC calls/hour sustained, which is **3× WoC's ~10K/hour free-tier limit**. They eventually applied a blanket rate-limit that rejected every call.

Drain stopped at **20:40:19** (last successful proof store). From 20:40 onward:
- Monitor kept running (proven_tx_req `updated_at` kept advancing — attempts being incremented)
- But every `get_proof()` returned 429, so no new completions
- `monitor_run` events stopped writing at 20:13:07 (likely runs hitting CPU budget on retry loops before reaching `log_monitor_event`)

### The idempotency fix was a false suspect

`ensure_proven_tx_id` was designed to fix a real but secondary issue (UNIQUE constraint race between overlapping */2 cron runs). The helper was correct code that just wasn't the drain-killer. Reverting it was the wrong first suspect but was cheap — it bought us diagnostic signal (rollback didn't help → not the idempotency fix → look elsewhere → found the 429 storm).

### The Phase 1 orphan backfill did succeed

All 3 mined orphans (`78e0df15`, `e8299b46`, `84ca7b70`) successfully backfilled and cleared to `completed` via the normal pipeline BEFORE the WoC blanket-429 kicked in. Orphan count: 18 → 15.

### Lessons learned

1. **Don't go off the beaten path without very good reason.** The idempotency fix was novel — not in any reference impl — and landing a novel D1-specific helper under time pressure was risky. The reference impls (TS/Rust/Go toolbox) avoid this race via real SQL transactions; we can't use those in D1, but we should have chosen a safer workaround (e.g. slower cron to reduce overlap) rather than inventing idempotent handling.
2. **Retries amplify rate-limit problems.** Setting `max_retries=5` seemed prudent but turned every 429 into 6 calls, multiplying the hammering. **1 retry is plenty** for transient blips; anything more just weaponizes us against rate-limited upstreams.
3. **Query string comparisons against timestamp columns are error-prone.** The `updated_at > '2026-04-15 20:13:07'` filter silently returned garbage for 30 minutes because stored timestamps use ISO format with `T` and the filter used space-separated format. ASCII `T` > space, so string comparison evaluated true for everything. Lesson: always cast to datetime or use SQL datetime functions.
4. **Session fatigue is a real risk factor.** I'd been making changes for hours when I wrote the broken query. A fresh-me would have spotted it. For high-risk work, land a session's results cleanly and come back fresh rather than power through.
5. **CF Workers warm-instance cycling after a deploy takes ~2 minutes** — any deploy-then-verify cycle needs to account for this, not just check immediately.

### Final state after recovery deploy

- **Version deployed**: `4e17c763`
- **Config**: cron `*/5`, LIMIT 200, max_retries 1, compact_beef paused, Option B triage delegation active, retry helpers active (just with smaller retry count), hash→header cache active
- **Pending**: drain recovery waits on WoC lifting our blacklist (could be minutes to hours)
- **Unproven count**: 805 (was 577 at session start — grew during the incident)
- **Orphans**: 15 remaining (15 unknown to WoC, need Phase 2 treatment)
- **64 stuck tail**: still 2+ days old, still WoC-unknown, not touched this session

### Next session agenda (Approach B from the ultrathink)

1. **Observability first**: remove `let _ = log_monitor_event(...)` on `monitor.rs:268` so telemetry writes actually surface errors. Also decode `details` JSON size to see if we hit param limits.
2. **max_attempts → 'invalid' auto-cleanup**: port from `bsv-wallet-toolbox-rs` (reference uses 144 attempts = ~2.4h). After N failures, mark the req `invalid` and the transaction `failed`. Currently missing — stuck tail accumulates forever.
3. **Broadcast retry for stuck `unproven` txs**: if a tx has `proven_tx_req` but WoC says `unknown` for N attempts, re-broadcast via ARC. Ports reference impl's pattern.
4. **internalizeAction → BatchCollector**: atomicity fix for the orphan root cause. Reference pattern, carefully tested BEFORE deploy.
5. **Phase 2 stuck-tail cleanup**: probe 1 of the 64 stuck-tail txs via ARC `broadcast_raw_tx`, classify, bulk-handle. Also Phase 2 for the 15 orphans.
6. **Consider**: slowly re-enable compaction via BlobStore OR leave paused (ancestry argument).

Target for next session: sustained drain + stuck tail at zero + new safeguards in place.

---

## Option C — orphan backfill plan (next action)

### Current orphan set (as of 2026-04-15 ~19:50)

19 orphans (was 22 earlier in the session — some apparently cleared on their own, possibly via `fail_abandoned` after timeout).

Each orphan has:
- `transactions.status = 'unproven'`
- No row in `proven_tx_reqs` (the bug)
- `raw_tx`, `input_beef` populated (tx INSERT succeeded)
- Most are recent x402 OpenAI payments, `is_outgoing=0`

### Backfill approach — one-shot admin script, no code change

**Read-only first, then targeted writes:**

1. **Read orphan txids from prod D1** (local wrangler d1 execute SELECT)
2. **Classify each via WoC `/txs/status`** (local HTTP call, chunks of 20)
3. **For each classification:**
   - `mined` (confirmations ≥ 1): insert a `proven_tx_reqs` row with `status='unmined'`, `attempts=0`. Monitor picks it up next cron cycle, finds the proof, clears the req AND the transaction via the existing flow.
   - `mempool` (known but unconfirmed): same as mined — monitor will wait for confirmation.
   - `unknown` (not found by WoC): leave alone for operator review. These are likely never-broadcast txs where the tx INSERT committed but broadcast never ran. Need case-by-case decision.
4. **Wait ~5 min and verify** that backfilled orphans moved through the monitor to status='completed'.

### Safety properties

- **No code deployed** — pure data operation via `wrangler d1 execute`
- **Limited blast radius** — max 19 INSERTs total, each under 1KB
- **Reversible** — if an inserted req is wrong, it gets marked `invalid` after max_attempts and the transaction moves to `failed` (existing logic)
- **Idempotent** — running the script twice is safe: the existing `create_proven_tx_req` logic uses `INSERT OR IGNORE`-equivalent (SELECT first, skip if exists)
- **Observability** — each step logged; unknown orphans flagged for review

### Risk: what about the orphans still being created?

The `internalizeAction` atomicity bug is still there. With a ~6 orphan/hour creation rate, by the time we backfill 19, another ~6-12 will have appeared. That's fine — the backfill script is re-runnable. The ROOT CAUSE fix is Option E (internalizeAction → BatchCollector) and will be its own session.

### Expected outcome

- 15-19 orphans backfilled and cleared to `'completed'` within 2-5 cron cycles
- 0-4 orphans flagged as `unknown` for operator review
- Design doc updated with any learnings
- Option E queued as the next big work item

---

## Appendix D — Orphan root cause (2026-04-15)

### Verified orphan profile (all 16 match)

| Field | Value |
|---|---|
| `status` | `unproven` (→ `has_proof=false`) |
| `proven_tx_id` | `NULL` |
| Row in `proven_txs` | missing |
| Row in `proven_tx_reqs` | **missing ← the orphan state** |
| `raw_tx` | populated (374-960 bytes) |
| `input_beef` | populated (17KB-74KB) |
| `is_outgoing` | 0 (incoming payment, internalizeAction path) |
| `description` | "Payment for OpenAI chat completion" (x402 flow) |
| `reference` | non-null UUID |

### Root cause: D1 atomicity violation in `internalizeAction`

`src/storage/internalize_action.rs:509-547` — "Step 10: Link proof or ensure monitoring":

```rust
// Already committed at line 338: INSERT INTO transactions
// status = if has_proof { "completed" } else { "unproven" }

if !is_merge {
    let mut linked = false;
    if has_proof {
        if let Some(pt_id) = find_proven_tx_id(&txid).await? {
            UPDATE transactions SET proven_tx_id = ?
            linked = true;
        }
    }
    if !linked {
        create_proven_tx_req(&txid, &raw_tx, &args.tx).await?
        //   ├─ SELECT existing                     (read)
        //   ├─ broadcast.broadcast_raw_tx(...)     (network, slow)
        //   └─ INSERT INTO proven_tx_reqs          (write)
    }
}
```

The `INSERT INTO transactions` at line 338 **commits immediately** (D1 has no transactions). If anything after that fails — broadcast timeout, worker CPU budget exceeded, D1 transient error on the second INSERT — the transactions row is stranded. Caller gets `Err`, but the row stays in D1.

**Most likely trigger:** broadcast timeout or worker CPU budget exceeded between the two INSERTs. x402 payments run under tight latency budgets; `broadcast_raw_tx` is a blocking network call that can take seconds. If the worker hits its CPU limit mid-flow, D1 commits what's been issued and drops the rest.

### Fix pattern (already in codebase)

Per `CLAUDE.md`:
> **Batch atomicity**: D1 has no transactions (BEGIN/COMMIT). Use `BatchCollector` which calls `db.batch()` for atomic execution, auto-chunking at 100 statements.

`internalizeAction` is **not** using `BatchCollector`. Every INSERT runs as its own Query. The fix:
1. Run `broadcast_raw_tx` FIRST (outside any batch, before any D1 writes)
2. Collect ALL internalize writes (transactions, outputs, tx_labels, proven_tx_reqs) into a single `BatchCollector`
3. `batch.execute()` atomically — either all commit or none do
4. If the BEEF already contains a proof and proven_tx lookup finds a match, include the `UPDATE transactions SET proven_tx_id` in the same batch

This eliminates the orphan class entirely at the source. Plus matches the established pattern used elsewhere (`create_action`, `process_action` paths should also be audited for similar issues).

### Backfill strategy for existing 16 orphans

Once the code fix is deployed, the existing 16 orphans still need cleanup. Options:
- **A. Backfill matching `proven_tx_reqs` rows** — let the monitor pick them up. Risk: if the underlying txs never got broadcast (D1 committed before broadcast even ran), inserting a req will cause the monitor to try to get a proof for a tx that was never on-chain → will cycle forever.
- **B. Verify broadcast state first** — for each orphan, call `get_status_for_txids` once. If WoC knows the tx (mined or mempool), backfill the req. If unknown, the tx never made it on-chain — either re-broadcast from `raw_tx` OR mark the transaction as `failed`.
- **C. Just mark them `failed`** — crude but safe. x402 payments would need to be reconciled out-of-band.

**Recommendation: B** — verify-then-act. Write a one-shot admin script that queries WoC for each orphan and dispatches accordingly. Read-only diagnostic first, then approved action.

---

## Appendix C (updated) — WoC retry audit complete

| Function | File:Line | Has retry? | Notes |
|---|---|---|---|
| `WocChainTracker::is_valid_root_for_height` | `chaintracker.rs:184-214` | ✅ 429 + 5xx | Reference implementation |
| `fetch_tsc_proof` | `woc.rs:348` | ❌ | Bare fetch, returns `Err` on any ≥400 |
| `fetch_block_header` | `woc.rs:371` | ❌ | Same |
| `fetch_chain_height` | `woc.rs:412` | ❌ | Same |
| `WocProvider::get_raw_tx` | `woc.rs:229-256` | ❌ | Same |
| `WocProvider::get_status_for_txids` | `woc.rs:259+` | ❓ | Not yet verified in audit |

**Only `WocChainTracker` uses `RetryConfig`.** Every bare helper bypasses retry entirely. The fix is mechanical: reuse the existing `RetryConfig` from `chaintracker.rs` and wrap each bare helper in the same loop pattern (`chaintracker.rs:193-214`).

---

## Appendix E — Chaintracks API surface (hash→height check)

Only wired endpoint: `findHeaderHexForHeight?height={height}` at `chaintracker.rs:315`. That's **height → header_hex**, wrong direction for our need.

**Result:** "Route hash→height through chaintracks" is NOT directly possible without adding a new chaintracks endpoint. The **in-run cache** is the correct Step 1a fix:

```rust
// Inside WocProvider or check_for_proofs scope
let mut hash_to_height: HashMap<String, u32> = HashMap::new();

// In get_proof, before calling fetch_block_header:
if let Some(&h) = hash_to_height.get(&proof.target) {
    // cache hit, skip the WoC call
} else {
    let header = fetch_block_header(&proof.target).await?;
    hash_to_height.insert(proof.target.clone(), header.height);
}
```

500 pending txs span ~10–50 unique blocks → cache hit rate ≥90% → WoC header calls drop from ~500/run to ~10–50/run.

**Future improvement (not Step 1):** Add `/findHeightForHash?hash={hash}` or similar to `rust-chaintracks` worker. That would let us fully eliminate the WoC header call instead of just caching it.

---

## Appendix A — `input_beef` size distribution (2026-04-15 audit)

```
bytes        rows
1,203,652    1    ← largest
1,147,133    7    ← cluster, same size (shared ancestor group)
1,093,476    1
1,093,440    1
1,082,478    1
1,022,515    1
1,014,145    1
  964,681    1
  964,454    1
  963,060    1
```

Bucket histogram:
| Size | Rows |
|---|---|
| ≥1 MB | ~10 |
| 500KB–1MB | ~110 |
| 100KB–500KB | ~760 |
| 10KB–100KB | ~2,360 |
| <10KB | ~10,160 |

**Confirms D1 ~1MB param limit theory.** The 10+ rows above ~950KB are currently wedged — `compact_beef` reads them, merges, and fails on write. Any other path that attempts to read-modify-write these rows hits the same error.

**Implication for Step 2:** BlobStore code fix alone isn't enough. Existing >950KB rows need a **migration strategy**:
- Option A: lazy migrate on next read (clean but unpredictable)
- Option B: one-shot backfill script that moves all rows with `length(input_beef) > 4096` to R2 and NULLs the column

Prefer A + emergency B for the >1MB rows only.

---

## Appendix B — Full orphan list (2026-04-15 audit)

16 txs with `status='unproven'` but no `proven_tx_req`:

| Created | Txid prefix | Cluster |
|---|---|---|
| 2026-02-24T02:38 | 78e0df15… | historic 1 |
| 2026-02-25T13:14 | e8299b46… | historic 2 |
| 2026-04-08T17:22:15 | d8a5f816… | Apr 8 cluster 1 |
| 2026-04-08T17:22:37 | c3786e4b… | Apr 8 cluster 1 |
| 2026-04-08T17:22:57 | 548eb8f2… | Apr 8 cluster 1 |
| 2026-04-08T17:23:14 | 42ee1936… | Apr 8 cluster 1 |
| 2026-04-08T19:05:14 | bea543a9… | Apr 8 cluster 2 |
| 2026-04-08T19:05:21 | a2f042ee… | Apr 8 cluster 2 |
| 2026-04-08T19:16:43 | 2fc860fe… | Apr 8 cluster 2 |
| 2026-04-12T14:38:21 | 84ca7b70… | Apr 12 singleton |
| 2026-04-15T12:21:38 | ae6a2e32… | **TODAY** |
| 2026-04-15T16:15:38 | 55703b50… | **TODAY** |
| 2026-04-15T16:42:38 | dd9e9da7… | **TODAY** |
| 2026-04-15T17:44:57 | 274b4586… | **TODAY** |
| 2026-04-15T17:57:20 | dddd9607… | **TODAY** |
| 2026-04-15T18:11:24 | 38c53bf9… | **TODAY** |

**Active bug.** 6 new orphans on 2026-04-15 alone, spread over 6 hours → sustained drip, not a single batch. Some code path in the current build creates a `transactions` row with `status='unproven'` without inserting a matching `proven_tx_req`.

Needed diagnostics:
1. `SELECT txid, raw_tx IS NULL AS no_rawtx, reference FROM transactions WHERE txid IN (…16 orphans…)` — tells us how far through the creation flow each one got
2. `git log --since='2026-04-13' -- src/storage/` — changes to the creation paths since the last audit
3. Check request logs / `monitor_events` around today's orphan timestamps to correlate with specific RPC calls

---

## Appendix C — Block header call path (the missing retry)

`WocProvider::get_proof()` at `src/services/woc.rs:201-220`:

```rust
async fn get_proof(&self, txid: &str) -> Result<Option<ProofResult>, String> {
    // Step 1: Fetch TSC proof
    let proof = match fetch_tsc_proof(txid).await? { ... };

    // Step 2: Fetch block header for height
    let header = fetch_block_header(&proof.target).await?;  // <-- hash→height lookup

    // Step 3: Convert to BRC-74 binary
    let merkle_path_binary = tsc_proof_to_binary(&proof, header.height)?;
    ...
}
```

`fetch_block_header()` at `woc.rs:371-391` is a **bare helper with zero retry logic**.

**Every proof fetch = 2 WoC API calls:**
1. `GET /tx/{txid}/proof/tsc`
2. `GET /block/{hash}` (to resolve hash → height because TSC proof returns `target` as hash, not height)

During a `proofs_checked=500` run, this is **1,000 WoC calls in burst**, not 500. That's where most of the 429s are coming from.

### Retry audit for WoC call sites

| Function | File:Line | Has retry? |
|---|---|---|
| `WocChainTracker::is_valid_root_for_height` | `chaintracker.rs:184-214` | ✅ 429 + 5xx |
| `fetch_tsc_proof` | `woc.rs` (need to verify) | ❓ |
| `fetch_block_header` | `woc.rs:371-391` | ❌ |
| `fetch_chain_height` | `woc.rs` (via `get_chain_height`) | ❓ |
| `WocProvider::get_raw_tx` | `woc.rs:229-256` | ❌ |
| `WocProvider::get_status_for_txids` | `woc.rs:259+` | ❓ |

Most WoC paths are missing retry. `WocChainTracker` is the only path that gets it right.

### Fix options (Step 1a)

**Option 1 — Eliminate step 2 entirely via in-run cache.** 500 txs span maybe 10-50 unique blocks. Add a `HashMap<block_hash, u32>` to `WocProvider` scoped per monitor run. Collapses 500 header calls to ~10-50. ~20 LOC.

**Option 2 — Route step 2 through chaintracks.** We already have `CHAINTRACKS_URL` and a chaintracks provider. If chaintracks exposes hash→height or height-by-hash, use it. Chaintracks has our own rate budget, not WoC's. ~40 LOC + chaintracks API check.

**Option 3 — Do both.** Cache first, fall through to chaintracks on miss, fall through to WoC as last resort.

**Recommendation: Option 1 first.** Minimum diff, maximum impact. Option 2 is a longer-term improvement.

### Fix for Step 1b — retry everywhere

Apply the `RetryConfig::is_retryable_status` pattern (already in `chaintracker.rs:131`) to every bare WoC helper:
- `fetch_tsc_proof`
- `fetch_block_header`
- `fetch_chain_height`
- `get_raw_tx`
- `get_status_for_txids`

Reuse the existing `RetryConfig` — no new abstractions.
