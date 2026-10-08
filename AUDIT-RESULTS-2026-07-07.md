# Correctness hardening — rust-wallet-infra + rust-chaintracks vs TS/Go references

**Date:** 2026-07-07/08 · **Scope:** HANDOFF-MONITOR-FALSE-FAIL.md (P1) + full-lifecycle audits (P2/P3) + 5-agent verification swarm
**References (all `git pull`ed 2026-07-07):** `~/bsv/wallet-toolbox` (TS, v2.1.24), `~/bsv/go-wallet-toolbox`, `~/bsv/wallet-infra` (TS service), `~/bsv/chaintracks-server` (deployed TS, toolbox 1.6.26), `~/bsv/ts-sdk`, `~/bsv/go-sdk`.
**Commits:** wallet-infra `9059fea..40cd93c` (red→green + 4 fix batches) · rust-chaintracks `3a19c0e..18ed730`.
**Deployed:** rust-chaintracks version `0801ec22…`, wallet-infra version `115393df…` (2026-07-08 UTC).

## THE INVARIANT (now enforced end to end)

`transactions.status='failed'` / `proven_tx_reqs.status IN ('invalid','doubleSpend')` are reachable **only** on a positive never-on-chain signal:
1. ARC hard-reject (DoubleSpend/InvalidTx) at/after broadcast — **chain-truth gated**: if the network reports the txid known/mined, the reject is disregarded (stale-cache "missing inputs" on an old mined tx is spent-by-ITSELF, not a double spend);
2. a confirmed input double-spend: an input consumed by a **different** txid **and** that spending txid is itself network-known;
3. rawTx-doesn't-hash-to-txid corruption (TS `TaskCheckForProofs.ts:139-151` parity);
4. user-initiated abort of a never-broadcast draft.

**Never** on attempt budgets, proof-provider lag, timeouts, or client claims. Self-correction (auto-unfail canary) is unbounded-while-visible and covers `invalid` **and** `doubleSpend`.

## P1 — the false-FAIL bug (mainnet-proven twice: 170,227 + 97,727 sat)

| Divergence (ours, pre-fix) | Reference semantics | Fix |
|---|---|---|
| `attempts>=12` SQL sweep → `invalid` (monitor.rs:647-657), wall-clocked 5-min cron ticks ≈ 60 min, no chain check | TS: attempts tick only on new-block events (`TaskCheckForProofs.ts:50,229`; `Monitor.ts:397-403`), limit 144 blocks (`Monitor.ts:106`). Go: whole sync skipped unless tip moved (`synchronize_tx_statuses.go` lastBlockKey); exhaustion → **rebroadcast**, `unsent`, attempts=0 (`known_tx.go` proofTimeoutUpdates) — never straight to invalid for a broadcast tx. (The old comment "Go default is 10" was wrong — it's **100**.) | Both attempt-based fail predicates **deleted** (sweep + `increment_attempts` invalid branch). Attempts are a block-clocked observability counter (`monitor_events('proof_attempt_height')`, tip from chaintracks-first `MultiProvider::get_chain_height`, WoC fallback, frozen when unavailable/0/regressing). 144 = alert only. |
| `"known"` (SEEN) triage discarded; SEEN txs counted toward failure | Go never counts attempts for mempool txs (depth filter) | SEEN ⇒ wait, never escalate, never fail, counter does not tick |
| network-`unknown` reqs waited forever or died at 12 ticks | Go: rebroadcast; TS: ARC-verdict + 3× re-poll before DS | after 3 block-clocked unknown attempts: re-poll status → per-input `get_spent_status` → conflicting spend by a network-known tx ⇒ `doubleSpend`; nothing conflicting ⇒ requeue `'unsent'` for ARC verdict; incomplete evidence ⇒ hold |
| auto-unfail capped `attempts<60`, proof-provider-coupled | TS TaskUnFail + TaskReviewDoubleSpends are chain-keyed | unbounded; covers doubleSpend; promotes SEEN/mined false-fails to `unmined/unproven` even before a proof exists; re-marks recovered inputs spent (TaskUnFail.ts:118-129); isUtxo re-check per restored output (TaskUnFail.ts:131-145); every catch logged to `monitor_events('false_fail_canary')` |

**Red→green proof:** commit `9059fea` encodes the reference semantics as 6 tests that FAIL against the legacy logic (mainnet-lag sim: 1000 cycles SEEN-but-unproven, constant tip ⇒ zero attempts, never invalid; exhaustive no-attempt-based-invalid sweep over all triage states × 2000 attempt values). Green from `b21eb31`. 761 tests total.

## P2 — full-lifecycle fixes (wallet-infra vs TS+Go)

- **C3** `send_waiting` posted the **ancestors-only** BEEF — the subject tx never reached the network while every status advanced (vs `mergeReqToBeefToShareExternally`). Now merges `raw_tx`, with **R2 fallback** for >4KB `input_beef` (the common case).
- **C2** abort left the req broadcastable after releasing inputs → req now `'invalid'` in the same batch (`StorageProvider.ts:279-286`).
- **C1** a network-rejected internalize stayed credited forever (writes precede broadcast; D1 has no rollback; reference is transactional, `internalizeAction.ts:417-437`) → chain-truth-gated compensation + an `'invalid'` req row so the canary can reverse a wrong verdict; **proof-verified payments are no longer re-broadcast at all**.
- **M1** DS/InvalidTx books: released the failed tx's *created* outputs (phantom money) and stranded its *input* locks → now releases inputs (`IN`-subquery — `transactions.txid` is not unique across users), de-recognizes created outputs (Go `RecreateSpentOutputs`/`MarkCreatedOutputsAsNotSpendable`). Same guards added to `review_status`, which now also propagates `doubleSpend` reqs.
- **M2** delayed sends commit as `'sending'` (TS `processAction.ts:177-186`), **without** inline broadcast; `fail_abandoned` widened to 30 min + live-req NOT-EXISTS guard; delayed-queued txs remain abortable (req still `unsent`).
- **M3** reorg proof-miss demotes only the req (attempts=0, Go `known_tx.go:685-695`) — no tx demotion, no spendable clamp (which permanently stranded basket-insertion outputs).
- **M5** strict BEEF verification + BUMPs + no header provider = hard error, never silent structural-only downgrade.
- **M6** sendWith companions actually enter the broadcast queue (`nosend`→`unsent`, tenant-scoped).
- minors: balance excludes `nosend` (TS specOpWalletBalance); `updateTransactionStatusAfterBroadcast` chain-truth-gated in **both** directions; internalize merge can't resurrect consumed/relinquished outputs.

## P3 — rust-chaintracks vs TS chaintracks/chaintracks-server

- **C1** `/isValidRootForHeight` tri-state: no active header at height ⇒ 404 "unable to verify" (Go BHS INVALID vs UNABLE_TO_VERIFY), root mismatch ⇒ `false`. (wallet-infra consumers already fall back to WoC on error.)
- **C2** equal-height reorg wedge: `bestblockhash` compare at equal height + by-hash competitor fetch (**WoC path fixed — `/block/{hash}/header`**; the first version used a non-existent path and was dead on arrival — caught by the swarm) + bounded (36) parent backfill + orphan relink/re-evaluation; dupe-orphan rows resume backfill (crash-window wedge closed).
- **C3** dual-active heights: inserts are active only when extending the tip; reorg winners insert **inactive** and flags flip only after the walk succeeds (a *refused* reorg now truly leaves the tip untouched); deterministic `ORDER BY header_id DESC` readers; self-heal sweep after catch-up **and** admin bulk paths.
- **C4** `/currentHeight` 503s when no tip row (was success:0); tip flips are single-batch transactional; reorg deactivate+activate batched (crash mid-walk no longer leaves permanent inactive holes).
- **M1/M2** exact 256-bit chain work (`2^256/(target+1)`, vector: genesis `0x100010001`; verified against a bigint harness on 10k random cases) with **cumulative** accumulation in both live and batch inserts, more-work tip selection (`isMoreWork` parity), malformed bits ⇒ zero work (can never win), and a per-cron `repair_cumulative_work(144)` pass that heals legacy/bulk non-cumulative work across the fork-relevant window.
- **M3** ingest hash integrity: recomputed from the 80 bytes, mismatch rejected (`validateHeaderFormat` parity); badPrev height check (`ChaintracksStorageKnex.ts:276-279`).
- **M4** linkage guards: upstream batches anchor batch[0] to our stored parent and truncate at breaks; bulk CDN files likewise; R2 export refuses misaligned files (start-anchor + hole guard) and no longer truncates at 10k (public route keeps the cap).
- Wire: `/getPresentHeight` added (WoC-primary, tip fallback — the toolbox-rs client and rust-overlay probe it and hard-error on 404); `wrap_error` codes track HTTP status; `/getInfo` exposes `lastSyncedAt`/`lastSyncedHeight` freshness.

## Deliberate over-reference behaviors (preserved, do NOT "fix" back)

1. TTL'd `reserveOutputs` lease (G1/G4) — no TS/Go equivalent; audited sound (atomic claim, no resurrection paths, expiry-only release).
2. G5 external-spend scan — continuous outpoint sweep; TS has only manual one-shot analogs.
3. Unbounded auto-unfail canary + `false_fail_canary` events + SEEN-promotion (SEEN=final owner rule) — strictly stronger self-correction than TS/Go.
4. Escalation-with-input-evidence — positive DS confirmation for network-unknown txs at ~3 blocks (Go waits 100 attempts and never DS-fails from polling; TS needs an ARC verdict) with a double positive-signal requirement.
5. Block-clock freezes on tip-unavailable/zero/regression — stricter than both references.
6. `review_status` fail-sync excludes `completed` txs (TS's raw `whereNot('failed')` could fail a completed tx off a stale req).
7. Reorg demote-only + hourly shallow proof sweep; purge never touches spendable.
8. chaintracks: loud no-ancestor reorg refusal; `canonicalize_heights` + `/admin/backfill` repair tools; server-side root-check endpoint; malformed-bits ⇒ zero work (TS computes ~2^256 — inverted danger).
9. `INTERNALIZE_ZERO_CONF=true` env lever (deployed) — deliberate 0-conf credit for operator-funded deposits; ServiceError-only path, hard rejects still reject.

## Reconciliation (live books)

Pre-deploy manual canary over all 105 `failed` transactions: **20 mined on-chain** (false-fails: the 13 known + 7 more, incl. all 4 handoff evidence txids), 85 genuinely absent, **200,454 sat** stranded across 9 outputs. Post-deploy the auto-unfail sweep recovers all 20 (LIMIT 20/sweep, hourly backoff → daily after 24 checks); each catch is recorded in `monitor_events('false_fail_canary')`. Verification queries: see the checklist in this file's companion section of D1-QUERIES.md usage (post-deploy checks B–E from the swarm critic).

## Known-open (documented, not blocking)

- `/admin/*` on rust-chaintracks is **unauthenticated** (pre-existing; biggest operational exposure — recommend a token or CF Access rule).
- No cron-overlap lease on chaintracks (mitigated by deterministic reads + transactional tip flips).
- No PoW target validation — parity (TS `validateHeaderDifficulty` is dead code there too).
- `begin/commit/rollbackStorageTransaction` RPCs are success-returning no-ops (documented; erroring would break toolbox-derived clients).
- `relinquishOutput` is a one-way door (deliberate G5 semantics; documented on the RPC).
- rust-overlay fail-open vs fail-closed on fresh-block BEEFs if repointed at rust-chaintracks — **owner decision pending**.
- chaintracks >36-deep reorg needs operator intervention (TS re-runs bulk sync; theoretical on mainnet).
- Cross-tenant `spent_by` at 10× multi-wallet load: `remark_inputs_spent` is now user-scoped and link-preserving; the wallet-infra D1 verdict at 10× (HANDOFF-PRODUCTION §0c) should re-check this area.

---

## Addendum 2026-07-08 — ts-stack (wallet-toolbox 2.4.0) follow-up

`~/bsv/ts-stack` (github.com/bsv-blockchain/ts-stack) is now the CANONICAL
upstream — the standalone wallet-toolbox and ts-sdk repos are archived
redirects. Its wallet-toolbox 2.4.0 independently deleted the same
attempt-based invalid predicate we removed (commit fc34338d5: proof timeout
→ rebroadcast, wasBroadcast/rebroadcastAttempts) — upstream converged on THE
INVARIANT.

Adopted (deployed wallet-infra 01bb25af…): nosend lifecycle advance on
internalize (funds-invisible bug), 'sending' merge target, parent-tx-failed
guard on both input-release sites (chained-failure phantom, G5-blind),
pre-abort chain gate. Plus in rust-chaintracks: ADMIN_TOKEN gate on /admin/*
(fail-closed) and the 6-block verified read-through grace for fresh blocks
(proven live at height 956898).

GHSA-36f9-7rg5-cpf8 / CVE-2026-56744 (malicious storage substitutes output
scripts; client signs verbatim): rust-wallet-infra is the STORAGE side —
not vulnerable. The vulnerable CLIENT pattern was in
~/bsv/rust-wallet-toolbox::build_unsigned_transaction — fixed (20e23a7,
stricter than upstream: byte-identical echo, always-re-derive change, NO
commission allowance) with 6 regression tests. NOTE: consumers
(~/bsv/agents/*) pick the fix up on their next rebuild/redeploy.

Deferred (tracked): inline chain-verified input eviction at fail sites and
internalize-time input-spend marking (G5 sweep covers both within ~1h);
conformance-vector wiring into bsv-rs (script-eval 5,116 vectors + BUMP/BEEF
suites — no vectors exist upstream for chain-work/reorg/monitor semantics,
so our red→green tests remain the only guard there); /chaintracks/v2 wire
shim (only needed if overlay-express-style v2 clients are ever pointed at
rust-chaintracks; our legacy surface remains byte-equivalent to the 2.4.0
default client).
