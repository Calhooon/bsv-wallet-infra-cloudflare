# HANDOFF — wallet-infra monitor false-FAILs on-chain transactions

**Repo:** `~/bsv/rust-wallet-infra` (our Rust→Cloudflare-Worker port; deployed,
DB `wallet-infra`, id `<your-d1-database-id>`).
**Reference:** `~/bsv/wallet-toolbox` (canonical TS wallet-toolbox — the
monitor/ProvenTxReq state machine is the semantics reference). `git pull` it
before comparing; it may be a stale clone.
**Deploy:** `source ~/bsv/rust-wallet-infra/secrets.md` for the CF token; never
`wrangler login`. D1 query cookbook + token: `~/bsv/rust-wallet-infra/D1-QUERIES.md`.

## The invariant that is being violated

`transactions.status = 'failed'` (and `proven_tx_reqs.status = 'invalid'`) MUST
mean, with 100% factual accuracy: **this transaction will provably never be
on-chain.** Today the monitor assigns `failed` to transactions that are simply
broadcast-but-not-yet-proven — which is a lie the moment the proof provider
lags mining (routine).

## Evidence (mainnet, observed twice)

- Once stranded **170,227 sat** out of the house books (recovered manually via
  `proven_tx_req → status 'unfail'`).
- A 2026-07-07 load test false-FAILed **13 pool outputs / 97,727 sat** whose
  txids ALL return HTTP 200 from WhatsOnChain `/tx/{txid}/hex` (they exist
  on-chain). Verify any of them:
  `29b735cc3a35d4da159b4fcceffe83b3a18f9cb330be18be72a077d663b6f364`,
  `d9c01939e81441f6118ca54c16dc254b78852668d1fb94b5fbe7761596da89e2`,
  `ea05f897eb2db9abeb8dfdc70583b0228f183c79f7080daeab237e926df05c7f`,
  `f107b5105e5818fdcf3b0ff62208f6ff146d7a993ca159d0201b47baaad89b2f`.

## Root cause (one line)

Our monitor fails a `proven_tx_req` on **attempt-budget exhaustion counted in
wall-clock cron cycles** (`MAX_PROOF_ATTEMPTS = 12`, one per ~5-min tick ≈ 60
min), with **no chain-truth check**. The reference only ever fails on a
**positive never-on-chain signal**, and its one attempt-based path is gated on
**~144 new blocks** of confirmed on-chain absence (block-clocked, not
wall-clocked). TAAL's proof feed has been observed lagging mining by 7+ blocks
(~70+ min > 60 min) → deterministic false-fail during normal provider lag.

## The false-FAIL code paths (OURS — `src/`)

- **`monitor.rs:41`** `MAX_PROOF_ATTEMPTS = 12`.
- **`monitor.rs:647-657`** — SQL sweep: `UPDATE proven_tx_reqs SET status='invalid'
  WHERE status IN (unmined,…) AND attempts >= 12`. Fails on attempt count alone,
  no chain query. **Primary culprit.**
- **`monitor.rs:918-945`** (`increment_attempts`) — `attempts+1 > 12 → invalid`.
  Called from `monitor.rs:774` (`get_proof → Ok(None)`) and `monitor.rs:748-753`
  (batch triage says txid not `"mined"` — but this bucket INCLUDES `"known"` =
  mempool/SEEN, which is thrown away). **Primary culprit.**
- **`services/woc.rs:281-291`** — `get_proof` returns `Ok(None)` for a
  SEEN-but-unproven tx, **indistinguishable** from a never-existed tx. This
  conflation is what feeds the attempt-based fails.
- **`monitor.rs:1110-1157`** (`review_status`) — propagates req `invalid` →
  `transactions.status='failed'` and releases the locked UTXOs. (Not a source,
  but where the false-fail becomes the observable books mutation.)
- **`monitor.rs:2130-2146`** — reorg demote to `unmined` is CORRECT, but a
  demoted req re-enters the attempt sweep (second door to the same bug).
- **Correct today (keep):** `send_waiting` fails only on ARC `DoubleSpend` /
  `InvalidTx` (`monitor.rs:544-596`) — positive signals.
- **Band-aid recovery (make rare-to-never):** auto-unfail
  (`monitor.rs:1438-1496`) caps at `attempts < 60` (~47h) and re-uses the same
  lagging `get_proof` → a long provider outage makes the false-fail permanent.

## The reference's correct state machine (`~/bsv/wallet-toolbox/src/`)

Statuses: `sdk/types.ts:54-67`; `invalid` doc (`types.ts:48`): "…rejected by
the network. Will never be re-attempted." — reserved for provably-dead.

| Transition | Predicate | Cite |
|---|---|---|
| →`doubleSpend` | ARC DS **AND** `confirmDoubleSpend` re-polls `getStatusForTxids` 3× still network-`unknown` | `attemptToPostReqsToNetwork.ts:161-164,298-336` |
| →`invalid` | ARC hard-reject `invalidTx` (statusErrorCount>0, no success, no DS) | `attemptToPostReqsToNetwork.ts:236-238` |
| →`invalid` | ARC SSE `REJECTED` | `TaskArcSSE.ts:166-169` |
| →`invalid` | rawTx doesn't hash to txid (corruption) | `TaskCheckForProofs.ts:145-152` |
| →`invalid` | `attempts > unprovenAttemptsLimit` (**144 main** / 10 test) | `TaskCheckForProofs.ts:154-165`; `Monitor.ts:104-106` |
| net error | **NOT a fail** — re-queue `sending`, attempts++ | `attemptToPostReqsToNetwork.ts:240-243` |

Critical: `attempts` increments **only on a new-block event**
(`countsAsAttempt = checkNow`, fired by the Chaintracks new-header event —
`TaskCheckForProofs.ts:37-38,50,193,229`). So 144 attempts ≈ 144 blocks ≈ ~24h
of *confirmed on-chain absence*, not 60 min. `nosend` reqs are exempt and retry
forever (`types.ts:22`). Abandonment (`TaskFailAbandoned.ts:37-46`) can only
touch `unsigned`/`unprocessed` drafts — never a broadcast tx. Self-correction
is chain-keyed and automatic (`TaskUnFail.ts:65-84` via `getMerklePath`;
`TaskReviewDoubleSpends.ts:93-109` via `getStatusForTxids`). Spendability keeps
`unproven` outputs spendable while awaiting proof (`StorageProvider.ts:183-198`).

## The divergences (this is the bug)

1. **Attempt clock is wall-clock, not block-height** (12 cron ticks ≈ 60 min vs
   144 blocks ≈ 24h). ~24× faster AND decoupled from chain progress — we fail a
   tx during a provider stall even though zero blocks passed without it. The
   comment at `monitor.rs:28-41` cites the reference's 10/144 limits but
   silently changed the *unit*.
2. **No chain-truth gate before failing.** We already fetch `"known"` (SEEN) in
   triage (`monitor.rs:702-703`) and throw it away. The primitives exist unused:
   `services/mod.rs:120,169-181` (`get_status_for_txids` → `mined`/`known`/
   `unknown`) and `services/mod.rs:188-194` (`get_spent_status`).
3. **`Ok(None)` conflates SEEN-but-unproven with never-existed** (`woc.rs:281-291`).
4. **Self-correction bounded + provider-coupled** (`monitor.rs:1446-1496`).
5. **No authoritative status feed** (reference has `TaskArcSSE`; we have none
   post-broadcast).

## God-tier fix (design — verify against the code before building)

**Enforced invariant:** set `invalid`/`doubleSpend`/`failed` ONLY on a positive
never-on-chain signal — (a) ARC hard-reject at/after broadcast; (b) a *confirmed*
double-spend (an input spent by a **different** network-known/mined tx); (c) a
reorg orphaning the tx with a conflicting spend. **Never** on attempt-budget or
`get_proof` timeout.

- **Delete the two attempt-based fail predicates** (`monitor.rs:647-657` and the
  `invalid` branch of `increment_attempts` 918-945). Keep `attempts` as a
  pure backoff/priority counter, never a fail trigger.
- **Add `chain_truth_check(txid)`** consulted before ANY fail, using the
  existing primitives: `get_status_for_txids` → `mined`⇒fetch proof⇒`completed`;
  `known`(SEEN)⇒stay `unmined`, re-check, **never fail**; `unknown`⇒check each
  input via `get_spent_status`: input spent by a *different* tx ⇒ `doubleSpend`;
  inputs still unspent ⇒ **re-broadcast** (via `send_waiting`), only fail if ARC
  then returns `InvalidTx`/`DoubleSpend`.
- **Block-height gate:** count "attempts" only when the chaintracks tip advances
  (re-derive the reference's `countsAsAttempt = new-block`); keep a ~144-block
  ceiling as an escalation/alert trigger, not a standalone fail. Optional
  additive nullable column `proven_tx_reqs.first_seen_height` (schema is
  free-form TEXT, no CHECK constraints — `migrations/0001_initial.sql:29,113` —
  so no status migration needed).
- **Use our own `rust-chaintracks`** (`services/chaintracker.rs`
  `ChainTracksProvider`, `CHAINTRACKS_URL`) for tip + merkle-root truth; WoC/TAAL
  only as proof-bytes fallback gated behind the chaintracks tip.
- **Make auto-unfail unbounded-while-visible** — drop the `attempts < 60` cap
  (`monitor.rs:1451`); a tx ARC/chaintracks still reports `known`/`mined` must
  never stop being re-checked. Keep `store_unfail_proof` as the idempotent
  restore.
- **Keep** `send_waiting` DS/InvalidTx fails and the reorg demote-to-`unmined`.

## Proof plan (make `failed` 100% factual)

- **Unit (mock `ProofService`):** (1) `get_proof→Ok(None)` for 1000 cycles while
  `get_status_for_txids→"known"` ⇒ stays `unmined`, outputs spendable, NEVER
  `failed` (reproduces the live bug; green only after the fix). (2) `"unknown"` +
  input `Spent{other}` ⇒ `doubleSpend`. (3) `"unknown"` + inputs `Unspent` ⇒
  re-broadcast, not fail. (4) proof arrives late ⇒ `completed` + re-spendable.
  (5) tip held constant across many ticks ⇒ attempts don't advance, no fail.
  (6) `store_unfail_proof` idempotent/reorg-safe.
- **Mainnet-lag sim:** replay the 4 evidence txids with a shim returning
  `Ok(None)` from `get_proof` but `"known"` from `get_status_for_txids`; assert
  none reach `failed`, then flip to `Ok(Some)` and assert `completed`.
- **Production canary (add + keep):** periodic invariant — for every
  `transactions.status='failed'`, assert WoC `/tx/{txid}/hex` == 404 (or
  chaintracks `unknown` AND a conflicting input-spend). Any 200 = a false-fail;
  alert. Would have caught both incidents.

## One-time reconciliation

Run the canary once over existing `failed`/`invalid` rows; sweep any txid that
returns 200 on-chain to `unfail` for restore (existing `store_unfail_proof`
handles it). Cleans the current 13 false-fails + any 170k-incident residue.

## Risks / notes

- No status-schema migration (free-form TEXT). `first_seen_height` is additive/
  nullable/backfillable from `chain_height` events.
- Slower failure of genuinely-dead txs is the correct trade (money-safety
  strictly improves); truly-dead txs still fail via ARC re-broadcast verdict.
- `get_spent_status` default impl returns `Unsupported` (`services/mod.rs:188-194`);
  ensure the WoC `/tx/{txid}/{vout}/spent` override is active, and where
  `Unsupported`, fall back to re-broadcast+ARC-verdict — never treat
  `Unsupported`/`Unspent` as license to fail.
- This layer holds ALL app money on the stack (blackjack + ~11 other agents);
  the blackjack side currently *compensates* via its own health-harness +
  §16i unfail path, but the correct fix is here.

---

## BROADER MANDATE — audit both services against their references, fix everything

The false-FAIL above is the KNOWN critical bug and the priority, but it is a
symptom of a class: **our Rust ports diverging from the canonical
reference-implementation semantics in ways that break the "status/state is
100% factual" guarantee.** While in here, do a correctness audit of BOTH
services and fix every divergence you can justify, not just the one bug.

### Scope 1 — `~/bsv/rust-wallet-infra` vs `~/bsv/wallet-toolbox`
Beyond the monitor: audit the full ProvenTxReq/transaction lifecycle, the
storage/UTXO spendability rules, `internalizeAction`/`createAction` status
handling, reservation/lock semantics, reorg handling, and BEEF validation —
against the TS wallet-toolbox. Any place where a status, a balance, a
spendable flag, or a lock is set on something other than a factual on-chain /
protocol signal is a bug of the same family. (Note the owner rule: our Rust
ports MAY legitimately EXCEED the reference — e.g. the TTL'd `reserveOutputs`
lease has no TS equivalent — so match SEMANTICS where sound, and where we
deliberately exceed it, document the comparison. Don't "fix" a deliberate
improvement back down to the reference.)

### Scope 2 — `~/bsv/rust-chaintracks` vs its reference
`rust-chaintracks` is OUR tip/header service and is the proposed proof/tip
source for the wallet-infra fix (4c/4d above) — so its correctness is
load-bearing for the wallet-infra fix. Audit it against its reference:
`~/bsv/bsv-wallet-toolbox-rs/src/chaintracks/` (the Rust chaintracks logic;
storage blueprints `storage/sqlite.rs`, `storage/memory.rs`) — see
`~/bsv/rust-chaintracks/CLAUDE.md:68-70,100,113` for the mapping and its
existing vector-parity tests. Focus where a wrong answer would MISLEAD the
wallet-infra monitor: tip-height accuracy, reorg/header-reorg handling,
`isValidRootForHeight` / merkle-root verification, and any status/height that
could be reported stale or optimistic. If chaintracks can ever report a tip or
a root that isn't factual, the wallet-infra fix inherits that lie. Also check
`chaintracks-server` / `chaintracks-cloudflare` if they carry reference
semantics rust-chaintracks should match.

### Rules for the audit
- `git pull` every reference clone before comparing (they may be stale).
- Every fix must be justified by a factual/reference divergence, not taste.
- Preserve deliberate improvements over the reference; document them.
- Verify-before-ship: unit tests reproducing each divergence (red→green), and
  where a service is chain-facing, a mainnet-lag / reorg simulation.
- Deploy discipline: `source secrets.md` for the CF token, never
  `wrangler login`. Same CF account `ea3e6d17…`. Assert real visibility after
  deploy; do not trust a relay's "success".
