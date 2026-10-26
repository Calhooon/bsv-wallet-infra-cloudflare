# HANDOFF: the captain of bsv-wallet-infra-cloudflare-public

The captain's record, kept current at every milestone. Re-orient from this file and from `cca runs --project bsv-wallet-infra-cloudflare-public --open` after any restart. The keeper of the stack is `captain-bsv-stack-lean` (message it with SendMessage); the division of captains binds: the crate's day-to-day is the captain's; the stack, the line, the pins, the Cloudflare layer's production and every outward act under the owner's name stay the keeper's; lanes never push, PR, deploy or open secrets; the keeper pushes, opens PRs, merges, tags, deploys. No network except reads. Secrets live in `~/vault/<project>.md`, read in place, never printed. `date -u` before every stamp.

## 2026-10-26 19:26Z: the owner's words of the evening; the god-tier programme filed; the fix merged and staged

**Landed on main since the entry below.** PR 35, the audit route behind the trigger key (`4e293c9`, six checks green 17:29Z); PR 36, this file (`05500df`). The production build of `4e293c9` is staged by the keeper, the deploy on the owner's word (tonight's brief, item (a)); the version id and the keyless 401 read come from the keeper. Local `main` equals `origin/main`; the fix worktree and the branches are removed.

**The owner's standing words of 2026-10-26 evening (recorded on the epic #21, the comment of 19:26Z).** (1) The plan is the god-tier north star, whole, no slicing: about 195 lane-days (150 here, 25 on the stack's side, 20 for WAB), eight to ten weeks of wall clock on eight to ten lanes, the beta soaking from week three; the plan rows are on #21. (2) Broadcasting: Arcade is the courier, our own node later when the Cloudflare node is done; ARC kept as the outage courier until then (the captain's reading, to be confirmed). (3) Chain scanners and explorers stay for break-glass only, WhatsOnChain, Bitails and Bananablocks as the fallbacks; where Bananablocks is read is the owner's to name. (4) WAB is in: its own repository and captain under the keeper; the custody reading (Twilio's terms and price, the data the reference keeps) and a one-page policy before the first SMS; the faucet off until the policy exists. (5) Like the rest, this repository gets to auto-research after the oracle, and the queue is ranked by god-tier infrastructure, architecture, design, solutions and speed.

**Filed today beyond the twelve readiness issues.** #37 (R13) the store's charter rows for this host: the busy word R8, the storage driver's server half, the Lean cited from here (the keeper placed the parts: the busy word ours, the driver's runner half the stack's H lane, the Lean the S lane's after the S4 to S7 reading; #26 holds on that reading). #38 to #43 (R14 to R19) the god-tier items: the tracker as the host (bsv-tracker), Arcade as the courier under the charter, push over poll (the chain-event feed), explorers break-glass only, the fleet scenario's server half, observability and the public face. #44 (R20) auto-research: `docs/NORTH-STAR.md` with the living scoreboard and the ranked research queue, the loop after the oracle. The ratchet baseline is on #16 (12 files over 800 lines, 43 functions over 60, 31 unwraps outside tests, 627 missing docs, about 270 pedantic warnings, 43 string matches on error text); the keeper's placement: one tool for the four crates from bsv-rs's `tools/ratchet`, added to the stack's template by the keeper, this repository's own `docs/ratchets.json` from the baseline, #15's fixtures before any refactor.

**Blocked, and on what.** The deploy of `4e293c9`: the owner's word. Every block's lanes: the owner's words per issue (the price of a call; the bindings' schemas; the spend of mainnet runs; Bananablocks' home; ARC's place confirmed). The keeper's items: the S4 to S7 reading (#26, #37), the ratchet in the template (#16), the chain-event feed's design (#40), the fleet scenario's runner (#42), the WAB repository and its captain.

**In flight.** No lane. The owner's overview page of the day is served on localhost from `/Volumes/Lanes/out/bsv-wallet-infra-cloudflare-public/readiness-2026-10-26.html` for the session; nothing of it is in the repository.

**The cadence.** One message an hour from the captain to the keeper, status and readings batched; a stop or a red on production immediate.

## 2026-10-26 17:08Z: the page merged; the audit route's fix reviewed MERGE and waiting on the keeper's push

**Landed.** PR 34 merged by rebase: the page is `docs/READINESS-2026-10-26.md` at `2869adc`, the issue numbers and this file at `bce393b` on `main`. Local `main` reset to `origin/main` at 16:5xZ; never pushed.

**The red on production, ruled by the keeper at 16:5xZ** (production of this Worker is the keeper's): `GET /monitor/audit` ran the audit and released locked outputs on an unauthenticated route (`src/lib.rs:356-373`; `src/audit.rs:175-257`). The fix is the one item of #22 that goes alone, on the keeper's word; the owner's "no code today" brief was for the readiness work and the keeper carries the ruling in tonight's brief.
- Branch `fix/audit-behind-key`, worktree `/Volumes/Lanes/targets/wallet-infra/fix-audit-behind-key`, one commit `65ce854eb5e43ff90503049adab83365f83c7c78` rebased onto `main` `bce393b`: the gate copied from `POST /monitor/run` before any binding, the README row, two Worker witness cases (three files, 30 insertions, 2 deletions). Code lane `fix-audit-behind-key` (openai-2), the gate green: fmt, clippy host and wasm32, 850 host tests, both Miniflare witnesses, scrub. Evidence `/Volumes/Lanes/out/bsv-wallet-infra-cloudflare-public/fix-audit-behind-key-20261026T165017Z/`.
- Cold review lane `fix-audit-behind-key-review` (claude-fable-5-1): VERDICT MERGE, no blocks; the witness proven by a reverted-gate build failing at the 401 assertion with 200. Evidence `/Volumes/Lanes/out/bsv-wallet-infra-cloudflare-public/review-fix-audit-behind-key-20261026/`.
- Two follow-ups, outside the diff by the keeper's "nothing else in it", noted on #22: `docs/E2E_TEST_HARNESS_DESIGN.md:150, 213` call the audit without a key and go stale at the merge; the outputs-unchanged assertion at `tests/worker_beef_at_rest.mjs:327` is inert on the fixture (nothing to repair) and a later witness plants a row locked by a failed transaction.
- Next: the keeper pushes the branch and opens the PR (base `main`); the captain sends the sha when the six checks are green; the keeper merges, deploys beta, then production on the owner's word.

**The cadence** (the owner's word of 16:3xZ on the pace): one message an hour from the captain to the keeper, status and readings batched; a stop or a red on production immediate; the keeper batches record landings on the half hour.

**In flight.** No lane. The worktree above holds the fix branch until the merge.

## 2026-10-26 16:5xZ: the readiness page landed; twelve gaps filed; nothing in flight

**The occasion.** Babbage's post of 2026-10-26 15:45Z: their hosted Wallet Storage server's price rises 40 times; others invited to run Toolbox Storage servers and compete on price. The owner's word of 16:1xZ opened this captain in this folder; the keeper's readings of 15:5xZ and 16:1xZ (`bsv-stack-lean` `docs/RULINGS-2026-10-12.md`) bind: the readiness half is the stack's, the business half is the plan program's, WAB is its own program.

**Landed on local `main`, not pushed (the keeper pushes).**
- `docs/READINESS-2026-10-26.md` (commit `59a8fd8`, then the issue numbers in the commit after it): the method table (45 reference methods at `ts-stack@edf6e03`; 14 served here, 4 tested at the door, 10 untested at the door, 31 missing), the door and the policies, the twelve ranked gaps with lane-days and the owner's word, the cost shape measured on production (read-only; the account's analytics API and one query of counts), WAB sized at 15 + 3 + 2 lane-days as its own program, the six tests (an option: no T1 or T2 failure; debits on T3, T5, T6), the inputs for the plan program.
- Issues #22 to #33 on the repository, one milestone each (5 to 16), label `readiness`; nothing closed.
- `main` was fast-forwarded from `1be4b6d` to `origin/main` `d0dba8d` at 16:17Z before the reading (the checkout had been seven commits behind).

**The headline numbers (from the page, section 4).** Production holds 402 wallets (67 with a transaction) in 759 MB of D1 and 2.95 GB of R2; every cost line is inside the Workers Paid quotas, so the marginal bill is $0 on the account's $5 plan; at the rack rate $2.70 a month, 52 sats a door call, 214 with the monitor's reads charged to calls (at $17.30 a BSV); the plan quota reached soonest is D1 rows read, the monitor's line, at about sixteen times today's load. The reference's TypeScript client at the pin refuses this server at `makeAvailable` on `chain = mainnet` and `dbtype = D1` (read, not run); the Rust toolbox's client is what the fleet runs.

**The smallest public offer.** Ranks 1 to 4 (#22 the door's policies, #23 wire compatibility, #24 the paid door, #25 the conformance witness) with the beta line of #14: 17 lane-days, about a week of wall clock on four lanes `[EST]`. Parity with the reference at the pin: 66 lane-days.

**Blocked, and on what.**
- The push of `main` and the Release of the page: the keeper.
- Every gap's start: the owner's words named per issue (a price, the public shapes, the bindings' schemas, the spend of a mainnet run); the stack's word on the merge rules (#26) and the transitions (#29, #30).
- Babbage's tariff before and after the rise: not published anywhere read today; the plan program's research page.

**In flight.** No lane. No worktree of this captain's. The lane volume's output folder for this project does not exist yet (`/Volumes/Lanes/out/bsv-wallet-infra-cloudflare-public/`).

**How the measurement was taken (for the next pass).** The analytics-read token in `~/vault/cloudflare.md` against the GraphQL datasets `workersInvocationsAdaptive` (by `scriptName`), `d1AnalyticsAdaptiveGroups` and `d1StorageAdaptiveGroups` (by `databaseId`), `r2StorageAdaptiveGroups` and `r2OperationsAdaptiveGroups` (by `bucketName`), `kvOperationsAdaptiveGroups` and `kvStorageAdaptiveGroups` (by `namespaceId`); the D1 info `GET` and the counts by read-only `SELECT`s through the D1 query API with the Worker's own deploy token (`~/vault/bsv-low.md` section 1). The production `wrangler.toml` with the real ids is the stack's infra copy under `~/bsv/infra/workers/wallet-infra/production/`. Shell note: the session shell is zsh, which does not word-split an unquoted variable in `for`.
