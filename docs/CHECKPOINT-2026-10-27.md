# Checkpoint 2026-10-27: where the wallet storage server stands against the north star

Written 2026-10-27 16:41Z by the captain of `bsv-wallet-infra-cloudflare`, on the owner's ask of 16:0xZ ("a clean checkpoint... where we are overall, progress, issue and milestone hygiene, if we are going to the north star"). The same shape is kept at every checkpoint so the numbers compare. Labels as on `docs/READINESS-2026-10-26.md`: `[SRC]` read at a pin, `[X]` measured, `[EST]` an estimate. Main at `0bfbf2a`. Everything of the day is landed and deployed; nothing is in flight.

## 1. The state in one table

| measure | now | the north star's line |
|---|---|---|
| reference methods served at parity (`ts-stack@edf6e03`, 45 methods) | 14 of 45 `[SRC]` (`docs/READINESS-2026-10-26.md` section 1) | 45, waves W26 to W28 (#21) |
| the readiness page's ranked gaps closed | 1 of 12: the door's policies (#22, PR 57, in production 15:08:01Z) | 12 |
| design notes of record on main | 3: the store (#28, #30), the courier and the switch (#39, #41), the paid door (#24) | one per block before its code |
| the oracle's evidence | 57 route fixtures replayed in CI's worker job (#15, PR 53); four Miniflare witnesses in CI (bytes at rest, the door, the replay, the proof hold) | the scoreboard measured in CI (#16, #19) |
| production deploys today `[X]` | 4: `4e293c9` at 12:29:24Z (version `45bfb18e`), `5ffe866` at 15:08:01Z (version `a0862112`), `9664616` at 16:21:09Z (version `e948008d`), `0bfbf2a` at 16:31:49Z (version `e8a79ba3`); each on the owner's word with the proof read | each on the owner's word with the proof read |
| the beta environment | exists since 14:5xZ behind its own domain, its own data and bindings; `5ffe866` at 15:02:18Z, `9664616` at 15:48Z, `0bfbf2a` at 16:30:16Z | the `beta` branch, CI on both lines, the tag rule (#14's rest) |
| lane-days spent `[EST]` | about 9 (14 code and design lanes, about 18 cold reviews) | about 190 here (#21's rows) |
| milestones | 24; 33 open issues, every one in a milestone except the epic; 8 closed | one issue per gap or god-tier item, each with its exit test |
| the shared crates | `Calhooon/bsv-courier` (3765dda) and `Calhooon/bsv-wallet-store` (9536364) created from the template on the owner's yes; WAB its own repository and captain | this repository shrinks to the door and the glue |
| the Lean statements for the store | not started (the keeper's, after the store's schema brief) | S4 to S9 as theorems over the per-wallet object |
| the conformance suite's identity | funded, 1,000,000 sats in the CLI seat, the key in the vault (#25) | the mainnet suite weekly against the beta, 50,000 sats a run |

## 2. What landed today (PRs, with the shas as rebased)

| PR | what | main |
|---|---|---|
| 46 | the readiness decisions page and HANDOFF | `fd252a8` |
| 48 | the store note | `a331e68` |
| 51 | the courier note | `2f8d544` |
| 52 | the paid-door note | `b05e607` |
| 53 | the route fixtures | `091ef23` |
| 57 | the door's policies, 18 commits | `5ffe866` |
| 58 | HANDOFF for the wave, the decisions addendum | `d2095b9` |
| 60 | the finding #59's docs correction | `b8b7ecd` |
| 63 | the proof hold (#49), 3 commits | `9664616` |
| 64 | the fee refusal as a hint (#50), 9 commits | `0bfbf2a` |

Nothing is in flight at this stamp. Both quick fixes are in production on the owner's word of 16:0xZ ("yeah fixes can go to production, especially if it dont break anyting"), each after its beta reads and with the proof read: #49's hold row readable as `select count(*) from monitor_events where event = 'proof_held'` (0 while the header service answers); #50's failed-transaction count 26,457 before and 26,458 after one monitor run, the one new row read on production and found unrelated (an overlay's SHIP/SLAP advertisement failing at creation every fifteen minutes, 0 satoshis, no txid, nothing reaching a courier; before and after the fix alike). Every lane worktree and branch is removed; local `main` equals `origin/main`.

## 3. The owner's words of the day, all given

Full parity (all 45 methods; ranks 1 to 4 the beta offer); WAB at full parity under its own captain; Arcade for everything, ARC deleted; the price (100 sats a call prepaid through the standard 402, 10,000-sat top-ups, 1,000 free calls per new identity); the schemas (the crate's session object, one SQLite Durable Object per wallet, the global D1, R2 for ancestry, blobs, parts and packs); the spend (one funded identity, 50,000 sats a run, the ledger, the beta only); Bananablocks' home (the overlay's reader, one courier crate); the two crates' names; money as the rail (the bindings for the unpaid paths in production and beta, the zone rule the address limit of record, the surge quote #54, the budget switch #55); the agents captain patches the viewer; the two quick fixes to production. The record: `docs/DECISIONS-2026-10-27.md` and #21.

## 4. Hygiene

Every open issue sits in a milestone (the epic #21 alone outside). Closed today as landed: #22 (the policies, follow-ups as #61), #15 (the fixtures, follow-ups as #62), #59 (the finding, with its witness), #49 and #50 (in production, with their version ids and reads). Closed as superseded: #3, #4, #5; #2 placed under the public face. Filed today: #47 (a `migrate` bug), #49 and #50 (the two fixes that go alone), #54, #55, #56 (the owner's word on money as the rail), #61, #62 (the follow-ups). No branch of the captain's is pushed; the keeper pushes, opens, merges and deploys; local `main` equals `origin/main`.

## 5. What is next, in order

1. #14's rest: the `beta` branch with main's protection, CI on both lines, the tag rule (the keeper's hour).
2. Wave W25, the beta offer: #23 the wire vocabulary, #24 the paid door's code after its note, #25 the conformance witness against the beta from the funded seat, #30 the session object; about 15 lane-days `[EST]`.
3. The keeper's items with the owner's yes: the ratchet template (#16's gate) and the chain-event feed's design (#40's gate); the courier crate's #1 reading this repository's readers; the store crate's #3 schema brief and #5 executor.
4. Then W26 to W29 as #21 lays them out: the methods by rank, the per-wallet store, the courier work, hardening and the public face; the oracle and auto-research after the scoreboard is measured in CI.

## 6. The honest distance

Today bought the foundations: the three designs of record, the fixtures as the oracle's evidence, the door's rails and two charter fixes in production, a beta environment, two crates and a funded seat, and about four percent of the lane-days. The method count is unchanged; it moves in W25 and after. The cost per wallet-month stays flat by design (`docs/DECISIONS-2026-10-27.md` section 3), and nothing waits on the owner.
