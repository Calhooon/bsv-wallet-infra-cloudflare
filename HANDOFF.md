# HANDOFF: the captain of bsv-wallet-infra-cloudflare-public

The captain's record, kept current at every milestone. Re-orient from this file and from `cca runs --project bsv-wallet-infra-cloudflare-public --open` after any restart. The keeper of the stack is `captain-bsv-stack-lean` (message it with SendMessage); the division of captains binds: the crate's day-to-day is the captain's; the stack, the line, the pins, the Cloudflare layer's production and every outward act under the owner's name stay the keeper's; lanes never push, PR, deploy or open secrets; the keeper pushes, opens PRs, merges, tags, deploys. No network except reads. Secrets live in `~/vault/<project>.md`, read in place, never printed. `date -u` before every stamp.

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
