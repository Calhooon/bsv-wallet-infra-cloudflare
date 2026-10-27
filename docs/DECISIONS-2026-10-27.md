# Decisions of 2026-10-27: the five inputs of the plan, the words given, the limits read as a field

Written 2026-10-27 12:4xZ by the captain of `bsv-wallet-infra-cloudflare` on the owner's asks of 2026-10-27 ("I need context and detail and you to think through them as a field expert... understand we are a part of an ecosystem with standards... then give me options and god tier north star recommendations"; "hearing 'limits' scares me... understand our god tier north star"), and the owner's words that followed: "I like your recommendations... yes" (12:3xZ), and, recorded verbatim by the keeper on the stack's rulings page at 12:3xZ, "arc is old af... we use arcade now for everything". Labels as on `docs/READINESS-2026-10-26.md`: `[SRC]` read at a pin, `[X]` measured today, `[EST]` an estimate, `[D]` an inference. The reference is `ts-stack@edf6e03`, read with `git show`, never the working tree.

## 1. What binds each word

| word | the standard or charter | where it is read |
|---|---|---|
| the price of a call | BRC-105: a 402 carrying `x-bsv-payment-satoshis-required` and a BRC-29 derivation prefix, the client's `x-bsv-payment` header; the owner's multipart amendment to BRC-105; the reference's price hook is per request and defaults to 100 sats; the reference client pays a 402 by itself, bounded by its wallet's `createAction` policy | `BRCs` `payments/0105.md:56-74, 82-93, 104-113`; `StorageServer.ts:550-558`; `packages/middleware/payment-express-middleware/API.md:47-55`, `src/index.ts:330-340`; `packages/sdk/src/auth/clients/AuthFetch.ts:158-164, 334, 708-742`; the crate `bsv-middleware-cloudflare 0.7.2` `src/middleware/multipart.rs:3-9` |
| the bindings' schemas | the reference's tables `auth_sessions`, `auth_message_nonces`, `payment_replays`, `sync_transfers`, `sync_transfer_parts`; the crate's `AuthSessionStore` Durable Object and `payment_claims` table; the platform's ceilings | `packages/wallet/wallet-toolbox/src/storage/schema/KnexMigrations.ts:102-186`; the crate's `src/storage/do_session.rs:1-45`, `src/payment_claims.rs:27-47`; `developers.cloudflare.com/durable-objects/platform/limits`, `/d1/platform/limits`, `/r2/platform/limits`, `/workers/platform/limits` (read 2026-10-27 12:2xZ) |
| the spend of mainnet runs | the reference's suite is mainnet only by design, with a preflight that every spendable output belongs to a completed action; the miners' fee policy | `e2e/storage/README.md:1-130`; `GET /policy` at Arcade, TAAL's ARC and GorillaPool's ARC `[X]` 11:57Z, 100 sats per 1,000 bytes at all three |
| Bananablocks' home | the owner's courier ruling of 2026-09-04 (BananaBlocks and WhatsOnChain rotate as each other's fallback, Bitails third); engine 0.1.5 made the public explorer rungs opt-in by name; rule 28's shape of a break-glass read | `bsv-overlay-cloudflare@9a0f3c2` `crates/overlay-cloudflare/src/proof_fetcher.rs:56-58, 288-292, 637, 667, 2136-2150`, `wrangler.low.toml:67-70`; the stack `docs/RULINGS-2026-10-12.md:139`, `docs/p0/rule-28-explorer-calls.md` |
| the courier | the broadcaster charter B3 (ruled 2026-10-19): submit to the instances the host names, every word recorded per instance, no vote, a proof settles; the owner's word of 2026-10-27 names one instance, Arcade | the stack `docs/charters/broadcaster.md:17`; this repo `src/services/selected.rs:38-70`, `src/services/multi.rs:19-34`, `src/lib.rs:79` (the ARC key read) |

Two readings of the day. The reference's toolbox at the pin is 2.14.6, past the fix at 2.4.0 for CVE-2026-56744 (GHSA-36f9-7rg5-cpf8, published 2026-09-24: clients below the fix signed storage-supplied output scripts unchecked), so the ecosystem's clients now verify what a storage server returns; the conformance witness (#25) proves this server returns the caller's scripts byte for byte. BSV was $17.18 at 11:58Z (CoinGecko) `[X]`.

## 2. The five words

### The price of a call

| number | value | label |
|---|---|---|
| the reference's default when monetized | 100 sats a request; monetization off by default (`infra/wallet-infra/src/index.ts:686-688`) | `[SRC]` |
| Babbage after the rise | unknown; 40 times an unpublished number; 4,000 sats a call if the base was the software's default | `[D]` |
| our cost at the rack rate | 52 sats a call at the door, 214 with the monitor's reads, a write 87 more; $0 inside the plan's quotas to about 2,600 wallets (`docs/READINESS-2026-10-26.md` section 4) | `[EST]` |
| a payment transaction's mining fee | about 23 sats at 100 sats per 1,000 bytes; the sweep of its output about 15 sats | `[EST]` |
| revenue at today's load of 1,876 calls a day | $0.97 a month at 100 sats a call; $38.70 at 4,000 | `[EST]` |

Options: A free (no economic rule at the door, T4 weak); B flat 100 sats paid per call, the reference's shape (about a quarter of each payment to miners, one dust output a call, a transaction per call is a T3 debit); C per method (reads free to 50 a day, writes 200, a sync chunk 100); **D prepaid credit through the same 402 (recommended and taken)**: list price 100 sats a call, the 402 asks a top-up of 10,000 sats which the reference client pays unchanged, an onboarding allowance of 1,000 calls per new identity, the credit a row in the wallet's object debited atomically with the call, the payment internalized into the operator's own wallet in this store; E one 402 a wallet-month (off the standard's unit).

**The word (2026-10-27 12:3xZ): D.** Issue #24.

### The bindings' schemas

| binding | the reference | here | why |
|---|---|---|---|
| sessions | `auth_sessions` and `auth_message_nonces`, 24 h | the crate's `AuthSessionStore` Durable Object, one per session nonce, binding `AUTH_SESSION_STORE` with its migration tag, 24 h; KV the cold index | an atomic replay guard; the crate measured KV at p50 404 ms and the object at a few ms (`do_session.rs:5-19`) |
| the wallet's rows | one database for every user | one SQLite Durable Object per wallet keyed by identity | one writer per store (S9) by construction; no cross-wallet scan; a backup is one file; point-in-time recovery for 30 days |
| shared ancestry and the tracker | the same database | the global D1 for the users index, `proven_tx_reqs`, `monitor_events`; the proven ancestry's bytes in R2 under `proofs/{txid}` with the index row in D1 | the one table that grew with every wallet's use of the chain leaves the row store |
| payment claims | `payment_replays`, txid primary, 365 days | the crate's `payment_claims` with a UNIQUE on `txid` through a crate issue, 365 days, pruned by the cron | one atomic claim covers the prefix and the txid |
| sync transfers | two slots, 15 minutes, parts as rows | the slot row in the wallet's object; the parts in R2 `BLOBS` under `sync/{identity}/{transfer}/{offset}` with lifecycle expiry; the digest checked at completion | 64 MiB never sits in rows |
| action-batch packs | two PUT routes (`StorageServer.ts:560-606`) | R2 `BLOBS` under `packs/{identity}/{batch}/` with lifecycle expiry | one bucket, no new binding |

Options: A the reference's tables verbatim on one D1; **B the shape above (recommended and taken)**; C the minimum. **The word: B.** Issues #28, #30, #27, #29, #24.

### The spend of mainnet runs

| run | transactions | fees burned | float funded | in dollars at $17.18 |
|---|---|---|---|---|
| the default run, 3 identities, 400 sats a funding output, batches 2, 4, 8 | about 40 | about 1,000 sats | about 16,000 sats | $0.0002 and $0.003 |
| the comparison run, 8 identities, 3 rounds, batches 4, 8, 16, three repeats | about 250 | about 6,000 sats | about 100,000 sats | $0.001 and $0.017 |

All four columns `[EST]` from the suite's defaults. The money is not the risk; the custody of a funded key and the preflight are. Options: testnet only (proves less: the suite is mainnet only by design and the miners' policy is the law); mainnet uncapped (no); **mainnet capped (recommended and taken)**: one dedicated identity funded with 1,000,000 sats (about $0.17), a ceiling of 50,000 sats a run enforced in the harness preflight, the key in `~/vault/` and never in CI, a ledger page in `docs/` with the sats before and after and the txids, runs against the beta only, weekly once the conformance witness lands. **The word: capped as above.** Issue #25.

### Bananablocks' home

It already has one. The overlay engine reads `https://bananablocks.com/api/v1` (GorillaPool's independent explorer backed by JungleBus) at 60 requests a minute with two rungs, the raw transaction and `GET /txo/{txid}/{vout}/spend`; it has no merkle-proof route; it answered JSON today `[X]` 11:57Z. This repo carries a WhatsOnChain reader of 1,520 lines and a Bitails reader of 233 and no Bananablocks; the header service carries a third ladder (`chaintracks-cloudflare` `src/couriers.rs:13, 75-77`). Options: copy a reader here (a fourth copy); leave Bananablocks to the overlay; **one courier crate for the Cloudflare layer (recommended and taken)**: the engine's four rungs, the header service's ladder and this repo's readers folded into one crate with the pacer, the rotating start, the break-glass switch, every read recorded, "could not look" distinct from "nothing there". The crate's name and home are the keeper's word. **The word: the overlay's reader is the home; one courier crate.** Issue #41.

### The courier

The charter's B3 names the instances the host names. The captain's reading of 2026-10-26 had kept ARC as the outage courier and the brief of 12:0xZ recommended B3's default of three instances. **The owner's word, verbatim, 2026-10-27 12:3xZ: "arc is old af... we use arcade now for everything."** Arcade is the courier for everything; our own node later (the Cloudflare node is design only today, `bsv-node-cloudflare` README:16); ARC is not the outage courier and not a rung of anything. What it changes here: `BROADCASTER` loses its `arc` selector and the ARC fallback (`selected.rs:38-70`, `multi.rs:19-34`), the monitor's ARC verdict paths become Arcade's, `src/services/arc.rs` goes, and the production secret `ARC_API_KEY` goes once the code no longer reads it (`src/lib.rs:79` today; the keeper holds the secret until the captain says so). WhatsOnChain leaves the broadcast path entirely (#41). Issue #39.

## 3. The limits, read as a field

Three kinds exist and only one is capacity.

| kind | what it is | how the design treats it |
|---|---|---|
| a per-unit ceiling of the platform | 10 GB per SQLite Durable Object; 10 GB per D1 database and 1 TB of D1 per account; 2 MB per row; 128 MB of memory and 30 s to 5 min of CPU per request `[X]` | the unit is one wallet; objects per account and storage per account are unlimited on the paid plan `[X]`, so capacity is the sum of unbounded units; bytes that could reach a row ceiling live in R2, unlimited per bucket `[X]` |
| a rail we set | the handshake rate limit, the per-identity limit, the price, the allowance, the spend ceiling | policy numbers on the scoreboard, changed by a word, never by a migration; the price is the rule at the door (T4) |
| a third party's quota | Bananablocks at 60 a minute; WhatsOnChain's 429 of 2026-04-15 (`wrangler.toml:85-90`) | off the routine path by the north star: Arcade carries the submissions, our own headers serve the proofs, the feed pushes the events, explorers bind only break-glass reads |

| ceiling `[X]` | one wallet today | headroom | what multiplies it |
|---|---|---|---|
| 10 GB per wallet object | 1.89 MB, of which 1.08 MB is shared ancestry that moves to R2, so 0.81 MB | about 12,000 times | objects and storage per account unlimited |
| 10 GB per D1 database, 1 TB per account | the global index at about 200 bytes a wallet `[EST]` | tens of millions of wallets in one database `[EST]` | 50,000 databases |
| 2 MB per row | blobs over 4,096 bytes already go to R2 (`src/r2.rs:92-98`) | not reached | R2 objects to 5 TiB |
| CPU 30 s default to 5 min, 128 MB memory | about 35 ms a call `[EST]`, a BEEF read as a stream | about 850 times | nothing buffered |
| 10,000 subrequests an invocation | two object calls a request, one courier POST a broadcast | thousands of times | alarms, queues and Workflows for fan-out |
| one writer per object | 28 calls a day on average `[EST]` | isolation | a heavy wallet slows only itself |
| point-in-time recovery, 30 days per object; D1 Time Travel 30 days | the backup floor | | our own snapshot to R2 on top (#26) |

The cost of the taken design as it grows, `[EST]` from the prices read 2026-10-26 and 2026-10-27 (Workers $0.30 a million requests and $0.02 a million CPU-ms past the plan; objects $0.15 a million requests, $12.50 a million GB-s past 400,000, rows written $1.00 a million past 50 million, rows read $0.001 a million past 25 billion, storage $0.20 a GB-month past 5 GB; R2 $0.015 a GB-month) and the page's ratios (840 calls a wallet-month, two object calls a request, 35 ms of CPU, 5 rows written and 50 read a call, 1.89 MB in the object and 7.3 MB in R2 a wallet):

| wallets | calls a month | the bill a month | a wallet-month | revenue a wallet-month at 100 sats a call |
|---|---|---|---|---|
| 10,000 | 8.4 million | about $16 | $0.0016 | $0.0144 |
| 100,000 | 84 million | about $530 | $0.0053 | $0.0144 |
| 1,000,000 | 840 million | about $5,800 | $0.0058 | $0.0144 |

Flat per wallet-month across two orders of magnitude and never carrying the chain's size: T1 as written. Rows written are 72 percent of the bill at scale, so the scoreboard's row "statements per door call, only falling" is the cost lever.

| option | the shape | the wall | the tests |
|---|---|---|---|
| A one D1, migrate later | today's shape | 10 GB at about 5,300 wallets; the monitor's scan grows with the wallets | fails T1 in spirit and T3's "no scan ever" |
| B D1 sharded by wallet | a database per group, a directory in front | 1 TB per account at about 530,000 wallets; the busy word is a lock we build | T1 to a point; S9 by code |
| **C one SQLite object per wallet (taken)** | the wallet's rows in its object; the crate's session object; a small global D1; ancestry, blobs, sync parts and packs in R2; an alarm per wallet for the age re-ask; the feed and the Arcade callback write proofs in; a WebSocket from the object to the user's wallet as an additive push | none shared; 10 GB per wallet | T1, T3, S9 by construction, PITR as the backup floor, T6 through a store driver |
| D C plus our own node and the feed as the only chain inputs | the end state once the Cloudflare node exists | none shared | T2 and T5 at their strongest |

Speed: the crate measured today's KV session path at p50 404 ms and p90 528 ms on the LOW relay and the object path at a few ms, so the session object moves ahead in the order. Scoreboard targets `[EST]`, goals to measure: a read under 100 ms at p99 at the edge, a write under 300 ms excluding the courier's round trip, the session hop under 5 ms. Design: the wire stays the reference's; everything new is additive. Maintainability: this repo owns 16 tables (`migrations/0001_initial.sql`) and 11,959 lines under `src/storage/`; the Rust toolbox owns a second store behind `WalletStorageReader`, `Writer`, `Sync`, `Provider` (`bsv-wallet-toolbox-rs` `src/storage/traits.rs:540-915`) with sqlx migrations. One store crate, the toolbox's traits with a Durable Object SQL executor beside the CLI's, gives one schema, one query set, one Lean model and one runner driver; this repo shrinks to the door and the glue.

## 4. The keeper's items this needs

The store crate's name and home; the courier crate's name and home; `Storage.lean` (S4 to S9) with the runner's store driver. Asked of the keeper 2026-10-27 12:4xZ.

## 5. Addendum, 2026-10-27 15:11Z: the words of the afternoon

- **13:0xZ, "yes":** the two crates exist from the template, `Calhooon/bsv-courier` (3765dda) and `Calhooon/bsv-wallet-store` (9536364), the names final; the mapping of this repository's asks is on #21.
- **14:3xZ, "love it...", on the price as the rail.** Three kinds of limit were read: a platform ceiling per unit, a rail we set, a third party's quota. The owner's question, verbatim: "why rate limits at all if we can make money on all this.. but at the same time we dont wanna leave shit open for like infinite cloudflare bills". The answer taken: on paid calls money is the only rail and the quote rises with the caller's rate (#54); the two Workers rate-limit bindings carry production and beta for the unpaid paths only, the handshake, a bad signature and the 402 itself, which are the one shape of an unbounded bill (about half a dollar a million refused requests at the rack rate); the flood stops at the zone with a firewall rule, the keeper's (#56, live, 167 POSTs per 10 seconds per address on the zone's plan); a daily budget switch bounds the worst day by a number the owner chooses (#55); the free onboarding allowance stays.
- **14:3xZ, "agents I believe probably....":** the agents captain patches the viewer's keyless status call; the keeper routes it.
- **14:5xZ, "we can do this yes":** the suite's mainnet identity funded with 1,000,000 sats and its key in the vault; the keeper's ratchet template and feed design proceed. **15:0xZ, "you can do it from port 3321":** the payment made from the owner's desktop wallet, txid `d93f713288091c913bd4ffe8a44efb7357d58abb9df7155c355b592693f60319`, the seat funded (#25).
- **The keeper's closings (14:3xZ):** the fleet measurement on #22 (seven days, 9,847 POSTs over 12 addresses, the worst minute per address 123, so the door's 300 a minute blocks no fleet caller); the zone rule read back on #56; the door policies in production at 15:08:01Z.
