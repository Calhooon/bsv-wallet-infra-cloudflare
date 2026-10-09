# rust-wallet-infra: private `bsv-auth-cloudflare` → public `bsv-middleware-cloudflare` on worker 0.8

Survey lane `mw-wi-survey` (Calgooon/agents#34, milestone M5), 2026-10-08. Read-only: nothing in
the repo was changed except this file. Every measurement below was taken in scratch copies under
`/tmp` (`/tmp/wi-base` = today's tree + the main checkout's `Cargo.lock`; `/tmp/wi-mw-probe` =
today's tree with only the two `Cargo.toml` lines swapped).

## 0. Headline

> **Production is probably exposed today; found by reading the code, not exercised (read-only lane).** The
> private crate wallet-infra runs (`bsv-auth-cloudflare` 0.1.3) has three properties:
> - `handle_initial_request` marks a session `is_authenticated = true` from an **unsigned** initialRequest
>   naming any identity key (`rust-middleware/bsv-auth-cloudflare/src/middleware/auth.rs:531-533`).
> - It looks a general message's session up by `your_nonce` (:183-184).
> - It verifies the signature with the **header's** key (:777), yet reports `AuthContext` = the
>   **session's** identity (:241).
>
> So a caller holding any key can open a session "as" victim V, then sign general messages with its own key,
> and wallet-infra dispatches them with `AuthId` = V (lib.rs:487). The 631cd02 commit message and test
> describe exactly this. The caller still cannot *sign* V's spends. It can read V's outputs and actions, and
> call `abortAction`, `relinquishOutput`, `reserveOutputs` and `createAction` (locking V's UTXOs), and
> `internalizeAction`, as V: privacy loss plus fund freezing and disruption. **This raises the migration's
> priority.** If the full lane cannot ship soon, a one-check hotfix in the private crate (refuse
> `auth_message.identity_key != session.peer_identity_key` before verify, as 631cd02 does) closes it on its
> own. Confirm it on staging with the negative test (a) below before relying on this reading.

- **The code compiles unchanged.** Swapping two lines in `Cargo.toml` (the middleware → `bsv-middleware-cloudflare`
  0.3.5 at `~/bsv/wt-middleware-lane`, aliased to the old crate name; `worker = "0.8"`) gives:
  `cargo check --target wasm32-unknown-unknown` with 0 warnings, `cargo clippy -D warnings` clean,
  `cargo test` **818 passed / 0 failed / 1 ignored**. The baseline (today's lock) gives the same 818/0/1.
- **The risk is behavioural.** The public middleware is stricter on the auth hot path than the private
  0.1.3: it binds identity to session, adds a replay guard, and refuses general messages with no nonce.
  Today wallet-infra turns every middleware `Err` into a bare `500 INTERNAL SERVER ERROR`, so a refused
  client never learns to re-handshake (§3).
- The dependency graph unifies. Today there are **two `bsv-rs` copies**: registry **0.3.15**, used directly by
  wallet-infra (pinned by the main checkout's lock), and local-path **0.3.32**, pulled in by the private
  middleware's `path = "../../bsv-rs"`. After the move there is one, registry **0.3.33**. So wallet-infra's
  own BEEF code jumps **0.3.15 → 0.3.33**. That jump matters more than the 0.3.32 → 0.3.33 one (§1.3).

## 1. Inventory

### 1.1 `bsv_auth_cloudflare::` symbols: 7, all in `src/lib.rs`

Imported at `src/lib.rs:25-31`. Nothing else in `src/` or `tests/` imports the crate.

| Symbol | Use sites | Public 0.3.5 |
|---|---|---|
| `add_cors_headers` | lib.rs:134, 172, 187, 199, 226, 240, 252, 258, 269, 281, 299, 309, 334, 361, 403, 465 | same signature; Expose-Headers gains the lane-offer header (additive) |
| `init_panic_hook` | lib.rs:112 | same (`console_error_panic_hook::set_once`); see R5 |
| `middleware::auth::handle_cors_preflight` | lib.rs:116 | same |
| `middleware::process_auth` | lib.rs:379 | same signature, still KV `AUTH_SESSIONS`, same prefix `auth`; **stricter behaviour** (§3) |
| `middleware::sign_json_response` | lib.rs:414, 423, 500 | same |
| `middleware::AuthMiddlewareOptions` | lib.rs:371 (`server_private_key`, `allow_unauthenticated`, `session_ttl_seconds`, `..Default`) | same fields, plus new `session_lane: None` by default (= reference behaviour) |
| `middleware::AuthResult` | lib.rs:384 (`Authenticated{context,request,session,body}`), 390 (`Response`) | same; `context.identity_key` read at lib.rs:487 |

The public twin `~/bsv/bsv-wallet-infra-cloudflare-public` uses an import block byte-identical to
`src/lib.rs:25-31`. It made the move with the **package alias** `bsv-auth-cloudflare = { package =
"bsv-middleware-cloudflare", version = "0.1.2" }`, so it needed zero source edits. But it **stayed on
worker 0.7** (0.1.x line), on purpose: its Cargo.toml comment says 0.2+ would split the graph into two
`worker` crates. So the twin is the precedent for the *alias* but **not** for the worker 0.8 step. Nobody
has done that step yet. Its lock has `bsv-rs` 0.3.17 and `worker` 0.7.4.

KV session compatibility between the private and public crates is intact in both directions. Both define
`StoredSession` with the same fields (public adds only `PartialEq, Eq` derives and comments). Both use the
key layout `auth:session:{nonce}` / `auth:identity:{key}:{nonce}`. The public crate adds `auth:nonce:{scope}:{nonce}`
(replay records), which the old code ignores. Live sessions survive the deploy and a rollback.

### 1.2 worker 0.7.4 → 0.8.7: 0 compile-breaking deltas; 6 behavioural or build deltas

Surface wallet-infra uses: D1 `prepare`/`bind` (420)/`first`/`all`/`run`/`batch`/`raw`/`meta`
(`src/d1/*`, `src/storage/*`); R2 `Bucket` `get`/`put`/`delete`/body `text`/`bytes` (`src/r2.rs`);
`Fetch::Request` + `Request::new_with_init` + `RequestInit::with_method/with_headers/with_body` + `Headers`
(`src/services/*`); `Delay::from` (6); `Env::secret/var/d1/bucket`; `#[event(fetch)]` lib.rs:110 and
`#[event(scheduled)]` lib.rs:503 (`ScheduledEvent`, `ScheduleContext`); `console_log!/console_error!`.
There is no KV use in wallet-infra itself (the middleware uses it), and no DO, Cache, Router or `worker::Date`.
Timing uses `js_sys::Date::now` (audit.rs:142, services/arcade.rs:402), which worker does not touch.

All of these compile unchanged on 0.8.7 (measured). The deltas that remain:

| # | Delta (0.7.4 → 0.8.7 source diff) | Effect on wallet-infra |
|---|---|---|
| W1 | **worker-build pairing**: 0.8 needs worker-build ^0.8 (new `async_export` glue, `__worker_init_state`) | `wrangler.toml:53` and `wrangler.arcade-stage.toml` pin `--version ^0.7`. **Must** change to `^0.8`, or the build bricks (the known "Unsupported version worker@…" trap) |
| W2 | New `init.rs`: `#[wasm_bindgen(start)]` installs a panic hook (`criticalError` flag) plus `set_on_abort → schedule_reinit` | After a panic the instance **reinitialises** instead of staying poisoned. Good. Isolate-global state resets; wallet-infra has no `static`/`thread_local` caches (grep clean). `init_panic_hook()` (lib.rs:112) then *replaces* worker's hook (R5) |
| W3 | D1 `raw()` path: `serde_wasm_bindgen::from_value(..).unwrap()` → `?` | A malformed row is now an `Err`, not a panic. Safer. One call site |
| W4 | D1 adds `with_session` / `D1DatabaseSession`, `IntoFuture` for statements, `#[must_use]` on `D1PreparedStatement` | Additive; 0 new warnings |
| W5 | `Delay` / `ScheduleContext::wait_until` wrap futures in `AssertUnwindSafe`; R2 `only_if`/`start_after` fields; `Headers::is_empty`; `Env::send_email` | Additive or internal |
| W6 | `#[event(fetch)]` error handling unchanged (`Err` → `Response::error("INTERNAL SERVER ERROR", 500)` in both) | No delta, but it shapes R1: every `process_auth` `Err` is an opaque 500 |

Size: the release wasm32 (pre-wasm-opt) goes from 3,506,404 to 4,185,383 bytes (+19%); gzip from 980,549 to
1,136,402. That is far under the Workers limit. The growth is the session-lane and Durable Object code that
wallet-infra does not use. The public crate exports the `AuthSessionStore` Durable Object class
(`pub use storage::do_session::AuthSessionStore`), and its wasm-bindgen exports are in the artifact
(`__wbg_authsessionstore_free_*`; the baseline has 0). So the shim will export an **unbound DO class**.
Cloudflare should accept an exported class with no binding or migration, but this is **unverified**: check it
with `wrangler deploy --dry-run` (step 4).

### 1.3 bsv-rs: local 0.3.32 vs published 0.3.33, and the real jump 0.3.15 → 0.3.33

- `~/bsv/bsv-rs` (0.3.32, c05edf5) vs crates.io 0.3.33: **one file differs, `src/script/spend.rs`** (the
  interpreter). Neither wallet-infra nor the middleware runs scripts. For the middleware's view this delta is nil.
- wallet-infra's own `bsv_sdk` surface (21 `use` lines) is `transaction::{Beef, BeefTx, MerklePath,
  MerklePathLeaf, Transaction}`, `primitives::{sha256d, to_hex}`, `sha`, and `wallet::{CreateActionArgs,
  CreateActionOutput, InternalizeActionArgs, AbortActionArgs, BasketInsertion, WalletPayment}`. It calls
  `merge_raw_tx`(12), `merge_bump`(6), `merge_beef`, `find_txid`(7), `find_bump`, `sort_txs`, `to_binary`(15),
  `to_binary_atomic`, `verify_valid`(5), `compute_root`(5), `Transaction::from_binary`, `MerklePath::from_binary`.
- 0.3.15 → 0.3.33 changes 25 files. Among those wallet-infra touches: `merkle_path.rs`, `beef_tx.rs`,
  `wallet/types.rs` and `primitives/hash.rs` are **identical**. `transaction/beef.rs` (+535 diff lines) adds
  memoised merkle-path re-attachment for `find_atomic_transaction` / `find_transaction_for_signing`, which
  wallet-infra **does not call**. `transaction/transaction.rs` (+353) adds `invalidate_caches`; no public
  signature changed. The other changes are script/sighash/ecdsa/auth-peer.
- Tests already run against 0.3.33 when the lock is absent. **`Cargo.lock` is gitignored** (`.gitignore`),
  so the worktree resolves fresh while production builds from whatever lock sits in the deploying checkout
  (today 0.3.15). The migration must **commit the lock** (R2).

## 2. Plan (one lane, ordered)

| Step | Change | Hand-written diff |
|---|---|---|
| 1 | **Pick the crate source.** wallet-infra uses none of the payment helpers, so it does not need 0.3.6. It does need a *published* crate: the registry cache shows 0.2.0–0.3.3, and session-lane 0.3.5 is a branch (`origin/session-lane`, contained in `lane/mw-port`). Depend on `bsv-middleware-cloudflare = "=0.3.6"` once mw-port publishes; until then pin a `git` rev of session-lane. Never use a path dep in the deployable. | – |
| 2 | `Cargo.toml`: `bsv-auth-cloudflare = { package = "bsv-middleware-cloudflare", version = "=0.3.6" }` (the alias the public twin uses, so `src/` keeps `bsv_auth_cloudflare::`) and `worker = { version = "0.8", features = ["d1"] }`. Leave `bsv-sdk` alone; it resolves to 0.3.33. | 2 lines |
| 3 | **Commit `Cargo.lock`**: drop `Cargo.lock` from `.gitignore`, generate, commit. Check there is exactly one `bsv-rs` (0.3.33) and one `worker` (0.8.7). | 1 line + generated lock (~1.9k lines) |
| 4 | `wrangler.toml:49-53` and `wrangler.arcade-stage.toml`: `--version ^0.8 worker-build`, and update the pairing comment. Run `worker-build --release` and `wrangler deploy --dry-run` (verifies the shim and the unbound `AuthSessionStore` export). | ~6 lines |
| 5 | `src/lib.rs:379-381`: stop collapsing `process_auth` errors into 500. Map `AuthCloudflareError` → `Response` with `e.status_code()` (InvalidAuthentication → **401**) and a JSON `{status:"error", code: e.code(), description}`, through `add_cors_headers`. Put it in a small pure mapping fn so it can be unit tested. Then identity-mismatch or bad-signature refusals become 401s, which `WorkerStorageClient`, the x402 helper and TS `AuthFetch` treat as "re-handshake". | ~20 lines + ~25 test |
| 6 | `src/lib.rs:112`: drop `init_panic_hook()` (worker 0.8's `init` already installs a logging panic hook, and replacing it loses the `criticalError` flag), or keep it and note why. | 1–2 lines |
| 7 | `.github/workflows/ci.yml:16-26`: drop the `Calgooon/rust-middleware` checkout. Today CI also lacks the `bsv-rs` sibling that the private crate path-deps, so the migration makes CI self-contained. Key the cache on the now-committed lock. | −6 lines |
| 8 | Docs: CLAUDE.md "Sibling Dependencies" and README. | ~10 lines |

**Estimated hand-written diff: ~70 lines (src ~45 incl. tests, config ~15, docs ~10) plus the generated `Cargo.lock`.**

### Test strategy

1. **Unit / static** (worktree, before any deploy): `cargo fmt --check`, `cargo clippy -- -D warnings`,
   `cargo test` (expect ≥818 pass, plus the new error-mapping tests), `cargo build --release --target
   wasm32-unknown-unknown`, `worker-build --release` (^0.8), `wrangler deploy --dry-run`.
2. **Staging** (`wrangler.arcade-stage.toml`: own worker `wallet-infra-arcade-stage`, own D1
   `wallet-infra-arcade-stage`, own KV; crons commented out):
   - `tests/e2e/run_all.sh <stage-url>` (health_check, json_rpc_smoke, monitor_health). Note that
     `json_rpc_smoke.sh` is **unauthenticated only**: it checks the unauth methods and that auth-required
     methods reject. It does **not** exercise the BRC-104 general path that changed.
   - An **authenticated** pass against staging with each real client family: toolbox-rs `StorageClient`
     (`agents/manage` against stage), `WorkerStorageClient` (public, KV resume), the TS `@bsv/wallet-toolbox`
     `StorageClient`/`AuthFetch`, and MetaNet Client pointed at stage if possible. Exercise `listOutputs`,
     `listActions`, `createAction` → `processAction` with a dust-sized self-send, and `internalizeAction`.
   - **Negative tests**: (a) a general message whose identity header ≠ the session's → expect **401**
     `ERR_INVALID_AUTH` (after step 5); (b) the same signed request sent twice → second gets 401
     `ERR_REPLAYED_REQUEST`; (c) no `x-bsv-auth-nonce` → 401.
   - The e2e harness (test-agent `/test-async`) end to end against stage.
3. **Read-only D1 check** (prod, `npx wrangler d1 execute wallet-infra --remote --command "…"`, SELECT only),
   just before and 15 min after the deploy. Expect the numbers to match apart from organic traffic:
   - `SELECT status, COUNT(*) FROM transactions GROUP BY status;`
   - `SELECT status, COUNT(*) FROM proven_tx_reqs GROUP BY status;`
   - `SELECT COUNT(*), SUM(satoshis) FROM outputs WHERE spendable = 1;`
   - `SELECT COUNT(*) FROM outputs WHERE reserved_until > strftime('%s','now')*1000;` (adjust to the column's unit)
   - `SELECT event, MAX(created_at) FROM monitor_events GROUP BY event ORDER BY 2 DESC LIMIT 15;`
     This proves the cron (`#[event(scheduled)]`, now on 0.8 glue) still fires and each task logs within two cycles.
4. **Prod smoke after deploy**: `tests/e2e/run_all.sh`, `GET /` (broadcaster field), one authenticated
   `listOutputs` per client family, and `wrangler tail` for 30 min filtered on 500s,
   `ERR_REPLAYED_REQUEST`, "Message identity key is not the session's", and `ERR_SESSION_NOT_FOUND`.

### Rollback

- No D1 migration and no binding change. KV sessions are schema-compatible both ways (§1.1). So
  **`npx wrangler rollback`** to the previous version is clean and needs no rebuild. That avoids the
  worker-build ^0.7/^0.8 pairing on the way back. Record the current version id (`wrangler deployments list`)
  in the deploy note before deploying.
- Source rollback: `git revert` the migration commit(s). The old build still needs the private crate and
  worker-build ^0.7, so prefer `wrangler rollback` under pressure.
- Replay records (`auth:nonce:*`) left in KV expire on their own TTL (session TTL, 3600 s); the old code ignores them.

### Deploy gate (funds-routing worker; 10 products depend on it)

Deploy only when **all** of these hold:
1. **Explicit owner approval** for this specific deploy. A lane never deploys.
2. Step-1 unit/static suite is green on the committed lock, and the lock shows a single `bsv-rs` 0.3.33 and a single `worker` 0.8.x.
3. Staging is green, **including the authenticated pass for every client family and all three negative tests**.
4. Prod is quiet: no open incident, the monitor's last cycle is healthy (`monitor_events`), no BEEF/proof
   backlog spike, and a low-traffic window.
5. The pre-deploy D1 snapshot and the rollback version id are recorded.
6. Abort or roll back if, in the first 30 min, the 500 rate exceeds baseline, **any** honest client logs an
   identity-mismatch or replay refusal, or a monitor cycle is missing.

## 3. Session-identity check (session-lane 631cd02) and the other stricter auth paths

Measured against the private 0.1.3 `process_auth_with_storage`. The public crate adds:

- **(A) identity binding** (631cd02): the `x-bsv-auth-identity-key` header must equal the identity of the
  session found by `your_nonce`, and the signature is verified against the session's identity. The private
  crate has neither check: `verify_message_signature` uses the *header's* key as counterparty
  (`bsv-auth-cloudflare/src/middleware/auth.rs:777`; the session's key appears only in `sign_message`, :734),
  so any wallet's honest signature verifies under another identity's session there. The public crate closes
  a real auth hole here; it is not just being stricter.
- **(B) replay guard** (c1084f0): each `x-bsv-auth-nonce` is consumed once per session (a KV write to a unique key); a reuse gets 401 `ERR_REPLAYED_REQUEST`.
- **(C) a missing nonce** gets 401.
- Also: a lazy, non-fatal liveness touch. The private crate writes the *same* session key on every request
  and fails the request if that write fails, which is the KV-429 → 500 class. The public crate adds a bounded
  KV transient retry, and a lane that is opt-in only (`session_lane: None`).

**Client sweep** (file:line evidence in the lane transcript's Explore report):

- bsv-rs `Peer`/`SimplifiedFetchTransport`, toolbox-rs `StorageClient`, public and legacy `WorkerStorageClient`,
  every fleet agent (kling, veo, reader, whisper, …), `agents/manage`, btc-relay, overlay-cloudflare,
  rust-message-box, dkls-wallet, bsv-mpc, a private program scripts, and TS `AuthFetch` + wallet-toolbox `StorageClient`:
  each binds one client to one wallet, signs every general message with a fresh random 32-byte nonce, and
  re-signs on retry. None re-sends pre-signed bytes. **They are not affected by (A), (B) or (C).**
- **x402 helper** (`~/bsv/x402-skill-repo`): the session file is keyed **only by a hash of the server URL**
  (`lib/session.py:75-78`), while the identity is read live from MetaNet on each request (`lib/auth_request.py:193`).
  If the MetaNet identity or profile changes while a session file under 1 h old exists, (A) fires. It
  self-heals **only if the refusal is a 401** (its retry is `:235-251`). Today that refusal would be an opaque
  500, which step 5 fixes. It does not target wallet-infra by default.
- `WorkerStorageClient` (public) KV resume snapshot does not record its own identity. A shared or stale KV key
  could pair identity Y with X's session. Its only caller (bsv-blackjack `pool.rs:112`) keys by identity, so
  the risk is theoretical. Its re-handshake retry matches 401/403, or a 500 *containing* "Invalid
  authentication". The opaque 500 does not contain that text, which step 5 also fixes.
- **MetaNet Client.app** is an opaque binary. It is presumably the TS SDK and toolbox above. **Unverified**;
  needs the staging pass.

Verdict: **no known honest client is affected** by the stricter checks in normal operation. One edge exists
(the x402 helper's URL-keyed cache across an identity switch). In wallet-infra the refusal shape (500
instead of 401) turns any such edge into a stuck client for up to the 1 h session TTL. Hence step 5.

## 4. Risks, ranked

0. **R0. (pre-existing, prod today) The private crate lets any key act as any identity (§0).** This is not a migration risk; it is the reason to migrate, or to hotfix first.
1. **R1. Stricter auth refuses an honest client, and the refusal is an opaque 500.** Sources are
   (A)/(B)/(C) above combined with lib.rs:379-381 mapping `Err` → 500. Known exposure: the x402 helper
   across an identity switch, MetaNet Client unverified. Mitigation: step 5 plus the staging client matrix
   and negative tests. **This is the stop condition: if any honest client family (MetaNet Client, a fleet
   agent's StorageClient or WorkerStorageClient, the TS toolbox) is refused by (A), (B) or (C) on staging,
   stop the migration.** Fix the client, or carry the reference-parity behaviour, before production. A refused
   storage client cannot create or process actions, so the funds of 10 products stop moving.
2. **R2. bsv-rs jumps 0.3.15 → 0.3.33 under wallet-infra's own BEEF/MerklePath code, and the lock is not
   committed.** The changed code paths are ones wallet-infra does not call, and 818 tests pass on 0.3.33.
   But this is the funds path, 18 releases apart, and builds are not reproducible across checkouts until the
   lock is committed. Mitigation: step 3 plus the staging `createAction`/`processAction`/`internalizeAction`
   pass and the D1 before/after check.
3. **R3. Build and runtime glue of worker 0.8.** The worker-build ^0.8 pairing (a wrong global install bricks
   the build), the new `#[wasm_bindgen(start)]` panic/reinit path, the cron on the new `async_export` glue,
   and the unbound `AuthSessionStore` DO class export are all unverified until `--dry-run` and staging.
   Mitigation: step 4, the monitor_events check, and `wrangler rollback`.
4. R4. The crate source is not yet published at the needed version: 0.3.5 is a branch and 0.3.6 is pending.
   Mitigation: step 1. Pin exactly (`=`). Never ship a path dep.
5. R5. `init_panic_hook()` replaces worker 0.8's hook. Logging still works; the `criticalError` flag is lost,
   but abort → reinit still runs via `set_on_abort`. Mitigation: step 6.
6. R6. KV write pattern change: one unique-key nonce write per request, replacing a same-key session write
   per request. The write count is roughly equal and it removes the 1-write/s/key 429. Watch the KV write
   bill and latency in `wrangler tail`.

## 5. Migration lane estimate

**~5–7 hours**: ~1.5 h code and config (steps 2–8, including the error-mapping tests), ~1 h build, dry-run
and CI, ~2–3 h staging client matrix and negative tests, ~1 h gate prep (D1 snapshot, rollback id, deploy
note). The prod deploy is separate and owner-gated.
