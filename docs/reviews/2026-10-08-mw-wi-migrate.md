# Review: lane/mw-wi-migrate (ed11fed) — private `bsv-auth-cloudflare` → `bsv-middleware-cloudflare` 0.3.6 on worker 0.8

**VERDICT: SAFE** — for merge and the staging gate. One must-fix before any production deploy (the TEMP path dep); none of the UNSAFE triggers fire.

Lane `mw-wi-review`, Calgooon/agents#34, M5. 2026-10-08. Read-only on the code; this file is the only write.
Reviewed `git diff main...lane/mw-wi-migrate` (merge-base 75a89ad, one commit ed11fed, 9 files) against
`docs/mw-migration-plan.md` (main) §2 steps 1–8, §3 and §4 R0–R6.

## UNSAFE triggers, each checked

| Trigger | Result |
|---|---|
| Test count drop | **No.** Measured 822 passed / 0 failed / 1 ignored (823 unit tests) + doctest 0/0/1. The diff adds 4 `#[test]` and removes 0 (`src/lib.rs:578-642`), so the base is 818, matching the plan's 818/0/1. |
| Money-path change beyond the crate/worker/SDK swap | **No.** `git diff` over `src/storage`, `src/monitor*`, `src/dispatch.rs`, `src/d1`, `src/r2.rs`, `migrations` is 0 lines. `src/lib.rs` changes only the import block, the `init_panic_hook` call, the `process_auth` error arm and a test module. `bsv-sdk` line is unchanged (resolves 0.3.33; the prod jump 0.3.15 → 0.3.33 is the plan's R2 and still needs the staging create/process/internalize pass). |
| `auth_error_parts` leaks internals or maps a non-auth error to 401 | **No on 401.** `status_code()` returns 401 only for `Unauthorized`, `InvalidAuthentication`, `SessionNotFound` (crate `src/error.rs`); every `Err` on `process_auth`'s path that is a refusal is one of those; KV/SDK/config/transport stay 500, serialization 400 (not reachable from `process_auth`). 401 bodies carry only the crate's refusal text. Verified the config-error text cannot echo the server key: `PrivateKey::from_hex` → `hex::decode` error ("Odd number of digits" / "Invalid character 'x' at position N"), `from_bytes` → "Invalid key bytes: …" (bsv-rs 0.3.33 `primitives/ec/private_key.rs:93-96,75`). **5xx bodies now carry the crate's `description`** (e.g. "KV storage error: …") where main sent a bare `INTERNAL SERVER ERROR` — minor disclosure, should-fix S1, not an UNSAFE trigger. |
| A BRC-104-signed error path now unsigned where it was signed | **No.** On main every `process_auth` `Err` was an unsigned opaque 500 (`map_err(|e| Error::from(e.to_string()))?`). The crate's own refusals (`AuthResult::Response`: UNAUTHORIZED, ERR_SESSION_NOT_FOUND, and new ERR_REPLAYED_REQUEST / missing nonce) are unsigned JSON in both crates (old `auth.rs:164-200`, new `auth.rs:298-355,417-450`). The three `sign_json_response` sites after auth are untouched. |
| `Cargo.lock` carrying anything but one bsv-rs 0.3.33 and one worker 0.8.7 | **Clean.** 178 packages, lockfile v4: `bsv-rs` 0.3.33 (registry, one entry), `worker`/`worker-macros`/`worker-sys` 0.8.7 (registry, one each), `bsv-middleware-cloudflare` 0.3.6 **with no `source`/`checksum`** (path dep — see M1). No git or other non-registry sources. |
| Absolute TEMP path dep missing its `# TEMP` marker | **Present:** `Cargo.toml:13` ends `# TEMP until 0.3.6 publishes (M5 #36)`, with a 3-line comment explaining the absolute path. |
| Secret, real id or `.dev.vars` in the diff | **None.** Added lines scanned for key/token/id patterns, 64-hex and WIF: nothing. `.dev.vars` is not tracked (`.gitignore:13`). The only identifiers in the diff are the issue numbers. |
| Cron/monitor (`scheduled`) or D1/R2/KV bindings altered | **No.** The `#[event(scheduled)]` hunk is unchanged; `wrangler.toml` / `wrangler.arcade-stage.toml` change only the `[build] command` pin (`^0.7` → `^0.8`) and its comment. The dry-run lists the same bindings as main: `AUTH_SESSIONS` KV, `DB` D1 `wallet-infra`, `BLOBS` R2 `wallet-infra-blobs`, 5 vars. |

## Must-fix (before deploy)

- **M1 `Cargo.toml:13`** — the deployable carries an absolute path dep on another lane's live worktree
  (`/Users/johncalhoun/bsv/wt-middleware-lane/.claude/worktrees/mw-port`, at 6cf4d84 "0.3.6", clean at
  review time). The plan's R4/step 1 says never ship a path dep. Consequences today: the lock pins the
  middleware by nothing (no checksum), so "production builds what the tests ran" holds for `bsv-rs` but not
  for the auth crate; the build is not reproducible from any other checkout; CI cannot resolve it
  (`.github/workflows/ci.yml:15-16` says so). Before the deploy gate: flip to
  `bsv-middleware-cloudflare = "=0.3.6"` once published, regenerate `Cargo.lock` (expect only the
  middleware's `source`/`checksum` lines to change, `bsv-rs` stays 0.3.33, `worker` 0.8.7), and re-run the
  step-1 suite and `--dry-run` on that lock. Until then this branch should not merge to `main` with a red CI
  unless the owner accepts that explicitly.

## Should-fix

- **S1 `src/lib.rs:112-114` with `:391-399`** — for `status >= 500` send an opaque body (`{"status":"error","code":<error_code()>}`) instead of `e.to_json()`. Today an infrastructure failure returns the crate's description ("KV storage error: …", "Configuration error: …", "SDK error: …") to the client; main returned a bare 500. No key material can appear (checked above), so minor, but it is strictly more disclosure than before. Keep the full text in the `console_error!` only.
- **S2 `src/lib.rs:392-394`** — log the 401 refusals too, e.g. `console_warn!("auth refused: {}", e.error_code())`. Only `>= 500` is logged, and the crate logs nothing on its own refusal paths (replay, missing nonce, session-not-found: no `console_*` in `auth.rs:298-450`). The plan's deploy-gate item 6 and the 30-min `wrangler tail` watch for "Message identity key is not the session's" / `ERR_REPLAYED_REQUEST` / `ERR_SESSION_NOT_FOUND` cannot see any of these: the tail shows status codes, not bodies. One line; do it before the prod deploy or the gate's abort condition is unobservable. Do not log identity keys.
- **S3 `.github/workflows/ci.yml:15-16`** — CI is red until M1 lands (acknowledged in the comment). The cache key now hashes the committed lock (plan step 7), good.
- **S4 unbound Durable Object export** — the bundle exports the crate's `AuthSessionStore` class (`build/index.js`: `AuthSessionStore`, `__wbg_authsessionstore_free`; also in the dry-run `shim.js`). `wrangler deploy --dry-run` bundles it without complaint but does not validate against the API; the staging deploy is the first real check (plan R3). Note the plan's 1.9k-line lock estimate was close: 1,765 lines.
- **S5 `cargo clippy --all-targets -- -D warnings`** fails with 42 pre-existing test-only lints in `src/storage/create_action.rs`, `src/monitor.rs`, `src/services/multi.rs` — none in this diff, and CI runs the lib-only form, which is clean. Not a regression; worth a separate cleanup.
- **S6 (note, no action)** — the lane renamed the import to `bsv_middleware_cloudflare::` instead of the plan's package alias; equivalent and cleaner. Dropping `init_panic_hook()` is correct: worker 0.8.7 `src/init.rs` installs a panic hook in `#[wasm_bindgen(start)]` that logs `Critical`, sets `criticalError`, and `set_on_abort → schedule_reinit`.

## What I ran (worktree `.claude/worktrees/mw-wi-migrate`, branch `lane/mw-wi-migrate` @ ed11fed, clean)

| Step | Command | Result |
|---|---|---|
| Tests | `cargo test` | 823 unit tests: **822 passed, 0 failed, 1 ignored**; doctests 0/0/1. Base by diff = 818 (+4 new, −0). |
| Format | `cargo fmt --check` | clean |
| Lint (CI form) | `cargo clippy -- -D warnings` | clean (native) |
| Lint (wasm) | `cargo clippy --target wasm32-unknown-unknown -- -D warnings` | clean |
| Lint (all targets) | `cargo clippy --all-targets -- -D warnings` | 42 errors, all pre-existing test code (S5) |
| Build | `worker-build --release` (global worker-build 0.8.7, no install needed) | ok; `index_bg.wasm` 2,712,735 B after wasm-opt, `index.js` 31.3 kB |
| Dry-run | `npx wrangler deploy --dry-run --outdir /tmp/wi-dryrun` (wrangler 4.54.0) | ok; Total Upload 2702.61 KiB / gzip 959.71 KiB; bindings as on main |
| Lock audit | grep of `Cargo.lock` | one `bsv-rs` 0.3.33, one `worker` 0.8.7; middleware 0.3.6 path (no checksum) |
| Negative test (a) | local `wrangler dev --port 8799 --persist-to /tmp/wi-dev-state --var SERVER_PRIVATE_KEY:<openssl rand -hex 32>` (throwaway key, no secrets read; state and key deleted after) | **reproduced**, see below |

### Negative test (a) on local `wrangler dev` — reproduced

Recipe (no signing client needed: the crate refuses the foreign identity by name *before* verifying the
signature, `auth.rs:368-377`):

1. `POST /.well-known/auth` body `{"version":"0.1","messageType":"initialRequest","identityKey":V,"initialNonce":<b64 32B>}`
   with V = `02c6047f…9ee5` (2·G). → **200** `initialResponse`; the body's `initialNonce` is V's session nonce.
2. `POST /` JSON-RPC body with `x-bsv-auth-identity-key: A` (the server's own identity key — any valid key ≠ V),
   `x-bsv-auth-your-nonce: <V's session nonce>`, fresh `x-bsv-auth-nonce`, `x-bsv-auth-request-id` (b64 32B),
   `x-bsv-auth-signature: deadbeef`.
   → **401**, `Content-Type: application/json`, `Access-Control-Allow-Origin: *`,
   `{"code":"ERR_INVALID_AUTH","description":"Invalid authentication: Message identity key is not the session's","status":"error"}`.
   Same result with the signature header absent.

Controls on the same server: identity V + bogus signature → 401 `ERR_INVALID_AUTH` "Invalid message signature"
(proves step 2 failed on identity, not signature); no auth headers → 401 `UNAUTHORIZED` (crate path, unchanged);
unknown `your-nonce` → 401 `ERR_SESSION_NOT_FOUND`; malformed identity header → 401 "Invalid identity key: …";
`GET /` → 200 `{"status":"ok","service":"wallet-infra","broadcaster":"arcade"}`. No 500 in the dev log.

Not run here: (b) replay and (c) missing nonce need a correctly signed general message (the signature check
precedes the nonce checks) — staging client matrix. Also not run: KV session compatibility across crates
(plan §1.1, read-only claim), the authenticated client-family pass, and any deploy. A lane never deploys.
