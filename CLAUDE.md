# bsv-wallet-infra-cloudflare

Cloudflare Workers port of [`bsv-blockchain/wallet-infra`](https://github.com/bsv-blockchain/wallet-infra) (TypeScript Express + MySQL, built on [`wallet-toolbox`](https://github.com/bsv-blockchain/wallet-toolbox)). Rust → WASM, single Worker. JSON-RPC 2.0, BRC-31 auth, BRC-29 payments. Wire-compatible with `storage.babbage.systems`.

Mapping: TS Express → CF Worker, MySQL → D1 (SQLite), MySQL `LONGBLOB` → R2 (4 KB overflow), in-memory session → KV (1 h TTL), Knex migrations → `wrangler d1 migrations apply`, separate cron process → `#[event(scheduled)]`.

## Build & Run

```bash
npm install                                                # wrangler
npm run dev                                                # local dev (D1/R2/KV emulated)
worker-build --release                                     # build WASM
npm run deploy                                             # deploy

npx wrangler d1 migrations apply wallet-infra              # remote
npx wrangler d1 migrations apply wallet-infra --local      # local
```

Build target: `wasm32-unknown-unknown`. Crate type: `cdylib`. Output: `build/worker/shim.mjs`.

## Architecture

```
Request → lib.rs (fetch) → BRC-31 auth → dispatch.rs → storage/ → D1 + R2
                                                        ↓
                                                   services/ → ARC + WoC + chaintracker
Cron (*/5 min) → lib.rs (scheduled) → monitor.rs → proof reconciliation + re-broadcast
```

- **lib.rs** — `#[event(fetch)]` entry. CORS preflight, `/` health check, `/.well-known/auth` handshake, then BRC-31 verify + JSON-RPC dispatch. `#[event(scheduled)]` runs the monitor cron.
- **dispatch.rs** — Routes JSON-RPC method names to handlers. Normalizes both positional arrays (from `bsv-wallet-toolbox-rs` `StorageClient`) and named objects.
- **bench.rs** — `BenchTimer` helper. Emits `BENCH <op>.<phase>: <ms> ms` lines for capture via `wrangler tail`. Production-wide instrumentation, zero cost when no one's tailing.
- **storage/** — D1 + R2 operations. One file per JSON-RPC method that mutates state.
- **services/** — Outbound HTTP clients (ARC, WoC, Bitails, chaintracks) behind `BroadcastService` + `ProofService` traits, composed via `MultiProvider`.
- **monitor.rs** — Cron monitor: fetches missing proofs, re-broadcasts unconfirmed txs, fails abandoned actions, reconciles status mismatches.
- **d1/** — Custom query builder for D1's `JsValue` binding model + `BatchCollector` for atomic 100-statement chunks.

## Cloudflare Bindings

| Binding | Type | Purpose |
|---|---|---|
| `DB` | D1 | All structured data (16 tables) |
| `BLOBS` | R2 | Blobs >4 KB (raw txs, BEEF, custom locking scripts) |
| `AUTH_SESSIONS` | KV | BRC-31 session cache (1h TTL) |
| `CHAINTRACKS_URL` | Var | URL of a chaintracks-cloudflare worker for merkle root verification |
| `BEEF_VERIFICATION` | Var | `strict` / `log_only` / `skip` |
| `SERVER_PRIVATE_KEY` | Secret | 64-char hex BRC-31 server identity |
| `ARC_API_KEY` | Secret | Optional TAAL ARC key for broadcast acceleration |
| `WOC_API_KEY` | Secret | Optional WhatsOnChain key for fallback / proof lookup |

## JSON-RPC Methods

| Method | Auth | Notes |
|---|---|---|
| `makeAvailable` | — | Health check |
| `migrate` | — | Initialize settings row, returns chain |
| `findOrInsertUser` | — | Lookup/create by identity key |
| `internalizeAction` | ✓ | Accept external tx (BRC-29 payment receipt or basket insertion) |
| `createAction` | ✓ | UTXO selection + change calc + BEEF construction (heavy) |
| `processAction` | ✓ | Sign + broadcast + persist (heavy; broadcasts to ARC) |
| `abortAction` | ✓ | Cancel unbroadcast tx, release locked inputs |
| `listOutputs` | ✓ | Spendable outputs by basket / tags |
| `listActions` | ✓ | Transactions by label |
| `getBalance` | ✓ | Aggregate spendable sats in a basket |
| `relinquishOutput` | ✓ | `basket_id = NULL` on a previously-tracked output |
| `listCertificates` / `insertCertificate` / `relinquishCertificate` | ✓ | Identity certificate CRUD |
| `updateTransactionStatusAfterBroadcast` | ✓ | Status reconciliation hook |
| `reviewStatus` | ✓ | Monitor health summary |
| `beginStorageTransaction` / `commitStorageTransaction` / `rollbackStorageTransaction` | — | Stubs (D1 has no real transactions) |

`AuthId` resolves to `user_id` via `resolve_auth()`, auto-creating the user row on first call.

## Cron Monitor

Runs every 5 minutes (`crons = ["*/5 * * * *"]` in wrangler.toml). Three tasks per tick:

1. **check_for_proofs** — Queries `proven_tx_reqs` with pending statuses, fans out to ARC → WoC → Bitails for canonical-chain merkle proofs, inserts into `proven_txs`, flips status → `completed`.
2. **fail_abandoned** — Marks outgoing txs stuck in `unsigned` / `unprocessed` for >30 min as `failed`, releases locked UTXOs.
3. **review_status** — Fixes mismatches where `proven_tx_req='completed'` but `transaction!='completed'` (rare, but happens after partial-failure recoveries).

Results logged to the `monitor_events` table.

## Key Patterns

- **D1 numeric types** — D1 returns all numbers as JS floats. Entity structs use `Option<f64>`, cast to `i64` / `i32` as needed.
- **Blob overflow** — `BlobStore` in `r2.rs` uses a 4,096-byte threshold. ≤4 KB stays inline in D1; >4 KB lands in R2 keyed `{table}/{id}/{column}`. The D1 column stores a sentinel.
- **Batch atomicity** — D1 has no `BEGIN`/`COMMIT`. `BatchCollector` calls `db.batch()` for atomic execution, auto-chunking at the 100-stmt limit.
- **No tokio** — WASM Workers use `wasm-bindgen-futures`. Concurrency primitives come from `futures-util` (e.g. `FuturesUnordered` for the ARC race).
- **Datetime parsing** — `parse_datetime()` in `writers.rs` handles both RFC 3339 and D1's `CURRENT_TIMESTAMP` format (`YYYY-MM-DD HH:MM:SS`).
- **Dual param format** — `extract_args()` in `dispatch.rs` normalizes positional `[auth, args]` arrays and named `{field: value}` objects into one shape.
- **BEEF building** — `create_action::build_input_beef` does a BFS walk of inputs' ancestors. Three-tier local lookup (`proven_txs` → `transactions` → `proven_tx_reqs`), R2 fallback for blob-overflowed columns, network fallback (WoC) when local misses, then optional `ChainTracker` verification gated on `BEEF_VERIFICATION`.
- **Parallel broadcast** — `arc_broadcast_with_failover` fires both ARC endpoints (TAAL + GorillaPool) concurrently via `FuturesUnordered`. First definitive result wins; transient errors wait for the other endpoint. Both still wait for `X-WaitFor: SEEN_ON_NETWORK`.
- **ChainTracker cache** — `FallbackChainTracker.RootCache` memoizes `(height, root) → bool` results so repeated proofs at the same height resolve in microseconds. Cold first call costs ~40-200 ms (HTTP to chaintracks); warm calls are free.
- **UUID generation** — Custom v4 via `getrandom` (avoids `uuid` crate bloat). Zero UUID is `00000000-0000-4000-8000-000000000000`.

## Database Schema

16 tables defined in `migrations/0001_initial.sql`, indexes in `0002_add_indexes.sql`. Key tables:

| Table | Notes |
|---|---|
| `users` | Identity-key keyed |
| `transactions` | Status: `completed` / `unprocessed` / `sending` / `unproven` / `unsigned` / `nosend` / `nonfinal` / `failed` |
| `outputs` | UTXOs with `basket_id`, `derivation_*`, `locking_script`, `change` flag |
| `output_baskets` | `default` basket = the change basket; created on demand |
| `proven_txs` | Confirmed txs with merkle proofs |
| `proven_tx_reqs` | Broadcast queue; status-tracked by monitor |
| `certificates` / `certificate_fields` | Identity certificates |
| `tx_labels` / `output_tags` | Many-to-many tagging via map tables |
| `monitor_events` | Cron observability |

## Quality Gates (no CI — you enforce)

```bash
cargo fmt --all                                                   # format
cargo clippy --target wasm32-unknown-unknown -- -D warnings       # zero warnings
cargo check --target wasm32-unknown-unknown                       # compiles to WASM
cargo test --lib                                                  # all unit tests pass (708+)
worker-build --release                                            # WASM binary builds
```

If any gate fails, fix it before moving on.

## Performance Instrumentation

`BENCH` lines are emitted on every `create_action` / `process_action` invocation. Capture via:

```bash
npx wrangler tail --format=json > /tmp/wi.log &
# ...generate traffic...
grep BENCH /tmp/wi.log | sed 's/^[[:space:]]*"//; s/"$//'
```

Phases instrumented:

```
create_action.{setup, allocate_inputs[n=N], persist_outputs, build_beef[n=N]}
build_input_beef.{bfs_walk[n=N], bump_repair, verify_structure[n=N], verify_chaintracker[n=N]}
chaintracker.is_valid_root[h=H]
process_action.{setup, write_batch_and_req, broadcast}
arc.post_tx[host,http=N]
arc.race_winner[host]
broadcast.arc[beef|raw,outcome=ok|...,bytes=N]
```

Each phase emits `BENCH <op>.<phase>: <ms> ms` plus a `<op>.total: <ms>` summary at function exit. Cost per `lap()` is ~µs (one `js_sys::Date::now()`). See [`bench.rs`](src/bench.rs).

## Conventions

- Entities: `#[serde(rename_all = "camelCase")]`
- Timestamps: `chrono::DateTime<Utc>`
- Error → JSON-RPC code: `ValidationError` → `-32602`, `NotFound` → `-32001`, `DatabaseError` / `InternalError` → `-32603`
- BRC-31 server identity: stored as 64-char hex secret in `SERVER_PRIVATE_KEY`
- All BSV broadcasts use `X-WaitFor: SEEN_ON_NETWORK` (do not weaken — see comment in `services/arc.rs`)

## Dependencies

| Crate | Source | Purpose |
|---|---|---|
| `bsv-middleware-cloudflare` (aliased as `bsv-auth-cloudflare`) | crates.io | BRC-31 auth + BRC-29 payment middleware |
| `bsv-rs` (aliased as `bsv-sdk`) | crates.io | BSV primitives |
| `worker` | crates.io | Cloudflare Workers SDK |
| `futures-util` | crates.io | `FuturesUnordered` for the ARC race |

No path deps. Repo builds standalone from a fresh clone.
