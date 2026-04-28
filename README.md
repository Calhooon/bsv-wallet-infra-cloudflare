# bsv-wallet-infra-cloudflare

Cloudflare Workers port of [`bsv-blockchain/wallet-infra`](https://github.com/bsv-blockchain/wallet-infra) — the BSV **UTXO Management Server** (also called "wallet storage server"). Rust compiled to WASM, packaged as a single Worker.

The upstream TypeScript implementation runs as an Express service backed by MySQL + Knex, gluing together the [`wallet-toolbox`](https://github.com/bsv-blockchain/wallet-toolbox) storage library, [`auth-express-middleware`](https://github.com/bitcoin-sv/auth-express-middleware), and [`payment-express-middleware`](https://github.com/bitcoin-sv/payment-express-middleware). This port collapses all of that into a single Cloudflare Worker:

| Concern | Upstream (TS) | This port (Rust/WASM) |
|---|---|---|
| Runtime | Node.js + Express | Cloudflare Workers (`wasm32-unknown-unknown`) |
| Storage library | [`wallet-toolbox`](https://github.com/bsv-blockchain/wallet-toolbox) | Reimplemented inline in `src/storage/` |
| Auth middleware | [`auth-express-middleware`](https://github.com/bitcoin-sv/auth-express-middleware) | [`bsv-middleware-cloudflare`](https://crates.io/crates/bsv-middleware-cloudflare) ([BRC-31](https://brc.dev/31)) |
| Payment middleware | [`payment-express-middleware`](https://github.com/bitcoin-sv/payment-express-middleware) | Same crate (BRC-29) |
| Structured data | MySQL + Knex | Cloudflare D1 (SQLite) |
| Blob storage | MySQL `LONGBLOB` | R2 with 4 KB overflow threshold |
| Session cache | Express memory / Redis | Cloudflare KV (1 h TTL) |
| Migrations | Knex auto-migrate | `wrangler d1 migrations apply` |
| Cron / monitor | Separate Node process | `#[event(scheduled)]` (`*/5 * * * *`) |

**Wire-compatible with `storage.babbage.systems`.** Same JSON-RPC method names, same param shapes, same response shapes. Clients built against the TS reference work unchanged against this server.

## What it does

- Accepts BRC-31-authenticated JSON-RPC requests over HTTPS
- Persists transactions, outputs, baskets, labels, and certificates in D1 (SQLite)
- Overflows blobs >4 KB (raw txs, BEEF, locking scripts) to R2
- Caches per-identity auth sessions in KV (1-hour TTL)
- Builds and verifies [BEEF](https://brc.dev/62) (BRC-62) for every action with full ancestor proofs
- Broadcasts via TAAL ARC + GorillaPool (parallel race), falls back to WhatsOnChain
- Issues automatic refunds via BRC-29 on upstream broadcast failures
- Re-broadcasts unconfirmed transactions on a 5-minute cron

## Architecture

```
                      POST / (JSON-RPC 2.0)
                      BRC-31 auth headers
                             │
                             ▼
                ┌──────────────────────────┐
                │  Cloudflare Worker       │
                │  Rust → wasm32           │
                ├──────────────────────────┤
                │  BRC-31 auth (KV cache)  │
                ├──────────────────────────┤
                │  JSON-RPC dispatch       │
                ├──────────────────────────┤
                │  StorageD1               │
                └────┬──────────────────┬──┘
                     │                  │
              ┌──────▼──────┐    ┌──────▼──────┐
              │ D1 (SQLite) │    │ R2 (blobs)  │
              │ 16 tables   │    │ >4 KB tier  │
              └─────────────┘    └─────────────┘
                     │
                ┌────▼─────────────────────┐
                │ ARC (TAAL + GorillaPool) │
                │ + WoC fallback           │
                └──────────────────────────┘
```

## Performance

Median end-to-end create_action + process_action latency for typical sends, measured against the live deploy:

| Operation | p50 | p95 | Notes |
|---|---:|---:|---|
| 1-input send | ~1.5 s | ~2.2 s | Dominated by ARC `SEEN_ON_NETWORK` wait |
| 10-input multi-input send | ~2.5 s | ~3 s | + sequential D1 UTXO locking |
| BEEF construction (1-input) | ~150 ms | ~200 ms | Includes ChainTracker root verification |
| ChainTracker validation (cached) | ~0 ms | ~80 ms | RootCache hits dominate steady-state |

Broadcast races TAAL ARC and GorillaPool ARC in parallel — the slow tail of one endpoint never blocks the other. The ARC `X-WaitFor: SEEN_ON_NETWORK` guarantee is preserved end-to-end. Per-phase timing for every `create_action` / `process_action` invocation is emitted as `BENCH <op>.<phase>: <ms> ms` console_log lines, capturable via `wrangler tail`.

## JSON-RPC API

All methods POST to `/` with a JSON-RPC 2.0 envelope. Auth via BRC-31 headers.

| Method | Auth | Purpose |
|---|---|---|
| `makeAvailable` | — | Health check; confirms D1 is reachable |
| `migrate` | — | Initialize `settings` row, returns chain (`mainnet`/`testnet`) |
| `findOrInsertUser` | — | Look up or create user by identity key |
| `internalizeAction` | ✓ | Accept an externally-built tx into the wallet (BRC-29 payment receipt) |
| `createAction` | ✓ | Create + sign + broadcast a tx (the heavy method) |
| `processAction` | ✓ | Process a pre-signed tx (broadcast + persist outputs) |
| `abortAction` | ✓ | Abort an unbroadcast tx, release locked inputs |
| `listOutputs` | ✓ | Query spendable outputs by basket / tags |
| `listActions` | ✓ | Query transactions by label |
| `getBalance` | ✓ | Aggregate spendable satoshis in a basket |
| `relinquishOutput` | ✓ | Mark an output as no longer the wallet's |
| `listCertificates` / `insertCertificate` / `relinquishCertificate` | ✓ | Identity certificate CRUD |
| `updateTransactionStatusAfterBroadcast` | ✓ | Status reconciliation hook |
| `reviewStatus` | ✓ | Monitor health summary |

Two non-RPC routes:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Liveness: `{"status":"ok","service":"wallet-infra"}` |
| `POST` | `/.well-known/auth` | BRC-31 handshake (handled by middleware) |

## Data Model

16 tables in `migrations/0001_initial.sql`:

| Domain | Tables |
|---|---|
| Transactions | `transactions`, `proven_txs`, `proven_tx_reqs`, `commissions` |
| Outputs | `outputs`, `output_baskets`, `output_tags`, `output_tags_map` |
| Labels | `tx_labels`, `tx_labels_map` |
| Identity | `users`, `certificates`, `certificate_fields` |
| System | `settings`, `sync_states`, `monitor_events` |

## Build & deploy

### Prerequisites

- Rust toolchain with `wasm32-unknown-unknown` target
- `cargo install worker-build`
- [`wrangler`](https://developers.cloudflare.com/workers/wrangler/) CLI v3+
- Cloudflare account with D1, R2, and KV enabled

### 1. Install

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build
npm install
```

### 2. Provision Cloudflare resources

```bash
npx wrangler d1 create wallet-infra
npx wrangler r2 bucket create wallet-infra-blobs
npx wrangler kv namespace create AUTH_SESSIONS
```

Copy the resulting IDs into `wrangler.toml` (`account_id`, `database_id`, KV `id`).

### 3. Set secrets

```bash
npx wrangler secret put SERVER_PRIVATE_KEY    # 64-char hex BRC-31 server key
npx wrangler secret put ARC_API_KEY           # optional: TAAL ARC API key
npx wrangler secret put WOC_API_KEY           # optional: WhatsOnChain API key
```

### 4. Run migrations

```bash
npx wrangler d1 migrations apply wallet-infra
```

### 5. Develop / deploy

```bash
npm run dev              # Local dev server (D1/R2/KV emulated)
worker-build --release   # Build WASM
npm run deploy           # Deploy to Cloudflare
```

### 6. Initialize the settings row

After first deploy, call `migrate` once over JSON-RPC (using a BRC-31 client) to create the `settings` row.

## Configuration

`wrangler.toml`:

| Variable | Default | Purpose |
|---|---|---|
| `CHAINTRACKS_URL` | `https://chaintracks.example.com` | Chaintracker for merkle root verification (override with your own deploy or another public chaintracks instance) |
| `BEEF_VERIFICATION` | `strict` | `strict` / `log_only` / `skip` — gates the post-BEEF chaintracker check |

Secrets (set via `wrangler secret put`):

| Secret | Required | Purpose |
|---|---|---|
| `SERVER_PRIVATE_KEY` | yes | BRC-31 server identity (64-char hex) |
| `ARC_API_KEY` | no | TAAL ARC API key (broadcast acceleration) |
| `WOC_API_KEY` | no | WhatsOnChain API key (fallback broadcast / proof lookup) |

## Project layout

```
src/
  lib.rs                       Worker entry, auth, routing, scheduled cron
  bench.rs                     Per-phase BENCH timing helper
  dispatch.rs                  JSON-RPC method router
  json_rpc.rs                  JSON-RPC 2.0 protocol types
  entities.rs                  Data model structs (16 tables)
  types.rs                     Query/result types, auth, sync
  error.rs                     Error enum
  r2.rs                        R2 blob store (4 KB inline / overflow threshold)
  audit.rs                     Read-only health audit (spendable contradictions, etc.)
  monitor.rs                   Scheduled re-broadcast + proof reconciliation
  d1/
    mod.rs                     D1 query builder + WhereBuilder
    batch.rs                   Atomic batch execution (100-stmt chunks)
  storage/
    mod.rs                     StorageD1 struct
    writers.rs                 Write ops: migrate, users, baskets, labels
    readers.rs                 listOutputs, listActions, getBalance
    create_action.rs           Tx creation, BEEF construction, ChainTracker verify
    process_action.rs          Sign + broadcast + persist outputs
    internalize_action.rs      External tx ingest (BRC-29 payment receipt)
    abort_action.rs            Release locked inputs on abandoned actions
    relinquish_output.rs       Mark output as no longer ours
    certificates.rs            Identity certificate CRUD
    beef_verification.rs       Merkle proof + BEEF structural validation
  services/
    mod.rs                     BroadcastService + ProofService traits
    arc.rs                     TAAL/GorillaPool ARC client (parallel race)
    woc.rs                     WhatsOnChain client
    bitails.rs                 Bitails proof lookup fallback
    chaintracker.rs            Block header / merkle root verifier (FallbackChainTracker)
    multi.rs                   MultiProvider — fan-out across ARC/WoC/Bitails
migrations/
  0001_initial.sql             16-table D1 schema
  0002_add_indexes.sql         Performance indexes
tests/e2e/                     Shell-script smoke tests against a deployed worker
```

## Key design decisions

**D1 query builder.** Cloudflare D1 uses `JsValue` bindings rather than `sqlx`. A custom `Query` + `WhereBuilder` provides type-safe parameter binding without pulling in a heavyweight ORM.

**Batch atomicity.** D1 has no `BEGIN`/`COMMIT`. `db.batch()` executes up to 100 statements atomically. `BatchCollector` auto-chunks larger batches and rolls per-chunk on partial failure.

**Hybrid storage.** Values ≤4 KB stay inline in D1 columns. Larger blobs (raw transactions, BEEF, custom locking scripts) overflow to R2 keyed by `{table}/{id}/{column}`. The D1 column stores a sentinel byte to indicate R2 overflow.

**Dual param format.** Dispatch accepts both positional arrays (from `bsv-wallet-toolbox-rs` `StorageClient`) and named objects (from direct JSON-RPC clients), normalizing before invoking handlers.

**Parallel broadcast.** ARC TAAL and ARC GorillaPool fire concurrently via `futures_util::FuturesUnordered`. The first definitive result wins (Ok / DoubleSpend / InvalidTx); transient errors wait for the other endpoint. Both run with `X-WaitFor: SEEN_ON_NETWORK` so the propagation guarantee is identical to a single-endpoint call.

**ChainTracker pluggability.** Default deploy uses an external chaintracks worker over HTTP. `FallbackChainTracker` wraps the primary with a WoC fallback and an in-memory `RootCache` so repeated proofs at the same height resolve in microseconds.

## Dependencies

| Crate | Source | Purpose |
|---|---|---|
| [`bsv-middleware-cloudflare`](https://crates.io/crates/bsv-middleware-cloudflare) | crates.io | BRC-31 auth + BRC-29 payment middleware |
| [`bsv-rs`](https://crates.io/crates/bsv-rs) | crates.io | BSV primitives (BEEF, transactions, wallet types) |
| [`worker`](https://crates.io/crates/worker) | crates.io | Cloudflare Workers Rust SDK |
| `futures-util` | crates.io | Concurrent futures (ARC endpoint race) |
| `serde` / `serde_json` | crates.io | Serialization |
| `chrono` | crates.io | Timestamps |
| `hex` / `base64` | crates.io | Encoding |

No path deps. Everything resolves from crates.io.

## License

[MIT](LICENSE).
