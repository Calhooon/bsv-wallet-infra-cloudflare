# rust-wallet-infra

Self-hosted BSV wallet storage server running on Cloudflare Workers.

Built in Rust, compiled to WebAssembly. Backed by D1 (SQLite) for structured data and R2 for blob overflow. Authenticated via [BRC-31](https://brc.dev/31) mutual identity protocol.

Drop-in replacement for `storage.babbage.systems`.

---

## Architecture

```
                          POST / (JSON-RPC 2.0)
                          BRC-31 auth headers
                                 |
                                 v
                    +------------------------+
                    |   Cloudflare Worker     |
                    |   (Rust -> WASM)        |
                    +------------------------+
                    |  BRC-31 Auth Middleware  |
                    |  Session via KV          |
                    +------------------------+
                    |  JSON-RPC Dispatch       |
                    +------------------------+
                         /            \
                        v              v
               +------------+   +------------+
               |  D1 (SQLite)|   |  R2 (Blobs)|
               |  16 tables  |   |  > 4KB     |
               +------------+   +------------+
```

**Stack**: Rust `cdylib` -> `wasm32-unknown-unknown` -> Cloudflare Workers

**Storage**: D1 handles all structured data. Blobs larger than 4,096 bytes overflow to R2 with key format `{table}/{id}/{column}`.

**Auth**: Every request is authenticated via BRC-31 mutual authentication. Sessions are cached in KV with 1-hour TTL. Responses are signed with the server's identity key.

---

## API

All methods use **JSON-RPC 2.0** over `POST /`.

| Method | Auth | Description |
|---|---|---|
| `makeAvailable` | No | Health check — confirms D1 is initialized |
| `migrate` | No | Initialize settings row, returns chain (`"mainnet"`) |
| `findOrInsertUser` | No | Find or create user by identity key |
| `internalizeAction` | Yes | Accept external BSV transactions into the wallet |
| `listOutputs` | Yes | Query spendable outputs with basket/tag filtering |
| `listActions` | Yes | Query transactions with label filtering |

**Additional endpoints:**

| Method | Path | Description |
|---|---|---|
| `GET /` | — | Health check: `{"status":"ok","service":"wallet-infra"}` |
| `POST /.well-known/auth` | — | BRC-31 handshake (handled by middleware) |

### Example request

```json
{
  "jsonrpc": "2.0",
  "method": "listOutputs",
  "params": {
    "basket": "default",
    "include": "locking scripts",
    "limit": 25
  },
  "id": 1
}
```

---

## Data Model

16 tables in D1 covering the full BSV wallet storage schema:

| Domain | Tables |
|---|---|
| **Transactions** | `transactions`, `proven_txs`, `proven_tx_reqs`, `commissions` |
| **Outputs** | `outputs`, `output_baskets`, `output_tags`, `output_tags_map` |
| **Labels** | `tx_labels`, `tx_labels_map` |
| **Identity** | `users`, `certificates`, `certificate_fields` |
| **System** | `settings`, `sync_states`, `monitor_events` |

---

## Setup

### Prerequisites

- Rust toolchain with `wasm32-unknown-unknown` target
- [wrangler](https://developers.cloudflare.com/workers/wrangler/) CLI v3+
- Cloudflare account with D1, R2, and KV enabled
- `bsv-middleware-cloudflare` 0.3.6 (crates.io; a local path until it publishes, see `Cargo.toml`)

### 1. Install dependencies

```bash
rustup target add wasm32-unknown-unknown
cargo install --version ^0.8 worker-build   # worker 0.8 needs worker-build ^0.8
npm install
```

### 2. Configure Cloudflare resources

Create the D1 database, R2 bucket, and KV namespace, then update `wrangler.toml` with your IDs:

```bash
npx wrangler d1 create wallet-infra
npx wrangler r2 bucket create wallet-infra-blobs
npx wrangler kv namespace create AUTH_SESSIONS
```

### 3. Set the server private key

```bash
npx wrangler secret put SERVER_PRIVATE_KEY
```

### 4. Run migrations

```bash
npx wrangler d1 migrations apply wallet-infra
```

### 5. Initialize storage

Call `migrate` via JSON-RPC to create the settings row:

```bash
curl -X POST https://your-domain.com/ \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"migrate","params":{"storageName":"wallet-infra"},"id":1}'
```

---

## Development

```bash
# Local dev server with D1/R2 emulation
npm run dev

# Build WASM (release)
worker-build --release

# Deploy to Cloudflare
npm run deploy
```

---

## Project Structure

```
src/
  lib.rs                     # Worker entry point, auth, routing
  dispatch.rs                # JSON-RPC method router
  json_rpc.rs                # JSON-RPC 2.0 protocol types
  entities.rs                # Data model structs (16 tables)
  types.rs                   # Query/result types, auth, sync
  error.rs                   # Error enum
  r2.rs                      # R2 blob store (4KB threshold)
  d1/
    mod.rs                   # D1 query builder, WhereBuilder
    batch.rs                 # Atomic batch execution (100-stmt chunks)
  storage/
    mod.rs                   # StorageD1 struct
    writers.rs               # Write ops: migrate, users, baskets, labels
    readers.rs               # Read ops: listOutputs, listActions
    internalize_action.rs    # BEEF parsing, tx internalization
    beef_verification.rs     # Merkle proof verification
migrations/
  0001_initial.sql           # D1 schema (16 tables + indexes)
```

---

## Key Design Decisions

**D1 query builder** — Cloudflare D1 uses JsValue bindings, not sqlx. A custom `Query` builder with `WhereBuilder` handles parameterized queries with type-safe value binding.

**Batch atomicity** — D1 has no `BEGIN`/`COMMIT`. Instead, `db.batch()` executes up to 100 statements atomically. `BatchCollector` auto-chunks larger batches.

**Hybrid storage** — Small values (<=4KB) stay inline in D1 columns. Larger blobs (raw transactions, BEEF data) overflow to R2 for cost efficiency.

**Dual param format** — The dispatch layer accepts both positional arrays (from BSV Toolbox StorageClient) and named objects (from direct callers), normalizing them before passing to handlers.

---

## Dependencies

| Crate | Purpose |
|---|---|
| `bsv-middleware-cloudflare` | BRC-31 auth middleware |
| `bsv-sdk` (`bsv-rs` 0.3) | BSV primitives (BEEF, transactions, wallet types) |
| `worker` | Cloudflare Workers Rust SDK |
| `serde` / `serde_json` | Serialization |
| `chrono` | Date/time handling |
| `hex` / `base64` | Encoding |

---

## License

Private. All rights reserved.

## internalizeAction result extensions (non-normative)

`mergeResults` / `noop` on the internalizeAction result are NON-NORMATIVE
debug hints (a guarded merge UPDATE that changed 0 rows). The ecosystem
protocol expresses merge no-ops by silence; the real client mechanism is a
read-back (`listOutputs`) after internalize — do not build client behavior
on these fields. Broadcast-failure demotion is expressed canonically via
`sendWithResults` (`unproven`) + `notDelayedResults` (`serviceError`), per
wallet-toolbox `WalletStorage.interfaces.ts`.
