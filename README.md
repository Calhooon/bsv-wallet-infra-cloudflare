# bsv-wallet-infra-cloudflare

Self-hosted BSV wallet storage on Cloudflare Workers. JSON-RPC dispatch uses
D1 for structured wallet state, R2 for blob storage and BRC-31 middleware for
identity authentication.

## Version and architecture

The crate is `bsv-wallet-infra-cloudflare` 0.6.0 in [Cargo.toml](Cargo.toml),
with **bsv-rs 0.8.1** (the caret) and **bsv-middleware-cloudflare =0.7.2**
(core 0.4.1). Its library keeps the identifier `rust_wallet_infra`. The newest
tag is `v0.5.0`; 0.6.0 is released when `v0.6.0` is tagged (see Releases below).

[src/lib.rs](src/lib.rs) handles HTTP and authentication,
[src/dispatch.rs](src/dispatch.rs) routes JSON-RPC methods, and
[src/storage](src/storage) implements wallet reads and writes.
The dispatch accepts positional arrays and named objects. Authentication and
ownership requirements belong to each method; a health response is not an
assertion that every RPC is public or every RPC requires authentication.

## Routes

From the fetch handler in [src/lib.rs](src/lib.rs):

| method and path | gate | answer |
|---|---|---|
| `OPTIONS` any path | none | CORS preflight |
| `GET /` | none | health: `status`, `service`, and the `broadcaster` selected by `BROADCASTER` |
| `POST /arcade/callback` | `Authorization: Bearer <ARCADE_CALLBACK_TOKEN>`; 404 when the secret is unset | Arcade's status webhook |
| `GET /monitor/status` | `?key=<MONITOR_TRIGGER_KEY>` | aggregate monitor counts |
| `POST /monitor/run` | `?key=<MONITOR_TRIGGER_KEY>` | one monitor run, now |
| `POST /monitor/reorg-sweep` | `?key=<MONITOR_TRIGGER_KEY>`, `&from=&to=` | a deep reorg sweep over at most 500 heights |
| `GET /monitor/probe-woc` | `?key=<MONITOR_TRIGGER_KEY>`, `&txid=` | the raw WhatsOnChain proof answer |
| `GET /monitor/audit` | `?key=<MONITOR_TRIGGER_KEY>` | the UTXO audit, `?level=` (default 2) |
| `POST /` | BRC-31 session (`SERVER_PRIVATE_KEY`, `AUTH_SESSIONS`) | JSON-RPC, signed responses |

The BRC-31 handshake is answered by the middleware; an authenticated request to any other path is a 404. The
cron trigger in [wrangler.toml](wrangler.toml) (`*/5 * * * *`) runs the monitor.

## Bindings and settings

[wrangler.toml](wrangler.toml) binds D1 `DB`, R2 `BLOBS` and `BEEF_AT_REST`,
and KV `AUTH_SESSIONS`. Its `[vars]` are `CHAINTRACKS_URL`,
`BEEF_VERIFICATION`, `INTERNALIZE_ZERO_CONF`, `BROADCASTER`,
`ARCADE_CALLBACK_URL` and `BEEF_AT_REST_BUCKET`. The code also reads
`ARCADE_URL`, `WOC_API_KEY`, `ARCADE_CALLBACK_TOKEN`, `MONITOR_TRIGGER_KEY` and
`SERVER_PRIVATE_KEY` when they are set as secrets.

The JSON-RPC `migrate` writes nothing: it answers the stored chain, as the
reference ignores it. The operator initializes the `settings` row once, after
the migrations, with the server's identity key:
`INSERT INTO settings (storage_identity_key, storage_name, chain, dbtype, max_output_script) VALUES ('<server-identity-key>', 'wallet-infra', 'mainnet', 'D1', 10000);`

## The server half of tracking

A broadcaster's refusal is a hint that schedules another check. It does not
by itself make a transaction invalid, release its inputs or write off its
change. Attempt counts are not evidence of chain rejection. The monitor keeps
the transaction pending while recording hints and checking evidence;
[src/monitor.rs](src/monitor.rs) and
[src/storage/tracker_rule.rs](src/storage/tracker_rule.rs) hold that behavior.

The authenticated `findProvenTxReqs` method is served by
[src/dispatch.rs](src/dispatch.rs), scoped to the caller's user. It lets the
wallet read its pending proof requests from shared storage. The server and
local toolbox must preserve the same distinction between hints and proof.

## Bytes at rest

The `BEEF_AT_REST` R2 binding lets the server read a relay's referenced payment
object during internalization. `BEEF_AT_REST_BUCKET` names the bucket holding
those bytes, and a reference naming any other bucket is refused; the server
checks recipient metadata and the referenced object before acceptance. Merely
declaring the binding does not create the relay's objects. This is separate
from wallet blob overflow storage. The compiled witness is
[tests/worker_beef_at_rest.mjs](tests/worker_beef_at_rest.mjs).

## Build, gate and deploy

```sh
cargo build --target wasm32-unknown-unknown --lib
cargo fmt --all -- --check
```

[.github/workflows/ci.yml](.github/workflows/ci.yml) defines `test`, `fmt`,
`clippy`, `wasm32` and `worker` on PR heads and main pushes. It runs host tests,
host and wasm clippy, builds the Worker with worker-build 0.8.4, and runs the
Miniflare bytes-at-rest witness with the JavaScript SDK. The Miniflare test
checks the compiled Worker and its D1/R2/auth behavior; a native build is not
that proof. The same job replays the route fixtures
([tests/worker_route_fixtures.mjs](tests/worker_route_fixtures.mjs)): every
HTTP route and JSON-RPC method against its recorded answer under
[tests/fixtures/routes](tests/fixtures/routes), failing by case name on any
difference; `UPDATE_FIXTURES=1` re-records them, and a behaviour change commits
its fixtures' diff on its own.

Dependabot moves the stack's crates twice a day, at 06:00Z and 18:00Z, under
[.github/dependabot.yml](.github/dependabot.yml).

For deployment, build from a clean release checkout, provision the D1, KV and
R2 bindings in [wrangler.toml](wrangler.toml), configure credentials privately,
and apply [migrations](migrations) before serving the Worker. Bind the at-rest
bucket only when the producer and recipient contract agree. The operator owns
resource creation, deployment and initialization; no deployment identifiers
belong in this README.

## Releases

The tag is the release of record; the repository's Releases page carries one
Release per tag. A `v*` tag push runs
[.github/workflows/release.yml](.github/workflows/release.yml): it checks the
tag against the version in [Cargo.toml](Cargo.toml), runs the gate of `ci.yml`,
and publishes `bsv-wallet-infra-cloudflare` to crates.io through trusted
publishing, so from 0.4.2 on the crate on crates.io carries the tag's version.
The Worker in production is deployed from the tag by the stack's infra, not by
this workflow. Earlier crates.io entries under this name are 0.3.0 and 0.3.1,
both of 2026-07-14; the tags `v0.4.0` and `v0.4.1` were not published there.

[CHANGELOG.md](CHANGELOG.md) records changes and [tests](tests) contains the
host and Worker witnesses. [docs/E2E_TEST_HARNESS_DESIGN.md](docs/E2E_TEST_HARNESS_DESIGN.md)
describes the integration harness. The licence is MIT, in [LICENSE](LICENSE).
