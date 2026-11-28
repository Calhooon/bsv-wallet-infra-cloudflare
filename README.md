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
| `GET /healthz` | none | `{"status":"ok","build":{...}}`, the build object being `lib_rev`, `built_at`, `dirty` and `crates` (the locked versions of bsv-rs, the middleware and this crate); `Cache-Control: no-store` |
| `GET /robots.txt` | none | `User-agent: *`, `Disallow: /` |
| `POST /arcade/callback` | `Authorization: Bearer <ARCADE_CALLBACK_TOKEN>`; 404 when the secret is unset | Arcade's status webhook |
| `GET /monitor/status` | `?key=<MONITOR_TRIGGER_KEY>` | aggregate monitor counts |
| `POST /monitor/run` | `?key=<MONITOR_TRIGGER_KEY>` | one monitor run, now |
| `POST /monitor/reorg-sweep` | `?key=<MONITOR_TRIGGER_KEY>`, `&from=&to=` | a deep reorg sweep over at most 500 heights |
| `GET /monitor/probe-woc` | `?key=<MONITOR_TRIGGER_KEY>`, `&txid=` | the raw WhatsOnChain proof answer |
| `GET /monitor/audit` | `?key=<MONITOR_TRIGGER_KEY>` | the UTXO audit, `?level=` (default 2) |
| `POST /` | BRC-31 session (`SERVER_PRIVATE_KEY`, `AUTH_SESSIONS`) | JSON-RPC, signed responses |

The door's policies (#22): an in-flight ceiling of 24 per isolate (503 with
`Retry-After: 1` beyond it), bodies and responses bounded at 8 MiB (413),
list limits of 1,000, a caller's BEEF measured before it is parsed (one over
the parse's budget, 24 MiB of heap, answered `-32602` as too large to process;
`door::beef_measure`), and two rate limits counted by the platform's
rate-limit bindings, 300 a minute per address before authentication and
1,000 a minute per identity after it (429 with the draft-8 `RateLimit`
headers). The bindings are approximate by the platform's own design: each
running instance checks a local count and the counts synchronize in the
background, so a flood can pass the line before they agree (#59). The
address limit of record is therefore the zone's firewall rule in front of
the Worker (#56), exact per window and enforced before any invocation; the
binding is the soft inner rail, and `GET /` names each binding `bound` or
`absent`. The exact per-identity rail belongs beside the credit in the
wallet's store (#54, #28), and the daily budget switch (#55) is the hard
bound.

### A payment's bytes

A door that takes a payment's bytes from a caller reads at most the figure in
force of them (the BEEF or the transaction as bytes, whatever encoding
carried it) and refuses a larger one before it parses or verifies any of it
and before anything is written (`door::payment`). The figure is the Worker's
setting `MAX_PAYMENT_BYTES`, a var in bytes; where it is missing, zero or not
a whole number it is 614,400 bytes (600 KiB). A zero or malformed value is
named once in the log (`setting.malformed MAX_PAYMENT_BYTES is not a
positive whole number of bytes; the figure in force is 614400`), and
`GET /` shows the figure in force as `max_payment_bytes`. An ordinary payment
is about a kilobyte (in 727 real payments the median was 1.2 KB and the
largest 434 KB); an operator who must take a larger one raises the setting. A
Worker's isolate is memory-limited and a door bounds what it reads. The
doors are `internalizeAction`'s AtomicBEEF inline (`tx`, a JSON array of
bytes or a hex string), `processAction`'s signed transaction (`rawTx`), and
`internalizeAction`'s BEEF at rest (`beefAtRest`, held on the length its
reference names, which the object's head must match, before the object is
opened). The arguments may be named (`{"tx": ...}`) or positional (an array
in the method's field order: `tx` first, `rawTx` seventh); in either form the
payment is held as the request is decoded, before the caller's user is
looked up or anything is written. An array is decoded no further than the
figure and a hex string is held on its length before it is decoded; a
request's other arguments keep their own limits. The request's bound of
1,000,000 array items is unchanged: an array between the figure and that
bound is refused by the figure. The figure is provisional: it is read again
at this crate's next bsv-rs pin and raised to the door's natural figure then.

The refusal is the JSON-RPC error of a refused argument, `-32602`, its
`message` one of two sentences with the door's values, and its `data` the
typed code with the figure in force and, where the door knows it, the size:

```json
{"code": -32602,
 "message": "Not judged: this payment is 614401 bytes, and this host reads at most 614400 bytes of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block.",
 "data": {"code": "ERR_PAYMENT_SIZE", "budget": 614400, "size": 614401}}
```

Where the payment is an array whose length is read only as it is decoded,
`size` is absent and the message reads `Not judged: this payment is more than
614400 bytes, the most this host reads of a payment: ...` with the same
close. Each refusal is counted by one log line:
`payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=<bytes or none> budget=<the figure> identity_key=<the caller's public identity key>`.
The reference names no such figure: its storage server holds a request's
JSON and binary bodies at 8 MiB and its arrays at 1,000,000 items in the
standard profile (bsv-blockchain/ts-stack@57c5f13,
`StorageServer.ts:547-558, 561-566, 402-408`); this host differs on purpose.

An answer a provider sends is read no further than twice the figure (its
bytes travel as hex) and 64 KiB for its JSON wrapping: an answer that declares
a longer `Content-Length` is not read, and one that passes the bound as it
arrives is not read on. What the Worker hands to bsv-rs is then held to the
same figure before it is parsed: a merkle proof from ARC (its `merklePath`),
from WhatsOnChain or Bitails (the TSC answer, whose nodes travel as hex, at
half its length), a raw transaction from WhatsOnChain (`/tx/{txid}/hex`), and
the merkle path the broadcaster's callback (`/arcade/callback`) carries or
its stored record names. Over the figure the answer is not parsed and that
provider is taken as unavailable at that moment, the road a provider's error
already takes (the next provider is asked, the request is held for the
monitor's next run); it is never a verdict on a transaction. Each is counted
by one log line:
`provider.answer_not_read provider=<arc|woc|bitails|arcade> answer=<proof|raw_tx> size=<bytes> budget=<the figure>`,
or, for an answer not read past the bound,
`provider.answer_not_read provider=<...> answer=<proof|raw_tx|record|callback> <declared|read>=<bytes> bound=<bytes>`.

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
Arcade's `400` carrying `status` 460 to 475 is its policy refusal: the
request's history records the code, the instance and the fee its `/policy`
asks, the inputs stay held and the monitor announces the transaction again
([src/services/policy_refusal.rs](src/services/policy_refusal.rs)). Its 466
is a conflict hint, recorded the same way with no fee asked; no word is
written until a competitor's checked proof.

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
