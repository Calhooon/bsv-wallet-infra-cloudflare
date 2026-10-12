# bsv-wallet-infra-cloudflare

Self-hosted BSV wallet storage on Cloudflare Workers. JSON-RPC dispatch uses
D1 for structured wallet state, R2 for blob storage and BRC-31 middleware for
identity authentication. The public repository is a sanitized release mirror;
changes are developed on canonical branches and cut by the maintainer.

## Version and architecture

The source line is **fb8237f**, with **bsv-rs =0.4.5** and
**bsv-middleware-cloudflare =0.5.1** in [Cargo.toml](Cargo.toml). The local package
version is 0.1.0; the source revision identifies this Worker release.

[src/lib.rs](src/lib.rs) handles HTTP and authentication,
[src/dispatch.rs](src/dispatch.rs) routes JSON-RPC methods, and
[src/storage](src/storage) implements wallet reads and writes.
The dispatch accepts positional arrays and named objects. Authentication and
ownership requirements belong to each method; a health response is not an
assertion that every RPC is public or every RPC requires authentication.

## The server half of tracking

A broadcaster's refusal is a hint that schedules another check. It does not
by itself make a transaction invalid, release its inputs or write off its
change. Attempt counts are not evidence of chain rejection. The monitor keeps
the transaction pending while recording hints and checking evidence;
[src/monitor.rs](src/monitor.rs) holds that behavior.

The authenticated `findProvenTxReqs` method is served by
[src/dispatch.rs](src/dispatch.rs), scoped to the caller's user. It lets the
wallet read its pending proof requests from shared storage. The server and
local toolbox must preserve the same distinction between hints and proof.

## Bytes at rest

The `BEEF_AT_REST` R2 binding lets the server read a relay's referenced payment
object during internalization. The binding must name the bucket holding those
bytes; the server checks recipient metadata and the referenced object before
acceptance. Merely declaring the binding does not create the relay's objects.
This is separate from wallet blob overflow storage. The compiled witness is
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
that proof.

For deployment, build from a clean release checkout, provision the D1, KV and
R2 bindings in [wrangler.toml](wrangler.toml), configure credentials privately,
and apply [migrations](migrations) before serving the Worker. Bind the at-rest
bucket only when the producer and recipient contract agree. The operator owns
resource creation, deployment and initialization; no deployment identifiers
belong in this README.

[CHANGELOG.md](CHANGELOG.md) records changes and [tests](tests) contains the
host and Worker witnesses. [docs/E2E_TEST_HARNESS_DESIGN.md](docs/E2E_TEST_HARNESS_DESIGN.md)
describes the integration harness. Licensing metadata is in [Cargo.toml](Cargo.toml).
