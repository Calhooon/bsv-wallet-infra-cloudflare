# Changelog

The tag is the release of record. From 0.4.2 on, the crate on crates.io,
`bsv-wallet-infra-cloudflare`, carries the tag's version; entries before it
are named by their program and their date, and the manifest then read 0.1.0.

## 0.7.1, 2026-11-17: the Extended Format conversion writes from the spent output; /healthz serves the build

### Changed

- **The Extended Format conversion** (`services::arcade::beef_to_ef_batch`,
  the Arcade broadcast) writes each input from its own fields and the
  satoshis and locking script of the one output it spends, read in place
  from the source transaction the BEEF holds; it no longer copies source
  transactions. Each transaction's bytes are unchanged: the tests pin them
  against bsv-rs `Transaction::to_ef` over the linked transaction, for every
  BEEF fixture in the tree and 600 generated graphs. A transaction the BEEF
  carries twice is written once, and a batch whose transactions spend one
  outpoint twice is refused (`inconsistent BEEF`); every other refusal keeps
  its words.

### Added

- **`GET /healthz` serves the build object** beside its status:
  `build: { lib_rev, built_at, dirty, crates }`, the crates being bsv-rs,
  bsv-middleware-cloudflare, bsv-middleware-core and this crate, at the
  versions the lock names, as the stack's other Workers serve it. Captured at
  build time by `build.rs` through `bsv-worker-build-info` `=0.4.0` (new;
  the lock adds that package only). `GET /` is unchanged.

## 0.7.0, 2026-11-17: bsv-rs 0.8.11, and the work main carried past 0.6.0

### Changed

- **bsv-rs `0.8.11`** (the caret, from `0.8.1`), the newest 0.8 on the index;
  bsv-middleware-cloudflare stays `=0.7.2` (core 0.4.1) and the lock holds one
  bsv-rs. The lock moves bsv-rs and the dependencies its 0.8.3 brought, nothing
  else: k256 and p256 to 0.14 (RUSTSEC-2026-0387), with ecdsa 0.17,
  elliptic-curve 0.14, sha2 0.11, sha1 0.11, hmac 0.13, pbkdf2 0.13, ripemd 0.2,
  rfc6979 0.6, digest 0.11 and their own (hybrid-array, crypto-bigint 0.7,
  primefield, wnaf, ctutils, cmov, cpubits, serdect, rand_core 0.10;
  pem-rfc7468 leaves). No source change: the Worker calls none of the doors
  0.8.11 bounds or tightens (`from_beef`, `from_atomic_beef`,
  `find_atomic_transaction`, `HttpRequest::from_payload`,
  `sighash::parse_transaction`), no template classifier, no broadcaster and
  no fee policy of bsv-rs's, and `from_ef` only in two tests over bytes they
  build (GHSA-82g9-h6jw-r99f has no patched version; no production read of
  Extended Format bytes is left to bound). Builds with the `overlay`,
  `kvstore`, `registry` and `identity` features off, so the KVStore,
  registry and discovery advisories reach no code here.
- **What a client sees.** An honest request is answered as on 0.8.1. Three
  refusals are new, each of bytes no miner accepts: `internalizeAction` with
  `beefAtRest` refuses, as `-32602` "does not spend", a spend of a coin proven
  below the Genesis height that the rules of its era refuse, and a spend of a
  coin locked `OP_HASH160 <20 bytes> OP_EQUAL` whose redeem script fails
  (GHSA-h4m7-c6p8-725m, GHSA-fr33-7jw7-mwwh); a BRC-103 signature in DER
  with bytes after it, a wrong sequence length or an unneeded zero is refused
  at the door as any bad signature is (0.8.6, GHSA-9c8r-76f3-m426); and a
  `processAction` raw transaction with bytes after it is no longer read by
  the local script check (0.8.5, GHSA-j8hr-pjhm-6h27): it goes to the
  broadcaster, whose answer decides, where 0.8.1 judged the prefix.
- **The version is 0.7.0**, a minor: the first tag over what main carried
  past v0.6.0, all of it in production at `0bfbf2a` since 2026-10-27. The
  door's policies (#22): bodies and responses bounded at 8 MiB (413), 300
  requests a minute per IP before auth and 1,000 per identity after (429
  with the draft-8 RateLimit headers), 24 requests in flight per isolate
  (503), list, offset, array and nesting limits, `findOrInsertUser` for the
  session's own key only, internal errors collapsed to `WERR_INTERNAL`'s
  text, `GET /healthz` and `GET /robots.txt`, `monitor/status` and
  `monitor/audit` behind the trigger key, and `GET /` naming each rate-limit
  binding. The route fixtures replayed on the compiled Worker (#15). The
  proof held pending until the header service answers (#49). Arcade's 400
  carrying 460 to 475 read as a policy refusal, a hint, never `InvalidTx`,
  its 466 a conflict hint, `/policy` read once per provider, and the local
  script check run on the inline BEEF before any write (#50). No migration.

## 0.6.0, 2026-10-22: bsv-rs 0.8.1 and the agents' adapter 0.7.2

### Changed

- **bsv-rs `0.8.1`** (the caret, from `=0.7.0`) and
  **bsv-middleware-cloudflare `=0.7.2`** (core 0.4.1, from `=0.7.0`; the
  adapter is on bsv-rs `^0.8`), so the lock holds one bsv-rs; the version is
  0.6.0, a dependency minor. bsv-rs 0.8.1 reads an AtomicBEEF by the
  subject's graph (BRC-95), so the BEEF at rest is accepted or refused by
  that rule; it types the wallet's busy answer, writes BSC3 stream cursors
  and marks `Error` non_exhaustive. No `match` here is over bsv-rs's
  `Error` and the Worker keeps no stream cursor, so the library source is
  unchanged. No wire change.
- **Two tests follow the library.** `to_binary_atomic` now writes only the
  subject's closure, so the inconsistent-atomic case of
  `ef_batch_prefers_consistent_atomic_and_rejects_inconsistent` frames the
  whole BEEF under the parent's prefix by hand; `beef_to_ef_batch` still
  refuses it. The at-rest stream index keeps each raw transaction's input
  txids for the atomic walk, so 135,299 links read 42.9 MiB (333 B per
  element, from 259); the bound in `tests/beef_at_rest.rs` moves from a
  third of the isolate to three eighths.

## 0.5.0, 2026-10-19: the agents' adapter 0.7.0 and bsv-rs 0.7.0

### Changed

- **bsv-middleware-cloudflare `=0.7.0`** (core 0.4.0, from `=0.6.2` and core
  0.3.2) and **bsv-rs `=0.7.0`** (from `=0.6.0`); the version is 0.5.0, a
  dependency minor. The adapter and core now hash and MAC through bsv-rs's
  `primitives::hash` and no longer depend on `sha2` or `hmac`, so
  `cargo deny check` is green against `deny.toml` unchanged: no wrapper added,
  no ignore. `cargo tree -i sha2` names bsv-rs, k256 and p256; `-i hmac`
  names bsv-rs, pbkdf2 and rfc6979. No source change was needed: the public
  API this crate calls (`process_auth`, `sign_json_response`, the CORS
  helpers, `AuthCloudflareError`) did not move.
- **The signing digest.** bsv-rs 0.7.0 moves `SigningContext::compute_sighash`
  to the node's dispatch for a hash type without FORKID or with CHRONICLE.
  The Worker signs no transaction (the client's wallet signs every input), so
  the change reaches no code here. Its signatures are BRC-103 auth messages
  (the handshake's and every response's), all made by core's `sign_message`
  through `ProtoWallet::create_signature` with `data`: ONE SHA-256 of the
  signing data, for a response its BRC-104 payload. `tests/response_signing_digest.rs` pins it:
  the signature `sign_http_response` makes equals the same key's signature
  over `sha256(payload)` given directly and differs from one over the double
  SHA-256, the client verifies it, and the fixed payload's digest is pinned
  from bytes built outside the crate.
- **The payment claims migration does not apply.** The adapter's 0.7.0 line
  for wallet-infra asks for a `PaymentPrefixStore` and a one-time
  `payment_claims` migration where a host runs the payment path with storage.
  This crate enables neither `refund` nor `d1-claims`, calls no payment
  helper, and has no `payment_claims` table, so there is nothing to migrate.

- `read_varint` in `src/storage/process_action.rs` is deleted: it was a
  wrapper over bsv-rs's `from_var_int`, and the fleet reading counted it by
  name as a hand-rolled primitive (bsv-stack-lean #105). `parse_output_scripts`
  now calls `bsv_sdk::primitives::from_var_int` itself; the refusals and the
  `varint at {pos}` message are unchanged, except that a varint past the end
  of the bytes now carries bsv-rs's message in place of "Unexpected end of
  transaction data". The varint tests read the same bytes through the new
  call, and one test adds the parser's refusal of a wide and a missing count.

## 0.4.2, 2026-10-18: published from the tag, the README refreshed

### Added

- `.github/workflows/release.yml`: a `v*` tag push checks the tag against the
  manifest's version and that the commit is on `main`, runs the gate of
  `ci.yml` (now callable), verifies the package with `cargo publish --dry-run`
  on the host, and publishes through crates.io trusted publishing. No token is
  stored.

### Changed

- The package is named `bsv-wallet-infra-cloudflare` on crates.io (it was
  `rust-wallet-infra` in the manifest); the library keeps the identifier
  `rust_wallet_infra`, so no `use` path moves. The manifest carries a
  description, the repository, the README and an `include` list (the sources,
  the migrations, `wrangler.toml`, the README, the licence and this file).
- The README is refreshed to the code at this tag and gains a "Releases"
  section.

Nothing else changes in the Worker: no route, binding, behaviour or wrangler
setting.

## L2, 2026-10-15: bsv-rs 0.6.0 exact, the deny file (the next tag after `v0.3.1`)

### Changed

- **bsv-rs `=0.6.0`** (from `=0.4.5`) and **bsv-middleware-cloudflare
  `=0.6.2`** (from `=0.5.1`; its core 0.3.2 is the one middleware line on
  bsv-rs 0.6, so the graph holds one bsv-rs).
- **One varint.** `processAction`'s output-script parser and the monitor's
  input-outpoint parser read bsv-rs's canonical `from_var_int`; the two
  hand-written decoders are deleted. A varint written wider than its value
  needs (`fd fc 00` for 252) is now refused, as the node's `ReadCompactSize`
  refuses it.
- **A decline at rest.** bsv-rs 0.6.0 checks an unproven subject's finality
  and a coinbase's maturity, and declines (`Verdict::Unverifiable`) when no
  mining tip is supplied; none is supplied here. `internalizeAction` with a
  reference at rest now answers such a BEEF (a nonzero lock time with a
  non-final sequence) with a validation error that names the decline,
  "not judged invalid", and writes nothing. 0.4.5 accepted it.
- **Memory at rest.** 0.6.0's stream index keeps a proven coinbase's height
  per transaction: the relay's 8 MiB line reads 33.4 MiB at peak where it read
  27.4 MiB. The test's bound is a third of the 128 MiB isolate, from a
  quarter.

### Added

- `deny.toml`, the stack's template byte for byte. `cargo deny check` is red
  on two facts outside this crate's code: `sha2` and `hmac` direct under
  bsv-middleware-cloudflare and bsv-middleware-core, and no license field on
  this crate. No local allowance.

## Unreleased

- bsv-rs 0.4.5: the lone leaf at offset 0 only; the LOCAL work bound; no behavior change under the defaults.
- Pin bsv-middleware-cloudflare to =0.5.1 (core 0.2.1).

## L2, 2026-10-11: broadcast refusals are history hints; request history is served (#72, #73)

### Fixed

- **The server post and the remote post result.** `processAction` no longer
  writes `failed`, `invalid` or `doubleSpend`, releases inputs or removes
  created outputs on a broadcaster's refusal. It records the word in the
  request's history and queues `unsent` for the existing five-minute monitor
  cadence. `updateTransactionStatusAfterBroadcast(false)` does the same,
  including when no status source knows the transaction. The monitor's next
  post also records a hint and retains the locks; a re-ask cannot turn the
  same refusal into a failure. Acceptance records a note too.
- **The local script verdict.** Both immediate paths run bsv-rs's `Spend`
  against locally stored, hash-matched parent transactions. A script that
  refuses under every supported block era and both UTXO eras can write
  `failed` / `invalid` and release inputs in an atomic D1 batch. A completed request blocks every book write. Policy flags
  are never used. Missing local parent bytes, an unavailable store, missing
  input context and local resource exhaustion cannot write a verdict.
  This is a conservative script check, not a complete transaction evaluator.
- **History writes.** Notes append against the current D1 value, preserve
  other history keys, mark client notes with their source, and retain the newest 32 notes as toolbox v0.7.4 does.
  The existing `history` column suffices; no D1 migration.

### Added

- **`findProvenTxReqs`.** Authenticated, scoped through the caller's
  transactions, with txid/status filters, since, ordering and pagination;
  named and positional arguments. Rows carry history and inline or R2 bytes.
- **Client post notes on the write path.** The status method accepts an
  optional third argument `{notes: [...]}` after `[txid, success]`, or
  `notes` beside `txid` and `success` in named arguments. Notes use the
  toolbox's `{when, what, ...}` shape and are read back from the same history.
  Legacy bool-only clients get a `clientBroadcastHint` recording exactly
  that bool. Toolbox v0.7.4 sends no detailed notes: the server cannot recover
  words that never crossed the wire; carrying those words needs a client
  change outside this lane. No client changed here.
- Tests: `tests/worker_tracker_rule.mjs`, the compiled Worker in Miniflare
  through BRC-31 with local D1 and mocked outbound requests, and
  `tests/tracker_rule.rs`, the pure evaluator's refusal, pass, policy and
  missing-context witnesses. `npm run test:worker` includes both Worker files.

## Broadcast body, 2026-10-09: ARC is handed the plain BEEF, never the AtomicBEEF (bsv-stack-lean #63)

### Fixed

- **The inline door's post to ARC.** `internalizeAction` with an inline `tx`
  (or an object at rest of at most 4,096 bytes) handed ARC the caller's
  AtomicBEEF, its 36-byte BRC-95 prefix kept, as `{"rawTx": hex}`. ARC tells a
  BEEF by bytes 2 and 3, `BE EF` (bitcoin-sv/arc@e7efc5b
  `internal/beef/beef.go:24-34`, `internal/validator/helpers.go:36-46`); the
  prefix's are `01 01`, so ARC read the body as a raw transaction and answered
  400 before any validation, both endpoints alike, and the payment's broadcast
  waited for the monitor's escalation (three blocks) to re-post it in a form
  ARC parses (bsv-stack-lean `docs/readings/broadcast-body-forms.md`, F1).
  ARC's BEEF post (`arc_beef_request`) now slices the prefix off and posts the
  BEEF's bytes as `application/octet-stream`, the same bytes the monitor's
  stream of a BEEF at rest posts (NL-7c). Every caller of `broadcast_beef`
  reaches ARC through it: the inline door, the Arcade outage fallback (which
  handed ARC the same AtomicBEEF), and the monitor's and `processAction`'s
  plain BEEFs, whose bytes are unchanged and now go as octet-stream instead
  of hex in JSON. Arcade still gets EF. A body that is not hex is not posted.

### Added

- `services::arc::arc_beef_request`.
- Tests: `tests/broadcast_body.rs` against bsv-stack-lean's six replay rows
  (`corpus/runners/broadcast-body/`, copied under
  `tests/fixtures/broadcast-body/` with their sha256 checked): the bytes ARC
  reads from the request are a BEEF for every row, an AtomicBEEF's byte for
  byte its plain row (the row the replay parses at ARC's pin) still holding
  its subject, a plain row unchanged; red at `bc4c9ba`'s body on the three
  AtomicBEEF rows. `tests/worker_beef_at_rest.mjs` gains the inline door's
  post of an unproven payment, received by ARC as octet-stream, leading
  `0200beef`, byte for byte the BEEF behind the prefix.

## NL-7c, 2026-10-09: a BEEF at rest is read only for the caller the object names; the monitor posts it as a stream

The two remaining items of NL-7 (bsv-stack-lean #62), before the binding
deploys. An abuse bound, never a size.

### Changed

- **The caller on the object.** A `beefAtRest` reference is honoured only when
  the object's custom metadata `recipient-identity-key` (the key the relay
  writes on every object it spools, rust-message-box NL-7b) equals the
  signed-in caller's BRC-31 identity key (hex, either case). The object's head
  is read first and carries no body; the caller is held before the etag and
  the size, so a refusal never says another caller's object's etag, size or
  owner; the etag-conditioned get is held again before its body is handed
  out. A missing or different key is a `-32602` by reason (`beefAtRest: the
  object at <key> names another recipient than the caller; it is read only
  for the caller it names`, or `names no recipient`), with no body read and
  no copy. Objects spooled without the metadata are refused: the relay's
  NL-7b writes it.
- **The monitor's broadcast of a subject at rest streams.** `send_waiting`
  reads a stored R2 `input_beef`'s 36-byte BRC-95 prefix (a ranged get); when
  it names the row's subject, the BEEF behind it is posted to ARC as an
  `application/octet-stream` body piped from R2 through a `FixedLengthStream`
  (its length known), one fresh etag-conditioned body per endpoint of the
  race, the bytes never in the Worker. ARC knows a BEEF by its marker
  (bitcoin-sv/arc@e7efc5b6 `internal/api/handler/parsers.go:50-51`,
  `internal/beef/beef.go:24-34`), so the AtomicBEEF prefix stays behind.
  Arcade's `/tx` takes no BEEF (bsv-blockchain/arcade@1ae1208
  `openapi/arcade.openapi.yaml:209-238`): under `BROADCASTER = "arcade"` the
  stream goes to ARC too. Any other stored shape (the `createAction`
  ancestors to merge, a BEEF in D1, another subject) takes the route it took.
  A store that does not answer leaves the row for the next cycle, no attempt
  counted.

### Added

- `beef_at_rest::{RECIPIENT_IDENTITY_KEY, hold_to}`; `src/broadcast_at_rest.rs`
  (`StoredBeef`, `StreamBroadcast`, `post_stored`, `R2Stored`);
  `StreamBroadcast` for ARC, `MultiProvider` and `SelectedProvider`.
- Tests: `tests/beef_at_rest_caller.rs` (the key's name, the caller honoured,
  another's and none refused by reason, the caller held first);
  `tests/broadcast_at_rest.rs` (the 100,000-link payment: 212.1 MiB on the
  base's route, 130.5 KiB streamed over two endpoints on the host, byte for
  byte); `tests/worker_beef_at_rest.mjs` gains the caller's refusals (bytes
  that are no BEEF, named for another, refused for the caller and never for
  the bytes; nothing copied) and the monitor's streamed post under `arc` and
  `arcade`, received by ARC byte for byte with its content-length.

## NL-7, 2026-10-09: `internalizeAction` takes bytes at rest

The posture of the no-limits program (bsv-stack-lean
`docs/charters/beef-of-any-size.md`): a BEEF is refused only for invalid bytes,
never for its size or its counts, and is read one element in hand.

### Added

- **The reference form of `internalizeAction`.** Beside the inline `tx`, the
  arguments may carry `beefAtRest: { r2Key, size, etag, bucket }`, a reference
  to an AtomicBEEF at rest in the R2 bucket bound as `BEEF_AT_REST` and named
  by `BEEF_AT_REST_BUCKET`. Exactly one of `tx` and `beefAtRest` is given. The
  object is held to the reference (the bucket, the etag as a conditional get
  and again on the object, the size), read as its body stream (never
  `bytes()`), and verified by bsv-rs 0.4.1's `AsyncStreamVerifier` with the
  scripts, one element in hand; every root is then asked of the header
  service. The storage is handed the subject alone; the BEEF is stored in
  `BLOBS` by a streamed copy (`transactions/<id>/input_beef`,
  `proven_tx_reqs/<id>/input_beef`) and never made whole. A refusal names the
  offset and the kind (`the BEEF at rest is refused at offset 41:
  RootNotCarried (..)`) or the refused spend; a store or header service that
  cannot answer is an internal error, never an acceptance.
- `src/beef_at_rest.rs`: the argument, the reading (host-tested over a chunked
  source), the R2 body stream as the reader's source.
- `[[r2_buckets]] BEEF_AT_REST` (the relay's `bsv-messagebox-beefs`) and
  `BEEF_AT_REST_BUCKET` in `wrangler.toml`: the deploy's item. Without them the
  reference form is refused and nothing else changes.
- Tests: `tests/beef_at_rest.rs` (the argument, the 100,000-link payment and
  the first chain over 8 MiB read at rest, the refusals by offset and kind,
  the size, a header service that fails); `tests/inline_beef_witness.rs`
  (what the inline door holds); `tests/worker_beef_at_rest.mjs` (the release
  Worker in Miniflare with D1, KV and both buckets, through BRC-31;
  `npm run test:worker` with `MINIFLARE_MODULE` and `BSV_SDK_MODULE` set).

### Changed

- bsv-rs 0.4.0 to 0.4.1 in the lock (the streaming reader's `NoInputs` kind).
- `internalizeAction` is split at the subject: the inline door parses and
  verifies as before (`BEEF_VERIFICATION` governs it) and hands the same
  subject on. The reference form is verified in full whatever
  `BEEF_VERIFICATION` says.

### Not changed, and named

- The inline door holds the BEEF many times over: measured on the host, 342 MiB
  for the 100,000-link payment (6.2 MB) and 539 MiB for 8 MiB, over an
  isolate's 128 MB. It stays for the callers that send `tx`.
- A subject at rest whose BUMP is not in the BEEF and that the broadcast
  services do not know is not posted from the request (posting it would make
  the BEEF whole); it is left to the monitor as a transient fault is, and
  `INTERNALIZE_ZERO_CONF` decides whether its outputs are spendable
  meanwhile. The monitor read a stored `input_beef` whole when it posted it
  (since NL-7c an AtomicBEEF naming its subject is posted as a stream).
- `createAction`'s `inputBEEF` is decoded by its argument type and never read
  by the storage; no reference form is built for it.
