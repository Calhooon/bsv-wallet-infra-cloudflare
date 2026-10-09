# Changelog

rust-wallet-infra is a deployed Worker, not a published crate: the version in
`Cargo.toml` stays 0.1.0 and an entry here is named by its program and its
date.

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
