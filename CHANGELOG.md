# Changelog

The tag is the release of record. From 0.4.2 on, the crate on crates.io,
`bsv-wallet-infra-cloudflare`, carries the tag's version; entries before it
are named by their program and their date, and the manifest then read 0.1.0.

## 0.7.9, 2026-11-28: bsv-rs 0.8.15

### Changed

- **bsv-rs `0.8.15`** (the caret, from `0.8.11`), the newest 0.8 on the index;
  bsv-middleware-cloudflare stays `=0.7.2` and the lock holds one bsv-rs. The
  lock moves bsv-rs and this crate's own version, nothing else. The published
  advisories of bsv-rs this release takes, by the release that carries each
  fix: 0.8.12, GHSA-33fm-jcm5-4m3m; 0.8.13, GHSA-j4w5-4g84-q3f4; 0.8.14,
  GHSA-wh67-gjx6-4qqh, GHSA-56fp-4337-6vfh, GHSA-7vx5-cf5p-5c48,
  GHSA-63w6-rpmj-r9gg, GHSA-grrj-gwrr-v7r3, GHSA-mqr8-3gvc-hhg3,
  GHSA-fm3p-cmwv-rf6h, GHSA-4gp5-q4cj-4qjf, GHSA-33px-hj7f-4cpj and
  GHSA-43mf-m7fg-m74w; 0.8.15, GHSA-pfv3-7r7f-mhgh, GHSA-f2rq-878r-pgxv,
  GHSA-mp9c-g2q9-q3qw, GHSA-jx5m-j63r-7fgw, GHSA-jgrp-wvcx-vxf3 and
  GHSA-jjhc-7hvh-c3hm.
- **The estimate's constants are measured against bsv-rs 0.8.15's reader.**
  The measure of a BEEF (`door::beef_measure`) counts, beside each BUMP's
  leaves, the leaves that carry a hash, over every BUMP and in the BUMP that
  carries the most; the estimate has a term for each. The four BUMP leaf
  constants are the least that hold the estimate a fifth over every measured
  parse of 0.8.15, rounded up. The budget (24 MiB), the payment figure and the refusals'
  wire shapes are unchanged.
- **Tests.** The many-leaves BUMP fixtures are as tall as their offsets need.
  `tests/support/beef_shapes.rs` builds five more BUMP families (a row of
  txids, leaves on every level of 64, txids on paths apart, one BUMP and
  many); `tests/beef_measure.rs` holds the estimate over the measured parse
  on every family at the payment figure and on the BUMP families at the sizes
  between, and holds the largest payment the doors admit within the payment
  figure, through `internalizeAction`'s steps, under the request ceiling.
- **`internalizeAction` takes a subject of exactly 64 bytes as unproven**: a
  BUMP is not read as its proof, in the inline and the at-rest form; it is
  posted and waits for its proof like any unproven subject. The rule is the
  library's (bsv-rs 0.8.15, GHSA-jjhc-7hvh-c3hm); one function,
  `beef_at_rest::bump_proves_subject`, says it for both forms. Every other
  length is read as before.
- **Tests.** The function at 63, 64 and 65 bytes; the at-rest reading and the
  inline form's word for a 64-byte subject; `tests/worker_beef_at_rest.mjs`
  holds both forms on the compiled Worker: unproven, posted, a request left
  for the monitor, no proof stored.
- No migration, no `wrangler.toml` change.

## 0.7.8, 2026-11-28: the monitor's reorg passes store a proof only after it is checked

### Changed

- **The monitor's two reorg passes store a provider's proof only after it is
  checked.** The hourly shallow sweep and the pass on a drop of the chain's
  height check a provider's proof against the transaction's own txid and the
  header service's root at that height before they store it. A stored row
  takes the check's own height, index and root; a proof that is not stored
  leaves the row as it is and is named in one log line (the txid, the
  provider, the reason).
- **On a drop of the chain's height, a stored proof whose root the header
  service no longer carries at its height is replaced only by a checked proof
  from a provider.** Otherwise the row and its request are left as they are,
  the pass records the row as the deep sweep does (`deep_reorg_unrepaired`),
  and the deep sweep asks for a proof again on each of its passes. When the
  header service does not answer, nothing is changed and nothing is stored.
  Neither pass writes a request, a transaction or an output.
- **Proof answers carry the name of the provider that gave them**, for the
  monitor's log lines.

## 0.7.7, 2026-11-28: a door reads at most 600 KiB of a payment's bytes

### Changed

- **A door reads at most the payment figure of a payment's bytes**: the
  Worker's setting `MAX_PAYMENT_BYTES`, in bytes; missing, zero or malformed,
  614,400 (600 KiB), and a zero or malformed value is named in the log once
  for each isolate.
  `GET /` shows the figure in force as `max_payment_bytes`. An ordinary
  payment is about a kilobyte; an operator who must take a larger one raises
  the setting. `internalizeAction`'s AtomicBEEF, inline (`tx`, a JSON array of
  bytes or a hex string) or at rest (`beefAtRest`, on the length the reference
  names, before the caller's user is looked up), and `processAction`'s signed
  transaction (`rawTx`) over it are
  refused before any of it is parsed or verified and before anything is
  written, with the arguments named or positional. A JSON array is decoded no
  further than the figure (an array between the figure and the request's
  1,000,000-item bound is refused by the figure); a hex string is held on its
  length before it is decoded. The answer is `-32602`, the code of every
  refused argument here, with the message `Not judged: this payment is {size}
  bytes, and this host reads at most {budget} bytes of a payment: ...` (or
  `... is more than {budget} bytes, the most this host reads of a payment:
  ...` where the size is not known) and `data` `{code: "ERR_PAYMENT_SIZE",
  budget, size}`; each refusal is counted by one `payment.not_judged` log
  line. A Worker's isolate is memory-limited and a door bounds what it reads.
  The reference names no such figure (its storage server holds a request's
  bodies at 8 MiB and its arrays at 1,000,000 items); this host differs on
  purpose. The figure is provisional: it is read again at this crate's next bsv-rs pin and raised to the door's natural figure then.
- **A provider's answer that carries a proof, a BEEF, a transaction, a
  transaction's status or a broadcast's outcome is read within a bound**: a
  merkle proof from ARC, WhatsOnChain or Bitails, a raw transaction from
  WhatsOnChain, Arcade's stored record, the answers of ARC, WhatsOnChain and
  Arcade to a posted transaction, the WhatsOnChain proof `GET
  /monitor/probe-woc` shows, and the body of `/arcade/callback` are read no
  further than twice the figure and 64 KiB (an answer declaring a longer
  length is not read); the merkle path or transaction inside is then held to
  the figure. Past either, that provider
  is taken as unavailable at that moment, as on its error; each is counted by
  one `provider.answer_not_read` log line. WhatsOnChain's status answers
  (`/txs/status`, the second source asked after a broadcaster's refusal) and
  its spent answers are read within the same bound; past it, the answer is
  WhatsOnChain's error, so after a refusal it is no answer and the rows are
  held without credit, as in 0.7.6. A broadcaster's answer to a post past
  the bound is no answer, the road of a broadcaster that cannot be reached
  (ARC: the next endpoint, then WhatsOnChain; WhatsOnChain: its service
  error; Arcade: asked again, then the outage's fallback to ARC and
  WhatsOnChain), never a refusal and never an acceptance. The answers that
  carry a block header, a chain height or a fee policy, and Arcade's event
  stream, are not held to this bound. No dependency moves; no migration, no
  `wrangler.toml` change.

### Tests

- `door::payment`'s and `services::arc`'s unit tests, the setting's fallback
  and its log line, the positional form, the answer bound, and a door under
  another figure; `tests/worker_door_policies.mjs` 1b and 8b-8e,
  `tests/worker_beef_at_rest.mjs` 3b and 3c and `tests/worker_proof_hold.mjs` g-j:
  one byte over refused at each door, named and positional, with no row
  written (the users table included) and the log line pinned, exactly the
  figure passed to the door's own handling, a Worker with
  `MAX_PAYMENT_BYTES = "2000"` following it and one with `"600k"` serving
  the default and saying so once, and a provider's answer over the figure or
  past the bound not read on any proof path. The at-rest suite's accepted
  chain is one of exactly the figure. 3c: a caller the store has never seen
  presents a reference one byte over the figure: the pinned refusal and log
  line, and no table gains a row, the users table included; a fresh caller's
  reference of exactly the figure is accepted.
- `tests/worker_beef_at_rest.mjs` 1c: ARC's answer to a post past the bound,
  one that read whole would accept and one that would refuse: each leaves
  the payment as an unanswered post leaves it (accepted, its row unproven,
  its output held), and a line names the answer not read.
- `tests/worker_tracker_rule.mjs` mk14: the broadcaster refuses and the
  second source's answer passes the bound (one that, read whole, would say
  "not known"): the caller is answered the refusal and the rows are held
  without credit, as in mk9.

## 0.7.6, 2026-11-28: a coin's row is marked from a checked proof of its spender

### Changed

- **`internalizeAction`: a coin's row is marked from a checked proof of its
  spender.** A call marks the internalizing user's own rows at the subject
  transaction's inputs. Any other user's row at an outpoint the subject spends
  is marked when the subject's checked proof is stored, from the stored bytes
  of the subject, and only when those bytes hash to the proven txid. Nothing
  changes for a row that another transaction already spends.
- **`internalizeAction`: a call's own transitions are restored unless its
  transaction is taken.** What the call marked at the subject's inputs is
  restored on every outcome of the broadcast except acceptance: a double-spend
  or invalid refusal, a policy refusal (460 to 475), and a transient fault
  under either `INTERNALIZE_ZERO_CONF` setting. When the broadcaster later
  accepts a policy-held transaction on a re-announce, the monitor marks those
  inputs again.
- **`internalizeAction`: a transaction whose inputs' parents are absent is
  refused.** If the subject of a BEEF passed in the call's `tx` carries no
  merkle proof and names an input whose parent the BEEF does not hold, the
  call is refused with `-32602` and the reference's message, "The tx parameter
  must be valid AtomicBEEF". The refusal comes before any write or broadcast,
  under every `BEEF_VERIFICATION` setting.
- **`internalizeAction`: a refused payment fails only the internalizing user's
  own row.** The compensation after a broadcaster's refusal fails only the
  internalizing call's own transactions row. It never touches a row that is
  `completed` or carries a proof, and it zeroes that row's outputs only while
  the row reads `failed`. The compensation runs even when the restore of the
  call's inputs returns a storage error, and the call then answers that error.
- **`internalizeAction`: a refusal the second source cannot answer holds the
  rows without credit.** If the broadcaster refuses the subject and the second
  source cannot be asked (an error rather than an answer), the caller is
  answered as before, with the refusal (`-32602`, "Transaction <txid> rejected
  by network (...)"). The transaction's rows are held without credit, its row
  `unproven`, its outputs `spendable = 0` and its request `unmined`, until one
  of three endings: its checked proof is stored (its outputs are credited
  then, once); the broadcaster accepts it on the monitor's re-announce (its
  outputs are credited then, as a taken transaction's are); or it fails by the
  request's own road (nothing of it was ever offered). A later presentation of
  the same transaction while it is held is answered the same refusal, without
  a broadcast: by the same user before any write, and by another user once
  that user's own rows are written, which are held the same way, without
  credit.

## 0.7.5, 2026-11-28: the script check runs under a stack-memory budget

### Changed

- **The local script check runs under a stack-memory budget of 1,000,000
  bytes** (`tracker_rule::SCRIPT_MEMORY_LIMIT`). The check behind `internalizeAction`,
  `processAction` and `updateTransactionStatusAfterBroadcast` passes the
  budget to every `Spend` it builds; the count is the interpreter's under the
  block word (both stacks, each element with its per-element overhead). A
  script whose stacks would pass the budget is declined by the interpreter as
  a resource limit, which the check answers as no verdict, as it answers any
  local resource limit: the broadcaster's answer decides, and no transaction
  is refused for it. A host that verifies a caller's script in a
  memory-limited isolate sets such a budget (GHSA-7vx5-cf5p-5c48). The lock
  is unchanged (bsv-rs 0.8.11); no migration, no `wrangler.toml` change.

### Tests

- `storage::tracker_rule::tests`: a spend whose stack passes the budget is
  no verdict; a small script keeps its verdict (with `tests/tracker_rule.rs`,
  which pins the honest verdicts); one element filling the budget exactly is
  judged and one byte more is declined, so the check runs under the named
  budget.

## 0.7.4, 2026-11-27: one row per payment under concurrent calls

### Changed

- **`internalizeAction`: the transaction row and the output row are written
  by guarded inserts.** The transaction row's insert carries, inside the
  statement, the predicate of the read that decides "new" or "merge" (the
  user and the txid), and writes the row only when that read would still find
  none; a call whose insert writes nothing reads the row that stands and goes
  on as the merge a later call is. The output row's insert holds the schema's
  key, one row to (`transaction_id`, `vout`, `user_id`); an output whose
  insert writes nothing is merged onto the row that stands. So a user holds
  one transaction row for an internalized txid and one output row for an
  outpoint, in whatever order the statements of concurrent calls reach D1;
  each call is answered as accepted, one as new and the others as merges, and
  the satoshis of the answers sum to the payment.
- **The other inserts on that path hold their keys the same way**: the proof
  request (one to a txid), the default basket, a named basket, a label and a
  tag (each insert carries its read's predicate), and the label and tag map
  rows (one to their pair). A call that finds a row standing uses it.
- **A call names each output index once.** A call whose `outputs` name one
  output index more than once is refused: under whichever protocols (two
  wallet payments, two basket insertions, one of each), and whether or not
  the storage holds the transaction or the output. The answer is `-32602`,
  the code of every refused argument here, with the message
  `The outputs parameter must be unique outputIndex values`
  (`WERR_INVALID_PARAMETER` of `outputs`). The message is the reference's,
  which gives it in one form only: a transaction held whose output row
  stands at the index, the index named as a wallet payment and as a basket
  insertion (the other forms: `docs/DIVERGENCES.md`, row 17). The reference
  was read at its head of 2026-11-27, `bsv-blockchain/ts-stack`
  57c5f13055b555b83c1ef368fff3650abd21b6ef, not run. The answer is given
  where the call's specifications are read, after an index out of range or
  a missing remittance is answered: no row of the transaction, of an output
  or of a proof request is written, and nothing is announced. The same call
  naming the index once is then answered as a call alone is
  (`docs/DIVERGENCES.md`, row 17).
- Every other call alone writes the rows it wrote and is answered as it was.
  No migration and no configuration change.

### Tests

- `storage::internalize_action` (`test_one_row_*`): the statements on SQLite
  over the actual migrations, a call's deciding statements run one at a time:
  two calls for one payment in a named order and in every order of their
  statements, the same with the second call naming another output index, one
  call after another, and each insert's condition against its read's
  predicate and the schema's key.
- `storage::writers` (`find_or_create_*`): the basket, label and tag
  statements the same way.
- `tests/worker_tracker_rule.mjs`, now run by the `worker` check: the
  compiled Worker on workerd's D1, one AtomicBEEF posted by two sessions of a
  user at the same moment, 40 payments in turn; each leaves one transaction
  row and one spendable output, the balance grows by one payment, and both
  answers are accepted. Then another output of a transaction the storage
  already holds, by two calls at once, 20 payments in turn: one row for the
  output, one payment more on the balance, both answers accepted as merges.
  Then the first two calls of a new user at once, 10 users, a basket
  insertion with a new basket, label and tag: one default basket, one row of
  each, both answers accepted.
- `storage::internalize_action` (`test_one_index_once_*`): the refusal and
  its answer on the wire; a call naming an index twice runs the read that
  decides "new" or "merge" and no statement after it, for a transaction the
  storage does not hold and for one it holds; and beside a call naming the
  index once, in every order of their statements, the first is refused and
  the second is answered as a call alone is.
- `tests/worker_tracker_rule.mjs` (checks `z1` to `z6`): one call, alone,
  naming an index twice, on the compiled Worker: as two wallet payments of a
  new transaction, as two basket insertions into two baskets, as two wallet
  payments of another output of a transaction held, and as a wallet payment
  and a basket insertion in either order; then for an output the storage
  holds and with another index between the two. Each is answered `-32602`
  with the message, every row of every table is as it was, nothing is
  announced and the balance stands; the same call naming the index once is
  then accepted, as new or as a merge. An index out of range among them is
  answered first. Nothing of the payment is written for a caller the server
  has not seen before either: its user row and its default basket are made
  first, as they are before every other refusal given where the
  specifications are read. Check `z6` is a user who holds no basket yet:
  answered the same, nothing announced, and the one row written is that
  user's default basket.

## 0.7.3, 2026-11-17: a list page of more than 100 rows is served

### Fixed

- **A page of more than 100 rows** of `listActions`, `listOutputs` and
  `listCertificates` is served. D1 binds at most 100 parameters a statement
  and answered `too many SQL variables` (the call `-32603`) when a page's
  labels, tags or certificate fields were fetched with every row's key bound
  in one statement; the door admits limits up to 1,000. Those fetches now run
  a statement a slice of at most 100 keys (`d1::chunks`), each key once
  across the slices; a page of 100 rows or fewer is the one statement it was,
  and its answer is the same bytes.
- **`listActions` with `includeOutputs`** fetches the page's outputs and
  their tags for the whole page, a statement a slice, where it ran one
  statement for each action and one for each output: a page of 1,000 actions
  with everything included makes about forty D1 statements, within the
  thousand a Worker invocation may make. Each action's outputs are in `vout`
  order with their own tags, as before.
- **A filter list** (the `labels` of `listActions`, the `tags` of
  `listOutputs`, the `certifiers` and `types` of `listCertificates`, the
  `txids` and `status` of `findProvenTxReqs`) binds as one JSON array read
  with `json_each`, so a filter of any length is one parameter and the paged,
  counted statement it filters stays one statement.
- **The stored BEEF's proof lookup** (`compact_stored_beef`) slices its txids
  at 100 a statement, not 400.

### Tests

- `storage::list_binds`: each of those statements on SQLite with the
  variable limit set to D1's 100 on the connection, over the actual
  migrations and 250 rows; the one-statement form is refused, the sliced
  rows are the one statement's rows, each once, in the same order within a
  key.
- `tests/worker_list_pages.mjs` (the `worker` check): the compiled Worker on
  workerd's D1, a wallet of 250 actions and 250 outputs with labels, tags and
  custom instructions, `listActions` and `listOutputs` at limits 100, 101,
  250 and 1,000 with and without each include, label and tag filters of 150
  names, `listCertificates` and `findProvenTxReqs` over filters of 260 names:
  every row once, the stated total, each row's labels, tags and outputs its
  own; a limit of 1,001 still `-32602`.

## 0.7.2, 2026-11-17: a BEEF is measured before it is parsed

### Added

- **A BEEF is measured before it is parsed** (`door::beef_measure`). One pass
  over the bytes in the BEEF layout (V1, V2 and the AtomicBEEF prefix: each
  BUMP with its levels and leaves, each entry with its format byte, its inputs
  and outputs by their varints) counts what bsv-rs's `Beef::from_binary` and
  `sort_txs` spend heap on, holding nothing per element; an estimate from the
  counts bounds the parse's heap, and a BEEF whose estimate is over the budget,
  24 MiB, is refused before it is parsed. The constants are measured on bsv-rs
  0.8.11 under a counting allocator, and `tests/beef_measure.rs` holds the
  estimate at or over the measured heap on every shape it builds and on random
  BEEFs. Bytes the walk cannot read go to the parser and its refusal, as
  before, unless what was read is already over the budget.
- **`internalizeAction`** measures the AtomicBEEF (`tx`, hex or bytes) before
  its parse (`storage::internalize_action::parse_inline_beef`) and answers one
  over the budget `-32602` "the BEEF is too large to process", the code and
  shape of a BEEF the parser refuses; nothing is written. The BEEF at rest
  (`beefAtRest`) is read as before.
- **`processAction`** measures the signed transaction with the stored BEEF it
  is merged into, before anything is written
  (`storage::process_action::admit_signed_tx`), and answers a pair over the
  budget `-32602` "the transaction is too large to process"; the action stays
  as `createAction` left it.

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
