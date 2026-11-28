// NL-7 and NL-7c on the compiled Worker, fully local: internalizeAction from a BEEF at
// rest in a Miniflare R2 bucket bound as BEEF_AT_REST, through the BRC-31
// door a client uses (@bsv/sdk's AuthFetch), with D1, KV and both buckets in
// Miniflare. Every request the Worker makes is answered here: the header
// service carries the chain's one root; the broadcast services know nothing.
// Nothing leaves this process. No deploy.
//
// worker-build --release   (worker-build ^0.8, the wrangler.toml [build] line)
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js \
// BSV_SDK_MODULE=<@bsv/sdk>/dist/esm/mod.js node tests/worker_beef_at_rest.mjs
import { readFileSync, readdirSync, mkdtempSync, rmSync } from 'node:fs'
import { resolve, join } from 'node:path'
import { tmpdir } from 'node:os'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'

const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const { AuthFetch, ProtoWallet, PrivateKey } = await import(process.env.BSV_SDK_MODULE ?? '@bsv/sdk')

// ---------------------------------------------------------------------------
// The chain of the no-limits program (tests/support/beef_chain.rs), AtomicBEEF
// ---------------------------------------------------------------------------

const sha = b => createHash('sha256').update(b).digest()
const sha256d = b => sha(sha(b))
const display = h => Buffer.from(h).reverse().toString('hex')
const HEIGHT = 800000
const varint = n => {
  if (n < 0xfd) return Buffer.from([n])
  if (n < 0x10000) { const b = Buffer.alloc(3); b[0] = 0xfd; b.writeUInt16LE(n, 1); return b }
  const b = Buffer.alloc(5); b[0] = 0xfe; b.writeUInt32LE(n, 1); return b
}
const u32 = n => { const b = Buffer.alloc(4); b.writeUInt32LE(n >>> 0); return b }
// `pad` bytes of OP_0 after the OP_1 of the first output's script (a payment of
// an exact length; at most 251).
const spend = (prev, outputs, pad = 0) => {
  const parts = [u32(1), Buffer.from([1]), prev, u32(0), Buffer.from([0]), u32(0xffffffff), varint(outputs)]
  for (let i = 0; i < outputs; i++) {
    const sats = Buffer.alloc(8); sats.writeBigUInt64LE(1000n)
    const extra = i === 0 ? pad : 0
    parts.push(sats, Buffer.from([1 + extra, 0x51]), Buffer.alloc(extra))
  }
  parts.push(u32(0))
  return Buffer.concat(parts)
}
const funding = spend(Buffer.alloc(32, 0xaa), 2)
const fundingRoot = sha256d(funding)

/** The AtomicBEEF of the chain of `n` transactions, and its subject (its
 * script padded by `pad` bytes). */
const chain = (n, pad = 0) => {
  const txs = [Buffer.concat([Buffer.from([1, 0]), funding])]
  let prev = fundingRoot
  let subject = funding
  for (let i = 1; i < n; i++) {
    const raw = spend(prev, 1, i === n - 1 ? pad : 0)
    prev = sha256d(raw)
    subject = raw
    txs.push(Buffer.concat([Buffer.from([0]), raw]))
  }
  const head = Buffer.concat([
    u32(0x01010101), prev,
    u32(0xefbe0002), Buffer.from([1]), varint(HEIGHT), Buffer.from([1, 1, 0, 2]), fundingRoot,
    varint(n),
  ])
  return { beef: Buffer.concat([head, ...txs]), txid: display(prev), raw: subject }
}

// The payment budget (door::payment): at most the figure in force of a
// payment is read, 614,400 bytes (600 KiB) where the setting MAX_PAYMENT_BYTES
// names no other (it is unset here).
const CAP = 614400
const LINKS = 9908
/** The chain of LINKS transactions `over` bytes past the budget (0: exactly it). */
const atBudget = (over = 0) => {
  const pad = CAP - chain(LINKS).beef.length + over
  const paid = chain(LINKS, pad)
  assert.equal(paid.beef.length, CAP + over)
  return paid
}
/** A proven AtomicBEEF of one transaction `over` bytes past the budget: its
 * second output a data carrier filling it (with `fill`), its root at HEIGHT + 1. */
const BIG_HEIGHT = HEIGHT + 1
const oneLarge = (over = 0, fill = 0x5a) => {
  const make = data => {
    const sats = Buffer.alloc(8); sats.writeBigUInt64LE(1000n)
    const carrier = Buffer.concat([Buffer.from([0x00, 0x6a, 0x4e]), u32(data), Buffer.alloc(data, fill)])
    const raw = Buffer.concat([
      u32(1), Buffer.from([1]), Buffer.alloc(32, 0xbb), u32(0), Buffer.from([0]), u32(0xffffffff),
      Buffer.from([2]), sats, Buffer.from([1, 0x51]), Buffer.alloc(8), varint(carrier.length), carrier, u32(0),
    ])
    const root = sha256d(raw)
    const beef = Buffer.concat([
      u32(0x01010101), root,
      u32(0xefbe0002), Buffer.from([1]), varint(BIG_HEIGHT), Buffer.from([1, 1, 0, 2]), root,
      Buffer.from([1]), Buffer.from([1, 0]), raw,
    ])
    return { beef, root, txid: display(root) }
  }
  const first = make(CAP - 200)
  const paid = make(CAP - 200 + (CAP + over - first.beef.length))
  assert.equal(paid.beef.length, CAP + over)
  return paid
}
const pinned = size => size === undefined
  ? `Not judged: this payment is more than ${CAP} bytes, the most this host reads of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block.`
  : `Not judged: this payment is ${size} bytes, and this host reads at most ${CAP} bytes of a payment: a bound of this host, not a rule of the chain, and no verdict on the payment. Pay with other coins. If this payment's size is unconfirmed ancestry, it will be smaller after the next block.`
const refusalOf = size => ({
  code: -32602,
  message: pinned(size),
  data: size === undefined ? { code: 'ERR_PAYMENT_SIZE', budget: CAP } : { code: 'ERR_PAYMENT_SIZE', budget: CAP, size },
})

// ---------------------------------------------------------------------------
// The Worker
// ---------------------------------------------------------------------------

const BUCKET = 'bsv-messagebox-beefs'
// The roots the header service carries, by height.
const rootAt = new Map([[HEIGHT, fundingRoot]])
// The Worker's log lines (each refusal of a payment's size is counted by one).
const logs = []
const MONITOR_KEY = 'nl7c-local'
const out = []
let carry = true
// NL-7c: while `arcAnswers`, ARC takes every post (SEEN_ON_NETWORK) and each
// is recorded: the host, the content type and length, the bytes' SHA-256.
let arcAnswers = false
const posts = []
// The bodies of those posts, in the same order.
const postBodies = []
// While `arcOver` names an answer, ARC answers each post with it followed by
// 1,300,000 spaces: past the answer bound (1,294,336 bytes at the default
// figure), an answer that read whole would accept (`seen`) or refuse (`rejected`).
let arcOver = null
const outbound = async request => {
  const url = new URL(request.url)
  out.push(url.host + url.pathname)
  if (arcOver && url.host.startsWith('arc.') && url.pathname === '/v1/tx' && request.method === 'POST') {
    try { await request.arrayBuffer() } catch { /* the race's loser, cancelled */ }
    const txStatus = { seen: 'SEEN_ON_NETWORK', rejected: 'REJECTED' }[arcOver]
    return new Response(JSON.stringify({ txid: '', txStatus }) + ' '.repeat(1_300_000),
      { headers: { 'content-type': 'application/json' } })
  }
  if (arcAnswers && url.host.startsWith('arc.') && url.pathname === '/v1/tx' && request.method === 'POST') {
    let bytes = null
    try { bytes = Buffer.from(await request.arrayBuffer()) } catch { /* the race's loser, cancelled */ }
    posts.push({
      host: url.host,
      type: request.headers.get('content-type'),
      length: request.headers.get('content-length'),
      bytes: bytes?.length,
      lead: bytes?.subarray(0, 4).toString('hex'),
      sha: bytes && createHash('sha256').update(bytes).digest('hex'),
    })
    postBodies.push(bytes)
    return new Response(JSON.stringify({ txid: '', txStatus: 'SEEN_ON_NETWORK' }),
      { headers: { 'content-type': 'application/json' } })
  }
  if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
    const height = Number(url.searchParams.get('height'))
    const root = carry ? rootAt.get(height) : undefined
    const merkleRoot = root ? display(root) : '00'.repeat(32)
    return new Response(JSON.stringify({ status: 'success', value: { merkleRoot, height } }),
      { headers: { 'content-type': 'application/json' } })
  }
  return new Response('not found', { status: 404 })
}

// Storage on disk, so it outlives setOptions (the arcade selection, step 7).
const persist = mkdtempSync(join(tmpdir(), 'nl7c-mf-'))
const options = {
  modules: true,
  scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [
    { type: 'ESModule', include: ['**/*.js', '**/*.mjs'] },
    { type: 'CompiledWasm', include: ['**/*.wasm'] },
  ],
  compatibilityDate: '2024-01-01',
  host: '127.0.0.1',
  port: 0,
  bindings: {
    SERVER_PRIVATE_KEY: PrivateKey.fromRandom().toHex(),
    CHAINTRACKS_URL: 'https://headers.test',
    BEEF_VERIFICATION: 'strict',
    INTERNALIZE_ZERO_CONF: 'false',
    BROADCASTER: 'arc',
    BEEF_AT_REST_BUCKET: BUCKET,
    MONITOR_TRIGGER_KEY: MONITOR_KEY,
  },
  d1Databases: ['DB'],
  kvNamespaces: ['AUTH_SESSIONS'],
  r2Buckets: ['BLOBS', 'BEEF_AT_REST'],
  d1Persist: join(persist, 'd1'),
  kvPersist: join(persist, 'kv'),
  r2Persist: join(persist, 'r2'),
  outboundService: outbound,
  handleStructuredLogs: ({ message }) => { logs.push(String(message).trim()) },
}
const mf = new Miniflare(options)

const lines = []
const say = line => { lines.push(line); console.log(line) }

try {
  const base = await mf.ready
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    // D1.exec splits on newlines; let SQLite find the complete statements.
    const split = spawnSync('python3', ['-c', `
import json, sqlite3, sys
statements, pending = [], ''
for char in sys.stdin.read():
    pending += char
    if char == ';' and sqlite3.complete_statement(pending):
        statements.append(pending)
        pending = ''
print(json.dumps(statements))
`], { input: readFileSync('migrations/' + name, 'utf8'), encoding: 'utf8' })
    assert.equal(split.status, 0, split.stderr)
    await db.batch(JSON.parse(split.stdout).map(sql => db.prepare(sql)))
  }
  const atRest = await mf.getR2Bucket('BEEF_AT_REST')
  const blobs = await mf.getR2Bucket('BLOBS')

  const callerKey = PrivateKey.fromRandom()
  const caller = callerKey.toPublicKey().toString()
  const another = PrivateKey.fromRandom().toPublicKey().toString()
  const client = new AuthFetch(new ProtoWallet(callerKey))
  let id = 0
  const rpc = async (method, args) => {
    const body = JSON.stringify({ jsonrpc: '2.0', method, id: ++id, params: [{ identityKey: '' }, args] })
    const response = await client.fetch(new URL('/', base).toString(), {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })
    const text = await response.text()
    assert.equal(response.status, 200, text)
    return JSON.parse(text)
  }
  const outputs = [{ outputIndex: 0, protocol: 'basket insertion', insertionRemittance: { basket: 'nl7' } }]
  const internalize = (beefArg, description) =>
    rpc('internalizeAction', { ...beefArg, outputs, description, labels: [], seekPermission: false })
  // The relay writes the recipient's identity key on every object it spools
  // (NL-7b); `recipient` null puts an object that names nobody.
  const upload = async (key, beef, recipient = caller) => {
    const customMetadata = recipient === null ? {} : { 'recipient-identity-key': recipient }
    // Miniflare's proxy takes a plain Uint8Array, not a Node Buffer.
    const object = await atRest.put(key, new Uint8Array(beef), { customMetadata })
    return { r2Key: key, size: beef.length, etag: object.etag, bucket: BUCKET }
  }
  const sha256 = b => createHash('sha256').update(b).digest('hex')
  const blobBytes = async key => {
    const object = await blobs.get(key)
    assert.ok(object, `no blob at ${key}`)
    return Buffer.from(await object.arrayBuffer())
  }

  // 1. The inline door is as it was: a proven payment of one transaction.
  {
    const { beef, txid } = chain(1)
    const r = await internalize({ tx: beef.toString('hex') }, 'NL-7 inline')
    assert.ok(r.result, JSON.stringify(r))
    assert.equal(r.result.accepted, true)
    assert.equal(r.result.txid, txid)
    say(`inline, 1 transaction (${beef.length} bytes): accepted ${txid}`)
  }

  // 1b. bsv-stack-lean #63 (F1): the inline door's post of an unproven
  // payment is the BEEF behind the AtomicBEEF's prefix, as bytes,
  // application/octet-stream, the form the monitor's stream posts; never the
  // prefix, which ARC at e7efc5b reads as a raw transaction and answers 400.
  {
    const { beef, txid } = chain(2)
    const want = { bytes: beef.length - 36, sha: sha256(beef.subarray(36)) }
    posts.length = 0
    arcAnswers = true
    const r = await internalize({ tx: beef.toString('hex') }, 'bb-1 inline, unproven')
    arcAnswers = false
    assert.ok(r.result, JSON.stringify(r))
    assert.equal(r.result.txid, txid)
    const whole = posts.filter(p => p.bytes !== undefined)
    assert.ok(whole.length >= 1, `inline: no post was received: ${JSON.stringify(posts)}`)
    for (const post of whole) {
      assert.equal(post.type, 'application/octet-stream', `inline: ${JSON.stringify(post)}`)
      assert.equal(post.lead, '0200beef', `inline: ${JSON.stringify(post)}`)
      assert.equal(post.bytes, want.bytes, `inline: ${JSON.stringify(post)}`)
      assert.equal(post.sha, want.sha, `inline: ${JSON.stringify(post)}`)
    }
    say(`inline, unproven (${beef.length} bytes): accepted ${txid}; posted to ${whole.map(p => p.host).join(' and ')}: application/octet-stream, leading 0200beef, byte for byte the BEEF behind the prefix (${want.bytes} bytes)`)
  }

  // 1c. The broadcaster's answer to a post is read within the answer bound:
  // past it, no answer, the road of a broadcaster that cannot be reached. An
  // answer that read whole would accept and one that would refuse each leave
  // the payment as an unanswered post leaves it: accepted, its row unproven,
  // its output held, its request unmined; never refused, never taken as seen.
  {
    const state = async (r, txid) => {
      assert.ok(r.result, JSON.stringify(r).slice(0, 600))
      const tx = await db.prepare('SELECT status FROM transactions WHERE txid = ?').bind(txid).first()
      const output = await db.prepare('SELECT spendable FROM outputs WHERE txid = ? AND vout = 0').bind(txid).first()
      const req = await db.prepare('SELECT status FROM proven_tx_reqs WHERE txid = ?').bind(txid).first()
      return { accepted: r.result.accepted, sent: r.result.sendWithResults, tx: tx?.status, spendable: output?.spendable, req: req?.status }
    }
    const unanswered = chain(2, 10)
    const baseline = await state(await internalize({ tx: unanswered.beef.toString('hex') }, 'bb-1c an unanswered post'), unanswered.txid)
    assert.equal(baseline.accepted, true)
    assert.deepEqual(baseline.sent, [{ txid: unanswered.txid, status: 'unproven' }])
    for (const [answer, pad] of [['seen', 11], ['rejected', 12]]) {
      const paid = chain(2, pad)
      const logsBefore = logs.length
      arcOver = answer
      const r = await internalize({ tx: paid.beef.toString('hex') }, `bb-1c an answer past the bound (${answer})`)
      arcOver = null
      const got = await state(r, paid.txid)
      assert.deepEqual({ ...got, sent: got.sent.map(s => s.status) }, { ...baseline, sent: baseline.sent.map(s => s.status) }, `${answer}: ${JSON.stringify(got)}`)
      const deadline = Date.now() + 5000
      const notRead = () => logs.slice(logsBefore).filter(l => l.startsWith('provider.answer_not_read provider=arc answer=broadcast '))
      while (notRead().length < 1 && Date.now() < deadline) await new Promise(r => setTimeout(r, 50))
      assert.ok(notRead().length >= 1, `${answer}: no line names the answer not read`)
      say(`inline, ARC's answer past the bound (${answer} if read whole): ${JSON.stringify(got)}, as an unanswered post; "${notRead()[0]}"`)
    }
  }

  // 1d. A subject of exactly 64 bytes is taken as unproven, whatever BUMP its
  // entry names (the library's rule, GHSA-jjhc-7hvh-c3hm): at rest with its
  // entry naming the parent's BUMP, and inline carried by a BUMP of its own,
  // its parent along. Each is recorded unproven, posted, and left to the
  // monitor with a request; no proof is stored.
  {
    const SUBJECT_HEIGHT = HEIGHT + 2
    // One input (funding:0, an empty unlock), one output of `sats` locked by
    // 51 61 61 61: 64 bytes.
    const tx64 = sats => {
      const value = Buffer.alloc(8); value.writeBigUInt64LE(BigInt(sats))
      const raw = Buffer.concat([u32(1), Buffer.from([1]), fundingRoot, u32(0), Buffer.from([0]), u32(0xffffffff),
        Buffer.from([1]), value, Buffer.from([4, 0x51, 0x61, 0x61, 0x61]), u32(0)])
      assert.equal(raw.length, 64)
      return { raw, id: sha256d(raw) }
    }
    const parentBump = Buffer.concat([varint(HEIGHT), Buffer.from([1, 1, 0, 2]), fundingRoot])
    const parentEntry = Buffer.concat([Buffer.from([1, 0]), funding])
    const beefOf = (id, bumps, entries) => Buffer.concat([
      u32(0x01010101), id, u32(0xefbe0002), varint(bumps.length), ...bumps, varint(entries.length), ...entries,
    ])
    const atRestSubject = tx64(1000)
    const atRestBeef = beefOf(atRestSubject.id, [parentBump],
      [parentEntry, Buffer.concat([Buffer.from([1, 0]), atRestSubject.raw])])
    const inlineSubject = tx64(999)
    const sibling = Buffer.alloc(32, 0x5a)
    const ownBump = Buffer.concat([varint(SUBJECT_HEIGHT), Buffer.from([1, 2, 0, 2]), inlineSubject.id, Buffer.from([1, 0]), sibling])
    rootAt.set(SUBJECT_HEIGHT, sha256d(Buffer.concat([inlineSubject.id, sibling])))
    const inlineBeef = beefOf(inlineSubject.id, [parentBump, ownBump],
      [parentEntry, Buffer.concat([Buffer.from([1, 1]), inlineSubject.raw])])
    for (const [form, subject, beefArg] of [
      ['at rest, its entry naming the parent\'s BUMP', atRestSubject, async () => ({ beefAtRest: await upload('02aa/w82-64-at-rest.beef', atRestBeef) })],
      ['inline, a BUMP of its own', inlineSubject, async () => ({ tx: inlineBeef.toString('hex') })],
    ]) {
      const txid = display(subject.id)
      postBodies.length = 0
      arcAnswers = true
      const r = await internalize(await beefArg(), `64 bytes ${form}`)
      const carried = () => postBodies.filter(b => b && b.includes(subject.raw)).length
      const deadline = Date.now() + 5000
      while (carried() < 1 && Date.now() < deadline) await new Promise(r => setTimeout(r, 50))
      arcAnswers = false
      assert.ok(r.result && r.result.accepted, `${form}: ${JSON.stringify(r).slice(0, 600)}`)
      assert.equal(r.result.txid, txid)
      const tx = await db.prepare('SELECT status, proven_tx_id FROM transactions WHERE txid = ?').bind(txid).first()
      assert.deepEqual([tx.status, tx.proven_tx_id], ['unproven', null], `${form}: ${JSON.stringify(tx)}`)
      assert.equal(await db.prepare('SELECT COUNT(*) AS n FROM proven_txs WHERE txid = ?').bind(txid).first('n'), 0)
      const req = await db.prepare('SELECT status FROM proven_tx_reqs WHERE txid = ?').bind(txid).first()
      assert.equal(req?.status, 'unmined', `${form}: ${JSON.stringify(req)}`)
      const output = await db.prepare('SELECT o.satoshis, b.name FROM outputs o JOIN output_baskets b ON b.basket_id = o.basket_id WHERE o.txid = ? AND o.vout = 0').bind(txid).first()
      assert.equal(output?.name, 'nl7', `${form}: ${JSON.stringify(output)}`)
      assert.ok(carried() >= 1, `${form}: the subject was not posted`)
      say(`64 bytes, ${form}: accepted ${txid}; unproven, no proof stored, the request unmined; posted ${carried()} times`)
    }
  }

  // 2 and 3. A chain at rest of exactly the payment budget (9,908 links,
  // 614,400 bytes): accepted, streamed, copied byte for byte.
  const paid = {}
  for (const [n, key] of [[LINKS, '02aa/nl7-at-budget.beef']]) {
    const { beef, txid, raw } = atBudget()
    paid[n] = { beef, txid }
    const reference = await upload(key, beef)
    const t0 = Date.now()
    const r = await internalize({ beefAtRest: reference }, `NL-7 at rest, ${n} links`)
    const ms = Date.now() - t0
    assert.ok(r.result, JSON.stringify(r).slice(0, 2000))
    assert.equal(r.result.accepted, true)
    assert.equal(r.result.txid, txid)
    // The subject is unproven and unknown to the broadcast services: left to
    // the monitor, the outputs demoted until it is seen (INTERNALIZE_ZERO_CONF false).
    assert.deepEqual(r.result.sendWithResults, [{ txid, status: 'unproven' }])
    const tx = await db.prepare('SELECT transaction_id, status, input_beef IS NULL AS in_r2 FROM transactions WHERE txid = ?').bind(txid).first()
    assert.equal(tx.status, 'unproven')
    assert.equal(tx.in_r2, 1)
    const stored = await blobBytes(`transactions/${tx.transaction_id}/input_beef`)
    assert.equal(stored.length, beef.length)
    assert.equal(sha256(stored), sha256(beef))
    const req = await db.prepare('SELECT proven_tx_req_id, status, length(raw_tx) AS raw FROM proven_tx_reqs WHERE txid = ?').bind(txid).first()
    assert.equal(req.status, 'unmined')
    assert.equal(req.raw, raw.length)
    assert.equal(sha256(await blobBytes(`proven_tx_reqs/${req.proven_tx_req_id}/input_beef`)), sha256(beef))
    const output = await db.prepare('SELECT o.satoshis, o.spendable, b.name FROM outputs o JOIN output_baskets b ON b.basket_id = o.basket_id WHERE o.txid = ? AND o.vout = 0').bind(txid).first()
    assert.deepEqual([output.satoshis, output.spendable, output.name], [1000, 0, 'nl7'])
    say(`at rest, ${n} links (${beef.length} bytes): accepted ${txid} in ${ms} ms; the BEEF copied into BLOBS twice byte for byte; the output in nl7, held until the monitor sees it`)
  }

  // 3b. The payment budget (door::payment): exactly the figure passes to the
  // door's own handling, inline and at rest; one byte more is refused before any of it is
  // parsed or read, by the pinned sentence and code, counted by one log line,
  // and no row is written and nothing is copied.
  {
    const counts = async () => {
      const out = {}
      for (const t of ['transactions', 'outputs', 'proven_tx_reqs', 'proven_txs']) {
        out[t] = await db.prepare(`SELECT COUNT(*) AS n FROM "${t}"`).first('n')
      }
      return out
    }
    const counted = []
    const line = size => `payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=${size ?? 'none'} budget=${CAP} identity_key=${caller}`
    const refused = async (name, r, size) => {
      assert.deepEqual(r.error, refusalOf(size), `${name}: ${JSON.stringify(r).slice(0, 600)}`)
      assert.deepEqual(Object.keys(r).sort(), ['error', 'id', 'jsonrpc'])
      counted.push(line(size))
      say(`refused, ${name}: -32602 ${JSON.stringify(r.error.data)} "${r.error.message.slice(0, 64)}..."`)
    }
    const before = await counts()
    const blobsBefore = (await blobs.list()).objects.length
    const logsBefore = logs.length

    // Inline, one byte over: as hex (its size known) and as an array of bytes
    // (read no further than the budget, its size not known).
    const big = oneLarge(1)
    rootAt.set(BIG_HEIGHT, big.root)
    await refused(`inline, ${big.beef.length} bytes as hex`, await internalize({ tx: big.beef.toString('hex') }, 'over the budget'), CAP + 1)
    await refused(`inline, ${big.beef.length} bytes as an array`, await internalize({ tx: [...big.beef] }, 'over the budget'), undefined)

    // At rest, one byte over, and the 100,000-link payment: refused on the
    // length the reference names, which the object's head matches. Bytes that
    // are no BEEF at all, one byte over, are refused the same way: never read.
    const over = atBudget(1)
    await refused(`at rest, ${over.beef.length} bytes`, await internalize({ beefAtRest: await upload('02aa/nl7-over-budget.beef', over.beef) }, 'over the budget'), CAP + 1)
    const hundred = chain(100000)
    await refused(`at rest, the 100000-link payment (${hundred.beef.length} bytes)`, await internalize({ beefAtRest: await upload('02aa/nl7-100k.beef', hundred.beef) }, 'over the budget'), hundred.beef.length)
    const garbage = Buffer.alloc(CAP + 1, 0x5a)
    await refused('at rest, bytes that are no BEEF, one byte over', await internalize({ beefAtRest: await upload('02aa/nl7-garbage-over.beef', garbage) }, 'over the budget'), CAP + 1)

    assert.deepEqual(await counts(), before, 'a refused payment wrote a row')
    assert.equal((await blobs.list()).objects.length, blobsBefore, 'a refused payment was copied into BLOBS')
    for (const txid of [big.txid, over.txid, hundred.txid]) {
      assert.equal(await db.prepare('SELECT COUNT(*) AS n FROM transactions WHERE txid = ?').bind(txid).first('n'), 0)
    }
    // The log lines arrive on the runtime's stream: give it a moment.
    const deadline = Date.now() + 5000
    const notJudged = () => logs.slice(logsBefore).filter(l => l.startsWith('payment.not_judged'))
    while (notJudged().length < counted.length && Date.now() < deadline) await new Promise(r => setTimeout(r, 50))
    assert.deepEqual(notJudged(), counted)
    say(`counted: ${counted.length} lines, the first "${counted[0]}"`)

    // Exactly the budget, inline: the one-transaction payment is accepted; the
    // chain of 9,908 meets the measure (its parse over the measure's budget).
    const exact = oneLarge(0)
    rootAt.set(BIG_HEIGHT, exact.root)
    const r = await internalize({ tx: exact.beef.toString('hex') }, 'exactly the budget')
    assert.ok(r.result && r.result.accepted, JSON.stringify(r).slice(0, 600))
    assert.equal(r.result.txid, exact.txid)
    const chained = await internalize({ tx: paid[LINKS].beef.toString('hex') }, 'exactly the budget, a chain')
    assert.deepEqual(chained.error, { code: -32602, message: 'the BEEF is too large to process' }, JSON.stringify(chained).slice(0, 600))
    say(`inline, exactly ${exact.beef.length} bytes: accepted ${exact.txid}; the chain of ${LINKS} at ${paid[LINKS].beef.length} bytes meets the measure: -32602 "${chained.error.message}"`)
  }

  // 3c. The payment budget at rest, from a caller the store has never seen:
  // a reference one byte over the figure is refused before the caller's user
  // is looked up, by the pinned sentence and code, counted by one log line,
  // and no table gains a row, the users table included. A reference of
  // exactly the figure from a fresh caller is handled as before: accepted,
  // the caller's user written.
  {
    const tables = ['users', 'transactions', 'outputs', 'proven_tx_reqs', 'proven_txs']
    const counts = async () => {
      const out = {}
      for (const t of tables) out[t] = await db.prepare(`SELECT COUNT(*) AS n FROM "${t}"`).first('n')
      return out
    }
    const userRows = key => db.prepare('SELECT COUNT(*) AS n FROM users WHERE identity_key = ?').bind(key).first('n')
    const freshRpc = async (key, args) => {
      const body = JSON.stringify({ jsonrpc: '2.0', method: 'internalizeAction', id: ++id, params: [{ identityKey: '' }, args] })
      const response = await new AuthFetch(new ProtoWallet(key)).fetch(new URL('/', base).toString(), {
        method: 'POST', headers: { 'content-type': 'application/json' }, body,
      })
      const text = await response.text()
      assert.equal(response.status, 200, text)
      return JSON.parse(text)
    }

    const overKey = PrivateKey.fromRandom()
    const overCaller = overKey.toPublicKey().toString()
    const over = atBudget(1)
    const reference = await upload('02cc/nl7-fresh-over.beef', over.beef, overCaller)
    const before = await counts()
    assert.equal(await userRows(overCaller), 0)
    const logsBefore = logs.length
    const r = await freshRpc(overKey, { beefAtRest: reference, outputs, description: 'a fresh caller, over the budget', labels: [], seekPermission: false })
    assert.deepEqual(r.error, refusalOf(CAP + 1), JSON.stringify(r).slice(0, 600))
    assert.deepEqual(await counts(), before, 'a refused reference from a fresh caller wrote a row')
    assert.equal(await userRows(overCaller), 0, 'a refused reference wrote the caller\'s user')
    const expected = [`payment.not_judged code=ERR_PAYMENT_SIZE resource=Size size=${CAP + 1} budget=${CAP} identity_key=${overCaller}`]
    const notJudged = () => logs.slice(logsBefore).filter(l => l.startsWith('payment.not_judged'))
    const deadline = Date.now() + 5000
    while (notJudged().length < expected.length && Date.now() < deadline) await new Promise(r => setTimeout(r, 50))
    assert.deepEqual(notJudged(), expected)
    say(`refused, at rest, a fresh caller, ${over.beef.length} bytes: -32602 ${JSON.stringify(r.error.data)}; counted by one line; users ${before.users} before and after, no row for the caller`)

    const exactKey = PrivateKey.fromRandom()
    const exactCaller = exactKey.toPublicKey().toString()
    const exact = oneLarge(0, 0x5b)
    rootAt.set(BIG_HEIGHT, exact.root)
    const usersBefore = (await counts()).users
    const accepted = await freshRpc(exactKey, { beefAtRest: await upload('02cc/nl7-fresh-exact.beef', exact.beef, exactCaller), outputs, description: 'a fresh caller, exactly the budget', labels: [], seekPermission: false })
    assert.ok(accepted.result && accepted.result.accepted, JSON.stringify(accepted).slice(0, 600))
    assert.equal(accepted.result.txid, exact.txid)
    assert.equal(await userRows(exactCaller), 1)
    assert.equal((await counts()).users, usersBefore + 1)
    say(`at rest, a fresh caller, exactly ${exact.beef.length} bytes: accepted ${exact.txid}; the caller's user written (users ${usersBefore} to ${usersBefore + 1})`)
  }

  // 4. A reference the object does not answer to is refused before a byte is read.
  {
    const { beef } = chain(3)
    const reference = await upload('02aa/nl7-3.beef', beef)
    for (const [name, bad, words] of [
      ['another etag', { ...reference, etag: 'not-the-upload' }, 'another upload is at'],
      ['another size', { ...reference, size: reference.size + 1 }, `is ${reference.size} bytes, the reference names ${reference.size + 1}`],
      ['another bucket', { ...reference, bucket: 'wallet-infra-blobs' }, 'is not bound here'],
      ['no object', { ...reference, r2Key: '02aa/none.beef' }, 'no object at'],
    ]) {
      const r = await internalize({ beefAtRest: bad }, 'NL-7 refused')
      assert.ok(r.error, `${name}: ${JSON.stringify(r)}`)
      assert.ok(r.error.message.includes(words), `${name}: ${r.error.message}`)
      say(`refused, ${name}: ${r.error.message}`)
    }
    const both = await internalize({ beefAtRest: reference, tx: beef.toString('hex') }, 'NL-7 both')
    assert.ok(both.error.message.includes('not both'), both.error.message)
    say(`refused, the BEEF inline and at rest: ${both.error.message}`)
  }

  // 4b. NL-7c: the reference is honoured only for the caller the object names.
  // A valid payment named for another caller, and one naming nobody, are
  // refused by reason; nothing is copied and no row is written. Bytes that
  // are not a BEEF at all, named for another, are refused for the caller,
  // never for the bytes: the body is not read.
  {
    const blobsBefore = (await blobs.list()).objects.length
    const { beef, txid } = chain(4)
    const garbage = Buffer.alloc(300, 0x5a)
    for (const [name, key, bytes, recipient] of [
      ['a payment named for another caller', '02bb/nl7c-another.beef', beef, another],
      ['a payment that names no recipient', '02bb/nl7c-nobody.beef', beef, null],
      ['bytes that are no BEEF, named for another', '02bb/nl7c-garbage.beef', garbage, another],
    ]) {
      const r = await internalize({ beefAtRest: await upload(key, bytes, recipient) }, 'NL-7c refused')
      assert.ok(r.error, `${name}: accepted ${JSON.stringify(r).slice(0, 300)}`)
      const words = recipient === null ? 'names no recipient' : 'names another recipient'
      assert.ok(r.error.message.includes(words), `${name}: ${r.error.message}`)
      assert.ok(!r.error.message.includes(another), `${name}: the refusal says whose: ${r.error.message}`)
      say(`refused, ${name}: ${r.error.message}`)
    }
    assert.equal((await blobs.list()).objects.length, blobsBefore, 'a refused reference copied into BLOBS')
    assert.equal(await db.prepare('SELECT COUNT(*) AS n FROM transactions WHERE txid = ?').bind(txid).first('n'), 0)
    // The same payment named for the caller is accepted.
    const r = await internalize({ beefAtRest: await upload('02aa/nl7c-mine.beef', beef) }, 'NL-7c mine')
    assert.ok(r.result && r.result.accepted, JSON.stringify(r).slice(0, 300))
    say(`accepted, the same payment named for the caller: ${txid}`)
  }

  // 5. Invalid bytes at rest are refused at their offset, by their kind.
  {
    const { beef } = chain(5)
    const broken = Buffer.from(beef)
    broken[beef.length - (4 + 1 + 32 + 4 + 1 + 4 + 1 + 8 + 1 + 1 + 4) + 5] ^= 0xff
    const r = await internalize({ beefAtRest: await upload('02aa/nl7-broken.beef', broken) }, 'NL-7 broken')
    assert.ok(r.error, JSON.stringify(r))
    assert.ok(r.error.message.includes('refused at offset 348: InputNamesNoElement'), r.error.message)
    say(`refused, an input naming nothing: ${r.error.message.slice(0, 120)}`)
    carry = false
    const { beef: three } = chain(3)
    const unrooted = await internalize({ beefAtRest: await upload('02aa/nl7-unrooted.beef', three) }, 'NL-7 unrooted')
    carry = true
    assert.ok(unrooted.error.message.includes('refused at offset 41: RootNotCarried'), unrooted.error.message)
    say(`refused, a root the header service does not carry: ${unrooted.error.message.slice(0, 120)}`)
  }

  // 5b. The audit's repair path requires the monitor trigger key. An output is
  // planted locked by a failed transaction, the row the repair would release,
  // so "outputs unchanged" holds only if the audit did not run (#22).
  const planted = {}
  {
    const user = await db.prepare('SELECT user_id FROM users WHERE identity_key = ?').bind(caller).first('user_id')
    const failed = await db.prepare("INSERT INTO transactions (user_id, status, reference, is_outgoing, description) VALUES (?, 'failed', 'w24-planted-failed', 1, 'planted: a failed spend') RETURNING transaction_id").bind(user).first('transaction_id')
    const output = await db.prepare('SELECT output_id FROM outputs WHERE spendable = 1 AND spent_by IS NULL ORDER BY output_id LIMIT 1').first('output_id')
    assert.ok(failed && output, `planting: transaction ${failed}, output ${output}`)
    await db.prepare('UPDATE outputs SET spendable = 0, spent_by = ? WHERE output_id = ?').bind(failed, output).run()
    Object.assign(planted, { failed, output })
    const before = (await db.prepare('SELECT * FROM outputs ORDER BY output_id').all()).results
    const response = await fetch(new URL('/monitor/audit', base))
    const refusal = await response.json()
    assert.equal(response.status, 401, JSON.stringify(refusal))
    assert.deepEqual(refusal, { error: 'unauthorized — set MONITOR_TRIGGER_KEY secret and pass ?key=<value>' })
    assert.deepEqual((await db.prepare('SELECT * FROM outputs ORDER BY output_id').all()).results, before)
    say(`audit, no key: 401; outputs unchanged, output ${output} still locked by failed transaction ${failed}`)
  }

  // 5c. The trigger key admits the audit and its summary; the repair releases the planted output.
  {
    const response = await fetch(new URL(`/monitor/audit?key=${MONITOR_KEY}`, base))
    const report = await response.json()
    assert.equal(response.status, 200, JSON.stringify(report))
    assert.ok(report.summary, JSON.stringify(report))
    assert.ok(report.summary.lockedByFailed >= 1, JSON.stringify(report.summary))
    const row = await db.prepare('SELECT spendable, spent_by FROM outputs WHERE output_id = ?').bind(planted.output).first()
    assert.deepEqual([row.spendable, row.spent_by], [1, null])
    await db.prepare('DELETE FROM transactions WHERE transaction_id = ?').bind(planted.failed).run()
    say(`audit, trigger key: 200; summary lockedByFailed ${report.summary.lockedByFailed}, output ${planted.output} released`)
  }

  // 7. NL-7c: the monitor posts the subject at rest, which has no proof and
  // which the network does not know, as a stream: the stored AtomicBEEF's
  // BEEF behind its 36-byte prefix, application/octet-stream, its length
  // known, byte for byte; never the whole BEEF parsed, re-written and
  // hex-encoded into JSON. The row is handed back as the escalation's
  // Rebroadcast hands it (`unsent`). Under BROADCASTER arc, then arcade
  // (Arcade's /tx takes no BEEF: the stream goes to ARC).
  {
    const { beef, txid } = paid[LINKS]
    const want = { bytes: beef.length - 36, sha: sha256(beef.subarray(36)) }
    for (const broadcaster of ['arc', 'arcade']) {
      if (broadcaster === 'arcade') {
        await mf.setOptions({ ...options, bindings: { ...options.bindings, BROADCASTER: 'arcade', ARCADE_URL: 'https://arcade.test' } })
      }
      const url = await mf.ready
      const d1 = await mf.getD1Database('DB')
      await d1.prepare("UPDATE proven_tx_reqs SET status = 'unsent', attempts = 0 WHERE txid = ?").bind(txid).run()
      posts.length = 0
      const before = out.length
      arcAnswers = true
      const t0 = Date.now()
      const response = await fetch(new URL(`/monitor/run?key=${MONITOR_KEY}`, url), { method: 'POST' })
      const ms = Date.now() - t0
      arcAnswers = false
      const run = await response.json()
      assert.equal(response.status, 200, JSON.stringify(run))
      const whole = posts.filter(p => p.bytes !== undefined)
      assert.ok(whole.length >= 1, `${broadcaster}: no post was received: ${JSON.stringify(posts)}`)
      for (const post of whole) {
        assert.equal(post.type, 'application/octet-stream', `${broadcaster}: ${JSON.stringify(post)}`)
        assert.equal(post.bytes, want.bytes, `${broadcaster}: ${JSON.stringify(post)}`)
        assert.equal(post.sha, want.sha, `${broadcaster}: ${JSON.stringify(post)}`)
        assert.equal(post.length, String(want.bytes), `${broadcaster}: ${JSON.stringify(post)}`)
      }
      assert.ok(run.sent >= 1, JSON.stringify(run).slice(0, 600))
      const req = await d1.prepare('SELECT status FROM proven_tx_reqs WHERE txid = ?').bind(txid).first()
      assert.equal(req.status, 'unmined')
      const arcade = out.slice(before).filter(u => u.startsWith('arcade.test/tx'))
      assert.deepEqual(arcade, [], `${broadcaster}: the BEEF went to Arcade's /tx`)
      say(`monitor, ${broadcaster}: the ${LINKS}-link subject at rest posted as a stream in ${ms} ms to ${whole.map(p => p.host).join(' and ')}: application/octet-stream, content-length ${want.bytes}, byte for byte the BEEF behind the prefix; the row unmined`)
    }
  }

  // 6. The requests the Worker made: the header service and the status lookups.
  const hosts = [...new Set(out.map(u => u.split('/')[0]))].sort()
  say(`outbound hosts answered here: ${hosts.join(', ')}`)
  console.log('Local compiled Worker GREEN (NL-7, NL-7c): internalizeAction takes a BEEF at rest in R2, streamed, held to its reference and to the caller the object names, refused at the offset and the kind; the monitor posts it as a stream')
} finally {
  await mf.dispose()
  rmSync(persist, { recursive: true, force: true })
}
