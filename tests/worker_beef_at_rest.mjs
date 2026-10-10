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
const spend = (prev, outputs) => {
  const parts = [u32(1), Buffer.from([1]), prev, u32(0), Buffer.from([0]), u32(0xffffffff), varint(outputs)]
  for (let i = 0; i < outputs; i++) {
    const sats = Buffer.alloc(8); sats.writeBigUInt64LE(1000n)
    parts.push(sats, Buffer.from([1, 0x51]))
  }
  parts.push(u32(0))
  return Buffer.concat(parts)
}
const funding = spend(Buffer.alloc(32, 0xaa), 2)
const fundingRoot = sha256d(funding)

/** The AtomicBEEF of the chain of `n` transactions, and its subject. */
const chain = n => {
  const txs = [Buffer.concat([Buffer.from([1, 0]), funding])]
  let prev = fundingRoot
  for (let i = 1; i < n; i++) {
    const raw = spend(prev, 1)
    prev = sha256d(raw)
    txs.push(Buffer.concat([Buffer.from([0]), raw]))
  }
  const head = Buffer.concat([
    u32(0x01010101), prev,
    u32(0xefbe0002), Buffer.from([1]), varint(HEIGHT), Buffer.from([1, 1, 0, 2]), fundingRoot,
    varint(n),
  ])
  return { beef: Buffer.concat([head, ...txs]), txid: display(prev) }
}

// ---------------------------------------------------------------------------
// The Worker
// ---------------------------------------------------------------------------

const BUCKET = 'bsv-messagebox-beefs'
const MONITOR_KEY = 'nl7c-local'
const out = []
let carry = true
// NL-7c: while `arcAnswers`, ARC takes every post (SEEN_ON_NETWORK) and each
// is recorded: the host, the content type and length, the bytes' SHA-256.
let arcAnswers = false
const posts = []
const outbound = async request => {
  const url = new URL(request.url)
  out.push(url.host + url.pathname)
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
    return new Response(JSON.stringify({ txid: '', txStatus: 'SEEN_ON_NETWORK' }),
      { headers: { 'content-type': 'application/json' } })
  }
  if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
    const height = Number(url.searchParams.get('height'))
    const merkleRoot = carry && height === HEIGHT ? display(fundingRoot) : '00'.repeat(32)
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

  // 2 and 3. The relay's two shapes at rest: the 100,000-link payment and the
  // first chain over the relay's 8 MiB line (DRAIN_INLINE_BYTES).
  const paid = {}
  for (const [n, key] of [[100000, '02aa/nl7-100k.beef'], [135299, '02aa/nl7-over-8mib.beef']]) {
    const { beef, txid } = chain(n)
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
    assert.equal(req.raw, 61)
    assert.equal(sha256(await blobBytes(`proven_tx_reqs/${req.proven_tx_req_id}/input_beef`)), sha256(beef))
    const output = await db.prepare('SELECT o.satoshis, o.spendable, b.name FROM outputs o JOIN output_baskets b ON b.basket_id = o.basket_id WHERE o.txid = ? AND o.vout = 0').bind(txid).first()
    assert.deepEqual([output.satoshis, output.spendable, output.name], [1000, 0, 'nl7'])
    say(`at rest, ${n} links (${beef.length} bytes): accepted ${txid} in ${ms} ms; the BEEF copied into BLOBS twice byte for byte; the output in nl7, held until the monitor sees it`)
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

  // 7. NL-7c: the monitor posts the subject at rest, which has no proof and
  // which the network does not know, as a stream: the stored AtomicBEEF's
  // BEEF behind its 36-byte prefix, application/octet-stream, its length
  // known, byte for byte; never the whole BEEF parsed, re-written and
  // hex-encoded into JSON. The row is handed back as the escalation's
  // Rebroadcast hands it (`unsent`). Under BROADCASTER arc, then arcade
  // (Arcade's /tx takes no BEEF: the stream goes to ARC).
  {
    const { beef, txid } = paid[100000]
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
      say(`monitor, ${broadcaster}: the 100000-link subject at rest posted as a stream in ${ms} ms to ${whole.map(p => p.host).join(' and ')}: application/octet-stream, content-length ${want.bytes}, byte for byte the BEEF behind the prefix; the row unmined`)
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
