// Local compiled-Worker witness for #49: hold the proof pending until the header answers.
// A proof pushed by the Arcade callback and a proof found by the monitor, with the header
// service unanswered, then answering, then a planted bad root, then a proof over the
// payment figure on both paths, then provider answers over the answer bound
// (WhatsOnChain's and Bitails's proofs, the callback's body, Arcade's stored
// record), each not read past it. Every request the Worker
// makes is answered here; nothing leaves this process. No deploy.
//
// worker-build --release
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js node tests/worker_proof_hold.mjs
import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { spawnSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'
const { Miniflare } = await import(process.env.MINIFLARE_MODULE ?? 'miniflare')
const sha = b => createHash('sha256').update(b).digest()
const hash = b => sha(sha(b))
const display = b => Buffer.from(b).reverse().toString('hex')
const u32 = n => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b }
const varint = n => { const b = Buffer.alloc(5); b[0] = 0xfe; b.writeUInt32LE(n, 1); return b }
const raw = marker => Buffer.concat([u32(1), Buffer.from([1]), Buffer.alloc(32, marker), u32(0), Buffer.from([0]), u32(0xffffffff), Buffer.from([1]), Buffer.from('e803000000000000', 'hex'), Buffer.from([1, 0x51]), u32(marker)])
// A two-leaf BRC-74 path: our txid at offset 0 (flag 2), a sibling at offset 1.
const bump = (tx, height) => {
  const leaf = hash(tx), sibling = Buffer.alloc(32, 0x5a)
  const path = Buffer.concat([varint(height), Buffer.from([1]), Buffer.from([2]), Buffer.from([0, 2]), leaf, Buffer.from([1, 0]), sibling])
  return { hex: path.toString('hex'), root: display(hash(Buffer.concat([leaf, sibling]))) }
}
// One height per proof: the header service carries one root per height.
const PUSHED = 900001, FOUND = 900002, BAD_PUSHED = 900003, BAD_FOUND = 900004
const TOKEN = 'fixture-callback', KEY = 'fixture-monitor'
let headersUp = false
const arcProofs = new Map() // txid -> { hex, height }
const roots = new Map() // height -> the root our header service carries
const logs = [] // the Worker's log lines
// Provider answers over the answer bound (door::payment::answer_bound: twice
// the figure and 64 KiB for the JSON wrapping), served by the harness.
const CAP = 614400, BOUND = 2 * CAP + 65536
const wocProofs = new Map(), bitailsProofs = new Map(), arcadeRecords = new Map() // txid -> 'declared' | 'stream'
// A streamed answer is eight times the bound: the Worker's own line names the
// bytes it held when it stopped, which must be past the bound and short of
// the whole.
const STREAM_TOTAL = 8 * BOUND
// A body that declares its length (over the bound), or one that is streamed
// with no length: `head`, then 'a' up to `total` bytes, then `tail`.
const declared = n => new Response('a'.repeat(n), { headers: { 'content-length': String(n) } })
const streamed = (total = STREAM_TOTAL, head = '', tail = '', chunk = 65536) => {
  const enc = new TextEncoder(), first = enc.encode(head), last = enc.encode(tail)
  let sent = 0, started = false
  return new ReadableStream({ pull(c) {
    if (!started) { started = true; if (first.length) { sent += first.length; return c.enqueue(first) } }
    const fill = total - last.length - sent
    if (fill <= 0) { if (last.length) c.enqueue(last); return c.close() }
    const n = Math.min(chunk, fill); sent += n; c.enqueue(new Uint8Array(n).fill(0x61))
  } })
}
const over = how => how === 'declared' ? declared(BOUND + 1) : new Response(streamed())
const mf = new Miniflare({ modules: true, scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [{ type: 'ESModule', include: ['**/*.js', '**/*.mjs'] }, { type: 'CompiledWasm', include: ['**/*.wasm'] }],
  compatibilityDate: '2024-01-01', host: '127.0.0.1', port: 0,
  bindings: { SERVER_PRIVATE_KEY: '11'.repeat(32), CHAINTRACKS_URL: 'https://headers.test', BROADCASTER: 'arc', MONITOR_TRIGGER_KEY: KEY, ARCADE_CALLBACK_TOKEN: TOKEN, ARCADE_URL: 'https://arcade.test' },
  d1Databases: ['DB'], kvNamespaces: ['AUTH_SESSIONS'], r2Buckets: ['BLOBS'],
  outboundService: async request => {
    const url = new URL(request.url)
    const height = Number(url.searchParams.get('height') ?? url.pathname.split('/').pop())
    if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
      if (!headersUp) return new Response('unavailable', { status: 503 })
      return new Response(JSON.stringify({ status: 'success', value: { merkleRoot: roots.get(height) ?? '00'.repeat(32), height } }), { headers: { 'content-type': 'application/json' } })
    }
    if (url.host === 'api.whatsonchain.com' && url.pathname.includes('/block/height/') && !headersUp) return new Response('unavailable', { status: 503 })
    const tsc = url.pathname.match(/\/tx\/([0-9a-f]{64})\/proof\/tsc$/)
    if (tsc && url.host === 'api.whatsonchain.com' && wocProofs.has(tsc[1])) return over(wocProofs.get(tsc[1]))
    if (tsc && url.host === 'api.bitails.io' && bitailsProofs.has(tsc[1])) return over(bitailsProofs.get(tsc[1]))
    const record = url.pathname.match(/^\/tx\/([0-9a-f]{64})$/)
    if (url.host === 'arcade.test' && record && arcadeRecords.has(record[1])) return over(arcadeRecords.get(record[1]))
    const m = url.pathname.match(/^\/v1\/tx\/([0-9a-f]{64})$/)
    if (url.host.startsWith('arc.') && request.method === 'GET' && m && arcProofs.has(m[1])) {
      const p = arcProofs.get(m[1])
      return new Response(JSON.stringify({ txid: m[1], txStatus: 'MINED', blockHeight: p.height, blockHash: 'cd'.repeat(32), merklePath: p.hex }), { headers: { 'content-type': 'application/json' } })
    }
    return new Response('not found', { status: 404 })
  },
  handleStructuredLogs: ({ message }) => { logs.push(String(message).trim()) },
})
let failed = 0
try {
  const base = await mf.ready
  const db = await mf.getD1Database('DB')
  for (const name of readdirSync('migrations').filter(n => n.endsWith('.sql')).sort()) {
    const split = spawnSync('python3', ['-c', `import json, sqlite3, sys
statements, pending = [], ''
for char in sys.stdin.read():
 pending += char
 if char == ';' and sqlite3.complete_statement(pending):
  statements.append(pending); pending = ''
print(json.dumps(statements))`], { input: readFileSync('migrations/' + name, 'utf8'), encoding: 'utf8' })
    assert.equal(split.status, 0)
    await db.batch(JSON.parse(split.stdout).map(sql => db.prepare(sql)))
  }
  const exec = (sql, ...args) => db.prepare(sql).bind(...args.map(a => Buffer.isBuffer(a) ? new Uint8Array(a) : a)).run()
  await exec("INSERT INTO users (identity_key) VALUES ('02' || ?)", 'aa'.repeat(32))
  const seed = async (marker, height) => {
    const tx = raw(marker), id = display(hash(tx))
    await exec("INSERT INTO transactions (user_id, reference, status, is_outgoing, description, txid, raw_tx) VALUES (1, ?, 'unproven', 1, 'fixture', ?, ?)", `ref-${marker}`, id, tx)
    await exec("INSERT INTO proven_tx_reqs (txid, raw_tx, status) VALUES (?, ?, 'unmined')", id, tx)
    const b = bump(tx, height)
    return { txid: id, height, ...b }
  }
  const req = s => db.prepare('SELECT r.status, r.attempts, r.proven_tx_id, r.history, t.status AS tx_status FROM proven_tx_reqs r JOIN transactions t ON t.txid = r.txid WHERE r.txid = ?').bind(s.txid).first()
  const proven = async s => db.prepare('SELECT merkle_root, height FROM proven_txs WHERE txid = ?').bind(s.txid).first()
  const provenCount = async () => (await db.prepare('SELECT COUNT(*) AS n FROM proven_txs').first()).n
  const holds = async s => (await db.prepare("SELECT details FROM monitor_events WHERE event = 'proof_held'").all()).results.map(r => JSON.parse(r.details)).filter(d => d.txid === s.txid)
  const callback = async s => {
    const r = await mf.dispatchFetch(new URL('/arcade/callback', base), { method: 'POST', headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' }, body: JSON.stringify({ txid: s.txid, txStatus: 'MINED', blockHash: 'cd'.repeat(32), blockHeight: s.height, merklePath: s.hex }) })
    assert.equal(r.status, 200, await r.clone().text()); return r.json()
  }
  const monitor = async () => { const r = await mf.dispatchFetch(new URL(`/monitor/run?key=${KEY}`, base), { method: 'POST' }); assert.equal(r.status, 200, await r.text()) }
  const check = async (name, f) => { try { await f(); console.log(`PASS ${name}`) } catch (e) { failed++; console.log(`FAIL ${name}: ${e.message}`) } }
  const pushed = await seed(1, PUSHED), found = await seed(2, FOUND)
  roots.set(PUSHED, pushed.root); roots.set(FOUND, found.root)
  arcProofs.set(found.txid, found)
  await check('a header unanswered: the pushed proof is held, nothing written, the request keeps its state, one row names the hold', async () => {
    const before = await req(pushed)
    const r = await callback(pushed); assert.equal(r.processed, 0)
    assert.equal(await provenCount(), 0); assert.deepEqual(await req(pushed), before)
    const h = await holds(pushed); assert.equal(h.length, 1); assert.equal(h[0].path, 'arcade_callback'); assert.ok(h[0].reason.length > 0)
  })
  await check('b header unanswered: the proof the monitor finds is held, nothing written, the request keeps its state, one row names the hold', async () => {
    const before = await req(found)
    await monitor()
    assert.equal(await provenCount(), 0); assert.deepEqual(await req(found), before); assert.equal(before.status, 'unmined')
    const h = await holds(found); assert.equal(h.length, 1); assert.equal(h[0].path, 'store_proof'); assert.equal(h[0].height, found.height); assert.ok(h[0].reason.length > 0)
  })
  await check('c the next run re-asks while the header stays unanswered: held again, still nothing written', async () => {
    await monitor()
    assert.equal(await provenCount(), 0); assert.equal((await req(found)).status, 'unmined'); assert.equal((await holds(found)).length, 2)
  })
  headersUp = true
  await check('d header answering: the pushed proof is checked against the root and stored', async () => {
    const r = await callback(pushed); assert.equal(r.processed, 1)
    const p = await proven(pushed); assert.equal(p.merkle_root, pushed.root); assert.equal(p.height, pushed.height)
    const s = await req(pushed); assert.equal(s.status, 'completed'); assert.equal(s.tx_status, 'completed'); assert.ok(s.proven_tx_id)
    assert.equal((await holds(pushed)).length, 1)
  })
  await check('e header answering: the proof the monitor finds is checked and stored on the next run', async () => {
    await monitor()
    const p = await proven(found); assert.equal(p.merkle_root, found.root)
    const s = await req(found); assert.equal(s.status, 'completed'); assert.equal(s.tx_status, 'completed')
    assert.equal((await holds(found)).length, 2)
  })
  const badPush = await seed(4, BAD_PUSHED), badFound = await seed(5, BAD_FOUND)
  roots.set(BAD_PUSHED, '11'.repeat(32)); roots.set(BAD_FOUND, '22'.repeat(32))
  arcProofs.clear(); arcProofs.set(badFound.txid, badFound)
  await check('f a planted bad root is refused on both paths: nothing written, no hold, the request keeps its state', async () => {
    const n = await provenCount(), b1 = await req(badPush), b2 = await req(badFound)
    const r = await callback(badPush); assert.equal(r.processed, 0)
    await monitor()
    assert.equal(await provenCount(), n); assert.equal(await proven(badPush), null); assert.equal(await proven(badFound), null)
    assert.deepEqual(await req(badPush), b1); assert.deepEqual(await req(badFound), b2)
    assert.equal((await holds(badPush)).length, 0); assert.equal((await holds(badFound)).length, 0)
  })
  // g. A provider's answer over the payment figure (door::payment, 614,400
  // bytes where MAX_PAYMENT_BYTES is unset) is not parsed: the pushed proof
  // and ARC's answer to the monitor each carry a merkle path one byte over,
  // its root the one the header service carries. Each is taken as that
  // provider unavailable: nothing is stored, no hold, the request keeps its
  // state, and one line names the provider and the size.
  const OVER_PUSHED = 900005, OVER_FOUND = 900006
  const overPush = await seed(6, OVER_PUSHED), overFound = await seed(7, OVER_FOUND)
  roots.set(OVER_PUSHED, overPush.root); roots.set(OVER_FOUND, overFound.root)
  for (const o of [overPush, overFound]) o.hex += '00'.repeat(CAP + 1 - o.hex.length / 2)
  arcProofs.clear(); arcProofs.set(overFound.txid, overFound)
  await check('g a proof over the payment figure is not read on either path: nothing written, no hold, the request keeps its state, a line names the provider and the size', async () => {
    assert.equal(overPush.hex.length / 2, CAP + 1)
    const n = await provenCount(), b1 = await req(overPush), b2 = await req(overFound)
    const logsBefore = logs.length
    const r = await callback(overPush); assert.equal(r.processed, 0)
    await monitor()
    assert.equal(await provenCount(), n); assert.equal(await proven(overPush), null); assert.equal(await proven(overFound), null)
    assert.deepEqual(await req(overPush), b1); assert.equal((await req(overFound)).status, b2.status)
    assert.equal((await req(overFound)).proven_tx_id, null)
    assert.equal((await holds(overPush)).length, 0); assert.equal((await holds(overFound)).length, 0)
    const line = provider => `provider.answer_not_read provider=${provider} answer=proof size=${CAP + 1} budget=${CAP}`
    const deadline = Date.now() + 5000
    const seen = provider => logs.slice(logsBefore).some(l => l.includes(line(provider)))
    while (!(seen('arcade') && seen('arc')) && Date.now() < deadline) await new Promise(r => setTimeout(r, 50))
    assert.ok(seen('arcade'), 'no line for the pushed proof')
    assert.ok(seen('arc'), "no line for ARC's answer")
    console.log(`  ${line('arcade')}; ${line('arc')}`)
  })
  // h-j. A provider's answer over the answer bound (twice the figure and
  // 64 KiB, 1,294,336 bytes where MAX_PAYMENT_BYTES is unset) is not read past
  // it: one that declares a longer length is not read, one streamed with no
  // length is read no further than the chunk that passes the bound (the
  // Worker's line names the bytes it held then: past the bound, short of
  // the whole). Each ends
  // on that provider's error road (as g), names the provider and the bound in
  // one line, and writes nothing.
  arcProofs.clear()
  const notReadLine = (provider, what) => new RegExp(`^provider\\.answer_not_read provider=${provider} answer=${what} (declared|read)=(\\d+) bound=${BOUND}$`)
  const waitFor = async (since, patterns) => {
    const deadline = Date.now() + 5000
    const found = () => patterns.map(p => logs.slice(since).find(l => p.test(l)))
    while (found().some(l => !l) && Date.now() < deadline) await new Promise(r => setTimeout(r, 50))
    return found()
  }
  const readOf = line => { const m = line.match(/ (declared|read)=(\d+) /); return { how: m[1], n: Number(m[2]) } }
  const HEIGHT_H = 900007, HEIGHT_I = 900008, HEIGHT_J = 900009
  const viaExplorers = await seed(8, HEIGHT_H), viaCallback = await seed(9, HEIGHT_I), viaRecord = await seed(10, HEIGHT_J)
  roots.set(HEIGHT_H, viaExplorers.root); roots.set(HEIGHT_I, viaCallback.root); roots.set(HEIGHT_J, viaRecord.root)
  await check("h WhatsOnChain's proof declared over the bound is not read, Bitails's streamed proof is not read past it: nothing written, the request keeps its state", async () => {
    wocProofs.set(viaExplorers.txid, 'declared'); bitailsProofs.set(viaExplorers.txid, 'stream')
    const n = await provenCount(), before = await req(viaExplorers), since = logs.length
    await monitor()
    const [woc, bitails] = await waitFor(since, [notReadLine('woc', 'proof'), notReadLine('bitails', 'proof')])
    assert.ok(woc, 'no line for WhatsOnChain'); assert.ok(bitails, 'no line for Bitails')
    assert.deepEqual(readOf(woc), { how: 'declared', n: BOUND + 1 })
    const b = readOf(bitails); assert.equal(b.how, 'read'); assert.ok(b.n > BOUND && b.n < STREAM_TOTAL, bitails)
    assert.equal(await provenCount(), n); assert.equal(await proven(viaExplorers), null)
    assert.deepEqual(await req(viaExplorers), before); assert.equal((await holds(viaExplorers)).length, 0)
    wocProofs.clear(); bitailsProofs.clear()
    console.log(`  ${woc}; ${bitails} (of ${STREAM_TOTAL} streamed)`)
  })
  await check("i the callback's body over the bound, declared and streamed, is not read past it: acknowledged, nothing written, the request keeps its state", async () => {
    // The event itself is good (its root the one the header service carries):
    // read whole, it would be stored. The padding carries it past the bound.
    // Declared: sent by fetch, which names its length (under the 8 MiB body
    // bound). Streamed: eight times the bound, with no length.
    const fields = { txid: viaCallback.txid, txStatus: 'MINED', blockHash: 'cd'.repeat(32), blockHeight: viaCallback.height, merklePath: viaCallback.hex }
    const event = JSON.stringify({ ...fields, pad: 'a'.repeat(BOUND) })
    assert.ok(event.length > BOUND)
    const n = await provenCount(), before = await req(viaCallback)
    const headers = { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' }
    let since = logs.length
    const declaredAnswer = await fetch(new URL('/arcade/callback', base), { method: 'POST', headers, body: event })
    assert.equal(declaredAnswer.status, 200); assert.deepEqual(await declaredAnswer.json(), { ok: true, processed: 0 })
    const [declaredLine] = await waitFor(since, [notReadLine('arcade', 'callback')])
    assert.ok(declaredLine, 'no line for the declared body'); assert.deepEqual(readOf(declaredLine), { how: 'declared', n: Buffer.byteLength(event) })
    since = logs.length
    const head = JSON.stringify(fields).slice(0, -1) + ',"pad":"'
    const body = streamed(STREAM_TOTAL, head, '"}')
    const streamedAnswer = await mf.dispatchFetch(new URL('/arcade/callback', base), { method: 'POST', headers, body, duplex: 'half' })
    assert.equal(streamedAnswer.status, 200); assert.deepEqual(await streamedAnswer.json(), { ok: true, processed: 0 })
    const [streamedLine] = await waitFor(since, [notReadLine('arcade', 'callback')])
    assert.ok(streamedLine, 'no line for the streamed body')
    const r = readOf(streamedLine); assert.equal(r.how, 'read'); assert.ok(r.n > BOUND && r.n < STREAM_TOTAL, streamedLine)
    assert.equal(await provenCount(), n); assert.equal(await proven(viaCallback), null)
    assert.deepEqual(await req(viaCallback), before); assert.equal((await holds(viaCallback)).length, 0)
    console.log(`  ${declaredLine}; ${streamedLine} (of ${STREAM_TOTAL} streamed)`)
  })
  await check("j Arcade's stored record over the bound, declared and streamed, is not read past it: the event is acknowledged and ignored, nothing written, the request keeps its state", async () => {
    const n = await provenCount(), before = await req(viaRecord)
    const lines = []
    for (const how of ['declared', 'stream']) {
      arcadeRecords.set(viaRecord.txid, how)
      const since = logs.length
      // A MINED event with no merkle path: the handler reads Arcade's record.
      const r = await mf.dispatchFetch(new URL('/arcade/callback', base), { method: 'POST', headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' }, body: JSON.stringify({ txid: viaRecord.txid, txStatus: 'MINED', blockHash: 'cd'.repeat(32), blockHeight: viaRecord.height }) })
      assert.equal(r.status, 200); const answer = await r.json(); assert.equal(answer.processed, 0, JSON.stringify(answer))
      const [line] = await waitFor(since, [notReadLine('arcade', 'record')])
      assert.ok(line, `no line for the ${how} record`)
      const got = readOf(line)
      if (how === 'declared') assert.deepEqual(got, { how: 'declared', n: BOUND + 1 })
      else { assert.equal(got.how, 'read'); assert.ok(got.n > BOUND && got.n < STREAM_TOTAL, line) }
      lines.push(line)
    }
    arcadeRecords.clear()
    assert.equal(await provenCount(), n); assert.equal(await proven(viaRecord), null)
    assert.deepEqual(await req(viaRecord), before); assert.equal((await holds(viaRecord)).length, 0)
    console.log(`  ${lines.join('; ')} (of ${STREAM_TOTAL} streamed)`)
  })
  console.log(`Proof hold Worker witnesses: ${10 - failed} passed; ${failed} failed`)
} finally { await mf.dispose() }
if (failed) process.exitCode = 1
