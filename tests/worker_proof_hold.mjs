// Local compiled-Worker witness for #49: hold the proof pending until the header answers.
// A proof pushed by the Arcade callback and a proof found by the monitor, with the header
// service unanswered, then answering, then a planted bad root. Every request the Worker
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
const mf = new Miniflare({ modules: true, scriptPath: resolve('build/worker/shim.mjs'),
  modulesRules: [{ type: 'ESModule', include: ['**/*.js', '**/*.mjs'] }, { type: 'CompiledWasm', include: ['**/*.wasm'] }],
  compatibilityDate: '2024-01-01', host: '127.0.0.1', port: 0,
  bindings: { SERVER_PRIVATE_KEY: '11'.repeat(32), CHAINTRACKS_URL: 'https://headers.test', BROADCASTER: 'arc', MONITOR_TRIGGER_KEY: KEY, ARCADE_CALLBACK_TOKEN: TOKEN },
  d1Databases: ['DB'], kvNamespaces: ['AUTH_SESSIONS'], r2Buckets: ['BLOBS'],
  outboundService: async request => {
    const url = new URL(request.url)
    const height = Number(url.searchParams.get('height') ?? url.pathname.split('/').pop())
    if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
      if (!headersUp) return new Response('unavailable', { status: 503 })
      return new Response(JSON.stringify({ status: 'success', value: { merkleRoot: roots.get(height) ?? '00'.repeat(32), height } }), { headers: { 'content-type': 'application/json' } })
    }
    if (url.host === 'api.whatsonchain.com' && url.pathname.includes('/block/height/') && !headersUp) return new Response('unavailable', { status: 503 })
    const m = url.pathname.match(/^\/v1\/tx\/([0-9a-f]{64})$/)
    if (url.host.startsWith('arc.') && request.method === 'GET' && m && arcProofs.has(m[1])) {
      const p = arcProofs.get(m[1])
      return new Response(JSON.stringify({ txid: m[1], txStatus: 'MINED', blockHeight: p.height, blockHash: 'cd'.repeat(32), merklePath: p.hex }), { headers: { 'content-type': 'application/json' } })
    }
    return new Response('not found', { status: 404 })
  },
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
  console.log(`Proof hold Worker witnesses: ${6 - failed} passed; ${failed} failed`)
} finally { await mf.dispose() }
if (failed) process.exitCode = 1
