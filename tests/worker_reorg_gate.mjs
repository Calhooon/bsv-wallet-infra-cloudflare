// Local compiled-Worker witness for the monitor's two reorg passes: a proof
// is stored only after the store gate checks it (the path holds this row's
// txid, its root is the header service's root at that height). A stored proof
// whose own root the header service no longer carries is replaced only by a
// proof the gate passes; otherwise the row and its request are left as they
// are, the row is recorded, and the deep sweep stores a checked proof later.
//
// The shallow sweep runs in the first five minutes of an hour, so the entry
// below sets the Worker's clock: minute 1 for the runs that should sweep,
// minute 31 for the others. Every request the Worker makes is answered here;
// nothing leaves this process. No deploy.
//
// worker-build --release
// MINIFLARE_MODULE=<miniflare>/dist/src/index.js node tests/worker_reorg_gate.mjs
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
// A two-leaf BRC-74 path at `height`: the leaf hashed from `tx` at `offset`
// (txid flag), a sibling beside it. Returns the path and its root.
const bump = (tx, height, offset = 0, siblingByte = 0x5a) => {
  const leaf = hash(tx), sibling = Buffer.alloc(32, siblingByte)
  const ours = Buffer.concat([Buffer.from([offset, 2]), leaf]), sib = Buffer.concat([Buffer.from([offset ^ 1, 0]), sibling])
  const level = offset === 0 ? [ours, sib] : [sib, ours]
  const root = display(hash(offset === 0 ? Buffer.concat([leaf, sibling]) : Buffer.concat([sibling, leaf])))
  return { hex: Buffer.concat([varint(height), Buffer.from([1]), Buffer.from([2]), ...level]).toString('hex'), root, height }
}
const KEY = 'fixture-monitor'
const STORED_HASH = 'ab'.repeat(32), PROVIDER_HASH = 'cd'.repeat(32)
let headersUp = true, tip = 900200
const roots = new Map() // height -> the root our header service carries
const arc = new Map() // txid -> { hex, height } ARC answers MINED with
const known = new Set() // txids WhatsOnChain's status calls known
const spentBy = new Map() // 'prevtxid:vout' -> the spender WhatsOnChain names
const logs = []
// The Worker's clock: minute 1 or minute 31 of the current hour.
const entry = `import Base from './shim.mjs'
export * from './shim.mjs'
const RealDate = Date
let shift = 0
const at = minute => { const now = RealDate.now(); shift = now - (now % 3600000) + minute * 60000 + 1000 - now }
class ShiftedDate extends RealDate {
  constructor(...a) { if (a.length === 0) super(RealDate.now() + shift); else super(...a) }
  static now() { return RealDate.now() + shift }
}
globalThis.Date = ShiftedDate
at(31)
export default class extends Base {
  async fetch(request) {
    const url = new URL(request.url)
    if (url.pathname === '/__clock') { at(Number(url.searchParams.get('minute'))); return new Response(String(new Date().getUTCMinutes())) }
    return super.fetch(request)
  }
}`
const mf = new Miniflare({ modules: true, script: entry, scriptPath: resolve('build/worker/reorg-gate-entry.mjs'),
  modulesRules: [{ type: 'ESModule', include: ['**/*.js', '**/*.mjs'] }, { type: 'CompiledWasm', include: ['**/*.wasm'] }],
  compatibilityDate: '2024-01-01', host: '127.0.0.1', port: 0,
  bindings: { SERVER_PRIVATE_KEY: '11'.repeat(32), CHAINTRACKS_URL: 'https://headers.test', BROADCASTER: 'arc', MONITOR_TRIGGER_KEY: KEY },
  d1Databases: ['DB'], kvNamespaces: ['AUTH_SESSIONS'], r2Buckets: ['BLOBS'],
  outboundService: async request => {
    const url = new URL(request.url)
    if (url.host === 'headers.test' && url.pathname === '/currentHeight') return Response.json({ status: 'success', value: tip })
    if (url.host === 'headers.test' && url.pathname === '/findHeaderHexForHeight') {
      if (!headersUp) return new Response('unavailable', { status: 503 })
      const height = Number(url.searchParams.get('height'))
      return Response.json({ status: 'success', value: { merkleRoot: roots.get(height) ?? '00'.repeat(32), height } })
    }
    if (url.host === 'api.whatsonchain.com' && url.pathname.includes('/block/height/') && !headersUp) return new Response('unavailable', { status: 503 })
    if (url.host === 'api.whatsonchain.com' && url.pathname.endsWith('/txs/status') && request.method === 'POST') {
      const { txids } = await request.json()
      return Response.json(txids.map(t => known.has(t) ? { txid: t } : { txid: t, error: 'unknown' }))
    }
    const spent = url.host === 'api.whatsonchain.com' && url.pathname.match(/\/tx\/([0-9a-f]{64})\/(\d+)\/spent$/)
    if (spent && spentBy.has(`${spent[1]}:${spent[2]}`)) return Response.json({ txid: spentBy.get(`${spent[1]}:${spent[2]}`), vin: 0, status: 'confirmed' })
    const m = url.pathname.match(/^\/v1\/tx\/([0-9a-f]{64})$/)
    if (url.host.startsWith('arc.') && request.method === 'GET' && m && arc.has(m[1])) {
      const p = arc.get(m[1])
      return Response.json({ txid: m[1], txStatus: 'MINED', blockHeight: p.height, blockHash: PROVIDER_HASH, merklePath: p.hex })
    }
    return new Response('not found', { status: 404 })
  },
  handleStructuredLogs: ({ message }) => { logs.push(String(message).trim()) },
})
let failed = 0, passed = 0
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
  await exec("INSERT INTO output_baskets (user_id, name) VALUES (1, 'default')")
  // A completed transaction of ours, its coin, and its checked proof at `height`.
  const seed = async (marker, height, { req = true } = {}) => {
    const tx = raw(marker), txid = display(hash(tx)), b = bump(tx, height)
    roots.set(height, b.root)
    const t = await exec("INSERT INTO transactions (user_id, reference, status, is_outgoing, description, txid, raw_tx) VALUES (1, ?, 'completed', 1, 'fixture', ?, ?)", `ref-${marker}`, txid, tx)
    await exec("INSERT INTO outputs (user_id, transaction_id, basket_id, spendable, change, vout, satoshis, provided_by, purpose, type, txid) VALUES (1, ?, 1, 1, 1, 0, 1000, 'storage', 'change', 'P2PKH', ?)", t.meta.last_row_id, txid)
    const p = await exec("INSERT INTO proven_txs (txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx) VALUES (?, ?, 0, ?, ?, ?, ?)", txid, height, STORED_HASH, b.root, Buffer.from(b.hex, 'hex'), tx)
    if (req) await exec("INSERT INTO proven_tx_reqs (proven_tx_id, txid, raw_tx, status, attempts) VALUES (?, ?, ?, 'completed', 2)", p.meta.last_row_id, txid, tx)
    return { tx, txid, height, ...b }
  }
  const rowsOf = async s => ({
    proven: await db.prepare('SELECT height, idx, block_hash, merkle_root, hex(merkle_path) AS merkle_path, hex(raw_tx) AS raw_tx, updated_at FROM proven_txs WHERE txid = ?').bind(s.txid).first(),
    req: await db.prepare('SELECT status, attempts, proven_tx_id, updated_at FROM proven_tx_reqs WHERE txid = ?').bind(s.txid).first(),
    tx: await db.prepare('SELECT status, updated_at FROM transactions WHERE txid = ?').bind(s.txid).first(),
    coins: (await db.prepare('SELECT output_id, spendable, spent_by FROM outputs WHERE txid = ?').bind(s.txid).all()).results,
  })
  const clock = async minute => { const r = await mf.dispatchFetch(new URL(`/__clock?minute=${minute}`, base)); assert.equal(await r.text(), String(minute)) }
  const monitor = async () => {
    const since = logs.length
    const r = await mf.dispatchFetch(new URL(`/monitor/run?key=${KEY}`, base), { method: 'POST' })
    assert.equal(r.status, 200, await r.clone().text())
    return { result: await r.json(), since }
  }
  // Log lines arrive asynchronously; wait a moment for each pattern.
  const saw = async (since, pattern) => {
    const deadline = Date.now() + 3000
    const find = () => logs.slice(since).find(l => pattern.test(l))
    while (!find() && Date.now() < deadline) await new Promise(r => setTimeout(r, 25))
    return find()
  }
  const recorded = async s => (await db.prepare("SELECT details FROM monitor_events WHERE event = 'deep_reorg_unrepaired'").all()).results.map(r => JSON.parse(r.details)).filter(d => d.txid === s.txid)
  const reqCount = async s => (await db.prepare('SELECT COUNT(*) AS n FROM proven_tx_reqs WHERE txid = ?').bind(s.txid).first()).n
  const check = async (name, f) => { try { await f(); passed++; console.log(`PASS ${name}`) } catch (e) { failed++; console.log(`FAIL ${name}: ${e.message.split('\n')[0]}`) } }

  // Shallow-sweep rows (within 200 blocks of the tip) and height-drop rows (above the drop).
  const s1 = await seed(1, 900150), s2 = await seed(2, 900152), s3 = await seed(3, 900154)
  const r1 = await seed(11, 900198), r2 = await seed(12, 900199), r3 = await seed(13, 900200), r4 = await seed(14, 900203)
  // r5: a row whose request is no longer held (completed requests are purged after 30 days).
  const r5 = await seed(15, 900201, { req: false })

  // Run 1 (minute 31, tip 900200): the tip is recorded; nothing else moves.
  await monitor()

  // Run 2 (minute 1, same tip): the shallow sweep asks ARC for each recent row.
  const other = raw(99)
  const otherProof = bump(other, 900151); roots.set(900151, otherProof.root)
  arc.set(s1.txid, otherProof) // a true proof of another transaction
  const offRoot = bump(s2.tx, 900153); roots.set(900153, 'ee'.repeat(32))
  arc.set(s2.txid, offRoot) // our txid, a root the header service does not carry
  const moved = bump(s3.tx, 900155, 1); roots.set(900155, moved.root); roots.set(s3.height, 'dd'.repeat(32))
  arc.set(s3.txid, moved) // a true proof at another height, our txid at offset 1
  const before2 = { s1: await rowsOf(s1), s2: await rowsOf(s2) }
  await clock(1)
  const run2 = await monitor()
  await clock(31)
  await check('shallow sweep: a proof of another transaction is not stored; the row is byte-equal and one line names the txid, the provider and the reason', async () => {
    assert.deepEqual(await rowsOf(s1), before2.s1)
    assert.ok(await saw(run2.since, new RegExp(`^shallow_reorg_sweep: proof for ${s1.txid} from arc not stored: BUMP root for ${s1.txid}`)), 'no line')
  })
  await check("shallow sweep: a proof whose root is not the header service's is not stored; the row is byte-equal and one line names it", async () => {
    assert.deepEqual(await rowsOf(s2), before2.s2)
    assert.ok(await saw(run2.since, new RegExp(`^shallow_reorg_sweep: proof for ${s2.txid} from arc not stored: root ${offRoot.root} is NOT canonical at height 900153$`)), 'no line')
  })
  await check("shallow sweep: a checked proof is stored with the gate's root and index", async () => {
    const p = (await rowsOf(s3)).proven
    assert.deepEqual({ height: p.height, idx: p.idx, root: p.merkle_root, path: p.merkle_path.toLowerCase() }, { height: 900155, idx: 1, root: moved.root, path: moved.hex })
    assert.equal((await rowsOf(s3)).req.status, 'completed')
    assert.ok(await saw(run2.since, new RegExp(`^shallow_reorg_sweep: ${s3.txid} proof replaced h=900154→900155 root=${moved.root} idx=1 \\(from arc\\)$`)), 'not stored by the shallow sweep')
  })
  arc.clear()

  // Run 3 (minute 31): the tip drops to 900197. The stored roots of r1 (900198)
  // and r5 (900201) are no longer the chain's: for r1 ARC says "still mined"
  // with that same proof (the gate refuses it), for r5 no provider holds a
  // proof. r2 and
  // r3 are still the chain's while ARC says "not found" (r2) or "still mined"
  // with another transaction's proof (r3).
  roots.set(r1.height, 'dd'.repeat(32)); roots.set(r5.height, 'd5'.repeat(32))
  arc.set(r1.txid, r1)
  arc.set(r3.txid, otherProof)
  const before3 = { r1: await rowsOf(r1), r2: await rowsOf(r2), r3: await rowsOf(r3), r4: await rowsOf(r4), r5: await rowsOf(r5) }
  tip = 900197
  const run3 = await monitor()
  await check('height drop: the stored root is no longer the chain\'s and the provider\'s "still mined" proof is refused: the row, its request, the transaction and its coin are left as they are, and the row is recorded', async () => {
    assert.equal(run3.result.reorg_detected, true)
    assert.deepEqual(await rowsOf(r1), before3.r1)
    assert.ok(await saw(run3.since, new RegExp(`^handle_reorg: ${r1.txid} stored root ${r1.root} is not the chain's at h=900198$`)), 'no evidence line')
    assert.ok(await saw(run3.since, new RegExp(`^handle_reorg: proof for ${r1.txid} from arc not stored: root ${r1.root} is NOT canonical at height 900198$`)), 'no refusal line')
    assert.ok(await saw(run3.since, new RegExp(`^handle_reorg: ${r1.txid} left unrepaired \\(was at height 900198\\): root ${r1.root} is NOT canonical at height 900198; row and request left for the deep sweep$`)), 'no unrepaired line')
    assert.ok((await recorded(r1)).some(d => d.proven_tx_id > 0 && d.height === 900198 && d.block_hash === STORED_HASH && d.merkle_root === r1.root), 'not recorded')
  })
  await check('height drop on a row whose request is no longer held, no provider holding a proof: the row is left as it is, still without a request, and recorded', async () => {
    assert.deepEqual(await rowsOf(r5), before3.r5)
    assert.equal(await reqCount(r5), 0)
    assert.ok(await saw(run3.since, new RegExp(`^handle_reorg: ${r5.txid} left unrepaired \\(was at height 900201\\): no canonical proof available; row and request left for the deep sweep$`)), 'no unrepaired line')
    assert.ok((await recorded(r5)).length > 0, 'not recorded')
  })
  await check('height drop: a stored root that is still the chain\'s is not lowered when the provider says "not found"', async () => {
    assert.deepEqual(await rowsOf(r2), before3.r2)
    assert.ok(await saw(run3.since, new RegExp(`^handle_reorg: ${r2.txid} stored root ${r2.root} is still the chain's at h=900199; nothing lowered$`)), 'no line')
  })
  await check('height drop: a stored root that is still the chain\'s is not lowered or replaced when the provider says "still mined" with another transaction\'s proof', async () => {
    assert.deepEqual(await rowsOf(r3), before3.r3)
  })
  arc.clear()

  // Runs 4-16 (minute 31): the tip climbs one block a run, past the
  // escalation's counted blocks. No provider holds a proof of r1; WhatsOnChain
  // calls r1 unknown and names another, known, spender of its input. A
  // request it knows keeps the status batch from being all unknown.
  const fillerTx = raw(98), filler = display(hash(fillerTx))
  await exec("INSERT INTO proven_tx_reqs (txid, raw_tx, status, attempts) VALUES (?, ?, 'unmined', 0)", filler, fillerTx)
  known.add(filler)
  const competitor = 'c0'.repeat(32)
  spentBy.set(`${'0b'.repeat(32)}:0`, competitor); known.add(competitor)
  for (let i = 0; i < 13; i++) { tip += 1; await monitor() }
  const after4 = { r1: await rowsOf(r1), r5: await rowsOf(r5) }
  await check('after the height drop, the proof passes run past the escalation\'s counted blocks with the explorer naming another spender: the transaction is still completed and its coin still offered', async () => {
    assert.equal(after4.r1.tx.status, 'completed')
    assert.deepEqual(after4.r1.coins, before3.r1.coins)
    assert.deepEqual(after4.r1.req, before3.r1.req)
    assert.deepEqual(after4.r1.proven, before3.r1.proven)
    assert.deepEqual(after4.r5, before3.r5)
    assert.equal(await reqCount(r5), 0)
  })
  spentBy.clear(); known.clear()

  // Runs 17-20 (minute 31): the header service carries r1 and r5 in new
  // blocks and ARC answers with their true proofs. The deep sweep stores
  // each through the gate.
  const new1 = bump(r1.tx, 900202, 1), new5 = bump(r5.tx, 900204, 1)
  roots.set(900202, new1.root); roots.set(900204, new5.root)
  arc.set(r1.txid, new1); arc.set(r5.txid, new5)
  const since6 = logs.length
  for (let i = 0; i < 4; i++) { await monitor(); if ((await rowsOf(r1)).proven.height === 900202 && (await rowsOf(r5)).proven.height === 900204) break }
  await check('then a true proof in a new block: the deep sweep stores it through the gate; the transaction and its coin are as they were', async () => {
    for (const [s, n] of [[r1, new1], [r5, new5]]) {
      const now = await rowsOf(s), was = before3[s === r1 ? 'r1' : 'r5']
      assert.deepEqual({ height: now.proven.height, idx: now.proven.idx, block_hash: now.proven.block_hash, root: now.proven.merkle_root, path: now.proven.merkle_path.toLowerCase(), raw: now.proven.raw_tx },
        { height: n.height, idx: 1, block_hash: PROVIDER_HASH, root: n.root, path: n.hex, raw: was.proven.raw_tx })
      assert.deepEqual(now.tx, was.tx)
      assert.deepEqual(now.coins, was.coins)
      assert.deepEqual(now.req, was.req)
      assert.ok(await saw(since6, new RegExp(`^deep_reorg_sweep: REPAIRED ${s.txid} ${s.height}→${n.height}`)), 'not stored by the deep sweep')
    }
  })
  arc.clear()

  // Run 5 (minute 31): the tip drops to 900201 while the header service is
  // unavailable; ARC says "still mined" for r4 with a proof the gate refuses.
  arc.set(r4.txid, otherProof)
  const before5 = await rowsOf(r4)
  headersUp = false
  tip = 900201
  const run5 = await monitor()
  headersUp = true
  await check('height drop with the header service unavailable: nothing is lowered and nothing is stored, and the pass says so', async () => {
    assert.equal(run5.result.reorg_detected, true)
    assert.deepEqual(await rowsOf(r4), before5)
    assert.ok(await saw(run5.since, new RegExp(`^handle_reorg: ${r4.txid} at h=900203 held: header service did not answer`)), 'no hold line')
  })
  arc.clear()
  console.log(`Reorg gate Worker witnesses: ${passed} passed; ${failed} failed`)
} finally { await mf.dispose() }
if (failed) process.exitCode = 1
